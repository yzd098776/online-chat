#!/usr/bin/env python3
"""在线聊天客户端 v3 —— 可靠性改造：心跳 / 断线重连 / 待发队列 / 离线消息 / 幂等 seq

与既有版本的关系：
  v1 (chat_client_fixed.py)  完整业务 GUI，`|`+`\\n` 文本协议（对照用）
  v2 (chat_client_v2.py)     长度前缀+JSON 帧层骨架（阶段1）
  v3 (本文件)                完整业务 + 可靠性引擎 + GUI/headless 双模式（本提示词）

用法:
  python3 chat_client_v3.py --user alice                          # 图形界面（需 tkinter）
  python3 chat_client_v3.py 127.0.0.1 --port 8888 --user bob      # 指定服务器
  python3 chat_client_v3.py --user alice --headless               # 无 GUI 交互模式（验收/测试用）
  python3 chat_client_v3.py --user alice --headless --ping-interval 1 --dead-timeout 3  # 快速验证参数

headless 命令:
  @用户名 内容     私聊        普通文本    群聊
  /retry          重试全部「失败可重试」消息
  /status         打印待发队列
  /quit           退出（发 LOGOUT）

可靠性行为（与需求一一对应）:
  1' 心跳: 每 ping-interval(默认10s) 发 PING；dead-timeout(默认30s) 无任何下行则判连接死亡
  2' 重连: 指数退避 1s,2s,4s...上限 30s，±20% 抖动（避免同时重连）；成功后自动带原用户名 LOGIN
  3' 待发队列: 发送失败/未确认的消息带 seq 落本地 JSON 文件，重连后按 seq 序补发；
     状态机 发送中 → 已送达(ACK) / 失败可重试(NACK 或重试超限)
  4' 离线消息: 私聊先落服务器 offline_message 表（已送达=已持久化必达），对方上线按时间补发，
     下行 MESSAGE 带 offline=1 时以「离线消息」样式展示；客户端每收一条私聊回 E2EACK
     （端到端确认），服务器据此删行——「转发写成功」证明不了对端应用收到（半开连接会吞写入）
  6' 幂等: 消息 seq per-user 单调递增并持久化；重发复用原 seq，服务器按 (user,seq) 去重；
     接收端再按 (from,seq) 显示去重兜底
"""

import argparse
import json
import os
import random
import socket
import struct
import sys
import threading
import time

MAX_FRAME = 1 << 20  # 1 MiB，与服务器一致
PROTO_VER = 1
MAX_ATTEMPTS = 8     # 单条消息最多自动补发次数，超限标「失败可重试」
JITTER = 0.2         # 退避抖动 ±20%（需求 2：避免多客户端同时重连惊群）


# ---------------- 帧层：struct 读长度头 + recv_exact 循环收满（与 v2 一致） ----------------

def recv_exact(sock, n):
    """循环 recv 直到收满 n 字节（处理半包）；对端关闭抛 ConnectionError"""
    data = bytearray()
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise ConnectionError("连接已关闭")
        data.extend(chunk)
    return bytes(data)


def recv_frame(sock):
    """读一帧 -> dict（JSON body 解析）"""
    header = recv_exact(sock, 4)
    (length,) = struct.unpack(">I", header)  # 4 字节大端 uint32
    if length > MAX_FRAME:
        raise ValueError("帧长 %d 超过 MAX_FRAME，协议错误" % length)
    body = recv_exact(sock, length)
    return json.loads(body.decode("utf-8"))


def send_frame(sock, obj):
    """dict -> JSON body -> 加长度头发出（sendall 处理短写）"""
    body = json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if len(body) > MAX_FRAME:
        raise ValueError("body %d 字节超过 MAX_FRAME" % len(body))
    sock.sendall(struct.pack(">I", len(body)) + body)


def make_msg(msg_type, from_, to_, content, seq, room=""):
    """构造协议 body（7 字段齐全）"""
    return {
        "ver": PROTO_VER,
        "type": msg_type,
        "from": from_,
        "to": to_,
        "room": room,
        "content": content,
        "ts": int(time.time()),
        "seq": seq,
    }


def fmt_ts(ts):
    return time.strftime("%H:%M:%S", time.localtime(ts))


# ---------------- 本地待发队列（需求 3）：JSON 持久化，进程被 kill -9 也不丢 ----------------

class PendingItem(object):
    __slots__ = ("seq", "to", "content", "ts", "attempts", "state", "last_sent")

    def __init__(self, seq, to, content, ts, attempts=0, state="sending", last_sent=0.0):
        self.seq = seq
        self.to = to
        self.content = content
        self.ts = ts
        self.attempts = attempts
        self.state = state          # 'sending'（发送中）| 'failed'（失败可重试）
        self.last_sent = last_sent  # 上次实际发出的时刻（重传超时判定用）

    def to_json(self):
        return {"seq": self.seq, "to": self.to, "content": self.content, "ts": self.ts,
                "attempts": self.attempts, "state": self.state, "last_sent": self.last_sent}

    @staticmethod
    def from_json(d):
        return PendingItem(d["seq"], d["to"], d["content"], d["ts"],
                           d.get("attempts", 0), d.get("state", "sending"),
                           d.get("last_sent", 0.0))


class PendingStore(object):
    """待发队列 + 单调 seq 计数器（需求 6：seq 跨进程持久化，保证 per-user 单调递增）。

    为什么 seq 必须持久化：服务器按 (user, seq) 去重，若客户端重启后 seq 归零重计，
    新消息会被当成历史重复帧丢弃。持久化 next_seq 后，同一用户永远向前计数。
    """

    def __init__(self, path, username):
        self.path = path
        self.username = username
        self._lock = threading.Lock()
        self.next_seq = 1
        self.items = {}  # seq -> PendingItem
        self._load()

    def _load(self):
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, ValueError):
            return
        if data.get("username") not in (None, self.username):
            return  # 文件属于其他用户，不混用
        self.next_seq = int(data.get("next_seq", 1))
        for d in data.get("items", []):
            it = PendingItem.from_json(d)
            self.items[it.seq] = it

    def _save(self):
        data = {"username": self.username, "next_seq": self.next_seq,
                "items": [it.to_json() for it in sorted(self.items.values(), key=lambda x: x.seq)]}
        tmp = self.path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=1)
            os.replace(tmp, self.path)  # 原子替换，防写一半被 kill
        except OSError as e:
            sys.stderr.write("[警告] 待发队列持久化失败: %r\n" % e)

    def alloc_seq(self):
        """分配单调递增 seq 并立刻持久化（消息/PING/LOGIN 共用同一计数器）。

        取 max(本地计数, 当前毫秒) 作为前沿：本地文件丢失/换机重装时 seq 不会
        归零撞上服务器 seen_message 里的历史 (user, seq)（那会让新消息被当重复
        帧吞掉）。同一毫秒内两个实例并发发号是残余边界，见 PROTOCOL.md。
        重发【不】走这里——复用 PendingItem.seq（幂等键，需求 6）。
        """
        with self._lock:
            floor = int(time.time() * 1000)
            if self.next_seq <= floor:
                self.next_seq = floor + 1
            seq = self.next_seq
            self.next_seq += 1
            self._save()
            return seq

    def add(self, item):
        with self._lock:
            self.items[item.seq] = item
            self._save()

    def remove(self, seq):
        with self._lock:
            self.items.pop(seq, None)
            self._save()

    def update(self, item):
        with self._lock:
            self.items[item.seq] = item
            self._save()

    def get(self, seq):
        with self._lock:
            return self.items.get(seq)

    def snapshot(self):
        with self._lock:
            return sorted(self.items.values(), key=lambda x: x.seq)


# ---------------- 可靠性引擎（GUI/headless 共用） ----------------

class ChatSession(object):
    """连接状态机 + 心跳 + 退避重连 + 待发队列补发。

    线程模型：
      conn 线程     —— 唯一拥有 socket 生命周期：connect → LOGIN → 阻塞收帧 → 断开 → 退避重连
      heartbeat 线程 —— 发 PING、无响应判死、重传超时未确认消息
      调用方线程     —— send()/retry_failed()（线程安全，可来自 GUI 主线程或 stdin 线程）
    回调在【工作线程】触发；Tk 层必须用 root.after 转回主线程（Tk 不允许跨线程碰控件）。
    """

    STATUS_IDLE = "未连接"
    STATUS_CONNECTING = "连接中"

    def __init__(self, host, port, username, pending_path=None,
                 ping_interval=10.0, dead_timeout=30.0, ack_timeout=15.0,
                 backoff_max=30.0, on_status=None, on_chat=None, on_sys=None,
                 on_msg_state=None, on_userlist=None, on_log=None):
        self.host = host
        self.port = port
        self.username = username
        self.ping_interval = ping_interval
        self.dead_timeout = dead_timeout
        self.ack_timeout = ack_timeout
        self.backoff_max = backoff_max

        self.on_status = on_status or (lambda s: None)
        self.on_chat = on_chat or (lambda m: None)
        self.on_sys = on_sys or (lambda s: None)
        self.on_msg_state = on_msg_state or (lambda seq, state, reason: None)
        self.on_userlist = on_userlist or (lambda names: None)
        self.on_log = on_log or (lambda s: None)

        if pending_path is None:
            pending_path = "chat_pending_%s.json" % username
        self.store = PendingStore(pending_path, username)

        self._sock = None
        self._io_lock = threading.Lock()       # 保护 _sock 的读写替换（写侧 send + 判亡 close）
        self._running = False
        self._stop_ev = threading.Event()
        self._conn_thread = None
        self._hb_thread = None
        self._last_rx = 0.0
        self._last_ping = 0.0
        self._rx_seen = set()                  # (from, seq) 显示去重（需求 6 兜底）
        self._connected = False

    # ---------- 对外 API ----------

    def start(self):
        self._running = True
        self._stop_ev.clear()
        self._conn_thread = threading.Thread(target=self._conn_loop, name="conn", daemon=True)
        self._hb_thread = threading.Thread(target=self._heartbeat_loop, name="heartbeat", daemon=True)
        self._conn_thread.start()
        self._hb_thread.start()

    def stop(self):
        """优雅退出：发 LOGOUT（尽力而为）→ 停线程"""
        self._running = False
        self._stop_ev.set()
        try:
            self._send_frame(make_msg("LOGOUT", self.username, "SERVER", "退出",
                                      self.store.alloc_seq()), optional=True)
        except (OSError, ValueError):
            pass
        self._drop_socket()
        for t in (self._conn_thread, self._hb_thread):
            if t and t.is_alive():
                t.join(timeout=2.0)
        self.on_status(self.STATUS_IDLE)

    def send(self, to, content):
        """发送一条消息：先入待发队列（带 seq）再尽力立即发出。返回 seq。"""
        seq = self.store.alloc_seq()
        item = PendingItem(seq, to, content, int(time.time()))
        self.store.add(item)
        self.on_msg_state(seq, "发送中", "")
        self._try_send_item(item)
        return seq

    def retry_failed(self, seq=None):
        """把「失败可重试」重新入列（需求 3）；seq=None 表示全部重试"""
        for it in self.store.snapshot():
            if it.state != "failed":
                continue
            if seq is not None and it.seq != seq:
                continue
            it.state = "sending"
            it.attempts = 0
            self.store.update(it)
            self.on_msg_state(it.seq, "发送中", "")
            self._try_send_item(it)

    def is_connected(self):
        return self._connected

    # ---------- 内部：发送路径 ----------

    def _send_frame(self, obj, optional=False):
        """写一帧到当前 socket；失败抛 OSError（optional=True 时吞掉）"""
        with self._io_lock:
            sock = self._sock
            if sock is None:
                if optional:
                    return
                raise OSError("连接不可用")
            try:
                send_frame(sock, obj)
            except OSError:
                if optional:
                    return
                raise

    def _try_send_item(self, item):
        """立即发送一条待发消息；连接不可用/失败则留在队列（重连后补发）"""
        if item.attempts >= MAX_ATTEMPTS:
            item.state = "failed"
            self.store.update(item)
            self.on_msg_state(item.seq, "失败可重试", "补发超限")
            return
        obj = make_msg("MESSAGE", self.username, item.to, item.content, item.seq)
        obj["ts"] = item.ts  # 保留最初发送时间（离线消息按时间排序的基础，需求 4）
        try:
            self._send_frame(obj)
        except (OSError, ValueError):
            return  # 留在待发队列，等重连/重传
        item.attempts += 1
        item.last_sent = time.time()
        self.store.update(item)

    def _flush_pending(self):
        """重连成功后按 seq 序补发（需求 3）；对服务器是重复帧也没关系——(user,seq) 去重（需求 6）"""
        items = self.store.snapshot()
        if not items:
            return
        self.on_log("[补发] 待发队列 %d 条，按 seq 序补发" % len(items))
        for it in items:
            if it.state != "sending":
                continue
            self._try_send_item(it)

    # ---------- 内部：连接状态机（需求 2） ----------

    def _set_status(self, text):
        self.on_status(text)

    def _drop_socket(self):
        with self._io_lock:
            sock = self._sock
            self._sock = None
            self._connected = False
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                sock.close()
            except OSError:
                pass

    def _conn_loop(self):
        attempt = 0
        while self._running:
            # ---- 阶段 1：连接（首次「连接中」；失败后「重连第 N 次」+ 指数退避） ----
            if attempt == 0:
                self._set_status(self.STATUS_CONNECTING)
            else:
                self._set_status("重连第 %d 次" % attempt)
                base = min(self.backoff_max, 2 ** (attempt - 1))          # 1,2,4,...,30
                delay = base * random.uniform(1.0 - JITTER, 1.0 + JITTER)  # ±20% 抖动
                delay = min(delay, self.backoff_max)
                self.on_log("[重连] 第 %d 次，%.1fs 后重试" % (attempt, delay))
                if self._stop_ev.wait(delay):  # 可中断退避
                    break
                self._set_status(self.STATUS_CONNECTING)
            try:
                sock = socket.create_connection((self.host, self.port), timeout=5)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                sock.settimeout(None)  # 收帧阻塞；判死交给心跳线程（应用层 PING，见服务器 Q1）
            except OSError as e:
                self.on_log("[连接失败] %s:%d -> %r" % (self.host, self.port, e))
                attempt += 1
                continue

            with self._io_lock:
                self._sock = sock
            self._last_rx = time.time()
            self._last_ping = time.time()
            try:
                # 需求 2：重连成功后自动带原用户名重新 LOGIN
                self._send_frame(make_msg("LOGIN", self.username, "SERVER", "登录",
                                          self.store.alloc_seq()))
                self._connected = True
                self._flush_pending()  # 需求 3：按序补发
            except (OSError, ValueError) as e:
                self.on_log("[登录失败] %r" % e)
                self._drop_socket()
                attempt += 1
                continue

            self._set_status("已连接")
            self.on_log("[连接] %s:%d 已连接并 LOGIN(%s)" % (self.host, self.port, self.username))
            attempt = 0

            # ---- 阶段 2：收帧直到出错 ----
            try:
                self._recv_loop(sock)
            except (OSError, ValueError, ConnectionError) as e:
                if self._running:
                    self.on_log("[断开] %r" % e)
            self._drop_socket()
            if not self._running:
                break
            attempt = 1  # 断开后进入重连循环（先退避 1s 抖动再试）
        self._set_status(self.STATUS_IDLE)

    def _recv_loop(self, sock):
        while self._running:
            frame = recv_frame(sock)
            self._last_rx = time.time()  # 任何下行都刷新活跃时间
            self._dispatch(frame)

    def _dispatch(self, frame):
        ftype = frame.get("type", "")
        if ftype == "PONG":
            pass  # 心跳应答：last_rx 已刷新，不刷日志（判死/重连才打日志）
        elif ftype == "ACK":
            # 需求 3：content = 被确认的客户端 seq → 「已送达」
            try:
                seq = int(frame.get("content", "0"))
            except ValueError:
                return
            if self.store.get(seq) is None:
                return  # 迟到/重复 ACK：状态早已置为已送达，不再刷状态行
            self.store.remove(seq)
            self.on_msg_state(seq, "已送达", "")
        elif ftype == "NACK":
            try:
                seq = int(frame.get("content", "0"))
            except ValueError:
                return
            reason = frame.get("reason", "")
            it = self.store.get(seq)
            if it:
                it.state = "failed"
                self.store.update(it)
            self.on_msg_state(seq, "失败可重试", reason)
            self.on_log("[失败] seq=%d %s" % (seq, reason))
        elif ftype == "MESSAGE":
            try:
                key = (frame.get("from", ""), int(frame.get("seq", 0)))
            except (TypeError, ValueError):
                return
            # 端到端确认（需求 4）：私聊收帧即回 E2EACK，服务器据此删 offline_message 行。
            # 只有 E2EACK 能删行——「转发写成功」证明不了对端应用收到（半开连接会吞写入）。
            # 显示去重命中的重复帧【也要回】：行不删掉，下次登录还会再推一遍。
            if frame.get("to") != "ALL" and key[1] > 0:
                self._send_frame(make_msg("E2EACK", self.username, frame.get("from", ""),
                                          str(key[1]), self.store.alloc_seq()), optional=True)
            if key in self._rx_seen:
                self.on_log("[去重] (%s, %s) 重复消息已忽略（需求 6 兜底）" % key)
                return
            self._rx_seen.add(key)
            self.on_chat(frame)
        elif ftype == "SYSTEM":
            self.on_sys(frame.get("content", ""))
        elif ftype == "USERLIST":
            names = frame.get("content", [])
            if isinstance(names, list):
                self.on_userlist(names)
        else:
            self.on_log("[未知帧] %s" % json.dumps(frame, ensure_ascii=False))

    # ---------- 内部：心跳与重传（需求 1） ----------

    def _heartbeat_loop(self):
        while self._running:
            if self._stop_ev.wait(0.2):
                break
            if not self._connected:
                continue
            now = time.time()
            # 1) 周期 PING（服务器回 PONG 并刷新 last_active）
            if now - self._last_ping >= self.ping_interval:
                self._last_ping = now
                try:
                    self._send_frame(make_msg("PING", self.username, "SERVER",
                                              "hb", self.store.alloc_seq()))
                except (OSError, ValueError):
                    self._drop_socket()  # 唤醒 conn 线程进入重连
                    continue
            # 2) 判死：dead_timeout 内无任何下行 → 主动断开重连（半开连接自愈）
            if now - self._last_rx > self.dead_timeout:
                self.on_log("[心跳] %.0fs 无响应，判连接死亡，强制重连" % (now - self._last_rx))
                self._drop_socket()
                continue
            # 3) 重传超时未确认的消息（ACK 丢失但连接还在的弱网场景）
            for it in self.store.snapshot():
                if it.state != "sending" or it.last_sent <= 0:
                    continue
                if now - it.last_sent < self.ack_timeout:
                    continue
                self.on_log("[重传] seq=%d 超过 %.0fs 未确认" % (it.seq, self.ack_timeout))
                self._try_send_item(it)


# ---------------- headless 模式（验收/自动化测试） ----------------

def run_headless(host, port, user, args):
    def on_status(s):
        print("[状态] %s" % s, flush=True)

    def on_log(s):
        print(s, flush=True)

    def on_sys(s):
        print("[系统] %s" % s, flush=True)

    def on_userlist(names):
        print("[在线] %s" % ",".join(names), flush=True)

    def on_msg_state(seq, state, reason):
        extra = (" reason=%s" % reason) if reason else ""
        if state == "已送达":
            print("[送达] seq=%d 已送达%s" % (seq, extra), flush=True)
        elif state == "失败可重试":
            print("[失败] seq=%d 失败可重试%s" % (seq, extra), flush=True)
        else:
            print("[状态] seq=%d %s%s" % (seq, state, extra), flush=True)

    def on_chat(m):
        tag = "[离线消息] " if m.get("offline") else ""
        who = m.get("from", "?")
        to = m.get("to", "")
        dest = "所有人" if to == "ALL" else "我"
        print("[消息] %s%s → %s: %s" % (tag, who, dest, m.get("content", "")), flush=True)

    sess = ChatSession(host, port, user, pending_path=args.pending_file,
                       ping_interval=args.ping_interval, dead_timeout=args.dead_timeout,
                       ack_timeout=args.ack_timeout, backoff_max=args.backoff_max,
                       on_status=on_status, on_chat=on_chat, on_sys=on_sys,
                       on_msg_state=on_msg_state, on_userlist=on_userlist, on_log=on_log)
    sess.start()
    print("[提示] headless 模式：@用户 内容=私聊，普通文本=群聊，/retry /status /quit", flush=True)
    try:
        for line in sys.stdin:
            line = line.rstrip("\n")
            if not line:
                continue
            if line == "/quit":
                break
            elif line == "/retry":
                sess.retry_failed()
            elif line == "/status":
                for it in sess.store.snapshot():
                    print("[队列] seq=%d to=%s state=%s attempts=%d content=%r"
                          % (it.seq, it.to, it.state, it.attempts, it.content), flush=True)
            elif line.startswith("@"):
                sp = line.find(" ")
                if sp <= 1:
                    print("[用法] @用户名 内容", flush=True)
                    continue
                sess.send(line[1:sp], line[sp + 1:])
            else:
                sess.send("ALL", line)
    except (KeyboardInterrupt, EOFError):
        pass
    sess.stop()
    print("[状态] 未连接", flush=True)


# ---------------- 图形界面（需求 2/3/4 的 UI 呈现） ----------------

def run_gui(host, port, user, args):
    import tkinter as tk
    from tkinter import scrolledtext, messagebox

    class App(object):
        def __init__(self):
            self.sess = None
            self.seq_marks = {}   # seq → (mark名, 当前状态文本)——原地更新「发送中/已送达/失败」
            self.retryable = {}   # seq → True（失败可重试，点击行可重试）

            self.root = tk.Tk()
            self.root.title("在线聊天 v3（可靠性：心跳/重连/待发队列/离线消息）")
            self.root.geometry("760x500")
            self.root.protocol("WM_DELETE_WINDOW", self.on_closing)

            # ---- 顶部：连接设置 ----
            conn = tk.Frame(self.root)
            conn.pack(fill=tk.X, padx=8, pady=4)
            tk.Label(conn, text="服务器:").pack(side=tk.LEFT)
            self.host_entry = tk.Entry(conn, width=14)
            self.host_entry.insert(0, host)
            self.host_entry.pack(side=tk.LEFT, padx=2)
            tk.Label(conn, text="端口:").pack(side=tk.LEFT)
            self.port_entry = tk.Entry(conn, width=6)
            self.port_entry.insert(0, str(port))
            self.port_entry.pack(side=tk.LEFT, padx=2)
            tk.Label(conn, text="用户名:").pack(side=tk.LEFT)
            self.user_entry = tk.Entry(conn, width=10)
            self.user_entry.insert(0, user or "")
            self.user_entry.pack(side=tk.LEFT, padx=2)
            self.connect_btn = tk.Button(conn, text="连接", width=8, command=self.toggle_conn)
            self.connect_btn.pack(side=tk.LEFT, padx=4)
            self.retry_btn = tk.Button(conn, text="重试失败消息", width=12,
                                       command=self.on_retry_all, state=tk.DISABLED)
            self.retry_btn.pack(side=tk.LEFT, padx=4)

            # ---- 中部：消息区 + 在线列表 ----
            mid = tk.Frame(self.root)
            mid.pack(fill=tk.BOTH, expand=True, padx=8, pady=4)
            self.log = scrolledtext.ScrolledText(mid, state=tk.DISABLED, wrap=tk.WORD)
            self.log.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
            self.log.tag_configure("offline", foreground="#777777", font=("TkDefaultFont", 9, "italic"))
            self.log.tag_configure("sys", foreground="#0066cc")
            self.log.tag_configure("ok", foreground="#008800")
            self.log.tag_configure("bad", foreground="#cc0000")
            self.log.tag_configure("pending", foreground="#aa6600")
            self.log.bind("<Button-1>", self.on_log_click)  # 点「失败可重试」行 → 单条重试

            side = tk.Frame(mid)
            side.pack(side=tk.RIGHT, fill=tk.Y, padx=(6, 0))
            tk.Label(side, text="在线用户").pack(anchor="w")
            self.userlist = tk.Listbox(side, width=14, exportselection=False)
            self.userlist.pack(fill=tk.Y, expand=True)
            self.userlist.bind("<<ListboxSelect>>", self.on_user_pick)

            # ---- 底部：输入 ----
            bottom = tk.Frame(self.root)
            bottom.pack(fill=tk.X, padx=8, pady=4)
            self.input = tk.Entry(bottom)
            self.input.pack(side=tk.LEFT, fill=tk.X, expand=True)
            self.input.bind("<Return>", lambda e: self.on_send())
            tk.Button(bottom, text="发送", width=8, command=self.on_send).pack(side=tk.LEFT, padx=4)

            # ---- 状态栏（需求 2：连接中/已连接/重连第 N 次）----
            self.status = tk.Label(self.root, text="未连接", anchor="w", relief=tk.SUNKEN)
            self.status.pack(fill=tk.X, side=tk.BOTTOM)

            self.append_log("就绪。输入 @用户名 内容 私聊，普通文本群聊。", "sys")

        # ---------- 回调（工作线程触发 → root.after 转主线程） ----------
        def cb_status(self, s):
            self.root.after(0, lambda: self.status.config(text=s))

        def cb_log(self, s):
            self.root.after(0, lambda: self.append_log(s))

        def cb_sys(self, s):
            self.root.after(0, lambda: self.append_log("[系统] " + s, "sys"))

        def cb_userlist(self, names):
            def upd():
                self.userlist.delete(0, tk.END)
                for n in names:
                    self.userlist.insert(tk.END, n)
            self.root.after(0, upd)

        def cb_chat(self, m):
            def show():
                tag = ""
                style = None
                if m.get("offline"):
                    tag = "[离线消息] "
                    style = "offline"  # 需求 4：「离线消息」样式（灰斜体）
                who = m.get("from", "?")
                to = m.get("to", "")
                dest = "所有人" if to == "ALL" else "我"
                line = "%s[%s] %s → %s: %s" % (tag, fmt_ts(m.get("ts", time.time())), who, dest,
                                               m.get("content", ""))
                self.append_log(line, style)
            self.root.after(0, show)

        def cb_msg_state(self, seq, state, reason):
            self.root.after(0, lambda: self.update_msg_state(seq, state, reason))

        # ---------- 消息气泡状态列（需求 3：发送中/已送达/失败可重试） ----------
        def append_sent(self, seq, to, content):
            to_disp = "所有人" if to == "ALL" else to
            line = "[%s] 我 → %s: %s  " % (fmt_ts(time.time()), to_disp, content)
            mark = "m%d" % seq
            self.log.config(state=tk.NORMAL)
            self.log.insert(tk.END, line)
            self.log.mark_set(mark, tk.END)          # 记录状态文本起点（mark 不随上方插入移动）
            self.log.mark_gravity(mark, tk.LEFT)
            self.log.insert(tk.END, "[发送中]", "pending")
            self.log.insert(tk.END, "\n")
            self.log.config(state=tk.DISABLED)
            self.log.see(tk.END)
            self.seq_marks[seq] = (mark, "[发送中]", "pending")

        def update_msg_state(self, seq, state, reason):
            info = self.seq_marks.get(seq)
            style = {"已送达": "ok", "失败可重试": "bad"}.get(state, "pending")
            text = "[%s]" % state
            if state == "失败可重试" and reason:
                text = "[失败可重试:%s]" % reason
            self.log.config(state=tk.NORMAL)
            if info:
                mark, old_text, _ = info
                start = self.log.index(mark)
                end = "%s+%dc" % (start, len(old_text))
                self.log.delete(start, end)
                self.log.insert(start, text, style)
                self.seq_marks[seq] = (mark, text, style)
            else:
                self.log.insert(tk.END, "[消息 seq=%d] %s\n" % (seq, text), style)
            self.log.config(state=tk.DISABLED)
            self.retryable[seq] = (state == "失败可重试")
            self.retry_btn.config(
                state=tk.NORMAL if any(self.retryable.values()) else tk.DISABLED)

        # ---------- 交互 ----------
        def append_log(self, text, style=None):
            self.log.config(state=tk.NORMAL)
            if style:
                self.log.insert(tk.END, text + "\n", style)
            else:
                self.log.insert(tk.END, text + "\n")
            self.log.config(state=tk.DISABLED)
            self.log.see(tk.END)

        def toggle_conn(self):
            if self.sess is None or not self.sess.is_connected():
                self.do_connect()
            else:
                self.do_disconnect()

        def do_connect(self):
            name = self.user_entry.get().strip()
            if not name:
                messagebox.showwarning("提示", "请填写用户名")
                return
            try:
                h = self.host_entry.get().strip()
                p = int(self.port_entry.get())
            except ValueError:
                messagebox.showwarning("提示", "端口必须是整数")
                return
            if self.sess is not None:
                self.sess.stop()
            self.sess = ChatSession(
                h, p, name, pending_path=args.pending_file,
                ping_interval=args.ping_interval, dead_timeout=args.dead_timeout,
                ack_timeout=args.ack_timeout, backoff_max=args.backoff_max,
                on_status=self.cb_status, on_chat=self.cb_chat, on_sys=self.cb_sys,
                on_msg_state=self.cb_msg_state, on_userlist=self.cb_userlist,
                on_log=self.cb_log)
            self.sess.start()
            self.connect_btn.config(text="断开")

        def do_disconnect(self):
            if self.sess is not None:
                self.sess.stop()
                self.sess = None
            self.connect_btn.config(text="连接")
            self.status.config(text="未连接")

        def on_send(self):
            if self.sess is None or not self.sess.is_connected():
                self.append_log("[提示] 未连接（连接后可发送；断线期间消息会进入待发队列）", "sys")
                return
            text = self.input.get()
            if not text:
                return
            self.input.delete(0, tk.END)
            if text.startswith("@") and " " in text:
                sp = text.find(" ")
                to, content = text[1:sp], text[sp + 1:]
            else:
                to, content = "ALL", text
            seq = self.sess.send(to, content)  # 立即显示「发送中」，ACK 后变「已送达」
            self.append_sent(seq, to, content)

        def on_retry_all(self):
            if self.sess:
                self.sess.retry_failed()

        def on_log_click(self, event):
            # 点击「失败可重试」行 → 单条重试（需求 3）
            index = "@%d,%d" % (event.x, event.y)
            click_line = self.log.index(index + " linestart")
            for seq, info in self.seq_marks.items():
                mark = info[0]
                if self.retryable.get(seq) and self.log.index(mark + " linestart") == click_line:
                    if self.sess:
                        self.sess.retry_failed(seq)
                    return

        def on_user_pick(self, event):
            sel = self.userlist.curselection()
            if not sel:
                return
            name = self.userlist.get(sel[0])
            cur = self.input.get()
            self.input.delete(0, tk.END)
            self.input.insert(0, "@%s %s" % (name, cur))

        def on_closing(self):
            if self.sess is not None:
                self.sess.stop()
            self.root.destroy()

        def run(self):
            self.root.mainloop()

    App().run()


# ---------------- 入口 ----------------

def main():
    ap = argparse.ArgumentParser(description="在线聊天客户端 v3（可靠性改造）")
    ap.add_argument("host", nargs="?", default="localhost", help="服务器地址（默认 localhost）")
    ap.add_argument("--port", type=int, default=8888, help="端口（默认 8888）")
    ap.add_argument("--user", default="", help="用户名（必填）")
    ap.add_argument("--headless", action="store_true", help="无 GUI 交互模式（验收/测试）")
    ap.add_argument("--pending-file", default=None,
                    help="待发队列持久化文件（默认 chat_pending_<user>.json）")
    ap.add_argument("--ping-interval", type=float, default=10.0, help="PING 周期秒数（默认 10）")
    ap.add_argument("--dead-timeout", type=float, default=30.0,
                    help="无响应判死秒数（默认 30，与服务器剔除阈值一致）")
    ap.add_argument("--ack-timeout", type=float, default=15.0, help="ACK 等待超时/重传秒数（默认 15）")
    ap.add_argument("--backoff-max", type=float, default=30.0, help="重连退避上限秒数（默认 30）")
    args = ap.parse_args()

    if not args.user:
        args.user = "user%d" % (os.getpid() % 10000)
        print("[提示] 未指定 --user，临时使用 %s" % args.user)

    if args.headless:
        run_headless(args.host, args.port, args.user, args)
        return
    try:
        import tkinter  # noqa: F401
        run_gui(args.host, args.port, args.user, args)
    except ImportError:
        print("[提示] 本机无 tkinter，改用 headless 模式（装 python3-tk 可启用 GUI）")
        run_headless(args.host, args.port, args.user, args)


if __name__ == "__main__":
    main()
