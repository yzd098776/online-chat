#!/usr/bin/env python3
"""在线聊天客户端 v4 —— 账号/房间/历史分页（配合 chat_server_v5.cpp）

与既有版本的关系：
  v1 (chat_client_fixed.py)  完整业务 GUI，`|`+`\\n` 文本协议（对照用）
  v2 (chat_client_v2.py)     长度前缀+JSON 帧层骨架
  v3 (chat_client_v3.py)     可靠性引擎（心跳/重连/待发队列/离线消息/幂等 seq）——本文件在其上增量
  v4 (本文件)                注册/登录/Token 免密重连 + 房间 + (ts,id) 游标历史分页（本提示词）

用法:
  python3 chat_client_v4.py --user alice --password 123        # GUI：口令登录（需 tkinter）
  python3 chat_client_v4.py --user alice                       # GUI：有会话 Token 则免密恢复
  python3 chat_client_v4.py --user alice --password 123 --headless
  python3 chat_client_v4.py --headless                         # 空启动：/register 或 /login 后进聊天

headless 命令:
  /register 用户名 密码   注册（一次性请求，不登录）
  /login 用户名 密码      登录（建立会话；断线后带 Token 自动免密重连）
  /join 房间名            加入房间（进入后自动拉最近 50 条历史）
  /leave                  离开当前房间
  /rooms                  列出全部房间
  /create 房间名          创建房间
  /older                  向上翻一页历史（ts 游标，GUI 里直接滚动到顶也会自动翻）
  /inbox                  拉私信收件箱最近 50 条
  @用户名 内容            私聊        普通文本    当前房间群聊
  /retry /status /quit    重试失败 / 待发队列 / 退出

可靠性行为沿用 v3（心跳/退避重连/待发队列/(user,seq) 幂等）：
  重连免密（本提示词）：Token 存 chat_session_<user>.json，重连先 AUTH(token)，
  失效（E1006）则回落内存中的口令登录（挑战-应答：LOGIN_HELLO → CHALLENGE →
  LOGIN{HMAC(PBKDF2(pwd,salt), nonce)}，口令/派生密钥不进帧，见 PROTOCOL.md 6.1）；
  口令只驻内存不落盘。
"""

import argparse
import hashlib
import hmac
import json
import os
import random
import socket
import ssl
import struct
import sys
import threading
import time

MAX_FRAME = 1 << 20  # 1 MiB，与服务器一致
PROTO_VER = 1
MAX_ATTEMPTS = 8     # 单条消息最多自动补发次数，超限标「失败可重试」
JITTER = 0.2         # 退避抖动 ±20%（避免多客户端同时重连惊群）
HIST_PAGE = 50       # 与服务器 kHistPageSize 一致


# ---------------- 帧层（与 v2/v3 一致） ----------------

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


# ---------------- 挑战-应答凭据（Q7：口令/K 不进帧） ----------------

def derive_key(password, salt_hex):
    """K = PBKDF2-HMAC-SHA256(pwd, salt, 100000) → 32B。与服务器 users.pwd_hash 同源。

    注册时 K 直接入库（播种，一次性）；登录时只用 K 算 HMAC 证明，K 本身不上线。
    """
    return hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"),
                               bytes.fromhex(salt_hex), 100000, 32)


def challenge_proof(derived, op, user, nonce_hex):
    """proof = HMAC-SHA256(K, op‖user‖nonce)（64 hex）。

    nonce 一次性过期 → 证明抗重放；op/user 进 HMAC 上下文 = 绑定协议语境与身份
    （Q7 纵深防御：跨协议/跨用户证明混淆的面彻底关死）。
    """
    return hmac.new(derived, (op + user).encode("utf-8") + bytes.fromhex(nonce_hex),
                    hashlib.sha256).hexdigest()


def wrap_ssl(sock, ca_file="", strict=False, hostname="localhost"):
    """C：给已建立的 TCP 连接套 TLS（客户端侧）。

    strict=False（教学默认）：自签证书跳过校验（CERT_NONE）——链路加密但不验身份，
      只防被动窃听，防不了主动中间人（README「已知限制」）。
    strict=True：用 ca_file（自签时即 server.crt）验链 + 校验主机名（SAN）。
    """
    if strict:
        ctx = ssl.create_default_context(cafile=ca_file or None)
        if ca_file:
            ctx.check_hostname = True
        return ctx.wrap_socket(sock, server_hostname=hostname)
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx.wrap_socket(sock, server_hostname=hostname)


def fmt_ts(ts):
    return time.strftime("%H:%M:%S", time.localtime(ts))


# ---------------- 本地会话 Token（重连免密，服务器 Q4） ----------------

class TokenStore(object):
    """chat_session_<user>.json：{username, token, token_exp}。

    为什么 Token 落盘而口令不落盘：Token 是可过期、可轮换的会话凭证（泄露可等它过期/
    被服务器顶号换掉），口令是长期密钥——落盘 Token 把「免密重连」的持久性要求与
    「口令绝不写盘」的安全要求解耦。文件权限 0600（POSIX）。
    """

    def __init__(self, path, username):
        self.path = path
        self.username = username
        self.token = ""
        self.token_exp = 0
        self._load()

    def _load(self):
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, ValueError):
            return
        if data.get("username") != self.username:
            return
        self.token = data.get("token", "")
        self.token_exp = int(data.get("token_exp", 0))

    def save(self, token, exp):
        self.token = token
        self.token_exp = int(exp)
        tmp = self.path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump({"username": self.username, "token": self.token,
                           "token_exp": self.token_exp}, f, ensure_ascii=False)
            try:
                os.chmod(tmp, 0o600)  # 会话凭证只属主可读
            except OSError:
                pass
            os.replace(tmp, self.path)
        except OSError as e:
            sys.stderr.write("[警告] 会话 Token 持久化失败: %r\n" % e)

    def clear(self):
        self.token = ""
        self.token_exp = 0
        try:
            os.remove(self.path)
        except OSError:
            pass

    def valid(self):
        return bool(self.token) and time.time() < self.token_exp


def register_account(host, port, username, password, timeout=10.0,
                     use_tls=False, ca_file="", tls_strict=False):
    """注册专用一次性请求（挑战-应答，Q7②）：
    连接 → REGISTER_HELLO → CHALLENGE{nonce, salt} → REGISTER{K, hmac} → REGISTER_OK/ERR。

    口令不进帧：K=PBKDF2(pwd, salt) 是入库播种值（一次性送达，等价库内容），
    hmac=HMAC(K, nonce) 绑定本次挑战（注册帧不可重放）。
    独立于 ChatSession：注册不需要（也不应该）建立聊天会话。
    返回 (ok, err_code, text)。同步阻塞，适合 GUI 按钮 / headless / 自动化测试。
    """
    sock = socket.create_connection((host, port), timeout=timeout)
    if use_tls:
        sock = wrap_ssl(sock, ca_file, tls_strict, hostname=host)
    try:
        send_frame(sock, make_msg("REGISTER_HELLO", username, "SERVER", "", 0))
        frame = recv_frame(sock)
        if frame.get("type") == "ERR":
            return False, int(frame.get("code", 0)), frame.get("content", "注册失败")
        if frame.get("type") != "CHALLENGE":
            return False, 0, "意外响应: %s" % frame.get("type")
        derived = derive_key(password, frame.get("salt", ""))
        proof = challenge_proof(derived, "REGISTER", username, frame.get("content", ""))
        body = make_msg("REGISTER", username, "SERVER", derived.hex(), 0)
        body["hmac"] = proof
        send_frame(sock, body)
        frame = recv_frame(sock)
        if frame.get("type") == "REGISTER_OK":
            return True, 0, frame.get("content", "注册成功")
        if frame.get("type") == "ERR":
            return False, int(frame.get("code", 0)), frame.get("content", "注册失败")
        return False, 0, "意外响应: %s" % frame.get("type")
    finally:
        try:
            sock.close()
        except OSError:
            pass


# ---------------- 本地待发队列（沿用 v3） ----------------

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
    """待发队列 + 单调 seq 计数器（per-user 单调递增；重发复用原 seq 做幂等键）。

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
        """分配单调递增 seq 并立刻持久化（消息/PING/控制帧共用计数器）。

        取 max(本地计数, 当前毫秒) 作为前沿：本地文件丢失/换机重装时 seq 不会
        归零撞上服务器去重窗口里的历史 (user, seq)。重发【不】走这里——复用
        PendingItem.seq（幂等键）。
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


# ---------------- 会话引擎（GUI/headless 共用） ----------------

class ChatSession(object):
    """连接状态机 + 认证（LOGIN/AUTH）+ 房间 + 历史分页 + v3 可靠性引擎。

    线程模型沿用 v3：
      conn 线程     —— 唯一拥有 socket 生命周期：connect → AUTH/LOGIN → 阻塞收帧 → 断开 → 退避重连
      heartbeat 线程 —— 发 PING、无响应判死、重传超时未确认消息
      调用方线程     —— send()/join_room()/fetch_history()...（线程安全）
    回调在【工作线程】触发；Tk 层必须用 root.after 转回主线程。
    """

    STATUS_IDLE = "未连接"
    STATUS_CONNECTING = "连接中"

    def __init__(self, host, port, username, password="", pending_path=None, session_path=None,
                 ping_interval=10.0, dead_timeout=30.0, ack_timeout=15.0, backoff_max=30.0,
                 use_tls=False, ca_file="", tls_strict=False,
                 on_status=None, on_chat=None, on_sys=None, on_msg_state=None, on_userlist=None,
                 on_log=None, on_err=None, on_room=None, on_rooms=None, on_history=None):
        self.host = host
        self.port = port
        self.username = username
        self.password = password          # 只驻内存（重连回落用），不落盘
        self.use_tls = use_tls            # C：TLS 传输（见 wrap_ssl 注释）
        self.ca_file = ca_file
        self.tls_strict = tls_strict
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
        self.on_err = on_err or (lambda code, text: None)
        self.on_room = on_room or (lambda room, members: None)
        self.on_rooms = on_rooms or (lambda rooms: None)
        self.on_history = on_history or (lambda items, cursor, more, kind, position: None)

        if pending_path is None:
            pending_path = "chat_pending_%s.json" % username
        if session_path is None:
            session_path = "chat_session_%s.json" % username
        self.store = PendingStore(pending_path, username)
        self.tokens = TokenStore(session_path, username)
        self.current_room = ""            # JOIN_OK 维护；LEAVE_OK/断开清空
        self._hist_cursor = ""            # 当前房间更早一页的游标（""=还没拉过/已到头）
        self._hist_busy = False           # 防并发 HIST（翻页是顺序操作）
        self._pending_hist_pos = ""       # "prepend"/""：下一页 HISTORY 的插入位置提示

        self._sock = None
        self._io_lock = threading.Lock()       # 保护 _sock 的读写替换（写侧 send + 判亡 close）
        self._running = False
        self._stop_ev = threading.Event()
        self._conn_thread = None
        self._hb_thread = None
        self._last_rx = 0.0
        self._last_ping = 0.0
        self._rx_seen = set()                  # (from, seq) 显示去重（实时消息兜底）
        self._connected = False
        self._authed = False
        self._tried_password_fallback = False  # 每次连接只回落一次口令（防死循环）
        self._slow_reconnect = False   # E4004 背压踢除：重连退避拉满（防快重连风暴）

    # ---------- 对外 API：连接/发送 ----------

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
        """把「失败可重试」重新入列；seq=None 表示全部重试"""
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

    # ---------- 对外 API：房间 / 历史（全部走控制帧，不进待发队列） ----------

    def join_room(self, room):
        self._hist_cursor = ""
        self._ctl("JOIN", room)

    def leave_room(self):
        self._ctl("LEAVE", "")

    def list_rooms(self):
        self._ctl("ROOMS", "")

    def create_room(self, room):
        self._ctl("CREATE", room)

    def fetch_history(self, cursor="", position=""):
        """拉一页房间历史。cursor='' 拉最近 50 条；否则按 (ts,id) 游标向上翻。

        position="prepend" 表示这是上翻的更早页（UI 前插），'' 表示进入房间的首屏。
        """
        if self._hist_busy:
            return
        self._hist_busy = True
        self._pending_hist_pos = position
        try:
            self._ctl("HIST", cursor)
        except (OSError, ValueError):
            self._hist_busy = False

    def fetch_inbox(self, cursor="", position=""):
        if self._hist_busy:
            return
        self._hist_busy = True
        self._pending_hist_pos = position
        try:
            self._ctl("INBOX", cursor)
        except (OSError, ValueError):
            self._hist_busy = False

    def fetch_older(self):
        """向上翻一页（GUI 滚到顶 / headless /older 都走这里）"""
        if self._hist_cursor:
            self.fetch_history(self._hist_cursor, position="prepend")
        else:
            self.on_sys("没有更早的历史了" if self.current_room else "尚未加入房间（/join 后拉历史）")

    def _ctl(self, msg_type, content):
        self._send_frame(make_msg(msg_type, self.username, "SERVER", content,
                                  self.store.alloc_seq()))

    # ---------- 内部：发送路径（沿用 v3） ----------

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
        obj["ts"] = item.ts  # 保留最初发送时间（历史/离线按时间排序的基础）
        try:
            self._send_frame(obj)
        except (OSError, ValueError):
            return  # 留在待发队列，等重连/重传
        item.attempts += 1
        item.last_sent = time.time()
        self.store.update(item)

    def _flush_pending(self):
        """重连成功后按 seq 序补发；对服务器是重复帧也没关系——(user,seq) 去重"""
        items = self.store.snapshot()
        if not items:
            return
        self.on_log("[补发] 待发队列 %d 条，按 seq 序补发" % len(items))
        for it in items:
            if it.state != "sending":
                continue
            self._try_send_item(it)

    # ---------- 内部：连接状态机 ----------

    def _set_status(self, text):
        self.on_status(text)

    def _drop_socket(self):
        with self._io_lock:
            sock = self._sock
            self._sock = None
            self._connected = False
            self._authed = False
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
                if self._slow_reconnect:
                    # 被背压踢除（E4004）：不是网络故障，快重连只会再被踢——直接拉满退避
                    self._slow_reconnect = False
                    delay = self.backoff_max
                else:
                    base = min(self.backoff_max, 2 ** (attempt - 1))          # 1,2,4,...,30
                    delay = base * random.uniform(1.0 - JITTER, 1.0 + JITTER)  # ±20% 抖动
                    delay = min(delay, self.backoff_max)
                self.on_log("[重连] 第 %d 次，%.1fs 后重试" % (attempt, delay))
                if self._stop_ev.wait(delay):  # 可中断退避
                    break
                self._set_status(self.STATUS_CONNECTING)
            try:
                sock = socket.create_connection((self.host, self.port), timeout=5)
                if self.use_tls:       # C：TCP 之上套 TLS（握手失败走重连退避）
                    sock = wrap_ssl(sock, self.ca_file, self.tls_strict, hostname=self.host)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                sock.settimeout(None)  # 收帧阻塞；判死交给心跳线程（应用层 PING）
            except OSError as e:
                self.on_log("[连接失败] %s:%d -> %r" % (self.host, self.port, e))
                attempt += 1
                continue

            with self._io_lock:
                self._sock = sock
            self._last_rx = time.time()
            self._last_ping = time.time()
            self._tried_password_fallback = False
            try:
                self._authenticate()     # AUTH(token) 免密优先，挑战-应答口令登录兜底
                self._connected = True
                # 补发等 AUTH_OK 后再 flush（Q7 挑战-应答是多步的，认证完成前业务帧会 E1007）
            except (OSError, ValueError) as e:
                self.on_log("[登录失败] %r" % e)
                self._drop_socket()
                attempt += 1
                continue

            self._set_status("已连接")
            self.on_log("[连接] %s:%d 已连接（%s）" % (self.host, self.port, self.username))
            attempt = 0

            # ---- 阶段 2：收帧直到出错 ----
            try:
                self._recv_loop(sock)
            except (OSError, ValueError, ConnectionError) as e:
                if self._running:
                    self.on_log("[断开] %r" % e)
            self._drop_socket()
            self.current_room = ""
            if not self._running:
                break
            attempt = 1  # 断开后进入重连循环（先退避 1s 抖动再试）
        self._set_status(self.STATUS_IDLE)

    def _authenticate(self):
        """重连免密：优先 AUTH(Token)；无/失效 Token 走挑战-应答口令登录（Q7）

        LOGIN_HELLO 后由 _dispatch 收 CHALLENGE 再回 LOGIN(proof)——口令/K 不进帧。
        """
        if self.tokens.valid():
            self.on_log("[认证] 用 Token 恢复会话（免密）")
            self._send_frame(make_msg("AUTH", self.username, "SERVER", self.tokens.token,
                                      self.store.alloc_seq()))
        elif self.password:
            self.on_log("[认证] 口令登录（挑战-应答，口令不上线）")
            self._send_frame(make_msg("LOGIN_HELLO", self.username, "SERVER", "",
                                      self.store.alloc_seq()))
        else:
            raise ValueError("无可用凭据（既无有效 Token 也无口令，请 /login）")

    def _answer_challenge(self, frame):
        """CHALLENGE{nonce, salt} → LOGIN{proof=HMAC(PBKDF2(pwd, salt), nonce)}"""
        if not self.password:
            self.on_err(1006, "需要口令完成挑战-应答（无 Token 也无口令）")
            return
        try:
            derived = derive_key(self.password, frame.get("salt", ""))
            proof = challenge_proof(derived, "LOGIN", self.username, frame.get("content", ""))
        except ValueError as e:
            self.on_log("[认证] 挑战帧非法: %r" % e)
            return
        self._send_frame(make_msg("LOGIN", self.username, "SERVER", proof,
                                  self.store.alloc_seq()))

    def _recv_loop(self, sock):
        while self._running:
            frame = recv_frame(sock)
            self._last_rx = time.time()  # 任何下行都刷新活跃时间
            self._dispatch(frame)

    def _dispatch(self, frame):
        ftype = frame.get("type", "")
        if ftype == "PONG":
            pass  # 心跳应答：last_rx 已刷新，不刷日志
        elif ftype == "ACK":
            # content = 被确认的客户端 seq → 「已送达」
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
        elif ftype == "ERR":
            self._handle_err(int(frame.get("code", 0)), frame.get("content", ""))
        elif ftype == "CHALLENGE":
            self._answer_challenge(frame)   # Q7：挑战-应答第二步
        elif ftype == "REGISTER_OK":
            self.on_sys(frame.get("content", "注册成功"))
        elif ftype == "AUTH_OK":
            self._authed = True
            self.tokens.save(frame.get("content", ""), frame.get("exp", 0))
            self.on_sys("认证成功（Token 已保存，重连免密）")
            self._flush_pending()           # 认证完成才补发（防认证前业务帧被 E1007）
            room = frame.get("room", "")
            if room:
                # 顶号恢复：服务器把原房间状态带回来（用户在别人眼里没离开过）
                self.current_room = room
                self._hist_cursor = ""
                self.on_room(room, [])
                self.fetch_history("")
        elif ftype == "JOIN_OK":
            room = frame.get("room", "")
            members = frame.get("content", [])
            self.current_room = room
            self._hist_cursor = ""
            self.on_room(room, members if isinstance(members, list) else [])
            self.fetch_history("")   # 需求：进入房间自动拉最近 50 条
        elif ftype == "LEAVE_OK":
            self.current_room = ""
            self._hist_cursor = ""
            self.on_sys(frame.get("content", "已离开房间"))
            self.on_room("", [])
        elif ftype == "CREATE_OK":
            self.on_sys(frame.get("content", "房间创建成功"))
        elif ftype == "ROOMS_LIST":
            rooms = frame.get("content", [])
            self.on_rooms(rooms if isinstance(rooms, list) else [])
        elif ftype in ("HISTORY", "INBOX"):
            self._handle_history(frame, ftype)
        elif ftype == "MESSAGE":
            self._handle_message(frame)
        elif ftype == "SYSTEM":
            room = frame.get("room", "") or ""
            prefix = ("[#%s] " % room) if room else ""
            self.on_sys(prefix + frame.get("content", ""))
        elif ftype == "USERLIST":
            names = frame.get("content", [])
            if isinstance(names, list):
                self.on_userlist(names)
        else:
            self.on_log("[未知帧] %s" % json.dumps(frame, ensure_ascii=False))

    def _handle_err(self, code, text):
        """错误码 + 中文文案：明确提示，绝不静默失败（服务端 Q5 要求）"""
        self.on_err(code, text)
        self.on_log("[错误 %d] %s" % (code, text))
        if code == 4004:
            # 背压踢除（读取过慢）：下一步重连退避拉满，避免「被踢→快重连→再被踢」风暴
            self._slow_reconnect = True
            self.on_sys("服务器发送积压超限（读取过慢）已断开，稍后慢速重连")
        if code == 1006 and self.password and not self._tried_password_fallback:
            # Token 失效（如服务器重启清了内存 Token 表）→ 回落挑战-应答口令登录
            self._tried_password_fallback = True
            self.tokens.clear()
            self.on_log("[认证] Token 失效，回落口令登录")
            try:
                self._send_frame(make_msg("LOGIN_HELLO", self.username, "SERVER", "",
                                          self.store.alloc_seq()))
            except (OSError, ValueError):
                pass
        elif code in (1002, 1003, 1004, 1005):
            # 认证类硬错误：停止无意义的自动重发（用户需介入），连接保留便于 /login 换号
            for it in self.store.snapshot():
                if it.state == "sending":
                    it.state = "failed"
                    self.store.update(it)

    def _handle_message(self, frame):
        try:
            seq = int(frame.get("seq", 0))
        except (TypeError, ValueError):
            seq = 0
        offline = bool(frame.get("offline"))
        if not offline and seq > 0:
            # 实时消息显示去重（(from,seq) 兜底；离线补发行来自 DB、天然唯一，不去重）
            key = (frame.get("from", ""), seq)
            if key in self._rx_seen:
                self.on_log("[去重] (%s, %s) 重复消息已忽略" % key)
                return
            self._rx_seen.add(key)
        self.on_chat(frame)

    def _handle_history(self, frame, kind):
        items = frame.get("content", [])
        cursor = frame.get("cursor", "")
        more = bool(frame.get("more"))
        self._hist_busy = False
        if kind == "HISTORY":
            self._hist_cursor = cursor if more else ""  # more=0 → 到头，不再翻
        position = self._pending_hist_pos
        self._pending_hist_pos = ""
        self.on_history(items if isinstance(items, list) else [], cursor, more, kind, position)

    # ---------- 内部：心跳与重传（沿用 v3） ----------

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

    def on_err(code, text):
        print("[错误 %d] %s" % (code, text), flush=True)

    def on_userlist(names):
        s = sess_holder.get("s")
        print("[在线#%s] %s" % ((s.current_room if s else "") or "-", ",".join(names)), flush=True)

    def on_room(room, members):
        if room:
            print("[房间] #%s（%d 人）%s" % (room, len(members), ",".join(members)), flush=True)
        else:
            print("[房间] 已离开", flush=True)

    def on_rooms(rooms):
        print("[房间列表] 共 %d 个" % len(rooms), flush=True)
        for r in rooms:
            print("    #%-12s 房主=%-8s 在线=%d" % (r.get("name", "?"), r.get("owner", "?"),
                                                r.get("members", 0)), flush=True)

    def on_history(items, cursor, more, kind, position):
        print("[历史 %s]%s 共 %d 条%s" % (kind, "(更早)" if position == "prepend" else "",
                                      len(items), "，还有更早" if more else "，到头"), flush=True)
        for m in items:
            who = m.get("from", "?")
            to = m.get("to", "")
            dest = "所有人" if to == "ALL" else to
            print("    [%s] %s → %s: %s" % (fmt_ts(m.get("ts", 0)), who, dest,
                                            m.get("content", "")), flush=True)

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
        room = m.get("room", "")
        if to == "ALL":
            dest = "#%s 所有人" % room if room else "所有人"
        else:
            dest = "我"
        print("[消息] %s%s → %s: %s" % (tag, who, dest, m.get("content", "")), flush=True)

    sess_holder = {}

    def start_session(u, password):
        old = sess_holder.get("s")
        if old is not None:
            old.stop()
        s = ChatSession(host, port, u, password=password, pending_path=args.pending_file,
                        ping_interval=args.ping_interval, dead_timeout=args.dead_timeout,
                        ack_timeout=args.ack_timeout, backoff_max=args.backoff_max,
                        use_tls=args.tls, ca_file=args.ca, tls_strict=args.tls_strict,
                        on_status=on_status, on_chat=on_chat, on_sys=on_sys,
                        on_msg_state=on_msg_state, on_userlist=on_userlist, on_log=on_log,
                        on_err=on_err, on_room=on_room, on_rooms=on_rooms,
                        on_history=on_history)
        sess_holder["s"] = s
        s.start()
        return s

    # 入口即登录：有 --password 或已存 Token 就直接开会话；否则等 /register //login
    sess = None
    if user and (args.password or TokenStore(args.session_file or
                                             ("chat_session_%s.json" % user), user).valid()):
        sess = start_session(user, args.password)
        sess_holder["s"] = sess
    else:
        print("[提示] 未自动登录。/register 用户名 密码 注册；/login 用户名 密码 登录", flush=True)

    def cur():
        return sess_holder.get("s")

    print("[提示] 命令：/register /login /join /leave /rooms /create /older /inbox "
          "@用户=私聊 /retry /status /quit", flush=True)
    try:
        for line in sys.stdin:
            line = line.rstrip("\n")
            if not line:
                continue
            s = cur()
            if line == "/quit":
                break
            elif line.startswith("/register "):
                sp = line.split()
                if len(sp) != 3:
                    print("[用法] /register 用户名 密码", flush=True)
                    continue
                ok, code, text = register_account(host, port, sp[1], sp[2],
                                                  use_tls=args.tls, ca_file=args.ca,
                                                  tls_strict=args.tls_strict)
                print(("[注册成功] %s" % text) if ok else ("[注册失败 %d] %s" % (code, text)),
                      flush=True)
            elif line.startswith("/login "):
                sp = line.split()
                if len(sp) != 3:
                    print("[用法] /login 用户名 密码", flush=True)
                    continue
                sess = start_session(sp[1], sp[2])
            elif s is None:
                print("[提示] 请先 /login 用户名 密码", flush=True)
            elif line == "/retry":
                s.retry_failed()
            elif line == "/status":
                for it in s.store.snapshot():
                    print("[队列] seq=%d to=%s state=%s attempts=%d content=%r"
                          % (it.seq, it.to, it.state, it.attempts, it.content), flush=True)
                print("[会话] room=%s token=%s" % (s.current_room or "-",
                                                   "有效" if s.tokens.valid() else "无/失效"),
                      flush=True)
            elif line == "/leave":
                s.leave_room()
            elif line == "/rooms":
                s.list_rooms()
            elif line == "/older":
                s.fetch_older()
            elif line == "/inbox":
                s.fetch_inbox()
            elif line.startswith("/join "):
                s.join_room(line[6:].strip())
            elif line.startswith("/create "):
                s.create_room(line[8:].strip())
            elif line.startswith("@"):
                sp = line.find(" ")
                if sp <= 1:
                    print("[用法] @用户名 内容", flush=True)
                    continue
                s.send(line[1:sp], line[sp + 1:])
            else:
                s.send("ALL", line)
    except (KeyboardInterrupt, EOFError):
        pass
    s = cur()
    if s is not None:
        s.stop()
    print("[状态] 未连接", flush=True)


# ---------------- 图形界面 ----------------

def run_gui(host, port, user, args):
    import tkinter as tk
    from tkinter import scrolledtext, messagebox

    class App(object):
        def __init__(self):
            self.sess = None
            self.seq_marks = {}   # seq → (mark名, 当前状态文本, 样式)——原地更新状态列
            self.retryable = {}   # seq → True（失败可重试，点击行可重试）
            self.hist_loading = False

            self.root = tk.Tk()
            self.root.title("在线聊天 v5（账号/房间/历史分页）客户端 v4")
            self.root.geometry("820x540")
            self.root.protocol("WM_DELETE_WINDOW", self.on_closing)

            # ---- 顶部：连接 + 账号 ----
            conn = tk.Frame(self.root)
            conn.pack(fill=tk.X, padx=8, pady=4)
            tk.Label(conn, text="服务器:").pack(side=tk.LEFT)
            self.host_entry = tk.Entry(conn, width=12)
            self.host_entry.insert(0, host)
            self.host_entry.pack(side=tk.LEFT, padx=2)
            tk.Label(conn, text="端口:").pack(side=tk.LEFT)
            self.port_entry = tk.Entry(conn, width=6)
            self.port_entry.insert(0, str(port))
            self.port_entry.pack(side=tk.LEFT, padx=2)
            tk.Label(conn, text="用户:").pack(side=tk.LEFT)
            self.user_entry = tk.Entry(conn, width=10)
            self.user_entry.insert(0, user or "")
            self.user_entry.pack(side=tk.LEFT, padx=2)
            tk.Label(conn, text="密码:").pack(side=tk.LEFT)
            self.pwd_entry = tk.Entry(conn, width=10, show="*")
            self.pwd_entry.pack(side=tk.LEFT, padx=2)
            self.connect_btn = tk.Button(conn, text="连接", width=8, command=self.toggle_conn)
            self.connect_btn.pack(side=tk.LEFT, padx=4)
            self.reg_btn = tk.Button(conn, text="注册", width=6, command=self.on_register)
            self.reg_btn.pack(side=tk.LEFT, padx=2)
            self.retry_btn = tk.Button(conn, text="重试失败", width=8,
                                       command=self.on_retry_all, state=tk.DISABLED)
            self.retry_btn.pack(side=tk.LEFT, padx=2)

            # ---- 房间工具条 ----
            room = tk.Frame(self.root)
            room.pack(fill=tk.X, padx=8, pady=2)
            self.room_label = tk.Label(room, text="当前房间：-（/join 房间名 加入）", anchor="w")
            self.room_label.pack(side=tk.LEFT)
            tk.Button(room, text="更早记录", width=8, command=self.on_older).pack(side=tk.RIGHT, padx=2)
            tk.Button(room, text="收件箱", width=6, command=self.on_inbox).pack(side=tk.RIGHT, padx=2)
            tk.Button(room, text="房间列表", width=8, command=self.on_rooms).pack(side=tk.RIGHT, padx=2)

            # ---- 中部：消息区 + 在线列表 ----
            mid = tk.Frame(self.root)
            mid.pack(fill=tk.BOTH, expand=True, padx=8, pady=4)
            self.log = scrolledtext.ScrolledText(mid, state=tk.DISABLED, wrap=tk.WORD)
            self.log.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
            self.log.tag_configure("offline", foreground="#777777", font=("TkDefaultFont", 9, "italic"))
            self.log.tag_configure("hist", foreground="#555555", font=("TkDefaultFont", 9))
            self.log.tag_configure("sys", foreground="#0066cc")
            self.log.tag_configure("ok", foreground="#008800")
            self.log.tag_configure("bad", foreground="#cc0000")
            self.log.tag_configure("pending", foreground="#aa6600")
            self.log.tag_configure("err", foreground="#cc0000", font=("TkDefaultFont", 9, "bold"))
            self.log.bind("<Button-1>", self.on_log_click)   # 点「失败可重试」行 → 单条重试
            # 向上滚动到顶 → 自动拉更早历史（需求：向上滚动翻页）
            for ev in ("<MouseWheel>", "<Button-4>", "<Button-5>"):
                self.log.bind(ev, self.on_scroll, add="+")

            side = tk.Frame(mid)
            side.pack(side=tk.RIGHT, fill=tk.Y, padx=(6, 0))
            self.userlist_label = tk.Label(side, text="房间成员")
            self.userlist_label.pack(anchor="w")
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

            # ---- 状态栏 ----
            self.status = tk.Label(self.root, text="未连接", anchor="w", relief=tk.SUNKEN)
            self.status.pack(fill=tk.X, side=tk.BOTTOM)

            self.append_log("就绪。「连接」登录（有 Token 免密）；「注册」新账号。", "sys")
            self.append_log("命令：/join 房间  /leave  /rooms  /create 房间  /older  @用户 私聊", "sys")

        # ---------- 回调（工作线程触发 → root.after 转主线程） ----------
        def cb_status(self, s):
            self.root.after(0, lambda: self.status.config(text=s))

        def cb_log(self, s):
            self.root.after(0, lambda: self.append_log(s))

        def cb_sys(self, s):
            self.root.after(0, lambda: self.append_log("[系统] " + s, "sys"))

        def cb_err(self, code, text):
            self.root.after(0, lambda: self.append_log("[错误 %d] %s" % (code, text), "err"))

        def cb_userlist(self, names):
            def upd():
                self.userlist.delete(0, tk.END)
                for n in names:
                    self.userlist.insert(tk.END, n)
                self.userlist_label.config(text="房间成员（%d）" % len(names))
            self.root.after(0, upd)

        def cb_room(self, room, members):
            def upd():
                if room:
                    self.room_label.config(text="当前房间：#%s" % room)
                    self.userlist_label.config(text="房间成员（%d）" % len(members))
                else:
                    self.room_label.config(text="当前房间：-（/join 房间名 加入）")
            self.root.after(0, upd)

        def cb_rooms(self, rooms):
            def upd():
                self.append_log("[房间列表] 共 %d 个" % len(rooms), "sys")
                for r in rooms:
                    self.append_log("    #%-12s 房主=%-8s 在线=%d"
                                    % (r.get("name", "?"), r.get("owner", "?"),
                                       r.get("members", 0)), "hist")
            self.root.after(0, upd)

        def cb_history(self, items, cursor, more, kind, position):
            def upd():
                self.hist_loading = False
                for m in items:
                    line = self._fmt_hist_line(m, kind)
                    if position == "prepend":
                        self.prepend_log(line, "hist")
                    else:
                        self.append_log(line, "hist")
                if position == "prepend" and not items:
                    self.append_log("[历史] 没有更早的了", "sys")
            self.root.after(0, upd)

        def cb_chat(self, m):
            def show():
                tag = ""
                style = None
                if m.get("offline"):
                    tag = "[离线消息] "
                    style = "offline"  # 「离线消息」样式（灰斜体）
                who = m.get("from", "?")
                to = m.get("to", "")
                room = m.get("room", "")
                if to == "ALL":
                    dest = "#%s 所有人" % room if room else "所有人"
                else:
                    dest = "我"
                line = "%s[%s] %s → %s: %s" % (tag, fmt_ts(m.get("ts", time.time())), who, dest,
                                               m.get("content", ""))
                self.append_log(line, style)
            self.root.after(0, show)

        def cb_msg_state(self, seq, state, reason):
            self.root.after(0, lambda: self.update_msg_state(seq, state, reason))

        @staticmethod
        def _fmt_hist_line(m, kind):
            who = m.get("from", "?")
            to = m.get("to", "")
            if kind == "INBOX":
                dest = "我"
            else:
                dest = "所有人" if to == "ALL" else to
            return "[%s] %s → %s: %s" % (fmt_ts(m.get("ts", 0)), who, dest, m.get("content", ""))

        # ---------- 消息区操作 ----------
        def append_log(self, text, style=None):
            self.log.config(state=tk.NORMAL)
            if style:
                self.log.insert(tk.END, text + "\n", style)
            else:
                self.log.insert(tk.END, text + "\n")
            self.log.config(state=tk.DISABLED)
            self.log.see(tk.END)

        def prepend_log(self, text, style=None):
            """历史「更早页」前插。插入后把原先的首行拉回视口，保持阅读位置大体不动。"""
            old_first = int(self.log.index("1.0").split(".")[0])
            n = text.count("\n")
            self.log.config(state=tk.NORMAL)
            if style:
                self.log.insert("1.0", text, style)
            else:
                self.log.insert("1.0", text)
            self.log.config(state=tk.DISABLED)
            # 旧首行行号下移 n 行，see 把它拉回视口（近似保持滚动位置）
            self.log.see("%d.0" % (old_first + n))

        def append_sent(self, seq, to, content):
            to_disp = "所有人(#%s)" % self.sess.current_room if to == "ALL" else to
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
        def on_register(self):
            name = self.user_entry.get().strip()
            pwd = self.pwd_entry.get()
            if not name or not pwd:
                messagebox.showwarning("提示", "注册需要用户名和密码")
                return
            try:
                h = self.host_entry.get().strip()
                p = int(self.port_entry.get())
            except ValueError:
                messagebox.showwarning("提示", "端口必须是整数")
                return

            def work():
                try:
                    ok, code, text = register_account(h, p, name, pwd,
                                                      use_tls=args.tls, ca_file=args.ca,
                                                      tls_strict=args.tls_strict)
                except OSError as e:
                    ok, code, text = False, 0, repr(e)
                def done():
                    if ok:
                        messagebox.showinfo("注册成功", text + "\n现在点「连接」登录")
                        self.append_log("[注册] " + text, "ok")
                    else:
                        messagebox.showwarning("注册失败", "[%d] %s" % (code, text))
                        self.append_log("[注册失败 %d] %s" % (code, text), "err")
                self.root.after(0, done)
            threading.Thread(target=work, daemon=True).start()

        def toggle_conn(self):
            if self.sess is None or not self.sess.is_connected():
                self.do_connect()
            else:
                self.do_disconnect()

        def do_connect(self):
            name = self.user_entry.get().strip()
            pwd = self.pwd_entry.get()
            if not name:
                messagebox.showwarning("提示", "请填写用户名")
                return
            try:
                h = self.host_entry.get().strip()
                p = int(self.port_entry.get())
            except ValueError:
                messagebox.showwarning("提示", "端口必须是整数")
                return
            # 有有效 Token 可不输密码（重连免密）；无 Token 则必须口令
            tok = TokenStore("chat_session_%s.json" % name, name)
            if not pwd and not tok.valid():
                messagebox.showwarning("提示", "首次登录请输入密码（有 Token 时可留空免密恢复）")
                return
            if self.sess is not None:
                self.sess.stop()
            self.sess = ChatSession(
                h, p, name, password=pwd, pending_path=args.pending_file,
                ping_interval=args.ping_interval, dead_timeout=args.dead_timeout,
                ack_timeout=args.ack_timeout, backoff_max=args.backoff_max,
                use_tls=args.tls, ca_file=args.ca, tls_strict=args.tls_strict,
                on_status=self.cb_status, on_chat=self.cb_chat, on_sys=self.cb_sys,
                on_msg_state=self.cb_msg_state, on_userlist=self.cb_userlist,
                on_log=self.cb_log, on_err=self.cb_err, on_room=self.cb_room,
                on_rooms=self.cb_rooms, on_history=self.cb_history)
            self.sess.start()
            self.connect_btn.config(text="断开")

        def do_disconnect(self):
            if self.sess is not None:
                self.sess.stop()
                self.sess = None
            self.connect_btn.config(text="连接")
            self.status.config(text="未连接")
            self.cb_room("", [])

        def on_send(self):
            if self.sess is None or not self.sess.is_connected():
                self.append_log("[提示] 未连接（连接后可发送；断线期间消息会进入待发队列）", "sys")
                return
            text = self.input.get()
            if not text:
                return
            self.input.delete(0, tk.END)
            if text == "/rooms":
                self.sess.list_rooms()
                return
            if text == "/leave":
                self.sess.leave_room()
                return
            if text == "/older":
                self.on_older()
                return
            if text == "/inbox":
                self.on_inbox()
                return
            if text.startswith("/join "):
                self.sess.join_room(text[6:].strip())
                return
            if text.startswith("/create "):
                self.sess.create_room(text[8:].strip())
                return
            if text.startswith("@") and " " in text:
                sp = text.find(" ")
                to, content = text[1:sp], text[sp + 1:]
            else:
                to, content = "ALL", text
            seq = self.sess.send(to, content)  # 立即显示「发送中」，ACK 后变「已送达」
            self.append_sent(seq, to, content)

        def on_older(self):
            if self.sess is None:
                return
            if self.hist_loading:
                return
            self.hist_loading = True
            self.sess.fetch_older()

        def on_inbox(self):
            if self.sess is None:
                return
            if self.hist_loading:
                return
            self.hist_loading = True
            self.sess.fetch_inbox()

        def on_rooms(self):
            if self.sess:
                self.sess.list_rooms()

        def on_scroll(self, event):
            # 滚动到顶（yview 起点 == 0.0）→ 自动拉更早一页（需求：向上滚动翻页）
            def check():
                if self.sess is None or self.hist_loading:
                    return
                if self.log.yview()[0] <= 0.0 and self.sess._hist_cursor:
                    self.hist_loading = True
                    self.sess.fetch_older()
            self.root.after(50, check)  # 等滚动生效后再看位置
            return None  # 不吞事件（保留默认滚动）

        def on_retry_all(self):
            if self.sess:
                self.sess.retry_failed()

        def on_log_click(self, event):
            # 点击「失败可重试」行 → 单条重试
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
    ap = argparse.ArgumentParser(description="在线聊天客户端 v4（账号/房间/历史分页）")
    ap.add_argument("host", nargs="?", default="localhost", help="服务器地址（默认 localhost）")
    ap.add_argument("--port", type=int, default=8888, help="端口（默认 8888）")
    ap.add_argument("--user", default="", help="用户名")
    ap.add_argument("--password", default="", help="密码（留空则尝试 Token 免密恢复）")
    ap.add_argument("--headless", action="store_true", help="无 GUI 交互模式（验收/测试）")
    ap.add_argument("--pending-file", default=None,
                    help="待发队列持久化文件（默认 chat_pending_<user>.json）")
    ap.add_argument("--session-file", default=None,
                    help="会话 Token 文件（默认 chat_session_<user>.json）")
    ap.add_argument("--ping-interval", type=float, default=10.0, help="PING 周期秒数（默认 10）")
    ap.add_argument("--dead-timeout", type=float, default=30.0,
                    help="无响应判死秒数（默认 30，与服务器剔除阈值一致）")
    ap.add_argument("--ack-timeout", type=float, default=15.0, help="ACK 等待超时/重传秒数（默认 15）")
    ap.add_argument("--backoff-max", type=float, default=30.0, help="重连退避上限秒数（默认 30）")
    ap.add_argument("--tls", action="store_true", help="启用 TLS 传输（服务器 --tls 同开）")
    ap.add_argument("--ca", default="", help="CA/自签证书路径（配合 --tls-strict 校验）")
    ap.add_argument("--tls-strict", action="store_true",
                    help="严格校验服务器证书（默认关闭：自签教学模式）")
    args = ap.parse_args()

    if not args.user and not args.headless:
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
