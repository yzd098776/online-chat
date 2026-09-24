#!/usr/bin/env python3
"""tools/test_reliability.py —— 可靠性改造自动化验收（纯标准库）

把 VERIFY.md 的 3 类异常场景在一台机器上自动跑一遍（快速参数，语义与需求默认值一致）：
  T0  冒烟        登录 / 群聊 / 私聊 / ACK 状态
  T1  kill -9     客户端被强杀 → 服务器剔除并广播下线（FIN 路径，秒级；满足「30s 内」）
  T1b 半开超时    kill -STOP 模拟死机/拔线（无 FIN）→ 心跳扫描线程按 idle 阈值剔除并广播
  T1c 顶号重连    半开僵尸占名时同名重连 → 顶号成功，不锁门
  T2  拔网线      中间代理被断 → 退避重连 + 自动 LOGIN + 待发队列按 seq 补发 + 收离线消息
  T2b 幂等        同一 (user, seq) 重复帧只投递一次（重连补发产生重复的去重验证）
  T2c 离线消息    私聊不在线 → 入 offline_message，上线按 ts 排序补发，带「离线消息」样式
  T3  服务器重启  kill 服务器 → 客户端重连 → 在线列表重建正确，断档期消息不丢

用法:
  python3 tools/test_reliability.py                 # 全部场景
  python3 tools/test_reliability.py T2 T3           # 只跑指定场景（名字前缀匹配）
"""

import os
import re
import shutil
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SERVER_BIN = os.path.join(ROOT, "chat_server_v4")
CLIENT = os.path.join(ROOT, "chat_client_v3.py")

# 快速参数：语义与需求默认值（10s PING / 5s 扫描 / 30s 判死）一致，仅压缩时间轴
SRV_IDLE_MS, SRV_SCAN_MS = 3000, 500
CLI_PING, CLI_DEAD, CLI_ACK, CLI_BACKOFF = 0.5, 2.0, 1.0, 2.0


# ---------------- 子进程封装 ----------------

class Client(object):
    """headless 客户端子进程：行式 stdout 收集 + stdin 命令。

    pending 文件默认按用户名共享（同一用户 = 同一台机器 = 同一 seq 计数器，贴近真实）；
    仅当同名双实例并发时用 file_tag 隔离（见 T1c 注释）。
    """

    def __init__(self, name, port, workdir, file_tag=""):
        self.name = name
        self.pending = os.path.join(workdir, "pending_%s%s.json" % (name, file_tag))
        cmd = [sys.executable, CLIENT, "127.0.0.1", "--port", str(port), "--user", name,
               "--headless", "--pending-file", self.pending,
               "--ping-interval", str(CLI_PING), "--dead-timeout", str(CLI_DEAD),
               "--ack-timeout", str(CLI_ACK), "--backoff-max", str(CLI_BACKOFF)]
        self.proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                     stderr=subprocess.STDOUT, text=True, bufsize=1,
                                     cwd=workdir)
        self.lines = []
        self._lock = threading.Lock()
        threading.Thread(target=self._read_loop, daemon=True).start()

    def _read_loop(self):
        try:
            for line in self.proc.stdout:
                with self._lock:
                    self.lines.append(line.rstrip("\n"))
        except (OSError, ValueError):
            pass

    def mark(self):
        """当前位置：只等待这之后出现的行（调用纪律：mark 必须在触发动作【之前】取）"""
        with self._lock:
            return len(self.lines)

    def wait(self, pattern, timeout=10.0, since=0):
        """等待 since 之后出现匹配行，返回行号"""
        rx = re.compile(pattern)
        deadline = time.time() + timeout
        i = since
        while time.time() < deadline:
            with self._lock:
                while i < len(self.lines):
                    if rx.search(self.lines[i]):
                        return i
                    i += 1
            time.sleep(0.05)
        self.dump()
        raise AssertionError("[%s] %.1fs 内未等到 /%s/" % (self.name, timeout, pattern))

    def count(self, pattern, since=0):
        rx = re.compile(pattern)
        with self._lock:
            return sum(1 for ln in self.lines[since:] if rx.search(ln))

    def send(self, line):
        self.proc.stdin.write(line + "\n")
        self.proc.stdin.flush()

    def kill9(self):
        """SIGKILL：等价任务管理器「结束进程」/ kill -9"""
        self.proc.kill()
        self.proc.wait(timeout=5)
        try:
            self.proc.stdin.close()  # 先关干净，免得 GC 时 BrokenPipe 打噪音
        except (OSError, ValueError):
            pass

    def freeze(self):
        """SIGSTOP：进程冻结，socket 不关闭、内核不发 FIN（模拟死机/断电/拔线的半开连接）"""
        self.proc.send_signal(signal.SIGSTOP)

    def unfreeze(self):
        self.proc.send_signal(signal.SIGCONT)

    def quit(self):
        try:
            self.send("/quit")
        except (OSError, ValueError):
            pass
        try:
            self.proc.stdin.close()
        except (OSError, ValueError):
            pass
        try:
            self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass

    def dump(self):
        with self._lock:
            print("---- %s 最近输出 ----" % self.name)
            for ln in self.lines[-30:]:
                print("  | " + ln)
            print("---- %s 输出结束 ----" % self.name)


class Server(object):
    def __init__(self, port, workdir):
        self.port = port
        self.workdir = workdir
        self.db = os.path.join(workdir, "server.db")
        self.proc = None
        self.start()

    def start(self):
        self.log = open(os.path.join(self.workdir, "server.log"), "ab")
        self.proc = subprocess.Popen(
            [SERVER_BIN, str(self.port), "--db", self.db,
             "--idle", str(SRV_IDLE_MS), "--scan", str(SRV_SCAN_MS)],
            stdout=self.log, stderr=subprocess.STDOUT, cwd=self.workdir)
        deadline = time.time() + 5
        while time.time() < deadline:
            try:
                s = socket.create_connection(("127.0.0.1", self.port), timeout=0.3)
                s.close()
                return
            except OSError:
                time.sleep(0.05)
        raise AssertionError("服务器 5s 内未就绪")

    def kill9(self):
        self.proc.kill()
        self.proc.wait(timeout=5)

    def stop(self):
        self.proc.terminate()
        try:
            self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.proc.kill()
        try:
            self.log.close()
        except Exception:
            pass

    def dump_log(self, tail=40):
        path = os.path.join(self.workdir, "server.log")
        try:
            with open(path, "rb") as f:
                lines = f.read().decode("utf-8", "replace").splitlines()
        except OSError:
            return
        print("---- 服务器日志（最近 %d 行）----" % tail)
        for ln in lines[-tail:]:
            print("  | " + ln)
        print("---- 服务器日志结束 ----")


class _Pair(object):
    """一条被代理的连接对。yanked=True 表示已「拔线」：双向黑洞，且【不】给对端发 FIN。"""
    __slots__ = ("c", "u", "yanked")

    def __init__(self, c, u):
        self.c = c
        self.u = u
        self.yanked = False


class TcpProxy(object):
    """极简 TCP 代理：kill() 模拟拔网线 / 关 WiFi（双向黑洞、无 FIN），restart() 恢复

    为什么是黑洞而不是 close()：真拔线时对端收不到 FIN——服务器会留一个半开僵尸
    直到 idle 扫描剔除（需求 1）或同名 LOGIN 顶号（Q4）。若 kill 时给服务器侧发 FIN，
    「断线瞬间服务器仍以为对方在线」的丢消息窗口就被掩盖了——而那正是 store-first
    要堵的产品级漏洞（转发写进半开 socket「成功」但数据丢）。
    """

    def __init__(self, listen_port, target_port):
        self.listen_port = listen_port
        self.target_port = target_port
        self.lock = threading.Lock()
        self.pairs = []      # 活跃连接对
        self.zombies = []    # 已拔线连接对：故意不关，防 GC 隐式 close 发出 FIN
        self.listener = None
        self.running = True
        self._open_listener()
        threading.Thread(target=self._accept_loop, daemon=True).start()

    def _open_listener(self):
        self.listener = socket.socket()
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("127.0.0.1", self.listen_port))
        self.listener.listen(8)

    def _accept_loop(self):
        while self.running:
            try:
                c, _ = self.listener.accept()
            except OSError:
                break
            try:
                u = socket.create_connection(("127.0.0.1", self.target_port), timeout=3)
            except OSError:
                c.close()
                continue
            pair = _Pair(c, u)
            with self.lock:
                self.pairs.append(pair)
            threading.Thread(target=self._pump, args=(pair, c, u), daemon=True).start()
            threading.Thread(target=self._pump, args=(pair, u, c), daemon=True).start()

    @staticmethod
    def _pump(pair, a, b):
        try:
            while True:
                data = a.recv(4096)
                if not data:
                    break
                if pair.yanked:
                    continue  # 黑洞：吞掉数据（真拔线包也到不了对端）
                b.sendall(data)
        except OSError:
            pass
        if not pair.yanked:
            for s in (a, b):
                try:
                    s.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
        # yank 模式下【不】shutdown/close：那会给对端发 FIN，抹掉半开窗口

    def kill(self):
        self.running = False
        with self.lock:
            pairs, self.pairs = self.pairs, []
            for p in pairs:
                p.yanked = True
            self.zombies.extend(pairs)  # 托住 socket 引用：GC 的隐式 close 会发 FIN
        try:
            # Linux 陷阱：accept() 阻塞中仅 close() 不释放监听端口（socket 引用被 accept 持有），
            # 必须先 shutdown 唤醒 accept 再 close，否则 restart() bind 会 EADDRINUSE
            self.listener.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self.listener.close()
        except OSError:
            pass

    def restart(self):
        self.running = True
        self._open_listener()
        threading.Thread(target=self._accept_loop, daemon=True).start()


# ---------------- 裸帧客户端（幂等/离线库直测） ----------------

def raw_connect(port):
    s = socket.create_connection(("127.0.0.1", port), timeout=5)
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    return s


def raw_send(s, obj):
    body = __import__("json").dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    s.sendall(struct.pack(">I", len(body)) + body)


def raw_recv(s, timeout=5.0):
    s.settimeout(timeout)
    hdr = b""
    while len(hdr) < 4:
        chunk = s.recv(4 - len(hdr))
        if not chunk:
            raise ConnectionError("closed")
        hdr += chunk
    (n,) = struct.unpack(">I", hdr)
    body = b""
    while len(body) < n:
        chunk = s.recv(n - len(body))
        if not chunk:
            raise ConnectionError("closed")
        body += chunk
    return __import__("json").loads(body.decode("utf-8"))


def raw_wait(s, ftype, timeout=5.0):
    """收帧直到指定 type（跳过 USERLIST 等广播），返回该帧"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        f = raw_recv(s, timeout=max(0.1, deadline - time.time()))
        if f.get("type") == ftype:
            return f
    raise AssertionError("未等到 type=%s" % ftype)


def raw_msg(ftype, from_, to_, content, seq, ts=None):
    return {"ver": 1, "type": ftype, "from": from_, "to": to_, "room": "",
            "content": content, "ts": int(time.time()) if ts is None else ts, "seq": seq}


def raw_login(port, user, seq=1):
    s = raw_connect(port)
    raw_send(s, raw_msg("LOGIN", user, "SERVER", "登录", seq))
    raw_wait(s, "SYSTEM", timeout=5)
    return s


# ---------------- 场景 ----------------

CASES = []


def run_case(name):
    def deco(fn):
        CASES.append((name, fn))
        return fn
    return deco


def expect(cond, msg):
    if not cond:
        raise AssertionError(msg)


@run_case("T0 冒烟：登录/群聊/私聊/ACK")
def t0(ctx):
    ma0, mb0 = None, None
    alice = Client("alice", ctx.port, ctx.workdir, )
    bob = Client("bob", ctx.port, ctx.workdir, )
    ctx.clients = [alice, bob]
    alice.wait(r"\[状态\] 已连接")
    bob.wait(r"\[状态\] 已连接")
    alice.wait(r"登录成功")
    bob.wait(r"登录成功")
    bob.wait(r"\[在线\].*alice")  # 登录触发的 USERLIST 广播

    # mark 在【触发动作】之前取（纪律：动作后的 mark 可能错过背靠背到达的行）
    ma, mb = alice.mark(), bob.mark()
    alice.send("hello all")
    alice.wait(r"\[送达\] seq=\d+ 已送达", since=ma)           # 需求 3：已送达
    bob.wait(r"\[消息\] alice → 所有人: hello all", since=mb)  # 群聊送达

    ma, mb = alice.mark(), bob.mark()
    alice.send("@bob secret")
    bob.wait(r"\[消息\] alice → 我: secret", since=mb)          # 私聊送达
    print("  通过：登录、群聊、私聊、ACK→已送达、在线列表")


@run_case("T1 kill -9：服务器秒级剔除并广播下线（FIN 路径）")
def t1(ctx):
    alice = Client("alice", ctx.port, ctx.workdir, )
    bob = Client("bob", ctx.port, ctx.workdir, )
    ctx.clients = [alice, bob]
    alice.wait(r"\[状态\] 已连接")
    bob.wait(r"\[状态\] 已连接")

    mb = bob.mark()
    alice.kill9()                                                      # 需求 5 场景一
    i = bob.wait(r"\[系统\] alice 掉线，已下线", since=mb, timeout=5)   # 广播下线
    # 链式 since=i：USERLIST 与 SYSTEM 背靠背到达，重新 mark 会错过
    j = bob.wait(r"\[在线\] bob", since=i)                             # 在线列表已去掉 alice
    expect(bob.count(r"alice", since=j) == 0, "USERLIST 仍含 alice")
    print("  通过：kill -9 → FIN 到达 → 秒级剔除并广播（「30s 内」满足；见 VERIFY 注1）")


@run_case("T1b 半开超时：无 FIN 时按 idle 阈值剔除（30s 路径的快进版）")
def t1b(ctx):
    bob = Client("bob", ctx.port, ctx.workdir, )
    ctx.clients = [bob]
    bob.wait(r"\[状态\] 已连接")

    mb = bob.mark()
    alice = Client("alice", ctx.port, ctx.workdir, )
    ctx.clients.append(alice)
    bob.wait(r"\[系统\] alice 进入了聊天室", since=mb)
    alice.wait(r"\[状态\] 已连接")

    mb = bob.mark()
    alice.freeze()  # SIGSTOP：无 FIN 的半开连接（死机/断电/拔线就是这个状态）
    # 服务器扫描线程 idle=3s/scan=0.5s：超时剔除并广播「连接超时」（默认参数=30s/5s）
    bob.wait(r"\[系统\] alice 连接超时，已下线", since=mb, timeout=10)
    print("  通过：半开连接被扫描线程按 idle 阈值剔除（默认参数即需求 1 的 30s/5s）")
    alice.unfreeze()
    alice.quit()
    ctx.clients = [bob]


@run_case("T1c 顶号：僵尸占名时同名重连不锁门")
def t1c(ctx):
    bob = Client("bob", ctx.port, ctx.workdir, )
    ctx.clients = [bob]
    bob.wait(r"\[状态\] 已连接")

    mb = bob.mark()
    alice1 = Client("alice", ctx.port, ctx.workdir, )
    ctx.clients.append(alice1)
    alice1.wait(r"\[状态\] 已连接")
    bob.wait(r"\[系统\] alice 进入了聊天室", since=mb)
    alice1.freeze()  # 僵尸占名（旧连接未被 30s 剔除前的窗口期）

    alice2 = Client("alice", ctx.port, ctx.workdir, file_tag="_alt")
    ctx.clients.append(alice2)
    alice2.wait(r"\[状态\] 已连接", timeout=10)   # 若同名被拒绝登录，这里会超时 → 失败
    alice2.wait(r"登录成功")

    mb = bob.mark()
    bob.send("@alice hi-takeover")
    alice2.wait(r"\[消息\] bob → 我: hi-takeover", timeout=8)  # 消息路由到新连接
    expect(alice2.count(r"登录成功") == 1, "重复登录成功回执")
    print("  通过：同名顶号接管，不被自己的僵尸锁在门外（Q4）")
    alice2.quit()
    alice1.unfreeze()
    alice1.quit()
    ctx.clients = [bob]


@run_case("T2 拔网线：退避重连 + 自动 LOGIN + 补发不丢不重 + 收离线消息")
def t2(ctx):
    proxy = TcpProxy(ctx.proxy_port, ctx.port)
    ctx.proxy = proxy
    alice = Client("alice", ctx.proxy_port, ctx.workdir, )
    bob = Client("bob", ctx.port, ctx.workdir, )
    ctx.clients = [alice, bob]
    alice.wait(r"\[状态\] 已连接", timeout=10)
    bob.wait(r"\[状态\] 已连接")

    # ---- 拔网线（双向黑洞、无 FIN）：服务器侧留半开僵尸，客户端心跳判死 ----
    ma = alice.mark()
    proxy.kill()

    # ---- 对端趁半开窗口发私聊：此刻服务器眼里 alice 还「在线」，提前投递写进黑洞——
    # store-first 先落库保证不丢（产品级修复点），alice 重连后以「离线消息」补发 ----
    mb = bob.mark()
    bob.send("@alice store-for-alice")
    bob.wait(r"\[送达\] seq=\d+ 已送达", since=mb, timeout=8)   # 已送达=已持久化

    # ---- 客户端心跳判死 → 退避重连循环（需求 1 客户端侧 + 需求 2）----
    alice.wait(r"\[心跳\].*判连接死亡", since=ma, timeout=8)
    alice.wait(r"\[状态\] 重连第 \d+ 次", since=ma, timeout=8)   # 需求 2：状态栏

    # ---- 断网期间发送：进本地待发队列（需求 3）----
    alice.send("@bob offline-1")
    alice.send("@bob offline-2")
    alice.send("@bob offline-3")
    time.sleep(0.3)
    ma = alice.mark()
    alice.send("/status")
    i = alice.wait(r"\[队列\] seq=\d+ to=bob state=sending", since=ma, timeout=5)
    # 链式等满 3 条（/status 三行背靠背；同步 count 会与输出到达赛跑）
    i = alice.wait(r"\[队列\]", since=i + 1, timeout=5)
    alice.wait(r"\[队列\]", since=i + 1, timeout=5)

    # ---- 网络恢复 → 自动重连 + 原用户名 LOGIN + 按 seq 序补发 ----
    # mark 在 restart【之前】取——补发/离线消息到达极快，动作后 mark 必然错过
    ma, mb = alice.mark(), bob.mark()
    proxy.restart()
    alice.wait(r"\[状态\] 已连接", since=ma, timeout=15)         # 需求 2：自动重连
    alice.wait(r"\[补发\] 待发队列 3 条", since=ma, timeout=8)   # 需求 3：按序补发
    # 三条补发各自获 ACK（链式等满；同步 count 会与 ACK 到达赛跑）
    i = alice.wait(r"\[送达\] seq=\d+ 已送达", since=ma, timeout=8)
    i = alice.wait(r"\[送达\] seq=\d+ 已送达", since=i + 1, timeout=8)
    i = alice.wait(r"\[送达\] seq=\d+ 已送达", since=i + 1, timeout=8)
    expect(alice.count(r"\[送达\] seq=\d+ 已送达", since=ma) == 3, "补发获 ACK 数不是 3")
    alice.wait(r"\[消息\] \[离线消息\] bob → 我: store-for-alice", since=ma, timeout=8)  # 需求 4

    # ---- bob 恰好一次收到补发的三条（需求 6 去重兜底验证）----
    i1 = bob.wait(r"\[消息\] alice → 我: offline-1", since=mb, timeout=8)
    i2 = bob.wait(r"\[消息\] alice → 我: offline-2", since=i1, timeout=8)
    bob.wait(r"\[消息\] alice → 我: offline-3", since=i2, timeout=8)
    time.sleep(0.5)
    expect(bob.count(r"offline-1", since=mb) == 1, "offline-1 重复投递")
    expect(bob.count(r"offline-2", since=mb) == 1, "offline-2 重复投递")
    expect(bob.count(r"offline-3", since=mb) == 1, "offline-3 重复投递")
    print("  通过：断网入队不丢；恢复后退避重连、自动 LOGIN、按序补发恰好一次；离线消息入库补发")


@run_case("T2b 幂等：同一 (user, seq) 重复帧只投递一次")
def t2b(ctx):
    bob = Client("bob", ctx.port, ctx.workdir, )
    ctx.clients = [bob]
    bob.wait(r"\[状态\] 已连接")

    raw = raw_login(ctx.port, "rawuser")
    mb = bob.mark()
    frame = raw_msg("MESSAGE", "rawuser", "bob", "dup-check", 7)
    raw_send(raw, frame)
    ack1 = raw_wait(raw, "ACK")
    expect(ack1.get("content") == "7", "首次未获 ACK(seq=7)")
    raw_send(raw, frame)  # 重连补发产生的重复帧
    ack2 = raw_wait(raw, "ACK")
    expect(ack2.get("content") == "7", "重复帧也应 ACK（幂等应答——需求 6）")

    bob.wait(r"\[消息\] rawuser → 我: dup-check", since=mb, timeout=8)
    time.sleep(0.5)  # 给潜在重复投递留时间窗
    expect(bob.count(r"dup-check", since=mb) == 1, "重复帧被二次投递")
    raw.close()
    print("  通过：(user,seq) 去重——重复帧无副作用但有 ACK，接收端只显示一次")


@run_case("T2c 离线消息：入库、按时间排序补发、「离线消息」样式")
def t2c(ctx):
    raw = raw_login(ctx.port, "mailer")
    # dave 不在线 → 三条私聊入库；ts 故意乱序发出（100,300,200），补发必须按 ts 排
    raw_send(raw, raw_msg("MESSAGE", "mailer", "dave", "m-ts100", 2, ts=100))
    raw_wait(raw, "ACK")
    raw_send(raw, raw_msg("MESSAGE", "mailer", "dave", "m-ts300", 3, ts=300))
    raw_wait(raw, "ACK")
    raw_send(raw, raw_msg("MESSAGE", "mailer", "dave", "m-ts200", 4, ts=200))
    raw_wait(raw, "ACK")

    dave = Client("dave", ctx.port, ctx.workdir, )
    ctx.clients = [dave]
    md = dave.mark()
    i1 = dave.wait(r"\[消息\] \[离线消息\] mailer → 我: m-ts100", since=md, timeout=8)  # 需求 4 样式
    i2 = dave.wait(r"\[消息\] \[离线消息\] mailer → 我: m-ts200", since=i1, timeout=8)
    dave.wait(r"\[消息\] \[离线消息\] mailer → 我: m-ts300", since=i2, timeout=8)
    expect(dave.count(r"m-ts100", since=md) == 1, "离线消息重复")
    raw.close()
    print("  通过：离线入库 → 上线按 ts 升序补发 → 「离线消息」样式、无重复")


@run_case("T3 服务器重启：客户端重连、在线列表重建、断档消息不丢")
def t3(ctx):
    alice = Client("alice", ctx.port, ctx.workdir, )
    bob = Client("bob", ctx.port, ctx.workdir, )
    ctx.clients = [alice, bob]
    alice.wait(r"\[状态\] 已连接", timeout=10)
    bob.wait(r"\[状态\] 已连接", timeout=10)
    bob.wait(r"\[在线\].*alice", timeout=8)

    # 服务器被强杀（等价 kill -9 / 崩溃 / 断电）
    ctx.server.kill9()
    ctx.server = None
    ma = alice.mark()
    alice.wait(r"\[状态\] 重连第 \d+ 次", since=ma, timeout=8)   # 双双进入重连

    # 断档期发送（消息不丢）→ 待发队列
    alice.send("@bob across-restart")
    time.sleep(0.3)
    ma = alice.mark()
    alice.send("/status")
    alice.wait(r"\[队列\] seq=\d+ to=bob state=sending", since=ma, timeout=5)

    # 服务器重启（同一 SQLite 文件 → 去重/离线状态延续）
    # mark 在重启动作【之前】取：登录触发的 USERLIST/消息到达很快
    ma, mb = alice.mark(), bob.mark()
    ctx.server = Server(ctx.port, ctx.workdir)
    alice.wait(r"\[状态\] 已连接", since=ma, timeout=15)
    bob.wait(r"\[状态\] 已连接", since=mb, timeout=15)

    # 在线列表重建：双方都应看到对方（登录触发 USERLIST 广播）
    alice.wait(r"\[在线\].*bob", since=ma, timeout=8)
    bob.wait(r"\[在线\].*alice", since=mb, timeout=8)

    # 断档消息恰好一次送达（bob 未上线则走离线库补发——两条路径都算）
    bob.wait(r"\[消息\]( \[离线消息\])? alice → 我: across-restart", since=mb, timeout=10)
    time.sleep(0.5)
    expect(bob.count(r"across-restart", since=mb) == 1, "重启后消息重复/丢失")
    print("  通过：服务器重启后全量重连、在线列表重建、断档期消息恰好一次送达")


# ---------------- 框架 ----------------

class Ctx(object):
    pass


def build_server():
    src = os.path.join(ROOT, "chat_server_v4.cpp")
    if os.path.exists(SERVER_BIN) and os.path.getmtime(SERVER_BIN) > os.path.getmtime(src):
        return
    print("编译 chat_server_v4 ...")
    sqlite = "/usr/lib/x86_64-linux-gnu/libsqlite3.so.0"
    cmd = ["g++", "-std=c++11", "-Wall", "-pthread", src, "-o", SERVER_BIN]
    cmd.append(sqlite if os.path.exists(sqlite) else "-lsqlite3")
    subprocess.check_call(cmd)


def cleanup_clients(ctx):
    for c in ctx.clients:
        if c:
            try:
                c.unfreeze()
            except Exception:
                pass
            try:
                c.quit()
            except Exception:
                pass
    ctx.clients = []


def main():
    only = set(sys.argv[1:])
    build_server()
    workdir = tempfile.mkdtemp(prefix="chat_rel_")
    ctx = Ctx()
    ctx.workdir = workdir
    ctx.port = 18901
    ctx.proxy_port = 18902
    ctx.clients = []
    ctx.proxy = None
    ctx.server = None
    failed = []
    try:
        ctx.server = Server(ctx.port, workdir)
        for name, fn in CASES:
            if only and not any(name.startswith(t) for t in only):
                continue
            print("== %s ==" % name)
            try:
                fn(ctx)
            except AssertionError as e:
                print("  失败: %s" % e)
                failed.append(name)
                for c in ctx.clients:
                    if c:
                        c.dump()
                if ctx.server:
                    ctx.server.dump_log()
            except Exception as e:
                print("  异常: %r" % e)
                failed.append(name)
                for c in ctx.clients:
                    if c:
                        c.dump()
                if ctx.server:
                    ctx.server.dump_log()
            finally:
                cleanup_clients(ctx)  # 用例间不残留同名客户端（防顶号互踢干扰）
                if ctx.proxy:
                    ctx.proxy.kill()
                    ctx.proxy = None
    finally:
        cleanup_clients(ctx)
        if ctx.proxy:
            ctx.proxy.kill()
        if ctx.server:
            ctx.server.stop()
        shutil.rmtree(workdir, ignore_errors=True)

    print()
    if failed:
        print("结果: %d 失败 -> %s" % (len(failed), ", ".join(failed)))
        return 1
    print("结果: 全部通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
