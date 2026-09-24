#!/usr/bin/env python3
"""tools/bench.py —— 聊天服务器压测：并发连接数 / 发送速率可调，输出 CSV + Markdown 表格

度量口径：
  QPS      —— 测量窗口内【收到 ACK 的消息数 / 窗口秒数】（端到端含落库，因为服务器
              先写 SQLite 再回 ACK——这是「已送达=已持久化」语义下的真实吞吐）
  延迟     —— 单条 MESSAGE 从 send() 到对应 ACK 到达的 RTT（同机 loopback 时含
              客户端调度误差）；P50/P90/P99 由样本排序取分位
  CPU/内存 —— 采样 /proc/<pid>/stat（utime+stime，换算测量窗口平均 CPU%）与
              /proc/<pid>/statm（RSS 峰值）；需要 --pid 或自动发现 chat_server_v5

用法示例：
  python3 tools/bench.py --connections 50 --rate 2 --duration 10   # 50 连接×2 msg/s=100 QPS 目标
  python3 tools/bench.py --connections 100 --rate 5 --pid 12345    # 指定服务器 pid 采样资源
  python3 tools/bench.py --room lobby                              # 全员挤大厅（含广播放大）
输出：
  tools/bench_result.csv    每次运行追加一行（参数 + 结果，便于多组对比）
  tools/bench_report.md     本次运行的 Markdown 报告（并同步打印到 stdout）
"""
import argparse
import csv
import hashlib
import hmac
import os
import socket
import ssl
import struct
import sys
import threading
import time

sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "client"))
from chat_client_v4 import make_msg, recv_frame, send_frame  # 帧层与正式客户端同源

CLK_TCK = os.sysconf("SC_CLK_TCK")
PAGE_SIZE = os.sysconf("SC_PAGE_SIZE")


# ---------------- /proc 采样（CPU/内存占用） ----------------

class ProcSampler(object):
    """采样 /proc/<pid>/stat 与 statm；线程安全快照，测量结束取平均 CPU% 与 RSS 峰值。"""

    def __init__(self, pid, interval=0.5):
        self.pid = pid
        self.interval = interval
        self._stop = threading.Event()
        self._thread = None
        self.rss_peak = 0
        self._first_t = None
        self._first_jiffies = None
        self._last_jiffies = 0

    def _read(self):
        with open("/proc/%d/stat" % self.pid) as f:
            stat = f.read()
        rparen = stat.rfind(")")
        fields = stat[rparen + 2:].split()
        utime, stime = int(fields[11]), int(fields[12])  # 第 14/15 字段（去掉 pid+comm 后 11/12）
        with open("/proc/%d/statm" % self.pid) as f:
            rss_pages = int(f.read().split()[1])
        return utime + stime, rss_pages * PAGE_SIZE

    def _loop(self):
        while not self._stop.is_set():
            try:
                jiffies, rss = self._read()
            except OSError:
                break
            now = time.time()
            self.rss_peak = max(self.rss_peak, rss)
            if self._first_t is None:
                self._first_t, self._first_jiffies = now, jiffies
            self._last_jiffies = jiffies
            self._stop.wait(self.interval)

    def start(self):
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)

    def summary(self):
        # CPU% = Δ(utime+stime) / 墙钟 —— 用总量比而非逐样本比率：
        # utime+stime 是整数 jiffy（10ms 粒度），0.5s 间隔逐样本差分在低占用时会被量化抹零
        if self._first_t is None or self._last_jiffies <= self._first_jiffies:
            return 0.0, self.rss_peak
        wall = time.time() - self._first_t
        if wall <= 0:
            return 0.0, self.rss_peak
        cpu = (self._last_jiffies - self._first_jiffies) / CLK_TCK / wall * 100.0
        return cpu, self.rss_peak


def find_server_pid():
    """按 /proc/<pid>/exe 精确匹配二进制名——不能 grep cmdline（shell 的命令行里也会
    含 'chat_server_v5' 字样，pgrep -f 会误抓外壳进程，压测 CPU 就全是 0）"""
    for name in os.listdir("/proc"):
        if not name.isdigit():
            continue
        try:
            exe = os.readlink("/proc/%s/exe" % name)
        except OSError:
            continue
        if exe.endswith("/chat_server_v5"):
            return int(name)
    return None


# ---------------- 压测 worker ----------------

class Worker(object):
    """一个并发连接：注册/登录/进房 → 按速率发 MESSAGE → 记录 send→ACK 延迟。"""

    def __init__(self, idx, host, port, room, rate, lock, use_tls=False, ca_file=""):
        self.idx = idx
        self.host, self.port = host, port
        self.room = ("bench_w%03d" % idx) if room == "private" else room
        self.use_tls = use_tls
        self.ca_file = ca_file
        self.rate = rate
        self.lock = lock
        self.user = "bench%03d" % idx
        self.pw = "bench-pw-1"
        self.seq = 0
        self.sent = 0
        self.acked = 0
        self.nacked = 0
        self.lost = 0
        self.ready = False
        self.latencies = []
        self.pending = {}               # seq -> send_ts
        self.sock = None

    def _recv(self, timeout):
        self.sock.settimeout(timeout)
        try:
            return recv_frame(self.sock)
        except (socket.timeout, TimeoutError):
            return None

    def _wait(self, ftype, timeout=8.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            f = self._recv(max(0.05, deadline - time.time()))
            if f is None:
                continue
            if f.get("type") == ftype:
                return f
            if f.get("type") == "ERR" and ftype != "ERR":
                raise RuntimeError("E%s %s" % (f.get("code"), f.get("content")))
        raise RuntimeError("等待 %s 超时" % ftype)

    def _ctl(self, mtype, content=""):
        self.seq += 1
        send_frame(self.sock, make_msg(mtype, self.user, "SERVER", content, self.seq))

    def _challenge_auth(self, hello_type, final_type):
        """挑战-应答（Q7）：*_HELLO → CHALLENGE{nonce,salt} → 证明帧。

        注册证明 = {K=PBKDF2(pwd,salt), hmac=HMAC(K,nonce)}（播种，Q7②）；
        登录证明 = content=HMAC(K,nonce)（口令/K 不进帧，Q7①）。
        返回最终帧（成功或 ERR）。超时放宽到 15s：客户端侧 PBKDF2 在 GIL 下百并发会排队。
        """
        self._ctl(hello_type, "")
        f = self._wait_any(("CHALLENGE", "ERR"), timeout=30.0)
        if f.get("type") == "ERR":
            return f
        k = hashlib.pbkdf2_hmac("sha256", self.pw.encode("utf-8"),
                                bytes.fromhex(f.get("salt", "")), 100000, 32)
        # proof = HMAC(K, op‖user‖nonce)：绑定协议语境+身份（Q7 纵深防御）
        proof = hmac.new(k, (final_type + self.user).encode("utf-8")
                         + bytes.fromhex(f.get("content", "")), hashlib.sha256).hexdigest()
        self.seq += 1
        if final_type == "REGISTER":
            body = make_msg("REGISTER", self.user, "SERVER", k.hex(), self.seq)
            body["hmac"] = proof
        else:
            body = make_msg("LOGIN", self.user, "SERVER", proof, self.seq)
        send_frame(self.sock, body)
        want = "REGISTER_OK" if final_type == "REGISTER" else "AUTH_OK"
        return self._wait_any((want, "ERR"), timeout=30.0)

    def _wait_any(self, types, timeout=8.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            f = self._recv(max(0.05, deadline - time.time()))
            if f is not None and f.get("type") in types:
                return f
        raise RuntimeError("等待 %s 超时" % (types,))

    def setup(self):
        self.sock = socket.create_connection((self.host, self.port), timeout=5)
        if self.use_tls:  # C：TLS 对比压测（自签教学默认不验）
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE if not self.ca_file else ssl.CERT_REQUIRED
            if self.ca_file:
                ctx.load_verify_locations(self.ca_file)
            self.sock = ctx.wrap_socket(self.sock, server_hostname=self.host)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        # 注册（E1001=已注册，忽略）。客户端侧 PBKDF2 是重操作，错峰建连时应答会迟到——
        # 必须等到 REGISTER_OK / ERR 为止（建连风暴踩过的坑）。
        self._challenge_auth("REGISTER_HELLO", "REGISTER")
        # 上一轮压测的同名连接可能还没被服务器判死 → E1004；稍候重试即可（不静默失败）
        for attempt in range(4):
            f = self._challenge_auth("LOGIN_HELLO", "LOGIN")
            if f.get("type") == "AUTH_OK":
                break
            if "1004" in str(f.get("code", "")) and attempt < 3:
                time.sleep(0.75)
                continue
            raise RuntimeError("E%s %s" % (f.get("code"), f.get("content")))
        self._ctl("CREATE", self.room)          # 房间不存在则建（重名 E2001 忽略）
        self._recv(3.0)
        self._ctl("JOIN", self.room)
        self._wait("JOIN_OK")
        while True:                            # 清掉进房 USERLIST/SYSTEM 噪声
            f = self._recv(0.2)
            if f is None:
                break
        self.ready = True

    def _pump(self, until):
        """收帧直到 until 时刻：完成 pending 的 ACK 记录，顺手丢弃广播回显"""
        while time.time() < until:
            f = self._recv(min(0.2, max(0.01, until - time.time())))
            if f is None:
                continue
            ftype = f.get("type")
            if ftype == "ACK":
                try:
                    seq = int(f.get("content", "0"))
                except ValueError:
                    continue
                t0 = self.pending.pop(seq, None)
                if t0 is not None:
                    self.acked += 1
                    self.latencies.append((time.time() - t0) * 1000.0)
            elif ftype == "NACK":
                try:
                    seq = int(f.get("content", "0"))
                except ValueError:
                    continue
                self.pending.pop(seq, None)
                self.nacked += 1
            elif ftype == "MESSAGE":
                pass                            # 房间广播回显/他人消息：只产生负载，不计样本

    def run_phase(self, duration, measure):
        """发 duration 秒；measure=False 为暖机（不计数）。按 rate 定节奏（rate<=0 = 尽力打满）"""
        t_start = time.time()
        t_end = t_start + duration
        interval = (1.0 / self.rate) if self.rate > 0 else 0.0
        next_send = t_start
        while time.time() < t_end:
            now = time.time()
            if now >= next_send:
                self.seq += 1
                body = "bench payload %d-%d" % (self.idx, self.seq)
                send_ts = time.time()
                send_frame(self.sock, make_msg("MESSAGE", self.user, "ALL", body, self.seq))
                if measure:
                    self.sent += 1
                    self.pending[self.seq] = send_ts
                next_send = (next_send + interval) if interval > 0 else time.time()
            self._pump(min(next_send if interval > 0 else time.time() + 0.001, t_end))
        self._pump(time.time() + 1.5)           # 尾窗：给在途 ACK 时间到达
        if measure:
            for seq, t0 in list(self.pending.items()):
                self.pending.pop(seq, None)
                self.lost += 1                  # 窗口结束仍未确认

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


def percentile(sorted_vals, p):
    if not sorted_vals:
        return 0.0
    k = (len(sorted_vals) - 1) * p / 100.0
    lo, hi = int(k), min(int(k) + 1, len(sorted_vals) - 1)
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (k - lo)


def main():
    ap = argparse.ArgumentParser(description="聊天服务器压测（并发/速率可调，CSV+Markdown）")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8888)
    ap.add_argument("--connections", type=int, default=50, help="并发连接数（默认 50）")
    ap.add_argument("--rate", type=float, default=2.0,
                    help="每连接发送速率 msg/s（默认 2；0=闭环尽力打满）")
    ap.add_argument("--duration", type=float, default=10.0, help="测量窗口秒数（默认 10）")
    ap.add_argument("--warmup", type=float, default=2.0, help="暖机秒数，不计数（默认 2）")
    ap.add_argument("--room", default="private",
                    help="压测房间：private=每连接独立房间（默认，测纯 RTT）；"
                         "任意名字=全员同房（含广播放大，测扇出）")
    ap.add_argument("--pid", type=int, default=None, help="服务器 pid（CPU/内存采样；缺省自动找）")
    ap.add_argument("--tls", action="store_true", help="走 TLS 传输（与服务器 --tls 配套）")
    ap.add_argument("--ca", default="", help="CA/自签证书路径（--tls 时可选严格校验）")
    ap.add_argument("--csv", default="tools/bench_result.csv")
    ap.add_argument("--markdown", default="tools/bench_report.md")
    args = ap.parse_args()

    pid = args.pid or find_server_pid()
    print("目标 %s:%d  并发=%d  速率=%.2f msg/s/连接（目标聚合 %.1f msg/s）  窗口=%.0fs"
          % (args.host, args.port, args.connections, args.rate,
             args.connections * args.rate, args.duration))
    print("服务器 pid=%s（CPU/内存采样%s）" % (pid, "开" if pid else "关——传 --pid 启用"))

    # 建连 + 认证 + 进房（建房者顺便把房间建出来）
    workers = []
    lock = threading.Lock()
    errors = []
    def setup_one(w):
        time.sleep(w.idx * 0.02)               # 错峰 20ms：模拟真实接入，避免百线程建连风暴
        try:
            w.setup()
        except (OSError, RuntimeError) as e:
            with lock:
                errors.append("W%d: %r" % (w.idx, e))

    for i in range(args.connections):
        workers.append(Worker(i, args.host, args.port, args.room, args.rate, lock,
                              use_tls=args.tls, ca_file=args.ca))
    threads = [threading.Thread(target=setup_one, args=(w,)) for w in workers]
    for t in threads: t.start()
    for t in threads: t.join()
    if errors:
        print("建连失败 %d 个（继续压测其余）：%s" % (len(errors), errors[0]))
    live = [w for w in workers if w.ready]
    if not live:
        print("没有可用连接，退出")
        sys.exit(1)
    print("就绪连接 %d/%d" % (len(live), args.connections))

    sampler = ProcSampler(pid) if pid else None
    if sampler:
        sampler.start()

    # 暖机（不计数）→ 测量窗口
    def run_phase_all(duration, measure):
        ts = [threading.Thread(target=w.run_phase, args=(duration, measure)) for w in live]
        for t in ts: t.start()
        for t in ts: t.join()

    if args.warmup > 0:
        print("暖机 %.0fs ..." % args.warmup)
        run_phase_all(args.warmup, False)
    print("测量 %.0fs ..." % args.duration)
    run_phase_all(args.duration, True)
    window = args.duration  # QPS 分母 = 发送窗口（各 worker 的 1.5s ACK 排空尾巴不计）

    if sampler:
        sampler.stop()
        cpu_pct, rss_peak = sampler.summary()
    else:
        cpu_pct, rss_peak = 0.0, 0

    for w in live:
        w.close()

    # ---- 汇总 ----
    lats = sorted(x for w in live for x in w.latencies)
    sent = sum(w.sent for w in live)
    acked = sum(w.acked for w in live)
    nacked = sum(w.nacked for w in live)
    lost = sum(w.lost for w in live)
    qps = acked / window if window > 0 else 0
    p50, p90, p99 = (percentile(lats, p) for p in (50, 90, 99))
    avg = (sum(lats) / len(lats)) if lats else 0.0

    row = {
        "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
        "mode": ("tls+" if args.tls else "")
                + ("private-rooms" if args.room == "private" else ("shared#" + args.room)),
        "connections": len(live),
        "rate_per_conn": args.rate,
        "duration_s": round(window, 2),
        "sent": sent,
        "acked": acked,
        "nacked": nacked,
        "lost": lost,
        "qps": round(qps, 1),
        "avg_ms": round(avg, 3),
        "p50_ms": round(p50, 3),
        "p90_ms": round(p90, 3),
        "p99_ms": round(p99, 3),
        "max_ms": round(lats[-1], 3) if lats else 0,
        "cpu_pct": round(cpu_pct, 1),
        "rss_peak_mb": round(rss_peak / 1048576.0, 1),
    }

    # CSV 追加（首行写表头）
    write_header = not os.path.exists(args.csv) or os.path.getsize(args.csv) == 0
    with open(args.csv, "a", newline="", encoding="utf-8") as f:
        wr = csv.DictWriter(f, fieldnames=list(row.keys()))
        if write_header:
            wr.writeheader()
        wr.writerow(row)

    # Markdown 报告
    md = [
        "# bench.py 压测报告",
        "",
        "- 时间：%s　目标：%s:%d　房间：%s" % (
            row["ts"], args.host, args.port,
            "每连接独立房（纯 RTT）" if args.room == "private" else "#" + args.room + "（含广播扇出）"),
        "- 并发连接 **%d**，每连接速率 **%s msg/s**，测量窗口 **%ss**（暖机 %ss 不计数）"
        % (row["connections"], args.rate, row["duration_s"], args.warmup),
        "- 传输：%s；延迟口径 = MESSAGE send → ACK RTT（服务器先写 SQLite 再 ACK）；"
        % ("TLS（自签，不验）" if args.tls else "明文 TCP"),
        "  CPU/内存来自 /proc/%s/{stat,statm} 采样。" % pid if pid else "- 无 --pid：未采样 CPU/内存。",
        "",
        "| 指标 | 数值 |",
        "|---|---|",
        "| 发送 / ACK / NACK / 丢失 | %d / %d / %d / %d |" % (sent, acked, nacked, lost),
        "| **QPS（ACKed/s）** | **%.1f** |" % qps,
        "| 延迟 avg | %.3f ms |" % avg,
        "| **延迟 P50** | **%.3f ms** |" % p50,
        "| 延迟 P90 | %.3f ms |" % p90,
        "| **延迟 P99** | **%.3f ms** |" % p99,
        "| 延迟 max | %.3f ms |" % (lats[-1] if lats else 0),
        "| CPU（窗口均值） | %.1f %% |" % cpu_pct,
        "| 内存 RSS 峰值 | %.1f MB |" % (rss_peak / 1048576.0),
        "",
    ]
    report = "\n".join(md)
    with open(args.markdown, "w", encoding="utf-8") as f:
        f.write(report)
    print()
    print(report)
    print("已写入：%s（追加） 与 %s" % (args.csv, args.markdown))


if __name__ == "__main__":
    main()
