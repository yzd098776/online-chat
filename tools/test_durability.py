#!/usr/bin/env python3
"""tools/test_durability.py —— 持久性对拍：ACK 过的消息，kill -9 后必须在库里一条不少

不变式（README/协议 6.2）：**ACK = 已持久化**。group commit 引入后尤其要回头验：
   写手塞进批次 → 批次 COMMIT（synchronous=FULL，fsync 落盘）→ 唤醒 → 才发 ACK
若为压延迟让写手入批即 ACK（fsync 交给别人做），kill -9/掉电 = **ACK 过的消息凭空消失**
——竞态是崩给你看，这个是悄悄错。

验证方法（VERIFY.md kill -9 场景的自动化版）：
  ① 起服务（关闭限流），N 连接洪水；客户端把收到的每个 ACK（(user, 消息内容)）记在内存；
  ② 洪水进行中 kill -9 服务端；
  ③ 打开库对拍：每个 ACK 过的内容必须能在 messages 表按 sender 查到——少一条即失败。
客户端进程活着（只杀服务端），所以 kill 前收到的 ACK 集合是可信的；服务器已发但还在
内核缓冲里的 ACK 读不到不影响结论（那些消息必然已 COMMIT，只会让对拍更宽松，不会漏判）。

用法：python3 tools/test_durability.py [--server-bin build/chat_server_v5] [--conns 30] [--flood 8]
"""
import argparse
import os
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_ROOT, "tools"))
sys.path.insert(0, os.path.join(_ROOT, "client"))
from test_v5_smoke import Client, find_free_port  # 与冒烟同源的裸帧客户端（挑战-应答已内置）


def worker(idx, port, stop_ev, acked, lock, errs):
    """一个洪水连接：注册/登录/建房进房 → 按 seq 连发并等 ACK，记录 (user, content)"""
    user = "dur%03d" % idx
    try:
        c = Client(port)
        f = c.do_register(user, "dur-pw")
        if f.get("type") != "REGISTER_OK" and int(f.get("code", 0)) != 1001:
            raise AssertionError("注册失败 %s" % f)
        f = c.do_login(user, "dur-pw")
        if f.get("type") != "AUTH_OK":
            raise AssertionError("登录失败 %s" % f)
        c.send("CREATE", "durroom%03d" % idx)
        c.recv(timeout=3.0)  # CREATE_OK / E2001
        c.send("JOIN", "durroom%03d" % idx)
        c.recv_type("JOIN_OK")
        while True:  # 清进房噪声
            try:
                if c.recv(timeout=0.1) is None:
                    break
            except (OSError, ValueError):
                break
        seq = 0
        pending = {}  # seq(str) -> content：流水线突发，一次发一批再收 ACK
        while not stop_ev.is_set():
            for _ in range(20):  # 突发窗口：同时在途 20 条，拉大「ACK 了但未提交」的可检测面
                seq += 1
                content = "dur-%s-%d" % (user, seq)
                sent = c.send("MESSAGE", content, to="ALL")
                pending[str(sent.get("seq"))] = content
            deadline = time.time() + 10.0
            while pending and time.time() < deadline and not stop_ev.is_set():
                f = c.recv(timeout=0.5)
                if f is None:
                    continue
                if f.get("type") == "ACK":
                    content = pending.pop(f.get("content", ""), None)
                    if content is not None:
                        with lock:
                            acked.append((user, content))  # 只记「已收 ACK」的
                elif f.get("type") in ("NACK", "ERR"):
                    raise AssertionError("被拒 %s" % f)
        c.close()
    except Exception as e:  # noqa: BLE001 —— 断开=预期（服务端被杀），记录供排查
        errs.append("w%d: %r" % (idx, e))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--server-bin", default=os.path.join(_ROOT, "build/chat_server_v5"))
    ap.add_argument("--conns", type=int, default=30)
    ap.add_argument("--flood", type=float, default=8.0, help="洪水时长秒数（中途 kill -9）")
    ap.add_argument("--keep-db", action="store_true")
    args = ap.parse_args()

    tmpdir = tempfile.mkdtemp(prefix="dur_")
    db_path = os.path.join(tmpdir, "dur.db")
    port = find_free_port()
    proc = subprocess.Popen(
        [args.server_bin, str(port), "--db", db_path, "--idle", "60000", "--scan", "1000",
         "--conn-rate", "0", "--msg-rate", "0", "--pepper", os.path.join(tmpdir, "pepper.key")],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        for _ in range(50):
            try:
                Client(port, timeout=0.3).close()
                break
            except OSError:
                time.sleep(0.1)
        else:
            print("服务器启动失败")
            return 1

        stop_ev = threading.Event()
        acked, errs, lock = [], [], threading.Lock()
        threads = [threading.Thread(target=worker, args=(i, port, stop_ev, acked, lock, errs),
                                    daemon=True) for i in range(args.conns)]
        for t in threads:
            t.start()
        print("洪水 %.0fs（%d 连接），中途 kill -9 ..." % (args.flood, args.conns))
        time.sleep(args.flood)
        mid = len(acked)
        proc.kill()               # SIGKILL：不给服务器任何收尾机会（≈ 掉电/崩溃）
        proc.wait(timeout=5)
        stop_ev.set()
        for t in threads:
            t.join(timeout=6)
        print("kill 时刻已收 ACK %d 条（断开后读到 %d 条，含 kill 前在途的）" % (mid, len(acked)))

        con = sqlite3.connect(db_path)
        cache, missing = {}, []
        for user, content in acked:
            if user not in cache:
                cache[user] = set(r[0] for r in con.execute(
                    "SELECT content FROM messages WHERE sender = ?", (user,)))
            if content not in cache[user]:
                missing.append((user, content))
        total_rows = con.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
        con.close()
        print("messages 表落库 %d 行；对拍 ACK 集合 %d 条，缺失 %d 条"
              % (total_rows, len(acked), len(missing)))
        if missing:
            print("✗ 持久性被破坏（ACK 过却不在库里，前 5 条）：%s" % missing[:5])
            return 1
        print("✓ 每个 ACK 都能在库里查到——ACK=已持久化 成立（kill -9 无丢）")
        if errs:
            print("（断开记录，预期内：%s 等共 %d 条）" % (errs[0], len(errs)))
        return 0
    finally:
        if proc.poll() is None:
            proc.kill()
        if not args.keep_db:
            for suf in ("", "-wal", "-shm"):
                try:
                    os.remove(db_path + suf)
                except OSError:
                    pass
            try:
                os.remove(os.path.join(tmpdir, "pepper.key"))
                os.rmdir(tmpdir)
            except OSError:
                pass


if __name__ == "__main__":
    sys.exit(main())
