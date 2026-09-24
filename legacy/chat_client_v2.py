#!/usr/bin/env python3
"""在线聊天客户端 v2 —— 长度前缀 + JSON 协议
阶段1：帧收发骨架 + PING/PONG 自测（业务消息在阶段2实现）

用法:
  python3 chat_client_v2.py                          # 图形界面（默认 localhost:8888）
  python3 chat_client_v2.py 192.168.1.10 --port 8888 # 指定服务器
  python3 chat_client_v2.py --selftest --count 3     # 无 GUI 帧层自测（3 条 PING）
  python3 chat_client_v2.py 192.168.1.10 --selftest  # 两机联测时在 Windows 侧跑

帧格式: [4 字节大端 uint32 长度][UTF-8 JSON body]，MAX_FRAME = 1 MiB
仅用 Python 标准库（json / struct / socket / threading / tkinter）。
验证平台：Ubuntu 回环自测；Windows 笔记本以同样命令联测（规则 2）。
"""

import argparse
import json
import socket
import struct
import sys
import threading
import time

MAX_FRAME = 1 << 20  # 1 MiB，与服务器一致
PROTO_VER = 1


# ---------------- 帧层：struct 读长度头 + recv_exact 循环收满 ----------------

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


# ---------------- 自测模式（无 GUI）：PING -> PONG 完整性校验 ----------------

def selftest(host, port, count):
    print("=" * 56)
    print("v2 帧层自测: %s:%d，PING x %d" % (host, port, count))
    print("内容故意含换行 / emoji / 竖线 |，逐字节比对回显")
    print("=" * 56)
    try:
        sock = socket.create_connection((host, port), timeout=5)
    except OSError as e:
        print("[失败] 无法连接 %s:%d -> %s" % (host, port, e))
        return 1
    sock.settimeout(5)
    try:
        for i in range(1, count + 1):
            content = "第%d号 第一行\n第二行 😀 |竖线|" % i
            send_frame(sock, make_msg("PING", "selftest", "SERVER", content, i))
            reply = recv_frame(sock)
            if reply.get("type") != "PONG" or reply.get("content") != content:
                print("[失败] 第 %d 条回显不一致:" % i)
                print("  发送: %r" % content)
                print("  收到: %r" % reply)
                return 1
        print("[通过] %d/%d 条 PING→PONG 完整往返（不多不少）" % (count, count))
        return 0
    except (OSError, ValueError, ConnectionError) as e:
        print("[失败] 收发异常: %r" % e)
        return 1
    finally:
        sock.close()


# ---------------- 图形界面骨架（阶段1：仅帧层联通；业务消息阶段2接入） ----------------

def run_gui(host, port):
    import tkinter as tk
    from tkinter import scrolledtext

    class App(object):
        def __init__(self):
            self.sock = None
            self.connected = False
            self.keep_running = True
            self.recv_thread = None
            self.seq = 0

            self.root = tk.Tk()
            self.root.title("在线聊天 v2（阶段1：帧层骨架）")
            self.root.geometry("640x460")
            self.root.protocol("WM_DELETE_WINDOW", self.on_closing)

            conn = tk.Frame(self.root)
            conn.pack(fill=tk.X, padx=8, pady=4)
            tk.Label(conn, text="服务器:").pack(side=tk.LEFT)
            self.host_entry = tk.Entry(conn, width=16)
            self.host_entry.insert(0, host)
            self.host_entry.pack(side=tk.LEFT, padx=2)
            tk.Label(conn, text="端口:").pack(side=tk.LEFT)
            self.port_entry = tk.Entry(conn, width=6)
            self.port_entry.insert(0, str(port))
            self.port_entry.pack(side=tk.LEFT, padx=2)
            self.connect_btn = tk.Button(conn, text="连接", width=8, command=self.toggle_conn)
            self.connect_btn.pack(side=tk.LEFT, padx=4)
            self.ping_btn = tk.Button(
                conn, text="发送 PING", width=10, command=self.send_ping, state=tk.DISABLED
            )
            self.ping_btn.pack(side=tk.LEFT, padx=4)

            self.log = scrolledtext.ScrolledText(self.root, state=tk.DISABLED, wrap=tk.WORD)
            self.log.pack(fill=tk.BOTH, expand=True, padx=8, pady=4)

            self.status = tk.Label(self.root, text="未连接", anchor="w", relief=tk.SUNKEN)
            self.status.pack(fill=tk.X, side=tk.BOTTOM)

            self.append_log("阶段1：仅帧层收发（LOGIN/MESSAGE 业务在阶段2实现）\n")

        def append_log(self, text):
            self.log.config(state=tk.NORMAL)
            self.log.insert(tk.END, text + "\n")
            self.log.config(state=tk.DISABLED)
            self.log.see(tk.END)

        def toggle_conn(self):
            if not self.connected:
                self.do_connect()
            else:
                self.do_disconnect()

        def do_connect(self):
            try:
                h = self.host_entry.get().strip()
                p = int(self.port_entry.get())
                self.sock = socket.create_connection((h, p), timeout=5)
                self.sock.settimeout(None)  # 阻塞模式，接收线程循环收帧
                self.connected = True
                self.keep_running = True
                self.recv_thread = threading.Thread(target=self.recv_loop, daemon=True)
                self.recv_thread.start()
                self.connect_btn.config(text="断开")
                self.ping_btn.config(state=tk.NORMAL)
                self.status.config(text="已连接 %s:%d（帧层）" % (h, p))
                self.append_log("[连接] %s:%d" % (h, p))
            except OSError as e:
                self.append_log("[失败] 连接失败: %r" % e)

        def do_disconnect(self):
            self.keep_running = False
            self.connected = False
            if self.sock:
                try:
                    self.sock.close()
                except OSError:
                    pass
                self.sock = None
            self.connect_btn.config(text="连接")
            self.ping_btn.config(state=tk.DISABLED)
            self.status.config(text="未连接")
            self.append_log("[断开] 已断开")

        def send_ping(self):
            if not self.connected:
                return
            self.seq += 1
            content = "PING #%d 😀\n第二行" % self.seq
            try:
                send_frame(self.sock, make_msg("PING", "gui", "SERVER", content, self.seq))
                self.append_log("[发] seq=%d PING content=%r" % (self.seq, content))
            except (OSError, ValueError) as e:
                self.append_log("[失败] 发送异常: %r" % e)

        def recv_loop(self):
            # 线程内不做任何 tkinter 调用；显示一律 root.after 回主线程
            while self.connected and self.keep_running:
                try:
                    frame = recv_frame(self.sock)
                    self.root.after(0, lambda f=frame: self.append_log("[收] %s" % json.dumps(f, ensure_ascii=False)))
                except (OSError, ValueError, ConnectionError) as e:
                    if self.keep_running:
                        self.root.after(0, lambda er=e: self.append_log("[断开] 接收终止: %r" % er))
                    self.root.after(0, self.do_disconnect)
                    break

        def on_closing(self):
            self.do_disconnect()
            self.root.destroy()

        def run(self):
            self.root.mainloop()

    App().run()


def main():
    ap = argparse.ArgumentParser(description="在线聊天客户端 v2（长度前缀 + JSON）")
    ap.add_argument("host", nargs="?", default="localhost", help="服务器地址（默认 localhost）")
    ap.add_argument("--port", type=int, default=8888, help="端口（默认 8888）")
    ap.add_argument("--selftest", action="store_true", help="无 GUI：PING/PONG 帧层自测后退出")
    ap.add_argument("--count", type=int, default=1, help="自测发送 PING 条数（默认 1）")
    args = ap.parse_args()

    if args.selftest:
        sys.exit(selftest(args.host, args.port, args.count))
    run_gui(args.host, args.port)


if __name__ == "__main__":
    main()
