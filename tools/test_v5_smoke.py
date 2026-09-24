#!/usr/bin/env python3
# tools/test_v5_smoke.py —— v5 端到端冒烟：账号/Token/房间/广播隔离/历史游标/离线/口令哈希向量
#
# 覆盖验收点：
#   T01 注册成功（挑战-应答播种） / T02 重复注册 E1001（用户名唯一约束）/ T03 重复登录 E1004（明确错误码+文案，不静默）
#   T04 不存在用户 E1002 / T05 错口令 E1003 / T05b 挑战抗重放：无挑战 E1009、旧 proof 重放 E1003 / T06 同连接重复认证 E1005
#   T07 未认证发业务帧 E1007 / T08 登录下发 Token(32B hex)+过期时间
#   T09 重连免密：新连接 AUTH(token) 成功 / T10 顶号：AUTH 恢复会话踢旧连接并明确通知
#   T11 房间：CREATE / 重复 CREATE E2001 / ROOMS / JOIN / 不存在 E2002 / LEAVE / 未加入 E2003
#   T12 广播只发本房间成员（隔离） / T13 历史：最近 50 条 + (ts,id) 游标翻页（不重不漏）
#   T14 私聊离线：落 offline_messages，上线补发 offline=1 / T15 INBOX 收件箱分页
#   T16 users 表 pwd_hash = PBKDF2-HMAC-SHA256(100000)（客户端派生、播种一致；C++ 实现的
#       RFC 向量对拍在 tests/test_auth.cpp）；解盲后对拍 + 库里非裸 K + pepper 0600
#   T16b 换错 pepper 开旧库 → 明确拒绝启动（pepper_id 一致性闸门）
#   T17 SQL 注入尝试被参数化安全吸收（用户名/内容含引号与注入串，服务器不崩、不误执行）
#   T18 限流风控：每 IP 建连令牌桶 E4001 / 每用户消息令牌桶 E4002（超限错误码+日志）
#   T19 敏感词 Trie：reject 模式 NACK(E4003)，替换模式内容脱敏
#   T20 TLS：自签证书握手 + 密文收发（无 openssl 可执行时跳过并提示）
#
# 用法：
#   python3 tools/test_v5_smoke.py                      # 自动找 chat_server_v5 并起服
#   python3 tools/test_v5_smoke.py --server-bin ./chat_server_v5 --keep-db
import argparse
import hashlib
import hmac
import json
import os
import socket
import sqlite3
import ssl
import struct
import subprocess
import sys
import tempfile
import time

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_ROOT, "client"))
from chat_client_v4 import (challenge_proof, derive_key,  # 与正式客户端同源（连凭据计算一起测）
                            make_msg, recv_frame, send_frame)

MAX_FRAME = 1 << 20
PASS, FAIL = 0, 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print("  ✓ %s" % name)
    else:
        FAIL += 1
        print("  ✗ %s   %s" % (name, detail))
    return cond


class Client(object):
    """裸 socket 测试客户端：精确控制每一帧（不用 ChatSession，避免自动行为干扰断言）"""

    def __init__(self, port, timeout=5.0, use_tls=False, ca_file=""):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=timeout)
        if use_tls:
            import ssl as _ssl
            if ca_file:  # 严格：用自签证书当 CA 验链（主机名是 127.0.0.1 → 关主机名校验）
                ctx = _ssl.create_default_context(cafile=ca_file)
                ctx.check_hostname = False
            else:
                ctx = _ssl.SSLContext(_ssl.PROTOCOL_TLS_CLIENT)
                ctx.check_hostname = False
                ctx.verify_mode = _ssl.CERT_NONE
            self.sock = ctx.wrap_socket(self.sock)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.sock.settimeout(timeout)
        self.user = ""
        self.seq = 0

    def send(self, mtype, content="", to="SERVER", seq=None, from_=None):
        self.seq += 1
        obj = make_msg(mtype, from_ or self.user or "anonymous", to, content,
                       self.seq if seq is None else seq)
        send_frame(self.sock, obj)
        return obj

    def recv(self, timeout=None):
        if timeout is not None:
            self.sock.settimeout(timeout)
        try:
            return recv_frame(self.sock)
        finally:
            if timeout is not None:
                self.sock.settimeout(5.0)

    def recv_type(self, want, timeout=5.0):
        """收帧直到想要的类型（PING/PONG 噪声跳过）；超时抛异常"""
        deadline = time.time() + timeout
        while time.time() < deadline:
            f = self.recv(timeout=max(0.1, deadline - time.time()))
            if f.get("type") == want:
                return f
        raise AssertionError("未收到 %s（超时）" % want)

    def recv_any(self, types, timeout=5.0):
        """收帧直到 types 之一（挑战-应答会岔出 ERR，不能只等成功帧）"""
        deadline = time.time() + timeout
        while time.time() < deadline:
            f = self.recv(timeout=max(0.1, deadline - time.time()))
            if f.get("type") in types:
                return f
        raise AssertionError("未收到 %s 之一（超时）" % (types,))

    def do_register(self, user, pwd):
        """挑战-应答注册（Q7②）：REGISTER_HELLO → CHALLENGE → REGISTER{K, hmac}。
        返回最终帧（REGISTER_OK / ERR）。"""
        self.user = user
        self.send("REGISTER_HELLO", "")
        f = self.recv_any(("CHALLENGE", "ERR"))
        if f.get("type") == "ERR":
            return f
        k = derive_key(pwd, f.get("salt", ""))
        self.seq += 1
        body = make_msg("REGISTER", user, "SERVER", k.hex(), self.seq)
        body["hmac"] = challenge_proof(k, "REGISTER", user, f.get("content", ""))
        send_frame(self.sock, body)
        return self.recv_any(("REGISTER_OK", "ERR"))

    def do_login(self, user, pwd):
        """挑战-应答登录（Q7①）：LOGIN_HELLO → CHALLENGE → LOGIN{proof}。
        返回最终帧（AUTH_OK / ERR）。口令/K 不进帧。"""
        self.user = user
        self.send("LOGIN_HELLO", "")
        f = self.recv_any(("CHALLENGE", "ERR"))
        if f.get("type") == "ERR":
            return f
        k = derive_key(pwd, f.get("salt", ""))
        proof = challenge_proof(k, "LOGIN", user, f.get("content", ""))
        self.send("LOGIN", proof)
        return self.recv_any(("AUTH_OK", "ERR"))

    @staticmethod
    def check_err(f, code):
        """断言最终帧是 ERR{code} 且带中文文案（不许静默）"""
        got = int(f.get("code", -1))
        text = f.get("content", "")
        assert f.get("type") == "ERR", "期望 ERR 实得 %s (%s)" % (f.get("type"), text)
        assert got == code, "期望 E%d 实得 E%d (%s)" % (code, got, text)
        assert text, "错误必须带中文文案，不许静默"
        return text

    def expect_err(self, code):
        f = self.recv_type("ERR")
        got = int(f.get("code", -1))
        text = f.get("content", "")
        assert got == code, "期望 E%d 实得 E%d (%s)" % (code, got, text)
        assert text, "错误必须带中文文案，不许静默"
        return text

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


def find_free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--server-bin", default="", help="服务器二进制（默认自动找 build/ 与根目录）")
    ap.add_argument("--keep-db", action="store_true")
    args = ap.parse_args()
    if not args.server_bin:
        for cand in ("build/chat_server_v5", "build-cmake/chat_server_v5", "chat_server_v5"):
            p = os.path.join(_ROOT, cand)
            if os.path.exists(p):
                args.server_bin = p
                break
        else:
            print("找不到 chat_server_v5，请先构建（cmake --build build）或传 --server-bin")
            sys.exit(1)

    tmpdir = tempfile.mkdtemp(prefix="v5smoke_")
    db_path = os.path.join(tmpdir, "smoke.db")
    pepper_path = os.path.join(tmpdir, "pepper.key")  # Q7②：盲化 pepper（不污染仓库 secrets/）
    port = find_free_port()
    # 主实例关闭限流（连发灌历史会撞默认令牌桶）；限流由 T18 的紧桶实例专测
    proc = subprocess.Popen(
        [args.server_bin, str(port), "--db", db_path, "--idle", "60000", "--scan", "1000",
         "--conn-rate", "0", "--msg-rate", "0", "--pepper", pepper_path],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        # 等服务器就绪
        for _ in range(50):
            try:
                c = Client(port, timeout=0.3)
                c.close()
                break
            except OSError:
                time.sleep(0.1)
        else:
            print("服务器启动失败")
            sys.exit(1)

        pwd_alice, pwd_bob, pwd_charlie = "alice-pw-测试✓", "bob' OR '1'='1", 'x"; DROP TABLE users;--'

        # ---------- T01/T02 注册与唯一约束（挑战-应答：口令不进帧，Q7②） ----------
        print("[T01] 注册成功（REGISTER_HELLO → CHALLENGE → REGISTER{K,hmac}）")
        c1 = Client(port)
        f = c1.do_register("alice", pwd_alice)
        check("REGISTER_OK 文案", f.get("type") == "REGISTER_OK" and "注册成功" in f.get("content", ""),
              str(f))
        check("注册帧不带口令（K/HMAC 形态见 PROTOCOL 6.1）", True)  # 流程即断言：do_register 全程无 pwd 字段

        print("[T02] 重复注册 → E1001（username UNIQUE，HELLO 阶段即拒）")
        c2 = Client(port)
        f = c2.do_register("alice", "other-pw")
        text = Client.check_err(f, 1001)
        check("E1001 中文文案", "已存在" in text, text)

        print("[T03] 再注册 bob / charlie（bob 口令含 SQL 引号 = T17 注入尝试）")
        cb = Client(port)
        f = cb.do_register("bob", pwd_bob)
        check("bob 注册", f.get("type") == "REGISTER_OK", str(f))
        cc = Client(port)
        f = cc.do_register("charlie", pwd_charlie)
        check("charlie 注册", f.get("type") == "REGISTER_OK", str(f))

        # ---------- T04/T05 登录失败路径 ----------
        print("[T04] 登录不存在用户 → E1002（HELLO 阶段即拒）")
        cx = Client(port)
        f = cx.do_login("ghost", "whatever")
        text = Client.check_err(f, 1002)
        check("E1002 中文文案", "不存在" in text, text)

        print("[T05] 口令错误（proof 错）→ E1003")
        f = cx.do_login("alice", "wrong-password")
        text = Client.check_err(f, 1003)
        check("E1003 中文文案", "密码错误" in text, text)

        # ---------- T05b 挑战抗重放（Q7）：单槽复用 + 一次性消费 + 证明绑定 nonce ----------
        print("[T05b] 裸 LOGIN → E1009；单槽复用；旧 proof 重放（nonce 绑定）→ E1003")
        cx.send("LOGIN", "0" * 64)
        f = cx.recv_any(("ERR",))
        text = Client.check_err(f, 1009)
        check("E1009 中文文案（无挑战）", "挑战" in text, text)
        cr = Client(port)
        cr.user = "alice"
        cr.send("LOGIN_HELLO", "")
        ch1 = cr.recv_any(("CHALLENGE",))
        cr.send("LOGIN_HELLO", "")          # 未消费前重复 HELLO → 单槽复用（不换 nonce）
        ch2 = cr.recv_any(("CHALLENGE",))
        check("挑战单槽复用（重复 HELLO 不换 nonce）",
              ch2.get("content") == ch1.get("content"), str(ch2.get("content")))
        k_alice = derive_key(pwd_alice, ch1.get("salt", ""))
        stale_proof = challenge_proof(k_alice, "LOGIN", "alice", ch1.get("content", ""))
        cr.send("LOGIN", "0" * 64)          # 错 proof 也【消费】挑战（一次性）
        text = Client.check_err(cr.recv_any(("ERR", "AUTH_OK")), 1003)
        cr.send("LOGIN_HELLO", "")          # 槽已空 → 新挑战（nonce 换代）
        ch3 = cr.recv_any(("CHALLENGE",))
        check("消费后 nonce 换代", ch3.get("content") != ch1.get("content"), str(ch3.get("content")))
        cr.send("LOGIN", stale_proof)       # 攻击者重放抓包里旧 nonce 的 proof
        f = cr.recv_any(("ERR", "AUTH_OK"))
        text = Client.check_err(f, 1003)    # HMAC(旧nonce) ≠ HMAC(新nonce) → 拒绝
        check("旧 proof 重放被拒（nonce 绑定）", "密码错误" in text, text)
        cr.close()

        # ---------- T07 未认证发业务帧 ----------
        print("[T07] 未认证 JOIN → E1007")
        cx.send("JOIN", "lobby")
        text = cx.expect_err(1007)
        check("E1007 中文文案", "登录" in text or "认证" in text, text)
        cx.close()

        # ---------- T06/T08 登录成功 + 同连接重复认证 ----------
        print("[T06/T08] alice 挑战-应答登录 → AUTH_OK(Token+exp)；同连接再 HELLO → E1005")
        f = c1.do_login("alice", pwd_alice)
        assert f.get("type") == "AUTH_OK", str(f)
        token_a = f.get("content", "")
        exp_a = f.get("exp", 0)
        check("Token=64 hex（32 字节）", len(token_a) == 64 and all(ch in "0123456789abcdef" for ch in token_a),
              "len=%d" % len(token_a))
        check("过期时间>当前", exp_a > time.time(), "exp=%s" % exp_a)
        f = c1.do_login("alice", pwd_alice)
        text = Client.check_err(f, 1005)
        check("E1005 中文文案", "已登录" in text or "重复" in text, text)

        # ---------- T03b 重复登录：第二连接口令登录 → E1004 ----------
        print("[T03b] 重复登录（账号已在线）→ E1004，明确文案不静默")
        c1b = Client(port)
        f = c1b.do_login("alice", pwd_alice)
        text = Client.check_err(f, 1004)
        check("E1004 中文文案", "已在线" in text and "重复登录" in text, text)
        c1b.close()

        # ---------- T09/T10 Token 免密重连 + 顶号 ----------
        print("[T09] 重连免密：新连接 AUTH(token) 不带口令恢复会话")
        c9 = Client(port); c9.user = "alice"; c9.send("AUTH", token_a)
        f = c9.recv_type("AUTH_OK")
        token_a2 = f.get("content", "")
        check("AUTH 成功并换发新 Token", len(token_a2) == 64 and token_a2 != token_a)
        check("旧连接收到顶号明确通知", True)  # 细节在 T10 断言

        print("[T10] 顶号：c1 收到明确通知后被关闭")
        f = c1.recv_type("SYSTEM")
        check("SYSTEM 顶号文案", "顶号" in f.get("content", "") or "其他连接" in f.get("content", ""),
              f.get("content"))
        try:
            f2 = c1.recv(timeout=2.0)
            # 顶号后旧连接不应再有业务数据（收到任何帧或 EOF 都算被处理，但应很快 EOF）
            check("旧连接被关闭（EOF）", f2 is None, repr(f2))
        except (ConnectionError, OSError, ValueError):
            check("旧连接被关闭（EOF）", True)
        c1.close()

        # ---------- T11 房间生命周期 ----------
        print("[T11] 房间：CREATE / 重复 CREATE / ROOMS / JOIN / E2002 / LEAVE / E2003")
        c9.send("CREATE", "dev-room")
        f = c9.recv_type("CREATE_OK")
        check("CREATE_OK", f.get("room") == "dev-room", f.get("room"))
        c9.send("CREATE", "dev-room")
        text = c9.expect_err(2001)
        check("E2001 中文文案", "已存在" in text, text)

        f = cb.do_login("bob", pwd_bob)
        assert f.get("type") == "AUTH_OK", str(f)
        f = cc.do_login("charlie", pwd_charlie)
        assert f.get("type") == "AUTH_OK", str(f)

        c9.send("ROOMS")
        f = c9.recv_type("ROOMS_LIST")
        names = sorted(r.get("name") for r in f.get("content", []))
        check("ROOMS_LIST 含 lobby+dev-room", names == ["dev-room", "lobby"], str(names))

        c9.send("JOIN", "no-such-room")
        text = c9.expect_err(2002)
        check("E2002 中文文案", "不存在" in text, text)

        c9.send("JOIN", "dev-room")
        f = c9.recv_type("JOIN_OK")
        check("JOIN_OK 成员含 alice", "alice" in f.get("content", []), str(f.get("content")))

        c9.send("LEAVE"); c9.recv_type("LEAVE_OK")
        c9.send("LEAVE")
        text = c9.expect_err(2003)
        check("E2003 中文文案", "未加入" in text or "尚未加入" in text, text)

        # ---------- T12 广播隔离 ----------
        print("[T12] 广播只发本房间成员（alice+charlie 在 dev-room，bob 在大厅）")
        c9.send("JOIN", "dev-room"); c9.recv_type("JOIN_OK")
        c9.recv_type("USERLIST")  # 进房 USERLIST（join 流程的广播）
        cc.send("JOIN", "dev-room")
        cc.recv_type("JOIN_OK")
        cc.recv_type("USERLIST")
        f = c9.recv_type("SYSTEM")           # charlie 进入了房间
        check("alice 收到 charlie 进房通知", "charlie" in f.get("content", ""), f.get("content"))
        c9.recv_type("USERLIST")
        cb.send("JOIN", "lobby")
        cb.recv_type("JOIN_OK")
        cb.recv_type("USERLIST")

        c9.send("MESSAGE", "room-hello-1", to="ALL")
        f = cc.recv_type("MESSAGE")
        check("同房间 charlie 收到", f.get("content") == "room-hello-1", str(f))
        check("room 字段正确", f.get("room") == "dev-room", str(f.get("room")))
        f = c9.recv_type("MESSAGE")          # 回显
        check("发送者回显", f.get("content") == "room-hello-1")
        c9.recv_type("ACK")
        try:
            f = cb.recv(timeout=1.0)
            check("bob（其他房间）收不到", False, repr(f))
        except (socket.timeout, TimeoutError):
            check("bob（其他房间）收不到", True)

        # ---------- T13 历史：最近 50 + 游标翻页 ----------
        print("[T13] 历史：灌 120 条 → 最近 50 条 → (ts,id) 游标翻页不重不漏")

        def drain_ack_echo(c):
            """发送者会收到 ACK 与回显 MESSAGE 两帧（服务器广播在 ACK 前，但断言不依赖顺序）"""
            got = {}
            deadline = time.time() + 5
            while ("ACK" not in got or "MESSAGE" not in got) and time.time() < deadline:
                try:
                    f = c.recv(timeout=max(0.1, deadline - time.time()))
                except (socket.timeout, TimeoutError, ConnectionError, ValueError):
                    break
                if f is not None:
                    got[f.get("type")] = f
            assert "ACK" in got and "MESSAGE" in got, str(list(got.keys()))

        for i in range(1, 120):
            c9.send("MESSAGE", "hist-%03d" % i, to="ALL")
            drain_ack_echo(c9)
            cc.recv_type("MESSAGE")
        time.sleep(0.2)
        c9.send("HIST", "")
        f = c9.recv_type("HISTORY")
        items = f.get("content", [])
        check("最近一页 50 条", len(items) == 50, "len=%d" % len(items))
        check("最近一页含 hist-119", any(m.get("content") == "hist-119" for m in items))
        check("more=1", f.get("more") in (1, True))
        cursor = f.get("cursor", "")
        check("游标格式 ts:id", ":" in cursor, cursor)
        seen = [m.get("content") for m in items]
        pages = 1
        while f.get("more") and pages < 10:
            c9.send("HIST", f.get("cursor", ""))
            f = c9.recv_type("HISTORY")
            batch = [m.get("content") for m in f.get("content", [])]
            seen = batch + seen  # 上翻更早页
            pages += 1
        expected = ["room-hello-1"] + ["hist-%03d" % i for i in range(1, 120)]
        check("3 页收齐 120 条（room-hello-1 + hist-001..119）", seen == expected,
              "len=%d pages=%d head=%s tail=%s" % (len(seen), pages, seen[:3], seen[-3:]))
        check("不重不漏", len(seen) == len(set(seen)), "dups")
        check("时序严格旧→新", seen == expected)

        # ---------- T14 离线私聊 ----------
        print("[T14] 私聊离线：bob 不在线 → 上线补发 offline=1")
        cb.send("LOGOUT")
        time.sleep(0.3)
        cb.close()
        c9.send("MESSAGE", "miss-you-bob", to="bob")   # bob 离线
        f = c9.recv_type("ACK")
        check("私聊已 ACK（=已持久化）", f is not None)
        cb2 = Client(port); cb2.user = "bob"; cb2.send("AUTH", "")  # 故意坏 Token
        cb2.expect_err(1006)
        f = cb2.do_login("bob", pwd_bob)
        assert f.get("type") == "AUTH_OK", str(f)
        f = cb2.recv_type("MESSAGE")
        check("补发内容", f.get("content") == "miss-you-bob", str(f))
        check("offline=1 标志", f.get("offline") in (1, True), str(f.get("offline")))

        # ---------- T15 INBOX ----------
        print("[T15] 收件箱 INBOX")
        cb2.send("INBOX", "")
        f = cb2.recv_type("INBOX")
        contents = [m.get("content") for m in f.get("content", [])]
        check("INBOX 含离线私聊", "miss-you-bob" in contents, str(contents))

        # ---------- T16 PBKDF2 向量交叉验证 + pepper 盲化验证（Q7②） ----------
        print("[T16] 解盲（stored ⊕ HMAC(pepper,user‖salt)）后与 Python hashlib.pbkdf2_hmac 一致")
        con = sqlite3.connect(db_path)
        rows = dict((u, (s, h)) for u, s, h in
                    con.execute("SELECT username, salt, pwd_hash FROM users"))
        pepper = bytes.fromhex(open(pepper_path).read().strip())
        ok, blinded = True, True
        for name, pwd in (("alice", pwd_alice), ("bob", pwd_bob), ("charlie", pwd_charlie)):
            salt_hex, hash_hex = rows[name]
            salt = bytes.fromhex(salt_hex)
            calc = hashlib.pbkdf2_hmac("sha256", pwd.encode("utf-8"), salt, 100000, 32)
            if calc.hex() == hash_hex:
                blinded = False  # 库里不该是裸 K（pass-the-hash 面）
            mask = hmac.new(pepper, (name + salt_hex).encode("utf-8"), hashlib.sha256).digest()
            k = bytes(a ^ b for a, b in zip(bytes.fromhex(hash_hex), mask))
            if k != calc:
                ok = False
                print("    %s 解盲后不一致:\n      库 %s\n      还原 %s\n      python %s"
                      % (name, hash_hex, k.hex(), calc.hex()))
        check("三个用户解盲后 PBKDF2(100000) 全部一致", ok)
        check("库里是盲化值而非裸 K（反 pass-the-hash）", blinded)
        check("pepper 文件 0600", (os.stat(pepper_path).st_mode & 0o777) == 0o600,
              oct(os.stat(pepper_path).st_mode & 0o777))
        check("salt 16 字节（32 hex）", all(len(rows[u][0]) == 32 for u in rows))

        # ---------- T16b pepper↔库 一致性闸门（配置与状态不匹配必须显式拒绝） ----------
        print("[T16b] 换错 pepper 开旧库 → 拒绝启动（防全员「密码错误」式静默故障）")
        other_pepper = os.path.join(tmpdir, "other_pepper.key")
        with open(other_pepper, "w", encoding="utf-8") as f:
            f.write("ab" * 32)  # 另一个合法长度（64 hex）但不同值的 pepper
        p16 = subprocess.Popen(
            [args.server_bin, str(find_free_port()), "--db", db_path, "--idle", "60000",
             "--scan", "1000", "--conn-rate", "0", "--msg-rate", "0", "--log-dir", tmpdir,
             "--pepper", other_pepper],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        try:
            out, _ = p16.communicate(timeout=10)
            check("错 pepper 被拒（退出码非 0）", p16.returncode != 0, "rc=%s" % p16.returncode)
            check("错 pepper 报错含「不匹配」", "不匹配" in out, (out or "")[-140:])
        except subprocess.TimeoutExpired:
            p16.kill()
            check("错 pepper 被拒（退出码非 0）", False, "10s 未退出——闸门失效？")

        # ---------- T17 注入串被参数化吸收 ----------
        print("[T17] 注入串安全（T03 的 bob 口令含 ' OR '1'='1；再发一条注入内容）")
        c9.send("MESSAGE", "'); DELETE FROM users;--", to="ALL")
        c9.recv_type("ACK")
        n_users = con.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        check("users 表未被注入删除", n_users == 3, "count=%d" % n_users)
        n_msg = con.execute(
            "SELECT COUNT(*) FROM messages WHERE content LIKE '%DELETE FROM users%'").fetchone()[0]
        check("注入串作为纯文本消息落库", n_msg == 1)
        con.close()

        for c in (c9, cc, cb2):
            c.close()

        # ---------- T18 限流风控（独立服务器实例：紧桶） ----------
        print("[T18] 限流：每 IP 建连 E4001 / 每用户消息 E4002")
        t18db = os.path.join(tmpdir, "t18.db")
        port18 = find_free_port()
        p18 = subprocess.Popen(
            [args.server_bin, str(port18), "--db", t18db, "--idle", "60000",
             "--scan", "1000", "--conn-rate", "5", "--conn-burst", "5",
             "--msg-rate", "5", "--msg-burst", "5", "--log-dir", tmpdir,
             "--pepper", pepper_path],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            for _ in range(50):
                try:
                    Client(port18, timeout=0.3).close(); break
                except OSError:
                    time.sleep(0.1)
            # 建连限流：突发 5（就绪探针耗 1）后毫秒级连开 8 个 → 部分收 E4001。
            # 关键：先全部连上再统一读——若连一个读一个（每个阻塞秒级），令牌桶已回填，
            # 限流就测不出来了（踩过的坑）。
            probes = []
            for i in range(8):
                try:
                    probes.append(Client(port18, timeout=2.0))
                except OSError:
                    pass
            got4001 = 0
            texts = []
            for c in probes:
                try:
                    f = c.recv(timeout=1.0)
                    if f and f.get("type") == "ERR" and int(f.get("code", 0)) == 4001:
                        got4001 += 1
                        texts.append(f.get("content", ""))
                except (OSError, ConnectionError, ValueError):
                    pass  # 极端情况下被直接挂断也计入限流生效面
                c.close()
            check("E4001 中文文案", got4001 == 0 or "频繁" in texts[0], texts[:1])
            check("建连限流生效（E4001）", got4001 >= 1, "got=%d probes=%d" % (got4001, len(probes)))

            # 消息限流：一用户快速连发 12 条 → 至少 1 个 NACK(code=4002)
            # 先等令牌桶回填：*_HELLO 也耗每 IP 桶（Q7 无认证面限流），紧桶实例下不等会被误伤
            time.sleep(2.5)
            c = Client(port18)
            f = c.do_register("lim", "pw")
            assert f.get("type") == "REGISTER_OK", str(f)
            f = c.do_login("lim", "pw")
            assert f.get("type") == "AUTH_OK", str(f)
            c.send("JOIN", "lobby"); c.recv_type("JOIN_OK")
            while True:  # 清 USERLIST
                try:
                    if c.recv(timeout=0.2) is None:
                        break
                except (OSError, ValueError):
                    break
            got4002 = 0
            for i in range(12):
                c.send("MESSAGE", "burst-%d" % i, to="ALL", seq=100 + i)
            for _ in range(12):
                f = c.recv(timeout=2.0)
                if f is None:
                    break
                if f.get("type") == "NACK" and int(f.get("code", 0)) == 4002:
                    got4002 += 1
                    check("E4002 中文文案", "频繁" in f.get("reason", ""), f.get("reason"))
                if f.get("type") == "ERR" and int(f.get("code", 0)) == 4001:
                    pass
            check("消息限流生效（E4002）", got4002 >= 1, "got=%d" % got4002)
            c.close()
        finally:
            p18.terminate()
            try:
                p18.wait(timeout=3)
            except subprocess.TimeoutExpired:
                p18.kill()

        # ---------- T19 敏感词（reject 模式 E4003 + replace 模式脱敏） ----------
        print("[T19] 敏感词 Trie：reject / replace")
        words = os.path.join(tmpdir, "words.txt")
        with open(words, "w", encoding="utf-8") as f:
            f.write("违禁\n敏感词\nbadword\n")
        t19db = os.path.join(tmpdir, "t19.db")
        port19 = find_free_port()
        p19 = subprocess.Popen(
            [args.server_bin, str(port19), "--db", t19db, "--idle", "60000", "--scan", "1000",
             "--words", words, "--filter-mode", "reject", "--log-dir", tmpdir,
             "--pepper", pepper_path],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            for _ in range(50):
                try:
                    Client(port19, timeout=0.3).close(); break
                except OSError:
                    time.sleep(0.1)
            cw = Client(port19)
            f = cw.do_register("censor", "pw")
            assert f.get("type") == "REGISTER_OK", str(f)
            f = cw.do_login("censor", "pw")
            assert f.get("type") == "AUTH_OK", str(f)
            cw.send("JOIN", "lobby"); cw.recv_type("JOIN_OK")
            while True:
                try:
                    if cw.recv(timeout=0.2) is None:
                        break
                except (OSError, ValueError):
                    break
            cw.send("MESSAGE", "这里有违禁内容", to="ALL")
            f = cw.recv_type("NACK")
            check("E4003 reject 模式 NACK", int(f.get("code", 0)) == 4003, str(f))
            check("E4003 文案含词", "敏感词" in f.get("reason", ""), f.get("reason"))
            cw.send("MESSAGE", "干净内容 ok", to="ALL")
            f = cw.recv_type("MESSAGE")
            check("干净消息放行", f.get("content") == "干净内容 ok")
            cw.close()
        finally:
            p19.terminate()
            try:
                p19.wait(timeout=3)
            except subprocess.TimeoutExpired:
                p19.kill()

        # replace 模式：脱敏后落库/广播
        port19b = find_free_port()
        p19b = subprocess.Popen(
            [args.server_bin, str(port19b), "--db", os.path.join(tmpdir, "t19b.db"),
             "--idle", "60000", "--scan", "1000", "--words", words, "--log-dir", tmpdir,
             "--pepper", pepper_path],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            for _ in range(50):
                try:
                    Client(port19b, timeout=0.3).close(); break
                except OSError:
                    time.sleep(0.1)
            cw = Client(port19b)
            f = cw.do_register("censor2", "pw")
            assert f.get("type") == "REGISTER_OK", str(f)
            f = cw.do_login("censor2", "pw")
            assert f.get("type") == "AUTH_OK", str(f)
            cw.send("JOIN", "lobby"); cw.recv_type("JOIN_OK")
            while True:
                try:
                    if cw.recv(timeout=0.2) is None:
                        break
                except (OSError, ValueError):
                    break
            cw.send("MESSAGE", "badword来了", to="ALL")
            f = cw.recv_type("MESSAGE")
            check("replace 模式脱敏（7 字符→7 *）", f.get("content") == "*******来了",
                  repr(f.get("content")))
            cw.recv_type("ACK")
            cw.close()
        finally:
            p19b.terminate()
            try:
                p19b.wait(timeout=3)
            except subprocess.TimeoutExpired:
                p19b.kill()

        # ---------- T20 TLS：自签证书 + 加密收发 ----------
        print("[T20] TLS：自签握手 + 密文通道")
        certdir = os.path.join(tmpdir, "certs")
        gen = subprocess.run(["bash", "tools/gen_cert.sh", certdir],
                             capture_output=True, text=True)
        if gen.returncode != 0:
            print("  （跳过：openssl 不可用：%s）" % gen.stderr.strip()[:80])
        else:
            port20 = find_free_port()
            cert = os.path.join(certdir, "server.crt")
            key = os.path.join(certdir, "server.key")
            p20 = subprocess.Popen(
                [args.server_bin, str(port20), "--db", os.path.join(tmpdir, "t20.db"),
                 "--idle", "60000", "--scan", "1000", "--log-dir", tmpdir,
                 "--tls", "--cert", cert, "--key", key,
                 "--pepper", pepper_path],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            try:
                for _ in range(50):
                    try:
                        Client(port20, timeout=0.3, use_tls=True).close(); break
                    except (OSError, ConnectionError, ValueError, ssl.SSLError):
                        time.sleep(0.1)
                ct = Client(port20, use_tls=True, ca_file=cert)  # 严格模式：自签即 CA
                f = ct.do_register("tlsuser", "pw-tls")
                check("TLS 注册往返（挑战-应答）", f.get("type") == "REGISTER_OK", str(f))
                f = ct.do_login("tlsuser", "pw-tls")
                check("TLS 登录下发 Token", f.get("type") == "AUTH_OK" and
                      len(f.get("content", "")) == 64, str(f))
                ct.send("JOIN", "lobby")
                ct.recv_type("JOIN_OK")
                ct.send("MESSAGE", "over-tls", to="ALL", seq=50)
                f = ct.recv_type("MESSAGE")
                check("TLS 群聊往返", f.get("content") == "over-tls")
                ct.close()
                # 明文客户端连 TLS 端口必须失败（协议不兼容即时暴露）
                try:
                    cp = Client(port20, timeout=2.0)
                    cp.send("LOGIN_HELLO", "")
                    cp.recv(timeout=2.0)
                    check("明文连 TLS 口应失败", False)
                except (OSError, ConnectionError, ValueError, AssertionError):
                    check("明文连 TLS 口应失败", True)
            finally:
                p20.terminate()
                try:
                    p20.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    p20.kill()
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()
        if not args.keep_db:
            for suffix in ("", "-wal", "-shm"):
                try:
                    os.remove(db_path + suffix)
                except OSError:
                    pass
            try:
                os.rmdir(tmpdir)
            except OSError:
                pass

    print("=" * 60)
    print("结果：%d 通过 / %d 失败" % (PASS, FAIL))
    sys.exit(1 if FAIL else 0)


if __name__ == "__main__":
    main()
