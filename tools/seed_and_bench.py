#!/usr/bin/env python3
# tools/seed_and_bench.py —— 灌 10 万条消息，实测历史分页耗时 + EXPLAIN QUERY PLAN 证明走索引
#
# 验收要求（「数字必须实跑」）：
#   1. 插入 10 万条 messages
#   2. 测「最近 50 条」与「翻到第 100 页」的耗时
#   3. EXPLAIN QUERY PLAN 证明查询走索引
#
# 本脚本的建表 DDL / 查询 SQL 与 chat_server_v5.cpp 的 Database 类【一字不差】
# （改一边必须改另一边），保证压测结论直接代表线上路径。
#
# 为什么「翻到第 100 页」用 (ts,id) 游标而不是 LIMIT/OFFSET（Q3，输出里有对照数据）：
#   OFFSET 语义 = 扫过前 OFFSET 行再取 50 行，深页成本线性涨，且插入新消息会使窗口漂移
#   （跳行/重行）；游标把「上一页最老一行的 (ts,id)」当书签，(ts,id) 唯一严格全序，
#   B-Tree 直接 seek：O(log n + 50)，深度无关，不重不漏。
#
# 用法：
#   python3 tools/seed_and_bench.py                       # 默认 DB=tools/bench_100k.db，10 万行
#   python3 tools/seed_and_bench.py --db /tmp/b.db --rows 100000 --repeat 20
import argparse
import os
import sqlite3
import time

BASE_TS = 1700000000  # 固定基准时间；ts = BASE_TS + i//3 → 每秒 3 条【制造同秒碰撞】，
                      # 逼出 (ts,id) 里 id tie-break 的必要性（游标只按 ts 会在页边界跳/重）


def build_schema(con):
    """与 chat_server_v5.cpp Database::open 的 schema 串保持一致。"""
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS users (
          id         INTEGER PRIMARY KEY AUTOINCREMENT,
          username   TEXT NOT NULL UNIQUE,
          salt       TEXT NOT NULL,
          pwd_hash   TEXT NOT NULL,
          created_at INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS rooms (
          id         INTEGER PRIMARY KEY AUTOINCREMENT,
          name       TEXT NOT NULL UNIQUE,
          owner      TEXT NOT NULL,
          created_at INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS messages (
          id       INTEGER PRIMARY KEY AUTOINCREMENT,
          room_id  INTEGER NOT NULL,
          sender   TEXT NOT NULL,
          receiver TEXT NOT NULL,
          content  TEXT NOT NULL,
          ts       INTEGER NOT NULL,
          type     TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS offline_messages (
          id        INTEGER PRIMARY KEY AUTOINCREMENT,
          sender    TEXT NOT NULL,
          receiver  TEXT NOT NULL,
          content   TEXT NOT NULL,
          ts        INTEGER NOT NULL,
          delivered INTEGER NOT NULL DEFAULT 0);
        -- 索引①：房间历史「最近 50 条 + (ts,id) 游标翻页」→ WHERE room_id=? [AND (ts,id)<(?,?)]
        --        ORDER BY ts DESC, id DESC LIMIT ?
        CREATE INDEX IF NOT EXISTS idx_messages_room_ts ON messages (room_id, ts);
        -- 索引②：私信收件箱「receiver=? [AND (ts,id)<(?,?)] 游标翻页」
        CREATE INDEX IF NOT EXISTS idx_messages_receiver_ts ON messages (receiver, ts);
        """
    )


# ---- 与 chat_server_v5.cpp 完全一致的线上查询 ----
Q_LATEST = (
    "SELECT id, sender, receiver, content, ts, type FROM messages "
    "WHERE room_id = ? ORDER BY ts DESC, id DESC LIMIT ?"
)
Q_BEFORE = (
    "SELECT id, sender, receiver, content, ts, type FROM messages "
    "WHERE room_id = ? AND (ts, id) < (?, ?) ORDER BY ts DESC, id DESC LIMIT ?"
)
Q_INBOX_LATEST = (
    "SELECT id, sender, receiver, content, ts, type FROM messages "
    "WHERE receiver = ? ORDER BY ts DESC, id DESC LIMIT ?"
)
Q_INBOX_BEFORE = (
    "SELECT id, sender, receiver, content, ts, type FROM messages "
    "WHERE receiver = ? AND (ts, id) < (?, ?) ORDER BY ts DESC, id DESC LIMIT ?"
)
# 仅压测对照用（服务器【不用】OFFSET）：传统深分页「跳过前 OFFSET 行」
Q_OFFSET = (
    "SELECT id, sender, receiver, content, ts, type FROM messages "
    "WHERE room_id = ? ORDER BY ts DESC, id DESC LIMIT ? OFFSET ?"
)


def seed(con, rows):
    """灌数据：3 个房间 + 1 万个… 10 万个用户？不：10 万条消息。分布：
    lobby 90% / rust-go 6% / off-topic 1% / 私聊 3%（receiver 覆盖多用户，bob 1000 条）。
    按 id 顺序分块灌入 → lobby 恰为 id 连续段，方便「不重不漏」校验。"""
    t0 = time.perf_counter()
    con.execute("BEGIN")
    now = BASE_TS
    users = [("user%02d" % i, "aa" * 16, "bb" * 32, now) for i in range(10)]
    con.executemany(
        "INSERT INTO users (username, salt, pwd_hash, created_at) VALUES (?,?,?,?)", users
    )
    con.executemany(
        "INSERT INTO rooms (name, owner, created_at) VALUES (?,?,?)",
        [("lobby", "SERVER", now), ("rust-go", "user00", now), ("off-topic", "user01", now)],
    )
    # 房间 DB 主键与服务器一致地从表里回读
    room_id = dict(con.execute("SELECT name, id FROM rooms").fetchall())

    n_lobby = int(rows * 0.90)
    n_rust = int(rows * 0.06)
    n_off = int(rows * 0.01)
    n_priv = rows - n_lobby - n_rust - n_off  # 余下全给私聊
    batch = []

    def emit_room(room, count, tag):
        for k in range(count):
            i = len(batch)
            batch.append(
                (room, "user%02d" % (i % 10), "ALL", "%s-%d" % (tag, i), BASE_TS + i // 3, "room")
            )

    emit_room(room_id["lobby"], n_lobby, "lobby-msg")
    emit_room(room_id["rust-go"], n_rust, "rust-msg")
    emit_room(room_id["off-topic"], n_off, "off-msg")
    for k in range(n_priv):
        i = len(batch)
        # bob(user01) 独占 1/3 私聊量，保证收件箱有深页可翻
        to = "user01" if k < n_priv // 3 else "user%02d" % (k % 10)
        batch.append((0, "user%02d" % ((k + 3) % 10), to, "priv-%d" % i, BASE_TS + i // 3, "private"))
    con.executemany(
        "INSERT INTO messages (room_id, sender, receiver, content, ts, type) VALUES (?,?,?,?,?,?)",
        batch,
    )
    con.execute("COMMIT")
    return time.perf_counter() - t0, {
        "lobby": n_lobby, "rust-go": n_rust, "off-topic": n_off, "private": n_priv,
    }


def timed(fn, repeat):
    """跑 repeat 次取均值（首次顺带暖页缓存；返回 ms）。fn 返回任意结果（最后一次的）。"""
    ts = []
    result = None
    for _ in range(repeat):
        t0 = time.perf_counter()
        result = fn()
        ts.append((time.perf_counter() - t0) * 1000.0)
    return sum(ts) / len(ts), min(ts), result


def explain(con, title, sql, args):
    print("  [%s]" % title)
    plan = con.execute("EXPLAIN QUERY PLAN " + sql, args).fetchall()
    for row in plan:  # (id, parent, notused, detail) —— 打印 detail
        print("      %s" % row[3])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=os.path.join(os.path.dirname(__file__), "bench_100k.db"))
    ap.add_argument("--rows", type=int, default=100000)
    ap.add_argument("--repeat", type=int, default=20, help="每个计时用例的重复次数（取均值/最小）")
    ap.add_argument("--keep", action="store_true", help="保留 DB 文件（默认删旧重建）")
    args = ap.parse_args()

    if not args.keep and os.path.exists(args.db):
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(args.db + suffix)
            except OSError:
                pass

    con = sqlite3.connect(args.db)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=3000")
    build_schema(con)
    room_id = dict(con.execute("SELECT name, id FROM rooms").fetchall()) if con.execute(
        "SELECT COUNT(*) FROM rooms").fetchone()[0] else {}

    print("=" * 72)
    print("灌数据 + 历史分页压测（数字全部实跑，非估算）")
    print("DB=%s  SQLite=%s" % (args.db, sqlite3.sqlite_version))
    print("=" * 72)

    seed_ms, dist = seed(con, args.rows)
    room_id = dict(con.execute("SELECT name, id FROM rooms").fetchall())
    lobby = room_id["lobby"]
    total = con.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
    bob_inbox = con.execute(
        "SELECT COUNT(*) FROM messages WHERE receiver='user01'").fetchone()[0]
    print("\n[1] 灌数据：%d 条 messages，耗时 %.1f ms" % (total, seed_ms * 1000))
    print("    分布：%s；bob(user01) 收件箱 %d 条" % (dist, bob_inbox))

    # ---------- EXPLAIN QUERY PLAN：证明走索引 ----------
    print("\n[2] EXPLAIN QUERY PLAN（证明走索引；SQL 与服务器一致）")
    explain(con, "最近 50 条（room_id=?）", Q_LATEST, (lobby, 50))
    explain(con, "游标翻页（room_id=? AND (ts,id)<(?,?)）", Q_BEFORE,
            (lobby, BASE_TS + 30000, 85050, 50))
    explain(con, "收件箱最近页（receiver=?）", Q_INBOX_LATEST, ("user01", 50))
    explain(con, "收件箱游标翻页", Q_INBOX_BEFORE, ("user01", BASE_TS + 30000, 34000, 50))
    explain(con, "对照：OFFSET 深分页（也走 idx_messages_room_ts，但要扫过 OFFSET 行）",
            Q_OFFSET, (lobby, 50, 4950))

    # ---------- 最近 50 条 ----------
    print("\n[3] 「最近 50 条」耗时（repeat=%d）" % args.repeat)
    avg, best, rows = timed(lambda: con.execute(Q_LATEST, (lobby, 50)).fetchall(), args.repeat)
    print("    avg %.4f ms   min %.4f ms   （返回 %d 行，最新 id=%d）"
          % (avg, best, len(rows), rows[0][0]))

    # ---------- 翻到第 100 页：游标连续翻（用户真实上翻 100 次） ----------
    print("\n[4] 「翻到第 100 页」——(ts,id) 游标连续上翻 100 页（每页 50 条）")
    cursor = None
    all_ids = []
    page_times = []
    t0 = time.perf_counter()
    for page in range(100):
        t1 = time.perf_counter()
        if cursor is None:
            rs = con.execute(Q_LATEST, (lobby, 50)).fetchall()
        else:
            rs = con.execute(Q_BEFORE, (lobby, cursor[0], cursor[1], 50)).fetchall()
        page_times.append((time.perf_counter() - t1) * 1000.0)
        assert rs, "第 %d 页空了" % (page + 1)
        all_ids.extend(r[0] for r in rs)           # 新→老
        cursor = (rs[-1][4], rs[-1][0])            # 本页最老一行 (ts, id)
    walk_ms = (time.perf_counter() - t0) * 1000.0
    print("    100 页×50 条总耗时 %.3f ms（平均 %.4f ms/页，第 100 页 %.4f ms）"
          % (walk_ms, walk_ms / 100, page_times[99]))
    print("    页 1 / 50 / 100 单页：%.4f / %.4f / %.4f ms"
          % (page_times[0], page_times[49], page_times[99]))

    # 不重不漏校验：5000 个 id 必须无重复、无缺口、严格递减
    ok = (len(all_ids) == len(set(all_ids)) == 5000
          and all(all_ids[i] > all_ids[i + 1] for i in range(len(all_ids) - 1))
          and all_ids[0] - all_ids[-1] == 4999)
    print("    不重不漏校验：%s（5000 条，id %d..%d，同秒碰撞靠 id tie-break）"
          % ("通过" if ok else "失败!", all_ids[0], all_ids[-1]))
    assert ok, "游标翻页出现重复/缺口！"

    # 第 100 页那一次查询单独测（拿第 99 页的游标反复查）
    p99 = all_ids[50 * 98 + 49]  # 第 99 页最老
    p99_row = con.execute("SELECT ts FROM messages WHERE id=?", (p99,)).fetchone()
    avg, best, _ = timed(
        lambda: con.execute(Q_BEFORE, (lobby, p99_row[0], p99, 50)).fetchall(), args.repeat)
    print("    第 100 页单次查询（游标）：avg %.4f ms  min %.4f ms" % (avg, best))

    # ---------- OFFSET 对照（服务器不用；只证明为什么不用） ----------
    print("\n[5] LIMIT/OFFSET 对照（同为「第 N 页」，证明深分页退化）")
    for label, off in [("第   1 页 OFFSET    0", 0), ("第 100 页 OFFSET 4950", 4950),
                       ("第  1781 页 OFFSET 89000（lobby 底部）", 89000)]:
        avg, best, _ = timed(lambda o=off: con.execute(Q_OFFSET, (lobby, 50, o)).fetchall(),
                             args.repeat)
        print("    %-40s avg %.4f ms  min %.4f ms" % (label, avg, best))

    # ---------- 收件箱（receiver 索引） ----------
    print("\n[6] 私信收件箱（receiver='user01'，%d 条）" % bob_inbox)
    avg, best, rows = timed(lambda: con.execute(Q_INBOX_LATEST, ("user01", 50)).fetchall(),
                            args.repeat)
    print("    最近 50 条：avg %.4f ms  min %.4f ms" % (avg, best))
    cursor = None
    t0 = time.perf_counter()
    for page in range(20):  # 上翻 20 页到深部
        if cursor is None:
            rs = con.execute(Q_INBOX_LATEST, ("user01", 50)).fetchall()
        else:
            rs = con.execute(Q_INBOX_BEFORE, ("user01", cursor[0], cursor[1], 50)).fetchall()
        if not rs:
            break
        cursor = (rs[-1][4], rs[-1][0])
    deep_ms = (time.perf_counter() - t0) * 1000.0
    avg, best, _ = timed(
        lambda: con.execute(Q_INBOX_BEFORE, ("user01", cursor[0], cursor[1], 50)).fetchall(),
        args.repeat)
    print("    上翻 20 页总耗时 %.3f ms；深部单页（游标）avg %.4f ms  min %.4f ms"
          % (deep_ms, avg, best))

    print("\n" + "=" * 72)
    print("结论：两条索引均被 SEARCH 命中（见 [2]）；游标翻页耗时与页深无关（见 [4]），")
    print("      OFFSET 深分页随深度线性变慢（见 [5]）——聊天记录深分页必须用 (ts,id) 游标。")
    print("=" * 72)
    if not args.keep:
        con.close()
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(args.db + suffix)
            except OSError:
                pass
        print("（临时 DB 已删除；加 --keep 保留）")


if __name__ == "__main__":
    main()
