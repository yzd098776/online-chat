// tests/test_database.cpp —— 持久层单测：账号/房间/离线/游标分页 + group commit 并发
//
// group commit 的重点验证：多线程并发 insert_room_msg 时
//   ① 每条都拿到有效 id（批事务最终提交成功才返回——「已送达=已持久化」语义）
//   ② 行数一条不少（批内搭车者的行不能因回滚/竞态丢失）
//   ③ id 严格递增唯一（(ts,id) 游标全序的基础）
// 落盘用临时库文件（WAL 形态与生产一致），用例结束清理 -wal/-shm。
#include <atomic>
#include <cstdio>
#include <string>
#include <thread>
#include <vector>

#include "chat/database.h"
#include "chat/protocol.h"
#include "minitest.h"

MINI_SUITE(database)

namespace {

const char* kDb = "chat_test_tmp.db";

void fresh_db() {
    for (int i = 0; i < 3; ++i) {
        std::string suf[3] = {"", "-wal", "-shm"};
        std::remove((kDb + suf[i]).c_str());
    }
}

}  // namespace

MINI_TEST(user_roundtrip_and_dup) {
    fresh_db();
    chat::Database db;
    MINI_ASSERT(db.open(kDb));
    MINI_ASSERT_EQ(db.create_user("alice", "salt32hex", "hash64hex", 100), 0);
    chat::UserRow u;
    MINI_ASSERT(db.find_user("alice", u));
    MINI_ASSERT_EQ(u.username, "alice");
    MINI_ASSERT_EQ(u.salt, "salt32hex");
    MINI_ASSERT_EQ(u.pwd_hash, "hash64hex");
    MINI_ASSERT_EQ(u.created_at, 100);
    MINI_ASSERT_EQ(db.create_user("alice", "s", "h", 101), (int)chat::kErrDupUser);  // UNIQUE 约束
    MINI_ASSERT(!db.find_user("nobody", u));
    db.close();
}

MINI_TEST(room_roundtrip) {
    fresh_db();
    chat::Database db;
    MINI_ASSERT(db.open(kDb));
    MINI_ASSERT_EQ(db.create_room("lobby", "SERVER", 1), 0);
    MINI_ASSERT_EQ(db.create_room("dev", "alice", 2), 0);
    MINI_ASSERT_EQ(db.create_room("dev", "bob", 3), (int)chat::kErrRoomExists);
    MINI_ASSERT(db.find_room_id("dev") > 0);
    MINI_ASSERT_EQ(db.find_room_id("nope"), 0LL);
    std::vector<chat::RoomRow> rooms = db.list_rooms();
    MINI_ASSERT_EQ(rooms.size(), 2u);
    MINI_ASSERT_EQ(rooms[0].name, "lobby");
    db.close();
}

// group commit：8 线程 × 50 条并发写，批事务提交后才返回 id——一条不少、id 唯一递增
MINI_TEST(group_commit_concurrent_inserts) {
    fresh_db();
    chat::Database db;
    MINI_ASSERT(db.open(kDb));
    MINI_ASSERT_EQ(db.create_room("lobby", "SERVER", 1), 0);
    long long room_id = db.find_room_id("lobby");

    const int kThreads = 8, kPerThread = 50;
    std::atomic<int> ok_count(0);
    std::vector<long long> ids((size_t)(kThreads * kPerThread), -1);
    std::vector<std::thread> pool;
    for (int t = 0; t < kThreads; ++t) {
        pool.push_back(std::thread([&, t]() {
            for (int i = 0; i < kPerThread; ++i) {
                long long id = db.insert_room_msg(room_id, "u" + std::to_string(t),
                                                  "msg-" + std::to_string(t) + "-" +
                                                      std::to_string(i),
                                                  1000 + t * kPerThread + i);
                if (id > 0) {
                    ++ok_count;
                    ids[(size_t)(t * kPerThread + i)] = id;
                }
            }
        }));
    }
    for (size_t i = 0; i < pool.size(); ++i) pool[i].join();

    MINI_ASSERT_EQ(ok_count.load(), kThreads * kPerThread);  // 全部提交成功才返回 id
    // id 全局唯一；同线程内严格递增（跨线程按拿锁顺序交错是预期——(ts,id) 全序仍成立）
    std::vector<char> seen((size_t)(kThreads * kPerThread) + 16, 0);
    int uniq = 0;
    for (int t = 0; t < kThreads; ++t) {
        long long prev = 0;
        for (int i = 0; i < kPerThread; ++i) {
            long long id = ids[(size_t)(t * kPerThread + i)];
            if (id <= 0) continue;
            MINI_ASSERT(id > prev);  // 同线程按插入序递增
            prev = id;
            MINI_ASSERT(id < (long long)seen.size() && !seen[(size_t)id]);
            seen[(size_t)id] = 1;
            ++uniq;
        }
    }
    MINI_ASSERT_EQ(uniq, kThreads * kPerThread);
    // 落盘行数一条不少
    std::vector<chat::MsgRow> rows = db.history_latest(room_id, kThreads * kPerThread + 1);
    MINI_ASSERT_EQ(rows.size(), (size_t)(kThreads * kPerThread));
    db.close();
}

// 私聊 + 离线：undelivered/mark_delivered 语义（协议 6.5）
MINI_TEST(private_and_offline) {
    fresh_db();
    chat::Database db;
    MINI_ASSERT(db.open(kDb));
    MINI_ASSERT(db.insert_private_msg("alice", "bob", "hi-bob", 10) > 0);
    std::vector<chat::OffRow> rows = db.undelivered("bob");
    MINI_ASSERT_EQ(rows.size(), 0u);  // messages 不进离线表（offline_messages 只放待补发行）
    db.insert_offline("alice", "bob", "offline-1", 11);
    db.insert_offline("alice", "bob", "offline-2", 12);
    rows = db.undelivered("bob");
    MINI_ASSERT_EQ(rows.size(), 2u);
    MINI_ASSERT_EQ(rows[0].content, "offline-1");  // 按 (ts,id) 升序补发
    db.mark_delivered(rows[0].id);
    rows = db.undelivered("bob");
    MINI_ASSERT_EQ(rows.size(), 1u);
    MINI_ASSERT_EQ(rows[0].content, "offline-2");
    std::vector<chat::MsgRow> inbox = db.inbox_latest("bob", 10);
    MINI_ASSERT_EQ(inbox.size(), 1u);  // messages 表里的私聊（离线表不算 inbox）
    db.close();
}

// (ts,id) 游标分页：与服务器同款用法（LIMIT n+1 判 more、最老一行当游标）翻页不重不漏
MINI_TEST(history_cursor_paging) {
    fresh_db();
    chat::Database db;
    MINI_ASSERT(db.open(kDb));
    MINI_ASSERT_EQ(db.create_room("r", "SERVER", 1), 0);
    long long room_id = db.find_room_id("r");
    for (int i = 0; i < 30; ++i)
        MINI_ASSERT(db.insert_room_msg(room_id, "u", "n" + std::to_string(i), 100 + i) > 0);

    std::vector<std::string> got;  // 旧→新（翻页从新往旧，逐页【前插】拼出全序）
    long long cts = 0, cid = 0;
    bool more = true;
    while (more) {
        std::vector<chat::MsgRow> page;
        if (cts == 0 && cid == 0) page = db.history_latest(room_id, 11);
        else page = db.history_before(room_id, cts, cid, 11);
        more = page.size() > 10;   // 多取第 11 行判 has_more（不用 COUNT(*)）
        if (more) page.resize(10);
        std::vector<std::string> batch;
        for (size_t i = page.size(); i-- > 0;) batch.push_back(page[i].content);  // 页内转旧→新
        got.insert(got.begin(), batch.begin(), batch.end());  // 更早的页插到前面
        if (!page.empty()) { cts = page.back().ts; cid = page.back().id; }       // 最老一行=游标
    }
    MINI_ASSERT_EQ(got.size(), 30u);
    for (int i = 0; i < 30; ++i)  // 不重不漏 + 严格旧→新
        MINI_ASSERT_EQ(got[(size_t)i], "n" + std::to_string(i));
    db.close();
}
