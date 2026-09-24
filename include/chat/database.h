// include/chat/database.h —— SQLite 持久层：users / rooms / messages / offline_messages
//
// 【所有】含用户输入的 SQL 一律 sqlite3_prepare_v2 + sqlite3_bind_*（参数化绑定，防注入，
// 见服务器 Q6）；只有 DDL/PRAGMA 等固定串走 sqlite3_exec。一把互斥锁串行化全部语句
// （单连接 + WAL）。表结构与两条索引的「索引 → 查询」说明见 src/database.cpp 建表处。
#ifndef CHAT_DATABASE_H_
#define CHAT_DATABASE_H_

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <string>
#include <vector>

#if defined(__has_include) && __has_include(<sqlite3.h>)
#include <sqlite3.h>
#else
#include "chat/sqlite3_api.h"  // 自带最小声明，链接 libsqlite3.so.0
#endif

namespace chat {

struct UserRow {
    long long id;
    std::string username;
    std::string salt;
    std::string pwd_hash;
    long long created_at;
};

struct RoomRow {
    long long id;
    std::string name;
    std::string owner;
    long long created_at;
};

struct MsgRow {
    long long id;
    std::string sender;
    std::string receiver;
    std::string content;
    long long ts;
    std::string type;
};

struct OffRow {
    long long id;
    std::string sender;
    std::string content;
    long long ts;
};

class Database {
public:
    Database();
    ~Database();
    bool open(const std::string& path);
    void close();

    // ---------- 账号 ----------
    // 0=成功；kErrDupUser=用户名已存在（UNIQUE→SQLITE_CONSTRAINT）；-1=DB 失败
    int create_user(const std::string& name, const std::string& salt,
                    const std::string& pwd_hash, long long ts);
    bool find_user(const std::string& name, UserRow& out);

    // ---------- 房间 ----------
    // 0=成功；kErrRoomExists=已存在；-1=DB 失败
    int create_room(const std::string& name, const std::string& owner, long long ts);
    long long find_room_id(const std::string& name);
    std::vector<RoomRow> list_rooms();

    // ---------- 消息落库（group commit：多条攒一个事务，见 database.cpp 头注释） ----------
    long long insert_room_msg(long long room_id, const std::string& sender,
                              const std::string& content, long long ts);
    long long insert_private_msg(const std::string& sender, const std::string& receiver,
                                 const std::string& content, long long ts);

    // ---------- 离线（delivered 标志模型，协议 6.5） ----------
    void insert_offline(const std::string& sender, const std::string& receiver,
                        const std::string& content, long long ts);
    std::vector<OffRow> undelivered(const std::string& receiver);
    void mark_delivered(long long id);

    // ---------- 历史分页（(ts,id) 游标，协议 6.3） ----------
    std::vector<MsgRow> history_latest(long long room_id, int limit);
    std::vector<MsgRow> history_before(long long room_id, long long ts, long long id, int limit);
    std::vector<MsgRow> inbox_latest(const std::string& receiver, int limit);
    std::vector<MsgRow> inbox_before(const std::string& receiver, long long ts, long long id,
                                     int limit);

    // ---------- 库元信息（schema_ver / pepper_id 等；启动期配置一致性闸门用） ----------
    std::string meta_get(const std::string& k);           // 无则空串
    bool meta_set(const std::string& k, const std::string& v);

private:
    bool exec(const char* sql);
    long long last_insert_id();
    long long count_users();                              // v1 旧库探测用

    // ---- group commit 内部协议（database.cpp 头注释有完整设计问答） ----
    // 单连接 SQLite 的经典 group commit：并发写手把多条 INSERT 攒进【同一个显式事务】，
    // 一次 COMMIT 落盘——WAL 下 fsync 是写吞吐瓶颈，批内 N 条消息共享一次 fsync。
    bool enter_batch_locked(std::unique_lock<std::mutex>& lk, int& my_gen);
    bool finish_batch_locked(std::unique_lock<std::mutex>& lk, bool starter, int my_gen);
    void wait_tx_idle(std::unique_lock<std::mutex>& lk);  // 非批写手：等开口事务收尾
    void resolve_tx_locked(bool ok);                      // COMMIT/ROLLBACK + 唤醒批内写手

    sqlite3* db_;
    std::mutex m_;
    std::condition_variable cv_;
    bool in_tx_;          // 是否有开口的批事务
    int tx_pending_;      // 本批已入账的写手数
    int tx_gen_;          // 提交代数：每次 COMMIT/ROLLBACK +1（唤醒批内等待者）
    bool tx_ok_;          // 最近一次提交的结果（批内写手据此返回 id/-1）
    std::atomic<int> writers_waiting_;  // 门外等锁的写手数（自适应凑批：无人等则立即提交）
    std::atomic<long long> commit_cost_ns_;  // COMMIT 成本 EMA（凑批窗口开/关信号）
};

}  // namespace chat

#endif  // CHAT_DATABASE_H_
