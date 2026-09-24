// src/database.cpp —— 持久层实现（全部参数化绑定）
//
// ==================== 设计问答：group commit（P1 写吞吐） ====================
// Q：单连接 SQLite + 一把互斥锁，写吞吐瓶颈在哪？怎么用 group commit 解？
//    WAL 下每条 INSERT 各走一次 COMMIT ≈ 一次 fsync（把 WAL 刷到磁盘），互斥锁串行的
//    是【事务】而不是【行】——fsync 毫秒级，百级 QPS 就把 CPU 全耗在提交上。
//    group commit：并发写手把多条 INSERT 攒进【同一个显式事务】，一次 COMMIT 落盘，
//    批内 N 条消息共享一次 fsync——吞吐近似 ×N（上限 kBatchMax），单条延迟最多 +kBatchWaitMs。
// Q：怎么凑批？谁来提交？
//    首个写手拿锁后 BEGIN + INSERT，然后【放锁等待】kBatchWaitMs（或批满 kBatchMax）——
//    等待期间门外的写手进锁、INSERT 进同一事务（搭便车）、再放锁等提交结果；
//    首个写手（=本批提交者）到点/批满后 COMMIT 并【代数 tx_gen_+1】唤醒所有搭车者。
//    自适应关键（两道闸，细节见 finish_batch_locked）：门外 ≥4 写手【且】上次 COMMIT
//    实测 ≥1ms（fsync-bound）才等窗口——低负载零延迟；慢盘攒批共享 fsync；快盘深队列
//    也不等（窗口周期会钉死吞吐）。这就是 MySQL/Postgres group commit 的「凑批」策略，
//    两次调参踩坑（中负载白等 / 深队列等窗口反成瓶颈）都写在实现注释里。
// Q：正确性边界？
//    ① 幂等/顺序：批内行序 = 谁先拿锁谁先进事务，(ts,id) 主键仍严格递增（id=ROWID）；
//    ② ACK 语义不变：insert_* 返回值在【本批提交后】才返回（已送达=已持久化）；
//    ③ 失败：COMMIT 失败 → ROLLBACK + 代数唤醒，批内所有写手拿 -1（NACK 走重试）；
//       单条 INSERT 失败：starter 回滚整批；joiner 只自己失败（语句级失败不毒化事务，
//       真致命错误会让 COMMIT 失败兜住）；
//    ④ 非批写手（users/rooms/offline_messages 的写与建号建房）先等开口事务收尾再写，
//       避免被无界卷进批；读查询不需要等（同连接能看到未提交行，2ms 窗口无害）。
// =================================================================

#include "chat/database.h"

#include <chrono>

#include "chat/log.h"
#include "chat/protocol.h"

namespace chat {

static const int kBatchMax = 32;      // 一事务最多攒 32 条写
static const int kBatchWaitMs = 2;    // 凑批窗口上限 2ms（条件不满足则 0ms 立即提交）
static const int kBatchNeed = 4;      // 门外 ≥4 个写手才凑批（中低负载零延迟，防「白等」）
static const long long kCommitCostlyNs = 1000000;  // COMMIT ≥1ms 才算「贵」（fsync-bound）

// 门外写手计数（RAII）：enter 前 +1，完成 -1——starter 据此决定要不要凑批
struct WriterProbe {
    explicit WriterProbe(std::atomic<int>& c) : c_(c) { ++c_; }
    ~WriterProbe() { --c_; }
    std::atomic<int>& c_;
};

Database::Database()
    : db_(NULL), in_tx_(false), tx_pending_(0), tx_gen_(0), tx_ok_(true), writers_waiting_(0),
      commit_cost_ns_(0) {}
Database::~Database() { close(); }

bool Database::open(const std::string& path) {
    if (sqlite3_open_v2(path.c_str(), &db_,
                        SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL) != SQLITE_OK) {
        LOG_ERROR() << "打开 SQLite 失败: " << path;
        return false;
    }
    exec("PRAGMA journal_mode=WAL;");
    exec("PRAGMA busy_timeout=3000;");
    // 持久性前提（「已送达 = 已持久化」不变式的底座）：synchronous=FULL —— 每次 COMMIT
    // 都 fsync 到磁盘，kill -9 / 掉电后已提交的写不丢。显式写死（不靠库里默认，防被改）；
    // group commit 批内多条共享一次 fsync，但 ACK 一律等 COMMIT 返回后才发（doMessage 路径）。
    exec("PRAGMA synchronous=FULL;");
    {   // 回读生效值记日志（验收证据：journal_mode=wal / synchronous=2(FULL)）
        sqlite3_stmt* st = NULL;
        if (sqlite3_prepare_v2(db_, "PRAGMA journal_mode;", -1, &st, NULL) == SQLITE_OK) {
            if (sqlite3_step(st) == SQLITE_ROW) {
                const unsigned char* p = sqlite3_column_text(st, 0);
                LOG_INFO() << "SQLite journal_mode=" << (p ? (const char*)p : "?");
            }
            sqlite3_finalize(st);
        }
        if (sqlite3_prepare_v2(db_, "PRAGMA synchronous;", -1, &st, NULL) == SQLITE_OK) {
            if (sqlite3_step(st) == SQLITE_ROW)
                LOG_INFO() << "SQLite synchronous=" << sqlite3_column_int(st, 0)
                           << "（2=FULL：每次 COMMIT fsync → ACK=已落盘的前提）";
            sqlite3_finalize(st);
        }
    }
    // 两条索引各服务的查询见 chat_server 源码头部 Q1（EXPLAIN QUERY PLAN 实测走索引，
    // 见 tools/seed_and_bench.py）：
    //   idx_messages_room_ts     → 房间历史「最近 50 条 + (ts,id) 游标翻页」
    //   idx_messages_receiver_ts → 私信收件箱「receiver=? + (ts,id) 游标翻页」
    const char* schema =
        "CREATE TABLE IF NOT EXISTS users ("
        "  id         INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  username   TEXT NOT NULL UNIQUE,"  // 用户名唯一约束：重复注册 → E1001
        "  salt       TEXT NOT NULL,"         // hex(16 字节随机盐)
        "  pwd_hash   TEXT NOT NULL,"         // hex(PBKDF2-HMAC-SHA256(pwd, salt, 100000))
        "  created_at INTEGER NOT NULL);"
        "CREATE TABLE IF NOT EXISTS rooms ("
        "  id         INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  name       TEXT NOT NULL UNIQUE,"  // 房间名唯一：/create 判重 → E2001
        "  owner      TEXT NOT NULL,"
        "  created_at INTEGER NOT NULL);"
        "CREATE TABLE IF NOT EXISTS messages ("
        "  id       INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  room_id  INTEGER NOT NULL,"
        "  sender   TEXT NOT NULL,"
        "  receiver TEXT NOT NULL,"
        "  content  TEXT NOT NULL,"
        "  ts       INTEGER NOT NULL,"
        "  type     TEXT NOT NULL);"
        "CREATE TABLE IF NOT EXISTS offline_messages ("
        "  id        INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  sender    TEXT NOT NULL,"
        "  receiver  TEXT NOT NULL,"
        "  content   TEXT NOT NULL,"
        "  ts        INTEGER NOT NULL,"
        "  delivered INTEGER NOT NULL DEFAULT 0);"
        // 索引①：房间历史「最近 50 条 + (ts,id) 游标翻页」
        "CREATE INDEX IF NOT EXISTS idx_messages_room_ts ON messages (room_id, ts);"
        // 索引②：私信收件箱「receiver=? AND (ts,id)<(?,?) 游标翻页」
        "CREATE INDEX IF NOT EXISTS idx_messages_receiver_ts ON messages (receiver, ts);"
        // schema 版本标记（meta['schema_ver']）：v2 = pepper 盲化存储（Q7②）
        "CREATE TABLE IF NOT EXISTS meta ("
        "  k TEXT PRIMARY KEY,"
        "  v TEXT NOT NULL);";
    if (!exec(schema)) return false;

    // ---- 库格式版本闸门：v1（users 落裸 K）→ v2（落盲化值）是【不可逆推】的存储变更 ----
    // 拿 v1 老库直接启动，所有老用户登录必然失败且现象像「密码全错了」——明确拒绝并
    // 指路，绝不放行（旧库兼容要么重建、要么拿 pepper 手工迁移，见 docs/migration.md）
    std::string ver = meta_get("schema_ver");
    if (ver.empty()) {
        if (count_users() > 0) {
            LOG_ERROR() << "检测到 v1 格式库（users 表存未盲化派生密钥），本版本存储格式为 v2"
                        << "（pepper 盲化，Q7②）：请重建用户库或按 docs/migration.md 迁移后重启"
                        << "——拒绝启动以防老用户登录全部失败且原因不明";
            return false;
        }
        return meta_set("schema_ver", "2");  // 空库：直接落 v2 标记
    }
    if (ver != "2") {
        LOG_ERROR() << "不支持的库格式版本 schema_ver=" << ver << "（本版本支持 2）";
        return false;
    }
    return true;
}

void Database::close() {
    if (db_) { sqlite3_close_v2(db_); db_ = NULL; }
}

// ---------- group commit 协议（头注释设计问答） ----------

bool Database::enter_batch_locked(std::unique_lock<std::mutex>& lk, int& my_gen) {
    while (in_tx_ && tx_pending_ >= kBatchMax) cv_.wait(lk);  // 批满：等上一批提交
    my_gen = tx_gen_;
    if (in_tx_) {          // 搭便车：进当前开口事务
        ++tx_pending_;
        cv_.notify_all();  // 批满时叫醒 starter 立即提交（不空等凑批窗口）
        return false;
    }
    if (!exec("BEGIN IMMEDIATE;")) {
        my_gen = -1;       // 兜底：开不了事务就走单条自动提交（finish 直接放行）
        return false;
    }
    in_tx_ = true;
    tx_pending_ = 1;
    return true;           // starter：本批由我提交
}

bool Database::finish_batch_locked(std::unique_lock<std::mutex>& lk, bool starter, int my_gen) {
    if (my_gen < 0) return true;  // 兜底自动提交模式：INSERT 已随语句落盘
    if (!starter) {
        cv_.wait(lk, [this, my_gen] { return tx_gen_ != my_gen; });  // 等 starter 提交/回滚
        return tx_ok_;
    }
    // 自适应凑批（两道闸，都是「等窗口只为贵 fsync 服务」这条原则的展开）：
    // ① 门外 ≥kBatchNeed 个写手才等——2 核机上 50+ 连接线程唤醒成簇、两两撞锁很常见，
    //    kBatchNeed=2 时中负载就条条等满窗口，P50 0.6→3.5ms（A/B 实测的第一次踩坑）；
    // ② 且【上次 COMMIT 实测够贵】（EMA ≥1ms，fsync-bound）才等——快盘上凑批是纯排队税：
    //    提交周期=窗口+fsync，2ms 窗口把吞吐钉死在 ~1/(2ms+fsync)，Little's law 反算
    //    in-flight≈45 正是这么来的（第二次踩坑：500 连接时写手远超阈值，条条等窗口，
    //    膝点≈2500 QPS 其实是窗口周期不是线程模型）。慢盘（fsync 贵）保留攒批收益。
    if (writers_waiting_.load() >= kBatchNeed &&
        (long long)commit_cost_ns_.load() >= kCommitCostlyNs)
        cv_.wait_for(lk, std::chrono::milliseconds(kBatchWaitMs),
                     [this] { return tx_pending_ >= kBatchMax; });
    resolve_tx_locked(true);
    return tx_ok_;
}

void Database::wait_tx_idle(std::unique_lock<std::mutex>& lk) {
    while (in_tx_) cv_.wait(lk);  // 非批写手不卷进批事务（自包含性/失败域隔离）
}

void Database::resolve_tx_locked(bool ok) {
    if (!in_tx_) return;  // 已被并发解析（幂等）
    bool committed = false;
    if (ok) {
        long long t0 = std::chrono::duration_cast<std::chrono::nanoseconds>(
                           std::chrono::steady_clock::now().time_since_epoch()).count();
        committed = exec("COMMIT;");
        long long cost = std::chrono::duration_cast<std::chrono::nanoseconds>(
                             std::chrono::steady_clock::now().time_since_epoch()).count() - t0;
        // 提交成本 EMA（4:1）：凑批窗口的开/关信号（见 finish_batch_locked 注释）
        commit_cost_ns_.store((commit_cost_ns_.load() * 3 + cost) / 4);
        if (!committed) exec("ROLLBACK;");
    } else {
        exec("ROLLBACK;");
    }
    in_tx_ = false;
    tx_pending_ = 0;
    tx_ok_ = committed;
    ++tx_gen_;            // 代数推进：唤醒批内所有搭车者
    cv_.notify_all();
}

// ---------- 账号 ----------

int Database::create_user(const std::string& name, const std::string& salt,
                          const std::string& pwd_hash, long long ts) {
    std::unique_lock<std::mutex> lk(m_);
    wait_tx_idle(lk);  // 建号是低频路径：等批事务收尾后单独自动提交（失败域隔离）
    sqlite3_stmt* st = NULL;
    if (sqlite3_prepare_v2(db_,
                           "INSERT INTO users (username, salt, pwd_hash, created_at) "
                           "VALUES (?,?,?,?);",
                           -1, &st, NULL) != SQLITE_OK)
        return -1;
    sqlite3_bind_text(st, 1, name.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, salt.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 3, pwd_hash.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(st, 4, ts);
    int rc = sqlite3_step(st);
    sqlite3_finalize(st);
    if (rc == SQLITE_CONSTRAINT) return kErrDupUser;  // 唯一约束生效：明确错误码，不静默
    return rc == SQLITE_DONE ? 0 : -1;
}

bool Database::find_user(const std::string& name, UserRow& out) {
    std::lock_guard<std::mutex> lk(m_);
    sqlite3_stmt* st = NULL;
    if (sqlite3_prepare_v2(db_,
                           "SELECT id, username, salt, pwd_hash, created_at "
                           "FROM users WHERE username = ?;",
                           -1, &st, NULL) != SQLITE_OK)
        return false;
    sqlite3_bind_text(st, 1, name.c_str(), -1, SQLITE_TRANSIENT);
    bool found = false;
    if (sqlite3_step(st) == SQLITE_ROW) {
        out.id = sqlite3_column_int64(st, 0);
        const unsigned char* p;
        out.username = (p = sqlite3_column_text(st, 1)) ? (const char*)p : "";
        out.salt = (p = sqlite3_column_text(st, 2)) ? (const char*)p : "";
        out.pwd_hash = (p = sqlite3_column_text(st, 3)) ? (const char*)p : "";
        out.created_at = sqlite3_column_int64(st, 4);
        found = true;
    }
    sqlite3_finalize(st);
    return found;
}

// ---------- 房间 ----------

int Database::create_room(const std::string& name, const std::string& owner, long long ts) {
    std::unique_lock<std::mutex> lk(m_);
    wait_tx_idle(lk);
    sqlite3_stmt* st = NULL;
    if (sqlite3_prepare_v2(db_,
                           "INSERT INTO rooms (name, owner, created_at) VALUES (?,?,?);",
                           -1, &st, NULL) != SQLITE_OK)
        return -1;
    sqlite3_bind_text(st, 1, name.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, owner.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(st, 3, ts);
    int rc = sqlite3_step(st);
    sqlite3_finalize(st);
    if (rc == SQLITE_CONSTRAINT) return kErrRoomExists;
    return rc == SQLITE_DONE ? 0 : -1;
}

long long Database::find_room_id(const std::string& name) {
    std::lock_guard<std::mutex> lk(m_);
    sqlite3_stmt* st = NULL;
    if (sqlite3_prepare_v2(db_, "SELECT id FROM rooms WHERE name = ?;", -1, &st, NULL) != SQLITE_OK)
        return 0;
    sqlite3_bind_text(st, 1, name.c_str(), -1, SQLITE_TRANSIENT);
    long long id = 0;
    if (sqlite3_step(st) == SQLITE_ROW) id = sqlite3_column_int64(st, 0);
    sqlite3_finalize(st);
    return id;
}

std::vector<RoomRow> Database::list_rooms() {
    std::vector<RoomRow> rows;
    std::lock_guard<std::mutex> lk(m_);
    sqlite3_stmt* st = NULL;
    if (sqlite3_prepare_v2(db_, "SELECT id, name, owner, created_at FROM rooms ORDER BY id ASC;",
                           -1, &st, NULL) != SQLITE_OK)
        return rows;
    while (sqlite3_step(st) == SQLITE_ROW) {
        RoomRow r;
        r.id = sqlite3_column_int64(st, 0);
        const unsigned char* p;
        r.name = (p = sqlite3_column_text(st, 1)) ? (const char*)p : "";
        r.owner = (p = sqlite3_column_text(st, 2)) ? (const char*)p : "";
        r.created_at = sqlite3_column_int64(st, 3);
        rows.push_back(r);
    }
    sqlite3_finalize(st);
    return rows;
}

// ---------- 消息落库（group commit 热路径：多条攒一个事务） ----------

long long Database::insert_room_msg(long long room_id, const std::string& sender,
                                    const std::string& content, long long ts) {
    WriterProbe probe(writers_waiting_);
    std::unique_lock<std::mutex> lk(m_);
    int my_gen = 0;
    bool starter = enter_batch_locked(lk, my_gen);
    sqlite3_stmt* st = NULL;
    long long id = -1;
    if (sqlite3_prepare_v2(db_,
                           "INSERT INTO messages (room_id, sender, receiver, content, ts, type) "
                           "VALUES (?,?,'ALL',?,?,'room');",
                           -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_int64(st, 1, room_id);
        sqlite3_bind_text(st, 2, sender.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 3, content.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(st, 4, ts);
        if (sqlite3_step(st) == SQLITE_DONE) id = last_insert_id();
        sqlite3_finalize(st);
    }
    if (id < 0 && starter) { resolve_tx_locked(false); return -1; }
    if (id < 0) return -1;  // joiner 语句级失败不毒化整批（致命错误由 COMMIT 兜住）
    return finish_batch_locked(lk, starter, my_gen) ? id : -1;
}

long long Database::insert_private_msg(const std::string& sender, const std::string& receiver,
                                       const std::string& content, long long ts) {
    WriterProbe probe(writers_waiting_);
    std::unique_lock<std::mutex> lk(m_);
    int my_gen = 0;
    bool starter = enter_batch_locked(lk, my_gen);
    sqlite3_stmt* st = NULL;
    long long id = -1;
    if (sqlite3_prepare_v2(db_,
                           "INSERT INTO messages (room_id, sender, receiver, content, ts, type) "
                           "VALUES (0,?,?,?,?,'private');",
                           -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, sender.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 2, receiver.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 3, content.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(st, 4, ts);
        if (sqlite3_step(st) == SQLITE_DONE) id = last_insert_id();
        sqlite3_finalize(st);
    }
    if (id < 0 && starter) { resolve_tx_locked(false); return -1; }
    if (id < 0) return -1;
    return finish_batch_locked(lk, starter, my_gen) ? id : -1;
}

// ---------- 离线 ----------

void Database::insert_offline(const std::string& sender, const std::string& receiver,
                              const std::string& content, long long ts) {
    std::unique_lock<std::mutex> lk(m_);
    wait_tx_idle(lk);  // 非批写手：不卷进消息批事务（低频路径，隔离失败域）
    sqlite3_stmt* st = NULL;
    if (sqlite3_prepare_v2(db_,
                           "INSERT INTO offline_messages (sender, receiver, content, ts, delivered) "
                           "VALUES (?,?,?,?,0);",
                           -1, &st, NULL) != SQLITE_OK)
        return;
    sqlite3_bind_text(st, 1, sender.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, receiver.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 3, content.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(st, 4, ts);
    sqlite3_step(st);
    sqlite3_finalize(st);
}

std::vector<OffRow> Database::undelivered(const std::string& receiver) {
    std::vector<OffRow> rows;
    std::lock_guard<std::mutex> lk(m_);
    sqlite3_stmt* st = NULL;
    if (sqlite3_prepare_v2(db_,
                           "SELECT id, sender, content, ts FROM offline_messages "
                           "WHERE receiver = ? AND delivered = 0 ORDER BY ts ASC, id ASC;",
                           -1, &st, NULL) != SQLITE_OK)
        return rows;
    sqlite3_bind_text(st, 1, receiver.c_str(), -1, SQLITE_TRANSIENT);
    while (sqlite3_step(st) == SQLITE_ROW) {
        OffRow r;
        r.id = sqlite3_column_int64(st, 0);
        const unsigned char* p;
        r.sender = (p = sqlite3_column_text(st, 1)) ? (const char*)p : "";
        r.content = (p = sqlite3_column_text(st, 2)) ? (const char*)p : "";
        r.ts = sqlite3_column_int64(st, 3);
        rows.push_back(r);
    }
    sqlite3_finalize(st);
    return rows;
}

void Database::mark_delivered(long long id) {
    std::unique_lock<std::mutex> lk(m_);
    wait_tx_idle(lk);
    sqlite3_stmt* st = NULL;
    if (sqlite3_prepare_v2(db_, "UPDATE offline_messages SET delivered = 1 WHERE id = ?;",
                           -1, &st, NULL) != SQLITE_OK)
        return;
    sqlite3_bind_int64(st, 1, id);
    sqlite3_step(st);
    sqlite3_finalize(st);
}

// ---------- 历史分页（Q3：(ts,id) 游标，不用 OFFSET） ----------
// 四条查询都是「等值列 + 行值游标 + ORDER BY ts DESC, id DESC + LIMIT」，
// EXPLAIN QUERY PLAN 各走 idx_messages_room_ts / idx_messages_receiver_ts。
// 调用方多取 1 行判断 has_more（LIMIT n+1），不用 COUNT(*)（多扫一遍索引）。

std::vector<MsgRow> Database::history_latest(long long room_id, int limit) {
    std::lock_guard<std::mutex> lk(m_);
    sqlite3_stmt* st = NULL;
    std::vector<MsgRow> rows;
    if (sqlite3_prepare_v2(db_,
                           "SELECT id, sender, receiver, content, ts, type FROM messages "
                           "WHERE room_id = ? ORDER BY ts DESC, id DESC LIMIT ?;",
                           -1, &st, NULL) != SQLITE_OK)
        return rows;
    sqlite3_bind_int64(st, 1, room_id);
    sqlite3_bind_int(st, 2, limit);
    while (sqlite3_step(st) == SQLITE_ROW) {
        MsgRow r;
        r.id = sqlite3_column_int64(st, 0);
        const unsigned char* p;
        r.sender = (p = sqlite3_column_text(st, 1)) ? (const char*)p : "";
        r.receiver = (p = sqlite3_column_text(st, 2)) ? (const char*)p : "";
        r.content = (p = sqlite3_column_text(st, 3)) ? (const char*)p : "";
        r.ts = sqlite3_column_int64(st, 4);
        r.type = (p = sqlite3_column_text(st, 5)) ? (const char*)p : "";
        rows.push_back(r);
    }
    sqlite3_finalize(st);
    return rows;
}

std::vector<MsgRow> Database::history_before(long long room_id, long long ts, long long id,
                                             int limit) {
    std::lock_guard<std::mutex> lk(m_);
    sqlite3_stmt* st = NULL;
    std::vector<MsgRow> rows;
    if (sqlite3_prepare_v2(db_,
                           "SELECT id, sender, receiver, content, ts, type FROM messages "
                           "WHERE room_id = ? AND (ts, id) < (?, ?) "
                           "ORDER BY ts DESC, id DESC LIMIT ?;",
                           -1, &st, NULL) != SQLITE_OK)
        return rows;
    sqlite3_bind_int64(st, 1, room_id);
    sqlite3_bind_int64(st, 2, ts);
    sqlite3_bind_int64(st, 3, id);
    sqlite3_bind_int(st, 4, limit);
    while (sqlite3_step(st) == SQLITE_ROW) {
        MsgRow r;
        r.id = sqlite3_column_int64(st, 0);
        const unsigned char* p;
        r.sender = (p = sqlite3_column_text(st, 1)) ? (const char*)p : "";
        r.receiver = (p = sqlite3_column_text(st, 2)) ? (const char*)p : "";
        r.content = (p = sqlite3_column_text(st, 3)) ? (const char*)p : "";
        r.ts = sqlite3_column_int64(st, 4);
        r.type = (p = sqlite3_column_text(st, 5)) ? (const char*)p : "";
        rows.push_back(r);
    }
    sqlite3_finalize(st);
    return rows;
}

std::vector<MsgRow> Database::inbox_latest(const std::string& receiver, int limit) {
    std::lock_guard<std::mutex> lk(m_);
    sqlite3_stmt* st = NULL;
    std::vector<MsgRow> rows;
    if (sqlite3_prepare_v2(db_,
                           "SELECT id, sender, receiver, content, ts, type FROM messages "
                           "WHERE receiver = ? ORDER BY ts DESC, id DESC LIMIT ?;",
                           -1, &st, NULL) != SQLITE_OK)
        return rows;
    sqlite3_bind_text(st, 1, receiver.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(st, 2, limit);
    while (sqlite3_step(st) == SQLITE_ROW) {
        MsgRow r;
        r.id = sqlite3_column_int64(st, 0);
        const unsigned char* p;
        r.sender = (p = sqlite3_column_text(st, 1)) ? (const char*)p : "";
        r.receiver = (p = sqlite3_column_text(st, 2)) ? (const char*)p : "";
        r.content = (p = sqlite3_column_text(st, 3)) ? (const char*)p : "";
        r.ts = sqlite3_column_int64(st, 4);
        r.type = (p = sqlite3_column_text(st, 5)) ? (const char*)p : "";
        rows.push_back(r);
    }
    sqlite3_finalize(st);
    return rows;
}

std::vector<MsgRow> Database::inbox_before(const std::string& receiver, long long ts, long long id,
                                           int limit) {
    std::lock_guard<std::mutex> lk(m_);
    sqlite3_stmt* st = NULL;
    std::vector<MsgRow> rows;
    if (sqlite3_prepare_v2(db_,
                           "SELECT id, sender, receiver, content, ts, type FROM messages "
                           "WHERE receiver = ? AND (ts, id) < (?, ?) "
                           "ORDER BY ts DESC, id DESC LIMIT ?;",
                           -1, &st, NULL) != SQLITE_OK)
        return rows;
    sqlite3_bind_text(st, 1, receiver.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(st, 2, ts);
    sqlite3_bind_int64(st, 3, id);
    sqlite3_bind_int(st, 4, limit);
    while (sqlite3_step(st) == SQLITE_ROW) {
        MsgRow r;
        r.id = sqlite3_column_int64(st, 0);
        const unsigned char* p;
        r.sender = (p = sqlite3_column_text(st, 1)) ? (const char*)p : "";
        r.receiver = (p = sqlite3_column_text(st, 2)) ? (const char*)p : "";
        r.content = (p = sqlite3_column_text(st, 3)) ? (const char*)p : "";
        r.ts = sqlite3_column_int64(st, 4);
        r.type = (p = sqlite3_column_text(st, 5)) ? (const char*)p : "";
        rows.push_back(r);
    }
    sqlite3_finalize(st);
    return rows;
}

// ---------- 工具 ----------

bool Database::exec(const char* sql) {
    char* err = NULL;
    int rc = sqlite3_exec(db_, sql, NULL, NULL, &err);
    if (rc != SQLITE_OK) {
        LOG_ERROR() << "SQL 失败: " << (err ? err : "?");
        return false;
    }
    return true;
}

long long Database::last_insert_id() {
    sqlite3_stmt* st = NULL;
    long long id = -1;
    if (sqlite3_prepare_v2(db_, "SELECT last_insert_rowid();", -1, &st, NULL) == SQLITE_OK) {
        if (sqlite3_step(st) == SQLITE_ROW) id = sqlite3_column_int64(st, 0);
        sqlite3_finalize(st);
    }
    return id;
}

// meta 键值（schema_ver 等）：固定键名的低频路径，参数化纪律不豁免
std::string Database::meta_get(const std::string& k) {
    std::lock_guard<std::mutex> lk(m_);
    sqlite3_stmt* st = NULL;
    std::string v;
    if (sqlite3_prepare_v2(db_, "SELECT v FROM meta WHERE k = ?;", -1, &st, NULL) != SQLITE_OK)
        return v;
    sqlite3_bind_text(st, 1, k.c_str(), -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW) {
        const unsigned char* p = sqlite3_column_text(st, 0);
        if (p) v = (const char*)p;
    }
    sqlite3_finalize(st);
    return v;
}

bool Database::meta_set(const std::string& k, const std::string& v) {
    std::lock_guard<std::mutex> lk(m_);
    sqlite3_stmt* st = NULL;
    if (sqlite3_prepare_v2(db_,
                           "INSERT OR REPLACE INTO meta (k, v) VALUES (?,?);",
                           -1, &st, NULL) != SQLITE_OK)
        return false;
    sqlite3_bind_text(st, 1, k.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, v.c_str(), -1, SQLITE_TRANSIENT);
    bool ok = (sqlite3_step(st) == SQLITE_DONE);
    sqlite3_finalize(st);
    return ok;
}

long long Database::count_users() {
    std::lock_guard<std::mutex> lk(m_);
    sqlite3_stmt* st = NULL;
    long long n = 0;
    if (sqlite3_prepare_v2(db_, "SELECT COUNT(*) FROM users;", -1, &st, NULL) != SQLITE_OK)
        return 0;
    if (sqlite3_step(st) == SQLITE_ROW) n = sqlite3_column_int64(st, 0);
    sqlite3_finalize(st);
    return n;
}

}  // namespace chat
