// chat_server_v5.cpp —— 在线聊天服务器 v5：账号体系 + 房间路由 + 消息持久化与历史分页
//
// 与既有版本的关系：
//   v1 (chat_server_fixed.cpp)  完整业务，`|`+`\n` 文本协议（对照用）
//   v2 (chat_server_v2.cpp)     长度前缀+JSON 帧层 + PING/PONG
//   v3 (chat_server_v3.cpp)     Reactor 性能实验（epoll ET + 线程池，仅帧层）
//   v4 (chat_server_v4.cpp)     可靠性改造主力（心跳/幂等去重/离线 E2EACK，保留对照）
//   v5 (本文件)                 账号 + 房间 + 持久化/游标分页（本提示词）；帧层沿用 v2，
//                               心跳/ACK/内存去重保留轻量版，离线库换成 delivered 标志模型。
// 线程模型：thread-per-connection + 心跳扫描线程 + 单连接 SQLite（一把互斥锁串行，WAL）。
//
// 编译（Linux，只有 libsqlite3.so.0 运行库、无 dev 头——本项目裸机环境）:
//   g++ -std=c++11 -Wall -pthread chat_server_v5.cpp -o chat_server_v5
//       /usr/lib/x86_64-linux-gnu/libsqlite3.so.0     （续行，同一命令）
//   （sqlite3 头：__has_include(<sqlite3.h>) 为假时自动用自带 sqlite3_api.h 声明）
//   （口令哈希：无 <openssl/evp.h> 时自动用 pwd_hash.h 内置 PBKDF2-HMAC-SHA256）
// 编译（Docker / 装有 libsqlite3-dev + libssl-dev）:
//   g++ -std=c++11 -Wall -pthread chat_server_v5.cpp -o chat_server_v5 -lsqlite3 -lcrypto
// 教学降级口令哈希（salt‖pwd SHA-256 迭代；缺陷见 pwd_hash.h 大段注释）:
//   g++ ... -DCHAT_PWD_DEGRADED=1 ...
// 用法: ./chat_server_v5 [port] [--db FILE] [--idle MS] [--scan MS] [--token-ttl SEC]
//       默认 8888 端口 / chat_server_v5.db / 30000 / 5000 / 7 天
//
// ==================== 设计问答（账号/房间/持久化改造） ====================
// Q1 两条索引各服务哪条查询？为什么是 (room_id, ts) / (receiver, ts) 这两个复合？
//    ① idx_messages_room_ts (room_id, ts) —— 服务「进入房间拉最近 50 条 + 向上翻页」：
//         最近页: SELECT ... WHERE room_id=?                         ORDER BY ts DESC, id DESC LIMIT 50
//         翻页:   SELECT ... WHERE room_id=? AND (ts,id)<(?,?)       ORDER BY ts DESC, id DESC LIMIT 50
//       复合索引先按 room_id 等值定位（B-Tree 一次 seek 到该房间的消息段），再在段内按 ts
//       有序扫描；ts 相同的行按 rowid（= id）天然有序，所以 ORDER BY ts DESC, id DESC
//       直接反向扫索引即可满足，【不需要临时 B-Tree 排序】。
//       EXPLAIN QUERY PLAN（tools/seed_and_bench.py 实测输出）：
//         SEARCH messages USING INDEX idx_messages_room_ts (room_id=? AND ts<?)
//    ② idx_messages_receiver_ts (receiver, ts) —— 服务「私信收件箱翻页」：
//         SELECT ... WHERE receiver=? AND (ts,id)<(?,?) ORDER BY ts DESC, id DESC LIMIT 50
//       群聊消息 receiver 恒为 'ALL'，不会与个人收件查询混淆；EXPLAIN：
//         SEARCH messages USING INDEX idx_messages_receiver_ts (receiver=? AND ts<?)
//    为什么不用单列索引：单列 (ts) 无法先按房间/收件人裁剪——每页都要扫全表时间轴再过滤；
//    复合索引把「等值列放前、排序列放后」，等值条件收窄到一段连续索引，ts 顺序直接可扫。
// Q2 广播为什么从 O(N) 全量改成按房间路由？
//    v4 是单一大厅：broadcastSystem/broadcastUserlist/broadcastRelay 都拿 online_ 全表快照，
//    每条消息对每个在线连接各写一次 socket——N 个在线用户时一次广播 O(N) 次 send，
//    且互不相关的两个小房间也互相打扰（无隔离）。
//    v5 服务器维护 room → members（用户名集合）与 user → room 映射（本文件 RoomRegistry）：
//    房间消息只遍历【本房间成员】（broadcastRoom）：O(房间成员数)。USERLIST/进出房通知
//    同样限定本房间。1000 人在线但各在 10 个百人房时，单条广播成本从 1000 降到 ~100，
//    还天然实现了「广播只发给本房间成员」的隔离语义。
// Q3 为什么深分页用 (ts, id) 游标，而不是 LIMIT/OFFSET？
//    LIMIT/OFFSET 的语义是「扫过前 OFFSET 行再取 50 行」：第 100 页要先白白丢掉 4950 行，
//    页越深成本线性涨（实测 OFFSET 89000 ≈ 4ms，游标深页 ≈ 0.04ms，见 seed_and_bench 输出）；
//    且翻页期间有新消息插入时 OFFSET 会「跳行/重行」（窗口漂移）。
//    游标（keyset）把「上一页最后一行的 (ts, id)」当书签，WHERE (ts,id)<(?,?) 直接 seek 到
//    B-Tree 的对应位置：深度无关 O(log n + 50)；(ts,id) 是唯一键（id=主键），严格全序，
//    插入新消息不影响旧书签，永不重不漏（seed_and_bench.py 对 100 页×50 条做了不重不漏校验）。
//    为什么是 (ts, id) 两个字段：ts 秒级，同一秒可有多条消息，只按 ts 裁剪会把同秒的
//    边界消息跳过/重复；id 做 tie-break，行值比较 (ts,id)<(?,?) 让索引仍可用
//    （见 Q1 的 EXPLAIN：room_id=? AND ts<?）。OFFSET 的唯一优势是「跳页」（直接跳到
//    第 100 页），聊天记录是顺序上翻场景，游标是正确工具。
// Q4 Token 怎么设计？重连免密怎么生效？
//    登录成功（口令校验通过）后服务器生成 Token：32 字节 CSPRNG（/dev/urandom）+ 过期时间
//    （--token-ttl，默认 7 天），hex(64 字符) 随 AUTH_OK 下发；客户端存本地会话文件。
//    之后所有业务帧以连接为信任边界（服务器认「已通过 AUTH/LOGIN 的连接」，from 字段不
//    可信，一律取会话身份——防伪造他人身份）；客户端断线重连免密：新连接直接 AUTH(token)。
//    Token 存服务器内存表 token → (user, exp)：服务器重启后失效，客户端回落口令登录
//    （教学取舍——生产应存 users 旁的 tokens 表/Redis，并加吊销与滑动续期）。
//    Token 是持有者凭证（bearer）：明文 TCP 下有窃听风险，生产必须 TLS（本项目 v1 起的
//    已知短板，README 已注明），且不该写进日志。
// Q5 同名「重复登录」怎么处理？
//    口令 LOGIN 时该账号已在线 → 显式拒绝 E1004「该账号已在线，不允许重复登录」
//    （不静默失败；同一连接重复 LOGIN/AUTH → E1005）。Token AUTH（重连免密的恢复路径）
//    则走「顶号」：踢掉旧连接并明确通知旧连接「已在其他连接恢复会话」——弱网重连时旧连接
//    往往还是半开僵尸，若也拒绝，用户会被自己的尸体锁在门外直到心跳超时（v4 Q4 论证）。
//    两条路径的区别是身份强度：口令=人在另一台机器前操作（拒绝），Token=同一会话续传（顶替）。
// Q6 所有 SQL 为什么一律参数化绑定？
//    字符串拼接 SQL = 注入面：用户名/房间名/消息内容里的 ' OR '1'='1、';DROP TABLE ...;
//    都会被当成 SQL 语法执行。本文件【所有】用户数据路径都走 sqlite3_prepare_v2 +
//    sqlite3_bind_text/int64（占位符 ?），数据永远不进入 SQL 文本——SQLite 把绑定值当
//    纯数据解析，无论内容里有什么字符。DDL/PRAGMA 等无用户输入的固定串才用 sqlite3_exec。
//    面试常问「参数化为什么防注入」：不是转义，是【语法与数据分离】——解析器在见到绑定值
//    之前已经完成语句语法分析，绑定值没有机会改变语句结构。
// =================================================================

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#if defined(__has_include) && __has_include(<sqlite3.h>)
#include <sqlite3.h>
#else
#include "sqlite3_api.h"  // 自带最小声明，链接 libsqlite3.so.0
#endif

#include "pwd_hash.h"  // salt(16B) + PBKDF2-HMAC-SHA256(100000)（档位见该文件头注释）

#ifdef _WIN32
#ifndef FD_SETSIZE
#define FD_SETSIZE 1024
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
typedef SOCKET socket_t;
#else
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>
typedef int socket_t;
#endif

// ---------------- 错误码（全表见 PROTOCOL.md 第 6 节：错误码 + 中文文案，禁止静默失败） ----------------
static const int kErrDupUser = 1001;        // 用户名已存在（users.username UNIQUE 约束）
static const int kErrNoSuchUser = 1002;     // 用户不存在
static const int kErrBadPass = 1003;        // 密码错误
static const int kErrDupLogin = 1004;       // 重复登录：该账号已在线
static const int kErrAlreadyAuth = 1005;    // 重复登录：本连接已在登录态
static const int kErrBadToken = 1006;       // Token 无效或已过期
static const int kErrNotAuth = 1007;        // 尚未登录
static const int kErrBadCredFmt = 1008;     // 用户名/密码格式非法
static const int kErrRoomExists = 2001;     // 房间已存在
static const int kErrNoSuchRoom = 2002;     // 房间不存在
static const int kErrNotInRoom = 2003;      // 尚未加入房间
static const int kErrBadRoomName = 2004;    // 房间名非法
static const int kErrBadCursor = 3001;      // 历史游标非法
static const int kErrDb = 9001;             // 数据库错误

// ---------------- socket 薄封装（与 v4 同款） ----------------

static bool sock_valid(socket_t s) {
#ifdef _WIN32
    return s != INVALID_SOCKET;
#else
    return s >= 0;
#endif
}

static void sock_close(socket_t s) {
#ifdef _WIN32
    closesocket(s);
#else
    close(s);
#endif
}

static void sock_shutdown(socket_t s) {
#ifdef _WIN32
    shutdown(s, SD_BOTH);
#else
    shutdown(s, SHUT_RDWR);
#endif
}

static long sock_send(socket_t fd, const char* buf, size_t len) {
#ifdef _WIN32
    return ::send(fd, buf, (int)len, 0);
#else
    return ::send(fd, buf, len, MSG_NOSIGNAL);
#endif
}

static long sock_recv(socket_t fd, char* buf, size_t len) {
#ifdef _WIN32
    return ::recv(fd, buf, (int)len, 0);
#else
    return ::recv(fd, buf, len, 0);
#endif
}

static bool sock_would_block() {
#ifdef _WIN32
    return WSAGetLastError() == WSAEWOULDBLOCK;
#else
    return errno == EAGAIN || errno == EWOULDBLOCK;
#endif
}

static bool sock_interrupted() {
#ifdef _WIN32
    return WSAGetLastError() == WSAEINTR;
#else
    return errno == EINTR;
#endif
}

static long long now_ms() {
    return (long long)std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

static long long now_ts() { return now_ms() / 1000; }

static const int kKeepIdleSec = 15;
static const int kKeepIntvlSec = 5;
static const int kKeepCnt = 3;
static const int kSendTimeoutSec = 3;
static const int kSendRetry = 2;
static const size_t kMaxFrame = 1u << 20;
static const int kHistPageSize = 50;  // 需求：进入房间拉最近 50 条

static std::string make_wire_frame(const std::string& body) {
    std::string pkt;
    unsigned int n = (unsigned int)body.size();
    pkt += (char)((n >> 24) & 0xFF);
    pkt += (char)((n >> 16) & 0xFF);
    pkt += (char)((n >> 8) & 0xFF);
    pkt += (char)(n & 0xFF);
    pkt += body;
    return pkt;
}

static void apply_socket_opts(socket_t fd) {
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (const char*)&one, sizeof(one));
#ifdef __linux__
    setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, (const char*)&one, sizeof(one));
    setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, (const char*)&kKeepIdleSec, sizeof(kKeepIdleSec));
    setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, (const char*)&kKeepIntvlSec, sizeof(kKeepIntvlSec));
    setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, (const char*)&kKeepCnt, sizeof(kKeepCnt));
#endif
#ifdef _WIN32
    DWORD ms = kSendTimeoutSec * 1000;
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, (const char*)&ms, sizeof(ms));
#else
    timeval tv;
    tv.tv_sec = kSendTimeoutSec;
    tv.tv_usec = 0;
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, (const char*)&tv, sizeof(tv));
#endif
}

// ---------------- MiniJson（v2/v4 同款扩展：支持嵌套对象/数组，服务 HISTORY/ROOMS_LIST） ----------------
namespace minijson {

struct Object;

struct Value {
    enum Type { T_STR, T_NUM, T_LIST, T_OBJ } type;
    std::string str;
    long long num;
    std::vector<Value> list;
    std::shared_ptr<Object> obj;
    Value() : type(T_STR), num(0) {}
    static Value make_str(const std::string& s) { Value v; v.type = T_STR; v.str = s; return v; }
    static Value make_num(long long n) { Value v; v.type = T_NUM; v.num = n; return v; }
    static Value make_list(const std::vector<std::string>& l) {
        Value v;
        v.type = T_LIST;
        for (size_t i = 0; i < l.size(); ++i) v.list.push_back(make_str(l[i]));
        return v;
    }
    static Value make_obj(const Object& o);
};

struct Object {
public:
    std::vector<std::pair<std::string, Value> > fields;
    void set(const std::string& k, const Value& v) {
        for (size_t i = 0; i < fields.size(); ++i)
            if (fields[i].first == k) { fields[i].second = v; return; }
        fields.push_back(std::make_pair(k, v));
    }
    void set_str(const std::string& k, const std::string& s) { set(k, Value::make_str(s)); }
    void set_num(const std::string& k, long long n) { set(k, Value::make_num(n)); }
    void set_list(const std::string& k, const std::vector<std::string>& l) { set(k, Value::make_list(l)); }
    void set_objs(const std::string& k, const std::vector<Object>& objs) {
        Value v;
        v.type = Value::T_LIST;
        for (size_t i = 0; i < objs.size(); ++i) v.list.push_back(Value::make_obj(objs[i]));
        set(k, v);
    }
    std::string get_str(const std::string& k, const std::string& def = "") const {
        for (size_t i = 0; i < fields.size(); ++i)
            if (fields[i].first == k && fields[i].second.type == Value::T_STR) return fields[i].second.str;
        return def;
    }
    long long get_num(const std::string& k, long long def = 0) const {
        for (size_t i = 0; i < fields.size(); ++i)
            if (fields[i].first == k && fields[i].second.type == Value::T_NUM) return fields[i].second.num;
        return def;
    }
    std::vector<std::string> get_list(const std::string& k) const {
        std::vector<std::string> out;
        for (size_t i = 0; i < fields.size(); ++i) {
            if (fields[i].first != k || fields[i].second.type != Value::T_LIST) continue;
            for (size_t j = 0; j < fields[i].second.list.size(); ++j)
                if (fields[i].second.list[j].type == Value::T_STR)
                    out.push_back(fields[i].second.list[j].str);
        }
        return out;
    }
    std::vector<Object> get_objs(const std::string& k) const {
        std::vector<Object> out;
        for (size_t i = 0; i < fields.size(); ++i) {
            if (fields[i].first != k || fields[i].second.type != Value::T_LIST) continue;
            for (size_t j = 0; j < fields[i].second.list.size(); ++j)
                if (fields[i].second.list[j].type == Value::T_OBJ && fields[i].second.list[j].obj)
                    out.push_back(*fields[i].second.list[j].obj);
        }
        return out;
    }
};

inline Value Value::make_obj(const Object& o) {
    Value v;
    v.type = T_OBJ;
    v.obj.reset(new Object(o));
    return v;
}

static void skip_ws(const std::string& t, size_t& i) {
    while (i < t.size() && (t[i] == ' ' || t[i] == '\t' || t[i] == '\n' || t[i] == '\r')) ++i;
}

static bool append_utf8(std::string& out, unsigned long cp) {
    if (cp <= 0x7F) out += (char)cp;
    else if (cp <= 0x7FF) { out += (char)(0xC0 | (cp >> 6)); out += (char)(0x80 | (cp & 0x3F)); }
    else if (cp <= 0xFFFF) {
        out += (char)(0xE0 | (cp >> 12));
        out += (char)(0x80 | ((cp >> 6) & 0x3F));
        out += (char)(0x80 | (cp & 0x3F));
    } else if (cp <= 0x10FFFF) {
        out += (char)(0xF0 | (cp >> 18));
        out += (char)(0x80 | ((cp >> 12) & 0x3F));
        out += (char)(0x80 | ((cp >> 6) & 0x3F));
        out += (char)(0x80 | (cp & 0x3F));
    } else return false;
    return true;
}

static bool hex4(const std::string& t, size_t pos, unsigned long& out) {
    if (pos + 4 > t.size()) return false;
    out = 0;
    for (size_t k = 0; k < 4; ++k) {
        char h = t[pos + k];
        out <<= 4;
        if (h >= '0' && h <= '9') out |= (unsigned long)(h - '0');
        else if (h >= 'a' && h <= 'f') out |= (unsigned long)(h - 'a' + 10);
        else if (h >= 'A' && h <= 'F') out |= (unsigned long)(h - 'A' + 10);
        else return false;
    }
    return true;
}

static bool parse_string(const std::string& t, size_t& i, std::string& out) {
    if (i >= t.size() || t[i] != '"') return false;
    ++i;
    out.clear();
    while (i < t.size()) {
        unsigned char c = (unsigned char)t[i];
        if (c == '"') { ++i; return true; }
        if (c == '\\') {
            ++i;
            if (i >= t.size()) return false;
            char e = t[i];
            switch (e) {
                case '"':  out += '"';  ++i; break;
                case '\\': out += '\\'; ++i; break;
                case '/':  out += '/';  ++i; break;
                case 'b':  out += '\b'; ++i; break;
                case 'f':  out += '\f'; ++i; break;
                case 'n':  out += '\n'; ++i; break;
                case 'r':  out += '\r'; ++i; break;
                case 't':  out += '\t'; ++i; break;
                case 'u': {
                    unsigned long cu = 0;
                    if (!hex4(t, i + 1, cu)) return false;
                    i += 5;
                    unsigned long cp = cu;
                    if (cu >= 0xD800 && cu <= 0xDBFF) {
                        if (i + 1 >= t.size() || t[i] != '\\' || t[i + 1] != 'u') return false;
                        unsigned long lo = 0;
                        if (!hex4(t, i + 2, lo)) return false;
                        if (lo < 0xDC00 || lo > 0xDFFF) return false;
                        cp = 0x10000UL + ((cu - 0xD800UL) << 10) + (lo - 0xDC00UL);
                        i += 6;
                    } else if (cu >= 0xDC00 && cu <= 0xDFFF) return false;
                    if (!append_utf8(out, cp)) return false;
                    break;
                }
                default: return false;
            }
        } else if (c < 0x20) return false;
        else { out += t[i]; ++i; }
    }
    return false;
}

static bool parse_number(const std::string& t, size_t& i, long long& out) {
    bool neg = false;
    if (i < t.size() && t[i] == '-') { neg = true; ++i; }
    if (i >= t.size() || t[i] < '0' || t[i] > '9') return false;
    long long v = 0;
    while (i < t.size() && t[i] >= '0' && t[i] <= '9') { v = v * 10 + (t[i] - '0'); ++i; }
    if (i < t.size() && t[i] == '.') { ++i; while (i < t.size() && t[i] >= '0' && t[i] <= '9') ++i; }
    if (i < t.size() && (t[i] == 'e' || t[i] == 'E')) {
        ++i;
        if (i < t.size() && (t[i] == '+' || t[i] == '-')) ++i;
        while (i < t.size() && t[i] >= '0' && t[i] <= '9') ++i;
    }
    out = neg ? -v : v;
    return true;
}

static bool parse_value(const std::string& t, size_t& i, Value& out);  // 前置：对象/数组递归

static bool parse_object_body(const std::string& t, size_t& i, Object& out) {
    // 进入时 i 指向 '{'
    ++i;
    out.fields.clear();
    skip_ws(t, i);
    if (i < t.size() && t[i] == '}') { ++i; return true; }
    while (true) {
        skip_ws(t, i);
        std::string key;
        if (!parse_string(t, i, key)) return false;
        skip_ws(t, i);
        if (i >= t.size() || t[i] != ':') return false;
        ++i;
        Value v;
        if (!parse_value(t, i, v)) return false;
        out.set(key, v);
        skip_ws(t, i);
        if (i < t.size() && t[i] == ',') { ++i; continue; }
        if (i < t.size() && t[i] == '}') { ++i; return true; }
        return false;
    }
}

static bool parse_value(const std::string& t, size_t& i, Value& out) {
    skip_ws(t, i);
    if (i >= t.size()) return false;
    char c = t[i];
    if (c == '"') {
        std::string s;
        if (!parse_string(t, i, s)) return false;
        out = Value::make_str(s);
        return true;
    }
    if (c == '-' || (c >= '0' && c <= '9')) {
        long long n = 0;
        if (!parse_number(t, i, n)) return false;
        out = Value::make_num(n);
        return true;
    }
    if (c == '{') {  // 嵌套对象（HISTORY 条目 / ROOMS_LIST 条目）
        Object o;
        if (!parse_object_body(t, i, o)) return false;
        out = Value::make_obj(o);
        return true;
    }
    if (c == '[') {
        ++i;
        Value arr;
        arr.type = Value::T_LIST;
        skip_ws(t, i);
        if (i < t.size() && t[i] == ']') { ++i; out = arr; return true; }
        while (true) {
            skip_ws(t, i);
            Value item;
            if (!parse_value(t, i, item)) return false;
            arr.list.push_back(item);
            skip_ws(t, i);
            if (i < t.size() && t[i] == ',') { ++i; continue; }
            if (i < t.size() && t[i] == ']') { ++i; out = arr; return true; }
            return false;
        }
    }
    if (t.compare(i, 4, "null") == 0) { i += 4; out = Value::make_str(""); return true; }
    return false;
}

bool parse(const std::string& text, Object& out) {
    size_t i = 0;
    skip_ws(text, i);
    if (i >= text.size() || text[i] != '{') return false;
    if (!parse_object_body(text, i, out)) return false;
    skip_ws(text, i);
    return i == text.size();
}

static std::string escape_string(const std::string& s) {
    std::string o;
    for (size_t i = 0; i < s.size(); ++i) {
        unsigned char c = (unsigned char)s[i];
        switch (c) {
            case '"':  o += "\\\""; break;
            case '\\': o += "\\\\"; break;
            case '\b': o += "\\b";  break;
            case '\f': o += "\\f";  break;
            case '\n': o += "\\n";  break;
            case '\r': o += "\\r";  break;
            case '\t': o += "\\t";  break;
            default:
                if (c < 0x20) {
                    char buf[8];
                    std::snprintf(buf, sizeof(buf), "\\u%04x", c);
                    o += buf;
                } else o += (char)c;
        }
    }
    return o;
}

static void serialize_value(const Value& v, std::string& s) {
    switch (v.type) {
        case Value::T_STR: s += "\"" + escape_string(v.str) + "\""; break;
        case Value::T_NUM: s += std::to_string(v.num); break;
        case Value::T_OBJ:
            if (v.obj) {
                s += "{";
                for (size_t i = 0; i < v.obj->fields.size(); ++i) {
                    if (i) s += ",";
                    s += "\"" + escape_string(v.obj->fields[i].first) + "\":";
                    serialize_value(v.obj->fields[i].second, s);
                }
                s += "}";
            } else s += "null";
            break;
        case Value::T_LIST: {
            s += "[";
            for (size_t j = 0; j < v.list.size(); ++j) {
                if (j) s += ",";
                serialize_value(v.list[j], s);
            }
            s += "]";
            break;
        }
    }
}

std::string serialize(const Object& obj) {
    std::string s = "{";
    for (size_t i = 0; i < obj.fields.size(); ++i) {
        if (i) s += ",";
        s += "\"" + escape_string(obj.fields[i].first) + "\":";
        serialize_value(obj.fields[i].second, s);
    }
    s += "}";
    return s;
}

}  // namespace minijson

// ---------------- FrameReader（与 v2/v4 同款：累积缓冲 + 循环取帧） ----------------
class FrameReader {
public:
    explicit FrameReader(size_t max_frame = kMaxFrame) : max_frame_(max_frame), failed_(false) {}
    void feed(const char* data, size_t len) { if (!failed_) buf_.append(data, len); }
    bool next(std::string& body) {
        if (failed_) return false;
        if (buf_.size() < 4) return false;
        size_t len = ((size_t)(unsigned char)buf_[0] << 24) | ((size_t)(unsigned char)buf_[1] << 16) |
                     ((size_t)(unsigned char)buf_[2] << 8) | ((size_t)(unsigned char)buf_[3]);
        if (len > max_frame_) { failed_ = true; return false; }
        if (buf_.size() < 4 + len) return false;
        body.assign(buf_, 4, len);
        buf_.erase(0, 4 + len);
        return true;
    }
    bool failed() const { return failed_; }

private:
    std::string buf_;
    size_t max_frame_;
    bool failed_;
};

// ---------------- SQLite 持久层：users / rooms / messages / offline_messages ----------------
//
// 【所有】含用户输入的 SQL 一律 sqlite3_prepare_v2 + sqlite3_bind_*（见文件头 Q6，防注入）；
// 只有 DDL/PRAGMA 等固定串走 sqlite3_exec。一把互斥锁串行化全部语句（单连接 + WAL）。
//
// 表结构（题面要求的列一个不多一个不少；rooms.name 另加 UNIQUE 以便 /create 判重）：
//   users(id, username UNIQUE, salt, pwd_hash, created_at)
//   rooms(id, name, owner, created_at)
//   messages(id, room_id, sender, receiver, content, ts, type)
//       —— room_id: 房间消息=rooms.id，私聊=0；receiver: 房间消息='ALL'，私聊=目标用户名
//       —— type: 'room' | 'private'
//   offline_messages(id, sender, receiver, content, ts, delivered)
//       —— delivered: 0=待投递，1=已投递（上线推送后置位；行保留做对账，不再重复推）
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
    Database() : db_(NULL) {}
    ~Database() { close(); }

    bool open(const std::string& path) {
        if (sqlite3_open_v2(path.c_str(), &db_,
                            SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL) != SQLITE_OK) {
            std::cerr << "[错误] 打开 SQLite 失败: " << path << std::endl;
            return false;
        }
        exec("PRAGMA journal_mode=WAL;");
        exec("PRAGMA busy_timeout=3000;");
        // 两条索引各服务的查询见文件头 Q1（EXPLAIN QUERY PLAN 实测走索引，见 tools/seed_and_bench.py）
        const char* schema =
            "CREATE TABLE IF NOT EXISTS users ("
            "  id         INTEGER PRIMARY KEY AUTOINCREMENT,"
            "  username   TEXT NOT NULL UNIQUE,"  // 用户名唯一约束：重复注册由 DB 拦截 → E1001
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
            "CREATE INDEX IF NOT EXISTS idx_messages_receiver_ts ON messages (receiver, ts);";
        return exec(schema);
    }

    void close() {
        if (db_) { sqlite3_close_v2(db_); db_ = NULL; }
    }

    // ---------- 账号 ----------

    // 0=成功；1001=用户名已存在（UNIQUE 约束触发 SQLITE_CONSTRAINT）；-1=DB 失败
    int create_user(const std::string& name, const std::string& salt,
                    const std::string& pwd_hash, long long ts) {
        std::lock_guard<std::mutex> lk(m_);
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

    bool find_user(const std::string& name, UserRow& out) {
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
            out.username = col_text(st, 1);
            out.salt = col_text(st, 2);
            out.pwd_hash = col_text(st, 3);
            out.created_at = sqlite3_column_int64(st, 4);
            found = true;
        }
        sqlite3_finalize(st);
        return found;
    }

    // ---------- 房间 ----------

    // 0=成功；2001=房间已存在；-1=DB 失败
    int create_room(const std::string& name, const std::string& owner, long long ts) {
        std::lock_guard<std::mutex> lk(m_);
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

    // 取房间的 DB 主键（/create 后回填 RoomRuntime.db_id 用；0=不存在）
    long long find_room_id(const std::string& name) {
        std::lock_guard<std::mutex> lk(m_);
        sqlite3_stmt* st = NULL;
        if (sqlite3_prepare_v2(db_, "SELECT id FROM rooms WHERE name = ?;", -1, &st, NULL) !=
            SQLITE_OK)
            return 0;
        sqlite3_bind_text(st, 1, name.c_str(), -1, SQLITE_TRANSIENT);
        long long id = 0;
        if (sqlite3_step(st) == SQLITE_ROW) id = sqlite3_column_int64(st, 0);
        sqlite3_finalize(st);
        return id;
    }

    std::vector<RoomRow> list_rooms() {
        std::vector<RoomRow> rows;
        std::lock_guard<std::mutex> lk(m_);
        sqlite3_stmt* st = NULL;
        if (sqlite3_prepare_v2(db_,
                               "SELECT id, name, owner, created_at FROM rooms ORDER BY id ASC;",
                               -1, &st, NULL) != SQLITE_OK)
            return rows;
        while (sqlite3_step(st) == SQLITE_ROW) {
            RoomRow r;
            r.id = sqlite3_column_int64(st, 0);
            r.name = col_text(st, 1);
            r.owner = col_text(st, 2);
            r.created_at = sqlite3_column_int64(st, 3);
            rows.push_back(r);
        }
        sqlite3_finalize(st);
        return rows;
    }

    // ---------- 消息落库 ----------

    // 群聊：receiver='ALL'，type='room'。返回新行 id（失败 -1）
    long long insert_room_msg(long long room_id, const std::string& sender,
                              const std::string& content, long long ts) {
        std::lock_guard<std::mutex> lk(m_);
        sqlite3_stmt* st = NULL;
        if (sqlite3_prepare_v2(db_,
                               "INSERT INTO messages (room_id, sender, receiver, content, ts, type) "
                               "VALUES (?,?,'ALL',?,?,'room');",
                               -1, &st, NULL) != SQLITE_OK)
            return -1;
        sqlite3_bind_int64(st, 1, room_id);
        sqlite3_bind_text(st, 2, sender.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 3, content.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(st, 4, ts);
        int rc = sqlite3_step(st);
        long long id = (rc == SQLITE_DONE) ? sqlite3_last_id() : -1;
        sqlite3_finalize(st);
        return id;
    }

    // 私聊：room_id=0，type='private'
    long long insert_private_msg(const std::string& sender, const std::string& receiver,
                                 const std::string& content, long long ts) {
        std::lock_guard<std::mutex> lk(m_);
        sqlite3_stmt* st = NULL;
        if (sqlite3_prepare_v2(db_,
                               "INSERT INTO messages (room_id, sender, receiver, content, ts, type) "
                               "VALUES (0,?,?,?,?,'private');",
                               -1, &st, NULL) != SQLITE_OK)
            return -1;
        sqlite3_bind_text(st, 1, sender.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 2, receiver.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 3, content.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(st, 4, ts);
        int rc = sqlite3_step(st);
        long long id = (rc == SQLITE_DONE) ? sqlite3_last_id() : -1;
        sqlite3_finalize(st);
        return id;
    }

    void insert_offline(const std::string& sender, const std::string& receiver,
                        const std::string& content, long long ts) {
        std::lock_guard<std::mutex> lk(m_);
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

    std::vector<OffRow> undelivered(const std::string& receiver) {
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
            r.sender = col_text(st, 1);
            r.content = col_text(st, 2);
            r.ts = sqlite3_column_int64(st, 3);
            rows.push_back(r);
        }
        sqlite3_finalize(st);
        return rows;
    }

    // 单行置位 delivered=1（推送成功一条置一条；中途断线余下仍为 0，下次上线续推）
    void mark_delivered(long long id) {
        std::lock_guard<std::mutex> lk(m_);
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
    // EXPLAIN QUERY PLAN 各走 idx_messages_room_ts / idx_messages_receiver_ts（Q1）。
    // 调多取 1 行判断 has_more（LIMIT n+1），不用 COUNT(*)（那会多扫一遍索引）。

    std::vector<MsgRow> history_latest(long long room_id, int limit) {
        // 最近一页：WHERE room_id=? ORDER BY ts DESC, id DESC LIMIT ?
        std::lock_guard<std::mutex> lk(m_);
        sqlite3_stmt* st = NULL;
        if (sqlite3_prepare_v2(db_,
                               "SELECT id, sender, receiver, content, ts, type FROM messages "
                               "WHERE room_id = ? ORDER BY ts DESC, id DESC LIMIT ?;",
                               -1, &st, NULL) != SQLITE_OK)
            return std::vector<MsgRow>();
        sqlite3_bind_int64(st, 1, room_id);
        sqlite3_bind_int(st, 2, limit);
        std::vector<MsgRow> rows = step_rows(st);
        sqlite3_finalize(st);
        return rows;
    }

    std::vector<MsgRow> history_before(long long room_id, long long ts, long long id, int limit) {
        // 向上翻页：WHERE room_id=? AND (ts,id)<(?,?) ORDER BY ts DESC, id DESC LIMIT ?
        std::lock_guard<std::mutex> lk(m_);
        sqlite3_stmt* st = NULL;
        if (sqlite3_prepare_v2(db_,
                               "SELECT id, sender, receiver, content, ts, type FROM messages "
                               "WHERE room_id = ? AND (ts, id) < (?, ?) "
                               "ORDER BY ts DESC, id DESC LIMIT ?;",
                               -1, &st, NULL) != SQLITE_OK)
            return std::vector<MsgRow>();
        sqlite3_bind_int64(st, 1, room_id);
        sqlite3_bind_int64(st, 2, ts);
        sqlite3_bind_int64(st, 3, id);
        sqlite3_bind_int(st, 4, limit);
        std::vector<MsgRow> rows = step_rows(st);
        sqlite3_finalize(st);
        return rows;
    }

    std::vector<MsgRow> inbox_latest(const std::string& receiver, int limit) {
        std::lock_guard<std::mutex> lk(m_);
        sqlite3_stmt* st = NULL;
        if (sqlite3_prepare_v2(db_,
                               "SELECT id, sender, receiver, content, ts, type FROM messages "
                               "WHERE receiver = ? ORDER BY ts DESC, id DESC LIMIT ?;",
                               -1, &st, NULL) != SQLITE_OK)
            return std::vector<MsgRow>();
        sqlite3_bind_text(st, 1, receiver.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 2, limit);
        std::vector<MsgRow> rows = step_rows(st);
        sqlite3_finalize(st);
        return rows;
    }

    std::vector<MsgRow> inbox_before(const std::string& receiver, long long ts, long long id,
                                     int limit) {
        std::lock_guard<std::mutex> lk(m_);
        sqlite3_stmt* st = NULL;
        if (sqlite3_prepare_v2(db_,
                               "SELECT id, sender, receiver, content, ts, type FROM messages "
                               "WHERE receiver = ? AND (ts, id) < (?, ?) "
                               "ORDER BY ts DESC, id DESC LIMIT ?;",
                               -1, &st, NULL) != SQLITE_OK)
            return std::vector<MsgRow>();
        sqlite3_bind_text(st, 1, receiver.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(st, 2, ts);
        sqlite3_bind_int64(st, 3, id);
        sqlite3_bind_int(st, 4, limit);
        std::vector<MsgRow> rows = step_rows(st);
        sqlite3_finalize(st);
        return rows;
    }

private:
    bool exec(const char* sql) {
        char* err = NULL;
        int rc = sqlite3_exec(db_, sql, NULL, NULL, &err);
        if (rc != SQLITE_OK) {
            std::cerr << "[错误] SQL 失败: " << (err ? err : "?") << std::endl;
            return false;
        }
        return true;
    }

    static std::string col_text(sqlite3_stmt* st, int i) {
        const unsigned char* p = sqlite3_column_text(st, i);
        return p ? (const char*)p : "";
    }

    long long sqlite3_last_id() {
        sqlite3_stmt* st = NULL;
        long long id = -1;
        if (sqlite3_prepare_v2(db_, "SELECT last_insert_rowid();", -1, &st, NULL) == SQLITE_OK) {
            if (sqlite3_step(st) == SQLITE_ROW) id = sqlite3_column_int64(st, 0);
            sqlite3_finalize(st);
        }
        return id;
    }

    static std::vector<MsgRow> step_rows(sqlite3_stmt* st) {
        std::vector<MsgRow> rows;
        while (sqlite3_step(st) == SQLITE_ROW) {
            MsgRow r;
            r.id = sqlite3_column_int64(st, 0);
            r.sender = col_text(st, 1);
            r.receiver = col_text(st, 2);
            r.content = col_text(st, 3);
            r.ts = sqlite3_column_int64(st, 4);
            r.type = col_text(st, 5);
            rows.push_back(r);
        }
        return rows;
    }

    sqlite3* db_;
    std::mutex m_;
};

// ---------------- Token 表（内存态，见文件头 Q4） ----------------

class TokenBook {
public:
    explicit TokenBook(long long ttl_sec) : ttl_sec_(ttl_sec) {}

    // 32 字节 CSPRNG + 过期时间 → hex token
    std::string issue(const std::string& user, long long* exp_out) {
        std::string token = pwd_hash::random_hex(32);
        long long exp = now_ts() + ttl_sec_;
        std::lock_guard<std::mutex> lk(m_);
        tokens_[token].user = user;
        tokens_[token].exp = exp;
        if (tokens_.size() > 100000) gc_locked();
        if (exp_out) *exp_out = exp;
        return token;
    }

    bool validate(const std::string& token, std::string& user_out) {
        std::lock_guard<std::mutex> lk(m_);
        std::unordered_map<std::string, Token>::iterator it = tokens_.find(token);
        if (it == tokens_.end()) return false;
        if (it->second.exp < now_ts()) {
            tokens_.erase(it);  // 过期即删
            return false;
        }
        user_out = it->second.user;
        return true;
    }

private:
    struct Token {
        std::string user;
        long long exp;
    };
    void gc_locked() {
        long long now = now_ts();
        for (std::unordered_map<std::string, Token>::iterator it = tokens_.begin();
             it != tokens_.end();) {
            if (it->second.exp < now) it = tokens_.erase(it);
            else ++it;
        }
    }
    long long ttl_sec_;
    std::mutex m_;
    std::unordered_map<std::string, Token> tokens_;
};

// ---------------- 连接对象与服务器 ----------------
//
// fd 生命周期纪律与 v4 相同：收线程唯一最终 close；踢人只摘表 + shutdown；send/close 持 send_m。
struct ClientConn {
    socket_t fd;
    uint64_t id;
    std::string peer;
    FrameReader reader;
    std::mutex send_m;
    std::atomic<long long> last_active;
    std::string username;          // 认证成功后写一次（会话身份，防伪造 from）
    std::atomic<bool> authed;
    long long send_seq;
    ClientConn() : fd(-1), id(0), last_active(0), authed(false), send_seq(0) {}
};
typedef std::shared_ptr<ClientConn> ConnPtr;

class ChatServer {
public:
    ChatServer(int port, const std::string& db_path, long long idle_ms, long long scan_ms,
               long long token_ttl)
        : listen_fd_(-1), port_(port), db_path_(db_path), idle_ms_(idle_ms), scan_ms_(scan_ms),
          tokens_(token_ttl), next_id_(0), running_(false) {
#ifdef _WIN32
        listen_fd_ = INVALID_SOCKET;
#endif
    }

    bool start() {
        if (!db_.open(db_path_)) return false;
        if (!loadRooms()) return false;
        if (!startListen()) return false;
        running_ = true;
        monitor_ = std::thread(&ChatServer::monitorLoop, this);

        std::cout << "==========================================" << std::endl;
        std::cout << "聊天服务器 v5 已启动（账号 + 房间 + 持久化/游标分页）" << std::endl;
        std::cout << "监听端口: " << port_ << "（0.0.0.0），MAX_FRAME=1MiB" << std::endl;
        std::cout << "口令哈希: " << pwd_hash::backend_name() << std::endl;
        std::cout << "持久化: " << db_path_
                  << "（users/rooms/messages/offline_messages + 2 条索引）" << std::endl;
        std::cout << "房间: 默认 lobby，广播按 room->members 路由（O(房间成员)）" << std::endl;
        std::cout << "历史: 进房拉最近 50 条，(ts,id) 游标向上翻页（不用 OFFSET）" << std::endl;
        std::cout << "==========================================" << std::endl;

        acceptLoop();
        shutdownAll();
        return true;
    }

    void requestStop() {
        running_ = false;
        if (sock_valid(listen_fd_)) sock_shutdown(listen_fd_);
    }

private:
    // ---------- 房间注册表（内存态：room → 成员集合；成员按用户名，天然支持顶号续传） ----------
    struct RoomRuntime {
        long long db_id;
        std::string name;
        std::string owner;
        long long created_at;
        std::set<std::string> members;  // 当前在本房间的用户名（Q2：广播只遍历这里）
    };

    bool loadRooms() {
        std::vector<RoomRow> rows = db_.list_rooms();
        {
            std::lock_guard<std::mutex> lk(g_m_);
            for (size_t i = 0; i < rows.size(); ++i) {
                RoomRuntime rt;
                rt.db_id = rows[i].id;
                rt.name = rows[i].name;
                rt.owner = rows[i].owner;
                rt.created_at = rows[i].created_at;
                rooms_[rt.name] = rt;
            }
        }
        // 默认房间 lobby：不存在则创建（owner=SERVER）
        if (!roomExists("lobby")) {
            int rc = db_.create_room("lobby", "SERVER", now_ts());
            if (rc != 0 && rc != kErrRoomExists) {
                std::cerr << "[错误] 创建默认房间 lobby 失败" << std::endl;
                return false;
            }
            std::vector<RoomRow> again = db_.list_rooms();
            std::lock_guard<std::mutex> lk(g_m_);
            for (size_t i = 0; i < again.size(); ++i) {
                if (rooms_.count(again[i].name)) continue;
                RoomRuntime rt;
                rt.db_id = again[i].id;
                rt.name = again[i].name;
                rt.owner = again[i].owner;
                rt.created_at = again[i].created_at;
                rooms_[rt.name] = rt;
            }
        }
        return true;
    }

    bool roomExists(const std::string& name) {
        std::lock_guard<std::mutex> lk(g_m_);
        return rooms_.count(name) != 0;
    }

    bool startListen() {
#ifdef _WIN32
        listen_fd_ = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
#else
        listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
#endif
        if (!sock_valid(listen_fd_)) { std::cerr << "创建 socket 失败" << std::endl; return false; }
        int opt = 1;
        setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, (const char*)&opt, sizeof(opt));
        sockaddr_in addr;
        std::memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons((unsigned short)port_);
        if (bind(listen_fd_, (sockaddr*)&addr, sizeof(addr)) < 0) {
            std::cerr << "绑定端口 " << port_ << " 失败（是否被占用？）" << std::endl;
            return false;
        }
        if (listen(listen_fd_, 128) < 0) { std::cerr << "监听失败" << std::endl; return false; }
        return true;
    }

    void acceptLoop() {
        while (running_) {
            sockaddr_in ca;
            socklen_t cl = sizeof(ca);
            socket_t cfd = accept(listen_fd_, (sockaddr*)&ca, &cl);
            if (!sock_valid(cfd)) {
                if (!running_) break;
                if (sock_interrupted()) continue;
                std::cerr << "[错误] accept 失败" << std::endl;
                continue;
            }
            apply_socket_opts(cfd);
            ConnPtr c(new ClientConn());
            {
                std::lock_guard<std::mutex> lk(g_m_);
                c->id = ++next_id_;
                c->fd = cfd;
                c->peer = std::string(inet_ntoa(ca.sin_addr)) + ":" + std::to_string(ntohs(ca.sin_port));
                c->last_active = now_ms();
                conns_[c->id] = c;
            }
            std::cout << "[连接] " << c->peer << " id=" << c->id << "（当前 " << connCount()
                      << " 路）" << std::endl;
            std::thread(&ChatServer::handleClient, this, c).detach();
        }
    }

    // ---------- 收发 ----------

    void handleClient(ConnPtr c) {
        char chunk[4096];
        while (running_) {
            long n = sock_recv(c->fd, chunk, sizeof(chunk));
            if (n > 0) {
                c->last_active = now_ms();
                c->reader.feed(chunk, (size_t)n);
                std::string body;
                while (c->reader.next(body)) {
                    if (handleBody(c, body)) {
                        dropConn(c, "退出");
                        closeConn(c);
                        return;
                    }
                }
                if (c->reader.failed()) {
                    std::cout << "[协议错误] id=" << c->id << " 帧超限/坏帧，断开" << std::endl;
                    break;
                }
            } else if (n == 0) {
                std::cout << "[断开] " << c->peer << " 对端关闭" << std::endl;
                break;
            } else {
                if (sock_interrupted()) continue;
                std::cout << "[断开] " << c->peer << " recv 出错" << std::endl;
                break;
            }
        }
        dropConn(c, "掉线");
        closeConn(c);
    }

    // 返回 true = 本连接收尾退出（LOGOUT）
    bool handleBody(const ConnPtr& c, const std::string& body) {
        minijson::Object obj;
        if (!minijson::parse(body, obj)) {
            std::cout << "[错误] JSON 解析失败（id=" << c->id << "）" << std::endl;
            return false;
        }
        std::string type = obj.get_str("type");
        if (type == "PING") {
            minijson::Object pong;
            pong.set_num("ver", 1);
            pong.set_str("type", "PONG");
            pong.set_str("from", "SERVER");
            pong.set_str("to", c->username);
            pong.set_str("room", "");
            pong.set_str("content", obj.get_str("content"));
            pong.set_num("ts", now_ts());
            sendFrame(c, pong);
        } else if (type == "REGISTER") {
            doRegister(c, obj);
        } else if (type == "LOGIN") {
            doLogin(c, obj);
        } else if (type == "AUTH") {
            doAuth(c, obj);
        } else if (type == "LOGOUT") {
            std::cout << "[退出] " << userName(c) << " 主动 LOGOUT" << std::endl;
            dropConn(c, "退出");
            return true;
        } else if (!c->authed) {
            // 业务帧一律要求先认证（Q4：连接是信任边界）
            sendErr(c, kErrNotAuth, "尚未登录，请先 LOGIN/AUTH 认证");
        } else if (type == "JOIN") {
            doJoin(c, obj);
        } else if (type == "LEAVE") {
            doLeave(c);
        } else if (type == "ROOMS") {
            doRooms(c);
        } else if (type == "CREATE") {
            doCreate(c, obj);
        } else if (type == "HIST") {
            doHist(c, obj);
        } else if (type == "INBOX") {
            doInbox(c, obj);
        } else if (type == "MESSAGE") {
            doMessage(c, obj);
        } else {
            std::cout << "[警告] 未处理类型: " << type << "（id=" << c->id << "）" << std::endl;
        }
        return false;
    }

    bool sendLocked(const ConnPtr& c, const std::string& pkt) {
        if (c->fd < 0) return false;
        size_t sent = 0;
        int retries = 0;
        while (sent < pkt.size()) {
            long n = sock_send(c->fd, pkt.data() + sent, pkt.size() - sent);
            if (n > 0) { sent += (size_t)n; continue; }
            if (n < 0 && sock_interrupted()) continue;
            if (n < 0 && sock_would_block()) {
                if (++retries > kSendRetry) return false;
                continue;
            }
            return false;
        }
        return true;
    }

    bool sendFrame(const ConnPtr& c, minijson::Object obj) {
        std::lock_guard<std::mutex> lk(c->send_m);
        if (c->fd < 0) return false;
        obj.set_num("seq", ++c->send_seq);
        std::string body = minijson::serialize(obj);
        if (body.size() > kMaxFrame) return false;
        return sendLocked(c, make_wire_frame(body));
    }

    // 转发聊天 MESSAGE：seq 用发送方原始 seq（客户端按 (from,seq) 显示去重）
    bool sendRelay(const ConnPtr& c, minijson::Object obj, long long origin_seq) {
        std::lock_guard<std::mutex> lk(c->send_m);
        if (c->fd < 0) return false;
        obj.set_num("seq", origin_seq);
        std::string body = minijson::serialize(obj);
        if (body.size() > kMaxFrame) return false;
        return sendLocked(c, make_wire_frame(body));
    }

    void kickConn(const ConnPtr& c, const std::string& reason, bool notify_room) {
        dropConn(c, reason, notify_room);
        std::lock_guard<std::mutex> lk(c->send_m);
        if (c->fd >= 0) sock_shutdown(c->fd);
    }

    // ---------- 基础校验 ----------

    static bool validUsername(const std::string& u) {
        if (u.empty() || u.size() > 32) return false;
        if (u == "SERVER" || u == "ALL") return false;
        for (size_t i = 0; i < u.size(); ++i)
            if ((unsigned char)u[i] < 0x20) return false;
        return true;
    }

    static bool validRoomName(const std::string& r) {
        if (r.empty() || r.size() > 32) return false;
        if (r == "SERVER" || r == "ALL") return false;
        for (size_t i = 0; i < r.size(); ++i)
            if ((unsigned char)r[i] < 0x20) return false;
        return true;
    }

    static bool validPassword(const std::string& p) {
        return !p.empty() && p.size() <= 128;
    }

    std::string userName(const ConnPtr& c) {
        return c->authed ? c->username : ("?" + std::to_string(c->id));
    }

    void sendErr(const ConnPtr& c, int code, const std::string& text) {
        minijson::Object err;
        err.set_num("ver", 1);
        err.set_str("type", "ERR");
        err.set_str("from", "SERVER");
        err.set_str("to", c->username);
        err.set_str("room", "");
        err.set_num("code", code);         // 错误码
        err.set_str("content", text);      // 中文文案（不静默失败）
        err.set_num("ts", now_ts());
        sendFrame(c, err);
        std::cout << "[ERR] -> " << userName(c) << " code=" << code << " " << text << std::endl;
    }

    void sendAck(const ConnPtr& c, long long client_seq) {
        minijson::Object ack;
        ack.set_num("ver", 1);
        ack.set_str("type", "ACK");
        ack.set_str("from", "SERVER");
        ack.set_str("to", c->username);
        ack.set_str("room", "");
        ack.set_str("content", std::to_string(client_seq));
        ack.set_num("ts", now_ts());
        if (!sendFrame(c, ack)) std::cout << "[警告] ACK 发送失败（id=" << c->id << "）" << std::endl;
    }

    void sendNack(const ConnPtr& c, long long client_seq, const std::string& reason) {
        minijson::Object nack;
        nack.set_num("ver", 1);
        nack.set_str("type", "NACK");
        nack.set_str("from", "SERVER");
        nack.set_str("to", c->username);
        nack.set_str("room", "");
        nack.set_str("content", std::to_string(client_seq));
        nack.set_str("reason", reason);
        nack.set_num("ts", now_ts());
        sendFrame(c, nack);
        std::cout << "[NACK] -> " << userName(c) << " seq=" << client_seq << " reason=" << reason
                  << std::endl;
    }

    // ---------- 认证三入口（Q4/Q5） ----------

    void doRegister(const ConnPtr& c, minijson::Object& obj) {
        if (c->authed) {
            sendErr(c, kErrAlreadyAuth, "本连接已登录，请勿重复登录/注册");
            return;
        }
        std::string name = obj.get_str("from");
        std::string pwd = obj.get_str("content");
        if (!validUsername(name) || !validPassword(pwd)) {
            sendErr(c, kErrBadCredFmt, "用户名或密码格式非法（用户名 1-32 字符非保留字，密码 1-128 字符）");
            return;
        }
        // salt(16 字节随机) + PBKDF2-HMAC-SHA256(100000)（pwd_hash.h）
        std::string salt = pwd_hash::random_salt_hex();
        std::string hash = pwd_hash::hash_password(pwd, salt);
        if (hash.empty()) {
            sendErr(c, kErrDb, "口令哈希失败");
            return;
        }
        int rc = db_.create_user(name, salt, hash, now_ts());
        if (rc == kErrDupUser) {
            // 用户名唯一约束生效 → 明确错误码 + 中文文案，绝不静默失败
            sendErr(c, kErrDupUser, "注册失败：用户名已存在，请更换用户名");
            return;
        }
        if (rc != 0) {
            sendErr(c, kErrDb, "注册失败：数据库错误");
            return;
        }
        std::cout << "[注册] " << name << "（id=" << c->id << "）成功" << std::endl;
        minijson::Object ok;
        ok.set_num("ver", 1);
        ok.set_str("type", "REGISTER_OK");
        ok.set_str("from", "SERVER");
        ok.set_str("to", name);
        ok.set_str("room", "");
        ok.set_str("content", "注册成功，请登录");
        ok.set_num("ts", now_ts());
        sendFrame(c, ok);
    }

    // 共享的「会话建立」收尾：顶号/入座/补离线/通知
    void finishAuth(const ConnPtr& c, const std::string& name, const std::string& token,
                    long long token_exp, bool takeover, const ConnPtr& old) {
        minijson::Object ok;
        ok.set_num("ver", 1);
        ok.set_str("type", "AUTH_OK");
        ok.set_str("from", "SERVER");
        ok.set_str("to", name);
        ok.set_str("room", userRoom(name));
        ok.set_str("content", token);      // Q4：32 字节随机 Token（hex 64 字符）
        ok.set_num("exp", token_exp);      // 过期时间（Unix 秒）
        ok.set_num("ts", now_ts());
        sendFrame(c, ok);

        pushOffline(c, name);

        std::string room = userRoom(name);
        if (takeover) {
            // 顶号（Token 重连）：用户在别人眼里没离开过（成员集按用户名，v4 Q4 论证）
            std::cout << "[顶号] " << name << " 新连接 id=" << c->id << " 顶替旧连接 id=" << old->id
                      << std::endl;
            // 明确通知旧连接再踢（Q5：不静默失败）——旧连接先收到说明，随后被 shutdown
            minijson::Object sys;
            sys.set_num("ver", 1);
            sys.set_str("type", "SYSTEM");
            sys.set_str("from", "SERVER");
            sys.set_str("to", name);
            sys.set_str("room", room);
            sys.set_str("content", "该账号已在其他连接恢复会话（Token 重连顶号），本连接即将关闭");
            sys.set_num("ts", now_ts());
            sendFrame(old, sys);
            kickConn(old, "被顶替", false);  // 不广播、不摘房间成员
            if (!room.empty()) sendJoinState(c, room);  // 会话续传：把房间状态同步给新连接
        } else if (!room.empty()) {
            sendJoinState(c, room);  // 防御路径：正常登录不应有残留房间
        }
    }

    void doLogin(const ConnPtr& c, minijson::Object& obj) {
        if (c->authed) {
            sendErr(c, kErrAlreadyAuth, "本连接已登录，请勿重复登录");
            return;
        }
        std::string name = obj.get_str("from");
        std::string pwd = obj.get_str("content");
        if (!validUsername(name) || !validPassword(pwd)) {
            sendErr(c, kErrBadCredFmt, "用户名或密码格式非法");
            return;
        }
        UserRow u;
        if (!db_.find_user(name, u)) {
            sendErr(c, kErrNoSuchUser, "登录失败：用户不存在，请先注册");
            return;
        }
        if (!pwd_hash::verify(pwd, u.salt, u.pwd_hash)) {
            // 校验失败不区分「用户不存在/密码错误」的时间侧信道已在 verify 恒定时间比较覆盖；
            // 这里仍分开报错（教学友好），生产可合并为「用户名或密码错误」防枚举
            sendErr(c, kErrBadPass, "登录失败：密码错误，请重新输入");
            return;
        }
        // Q5：口令登录时账号已在线 → 显式拒绝重复登录（不静默、不顶号）
        ConnPtr old;
        {
            std::lock_guard<std::mutex> lk(g_m_);
            std::unordered_map<std::string, ConnPtr>::iterator it = online_.find(name);
            if (it != online_.end()) old = it->second;
        }
        if (old) {
            sendErr(c, kErrDupLogin,
                    "重复登录：该账号已在线，不允许重复登录（断线重连请走 Token/AUTH 免密恢复）");
            return;
        }
        long long exp = 0;
        std::string token = tokens_.issue(name, &exp);
        {
            std::lock_guard<std::mutex> lk(g_m_);
            c->username = name;
            c->authed = true;
            online_[name] = c;
        }
        std::cout << "[登录] " << name << "（id=" << c->id << "）口令登录成功" << std::endl;
        finishAuth(c, name, token, exp, false, ConnPtr());
    }

    void doAuth(const ConnPtr& c, minijson::Object& obj) {
        if (c->authed) {
            sendErr(c, kErrAlreadyAuth, "本连接已登录，请勿重复认证");
            return;
        }
        std::string token = obj.get_str("content");
        std::string name;
        if (token.empty() || !tokens_.validate(token, name)) {
            sendErr(c, kErrBadToken, "Token 无效或已过期，请重新登录");
            return;
        }
        ConnPtr old;
        bool takeover = false;
        {
            std::lock_guard<std::mutex> lk(g_m_);
            std::unordered_map<std::string, ConnPtr>::iterator it = online_.find(name);
            if (it != online_.end() && it->second.get() != c.get()) {
                old = it->second;
                takeover = true;  // Q5：Token=同一会话续传 → 顶号（防被自己的僵尸连接锁门）
            }
            c->username = name;
            c->authed = true;
            online_[name] = c;
        }
        long long exp = 0;
        std::string new_token = tokens_.issue(name, &exp);  // 每次恢复换新 Token（旧的自然过期）
        std::cout << "[认证] " << name << "（id=" << c->id << "）Token 恢复会话"
                  << (takeover ? "（顶号）" : "") << std::endl;
        finishAuth(c, name, new_token, exp, takeover, old);
    }

    // ---------- 房间业务（Q2） ----------

    std::string userRoom(const std::string& name) {
        std::lock_guard<std::mutex> lk(g_m_);
        std::unordered_map<std::string, std::string>::iterator it = user_room_.find(name);
        return it == user_room_.end() ? "" : it->second;
    }

    void doJoin(const ConnPtr& c, minijson::Object& obj) {
        std::string room = obj.get_str("content");
        if (!validRoomName(room)) {
            sendErr(c, kErrBadRoomName, "房间名非法（1-32 字符，非保留字）");
            return;
        }
        bool exists = false;
        {
            std::lock_guard<std::mutex> lk(g_m_);
            exists = rooms_.count(room) != 0;
        }
        if (!exists) {
            sendErr(c, kErrNoSuchRoom, "加入失败：房间不存在，可用 /create 创建");
            return;
        }
        std::string name = c->username;
        std::string prev = userRoom(name);
        if (prev == room) {
            sendJoinState(c, room);  // 幂等：重复 JOIN 回当前状态
            return;
        }
        if (!prev.empty()) leaveRoomNotify(c, name, prev);  // 换房先退旧房
        {
            std::lock_guard<std::mutex> lk(g_m_);
            rooms_[room].members.insert(name);
            user_room_[name] = room;
        }
        std::cout << "[加入] " << name << " -> #" << room << std::endl;
        sendJoinState(c, room);
        broadcastRoomSystem(room, name + " 进入了房间", c);
        broadcastRoomUserlist(room);
    }

    void doLeave(const ConnPtr& c) {
        std::string name = c->username;
        std::string room = userRoom(name);
        if (room.empty()) {
            sendErr(c, kErrNotInRoom, "尚未加入任何房间，无法 /leave");
            return;
        }
        leaveRoomNotify(c, name, room);
        minijson::Object ok;
        ok.set_num("ver", 1);
        ok.set_str("type", "LEAVE_OK");
        ok.set_str("from", "SERVER");
        ok.set_str("to", name);
        ok.set_str("room", room);
        ok.set_str("content", "已离开房间 " + room);
        ok.set_num("ts", now_ts());
        sendFrame(c, ok);
    }

    void leaveRoomNotify(const ConnPtr& c, const std::string& name, const std::string& room) {
        {
            std::lock_guard<std::mutex> lk(g_m_);
            std::map<std::string, RoomRuntime>::iterator it = rooms_.find(room);
            if (it != rooms_.end()) it->second.members.erase(name);
            std::unordered_map<std::string, std::string>::iterator ur = user_room_.find(name);
            if (ur != user_room_.end() && ur->second == room) user_room_.erase(ur);
        }
        std::cout << "[离开] " << name << " <- #" << room << std::endl;
        broadcastRoomSystem(room, name + " 离开了房间", ConnPtr());
        broadcastRoomUserlist(room);
        (void)c;
    }

    // JOIN_OK：房间 + 成员名单（客户端据此刷 UI，然后自己拉 HIST）
    void sendJoinState(const ConnPtr& c, const std::string& room) {
        minijson::Object ok;
        ok.set_num("ver", 1);
        ok.set_str("type", "JOIN_OK");
        ok.set_str("from", "SERVER");
        ok.set_str("to", c->username);
        ok.set_str("room", room);
        ok.set_list("content", roomMembers(room));
        ok.set_num("ts", now_ts());
        sendFrame(c, ok);
    }

    std::vector<std::string> roomMembers(const std::string& room) {
        std::vector<std::string> v;
        std::lock_guard<std::mutex> lk(g_m_);
        std::map<std::string, RoomRuntime>::iterator it = rooms_.find(room);
        if (it != rooms_.end())
            for (std::set<std::string>::iterator m = it->second.members.begin();
                 m != it->second.members.end(); ++m) v.push_back(*m);
        return v;
    }

    void doRooms(const ConnPtr& c) {
        // 在线人数从内存成员集取；房间清单从 DB（含无人房间）
        std::vector<minijson::Object> items;
        {
            std::lock_guard<std::mutex> lk(g_m_);
            for (std::map<std::string, RoomRuntime>::iterator it = rooms_.begin();
                 it != rooms_.end(); ++it) {
                minijson::Object r;
                r.set_str("name", it->second.name);
                r.set_str("owner", it->second.owner);
                r.set_num("members", (long long)it->second.members.size());
                r.set_num("created_at", it->second.created_at);
                items.push_back(r);
            }
        }
        minijson::Object lst;
        lst.set_num("ver", 1);
        lst.set_str("type", "ROOMS_LIST");
        lst.set_str("from", "SERVER");
        lst.set_str("to", c->username);
        lst.set_str("room", "");
        lst.set_objs("content", items);
        lst.set_num("ts", now_ts());
        sendFrame(c, lst);
    }

    void doCreate(const ConnPtr& c, minijson::Object& obj) {
        std::string room = obj.get_str("content");
        if (!validRoomName(room)) {
            sendErr(c, kErrBadRoomName, "房间名非法（1-32 字符，非保留字）");
            return;
        }
        int rc = db_.create_room(room, c->username, now_ts());
        if (rc == kErrRoomExists) {
            sendErr(c, kErrRoomExists, "创建失败：房间已存在");
            return;
        }
        if (rc != 0) {
            sendErr(c, kErrDb, "创建失败：数据库错误");
            return;
        }
        long long db_id = db_.find_room_id(room);  // 回填真实主键（消息落库 room_id 需要）
        {
            std::lock_guard<std::mutex> lk(g_m_);
            RoomRuntime rt;
            rt.db_id = db_id;
            rt.name = room;
            rt.owner = c->username;
            rt.created_at = now_ts();
            rooms_[room] = rt;
        }
        std::cout << "[创建] " << c->username << " 创建房间 #" << room << std::endl;
        minijson::Object ok;
        ok.set_num("ver", 1);
        ok.set_str("type", "CREATE_OK");
        ok.set_str("from", "SERVER");
        ok.set_str("to", c->username);
        ok.set_str("room", room);
        ok.set_str("content", "房间 " + room + " 创建成功，用 /join " + room + " 加入");
        ok.set_num("ts", now_ts());
        sendFrame(c, ok);
    }

    // ---------- 历史分页（Q3） ----------

    // 游标："ts:id"（上一页最老一行）；空串 = 最近一页。返回 false = 游标非法
    static bool parseCursor(const std::string& cur, long long& ts, long long& id) {
        if (cur.empty()) return false;
        size_t p = cur.find(':');
        if (p == std::string::npos || p == 0 || p + 1 >= cur.size()) return false;
        ts = std::atoll(cur.substr(0, p).c_str());
        id = std::atoll(cur.substr(p + 1).c_str());
        return ts > 0 && id > 0;
    }

    // rows：ts DESC, id DESC（新→老），已按 kHistPageSize 截断；more=调用方多取第 n+1 行的判断
    void sendHistPage(const ConnPtr& c, const std::string& kind, const std::string& room,
                      std::vector<MsgRow> rows, bool more) {
        // 转成旧→新展示序（客户端直接顺序渲染/前插）
        std::vector<minijson::Object> items;
        std::string cursor;
        for (size_t i = rows.size(); i-- > 0;) {
            minijson::Object m;
            m.set_num("id", rows[i].id);
            m.set_str("from", rows[i].sender);
            m.set_str("to", rows[i].receiver);
            m.set_str("content", rows[i].content);
            m.set_num("ts", rows[i].ts);
            m.set_str("type", rows[i].type);
            items.push_back(m);
        }
        if (!rows.empty())
            cursor = std::to_string(rows.back().ts) + ":" + std::to_string(rows.back().id);
        minijson::Object page;
        page.set_num("ver", 1);
        page.set_str("type", kind);  // HISTORY / INBOX
        page.set_str("from", "SERVER");
        page.set_str("to", c->username);
        page.set_str("room", room);
        page.set_objs("content", items);
        page.set_str("cursor", cursor);  // 下一页游标（本页最老一行）；空 = 到头
        page.set_num("more", more ? 1 : 0);
        page.set_num("ts", now_ts());
        sendFrame(c, page);
    }

    void doHist(const ConnPtr& c, minijson::Object& obj) {
        std::string room = userRoom(c->username);
        if (room.empty()) {
            sendErr(c, kErrNotInRoom, "尚未加入房间，无法拉取历史（/join 后自动拉最近 50 条）");
            return;
        }
        long long room_id = 0;
        {
            std::lock_guard<std::mutex> lk(g_m_);
            std::map<std::string, RoomRuntime>::iterator it = rooms_.find(room);
            if (it != rooms_.end()) room_id = it->second.db_id;
        }
        if (room_id <= 0) {
            sendErr(c, kErrDb, "房间数据异常");
            return;
        }
        std::string cur = obj.get_str("content");
        std::vector<MsgRow> rows;
        if (cur.empty()) {
            rows = db_.history_latest(room_id, kHistPageSize + 1);  // Q3：多取 1 行判 has_more
        } else {
            long long cts = 0, cid = 0;
            if (!parseCursor(cur, cts, cid)) {
                sendErr(c, kErrBadCursor, "历史游标非法（应为 ts:id）");
                return;
            }
            rows = db_.history_before(room_id, cts, cid, kHistPageSize + 1);
        }
        bool more = rows.size() > (size_t)kHistPageSize;
        if (more) rows.resize(kHistPageSize);
        sendHistPage(c, "HISTORY", room, rows, more);
    }

    void doInbox(const ConnPtr& c, minijson::Object& obj) {
        std::string cur = obj.get_str("content");
        std::vector<MsgRow> rows;
        if (cur.empty()) {
            rows = db_.inbox_latest(c->username, kHistPageSize + 1);
        } else {
            long long cts = 0, cid = 0;
            if (!parseCursor(cur, cts, cid)) {
                sendErr(c, kErrBadCursor, "收件箱游标非法（应为 ts:id）");
                return;
            }
            rows = db_.inbox_before(c->username, cts, cid, kHistPageSize + 1);
        }
        bool more = rows.size() > (size_t)kHistPageSize;
        if (more) rows.resize(kHistPageSize);
        sendHistPage(c, "INBOX", "", rows, more);
    }

    // ---------- 消息 ----------

    void doMessage(const ConnPtr& c, minijson::Object& obj) {
        const std::string from = c->username;  // 防伪：以会话身份为准，忽略帧内 from
        long long seq = obj.get_num("seq");
        long long ts = obj.get_num("ts");
        if (ts <= 0) ts = now_ts();
        if (seq <= 0) {
            sendNack(c, seq, "seq 必须为正整数（去重键）");
            return;
        }
        std::string to = obj.get_str("to");
        std::string content = obj.get_str("content");

        // 内存 (user, seq) 去重窗口：重发/ACK 丢失重传幂等（v4 的 seen_message 落盘去重
        // 与本版 4 表 schema 不冲突但未列入——重启后窗口清零，客户端 (from,seq) 显示去重兜底）
        std::string dedup_key = from + ":" + std::to_string(seq);
        {
            std::lock_guard<std::mutex> lk(g_m_);
            if (recent_seen_.count(dedup_key)) {
                std::cout << "[去重] " << from << " seq=" << seq << " 重复帧，仅回 ACK" << std::endl;
                sendAck(c, seq);
                return;
            }
            if (recent_seen_.size() > 500000) recent_seen_.clear();
            recent_seen_[dedup_key] = ts;
        }

        if (to == "ALL") {
            // ---- 群聊 → 当前房间 ----
            std::string room = userRoom(from);
            if (room.empty()) {
                sendNack(c, seq, "尚未加入房间，无法群聊（/join 后再发）");
                return;
            }
            long long room_id = 0;
            {
                std::lock_guard<std::mutex> lk(g_m_);
                std::map<std::string, RoomRuntime>::iterator it = rooms_.find(room);
                if (it != rooms_.end()) room_id = it->second.db_id;
            }
            long long id = db_.insert_room_msg(room_id, from, content, ts);
            if (id < 0) {
                sendNack(c, seq, "消息落库失败");
                return;
            }
            std::cout << "[消息] #" << room << " " << from << ": " << content << std::endl;
            minijson::Object m;
            m.set_num("ver", 1);
            m.set_str("type", "MESSAGE");
            m.set_str("from", from);
            m.set_str("to", "ALL");
            m.set_str("room", room);
            m.set_str("content", content);
            m.set_num("ts", ts);
            broadcastRoomRelay(room, m, seq);  // Q2：只发本房间成员（含回显）
            sendAck(c, seq);
            return;
        }

        // ---- 私聊（store-and-forward：messages 落历史 + 不在线进 offline_messages）----
        if (to.empty() || to == "SERVER" || !validUsername(to)) {
            sendNack(c, seq, "接收者非法");
            return;
        }
        UserRow victim;
        if (!db_.find_user(to, victim)) {
            sendNack(c, seq, "用户不存在：" + to);
            return;
        }
        if (db_.insert_private_msg(from, to, content, ts) < 0) {
            sendNack(c, seq, "消息落库失败");
            return;
        }
        std::cout << "[私聊] " << from << " -> " << to << ": " << content << std::endl;
        sendAck(c, seq);  // 已送达 = 已持久化（历史 + 必要时离线行）

        ConnPtr target;
        {
            std::lock_guard<std::mutex> lk(g_m_);
            std::unordered_map<std::string, ConnPtr>::iterator it = online_.find(to);
            if (it != online_.end()) target = it->second;
        }
        minijson::Object m;
        m.set_num("ver", 1);
        m.set_str("type", "MESSAGE");
        m.set_str("from", from);
        m.set_str("to", to);
        m.set_str("room", "");
        m.set_str("content", content);
        m.set_num("ts", ts);
        if (target && sendRelay(target, m, seq)) return;  // 在线尽力即时投递

        // 不在线（或即时投递失败）→ 离线行，receiver 上线时按 ts 补发并置 delivered=1
        db_.insert_offline(from, to, content, ts);
        std::cout << "[离线存储] " << from << " -> " << to << " 已入库（待投递）" << std::endl;
        if (target) kickConn(target, "掉线", true);
    }

    // ---------- 广播（Q2：O(房间成员) 而不是 O(全在线)） ----------

    void broadcastRoomSystem(const std::string& room, const std::string& text,
                             const ConnPtr& exclude) {
        minijson::Object sys;
        sys.set_num("ver", 1);
        sys.set_str("type", "SYSTEM");
        sys.set_str("from", "SERVER");
        sys.set_str("to", "ALL");
        sys.set_str("room", room);
        sys.set_str("content", text);
        sys.set_num("ts", now_ts());
        std::vector<ConnPtr> targets = roomConns(room);
        for (size_t i = 0; i < targets.size(); ++i)
            if (targets[i].get() != exclude.get()) sendFrame(targets[i], sys);
    }

    void broadcastRoomUserlist(const std::string& room) {
        minijson::Object ul;
        ul.set_num("ver", 1);
        ul.set_str("type", "USERLIST");
        ul.set_str("from", "SERVER");
        ul.set_str("to", "ALL");
        ul.set_str("room", room);
        ul.set_list("content", roomMembers(room));
        ul.set_num("ts", now_ts());
        std::vector<ConnPtr> targets = roomConns(room);
        for (size_t i = 0; i < targets.size(); ++i) sendFrame(targets[i], ul);
    }

    void broadcastRoomRelay(const std::string& room, minijson::Object& m, long long origin_seq) {
        std::vector<ConnPtr> targets = roomConns(room);
        for (size_t i = 0; i < targets.size(); ++i) {
            if (!sendRelay(targets[i], m, origin_seq)) {
                std::cout << "[警告] 房间下发失败（id=" << targets[i]->id << "），剔除" << std::endl;
                kickConn(targets[i], "掉线", true);
            }
        }
    }

    // 只取本房间成员的连接（Q2 的核心：从 O(N 全在线) 到 O(房间成员)）
    std::vector<ConnPtr> roomConns(const std::string& room) {
        std::vector<ConnPtr> v;
        std::lock_guard<std::mutex> lk(g_m_);
        std::map<std::string, RoomRuntime>::iterator it = rooms_.find(room);
        if (it == rooms_.end()) return v;
        for (std::set<std::string>::iterator m = it->second.members.begin();
             m != it->second.members.end(); ++m) {
            std::unordered_map<std::string, ConnPtr>::iterator o = online_.find(*m);
            if (o != online_.end()) v.push_back(o->second);
        }
        return v;
    }

    void pushOffline(const ConnPtr& c, const std::string& name) {
        std::vector<OffRow> rows = db_.undelivered(name);
        if (rows.empty()) return;
        std::cout << "[离线补发] " << name << " 共 " << rows.size() << " 条" << std::endl;
        for (size_t i = 0; i < rows.size(); ++i) {
            minijson::Object m;
            m.set_num("ver", 1);
            m.set_str("type", "MESSAGE");
            m.set_str("from", rows[i].sender);
            m.set_str("to", name);
            m.set_str("room", "");
            m.set_str("content", rows[i].content);
            m.set_num("ts", rows[i].ts);  // 原始发送时间
            m.set_num("offline", 1);      // 客户端以「离线消息」样式展示
            if (!sendRelay(c, m, 0)) {
                std::cout << "[离线补发] " << name << " 中断（连接故障），余下下次再补" << std::endl;
                return;  // 未置 delivered 的行下次上线续推
            }
            db_.mark_delivered(rows[i].id);  // 推送成功一条置一条
        }
    }

    // ---------- 连接摘除 / 心跳扫描 ----------

    // 幂等摘表。顶号（"被顶替"）不摘房间成员、不广播——用户在别人眼里没离开过（Q5）
    void dropConn(const ConnPtr& c, const std::string& reason, bool notify_room = true) {
        std::string name;
        bool was_online = false;
        std::string room;
        {
            std::lock_guard<std::mutex> lk(g_m_);
            std::unordered_map<uint64_t, ConnPtr>::iterator it = conns_.find(c->id);
            if (it == conns_.end() || it->second.get() != c.get()) return;
            conns_.erase(it);
            if (c->authed) {
                std::unordered_map<std::string, ConnPtr>::iterator o = online_.find(c->username);
                if (o != online_.end() && o->second.get() == c.get()) {
                    online_.erase(o);
                    was_online = true;
                    name = c->username;
                    std::unordered_map<std::string, std::string>::iterator ur = user_room_.find(name);
                    if (ur != user_room_.end()) {
                        room = ur->second;
                        std::map<std::string, RoomRuntime>::iterator rm = rooms_.find(room);
                        if (rm != rooms_.end()) rm->second.members.erase(name);
                        user_room_.erase(ur);
                    }
                }
                c->authed = false;
            }
        }
        if (was_online && notify_room && !room.empty()) {
            std::cout << "[下线] " << name << (reason.empty() ? "" : ("（" + reason + "）")) << std::endl;
            broadcastRoomSystem(room, name + " 离开了房间", ConnPtr());
            broadcastRoomUserlist(room);
        } else if (was_online) {
            std::cout << "[下线] " << name << (reason.empty() ? "" : ("（" + reason + "）")) << std::endl;
        }
    }

    void closeConn(const ConnPtr& c) {
        std::lock_guard<std::mutex> lk(c->send_m);
        if (c->fd >= 0) {
            sock_close(c->fd);
            c->fd = (socket_t)-1;
        }
    }

    void monitorLoop() {
        std::cout << "[心跳] 扫描线程启动（每 " << scan_ms_ << "ms 一轮，超时阈值 " << idle_ms_
                  << "ms）" << std::endl;
        while (running_) {
            long long waited = 0;
            while (running_ && waited < scan_ms_) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                waited += 100;
            }
            if (!running_) break;
            long long now = now_ms();
            std::vector<ConnPtr> victims;
            {
                std::lock_guard<std::mutex> lk(g_m_);
                for (std::unordered_map<uint64_t, ConnPtr>::iterator it = conns_.begin();
                     it != conns_.end(); ++it) {
                    if (now - it->second->last_active.load() > idle_ms_) victims.push_back(it->second);
                }
            }
            for (size_t i = 0; i < victims.size(); ++i) {
                std::cout << "[心跳超时] " << userName(victims[i]) << " id=" << victims[i]->id
                          << " 超过 " << idle_ms_ / 1000.0 << "s 无活跃，剔除" << std::endl;
                kickConn(victims[i], "心跳超时", true);
            }
        }
        std::cout << "[心跳] 扫描线程退出" << std::endl;
    }

    size_t connCount() {
        std::lock_guard<std::mutex> lk(g_m_);
        return conns_.size();
    }

    void shutdownAll() {
        std::cout << "[退出] 关闭全部连接..." << std::endl;
        {
            std::lock_guard<std::mutex> lk(g_m_);
            for (std::unordered_map<uint64_t, ConnPtr>::iterator it = conns_.begin();
                 it != conns_.end(); ++it) {
                std::lock_guard<std::mutex> lk2(it->second->send_m);
                if (it->second->fd >= 0) sock_shutdown(it->second->fd);
            }
        }
        if (monitor_.joinable()) monitor_.join();
        if (sock_valid(listen_fd_)) { sock_close(listen_fd_); listen_fd_ = (socket_t)-1; }
        std::cout << "[退出] 服务器已停止" << std::endl;
    }

    socket_t listen_fd_;
    int port_;
    std::string db_path_;
    long long idle_ms_;
    long long scan_ms_;
    TokenBook tokens_;
    Database db_;
    std::thread monitor_;

    std::mutex g_m_;                                   // 保护下面全部表
    std::unordered_map<uint64_t, ConnPtr> conns_;      // id → 连接
    std::unordered_map<std::string, ConnPtr> online_;  // 用户名 → 连接（已认证）
    std::map<std::string, RoomRuntime> rooms_;         // 房间名 → 房间（Q2 路由表）
    std::unordered_map<std::string, std::string> user_room_;  // 用户名 → 当前房间
    std::unordered_map<std::string, long long> recent_seen_;  // 内存 (user,seq) 去重窗口
    uint64_t next_id_;

    std::atomic<bool> running_;
};

static ChatServer* g_server = NULL;

static void on_signal(int) {
    if (g_server) g_server->requestStop();
}

int main(int argc, char** argv) {
#ifdef _WIN32
    WSADATA wsa;
    if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0) {
        std::cerr << "WSAStartup 失败" << std::endl;
        return 1;
    }
#else
    std::signal(SIGPIPE, SIG_IGN);
#endif
    std::srand((unsigned int)(now_ms() & 0x7fffffff));

    int port = 8888;
    std::string db_path = "chat_server_v5.db";
    long long idle_ms = 30000;
    long long scan_ms = 5000;
    long long token_ttl = 7 * 24 * 3600;  // Q4：Token 默认 7 天过期
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--db") == 0 && i + 1 < argc) db_path = argv[++i];
        else if (std::strcmp(argv[i], "--idle") == 0 && i + 1 < argc) idle_ms = std::atoll(argv[++i]);
        else if (std::strcmp(argv[i], "--scan") == 0 && i + 1 < argc) scan_ms = std::atoll(argv[++i]);
        else if (std::strcmp(argv[i], "--token-ttl") == 0 && i + 1 < argc)
            token_ttl = std::atoll(argv[++i]);
        else if (argv[i][0] != '-') port = std::atoi(argv[i]);
    }
    if (idle_ms < 1000) idle_ms = 1000;
    if (scan_ms < 100) scan_ms = 100;
    if (token_ttl < 60) token_ttl = 60;

    std::cout << "正在启动聊天服务器 v5 ..." << std::endl;
    ChatServer server(port, db_path, idle_ms, scan_ms, token_ttl);
    g_server = &server;
    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);
    bool ok = server.start();
    g_server = NULL;
#ifdef _WIN32
    WSACleanup();
#endif
    return ok ? 0 : 1;
}
