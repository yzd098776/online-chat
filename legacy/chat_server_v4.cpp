// chat_server_v4.cpp —— 在线聊天服务器 v4：完整业务 + 可靠性（心跳/去重/离线消息）
//
// 与既有版本的关系：
//   v1 (chat_server_fixed.cpp)  完整业务，`|`+`\n` 文本协议（对照用）
//   v2 (chat_server_v2.cpp)     长度前缀+JSON 帧层 + PING/PONG（阶段1）
//   v3 (chat_server_v3.cpp)     Reactor 性能实验（epoll ET + 线程池，仅帧层 R1）
//   v4 (本文件)                 在 v2 帧层上补全业务并落地可靠性改造（本提示词）
// 线程模型：thread-per-connection（每连接一个收发线程）+ 独立心跳扫描线程 + SQLite 持久化。
// 选它而不是继续 v3 Reactor 的原因：本提示词的验收是「状态一致性/消息不丢」，
// 阻塞 I/O + 每连接一线程让「广播加锁发送 / 扫描线程 shutdown 踢人 / DB 落库」都变成
// 直来直去的代码；v3 的 I/O 线程专属缓冲模型做这些跨线程动作都要绕道任务队列。
//
// 编译（Linux，装有 libsqlite3-dev）:
//   g++ -std=c++11 -Wall -pthread chat_server_v4.cpp -o chat_server_v4 -lsqlite3
// 编译（Linux，只有 libsqlite3.so.0 运行库、无头文件——本项目裸机环境）:
//   g++ -std=c++11 -Wall -pthread chat_server_v4.cpp -o chat_server_v4 /usr/lib/x86_64-linux-gnu/libsqlite3.so.0
//   （源码里 __has_include(<sqlite3.h>) 为假时自动改用自带的 sqlite3_api.h 声明）
// 编译（Docker，见 Dockerfile / VERIFY.md）: 镜像内 apt 装 libsqlite3-dev 后 -lsqlite3
// 用法: ./chat_server_v4 [port] [--db FILE] [--idle MS] [--scan MS]
//       默认 8888 端口 / chat_server_v4.db / --idle 30000 / --scan 5000
//       （--idle/--scan 仅用于把验收等待时间调短，功能语义不变）
//
// ==================== 设计问答（可靠性改造） ====================
// Q1 为什么不用 TCP Keepalive？两者如何配合？
//    ① 默认参数判死太慢：Linux SO_KEEPALIVE 默认 idle=7200s、interval=75s、probes=9，
//       对端断电后要 ~2 小时才报错——聊天系统要求 30s 内剔除，必须逐平台调
//       TCP_KEEPIDLE/KEEPINTVL/KEEPCNT（Windows 还要较新 API），可移植性差、语义不统一。
//    ② 探测主体不对：keepalive 探测包由【内核】应答。对端进程卡死（应用 hang 住）而
//       内核 TCP 栈还活着时，keepalive 照样探测成功——但聊天应用已经死了，消息发过去
//       没人处理。应用层 PING 必须由【应用】回 PONG，进程死了就不会回，这才是端到端存活。
//    ③ 一专多能：应用 PING/PONG 顺便刷新 last_active（在线状态的唯一依据）、测 RTT、
//       周期发包还保住了 NAT/防火墙的会话映射（keepalive 默认 2h 一次，NAT 早超时了）。
//    ④ 配合方式（本实现两者都开）：应用层 PING/PONG 是判活/剔除/广播下线的唯一依据；
//       同时给每个连接开 SO_KEEPALIVE 并调紧参数（本文件 kKeepIdle/KeepIntvl/KeepCnt），
//       作为传输层兜底——在两次应用心跳之间更快回收「FIN 丢失的半开连接」占用的内核资源。
//       分层记忆：传输层 keepalive 管 socket 资源，应用层 PING 管用户状态。
// Q2 为什么重连补发必然产生重复消息？怎么去重？
//    TCP 只保证「字节流不丢不重」，不保证「业务层请求-应答原子」。客户端发 MESSAGE(seq=5)
//    后等 ACK，存在三种结局不可区分：①没送到；②送到了但 ACK 丢了；③送到了、服务器已
//    转发/入库，但连接在写 ACK 前断了。客户端无法区分 ①与 ②③，只能重发（at-least-once），
//    于是服务器侧必然见到重复帧。此外「在线提前投递 + 未及 E2EACK 就断线、行留待上线
//    补发」也会重复（提前投递与补发是两条独立路径，至多各到达一次）。
//    去重三层：
//    a. 客户端每条消息带 per-user 单调递增 seq（跨重连不变，重发【复用原 seq】）；
//    b. 服务器 SQLite 表 seen_message 以 (user_name, seq) 为主键：首次见到才入库路由，
//       重复帧只回 ACK 不再转发（幂等——重复帧「无效果但有应答」）；表在磁盘上，
//       服务器重启后补发照样去重；
//    c. 接收端再按 (from, seq) 做显示层去重，兜住「在线投递 + 离线补发」的边界重复。
// Q3 离线消息为什么存在 SQLite 而不是内存队列？什么时候删行？
//    「服务器重启后消息不丢」是验收场景 3 的前提；内存队列随进程消失。
//    offline_message 表按 (to_user, ts, id) 排序补发，(from_user, seq) 唯一约束与
//    seen_message 双保险防重复入库（且同一事务提交，防「受理后、落库前」崩溃丢消息）。
//    私聊一律【先落库、再尽力提前转发】：sendRelay 写成功 ≠ 对端应用收到——半开
//    （zombie）连接的内核缓冲区照样收下写入然后丢弃，「转发成功」不能当送达证据。
//    故 ACK=已持久化必达；行只在收到接收端 E2EACK（应用层端到端确认）后删除。
//    E2EACK 丢失则该行下次登录重推，「提前投递 + 离线补发」的边界重复由接收端
//    (from, seq) 显示去重兜底（Q2c）。
// Q4 同名用户重连（弱网常见）为什么必须「顶号」而不是拒绝登录？
//    拔网线后客户端会带原用户名重连；此时旧连接可能还没被 30s 超时剔除（半开僵尸）。
//    若拒绝重名登录，用户会被自己的尸体锁在门外直到超时。本实现：新 LOGIN 顶掉旧连接
//    （shutdown 唤醒其收线程，不再广播下线——用户在别人眼里从没离开过），刷新 USERLIST。
// ================================================================

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
#include <memory>
#include <mutex>
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

// ---------------- socket 薄封装 ----------------

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

// TCP Keepalive 兜底参数（Q1④）：应用 PING 是判活唯一依据，这里只管更快回收半开 socket
static const int kKeepIdleSec = 15;   // 空闲 15s 开始探测
static const int kKeepIntvlSec = 5;   // 每 5s 一探
static const int kKeepCnt = 3;        // 3 探无应答由内核判死
static const int kSendTimeoutSec = 3; // 单次 send 最多阻塞 3s（防半开连接卡死广播）
static const int kSendRetry = 2;      // send 超时（EAGAIN）最多重试 2 次后判发送失败
static const size_t kMaxFrame = 1u << 20;

// 组装完整线上帧 [4字节大端长度][body]
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
    // Q1④：传输层 keepalive 兜底（判活/剔除仍走应用层 PING）
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

// ---------------- MiniJson（与 v2/v3 同款，0 依赖） ----------------
namespace minijson {

struct Value {
    enum Type { T_STR, T_NUM, T_LIST } type;
    std::string str;
    long long num;
    std::vector<std::string> list;
    Value() : type(T_STR), num(0) {}
    static Value make_str(const std::string& s) { Value v; v.type = T_STR; v.str = s; return v; }
    static Value make_num(long long n) { Value v; v.type = T_NUM; v.num = n; return v; }
    static Value make_list(const std::vector<std::string>& l) { Value v; v.type = T_LIST; v.list = l; return v; }
};

class Object {
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
        for (size_t i = 0; i < fields.size(); ++i)
            if (fields[i].first == k && fields[i].second.type == Value::T_LIST) return fields[i].second.list;
        return std::vector<std::string>();
    }
};

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
    if (c == '[') {
        ++i;
        std::vector<std::string> list;
        skip_ws(t, i);
        if (i < t.size() && t[i] == ']') { ++i; out = Value::make_list(list); return true; }
        while (true) {
            skip_ws(t, i);
            std::string s;
            if (!parse_string(t, i, s)) return false;
            list.push_back(s);
            skip_ws(t, i);
            if (i < t.size() && t[i] == ',') { ++i; continue; }
            if (i < t.size() && t[i] == ']') { ++i; out = Value::make_list(list); return true; }
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
    ++i;
    out.fields.clear();
    skip_ws(text, i);
    if (i < text.size() && text[i] == '}') { ++i; skip_ws(text, i); return i == text.size(); }
    while (true) {
        skip_ws(text, i);
        std::string key;
        if (!parse_string(text, i, key)) return false;
        skip_ws(text, i);
        if (i >= text.size() || text[i] != ':') return false;
        ++i;
        Value v;
        if (!parse_value(text, i, v)) return false;
        out.set(key, v);
        skip_ws(text, i);
        if (i < text.size() && text[i] == ',') { ++i; continue; }
        if (i < text.size() && text[i] == '}') { ++i; skip_ws(text, i); return i == text.size(); }
        return false;
    }
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

std::string serialize(const Object& obj) {
    std::string s = "{";
    for (size_t i = 0; i < obj.fields.size(); ++i) {
        if (i) s += ",";
        s += "\"" + escape_string(obj.fields[i].first) + "\":";
        const Value& v = obj.fields[i].second;
        switch (v.type) {
            case Value::T_STR: s += "\"" + escape_string(v.str) + "\""; break;
            case Value::T_NUM: s += std::to_string(v.num); break;
            case Value::T_LIST: {
                s += "[";
                for (size_t j = 0; j < v.list.size(); ++j) {
                    if (j) s += ",";
                    s += "\"" + escape_string(v.list[j]) + "\"";
                }
                s += "]";
                break;
            }
        }
    }
    s += "}";
    return s;
}

}  // namespace minijson

// ---------------- FrameReader（与 v2 同款） ----------------
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

// ---------------- SQLite 持久层：seen_message 去重 + offline_message 离线库 ----------------
// 全部语句在一把互斥锁下串行（单连接 SQLite 足够课程规模；WAL 提高崩溃安全）
struct OfflineRow {
    long long id;
    std::string from_user;
    std::string content;
    long long ts;
    long long seq;
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
        // seen_message：(user, seq) 幂等去重表（Q2b），跨服务器重启仍有效
        // offline_message：私聊离线库（Q3），(from_user, seq) 唯一约束防重复入库
        const char* schema =
            "CREATE TABLE IF NOT EXISTS seen_message ("
            "  user_name TEXT NOT NULL,"
            "  seq       INTEGER NOT NULL,"
            "  ts        INTEGER NOT NULL,"
            "  PRIMARY KEY (user_name, seq));"
            "CREATE TABLE IF NOT EXISTS offline_message ("
            "  id        INTEGER PRIMARY KEY AUTOINCREMENT,"
            "  to_user   TEXT NOT NULL,"
            "  from_user TEXT NOT NULL,"
            "  content   TEXT NOT NULL,"
            "  ts        INTEGER NOT NULL,"
            "  seq       INTEGER NOT NULL,"
            "  UNIQUE (from_user, seq));"
            "CREATE INDEX IF NOT EXISTS idx_offline_to ON offline_message (to_user, ts, id);";
        return exec(schema);
    }

    void close() {
        if (db_) { sqlite3_close_v2(db_); db_ = NULL; }
    }

    // 需求 6 + 4 的原子落点：(user,seq) 去重标记与私聊离线行【同一事务】提交。
    // 为什么必须原子：若先 mark_seen 后 store_offline，两步之间进程崩溃，
    // 客户端重发会被 seen_message 判重只回 ACK——消息永远丢了（假「已送达」）。
    // to_user 传空串表示群聊（需求 4 只覆盖私聊，不写离线行）。
    // 返回 1=首次受理（调用方继续路由/回 ACK）；0=重复帧（只回 ACK，零副作用）；-1=DB 失败（回 NACK）
    int acceptMessage(const std::string& user, long long seq, long long ts,
                      const std::string& to_user, const std::string& content) {
        std::lock_guard<std::mutex> lk(m_);
        if (sqlite3_exec(db_, "BEGIN IMMEDIATE;", NULL, NULL, NULL) != SQLITE_OK) return -1;
        int result = -1;
        sqlite3_stmt* st = NULL;
        do {
            if (sqlite3_prepare_v2(db_, "INSERT OR IGNORE INTO seen_message VALUES (?,?,?);",
                                   -1, &st, NULL) != SQLITE_OK) break;
            sqlite3_bind_text(st, 1, user.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int64(st, 2, seq);
            sqlite3_bind_int64(st, 3, ts);
            if (sqlite3_step(st) != SQLITE_DONE) break;
            sqlite3_finalize(st);
            st = NULL;
            if (sqlite3_changes(db_) != 1) { result = 0; break; }  // 重复帧（Q2b）
            if (!to_user.empty()) {
                if (sqlite3_prepare_v2(db_,
                                       "INSERT OR IGNORE INTO offline_message "
                                       "(to_user, from_user, content, ts, seq) VALUES (?,?,?,?,?);",
                                       -1, &st, NULL) != SQLITE_OK) break;
                sqlite3_bind_text(st, 1, to_user.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(st, 2, user.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(st, 3, content.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_int64(st, 4, ts);
                sqlite3_bind_int64(st, 5, seq);
                if (sqlite3_step(st) != SQLITE_DONE) break;
                sqlite3_finalize(st);
                st = NULL;
            }
            result = 1;
        } while (false);
        if (st) sqlite3_finalize(st);
        sqlite3_exec(db_, result >= 0 ? "COMMIT;" : "ROLLBACK;", NULL, NULL, NULL);
        return result;
    }

    // 按 (ts, id) 升序取离线消息——「补发按时间排序」（需求 4）
    std::vector<OfflineRow> fetch_offline(const std::string& to_user) {
        std::vector<OfflineRow> rows;
        std::lock_guard<std::mutex> lk(m_);
        sqlite3_stmt* st = NULL;
        if (sqlite3_prepare_v2(db_,
                               "SELECT id, from_user, content, ts, seq FROM offline_message "
                               "WHERE to_user = ? ORDER BY ts ASC, id ASC;",
                               -1, &st, NULL) != SQLITE_OK) return rows;
        sqlite3_bind_text(st, 1, to_user.c_str(), -1, SQLITE_TRANSIENT);
        while (sqlite3_step(st) == SQLITE_ROW) {
            OfflineRow r;
            r.id = sqlite3_column_int64(st, 0);
            const unsigned char* f = sqlite3_column_text(st, 1);
            const unsigned char* c = sqlite3_column_text(st, 2);
            r.from_user = f ? (const char*)f : "";
            r.content = c ? (const char*)c : "";
            r.ts = sqlite3_column_int64(st, 3);
            r.seq = sqlite3_column_int64(st, 4);
            rows.push_back(r);
        }
        sqlite3_finalize(st);
        return rows;
    }

    // 行只在收到接收端 E2EACK（应用层端到端确认）后删除（Q3）。
    // (to_user, from_user, seq) 精确定位——from_user+seq 是端到端消息 id；重复删除是 no-op。
    void delete_offline_for(const std::string& to_user, const std::string& from_user,
                            long long seq) {
        std::lock_guard<std::mutex> lk(m_);
        sqlite3_stmt* st = NULL;
        if (sqlite3_prepare_v2(db_,
                               "DELETE FROM offline_message "
                               "WHERE to_user = ? AND from_user = ? AND seq = ?;",
                               -1, &st, NULL) != SQLITE_OK) return;
        sqlite3_bind_text(st, 1, to_user.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 2, from_user.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(st, 3, seq);
        sqlite3_step(st);
        sqlite3_finalize(st);
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
    sqlite3* db_;
    std::mutex m_;
};

// ---------------- 连接对象与服务器 ----------------
//
// fd 生命周期纪律（防 close/写/reuse 三方竞态）：
//   * 收线程（每连接唯一）阻塞在 recv 上，是唯一【最终 close】者（closeConn 只在它收尾时调用）；
//   * 任何线程踢人只做：从全局表摘除 + shutdown(fd)（kickConn，唤醒 recv，不释放 fd 号）；
//   * 所有 send/close/shutdown 都持有 conn->send_m；close 前把 fd 置 -1，
//     此后该对象永不再碰 fd 号——fd 号被 accept 复用也不会写/关错连接。
struct ClientConn {
    socket_t fd;
    uint64_t id;                 // 防 fd 号复用串话
    std::string peer;
    FrameReader reader;
    std::mutex send_m;           // 串行化 send/shutdown/close（见上方纪律）
    std::atomic<long long> last_active;  // 任意入站帧刷新（PING 自然在内）
    std::string username;        // 登录时写一次，此后只读
    std::atomic<bool> logged_in;
    long long send_seq;          // 本连接下行帧序号（持 send_m 时自增）
    ClientConn() : fd(-1), id(0), last_active(0), logged_in(false), send_seq(0) {}
};
typedef std::shared_ptr<ClientConn> ConnPtr;

class ChatServer {
public:
    ChatServer(int port, const std::string& db_path, long long idle_ms, long long scan_ms)
        : listen_fd_(-1), port_(port), db_path_(db_path), idle_ms_(idle_ms), scan_ms_(scan_ms),
          running_(false), next_id_(0) {
#ifdef _WIN32
        listen_fd_ = INVALID_SOCKET;
#endif
    }

    bool start() {
        if (!db_.open(db_path_)) return false;
        if (!startListen()) return false;
        running_ = true;
        monitor_ = std::thread(&ChatServer::monitorLoop, this);

        std::cout << "==========================================" << std::endl;
        std::cout << "聊天服务器 v4 已启动（业务 + 可靠性）" << std::endl;
        std::cout << "监听端口: " << port_ << "（0.0.0.0），MAX_FRAME=1MiB" << std::endl;
        std::cout << "心跳: 客户端 PING→PONG；扫描线程每 " << scan_ms_ / 1000.0
                  << "s 一轮，" << idle_ms_ / 1000.0 << "s 无活跃判定死亡并广播下线" << std::endl;
        std::cout << "持久化: " << db_path_ << "（seen_message 去重 + offline_message 离线库）"
                  << std::endl;
        std::cout << "Keepalive: SO_KEEPALIVE 兜底 " << kKeepIdleSec << "/" << kKeepIntvlSec
                  << "/" << kKeepCnt << "s（判活以应用层 PING 为准，见文件头 Q1）" << std::endl;
        std::cout << "==========================================" << std::endl;

        acceptLoop();
        shutdownAll();
        return true;
    }

    // 信号处理线程/主循环外调用：唤醒 accept 退出
    void requestStop() {
        running_ = false;
        if (sock_valid(listen_fd_)) sock_shutdown(listen_fd_);
    }

private:
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

    // ---------- 收发（连接线程） ----------

    void handleClient(ConnPtr c) {
        char chunk[4096];
        while (running_) {
            long n = sock_recv(c->fd, chunk, sizeof(chunk));
            if (n > 0) {
                c->last_active = now_ms();  // 任何入站流量都算活跃（PING 自然包含）
                c->reader.feed(chunk, (size_t)n);
                std::string body;
                while (c->reader.next(body)) {
                    if (handleBody(c, body)) {  // LOGOUT 等要求本连接收尾
                        dropConn(c, "", true);
                        closeConn(c);
                        return;
                    }
                }
                if (c->reader.failed()) {
                    std::cout << "[协议错误] id=" << c->id << " 帧超限/坏帧，断开" << std::endl;
                    break;
                }
            } else if (n == 0) {
                std::cout << "[断开] " << c->peer << " 对端关闭（kill -9/正常退出会走这条 FIN 路径）"
                          << std::endl;
                break;
            } else {
                if (sock_interrupted()) continue;
                std::cout << "[断开] " << c->peer << " recv 出错（被踢/网络异常）" << std::endl;
                break;
            }
        }
        dropConn(c, c->logged_in ? "掉线" : "", true);  // 摘表 + 广播下线（幂等）
        closeConn(c);                                   // 唯一 close 点
    }

    // 返回 true = 本连接需要收尾退出（LOGOUT）
    bool handleBody(const ConnPtr& c, const std::string& body) {
        minijson::Object obj;
        if (!minijson::parse(body, obj)) {
            std::cout << "[错误] JSON 解析失败（id=" << c->id << "）" << std::endl;
            return false;
        }
        std::string type = obj.get_str("type");
        if (type == "PING") {
            // 需求 1：回 PONG 并刷新 last_active（last_active 在收线程已刷）
            minijson::Object pong;
            pong.set_num("ver", 1);
            pong.set_str("type", "PONG");
            pong.set_str("from", "SERVER");
            pong.set_str("to", obj.get_str("from"));
            pong.set_str("room", "");
            pong.set_str("content", obj.get_str("content"));  // 原样回显
            pong.set_num("ts", now_ts());
            sendFrame(c, pong);
        } else if (type == "LOGIN") {
            doLogin(c, obj);
        } else if (type == "MESSAGE") {
            doMessage(c, obj);
        } else if (type == "E2EACK") {
            // 接收端应用层确认（端到端）→ 删除 offline_message 行。
            // 行的删除【只】发生在这里：转发/补发的 sendRelay「写成功」只证明字节进了
            // 对端内核缓冲区，半开（zombie）连接照样收下写入然后丢弃——只有接收端回
            // E2EACK 才是「对方应用已收到」的证据（Q3）。重复 E2EACK 是无害 no-op。
            if (!c->logged_in) return false;
            std::string origin_from = obj.get_str("to");                  // 原消息发送者 = 行的 from_user
            long long origin_seq = std::atoll(obj.get_str("content").c_str());  // content=原消息 seq（与 ACK 约定一致）
            if (origin_seq > 0 && !origin_from.empty()) {
                db_.delete_offline_for(c->username, origin_from, origin_seq);
                std::cout << "[E2EACK] " << c->username << " 确认 " << origin_from
                          << " seq=" << origin_seq << "，离线行已删（若有）" << std::endl;
            }
        } else if (type == "LOGOUT") {
            std::cout << "[退出] " << userName(c) << " 主动 LOGOUT" << std::endl;
            dropConn(c, "退出", true);  // 广播「退出了聊天室」+ USERLIST
            return true;
        } else {
            std::cout << "[警告] 未处理类型: " << type << "（id=" << c->id << "）" << std::endl;
        }
        return false;
    }

    // 持 send_m 把整帧写完（sendLocked 必须在持有 send_m 时调用）；false = 发送失败
    // 重试预算：SO_SNDTIMEO 到点 send 返回 EAGAIN，重试 kSendRetry 次仍失败即认失败
    //（不能无限 continue——对端 TCP 窗口长期为 0 时会把广播线程挂死在死循环里）
    bool sendLocked(const ConnPtr& c, const std::string& pkt) {
        if (c->fd < 0) return false;
        size_t sent = 0;
        int retries = 0;
        while (sent < pkt.size()) {
            long n = sock_send(c->fd, pkt.data() + sent, pkt.size() - sent);
            if (n > 0) { sent += (size_t)n; continue; }
            if (n < 0 && sock_interrupted()) continue;
            if (n < 0 && sock_would_block()) {
                if (++retries > kSendRetry) return false;  // 发送超时
                continue;
            }
            return false;
        }
        return true;
    }

    // 带服务器下行 seq 的发送（SYSTEM/USERLIST/PONG/ACK/NACK）：seq 在 send_m 内自增保证有序
    bool sendFrame(const ConnPtr& c, minijson::Object obj) {
        std::lock_guard<std::mutex> lk(c->send_m);
        if (c->fd < 0) return false;
        obj.set_num("seq", ++c->send_seq);  // 与写出同锁：帧序号不会与内容交错
        std::string body = minijson::serialize(obj);
        if (body.size() > kMaxFrame) return false;
        return sendLocked(c, make_wire_frame(body));
    }

    // 转发聊天 MESSAGE：seq 用【发送方原始 seq】（端到端消息 id，接收端按 (from,seq) 显示去重）
    bool sendRelay(const ConnPtr& c, minijson::Object obj, long long origin_seq) {
        std::lock_guard<std::mutex> lk(c->send_m);
        if (c->fd < 0) return false;
        obj.set_num("seq", origin_seq);
        std::string body = minijson::serialize(obj);
        if (body.size() > kMaxFrame) return false;
        return sendLocked(c, make_wire_frame(body));
    }

    // 踢人：摘表（幂等）+ 在 send_m 内 shutdown 唤醒收线程（防 fd 号复用后误伤新连接）
    void kickConn(const ConnPtr& c, const std::string& reason, bool broadcast_leave) {
        dropConn(c, reason, broadcast_leave);
        std::lock_guard<std::mutex> lk(c->send_m);
        if (c->fd >= 0) sock_shutdown(c->fd);
    }

    // ---------- 业务 ----------

    static bool validUsername(const std::string& u) {
        if (u.empty() || u.size() > 32) return false;
        if (u == "SERVER" || u == "ALL") return false;  // 保留字
        for (size_t i = 0; i < u.size(); ++i)
            if ((unsigned char)u[i] < 0x20) return false;
        return true;
    }

    std::string userName(const ConnPtr& c) {
        return c->logged_in ? c->username : ("?" + std::to_string(c->id));
    }

    void doLogin(const ConnPtr& c, minijson::Object& obj) {
        if (c->logged_in) {
            sendNack(c, obj.get_num("seq"), "重复 LOGIN");
            return;
        }
        std::string name = obj.get_str("from");
        if (!validUsername(name)) {
            sendNack(c, obj.get_num("seq"), "用户名非法（空/超长/保留字）");
            return;
        }
        ConnPtr old;
        bool was_online = false;
        bool dup_self = false;
        {
            std::lock_guard<std::mutex> lk(g_m_);
            std::unordered_map<std::string, ConnPtr>::iterator o = online_.find(name);
            if (o != online_.end()) {
                was_online = true;
                old = o->second;
                if (old.get() == c.get()) dup_self = true;  // 自己重复登录
                else {
                    // Q4 顶号：旧连接视为被顶替（不再广播下线——用户在别人眼里没离开过）
                    online_.erase(o);
                    old->logged_in = false;
                }
            }
            if (!dup_self) {
                c->username = name;
                c->logged_in = true;
                online_[name] = c;
            }
        }
        if (dup_self) {
            sendNack(c, obj.get_num("seq"), "已在登录态");
            return;
        }
        if (old) {
            std::cout << "[顶号] " << name << " 新连接 id=" << c->id << " 顶替旧连接 id=" << old->id
                      << std::endl;
            kickConn(old, "", false);  // 摘表（幂等）+ 不广播，唤醒旧收线程
        }
        std::cout << "[登录] " << name << "（id=" << c->id << "）" << std::endl;

        // 登录回执
        minijson::Object sys;
        sys.set_num("ver", 1);
        sys.set_str("type", "SYSTEM");
        sys.set_str("from", "SERVER");
        sys.set_str("to", name);
        sys.set_str("room", "");
        sys.set_str("content", "登录成功");
        sys.set_num("ts", now_ts());
        sendFrame(c, sys);

        pushOffline(c, name);  // 需求 4：上线立即补发离线消息（按 ts,id 升序）

        // 顶号场景不重复播「上线」（他一直在线）；否则播进入通知
        if (!was_online) {
            std::cout << "[广播] " << name << " 进入聊天室" << std::endl;
            broadcastSystem(name + " 进入了聊天室");
        }
        broadcastUserlist();
    }

    void pushOffline(const ConnPtr& c, const std::string& name) {
        std::vector<OfflineRow> rows = db_.fetch_offline(name);
        if (rows.empty()) return;
        std::cout << "[离线补发] " << name << " 共 " << rows.size() << " 条" << std::endl;
        for (size_t i = 0; i < rows.size(); ++i) {
            minijson::Object m;
            m.set_num("ver", 1);
            m.set_str("type", "MESSAGE");
            m.set_str("from", rows[i].from_user);
            m.set_str("to", name);
            m.set_str("room", "");
            m.set_str("content", rows[i].content);
            m.set_num("ts", rows[i].ts);       // 原始发送时间——客户端按此排序/展示
            m.set_num("offline", 1);           // 需求 4：客户端以「离线消息」样式展示
            if (!sendRelay(c, m, rows[i].seq)) {
                std::cout << "[离线补发] " << name << " 中断（连接故障），余下下次登录再补" << std::endl;
                return;  // 行未删 = 至少一次投递；客户端按 (from,seq) 显示去重
            }
            // 行【不】在此删：sendRelay 成功 ≠ 对端应用收到（半开连接会吞写入）。
            // 收到接收端 E2EACK 才删行（handleBody）；E2EACK 丢了则下次登录重推，
            // 「提前投递 + 离线补发」的边界重复由接收端 (from,seq) 显示去重兜底（Q2c）。
        }
    }

    void doMessage(const ConnPtr& c, minijson::Object& obj) {
        if (!c->logged_in) {
            sendNack(c, obj.get_num("seq"), "未登录");
            return;
        }
        const std::string from = c->username;  // 防伪：以会话身份为准，不信任帧内 from
        long long seq = obj.get_num("seq");
        long long ts = obj.get_num("ts");
        if (ts <= 0) ts = now_ts();
        if (seq <= 0) {
            sendNack(c, seq, "seq 必须为正整数（去重键）");
            return;
        }
        std::string to = obj.get_str("to");
        std::string content = obj.get_str("content");

        // 目标校验放在受理【之前】：非法目标若先烧掉 (user,seq)，客户端手工重试同 seq
        // 会被判重回假 ACK——「失败可重试」形同虚设
        if (to != "ALL" && (to.empty() || to == "SERVER" || !validUsername(to))) {
            sendNack(c, seq, "接收者非法");
            return;
        }

        // 需求 6 + 4：(user,seq) 去重 + 私聊行原子落库（acceptMessage，Q2/Q3）。
        // 重复帧只回 ACK 不再产生任何副作用；DB 失败回 NACK（客户端可重试）。
        int acc = db_.acceptMessage(from, seq, ts, to == "ALL" ? std::string() : to, content);
        if (acc < 0) {
            sendNack(c, seq, "离线库写入失败");
            return;
        }
        if (acc == 0) {
            std::cout << "[去重] " << from << " seq=" << seq << " 重复帧，仅回 ACK" << std::endl;
            sendAck(c, seq);
            return;
        }
        std::cout << "[消息] " << from << " -> " << to << " seq=" << seq << " content=" << content
                  << std::endl;

        if (to == "ALL") {
            minijson::Object m;
            m.set_num("ver", 1);
            m.set_str("type", "MESSAGE");
            m.set_str("from", from);
            m.set_str("to", "ALL");
            m.set_str("room", "");
            m.set_str("content", content);
            m.set_num("ts", ts);
            broadcastRelay(m, seq, c);  // 含回显给发送者自己（其 UI 靠 ACK 更新状态，回显可显示）
            sendAck(c, seq);
            return;
        }

        // 私聊 store-and-forward：行已在 acceptMessage 落库——「转发成功」不能当送达证据，
        // sendRelay 写进半开（zombie）socket 的内核缓冲区也会返回成功但数据随连接丢（Q3）。
        // ACK=已持久化必达；行只在收到接收端 E2EACK 后删除，对方上线由 pushOffline 按 (ts,id) 补发。
        std::cout << "[离线存储] " << from << " -> " << to << " seq=" << seq << " 已入库（E2EACK 后删）" << std::endl;
        sendAck(c, seq);  // 已送达 = 已持久化，对方必达

        // 尽力提前投递：对方在线则立刻推一份（与上线补发的边界重复由接收端 (from,seq) 显示去重兜底，Q2c）
        ConnPtr target;
        {
            std::lock_guard<std::mutex> lk(g_m_);
            std::unordered_map<std::string, ConnPtr>::iterator it = online_.find(to);
            if (it != online_.end()) target = it->second;
        }
        if (!target) return;  // 不在线：行已入库，对方上线 pushOffline 补发
        minijson::Object m;
        m.set_num("ver", 1);
        m.set_str("type", "MESSAGE");
        m.set_str("from", from);
        m.set_str("to", to);
        m.set_str("room", "");
        m.set_str("content", content);
        m.set_num("ts", ts);
        if (!sendRelay(target, m, seq)) {
            // 提前投递失败（半开僵尸写不进）：剔除目标让下线广播/扫描尽快跟上；
            // 行【保留】——对方重连/上线照样补发（这正是丢消息事故的根源路径，已堵死）
            std::cout << "[投递失败] " << to << " 连接故障，剔除僵尸连接（行已入库待补发）" << std::endl;
            kickConn(target, "掉线", true);
        }
    }

    void sendAck(const ConnPtr& c, long long client_seq) {
        minijson::Object ack;
        ack.set_num("ver", 1);
        ack.set_str("type", "ACK");
        ack.set_str("from", "SERVER");
        ack.set_str("to", c->username);
        ack.set_str("room", "");
        ack.set_str("content", std::to_string(client_seq));  // content = 被确认的客户端 seq
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
        nack.set_str("reason", reason);  // 扩展字段：失败原因（客户端标记「失败可重试」）
        nack.set_num("ts", now_ts());
        sendFrame(c, nack);
        std::cout << "[NACK] -> " << userName(c) << " seq=" << client_seq << " reason=" << reason
                  << std::endl;
    }

    // ---------- 广播 ----------

    std::vector<ConnPtr> onlineSnapshot() {
        std::vector<ConnPtr> v;
        std::lock_guard<std::mutex> lk(g_m_);
        for (std::unordered_map<std::string, ConnPtr>::iterator it = online_.begin();
             it != online_.end(); ++it) v.push_back(it->second);
        return v;
    }

    std::vector<std::string> onlineNames() {
        std::vector<std::string> v;
        std::lock_guard<std::mutex> lk(g_m_);
        for (std::unordered_map<std::string, ConnPtr>::iterator it = online_.begin();
             it != online_.end(); ++it) v.push_back(it->first);
        return v;
    }

    void broadcastSystem(const std::string& text) {
        minijson::Object sys;
        sys.set_num("ver", 1);
        sys.set_str("type", "SYSTEM");
        sys.set_str("from", "SERVER");
        sys.set_str("to", "ALL");
        sys.set_str("room", "");
        sys.set_str("content", text);
        sys.set_num("ts", now_ts());
        std::vector<ConnPtr> targets = onlineSnapshot();
        for (size_t i = 0; i < targets.size(); ++i) {
            if (!sendFrame(targets[i], sys)) {
                std::cout << "[警告] SYSTEM 下发失败（id=" << targets[i]->id << "）" << std::endl;
            }
        }
    }

    void broadcastUserlist() {
        minijson::Object ul;
        ul.set_num("ver", 1);
        ul.set_str("type", "USERLIST");
        ul.set_str("from", "SERVER");
        ul.set_str("to", "ALL");
        ul.set_str("room", "");
        ul.set_list("content", onlineNames());
        ul.set_num("ts", now_ts());
        std::vector<ConnPtr> targets = onlineSnapshot();
        for (size_t i = 0; i < targets.size(); ++i) sendFrame(targets[i], ul);
    }

    // 群聊转发：seq 用发送方原始 seq；exclude = 发送者（其 UI 走 ACK 状态列，可不再回显）
    // —— 但为了「发送者也能看到自己的消息进历史」，v4 选择【包含发送者】回显。
    void broadcastRelay(minijson::Object& m, long long origin_seq, const ConnPtr& /*sender*/) {
        std::vector<ConnPtr> targets = onlineSnapshot();
        for (size_t i = 0; i < targets.size(); ++i) {
            if (!sendRelay(targets[i], m, origin_seq)) {
                std::cout << "[警告] 广播下发失败（id=" << targets[i]->id << "），剔除" << std::endl;
                kickConn(targets[i], "掉线", true);
            }
        }
    }

    // ---------- 连接摘除 / 心跳扫描 ----------

    // 幂等摘表。broadcast_leave=true 且曾登录 → 广播「下线」+ USERLIST（需求 1/5）
    void dropConn(const ConnPtr& c, const std::string& reason, bool broadcast_leave) {
        bool was_online = false;
        std::string name;
        {
            std::lock_guard<std::mutex> lk(g_m_);
            std::unordered_map<uint64_t, ConnPtr>::iterator it = conns_.find(c->id);
            if (it == conns_.end() || it->second.get() != c.get()) return;  // 已摘除
            conns_.erase(it);
            if (c->logged_in) {
                std::unordered_map<std::string, ConnPtr>::iterator o = online_.find(c->username);
                if (o != online_.end() && o->second.get() == c.get()) {
                    online_.erase(o);
                    was_online = true;
                    name = c->username;
                }
                c->logged_in = false;
            }
        }
        if (was_online) {
            std::cout << "[下线] " << name << (reason.empty() ? "" : ("（" + reason + "）")) << std::endl;
            if (broadcast_leave) {
                if (reason == "退出") broadcastSystem(name + " 退出了聊天室");
                else if (reason == "心跳超时") broadcastSystem(name + " 连接超时，已下线");
                else if (reason == "被顶替") { /* 不播（Q4） */ }
                else broadcastSystem(name + " 掉线，已下线");
                broadcastUserlist();
            }
        }
    }

    // 唯一 close 点：send_m 内置 fd=-1 再 close（fd 号不可能被本对象再误用）
    void closeConn(const ConnPtr& c) {
        std::lock_guard<std::mutex> lk(c->send_m);
        if (c->fd >= 0) {
            sock_close(c->fd);
            c->fd = (socket_t)-1;
        }
    }

    // 需求 1：独立线程每 scan_ms 扫描一次，idle_ms 无活跃 → close 并广播下线
    void monitorLoop() {
        std::cout << "[心跳] 扫描线程启动（每 " << scan_ms_ << "ms 一轮，超时阈值 " << idle_ms_
                  << "ms）" << std::endl;
        while (running_) {
            // 1s 切片睡，保证 requestStop 能及时退出
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
                          << " 超过 " << idle_ms_ / 1000.0 << "s 无活跃，剔除并广播下线" << std::endl;
                kickConn(victims[i], "心跳超时", true);  // 需求 1：广播该用户下线
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
    std::atomic<bool> running_;
    Database db_;
    std::thread monitor_;

    std::mutex g_m_;                                 // 保护下面两张表
    std::unordered_map<uint64_t, ConnPtr> conns_;    // id → 连接（含未登录）
    std::unordered_map<std::string, ConnPtr> online_; // 用户名 → 连接（已登录）
    uint64_t next_id_;
};

static ChatServer* g_server = NULL;

static void on_signal(int) {
    if (g_server) g_server->requestStop();  // shutdown(listen) 唤醒 accept
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

    int port = 8888;
    std::string db_path = "chat_server_v4.db";
    long long idle_ms = 30000;  // 需求 1：30s 无活跃判死
    long long scan_ms = 5000;   // 需求 1：每 5s 扫描
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--db") == 0 && i + 1 < argc) db_path = argv[++i];
        else if (std::strcmp(argv[i], "--idle") == 0 && i + 1 < argc) idle_ms = std::atoll(argv[++i]);
        else if (std::strcmp(argv[i], "--scan") == 0 && i + 1 < argc) scan_ms = std::atoll(argv[++i]);
        else if (argv[i][0] != '-') port = std::atoi(argv[i]);
    }
    if (idle_ms < 1000) idle_ms = 1000;
    if (scan_ms < 100) scan_ms = 100;

    std::cout << "正在启动聊天服务器 v4 ..." << std::endl;
    ChatServer server(port, db_path, idle_ms, scan_ms);
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
