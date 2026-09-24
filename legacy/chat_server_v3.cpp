// chat_server_v3.cpp —— 在线聊天服务器 v3：Reactor（单线程 epoll ET + 线程池业务）
// 阶段 R1：帧层 + PING/PONG 进线程池（业务消息 R2 接入）
//
// 编译（Linux/Ubuntu，验证平台）: g++ -std=c++11 -Wall -pthread chat_server_v3.cpp -o chat_server_v3
// 编译（Windows MinGW-w64）:      g++ -std=c++11 chat_server_v3.cpp -o chat_server_v3.exe -lws2_32
// 编译（Windows MSVC）:           cl /EHsc chat_server_v3.cpp ws2_32.lib
// 用法: ./chat_server_v3 [port] [--workers N]     默认 8888 端口、workers=核数
//
// 拍板项落实：工作线程默认=核数；出站缓冲超限 8MiB 断开慢消费者；退出打印 fd 数日志。
//
// ==================== 设计问答（与回复一致） ====================
// Q1 ET 和 LT 的区别？为什么 ET 必须配非阻塞？漏读一次会怎样？
//    LT（水平触发）：缓冲区"有数据可读/有空间可写"这个状态持续满足就持续通知，允许每次读一点。
//    ET（边缘触发）：只在状态跳变（空→有数据）那一根沿通知一次，之后剩多少都不再通知。
//    所以 ET 必须配非阻塞并循环读到 EAGAIN 为止：若只读一次就停，剩余数据永远不会产生新事件；
//    若用阻塞 recv，取干后的下一次 recv 会把线程挂死。漏读一次 = 数据卡在内核缓冲区，
//    连接"活着但收不到消息"，对端等回复则永久挂死（对端再发数据只添新数据，不救旧数据）。
// Q2 为什么 accept 要 while 循环到 EAGAIN？
//    backlog 同时进入 k 个连接时 ET 只给一根"可读"沿；accept 一次就返回的话，剩下 k-1 个
//    躺在队列里不会再有通知——客户端 connect 成功却永远不被服务。必须非阻塞 listen fd +
//    while(accept) 抽干到 EAGAIN/EWOULDBLOCK。EMFILE（fd 耗尽）单独记日志。
// Q3 线程池开多少线程？为什么？CPU 密集 vs I/O 密集的差异？
//    默认 = std::thread::hardware_concurrency()（核数），--workers N 可配（0 回退 4）。
//    业务任务 = JSON 解析 + 路由 + 一次序列化，纯内存轻计算、无阻塞等待 → 核数即并行收益
//    最大点，再多只有上下文切换开销。CPU 密集（编解码/压缩/加密）超过核数互相抢核、吞吐反降；
//    I/O 密集（磁盘/DB/远端 HTTP）线程大半在睡眠，可开核数的数倍填等待间隙。本任务偏 CPU
//    轻量故取核数；将来加消息持久化（磁盘）再调 核数×2~×4，并以压测实测为准。
// Q4 惊群问题存不存在？怎么避免？
//    本模型不存在：单 acceptor + 单 epoll 线程，只有它一个线程在 accept；线程池按 fd 哈希
//    固定到同一 worker（保证单连接 FIFO 有序）；worker→I/O 出站队列用 eventfd 只唤醒 epoll
//    线程一个等待者。会出惊群的场景是多线程/多进程抢同一 listen fd、或多 epoll 实例监视同一
//    fd——规避手段：单 acceptor（本设计）、Linux4.5+ EPOLLEXCLUSIVE、或 SO_REUSEPORT 分流。
//    将来若拆多 reactor 会先上 EPOLLEXCLUSIVE。
// Q5 这种模型的瓶颈在哪？单线程 epoll 什么时候成瓶颈？下一步怎么拆？
//    天花板 = 单核的系统调用 + 内存拷贝吞吐。热点排序：(1) 广播扇出——1 条消息 → N 份 send
//    全在 I/O 线程，N=5000 最先到顶（本轮缓解：报文只序列化一次，出站缓冲挂 shared_ptr
//    不可变帧）；(2) 收发 syscall 次数；(3) 出站队列锁争用。单核打满 / P99 随投递速率陡增 /
//    出站队列持续堆积 即瓶颈信号。下一步拆分（本轮不做）：多 reactor（连接按 fd 分片，每
//    工作线程一个 epoll）+ 独立 acceptor（EPOLLEXCLUSIVE）、SO_REUSEPORT 多进程、扇出展开
//    挪到 worker、io_uring 替换 epoll+send。
// Q6 为什么不能一直开着 EPOLLOUT？
//    LT 下套接字几乎永远可写（发送缓冲区常年有空），EPOLLOUT 电平持续满足 → epoll_wait
//    每次立刻返回 → 忙循环 100% CPU。ET 下不会严格忙循环，但"等不到下一根沿"易挂死、MOD
//    状态易错。纪律（本实现）：平时只挂 EPOLLIN|EPOLLRDHUP|EPOLLET；send 写到 EAGAIN 且
//    出站还有剩余才 MOD 加 EPOLLOUT；EPOLLOUT 事件里继续冲，冲干净立刻 MOD 摘掉。
// Q7 锁粒度？
//    Connection 表与所有收/发缓冲只被 I/O 线程访问（线程归属）→ 无锁；任务队列每 worker
//    一把 mutex+condvar（fd 哈希分片）；出站队列一把 mutex + eventfd 唤醒；server seq 用
//    atomic。不配 per-connection 锁：worker 直写缓冲会与 I/O 线程冲缓冲/EPOLLOUT 状态互踩，
//    5000 路广播时锁争用爆炸——跨线程共享面压缩到 2 个队列。
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
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#ifdef _WIN32
#ifndef FD_SETSIZE
#define FD_SETSIZE 1024  // WinSock select 上限（5000 路压测仅 Linux epoll 可做）
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
typedef SOCKET socket_t;
#else
#include <arpa/inet.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>
typedef int socket_t;
#endif

#ifdef __linux__
#include <sys/epoll.h>
#include <sys/eventfd.h>
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

static bool set_nonblocking(socket_t s) {
#ifdef _WIN32
    u_long mode = 1;
    return ioctlsocket(s, FIONBIO, &mode) == 0;
#else
    int fl = fcntl(s, F_GETFL, 0);
    return fl >= 0 && fcntl(s, F_SETFL, fl | O_NONBLOCK) == 0;
#endif
}

static long long now_ts() {
    return (long long)std::chrono::duration_cast<std::chrono::seconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

// 退出前 fd 计数（拍板项4④）；非 Linux 返回 -1
static int count_open_fds() {
#ifdef __linux__
    int count = 0;
    DIR* d = opendir("/proc/self/fd");
    if (!d) return -1;
    while (readdir(d)) ++count;
    closedir(d);
    return count - 2;  // 去掉 "." 与 ".."
#else
    return -1;
#endif
}

static const size_t kMaxFrame = 1u << 20;  // 单帧上限 1 MiB
static const size_t kMaxOutBuf = 8u << 20; // 出站缓冲上限 8 MiB（拍板项1：超限断开慢消费者）

// 组装完整线上帧 [4字节大端长度][body]——出站共享帧就用它（序列化一次，广播共享）
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

// ---------------- MiniJson（与 v2 同款，0 依赖） ----------------
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

// ---------------- ThreadPool：fd 哈希分片保证单连接 FIFO，优雅退出 ----------------
class ThreadPool {
public:
    explicit ThreadPool(size_t n) {
        if (n == 0) n = 4;
        workers_.resize(n);
        for (size_t i = 0; i < n; ++i) workers_[i].reset(new Worker());
        for (size_t i = 0; i < n; ++i) threads_.push_back(std::thread(&ThreadPool::workerLoop, this, i));
        std::cout << "[线程池] " << n << " 个工作线程" << std::endl;
    }
    ~ThreadPool() { stop(); }

    // key=fd：同一连接的任务永远落到同一 worker，保证处理顺序 = 到达顺序（无乱序前提）
    void submit(size_t key, std::function<void()> job) {
        Worker& w = *workers_[key % workers_.size()];
        {
            std::lock_guard<std::mutex> lk(w.m);
            if (w.stopping) return;
            w.jobs.push_back(std::move(job));
        }
        w.cv.notify_one();
    }

    void stop() {  // 优雅退出：排空各自队列后退出，join 全部
        for (size_t i = 0; i < workers_.size(); ++i) {
            { std::lock_guard<std::mutex> lk(workers_[i]->m); workers_[i]->stopping = true; }
            workers_[i]->cv.notify_one();
        }
        for (size_t i = 0; i < threads_.size(); ++i)
            if (threads_[i].joinable()) threads_[i].join();
    }

private:
    struct Worker {
        std::deque<std::function<void()> > jobs;
        std::mutex m;
        std::condition_variable cv;
        bool stopping;
        Worker() : stopping(false) {}
    };
    void workerLoop(size_t idx) {
        Worker& w = *workers_[idx];
        for (;;) {
            std::function<void()> job;
            {
                std::unique_lock<std::mutex> lk(w.m);
                w.cv.wait(lk, [&w] { return w.stopping || !w.jobs.empty(); });
                if (w.jobs.empty()) return;  // stopping 且已排空
                job = std::move(w.jobs.front());
                w.jobs.pop_front();
            }
            job();
        }
    }
    std::vector<std::unique_ptr<Worker> > workers_;
    std::vector<std::thread> threads_;
};

// ---------------- Reactor 服务器 ----------------

static volatile sig_atomic_t g_stop = 0;
#ifdef __linux__
static int g_wake_fd = -1;  // eventfd：worker 通知 I/O 线程有出站数据
#endif
static std::atomic<long long> g_server_seq(0);   // 服务器下行帧全局递增 seq
static std::atomic<unsigned long long> g_next_gen(0);

struct OutSeg {
    std::shared_ptr<const std::string> data;
    size_t off;
};

struct Connection {
    socket_t fd;
    FrameReader reader;
    std::deque<OutSeg> out;
    size_t out_bytes;
    std::string username;
    long long last_active;
    int state;  // 0=已连接 1=已登录（R2 用）
    unsigned long long gen;  // 防 fd 复用串话
    bool out_armed;
    Connection() : fd(-1), out_bytes(0), last_active(0), state(0), gen(0), out_armed(false) {}
};

struct OutTarget {
    socket_t fd;
    unsigned long long gen;
};

struct OutputTask {
    std::vector<OutTarget> targets;
    std::shared_ptr<const std::string> frame;
};

class ChatServer {
public:
    ChatServer(int port, size_t workers)
        : listen_fd_(-1), port_(port), pool_(workers) {
#ifdef _WIN32
        listen_fd_ = INVALID_SOCKET;
#endif
    }

    void run() {
        if (!startListen() || !ioInit()) {
            std::cerr << "初始化失败" << std::endl;
            return;
        }
        std::cout << "==========================================" << std::endl;
        std::cout << "聊天服务器 v3 已启动（Reactor：epoll ET + 线程池，R1 帧层）" << std::endl;
        std::cout << "监听端口: " << port_ << "（0.0.0.0），MAX_FRAME=1MiB，出站上限=8MiB" << std::endl;
        std::cout << "==========================================" << std::endl;
        ioLoop();
        shutdownAll();
    }

private:
    // ---- 启动 ----
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
            std::cerr << "绑定端口 " << port_ << " 失败" << std::endl;
            return false;
        }
        if (listen(listen_fd_, 128) < 0) { std::cerr << "监听失败" << std::endl; return false; }
        if (!set_nonblocking(listen_fd_)) { std::cerr << "listen fd 设非阻塞失败" << std::endl; return false; }
        return true;
    }

    // ---- 平台 I/O 初始化 / 事件注册 ----
    bool ioInit() {
#ifdef __linux__
        epfd_ = epoll_create1(0);
        if (epfd_ < 0) return false;
        g_wake_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (g_wake_fd < 0) return false;
        epoll_event ev;
        std::memset(&ev, 0, sizeof(ev));
        ev.events = EPOLLIN | EPOLLET;
        ev.data.fd = g_wake_fd;
        if (epoll_ctl(epfd_, EPOLL_CTL_ADD, g_wake_fd, &ev) < 0) return false;
        ev.events = EPOLLIN | EPOLLRDHUP | EPOLLET;
        ev.data.fd = listen_fd_;
        if (epoll_ctl(epfd_, EPOLL_CTL_ADD, listen_fd_, &ev) < 0) return false;
#else
        // select 分支（Windows/其他）：出站队列靠 5ms 超时轮询，无 eventfd
#endif
        return true;
    }

    void ioAdd(socket_t fd) {
#ifdef __linux__
        epoll_event ev;
        std::memset(&ev, 0, sizeof(ev));
        ev.events = EPOLLIN | EPOLLRDHUP | EPOLLET;
        ev.data.fd = fd;
        epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &ev);
#endif
    }

    void ioMod(socket_t fd, bool want_out) {
#ifdef __linux__
        epoll_event ev;
        std::memset(&ev, 0, sizeof(ev));
        ev.events = EPOLLIN | EPOLLRDHUP | EPOLLET | (want_out ? (uint32_t)EPOLLOUT : 0u);
        ev.data.fd = fd;
        epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &ev);
#else
        (void)fd;
        (void)want_out;  // select 分支：write 集每轮按 out 非空重建
#endif
    }

    void ioDel(socket_t fd) {
#ifdef __linux__
        epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, NULL);
#else
        (void)fd;
#endif
    }

    void wake() {
#ifdef __linux__
        if (g_wake_fd >= 0) {
            uint64_t one = 1;
            ssize_t r = write(g_wake_fd, &one, sizeof(one));
            (void)r;
        }
#endif
    }

    void ioClose() {
#ifdef __linux__
        if (g_wake_fd >= 0) { sock_close(g_wake_fd); g_wake_fd = -1; }
        if (epfd_ >= 0) { close(epfd_); epfd_ = -1; }
#endif
    }

    // ---- 主循环：Linux epoll ET / 其他 select ----
    void ioLoop() {
#ifdef __linux__
        epoll_event events[256];
        while (!g_stop) {
            int n = epoll_wait(epfd_, events, 256, 500);
            if (n < 0) { if (sock_interrupted()) continue; break; }
            drainOutputQueue();
            for (int i = 0; i < n; ++i) {
                socket_t fd = events[i].data.fd;
                uint32_t e = events[i].events;
                if (fd == g_wake_fd) {
                    uint64_t x;
                    ssize_t r = read(g_wake_fd, &x, sizeof(x));
                    (void)r;
                    continue;
                }
                if (fd == listen_fd_) { acceptAll(); continue; }
                std::unordered_map<socket_t, Connection>::iterator it = conns_.find(fd);
                if (it == conns_.end()) continue;
                if (e & EPOLLOUT) {
                    if (!flushConn(fd, it->second)) { closeConn(fd); continue; }
                    if (it->second.out.empty() && it->second.out_armed) disarmOut(fd, it->second);
                }
                if (e & (EPOLLIN | EPOLLRDHUP | EPOLLHUP | EPOLLERR)) {
                    if (handleRead(fd, it->second)) { closeConn(fd); continue; }
                }
            }
        }
#else
        while (!g_stop) {
            fd_set rfds, wfds;
            FD_ZERO(&rfds);
            FD_ZERO(&wfds);
            FD_SET(listen_fd_, &rfds);
            socket_t maxfd = listen_fd_;
            for (std::unordered_map<socket_t, Connection>::iterator it = conns_.begin();
                 it != conns_.end(); ++it) {
                FD_SET(it->first, &rfds);
                if (!it->second.out.empty()) FD_SET(it->first, &wfds);
                if (it->first > maxfd) maxfd = it->first;
            }
            timeval tv;
            tv.tv_sec = 0;
            tv.tv_usec = 5000;  // 5ms：兼容分支轮询出站队列/停止标志
            int r = select((int)maxfd + 1, &rfds, &wfds, NULL, &tv);
            if (r < 0) { if (sock_interrupted()) continue; break; }
            drainOutputQueue();
            if (FD_ISSET(listen_fd_, &rfds)) acceptAll();
            std::vector<socket_t> to_close;
            for (std::unordered_map<socket_t, Connection>::iterator it = conns_.begin();
                 it != conns_.end(); ++it) {
                socket_t fd = it->first;
                if (FD_ISSET(fd, &wfds)) {
                    if (!flushConn(fd, it->second)) to_close.push_back(fd);
                }
                if (FD_ISSET(fd, &rfds)) {
                    if (handleRead(fd, it->second)) to_close.push_back(fd);
                }
            }
            for (size_t i = 0; i < to_close.size(); ++i) closeConn(to_close[i]);
        }
#endif
    }

    // ---- accept：while 到 EAGAIN（Q2）----
    void acceptAll() {
        for (;;) {
            sockaddr_in ca;
            socklen_t cl = sizeof(ca);
            socket_t cfd = accept(listen_fd_, (sockaddr*)&ca, &cl);
            if (!sock_valid(cfd)) {
                if (sock_would_block()) break;  // 抽干结束
                if (sock_interrupted()) continue;
                std::cerr << "[错误] accept 失败" << std::endl;
                break;
            }
#ifndef __linux__
            if (conns_.size() >= FD_SETSIZE - 8) {
                std::cerr << "[警告] select 分支连接数接近 FD_SETSIZE(" << FD_SETSIZE
                          << ")，拒绝新连接" << std::endl;
                sock_close(cfd);
                continue;
            }
#endif
            set_nonblocking(cfd);
            int one = 1;
            setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, (const char*)&one, sizeof(one));
            Connection c;
            c.fd = cfd;
            c.gen = ++g_next_gen;
            c.last_active = now_ts();
            conns_.emplace(cfd, std::move(c));
            ioAdd(cfd);
            std::cout << "[连接] " << inet_ntoa(ca.sin_addr) << ":" << ntohs(ca.sin_port)
                      << " fd=" << cfd << "（当前 " << conns_.size() << " 路）" << std::endl;
        }
    }

    // ---- 读事件：循环读到 EAGAIN（Q1）+ 拆帧投递 ----
    bool handleRead(socket_t fd, Connection& c) {  // true = 需关闭
        char buf[4096];
        for (;;) {
            long n = sock_recv(fd, buf, sizeof(buf));
            if (n > 0) {
                c.reader.feed(buf, (size_t)n);
                c.last_active = now_ts();
                std::string body;
                while (c.reader.next(body)) dispatchFrame(fd, c.gen, body);
                if (c.reader.failed()) return true;  // 超限/坏帧
            } else if (n == 0) {
                return true;  // 对端关闭
            } else {
                if (sock_would_block()) return false;  // 取干
                if (sock_interrupted()) continue;
                return true;
            }
        }
    }

    void dispatchFrame(socket_t fd, unsigned long long gen, const std::string& body) {
        pool_.submit((size_t)fd, [this, fd, gen, body] { handleBody(fd, gen, body); });
    }

    // ---- 业务（worker 线程）：R1 只做 PING→PONG ----
    void handleBody(socket_t fd, unsigned long long gen, const std::string& body) {
        minijson::Object obj;
        if (!minijson::parse(body, obj)) {
            std::cout << "[错误] JSON 解析失败（fd=" << fd << "）" << std::endl;
            return;
        }
        std::string type = obj.get_str("type");
        if (type == "PING") {
            minijson::Object pong;
            pong.set_num("ver", 1);
            pong.set_str("type", "PONG");
            pong.set_str("from", "SERVER");
            pong.set_str("to", obj.get_str("from"));
            pong.set_str("room", "");
            pong.set_str("content", obj.get_str("content"));  // 原样回显
            pong.set_num("ts", now_ts());
            pong.set_num("seq", ++g_server_seq);  // 全局递增（下行共享帧可序列化一次）
            std::shared_ptr<const std::string> frame(
                new std::string(make_wire_frame(minijson::serialize(pong))));
            OutputTask t;
            t.targets.push_back(OutTarget{fd, gen});
            t.frame = frame;
            enqueueOutput(t);
        } else {
            std::cout << "[R1] 未处理类型: " << type << "（业务消息 R2 实现）" << std::endl;
        }
    }

    // ---- 出站：worker 入队 + I/O 线程消费（1 mutex + eventfd）----
    void enqueueOutput(const OutputTask& t) {
        {
            std::lock_guard<std::mutex> lk(outq_m_);
            outq_.push_back(t);
        }
        wake();
    }

    void drainOutputQueue() {
        std::deque<OutputTask> local;
        {
            std::lock_guard<std::mutex> lk(outq_m_);
            local.swap(outq_);
        }
        for (size_t i = 0; i < local.size(); ++i) applyOutput(local[i]);
    }

    void applyOutput(const OutputTask& t) {
        for (size_t i = 0; i < t.targets.size(); ++i) {
            socket_t fd = t.targets[i].fd;
            std::unordered_map<socket_t, Connection>::iterator it = conns_.find(fd);
            if (it == conns_.end() || it->second.gen != t.targets[i].gen) continue;  // 已关闭/复用
            Connection& c = it->second;
            c.out_bytes += t.frame->size();
            if (c.out_bytes > kMaxOutBuf) {  // 拍板项1：超限断开慢消费者
                std::cout << "[慢消费者] fd=" << fd << " 出站 " << c.out_bytes
                          << " 字节超限，断开" << std::endl;
                closeConn(fd);
                continue;
            }
            OutSeg seg;
            seg.data = t.frame;
            seg.off = 0;
            c.out.push_back(seg);
            if (!flushConn(fd, c)) { closeConn(fd); continue; }
            if (!c.out.empty() && !c.out_armed) armOut(fd, c);  // EAGAIN 有剩余 → 挂 EPOLLOUT
        }
    }

    // 冲发送缓冲：true=正常（冲干净或 EAGAIN 有剩余）；false=出错需关闭
    bool flushConn(socket_t fd, Connection& c) {
        while (!c.out.empty()) {
            OutSeg& seg = c.out.front();
            long n = sock_send(fd, seg.data->data() + seg.off, seg.data->size() - seg.off);
            if (n > 0) {
                seg.off += (size_t)n;
                c.out_bytes -= (size_t)n;
                if (seg.off == seg.data->size()) c.out.pop_front();
                continue;
            }
            if (n < 0 && sock_would_block()) return true;  // 内核缓冲满 → 留给 EPOLLOUT
            if (n < 0 && sock_interrupted()) continue;
            return false;
        }
        return true;
    }

    void armOut(socket_t fd, Connection& c) {
        c.out_armed = true;
        ioMod(fd, true);
    }

    void disarmOut(socket_t fd, Connection& c) {
        c.out_armed = false;
        ioMod(fd, false);  // 冲干净立刻摘 EPOLLOUT（Q6）
    }

    void closeConn(socket_t fd) {
        std::unordered_map<socket_t, Connection>::iterator it = conns_.find(fd);
        if (it == conns_.end()) return;
        ioDel(fd);
        sock_close(fd);
        std::cout << "[断开] fd=" << fd << "（剩余 " << (conns_.size() - 1) << " 路）" << std::endl;
        conns_.erase(it);
    }

    // ---- 优雅退出（需求 4）----
    void shutdownAll() {
        std::cout << "[退出] 停止 accept，排空线程池..." << std::endl;
        if (sock_valid(listen_fd_)) {
            ioDel(listen_fd_);
            sock_close(listen_fd_);
#ifdef _WIN32
            listen_fd_ = INVALID_SOCKET;
#else
            listen_fd_ = -1;
#endif
        }
        pool_.stop();           // 排空任务队列，join 全部 worker
        drainOutputQueue();     // 尽力冲出残留出站
        int fds_before = count_open_fds();
        std::cout << "[退出] 关闭前 fd 数: " << fds_before << "（含 listen/epoll 等非连接 fd）" << std::endl;
        for (std::unordered_map<socket_t, Connection>::iterator it = conns_.begin();
             it != conns_.end(); ++it) {
            ioDel(it->first);
            sock_close(it->first);
        }
        conns_.clear();
        ioClose();
        std::cout << "[退出] 全部 fd 与线程已释放" << std::endl;
    }

    socket_t listen_fd_;
    int port_;
    ThreadPool pool_;
    std::unordered_map<socket_t, Connection> conns_;  // 只被 I/O 线程访问（无锁）
    std::deque<OutputTask> outq_;
    std::mutex outq_m_;
#ifdef __linux__
    int epfd_;
#endif
};

static void on_signal(int signum) {
    g_stop = 1;
#ifdef __linux__
    if (g_wake_fd >= 0) {  // write 是 async-signal-safe，立刻唤醒 epoll 线程
        uint64_t one = 1;
        ssize_t r = write(g_wake_fd, &one, sizeof(one));
        (void)r;
    }
#endif
    (void)signum;
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
    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);

    int port = 8888;
    size_t workers = std::thread::hardware_concurrency();
    if (workers == 0) workers = 4;
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--workers") == 0 && i + 1 < argc) {
            long n = std::atol(argv[++i]);
            if (n > 0) workers = (size_t)n;
        } else if (argv[i][0] != '-') {
            port = std::atoi(argv[i]);
        }
    }

    std::cout << "正在启动聊天服务器 v3（Reactor）..." << std::endl;
    ChatServer server(port, workers);
    server.run();
#ifdef _WIN32
    WSACleanup();
#endif
    return 0;
}
