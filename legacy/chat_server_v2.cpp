// chat_server_v2.cpp —— 在线聊天服务器 v2（长度前缀 + JSON 协议）
// 阶段1：FrameReader / FrameWriter / MiniJson + PING/PONG（业务消息在阶段2实现）
// 编译（Linux）: g++ -std=c++11 -Wall -pthread chat_server_v2.cpp -o chat_server_v2
// 编译（Windows/MinGW-w64）: g++ -std=c++11 chat_server_v2.cpp -o chat_server_v2.exe -lws2_32
// 验证平台：Ubuntu（本文件的 Linux 分支）；Windows 分支未在本机验证。
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
typedef SOCKET socket_t;
#else
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
typedef int socket_t;
#endif

// ---------------- socket 薄封装（跨平台差异集中在这里） ----------------

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

// 返回: >0 已发送字节; <0 出错（可用 sock_would_block 判断）
static long sock_send(socket_t fd, const char* buf, size_t len) {
#ifdef _WIN32
    return ::send(fd, buf, (int)len, 0);
#else
    return ::send(fd, buf, len, MSG_NOSIGNAL);  // 抑制 SIGPIPE
#endif
}

// 返回: >0 收到字节; 0 对端关闭; <0 出错
static long sock_recv(socket_t fd, char* buf, size_t len) {
#ifdef _WIN32
    return ::recv(fd, buf, (int)len, 0);
#else
    return ::recv(fd, buf, len, 0);
#endif
}

static bool sock_would_block() {
#ifdef _WIN32
    int e = WSAGetLastError();
    return e == WSAEWOULDBLOCK;
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

static long long now_ts() {
    return (long long)std::chrono::duration_cast<std::chrono::seconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

static const size_t kMaxFrame = 1u << 20;  // 1 MiB，超限视为协议错误并断开

// ---------------- MiniJson：扁平 schema 的最小 JSON 编解码（0 依赖） ----------------
// 仅支持本协议所需形态：顶层对象；值 = 字符串 | 整数 | 字符串数组 | null(视作 "")
namespace minijson {

struct Value {
    enum Type { T_STR, T_NUM, T_LIST } type;
    std::string str;
    long long num;
    std::vector<std::string> list;
    Value() : type(T_STR), num(0) {}
    static Value make_str(const std::string& s) {
        Value v;
        v.type = T_STR;
        v.str = s;
        return v;
    }
    static Value make_num(long long n) {
        Value v;
        v.type = T_NUM;
        v.num = n;
        return v;
    }
    static Value make_list(const std::vector<std::string>& l) {
        Value v;
        v.type = T_LIST;
        v.list = l;
        return v;
    }
};

class Object {
public:
    std::vector<std::pair<std::string, Value> > fields;

    void set(const std::string& k, const Value& v) {
        for (size_t i = 0; i < fields.size(); ++i) {
            if (fields[i].first == k) {
                fields[i].second = v;
                return;
            }
        }
        fields.push_back(std::make_pair(k, v));
    }
    void set_str(const std::string& k, const std::string& s) { set(k, Value::make_str(s)); }
    void set_num(const std::string& k, long long n) { set(k, Value::make_num(n)); }
    void set_list(const std::string& k, const std::vector<std::string>& l) {
        set(k, Value::make_list(l));
    }

    std::string get_str(const std::string& k, const std::string& def = "") const {
        for (size_t i = 0; i < fields.size(); ++i)
            if (fields[i].first == k && fields[i].second.type == Value::T_STR)
                return fields[i].second.str;
        return def;
    }
    long long get_num(const std::string& k, long long def = 0) const {
        for (size_t i = 0; i < fields.size(); ++i)
            if (fields[i].first == k && fields[i].second.type == Value::T_NUM)
                return fields[i].second.num;
        return def;
    }
    std::vector<std::string> get_list(const std::string& k) const {
        for (size_t i = 0; i < fields.size(); ++i)
            if (fields[i].first == k && fields[i].second.type == Value::T_LIST)
                return fields[i].second.list;
        return std::vector<std::string>();
    }
};

static void skip_ws(const std::string& t, size_t& i) {
    while (i < t.size() && (t[i] == ' ' || t[i] == '\t' || t[i] == '\n' || t[i] == '\r')) ++i;
}

static bool append_utf8(std::string& out, unsigned long cp) {
    if (cp <= 0x7F) {
        out += (char)cp;
    } else if (cp <= 0x7FF) {
        out += (char)(0xC0 | (cp >> 6));
        out += (char)(0x80 | (cp & 0x3F));
    } else if (cp <= 0xFFFF) {
        out += (char)(0xE0 | (cp >> 12));
        out += (char)(0x80 | ((cp >> 6) & 0x3F));
        out += (char)(0x80 | (cp & 0x3F));
    } else if (cp <= 0x10FFFF) {
        out += (char)(0xF0 | (cp >> 18));
        out += (char)(0x80 | ((cp >> 12) & 0x3F));
        out += (char)(0x80 | ((cp >> 6) & 0x3F));
        out += (char)(0x80 | (cp & 0x3F));
    } else {
        return false;
    }
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
        if (c == '"') {
            ++i;
            return true;
        }
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
                        // 代理对（emoji 走这里）：紧跟第二个 \uXXXX
                        if (i + 1 >= t.size() || t[i] != '\\' || t[i + 1] != 'u') return false;
                        unsigned long lo = 0;
                        if (!hex4(t, i + 2, lo)) return false;
                        if (lo < 0xDC00 || lo > 0xDFFF) return false;
                        cp = 0x10000UL + ((cu - 0xD800UL) << 10) + (lo - 0xDC00UL);
                        i += 6;
                    } else if (cu >= 0xDC00 && cu <= 0xDFFF) {
                        return false;  // 孤立低代理
                    }
                    if (!append_utf8(out, cp)) return false;
                    break;
                }
                default:
                    return false;
            }
        } else if (c < 0x20) {
            return false;  // JSON 字符串内不允许裸控制字符
        } else {
            out += t[i];  // 普通字节（含 UTF-8 多字节序列）原样拷贝
            ++i;
        }
    }
    return false;  // 未闭合
}

static bool parse_number(const std::string& t, size_t& i, long long& out) {
    bool neg = false;
    if (i < t.size() && t[i] == '-') {
        neg = true;
        ++i;
    }
    if (i >= t.size() || t[i] < '0' || t[i] > '9') return false;
    long long v = 0;
    while (i < t.size() && t[i] >= '0' && t[i] <= '9') {
        v = v * 10 + (t[i] - '0');
        ++i;
    }
    if (i < t.size() && t[i] == '.') {  // 容忍小数（丢弃小数部分）
        ++i;
        while (i < t.size() && t[i] >= '0' && t[i] <= '9') ++i;
    }
    if (i < t.size() && (t[i] == 'e' || t[i] == 'E')) {  // 容忍指数
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
        if (i < t.size() && t[i] == ']') {
            ++i;
            out = Value::make_list(list);
            return true;
        }
        while (true) {
            skip_ws(t, i);
            std::string s;
            if (!parse_string(t, i, s)) return false;
            list.push_back(s);
            skip_ws(t, i);
            if (i < t.size() && t[i] == ',') {
                ++i;
                continue;
            }
            if (i < t.size() && t[i] == ']') {
                ++i;
                out = Value::make_list(list);
                return true;
            }
            return false;
        }
    }
    if (t.compare(i, 4, "null") == 0) {
        i += 4;
        out = Value::make_str("");
        return true;
    }
    return false;
}

bool parse(const std::string& text, Object& out) {
    size_t i = 0;
    skip_ws(text, i);
    if (i >= text.size() || text[i] != '{') return false;
    ++i;
    out.fields.clear();
    skip_ws(text, i);
    if (i < text.size() && text[i] == '}') {
        ++i;
        skip_ws(text, i);
        return i == text.size();
    }
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
        if (i < text.size() && text[i] == ',') {
            ++i;
            continue;
        }
        if (i < text.size() && text[i] == '}') {
            ++i;
            skip_ws(text, i);
            return i == text.size();  // 不允许尾部垃圾
        }
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
                } else {
                    o += (char)c;  // 含 UTF-8 多字节原样输出
                }
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
            case Value::T_STR:
                s += "\"" + escape_string(v.str) + "\"";
                break;
            case Value::T_NUM:
                s += std::to_string(v.num);
                break;
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

// ---------------- FrameReader：per-connection 累积缓冲，处理半包/粘包 ----------------

class FrameReader {
public:
    explicit FrameReader(size_t max_frame = kMaxFrame) : max_frame_(max_frame), failed_(false) {}

    void feed(const char* data, size_t len) {
        if (!failed_) buf_.append(data, len);
    }

    // 取出一帧完整 body；返回 false = 数据不够（或已 failed）
    bool next(std::string& body) {
        if (failed_) return false;
        if (buf_.size() < 4) return false;
        size_t len = ((size_t)(unsigned char)buf_[0] << 24) |
                     ((size_t)(unsigned char)buf_[1] << 16) |
                     ((size_t)(unsigned char)buf_[2] << 8) |
                     ((size_t)(unsigned char)buf_[3]);
        if (len > max_frame_) {  // 长度头超限：立即判协议错误，不等 body 到齐
            failed_ = true;
            return false;
        }
        if (buf_.size() < 4 + len) return false;  // 半包：等下次 feed
        body.assign(buf_, 4, len);                // 粘包：循环调用本函数逐帧取出
        buf_.erase(0, 4 + len);
        return true;
    }

    bool failed() const { return failed_; }

private:
    std::string buf_;
    size_t max_frame_;
    bool failed_;
};

// ---------------- FrameWriter：统一出口——加长度头 + send 循环（短写/EINTR/EAGAIN） ----------------

class FrameWriter {
public:
    explicit FrameWriter(socket_t fd) : fd_(fd) {}

    bool write_frame(const std::string& body) {
        if (body.size() > kMaxFrame) return false;
        std::string pkt;
        unsigned int n = (unsigned int)body.size();
        pkt += (char)((n >> 24) & 0xFF);
        pkt += (char)((n >> 16) & 0xFF);
        pkt += (char)((n >> 8) & 0xFF);
        pkt += (char)(n & 0xFF);
        pkt += body;
        return send_all(pkt.data(), pkt.size());
    }

private:
    bool send_all(const char* data, size_t len) {
        size_t sent = 0;
        while (sent < len) {
            long n = sock_send(fd_, data + sent, len - sent);
            if (n > 0) {
                sent += (size_t)n;
                continue;
            }
            if (n < 0 && sock_would_block()) {
                std::this_thread::yield();
                continue;
            }
            if (n < 0 && sock_interrupted()) continue;
            return false;
        }
        return true;
    }
    socket_t fd_;
};

// ---------------- ChatServer（阶段1：只做 PING→PONG） ----------------

class ChatServer {
public:
    explicit ChatServer(int port) : server_fd_(-1), port_(port) {}

    bool start() {
#ifdef _WIN32
        server_fd_ = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
#else
        server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
#endif
        if (!sock_valid(server_fd_)) {
            std::cerr << "创建 socket 失败" << std::endl;
            return false;
        }
        int opt = 1;
        setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, (const char*)&opt, sizeof(opt));

        sockaddr_in address;
        std::memset(&address, 0, sizeof(address));
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = INADDR_ANY;
        address.sin_port = htons((unsigned short)port_);
        if (bind(server_fd_, (sockaddr*)&address, sizeof(address)) < 0) {
            std::cerr << "绑定端口 " << port_ << " 失败" << std::endl;
            return false;
        }
        if (listen(server_fd_, 16) < 0) {
            std::cerr << "监听失败" << std::endl;
            return false;
        }

        std::cout << "==========================================" << std::endl;
        std::cout << "聊天服务器 v2 已启动（阶段1：帧层 + PING/PONG）" << std::endl;
        std::cout << "监听端口: " << port_ << "（绑定 0.0.0.0）" << std::endl;
        std::cout << "帧格式: [4字节大端长度][UTF-8 JSON body]，MAX_FRAME=1MiB" << std::endl;
        std::cout << "==========================================" << std::endl;
        std::cout << "等待客户端连接..." << std::endl;

        while (running_) {
            sockaddr_in client_addr;
            socklen_t client_len = sizeof(client_addr);
            socket_t client_socket = accept(server_fd_, (sockaddr*)&client_addr, &client_len);
            if (!sock_valid(client_socket)) {
                if (sock_interrupted()) continue;
                std::cerr << "accept 失败" << std::endl;
                continue;
            }
            std::cout << "[连接] " << inet_ntoa(client_addr.sin_addr) << ":"
                      << ntohs(client_addr.sin_port) << " 已接入" << std::endl;
            std::thread(&ChatServer::handleClient, this, client_socket).detach();
        }
        return true;
    }

    void stop() {
        running_ = false;
        if (sock_valid(server_fd_)) {
            sock_close(server_fd_);
            server_fd_ = (socket_t)-1;
        }
    }

private:
    void handleClient(socket_t fd);
    void handleBody(socket_t fd, const std::string& body, long long& send_seq);

    socket_t server_fd_;
    int port_;
    std::atomic<bool> running_{true};
};

void ChatServer::handleClient(socket_t fd) {
    FrameReader reader;
    char chunk[4096];
    long long send_seq = 0;  // 本连接下行帧序号（发送方递增）

    while (running_) {
        long n = sock_recv(fd, chunk, sizeof(chunk));  // 阻塞 recv（拍板项1→A，无空转）
        if (n > 0) {
            reader.feed(chunk, (size_t)n);
            std::string body;
            while (reader.next(body)) {
                handleBody(fd, body, send_seq);
            }
            if (reader.failed()) {
                std::cout << "[协议错误] 长度头超限或非法，断开连接" << std::endl;
                break;
            }
        } else if (n == 0) {
            std::cout << "[断开] 对端关闭连接" << std::endl;
            break;
        } else {
            if (sock_interrupted()) continue;
            std::cout << "[错误] recv 出错" << std::endl;
            break;
        }
    }
    sock_close(fd);
}

void ChatServer::handleBody(socket_t fd, const std::string& body, long long& send_seq) {
    std::cout << "[收到帧] " << body << std::endl;
    minijson::Object obj;
    if (!minijson::parse(body, obj)) {
        std::cout << "[错误] JSON 解析失败" << std::endl;
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
        pong.set_str("content", obj.get_str("content"));  // 原样回显，供完整性校验
        pong.set_num("ts", now_ts());
        pong.set_num("seq", ++send_seq);
        FrameWriter writer(fd);
        if (writer.write_frame(minijson::serialize(pong))) {
            std::cout << "[发送帧] PONG seq=" << send_seq << std::endl;
        } else {
            std::cout << "[错误] PONG 发送失败" << std::endl;
        }
    } else {
        std::cout << "[阶段1] 未处理类型: " << type << "（业务消息在阶段2实现）" << std::endl;
    }
}

static void signalHandler(int signum) {
    std::cout << "\n[服务器] 收到信号 " << signum << "，正在关闭..." << std::endl;
    std::exit(signum);
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
    std::signal(SIGINT, signalHandler);
    std::signal(SIGTERM, signalHandler);

    int port = 8888;
    if (argc > 1) port = std::atoi(argv[1]);

    std::cout << "正在启动聊天服务器 v2 ..." << std::endl;
    ChatServer server(port);
    bool ok = server.start();
    server.stop();
#ifdef _WIN32
    WSACleanup();
#endif
    return ok ? 0 : 1;
}
