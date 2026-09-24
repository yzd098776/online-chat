// tools/bench_client.cpp —— C++ 压测客户端（P1：绕开 Python GIL，标定服务器承压上限）
//
// 为什么要有它：tools/bench.py 是单进程 Python（GIL + 调度抖动），100 连接×5 msg/s 就到头，
// P99 里混着客户端噪声——测不出服务器极限。本工具按「T 线程各开 N/T 连接」压测：
//   · 直接复用 frame.* / minijson / pwd_hash（与服务器同源编解码/凭据计算）
//   · 每线程 poll() 事件循环 + 自主定节奏（rate msg/s/连接；rate<=0 闭环尽力打满）
//   · 延迟样本 = MESSAGE 发出 → ACK 到达，单调时钟，无解释器抖动
// 口径与 bench.py 一致：
//   QPS = 测量窗口内 ACKed/s（服务器先写 SQLite 再 ACK，端到端含落盘）；
//   CPU/RSS = /proc/<pid>/{stat,statm} 采样（--pid 或自动找 chat_server_v5）。
// 认证走挑战-应答（Q7）：*_HELLO → CHALLENGE → 证明帧（与正式协议一致）。
//
// 用法:
//   ./bench_client --port 8888 --connections 100 --threads 20 --rate 5 --duration 10
//   ./bench_client --port 8888 --connections 500 --threads 20 --rate 0   # 找上限
//   ./bench_client --room lobby ...            # 全员同房（含广播扇出放大）
// 输出: stdout 汇总 + 追加 tools/bench_result.csv（与 bench.py 同 schema，便于同表对比）
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <poll.h>
#include <sys/select.h>

#include "chat/frame.h"
#include "chat/minijson.h"
#include "chat/net.h"
#include "chat/pwd_hash.h"

namespace {

double now_s() {
    using namespace std::chrono;
    return duration_cast<duration<double> >(steady_clock::now().time_since_epoch()).count();
}

double percentile(std::vector<double>& v, double p) {
    if (v.empty()) return 0.0;
    std::sort(v.begin(), v.end());
    double k = (double)(v.size() - 1) * p / 100.0;
    size_t lo = (size_t)k, hi = lo + 1 < v.size() ? lo + 1 : lo;
    return v[lo] + (v[hi] - v[lo]) * (k - (double)lo);
}

// ---------------- /proc 采样（服务器 CPU% / RSS 峰值） ----------------

struct ProcSampler {
    int pid;
    std::atomic<bool> stop;
    std::atomic<long long> rss_peak;
    double cpu_pct;
    double first_t, wall;
    long long first_jiffies, last_jiffies;
    std::thread th;

    explicit ProcSampler(int p)
        : pid(p), stop(false), rss_peak(0), cpu_pct(0), first_t(0), wall(0),
          first_jiffies(-1), last_jiffies(0) {}

    void read_once(double t) {
        char path[64], buf[512];
        long long jiffies = 0;
        std::snprintf(path, sizeof(path), "/proc/%d/stat", pid);
        FILE* f = std::fopen(path, "r");
        if (f) {
            size_t n = std::fread(buf, 1, sizeof(buf) - 1, f);
            buf[n] = 0;
            std::fclose(f);
            char* rparen = std::strrchr(buf, ')');
            if (rparen) {  // pid (comm) state ... utime=14 字段 stime=15
                int idx = 0;
                long long utime = 0, stime = 0;
                for (char* tok = std::strtok(rparen + 2, " "); tok;
                     tok = std::strtok(NULL, " "), ++idx) {
                    if (idx == 11) utime = std::atoll(tok);
                    if (idx == 12) { stime = std::atoll(tok); break; }
                }
                jiffies = utime + stime;
            }
        }
        std::snprintf(path, sizeof(path), "/proc/%d/statm", pid);
        f = std::fopen(path, "r");
        if (f) {
            long long total = 0, rss = 0;
            if (std::fscanf(f, "%lld %lld", &total, &rss) == 2) {
                long long bytes = rss * 4096;
                if (bytes > rss_peak) rss_peak = bytes;
            }
            std::fclose(f);
        }
        if (first_jiffies < 0) { first_jiffies = jiffies; first_t = t; }
        last_jiffies = jiffies;
        wall = t - first_t;
    }

    void start() {
        th = std::thread([this]() {
            while (!stop.load()) {
                read_once(now_s());
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }
        });
    }
    void stop_and_sum() {
        stop = true;
        if (th.joinable()) th.join();
        read_once(now_s());
        if (wall > 0 && last_jiffies > first_jiffies)
            cpu_pct = (double)(last_jiffies - first_jiffies) / 100.0 / wall * 100.0;
    }
};

int find_server_pid() {
    // 按 /proc/<pid>/exe 精确匹配（pgrep -f 会误抓外壳进程，bench.py 头注释有血泪史）
    FILE* pipe = popen("ls -l /proc/*/exe 2>/dev/null | grep -F '/chat_server_v5' | "
                       "sed -E 's|/proc/([0-9]+)/.*|\\1|' | head -1",
                       "r");
    if (!pipe) return 0;
    char buf[64] = {0};
    if (!std::fgets(buf, sizeof(buf), pipe)) { pclose(pipe); return 0; }
    pclose(pipe);
    return std::atoi(buf);
}

// ---------------- 单连接压测状态 ----------------

struct Conn {
    socket_t fd;
    chat::FrameReader reader;
    std::string user;
    long long seq;
    long long sent, acked, nacked, lost;
    bool ready;
    double next_send;
    std::unordered_map<long long, double> pending;  // seq -> 发出时刻
    std::vector<double> lats;                       // 本连接 RTT 样本（ms）
    Conn() : fd(-1), reader(), seq(0), sent(0), acked(0), nacked(0), lost(0),
             ready(false), next_send(0) {}
};

struct Options {
    std::string host;
    int port, connections, threads;
    double rate, duration, warmup;
    std::string room;   // "private" = 每连接独立房（纯 RTT）；其它 = 全员同房（扇出）
    int pid;
    std::string csv;
    Options() : host("127.0.0.1"), port(8888), connections(100), threads(20),
                rate(5), duration(10), warmup(2), room("private"), pid(0),
                csv("tools/bench_result.csv") {}
};

// ---------------- 帧收发（与服务器同源 make_wire_frame/FrameReader） ----------------

bool send_obj(socket_t fd, const minijson::Object& obj) {
    std::string pkt = chat::make_wire_frame(minijson::serialize(obj));
    size_t sent = 0;
    while (sent < pkt.size()) {
        long n = chat::sock_send(fd, pkt.data() + sent, pkt.size() - sent);
        if (n > 0) { sent += (size_t)n; continue; }
        if (n < 0 && chat::sock_interrupted()) continue;
        return false;
    }
    return true;
}

minijson::Object make_msg(const char* type, const std::string& from, const char* to,
                          const std::string& content, long long seq) {
    minijson::Object o;
    o.set_num("ver", 1);
    o.set_str("type", type);
    o.set_str("from", from);
    o.set_str("to", to);
    o.set_str("room", "");
    o.set_str("content", content);
    o.set_num("ts", (long long)time(NULL));
    o.set_num("seq", seq);
    return o;
}

// 收一帧（阻塞至超时）；true=got
bool recv_obj(Conn& c, minijson::Object& out, double timeout_s) {
    double deadline = now_s() + timeout_s;
    char buf[4096];
    std::string body;
    for (;;) {
        if (c.reader.next(body)) return minijson::parse(body, out);
        double left = deadline - now_s();
        if (left <= 0) return false;
        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(c.fd, &rfds);
        timeval tv;
        tv.tv_sec = (long)left;
        tv.tv_usec = (long)((left - (double)tv.tv_sec) * 1e6);
        int r = select((int)c.fd + 1, &rfds, NULL, NULL, &tv);
        if (r <= 0) return false;
        long n = chat::sock_recv(c.fd, buf, sizeof(buf));
        if (n <= 0) return false;
        c.reader.feed(buf, (size_t)n);
    }
}

// 等指定类型（跳过噪声）；ERR 抛给调用方看 code
bool wait_type(Conn& c, const char* want, minijson::Object& out, double timeout_s = 15.0) {
    double deadline = now_s() + timeout_s;
    while (now_s() < deadline) {
        minijson::Object f;
        if (!recv_obj(c, f, deadline - now_s())) return false;
        std::string t = f.get_str("type");
        if (t == want) { out = f; return true; }
        if (t == "ERR") { out = f; return true; }  // 调用方按 type 分辨
        // USERLIST/SYSTEM/PONG/MESSAGE 等噪声跳过
    }
    return false;
}

bool wait_any(Conn& c, const char* a, const char* b, minijson::Object& out,
              double timeout_s = 15.0) {
    double deadline = now_s() + timeout_s;
    while (now_s() < deadline) {
        minijson::Object f;
        if (!recv_obj(c, f, deadline - now_s())) return false;
        std::string t = f.get_str("type");
        if (t == a || t == b) { out = f; return true; }
    }
    return false;
}

// 挑战-应答（Q7）：*_HELLO → CHALLENGE → 证明帧。pwd_hash 与服务器同源。
bool challenge_step(Conn& c, bool is_register, const std::string& pwd,
                    minijson::Object& final_out) {
    minijson::Object hello = make_msg(is_register ? "REGISTER_HELLO" : "LOGIN_HELLO",
                                      c.user, "SERVER", "", ++c.seq);
    if (!send_obj(c.fd, hello)) return false;
    minijson::Object ch;
    if (!wait_any(c, "CHALLENGE", "ERR", ch)) return false;
    if (ch.get_str("type") == "ERR") { final_out = ch; return true; }
    std::string k_hex = pwd_hash::hash_password(pwd, ch.get_str("salt"));
    // proof = HMAC(K, op‖user‖nonce)（Q7 纵深防御，与正式客户端同式）
    std::string proof = pwd_hash::challenge_proof(k_hex, is_register ? "REGISTER" : "LOGIN",
                                                 c.user, ch.get_str("content"));
    if (is_register) {
        minijson::Object reg = make_msg("REGISTER", c.user, "SERVER", k_hex, ++c.seq);
        reg.set_str("hmac", proof);
        if (!send_obj(c.fd, reg)) return false;
        return wait_any(c, "REGISTER_OK", "ERR", final_out);
    }
    minijson::Object login = make_msg("LOGIN", c.user, "SERVER", proof, ++c.seq);
    if (!send_obj(c.fd, login)) return false;
    return wait_any(c, "AUTH_OK", "ERR", final_out);
}

bool setup_conn(Conn& c, const Options& opt, int idx) {
    sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((unsigned short)opt.port);
    if (inet_pton(AF_INET, opt.host.c_str(), &addr.sin_addr) != 1) return false;
    c.fd = socket(AF_INET, SOCK_STREAM, 0);
    if (!chat::sock_valid(c.fd)) return false;
    if (connect(c.fd, (sockaddr*)&addr, sizeof(addr)) < 0) return false;
    chat::apply_socket_opts(c.fd);
    c.user = "cbench" + std::to_string(idx);

    // 注册（E1001=已注册，忽略）
    minijson::Object f;
    if (!challenge_step(c, true, "cbench-pw-1", f)) return false;
    if (f.get_str("type") == "ERR" && f.get_num("code") != 1001) return false;

    // 登录（E1004=上一轮残留同名在线，稍候重试）
    bool authed = false;
    for (int attempt = 0; attempt < 4 && !authed; ++attempt) {
        if (!challenge_step(c, false, "cbench-pw-1", f)) return false;
        if (f.get_str("type") == "AUTH_OK") { authed = true; break; }
        if (f.get_num("code") != 1004) return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(800));
    }
    if (!authed) return false;

    // 建房 + 进房（private=每连接独立房；shared=全员同房，首个建房）
    std::string room = (opt.room == "private")
                           ? ("cbw" + std::to_string(idx))
                           : opt.room;
    minijson::Object cr = make_msg("CREATE", c.user, "SERVER", room, ++c.seq);
    send_obj(c.fd, cr);
    recv_obj(c, f, 3.0);  // CREATE_OK / E2001，都无所谓
    minijson::Object join = make_msg("JOIN", c.user, "SERVER", room, ++c.seq);
    if (!send_obj(c.fd, join)) return false;
    if (!wait_type(c, "JOIN_OK", f)) return false;
    if (f.get_str("type") != "JOIN_OK") return false;
    // 清进房 USERLIST/SYSTEM 噪声
    while (recv_obj(c, f, 0.15)) {
    }
    c.ready = true;
    return true;
}

// 排干连接前缓冲（MSG_DONTWAIT，读到 EAGAIN 即返）并处理完整帧
void drain_conn(Conn& c) {
    char buf[8192];
    for (;;) {
        long n = chat::sock_recv_nb(c.fd, buf, sizeof(buf));
        if (n <= 0) {
            if (n < 0 && chat::sock_would_block()) break;
            if (n < 0 && chat::sock_interrupted()) continue;
            return;  // 关闭/错误：本轮不再读
        }
        c.reader.feed(buf, (size_t)n);
        std::string body;
        while (c.reader.next(body)) {
            minijson::Object f;
            if (!minijson::parse(body, f)) continue;
            std::string t = f.get_str("type");
            if (t == "ACK") {  // content = 被确认的客户端 seq（十进制字符串）
                long long seq = std::atoll(f.get_str("content").c_str());
                std::unordered_map<long long, double>::iterator it = c.pending.find(seq);
                if (it != c.pending.end()) {
                    c.lats.push_back((now_s() - it->second) * 1000.0);
                    c.pending.erase(it);
                    ++c.acked;
                }
            } else if (t == "NACK") {
                long long seq = std::atoll(f.get_str("content").c_str());
                if (c.pending.erase(seq)) ++c.nacked;
            }
        }
    }
}

void worker_fn(int tid, const Options* opt, std::vector<Conn>* conns,
               std::atomic<int>* ready_count, std::atomic<int>* setup_done,
               std::atomic<bool>* go, std::atomic<bool>* stop) {
    // ---- 建连/认证/进房（组内串行，组间并行） ----
    for (size_t i = 0; i < conns->size(); ++i) {
        Conn& c = (*conns)[i];
        if (!setup_conn(c, *opt, tid * 10000 + (int)i)) {
            std::fprintf(stderr, "[warn] 连接 %s 建立/认证失败\n", c.user.c_str());
            continue;
        }
        ++(*ready_count);
    }
    ++(*setup_done);
    while (!go->load()) std::this_thread::sleep_for(std::chrono::milliseconds(1));

    // ---- 压测：暖机 → 测量。一次 poll 本线程全部 fd（压测端自己绝不能 O(N) 串行阻塞）----
    double interval = (opt->rate > 0) ? 1.0 / opt->rate : 0.0;
    double t_begin = now_s();
    double t_measure_at = t_begin + opt->warmup;
    double t_end = t_measure_at + opt->duration;
    for (size_t i = 0; i < conns->size(); ++i) (*conns)[i].next_send = t_begin;

    std::vector<pollfd> pfds;
    bool measuring = false;
    for (;;) {
        double now = now_s();
        if (now >= t_end || stop->load()) break;
        if (!measuring && now >= t_measure_at) {
            measuring = true;
            for (size_t i = 0; i < conns->size(); ++i) {  // 暖机样本丢弃
                Conn& c = (*conns)[i];
                c.sent = c.acked = c.nacked = c.lost = 0;
                c.pending.clear();
                c.lats.clear();
            }
        }
        // 1) 到点的连接发消息（rate 定节奏；rate=0 闭环打满）
        double next_wake = t_end;
        for (size_t i = 0; i < conns->size(); ++i) {
            Conn& c = (*conns)[i];
            if (!c.ready || c.fd < 0) continue;
            if (now >= c.next_send) {
                ++c.seq;
                minijson::Object m = make_msg("MESSAGE", c.user, "ALL",
                                              "bench " + std::to_string(c.seq), c.seq);
                double t0 = now_s();
                if (send_obj(c.fd, m) && measuring) {
                    ++c.sent;
                    c.pending[c.seq] = t0;
                }
                c.next_send = (interval > 0) ? c.next_send + interval : now_s();
                if (c.next_send < now) c.next_send = now;  // 掉队顺延，不补偿突发
            }
            if (c.next_send < next_wake) next_wake = c.next_send;
        }
        // 2) poll 全部连接：超时 = 下一个发送时刻（上限 20ms，保收帧及时）
        double wait_s = std::min(next_wake, now + 0.02) - now_s();
        int wait_ms = wait_s <= 0 ? 0 : (int)(wait_s * 1000.0);
        std::vector<size_t> pidx;
        for (size_t i = 0; i < conns->size(); ++i)
            if ((*conns)[i].ready && (*conns)[i].fd >= 0) pidx.push_back(i);
        pfds.clear();
        for (size_t k = 0; k < pidx.size(); ++k) {
            pollfd p;
            p.fd = (*conns)[pidx[k]].fd;
            p.events = POLLIN;
            p.revents = 0;
            pfds.push_back(p);
        }
        if (!pfds.empty()) poll(&pfds[0], (nfds_t)pfds.size(), wait_ms);
        // 只排 poll 报可读的连接（盲扫全部连接的 recv_nb 是纯 syscall 浪费——单核压测端
        // 被自己的扫尾拖到 ~1400/s 定速上限，修正后同负载对照才可信）
        for (size_t k = 0; k < pfds.size(); ++k)
            if (pfds[k].revents & POLLIN) drain_conn((*conns)[pidx[k]]);
    }
    // 尾窗：给在途 ACK 1.5s
    double tail_end = now_s() + 1.5;
    while (now_s() < tail_end) {
        pfds.clear();
        for (size_t i = 0; i < conns->size(); ++i) {
            if (!(*conns)[i].ready || (*conns)[i].fd < 0) continue;
            pollfd p;
            p.fd = (*conns)[i].fd;
            p.events = POLLIN;
            p.revents = 0;
            pfds.push_back(p);
        }
        if (!pfds.empty()) poll(&pfds[0], (nfds_t)pfds.size(), 50);
        for (size_t i = 0; i < conns->size(); ++i) {
            Conn& c = (*conns)[i];
            if (c.ready && c.fd >= 0) drain_conn(c);
        }
    }
    for (size_t i = 0; i < conns->size(); ++i) {
        Conn& c = (*conns)[i];
        c.lost = (long long)c.pending.size();
        if (c.fd >= 0) { chat::sock_close(c.fd); c.fd = -1; }
    }
}

}  // namespace

int main(int argc, char** argv) {
    Options opt;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--host" && i + 1 < argc) opt.host = argv[++i];
        else if (a == "--port" && i + 1 < argc) opt.port = std::atoi(argv[++i]);
        else if (a == "--connections" && i + 1 < argc) opt.connections = std::atoi(argv[++i]);
        else if (a == "--threads" && i + 1 < argc) opt.threads = std::atoi(argv[++i]);
        else if (a == "--rate" && i + 1 < argc) opt.rate = std::atof(argv[++i]);
        else if (a == "--duration" && i + 1 < argc) opt.duration = std::atof(argv[++i]);
        else if (a == "--warmup" && i + 1 < argc) opt.warmup = std::atof(argv[++i]);
        else if (a == "--room" && i + 1 < argc) opt.room = argv[++i];
        else if (a == "--pid" && i + 1 < argc) opt.pid = std::atoi(argv[++i]);
        else if (a == "--csv" && i + 1 < argc) opt.csv = argv[++i];
    }
    if (opt.connections < 1) opt.connections = 1;
    if (opt.threads < 1) opt.threads = 1;
    if (opt.threads > opt.connections) opt.threads = opt.connections;

    std::printf("目标 %s:%d  并发=%d  线程=%d  速率=%.2f msg/s/连接  暖机=%.0fs 窗口=%.0fs  房间=%s\n",
                opt.host.c_str(), opt.port, opt.connections, opt.threads, opt.rate,
                opt.warmup, opt.duration, opt.room.c_str());
    int pid = opt.pid ? opt.pid : find_server_pid();
    std::printf("服务器 pid=%d（CPU/内存采样%s）\n", pid, pid ? "开" : "关——传 --pid 启用");

    std::vector<std::vector<Conn> > per_thread((size_t)opt.threads);
    for (int i = 0; i < opt.connections; ++i)
        per_thread[(size_t)(i % opt.threads)].push_back(Conn());

    std::atomic<int> ready_count(0), setup_done(0);
    std::atomic<bool> go(false), stop(false);
    std::vector<std::thread> pool;
    for (int t = 0; t < opt.threads; ++t)
        pool.push_back(std::thread(worker_fn, t, &opt, &per_thread[(size_t)t],
                                   &ready_count, &setup_done, &go, &stop));

    // 等全部线程 setup 结束（setup_done 计数，不用「就绪数稳定」启发式）
    int last = -1;
    while (setup_done.load() < opt.threads) {
        int r = ready_count.load();
        if (r != last) {
            std::printf("\r就绪连接 %d/%d", r, opt.connections);
            std::fflush(stdout);
            last = r;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    std::printf("\r就绪连接 %d/%d（失败 %d）\n", ready_count.load(), opt.connections,
                opt.connections - ready_count.load());
    if (ready_count.load() == 0) {
        stop = true;
        go = true;
        for (size_t t = 0; t < pool.size(); ++t) pool[t].join();
        return 1;
    }

    ProcSampler sampler(pid);
    if (pid) sampler.start();
    go = true;
    // 等 worker 跑完（暖机+测量+尾窗）
    for (size_t t = 0; t < pool.size(); ++t) pool[t].join();
    if (pid) sampler.stop_and_sum();

    // ---- 汇总 ----
    long long sent = 0, acked = 0, nacked = 0, lost = 0;
    std::vector<double> lats;
    for (int t = 0; t < opt.threads; ++t) {
        for (size_t i = 0; i < per_thread[(size_t)t].size(); ++i) {
            Conn& c = per_thread[(size_t)t][i];
            if (!c.ready) continue;
            sent += c.sent;
            acked += c.acked;
            nacked += c.nacked;
            lost += c.lost;
            lats.insert(lats.end(), c.lats.begin(), c.lats.end());
        }
    }
    double qps = (opt.duration > 0) ? (double)acked / opt.duration : 0;
    double p50 = percentile(lats, 50), p90 = percentile(lats, 90), p99 = percentile(lats, 99);
    double avg = 0, max = lats.empty() ? 0 : lats.back();  // percentile 已排序
    for (size_t i = 0; i < lats.size(); ++i) avg += lats[i];
    if (!lats.empty()) avg /= (double)lats.size();
    max = lats.empty() ? 0 : *std::max_element(lats.begin(), lats.end());

    std::printf("\n| 指标 | 数值 |\n|---|---|\n");
    std::printf("| 发送 / ACK / NACK / 丢失 | %lld / %lld / %lld / %lld |\n",
                sent, acked, nacked, lost);
    std::printf("| **QPS（ACKed/s）** | **%.1f** |\n", qps);
    std::printf("| 延迟 avg | %.3f ms |\n", avg);
    std::printf("| **延迟 P50** | **%.3f ms** |\n", p50);
    std::printf("| 延迟 P90 | %.3f ms |\n", p90);
    std::printf("| **延迟 P99** | **%.3f ms** |\n", p99);
    std::printf("| 延迟 max | %.3f ms |\n", max);
    std::printf("| CPU（窗口均值） | %.1f %% |\n", sampler.cpu_pct);
    std::printf("| 内存 RSS 峰值 | %.1f MB |\n", (double)sampler.rss_peak.load() / 1048576.0);

    // CSV 追加（与 bench.py 同 schema：mode 前缀 cpp+ 便于区分压测端）
    FILE* f = std::fopen(opt.csv.c_str(), "a");
    if (f) {
        bool header = (std::fseek(f, 0, SEEK_END) == 0);
        long sz = ftell(f);
        if (header && sz == 0)
            std::fprintf(f, "ts,mode,connections,rate_per_conn,duration_s,sent,acked,"
                            "nacked,lost,qps,avg_ms,p50_ms,p90_ms,p99_ms,max_ms,"
                            "cpu_pct,rss_peak_mb\n");
        char ts[32];
        time_t now = time(NULL);
        std::strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", std::localtime(&now));
        std::fprintf(f, "%s,cpp+%s,%d,%.2f,%.2f,%lld,%lld,%lld,%lld,%.1f,%.3f,%.3f,"
                        "%.3f,%.3f,%.3f,%.1f,%.1f\n",
                     ts, (opt.room == "private") ? "private-rooms" : ("shared#" + opt.room).c_str(),
                     ready_count.load(), opt.rate, opt.duration, sent, acked, nacked, lost,
                     qps, avg, p50, p90, p99, max, sampler.cpu_pct,
                     (double)sampler.rss_peak.load() / 1048576.0);
        std::fclose(f);
        std::printf("\n已追加：%s\n", opt.csv.c_str());
    }
    return 0;
}
