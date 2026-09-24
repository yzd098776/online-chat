// src/server.cpp —— 聊天服务器 v5 实现
//
// 与既有版本的关系（历史源码在 legacy/）：
//   v1 (chat_server_fixed.cpp)  完整业务，`|`+`\n` 文本协议（对照用）
//   v2 (chat_server_v2.cpp)     长度前缀+JSON 帧层 + PING/PONG
//   v3 (chat_server_v3.cpp)     Reactor 性能实验（epoll ET + 线程池，仅帧层）
//   v4 (chat_server_v4.cpp)     可靠性改造（心跳/幂等去重/离线 E2EACK，保留对照）
//   v5 (本目录工程化版)          账号 + 房间 + 持久化/游标分页；模块拆分 + 日志 + 单测 + CMake
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
//    v5 的 chat::RoomRouter 维护 room → members（用户名集合）与 user → room 映射：
//    房间消息只遍历【本房间成员】（roomConns → broadcastRoom*）：O(房间成员数)。
//    USERLIST/进出房通知同样限定本房间。1000 人在线但各在 10 个百人房时，单条广播成本
//    从 1000 降到 ~100，还天然实现了「广播只发给本房间成员」的隔离语义。
//    成员集按【用户名】而非连接：Token 顶号续传时用户对房间内其他人"从没离开过"（Q5）。
// Q3 为什么深分页用 (ts, id) 游标，而不是 LIMIT/OFFSET？
//    LIMIT/OFFSET 的语义是「扫过前 OFFSET 行再取 50 行」：第 100 页要先白白丢掉 4950 行，
//    页越深成本线性涨（实测 OFFSET 89000 ≈ 3.7ms，游标深页 ≈ 0.045ms，见 seed_and_bench 输出）；
//    且翻页期间有新消息插入时 OFFSET 会「跳行/重行」（窗口漂移）。
//    游标（keyset）把「上一页最后一行的 (ts, id)」当书签，WHERE (ts,id)<(?,?) 直接 seek 到
//    B-Tree 的对应位置：深度无关 O(log n + 50)；(ts,id) 是唯一键（id=主键），严格全序，
//    插入新消息不影响旧书签，永不重不漏（seed_and_bench.py 对 100 页×50 条做了不重不漏校验）。
//    为什么是 (ts, id) 两个字段：ts 秒级，同一秒可有多条消息，只按 ts 裁剪会把同秒的
//    边界消息跳过/重复；id 做 tie-break，行值比较 (ts,id)<(?,?) 让索引仍可用
//    （见 Q1 的 EXPLAIN：room_id=? AND ts<?）。OFFSET 的唯一优势是「跳页」（直接跳到
//    第 100 页），聊天记录是顺序上翻场景，游标是正确工具。
// Q4 Token 怎么设计？重连免密怎么生效？
//    登录成功（挑战-应答通过，见 Q7）后 chat::TokenBook 生成 Token：32 字节 CSPRNG
//    （/dev/urandom）+ 过期时间（--token-ttl，默认 7 天），hex(64 字符) 随 AUTH_OK 下发；
//    客户端存本地会话文件（0600）。之后所有业务帧以连接为信任边界（服务器认「已通过
//    AUTH/LOGIN 的连接」，from 字段不可信，一律取会话身份——防伪造他人身份）；
//    客户端断线重连免密：新连接直接 AUTH(token)。
//    Token 【内存只存 SHA256(token)】：token 是持有者凭证，明文进内存表则 core dump/
//    内存转储一泄等同于永久凭证；表键存哈希后，泄表只能「验」不能「冒」（token_book.h）。
//    服务器重启后表失效，客户端回落口令登录（教学取舍——生产应存 tokens 表/Redis +
//    吊销与滑动续期）。明文 TCP 下 Token 仍有被窃听重放的风险，生产必须 TLS
//    （README「已知限制」），且不该写进日志。
// Q5 同名「重复登录」怎么处理？
//    口令 LOGIN 时该账号已在线 → 显式拒绝 E1004「该账号已在线，不允许重复登录」
//    （不静默失败；同一连接重复 LOGIN/AUTH → E1005）。Token AUTH（重连免密的恢复路径）
//    则走「顶号」：先给旧连接发 SYSTEM 明确说明「已在其他连接恢复会话」再踢——弱网重连时
//    旧连接往往还是半开僵尸，若也拒绝，用户会被自己的尸体锁在门外直到心跳超时（v4 Q4 论证）。
//    两条路径的区别是身份强度：口令=人在另一台机器前操作（拒绝），Token=同一会话续传（顶替）。
// Q6 所有 SQL 为什么一律参数化绑定？
//    字符串拼接 SQL = 注入面：用户名/房间名/消息内容里的 ' OR '1'='1、';DROP TABLE ...;
//    都会被当成 SQL 语法执行。src/database.cpp【所有】用户数据路径都走 sqlite3_prepare_v2 +
//    sqlite3_bind_text/int64（占位符 ?），数据永远不进入 SQL 文本——SQLite 把绑定值当
//    纯数据解析，无论内容里有什么字符。DDL/PRAGMA 等无用户输入的固定串才用 sqlite3_exec。
//    面试常问「参数化为什么防注入」：不是转义，是【语法与数据分离】——解析器在见到绑定值
//    之前已经完成语句语法分析，绑定值没有机会改变语句结构。
// Q7 口令为什么不上线？登录怎么做？（挑战-应答，凭据安全）
//    【口令不进帧】。注册/登录都走三步：*_HELLO{user} → 服务端回 CHALLENGE{nonce, salt}
//    → 客户端回证明帧。salt = 16 字节随机（注册时现发、登录时取库内），nonce = 32 字节
//    CSPRNG、【一次性 + 60s 过期】（takeChallenge 用后即清——同一次抓包无法重放）。
//    ① 登录：客户端派生 K = PBKDF2-HMAC-SHA256(pwd, salt, 100000)，LOGIN 只带
//       proof = HMAC-SHA256(K, nonce)。服务端用库内存的派生值重算 proof，恒定时间比较。
//       帧里出现过的最强材料是 nonce 绑定的 HMAC——nonce 消费后即作废，抓包不可重放、
//       也不泄露口令/K。库里永远只有派生值（Q6 的 users 表设计不变）。
//    ② 注册：K 必须一次性送达服务器才能播种（无 PKI/PAKE 时的理论下界——服务器必须
//       拿到「能验证口令的东西」）。REGISTER 带 K 与 proof=HMAC(K, nonce)（nonce 绑定，
//       注册帧同样不可重放）。口令本身仍然不进帧——帧内最强材料从「明文口令」降为
//       「入库值 K」，且仅此一次。建号建议走 TLS（README「已知限制」）。
//    ③ 与 TLS 的关系（答辩话术）：TLS 是【通道安全】（防窃听/篡改整条链路），挑战-应答
//       是【凭据安全】（口令与 K 不上网、证明不可重放），两者正交——做了后者，前者可以
//       按需开（--tls），但 Token 传输等仍建议 TLS（见 Q4）。
//    错误语义不静默：无挑战/过期/错配 → E1009；proof 错 → E1003；其余沿用 E1002/E1004 等。
// =================================================================

#include "chat/server.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>

#include "chat/log.h"
#include "chat/pepper.h"
#include "chat/protocol.h"
#include "chat/pwd_hash.h"
#include "chat/util.h"

namespace chat {

static const int kHistPageSize = 50;  // 需求：进入房间拉最近 50 条

ClientConn::ClientConn()
#ifdef _WIN32
    : fd(INVALID_SOCKET),
#else
    : fd(-1),
#endif
      id(0),
      last_active(0),
      authed(false),
      send_seq(0),
      tls(NULL) {
    chal.active = false;
    chal.is_register = false;
    chal.exp_ms = 0;
}

ClientConn::~ClientConn() {
    delete tls;
    tls = NULL;
}

ChatServer::ChatServer(const ServerConfig& cfg)
    : listen_fd_(-1), cfg_(cfg), tokens_(cfg.token_ttl),
      dedup_(),
      conn_limit_(cfg.conn_rate > 0 ? cfg.conn_rate : 1e18,
                  cfg.conn_burst > 0 ? cfg.conn_burst : 1e18),
      msg_limit_(cfg.msg_rate > 0 ? cfg.msg_rate : 1e18,
                 cfg.msg_burst > 0 ? cfg.msg_burst : 1e18),
      next_id_(0), running_(false) {
#ifdef _WIN32
    listen_fd_ = INVALID_SOCKET;
#endif
}

bool ChatServer::start() {
    if (!db_.open(cfg_.db_path)) return false;
    pepper_hex_ = load_or_create_pepper(cfg_.pepper_path);  // Q7②：盲化用 pepper（自举 0600）
    if (pepper_hex_.size() != 64) {
        // 双保险：盲化绝不静默失效（load_or_create_pepper 内部已 fail-fast）
        std::fprintf(stderr, "FATAL: pepper 初始化异常（长度 %zu != 64）\n", pepper_hex_.size());
        std::abort();
    }
    // pepper 与库的一致性闸门（与 v1 库拒绝同一类防御）：换错 pepper 开旧库时，每个用户
    // 都表现为「口令错误」，排查极痛苦——启动期就把「配置与状态不匹配」显式报出来
    std::string pid_now = pwd_hash::pepper_id(pepper_hex_);
    std::string pid_db = db_.meta_get("pepper_id");
    if (pid_db.empty()) {
        db_.meta_set("pepper_id", pid_now);  // 空库首次绑定
    } else if (pid_db != pid_now) {
        LOG_ERROR() << "pepper 与库不匹配（库 pepper_id=" << pid_db << "，当前 pepper="
                    << pid_now << "）：拒绝启动，否则所有用户登录都会表现为「密码错误」——"
                    << "请用正确的 " << cfg_.pepper_path << " 重启（边界：丢了 pepper = 全库"
                    << "盲化解不开，备份须与 DB 分开保管）";
        return false;
    }
    if (!words_.empty() || !cfg_.words_file.empty()) {
        size_t n = words_.load_file(cfg_.words_file);
        LOG_INFO() << "敏感词表: " << cfg_.words_file << " 载入 " << n << " 词（"
                   << (cfg_.filter_reject ? "reject 拒绝发送" : "replace 替换 *") << "）";
    }
    if (cfg_.tls) {
        if (!tls_ctx_.init_server(cfg_.tls_cert, cfg_.tls_key)) {
            LOG_ERROR() << "TLS 初始化失败，无法启动";
            return false;
        }
    }
    loadOrCreateLobby();
    if (!startListen()) return false;
    running_ = true;
    monitor_ = std::thread(&ChatServer::monitorLoop, this);

    LOG_INFO() << "==========================================";
    LOG_INFO() << "聊天服务器 v5 已启动（账号 + 房间 + 持久化/游标分页）";
    LOG_INFO() << "监听端口: " << cfg_.port << "（0.0.0.0），MAX_FRAME=1MiB"
               << (cfg_.tls ? "，TLS 已启用" : "，明文 TCP");
    LOG_INFO() << "凭据安全: 挑战-应答登录（口令/K 不进帧，nonce 抗重放）+ " << pwd_hash::backend_name();
    LOG_INFO() << "口令存储: 派生值 ⊕ pepper 盲化入库（stored = K ⊕ HMAC(pepper, user‖salt)，pepper="
               << cfg_.pepper_path << "）——拖库拿不到登录凭据（反 pass-the-hash）";
    LOG_INFO() << "持久化: " << cfg_.db_path << "（users/rooms/messages/offline_messages + 2 条索引）";
    LOG_INFO() << "房间: 默认 lobby，广播按 room->members 路由（O(房间成员)）";
    LOG_INFO() << "历史: 进房拉最近 50 条，(ts,id) 游标向上翻页（不用 OFFSET）";
    LOG_INFO() << "限流: 每 IP 建连 " << cfg_.conn_rate << "/s（突发 " << cfg_.conn_burst
               << ")；每用户消息 " << cfg_.msg_rate << "/s（突发 " << cfg_.msg_burst << "）";
    LOG_INFO() << "==========================================";

    acceptLoop();
    shutdownAll();
    return true;
}

void ChatServer::requestStop() {
    running_ = false;
    if (sock_valid(listen_fd_)) sock_shutdown(listen_fd_);
}

// ---------- 房间装载 ----------

void ChatServer::loadOrCreateLobby() {
    std::vector<RoomRow> rows = db_.list_rooms();
    for (size_t i = 0; i < rows.size(); ++i)
        router_.create(rows[i].name, rows[i].owner, rows[i].id, rows[i].created_at);
    // 默认房间 lobby：不存在则创建（owner=SERVER）
    if (!router_.exists("lobby")) {
        long long ts = now_ts();
        int rc = db_.create_room("lobby", "SERVER", ts);
        if (rc != 0 && rc != kErrRoomExists) {
            LOG_ERROR() << "创建默认房间 lobby 失败";
            return;
        }
        long long id = db_.find_room_id("lobby");
        router_.create("lobby", "SERVER", id, ts);
    }
    LOG_INFO() << "房间装载完成: " << router_.room_count() << " 个（含默认 lobby）";
}

bool ChatServer::startListen() {
#ifdef _WIN32
    listen_fd_ = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
#else
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
#endif
    if (!sock_valid(listen_fd_)) {
        LOG_ERROR() << "创建 socket 失败";
        return false;
    }
    int opt = 1;
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, (const char*)&opt, sizeof(opt));
    sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons((unsigned short)cfg_.port);
    if (bind(listen_fd_, (sockaddr*)&addr, sizeof(addr)) < 0) {
        LOG_ERROR() << "绑定端口 " << cfg_.port << " 失败（是否被占用？）";
        return false;
    }
    if (listen(listen_fd_, 128) < 0) {
        LOG_ERROR() << "监听失败";
        return false;
    }
    return true;
}

void ChatServer::acceptLoop() {
    while (running_) {
        sockaddr_in ca;
        socklen_t cl = sizeof(ca);
        socket_t cfd = accept(listen_fd_, (sockaddr*)&ca, &cl);
        if (!sock_valid(cfd)) {
            if (!running_) break;
            if (sock_interrupted()) continue;
            LOG_ERROR() << "accept 失败";
            continue;
        }
        apply_socket_opts(cfd);
        std::string ip = inet_ntoa(ca.sin_addr);

        // B 风控①：每 IP 建连令牌桶。超限 → 显式错误码 E4001 + WARN 日志 + 立即断开
        // （不静默挂断：客户端能区分「限流」和「网络故障」）。时钟取 now_ms()/1000。
        if (cfg_.conn_rate > 0 && !conn_limit_.allow(ip, now_ms() / 1000.0)) {
            LOG_WARN() << "[限流] IP " << ip << " 建连过于频繁（>" << cfg_.conn_rate
                       << "/s），E4001 拒绝并断开";
            minijson::Object err;
            err.set_num("ver", 1);
            err.set_str("type", "ERR");
            err.set_str("from", "SERVER");
            err.set_str("to", "");
            err.set_str("room", "");
            err.set_num("code", kErrConnRate);
            err.set_str("content", "连接过于频繁：每 IP 每秒最多 " +
                                       std::to_string((long long)cfg_.conn_rate) + " 个新连接，请稍后再试");
            err.set_num("ts", now_ts());
            std::string pkt = make_wire_frame(minijson::serialize(err));
            sock_send(cfd, pkt.data(), pkt.size());
            sock_close(cfd);
            continue;
        }

        ConnPtr c(new ClientConn());
        {
            std::lock_guard<std::mutex> lk(g_m_);
            c->id = ++next_id_;
            c->fd = cfd;
            c->peer = ip + ":" + std::to_string(ntohs(ca.sin_port));
            c->last_active = now_ms();
            conns_[c->id] = c;
        }
        LOG_INFO() << "[连接] " << c->peer << " id=" << c->id << "（当前 " << connCount() << " 路）";
        std::thread(&ChatServer::handleClient, this, c).detach();
    }
}

// ---------- 收发 ----------

void ChatServer::handleClient(ConnPtr c) {
    // C：TLS 握手放在连接线程（不堵 acceptLoop）；失败 = 不可信客户端，直接收尾
    if (cfg_.tls) {
        c->tls = TlsConn::accept(tls_ctx_, c->fd);
        if (!c->tls) {
            LOG_WARN() << "[TLS] 握手失败，断开 " << c->peer;
            dropConn(c, "TLS 握手失败");
            closeConn(c);
            return;
        }
    }
    char chunk[4096];
    while (running_) {
        long n = recvLocked(c, chunk, sizeof(chunk));
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
                LOG_WARN() << "[协议错误] id=" << c->id << " 帧超限/坏帧，断开";
                break;
            }
        } else if (n == 0) {
            LOG_INFO() << "[断开] " << c->peer << " 对端关闭";
            break;
        } else {
            if (sock_interrupted()) continue;
            LOG_INFO() << "[断开] " << c->peer << " recv 出错";
            break;
        }
    }
    dropConn(c, "掉线");
    closeConn(c);
}

bool ChatServer::handleBody(const ConnPtr& c, const std::string& body) {
    minijson::Object obj;
    if (!minijson::parse(body, obj)) {
        LOG_WARN() << "[错误] JSON 解析失败（id=" << c->id << "）";
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
    } else if (type == "REGISTER_HELLO") {
        doRegisterHello(c, obj);
    } else if (type == "LOGIN_HELLO") {
        doLoginHello(c, obj);
    } else if (type == "REGISTER") {
        doRegister(c, obj);
    } else if (type == "LOGIN") {
        doLogin(c, obj);
    } else if (type == "AUTH") {
        doAuth(c, obj);
    } else if (type == "LOGOUT") {
        LOG_INFO() << "[退出] " << userName(c) << " 主动 LOGOUT";
        dropConn(c, "退出");
        return true;
    } else if (!c->authed) {
        sendErr(c, kErrNotAuth, "尚未登录，请先 LOGIN/AUTH 认证");  // 业务帧一律先认证（Q4）
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
        LOG_WARN() << "[警告] 未处理类型: " << type << "（id=" << c->id << "）";
    }
    return false;
}

// ---------- 背压发送（P1：每连接写队列上限 + 超限踢连接） ----------
// 问题：thread-per-connection 下「客户端不读 → send() 阻塞」会把【发送方线程】（广播者）
// 拖死在内核缓冲区上，慢一个连接拖垮一轮广播。解法：发送侧永不无界阻塞——
//   ① 快路径：outbuf 为空时直写（明文 MSG_DONTWAIT 非阻塞；TLS 有界 WANT_WRITE 预算）；
//   ② 写不动的字节进【有界队列】c->outbuf（上限 kSendQueueCap）；明文后续仍尝试非阻塞
//      排空（零成本），TLS 进入慢状态后【纯入队】（不碰 socket，杜绝发送线程被拖死）；
//   ③ 队列超限 → sendLocked 返回 false → 调用方 kickConn（慢客户端被踢，不影响别人）；
//      慢状态的 outbuf 由心跳扫描线程每轮尝试排空（monitorLoop），排空即退出慢状态。
// 对比换 epoll Reactor：这是 O(1) 的连接级隔离，成本远低于线程模型重写（红线：不动）。
static const size_t kSendQueueCap = 256 * 1024;  // 每连接待发上限 256 KiB（约 256 条满帧）
static const int kTlsWriteBudget = 2;            // 单次 TLS 直写/排空的 WANT_WRITE 重试预算

bool ChatServer::sendLocked(const ConnPtr& c, const std::string& pkt) {
    if (c->fd < 0) return false;
    bool was_empty = c->outbuf.empty();
    c->outbuf.append(pkt);
    if (c->outbuf.size() > kSendQueueCap) {
        notifyOverpressure(c);   // 踢前明确告知（E4004）：客户端据此慢速重连，防重连风暴
        return false;            // 背压超限 → 踢（调用方 kickConn）
    }
    // TLS 慢状态（outbuf 曾非空）只入队：排空交给 monitorLoop，发送线程绝不阻塞
    if (c->tls && !was_empty) return true;
    return drainOutLocked(c);
}

void ChatServer::notifyOverpressure(const ConnPtr& c) {
    // best-effort：绕过背压队列直写一条明确 ERR（E4004）再踢。慢客户端多半已读不动
    // （写失败即弃、不阻塞）；读得到的客户端识别 4004 后【不做快速指数重连】，
    // 否则「被踢→快重连→再被踢」会退化成重连风暴。无静默失败原则：能告知就告知。
    minijson::Object err;
    err.set_num("ver", 1);
    err.set_str("type", "ERR");
    err.set_str("from", "SERVER");
    err.set_str("to", c->username);
    err.set_str("room", "");
    err.set_num("code", kErrBackpressure);
    err.set_str("content", "发送积压超限（读取过慢）：连接将被关闭，请稍后慢速重连");
    err.set_num("ts", now_ts());
    std::string pkt = make_wire_frame(minijson::serialize(err));
    if (c->fd >= 0) sock_send_nb(c->fd, pkt.data(), pkt.size());
}

bool ChatServer::drainOutLocked(const ConnPtr& c) {
    // 排空 outbuf（须持 send_m）。false = 写坏/超限（调用方踢）；不碰 fd——收线程才 close
    while (!c->outbuf.empty()) {
        if (c->fd < 0) return false;
        size_t wrote = 0;
        if (c->tls) {
            long n = c->tls->write_progress(c->outbuf.data(), c->outbuf.size(), kTlsWriteBudget);
            if (n < 0) return false;  // 硬错误
            wrote = (size_t)n;
        } else {
            long n = sock_send_nb(c->fd, c->outbuf.data(), c->outbuf.size());
            if (n > 0) wrote = (size_t)n;
            else if (n < 0 && sock_interrupted()) continue;
            else if (n < 0 && sock_would_block()) return true;  // 内核缓冲满：留下次排空
            else return false;                                  // 硬错误
        }
        c->outbuf.erase(0, wrote);
        if (wrote == 0) return true;  // TLS 预算用尽仍写不动 → 慢状态，留待下轮
    }
    return true;
}

long ChatServer::recvLocked(const ConnPtr& c, char* buf, size_t len) {
    if (c->tls) return c->tls->read(buf, len);  // TLS：0=close_notify/EOF，<0=错误
    return sock_recv(c->fd, buf, len);
}

bool ChatServer::sendFrame(const ConnPtr& c, minijson::Object obj) {
    std::lock_guard<std::mutex> lk(c->send_m);
    if (c->fd < 0) return false;
    obj.set_num("seq", ++c->send_seq);
    std::string body = minijson::serialize(obj);
    if (body.size() > kMaxFrame) return false;
    return sendLocked(c, make_wire_frame(body));
}

bool ChatServer::sendRelay(const ConnPtr& c, minijson::Object obj, long long origin_seq) {
    std::lock_guard<std::mutex> lk(c->send_m);
    if (c->fd < 0) return false;
    obj.set_num("seq", origin_seq);  // 聊天 MESSAGE 沿用发送方 seq（客户端 (from,seq) 显示去重）
    std::string body = minijson::serialize(obj);
    if (body.size() > kMaxFrame) return false;
    return sendLocked(c, make_wire_frame(body));
}

void ChatServer::kickConn(const ConnPtr& c, const std::string& reason, bool notify_room) {
    dropConn(c, reason, notify_room);
    std::lock_guard<std::mutex> lk(c->send_m);
    if (c->fd >= 0) sock_shutdown(c->fd);
}

std::string ChatServer::userName(const ConnPtr& c) {
    return c->authed ? c->username : ("?" + std::to_string(c->id));
}

void ChatServer::sendErr(const ConnPtr& c, int code, const std::string& text) {
    minijson::Object err;
    err.set_num("ver", 1);
    err.set_str("type", "ERR");
    err.set_str("from", "SERVER");
    err.set_str("to", c->username);
    err.set_str("room", "");
    err.set_num("code", code);        // 错误码
    err.set_str("content", text);     // 中文文案（不静默失败）
    err.set_num("ts", now_ts());
    sendFrame(c, err);
    LOG_WARN() << "[ERR] -> " << userName(c) << " code=" << code << " " << text;
}

void ChatServer::sendAck(const ConnPtr& c, long long client_seq) {
    minijson::Object ack;
    ack.set_num("ver", 1);
    ack.set_str("type", "ACK");
    ack.set_str("from", "SERVER");
    ack.set_str("to", c->username);
    ack.set_str("room", "");
    ack.set_str("content", std::to_string(client_seq));
    ack.set_num("ts", now_ts());
    if (!sendFrame(c, ack)) LOG_WARN() << "[警告] ACK 发送失败（id=" << c->id << "）";
}

void ChatServer::sendNack(const ConnPtr& c, long long client_seq, const std::string& reason,
                          int code) {
    minijson::Object nack;
    nack.set_num("ver", 1);
    nack.set_str("type", "NACK");
    nack.set_str("from", "SERVER");
    nack.set_str("to", c->username);
    nack.set_str("room", "");
    nack.set_str("content", std::to_string(client_seq));
    nack.set_str("reason", reason);
    if (code > 0) nack.set_num("code", code);  // B：限流/敏感词带错误码（4002/4003）
    nack.set_num("ts", now_ts());
    sendFrame(c, nack);
    LOG_WARN() << "[NACK] -> " << userName(c) << " seq=" << client_seq << " code=" << code
               << " reason=" << reason;
}

// ---------- 认证（Q4/Q5/Q7：挑战-应答，口令不进帧） ----------

static const long long kChallengeTtlMs = 60000;  // 挑战有效期：一次性 nonce 60s 过期

void ChatServer::sendChallenge(const ConnPtr& c, const std::string& user,
                               const std::string& salt_hex, bool is_register) {
    // 【挑战单槽复用】已有未过期、同用户同类型的挑战 → 原样重发，不消耗 CSPRNG。
    // challenge「表」上限即每连接 1 槽（内存有界 = 连接数）；重发风暴被复用封死，
    // 生成速率 = 消费/过期速率——配合 *_HELLO 的每 IP 令牌桶，无认证放大面。
    bool reuse = c->chal.active && c->chal.is_register == is_register &&
                 c->chal.user == user && c->chal.exp_ms > now_ms();
    if (!reuse) {
        c->chal.active = true;
        c->chal.is_register = is_register;
        c->chal.user = user;
        // 注册盐在此现发（CSPRNG）；登录盐来自库内——复用路径两者都不消耗随机源
        c->chal.salt_hex = is_register ? pwd_hash::random_salt_hex() : salt_hex;
        c->chal.nonce_hex = pwd_hash::random_hex(32);  // 32B CSPRNG（Q7：抗重放核心）
        c->chal.exp_ms = now_ms() + kChallengeTtlMs;
    }
    minijson::Object ch;
    ch.set_num("ver", 1);
    ch.set_str("type", "CHALLENGE");
    ch.set_str("from", "SERVER");
    ch.set_str("to", user);
    ch.set_str("room", "");
    ch.set_str("content", c->chal.nonce_hex);  // 一次性 nonce（32B → 64 hex）
    ch.set_str("salt", c->chal.salt_hex);      // salt 不是秘密（抗彩虹表）；两路都由服务端生成
    ch.set_num("ts", now_ts());
    sendFrame(c, ch);
}

bool ChatServer::takeChallenge(const ConnPtr& c, const std::string& user, bool is_register,
                               Challenge& out) {
    // 校验并【一次性】消费：无论成败都作废本次挑战——同一次抓包的证明无法重放，
    // 猜错口令也必须重新 HELLO 拿新 nonce（每次猜测一轮往返 + 一次 PBKDF2）
    Challenge ch = c->chal;
    c->chal.active = false;
    c->chal.nonce_hex.clear();
    if (!ch.active || ch.is_register != is_register || ch.user != user ||
        ch.exp_ms < now_ms())
        return false;
    out = ch;
    return true;
}

void ChatServer::doRegisterHello(const ConnPtr& c, minijson::Object& obj) {
    if (c->authed) {
        sendErr(c, kErrAlreadyAuth, "本连接已登录，请勿重复登录/注册");
        return;
    }
    // Q7 无认证面限流：*_HELLO 每次都要查库/可能生成随机数——挂进每 IP 令牌桶（与建连
    // 共用 conn_limit_），超限 E4001 + WARN（显式拒绝，不静默；不踢连接，保护已有会话）
    std::string ip = c->peer.substr(0, c->peer.find(':'));
    if (cfg_.conn_rate > 0 && !conn_limit_.allow(ip, now_ms() / 1000.0)) {
        LOG_WARN() << "[限流] IP " << ip << " 认证请求过于频繁（*_HELLO），E4001 拒绝";
        sendErr(c, kErrConnRate, "认证请求过于频繁：请稍后再试");
        return;
    }
    std::string name = obj.get_str("from");
    if (!valid_username(name)) {
        sendErr(c, kErrBadCredFmt, "用户名格式非法（1-32 字符，非保留字）");
        return;
    }
    UserRow u;
    if (db_.find_user(name, u)) {
        // 用户名唯一约束提前显式报错（E1001，不静默；不发挑战）
        sendErr(c, kErrDupUser, "注册失败：用户名已存在，请更换用户名");
        return;
    }
    sendChallenge(c, name, "", true);  // salt(16B CSPRNG) + nonce(32B) 都在挑战内生成
    LOG_INFO() << "[注册挑战] " << name << "（id=" << c->id << "）";
}

void ChatServer::doLoginHello(const ConnPtr& c, minijson::Object& obj) {
    if (c->authed) {
        sendErr(c, kErrAlreadyAuth, "本连接已登录，请勿重复登录");
        return;
    }
    // 同 doRegisterHello：*_HELLO 挂每 IP 令牌桶（无认证面不给放大口子）
    std::string ip = c->peer.substr(0, c->peer.find(':'));
    if (cfg_.conn_rate > 0 && !conn_limit_.allow(ip, now_ms() / 1000.0)) {
        LOG_WARN() << "[限流] IP " << ip << " 认证请求过于频繁（*_HELLO），E4001 拒绝";
        sendErr(c, kErrConnRate, "认证请求过于频繁：请稍后再试");
        return;
    }
    std::string name = obj.get_str("from");
    if (!valid_username(name)) {
        sendErr(c, kErrBadCredFmt, "用户名格式非法");
        return;
    }
    UserRow u;
    if (!db_.find_user(name, u)) {
        sendErr(c, kErrNoSuchUser, "登录失败：用户不存在，请先注册");
        return;
    }
    sendChallenge(c, name, u.salt, false);  // 回库内 salt，客户端据此算 PBKDF2 派生值
}

void ChatServer::doRegister(const ConnPtr& c, minijson::Object& obj) {
    // Q7②：REGISTER = 播种帧。content = K=PBKDF2(pwd, salt)（入库值，一次性送达，
    // 口令本身不进帧）；hmac = HMAC(K, nonce)（证明对本次挑战应答，帧不可重放）
    if (c->authed) {
        sendErr(c, kErrAlreadyAuth, "本连接已登录，请勿重复登录/注册");
        return;
    }
    std::string name = obj.get_str("from");
    std::string derived = obj.get_str("content");
    std::string proof = obj.get_str("hmac");
    Challenge ch;
    if (!takeChallenge(c, name, true, ch)) {
        sendErr(c, kErrBadChallenge, "注册挑战缺失/已过期/不匹配，请重新 REGISTER_HELLO");
        return;
    }
    if (!valid_username(name) || derived.size() != 64 || proof.size() != 64) {
        sendErr(c, kErrBadCredFmt, "用户名或注册证明格式非法（派生密钥/HMAC 应为 64 hex）");
        return;
    }
    std::vector<unsigned char> tmp;
    if (!pwd_hash::from_hex(derived, tmp) || tmp.size() != 32 ||
        !pwd_hash::from_hex(proof, tmp) || tmp.size() != 32) {
        sendErr(c, kErrBadCredFmt, "注册证明格式非法（派生密钥/HMAC 应为 64 hex）");
        return;
    }
    // proof = HMAC(K, "REGISTER"‖user‖nonce)：绑定本次挑战 + 协议语境 + 身份（抗重放 +
    // 纵深防御），恒定时间比较
    if (!pwd_hash::constant_time_eq(
            pwd_hash::challenge_proof(derived, "REGISTER", name, ch.nonce_hex), proof)) {
        sendErr(c, kErrBadChallenge, "注册证明校验失败（挑战不匹配），请重新 REGISTER_HELLO");
        return;
    }
    // 【salt 不变式】salt 只来自挑战记录（doRegisterHello 服务端 CSPRNG 现发、随 CHALLENGE
    // 下发），客户端无法指定——不存在恶意注册构造重复盐/跨用户预计算的面；与 LOGIN 取库内
    // salt 的来源完全同构。校验「16B 长度/拒绝重复」因此天然成立，无需再设防。
    // Q7② 反 pass-the-hash：库里不落 K（K 就是登录凭据），落盲化值
    //   stored = K ⊕ HMAC-SHA256(pepper, user‖salt)
    // 拖库拿不到 K，攻击退回离线爆破（每猜测 100k PBKDF2）；pepper 边界见 pepper.h 头注释。
    std::string stored = pwd_hash::apply_pepper(derived, pepper_hex_, name, ch.salt_hex);
    if (stored.empty()) {
        sendErr(c, kErrDb, "口令盲化失败（pepper 配置异常）");
        return;
    }
    int rc = db_.create_user(name, ch.salt_hex, stored, now_ts());
    if (rc == kErrDupUser) {
        // 用户名唯一约束生效 → 明确错误码 + 中文文案，绝不静默失败
        sendErr(c, kErrDupUser, "注册失败：用户名已存在，请更换用户名");
        return;
    }
    if (rc != 0) {
        sendErr(c, kErrDb, "注册失败：数据库错误");
        return;
    }
    LOG_INFO() << "[注册] " << name << "（id=" << c->id << "）成功（PBKDF2 客户端侧派生）";
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

void ChatServer::finishAuth(const ConnPtr& c, const std::string& name, const std::string& token,
                            long long token_exp, bool takeover, const ConnPtr& old) {
    minijson::Object ok;
    ok.set_num("ver", 1);
    ok.set_str("type", "AUTH_OK");
    ok.set_str("from", "SERVER");
    ok.set_str("to", name);
    ok.set_str("room", router_.user_room(name));
    ok.set_str("content", token);      // Q4：32 字节随机 Token（hex 64 字符）
    ok.set_num("exp", token_exp);      // 过期时间（Unix 秒）
    ok.set_num("ts", now_ts());
    sendFrame(c, ok);

    pushOffline(c, name);

    std::string room = router_.user_room(name);
    if (takeover) {
        // 顶号（Token 重连）：用户在别人眼里没离开过（成员集按用户名，Q5/Q2）
        LOG_INFO() << "[顶号] " << name << " 新连接 id=" << c->id << " 顶替旧连接 id=" << old->id;
        // 明确通知旧连接再踢（Q5：不静默失败）
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
        if (!room.empty()) sendJoinState(c, room);  // 会话续传：房间状态同步给新连接
    } else if (!room.empty()) {
        sendJoinState(c, room);  // 防御路径：正常登录不应有残留房间
    }
}

void ChatServer::doLogin(const ConnPtr& c, minijson::Object& obj) {
    // Q7①：LOGIN = 证明帧。content = proof = HMAC-SHA256(K, nonce)，K = PBKDF2(pwd, salt)
    // 客户端侧派生。口令与 K 都不进帧；服务端用库内存的 K 重算 proof 恒定时间比较。
    if (c->authed) {
        sendErr(c, kErrAlreadyAuth, "本连接已登录，请勿重复登录");
        return;
    }
    std::string name = obj.get_str("from");
    std::string proof = obj.get_str("content");
    Challenge ch;
    if (!takeChallenge(c, name, false, ch)) {
        sendErr(c, kErrBadChallenge, "登录挑战缺失/已过期/不匹配，请重新 LOGIN_HELLO");
        return;
    }
    if (!valid_username(name) || proof.size() != 64) {
        sendErr(c, kErrBadCredFmt, "用户名或登录证明格式非法（HMAC 应为 64 hex）");
        return;
    }
    std::vector<unsigned char> tmp;
    if (!pwd_hash::from_hex(proof, tmp) || tmp.size() != 32) {
        sendErr(c, kErrBadCredFmt, "登录证明格式非法（HMAC 应为 64 hex）");
        return;
    }
    UserRow u;
    if (!db_.find_user(name, u)) {
        sendErr(c, kErrNoSuchUser, "登录失败：用户不存在，请先注册");
        return;
    }
    // Q7②：库里是盲化值 stored = K ⊕ HMAC(pepper, user‖salt)——XOR 对合，套同一函数还原 K
    std::string k_hex = pwd_hash::apply_pepper(u.pwd_hash, pepper_hex_, name, u.salt);
    // proof = HMAC(K, "LOGIN"‖user‖nonce)：user 双保险（takeChallenge 已核对挑战归属）
    if (k_hex.empty() || !pwd_hash::constant_time_eq(
                            pwd_hash::challenge_proof(k_hex, "LOGIN", name, ch.nonce_hex),
                            proof)) {
        // proof 错 = 口令错（K 派生自口令）。恒定时间比较防计时旁路；分开报错是教学友好，
        // 生产可合并为「用户名或密码错误」防枚举
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
    LOG_INFO() << "[登录] " << name << "（id=" << c->id << "）挑战-应答登录成功";
    finishAuth(c, name, token, exp, false, ConnPtr());
}

void ChatServer::doAuth(const ConnPtr& c, minijson::Object& obj) {
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
    LOG_INFO() << "[认证] " << name << "（id=" << c->id << "）Token 恢复会话"
               << (takeover ? "（顶号）" : "");
    finishAuth(c, name, new_token, exp, takeover, old);
}

// ---------- 房间业务（Q2） ----------

void ChatServer::doJoin(const ConnPtr& c, minijson::Object& obj) {
    std::string room = obj.get_str("content");
    if (!valid_room_name(room)) {
        sendErr(c, kErrBadRoomName, "房间名非法（1-32 字符，非保留字）");
        return;
    }
    if (!router_.exists(room)) {
        sendErr(c, kErrNoSuchRoom, "加入失败：房间不存在，可用 /create 创建");
        return;
    }
    std::string name = c->username;
    std::string prev;
    if (!router_.join(name, room, &prev)) {
        sendErr(c, kErrNoSuchRoom, "加入失败：房间不存在");
        return;
    }
    if (prev == room) {
        sendJoinState(c, room);  // 幂等：重复 JOIN 回当前状态
        return;
    }
    if (!prev.empty()) leaveRoomNotify(name, prev);  // 换房先退旧房
    LOG_INFO() << "[加入] " << name << " -> #" << room;
    sendJoinState(c, room);
    broadcastRoomSystem(room, name + " 进入了房间", c);
    broadcastRoomUserlist(room);
}

void ChatServer::doLeave(const ConnPtr& c) {
    std::string name = c->username;
    std::string room = router_.leave(name);
    if (room.empty()) {
        sendErr(c, kErrNotInRoom, "尚未加入任何房间，无法 /leave");
        return;
    }
    leaveRoomNotify(name, room);
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

void ChatServer::leaveRoomNotify(const std::string& name, const std::string& room) {
    LOG_INFO() << "[离开] " << name << " <- #" << room;
    broadcastRoomSystem(room, name + " 离开了房间", ConnPtr());
    broadcastRoomUserlist(room);
}

void ChatServer::sendJoinState(const ConnPtr& c, const std::string& room) {
    minijson::Object ok;
    ok.set_num("ver", 1);
    ok.set_str("type", "JOIN_OK");
    ok.set_str("from", "SERVER");
    ok.set_str("to", c->username);
    ok.set_str("room", room);
    ok.set_list("content", router_.members(room));
    ok.set_num("ts", now_ts());
    sendFrame(c, ok);
}

void ChatServer::doRooms(const ConnPtr& c) {
    std::vector<minijson::Object> items;
    std::vector<RoomInfo> rooms = router_.list();
    for (size_t i = 0; i < rooms.size(); ++i) {
        minijson::Object r;
        r.set_str("name", rooms[i].name);
        r.set_str("owner", rooms[i].owner);
        r.set_num("members", (long long)rooms[i].members.size());
        r.set_num("created_at", rooms[i].created_at);
        items.push_back(r);
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

void ChatServer::doCreate(const ConnPtr& c, minijson::Object& obj) {
    std::string room = obj.get_str("content");
    if (!valid_room_name(room)) {
        sendErr(c, kErrBadRoomName, "房间名非法（1-32 字符，非保留字）");
        return;
    }
    long long ts = now_ts();
    int rc = db_.create_room(room, c->username, ts);
    if (rc == kErrRoomExists) {
        sendErr(c, kErrRoomExists, "创建失败：房间已存在");
        return;
    }
    if (rc != 0) {
        sendErr(c, kErrDb, "创建失败：数据库错误");
        return;
    }
    long long db_id = db_.find_room_id(room);  // 回填真实主键（消息落库 room_id 需要）
    router_.create(room, c->username, db_id, ts);
    LOG_INFO() << "[创建] " << c->username << " 创建房间 #" << room;
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

void ChatServer::sendHistPage(const ConnPtr& c, const std::string& kind, const std::string& room,
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
        cursor = format_cursor(rows.back().ts, rows.back().id);  // 下一页游标（本页最老一行）
    minijson::Object page;
    page.set_num("ver", 1);
    page.set_str("type", kind);  // HISTORY / INBOX
    page.set_str("from", "SERVER");
    page.set_str("to", c->username);
    page.set_str("room", room);
    page.set_objs("content", items);
    page.set_str("cursor", cursor);  // 空 = 到头
    page.set_num("more", more ? 1 : 0);
    page.set_num("ts", now_ts());
    sendFrame(c, page);
}

void ChatServer::doHist(const ConnPtr& c, minijson::Object& obj) {
    std::string room = router_.user_room(c->username);
    if (room.empty()) {
        sendErr(c, kErrNotInRoom, "尚未加入房间，无法拉取历史（/join 后自动拉最近 50 条）");
        return;
    }
    long long room_id = router_.db_id(room);
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
        if (!parse_cursor(cur, cts, cid)) {
            sendErr(c, kErrBadCursor, "历史游标非法（应为 ts:id）");
            return;
        }
        rows = db_.history_before(room_id, cts, cid, kHistPageSize + 1);
    }
    bool more = rows.size() > (size_t)kHistPageSize;
    if (more) rows.resize(kHistPageSize);
    sendHistPage(c, "HISTORY", room, rows, more);
}

void ChatServer::doInbox(const ConnPtr& c, minijson::Object& obj) {
    std::string cur = obj.get_str("content");
    std::vector<MsgRow> rows;
    if (cur.empty()) {
        rows = db_.inbox_latest(c->username, kHistPageSize + 1);
    } else {
        long long cts = 0, cid = 0;
        if (!parse_cursor(cur, cts, cid)) {
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

void ChatServer::doMessage(const ConnPtr& c, minijson::Object& obj) {
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

    // B 风控②：每用户消息令牌桶。超限 → NACK(code=E4002) + WARN 日志。
    // 放在去重之前：拒绝帧【不消耗】(user,seq) 幂等槽位——客户端稍后 /retry 同 seq 可成功。
    if (cfg_.msg_rate > 0 && !msg_limit_.allow(from, now_ms() / 1000.0)) {
        LOG_WARN() << "[限流] 用户 " << from << " 发消息过于频繁（>" << cfg_.msg_rate << "/s），E4002";
        sendNack(c, seq,
                 "发送过于频繁：每用户每秒最多 " + std::to_string((long long)cfg_.msg_rate) +
                     " 条消息，请稍后重试",
                 kErrMsgRate);
        return;
    }

    // B 风控③：敏感词 Trie（word_filter.h 头注释含复杂度分析）
    if (!words_.empty()) {
        std::string hit;
        if (words_.contains(content, &hit)) {
            if (cfg_.filter_reject) {
                LOG_WARN() << "[敏感词] " << from << " 命中「" << hit << "」，E4003 拒绝发送";
                sendNack(c, seq, "消息包含敏感词「" + hit + "」，已拒绝发送", kErrSensitive);
                return;
            }
            content = words_.filter(content);  // replace 模式：脱敏后照常落库/广播
            LOG_INFO() << "[敏感词] " << from << " 命中「" << hit << "」，已替换为 *";
        }
    }

    if (!dedup_.check_and_add(from, seq)) {
        // (user,seq) 幂等窗口（协议 6.6）：重发/ACK 丢失重传 → 只回 ACK、零副作用
        LOG_INFO() << "[去重] " << from << " seq=" << seq << " 重复帧，仅回 ACK";
        sendAck(c, seq);
        return;
    }

    if (to == "ALL") {
        // ---- 群聊 → 当前房间 ----
        std::string room = router_.user_room(from);
        if (room.empty()) {
            sendNack(c, seq, "尚未加入房间，无法群聊（/join 后再发）");
            return;
        }
        long long room_id = router_.db_id(room);
        long long id = db_.insert_room_msg(room_id, from, content, ts);
        if (id < 0) {
            sendNack(c, seq, "消息落库失败");
            return;
        }
        LOG_DEBUG() << "[消息] #" << room << " " << from << ": " << content;
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
    if (to.empty() || to == "SERVER" || !valid_username(to)) {
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
    LOG_DEBUG() << "[私聊] " << from << " -> " << to << ": " << content;
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
    LOG_INFO() << "[离线存储] " << from << " -> " << to << " 已入库（待投递）";
    if (target) kickConn(target, "掉线", true);
}

// ---------- 广播（Q2：O(房间成员) 而不是 O(全在线)） ----------

void ChatServer::broadcastRoomSystem(const std::string& room, const std::string& text,
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

void ChatServer::broadcastRoomUserlist(const std::string& room) {
    minijson::Object ul;
    ul.set_num("ver", 1);
    ul.set_str("type", "USERLIST");
    ul.set_str("from", "SERVER");
    ul.set_str("to", "ALL");
    ul.set_str("room", room);
    ul.set_list("content", router_.members(room));
    ul.set_num("ts", now_ts());
    std::vector<ConnPtr> targets = roomConns(room);
    for (size_t i = 0; i < targets.size(); ++i) sendFrame(targets[i], ul);
}

void ChatServer::broadcastRoomRelay(const std::string& room, minijson::Object& m,
                                    long long origin_seq) {
    std::vector<ConnPtr> targets = roomConns(room);
    for (size_t i = 0; i < targets.size(); ++i) {
        if (!sendRelay(targets[i], m, origin_seq)) {
            LOG_WARN() << "[警告] 房间下发失败（id=" << targets[i]->id << "），剔除";
            kickConn(targets[i], "掉线", true);
        }
    }
}

std::vector<ConnPtr> ChatServer::roomConns(const std::string& room) {
    // 只取本房间成员的连接（Q2 的核心：从 O(N 全在线) 到 O(房间成员)）
    std::vector<ConnPtr> v;
    std::vector<std::string> names = router_.members(room);
    std::lock_guard<std::mutex> lk(g_m_);
    for (size_t i = 0; i < names.size(); ++i) {
        std::unordered_map<std::string, ConnPtr>::iterator o = online_.find(names[i]);
        if (o != online_.end()) v.push_back(o->second);
    }
    return v;
}

void ChatServer::pushOffline(const ConnPtr& c, const std::string& name) {
    std::vector<OffRow> rows = db_.undelivered(name);
    if (rows.empty()) return;
    LOG_INFO() << "[离线补发] " << name << " 共 " << rows.size() << " 条";
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
            LOG_INFO() << "[离线补发] " << name << " 中断（连接故障），余下下次再补";
            return;  // 未置 delivered 的行下次上线续推
        }
        db_.mark_delivered(rows[i].id);  // 推送成功一条置一条
    }
}

// ---------- 连接摘除 / 心跳扫描 ----------

void ChatServer::dropConn(const ConnPtr& c, const std::string& reason, bool notify_room) {
    // 幂等摘表。顶号（"被顶替"）不摘房间成员、不广播——用户在别人眼里没离开过（Q5）
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
                room = router_.user_room(name);
                if (!room.empty()) router_.leave(name);  // 摘房间成员（keyed by username）
            }
            c->authed = false;
        }
    }
    if (was_online) {
        LOG_INFO() << "[下线] " << name << (reason.empty() ? "" : ("（" + reason + "）"));
        if (notify_room && !room.empty()) {
            broadcastRoomSystem(room, name + " 离开了房间", ConnPtr());
            broadcastRoomUserlist(room);
        }
    }
}

void ChatServer::closeConn(const ConnPtr& c) {
    std::lock_guard<std::mutex> lk(c->send_m);
    if (c->tls) {
        c->tls->shutdown();  // 尽力 close_notify；TlsConn 析构随 ClientConn 释放
    }
    if (c->fd >= 0) {
        sock_close(c->fd);
#ifdef _WIN32
        c->fd = INVALID_SOCKET;
#else
        c->fd = -1;
#endif
    }
}

void ChatServer::monitorLoop() {
    LOG_INFO() << "[心跳] 扫描线程启动（每 " << cfg_.scan_ms << "ms 一轮，超时阈值 " << cfg_.idle_ms << "ms）";
    while (running_) {
        long long waited = 0;
        while (running_ && waited < cfg_.scan_ms) {
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
                if (now - it->second->last_active.load() > cfg_.idle_ms) victims.push_back(it->second);
            }
        }
        for (size_t i = 0; i < victims.size(); ++i) {
            LOG_INFO() << "[心跳超时] " << userName(victims[i]) << " id=" << victims[i]->id
                       << " 超过 " << cfg_.idle_ms / 1000.0 << "s 无活跃，剔除";
            kickConn(victims[i], "心跳超时", true);
        }
        // 背压：排空慢连接的 outbuf（TLS 慢状态的唯一排空点；明文通常已被发送侧顺手排掉）。
        // 写坏/超限的连接在 drain 里已判定 false → 这里踢掉，防僵尸长期占队列。
        std::vector<ConnPtr> all;
        {
            std::lock_guard<std::mutex> lk(g_m_);
            for (std::unordered_map<uint64_t, ConnPtr>::iterator it = conns_.begin();
                 it != conns_.end(); ++it)
                if (!it->second->outbuf.empty()) all.push_back(it->second);
        }
        for (size_t i = 0; i < all.size(); ++i) {
            bool ok;
            {
                std::lock_guard<std::mutex> lk(all[i]->send_m);
                ok = drainOutLocked(all[i]) && all[i]->outbuf.size() <= kSendQueueCap;
                if (!ok) notifyOverpressure(all[i]);  // 持 send_m 内告知（与 closeConn 互斥）
            }
            if (!ok) {
                LOG_WARN() << "[背压] " << userName(all[i]) << " id=" << all[i]->id
                           << " 发送积压 " << all[i]->outbuf.size() << "B 超限/写坏，踢除";
                kickConn(all[i], "发送积压超限（慢客户端）", true);
            }
        }
    }
    LOG_INFO() << "[心跳] 扫描线程退出";
}

size_t ChatServer::connCount() {
    std::lock_guard<std::mutex> lk(g_m_);
    return conns_.size();
}

void ChatServer::shutdownAll() {
    LOG_INFO() << "[退出] 关闭全部连接...";
    {
        std::lock_guard<std::mutex> lk(g_m_);
        for (std::unordered_map<uint64_t, ConnPtr>::iterator it = conns_.begin();
             it != conns_.end(); ++it) {
            std::lock_guard<std::mutex> lk2(it->second->send_m);
            if (it->second->fd >= 0) sock_shutdown(it->second->fd);
        }
    }
    if (monitor_.joinable()) monitor_.join();
    if (sock_valid(listen_fd_)) {
        sock_close(listen_fd_);
#ifdef _WIN32
        listen_fd_ = INVALID_SOCKET;
#else
        listen_fd_ = -1;
#endif
    }
    LOG_INFO() << "[退出] 服务器已停止";
}

}  // namespace chat
