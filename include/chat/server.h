// include/chat/server.h —— 聊天服务器 v5：账号体系 + 房间路由 + 消息持久化与历史分页
//
// 线程模型：thread-per-connection + 心跳扫描线程 + 单连接 SQLite（一把互斥锁串行，WAL）。
// 设计问答 Q1–Q6（索引→查询 / 广播路由 / 游标分页 / Token / 重复登录 / 参数化 SQL）
// 见 src/server.cpp 文件头。
#ifndef CHAT_SERVER_H_
#define CHAT_SERVER_H_

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include "chat/database.h"
#include "chat/frame.h"
#include "chat/minijson.h"
#include "chat/net.h"
#include "chat/rate_limiter.h"
#include "chat/room_router.h"
#include "chat/seq_dedup.h"
#include "chat/tls.h"
#include "chat/token_book.h"
#include "chat/word_filter.h"

namespace chat {

// 服务器配置（main.cpp 解析后整体注入）
struct ServerConfig {
    int port;
    std::string db_path;
    long long idle_ms;
    long long scan_ms;
    long long token_ttl;
    // B：限流（rate=个/秒，burst=突发额度；rate<=0 关闭该维度）
    double conn_rate, conn_burst;   // 每 IP 建连
    double msg_rate, msg_burst;     // 每用户消息
    // B：敏感词（words_file 空 = 关闭；reject 模式拒绝，否则替换 *）
    std::string words_file;
    bool filter_reject;
    // C：TLS（enabled=false 走明文）
    bool tls;
    std::string tls_cert, tls_key;
    // Q7②：pepper 文件路径（口令派生密钥盲化；缺省 secrets/pepper.key，首次启动自举）
    std::string pepper_path;
    ServerConfig()
        : port(8888), idle_ms(30000), scan_ms(5000), token_ttl(7 * 24 * 3600),
          conn_rate(50), conn_burst(100), msg_rate(20), msg_burst(40),
          filter_reject(false), tls(false), pepper_path("secrets/pepper.key") {}
};

// 挑战-应答登录状态（Q7）：服务端每次 *_HELLO 生成一次性 nonce，用后即清（抗重放）。
struct Challenge {
    bool active;
    bool is_register;   // true=REGISTER_HELLO 播种（客户端交派生密钥）；false=LOGIN_HELLO 证明
    std::string user;
    std::string salt_hex;
    std::string nonce_hex;
    long long exp_ms;   // 过期时刻（now_ms），默认 60s
};

// fd 生命周期纪律：收线程唯一最终 close；踢人只摘表 + shutdown；send/close 持 send_m。
// tls 非空时收发走 TLS 记录层（tls.h 设计问答），否则裸 TCP。
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
    TlsConn* tls;                  // NULL = 明文连接
    Challenge chal;                // Q7：待应答的挑战（一次性）
    std::string outbuf;            // 背压发送队列：慢客户端未发出的字节（send_m 保护）
    ClientConn();
    ~ClientConn();
};
typedef std::shared_ptr<ClientConn> ConnPtr;

class ChatServer {
public:
    explicit ChatServer(const ServerConfig& cfg);
    bool start();
    void requestStop();

private:
    void loadOrCreateLobby();
    bool startListen();
    void acceptLoop();
    void handleClient(ConnPtr c);
    bool handleBody(const ConnPtr& c, const std::string& body);

    bool sendLocked(const ConnPtr& c, const std::string& pkt);  // 背压：见实现注释
    bool drainOutLocked(const ConnPtr& c);                      // 排空 outbuf（send_m 内调用）
    void notifyOverpressure(const ConnPtr& c);                  // 踢前尽力发 E4004（防重连风暴）
    long recvLocked(const ConnPtr& c, char* buf, size_t len);  // TLS/明文统一入口
    bool sendFrame(const ConnPtr& c, minijson::Object obj);
    bool sendRelay(const ConnPtr& c, minijson::Object obj, long long origin_seq);
    void kickConn(const ConnPtr& c, const std::string& reason, bool notify_room);

    std::string userName(const ConnPtr& c);
    void sendErr(const ConnPtr& c, int code, const std::string& text);
    void sendAck(const ConnPtr& c, long long client_seq);
    void sendNack(const ConnPtr& c, long long client_seq, const std::string& reason, int code = 0);

    void doRegisterHello(const ConnPtr& c, minijson::Object& obj);  // Q7：挑战三步走
    void doLoginHello(const ConnPtr& c, minijson::Object& obj);
    void sendChallenge(const ConnPtr& c, const std::string& user, const std::string& salt_hex,
                       bool is_register);
    bool takeChallenge(const ConnPtr& c, const std::string& user, bool is_register,
                       Challenge& out);  // 校验并【一次性】消费挑战
    void doRegister(const ConnPtr& c, minijson::Object& obj);
    void doLogin(const ConnPtr& c, minijson::Object& obj);
    void doAuth(const ConnPtr& c, minijson::Object& obj);
    void finishAuth(const ConnPtr& c, const std::string& name, const std::string& token,
                    long long token_exp, bool takeover, const ConnPtr& old);

    void doJoin(const ConnPtr& c, minijson::Object& obj);
    void doLeave(const ConnPtr& c);
    void leaveRoomNotify(const std::string& name, const std::string& room);
    void sendJoinState(const ConnPtr& c, const std::string& room);
    void doRooms(const ConnPtr& c);
    void doCreate(const ConnPtr& c, minijson::Object& obj);

    void sendHistPage(const ConnPtr& c, const std::string& kind, const std::string& room,
                      std::vector<MsgRow> rows, bool more);
    void doHist(const ConnPtr& c, minijson::Object& obj);
    void doInbox(const ConnPtr& c, minijson::Object& obj);
    void doMessage(const ConnPtr& c, minijson::Object& obj);

    void broadcastRoomSystem(const std::string& room, const std::string& text,
                             const ConnPtr& exclude);
    void broadcastRoomUserlist(const std::string& room);
    void broadcastRoomRelay(const std::string& room, minijson::Object& m, long long origin_seq);
    std::vector<ConnPtr> roomConns(const std::string& room);
    void pushOffline(const ConnPtr& c, const std::string& name);

    void dropConn(const ConnPtr& c, const std::string& reason, bool notify_room = true);
    void closeConn(const ConnPtr& c);
    void monitorLoop();
    size_t connCount();
    void shutdownAll();

    socket_t listen_fd_;
    ServerConfig cfg_;
    std::string pepper_hex_;   // Q7②：32B pepper（hex）；入库存 K⊕HMAC(pepper,user‖salt)
    TokenBook tokens_;
    Database db_;
    RoomRouter router_;   // Q2 路由表：room → members（广播只发本房间）
    SeqDeduper dedup_;    // (user,seq) 幂等窗口（协议 6.6）
    KeyedRateLimiter conn_limit_;  // B：每 IP 建连令牌桶
    KeyedRateLimiter msg_limit_;   // B：每用户消息令牌桶
    WordFilter words_;             // B：敏感词 Trie（空 = 关闭）
    TlsContext tls_ctx_;           // C：TLS 上下文（cfg_.tls=false 时不初始化）
    std::thread monitor_;

    std::mutex g_m_;                                   // 保护 conns_/online_
    std::unordered_map<uint64_t, ConnPtr> conns_;      // id → 连接
    std::unordered_map<std::string, ConnPtr> online_;  // 用户名 → 连接（已认证）
    uint64_t next_id_;
    std::atomic<bool> running_;
};

}  // namespace chat

#endif  // CHAT_SERVER_H_
