// include/chat/tls.h —— TLS 传输封装（OpenSSL）
//
// ==================== 设计问答（TLS 改造，面试必问） ====================
// Q-TLS1 握手流程（TLS 1.3，OpenSSL 3.x 默认）：
//   TCP 三次握手建立可靠字节流后，再跑 TLS 握手：
//   1) C→S ClientHello（支持的密码套件、key_share 椭圆曲线公钥、随机数）
//   2) S→C ServerHello（选定套件、S 的 key_share）+ 证书 + CertificateVerify（用证书
//      私钥对握手摘要签名，证明自己持有证书对应私钥）+ Finished
//   3) C 校验证书（自签默认跳过；--tls-strict 用 CA 验链/主机名）→ 双方由
//      (EC)DHE 协商出共享主密钥 → C 发 Finished
//   之后进入记录层：应用数据按 TLS record（头部 5B，最大 16KB）分片、AEAD 加密
//   （AES-GCM/ChaCha20）+ HMAC 完整性，写进同一条 TCP 字节流。
//   TLS 1.2 旧流程多一轮 RTT（RSA/ECDHE 握手 2-RTT）且无 0-RTT——见 README 对照。
// Q-TLS2 为什么 TLS 在 TCP 之上？
//   TLS 是「可靠字节流之上的安全记录协议」：它需要下层提供【可靠、有序、不重复】的
//   字节流来承载记录边界与握手重组，TCP 恰好提供；加密/MAC/重放防护由 TLS 记录层负责，
//   重传/拥塞控制仍归 TCP——分层各管一段。放到 UDP 上要换 DTLS（自己处理丢包/乱序下的
//   握手与记录重放），放到 HTTP 就是 HTTPS（TLS+HTTP）。TLS 不能「在 TCP 之下」：IP 层
//   之下没有可靠字节流可依赖，且中间设备（NAT/防火墙）需要看到端口语义。
// Q-TLS3 性能开销大概多少？
//   ① 握手：TLS1.3 = 1-RTT + 2 次非对称运算（ECDHE + 签名/验签），局域网 ~1-2ms、
//      广域网 1×RTT 往返；长连接聊天摊薄到可忽略。② 稳态每条消息：AEAD 加解密 +
//      记录层开销 22~29 字节/record（5B 头 + 8B nonce + 16B tag），小消息占比明显；
//      AES-NI 硬件加速下 CPU 通常 +2~10%。③ 内存：每连接一个 SSL 对象（实测 RSS 峰值
//      100 连接组 +11.7MB ≈ 117KB/连接；50 连接组 ≈160KB/连接——固定开销小样本摊不开，
//      引用「每连接 KB」必须注明取自哪组）。实测对照表见 README（bench.py 同参明文 vs TLS）。
// ========================================================================
//
// 用法：TlsContext 全局一份（加载证书/CA）；每个连接 accept 后包一层 TlsConn。
// 头文件兼容：<openssl/ssl.h>（有 dev 头）或 openssl_min.h（仅运行库，同 sqlite3 手法）。
#ifndef CHAT_TLS_H_
#define CHAT_TLS_H_

#include <cstddef>
#include <string>

#if defined(__has_include) && __has_include(<openssl/ssl.h>)
#include <openssl/ssl.h>
#include <openssl/err.h>  // ERR_get_error / ERR_error_string_n（ssl.h 不传递包含）
#else
#include "chat/openssl_min.h"  // 自带最小声明，链接 libssl.so.3
#endif

#include "chat/net.h"

namespace chat {

bool tls_available();           // 本实现恒 true（编译期已定）；保留接口对称
const char* tls_backend_name();

// SSL_CTX 生命周期 + 证书配置（服务端/客户端两用）
class TlsContext {
public:
    TlsContext();
    ~TlsContext();
    // 服务端：加载证书 + 私钥（PEM）。失败记日志并返回 false
    bool init_server(const std::string& cert_file, const std::string& key_file);
    // 客户端：verify=false 跳过证书校验（自签教学默认）；true 则用 ca_file 验链
    bool init_client(bool verify, const std::string& ca_file);
    SSL_CTX* raw() { return ctx_; }

private:
    TlsContext(const TlsContext&);
    TlsContext& operator=(const TlsContext&);
    SSL_CTX* ctx_;
};

// 单连接 TLS 包装：握手 + 记录层读写（发送侧仍由上层 send_m 串行）
class TlsConn {
public:
    ~TlsConn();
    // 服务端在 accept 得到的 fd 上完成 TLS 握手；失败返回 NULL（fd 归还上层处理）
    static TlsConn* accept(TlsContext& ctx, socket_t fd);
    // 客户端主动连接侧握手（本项目 Python 客户端用 ssl 模块；此入口留给 C 测试/工具）
    static TlsConn* connect(TlsContext& ctx, socket_t fd);
    // 记录层读：>0 字节数；0=对端关闭；<0=错误/中断
    long read(char* buf, size_t len);
    // 有界写：尽力写出并返回【已写字节数】（可能 < len：对端读不动/超时，余量归调用方
    // 背压队列）；budget 为 WANT_WRITE 重试预算（每次最多阻塞 SO_SNDTIMEO）；<0 = 硬错误。
    // 慢客户端防护：发送线程用小预算（2），把余量交给队列，绝不无限重试拖死广播者。
    long write_progress(const char* data, size_t len, int budget);
    // 整块写（大预算的 write_progress）；false=失败
    bool write_all(const char* data, size_t len);
    void shutdown();
    const char* version() const;
    const char* cipher() const;

private:
    TlsConn(SSL* ssl);
    SSL* ssl_;
    bool done_;
};

}  // namespace chat

#endif  // CHAT_TLS_H_
