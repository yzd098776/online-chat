// src/tls.cpp —— TLS 封装实现（错误处理 + WANT_READ/WRITE 重试；复杂度说明见 tls.h）
#include "chat/tls.h"

#include <cstring>

#include "chat/log.h"

namespace chat {

bool tls_available() { return true; }

const char* tls_backend_name() {
    static bool inited = false;
    if (!inited) {
        OPENSSL_init_ssl(0, NULL);
        inited = true;
    }
    return "OpenSSL (libssl) —— TLS1.3 记录层";
}

static std::string ssl_err_string() {
    unsigned long e = ERR_get_error();
    if (e == 0) return "no error queued";
    char buf[256];
    ERR_error_string_n(e, buf, sizeof(buf));
    return std::string(buf);
}

TlsContext::TlsContext() : ctx_(NULL) {}
TlsContext::~TlsContext() {
    if (ctx_) SSL_CTX_free(ctx_);
}

bool TlsContext::init_server(const std::string& cert_file, const std::string& key_file) {
    OPENSSL_init_ssl(0, NULL);
    ctx_ = SSL_CTX_new(TLS_server_method());
    if (!ctx_) {
        LOG_ERROR() << "SSL_CTX_new 失败: " << ssl_err_string();
        return false;
    }
    if (SSL_CTX_use_certificate_file(ctx_, cert_file.c_str(), SSL_FILETYPE_PEM) != 1) {
        LOG_ERROR() << "加载证书失败: " << cert_file << " (" << ssl_err_string() << ")";
        return false;
    }
    if (SSL_CTX_use_PrivateKey_file(ctx_, key_file.c_str(), SSL_FILETYPE_PEM) != 1) {
        LOG_ERROR() << "加载私钥失败: " << key_file << " (" << ssl_err_string() << ")";
        return false;
    }
    if (SSL_CTX_check_private_key(ctx_) != 1) {
        LOG_ERROR() << "证书与私钥不匹配: " << ssl_err_string();
        return false;
    }
    SSL_CTX_set_verify(ctx_, SSL_VERIFY_NONE, NULL);  // 教学：不要求客户端证书（双向 mTLS 可改）
    LOG_INFO() << "TLS 上下文就绪 cert=" << cert_file << " key=" << key_file;
    return true;
}

bool TlsContext::init_client(bool verify, const std::string& ca_file) {
    OPENSSL_init_ssl(0, NULL);
    ctx_ = SSL_CTX_new(TLS_client_method());
    if (!ctx_) {
        LOG_ERROR() << "SSL_CTX_new 失败: " << ssl_err_string();
        return false;
    }
    if (verify) {
        SSL_CTX_set_verify(ctx_, SSL_VERIFY_PEER, NULL);
        if (!ca_file.empty() &&
            SSL_CTX_load_verify_locations(ctx_, ca_file.c_str(), NULL) != 1) {
            LOG_ERROR() << "加载 CA 失败: " << ca_file << " (" << ssl_err_string() << ")";
            return false;
        }
    } else {
        SSL_CTX_set_verify(ctx_, SSL_VERIFY_NONE, NULL);  // 自签教学默认：跳过校验
        LOG_WARN() << "TLS 证书校验已关闭（自签教学模式；生产请开 --tls-strict + CA）";
    }
    return true;
}

TlsConn::TlsConn(SSL* ssl) : ssl_(ssl), done_(false) {}
TlsConn::~TlsConn() {
    if (ssl_) {
        SSL_free(ssl_);
        ssl_ = NULL;
    }
}

TlsConn* TlsConn::accept(TlsContext& ctx, socket_t fd) {
    SSL* ssl = SSL_new(ctx.raw());
    if (!ssl) return NULL;
    SSL_set_fd(ssl, (int)fd);
    int rc = SSL_accept(ssl);
    if (rc != 1) {
        int err = SSL_get_error(ssl, rc);
        LOG_WARN() << "TLS 握手失败（服务端）ssl_err=" << err << " " << ssl_err_string();
        SSL_free(ssl);
        return NULL;
    }
    TlsConn* c = new TlsConn(ssl);
    LOG_INFO() << "TLS 握手完成（服务端）" << c->version() << " / " << c->cipher();
    return c;
}

TlsConn* TlsConn::connect(TlsContext& ctx, socket_t fd) {
    SSL* ssl = SSL_new(ctx.raw());
    if (!ssl) return NULL;
    SSL_set_fd(ssl, (int)fd);
    int rc = SSL_connect(ssl);
    if (rc != 1) {
        int err = SSL_get_error(ssl, rc);
        LOG_WARN() << "TLS 握手失败（客户端）ssl_err=" << err << " " << ssl_err_string();
        SSL_free(ssl);
        return NULL;
    }
    TlsConn* c = new TlsConn(ssl);
    LOG_INFO() << "TLS 握手完成（客户端）" << c->version() << " / " << c->cipher();
    return c;
}

long TlsConn::read(char* buf, size_t len) {
    if (done_) return 0;
    for (;;) {
        int n = SSL_read(ssl_, buf, (int)len);
        if (n > 0) return n;
        int err = SSL_get_error(ssl_, n);
        if (err == SSL_ERROR_ZERO_RETURN) { done_ = true; return 0; }  // 对端 close_notify
        if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE) continue;
        return -1;  // SSL_ERROR_SSL / SYSCALL：连接不可用
    }
}

long TlsConn::write_progress(const char* data, size_t len, int budget) {
    // 默认（重试同缓冲）模式：每次成功的 SSL_write 都完整写出当前切片；
    // WANT_WRITE = 对端/内核缓冲满（SO_SNDTIMEO 到点），消耗 budget 后返回进度——
    // 余量由调用方背压队列接管（历史版本这里无限 continue，慢客户端会把广播线程拖死）
    size_t sent = 0;
    int retries = 0;
    while (sent < len) {
        int n = SSL_write(ssl_, data + sent, (int)(len - sent));
        if (n > 0) { sent += (size_t)n; continue; }
        int err = SSL_get_error(ssl_, n);
        if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE) {
            if (++retries > budget) return (long)sent;  // 预算用尽：返回进度，剩余排队
            continue;
        }
        LOG_WARN() << "TLS 写失败 ssl_err=" << err << " " << ssl_err_string();
        return -1;
    }
    return (long)sent;
}

bool TlsConn::write_all(const char* data, size_t len) {
    return write_progress(data, len, 1 << 20) == (long)len;  // 大预算：语义同旧整块写
}

void TlsConn::shutdown() {
    if (ssl_ && !done_) {
        SSL_shutdown(ssl_);  // 尽力发 close_notify，失败不阻塞
        done_ = true;
    }
}

const char* TlsConn::version() const { return ssl_ ? SSL_get_version(ssl_) : "-"; }
const char* TlsConn::cipher() const {
    if (!ssl_) return "-";
    const SSL_CIPHER* c = SSL_get_current_cipher(ssl_);
    return c ? SSL_CIPHER_get_name(c) : "-";
}

}  // namespace chat
