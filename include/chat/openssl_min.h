// include/chat/openssl_min.h —— OpenSSL 最小声明（仅当系统没有 openssl/ssl.h 头文件时启用）
//
// 与 sqlite3_api.h 同一手法：本项目验证机装了 libssl.so.3 运行库（OpenSSL 3.x，冻结
// ABI），但没有 libssl-dev。这里只声明本项目用到的十几个入口（SSL/TLS 客户端-服务端
// 握手 + 记录层读写 + 证书加载），直接链接 libssl.so.3 / libcrypto.so.3：
//
//   g++ ... /usr/lib/x86_64-linux-gnu/libssl.so.3 /usr/lib/x86_64-linux-gnu/libcrypto.so.3
//
// 注意只用【真实导出的函数】，不用头文件宏（如 SSL_CTX_set_min_proto_version 是
// SSL_CTX_ctrl 的宏包装——宏不导出符号，声明了也链接不到）。若系统装有 libssl-dev，
// chat/tls.h 会优先 #include <openssl/ssl.h>，本文件不参与编译。
#ifndef CHAT_OPENSSL_MIN_H_
#define CHAT_OPENSSL_MIN_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef struct ssl_st SSL;
typedef struct ssl_ctx_st SSL_CTX;
typedef struct ssl_method_st SSL_METHOD;
typedef struct ssl_cipher_st SSL_CIPHER;

// 结果码/模式常量（openssl/ssl.h 公开 ABI 值）
#define SSL_ERROR_NONE 0
#define SSL_ERROR_SSL 1
#define SSL_ERROR_WANT_READ 2
#define SSL_ERROR_WANT_WRITE 3
#define SSL_ERROR_WANT_X509_LOOKUP 4
#define SSL_ERROR_SYSCALL 5
#define SSL_ERROR_ZERO_RETURN 6

#define SSL_VERIFY_NONE 0x00
#define SSL_VERIFY_PEER 0x01
#define SSL_VERIFY_FAIL_IF_NO_PEER_CERT 0x02

#define SSL_FILETYPE_PEM 1
#define SSL_FILETYPE_ASN1 2

int OPENSSL_init_ssl(unsigned long long opts, const void* settings);
const SSL_METHOD* TLS_server_method(void);
const SSL_METHOD* TLS_client_method(void);

SSL_CTX* SSL_CTX_new(const SSL_METHOD* meth);
void SSL_CTX_free(SSL_CTX* ctx);
int SSL_CTX_use_certificate_file(SSL_CTX* ctx, const char* file, int type);
int SSL_CTX_use_PrivateKey_file(SSL_CTX* ctx, const char* file, int type);
int SSL_CTX_check_private_key(SSL_CTX* ctx);
void SSL_CTX_set_verify(SSL_CTX* ctx, int mode,
                        int (*verify_callback)(int, void*));  // X509_STORE_CTX* 不透明传参
int SSL_CTX_load_verify_locations(SSL_CTX* ctx, const char* CAfile, const char* CApath);
long SSL_CTX_set_options(SSL_CTX* ctx, long options);
long SSL_CTX_set_mode(SSL_CTX* ctx, long mode);

SSL* SSL_new(SSL_CTX* ctx);
void SSL_free(SSL* ssl);
int SSL_set_fd(SSL* s, int fd);
int SSL_accept(SSL* ssl);
int SSL_connect(SSL* ssl);
int SSL_read(SSL* ssl, void* buf, int num);
int SSL_write(SSL* ssl, const void* buf, int num);
int SSL_shutdown(SSL* ssl);
int SSL_get_error(const SSL* ssl, int ret);
const char* SSL_get_version(const SSL* ssl);
// 注意：SSL_get_cipher 是头文件宏且 3.x 未导出——用 SSL_CIPHER_get_name(SSL_get_current_cipher())
const SSL_CIPHER* SSL_get_current_cipher(const SSL* ssl);
const char* SSL_CIPHER_get_name(const SSL_CIPHER* c);

unsigned long ERR_get_error(void);
void ERR_error_string_n(unsigned long e, char* buf, unsigned long len);

#ifdef __cplusplus
}
#endif

#endif  // CHAT_OPENSSL_MIN_H_
