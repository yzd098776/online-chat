// include/chat/net.h —— socket 薄封装（POSIX/Win 可移植层）
#ifndef CHAT_NET_H_
#define CHAT_NET_H_

#include <cstring>
#include <string>

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

namespace chat {

static const int kKeepIdleSec = 15;
static const int kKeepIntvlSec = 5;
static const int kKeepCnt = 3;
static const int kSendTimeoutSec = 3;
static const int kSendRetry = 2;

inline bool sock_valid(socket_t s) {
#ifdef _WIN32
    return s != INVALID_SOCKET;
#else
    return s >= 0;
#endif
}

inline void sock_close(socket_t s) {
#ifdef _WIN32
    closesocket(s);
#else
    close(s);
#endif
}

inline void sock_shutdown(socket_t s) {
#ifdef _WIN32
    shutdown(s, SD_BOTH);
#else
    shutdown(s, SHUT_RDWR);
#endif
}

inline long sock_send(socket_t fd, const char* buf, size_t len) {
#ifdef _WIN32
    return ::send(fd, buf, (int)len, 0);
#else
    return ::send(fd, buf, len, MSG_NOSIGNAL);
#endif
}

// 非阻塞发送（背压路径用）：内核发送缓冲满 → 立即 EAGAIN，绝不把发送线程挂住。
// Windows 无 MSG_DONTWAIT，退化为普通 send（行为同 sock_send；背压队列仍兜底）。
inline long sock_send_nb(socket_t fd, const char* buf, size_t len) {
#ifdef _WIN32
    return ::send(fd, buf, (int)len, 0);
#else
    return ::send(fd, buf, len, MSG_NOSIGNAL | MSG_DONTWAIT);
#endif
}

inline long sock_recv(socket_t fd, char* buf, size_t len) {
#ifdef _WIN32
    return ::recv(fd, buf, (int)len, 0);
#else
    return ::recv(fd, buf, len, 0);
#endif
}

// 非阻塞接收（排干用）：无数据立即 EAGAIN。Windows 无 MSG_DONTWAIT，退化为普通 recv。
inline long sock_recv_nb(socket_t fd, char* buf, size_t len) {
#ifdef _WIN32
    return ::recv(fd, buf, (int)len, 0);
#else
    return ::recv(fd, buf, len, MSG_DONTWAIT);
#endif
}

inline bool sock_would_block() {
#ifdef _WIN32
    return WSAGetLastError() == WSAEWOULDBLOCK;
#else
    return errno == EAGAIN || errno == EWOULDBLOCK;
#endif
}

inline bool sock_interrupted() {
#ifdef _WIN32
    return WSAGetLastError() == WSAEINTR;
#else
    return errno == EINTR;
#endif
}

inline void apply_socket_opts(socket_t fd) {
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

}  // namespace chat

#endif  // CHAT_NET_H_
