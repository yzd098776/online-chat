// src/main.cpp —— 服务器入口：参数解析 + 日志初始化 + 启停信号
//
// 用法: ./chat_server_v5 [port] [--db FILE] [--idle MS] [--scan MS] [--token-ttl SEC]
//                        [--log-dir DIR] [--log-level debug|info|warn|error]
//                        [--conn-rate N --conn-burst N]     # B：每 IP 建连令牌桶（0=关）
//                        [--msg-rate N --msg-burst N]       # B：每用户消息令牌桶（0=关）
//                        [--words FILE --filter-mode replace|reject]   # B：敏感词 Trie
//                        [--tls --cert F --key F]           # C：TLS 传输
//                        [--pepper FILE]                    # Q7②：盲化 pepper（缺省 secrets/pepper.key）
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <string>

#include "chat/log.h"
#include "chat/net.h"
#include "chat/server.h"
#include "chat/util.h"

static chat::ChatServer* g_server = NULL;

static void on_signal(int) {
    if (g_server) g_server->requestStop();
}

int main(int argc, char** argv) {
#ifdef _WIN32
    WSADATA wsa;
    if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0) {
        std::fprintf(stderr, "WSAStartup 失败\n");
        return 1;
    }
#else
    std::signal(SIGPIPE, SIG_IGN);
#endif

    chat::ServerConfig cfg;
    cfg.port = 8888;
    cfg.db_path = "chat_server_v5.db";
    std::string log_dir = "logs";
    std::string log_level = "info";

    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--db") == 0 && i + 1 < argc) cfg.db_path = argv[++i];
        else if (std::strcmp(argv[i], "--idle") == 0 && i + 1 < argc) cfg.idle_ms = std::atoll(argv[++i]);
        else if (std::strcmp(argv[i], "--scan") == 0 && i + 1 < argc) cfg.scan_ms = std::atoll(argv[++i]);
        else if (std::strcmp(argv[i], "--token-ttl") == 0 && i + 1 < argc)
            cfg.token_ttl = std::atoll(argv[++i]);
        else if (std::strcmp(argv[i], "--log-dir") == 0 && i + 1 < argc) log_dir = argv[++i];
        else if (std::strcmp(argv[i], "--log-level") == 0 && i + 1 < argc) log_level = argv[++i];
        // B：限流参数
        else if (std::strcmp(argv[i], "--conn-rate") == 0 && i + 1 < argc)
            cfg.conn_rate = std::atof(argv[++i]);
        else if (std::strcmp(argv[i], "--conn-burst") == 0 && i + 1 < argc)
            cfg.conn_burst = std::atof(argv[++i]);
        else if (std::strcmp(argv[i], "--msg-rate") == 0 && i + 1 < argc)
            cfg.msg_rate = std::atof(argv[++i]);
        else if (std::strcmp(argv[i], "--msg-burst") == 0 && i + 1 < argc)
            cfg.msg_burst = std::atof(argv[++i]);
        // B：敏感词
        else if (std::strcmp(argv[i], "--words") == 0 && i + 1 < argc) cfg.words_file = argv[++i];
        else if (std::strcmp(argv[i], "--filter-mode") == 0 && i + 1 < argc)
            cfg.filter_reject = (std::strcmp(argv[++i], "reject") == 0);
        // C：TLS
        else if (std::strcmp(argv[i], "--tls") == 0) cfg.tls = true;
        else if (std::strcmp(argv[i], "--cert") == 0 && i + 1 < argc) cfg.tls_cert = argv[++i];
        else if (std::strcmp(argv[i], "--key") == 0 && i + 1 < argc) cfg.tls_key = argv[++i];
        // Q7②：pepper 文件（口令派生密钥盲化；首次启动自举 0600）
        else if (std::strcmp(argv[i], "--pepper") == 0 && i + 1 < argc)
            cfg.pepper_path = argv[++i];
        else if (argv[i][0] != '-') cfg.port = std::atoi(argv[i]);
    }
    if (cfg.idle_ms < 1000) cfg.idle_ms = 1000;
    if (cfg.scan_ms < 100) cfg.scan_ms = 100;
    if (cfg.token_ttl < 60) cfg.token_ttl = 60;
    if (cfg.tls && (cfg.tls_cert.empty() || cfg.tls_key.empty())) {
        std::fprintf(stderr, "--tls 需要 --cert 与 --key（自签证书见 tools/gen_cert.sh）\n");
        return 1;
    }

    // 日志：控制台按 --log-level 过滤，文件常开 DEBUG 便于排障；按天切分 chat_YYYY-MM-DD.log
    chatlog::Level lv = chatlog::parse_level(log_level, chatlog::L_INFO);
    chatlog::init(lv, log_dir, chatlog::L_DEBUG);

    LOG_INFO() << "正在启动聊天服务器 v5 ..."
               << (cfg.tls ? "（TLS 模式）" : "（明文模式）");
    chat::ChatServer server(cfg);
    g_server = &server;
    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);
    bool ok = server.start();
    g_server = NULL;
    chatlog::shutdown();
#ifdef _WIN32
    WSACleanup();
#endif
    return ok ? 0 : 1;
}
