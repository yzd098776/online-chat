# 在线聊天系统统一镜像：服务器 / 交互客户端 / 自动化回归共用
#
#   构建:   docker compose build          （产出镜像 online-chat:latest）
#   服务器: docker compose up -d server
#   客户端: docker compose --profile client run --name chat-alice client \
#               --user alice --password 123 --pending-file /data/chat_pending_alice.json
#   回归:   docker compose --profile test run --rm test-smoke
#           docker compose --profile test run --rm test-bench
#
# 异常场景注入点（详见 VERIFY.md）：
#   docker network disconnect chatnet chat-alice  = 拔网线 / 关 WiFi
#   docker kill chat-alice                       = kill -9 / 任务管理器结束进程
#   docker restart chat-server                   = 服务器重启
FROM debian:bookworm-slim

# 默认 deb.debian.org 在部分网络下极慢——换 ustc 镜像源（可按需换成任意镜像）
# libssl-dev：pwd_hash.h 走 OpenSSL PBKDF2 档位（裸机无 dev 头时自动落内置标准实现）
RUN sed -i 's|deb.debian.org|mirrors.ustc.edu.cn|g' /etc/apt/sources.list.d/debian.sources \
    && apt-get update && apt-get install -y --no-install-recommends \
        g++ cmake make libsqlite3-dev libssl-dev openssl python3 python3-tk sqlite3 procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY CMakeLists.txt ./
COPY include/ ./include/
COPY src/ ./src/
COPY tests/ ./tests/
COPY client/ ./client/
COPY tools/ ./tools/

# CMake Release（-O2）构建 + 单测随镜像自检；可执行落到 /app 保持命令路径短
RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release \
    && cmake --build build -j \
    && ctest --test-dir build --output-on-failure \
    && cp build/chat_server_v5 /app/chat_server_v5

RUN mkdir -p /data
EXPOSE 8888
CMD ["./chat_server_v5", "8888", "--db", "/data/chat_server_v5.db", "--log-dir", "/data/logs"]
