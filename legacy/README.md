# legacy/ —— 历史版本存档（对照/演进参考）

当前主力代码已工程化为 `include/ + src/ + tests/`（见根目录 README）。本目录按版本
保留单文件时代的源码，**不再维护**，仅供对照阅读（协议演进、可靠性设计问答等）。

| 文件 | 版本 | 说明 |
|---|---|---|
| `chat_server_fixed.cpp` / `chat_client_fixed.py` | v1 | `\|`+`\n` 文本协议完整业务（含已知缺陷清单） |
| `ChatClientFixed.exe` | v1 | Windows 打包版 |
| `chat_server_v2.cpp` / `chat_client_v2.py` | v2 | 长度前缀+JSON 帧层骨架 |
| `chat_server_v3.cpp` | v3 | Reactor 性能实验（epoll ET + 线程池，仅帧层） |
| `chat_server_v4.cpp` / `chat_client_v3.py` | v4 | 可靠性改造主力（心跳/待发队列/离线 E2EACK/幂等，头部 Q1–Q4 设计问答） |
| `chat_server_v5.cpp` | v5 单文件版 | 账号/房间/持久化的**拆分前**单文件实现（头部 Q1–Q6 设计问答）；拆分后的工程化版即 `src/server.cpp` 等 |

编译旧服务器（头文件已迁入 `include/chat/`，加 `-I` 即可）：

```bash
cd legacy
g++ -std=c++11 -Wall -pthread -I../include chat_server_v4.cpp -o chat_server_v4 \
    /usr/lib/x86_64-linux-gnu/libsqlite3.so.0
# v5 单文件版还要 -I../include/chat（其 #include "pwd_hash.h" 按裸文件名引用）
g++ -std=c++11 -Wall -pthread -I../include/chat chat_server_v5.cpp -o chat_server_v5 \
    /usr/lib/x86_64-linux-gnu/libsqlite3.so.0
```
