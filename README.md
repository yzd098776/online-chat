# 在线聊天程序（Online Chat）

基于 TCP Socket 的多用户局域网聊天系统，**C++11 多线程服务器 + Python 图形化客户端**。
本项目为网络通信编程课程设计：账号体系、房间路由、消息持久化与游标历史分页，工程侧
配备 CMake 构建、分级日志、协议层单测（ctest）、可调压测与覆盖率报告。

**当前主力：`src/`（服务器 v5，工程化版）+ `client/chat_client_v4.py`**。协议见
[PROTOCOL.md](PROTOCOL.md)（含错误码表），目录迁移对照见 [docs/migration.md](docs/migration.md)，
历史版本存档见 [legacy/](legacy/README.md)。

## 功能特性

- **账号**：注册 / 登录；**挑战-应答认证（口令/派生密钥不进帧）**：`*_HELLO → CHALLENGE{nonce, salt} →
  LOGIN{HMAC(K, op‖user‖nonce)}`（K=PBKDF2(pwd,salt)；证明绑定语境/身份），nonce 一次性 +
  60s 过期（抗重放，HELLO 挂每 IP 限流）；
  口令 = `salt`(16B 服务端随机) + `PBKDF2-HMAC-SHA256`(100000)；**入库存 pepper 盲化值
  `K ⊕ HMAC(pepper, user‖salt)`**（反 pass-the-hash：拖库拿不到登录凭据，pepper 0600 独立保管）；
  登录下发 Token（32B 随机 + 过期时间，**内存只存 SHA256(token)**），断线重连免密恢复（失效自动回落口令）
- **错误显式**：重复注册/登录 → E1001/E1004/E1005 等**错误码 + 中文文案**（PROTOCOL.md 6.4），不静默失败
- **房间**：默认 `lobby`；`/join /leave /rooms /create`；**广播只发本房间成员**（O(房间成员)，按 room→members 路由）
- **持久化**：SQLite 四表 `users/rooms/messages/offline_messages`；**所有 SQL 参数化绑定**（防注入）；
  **group commit**（并发消息攒一个事务共享 fsync）；私聊落库即 ACK，不在线走 `offline_messages.delivered` 标志补发
- **历史**：进房自动拉最近 50 条；上滚/`/older` 按 **`(ts,id)` 游标**翻页——不用 OFFSET（实测深页快 80 倍）
- **可靠性**：10s 心跳 / 30s 判死；指数退避重连（±20% 抖动）；待发队列补发；(user,seq) 幂等去重；
  **背压**（每连接发送队列 256KB 上限，超限踢慢客户端，不拖垮广播）
- **风控**：令牌桶限流（每 IP 建连 / 每用户消息，超限 E4001/E4002 + 日志）；敏感词 **Trie** 过滤
  （replace 脱敏 / reject 拒绝 E4003）
- **TLS（可选）**：OpenSSL 传输加密（自签证书 + 客户端校验开关），明文/TLS 由命令行切换
- **工程**：CMake（Debug/Release/-O2）+ 分级日志（按天切分）+ 73 项单测 + Python/C++ 双压测端（CSV/Markdown）

## 架构

```
 ┌──────────────────────────┐                          ┌───────────────────────────────────────────┐
 │   client/chat_client_v4  │        TCP :8888         │              chat_server_v5                │
 │   (tkinter / headless)   │ ◄── 4B 长度前缀 + JSON ──►│                                           │
 │                          │        帧（1MiB）         │   src/main.cpp     参数解析 / 日志初始化    │
 │  · conn 线程 + 心跳线程  │                          │        │                                  │
 │  · PendingStore 待发队列 │                          │   src/server.cpp   连接会话 / 认证 / 广播   │
 │  · TokenStore 免密重连   │                          │        │                                  │
 └──────────────────────────┘                          │   ┌────┴─────┬───────────┬─────────────┐   │
                                                       │   ▼          ▼           ▼             ▼   │
                                                       │ frame.*    RoomRouter  seq_dedup   database.*│
                                                       │ minijson.* (room→成员)  (user,seq)  SQLite/WAL│
                                                       │ protocol.*  路由层      幂等窗口     4 表+2 索引│
                                                       │ net.h  token_book.h  pwd_hash.h             │
                                                       │                                           │
                                                       │   log.{h,cpp} ──► logs/chat_YYYY-MM-DD.log │
                                                       └───────────────────────────────────────────┘

 消息路径：MESSAGE(to=ALL) ─► seq 去重 ─► messages 落库(room_id) ─► RoomRouter.members(room)
                                       └─────────────► 只向本房间成员广播 ─► ACK（已送达=已持久化）
           私聊(to=用户)  ─► messages 落库(type=private) ─► 在线直投 / 离线 offline_messages 补发
```

| 层 | 文件 | 职责 |
|---|---|---|
| 协议层 | `frame.*` `minijson.h` `protocol.*` `seq_dedup.*` | 帧编解码（半包/粘包）、JSON、错误码/游标、幂等窗口 |
| 路由层 | `room_router.*` | room→members、user→room；广播目标集 = O(房间成员) |
| 存储层 | `database.*` + `pwd_hash.h` | 四表持久化（全参数化绑定）、PBKDF2 口令 |
| 装配层 | `server.*` `token_book.h` `net.h` | 连接生命周期、认证/顶号、广播扇出、心跳扫描 |
| 风控层 | `rate_limiter.*` `word_filter.*` | 令牌桶限流（每 IP 建连/每用户消息）、敏感词 Trie |
| 传输层 | `tls.*` + `openssl_min.h` | TLS 记录层（可选）；无 dev 头时冻结 ABI 声明直链 libssl.so.3 |
| 横切 | `log.*` | 分级日志（DEBUG/INFO/WARN/ERROR，时间戳+线程 id+文件行号，按天切分） |

## 通信协议（摘要）

帧 = `4 字节大端长度 + UTF-8 JSON`（细节与字段表见 [PROTOCOL.md](PROTOCOL.md)）：

| 帧 | 方向 | 语义 |
|---|---|---|
| `REGISTER_HELLO` / `LOGIN_HELLO` | C→S | 挑战-应答第一步（口令不进帧，Q7） |
| `CHALLENGE` | S→C | `content`=一次性 nonce(32B)，`salt`=16B 随机盐 |
| `REGISTER` / `LOGIN` | C→S | 播种 `K=PBKDF2(pwd,salt)`+`hmac` / 证明 `HMAC(K,nonce)` → `REGISTER_OK`/`AUTH_OK` |
| `AUTH` | C→S | Token 免密恢复 → `AUTH_OK{token, exp}` |
| `JOIN` `LEAVE` `ROOMS` `CREATE` | C→S | 房间四操作 → `JOIN_OK{members}` 等 |
| `HIST` / `INBOX` | C→S | 历史分页（`content` = `ts:id` 游标，空 = 最近 50 条）→ `HISTORY`/`INBOX` |
| `MESSAGE` | 双向 | `to=ALL` 群聊（本房间）/ `to=用户` 私聊；`seq` 为幂等键 |
| `ACK` / `NACK` | S→C | 投递确认/拒绝（`content` = 客户端 seq） |
| `ERR` | S→C | `code` + 中文 `content`（错误码表：PROTOCOL.md 6.4） |
| `PING`/`PONG`、`SYSTEM`、`USERLIST` | | 心跳 / 房间内系统通知 / 房间成员列表 |

## 从零开始的编译命令序列

> 依赖：`g++`（C++11）、`cmake` ≥ 3.13、`make`、SQLite3（`libsqlite3-dev`，或仅运行库
> `libsqlite3-0` + 本仓库自带的 `include/chat/sqlite3_api.h` 冻结 ABI 声明）。
> Debian/Ubuntu 一步：`sudo apt install g++ cmake make libsqlite3-dev`

```bash
# ── 步骤 1：配置（生成构建系统；默认 Release = -O2 -DNDEBUG） ──
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
```
作用：探测编译器/pthread/SQLite3，生成 Makefile。预期输出（节选）：
```
-- Build type: Release
-- Found Threads: TRUE
-- SQLite3: /usr/include /usr/lib/x86_64-linux-gnu/libsqlite3.so
-- Configuring done
-- Generating done
-- Build files have been written to: .../build
```

```bash
# ── 步骤 2：构建（-j 并行编译 chat_core 静态库 + 服务器 + 单测） ──
cmake --build build -j
```
作用：产出 `build/chat_server_v5` 与 `build/tests/chat_tests`。预期输出（末行）：
```
[100%] Built target chat_server_v5
[100%] Built target chat_tests
```

```bash
# ── 步骤 3：跑全部单测（ctest 逐套件：frame/minijson/protocol/room_router/seq_dedup/
#                                    rate_limiter/word_filter/auth/database 共 73 用例） ──
ctest --test-dir build --output-on-failure
```
预期输出：
```
100% tests passed, 0 tests failed out of 9
Total Test time (real) = 0.02 sec
```

```bash
# ── 步骤 4：启动服务器（默认 8888 端口；--log-dir 控制日志目录） ──
./build/chat_server_v5 8888 --db chat_server_v5.db --log-dir logs
```
预期输出（分级日志格式：时间戳 / 线程 id / 文件行号）：
```
[2026-09-24 22:55:00.123] [INFO ] [t:7f8a...] [main.cpp:66] 聊天服务器 v5 已启动（账号 + 房间 + 持久化/游标分页）
[2026-09-24 22:55:00.123] [INFO ] [t:7f8a...] [main.cpp:67] 监听端口: 8888（0.0.0.0），MAX_FRAME=1MiB
[2026-09-24 22:55:00.123] [INFO ] [t:7f8a...] [main.cpp:68] 口令哈希: builtin PBKDF2-HMAC-SHA256（RFC 2898，与 OpenSSL/Python 逐比特一致）
```

```bash
# ── 步骤 5：Debug 构建（可选；-g -O0，配 gdb）与覆盖率（可选；gcov） ──
cmake -S . -B build-debug -DCMAKE_BUILD_TYPE=Debug && cmake --build build-debug -j
bash tools/coverage.sh        # 无 cmake 环境的一键 gcov；CMake 等价路径见脚本头注释
```

无 CMake 的裸机直编（等价产物；SQLite/OpenSSL 无 dev 头时直链运行库）：
```bash
g++ -std=c++11 -Wall -O2 -pthread -Iinclude src/*.cpp \
    /usr/lib/x86_64-linux-gnu/libsqlite3.so.0 \
    /usr/lib/x86_64-linux-gnu/libssl.so.3 /usr/lib/x86_64-linux-gnu/libcrypto.so.3 \
    -o build/chat_server_v5     # 有 dev 头则换 -lsqlite3 -lssl -lcrypto

# C++ 压测端（可选）：src/ 全部 .cpp 换成除 main.cpp 外的清单 + tools/bench_client.cpp
g++ -std=c++11 -Wall -O2 -pthread -Iinclude src/log.cpp src/frame.cpp src/protocol.cpp \
    src/seq_dedup.cpp src/room_router.cpp src/database.cpp src/rate_limiter.cpp \
    src/word_filter.cpp src/tls.cpp src/server.cpp tools/bench_client.cpp \
    /usr/lib/x86_64-linux-gnu/libsqlite3.so.0 \
    /usr/lib/x86_64-linux-gnu/libssl.so.3 /usr/lib/x86_64-linux-gnu/libcrypto.so.3 \
    -o build/bench_client
```

## 运行

```bash
# 服务器
./build/chat_server_v5 8888 [--db F] [--idle 30000] [--scan 5000] [--token-ttl 604800]
                            [--log-dir logs] [--log-level debug|info|warn|error]

# 客户端（GUI 需 python3-tk）
python3 client/chat_client_v4.py --user alice --password 123
python3 client/chat_client_v4.py --user alice --password 123 --headless
#   /register /login /join /leave /rooms /create /older /inbox @用户=私聊 /status /retry /quit

# Docker
docker compose build && docker compose up -d server
docker compose --profile client run --name chat-alice client \
    --user alice --password 123 --pending-file /data/chat_pending_alice.json
```

### 日志

`chatlog`（`include/chat/log.h`，自研 ~180 行）：`LOG_INFO() << ...` 流式宏，输出
`[时间戳.mmm] [级别] [t:线程id] [文件:行号] 内容`；控制台按 `--log-level` 过滤，
文件常开 DEBUG、按天切分 `logs/chat_YYYY-MM-DD.log`。**业务代码禁止 printf/cout 打调试信息。**

### 测试与覆盖率

```bash
ctest --test-dir build --output-on-failure   # 9 套件 / 73 用例
bash tools/coverage.sh                       # gcov：协议与路由模块行覆盖率
```

| 模块 | 文件 | 行覆盖率（实测） |
|---|---|---|
| 协议层 | `frame.cpp` | 95.65% |
| 协议层 | `protocol.cpp` | 100.00% |
| 协议层 | `seq_dedup.cpp` | 100.00% |
| 路由层 | `room_router.cpp` | 100.00% |
| 协议层 | `minijson.h` | 65.83% |
| 风控层 | `rate_limiter.cpp` | 90.00% |
| 风控层 | `word_filter.cpp` | 85.57% |
| **合计（协议+路由+风控）** | | **73.4%（目标 ≥70% 达标）** |

> 合计口径 = **按代码行加权**（总覆盖行 731 / 总行数 996，大文件权重高），
> 不是分项百分比的算术平均（算术平均 ≈ 91%）——`minijson.h` 720 行体量最大，
> 它的 65.83% 把加权合计拉到 73.3%。

单测覆盖：编解码往返、半包/粘包拼接、超长帧、畸形/非法 JSON（截断转义、代理对、混型数组、
数字边界）、(user,seq) 去重、房间成员隔离、游标校验、令牌桶（突发/回填/时钟回拨/键隔离/GC）、
Trie（最长匹配/UTF-8/大小写）、**凭据安全**（PBKDF2/HMAC 对 RFC 公开向量、挑战 nonce 绑定、
Token 哈希表、CSPRNG 形态）、**持久层 group commit 并发**（8 线程×50 条一条不少）等
（`tests/`；极简断言框架 `minitest.h`——GoogleTest 需联网拉取，按项目约定未引入，接口同构可平移）。
端到端冒烟 `tools/test_v5_smoke.py` **63 项断言**（含挑战抗重放、限流 E4001/E4002、
敏感词 E4003、TLS 密文往返）。

## 限流与敏感词（B）

```bash
./build/chat_server_v5 8888 --conn-rate 50 --conn-burst 100     --msg-rate 20 --msg-burst 40 --words tools/words_sample.txt --filter-mode replace
```

- **令牌桶**：以 rate 个/秒补令牌、桶容量 burst；请求消耗 1 个，桶空拒绝（E4001=每 IP 建连、
  E4002=每用户消息，超限记 WARN 日志）。对比漏桶：漏桶强制匀速削平突发，令牌桶允许攒额度
  突发——更贴合建连/发消息的自然突发形态。单次判定 O(1)。消息限流在 (user,seq) 去重前，
  被拒不消耗幂等槽位（稍后重试同 seq 可成功）。
- **敏感词 Trie**：建树 **O(Σ|词|)**，扫描 **O(n×L)** 最坏（n=文本字节数，L=最长词字节数，
  失配即剪枝）；词表上万/文本很长时换 **AC 自动机**（Aho-Corasick）预处理 O(Σ|词|)、扫描
  **O(n)**。匹配策略：从左到右【最长匹配后整段跳过】（词表 {敏感, 敏感词} 打「敏感词」命中
  整词）；ASCII 大小写不敏感；替换按 UTF-8 字符数打 `*`。`--filter-mode`：`replace` 脱敏放行
  （默认）/ `reject` 拒绝（E4003）。

## TLS（C）

```bash
bash tools/gen_cert.sh certs                       # 自签证书（SAN=localhost/127.0.0.1）
./build/chat_server_v5 8888 --tls --cert certs/server.crt --key certs/server.key
python3 client/chat_client_v4.py --user alice --password 123 --tls            # 自签：默认不验
python3 client/chat_client_v4.py ... --tls --tls-strict --ca certs/server.crt  # 严格：验链
```

**握手流程**（TLS1.3，OpenSSL 3.x 默认；TCP 三次握手之后、应用数据之前）：

```
Client                                Server
  | ──── TCP SYN/ACK（可靠字节流就绪）─────── |
  | ──── ClientHello（套件/密钥份额/随机）───> |   ① 协商参数
  | <─── ServerHello + 证书 + 签名 + Finished |   ② 亮证书并用私钥签名证明持有
  |     （校验证书：--tls-strict 用 CA 验链） |
  | ──── Finished（双方算出主密钥）──────────> |   ③ (EC)DHE 协商共享密钥
  | ⟷═══ TLS 记录层（AEAD 加密应用数据）════⟷ |   ④ 每帧 5B 头+16B tag 的加密记录
```

**为什么 TLS 在 TCP 之上**：TLS 是「可靠字节流上的安全记录协议」——需要下层提供可靠、
有序、不重复的字节流来承载记录边界与握手重组（TCP 恰好提供）；加密/MAC 由 TLS 记录层负责，
重传/拥塞控制仍归 TCP，分层各管一段。换 UDP 要用 DTLS（自己在丢包/乱序下做握手与防重放）；
放到 HTTP 之上就是 HTTPS。不能放 TCP「之下」：IP 层之下没有可靠字节流可依赖。

**性能开销（实测对照）**：握手 1-RTT + 2 次非对称运算（长连接摊薄后可忽略）；稳态每条消息
AES-GCM 加解密 + 记录层 22~29B 开销。`tools/bench.py` 同参实跑（10s 窗口、暖机 2s、零丢失，
**并发竞态修复后实测**，明文/TLS 同条件配对）：

| 传输 | 并发 | 速率 | QPS | P50 | P99 | CPU | RSS 峰值 |
|---|---|---|---|---|---|---|---|
| 明文 TCP | 50 | 2/s | 100.0 | 0.48ms | 2.16ms | 1.3% | 10.3 MB |
| TLS1.3 | 50 | 2/s | 100.0 | 0.65ms | 3.21ms | 1.4% | 18.3 MB |
| 明文 TCP | 100 | 5/s | **500.0** | 0.50ms | 3.59ms | 5.2% | 12.6 MB |
| TLS1.3 | 100 | 5/s | **500.0** | 0.60ms | 3.21ms | 6.0% | 24.3 MB |

**口径**：上表绝对值只作**同批配对对照**（本机延迟绝对值受宿主噪声支配、不可复现，见已知
限制 #1）；结论取配对差值。即同机同载下 TLS ≈ **P50 +21~36%（100/50 连接组），P99 差值
被噪声淹没（±30% 无方向，不构成结论），CPU 相对 +12~15%**；内存开销分组标注：
**100 连接组 12.6→24.3 MB（+117KB/连接）**、**50 连接组 10.3→18.3 MB（+160KB/连接）**
——SSL 对象固定开销在小样本里摊不开，引用「每连接多少 KB」时须注明取自哪组。
无 OpenSSL 头文件的机器可用 `include/chat/openssl_min.h` 冻结 ABI 声明直链 `libssl.so.3`
（同 sqlite3_api.h 手法，零安装）。

## 压测

```bash
# Python 压测端（灵活；P99 含 Python 调度抖动，适合功能对照）
python3 tools/bench.py --connections 100 --rate 5 --duration 10 [--pid <服务器pid>] \
                       [--room private|大厅名] [--csv ...] [--markdown ...]

# C++ 压测端（20 线程 poll 多路复用，直接复用 frame.*/protocol 层；数字进报告用它）
./build/bench_client --port 8888 --connections 100 --threads 20 --rate 5 --duration 10 \
                     [--room private|大厅名] [--pid <服务器pid>] [--csv ...]
```
口径：QPS = 测量窗口内 ACKed/s（服务器先写 SQLite 再 ACK）；延迟 = MESSAGE send → ACK RTT；
CPU/内存采样 `/proc/<pid>/{stat,statm}`。每次运行追加一行到 `tools/bench_result.csv`
并生成 `tools/bench_report.md`。以下为本机实测（10s 窗口，暖机 2s，零丢失）——
**并发竞态（SeqDeduper 无锁）修复后实测，修复前的历史数字已作废**：

| 用例 | 并发 | 速率 | QPS | P50 | P90 | P99 | max | CPU | RSS 峰值 |
|---|---|---|---|---|---|---|---|---|---|
| 独立房间（纯 RTT） | 50 | 2 msg/s/连接 | **100.0** | 0.48ms | 1.04ms | 2.16ms | 3.55ms | 1.3% | 10.3 MB |
| 共享大厅（扇出×50） | 50 | 2 msg/s/连接 | **100.0** | 0.24ms | 0.59ms | 2.74ms | 3.82ms | 0.9% | 10.6 MB |
| 独立房间（高并发） | 100 | 5 msg/s/连接 | **500.0** | 0.50ms | 1.30ms | 3.59ms | 9.46ms | 5.2% | 12.6 MB |

> 口径提醒：本机延迟绝对值受宿主噪声支配（好/坏窗口差可达 20×，详见已知限制 #1），
> 上表数字只作**同批配对对照**用（如明文 vs TLS、优化前 vs 优化后）；承载能力看下节
> C++ 压测端的**丢失率**口径。

## 灌数据验证（历史分页，数字实跑）

`python3 tools/seed_and_bench.py`：灌 10 万条消息 + EXPLAIN QUERY PLAN + 游标/OFFSET 对照。

| 用例 | 实测 |
|---|---|
| 「最近 50 条」 | avg 0.049 ms |
| 「翻到第 100 页」（游标上翻 100 页） | 总 5.35 ms（0.054 ms/页；第 100 页单查 0.045 ms） |
| 对照 LIMIT/OFFSET 第 100 页 | 0.238 ms |
| 对照 LIMIT/OFFSET 第 1781 页 | **3.68 ms**（深分页线性退化） |

`EXPLAIN QUERY PLAN` 全部 `SEARCH messages USING INDEX idx_messages_room_ts/..._receiver_ts`，
无全表扫描、无临时排序。

## 项目结构

```
online-chat/
├── CMakeLists.txt            # C++11 / -O2 / Debug+Release / pthread / BUILD_TESTING / COVERAGE
├── include/chat/             # 公共头（frame/minijson/protocol/room_router/seq_dedup/
│                             #   rate_limiter/word_filter/tls/openssl_min/database/
│                             #   token_book/server/net/log/pwd_hash/sqlite3_api）
├── src/                      # 实现 + main.cpp
├── tests/                    # minitest.h + 9 套件 73 用例 → ctest（含 auth/database）
├── tools/                    # bench.py 压测（Python）/ bench_client.cpp 压测（C++，找上限）
│                             #   seed_and_bench.py 灌数据 / test_v5_smoke.py 冒烟
│                             #   test_durability.py kill -9 持久性对拍（ACK=已落盘）
│                             #   coverage.sh gcov 覆盖率 / gen_cert.sh 自签证书
│                             #   words_sample.txt 敏感词表样例
├── client/chat_client_v4.py  # Python 客户端（GUI + headless）
├── docs/migration.md         # 单文件 → 工程化布局迁移对照
├── legacy/                   # v1–v4 + v5 单文件版存档（legacy/README.md）
├── PROTOCOL.md               # 帧格式 + 可靠性扩展（第5节）+ 账号/房间/历史（第6节，含错误码表）
├── VERIFY.md                 # 异常场景逐步复现手册
└── Dockerfile / docker-compose.yml
```

## 已知限制（诚实边界）

1. **承压上限（有数字版；结论只建立在丢失率上——延迟绝对值本机不可测，理由见下）**。
   C++ 压测端（`build/bench_client`）同机 loopback、独立房间纯 RTT、**并发竞态修复后多轮
   实测**（修复前数字作废）：

   | 注入（1000 连接×速率） | 实测 QPS | 丢失（多轮） | RSS |
   |---|---|---|---|
   | 5000/s | 注满即达（未达上限） | **0**（多轮 3/3） | 40 MB |
   | 8000/s | 注满即达（未达上限） | **0（2 轮）/ 1.9 万（1 轮，坏窗口）** | 40 MB |
   | 2500/s（500 连接） | 2500 | 0 | 26 MB |

   - **「注入 5000 QPS 零丢失」是承载住了，不是峰值**；往上打到 8000 注入，VM 静默时仍零丢失。
     结论口径：**零丢失下限 5000 QPS，上限未标定（≥8000 受限宿主噪声不能钉）**；膝点写
     「5000 起，受宿主影响」。
   - **吞吐数字的前提**：本机是 VM，`synchronous=FULL` 的每次 COMMIT 实际由**宿主磁盘缓存**
     接管 fsync（我们已显式写死 FULL 并回读记日志，只保证「调用到 fsync」，无法保证宿主不骗人）。
     **真机物理盘上每 COMMIT 真实落盘，吞吐预计低于本报告值**——引用「5000/8000 QPS」时请带
     这个前提；耐用性口径（ACK=已落盘）不因此改变。
   - **延迟绝对值删去不报**：本机 2 核 VM 的宿主调度噪声主导延迟（同一配置好/坏窗口 P99
     差异达 20×，且曾出现「8000 注入档比 5000 档延迟更低」的物理不可能结果——即噪声铁证），
     单机 loopback 的绝对值不可复现、不可引用。**相对对照仍有效**（同一噪声窗口内配对测量，
     见下文 TLS 一节）。
   - **持久性优先级高于吞吐**：`tools/test_durability.py` 做了 kill -9 对拍——30 连接洪水
     中途 SIGKILL 服务端，客户端收到的 **73748 条 ACK 全部在 messages 表**（缺 0；库里
     73749 行，多出的 1 行正是「已提交、ACK 尚未送达」，即 ACK 不早于 COMMIT 的直接证据）。
     group commit 不牺牲该不变式：写手等批次 COMMIT 返回后才 ACK；`synchronous=FULL`
     显式写死（启动日志可验：journal_mode=wal / synchronous=2）。
   - **一段排查留档（证据链三件套，详见 VERIFY.md 附录 A）**：`SeqDeduper` 无锁数据竞争
     的证据 = ① dmesg 段错误留痕（`_M_find_before_node` 野指针）+ ② TSan **负对照**（临时
     剥锁，同一套编译/运行必报 `data race seq_dedup.cpp:13`，证明零告警不是漏报）+ ③ 修复后
     TSan 全量 + 200/500 连接各 30s + churn 零告警、`concurrent_stress` 回归用例通过。
     竞态修复前的 500/1000 档表**全部作废**（崩溃与宿主噪声双重污染，方向无法拆分）——
     「修完 bug 重跑了全部数字」，本页数字均为修复后实测。另注：压测期间还观察到几次服务端
     退出，复核为端口被占用导致绑定失败退出（与竞态无关，已单列）。**共享可变状态默认要锁**。
   - 结论（与架构段口径统一）：**「千级连接舒适区」是连接管理/内存维度**（1000 连接零丢失、
     ≈40KB/连接）；吞吐只报丢失率口径。背压（256KB/连接队列，超限发 E4004 并踢）把
     「慢客户端拖死广播」隔离到单连接；被踢客户端识别 E4004 后慢速重连，不会重连风暴。
2. **TLS 为可选教学实现**。`--tls` 已提供传输加密（TLS1.3），但默认客户端不验证书
   （自签场景，防窃听不防中间人）；生产必须 `--tls-strict` + 独立 CA 签发。口令已移出帧
   （挑战-应答，见 Q7/PROTOCOL.md 6.1）。**残余如实声明**：REGISTER 帧被抓取 = 攻击者获得
   派生密钥 K = **可永久登录本服**（但拿不到原口令、不跨站复用）——这是无 PKI/PAKE 下
   「注册必须一次性送达验证材料」的理论下界；K 入库前经 pepper 盲化（反 pass-the-hash，
   拖库拿不到 K），建号仍建议走 TLS。**pepper 边界**：pepper 与 DB 同机 = 「防拖库、
   不防拖整机」——整机被拿走则盲化可逆；真解是 PAKE/HSM/密钥分管，超出本项目范围。
   换错 pepper 开旧库会被启动闸门明确拒绝（`meta['pepper_id']` 比对，冒烟 T16b 覆盖），
   不会退化成「全员密码错误」式的静默故障；pepper 文件读不了/坏长度一律 abort，绝不降级。
   **备份纪律：`pepper.key` 必须随 DB 一起进备份集**——只备 DB 丢了 pepper = 全库盲化永久
   不可恢复（症状又是「所有人都口令错误」）；安全上建议二者分开存放/分管，但缺一不可
   （VERIFY.md 附录 B 有排查指引）。开销见上文对照表（100 连接组 P50 +21%、内存 +117KB/连接）。
3. **单进程单实例部署**。无多实例/集群：Token 表、(user,seq) 去重窗口、房间成员集都在
   内存，**服务器重启即失效**（客户端回落口令登录；重启窗口内的重发重复由客户端显示去重兜底）。
   SQLite 单写者：全部写路径串行于一把互斥锁（消息写走 group commit 攒批共享 fsync），
   WAL 下读写不互斥但写写互斥。
4. **Token 无吊销/滑动续期**：签发后只能等过期（`--token-ttl`，默认 7 天）；顶号换发新 Token
   但旧 Token 在过期前仍可被再次使用（教学取舍，生产应加吊销表与单次有效续期）。
5. **历史分页只能顺序上翻**（游标语义），无「跳到第 N 页」；无全文搜索/按发送者过滤索引。
6. **口令/房间无高级策略**：房间无密码/管理员/踢人；用户名无保留字以外的强制规范；
   日志按天切分但**无自动清理/压缩**（需外部 logrotate 或定时任务）。
7. **GUI 为 tkinter 教学级**：无表情/文件传输/已读回执；「上滚翻页」的视口保持是近似实现。
8. **压测精度分两档**：`tools/bench.py`（Python）灵活但 P99 含客户端调度抖动，适合功能对照
   （明文 vs TLS）；`build/bench_client`（C++）用于标定上限/尾延迟——数字进报告用后者。
9. **限流/敏感词是基础版**：令牌桶按 key 常驻内存（有键表上限与空闲 GC，非分布式限流）；
   敏感词为朴素 Trie（无 AC 自动机、无变体/谐音对抗），词表需人工维护。
10. **OpenSSL 冻结 ABI 声明（openssl_min.h）只覆盖本项目用到的入口**，刻意避开头文件宏
   （如 `SSL_CTX_set_min_proto_version`）；升级 OpenSSL 大版本时应回归 TLS 冒烟（T20）。

## 异常场景验证

kill -9 / 拔网线 / 服务器重启 三类异常的**逐步命令与预期现象**见 [VERIFY.md](VERIFY.md)。

## 历史版本

v1（文本协议）→ v2（帧层）→ v3（Reactor 实验）→ v4（可靠性）→ v5（账号/房间/持久化）的
源码与说明存档于 [legacy/](legacy/README.md)；迁移对照见 [docs/migration.md](docs/migration.md)。
