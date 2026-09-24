# VERIFY.md — 异常场景验收手册

对应【改造要求】5 的三类异常场景。每个场景给出 **操作（可照抄的命令）→ 预期现象**，分「裸机」与「Docker」两套复现步骤（语义一一对应；Docker 版用 `docker network disconnect / docker kill / docker restart` 注入故障，最贴近真机拔线/强杀/重启）。

文末附：自动化回归、两个加强场景（半开判死、顶号）、以及离线库/去重表的落盘自检。

> **v5 适配对照**（本手册写于 v4 时代，场景语义不变，命令按下表换算；历史源码在 `legacy/`）：
>
> | 手册中的 | v5 等价 |
> |---|---|
> | `g++ ... chat_server_v4.cpp ...` | `cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j` |
> | `./chat_server_v4 8888` | `./build/chat_server_v5 8888` |
> | `python3 chat_client_v3.py --user alice --headless` | `python3 client/chat_client_v4.py --user alice --password <口令> --headless`（先 `/register alice <口令>`） |
> | 同名再 LOGIN → 静默顶号 | **口令**重复登录 → **E1004 明确拒绝**；**Token** 重连 → 顶号 + 明确 SYSTEM 通知（PROTOCOL.md 6.1） |
> | `seen_message` 落盘去重 | (user,seq) 内存去重窗口 + 客户端显示去重（PROTOCOL.md 6.6） |
> | `E2EACK` 删离线行 | `offline_messages.delivered` 标志，推送成功逐条置 1（PROTOCOL.md 6.5） |
> | 落盘自检 `offline_message` 表 | `offline_messages` 表（sqlite3 查 `delivered` 列） |

三个场景共同的语义保证（先读这个，预期现象都由此推导）：

| 编号 | 保证 | 实现要点 |
|---|---|---|
| G1 | 消息不丢 | 私聊**先原子落库**（`offline_message`）**再回 ACK**——`已送达` = 已持久化必达；行只在收到接收端 `E2EACK` 后删除。转发写进半开 socket 的内核缓冲区也「成功」但数据会丢，所以转发成功不算送达 |
| G2 | 不重不漏不乱 | 每条消息 per-user 单调 seq，服务器按 (user,seq) 去重（SQLite `seen_message`，重启不丢）；补发按 (ts,id) 排序；接收端按 (from,seq) 显示去重兜底 |
| G3 | 状态一致 | 心跳判活（PING/PONG + 30s idle 扫描剔除并广播下线）；断线指数退避重连（1s,2s,4s…30s，±20% 抖动）+ 自动重 LOGIN；同名顶号防被自己的僵尸锁门 |

---

## 0. 准备

### 0.1 裸机准备（3 个终端）

```bash
# 终端 S：编译并启动服务器（v5；v4 见文首适配对照）
cd online-chat
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j
./build/chat_server_v5 8888
# 预期：[INFO] 聊天服务器 v5 已启动 ... 监听端口: 8888（扫描线程每 5s 一跳，idle 30s 判死）

# 终端 A：alice 客户端（首次先注册）
python3 client/chat_client_v4.py 127.0.0.1 --port 8888 --user alice --password pw-a --headless
> /register alice pw-a        # 仅首次
# 预期：[连接] ... 已连接（alice）
#       [系统] 认证成功（Token 已保存，重连免密）
#       [在线#-] ...          （/join lobby 后显示房间成员）

# 终端 B：bob 客户端
python3 client/chat_client_v4.py 127.0.0.1 --port 8888 --user bob --password pw-b --headless
> /register bob pw-b          # 仅首次
```

（GUI 版等价：`python3 chat_client_v3.py 127.0.0.1 --port 8888 --user alice`，状态栏/气泡状态与下述预期现象一一对应。）

### 0.2 Docker 准备

```bash
cd online-chat
docker compose build
docker compose up -d server
docker compose logs -f server          # 另开一个终端保持看日志（= 终端 S）
# 预期：[监听] 0.0.0.0:8888 ...

# 终端 A（保持前台看输出；--rm 不要加，场景 1/2 要对容器动手）：
docker compose --profile client run --name chat-alice client \
    --user alice --pending-file /data/chat_pending_alice.json
# 预期：[状态] 已连接 / [系统] 登录成功 / [在线] alice

# 终端 B：
docker compose --profile client run --name chat-bob client \
    --user bob --pending-file /data/chat_pending_bob.json
```

收尾清理（所有场景通用）：`docker rm -f chat-alice chat-bob && docker compose down`

---

## 场景 1：客户端进程被强杀（kill -9 / 任务管理器结束）→ 服务器 30s 内剔除并广播下线

### 1A 裸机

| # | 操作 | 预期现象 |
|---|---|---|
| 1 | 按 0.1 起好 S / A（alice）/ B（bob），互见在线 | 各端 `[在线] alice,bob` |
| 2 | 终端 A 执行 `kill -9 $(pgrep -f 'chat_client_v3.*alice')`（Windows：任务管理器结束 python 进程） | A 立即消失 |
| 3 | 看终端 B 与 S | **B**：`[系统] alice 掉线，已下线`，随后 `[在线] bob`；**S**：`[断开] ... 对端关闭` → `[下线] alice（掉线）` 并广播 |
| 4 | 看时钟（或 S 的日志时间戳） | 从强杀到 B 看到下线通知 **≤30s**；kill -9 时内核代发 FIN，实测 **1~3s**（FIN 快路径；见文末注1） |

### 1B Docker（等价注入：`docker kill`）

| # | 操作 | 预期现象 |
|---|---|---|
| 1 | 按 0.2 起好 chat-server / chat-alice / chat-bob | 各端互见在线 |
| 2 | `docker kill chat-alice`（SIGKILL，等价 kill -9 / 任务管理器结束） | chat-alice 容器退出 |
| 3 | 看 chat-bob 与 `docker logs chat-server` | 与 1A-3 相同：B 收 `[系统] alice 掉线，已下线` + `[在线] bob`；S 打 `[下线] alice（掉线）` |
| 4 | 看时钟 | **≤30s**；实测 **1~3s**（容器退出时内核代发 FIN，走快路径） |

**注1（为什么说「30s 内」）**：kill -9 时进程虽被 SIGKILL，内核仍会替它关闭 socket 并发出 FIN，服务器收线程立即感知 → 秒级剔除。**没有 FIN 的路径**（拔线、对端失联）才走独立扫描线程的 30s idle 判死，见场景 2 与加强项 A。两条路径都在 30s 内完成剔除+广播。

---

## 场景 2：拔网线 / 关 WiFi → 客户端退避重连，恢复后自动上线且收到离线消息

### 2A Docker（推荐——`docker network disconnect` 就是拔网线，且对端无 FIN）

| # | 操作 | 预期现象 |
|---|---|---|
| 1 | 按 0.2 起好 server / alice / bob | 互见在线 |
| 2 | 拔线：`docker network disconnect chatnet chat-alice` | chat-alice 的日志：本地 socket 报错 `[断开] ...` **或** `[心跳] ... 无响应，判连接死亡，强制重连`（视网络环境哪条先到）→ `[状态] 重连第 1 次` → `重连第 2 次/3 次...`（退避 1s,2s,4s… 上限 30s，±20% 抖动）；期间 `[连接失败] server:8888 -> gaierror(...)` 是正常噪音（断网时 Docker DNS 也不可达）。**不删容器** |
| 3 | 终端 B 发私聊（趁 alice 断线）：输入 `@alice 你掉线了吗` 回车 | B 显示自己这条消息的状态：`[状态] seq=... 发送中` → `[送达] seq=... 已送达`。**已送达 = 已持久化必达**（此刻消息已进 `offline_message`，不论 alice 在不在线） |
| 4 | （可选）S 侧落盘自检：`docker exec chat-server sqlite3 /data/chat_server_v4.db 'select to_user,from_user,content from offline_message;'` | 有 `alice|bob|你掉线了吗` **一行**——还没被消费；行只在 alice 端到端确认后才删 |
| 5 | 插回网线：`docker network connect chatnet chat-alice` | chat-alice：某一刻 `[状态] 已连接`（自动重连成功并以**原用户名** alice 重新 LOGIN）→ `[系统] 登录成功` → `[消息] [离线消息] bob → 我: 你掉线了吗`（灰色/斜体样式 = 离线消息；GUI 同） → `[在线] alice,bob` |
| 6 | 重复第 4 步查库 | 那一行**已消失**（alice 收到后回了 E2EACK，服务器删行） |
| 7 | （可选）断线期间 alice 也发：输入 `@bob 断线期的留言` 回车 | 状态 `发送中`（入本地待发队列，`/status` 可见 `state=sending`）；插回网线自动补发后 `[送达] seq=... 已送达`；bob 侧恰好一条 `alice → 我: 断线期的留言`（不重复、不乱序） |

> 时机细节：拔线后若本地 socket 还没报错，客户端要等 `dead-timeout`（默认 30s）心跳判死；断线后的重试可能要等一次退避窗口（最长 30s + 抖动）才试到插回后的接口，`已连接` 最迟约 1 分钟内出现。docker 拔线时服务端通常会在数秒内也收到连接关闭并广播下线——**半开窗口虽短，恰好落在窗口内的消息正是 store-first 要保的**（第 3~6 步即验证）；纯无 FIN 的 30s 判死路径见加强项 A。

### 2B 裸机（三种等价注入，任选其一）

**B-1 真拔网线 / 关 WiFi（最贴近需求原文）**：直接拔网线或关 WiFi，其余步骤与 2A 的 3→7 完全相同（`已送达` / 落库 / 重连后 `[离线消息]`）。关 WiFi 后客户端通常先报 recv 错误（本地接口 down），少数环境走心跳判死，两条路都进重连循环。

**B-2 iptables 丢包（需 root，等价防火墙断网）**：

```bash
# 在 alice 所在机器上（把 8888 换成服务器端口；双向都丢）
sudo iptables -A INPUT  -p tcp --sport 8888 -j DROP
sudo iptables -A OUTPUT -p tcp --dport 8888 -j DROP
# ...执行 2A 的 3~4 步（bob 发消息、查库）...
sudo iptables -D INPUT  -p tcp --sport 8888 -j DROP
sudo iptables -D OUTPUT -p tcp --dport 8888 -j DROP
# ...执行 2A 的 5~7 步...
```

**B-3 SIGSTOP 冻结（无需 root，等价「断网且对端无 FIN」）**：`kill -STOP $(pgrep -f 'chat_client_v3.*alice')` 冻住 alice（PING 停发，socket 不关，服务器只会在 30s idle 后判死）。之后同 2A 的 3~4 步；`kill -CONT ...` 解冻后同 5~7 步。

---

## 场景 3：服务器重启 → 所有客户端重连，在线列表重建正确

### 3A 裸机

| # | 操作 | 预期现象 |
|---|---|---|
| 1 | 按 0.1 起好 S / A / B，互见在线 | `[在线] alice,bob` |
| 2 | 终端 S：`Ctrl-C` 或 kill 掉服务器，立即（3s 内）`./chat_server_v4 8888` 重启（同一 DB 文件） | 服务器重新 `[监听] ...`；**DB 是磁盘文件，重启不丢**（场景 3 的前提） |
| 3 | 看终端 A / B | 各自 `[心跳] ... 无响应，判连接死亡`（或 `[断开]`）→ `重连第 N 次` → **≤ 数十秒内**双双 `[状态] 已连接` + `[系统] 登录成功`（原用户名自动重 LOGIN） |
| 4 | 重启间隙发消息：在 A 输入 `@bob 服务器挂了期间的留言`（**在它还没重连上时发**，状态只见 `发送中`） | A 重连成功后自动补发 → `[送达]`；B 侧恰好一条该消息（带不带 `[离线消息]` 前缀取决于到达路径，**恰好一次**，不会两条） |
| 5 | 在 B 看在线列表 | `[在线] alice,bob`（**重建正确**：每个客户端重 LOGIN 后 USERLIST 全量广播，无幽灵、无缺失） |
| 6 | （可选）查去重表：`sqlite3 chat_server_v4.db 'select user_name,seq from seen_message order by ts desc limit 5;'` | 重启前的 (user,seq) 仍在——补发撞重复帧会被正确判重（只回 ACK 不再投递） |

### 3B Docker（等价注入：`docker restart chat-server`）

| # | 操作 | 预期现象 |
|---|---|---|
| 1 | 按 0.2 起好 chat-server / chat-alice / chat-bob | 互见在线 |
| 2 | `docker restart chat-server` | 容器重启，日志重新 `[监听] 0.0.0.0:8888`；DB 在 `chat-data` 卷，**重启不丢** |
| 3 | 看 chat-alice / chat-bob 终端 | 同 3A-3：判死/断开 → `重连第 N 次` → 自动 LOGIN → `[在线] alice,bob` 重建（实测 15s 内；与退避相位有关，上限约 1 分钟） |
| 4 | 同 3A-4（可在 `docker restart` 返回后立刻在 B/A 断档窗口发送） | 消息恰好一次送达；`已送达` = 已持久化 |
| 5 | `docker exec chat-server sqlite3 /data/chat_server_v4.db 'select user_name,seq from seen_message order by ts desc limit 5;'` | 与 3A-6 相同 |

---

## 加强项 A：无 FIN 的 30s 判死（纯超时剔除路径）

kill -9 有 FIN（秒级剔除）；真正走「30s 超时剔除」的是**进程还活着但什么都不发**的半开态：

| # | 操作 | 预期现象 |
|---|---|---|
| 1 | 起好 S / A / B | 互见在线 |
| 2 | 冻结 alice（不关 socket，等价拔线后对端无 FIN）：`kill -STOP $(pgrep -f 'chat_client_v3.*alice')` | B 侧**暂无**任何变化（服务器眼里 alice 还在） |
| 3 | 等 30~35s（观察 S 日志） | S：`[下线] alice（连接超时）`——独立扫描线程每 5s 一跳，>30s 无活跃剔除并广播；B：`[系统] alice 连接超时，已下线` |
| 4 | `kill -CONT ...` 解冻 | alice 判死重连 + 顶号或正常重登录，恢复在线 |

> 快进版：`./chat_server_v4 8888 --idle 3000 --scan 500` 把 30s/5s 调成 3s/0.5s（自动化用例 T1b 即此参数，语义与默认 30s/5s 一致）。Docker 版把第 2 步换成 `docker network disconnect` + 在 30s 内不要 `connect`，S 日志同样出现连接超时下线。

## 加强项 B：顶号——断线后同名重连不被自己的僵尸锁门

| # | 操作 | 预期现象 |
|---|---|---|
| 1 | 起好 S / A(alice) / B | 在线 |
| 2 | `kill -STOP` 冻住 alice（造半开僵尸占着名字） | — |
| 3 | 再起一个 alice：`python3 chat_client_v3.py 127.0.0.1 --port 8888 --user alice --headless` | 新连接 `[系统] 登录成功`（**不被拒绝**）；S 把旧连接静默顶掉（不播「alice 下线」再「上线」——他在别人眼里没离开过）；B 的 USERLIST 仍是 `alice,bob` |
| 4 | B 发 `@alice hi` | 新 alice 恰好收到一条；旧僵尸收不到也不干扰 |

---

## 自动化回归（8 用例，一键跑完上述语义）

```bash
python3 tools/test_reliability.py
# 期望输出（尾行）：结果: 全部通过
```

| 用例 | 覆盖 |
|---|---|
| T0 | 冒烟：登录/群聊/私聊/ACK→已送达 |
| T1 | 场景 1（kill -9 → FIN 快路径剔除+广播） |
| T1b | 加强项 A（SIGSTOP 半开 → idle 超时剔除） |
| T1c | 加强项 B（顶号） |
| T2 | 场景 2（黑洞拔线：心跳判死、退避重连、断档入队补发不丢不重、**半开窗口内的私聊不丢**、收 `[离线消息]`） |
| T2b | 幂等：同一 (user,seq) 重复帧只投递一次、重复 ACK |
| T2c | 离线消息按 ts 排序补发 + `[离线消息]` 样式 |
| T3 | 场景 3（服务器重启：重连、在线列表重建、断档消息恰好一次） |

容器内跑同一套：`docker run --rm --entrypoint python3 online-chat:latest tools/test_reliability.py`

## 落盘自检（G1/G2 的物证）

```bash
# 离线库（私聊先落库的证据；正常运行时应趋近为空——都已被 E2EACK 删掉）
sqlite3 chat_server_v4.db 'select * from offline_message;'
# 去重表（(user,seq) 幂等的证据；服务器重启后仍在 = 判重不随进程消失）
sqlite3 chat_server_v4.db 'select user_name,seq,ts from seen_message order by ts desc limit 10;'
# Docker 版把库路径换成 /data/chat_server_v4.db（docker exec chat-server sqlite3 ...）
```

## 附录 A：SeqDeduper 竞态的证据链与「进程消失」的正确归类

**本条只主张有留痕的三件证据**（回答「你说竞态导致过死亡，证据呢」）：

| # | 证据 | 留痕形式 |
|---|---|---|
| ① | 段错误本体 | `dmesg`：500+ 连接压测中 `chat_server_v5[...]: segfault at ... ip ... in chat_server_v5`，`addr2line` 落到 `_M_find_before_node`（unordered_map 查找读野指针） |
| ② | 检测链路有效（负对照） | 临时剥掉 `src/seq_dedup.cpp` 的锁 → 同一套 `-fsanitize=thread` 编译/运行**必报** `data race src/seq_dedup.cpp:13 in check_and_add` |
| ③ | 修复有效 | 恢复锁后：TSan 全量单测 + 服务端 200/500 连接各 30s + churn **零告警**；`tests/test_seq_dedup.cpp::concurrent_stress`（8 线程并发插查）100% 通过 |

**其余「进程消失」的归类（不并入①的证据）**：压测期间还观察到几次服务端退出，复核后
**至少 TSan 系列那几次是端口被上一台实例占用、绑定失败即退**（`tsan4` 控制台留档：
`绑定端口 18992 失败`，而上一台实例实际存活 53 分钟；此前把它的 PID 判成「已死」是误读）。
另有更早一次（非 TSan、独占端口）**无内核留痕、无法归因**，不写进证据链——宁可少说，
不给「一次对不上、整段被质疑」留口子。

> 口径红线：文档里凡是提到竞态，一律引用上表① ② ③；不再出现「神秘死亡都是它」这类归因。

## 附录 B：持久性验收 —— kill -9 对拍（ACK = 已落盘）

不变式：**ACK = 已持久化**（服务器先 COMMIT 再回 ACK；group commit 下写手也必须等批次
COMMIT 返回）。场景 2（kill -9）只验证了「连接被正确剔除」，本附录验证**数据不丢**：

```bash
python3 tools/test_durability.py --server-bin build/chat_server_v5
# 期望：洪水 8s（30 连接流水线）中途 SIGKILL 服务端 →
#   ✓ 每个 ACK 都能在库里查到——ACK=已持久化 成立（kill -9 无丢）
```

**关于「库行数 ≥ ACK 条数」的差值（实测 73749 行 vs 73748 条 ACK，差 1）**：
这是**预期结果不是 bug**。差值的来源只有一个方向——**「已 COMMIT 但 ACK 尚未送达」**：
kill 落在「COMMIT 完成 → ACK 写进内核发送缓冲/被客户端读到」的窗口内。它证明了 ACK
**不早于** COMMIT（若顺序反了，差值会出现在另一侧：ACK 数 > 库行数，且那些行永久缺失）。

- 断言方向是单向的：**每个 ACK 过的消息必须在库里**（⊇），不要求相等；
- 若出现「ACK 条数 > 库行数」（有 ACK 查无此行）= 持久性被破坏，**一条即失败**；
- 前提检查（启动日志可验）：`journal_mode=wal` + `synchronous=2(FULL)`——每次 COMMIT
  fsync 落盘。VM 宿主磁盘缓存会接管 fsync（见 README 已知限制 #1 的吞吐口径说明）。

**备份纪律**：`secrets/pepper.key`（或 `--pepper` 指定文件）**必须随 DB 一起进备份集**——
只备 DB 丢了 pepper = 全库盲化永久不可恢复，且症状又是「所有人都口令错误」；安全上
建议二者分开存放/分管（防单点泄露），但**缺一不可**（漏 pepper 的表现与 T16b 的
「pepper 与库不匹配」同源，可据此排查）。
