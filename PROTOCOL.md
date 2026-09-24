# 通信协议规格 v2 + 可靠性扩展（长度前缀 + JSON）

本文件是 `chat_server_v2.cpp` / `chat_client_v2.py` 的协议规格；v1（`legacy/chat_server_fixed.cpp` / `legacy/chat_client_fixed.py`）保留原格式不动，用于对照。编译宏 `CHAT_PROTO_V1`（阶段4 实现）可让服务器走回 v1 格式。

**v3 可靠性扩展**（`legacy/chat_server_v4.cpp` / `legacy/chat_client_v3.py`）在 v2 帧格式上追加 ACK / NACK / E2EACK 帧、离线消息标记与幂等 seq 语义，见文末「第 5 节」。帧层（长度前缀、MAX_FRAME、半包/粘包策略）不变。

**v5 账号/房间/历史扩展**（`src/`（工程化服务器）/ `client/chat_client_v4.py`）在 v3 帧格式上追加认证（挑战-应答：REGISTER_HELLO/LOGIN_HELLO/CHALLENGE/REGISTER/LOGIN/AUTH，**口令不进帧**，见 6.1）、房间（JOIN/LEAVE/ROOMS/CREATE）与历史分页（HIST/INBOX）帧，见文末「第 6 节」。注意 v5 的离线消息模型与 v3 不同（`delivered` 标志替代 E2EACK 删行，见 6.5）。

## 1. 帧格式

```
偏移 0        4                        4+len
   +----------+------------------------+
   | len      | body（len 字节）        |
   | >u32 BE  | UTF-8 编码的 JSON 对象  |
   +----------+------------------------+
```

| 项 | 约定 |
|---|---|
| `len` | body **UTF-8 编码后的字节数**（非字符数；emoji 占 4 字节） |
| 字节序 | 大端（网络序），Python 侧 `struct.unpack(">I")` |
| body | 单个 UTF-8 JSON 对象，见第 2 节 |
| `MAX_FRAME` | 1 MiB。`len > MAX_FRAME` 即判协议错误，服务器断开该连接 |
| 心跳 | `PING` → 服务器回 `PONG`（content 原样回显） |

### 为什么用长度前缀，而不是 `\n` / `|` 分隔符

1. **「JSON 里有 `\n` 怎么办」**：定界只依赖头部长度数字，与内容字节无关。body 里出现 `\n`、`|`、`0x00` 都不影响分帧。v1 按 `\n` 分帧、按 `|` 分段，内容含这两种字符即坏帧。
2. **「半包怎么拼 / 粘包怎么拆」**：`recv` 的切片边界与消息边界无关，统一策略为**累积缓冲 + 循环取帧**：
   - 新数据追加进本连接缓冲区；
   - 缓冲区 ≥ 4 字节 → 读出 `len`；不足 `4+len` 字节 → 等下次（**半包**）；
   - 足够 → 取出 body 交上层，擦掉 `4+len`，继续循环（**粘包**：一次 `recv` 来 N 帧就取 N 次）。
   - 服务器：`FrameReader`（缓冲累积式）；客户端：`recv_exact(n)` 逐帧收满（等价实现）。
3. **短写**：发送侧先组好 `长度头+body` 整块，`send` 循环直到写完（处理短写 / `EAGAIN` / `EINTR`）。服务器 `FrameWriter::write_frame`，客户端 `socket.sendall`。

## 2. body 字段

| 字段 | 类型 | 说明 |
|---|---|---|
| `ver` | int | 协议版本，当前恒 `1` |
| `type` | string | `LOGIN` / `MESSAGE` / `LOGOUT` / `SYSTEM` / `USERLIST` / `PING` / `PONG` / `ACK` / `NACK` / `E2EACK`（后 3 者见第 5 节） |
| `from` | string | 发送方用户名；服务器发出时为 `"SERVER"` |
| `to` | string | `ALL`（广播）/ 具体用户名（私聊）/ `SERVER` |
| `room` | string | 预留（房间），当前恒 `""` |
| `content` | string 或 string[] | 消息正文；**`USERLIST` 时为在线用户名数组**（拍板项 2） |
| `ts` | int | Unix 秒 |
| `seq` | int | **发送方**从 1 递增的帧序号，用于丢帧/乱序检测 |

### 各 type 语义

| type | 方向 | 语义 |
|---|---|---|
| `LOGIN` | C→S | 登录，`from` = 用户名 |
| `MESSAGE` | C→S / S→C | 聊天消息；`to=ALL` 广播，`to=用户名` 私聊 |
| `LOGOUT` | C→S | 主动退出 |
| `SYSTEM` | S→C | 系统通知（上下线、私聊对象不在线等） |
| `USERLIST` | S→C | `content` = 当前全部在线用户名数组；**任何用户上/下线后向全员广播一次** |
| `PING` | C→S | 心跳/链路探测 |
| `PONG` | S→C | 对 PING 的应答，`content` 原样回显 |

## 3. 新旧协议对照（v1 → v2）

| 方面 | v1（`\|` + `\n` 文本行） | v2（长度前缀 + JSON） |
|---|---|---|
| 分帧 | 按 `\n` 切分 | 4 字节大端长度头 |
| 字段边界 | 按 `\|` 切 5 段 | JSON 命名字段 |
| 内容含 `\n` | 坏帧 | 安全 |
| 内容含 `\|` | 坏帧 | 安全 |
| 单条上限 | 1023 字节（超长截断） | 1 MiB，按帧完整送达 |
| USERLIST | 4 段文本 + 逗号名单（客户端要求 5 段 → 刷新失败） | `content` = JSON 字符串数组 |
| 私聊下发 | 裸 `send` 无 `\n`（延迟显示） | 与广播同走 `FrameWriter` / `sendall` |
| 列表刷新 | 仅登录时发给本人 | 上/下线后全量广播 |
| 丢帧检测 | 无 | `seq` 递增 |
| 心跳 | 无 | `PING` / `PONG` |
| 兼容开关 | — | 服务器 `-DCHAT_PROTO_V1` / 客户端 `--legacy`（阶段4 实现） |

### 报文示例

v1 线路字节（内容含换行即断成两条坏帧）：

```
MESSAGE|张三|李四|第一行
第二行|1699999999\n
```

v2 body（`content` 内换行转义为 `\n` 两个字符；帧长按 UTF-8 字节计）：

```json
{"ver":1,"type":"MESSAGE","from":"张三","to":"李四","room":"","content":"第一行\n第二行 😀","ts":1699999999,"seq":7}
```

## 4. 兼容对照（阶段4 实现）

| 端 | 新协议（默认） | 旧协议对照 |
|---|---|---|
| 服务器 | `./chat_server_v2` | `./chat_server_v2 -DCHAT_PROTO_V1`（编译期宏） |
| 客户端 | `python3 chat_client_v2.py` | `python3 chat_client_v2.py --legacy` |

**禁止混连**：v2 服务器 ↔ v2 客户端；v1 模式两端都要开 v1。

## 5. 可靠性扩展（v3：心跳 / 重连补发 / 离线消息 / 幂等）

实现：`legacy/chat_server_v4.cpp` + `legacy/chat_client_v3.py`。帧层完全沿用第 1 节；本节只定义新增语义。

### 5.1 帧类型追加

| type | 方向 | 语义 |
|---|---|---|
| `ACK` | S→C | 投递确认；`content` = **被确认的客户端 seq**（十进制字符串） |
| `NACK` | S→C | 拒绝/失败；`content` = 被拒绝的 seq，附加字段 `reason` = 原因文本，`code` = 错误码（限流 4002/敏感词 4003 等，见 6.7） |
| `E2EACK` | C→S | 端到端确认：接收端应用已收到并展示某条 MESSAGE；`to` = 原消息 `from`，`content` = **原消息 seq**（十进制字符串），本帧自身 `seq` 仍取发送方自己的计数器 |

### 5.2 MESSAGE 的 seq 语义（幂等键，需求 6）

- 客户端每条 MESSAGE 带 **per-user 单调递增** `seq`（消息/PING/LOGIN 共用一个持久化计数器；分配取「本地计数 vs 当前毫秒」的较大前沿，防本地文件丢失后归零撞历史去重键）。
- **重发必须复用原 seq**——seq 就是幂等键；换新 seq 重发等于新消息，必产生重复。
- 服务器下行转发 MESSAGE（即时投递与离线补发）一律 **沿用发送方原始 seq**：seq 是端到端消息 id，接收端据此做 (from, seq) 显示去重。
- 服务器按 `(user_name, seq)` 去重（`seen_message` 表，SQLite 落盘）：首次见到才路由；重复帧**只回 ACK、零副作用**（幂等应答——「无效果但有应答」，客户端据此收敛状态）。表在磁盘上，服务器重启后去重不丢。

为什么重发必然带来重复、为什么必须去重：见 `legacy/chat_server_v4.cpp` 头部注释 Q2（发送后 ACK 未到的三种结局不可区分，客户端只能 at-least-once 重发）。

### 5.3 私聊 store-and-forward 与 E2EACK（需求 3/4）

1. 私聊 MESSAGE 通过校验后**先原子落库**（`seen_message` + `offline_message` 同一事务），**再回 ACK**——`已送达` = **已持久化必达**；随后尽力向在线目标提前投递一份。
2. `offline_message` 行键：`UNIQUE(from_user, seq)`（幂等入库），按 `(to_user, ts, id)` 升序补发（原始 `ts` 保留，客户端按此排序/展示）。
3. **行只在收到接收端 `E2EACK` 后删除**。原因：`sendRelay` 写成功只证明字节进了对端内核缓冲区，半开（zombie）连接照样收下写入然后丢弃——**转发成功不是送达证据**，只有接收端应用回 E2EACK 才是。
4. 接收端收**每条私聊** MESSAGE 即回 E2EACK（含被显示层去重的重复帧——行不删掉，下次登录还会再推）；群聊不落离线行，不发 E2EACK。
5. 补发/提前投递至少一次：E2EACK 丢失则行留待下次登录重推，「提前投递 + 补发」的边界重复由接收端 (from, seq) 显示去重兜底。

下行 MESSAGE 追加字段：

| 字段 | 类型 | 说明 |
|---|---|---|
| `offline` | int | `1` = 离线补发（客户端以「离线消息」样式展示）；在线提前投递/群聊无此字段 |
| `ts` | int | 补发时为**原始发送时间**（不是补发时间） |

### 5.4 心跳与判活（需求 1）

| 侧 | 行为 |
|---|---|
| 客户端 | 每 `ping-interval`（默认 **10s**）发 `PING`（content=`hb`）；`dead-timeout`（默认 30s）内无任何下行则判连接死亡，强制断开走重连 |
| 服务器 | 收到任何帧刷新 `last_active`；对 `PING` 回 `PONG`（content 原样回显） |
| 扫描线程 | 每 `scan`（默认 **5s**）扫一遍，`idle`（默认 **30s**）无活跃的连接判死：close 并广播该用户下线 |
| 传输层兜底 | 同时开 `SO_KEEPALIVE` 并调紧（`TCP_KEEPIDLE=15/KEEPINTVL=5/KEEPCNT=3`），只负责更快回收半开 socket 的内核资源；**判活/剔除/广播下线的唯一依据是应用层 PING** |

为什么不用/不单靠 TCP Keepalive：见 `legacy/chat_server_v4.cpp` 头部注释 Q1。

### 5.5 顶号（同名重连，Q4）

新 `LOGIN` 与在线表同名时**顶掉旧连接**（shutdown 唤醒其收线程，不广播下线——用户在别人眼里从没离开过），刷新 USERLIST。弱网重连时旧连接往往还是未超时的半开僵尸，拒绝重名登录会把用户锁在自己的尸体外。

### 5.6 客户端待发队列与状态机（需求 2/3）

- 发送时连接不可用或 ACK 超时：MESSAGE 保留在本地 `PendingStore`（JSON 文件，`os.replace` 原子落盘），带原始 seq、原始 ts。
- 状态机：`发送中`（已入队）→ `已送达`（收到 ACK）/ `失败可重试`（收到 NACK，或补发超过 `MAX_ATTEMPTS=8`）。
- 重连成功并自动 `LOGIN` 后按 **seq 序**补发（复用原 seq，进入服务器去重窗口）。
- 重连退避：`min(30, 2^(attempt-1))` 秒 × ±20% 抖动；UI/状态栏输出 `连接中 / 已连接 / 重连第 N 次`。
- 重传：已发出超 `ack-timeout`（默认 15s）未确认的帧走重传（同一 seq）；从未发出的帧只等重连补发。

## 6. 账号 / 房间 / 历史分页扩展（v5）

实现：`src/`（`server.cpp`/`database.cpp` 等）+ `client/chat_client_v4.py`。帧层完全沿用第 1 节；`ACK`/`NACK`/心跳/待发队列沿用第 5 节。本节定义新增语义。**除 `MESSAGE` 的 `seq` 为幂等键外，控制帧的 `seq` 仅是发送计数器。**

### 6.1 认证（挑战-应答：REGISTER_HELLO / LOGIN_HELLO / CHALLENGE / REGISTER / LOGIN / AUTH）

**口令与派生密钥不进帧**（Q7：凭据安全，与 TLS 的通道安全正交）。注册/登录都是三步：

```
C → S  REGISTER_HELLO{from} 或 LOGIN_HELLO{from}
S → C  CHALLENGE{content=nonce(32B 随机, hex), salt(16B 随机, hex)}     ← nonce 一次性 + 60s 过期
C → S  REGISTER{from, content=K, hmac=proof}   或   LOGIN{from, content=proof}
        其中 K    = PBKDF2-HMAC-SHA256(pwd, salt, 100000)（客户端侧派生）
              proof = HMAC-SHA256(K, op ‖ user ‖ nonce)   ← op = "REGISTER"/"LOGIN"，
                    绑定协议语境+身份（纵深防御；user 另有 takeChallenge 校验双保险）
```

| type | 方向 | 语义 |
|---|---|---|
| `REGISTER_HELLO` | C→S | 注册第一步：`from` = 用户名。用户已存在 → **E1001**（HELLO 阶段即拒，不发挑战）；否则发 `CHALLENGE`（`salt` = 现发的 16B 随机盐） |
| `LOGIN_HELLO` | C→S | 登录第一步：用户不存在 → **E1002**；否则发 `CHALLENGE`（`salt` = 库内存盐） |
| `CHALLENGE` | S→C | `content` = **nonce**（32B CSPRNG 的 64 hex，一次性 + 60s 过期），`salt` = hex 盐（salt 不是秘密，公开防彩虹表） |
| `REGISTER` | C→S | 播种：`content` = **K**（PBKDF2 派生值，64 hex——入库值一次性送达，入库前 pepper 盲化），`hmac` = **proof**（绑定挑战+语境+身份，注册帧不可重放）。成功 → `REGISTER_OK` |
| `LOGIN` | C→S | 证明：`content` = **proof**（64 hex）。服务端出库解盲还原 K 后重算 proof 恒定时间比较。成功 → `AUTH_OK` |
| `AUTH` | C→S | Token 免密恢复：`content` = Token。成功 → `AUTH_OK`（并换发新 Token） |
| `AUTH_OK` | S→C | 认证成功：`content` = **Token**（32 字节 CSPRNG 的 64 hex），`exp` = 过期时间（Unix 秒），`room` = 恢复的房间（顶号续传时非空） |
| `REGISTER_OK` | S→C | 注册成功（`content` = 中文文案） |

- **口令存储**：`salt`（16 字节随机，服务端 CSPRNG 现发、随 `CHALLENGE` 下发——客户端无法指定，天然防重复盐构造）+ 派生密钥 `K = PBKDF2-HMAC-SHA256(pwd, salt, 100000)`。**入库不落 K，落盲化值** `stored = K ⊕ HMAC-SHA256(pepper, user‖salt)`（Q7② 反 pass-the-hash：K 就是登录凭据，库里存 K = 拖库即可登录）；pepper 32B 在 `secrets/pepper.key`（`--pepper` 可改），首次启动自举、0600、不进 DB/日志/git；库内 `meta['pepper_id']` 记指纹，**换错 pepper 启动即明确拒绝**（防「全员密码错误」式静默故障）。边界诚实：pepper 与 DB 同机 = 防拖库、不防拖整机，真解是 PAKE/HSM（README 已知限制）。实现见 `pwd_hash.h`/`pepper.h`；单测 `tests/test_auth.cpp` 对 RFC 公开向量，冒烟 T16 端到端解盲对拍。
- **抗重放**：nonce 一次性（`takeChallenge` 用后即清——错 proof 同样消费）+ 60s 过期——抓包得到的 `LOGIN`/`REGISTER` 帧换个场合重放必然失败（无挑战 → **E1009**；拿旧 nonce 的 proof 应答新挑战 → HMAC 不匹配 → **E1003**）。未消费前重复 `*_HELLO` 复用同一挑战（单槽，防 CSPRNG/查库放大）。
- **无认证面限流**：`REGISTER_HELLO`/`LOGIN_HELLO` 挂每 IP 令牌桶（与建连共用，超限 **E4001** + WARN）；挑战状态每连接单槽（内存有界 = 连接数），生成速率 = 消费/过期速率。
- **为什么口令不进帧**：TLS 是【通道安全】（防整条链路被窃听/篡改），挑战-应答是【凭据安全】（口令/K 不上网、证明不可重放），两者正交——明文模式下抓包也拿不到口令；`--tls` 按需再开。注册的理论边界：无 PKI/PAKE 时服务器必须拿到「能验证口令的东西」，故 K 一次性入帧（等价入库值，建议注册走 TLS）；登录帧里只有 nonce 绑定的 HMAC，无长期凭据。
- **Token**：登录成功后下发；后续连接 `AUTH` 免密恢复（重连免密）。服务器内存表 **只存 `SHA256(token)`** → (user, exp)（表泄不等于凭证泄），重启即失效（客户端回落口令）；`--token-ttl` 默认 7 天。
- **会话信任边界**：认证后 `from` 字段不再可信，服务器一律取会话身份（防伪造他人）。
- **重复登录（两路径区分，错误码 + 中文文案，绝不静默）**：
  - 口令 `LOGIN` 时账号已在线 → **E1004「该账号已在线，不允许重复登录」**（人在另一台机器前操作）；
  - `AUTH`（Token，同一会话续传）→ **顶号**：给旧连接先发 `SYSTEM` 明确说明「已在其他连接恢复会话」再踢（防被自己的半开僵尸锁门）；用户在别人眼里没离开过（房间成员集按用户名）；
  - 同一连接重复 `LOGIN_HELLO`/`REGISTER_HELLO`/`AUTH` → **E1005**。

### 6.2 房间（JOIN / LEAVE / ROOMS / CREATE）

| type | 方向 | 语义 |
|---|---|---|
| `JOIN` | C→S | `content` = 房间名。成功 → `JOIN_OK{room, content=[成员名...]}`，随后客户端自动拉 `HIST` |
| `LEAVE` | C→S | 离开当前房间 → `LEAVE_OK{room}`；未加入时 **E2003** |
| `ROOMS` | C→S | → `ROOMS_LIST{content=[{name, owner, members, created_at}...]}`（含无人房间） |
| `CREATE` | C→S | `content` = 房间名 → `CREATE_OK{room}`；重名 **E2001**（`rooms.name UNIQUE`） |
| `JOIN_OK` | S→C | `room` = 房间名，`content` = 成员名数组 |
| `LEAVE_OK` / `CREATE_OK` | S→C | `room` = 相关房间名 |
| `ROOMS_LIST` | S→C | `content` = 房间对象数组（嵌套 JSON） |

- 默认房间 `lobby` 服务器自建；`/leave` 后处于「无房间」态，`to=ALL` 的 MESSAGE 会被 **NACK**（`reason`=尚未加入房间）。
- **`USERLIST` / `SYSTEM` 改为房间内广播**（`room` 字段非空）：成员增减、进出房通知只发本房间成员。
- **广播路由**：`MESSAGE(to=ALL)` 落 `messages`（`room_id=当前房间`，`receiver='ALL'`，`type='room'`）后**只广播给本房间成员**——从 v4 的 O(全在线) 全量广播变为 O(房间成员) 按房间路由；私聊 `MESSAGE(to=用户名)` 落 `type='private'`（`room_id=0`），在线直投、离线进 `offline_messages`。

### 6.3 历史分页（HIST / INBOX，(ts,id) 游标）

| type | 方向 | 语义 |
|---|---|---|
| `HIST` | C→S | 拉当前房间历史一页；`content` = 游标（`""` = 最近 50 条）→ `HISTORY` |
| `INBOX` | C→S | 拉私信收件箱一页；`content` = 游标 → `INBOX` |
| `HISTORY` | S→C | `room` = 房间名；`content` = 消息对象数组（**旧→新**）；`cursor` = 下一页游标（本页最老行）；`more` = 1/0 |
| `INBOX` | S→C | 同上（`room` 为空，条目是发给我的私聊） |

- 消息对象：`{id, from, to, content, ts, type}`（`type` = `room`/`private`）。
- **游标格式 `ts:id`**（本页最老一行的时间戳与主键）。下一页查询：
  `WHERE room_id=? AND (ts,id) < (?,?) ORDER BY ts DESC, id DESC LIMIT 50`（收件箱换 `receiver=?`）。
  `ts` 秒级有碰撞，`id` 做 tie-break —— `(ts,id)` 唯一严格全序，翻页不重不漏。
- **为什么不用 LIMIT/OFFSET**：OFFSET 深分页要扫过前 N 行（实测第 1781 页 ≈ 3.7ms，游标深页 ≈ 0.045ms），且插入新消息会使 OFFSET 窗口漂移（跳行/重行）；游标深度无关 O(log n + 50)。实测与 EXPLAIN QUERY PLAN 见 `tools/seed_and_bench.py`（`more` 由服务器多取第 51 行判断）。
- 游标非法 → **E3001**。

### 6.4 错误帧 `ERR` 与错误码表

`ERR{code, content}`：`code` = 整型错误码，`content` = 中文文案。**所有失败路径必须回 ERR 或 NACK，禁止静默失败。**

| code | 文案（示例） | 触发 |
|---|---|---|
| 1001 | 注册失败：用户名已存在 | `users.username` UNIQUE（`REGISTER_HELLO`/`REGISTER`） |
| 1002 | 登录失败：用户不存在 | `LOGIN_HELLO`/私聊目标 |
| 1003 | 登录失败：密码错误 | HMAC 证明校验失败（K 派生自口令，proof 错 = 口令错） |
| 1004 | 重复登录：该账号已在线 | 口令 `LOGIN` 撞在线 |
| 1005 | 本连接已登录，请勿重复登录 | 同连接重复 `*_HELLO`/`AUTH` |
| 1006 | Token 无效或已过期 | `AUTH` 失败（客户端回落口令登录） |
| 1007 | 尚未登录 | 未认证发业务帧 |
| 1008 | 用户名/密码格式非法 | 校验失败（含 proof/K 非 64 hex） |
| 1009 | 登录挑战缺失/已过期/不匹配 | 裸 `LOGIN`/`REGISTER`（未先 HELLO）、挑战过期/被换代、注册 HMAC 与挑战不匹配（Q7 抗重放） |
| 2001 | 创建失败：房间已存在 | `rooms.name` UNIQUE |
| 2002 | 加入失败：房间不存在 | `JOIN` 目标不存在 |
| 2003 | 尚未加入房间 | `LEAVE`/`HIST`/群聊前提 |
| 2004 | 房间名非法 | 校验失败 |
| 3001 | 历史游标非法 | 游标解析失败 |
| 4001 | 连接过于频繁：每 IP 每秒最多 N 个新连接 | 建连令牌桶超限（ERR，随后断开）；`*_HELLO` 亦计入该桶 |
| 4002 | 发送过于频繁：每用户每秒最多 N 条消息 | 消息令牌桶超限（**NACK.code**，消息未受理、seq 不消耗可重试） |
| 4003 | 消息包含敏感词「x」，已拒绝发送 | 敏感词 reject 模式（**NACK.code**）；replace 模式则脱敏放行 |
| 4004 | 发送积压超限（读取过慢）：连接将被关闭 | 背压踢除（ERR 后断开；客户端识别后**慢速重连**，防被踢→快重连→再被踢的风暴） |
| 9001 | 数据库错误 | SQLite 失败 |

### 6.7 限流与敏感词（风控扩展）

- **令牌桶**两维度：每 IP 建连（`--conn-rate/--conn-burst`）、每用户消息（`--msg-rate/--msg-burst`），
  超限回上表错误码并记 WARN 日志。消息限流发生在 `(user,seq)` 去重**之前**——被拒消息
  不消耗幂等槽位，客户端稍后重试同 seq 可成功。
- **敏感词**：Trie 前缀树（`--words 词表文件 --filter-mode replace|reject`）；复杂度：
  建树 O(Σ|词|)，扫描 O(n×L) 最坏（n=文本长，L=最长词长）；词表上万可换 AC 自动机 O(n)。
  NACK 增加可选 `code` 字段（4002/4003 时出现）——客户端可按错误码分支处理。

### 6.5 离线消息模型（与 v5 的 4 表 schema 配套）

- 私聊落库即回 `ACK`（`已送达` = 已持久化必达）；不在线写 `offline_messages`（**`delivered` 0/1 标志**）。
- 上线补发：按 `(ts, id)` 升序推 `MESSAGE`（`offline=1`，`ts`=原始发送时间），**推送成功一条置一条 `delivered=1`**，中途断线余下不丢、下次续推。
- v5 **不再使用 v3 的 `E2EACK` 删行模型**（`offline_messages` 无 seq 列）；补发行天然唯一（来自 DB），客户端对 `offline=1` 帧不做 (from,seq) 显示去重。

### 6.6 幂等窗口（与 v3 的差异）

- `MESSAGE` 的 `(user, seq)` 去重改为**服务器内存窗口**（v3 是 `seen_message` 落盘）：重发/ACK 丢失重传幂等（重复帧只回 ACK）；服务器重启后窗口清零的残余重复由客户端 (from,seq) 显示去重兜底。取舍原因：题面 4 表 schema 未含 seq 列。


## 7. TLS 传输（可选，与帧层正交）

实现：`include/chat/tls.h` + `src/tls.cpp`。**帧格式完全不变**——TLS
是 TCP 与帧层之间的记录层：`4B 长度 + JSON body` 先作为字节流交给 TLS 加密，对端解密后
再走原分帧路径。明文/TLS 由服务器 `--tls --cert --key` 与客户端 `--tls` 同开决定，混连会
在握手即失败（TLS 握手字节与合法帧不兼容）。

| 项 | 约定 |
|---|---|
| 证书 | 自签（`tools/gen_cert.sh` 产出 server.crt/server.key，SAN=localhost/127.0.0.1） |
| 客户端校验开关 | 默认 `--tls` 不验（自签教学，防窃听不防中间人）；`--tls-strict --ca server.crt` 开启验链 |
| 协议版本 | OpenSSL 3.x 默认 TLS1.3（1-RTT 握手；无 0-RTT） |
| 错误行为 | 握手失败服务端记 WARN 并断开；明文客户端连 TLS 端口（反之亦然）立即暴露 |

握手流程 / 为什么在 TCP 之上 / 性能开销实测对照：见 README「TLS」一节。
