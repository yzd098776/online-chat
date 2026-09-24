# 目录迁移说明（单文件 → 工程化布局）

本次改造把 v5 的单文件实现拆进标准 C++ 工程布局。逐步迁移路径（重放一遍即可复现）：

## 1. 目标布局

```
online-chat/
├── CMakeLists.txt          # 构建入口（C++11 / -O2 / Debug+Release / pthread / BUILD_TESTING）
├── include/chat/           # 公共头：协议层/路由层/存储层/服务器声明
├── src/                    # 实现 + main.cpp
├── tests/                  # 单测（minitest.h 极简断言框架 + 各套件）→ ctest
├── tools/                  # 压测/灌数据/冒烟/覆盖率脚本
├── docs/                   # 本文件等
├── client/                 # Python 客户端（GUI + headless）
├── legacy/                 # v1–v4 历史版本存档（含 v5 拆分前单文件版）
├── README.md / PROTOCOL.md / VERIFY.md
└── Dockerfile / docker-compose.yml
```

## 2. 文件迁移对照（旧 → 新）

| 旧（根目录单文件） | 新 | 变化 |
|---|---|---|
| `chat_server_v5.cpp`（整文件） | `legacy/chat_server_v5.cpp` | 原样存档 |
| ├ minijson 段 | `include/chat/minijson.h` | 独立头；`parse/serialize` 加 `inline`（多 TU 可用） |
| ├ 帧层段（FrameReader/make_wire_frame） | `include/chat/frame.h` + `src/frame.cpp` | 协议层独立，可单测 |
| ├ socket 薄封装段 | `include/chat/net.h` | 可移植层独立 |
| ├ 错误码/校验/游标 | `include/chat/protocol.h` + `src/protocol.cpp` | 协议语义独立，可单测 |
| ├ Database 类 | `include/chat/database.h` + `src/database.cpp` | 存储层独立 |
| ├ TokenBook 类 | `include/chat/token_book.h` | 头文件库 |
| ├ 房间表/成员集（原 RoomRuntime+g_m_） | `include/chat/room_router.h` + `src/room_router.cpp` | **路由层独立**（Q2 的核心数据结构），可单测 |
| ├ (user,seq) 去重（原 recent_seen_） | `include/chat/seq_dedup.h` + `src/seq_dedup.cpp` | 幂等窗口独立，可单测 |
| ├ ChatServer 类 | `include/chat/server.h` + `src/server.cpp` | 服务器装配层 |
| ├ main | `src/main.cpp` | 参数解析 + `chatlog::init` 日志初始化 |
| ├ std::cout 日志 | `include/chat/log.h` + `src/log.cpp` | **分级日志**（DEBUG/INFO/WARN/ERROR + 时间戳/线程 id/文件行号 + 按天切分） |
| `pwd_hash.h` | `include/chat/pwd_hash.h` | 仅移动 |
| `sqlite3_api.h` | `include/chat/sqlite3_api.h` | 仅移动 |
| `chat_client_v4.py` | `client/chat_client_v4.py` | 仅移动（帧层被 tests/tools 复用） |
| `tools/bench.py`（v4 旧物） | `tools/bench.py`（新写） | 并发/速率可调 + CSV/Markdown + /proc 采样 |
| v1–v4 源码 | `legacy/` | 见 `legacy/README.md` |

## 3. 头文件引用规则

- 工程内统一 `#include "chat/xxx.h"`，由 CMake 的 `target_include_directories(chat_core PUBLIC include)` 解析；
- `legacy/` 旧源码保持原样（`#include "pwd_hash.h"` 裸名），编译时补 `-I../include/chat`（见 legacy/README.md）。

## 4. 构建产物

- `build/chat_server_v5` —— 服务器可执行（`src/main.cpp`）；
- `build/tests/chat_tests` —— 单测二进制（`ctest` 逐套件调用）；
- 历史产物 `chat_server_v2/v3/v4` 等根目录二进制已清理，可按 legacy/README.md 重编。

## 5. 行为对照

拆分是**纯结构重构**：协议、SQL、错误码、帧格式与拆分前 v5 单文件版逐字节兼容；
`tools/test_v5_smoke.py`（41 项断言）在拆分前后同一套全绿，作为行为锁。

## 库格式 v1 → v2（pepper 盲化，Q7②）

`users.pwd_hash` 的语义在 v2 变为**盲化值** `stored = K ⊕ HMAC-SHA256(pepper, user‖salt)`
（K = PBKDF2 派生密钥；反 pass-the-hash）。`meta['schema_ver']` 标记格式版本：

- **v2 起步的库**：空库首次打开自动写入 `schema_ver=2`。
- **v1 旧库（users 有数据、无 meta 标记）**：启动**明确拒绝**并打印「检测到 v1 格式库…请重建
  或迁移」——盲化不可逆推（没有当时的 pepper 也无从迁移），教学阶段直接重建用户库即可；
  硬要迁移：取 v1 库里的裸 K 与 salt，按上式用新 pepper 算 stored 回填（K 在手才可迁）。
- **pepper 文件**（`secrets/pepper.key`，`--pepper` 可改）丢了 = 全库盲化解不开，与丢 v2
  库等价。**备份集必须同时包含 DB 与 pepper.key**（缺一即全库永久不可恢复，症状是「所有
  人都口令错误」）；安全上建议二者分开存放/分管，但缺一不可（见 README 已知限制 #2、
  VERIFY.md 附录 B 的排查指引；启动期有 `meta['pepper_id']` 一致性闸门可判「是不是 pepper 错了」）。
