# CMakeLists.txt —— 在线聊天系统构建（C++11 / -O2 / Debug+Release / pthread / 可选测试）
#
# 快速构建（详见 README「从零开始的编译命令序列」）：
#   cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
#   cmake --build build -j
#   ctest --test-dir build --output-on-failure
cmake_minimum_required(VERSION 3.13)
project(online_chat VERSION 5.0 LANGUAGES CXX)

# ---------- 语言与构建类型 ----------
set(CMAKE_CXX_STANDARD 11)            # 题面/课程约定 C++11
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)       # 默认 Release
endif()
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0")   # Debug：带符号、关优化（方便 gdb / 覆盖率）
set(CMAKE_CXX_FLAGS_RELEASE "-O2 -DNDEBUG")  # Release：题目要求 -O2（不用 -O3）
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")

# ---------- 选项 ----------
option(BUILD_TESTING "Build unit tests (ctest)" ON)
option(COVERAGE "gcov 覆盖率插桩（与 -O0 配合）" OFF)

# ---------- 依赖：pthread + SQLite3 + OpenSSL(TLS) ----------
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

# OpenSSL（TLS）：优先标准 FindOpenSSL；无 dev 头则直接链运行库 .so.3
# （与 include/chat/openssl_min.h 冻结 ABI 声明配套——同 sqlite3_api.h 手法）
find_package(OpenSSL QUIET)
if(OpenSSL_FOUND)
  set(CHAT_TLS_LIB OpenSSL::SSL OpenSSL::Crypto)
  message(STATUS "OpenSSL: ${OPENSSL_INCLUDE_DIR} ${OPENSSL_LIBRARIES}")
else()
  find_library(SSL_RUNTIME NAMES ssl libssl.so.3
               PATHS /usr/lib/x86_64-linux-gnu /usr/lib /usr/local/lib)
  find_library(CRYPTO_RUNTIME NAMES crypto libcrypto.so.3
               PATHS /usr/lib/x86_64-linux-gnu /usr/lib /usr/local/lib)
  if(NOT SSL_RUNTIME OR NOT CRYPTO_RUNTIME)
    message(FATAL_ERROR "找不到 OpenSSL：请安装 libssl-dev 或 libssl3+libcrypto3 运行库")
  endif()
  set(CHAT_TLS_LIB ${SSL_RUNTIME} ${CRYPTO_RUNTIME})
  message(STATUS "OpenSSL (runtime): ${SSL_RUNTIME}（配套 include/chat/openssl_min.h）")
endif()

# SQLite3：优先标准 FindSQLite3（有 libsqlite3-dev 时）；无 dev 头则直接链运行库 .so.0
# （与 include/chat/sqlite3_api.h 的冻结 ABI 声明配套——本项目裸机验证环境就是这种）
find_package(SQLite3 QUIET)
if(SQLite3_FOUND)
  set(CHAT_SQLITE_LIB SQLite::SQLite3)
  message(STATUS "SQLite3: ${SQLite3_INCLUDE_DIRS} ${SQLite3_LIBRARIES}")
else()
  find_library(SQLITE3_RUNTIME NAMES sqlite3 libsqlite3.so.0
               PATHS /usr/lib/x86_64-linux-gnu /usr/lib /usr/local/lib)
  if(NOT SQLITE3_RUNTIME)
    message(FATAL_ERROR "找不到 SQLite3：请安装 libsqlite3-dev 或 libsqlite3-0")
  endif()
  set(CHAT_SQLITE_LIB ${SQLITE3_RUNTIME})
  message(STATUS "SQLite3 (runtime): ${SQLITE3_RUNTIME}（配套 include/chat/sqlite3_api.h）")
endif()

# ---------- 覆盖率插桩 ----------
if(COVERAGE)
  add_compile_options(--coverage -fprofile-arcs -ftest-coverage -O0 -g)
  add_link_options(--coverage)
  message(STATUS "COVERAGE=ON：gcov 插桩")
endif()

# ---------- 核心静态库（协议层 + 路由层 + 存储层 + 服务器） ----------
add_library(chat_core STATIC
  src/log.cpp
  src/frame.cpp
  src/protocol.cpp
  src/seq_dedup.cpp
  src/room_router.cpp
  src/database.cpp
  src/rate_limiter.cpp
  src/word_filter.cpp
  src/tls.cpp
  src/server.cpp
)
target_include_directories(chat_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(chat_core PUBLIC Threads::Threads ${CHAT_SQLITE_LIB} ${CHAT_TLS_LIB})

# ---------- 服务器可执行 ----------
add_executable(chat_server_v5 src/main.cpp)
target_link_libraries(chat_server_v5 PRIVATE chat_core)

# ---------- C++ 压测客户端（P1：绕开 Python GIL，标定承压上限；tools/bench_client.cpp 头注释） ----------
add_executable(bench_client tools/bench_client.cpp)
target_link_libraries(bench_client PRIVATE chat_core)

# ---------- 单元测试（ctest）----------
if(BUILD_TESTING)
  enable_testing()
  add_subdirectory(tests)
endif()
