#!/usr/bin/env bash
# tools/coverage.sh —— 单测覆盖率（gcov）：聚焦协议层（frame/minijson/protocol/seq_dedup）
# 与路由层（room_router），目标 ≥ 70%。
#
# 用法：bash tools/coverage.sh          （在仓库根目录执行）
# 产物：build-cov/ 下的 .gcno/.gcda/.gcov；stdout 打印 Markdown 汇总表。
# 等价 CMake 路径：cmake -B build -DCOVERAGE=ON && cmake --build build && ctest --test-dir build
#                  && gcov -o build/CMakeFiles/chat_tests.dir/... （本脚本是无 cmake 的一键版）
set -euo pipefail
cd "$(dirname "$0")/.."

CC=${CXX:-g++}
OUT=build-cov
SQLITE=""
SSL=""
CRYPTO=""
for p in /usr/lib/x86_64-linux-gnu/libsqlite3.so.0 /usr/lib/libsqlite3.so.0 /usr/local/lib/libsqlite3.so.0; do
  [ -e "$p" ] && SQLITE="$p" && break
done
for p in /usr/lib/x86_64-linux-gnu/libssl.so.3 /usr/lib/libssl.so.3; do
  [ -e "$p" ] && SSL="$p" && break
done
for p in /usr/lib/x86_64-linux-gnu/libcrypto.so.3 /usr/lib/libcrypto.so.3; do
  [ -e "$p" ] && CRYPTO="$p" && break
done
[ -n "$SQLITE" ] || { echo "找不到 libsqlite3.so.0"; exit 1; }

rm -rf "$OUT"
mkdir -p "$OUT"

# 与 CMake COVERAGE=ON 等价的插桩编译：--coverage = -fprofile-arcs -ftest-coverage
SRCS="src/log.cpp src/frame.cpp src/protocol.cpp src/seq_dedup.cpp src/room_router.cpp src/database.cpp src/rate_limiter.cpp src/word_filter.cpp src/tls.cpp src/server.cpp"
TESTS="tests/test_main.cpp tests/test_frame.cpp tests/test_minijson.cpp tests/test_protocol.cpp tests/test_room_router.cpp tests/test_seq_dedup.cpp tests/test_rate_limiter.cpp tests/test_word_filter.cpp tests/test_auth.cpp tests/test_database.cpp"
OBJS=""
for f in $SRCS $TESTS; do
  o="$OUT/$(basename "$f" .cpp).o"
  $CC -std=c++11 --coverage -O0 -g -pthread -Iinclude -c "$f" -o "$o"
  OBJS="$OBJS $o"
done
$CC --coverage $OBJS "$SQLITE" "$SSL" "$CRYPTO" -pthread -o "$OUT/chat_tests"

echo "== 运行单测 =="
"$OUT/chat_tests"

echo
echo "== gcov 逐文件行覆盖率 =="
# 在仓库根目录跑 gcov（源路径按编译时的相对路径解析）；-n 只输出统计不写 .gcov 文件。
# 「File '...' / Lines executed:X% of N」成对出现在 gcov 的【stdout】，.gcov 文件里没有。
gcov -n -o "$OUT" "$OUT"/*.gcda > "$OUT/gcov_summary.txt" 2>&1 || true

# 汇总目标模块（协议与路由）行覆盖率（按 basename 精确匹配，避免 test_frame.cpp 撞 frame.cpp）
printf '\n| 模块 | 文件 | 行覆盖率 |\n|---|---|---|\n'
total_hit=0; total_lines=0
for name in frame.cpp protocol.cpp seq_dedup.cpp room_router.cpp minijson.h rate_limiter.cpp word_filter.cpp; do
  summary=$(awk -v want="$name" '
    /^File / {
      path = $0
      sub(/^File .\047/, "", path)
      sub(/\047$/, "", path)
      n = split(path, parts, "/")
      hit = (parts[n] == want)
    }
    hit && /Lines executed:/ { print; exit }
  ' "$OUT/gcov_summary.txt")
  [ -n "$summary" ] || continue
  pct=$(echo "$summary" | sed -E "s/.*Lines executed:([0-9.]+)%.*/\1/")
  n=$(echo "$summary" | sed -E "s/.* of ([0-9]+)/\1/")
  label="协议层"
  [ "$name" = "room_router.cpp" ] && label="路由层"
  [ "$name" = "rate_limiter.cpp" ] && label="风控层"
  [ "$name" = "word_filter.cpp" ] && label="风控层"
  printf '| %s | %s | %s%% (%s 行) |\n' "$label" "$name" "$pct" "$n"
  hit=$(python3 -c "print(int(round($pct * $n / 100)))")
  total_hit=$((total_hit + hit))
  total_lines=$((total_lines + n))
done
pct_all=$(python3 -c "print('%.1f' % (100.0 * $total_hit / $total_lines)) if $total_lines else print('0.0')")
printf '| **合计（协议+路由）** | | **%s%%**（%d/%d 行） |\n' "$pct_all" "$total_hit" "$total_lines"
echo
echo "目标 ≥ 70%：$(python3 -c "print('达标' if $total_lines and 100.0*$total_hit/$total_lines >= 70 else '未达标')")"
