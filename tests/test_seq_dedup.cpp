// tests/test_seq_dedup.cpp —— (user, seq) 幂等去重窗口
#include <atomic>
#include <thread>
#include <vector>

#include "chat/seq_dedup.h"
#include "minitest.h"

MINI_SUITE(seq_dedup)

using chat::SeqDeduper;

// 首次放行、重复拦截（重发/ACK 丢失重传 → 只回 ACK 的依据）
MINI_TEST(dedup_basic) {
    SeqDeduper d;
    MINI_ASSERT(d.check_and_add("alice", 1));   // 首次
    MINI_ASSERT(!d.check_and_add("alice", 1));  // 重复帧
    MINI_ASSERT(!d.check_and_add("alice", 1));  // 持续拦截
    MINI_ASSERT(d.seen("alice", 1));
    MINI_ASSERT_EQ(d.size(), 1u);
}

// (user, seq) 复合键：不同用户各自计数空间互不干扰
MINI_TEST(per_user_namespace) {
    SeqDeduper d;
    MINI_ASSERT(d.check_and_add("alice", 5));
    MINI_ASSERT(d.check_and_add("bob", 5));     // bob 的 seq=5 是另一条消息
    MINI_ASSERT(!d.check_and_add("alice", 5));
    MINI_ASSERT(!d.check_and_add("bob", 5));
    MINI_ASSERT_EQ(d.size(), 2u);
}

// 单调递增正常放行
MINI_TEST(monotonic_seqs) {
    SeqDeduper d;
    for (long long s = 1; s <= 100; ++s) MINI_ASSERT(d.check_and_add("u", s));
    MINI_ASSERT_EQ(d.size(), 100u);
    MINI_ASSERT(!d.check_and_add("u", 50));  // 旧 seq 重发仍拦截
}

// 容量超限整窗淘汰（头注释取舍）：淘汰后旧键可再次放行（残余重复靠客户端显示去重兜底）
MINI_TEST(capacity_reset) {
    SeqDeduper d(4);
    MINI_ASSERT(d.check_and_add("u", 1));
    d.check_and_add("u", 2);
    d.check_and_add("u", 3);
    d.check_and_add("u", 4);
    MINI_ASSERT_EQ(d.size(), 4u);
    MINI_ASSERT(d.check_and_add("u", 5));      // 触发整窗清空后放行
    MINI_ASSERT(d.size() < 5u);
    MINI_ASSERT(d.check_and_add("u", 1));      // 被淘汰的旧键重新可见（文档化的行为）
}

// clear() 显式重置
MINI_TEST(clear) {
    SeqDeduper d;
    d.check_and_add("u", 1);
    d.clear();
    MINI_ASSERT_EQ(d.size(), 0u);
    MINI_ASSERT(!d.seen("u", 1));
    MINI_ASSERT(d.check_and_add("u", 1));
}

// 多线程回归（永久防线）：N 线程并发插查同一批 (user,seq)——
//   ① 行为：每个 (user,seq) 恰好放行一次（check+add 必须原子，重复竞争不得双放行）；
//   ② 存活：无锁的 unordered_map 并发读写会在 rehash×查找交错时 segfault（500+ 连接
//      压测实崩过，dmesg _M_find_before_node 野指针），TSan 下此用例 100% 报出旧 bug。
// 动 seq_dedup 的人必须过这一关——「共享可变状态默认要锁」从经验升级为约束就靠它。
MINI_TEST(concurrent_stress) {
    const int kThreads = 8, kUsers = 4, kSeqs = 5000, kRounds = 3;
    SeqDeduper d((size_t)(kThreads * kUsers * kSeqs * kRounds));  // 关掉整窗淘汰，专测竞争
    std::atomic<int> admitted(0);
    std::vector<std::thread> pool;
    for (int t = 0; t < kThreads; ++t) {
        pool.push_back(std::thread([&d, &admitted, t]() {
            for (int r = 0; r < kRounds; ++r)
                for (int s = 0; s < kSeqs; ++s) {
                    // 多线程高度重叠地插同一批键（round 间还带重复轮次）
                    std::string user = "u" + std::to_string((t + s) % kUsers);
                    if (d.check_and_add(user, s)) ++admitted;
                    (void)d.seen(user, s);  // 并发查（读路径也在竞争面上）
                }
        }));
    }
    for (size_t i = 0; i < pool.size(); ++i) pool[i].join();
    MINI_ASSERT_EQ(admitted.load(), kUsers * kSeqs);  // 每键恰好放行一次，无丢失无双放行
    MINI_ASSERT_EQ(d.size(), (size_t)(kUsers * kSeqs));
}
