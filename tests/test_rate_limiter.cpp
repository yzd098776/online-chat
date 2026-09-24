// tests/test_rate_limiter.cpp —— 令牌桶限流：突发额度、稳态速率、按 key 隔离、GC
#include "chat/rate_limiter.h"
#include "minitest.h"

MINI_SUITE(rate_limiter)

using chat::KeyedRateLimiter;
using chat::TokenBucket;

// 突发：桶容量内的连续请求全部放行，超出拒绝（时钟注入，无 sleep）
MINI_TEST(burst_capacity) {
    TokenBucket b(2.0, 5.0);  // 2 个/秒，突发 5
    for (int i = 0; i < 5; ++i) MINI_ASSERT(b.try_consume(100.0));
    MINI_ASSERT(!b.try_consume(100.0));  // 第 6 个（同一时刻）拒绝
    MINI_ASSERT(!b.try_consume(100.0));
}

// 稳态：按速率补给（1 秒后 +2 个），不超过桶容量（突发额度不无限囤积）
MINI_TEST(refill_rate) {
    TokenBucket b(2.0, 5.0);
    for (int i = 0; i < 5; ++i) b.try_consume(100.0);
    MINI_ASSERT(!b.try_consume(100.0));      // t=100 桶空
    MINI_ASSERT(b.try_consume(101.0, 2.0));  // 1 秒恰好补 2 个，一次用完
    MINI_ASSERT(!b.try_consume(101.0));
    // 空转 10 秒只补到桶容量 5（不囤到 20）
    MINI_ASSERT(b.tokens(111.0) == 5.0);
    MINI_ASSERT(b.try_consume(111.0, 5.0));
    MINI_ASSERT(!b.try_consume(111.0));
}

// 时间回拨/同刻：不补给（保守侧拒绝，防时钟调整绕过限流）
MINI_TEST(clock_rollback) {
    TokenBucket b(2.0, 2.0);
    MINI_ASSERT(b.try_consume(100.0));
    MINI_ASSERT(b.try_consume(100.0));
    MINI_ASSERT(!b.try_consume(90.0));   // 时钟回拨不补令牌
    MINI_ASSERT(!b.try_consume(100.0));
    MINI_ASSERT(b.try_consume(100.5));   // 正常补给恢复
}

// 按 key 隔离：A 打满不影响 B（每 IP / 每用户独立桶）
MINI_TEST(key_isolation) {
    KeyedRateLimiter r(2.0, 2.0);
    MINI_ASSERT(r.allow("10.0.0.1", 1.0));
    MINI_ASSERT(r.allow("10.0.0.1", 1.0));
    MINI_ASSERT(!r.allow("10.0.0.1", 1.0));  // A 超限
    MINI_ASSERT(r.allow("10.0.0.2", 1.0));   // B 不受影响
    MINI_ASSERT(r.allow("alice", 1.0));
    MINI_ASSERT_EQ(r.size(), 3u);
    MINI_ASSERT(r.hits() >= 3);
    MINI_ASSERT(r.rejects() >= 1);
}

// 稳态速率在 key 维度同样生效
MINI_TEST(keyed_refill) {
    KeyedRateLimiter r(1.0, 1.0);
    MINI_ASSERT(r.allow("u", 1.0));
    MINI_ASSERT(!r.allow("u", 1.2));
    MINI_ASSERT(r.allow("u", 2.0));   // 1 秒后补 1 个
    MINI_ASSERT(!r.allow("u", 2.0));
}

// 键表 GC：空闲键回收，控制内存
MINI_TEST(idle_gc) {
    KeyedRateLimiter r(1000.0, 1000.0, 4, 10.0);  // max_keys=4, idle=10s
    for (int i = 0; i < 4; ++i) {
        char k[16];
        std::snprintf(k, sizeof(k), "ip%d", i);
        MINI_ASSERT(r.allow(k, 1.0));
    }
    MINI_ASSERT_EQ(r.size(), 4u);
    r.allow("ip4", 100.0);  // 触发 GC：旧键空闲 >10s 被回收
    MINI_ASSERT(r.size() < 5u);
}
