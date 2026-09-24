// include/chat/rate_limiter.h —— 令牌桶限流（风控）
//
// 两个维度（题面要求）：
//   ① 每 IP 每秒建连数    —— acceptLoop 入口查 KeyedRateLimiter(ip)
//   ② 每用户每秒消息数    —— doMessage 入口查 KeyedRateLimiter(username)
// 超限返回错误码 + 中文文案（E4001/E4002，见 protocol.h）并记 WARN 日志。
//
// 令牌桶（token bucket）语义：
//   - 以恒定速率 rate（个/秒）向桶里加令牌，桶容量 burst（允许的突发上限）；
//   - 每次请求消耗 1 个令牌，桶空则拒绝——瞬时可打满 burst 个，稳态不超过 rate 个/秒；
//   - 与漏桶的区别：漏桶强制匀速出流（削平突发），令牌桶允许攒额度突发（更贴合
//     「建连/发消息」这类自然有突发的流量；面试常考点）。
// 时间复杂度：单次判定 O(1)（摊还）；按 key 隔离时含一次哈希查找 O(1)。
// 时钟从参数注入（now_sec）——单测可精确控制时间，不依赖 sleep。
#ifndef CHAT_RATE_LIMITER_H_
#define CHAT_RATE_LIMITER_H_

#include <cstddef>
#include <map>
#include <mutex>
#include <string>

namespace chat {

class TokenBucket {
public:
    TokenBucket(double rate_per_sec, double burst)
        : rate_(rate_per_sec), burst_(burst), tokens_(burst), last_refill_(0) {}
    // 尝试消耗 n 个令牌；true=放行。now_sec 单调递增即可（测试可传假时钟）
    bool try_consume(double now_sec, double n = 1.0);
    double tokens(double now_sec);  // 当前余量（补给后，观测/测试用）

private:
    void refill(double now_sec);
    double rate_;        // 稳态速率（个/秒）
    double burst_;       // 桶容量 = 突发上限
    double tokens_;
    double last_refill_;
};

class KeyedRateLimiter {
public:
    // max_keys：键表上限（防内存被海量 IP/用户名撑爆）；空闲键按 idle_gc_sec 回收
    KeyedRateLimiter(double rate_per_sec, double burst, size_t max_keys = 10000,
                     double idle_gc_sec = 30.0)
        : rate_(rate_per_sec), burst_(burst), max_keys_(max_keys),
          idle_gc_sec_(idle_gc_sec), hits_(0), rejects_(0) {}
    // true=放行；false=超限（调用方回 E4001/E4002 + 记日志）
    bool allow(const std::string& key, double now_sec);
    size_t size() const;
    long long hits() const { return hits_; }
    long long rejects() const { return rejects_; }

private:
    struct Entry {
        TokenBucket bucket;
        double last_seen;
        Entry(double r, double b, double t) : bucket(r, b), last_seen(t) {}
    };
    void gc_locked(double now_sec);

    double rate_, burst_;
    size_t max_keys_;
    double idle_gc_sec_;
    long long hits_, rejects_;
    mutable std::mutex m_;
    std::map<std::string, Entry> table_;
};

}  // namespace chat

#endif  // CHAT_RATE_LIMITER_H_
