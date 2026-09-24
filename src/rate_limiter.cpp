// src/rate_limiter.cpp —— 令牌桶实现
#include "chat/rate_limiter.h"

namespace chat {

void TokenBucket::refill(double now_sec) {
    if (now_sec <= last_refill_) return;  // 时钟回拨/同刻：不补给（保守侧拒绝）
    double add = (now_sec - last_refill_) * rate_;
    tokens_ += add;
    if (tokens_ > burst_) tokens_ = burst_;
    last_refill_ = now_sec;
}

bool TokenBucket::try_consume(double now_sec, double n) {
    refill(now_sec);
    if (tokens_ + 1e-9 < n) return false;
    tokens_ -= n;
    return true;
}

double TokenBucket::tokens(double now_sec) {
    refill(now_sec);
    return tokens_;
}

bool KeyedRateLimiter::allow(const std::string& key, double now_sec) {
    std::lock_guard<std::mutex> lk(m_);
    std::map<std::string, Entry>::iterator it = table_.find(key);
    if (it == table_.end()) {
        if (table_.size() >= max_keys_) gc_locked(now_sec);
        table_.insert(std::make_pair(key, Entry(rate_, burst_, now_sec)));
        it = table_.find(key);
    }
    it->second.last_seen = now_sec;
    bool ok = it->second.bucket.try_consume(now_sec);
    if (ok) ++hits_;
    else ++rejects_;
    return ok;
}

size_t KeyedRateLimiter::size() const {
    std::lock_guard<std::mutex> lk(m_);
    return table_.size();
}

void KeyedRateLimiter::gc_locked(double now_sec) {
    for (std::map<std::string, Entry>::iterator it = table_.begin(); it != table_.end();) {
        if (now_sec - it->second.last_seen > idle_gc_sec_) it = table_.erase(it);
        else ++it;
    }
    // 极端情况：全部键都还活跃 → 再清一半（保内存上限优先，限流精度让位）
    if (table_.size() >= max_keys_) {
        size_t drop = table_.size() / 2;
        std::map<std::string, Entry>::iterator it = table_.begin();
        while (drop-- && it != table_.end()) it = table_.erase(it);
    }
}

}  // namespace chat
