// src/seq_dedup.cpp —— (user, seq) 去重窗口实现（table_ 并发访问，全程持 m_，见头注释）
#include "chat/seq_dedup.h"

namespace chat {

std::string SeqDeduper::key(const std::string& user, long long seq) {
    return user + ":" + std::to_string(seq);
}

bool SeqDeduper::check_and_add(const std::string& user, long long seq) {
    std::lock_guard<std::mutex> lk(m_);
    std::string k = key(user, seq);
    if (table_.count(k)) return false;
    if (table_.size() >= max_entries_) table_.clear();  // 整窗淘汰：见头注释取舍
    table_[k] = seq;
    return true;
}

bool SeqDeduper::seen(const std::string& user, long long seq) const {
    std::lock_guard<std::mutex> lk(m_);
    return table_.count(key(user, seq)) != 0;
}

}  // namespace chat
