// include/chat/token_book.h —— 会话 Token 表（内存态，服务器 Q4）
//
// 32 字节 CSPRNG + 过期时间（--token-ttl）；表键 = SHA256(token) → (user, exp)。
// 【内存只存哈希】：Token 是持有者凭证，明文进内存表则日志/core dump/内存转储一泄
// 就等同于永久凭证；存 SHA256(token) 后，泄露的表只能用来「验」不能用来「冒」——
// 查表时对出示的 Token 先算同一哈希再比对（键即哈希，O(1) 查找不变）。
// 取舍：服务器重启失效（客户端回落口令）；生产应存 tokens 表/Redis + 吊销与滑动续期。
#ifndef CHAT_TOKEN_BOOK_H_
#define CHAT_TOKEN_BOOK_H_

#include <mutex>
#include <string>
#include <unordered_map>

#include "chat/pwd_hash.h"
#include "chat/util.h"

namespace chat {

class TokenBook {
public:
    explicit TokenBook(long long ttl_sec) : ttl_sec_(ttl_sec) {}

    // 32 字节 CSPRNG → hex token；exp_out 收过期时间（Unix 秒）。
    // 明文 token 只出现在返回值里（发给客户端），表里落 SHA256(token)。
    std::string issue(const std::string& user, long long* exp_out) {
        std::string token = pwd_hash::random_hex(32);
        long long exp = now_ts() + ttl_sec_;
        {
            std::lock_guard<std::mutex> lk(m_);
            Token& t = tokens_[hash_token(token)];
            t.user = user;
            t.exp = exp;
            if (tokens_.size() > 100000) gc_locked();
        }
        if (exp_out) *exp_out = exp;
        return token;
    }

    bool validate(const std::string& token, std::string& user_out) {
        if (token.empty()) return false;
        std::lock_guard<std::mutex> lk(m_);
        std::unordered_map<std::string, Token>::iterator it = tokens_.find(hash_token(token));
        if (it == tokens_.end()) return false;
        if (it->second.exp < now_ts()) {
            tokens_.erase(it);  // 过期即删
            return false;
        }
        user_out = it->second.user;
        return true;
    }

    size_t size() const {
        std::lock_guard<std::mutex> lk(m_);
        return tokens_.size();
    }

private:
    struct Token {
        std::string user;
        long long exp;
    };
    // 表键：SHA256(token)（hex）。单向——表泄不等于凭证泄。
    static std::string hash_token(const std::string& token) {
        unsigned char d[32];
        pwd_hash::sha256::digest((const unsigned char*)token.data(), token.size(), d);
        return pwd_hash::to_hex(d, 32);
    }
    void gc_locked() {
        long long now = now_ts();
        for (std::unordered_map<std::string, Token>::iterator it = tokens_.begin();
             it != tokens_.end();) {
            if (it->second.exp < now) it = tokens_.erase(it);
            else ++it;
        }
    }
    long long ttl_sec_;
    mutable std::mutex m_;
    std::unordered_map<std::string, Token> tokens_;  // SHA256(token) → (user, exp)
};

}  // namespace chat

#endif  // CHAT_TOKEN_BOOK_H_
