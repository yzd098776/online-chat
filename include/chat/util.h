// include/chat/util.h —— 时间等小工具
#ifndef CHAT_UTIL_H_
#define CHAT_UTIL_H_

#include <chrono>

namespace chat {

inline long long now_ms() {
    return (long long)std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

inline long long now_ts() { return now_ms() / 1000; }

}  // namespace chat

#endif  // CHAT_UTIL_H_
