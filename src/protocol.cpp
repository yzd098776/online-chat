// src/protocol.cpp —— 协议语义层实现
#include "chat/protocol.h"

#include <cstdlib>

namespace chat {

static bool printable_name(const std::string& u) {
    for (size_t i = 0; i < u.size(); ++i)
        if ((unsigned char)u[i] < 0x20) return false;
    return true;
}

bool valid_username(const std::string& u) {
    if (u.empty() || u.size() > 32) return false;
    if (u == "SERVER" || u == "ALL") return false;
    return printable_name(u);
}

bool valid_room_name(const std::string& r) {
    if (r.empty() || r.size() > 32) return false;
    if (r == "SERVER" || r == "ALL") return false;
    return printable_name(r);
}

bool valid_password(const std::string& p) { return !p.empty() && p.size() <= 128; }

std::string format_cursor(long long ts, long long id) {
    char buf[48];
    std::snprintf(buf, sizeof(buf), "%lld:%lld", ts, id);
    return std::string(buf);
}

bool parse_cursor(const std::string& cur, long long& ts, long long& id) {
    if (cur.empty()) return false;
    size_t p = cur.find(':');
    if (p == std::string::npos || p == 0 || p + 1 >= cur.size()) return false;
    ts = std::atoll(cur.substr(0, p).c_str());
    id = std::atoll(cur.substr(p + 1).c_str());
    return ts > 0 && id > 0;
}

}  // namespace chat
