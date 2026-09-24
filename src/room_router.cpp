// src/room_router.cpp —— 房间路由表实现（全方法持锁，可多线程直调）
#include "chat/room_router.h"

namespace chat {

bool RoomRouter::create(const std::string& name, const std::string& owner, long long db_id,
                        long long created_at) {
    std::lock_guard<std::mutex> lk(m_);
    if (rooms_.count(name)) return false;
    RoomInfo r;
    r.db_id = db_id;
    r.name = name;
    r.owner = owner;
    r.created_at = created_at;
    rooms_[name] = r;
    return true;
}

bool RoomRouter::exists(const std::string& name) const {
    std::lock_guard<std::mutex> lk(m_);
    return rooms_.count(name) != 0;
}

bool RoomRouter::join(const std::string& user, const std::string& room, std::string* prev_room) {
    std::lock_guard<std::mutex> lk(m_);
    if (!rooms_.count(room)) return false;
    std::string prev;
    std::unordered_map<std::string, std::string>::iterator it = user_room_.find(user);
    if (it != user_room_.end()) {
        prev = it->second;
        if (prev == room) {  // 幂等：重复 JOIN
            if (prev_room) *prev_room = prev;
            return true;
        }
        std::map<std::string, RoomInfo>::iterator pr = rooms_.find(prev);
        if (pr != rooms_.end()) pr->second.members.erase(user);
    }
    rooms_[room].members.insert(user);
    user_room_[user] = room;
    if (prev_room) *prev_room = prev;
    return true;
}

std::string RoomRouter::leave(const std::string& user) {
    std::lock_guard<std::mutex> lk(m_);
    std::unordered_map<std::string, std::string>::iterator it = user_room_.find(user);
    if (it == user_room_.end()) return "";
    std::string room = it->second;
    user_room_.erase(it);
    std::map<std::string, RoomInfo>::iterator r = rooms_.find(room);
    if (r != rooms_.end()) r->second.members.erase(user);
    return room;
}

std::string RoomRouter::user_room(const std::string& user) const {
    std::lock_guard<std::mutex> lk(m_);
    std::unordered_map<std::string, std::string>::const_iterator it = user_room_.find(user);
    return it == user_room_.end() ? "" : it->second;
}

std::vector<std::string> RoomRouter::members(const std::string& room) const {
    std::vector<std::string> v;
    std::lock_guard<std::mutex> lk(m_);
    std::map<std::string, RoomInfo>::const_iterator it = rooms_.find(room);
    if (it != rooms_.end())
        for (std::set<std::string>::const_iterator m = it->second.members.begin();
             m != it->second.members.end(); ++m) v.push_back(*m);
    return v;
}

long long RoomRouter::db_id(const std::string& room) const {
    std::lock_guard<std::mutex> lk(m_);
    std::map<std::string, RoomInfo>::const_iterator it = rooms_.find(room);
    return it == rooms_.end() ? 0 : it->second.db_id;
}

bool RoomRouter::meta(const std::string& room, RoomInfo& out) const {
    std::lock_guard<std::mutex> lk(m_);
    std::map<std::string, RoomInfo>::const_iterator it = rooms_.find(room);
    if (it == rooms_.end()) return false;
    out = it->second;
    return true;
}

std::vector<RoomInfo> RoomRouter::list() const {
    std::vector<RoomInfo> v;
    std::lock_guard<std::mutex> lk(m_);
    for (std::map<std::string, RoomInfo>::const_iterator it = rooms_.begin();
         it != rooms_.end(); ++it) v.push_back(it->second);
    return v;
}

size_t RoomRouter::room_count() const {
    std::lock_guard<std::mutex> lk(m_);
    return rooms_.size();
}

}  // namespace chat
