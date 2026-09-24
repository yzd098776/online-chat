// include/chat/room_router.h —— 房间路由表：room → members 与 user → room（服务器 Q2）
//
// 这是「O(N 全量广播 → O(房间成员) 按房间路由」改造的核心数据结构：
// 广播目标查询 members(room) 只返回本房间用户名，无关用户零成本。
// 成员集按【用户名】而非连接：Token 顶号续传时用户对房间内其他人"从没离开过"（Q5）。
#ifndef CHAT_ROOM_ROUTER_H_
#define CHAT_ROOM_ROUTER_H_

#include <map>
#include <mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

namespace chat {

struct RoomInfo {
    long long db_id;
    std::string name;
    std::string owner;
    long long created_at;
    std::set<std::string> members;
};

class RoomRouter {
public:
    // 创建房间（db_id 为 messages.room_id 用的主键）。false = 已存在
    bool create(const std::string& name, const std::string& owner, long long db_id,
                long long created_at);
    bool exists(const std::string& name) const;

    // 加入房间；已在其他房间则自动摘除旧房（prev_room 返回旧房名，""=之前不在房间）。
    // false = 房间不存在。重复加入同一房间幂等（prev_room = 该房间名）
    bool join(const std::string& user, const std::string& room, std::string* prev_room);
    // 离开当前房间，返回离开的房间名（""=本来就不在）
    std::string leave(const std::string& user);
    // 摘除房间内某成员（顶号踢旧连接等场景不用它——成员集按用户名，见头注释）

    std::string user_room(const std::string& user) const;
    std::vector<std::string> members(const std::string& room) const;
    long long db_id(const std::string& room) const;
    bool meta(const std::string& room, RoomInfo& out) const;
    std::vector<RoomInfo> list() const;
    size_t room_count() const;

private:
    mutable std::mutex m_;
    std::map<std::string, RoomInfo> rooms_;
    std::unordered_map<std::string, std::string> user_room_;
};

}  // namespace chat

#endif  // CHAT_ROOM_ROUTER_H_
