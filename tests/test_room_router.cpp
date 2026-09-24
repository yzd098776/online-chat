// tests/test_room_router.cpp —— 路由层：房间生命周期、成员隔离（广播路由核心）、顶号语义
#include "chat/room_router.h"
#include "minitest.h"

MINI_SUITE(room_router)

using chat::RoomInfo;
using chat::RoomRouter;

// 建房 / 判重 / 元数据
MINI_TEST(create_and_meta) {
    RoomRouter r;
    MINI_ASSERT(r.create("lobby", "SERVER", 1, 100));
    MINI_ASSERT(!r.create("lobby", "alice", 2, 200));  // 重名拒绝
    MINI_ASSERT(r.exists("lobby"));
    MINI_ASSERT(!r.exists("nope"));
    RoomInfo m;
    MINI_ASSERT(r.meta("lobby", m));
    MINI_ASSERT_EQ(m.db_id, 1);
    MINI_ASSERT_EQ(m.owner, "SERVER");
    MINI_ASSERT_EQ(r.db_id("lobby"), 1);
    MINI_ASSERT_EQ(r.db_id("nope"), 0);
    MINI_ASSERT_EQ(r.room_count(), 1u);
}

// 加入 / 离开 / 幂等
MINI_TEST(join_leave) {
    RoomRouter r;
    r.create("lobby", "SERVER", 1, 100);
    std::string prev;
    MINI_ASSERT(r.join("alice", "lobby", &prev));
    MINI_ASSERT_EQ(prev, "");                    // 之前不在房间
    MINI_ASSERT_EQ(r.user_room("alice"), "lobby");
    MINI_ASSERT(r.join("alice", "lobby", &prev));  // 重复 JOIN 幂等
    MINI_ASSERT_EQ(prev, "lobby");
    MINI_ASSERT_EQ(r.members("lobby").size(), 1u);
    MINI_ASSERT_EQ(r.leave("alice"), "lobby");
    MINI_ASSERT_EQ(r.user_room("alice"), "");
    MINI_ASSERT_EQ(r.leave("alice"), "");        // 二次离开无副作用
    MINI_ASSERT_EQ(r.members("lobby").size(), 0u);
}

// 加入不存在的房间失败
MINI_TEST(join_missing_room) {
    RoomRouter r;
    std::string prev;
    MINI_ASSERT(!r.join("alice", "ghost", &prev));
    MINI_ASSERT_EQ(r.user_room("alice"), "");
}

// 换房：自动摘旧房成员（进新出旧通知的前置状态）
MINI_TEST(switch_room) {
    RoomRouter r;
    r.create("a", "u", 1, 1);
    r.create("b", "u", 2, 2);
    std::string prev;
    r.join("alice", "a", &prev);
    MINI_ASSERT(r.join("alice", "b", &prev));
    MINI_ASSERT_EQ(prev, "a");
    MINI_ASSERT_EQ(r.user_room("alice"), "b");
    MINI_ASSERT_EQ(r.members("a").size(), 0u);   // 旧房已摘
    MINI_ASSERT_EQ(r.members("b").size(), 1u);
}

// 【广播路由核心】房间成员隔离：members() 只回本房间用户名——O(房间成员) 广播的目标集
MINI_TEST(membership_isolation) {
    RoomRouter r;
    r.create("dev", "u", 1, 1);
    r.create("ops", "u", 2, 2);
    std::string prev;
    r.join("alice", "dev", &prev);
    r.join("charlie", "dev", &prev);
    r.join("bob", "ops", &prev);
    std::vector<std::string> dev = r.members("dev");
    std::vector<std::string> ops = r.members("ops");
    MINI_ASSERT_EQ(dev.size(), 2u);
    MINI_ASSERT_EQ(ops.size(), 1u);
    // dev 房广播目标不含 bob
    bool bob_in_dev = false;
    for (size_t i = 0; i < dev.size(); ++i)
        if (dev[i] == "bob") bob_in_dev = true;
    MINI_ASSERT(!bob_in_dev);
    MINI_ASSERT_EQ(ops[0], "bob");
}

// 【顶号语义】成员集按用户名：同名用户"从没离开过"（Q5/Q2）——
// 新旧两个会话共享同一成员身份，join 同名幂等，不会把用户踢出房间
MINI_TEST(takeover_membership_by_username) {
    RoomRouter r;
    r.create("lobby", "u", 1, 1);
    std::string prev;
    r.join("alice", "lobby", &prev);      // 旧连接在房间
    MINI_ASSERT(r.join("alice", "lobby", &prev));  // Token 顶号后新连接重进：幂等
    MINI_ASSERT_EQ(r.members("lobby").size(), 1u);  // 不是 2，也不是 0——用户一直在
    MINI_ASSERT_EQ(r.user_room("alice"), "lobby");
}

// list() 全量快照（ROOMS_LIST 数据源）
MINI_TEST(list_rooms) {
    RoomRouter r;
    r.create("lobby", "SERVER", 1, 10);
    r.create("dev", "alice", 2, 20);
    std::vector<RoomInfo> all = r.list();
    MINI_ASSERT_EQ(all.size(), 2u);
    MINI_ASSERT_EQ(all[0].name, "dev");    // 按名字典序（std::map）
    MINI_ASSERT_EQ(all[1].name, "lobby");
}
