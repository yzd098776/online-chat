// tests/test_protocol.cpp —— 协议语义：字段校验、(ts,id) 游标编解码、错误码常量
#include "chat/protocol.h"
#include "minitest.h"

MINI_SUITE(protocol)

// 用户名校验：长度边界 / 保留字 / 控制字符
MINI_TEST(username_validation) {
    MINI_ASSERT(chat::valid_username("alice"));
    MINI_ASSERT(chat::valid_username("user_01-测试"));
    MINI_ASSERT(chat::valid_username(std::string(32, 'a')));
    MINI_ASSERT(!chat::valid_username(""));
    MINI_ASSERT(!chat::valid_username(std::string(33, 'a')));
    MINI_ASSERT(!chat::valid_username("SERVER"));
    MINI_ASSERT(!chat::valid_username("ALL"));
    MINI_ASSERT(!chat::valid_username("a\nb"));
}

// 房间名与用户名同约束
MINI_TEST(room_name_validation) {
    MINI_ASSERT(chat::valid_room_name("lobby"));
    MINI_ASSERT(chat::valid_room_name("rust-go"));
    MINI_ASSERT(!chat::valid_room_name(""));
    MINI_ASSERT(!chat::valid_room_name("ALL"));
    MINI_ASSERT(!chat::valid_room_name("x\ty"));
}

// 口令：1-128
MINI_TEST(password_validation) {
    MINI_ASSERT(chat::valid_password("x"));
    MINI_ASSERT(chat::valid_password(std::string(128, 'p')));
    MINI_ASSERT(!chat::valid_password(""));
    MINI_ASSERT(!chat::valid_password(std::string(129, 'p')));
}

// 游标编解码往返：ts 秒级碰撞靠 id tie-break，值必须原样还原
MINI_TEST(cursor_roundtrip) {
    std::string cur = chat::format_cursor(1700000000, 85050);
    MINI_ASSERT_EQ(cur, "1700000000:85050");
    long long ts = 0, id = 0;
    MINI_ASSERT(chat::parse_cursor(cur, ts, id));
    MINI_ASSERT_EQ(ts, 1700000000);
    MINI_ASSERT_EQ(id, 85050);
}

// 非法游标（协议 6.3 → E3001 的触发面）
MINI_TEST(cursor_reject_invalid) {
    long long ts = 0, id = 0;
    MINI_ASSERT(!chat::parse_cursor("", ts, id));
    MINI_ASSERT(!chat::parse_cursor("no-colon", ts, id));
    MINI_ASSERT(!chat::parse_cursor(":123", ts, id));
    MINI_ASSERT(!chat::parse_cursor("123:", ts, id));
    MINI_ASSERT(!chat::parse_cursor("123:456:789", ts, id) || (ts == 123 && id > 0));
    MINI_ASSERT(!chat::parse_cursor("0:5", ts, id));   // ts 必须 > 0
    MINI_ASSERT(!chat::parse_cursor("5:0", ts, id));   // id 必须 > 0
    MINI_ASSERT(!chat::parse_cursor("-5:9", ts, id));
}

// 错误码常量稳定（协议 6.4 错误码表——客户端按号分支，不许漂移）
MINI_TEST(error_codes_stable) {
    MINI_ASSERT_EQ(chat::kErrDupUser, 1001);
    MINI_ASSERT_EQ(chat::kErrNoSuchUser, 1002);
    MINI_ASSERT_EQ(chat::kErrBadPass, 1003);
    MINI_ASSERT_EQ(chat::kErrDupLogin, 1004);
    MINI_ASSERT_EQ(chat::kErrAlreadyAuth, 1005);
    MINI_ASSERT_EQ(chat::kErrBadToken, 1006);
    MINI_ASSERT_EQ(chat::kErrNotAuth, 1007);
    MINI_ASSERT_EQ(chat::kErrBadCredFmt, 1008);
    MINI_ASSERT_EQ(chat::kErrRoomExists, 2001);
    MINI_ASSERT_EQ(chat::kErrNoSuchRoom, 2002);
    MINI_ASSERT_EQ(chat::kErrNotInRoom, 2003);
    MINI_ASSERT_EQ(chat::kErrBadRoomName, 2004);
    MINI_ASSERT_EQ(chat::kErrBadCursor, 3001);
    MINI_ASSERT_EQ(chat::kErrConnRate, 4001);
    MINI_ASSERT_EQ(chat::kErrMsgRate, 4002);
    MINI_ASSERT_EQ(chat::kErrSensitive, 4003);
    MINI_ASSERT_EQ(chat::kErrDb, 9001);
}
