// include/chat/protocol.h —— 协议语义层：错误码、帧类型、字段校验、历史游标（PROTOCOL.md 第 6 节）
#ifndef CHAT_PROTOCOL_H_
#define CHAT_PROTOCOL_H_

#include <string>

namespace chat {

// ---------------- 错误码（全表见 PROTOCOL.md 6.4：错误码 + 中文文案，禁止静默失败） ----------------
enum ErrorCode {
    kErrDupUser = 1001,      // 用户名已存在（users.username UNIQUE 约束）
    kErrNoSuchUser = 1002,   // 用户不存在
    kErrBadPass = 1003,      // 密码错误
    kErrDupLogin = 1004,     // 重复登录：该账号已在线
    kErrAlreadyAuth = 1005,  // 重复登录：本连接已在登录态
    kErrBadToken = 1006,     // Token 无效或已过期
    kErrNotAuth = 1007,      // 尚未登录
    kErrBadCredFmt = 1008,   // 用户名/密码格式非法
    kErrBadChallenge = 1009, // 挑战缺失/过期/不匹配（须先 LOGIN_HELLO/REGISTER_HELLO）
    kErrRoomExists = 2001,   // 房间已存在
    kErrNoSuchRoom = 2002,   // 房间不存在
    kErrNotInRoom = 2003,    // 尚未加入房间
    kErrBadRoomName = 2004,  // 房间名非法
    kErrBadCursor = 3001,    // 历史游标非法
    kErrConnRate = 4001,     // 限流：每 IP 建连过于频繁（ERR + 立即断开）
    kErrMsgRate = 4002,      // 限流：每用户发消息过于频繁（NACK.code + 保留待发队列可重试）
    kErrSensitive = 4003,    // 敏感词：reject 模式拒绝发送（NACK.code）
    kErrBackpressure = 4004, // 背压：发送积压超限/读取过慢（ERR + 断开；客户端应慢速重连）
    kErrDb = 9001            // 数据库错误
};

// ---------------- 字段校验 ----------------
// 用户名：1-32 字符、不含控制字符、非保留字（SERVER/ALL 是帧里的语义名）
bool valid_username(const std::string& u);
// 房间名：同上约束（保留字避免与 to=ALL / from=SERVER 混淆）
bool valid_room_name(const std::string& r);
// 口令：1-128 字符（明文 TCP 下过长无意义；教学限制见 README「已知限制」）
bool valid_password(const std::string& p);

// ---------------- 历史游标（(ts,id) keyset，见服务器 Q3） ----------------
std::string format_cursor(long long ts, long long id);           // "ts:id"
bool parse_cursor(const std::string& cur, long long& ts, long long& id);

}  // namespace chat

#endif  // CHAT_PROTOCOL_H_
