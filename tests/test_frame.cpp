// tests/test_frame.cpp —— 帧层单测：编解码往返、半包/粘包拼接、超长帧
#include "chat/frame.h"
#include "minitest.h"

MINI_SUITE(frame)

using chat::FrameReader;
using chat::kMaxFrame;
using chat::make_wire_frame;

// 编解码往返：body -> 加长度头 -> FrameReader 原样取回
MINI_TEST(roundtrip) {
    std::string body = "{\"type\":\"MESSAGE\",\"content\":\"你好\\nworld\"}";
    std::string wire = make_wire_frame(body);
    FrameReader r;
    r.feed(wire.data(), wire.size());
    std::string out;
    MINI_ASSERT(r.next(out));
    MINI_ASSERT_EQ(out, body);
    MINI_ASSERT(!r.next(out));  // 没有下一帧
}

// 空 body 也是合法帧（len=0）
MINI_TEST(roundtrip_empty_body) {
    std::string wire = make_wire_frame("");
    FrameReader r;
    r.feed(wire.data(), wire.size());
    std::string out;
    MINI_ASSERT(r.next(out));
    MINI_ASSERT_EQ(out.size(), 0u);
}

// 半包：逐字节喂入，缓冲不足时 next=false 且不消费，收齐即出帧
MINI_TEST(half_packet) {
    std::string body = "half-packet-body";
    std::string wire = make_wire_frame(body);
    FrameReader r;
    std::string out;
    for (size_t i = 0; i + 1 < wire.size(); ++i) {  // 始终差 1 字节
        r.feed(&wire[i], 1);
        MINI_ASSERT(!r.next(out));
    }
    MINI_ASSERT_EQ(r.buffered(), wire.size() - 1);
    r.feed(&wire[wire.size() - 1], 1);  // 最后 1 字节到齐
    MINI_ASSERT(r.next(out));
    MINI_ASSERT_EQ(out, body);
    MINI_ASSERT_EQ(r.buffered(), 0u);
}

// 粘包：一次喂入 3 帧，循环取出 3 个 body（顺序保持）
MINI_TEST(sticky_packets) {
    std::string wire = make_wire_frame("AAA") + make_wire_frame("BBB") + make_wire_frame("CCC");
    FrameReader r;
    r.feed(wire.data(), wire.size());
    std::string out;
    MINI_ASSERT(r.next(out)); MINI_ASSERT_EQ(out, "AAA");
    MINI_ASSERT(r.next(out)); MINI_ASSERT_EQ(out, "BBB");
    MINI_ASSERT(r.next(out)); MINI_ASSERT_EQ(out, "CCC");
    MINI_ASSERT(!r.next(out));
}

// 半包 + 粘包混合：一帧拆两半夹着另一整帧
MINI_TEST(half_then_sticky) {
    std::string w1 = make_wire_frame("first-message");
    std::string w2 = make_wire_frame("second");
    FrameReader r;
    r.feed(w1.data(), 5);              // w1 前 5 字节（4 头 + 1 body）
    std::string out;
    MINI_ASSERT(!r.next(out));
    std::string rest = w1.substr(5) + w2;  // w1 剩余 + 整个 w2 一次到达
    r.feed(rest.data(), rest.size());
    MINI_ASSERT(r.next(out)); MINI_ASSERT_EQ(out, "first-message");
    MINI_ASSERT(r.next(out)); MINI_ASSERT_EQ(out, "second");
}

// 超长帧：声明长度 > MAX_FRAME → 协议错误，reader 进入 failed 且不再出帧
MINI_TEST(oversized_frame) {
    std::string wire;
    unsigned int n = (unsigned int)kMaxFrame + 1;
    wire += (char)((n >> 24) & 0xFF);
    wire += (char)((n >> 16) & 0xFF);
    wire += (char)((n >> 8) & 0xFF);
    wire += (char)(n & 0xFF);
    FrameReader r;
    r.feed(wire.data(), wire.size());
    std::string out;
    MINI_ASSERT(!r.next(out));
    MINI_ASSERT(r.failed());
    r.feed("more", 4);  // failed 后拒绝再喂
    MINI_ASSERT(!r.next(out));
    MINI_ASSERT_EQ(r.buffered(), 4u);  // 原缓冲保留但不消费（调用方负责断开）
}

// 恰好 MAX_FRAME 的帧合法
MINI_TEST(max_frame_boundary) {
    std::string body(kMaxFrame, 'x');
    std::string wire = make_wire_frame(body);
    FrameReader r;
    r.feed(wire.data(), wire.size());
    std::string out;
    MINI_ASSERT(r.next(out));
    MINI_ASSERT_EQ(out.size(), kMaxFrame);
    MINI_ASSERT(!r.failed());
}
