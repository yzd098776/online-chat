// include/chat/frame.h —— 帧层：4 字节大端长度前缀 + JSON body（协议第 1 节）
//
// 职责边界：只管「字节 ↔ 完整 body 字符串」，不理解 JSON 语义（那是 minijson/protocol 的事）。
// 半包/粘包统一策略 = 累积缓冲 + 循环取帧（FrameReader）。
#ifndef CHAT_FRAME_H_
#define CHAT_FRAME_H_

#include <cstddef>
#include <string>

namespace chat {

static const size_t kMaxFrame = 1u << 20;  // 1 MiB：超限即协议错误

// body -> [len|body] 待发字节块（调用方负责整块发出/处理短写）
std::string make_wire_frame(const std::string& body);

class FrameReader {
public:
    explicit FrameReader(size_t max_frame = kMaxFrame) : max_frame_(max_frame), failed_(false) {}
    void feed(const char* data, size_t len);
    // 取出下一帧 body；true=成功。半包时返回 false 且不消费。
    bool next(std::string& body);
    bool failed() const { return failed_; }
    size_t buffered() const { return buf_.size(); }

private:
    std::string buf_;
    size_t max_frame_;
    bool failed_;
};

}  // namespace chat

#endif  // CHAT_FRAME_H_
