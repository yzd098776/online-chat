// src/frame.cpp —— 帧层实现
#include "chat/frame.h"

namespace chat {

std::string make_wire_frame(const std::string& body) {
    std::string pkt;
    unsigned int n = (unsigned int)body.size();
    pkt += (char)((n >> 24) & 0xFF);
    pkt += (char)((n >> 16) & 0xFF);
    pkt += (char)((n >> 8) & 0xFF);
    pkt += (char)(n & 0xFF);
    pkt += body;
    return pkt;
}

void FrameReader::feed(const char* data, size_t len) {
    if (!failed_) buf_.append(data, len);
}

bool FrameReader::next(std::string& body) {
    if (failed_) return false;
    if (buf_.size() < 4) return false;
    size_t len = ((size_t)(unsigned char)buf_[0] << 24) | ((size_t)(unsigned char)buf_[1] << 16) |
                 ((size_t)(unsigned char)buf_[2] << 8) | ((size_t)(unsigned char)buf_[3]);
    if (len > max_frame_) { failed_ = true; return false; }  // 超长帧：协议错误
    if (buf_.size() < 4 + len) return false;                  // 半包：等更多数据
    body.assign(buf_, 4, len);
    buf_.erase(0, 4 + len);                                   // 粘包：一次 feed 多帧时循环取
    return true;
}

}  // namespace chat
