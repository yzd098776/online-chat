// include/chat/seq_dedup.h —— (user, seq) 幂等去重窗口（协议第 6.6 节）
//
// 语义：同一 (user, seq) 只放行一次；重复帧返回 false（调用方只回 ACK、零副作用）。
// 取舍：内存窗口（题面 4 表 schema 无 seq 列）；重启清零的残余重复由客户端 (from,seq)
// 显示去重兜底。容量超限整体清空（教学规模够用；生产应 LRU/落盘，见 README 已知限制）。
//
// 【线程安全】表被全部连接线程并发调用（每条 MESSAGE 一次）——必须自带锁：
// 无锁的 unordered_map 并发读写是数据竞争，rehash 与查找交错会读野指针。
// 证据链（三件套，均有留痕；排查档案见 VERIFY.md 附录 A）：
//   ① 段错误本体：dmesg 留 `chat_server_v5[...]: segfault ...`，addr2line 落到本文件的
//      _M_find_before_node（unordered_map 查找）；
//   ② 负对照：临时剥掉本文件的锁，同一套 -fsanitize=thread 编译/运行【必报】
//      data race seq_dedup.cpp:13——证明检测链路有效，修复后的零告警不是漏报；
//   ③ 修复有效：concurrent_stress（8 线程并发插查）100% 通过 + TSan 全量零告警。
// （压测期间另有几次「进程消失」属端口占用导致绑定失败退出，与竞态无关，已单列说明。）
#ifndef CHAT_SEQ_DEDUP_H_
#define CHAT_SEQ_DEDUP_H_

#include <cstddef>
#include <mutex>
#include <string>
#include <unordered_map>

namespace chat {

class SeqDeduper {
public:
    explicit SeqDeduper(size_t max_entries = 500000) : max_entries_(max_entries) {}
    // true = 首次（放行）；false = 重复（应幂等应答）
    bool check_and_add(const std::string& user, long long seq);
    bool seen(const std::string& user, long long seq) const;
    size_t size() const {
        std::lock_guard<std::mutex> lk(m_);
        return table_.size();
    }
    void clear() {
        std::lock_guard<std::mutex> lk(m_);
        table_.clear();
    }

private:
    static std::string key(const std::string& user, long long seq);
    size_t max_entries_;
    mutable std::mutex m_;  // 保护 table_（并发访问的唯一闸门）
    std::unordered_map<std::string, long long> table_;
};

}  // namespace chat

#endif  // CHAT_SEQ_DEDUP_H_
