// include/chat/word_filter.h —— 敏感词过滤（Trie 前缀树）
//
// 结构：字典 Trie（每个节点 256 路子节点表的稀疏 map 实现），词表逐词插入。
// 【时间复杂度】（面试必问）：
//   建树：O(Σ|w|) 时间与空间（所有词的总长度）——load_file 全量插入；
//   查询（朴素 Trie 扫描）：O(n × L) 最坏（n=文本字节数，L=最长词字节数）；
//     实际远好于最坏——每个起点沿树走，失配即剪枝。对中文 UTF-8，词按【字节】匹配
//     （合法 UTF-8 词的字节序列不会落在多字节字符内部造成假匹配）。
//   更优：AC 自动机（Aho-Corasick）预处理 O(Σ|w|)，扫描 O(n)，适合词表上万/文本很长；
//     本项目词表规模小，朴素 Trie 更简单、常数更小，故不引入 AC（README 有对照说明）。
// 匹配策略：从左到右扫描，起点 i 取【最长】匹配后整段跳过（非重叠）：
//   词表 {敏感, 敏感词} 打「敏感词」→ 命中「敏感词」整词（3 个 *），不是「敏感」+剩「词」。
// 大小写：ASCII 统一小写后匹配（匹配用副本，替换回写原文，字节偏移不变）。
#ifndef CHAT_WORD_FILTER_H_
#define CHAT_WORD_FILTER_H_

#include <cstddef>
#include <map>
#include <string>
#include <vector>

namespace chat {

class WordFilter {
public:
    WordFilter();
    ~WordFilter();
    void clear();
    // 一行一词（# 开头=注释行）；返回成功加入的词数
    size_t load_file(const std::string& path);
    bool add_word(const std::string& word);
    size_t word_count() const { return words_; }
    size_t node_count() const { return nodes_.size(); }  // Trie 节点数（测试/观测）
    bool empty() const { return words_ == 0; }

    // 命中检测；hit 收第一个命中的词。true=含敏感词
    bool contains(const std::string& text, std::string* hit = NULL) const;
    // 过滤：命中段替换为 mask 字符（按 UTF-8 字符数重复，「敏感词」→「＊＊＊」）
    std::string filter(const std::string& text, char mask = '*') const;

private:
    struct Node {
        std::map<unsigned char, int> next;  // 字节转移 → 节点下标
        bool terminal;
        std::string word;                   // 终点词（替换/命中用）
        Node() : terminal(false) {}
    };
    struct Match {
        size_t start, end;  // 原文/小写副本的字节区间 [start, end)
        std::string word;
    };
    int new_node();
    void scan(const std::string& lowered, std::vector<Match>& out) const;  // 非重叠最长匹配

    std::vector<Node> nodes_;
    size_t words_;
};

}  // namespace chat

#endif  // CHAT_WORD_FILTER_H_
