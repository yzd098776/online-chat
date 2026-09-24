// src/word_filter.cpp —— Trie 敏感词过滤实现（复杂度说明见 word_filter.h）
#include "chat/word_filter.h"

#include <cctype>
#include <cstdio>
#include <fstream>

namespace chat {

WordFilter::WordFilter() : words_(0) { new_node(); }  // 根节点
WordFilter::~WordFilter() {}

void WordFilter::clear() {
    nodes_.clear();
    words_ = 0;
    new_node();
}

int WordFilter::new_node() {
    nodes_.push_back(Node());
    return (int)nodes_.size() - 1;
}

static std::string ascii_lower(const std::string& s) {
    std::string o(s);
    for (size_t i = 0; i < o.size(); ++i)
        if (o[i] >= 'A' && o[i] <= 'Z') o[i] = (char)(o[i] - 'A' + 'a');
    return o;
}

bool WordFilter::add_word(const std::string& word) {
    if (word.empty()) return false;
    std::string w = ascii_lower(word);
    int cur = 0;
    for (size_t i = 0; i < w.size(); ++i) {
        unsigned char c = (unsigned char)w[i];
        std::map<unsigned char, int>::iterator it = nodes_[cur].next.find(c);
        if (it == nodes_[cur].next.end()) {
            int id = new_node();
            nodes_[cur].next[c] = id;
            cur = id;
        } else {
            cur = it->second;
        }
    }
    if (!nodes_[cur].terminal) {
        nodes_[cur].terminal = true;
        nodes_[cur].word = word;  // 保留原始大小写用于命中报告
        ++words_;
    }
    return true;
}

size_t WordFilter::load_file(const std::string& path) {
    std::ifstream in(path.c_str());
    if (!in) return 0;
    size_t added = 0;
    std::string line;
    while (std::getline(in, line)) {
        while (!line.empty() && (line[line.size() - 1] == '\r' || line[line.size() - 1] == '\n'))
            line.erase(line.size() - 1);
        if (line.empty() || line[0] == '#') continue;  // # 注释行
        if (add_word(line)) ++added;
    }
    return added;
}

void WordFilter::scan(const std::string& lowered, std::vector<Match>& out) const {
    size_t i = 0, n = lowered.size();
    while (i < n) {
        int cur = 0;
        size_t best_end = 0;
        int best_terminal = -1;
        size_t j = i;
        while (j < n) {
            std::map<unsigned char, int>::const_iterator it =
                nodes_[cur].next.find((unsigned char)lowered[j]);
            if (it == nodes_[cur].next.end()) break;
            cur = it->second;
            ++j;
            if (nodes_[cur].terminal) {  // 记录最长匹配
                best_end = j;
                best_terminal = cur;
            }
        }
        if (best_terminal >= 0) {  // 命中：整段跳过（非重叠最长匹配）
            Match m;
            m.start = i;
            m.end = best_end;
            m.word = nodes_[best_terminal].word;
            out.push_back(m);
            i = best_end;
        } else {
            ++i;
        }
    }
}

bool WordFilter::contains(const std::string& text, std::string* hit) const {
    std::vector<Match> ms;
    scan(ascii_lower(text), ms);
    if (ms.empty()) return false;
    if (hit) *hit = ms[0].word;
    return true;
}

// UTF-8 字符数（用于把命中段替换成等长的 *，「敏感词」→「***」而非「*」）
static size_t utf8_chars(const std::string& s) {
    size_t n = 0;
    for (size_t i = 0; i < s.size(); ++i)
        if (((unsigned char)s[i] & 0xC0) != 0x80) ++n;  // 非续字节 = 一个字符
    return n;
}

std::string WordFilter::filter(const std::string& text, char mask) const {
    std::vector<Match> ms;
    scan(ascii_lower(text), ms);
    if (ms.empty()) return text;
    std::string out;
    size_t pos = 0;
    for (size_t k = 0; k < ms.size(); ++k) {
        out.append(text, pos, ms[k].start - pos);
        size_t chars = utf8_chars(text.substr(ms[k].start, ms[k].end - ms[k].start));
        out.append(chars ? chars : 1, mask);
        pos = ms[k].end;
    }
    out.append(text, pos, std::string::npos);
    return out;
}

}  // namespace chat
