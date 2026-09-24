// tests/test_word_filter.cpp —— Trie 敏感词：命中/最长匹配/中文/大小写/替换/边界
#include "chat/word_filter.h"
#include "minitest.h"

MINI_SUITE(word_filter)

using chat::WordFilter;

static WordFilter make_filter() {
    WordFilter f;
    f.add_word("敏感词");
    f.add_word("敏感");
    f.add_word("违禁");
    f.add_word("badword");
    return f;
}

// 基本命中；contains 报第一个命中词
MINI_TEST(basic_hit) {
    WordFilter f = make_filter();
    std::string hit;
    MINI_ASSERT(f.contains("这里有敏感内容", &hit));
    MINI_ASSERT(!hit.empty());
    MINI_ASSERT(!f.contains("完全干净的句子"));
    MINI_ASSERT_EQ(f.word_count(), 4u);
}

// 最长匹配：词表 {敏感, 敏感词} 打「敏感词」命中整词（3 字符段），非重叠跳过
MINI_TEST(longest_match) {
    WordFilter f = make_filter();
    std::string out = f.filter("这是敏感词测试");
    MINI_ASSERT_EQ(out, std::string("这是***测试"));   // 3 个 UTF-8 字符 → 3 个 *
    // 连续两次命中：违禁词违禁 → 各自按字符数替换
    WordFilter g;
    g.add_word("违禁");
    MINI_ASSERT_EQ(g.filter("违禁词违禁"), std::string("**词**"));
}

// ASCII 大小写不敏感（匹配用小写副本，原文替换）
MINI_TEST(case_insensitive_ascii) {
    WordFilter f = make_filter();
    std::string hit;
    MINI_ASSERT(f.contains("This BadWord here", &hit));
    MINI_ASSERT_EQ(f.filter("A BADWORD!"), std::string("A *******!"));
}

// 中文/UTF-8 多字节安全（* 数按字符不按字节）
MINI_TEST(utf8_masks) {
    WordFilter f = make_filter();
    MINI_ASSERT_EQ(f.filter("违禁"), std::string("**"));      // 2 字符→2 个 *
    MINI_ASSERT_EQ(f.filter("敏感"), std::string("**"));      // 2 字符→2 个 *
    // 词根嵌在更长文本中间
    MINI_ASSERT_EQ(f.filter("说敏感的话"), std::string("说**的话"));
}

// 命中段后继续扫描（一个文本多个命中）；无命中原样返回
MINI_TEST(multiple_hits_and_clean) {
    WordFilter f = make_filter();
    MINI_ASSERT_EQ(f.filter("badword和违禁都删"), std::string("*******和**都删"));
    std::string clean = "没有任何问题的文本 with normal english";
    MINI_ASSERT_EQ(f.filter(clean), clean);
}

// 空词/空文本/注释文件加载
MINI_TEST(edges) {
    WordFilter f;
    MINI_ASSERT(!f.add_word(""));
    MINI_ASSERT(!f.contains(""));
    MINI_ASSERT_EQ(f.filter(""), std::string(""));
    MINI_ASSERT_EQ(f.load_file("/nonexistent/words.txt"), 0u);
    MINI_ASSERT(f.empty());
    // 前缀不误伤：词「敏感」不应命中「敏」
    f.add_word("敏感");
    MINI_ASSERT(!f.contains("敏捷的思维"));
    MINI_ASSERT(f.contains("敏感地带"));
}
