// tests/test_minijson.cpp —— JSON 编解码往返、转义/Unicode、嵌套结构、非法输入
#include "chat/minijson.h"
#include "minitest.h"

MINI_SUITE(minijson)

using minijson::Object;

// 编解码往返：serialize -> parse 字段逐一还原
MINI_TEST(roundtrip) {
    Object o;
    o.set_str("type", "MESSAGE");
    o.set_str("from", "张三");
    o.set_num("seq", 42);
    o.set_list("content", std::vector<std::string>{"alice", "bob"});
    std::string s = minijson::serialize(o);
    Object p;
    MINI_ASSERT(minijson::parse(s, p));
    MINI_ASSERT_EQ(p.get_str("type"), "MESSAGE");
    MINI_ASSERT_EQ(p.get_str("from"), "张三");
    MINI_ASSERT_EQ(p.get_num("seq"), 42);
    std::vector<std::string> l = p.get_list("content");
    MINI_ASSERT_EQ(l.size(), 2u);
    MINI_ASSERT_EQ(l[0], "alice");
    MINI_ASSERT_EQ(l[1], "bob");
}

// 转义往返：引号/反斜杠/换行/控制字符
MINI_TEST(escape_roundtrip) {
    Object o;
    o.set_str("content", "line1\nline2\t\"quoted\" \\slash\\ \x01");
    Object p;
    MINI_ASSERT(minijson::parse(minijson::serialize(o), p));
    MINI_ASSERT_EQ(p.get_str("content"), o.get_str("content"));
}

// \uXXXX 解析（含 BMP 代理对 = emoji 😀 U+1F600）
MINI_TEST(unicode_escapes) {
    Object p;
    MINI_ASSERT(minijson::parse("{\"a\":\"\\u4f60\\u597d\"}", p));
    MINI_ASSERT_EQ(p.get_str("a"), "你好");  // U+4F60 U+597D 的 UTF-8
    MINI_ASSERT(minijson::parse("{\"b\":\"\\ud83d\\ude00\"}", p));
    std::string emoji = p.get_str("b");
    MINI_ASSERT_EQ(emoji.size(), 4u);  // UTF-8 4 字节
}

// 嵌套结构：HISTORY 条目数组 / ROOMS_LIST 对象数组（协议 6.3）
MINI_TEST(nested_objects) {
    Object item;
    item.set_num("id", 7);
    item.set_str("from", "alice");
    item.set_str("content", "hi");
    Object page;
    page.set_str("type", "HISTORY");
    std::vector<Object> items;
    items.push_back(item);
    page.set_objs("content", items);
    Object p;
    MINI_ASSERT(minijson::parse(minijson::serialize(page), p));
    std::vector<Object> got = p.get_objs("content");
    MINI_ASSERT_EQ(got.size(), 1u);
    MINI_ASSERT_EQ(got[0].get_num("id"), 7);
    MINI_ASSERT_EQ(got[0].get_str("from"), "alice");
}

// 数字含负数/小数（小数丢弃小数部分——子集语义）
MINI_TEST(numbers) {
    Object p;
    MINI_ASSERT(minijson::parse("{\"a\":-123,\"b\":3.99,\"c\":0}", p));
    MINI_ASSERT_EQ(p.get_num("a"), -123);
    MINI_ASSERT_EQ(p.get_num("b"), 3);
    MINI_ASSERT_EQ(p.get_num("c"), 0);
}

// 非法 JSON 一律拒绝（坏帧不许静默变成空对象）
MINI_TEST(reject_invalid) {
    Object p;
    MINI_ASSERT(!minijson::parse("", p));
    MINI_ASSERT(!minijson::parse("not-json", p));
    MINI_ASSERT(!minijson::parse("[1,2]", p));            // 顶层必须是对象
    MINI_ASSERT(!minijson::parse("{\"a\":1,}", p));       // 尾逗号
    MINI_ASSERT(!minijson::parse("{\"a\" 1}", p));        // 缺冒号
    MINI_ASSERT(!minijson::parse("{\"a\":\"unterminated}", p));
    MINI_ASSERT(!minijson::parse("{\"a\":1} extra", p));  // 尾部垃圾
    MINI_ASSERT(!minijson::parse("{\"a\":\"\\q\"}", p));  // 非法转义
    MINI_ASSERT(!minijson::parse("{", p));
}

// 缺字段回默认值（get_* 的防御语义）
MINI_TEST(defaults) {
    Object p;
    MINI_ASSERT(minijson::parse("{}", p));
    MINI_ASSERT_EQ(p.get_str("nope", "def"), "def");
    MINI_ASSERT_EQ(p.get_num("nope", 9), 9);
    MINI_ASSERT_EQ(p.get_list("nope").size(), 0u);
    MINI_ASSERT_EQ(p.get_objs("nope").size(), 0u);
}

// 转义全集 + 非法转义/代理对拒绝（parse_string 分支补全）
MINI_TEST(escape_and_surrogate_edges) {
    Object o;
    o.set_str("c", "a\"b\\c\bd\fe\rf");
    Object p;
    MINI_ASSERT(minijson::parse(minijson::serialize(o), p));
    MINI_ASSERT_EQ(p.get_str("c"), o.get_str("c"));
    Object bad;
    MINI_ASSERT(!minijson::parse("{\"a\":\"\\uZZZZ\"}", bad));  // 非法 hex
    MINI_ASSERT(!minijson::parse("{\"a\":\"\\udc00\"}", bad));  // 孤立低代理
    MINI_ASSERT(!minijson::parse("{\"a\":\"\\ud83d\\u0041\"}", bad));  // 高代理后非低代理
    MINI_ASSERT(!minijson::parse("{\"a\":\"\x01\"}", bad));     // 裸控制字符
}

// 数字子集语义（整数部分截断，小数/指数丢弃）；null 映射空串；空数组/空对象
MINI_TEST(numbers_null_containers) {
    Object p;
    MINI_ASSERT(minijson::parse("{\"a\":1e5,\"b\":2E+2,\"c\":null,\"d\":[],\"e\":{}}", p));
    MINI_ASSERT_EQ(p.get_num("a"), 1);            // 子集语义：1e5 的指数部分丢弃 → 1（头注释）
    MINI_ASSERT_EQ(p.get_num("b"), 2);            // 同上：2E+2 → 2
    MINI_ASSERT_EQ(p.get_str("c"), "");           // null → 空串（子集语义）
    MINI_ASSERT_EQ(p.get_list("d").size(), 0u);
    MINI_ASSERT_EQ(p.get_objs("e").size(), 0u);
    // set 覆盖同名键（不追加重复字段）
    p.set_str("a", "replaced");
    MINI_ASSERT_EQ(p.get_str("a"), "replaced");
    p.set_num("a", 7);
    MINI_ASSERT_EQ(p.get_num("a"), 7);
}

// 空 obj 的 Value 序列化为 null（serialize_value T_OBJ 空指针分支）
MINI_TEST(serialize_null_obj_value) {
    minijson::Value v;
    v.type = minijson::Value::T_OBJ;  // obj 为空 shared_ptr
    Object o;
    o.set("x", v);
    MINI_ASSERT_EQ(minijson::serialize(o), "{\"x\":null}");
}

// 尾部空白容忍；多元素对象数组（ROOMS_LIST 形态）
MINI_TEST(trailing_ws_and_multi_items) {
    Object p;
    MINI_ASSERT(minijson::parse("  {\"a\":1}  \t\n", p));
    MINI_ASSERT_EQ(p.get_num("a"), 1);
    Object r1, r2;
    r1.set_str("name", "lobby");
    r2.set_str("name", "dev");
    std::vector<Object> rooms;
    rooms.push_back(r1);
    rooms.push_back(r2);
    Object lst;
    lst.set_objs("content", rooms);
    Object q;
    MINI_ASSERT(minijson::parse(minijson::serialize(lst), q));
    std::vector<Object> got = q.get_objs("content");
    MINI_ASSERT_EQ(got.size(), 2u);
    MINI_ASSERT_EQ(got[1].get_str("name"), "dev");
}

// 转义 "/"（JSON 合法转义，常被漏实现）；键名含转义字符的序列化/解析
MINI_TEST(slash_escape_and_escaped_keys) {
    Object p;
    MINI_ASSERT(minijson::parse("{\"a\":\"x\\/y\"}", p));
    MINI_ASSERT_EQ(p.get_str("a"), "x/y");
    Object o;
    o.set_str("k\"q", "v\\a");
    Object q;
    MINI_ASSERT(minijson::parse(minijson::serialize(o), q));
    MINI_ASSERT_EQ(q.get_str("k\"q"), "v\\a");
}

// \uXXXX 的 UTF-8 边界：1/2/3/4 字节长度各验（append_utf8 四分支）
MINI_TEST(unicode_utf8_boundaries) {
    Object p;
    MINI_ASSERT(minijson::parse("{\"a\":\"\\u0041\\u00e9\\u07ff\\u0800\\uffff\\u0100\"}", p));
    std::string s = p.get_str("a");
    MINI_ASSERT_EQ(s.size(), (size_t)(1 + 2 + 2 + 3 + 3 + 2));  // 1+2+2+3+3+2 字节
}

// \u 截断/代理对截断（EOF 边界）——hex4 短缓冲与代理对分支
MINI_TEST(unicode_truncated_at_eof) {
    Object bad;
    MINI_ASSERT(!minijson::parse("{\"a\":\"\\u12\"}", bad));    // 4 hex 不足
    MINI_ASSERT(!minijson::parse("{\"a\":\"\\u\"}", bad));      // 0 hex
    MINI_ASSERT(!minijson::parse("{\"a\":\"\\ud83d\"}", bad));  // 高代理后 EOF（无低代理）
    MINI_ASSERT(!minijson::parse("{\"a\":\"\\ud83d\\u12\"}", bad));  // 低代理截断
}

// 值层面的畸形：布尔/裸字面量/未闭合/空元素（parse_value 各失败分支）
MINI_TEST(malformed_values) {
    Object p;
    MINI_ASSERT(!minijson::parse("{\"a\":true}", p));   // 不支持布尔（子集语义，显式拒绝）
    MINI_ASSERT(!minijson::parse("{\"a\":false}", p));
    MINI_ASSERT(!minijson::parse("{\"a\":tru}", p));
    MINI_ASSERT(!minijson::parse("{\"a\":nul}", p));
    MINI_ASSERT(!minijson::parse("{\"a\":}", p));       // 缺值
    MINI_ASSERT(!minijson::parse("{\"a\":[1,2}", p));   // 数组未闭合
    MINI_ASSERT(!minijson::parse("{\"a\":[,]}", p));    // 空元素
    MINI_ASSERT(!minijson::parse("{\"a\":\"b\",,\"c\":1}", p));  // 双逗号
    MINI_ASSERT(minijson::parse("{\"a\":[[1],{\"b\":2}]}", p));  // 混合嵌套合法
}

// 数字边界：+号非法、裸小数点、负号空、数字后垃圾；子集语义容忍空指数尾
MINI_TEST(number_edges) {
    Object p;
    MINI_ASSERT(!minijson::parse("{\"a\":+1}", p));   // JSON 无 + 前缀
    MINI_ASSERT(!minijson::parse("{\"a\":.5}", p));   // 裸小数点
    MINI_ASSERT(!minijson::parse("{\"a\":-}", p));    // 负号后无数字
    MINI_ASSERT(!minijson::parse("{\"a\":01x}", p));  // 数字后垃圾
    MINI_ASSERT(minijson::parse("{\"a\":1e,\"b\":-0,\"c\":1e-5}", p));
    MINI_ASSERT_EQ(p.get_num("a"), 1);   // 空指数尾容忍 → 整数部分（头注释子集语义）
    MINI_ASSERT_EQ(p.get_num("b"), 0);
    MINI_ASSERT_EQ(p.get_num("c"), 1);
}

// get_* 类型不匹配回默认（数字字段当字符串取等）
MINI_TEST(get_type_mismatch_defaults) {
    Object p;
    MINI_ASSERT(minijson::parse("{\"n\":5,\"s\":\"x\"}", p));
    MINI_ASSERT_EQ(p.get_str("n", "def"), "def");   // 数字!=字符串 → 默认
    MINI_ASSERT_EQ(p.get_num("s", 9), 9);           // 字符串!=数字 → 默认
    MINI_ASSERT_EQ(p.get_list("n").size(), 0u);
    MINI_ASSERT_EQ(p.get_objs("s").size(), 0u);
}

// get_list 只保留字符串元素（混型数组过滤语义）；深嵌套对象解析
MINI_TEST(list_filtering_and_deep_nesting) {
    Object p;
    MINI_ASSERT(minijson::parse("{\"l\":[\"a\",1,[\"x\"],{\"b\":2},\"c\"],"
                                "\"deep\":{\"o\":{\"o\":{\"o\":{\"v\":7}}}}}", p));
    std::vector<std::string> l = p.get_list("l");
    MINI_ASSERT_EQ(l.size(), 2u);
    MINI_ASSERT_EQ(l[0], "a");
    MINI_ASSERT_EQ(l[1], "c");
    MINI_ASSERT_EQ(p.get_objs("l").size(), 1u);  // 混型数组里的对象元素按 obj 取出
    Object deep;
    MINI_ASSERT(minijson::parse("{\"d\":{\"o\":{\"o\":{\"o\":{\"v\":7}}}}}", deep));  // 4 层嵌套
}

// 重复键覆盖（后值赢）；顶层非对象一律拒绝
MINI_TEST(null_dupkey_toplevel) {
    Object p;
    MINI_ASSERT(minijson::parse("{\"a\":null,\"a\":2,\"a\":\"z\"}", p));
    MINI_ASSERT_EQ(p.get_str("a"), "z");  // set 语义：重复键覆盖，最后一个赢
    Object b;
    MINI_ASSERT(!minijson::parse("null", b));  // 顶层必须是对象
    MINI_ASSERT(!minijson::parse("\"str\"", b));
    MINI_ASSERT(!minijson::parse("42", b));
}
