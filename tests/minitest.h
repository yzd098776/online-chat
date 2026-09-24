// tests/minitest.h —— 极简断言测试框架（自研，~120 行）
//
// 为什么不用 GoogleTest：本机未装，拉取 googletest 需联网下载/安装（按项目约定不擅自
// 联网装依赖）。本框架提供与 gtest 同构的最小能力：套件/用例静态注册、断言宏、
// 按套件过滤运行、汇总通过/失败——保证 `ctest` 一条命令跑全部用例。
// 若之后引入 GoogleTest，测试源只需改宏名（ASSERT_TRUE→EXPECT_TRUE 等），结构不变。
//
// 用法：
//   MINI_SUITE(frame)
//   MINI_TEST(roundtrip) { MINI_ASSERT(1 + 1 == 2); }
//   // 二进制：./chat_tests 跑全部；./chat_tests frame 只跑 frame 套件
#ifndef CHAT_MINITEST_H_
#define CHAT_MINITEST_H_

#include <cstdio>
#include <functional>
#include <string>
#include <vector>

namespace minitest {

struct TestCase {
    std::string suite;
    std::string name;
    std::function<void()> fn;
};

inline std::vector<TestCase>& registry() {
    static std::vector<TestCase> r;
    return r;
}

struct Registrar {
    Registrar(const char* suite, const char* name, std::function<void()> fn) {
        TestCase t;
        t.suite = suite;
        t.name = name;
        t.fn = fn;
        registry().push_back(t);
    }
};

inline int& failures() {
    static int f = 0;
    return f;
}

inline void report_fail(const char* file, int line, const std::string& expr) {
    ++failures();
    std::printf("    ✗ %s:%d  ASSERT(%s)\n", file, line, expr.c_str());
}

// argv 过滤后运行；返回非 0 = 有用例失败（供 ctest 判定）
inline int run(int argc, char** argv) {
    std::string filter = (argc > 1) ? argv[1] : "";
    int total = 0, passed = 0;
    std::string last_suite;
    for (size_t i = 0; i < registry().size(); ++i) {
        TestCase& t = registry()[i];
        if (!filter.empty() && t.suite != filter) continue;
        if (t.suite != last_suite) {
            std::printf("[%s]\n", t.suite.c_str());
            last_suite = t.suite;
        }
        ++total;
        int before = failures();
        t.fn();
        if (failures() == before) {
            ++passed;
            std::printf("  ✓ %s\n", t.name.c_str());
        } else {
            std::printf("  ✗ %s\n", t.name.c_str());
        }
    }
    std::printf("---- %d/%d 通过, %d 失败 ----\n", passed, total, failures());
    return failures() ? 1 : 0;
}

}  // namespace minitest

#define MINI_SUITE(suite_name) static const char* kMiniSuite = #suite_name;

#define MINI_TEST(test_name)                                                          \
    static void mini_test_##test_name();                                              \
    static ::minitest::Registrar mini_reg_##test_name(                                \
        kMiniSuite, #test_name, mini_test_##test_name);                               \
    static void mini_test_##test_name()

#define MINI_ASSERT(expr)                                                             \
    do {                                                                              \
        if (!(expr)) ::minitest::report_fail(__FILE__, __LINE__, #expr);              \
    } while (0)

#define MINI_ASSERT_EQ(a, b)                                                          \
    do {                                                                              \
        if (!((a) == (b))) ::minitest::report_fail(__FILE__, __LINE__, #a " == " #b); \
    } while (0)

#endif  // CHAT_MINITEST_H_
