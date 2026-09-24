// include/chat/log.h —— 轻量分级日志（自研，无第三方依赖）
//
// 特性：DEBUG/INFO/WARN/ERROR 四级；时间戳（毫秒）+ 线程 id + 文件名:行号；
//       控制台与文件双通道、各自级别过滤；文件按天切分（chat_YYYY-MM-DD.log）。
// 用法：LOG_INFO() << "登录成功 user=" << name;      // 流式拼接，惰性求值
// 约定：业务代码【禁止】直接 printf/cout 打调试信息，一律走本模块（面试考点）。
// 实现见 src/log.cpp；两端合计 < 200 行。
#ifndef CHAT_LOG_H_
#define CHAT_LOG_H_

#include <sstream>
#include <string>

namespace chatlog {

enum Level { L_DEBUG = 0, L_INFO = 1, L_WARN = 2, L_ERROR = 3 };

// 初始化：console/file 各自的最低输出级别；dir 为空则不落盘（仅控制台）。
// 重复调用 = 改配置（线程安全）。文件名固定前缀 "chat_"，按天切分。
void init(Level console_level, const std::string& dir, Level file_level);
void shutdown();

// "debug"/"info"/"warn"/"error"（大小写不敏感）；非法值返回 fallback
Level parse_level(const std::string& name, Level fallback);
const char* level_name(Level lv);

bool enabled(Level lv);
void write(Level lv, const char* file, int line, const std::string& msg);

// RAII 流式日志行：析构时一次性写出（级别不过滤则整行丢弃，<< 不求值浪费极小）
class Line {
public:
    Line(Level lv, const char* file, int line) : lv_(lv), file_(file), line_(line) {}
    ~Line() { write(lv_, file_, line_, ss_.str()); }
    template <typename T>
    Line& operator<<(const T& v) {
        ss_ << v;
        return *this;
    }

private:
    Line(const Line&);
    Line& operator=(const Line&);
    Level lv_;
    const char* file_;
    int line_;
    std::ostringstream ss_;
};

}  // namespace chatlog

#define LOG_DEBUG() chatlog::Line(chatlog::L_DEBUG, __FILE__, __LINE__)
#define LOG_INFO() chatlog::Line(chatlog::L_INFO, __FILE__, __LINE__)
#define LOG_WARN() chatlog::Line(chatlog::L_WARN, __FILE__, __LINE__)
#define LOG_ERROR() chatlog::Line(chatlog::L_ERROR, __FILE__, __LINE__)

#endif  // CHAT_LOG_H_
