// src/log.cpp —— log.h 的实现：格式化 + 双通道 + 按天切分（与 log.h 合计 < 200 行）
#include "chat/log.h"

#include <chrono>
#include <cstdio>
#include <ctime>
#include <fstream>
#include <mutex>
#include <thread>

namespace chatlog {
namespace {

struct State {
    std::mutex m;
    Level console_level;
    Level file_level;
    std::string dir;          // 空 = 不落盘
    std::string open_date;    // 当前文件对应的 "YYYY-MM-DD"
    std::ofstream out;
    bool inited;
    State() : console_level(L_INFO), file_level(L_DEBUG), inited(false) {}
};

State& st() {
    static State s;
    return s;
}

// "YYYY-MM-DD HH:MM:SS.mmm"；date_out 收 "YYYY-MM-DD"（文件切分用）
std::string now_stamp(std::string* date_out) {
    using namespace std::chrono;
    system_clock::time_point tp = system_clock::now();
    std::time_t secs = system_clock::to_time_t(tp);
    int ms = (int)(duration_cast<milliseconds>(tp.time_since_epoch()).count() % 1000);
    std::tm tmv;
#if defined(_WIN32)
    localtime_s(&tmv, &secs);
#else
    localtime_r(&secs, &tmv);
#endif
    char buf[80];
    std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d.%03d",
                  tmv.tm_year + 1900, tmv.tm_mon + 1, tmv.tm_mday,
                  tmv.tm_hour, tmv.tm_min, tmv.tm_sec, ms);
    if (date_out) {
        char d[32];
        std::snprintf(d, sizeof(d), "%04d-%02d-%02d", tmv.tm_year + 1900, tmv.tm_mon + 1, tmv.tm_mday);
        *date_out = d;
    }
    return buf;
}

const char* base_name(const char* path) {
    const char* p = path;
    for (const char* q = path; *q; ++q)
        if (*q == '/' || *q == '\\') p = q + 1;
    return p;
}

// 按天切分：日期变了就关旧文件、开 chat_YYYY-MM-DD.log（调用方持锁）
void ensure_file_locked(const std::string& date) {
    State& s = st();
    if (s.dir.empty()) return;
    if (s.out.is_open() && s.open_date == date) return;
    if (s.out.is_open()) s.out.close();
    std::string path = s.dir + "/chat_" + date + ".log";
    s.out.open(path.c_str(), std::ios::app);
    if (!s.out) std::fprintf(stderr, "[log] 打开日志文件失败: %s\n", path.c_str());
    s.open_date = date;
}

}  // namespace

void init(Level console_level, const std::string& dir, Level file_level) {
    State& s = st();
    std::lock_guard<std::mutex> lk(s.m);
    s.console_level = console_level;
    s.file_level = file_level;
    s.dir = dir;
    s.inited = true;
    if (!dir.empty()) {
        std::string date;
        now_stamp(&date);
        ensure_file_locked(date);
    }
}

void shutdown() {
    State& s = st();
    std::lock_guard<std::mutex> lk(s.m);
    if (s.out.is_open()) s.out.close();
}

bool enabled(Level lv) {
    State& s = st();
    std::lock_guard<std::mutex> lk(s.m);
    return lv >= s.console_level || (!s.dir.empty() && lv >= s.file_level);
}

void write(Level lv, const char* file, int line, const std::string& msg) {
    State& s = st();
    std::lock_guard<std::mutex> lk(s.m);
    if (lv < s.console_level && (s.dir.empty() || lv < s.file_level)) return;
    std::string date;
    std::string stamp = now_stamp(&date);
    std::ostringstream tid;
    tid << std::this_thread::get_id();
    std::ostringstream row;
    row << "[" << stamp << "] [" << level_name(lv) << "] [t:" << tid.str() << "] ["
        << base_name(file) << ":" << line << "] " << msg << "\n";
    const std::string& text = row.str();
    if (lv >= s.console_level) {
        std::fwrite(text.data(), 1, text.size(), lv >= L_ERROR ? stderr : stdout);
        std::fflush(lv >= L_ERROR ? stderr : stdout);
    }
    if (!s.dir.empty() && lv >= s.file_level) {
        ensure_file_locked(date);
        if (s.out.is_open()) s.out << text << std::flush;
    }
}

Level parse_level(const std::string& name, Level fallback) {
    std::string n;
    for (size_t i = 0; i < name.size(); ++i) n += (char)tolower((unsigned char)name[i]);
    if (n == "debug") return L_DEBUG;
    if (n == "info") return L_INFO;
    if (n == "warn" || n == "warning") return L_WARN;
    if (n == "error") return L_ERROR;
    return fallback;
}

const char* level_name(Level lv) {
    static const char* names[] = {"DEBUG", "INFO ", "WARN ", "ERROR"};
    return names[lv < L_DEBUG ? L_DEBUG : (lv > L_ERROR ? L_ERROR : lv)];
}

}  // namespace chatlog
