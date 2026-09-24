// include/chat/pepper.h —— pepper 加载/自举（口令派生密钥盲化，Q7② 反 pass-the-hash）
//
// pepper = 32 字节 CSPRNG，文件默认 secrets/pepper.key（--pepper 可改）：
//   · 首次启动自举生成，权限 0600（只属主可读）；secrets/ 整目录进 .gitignore
//   · 不进 DB、不进日志、不随备份走——库里只有 stored = K ⊕ HMAC(pepper, user‖salt)，
//     拖库拿不到 K（登录凭据），攻击退回离线爆破（每猜测 100k 次 PBKDF2）
//   · 诚实边界（README 已知限制）：pepper 与 DB 同机 = 防拖库、不防拖整机；
//     真解是 PAKE/HSM/密钥分管，与本项目声明范围一致
// 失败（读不了/写不了/格式坏）直接 abort：pepper 丢失/损坏 = 全库登录凭据作废，
// 快速失败好过静默用弱 pepper。
#ifndef CHAT_PEPPER_H_
#define CHAT_PEPPER_H_

#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

#include <sys/stat.h>
#ifndef _WIN32
#include <sys/types.h>
#else
#include <direct.h>
#endif

#include "chat/pwd_hash.h"

namespace chat {

inline void pepper_ensure_dir(const std::string& path) {
    // 建父目录（单层即可：默认 secrets/）；失败不致命——下面写文件会兜住
    size_t slash = path.find_last_of("/\\");
    if (slash == std::string::npos) return;
    std::string dir = path.substr(0, slash);
#ifdef _WIN32
    _mkdir(dir.c_str());
#else
    mkdir(dir.c_str(), 0700);
#endif
}

// 载入 pepper（hex 64）；文件【不存在】才 CSPRNG 自举（0600 落盘）。
// 任何异常 → abort，绝不静默降级（空 pepper = 盲化失效，且从日志看不出来）：
//   · 存在但读不了 / 坏长度 / 非法 hex → abort——尤其不能当「不存在」去重建：
//     那会用新 pepper【覆盖】旧文件，全库盲化瞬间解不开（「密码全错了」事故）；
//   · 自举写失败 → abort。
inline std::string load_or_create_pepper(const std::string& path) {
    if (path.empty()) {
        std::fprintf(stderr, "FATAL: pepper 路径为空（禁用盲化是不允许的）\n");
        std::abort();
    }
    struct stat st;
    bool exists = (::stat(path.c_str(), &st) == 0);  // stat 区分「不存在」与「存在但读不了」
    if (exists) {
        FILE* f = std::fopen(path.c_str(), "rb");
        if (!f) {
            std::fprintf(stderr, "FATAL: pepper 文件存在但无法读取（权限/IO）: %s\n", path.c_str());
            std::abort();
        }
        char hex[65] = {0};
        size_t n = std::fread(hex, 1, 64, f);
        std::fclose(f);
        if (n != 64) {
            std::fprintf(stderr, "FATAL: pepper 文件损坏（应为 64 hex）: %s\n", path.c_str());
            std::abort();
        }
        std::vector<unsigned char> raw;
        if (!pwd_hash::from_hex(std::string(hex, 64), raw) || raw.size() != 32) {
            std::fprintf(stderr, "FATAL: pepper 文件非法 hex: %s\n", path.c_str());
            std::abort();
        }
        return std::string(hex, 64);
    }
    // 自举：32B CSPRNG（random_hex 内部走 /dev/urandom / RAND_bytes，无弱源）
    std::string hex = pwd_hash::random_hex(32);
    pepper_ensure_dir(path);
    FILE* f = std::fopen(path.c_str(), "wb");
    if (!f) {
        std::fprintf(stderr, "FATAL: 无法写 pepper 文件: %s\n", path.c_str());
        std::abort();
    }
    std::fwrite(hex.data(), 1, 64, f);
    std::fclose(f);
#ifndef _WIN32
    ::chmod(path.c_str(), 0600);  // 会话凭证级权限：只属主可读
#endif
    return hex;
}

}  // namespace chat

#endif  // CHAT_PEPPER_H_
