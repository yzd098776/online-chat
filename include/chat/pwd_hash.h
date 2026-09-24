// pwd_hash.h —— 口令哈希：salt(16 字节随机) + PBKDF2-HMAC-SHA256（迭代 100000 次）
//
// 存储格式（users 表两列）：
//   salt     = hex(16 字节随机盐)          → 32 个 hex 字符
//   pwd_hash = hex(PBKDF2-HMAC-SHA256(pwd, salt, 100000)[0:32]) → 64 个 hex 字符
//   = 派生密钥 K（挑战-应答的 HMAC 密钥，Q7）。登录校验：proof=HMAC(K,nonce) 恒定时间
//   比对（challenge_proof）；verify() 保留为「盐+哈希重算比对」原语（单测/对照用）。
//
// ==================== 三个实现档位（编译期选择） ====================
// ① 有 <openssl/evp.h>（如本项目 Dockerfile 装了 libssl-dev）：
//    直接用 OpenSSL PKCS5_PBKDF2_HMAC(EVP_sha256)，生产路径，链接 -lcrypto。
// ② 默认（无 OpenSSL 头文件——本项目裸机验证机只有 libcrypto.so.3 运行库、没有 dev 头）：
//    本文件内置 SHA-256 + HMAC-SHA256 + PBKDF2 的标准实现（RFC 2898 / RFC 8018 / FIPS 180-4）。
//    输出与 ① / Python hashlib.pbkdf2_hmac("sha256", ...) 逐比特一致
//    （tools/test_v5_smoke.py 注册用户后回读 users 表，用 Python 重算比对——三个平台同一向量）。
//    为什么不直接退化到「salt + SHA-256 多次迭代」：有标准 KDF 可用时退化到自定义哈希
//    是无谓降级；真正的降级路径在 ③，仅作为教学演示开关保留。
// ③ 编译加 -DCHAT_PWD_DEGRADED=1：教学环境降级方案（salt‖pwd 的 SHA-256 迭代 100000 轮）。
//    「教学环境降级方案及其缺陷」见下方 degraded_hash() 大段注释——面试必问，缺陷写全。
//
// 编译：
//   ② 裸机：g++ -std=c++11 -Wall -pthread chat_server_v5.cpp -o chat_server_v5
//            /usr/lib/x86_64-linux-gnu/libsqlite3.so.0   （续行，同一命令）
//   ① Docker/装有 libssl-dev：g++ ... -lsqlite3 -lcrypto
#ifndef PWD_HASH_H_
#define PWD_HASH_H_

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#if defined(__has_include) && __has_include(<openssl/evp.h>)
#define PWD_HASH_HAVE_OPENSSL 1
#include <openssl/evp.h>   // 档位①：OpenSSL PBKDF2
#include <openssl/rand.h>  // RAND_bytes：随机源第二级（/dev/urandom 之后）
#endif

namespace pwd_hash {

static const int kSaltBytes = 16;     // 需求：16 字节随机盐
static const int kHashBytes = 32;     // SHA-256 摘要长度
static const int kIters = 100000;     // 需求：迭代 100000 次

// ---------------- hex 工具 ----------------

inline std::string to_hex(const unsigned char* data, size_t n) {
    static const char* d = "0123456789abcdef";
    std::string out;
    out.reserve(n * 2);
    for (size_t i = 0; i < n; ++i) {
        out += d[(data[i] >> 4) & 0xF];
        out += d[data[i] & 0xF];
    }
    return out;
}

inline bool from_hex(const std::string& hex, std::vector<unsigned char>& out) {
    if (hex.size() % 2) return false;
    out.clear();
    out.reserve(hex.size() / 2);
    for (size_t i = 0; i < hex.size(); i += 2) {
        int hi = -1, lo = -1;
        for (int k = 0; k < 16; ++k) {
            if (hex[i] == "0123456789abcdef"[k]) hi = k;
            if (hex[i + 1] == "0123456789abcdef"[k]) lo = k;
        }
        if (hi < 0 || lo < 0) return false;
        out.push_back((unsigned char)((hi << 4) | lo));
    }
    return true;
}

// 恒定时间比较（防计时旁路枚举口令/哈希前缀）
inline bool constant_time_eq(const std::string& a, const std::string& b) {
    if (a.size() != b.size()) return false;
    unsigned char diff = 0;
    for (size_t i = 0; i < a.size(); ++i) diff |= (unsigned char)(a[i] ^ b[i]);
    return diff == 0;
}

// ---------------- 档位②③ 公用底座：SHA-256（FIPS 180-4） ----------------
// 档位①用不到本节；② 需要 HMAC-SHA256 + PBKDF2；③ 只要裸 SHA-256。
namespace sha256 {

struct Ctx {
    unsigned int h[8];
    unsigned long long bitlen;
    unsigned char block[64];
    size_t blocklen;
};

inline unsigned int rotr(unsigned int x, int n) { return (x >> n) | (x << (32 - n)); }

inline void init(Ctx& c) {
    static const unsigned int iv[8] = {
        0x6a09e667u, 0xbb67ae85u, 0x3c6ef372u, 0xa54ff53au,
        0x510e527fu, 0x9b05688cu, 0x1f83d9abu, 0x5be0cd19u};
    std::memcpy(c.h, iv, sizeof(iv));
    c.bitlen = 0;
    c.blocklen = 0;
}

inline void compress(Ctx& c, const unsigned char* p) {
    static const unsigned int k[64] = {
        0x428a2f98u, 0x71374491u, 0xb5c0fbcfu, 0xe9b5dba5u, 0x3956c25bu, 0x59f111f1u,
        0x923f82a4u, 0xab1c5ed5u, 0xd807aa98u, 0x12835b01u, 0x243185beu, 0x550c7dc3u,
        0x72be5d74u, 0x80deb1feu, 0x9bdc06a7u, 0xc19bf174u, 0xe49b69c1u, 0xefbe4786u,
        0x0fc19dc6u, 0x240ca1ccu, 0x2de92c6fu, 0x4a7484aau, 0x5cb0a9dcu, 0x76f988dau,
        0x983e5152u, 0xa831c66du, 0xb00327c8u, 0xbf597fc7u, 0xc6e00bf3u, 0xd5a79147u,
        0x06ca6351u, 0x14292967u, 0x27b70a85u, 0x2e1b2138u, 0x4d2c6dfcu, 0x53380d13u,
        0x650a7354u, 0x766a0abbu, 0x81c2c92eu, 0x92722c85u, 0xa2bfe8a1u, 0xa81a664bu,
        0xc24b8b70u, 0xc76c51a3u, 0xd192e819u, 0xd6990624u, 0xf40e3585u, 0x106aa070u,
        0x19a4c116u, 0x1e376c08u, 0x2748774cu, 0x34b0bcb5u, 0x391c0cb3u, 0x4ed8aa4au,
        0x5b9cca4fu, 0x682e6ff3u, 0x748f82eeu, 0x78a5636fu, 0x84c87814u, 0x8cc70208u,
        0x90befffau, 0xa4506cebu, 0xbef9a3f7u, 0xc67178f2u};
    unsigned int w[64];
    for (int i = 0; i < 16; ++i)
        w[i] = ((unsigned int)p[i * 4] << 24) | ((unsigned int)p[i * 4 + 1] << 16) |
               ((unsigned int)p[i * 4 + 2] << 8) | (unsigned int)p[i * 4 + 3];
    for (int i = 16; i < 64; ++i) {
        unsigned int s0 = rotr(w[i - 15], 7) ^ rotr(w[i - 15], 18) ^ (w[i - 15] >> 3);
        unsigned int s1 = rotr(w[i - 2], 17) ^ rotr(w[i - 2], 19) ^ (w[i - 2] >> 10);
        w[i] = w[i - 16] + s0 + w[i - 7] + s1;
    }
    unsigned int a = c.h[0], b = c.h[1], cc = c.h[2], d = c.h[3];
    unsigned int e = c.h[4], f = c.h[5], g = c.h[6], h = c.h[7];
    for (int i = 0; i < 64; ++i) {
        unsigned int s1 = rotr(e, 6) ^ rotr(e, 11) ^ rotr(e, 25);
        unsigned int ch = (e & f) ^ (~e & g);
        unsigned int t1 = h + s1 + ch + k[i] + w[i];
        unsigned int s0 = rotr(a, 2) ^ rotr(a, 13) ^ rotr(a, 22);
        unsigned int maj = (a & b) ^ (a & cc) ^ (b & cc);
        unsigned int t2 = s0 + maj;
        h = g; g = f; f = e; e = d + t1;
        d = cc; cc = b; b = a; a = t1 + t2;
    }
    c.h[0] += a; c.h[1] += b; c.h[2] += cc; c.h[3] += d;
    c.h[4] += e; c.h[5] += f; c.h[6] += g; c.h[7] += h;
}

inline void update(Ctx& c, const unsigned char* data, size_t n) {
    for (size_t i = 0; i < n; ++i) {
        c.block[c.blocklen++] = data[i];
        if (c.blocklen == 64) {
            compress(c, c.block);
            c.bitlen += 512;
            c.blocklen = 0;
        }
    }
}

inline void final(Ctx& c, unsigned char out[32]) {
    unsigned long long total = c.bitlen + (unsigned long long)c.blocklen * 8;
    unsigned char pad = 0x80;
    update(c, &pad, 1);
    unsigned char zero = 0;
    while (c.blocklen != 56) update(c, &zero, 1);
    unsigned char lenb[8];
    for (int i = 0; i < 8; ++i) lenb[i] = (unsigned char)((total >> (56 - i * 8)) & 0xFF);
    update(c, lenb, 8);
    for (int i = 0; i < 8; ++i) {
        out[i * 4] = (unsigned char)(c.h[i] >> 24);
        out[i * 4 + 1] = (unsigned char)(c.h[i] >> 16);
        out[i * 4 + 2] = (unsigned char)(c.h[i] >> 8);
        out[i * 4 + 3] = (unsigned char)(c.h[i]);
    }
}

inline void digest(const unsigned char* data, size_t n, unsigned char out[32]) {
    Ctx c;
    init(c);
    update(c, data, n);
    final(c, out);
}

}  // namespace sha256

// ---------------- 档位②③ 公用底座：HMAC-SHA256（RFC 2104） ----------------
inline void hmac_sha256(const unsigned char* key, size_t keylen,
                        const unsigned char* msg, size_t msglen,
                        unsigned char out[32]) {
    unsigned char k[64];
    std::memset(k, 0, sizeof(k));
    if (keylen > 64) {
        sha256::digest(key, keylen, k);  // 超长密钥先摘要
    } else {
        std::memcpy(k, key, keylen);
    }
    unsigned char ipad[64], opad[64];
    for (int i = 0; i < 64; ++i) {
        ipad[i] = (unsigned char)(k[i] ^ 0x36);
        opad[i] = (unsigned char)(k[i] ^ 0x5c);
    }
    unsigned char inner[32];
    sha256::Ctx c;
    sha256::init(c);
    sha256::update(c, ipad, 64);
    sha256::update(c, msg, msglen);
    sha256::final(c, inner);
    sha256::init(c);
    sha256::update(c, opad, 64);
    sha256::update(c, inner, 32);
    sha256::final(c, out);
}

// ---------------- 档位②：PBKDF2-HMAC-SHA256（RFC 2898 §5.2） ----------------
// 与 OpenSSL PKCS5_PBKDF2_HMAC(EVP_sha256) / Python hashlib.pbkdf2_hmac("sha256",...)
// 输出一致；差异仅在实现载体，不在算法。
#ifndef PWD_HASH_DEGRADED
inline void pbkdf2_hmac_sha256(const unsigned char* pwd, size_t pwdlen,
                               const unsigned char* salt, size_t saltlen,
                               int iters, unsigned char* out, size_t outlen) {
    unsigned int block_index = 1;  // 块号从 1 开始（RFC 2898）
    size_t done = 0;
    while (done < outlen) {
        // U1 = HMAC(pwd, salt || INT(block_index))
        unsigned char u[32];
        std::vector<unsigned char> msg(salt, salt + saltlen);
        msg.push_back((unsigned char)((block_index >> 24) & 0xFF));
        msg.push_back((unsigned char)((block_index >> 16) & 0xFF));
        msg.push_back((unsigned char)((block_index >> 8) & 0xFF));
        msg.push_back((unsigned char)(block_index & 0xFF));
        hmac_sha256(pwd, pwdlen, msg.empty() ? NULL : &msg[0], msg.size(), u);
        unsigned char t[32];
        std::memcpy(t, u, 32);
        // U2..Uc 迭代：Ui = HMAC(pwd, U(i-1))；T ^= Ui
        for (int i = 1; i < iters; ++i) {
            hmac_sha256(pwd, pwdlen, u, 32, u);
            for (int j = 0; j < 32; ++j) t[j] ^= u[j];
        }
        size_t take = outlen - done < 32 ? outlen - done : 32;
        std::memcpy(out + done, t, take);
        done += take;
        ++block_index;
    }
}
#endif  // !PWD_HASH_DEGRADED

// ---------------- 档位③：教学环境降级方案 salt‖pwd 的 SHA-256 迭代 ----------------
//
// 【教学环境降级方案及其缺陷】（要求写明；面试高频追问点）
//
// 方案：h0 = SHA-256(salt ‖ pwd)；hi = SHA-256(h(i-1) ‖ salt ‖ pwd)，共 100000 轮，取 h_N。
// 在既无 OpenSSL、又裁掉内置 PBKDF2 的极限裁剪环境（单片机教学实验等）下，
// 它仍然「比明文/单次哈希强」：有随机盐抗彩虹表、有高迭代数抬高单次爆破成本。
//
// 缺陷（为什么生产绝不能用，为什么它只是降级）：
//   a. 非标准 KDF，无互操作性：没有 RFC 测试向量，无法证明实现正确；Python/Java/任何
//      标准库都没有一行对应物（hashlib.pbkdf2_hmac 一行搞定的事，这里要自己再写一遍并
//      自证），跨语言重写登录校验时极易写错且测不出来。
//   b. 无 HMAC 的密钥化结构：PBKDF2 每轮都过 HMAC 的内外层密钥调度，本方案只是把
//      salt‖pwd 反复粘在摘要输入尾部——批量预计算（一次攻 N 个口令）的工程摊销空间
//      比 PBKDF2 大；salt 只「混在输入里」而不是「参与每轮密钥材料」。
//   c. 自研迭代链的安全性未经公开分析：「迭代 10 万次 ≈ 暴力成本 ×10 万」这个直觉对
//      PBKDF2 近似成立（有大量公开分析撑着），对自定义链不成立——GPU/ASIC 上的
//      时间-记忆权衡（TMTO）、多口令批处理攻击的加速比是未知数。
//   d. Merkle–Damgård 长度扩展：SHA-256(h ‖ m) 的链式结构对「已知 h 求扩展消息摘要」
//      不设防。本方案里长度扩展不直接伪造口令，但同一函数一旦被复用做消息认证
//      （H(h, msg) 式签名），就是可直接利用的注入面——降级组件的最大风险是被顺手复用。
//   e. 输出 32 字节直接当「口令等价物」存库：一旦库被拖走，自研链的离线爆破工具链要
//      自己写（攻击者反而要多花一周）——「攻击门槛高」不是安全属性，PBKDF2/scrypt/
//      argon2id 的抗爆破强度来自【公开分析过的成本参数】，不是来自实现小众。
//
// 结论：仅限教学演示/断网实验。生产密码存储请用 ①/②（PBKDF2-HMAC-SHA256 ≥ 100000 轮）
// 或 argon2id / scrypt / bcrypt（带内存硬度参数，抗 GPU 更优）。
#ifdef PWD_HASH_DEGRADED
inline void degraded_hash(const unsigned char* pwd, size_t pwdlen,
                          const unsigned char* salt, size_t saltlen,
                          int iters, unsigned char out[32]) {
    std::vector<unsigned char> first(salt, salt + saltlen);
    first.insert(first.end(), pwd, pwd + pwdlen);
    sha256::digest(first.empty() ? NULL : &first[0], first.size(), out);
    std::vector<unsigned char> msg;
    msg.reserve(32 + saltlen + pwdlen);
    for (int i = 1; i < iters; ++i) {
        msg.assign(out, out + 32);
        msg.insert(msg.end(), salt, salt + saltlen);
        msg.insert(msg.end(), pwd, pwd + pwdlen);
        sha256::digest(&msg[0], msg.size(), out);
    }
}
#endif  // PWD_HASH_DEGRADED

// ---------------- 对外 API ----------------

// n 字节 CSPRNG → hex。【只用密码学随机源】：/dev/urandom → OpenSSL RAND_bytes，
// 两级都拿不到就 abort——salt / Token / 挑战 nonce 一旦可预测（rand()/time() 种子），
// 口令哈希与会话凭证整体形同虚设，所以这里【禁止】任何非密码学降级路径
// （历史版本的 std::rand 兜底已删除：可预测的 Token 等于没有 Token）。
// Token（Q4：随机 32 字节 + 过期时间）用 random_hex(32)；盐用 random_hex(16)；
// 登录挑战 nonce（Q7）用 random_hex(32)。
inline std::string random_hex(size_t n) {
    std::vector<unsigned char> buf(n, 0);
    FILE* f = std::fopen("/dev/urandom", "rb");
    bool ok = false;
    if (f) {
        ok = (std::fread(buf.empty() ? NULL : &buf[0], 1, n, f) == n);
        std::fclose(f);
    }
#if defined(PWD_HASH_HAVE_OPENSSL)
    if (!ok) {
        // 第二级：OpenSSL CSPRNG（FIPS 186-4 / ANSI X9.31 DRBG）
        ok = (RAND_bytes(buf.empty() ? NULL : &buf[0], (int)n) == 1);
    }
#endif
    if (!ok) {
        // 无 CSPRNG = 无法安全签发 salt/Token/nonce：快速失败，绝不静默降级
        std::fprintf(stderr, "FATAL: 无法获取密码学随机源（/dev/urandom、RAND_bytes 均失败）\n");
        std::abort();
    }
    return to_hex(buf.empty() ? NULL : &buf[0], n);
}

// 16 字节随机盐 → hex（需求：salt 16 字节随机）
inline std::string random_salt_hex() { return random_hex(kSaltBytes); }

// pwd + salt(hex) → pwd_hash(hex)。盐是【解码后的原始字节】参与运算，不是 hex 字符串本身。
inline std::string hash_password(const std::string& pwd, const std::string& salt_hex) {
    std::vector<unsigned char> salt;
    if (!from_hex(salt_hex, salt) || salt.empty()) return "";
    unsigned char out[kHashBytes];
#if defined(PWD_HASH_HAVE_OPENSSL) && !defined(PWD_HASH_DEGRADED)
    // 档位①：OpenSSL 生产路径
    if (PKCS5_PBKDF2_HMAC(pwd.data(), (int)pwd.size(), salt.data(), (int)salt.size(),
                          kIters, EVP_sha256(), kHashBytes, out) != 1)
        return "";
#elif defined(PWD_HASH_DEGRADED)
    degraded_hash((const unsigned char*)pwd.data(), pwd.size(), salt.empty() ? NULL : &salt[0],
                  salt.size(), kIters, out);  // 档位③：教学降级（缺陷见上方注释）
#else
    // 档位②：内置标准 PBKDF2-HMAC-SHA256
    pbkdf2_hmac_sha256((const unsigned char*)pwd.data(), pwd.size(),
                       salt.empty() ? NULL : &salt[0], salt.size(), kIters, out, kHashBytes);
#endif
    return to_hex(out, kHashBytes);
}

// 校验：重算 + 恒定时间比较
inline bool verify(const std::string& pwd, const std::string& salt_hex,
                   const std::string& hash_hex) {
    return constant_time_eq(hash_password(pwd, salt_hex), hash_hex);
}

// 挑战-应答证明（Q7）：proof = HMAC-SHA256(K, op ‖ user ‖ nonce) → hex。
//   key_hex   = 派生密钥 K（PBKDF2 结果；服务端出库时先经 apply_pepper 还原）
//   op        = "LOGIN" / "REGISTER"（帧类型名——证明绑定协议语境）
//   user      = 用户名（证明绑定身份——与 takeChallenge 的 user 校验双保险）
//   nonce_hex = 服务器挑战里的 32 字节一次性随机数（hex 64 字符）
// 消息拼接无歧义：op 互不为前缀、nonce 定长 32B 在尾部。绑定 op/user 是纵深防御：
// 「每连接单槽 + nonce 一次性」已安全，这里把跨协议/跨用户证明混淆的面彻底关死。
// 登录帧只带 proof：口令与 K 都不进帧；服务端重算后【恒定时间比较】。Python 对应
// hmac.new(K, (op+user).encode()+bytes.fromhex(nonce), hashlib.sha256)。
inline std::string challenge_proof(const std::string& key_hex, const char* op,
                                   const std::string& user, const std::string& nonce_hex) {
    std::vector<unsigned char> key, nonce;
    if (!from_hex(key_hex, key) || key.size() != kHashBytes) return "";
    if (!from_hex(nonce_hex, nonce) || nonce.size() != kHashBytes) return "";
    std::string msg = std::string(op) + user;
    msg.append((const char*)&nonce[0], nonce.size());
    unsigned char out[kHashBytes];
    hmac_sha256(&key[0], key.size(), (const unsigned char*)msg.data(), msg.size(), out);
    return to_hex(out, kHashBytes);
}

// pepper 指纹（防「换错 pepper 开旧库 → 全员密码错误」的静默故障）：
// pepper_id = hex(HMAC-SHA256(pepper, "pepper-id")) 前 8 位，随库存 meta['pepper_id']，
// 启动比对不上就明确报「pepper 与库不匹配」。8 hex = 32bit：撞概率 2^-32，误报可忽略；
// 只做标识【不做验证】（反向恢复 pepper 不可行，HMAC 单向）。
inline std::string pepper_id(const std::string& pepper_hex) {
    std::vector<unsigned char> pepper;
    if (!from_hex(pepper_hex, pepper) || pepper.size() != kHashBytes) return "";
    unsigned char out[kHashBytes];
    hmac_sha256(&pepper[0], pepper.size(), (const unsigned char*)"pepper-id", 9, out);
    return to_hex(out, kHashBytes).substr(0, 8);
}

// pepper 盲化（Q7② 反 pass-the-hash）：stored = K ⊕ HMAC-SHA256(pepper, user‖salt)。
// K 就是登录凭据（HMAC(K,nonce) 的密钥）——库里若直接存 K，拖库即可直接登录（pass-the-hash）。
// 盲化后库里只有 stored；还原 K 需要 pepper（pepper 在 secrets/pepper.key，不进 DB/日志/git）。
// XOR 是对合：blind 与 unblind 是【同一个函数】——入库前套一次，登录出库后再套一次即回 K。
// 边界诚实：pepper 与 DB 同机 = 「防拖库、不防拖整机」；真解是 PAKE/HSM（README 已知限制）。
inline std::string apply_pepper(const std::string& key_hex, const std::string& pepper_hex,
                                const std::string& user, const std::string& salt_hex) {
    std::vector<unsigned char> key, pepper;
    if (!from_hex(key_hex, key) || key.size() != kHashBytes) return "";
    if (!from_hex(pepper_hex, pepper) || pepper.size() != kHashBytes) return "";
    std::string msg = user + salt_hex;  // salt 定长 32 hex 后缀 → 拼接无歧义
    unsigned char mask[kHashBytes];
    hmac_sha256(&pepper[0], pepper.size(), (const unsigned char*)msg.data(), msg.size(), mask);
    unsigned char out[kHashBytes];
    for (int i = 0; i < kHashBytes; ++i) out[i] = (unsigned char)(key[i] ^ mask[i]);
    return to_hex(out, kHashBytes);
}

// 当前编译走的是哪个档位（启动横幅打印，方便验收确认）
inline const char* backend_name() {
#ifdef PWD_HASH_DEGRADED
    return "DEGRADED: salt||pwd SHA-256 x100000（教学降级，勿用于生产，见 pwd_hash.h）";
#elif defined(PWD_HASH_HAVE_OPENSSL)
    return "OpenSSL PKCS5_PBKDF2_HMAC(SHA256)";
#else
    return "builtin PBKDF2-HMAC-SHA256（RFC 2898，与 OpenSSL/Python 逐比特一致）";
#endif
}

}  // namespace pwd_hash

#endif  // PWD_HASH_H_
