// tests/test_auth.cpp —— 凭据安全单测：CSPRNG / PBKDF2 向量 / HMAC 挑战证明 / Token 哈希表
//
// 为什么要有这套：Q7 挑战-应答的密码学原语必须对拍【公开标准向量】（不是自证）——
// PBKDF2-HMAC-SHA256 对 RFC 7914 §11 / 通用测试向量，HMAC-SHA256 对 RFC 4231 case 1，
// 与 Python hashlib 逐比特一致（tools/test_v5_smoke.py T16 是端到端播种一致性）。
#include <string>
#include <vector>

#include "chat/pwd_hash.h"
#include "chat/token_book.h"
#include "minitest.h"

MINI_SUITE(auth)

#ifndef PWD_HASH_DEGRADED  // 降级档位无 PBKDF2（教学开关），向量测试不适用

// PBKDF2-HMAC-SHA256 公开测试向量（password/salt，c=1/2/4096 → 32B）。
// 同 Python：hashlib.pbkdf2_hmac("sha256", b"password", b"salt", c, 32)
MINI_TEST(pbkdf2_vectors) {
    unsigned char out[32];
    const unsigned char* pwd = (const unsigned char*)"password";
    const unsigned char* salt = (const unsigned char*)"salt";
    pwd_hash::pbkdf2_hmac_sha256(pwd, 8, salt, 4, 1, out, 32);
    MINI_ASSERT_EQ(pwd_hash::to_hex(out, 32),
                   std::string("120fb6cffcf8b32c43e7225256c4f837a86548c92ccc35480805987cb70be17b"));
    pwd_hash::pbkdf2_hmac_sha256(pwd, 8, salt, 4, 2, out, 32);
    MINI_ASSERT_EQ(pwd_hash::to_hex(out, 32),
                   std::string("ae4d0c95af6b46d32d0adff928f06dd02a303f8ef3c251dfd6e2d85a95474c43"));
    pwd_hash::pbkdf2_hmac_sha256(pwd, 8, salt, 4, 4096, out, 32);
    MINI_ASSERT_EQ(pwd_hash::to_hex(out, 32),
                   std::string("c5e478d59288c841aa530db6845c4c8d962893a001ce4e11a4963873aa98134a"));
}

// 多块输出（dkLen>32 走 block_index=2）：RFC 7914 §11 首向量前 40 字节
MINI_TEST(pbkdf2_multiblock) {
    unsigned char out[40];
    pwd_hash::pbkdf2_hmac_sha256((const unsigned char*)"passwd", 6,
                                 (const unsigned char*)"salt", 4, 1, out, 40);
    MINI_ASSERT_EQ(pwd_hash::to_hex(out, 40),
                   std::string("55ac046e56e3089fec1691c22544b605f94185216dde0465e68b9d57c20dacbc"
                               "49ca9cccf179b645"));
}

// hash_password 封装：salt 参与运算的是【解码后的字节】；坏 salt 返回空串；verify 正/反例
MINI_TEST(hash_password_sad_paths) {
    std::string salt = pwd_hash::random_salt_hex();
    std::string h = pwd_hash::hash_password("pw", salt);
    MINI_ASSERT_EQ(h.size(), 64u);  // 32 字节 → 64 hex
    MINI_ASSERT(pwd_hash::verify("pw", salt, h));
    MINI_ASSERT(!pwd_hash::verify("bad", salt, h));
    MINI_ASSERT(!pwd_hash::verify("pw", pwd_hash::random_salt_hex(), h));  // 换盐必失配
    MINI_ASSERT(pwd_hash::hash_password("pw", "zz").empty());      // 非法 hex
    MINI_ASSERT(pwd_hash::hash_password("pw", "").empty());        // 空盐
}

// HMAC-SHA256：RFC 4231 case 1（key=0x0b×20，msg="Hi There"）
MINI_TEST(hmac_rfc4231_case1) {
    unsigned char key[20];
    for (int i = 0; i < 20; ++i) key[i] = 0x0b;
    unsigned char out[32];
    pwd_hash::hmac_sha256(key, 20, (const unsigned char*)"Hi There", 8, out);
    MINI_ASSERT_EQ(pwd_hash::to_hex(out, 32),
                   std::string("b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"));
}

// SHA-256 裸向量："abc"
MINI_TEST(sha256_vector) {
    unsigned char out[32];
    pwd_hash::sha256::digest((const unsigned char*)"abc", 3, out);
    MINI_ASSERT_EQ(pwd_hash::to_hex(out, 32),
                   std::string("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"));
}

#endif  // !PWD_HASH_DEGRADED

// 挑战证明 Q7：proof = HMAC-SHA256(K, op‖user‖nonce)。向量 = Python
// hmac.new(bytes.fromhex("11"*32), b"LOGIN"+b"alice"+bytes.fromhex("22"*32),
//          hashlib.sha256).hexdigest()
// 另验证【nonce/身份/语境绑定】：换任一输入 proof 必须变（重放/混淆不过恒定时间比较）
MINI_TEST(challenge_proof_nonce_binding) {
    std::string key_hex(64, '1');   // K = 0x11×32
    std::string nonce_hex(64, '2'); // nonce = 0x22×32
    std::string proof = pwd_hash::challenge_proof(key_hex, "LOGIN", "alice", nonce_hex);
    MINI_ASSERT_EQ(proof,
                   std::string("99f07fc9ddc1bb11cfa35c19ccaa6bdfdaafaecf26d7bc786a45483bcf8b8ace"));
    std::string other_nonce(64, '3');
    MINI_ASSERT(!pwd_hash::constant_time_eq(
        proof, pwd_hash::challenge_proof(key_hex, "LOGIN", "alice", other_nonce)));  // nonce 绑定
    MINI_ASSERT(!pwd_hash::constant_time_eq(
        proof, pwd_hash::challenge_proof(key_hex, "LOGIN", "bob", nonce_hex)));      // 身份绑定
    MINI_ASSERT(!pwd_hash::constant_time_eq(
        proof, pwd_hash::challenge_proof(key_hex, "REGISTER", "alice", nonce_hex))); // 语境绑定
    MINI_ASSERT(pwd_hash::challenge_proof("zz", "LOGIN", "alice", nonce_hex).empty());  // 坏 key
    MINI_ASSERT(pwd_hash::challenge_proof(key_hex, "LOGIN", "alice", "").empty());      // 空 nonce
}

// pepper 盲化（Q7② 反 pass-the-hash）：XOR 对合（盲化两次 = 原值）、换 pepper/换用户失配
MINI_TEST(pepper_blind_is_involution) {
    std::string k_hex(64, 'a');    // K = 0xaa×32
    std::string pepper(64, 'b');   // pepper = 0xbb×32
    std::string salt(64, 'c');     // salt hex
    std::string stored = pwd_hash::apply_pepper(k_hex, pepper, "alice", salt);
    MINI_ASSERT_EQ(stored.size(), 64u);
    MINI_ASSERT(stored != k_hex);                                   // 盲化确实打乱
    MINI_ASSERT_EQ(pwd_hash::apply_pepper(stored, pepper, "alice", salt), k_hex);  // 对合还原
    // 换 pepper / 换用户名 / 换 salt 都解不出 K（拖库者三者缺一不可）
    MINI_ASSERT(pwd_hash::apply_pepper(stored, std::string(64, 'd'), "alice", salt) != k_hex);
    MINI_ASSERT(pwd_hash::apply_pepper(stored, pepper, "bob", salt) != k_hex);
    MINI_ASSERT(pwd_hash::apply_pepper(stored, pepper, "alice", std::string(64, 'e')) != k_hex);
    MINI_ASSERT(pwd_hash::apply_pepper("zz", pepper, "alice", salt).empty());   // 坏 key hex
    MINI_ASSERT(pwd_hash::apply_pepper(k_hex, "zz", "alice", salt).empty());    // 坏 pepper hex
}

// 恒定时间比较：长度不同即 false；逐字节差一即 false
MINI_TEST(constant_time_eq) {
    MINI_ASSERT(pwd_hash::constant_time_eq("abc", "abc"));
    MINI_ASSERT(!pwd_hash::constant_time_eq("abc", "abd"));
    MINI_ASSERT(!pwd_hash::constant_time_eq("abc", "abcd"));
    MINI_ASSERT(!pwd_hash::constant_time_eq("", "a"));
    MINI_ASSERT(pwd_hash::constant_time_eq("", ""));
}

// CSPRNG：长度/hex 字符集/两次调用不重复（rand()/time() 种子会撞）
MINI_TEST(random_hex_csprng_shape) {
    std::string a = pwd_hash::random_hex(32);
    std::string b = pwd_hash::random_hex(32);
    MINI_ASSERT_EQ(a.size(), 64u);
    MINI_ASSERT_EQ(b.size(), 64u);
    MINI_ASSERT(a != b);
    for (size_t i = 0; i < a.size(); ++i) {
        char ch = a[i];
        MINI_ASSERT((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f'));
    }
    MINI_ASSERT_EQ(pwd_hash::random_salt_hex().size(), 32u);  // 16 字节盐
}

// TokenBook：内存只存 SHA256(token)（行为验证：能验、不能拿错 token 冒）、过期即失效
MINI_TEST(token_book_issue_validate) {
    chat::TokenBook book(3600);
    long long exp = 0;
    std::string tok = book.issue("alice", &exp);
    MINI_ASSERT_EQ(tok.size(), 64u);
    MINI_ASSERT(exp > 0);
    std::string user;
    MINI_ASSERT(book.validate(tok, user));
    MINI_ASSERT_EQ(user, "alice");
    MINI_ASSERT(!book.validate("", user));                          // 空 token
    std::string wrong = tok;
    wrong[0] = (wrong[0] == '0') ? '1' : '0';
    MINI_ASSERT(!book.validate(wrong, user));                       // 伪造 token
    MINI_ASSERT_EQ(book.size(), 1u);
}

MINI_TEST(token_book_expiry) {
    chat::TokenBook book(-10);  // 已过期的 TTL：签发即过期
    std::string tok = book.issue("bob", NULL);
    std::string user;
    MINI_ASSERT(!book.validate(tok, user));  // 过期即删
    MINI_ASSERT_EQ(book.size(), 0u);
}
