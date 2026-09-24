// sqlite3_api.h —— SQLite3 C API 最小声明（仅当系统没有 sqlite3.h 头文件时启用）
//
// 背景：本项目验证机只装了 libsqlite3-0（运行库），没有 libsqlite3-dev（头文件）。
// SQLite 的 C API 是冻结 ABI，这里只声明本项目用到的十几个入口，
// 与 <sqlite3.h> 完全兼容；链接时直接指向 libsqlite3.so.0 即可：
//
//   g++ -std=c++11 -Wall -pthread chat_server_v4.cpp -o chat_server_v4 /usr/lib/x86_64-linux-gnu/libsqlite3.so.0
//
// 若系统装有 libsqlite3-dev，chat_server_v4.cpp 会优先 #include <sqlite3.h>
// （见其中 __has_include 分支），此时正常用 -lsqlite3 链接，本文件不参与编译。
#ifndef SQLITE3_API_MIN_H_
#define SQLITE3_API_MIN_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef struct sqlite3 sqlite3;
typedef struct sqlite3_stmt sqlite3_stmt;
typedef void (*sqlite3_destructor_type)(void*);

// 结果码（只列用到的）
#define SQLITE_OK 0
#define SQLITE_ERROR 1
#define SQLITE_BUSY 5
#define SQLITE_ROW 100
#define SQLITE_DONE 101
// UNIQUE/NOT NULL 等约束违规（sqlite3_step 直接返回；users.username 唯一约束判重用）
#define SQLITE_CONSTRAINT 19

// 打开标志
#define SQLITE_OPEN_READWRITE 0x00000002
#define SQLITE_OPEN_CREATE 0x00000004

// 列类型
#define SQLITE_INTEGER 1
#define SQLITE_FLOAT 2
#define SQLITE_TEXT 3
#define SQLITE_NULL 5

// 绑定字符串的析构约定：SQLITE_TRANSIENT 让 SQLite 立即拷贝，调用方随后可释放
#define SQLITE_STATIC ((sqlite3_destructor_type)0)
#define SQLITE_TRANSIENT ((sqlite3_destructor_type)-1)

int sqlite3_open_v2(const char* filename, sqlite3** ppDb, int flags, const char* zVfs);
int sqlite3_close_v2(sqlite3* db);
const char* sqlite3_errmsg(sqlite3* db);
int sqlite3_exec(sqlite3* db, const char* sql,
                 int (*callback)(void*, int, char**, char**), void* arg, char** errmsg);
int sqlite3_changes(sqlite3* db);

int sqlite3_prepare_v2(sqlite3* db, const char* zSql, int nByte,
                       sqlite3_stmt** ppStmt, const char** pzTail);
int sqlite3_step(sqlite3_stmt* stmt);
int sqlite3_finalize(sqlite3_stmt* stmt);
int sqlite3_reset(sqlite3_stmt* stmt);

int sqlite3_bind_int64(sqlite3_stmt* stmt, int idx, long long value);
int sqlite3_bind_int(sqlite3_stmt* stmt, int idx, int value);
int sqlite3_bind_text(sqlite3_stmt* stmt, int idx, const char* value, int nByte,
                      sqlite3_destructor_type destructor);

long long sqlite3_column_int64(sqlite3_stmt* stmt, int iCol);
int sqlite3_column_int(sqlite3_stmt* stmt, int iCol);
const unsigned char* sqlite3_column_text(sqlite3_stmt* stmt, int iCol);
int sqlite3_column_type(sqlite3_stmt* stmt, int iCol);

#ifdef __cplusplus
}
#endif

#endif  // SQLITE3_API_MIN_H_
