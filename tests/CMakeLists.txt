# tests/CMakeLists.txt —— 单元测试（自研极简断言框架，见 minitest.h 头注释）
#
# 每个测试套件注册为一条 ctest 用例：ctest --test-dir build -R 'frame|minijson|...' 均可单跑。
add_executable(chat_tests
  test_main.cpp
  test_frame.cpp
  test_minijson.cpp
  test_protocol.cpp
  test_room_router.cpp
  test_seq_dedup.cpp
  test_rate_limiter.cpp
  test_word_filter.cpp
  test_auth.cpp
  test_database.cpp
)
target_link_libraries(chat_tests PRIVATE chat_core)

# 套件名 = 测试源里 MINI_SUITE() 的名字；一一对应注册
foreach(suite frame minijson protocol room_router seq_dedup rate_limiter word_filter auth database)
  add_test(NAME ${suite} COMMAND chat_tests ${suite})
endforeach()
