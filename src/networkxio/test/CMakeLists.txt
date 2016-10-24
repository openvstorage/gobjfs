
ADD_EXECUTABLE(XioTester
  TestNetworkServer.cpp
  TestMultipleServers.cpp
  TestCompileByC.c
  TestStats.cpp
  TestGetenv.cpp
  TestMain.cpp
  )

TARGET_LINK_LIBRARIES(XioTester
  gobjfs_client_shared
  gobjfs_server_shared
  gtest)

ADD_TEST(NAME xiotester COMMAND XioTester)
