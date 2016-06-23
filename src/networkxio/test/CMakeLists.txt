
ADD_EXECUTABLE(XioTester
  TestNetworkServer.cpp
  TestMultipleServers.cpp
  TestCompileByC.c
  TestMain.cpp
  )

TARGET_LINK_LIBRARIES(XioTester
  gobjfs_client_shared
  gobjfs_server_shared
  gobjfs_shared
  xio
  glog gtest
  pthread rt z)

ADD_TEST(NAME xiotester COMMAND XioTester)
