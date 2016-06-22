
ADD_EXECUTABLE(XioTester
  TestNetworkServer.cpp
  TestMain.cpp
  TestCompileByC.c
  )

TARGET_LINK_LIBRARIES(XioTester
  gobjfs_client
  gobjfs_server
  gobjfs
  xio
  glog gtest
  pthread rt z)

ADD_TEST(NAME xiotester COMMAND XioTester)
