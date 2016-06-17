
ADD_EXECUTABLE(ObjfsTester
  MempoolTest.cpp
  TestIOExecFile.cpp
  TestIOExecAPI.cpp
  TestMain.cpp
  )

TARGET_LINK_LIBRARIES(ObjfsTester
  gobjfs_server
  glog gtest
  pthread rt z)

ADD_TEST(NAME objfstest COMMAND ObjfsTester)
