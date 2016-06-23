
ADD_EXECUTABLE(ObjfsTester
  MempoolTest.cpp
  TestIOExecFile.cpp
  TestIOExecAPI.cpp
  TestNonAligned.cpp
  TestMain.cpp
  )

TARGET_LINK_LIBRARIES(ObjfsTester
  gobjfs_shared
  glog gtest
  pthread rt z)

ADD_TEST(NAME objfstest COMMAND ObjfsTester)
