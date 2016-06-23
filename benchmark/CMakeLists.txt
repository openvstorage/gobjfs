
ADD_EXECUTABLE(BenchIOExecFile
  BenchIOExecFile.cpp
  )

TARGET_LINK_LIBRARIES(BenchIOExecFile
  gobjfs_shared
  glog gtest
  pthread rt z)

