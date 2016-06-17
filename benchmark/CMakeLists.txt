
ADD_EXECUTABLE(BenchIOExecFile
  BenchIOExecFile.cpp
  )

TARGET_LINK_LIBRARIES(BenchIOExecFile
  gobjfs
  glog gtest
  pthread rt z)

