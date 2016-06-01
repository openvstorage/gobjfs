
ADD_EXECUTABLE(BenchIOExecFile
  BenchIOExecFile.cpp
  )

TARGET_LINK_LIBRARIES(BenchIOExecFile
  gobjfs
  glog gtest
  pthread rt z)

ADD_EXECUTABLE(BenchIORaw
  BenchIORaw.cpp
  )

TARGET_LINK_LIBRARIES(BenchIORaw
  gobjfs
  glog gtest
  pthread rt z)

