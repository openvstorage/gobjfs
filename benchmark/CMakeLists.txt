
ADD_EXECUTABLE(BenchIOExecFile
  BenchIOExecFile.cpp
  )

TARGET_LINK_LIBRARIES(BenchIOExecFile
  gobjfs
  glog gtest
  pthread rt z)

ADD_EXECUTABLE(DummyServer
  DummyServer.cpp
  )

ADD_EXECUTABLE(DummyClient
  DummyClient.cpp
  )

TARGET_LINK_LIBRARIES(DummyClient
  glog
  pthread rt z)

