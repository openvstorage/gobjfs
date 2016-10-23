
ADD_EXECUTABLE(RoraTester
  TestEdgeQueue.cpp
  TestGatewayProtocol.cpp
  TestMain.cpp
  )

TARGET_LINK_LIBRARIES(RoraTester
  rora_shared
  gtest)

ADD_TEST(NAME roratester COMMAND RoraTester)
