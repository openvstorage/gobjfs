
ADD_EXECUTABLE(RoraTester
  TestEdgeQueue.cpp
  TestMain.cpp
  )

TARGET_LINK_LIBRARIES(RoraTester
  rora_shared
  gtest)

ADD_TEST(NAME roratester COMMAND RoraTester)
