
ADD_EXECUTABLE(UtilTester
  StatsTest.cpp
  ShutdownNotifierTest.cpp
  SemaphoreWrapperTest.cpp
  OSUtilsTest.cpp
  TestMain.cpp)

TARGET_LINK_LIBRARIES(UtilTester 
  gobjfs_shared
	gtest)

ADD_TEST(NAME util COMMAND UtilTester)
