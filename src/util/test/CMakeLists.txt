
ADD_EXECUTABLE(UtilTester
  StatsTest.cpp
  ShutdownNotifierTest.cpp
  SemaphoreWrapperTest.cpp
  OSUtilsTest.cpp
  TestMain.cpp)

TARGET_LINK_LIBRARIES(UtilTester 
  gobjfs
	boost_thread boost_system boost_program_options
	glog gtest 
	pthread rt z)

ADD_TEST(NAME util COMMAND UtilTester)
