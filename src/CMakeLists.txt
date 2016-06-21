SET(SOURCES
  Mempool.cpp
  gMempool.cpp
  gparse.cpp
  gcommon.cpp
  IOExecutor.cpp
  FilerJob.cpp
  gIOExecFile.cpp
  util/os_utils.cpp
  util/ShutdownNotifier.cpp
  util/CpuStats.cpp
  util/SemaphoreWrapper.cpp
  util/TimerNotifier.cpp
  )

SET(LIBRARY_LIST
  aio
	boost_thread boost_system boost_program_options
	glog gflags unwind
	bz2 pthread rt z)


ADD_LIBRARY(gobjfs SHARED ${SOURCES})
TARGET_LINK_LIBRARIES(gobjfs ${LIBRARY_LIST})
SET_TARGET_PROPERTIES(gobjfs PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/lib)

ADD_SUBDIRECTORY(util)
ADD_SUBDIRECTORY(test)
ADD_SUBDIRECTORY(networkxio)
