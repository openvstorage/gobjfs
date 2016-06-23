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

# install the library under install_dir/lib
install(TARGETS gobjfs LIBRARY DESTINATION lib)

ADD_SUBDIRECTORY(util)
ADD_SUBDIRECTORY(test)
ADD_SUBDIRECTORY(networkxio)
