ADD_LIBRARY(gobjfs_util SHARED
  os_utils.cpp
  ShutdownNotifier.cpp
  CpuStats.cpp
  SemaphoreWrapper.cpp
  TimerNotifier.cpp
  )

TARGET_LINK_LIBRARIES(gobjfs_util glog)

SET_TARGET_PROPERTIES(gobjfs_util PROPERTIES
  LIBRARY_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/lib)

ADD_SUBDIRECTORY(test)
