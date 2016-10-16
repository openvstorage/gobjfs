set(SOURCES
  Mempool.cpp
  gMempool.cpp
  gparse.cpp
  gcommon.cpp
  IOExecutor.cpp
  FilerJob.cpp
  gIOExecFile.cpp
  util/os_utils.cpp
  util/CpuStats.cpp
  util/SemaphoreWrapper.cpp
  util/TimerNotifier.cpp
  )

set(LIBRARY_LIST
  aio
	boost_thread boost_system boost_program_options
  boost_log  boost_log_setup
	bz2 pthread rt z)

add_library(gobjfs OBJECT ${SOURCES})
set_property(TARGET ${gobjfs} PROPERTY POSITION_INDEPENDENT_CODE 1)

add_library(gobjfs_shared SHARED $<TARGET_OBJECTS:gobjfs>)
set_target_properties(gobjfs_shared PROPERTIES OUTPUT_NAME gobjfs)
target_link_libraries(gobjfs_shared ${LIBRARY_LIST})

add_library(gobjfs_static STATIC $<TARGET_OBJECTS:gobjfs>)
set_target_properties(gobjfs_static PROPERTIES OUTPUT_NAME gobjfs)
target_link_libraries(gobjfs_static ${LIBRARY_LIST})

# install the library under install_dir/lib
install(TARGETS gobjfs_shared LIBRARY DESTINATION lib)
install(TARGETS gobjfs_static ARCHIVE DESTINATION lib)

add_subdirectory(util)
add_subdirectory(test)
add_subdirectory(networkxio)
