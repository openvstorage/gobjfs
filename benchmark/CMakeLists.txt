
ADD_EXECUTABLE(BenchIOExecFile
  BenchIOExecFile.cpp
  )

TARGET_LINK_LIBRARIES(BenchIOExecFile
  gobjfs_shared
  gtest
  pthread rt z)

add_subdirectory(networkxio)
add_subdirectory(rora)
add_subdirectory(shmq)
add_subdirectory(bip_deque)

