
ADD_EXECUTABLE(BenchNetClient
  BenchNetClient.cpp
  )

TARGET_LINK_LIBRARIES(BenchNetClient
  gobjfs_client_shared
  gobjfs_shared)

