
ADD_EXECUTABLE(BenchNetClient
  BenchNetClient.cpp
  )

TARGET_LINK_LIBRARIES(BenchNetClient
  gobjfs_client_shared
  gobjfs_shared)

ADD_EXECUTABLE(BenchNetServer
  BenchNetServer.cpp
  )

TARGET_LINK_LIBRARIES(BenchNetServer
  gobjfs_server_shared
  gobjfs_shared)


