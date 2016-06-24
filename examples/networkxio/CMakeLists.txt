
ADD_EXECUTABLE(SyncClient SyncClient.cpp)
TARGET_LINK_LIBRARIES(SyncClient gobjfs_client_shared)

ADD_EXECUTABLE(MultiAsyncClient MultiAsyncClient.cpp)
TARGET_LINK_LIBRARIES(MultiAsyncClient gobjfs_client_shared)

ADD_EXECUTABLE(AsyncClient AsyncClient.cpp)
TARGET_LINK_LIBRARIES(AsyncClient gobjfs_client_shared)

ADD_EXECUTABLE(NetworkServer NetworkServer.cpp)
TARGET_LINK_LIBRARIES(NetworkServer gobjfs_server_shared gobjfs_shared)

# how to add target-specific compile-time macros
# target_compile_definitions(NetworkServer PRIVATE DEBUG_TRACE=1)
