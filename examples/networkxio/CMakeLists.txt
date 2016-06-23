
SET(LIBRARY_LIST
    aio
    xio
    boost_thread 
    glog 
    gflags 
    unwind
	bz2 pthread rt z)

ADD_EXECUTABLE(SyncClient SyncClient.cpp)
TARGET_LINK_LIBRARIES(SyncClient gobjfs_client_shared ${LIBRARY_LIST})

ADD_EXECUTABLE(MultiAsyncClient MultiAsyncClient.cpp)
TARGET_LINK_LIBRARIES(MultiAsyncClient gobjfs_client_shared ${LIBRARY_LIST})

ADD_EXECUTABLE(AsyncClient AsyncClient.cpp)
TARGET_LINK_LIBRARIES(AsyncClient gobjfs_client_shared ${LIBRARY_LIST})


ADD_EXECUTABLE(NetworkServer NetworkServer.cpp)
TARGET_LINK_LIBRARIES(NetworkServer gobjfs_server_shared gobjfs_shared ${LIBRARY_LIST})

# how to add target-specific compile-time macros
# target_compile_definitions(NetworkServer PRIVATE DEBUG_TRACE=1)
