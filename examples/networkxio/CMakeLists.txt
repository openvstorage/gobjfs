
SET(LIBRARY_LIST
    aio
    msgpack
    xio
    boost_thread 
    glog 
    gflags 
    unwind
	bz2 pthread rt z)

ADD_EXECUTABLE(SyncClient SyncClient.cpp)
TARGET_LINK_LIBRARIES(SyncClient gobjfs_client ${LIBRARY_LIST})

ADD_EXECUTABLE(MultiAsyncClient MultiAsyncClient.cpp)
TARGET_LINK_LIBRARIES(MultiAsyncClient gobjfs_client ${LIBRARY_LIST})

ADD_EXECUTABLE(AsyncClient AsyncClient.cpp)
TARGET_LINK_LIBRARIES(AsyncClient gobjfs_client ${LIBRARY_LIST})

ADD_EXECUTABLE(NetworkServer NetworkServer.cpp)

TARGET_LINK_LIBRARIES(NetworkServer gobjfs_server gobjfs ${LIBRARY_LIST})
