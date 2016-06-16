INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/thirdparty/xio/include)
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/thirdparty/msgpack/include)

SET(LIBRARY_LIST
    aio
    msgpack
    xio
    boost_thread 
    glog 
    gflags 
    unwind
    pthread
	bz2 pthread rt z)

ADD_EXECUTABLE(SyncClient SyncClient.cpp)
TARGET_LINK_LIBRARIES(SyncClient gobjfs_client ${LIBRARY_LIST})

ADD_EXECUTABLE(MultiAsyncClient MultiAsyncClient.cpp)
TARGET_LINK_LIBRARIES(MultiAsyncClient gobjfs_client ${LIBRARY_LIST})

ADD_EXECUTABLE(AsyncClient AsyncClient.cpp)
TARGET_LINK_LIBRARIES(AsyncClient gobjfs_client ${LIBRARY_LIST})
