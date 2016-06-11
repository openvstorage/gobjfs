INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/thirdparty/xio/include)
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/thirdparty/msgpack/include)

SET(SOURCES 
    volumedriver.cpp
    NetworkXioClient.cpp
    NetworkXioIOHandler.cpp
    NetworkXioServer.cpp
   )

SET(LIBRARY_LIST
    gobjfs
    gobjfs_util
    aio
    msgpack
    xio
    boost_thread 
    glog 
    gflags 
    unwind
    tcmalloc
    pthread
	bz2 pthread rt z)

ADD_LIBRARY(networkServer SHARED ${SOURCES})

SET_TARGET_PROPERTIES(networkServer 
  PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/lib)

ADD_EXECUTABLE(NetworkServer NetworkServer.cpp)

TARGET_LINK_LIBRARIES(NetworkServer networkServer ${LIBRARY_LIST})

ADD_EXECUTABLE(NetworkClient NetworkClient.cpp)

TARGET_LINK_LIBRARIES(NetworkClient networkServer ${LIBRARY_LIST})

