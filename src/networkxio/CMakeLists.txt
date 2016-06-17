INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/thirdparty/xio/include)
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/thirdparty/msgpack/include)

SET(CLIENT_SOURCES 
    gobjfs_client.cpp
    NetworkXioClient.cpp
   )

SET(LIBRARY_LIST
    gobjfs_server
    aio
    msgpack
    xio
    boost_thread 
    glog 
    gflags 
    unwind
    pthread
	bz2 pthread rt z)

ADD_LIBRARY(gobjfs_client SHARED ${CLIENT_SOURCES})
SET_TARGET_PROPERTIES(gobjfs_client 
  PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/lib)

ADD_EXECUTABLE(NetworkServer 
  NetworkServer.cpp 
  NetworkXioIOHandler.cpp
  NetworkXioServer.cpp)

TARGET_LINK_LIBRARIES(NetworkServer ${LIBRARY_LIST})
