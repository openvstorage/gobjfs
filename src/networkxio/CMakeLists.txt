INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/thirdparty/msgpack/include)

SET(CLIENT_SOURCES 
  gobjfs_client.cpp
  NetworkXioClient.cpp
  )

SET(LIBRARY_LIST
    gobjfs_client
    xio
    boost_thread 
    glog 
    gflags 
    unwind
	bz2 pthread rt z)

ADD_LIBRARY(gobjfs_client SHARED ${CLIENT_SOURCES})
SET_TARGET_PROPERTIES(gobjfs_client 
  PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/lib)

SET(SERVER_SOURCES 
  NetworkXioIOHandler.cpp
  NetworkXioServer.cpp
  )

SET(LIBRARY_LIST
    gobjfs_server
    aio
    xio
    boost_thread 
    glog 
    gflags 
    unwind
	bz2 pthread rt z)

ADD_LIBRARY(gobjfs_server SHARED ${SERVER_SOURCES})
SET_TARGET_PROPERTIES(gobjfs_server 
  PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/lib)
