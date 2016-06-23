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

SET(SERVER_SOURCES 
  NetworkXioIOHandler.cpp
  NetworkXioServer.cpp
  gobjfs_server.cpp
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

# install both libraries under install_dir/lib
install(TARGETS gobjfs_client gobjfs_server 
  LIBRARY DESTINATION lib)

ADD_SUBDIRECTORY(test)
