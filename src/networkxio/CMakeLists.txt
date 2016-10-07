include_directories(${PROJECT_SOURCE_DIR}/thirdparty/msgpack/include)

###################
# gobjfs_client 
###################

set(CLIENT_SOURCES 
  gobjfs_client.cpp
  NetworkXioCommon.cpp
  NetworkXioClient.cpp
  )

set(LIBRARY_LIST
    xio
    boost_thread 
    boost_log 
	bz2 pthread rt z)

# create shared and static library from same set of sources
add_library(gobjfs_client OBJECT ${CLIENT_SOURCES})
set_property(TARGET ${gobjfs_client} PROPERTY POSITION_INDEPENDENT_CODE 1)

add_library(gobjfs_client_shared SHARED $<TARGET_OBJECTS:gobjfs_client>)
set_target_properties(gobjfs_client_shared PROPERTIES OUTPUT_NAME gobjfs_client)
target_link_libraries(gobjfs_client_shared ${LIBRARY_LIST})

add_library(gobjfs_client_static STATIC $<TARGET_OBJECTS:gobjfs_client>)
set_target_properties(gobjfs_client_static PROPERTIES OUTPUT_NAME gobjfs_client)
target_link_libraries(gobjfs_client_static ${LIBRARY_LIST})

# install library under install_dir/lib
install(TARGETS gobjfs_client_shared
  LIBRARY DESTINATION lib)
install(TARGETS gobjfs_client_static 
  ARCHIVE DESTINATION lib)

###################
# gobjfs_server 
###################

set(SERVER_SOURCES 
  NetworkXioIOHandler.cpp
  NetworkXioServer.cpp
  PortalThreadData.cpp
  NetworkXioCommon.cpp
  gobjfs_server.cpp
  )

# create shared and static library from same set of sources
add_library(gobjfs_server OBJECT ${SERVER_SOURCES})
set_property(TARGET ${gobjfs_server} PROPERTY POSITION_INDEPENDENT_CODE 1)

add_library(gobjfs_server_shared SHARED $<TARGET_OBJECTS:gobjfs_server>)
set_target_properties(gobjfs_server_shared PROPERTIES OUTPUT_NAME gobjfs_server)
target_link_libraries(gobjfs_server_shared ${LIBRARY_LIST})

add_library(gobjfs_server_static STATIC $<TARGET_OBJECTS:gobjfs_server>)
set_target_properties(gobjfs_server_static PROPERTIES OUTPUT_NAME gobjfs_server)
target_link_libraries(gobjfs_server_static ${LIBRARY_LIST})

# install library under install_dir/lib
install(TARGETS gobjfs_server_shared
  LIBRARY DESTINATION lib)
install(TARGETS gobjfs_server_static 
  ARCHIVE DESTINATION lib)

add_subdirectory(test)
