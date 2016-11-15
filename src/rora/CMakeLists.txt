include_directories(${PROJECT_SOURCE_DIR}/thirdparty/msgpack/include)

set(LIBRARY_LIST
  gobjfs_client_shared
  boost_system boost_program_options
  boost_log
	pthread rt z)

########################################
# RORA GATEWAY LIBRARY
########################################

set(RORA_SOURCES 
  EdgeQueue.cpp
  ASDQueue.cpp
  GatewayProtocol.cpp
  AdminQueue.cpp
  RoraGateway.cpp
  )

# create shared and static library from same set of sources
add_library(rora OBJECT ${RORA_SOURCES})
set_property(TARGET ${rora} PROPERTY POSITION_INDEPENDENT_CODE 1)

add_library(rora_shared SHARED $<TARGET_OBJECTS:rora>)
set_target_properties(rora_shared PROPERTIES OUTPUT_NAME rora)
target_link_libraries(rora_shared ${LIBRARY_LIST})

add_library(rora_static STATIC $<TARGET_OBJECTS:rora>)
set_target_properties(rora_static PROPERTIES OUTPUT_NAME rora)
target_link_libraries(rora_static ${LIBRARY_LIST})

# install library under install_dir/lib
install(TARGETS rora_shared
  LIBRARY DESTINATION lib)
install(TARGETS rora_static 
  ARCHIVE DESTINATION lib)

########################################
#  RORA CLIENT LIBRARY
########################################

set(CLIENT_SOURCES
  EdgeQueue.cpp
  ASDQueue.cpp
  GatewayProtocol.cpp
  AdminQueue.cpp
  GatewayClient.cpp
  )

# create shared and static library from same set of sources
add_library(rora_client OBJECT ${CLIENT_SOURCES})
set_property(TARGET ${rora_client} PROPERTY POSITION_INDEPENDENT_CODE 1)

add_library(rora_client_shared SHARED $<TARGET_OBJECTS:rora_client>)
set_target_properties(rora_client_shared PROPERTIES OUTPUT_NAME rora_client)
target_link_libraries(rora_client_shared ${LIBRARY_LIST})

add_library(rora_client_static STATIC $<TARGET_OBJECTS:rora_client>)
set_target_properties(rora_client_static PROPERTIES OUTPUT_NAME rora_client)
target_link_libraries(rora_client_static ${LIBRARY_LIST})

# install library under install_dir/lib
install(TARGETS rora_client_shared
  LIBRARY DESTINATION lib)
install(TARGETS rora_client_static 
  ARCHIVE DESTINATION lib)

########################################
#  Add subdir
########################################

add_subdirectory(test)
