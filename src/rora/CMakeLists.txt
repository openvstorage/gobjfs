include_directories(${PROJECT_SOURCE_DIR}/thirdparty/msgpack/include)

set(SOURCES 
  EdgeQueue.cpp
  ASDQueue.cpp
  GatewayProtocol.cpp
  RoraGateway.cpp
  )

set(LIBRARY_LIST
  gobjfs_client_shared
  boost_system boost_program_options
  glog
	pthread rt z)

# create shared and static library from same set of sources
add_library(rora OBJECT ${SOURCES})
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

add_subdirectory(test)
