include_directories(${PROJECT_SOURCE_DIR}/thirdparty/msgpack/include)

ADD_EXECUTABLE(sclient ShmemClient.cpp)
TARGET_LINK_LIBRARIES(sclient rora_shared )

ADD_EXECUTABLE(sserver ShmemServer.cpp)
TARGET_LINK_LIBRARIES(sserver rora_shared )

