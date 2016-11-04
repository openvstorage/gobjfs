include_directories(${PROJECT_SOURCE_DIR}/thirdparty/msgpack/include)

ADD_EXECUTABLE(ShmemClient ShmemClient.cpp)
TARGET_LINK_LIBRARIES(ShmemClient rora_shared )

ADD_EXECUTABLE(ShmemServer ShmemServer.cpp)
TARGET_LINK_LIBRARIES(ShmemServer rora_shared )

