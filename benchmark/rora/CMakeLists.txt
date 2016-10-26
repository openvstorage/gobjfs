include_directories(${PROJECT_SOURCE_DIR}/thirdparty/msgpack/include)

ADD_EXECUTABLE(EdgeProcess EdgeProcess.cpp)
TARGET_LINK_LIBRARIES(EdgeProcess rora_shared)

ADD_EXECUTABLE(RoraGateway RoraGateway.cpp)
TARGET_LINK_LIBRARIES(RoraGateway rora_shared)

