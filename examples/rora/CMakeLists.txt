include_directories(${PROJECT_SOURCE_DIR}/thirdparty/msgpack/include)

ADD_EXECUTABLE(EdgeClient EdgeClient.cpp)
TARGET_LINK_LIBRARIES(EdgeClient rora_shared)

# how to add target-specific compile-time macros
# target_compile_definitions(NetworkServer PRIVATE DEBUG_TRACE=1)
