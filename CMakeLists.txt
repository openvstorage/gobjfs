#
# See
# https://cmake.org/cmake/help/v3.0/manual/cmake-commands.7.html
# http://www.slideshare.net/DanielPfeifer1/cmake-48475415
#
CMAKE_MINIMUM_REQUIRED(VERSION 2.8 FATAL_ERROR)

# Project(<name> VERSION <version> LANGUAGES CXX)
PROJECT(GOBJFS C CXX ASM)

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/CMake" "${CMAKE_MODULE_PATH}")

# Include the thread, address, memory sanitizer cmake files
include(FindUBSan)
include(FindTSan)
include(FindASan)
include(FindMSan)

# so that "make" will echo the command line for easy debugging
SET(CMAKE_VERBOSE_MAKEFILE on)

# Specify command-line options
# OPTION(<option_variable> "help string describing option" [initial_value])

SET (CMAKE_BUILD_TYPE Release)
# CMAKE_BUILD_TYPE can be Release/Debug/RelWithDebInfo/MinSizeRel
# For Sanitizer builds, set it to ASan/MSan/TSan.  
# See the CMake directory for sanitizer cmake files
if (NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(STATUS "No build type selected, default to Debug")
  set(CMAKE_BUILD_TYPE "Debug")
endif()

# turns on colors in generated Makefile
SET(CMAKE_COLOR_MAKEFILE)

# overwriting the source and binary directories with the current ones
# this is useful for other projects reusing this project as a subcomponent
SET(CMAKE_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR})
SET(CMAKE_BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR})

# this will prohibit in-source-builds
IF(${CMAKE_BINARY_DIR} STREQUAL ${CMAKE_SOURCE_DIR})
  MESSAGE(STATUS "In-source-builds are not allowed")
  MESSAGE(STATUS "Clean your source directory (e.g. delete the CMakeCache.txt file)")
  MESSAGE(FATAL_ERROR "Please create a separate build directory and call CMake again")
ENDIF(${CMAKE_BINARY_DIR} STREQUAL ${CMAKE_SOURCE_DIR})
#
# all headers specified wrt the top-level src dir
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/src)
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR})
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/include)

LINK_DIRECTORIES(${PROJECT_SOURCE_DIR}/lib)
LINK_DIRECTORIES(${PROJECT_SOURCE_DIR}/thirdparty/msgpack/lib)
LINK_DIRECTORIES(${PROJECT_SOURCE_DIR}/thirdparty/xio/lib)

set(CUSTOM_LIBRARY_PATH ${PROJECT_SOURCE_DIR}/lib)

# set compiler to gcc
SET(CMAKE_CXX_COMPILER "/usr/bin/g++")
SET(CMAKE_C_COMPILER "/usr/bin/gcc")

SET (CXX_STANDARD "c++11")
SET (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=${CXX_STANDARD}")

ADD_COMPILE_OPTIONS(
  -Wall
  -Wextra
  -Wstrict-aliasing
  -Wno-unused-parameter
  -Wno-missing-field-initializers
  -Wnon-virtual-dtor
  -Wchar-subscripts
  -Wpointer-arith
  -Woverloaded-virtual
  -Wformat
  -Wformat-security
  -Werror=format-security
  -fstack-protector-all
  -fPIE
  -fpie
  -fPIC
  -fpic
  -pipe
  -fdata-sections
  -ffunction-sections
)

# Include cppcheck target.  must come after compiler flags defined
include(cppcheck)
include(clang-format)

# to enable tests, add this to top-level CMakeLists.txt
ENABLE_TESTING()
# then add this command in any subdir
# ADD_TEST(testname <executable> [arg1...])
# this will enable "make test" to run tests

# add subdirs which have CMakeLists files
# this should be done after all options are specified
# otherwise they are not visible in subdirs
# All third-pary libs should really use ExternalProject_Add
# but hack for now

ADD_SUBDIRECTORY(src)
ADD_SUBDIRECTORY(benchmark)
ADD_SUBDIRECTORY(examples)


# Set compiler options
# if (CMAKE_COMPILER_IS_GNUCXX)
#   target_compile_options(<executable>,
#	  PUBLIC -fno-elide-constructors
#    )
# endif()

# Set required compiler features
# TARGET_COMPILE_FEATURES(<executable>
#   PUBLIC
#     cxx_auto_type
#   PRIVATE
#     cxx_variadic_templates
#   )

# To generate a library called lib<libname>.so, add
# ADD_LIBRARY(<libname> STATIC/MODULE/SHARED/INTERFACE ${SOURCES})
# to set location for library installation
# INSTALL(TARGETS hello DESTINATION bin)

# to manually add sources, you can use
# set(SOURCES src/filename.cpp)

# specify sources in the executable
# ADD_EXECUTABLE(<executable_name> ${SOURCES})
# to specify library to use when linking an executable, use
# TARGET_LINK_LIBRARIES(<target_name> debug/optimized/general <dependent_library>)
# target command must be in same dir as corresponding add_library cmd

# for file IO
# FILE(DOWNLOAD...)

# String functions
# STRING(REGEX ..)

# Operations on a List
# LIST(GET/LENGTH/REVERSE/ etc)

# Execute a process commands
# EXECUTE_PROCESS(COMMAND INPUT_FILE TIMEOUT etc)

# if-then
# if (EXISTS ${ABC})
#    list(APPEND <listname> ${ABC})
# else()
#    message(FATAL_ERROR "not found")
# endif()
