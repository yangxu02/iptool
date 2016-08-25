CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
PROJECT(ip-index-tools)
SET(LIBRARY_OUTPUT_PATH  ${PROJECT_SOURCE_DIR})
SET(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR})
ADD_DEFINITIONS(
    -g
    -std=c++0x
    -O2
    #-O0
    -Wall
    -Werror
    -fpermissive
    -Wno-write-strings
    -Wno-literal-suffix
    -Wno-ignored-qualifiers
    -Wno-unused-local-typedefs
    -Wno-invalid-offsetof
    -Wno-unused-parameter
    -Wno-unused-function
    -Wno-narrowing
    -D_GLIBCXX_HAVE_GTHR_DEFAULT
    -Wno-strict-aliasing
    -Wno-parentheses
    -Wno-unused-but-set-variable
    -Wno-conversion
    -Wno-deprecated
)
SET(WORK_ROOT_DIR ${PROJECT_SOURCE_DIR})
INCLUDE_DIRECTORIES(
    /usr/include
    ${WORK_ROOT_DIR}/third/redis/include 
    )
LINK_DIRECTORIES(
    /usr/lib/x86_64-linux-gnu
    ${WORK_ROOT_DIR}/third/redis/lib
    )

AUX_SOURCE_DIRECTORY(. SRC_LIST)

ADD_EXECUTABLE(ipdb ipdb.cpp)
TARGET_LINK_LIBRARIES(ipdb
    libboost_system.a
    libboost_program_options.a
    libredisclient.a
    libgflags.a
    pthread
    )

ADD_EXECUTABLE(ipverify ip_verify.cpp ip_index_helper.cpp)
TARGET_LINK_LIBRARIES(ipverify
    libboost_system.a
    libboost_program_options.a
    pthread
    )

