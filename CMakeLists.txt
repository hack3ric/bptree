cmake_minimum_required(VERSION 3.10)
project(bptree
        VERSION 1.0
        DESCRIPTION "Concurrent B+ Tree Implementation")

set(CMAKE_C_STANDARD 17)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Threads REQUIRED)

# libbptree
add_library(libbptree STATIC
    src/bptree.c
)
set_target_properties(libbptree PROPERTIES
    OUTPUT_NAME bptree
    VERSION ${PROJECT_VERSION}
    SOVERSION ${PROJECT_VERSION_MAJOR}
)
target_include_directories(libbptree
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)
target_link_libraries(libbptree PUBLIC Threads::Threads)

# libbptree_mira
add_library(libbptree_mira STATIC
    src/bptree_mira.cpp
    src/bptree_mira_internal.cpp
)
set_target_properties(libbptree_mira PROPERTIES
    OUTPUT_NAME bptree_mira
    VERSION ${PROJECT_VERSION}
    SOVERSION ${PROJECT_VERSION_MAJOR}
)
target_include_directories(libbptree_mira
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)
target_link_libraries(libbptree_mira PUBLIC Threads::Threads)

# bptree_test
add_executable(bptree_test
    src/bptree_test.c
)
target_link_libraries(bptree_test
    PRIVATE
        libbptree
)

# bptree_test_mira
add_executable(bptree_test_mira
    src/bptree_mira_test.cpp
)
target_link_libraries(bptree_test_mira
    PRIVATE
        libbptree_mira
    PUBLIC
        # HACK: use absolute path for now, later use cmake target or relative path
        /home/hacker/Projects/Mira/runtime/build/libcommon2/lib/libcommon2.a
)

# TODO: add Pentathlon targets
