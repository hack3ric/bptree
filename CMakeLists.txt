cmake_minimum_required(VERSION 3.10)
project(bptree
        VERSION 1.0
        DESCRIPTION "Concurrent B+ Tree Implementation")

set(CMAKE_C_STANDARD 17)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
add_custom_target(copy-compile-commands ALL
    ${CMAKE_COMMAND} -E copy_if_different
        ${CMAKE_BINARY_DIR}/compile_commands.json
        ${CMAKE_CURRENT_LIST_DIR}
)

find_package(Threads REQUIRED)

include_directories(
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

# libbptree
add_library(libbptree STATIC
    src/bptree.c
)
target_link_libraries(libbptree PUBLIC Threads::Threads)

# libbptree_mira_local
add_library(libbptree_mira_local STATIC
    src/bptree_mira.cpp
    src/bptree_mira_internal.cpp
)
target_compile_definitions(libbptree_mira_local PUBLIC BPTREE_MIRA_LOCAL)
target_link_libraries(libbptree_mira_local PUBLIC Threads::Threads)

# bptree_test
add_executable(bptree_test src/bptree_test.c)
target_link_libraries(bptree_test PRIVATE libbptree)

# bptree_test_mira_local
add_executable(bptree_test_mira_local src/bptree_mira_test.cpp)
target_link_libraries(bptree_test_mira_local
    PRIVATE
        libbptree_mira_local
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/../Mira/runtime/build/libcommon2/lib/libcommon2.a
)

# TODO: add Pentathlon targets
