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
add_library(bptree STATIC
    src/bptree.c
)
target_link_libraries(bptree PUBLIC Threads::Threads)

# libbptree_mira_local
add_library(bptree_mira_local STATIC
    # src/bptree_mira.cpp
    lower_ref.o
    src/bptree_mira_internal.cpp
)
target_compile_definitions(bptree_mira_local PUBLIC BPTREE_MIRA_LOCAL)
target_link_libraries(bptree_mira_local PUBLIC Threads::Threads)

# libbptree_mira
add_library(bptree_mira STATIC
    # src/bptree_mira.cpp
    lower.o
    src/bptree_mira_internal.cpp
)
target_link_libraries(bptree_mira PUBLIC Threads::Threads)

# bptree_test
add_executable(bptree_test src/bptree_test.c)
target_link_libraries(bptree_test PRIVATE bptree)

# bptree_test_mira_local
add_executable(bptree_test_mira_local src/bptree_mira_test.cpp)
target_link_libraries(bptree_test_mira_local
    PRIVATE
        bptree_mira_local
)

# bptree_test_mira
add_executable(bptree_test_mira src/bptree_mira_test.cpp)
target_link_libraries(bptree_test_mira
    PRIVATE
        bptree_mira
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/../Mira/runtime/build/libcommon2/lib/libcommon2.a
        ibverbs
        nng
)

# TODO: add Pentathlon targets
