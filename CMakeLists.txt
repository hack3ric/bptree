cmake_minimum_required(VERSION 3.10)
project(bptree
        VERSION 1.0
        DESCRIPTION "Concurrent B+ Tree Implementation")

set(CMAKE_C_STANDARD 17)
set(CMAKE_C_STANDARD_REQUIRED ON)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find required packages
find_package(Threads REQUIRED)

# Create library target
add_library(libbptree STATIC
    src/bptree.cpp
)

# Set library properties
set_target_properties(libbptree PROPERTIES
    OUTPUT_NAME bptree
    VERSION ${PROJECT_VERSION}
    SOVERSION ${PROJECT_VERSION_MAJOR}
)

# Set include directories for the library
target_include_directories(libbptree
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

# Link pthread to the library
target_link_libraries(libbptree PUBLIC Threads::Threads)

# Create executable target
add_executable(bptree_test
    src/bptree_test.c
)

# Link the executable with our library
target_link_libraries(bptree_test
    PRIVATE
        libbptree
)

# TODO: add Mira and Pentathlon targets
