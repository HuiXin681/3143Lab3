cmake_minimum_required(VERSION 4.3)
project(3143Lab3)

set(CMAKE_CXX_STANDARD 20)

find_package(MPI REQUIRED)

add_executable(3143Lab3 main.cpp
        task1.c)

target_link_libraries(3143Lab3 PUBLIC MPI::MPI_CXX)