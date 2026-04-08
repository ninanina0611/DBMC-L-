cmake_minimum_required(VERSION 3.10)
project(lightdb_fileops LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(${CMAKE_SOURCE_DIR}/include)

add_executable(lightdb_demo src/main.cpp src/DemoTest.cpp src/FileManager.cpp)
