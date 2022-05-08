cmake_minimum_required(VERSION 3.20)
project(ngramIndexcpp)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_FLAGS "-march=native")

link_libraries(rocksdb zstd lz4 bz2 snappy z pthread dl mysqlcppconn8)


add_executable(buildIndex buildIndex.cpp csv.hpp NcharIndex.cpp NcharIndex.h)

add_executable(queryIndex queryIndex.cpp csv.hpp NcharIndex.cpp NcharIndex.h)

add_executable(indexingtest indexingtest.cpp csv.hpp NcharIndex.cpp NcharIndex.h)

add_executable(indexingtestM  NcharIndexM.cpp NcharIndexM.h indexingtestM.cpp)

target_include_directories(indexingtestM BEFORE PRIVATE /home/ligonliu/boost/include)
target_link_directories(indexingtestM BEFORE PRIVATE /home/ligonliu/boost/lib)