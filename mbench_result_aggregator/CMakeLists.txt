cmake_minimum_required(VERSION 3.0.2)
project(result_aggregator)

# Set CMake modules path
list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

find_package(Boost 1.58 REQUIRED COMPONENTS system)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -mcx16")

find_package(Threads)

set(SRCS main.cpp sqlite3.h sqlite3.c)
add_executable(resaggr ${SRCS})
target_include_directories(resaggr PUBLIC ${Boost_INCLUDE_DIRS})
target_link_libraries(resaggr ${Boost_LIBRARIES})
target_link_libraries(resaggr ${CMAKE_THREAD_LIBS_INIT})
target_link_libraries(resaggr dl)

