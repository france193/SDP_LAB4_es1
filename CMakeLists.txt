cmake_minimum_required(VERSION 3.10)
project(SDP_LAB4_es1 C)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -pthread")

add_executable(SDP_LAB4_es1 main.c)