cmake_minimum_required(VERSION 3.17)
project(pdb)

set(CMAKE_CXX_STANDARD 17)

add_executable(pdb main.cpp log_writer.cpp log_writer.h input_types.h input.h concurrency_control.cpp concurrency_control.h record.h utils.cpp utils.h execution.h execution.cpp constants.h)