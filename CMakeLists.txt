cmake_minimum_required(VERSION 3.17)
project(pdb)

set(CMAKE_CXX_STANDARD 20)

find_package(GTest CONFIG REQUIRED)

# Download and unpack googletest at configure time
configure_file(CMakeLists.txt.in googletest-download/CMakeLists.txt)
execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
        RESULT_VARIABLE result
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/googletest-download )
if(result)
    message(FATAL_ERROR "CMake step for googletest failed: ${result}")
endif()
execute_process(COMMAND ${CMAKE_COMMAND} --build .
        RESULT_VARIABLE result
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/googletest-download )
if(result)
    message(FATAL_ERROR "Build step for googletest failed: ${result}")
endif()

# Prevent overriding the parent project's compiler/linker
# settings on Windows
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

# Add googletest directly to our build. This defines
# the gtest and gtest_main targets.
add_subdirectory(${CMAKE_CURRENT_BINARY_DIR}/googletest-src
        ${CMAKE_CURRENT_BINARY_DIR}/googletest-build
        EXCLUDE_FROM_ALL)

find_package(Boost 1.71.0 COMPONENTS system filesystem thread REQUIRED)
if(Boost_FOUND)
    message(STATUS "Boost_INCLUDE_DIRS: ${Boost_INCLUDE_DIRS}")
    message(STATUS "Boost_LIBRARIES: ${Boost_LIBRARIES}")
    message(STATUS "Boost_VERSION: ${Boost_VERSION}")
    include_directories(${Boost_INCLUDE_DIRS})
endif()

list(APPEND CMAKE_MODULE_PATH "~/CLionProjects/FindTBB")

find_package(TBB COMPONENTS tbbmalloc tbbmalloc_proxy tbb_preview)
if(NOT TARGET spdlog)
    # Stand-alone build
    find_package(spdlog REQUIRED)
endif()


set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

add_executable(pdb main.cpp log_writer.cpp log_writer.h structures/input_types.h structures/input.h concurrency_control.cpp concurrency_control.h structures/record.h utils.cpp utils.h execution.h execution.cpp constants.h structures/state.h
        tests/integration_tests.cpp tests/gtest_main.cc tests/cc_tests.cpp tests/execution_tests.cpp read_input_file.h tests/read_input_file_tests.cpp read_input_file.cpp)

target_link_libraries(pdb gtest_main)
target_link_libraries(pdb tbb)
target_link_libraries(pdb Threads::Threads)
target_link_libraries(pdb ${Boost_LIBRARIES})

# for testing
#add_definitions(-DTESTING)



