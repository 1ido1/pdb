cmake_minimum_required(VERSION 3.17)
project(pdb)

set(CMAKE_CXX_STANDARD 20)

find_package(Boost 1.71.0 COMPONENTS system filesystem thread REQUIRED)
if(Boost_FOUND)
    message(STATUS "Boost_INCLUDE_DIRS: ${Boost_INCLUDE_DIRS}")
    message(STATUS "Boost_LIBRARIES: ${Boost_LIBRARIES}")
    message(STATUS "Boost_VERSION: ${Boost_VERSION}")
    include_directories(${Boost_INCLUDE_DIRS})
endif()

list(APPEND CMAKE_MODULE_PATH "~/CLionProjects/FindTBB")

find_package(TBB COMPONENTS tbbmalloc tbbmalloc_proxy tbb_preview)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

add_executable(pdb main.cpp log_writer.cpp log_writer.h input_types.h input.h concurrency_control.cpp concurrency_control.h record.h utils.cpp utils.h execution.h execution.cpp constants.h state.h)
target_link_libraries(pdb tbb)
target_link_libraries(pdb Threads::Threads)
target_link_libraries(pdb ${Boost_LIBRARIES})

