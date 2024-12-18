cmake_minimum_required(VERSION 3.15)

project(ef VERSION 1.0.0)

set(CMAKE_C_STANDARD 99)
set(CMAKE_CXX_STANDARD 20)

set(YAEF_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/include")

set(YAEF_OPTS_ENABLE_TESTS OFF "enable unit tests")
set(YAEF_OPTS_ENABLE_BENCHMARKS OFF "enable benchmarks")

set(_YAEF_DEV_OPTS_ENABLE_TEMP_RESULT_OUTPUT OFF "(DEV-only) output the intemediate assembly code")

add_library(yaef INTERFACE)
add_library(yaef::yaef ALIAS yaef)
target_include_directories(yaef INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}/include")

if(MSVC)
    target_compile_options(yaef INTERFACE /FAs /arch:AVX2)
else()
    target_compile_options(yaef INTERFACE -Wall -Wextra 
        -msse -msse2 -msse3 -mssse3 -msse4 -msse4a -msse4.1 -msse4.2 -mavx -mavx2 -mbmi -mbmi2)
    if(_YAEF_DEV_OPTS_ENABLE_TEMP_RESULT_OUTPUT)
        target_compile_options(yaef INTERFACE  -save-temps -fverbose-asm -masm=intel)
    endif()
endif()

if(YAEF_OPTS_ENABLE_TESTS)
    find_package(Catch2 3 REQUIRED)
    if(NOT Catch2_FOUND)
        message(FATAL_ERROR "Cannot find Catch2 library.")
    endif()
    add_subdirectory(tests)
endif()

if(YAEF_OPTS_ENABLE_BENCHMARKS)
    add_subdirectory(benchmarks)
endif()