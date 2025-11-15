cmake_minimum_required(VERSION 3.15)

project(yaef VERSION 0.1.0 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)

include(CheckCXXCompilerFlag)

set(YAEF_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/include")
set(YAEF_TESTS_DIR "${CMAKE_CURRENT_SOURCE_DIR}/tests")
set(YAEF_BENCHMARKS_DIR "${YAEF_TEST_DIR}/benchmarks")

# normal library options
option(YAEF_OPTS_ENABLE_TESTS "enable unit tests" OFF)
option(YAEF_OPTS_ENABLE_BENCHMARKS "enable benchmarks" OFF)

# development-only options
option(_YAEF_DEV_OPTS_ENABLE_TEMP_RESULT_OUTPUT "(DEV-only) output the intemediate assembly code" OFF)

macro(yaef_info _msg)
  string(CONCAT rem_msg ${ARGN})
  message(STATUS "[yaef] ${_msg}${rem_msg}")
endmacro()

macro(yaef_fatal _msg)
  string(CONCAT rem_msg ${ARGN})
  message(FATAL_ERROR "[yaef] ${_msg}${rem_msg}")
endmacro()

if(MSVC)
  set(YAEF_HAVE_AVX512 FALSE)
else()
  set(YAEF_AVX512_COPTS
    "-mavx512f" "-mavx512vl" "-mavx512bw" "-mavx512dq" 
    "-mavx512vbmi" "-mavx512vbmi2" "-mavx512vpopcntdg" "-mavx512bitalg"
  )
  string(JOIN " " YAEF_AVX512_COPTS_STRING "${YAEF_AVX512_COPTS}")

  yaef_info("checking for AVX-512 support with flags: ${YAEF_AVX512_COPTS_STRING}")
  check_cxx_compiler_flag("${YAEF_AVX512_COPTS_STRING}" YAEF_HAVE_AVX512)
endif()

if(YAEF_OPTS_ENABLE_TESTS)
  yaef_info("unit tests are enabled")
else()
  yaef_info("unit tests are disabled")
endif()

if(YAEF_OPTS_ENABLE_BENCHMARKS)
  yaef_info("benchmarks are enabled")
else()
  yaef_info("benchmarks are disabled")
endif()

add_library(yaef INTERFACE)
add_library(yaef::yaef ALIAS yaef)
target_include_directories(yaef INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}/include")

if(YAEF_HAVE_AVX512)
  yaef_info("AVX-512 enabled")
  target_compile_definitions(yaef INTERFACE _YAEF_INTRINSICS_HAVE_AVX512)
endif()

if(MSVC)
  target_compile_options(yaef INTERFACE /arch:AVX2)
  if(_YAEF_DEV_OPTS_ENABLE_TEMP_RESULT_OUTPUT)
    if(NOT ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang" AND "_${CMAKE_CXX_SIMULATE_ID}" STREQUAL "_MSVC"))
      target_compile_options(yaef INTERFACE /FAs)
    endif()
  endif()
else()
  target_compile_options(yaef INTERFACE 
    -Wall -Wextra 
    -msse -msse2 -msse3 -mssse3 -msse4 -msse4a -msse4.1 -msse4.2 -mavx -mavx2 -mbmi -mbmi2
  )
  if(_YAEF_DEV_OPTS_ENABLE_TEMP_RESULT_OUTPUT)
    target_compile_options(yaef INTERFACE -save-temps -fverbose-asm -masm=intel)
  endif()
endif()

if(YAEF_OPTS_ENABLE_TESTS)
  add_subdirectory(deps/Catch2)
  add_subdirectory(tests)
endif()

if(YAEF_OPTS_ENABLE_BENCHMARKS)
  add_subdirectory(tests/benchmarks)
endif()