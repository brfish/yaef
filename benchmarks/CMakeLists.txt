add_executable(benchmark "${CMAKE_CURRENT_SOURCE_DIR}/benchmark.cpp")
target_link_libraries(benchmark PRIVATE yaef::yaef)
set_property(TARGET benchmark PROPERTY CXX_STANDARD 11)