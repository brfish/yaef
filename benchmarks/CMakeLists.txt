add_executable(benchmark "${CMAKE_CURRENT_SOURCE_DIR}/benchmark.cpp")
target_link_libraries(benchmark PRIVATE yaef::yaef)
set_property(TARGET benchmark PROPERTY CXX_STANDARD 11)

add_executable(single_eliasfano_benchmark "${CMAKE_CURRENT_SOURCE_DIR}/single_eliasfano_benchmark.cpp")
target_link_libraries(single_eliasfano_benchmark PRIVATE yaef::yaef)
set_property(TARGET single_eliasfano_benchmark PROPERTY CXX_STANDARD 11)