add_executable(benchmark "${CMAKE_CURRENT_SOURCE_DIR}/benchmark.cpp")
target_link_libraries(benchmark PRIVATE yaef::yaef)
target_include_directories(benchmark PRIVATE "${YAEF_TESTS_DIR}")
set_property(TARGET benchmark PROPERTY CXX_STANDARD 11)

add_executable(single_eliasfano_benchmark "${CMAKE_CURRENT_SOURCE_DIR}/single_eliasfano_benchmark.cpp")
target_link_libraries(single_eliasfano_benchmark PRIVATE yaef::yaef)
target_include_directories(single_eliasfano_benchmark PRIVATE "${YAEF_TESTS_DIR}")
set_property(TARGET single_eliasfano_benchmark PROPERTY CXX_STANDARD 11)

add_executable(selectable_dense_bits_benchmark "${CMAKE_CURRENT_SOURCE_DIR}/selectable_dense_bits_benchmark.cpp")
target_link_libraries(selectable_dense_bits_benchmark PRIVATE yaef::yaef)
target_include_directories(selectable_dense_bits_benchmark PRIVATE "${YAEF_TESTS_DIR}")
set_property(TARGET selectable_dense_bits_benchmark PROPERTY CXX_STANDARD 11)