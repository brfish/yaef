# bit_view_test
add_executable(bit_view_test "${CMAKE_CURRENT_SOURCE_DIR}/bit_view_test.cpp")
target_link_libraries(bit_view_test PRIVATE yaef::yaef Catch2::Catch2WithMain)

# bitset_foreach_test
add_executable(bitset_foreach_test "${CMAKE_CURRENT_SOURCE_DIR}/bitset_foreach_test.cpp")
target_link_libraries(bitset_foreach_test PRIVATE yaef::yaef Catch2::Catch2WithMain)

# eliasfano_code_test
add_executable(eliasfano_code_test "${CMAKE_CURRENT_SOURCE_DIR}/eliasfano_code_test.cpp")
target_link_libraries(eliasfano_code_test PRIVATE yaef::yaef Catch2::Catch2WithMain)

# eliasfano_list_test
add_executable(eliasfano_list_test "${CMAKE_CURRENT_SOURCE_DIR}/eliasfano_list_test.cpp")
target_link_libraries(eliasfano_list_test PRIVATE yaef::yaef Catch2::Catch2WithMain)

# eliasfano_sequence_test
add_executable(eliasfano_sequence_test "${CMAKE_CURRENT_SOURCE_DIR}/eliasfano_sequence_test.cpp")
target_link_libraries(eliasfano_sequence_test PRIVATE yaef::yaef Catch2::Catch2WithMain)

# packed_int_view_test
add_executable(packed_int_view_test "${CMAKE_CURRENT_SOURCE_DIR}/packed_int_view_test.cpp")
target_link_libraries(packed_int_view_test PRIVATE yaef::yaef Catch2::Catch2WithMain)

# selectable_dense_bits_test
add_executable(selectable_dense_bits_test "${CMAKE_CURRENT_SOURCE_DIR}/selectable_dense_bits_test.cpp")
target_link_libraries(selectable_dense_bits_test PRIVATE yaef::yaef Catch2::Catch2WithMain)