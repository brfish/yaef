macro(yaef_add_test _name _srcfile)
  add_executable(${_name} "${_srcfile}" ${ARGN})
  target_link_libraries(${_name} PRIVATE yaef::yaef Catch2::Catch2WithMain)
  target_include_directories(${_name} PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}")
  set_property(TARGET ${_name} PROPERTY CXX_STANDARD 11)
endmacro()

# bit_view_test
yaef_add_test(bit_view_test "bit_view_test.cpp")

# bitmap_foreach_test
yaef_add_test(bitmap_foreach_test "bitmap_foreach_test.cpp")

# eliasfano_code_test
yaef_add_test(eliasfano_code_test "eliasfano_code_test.cpp")

# eliasfano_list_test
yaef_add_test(eliasfano_list_test "eliasfano_list_test.cpp")

# eliasfano_sequence_test
yaef_add_test(eliasfano_sequence_test "eliasfano_sequence_test.cpp")

# eliasfano_sequence_test
yaef_add_test(eliasfano_sparse_bitmap_test "eliasfano_sparse_bitmap_test.cpp")

# packed_int_view_test
yaef_add_test(packed_int_view_test "packed_int_view_test.cpp")

# selectable_dense_bits_test
yaef_add_test(selectable_dense_bits_test "selectable_dense_bits_test.cpp")