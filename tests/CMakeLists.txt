macro(yaef_add_test _name _srcfile)
  add_executable(${_name} "${_srcfile}" ${ARGN})
  target_link_libraries(${_name} PRIVATE yaef::yaef Catch2::Catch2WithMain)
  set_property(TARGET ${_name} PROPERTY CXX_STANDARD 20)
endmacro()

# bit_view_test
yaef_add_test(bit_view_test "bit_view_test.cpp")

# bitset_foreach_test
yaef_add_test(bitset_foreach_test "bitset_foreach_test.cpp")

# eliasfano_code_test
yaef_add_test(eliasfano_code_test "eliasfano_code_test.cpp")

# eliasfano_list_test
yaef_add_test(eliasfano_list_test "eliasfano_list_test.cpp")

# eliasfano_sequence_test
yaef_add_test(eliasfano_sequence_test "eliasfano_sequence_test.cpp")

# packed_int_view_test
yaef_add_test(packed_int_view_test "packed_int_view_test.cpp")

# selectable_dense_bits_test
yaef_add_test(selectable_dense_bits_test "selectable_dense_bits_test.cpp")