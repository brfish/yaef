#include <iostream>
#include <map>

#include "catch2/catch_test_macros.hpp"

#include "yaef/utils/int_generator.hpp"
#include "yaef/yaef.hpp"

_YAEF_STATIC_ASSERT_NOMSG(std::is_same<yaef::eliasfano_list<uint32_t>::size_type, size_t>::value);
_YAEF_STATIC_ASSERT_NOMSG(std::is_same<yaef::eliasfano_list<uint32_t>::difference_type, ptrdiff_t>::value);

_YAEF_STATIC_ASSERT_NOMSG(std::is_copy_constructible<yaef::eliasfano_list<uint32_t>>::value);
_YAEF_STATIC_ASSERT_NOMSG(std::is_move_constructible<yaef::eliasfano_list<uint32_t>>::value);
_YAEF_STATIC_ASSERT_NOMSG(std::is_copy_assignable<yaef::eliasfano_list<uint32_t>>::value);
_YAEF_STATIC_ASSERT_NOMSG(std::is_move_assignable<yaef::eliasfano_list<uint32_t>>::value);

TEST_CASE("eliasfano_list_test", "[public]") {
    SECTION("construct from sorted unsigned integer list and random access") {
        using int_type = uint32_t;
        yaef::utils::uniform_int_generator<int_type> gen{std::numeric_limits<int_type>::min(), 
                                                         std::numeric_limits<int_type>::max(),
                                                         yaef::utils::make_random_seed()};
        auto ints = gen.make_sorted_list(80000);

        yaef::eliasfano_list<int_type> list(yaef::from_sorted, ints.begin(), ints.end());
        REQUIRE(list.size() == ints.size());

        std::map<size_t, size_t> hist_one;
        for (size_t i = 0; i < ints.size(); ++i) {
            auto stats = list.get_high_bits().select_one_scan_count(i);
            ++hist_one[stats.num_popcount];
        }
        std::cout << "hist for ones:\n";
        for (auto p : hist_one) {
            std::cout << "[" << p.first << "] " << p.second << '\n';
        }

        std::map<size_t, size_t> hist_zero;
        for (size_t i = 0; i < list.get_high_bits().size() - ints.size(); ++i) {
            auto stats = list.get_high_bits().select_zero_scan_count(i);
            ++hist_zero[stats.num_popcount];
        }
        std::cout << "hist for zeros:\n";
        for (auto p : hist_zero) {
            std::cout << "[" << p.first << "] " << p.second << '\n';
        }

        for (size_t i = 0; i < ints.size(); ++i) {
            uint64_t expected = ints[i];
            uint64_t actual = list.at(i);
            REQUIRE(expected == actual);
        }
        size_t origin_size_in_bytes = ints.size() * sizeof(int_type);
        size_t compact_size_in_bytes = list.space_usage_in_bytes();
        size_t packed_size_in_bytes = yaef::details::bits64::idiv_ceil(
            ints.size() * yaef::details::bits64::bit_width(ints.back()), 8);

        double ratio1 = static_cast<double>(compact_size_in_bytes) / static_cast<double>(origin_size_in_bytes);
        double ratio2 = static_cast<double>(compact_size_in_bytes) / static_cast<double>(packed_size_in_bytes);
        std::cout << "ratio1: " << ratio1 * 100.0 << "%\n"
                  << "ratio2: " << ratio2 * 100.0 << "%\n";
    }

    SECTION("construct from sorted signed integer list and random access") {
        using int_type = int32_t;
        yaef::utils::uniform_int_generator<int_type> gen{std::numeric_limits<int_type>::min(), 
                                                         std::numeric_limits<int_type>::max(),
                                                         yaef::utils::make_random_seed()};
        auto ints = gen.make_sorted_list(80000);

        yaef::eliasfano_list<int_type> list(yaef::from_sorted, ints.begin(), ints.end());
        REQUIRE(list.size() == ints.size());

        for (size_t i = 0; i < ints.size(); ++i) {
            int64_t expected = ints[i];
            int64_t actual = list.at(i);
            REQUIRE(expected == actual);
        }
        size_t origin_size_in_bytes = ints.size() * sizeof(int_type);
        size_t compact_size_in_bytes = list.space_usage_in_bytes();
        size_t packed_size_in_bytes = yaef::details::bits64::idiv_ceil(
            ints.size() * yaef::details::bits64::bit_width(ints.back()), 8);

        double ratio1 = static_cast<double>(compact_size_in_bytes) / static_cast<double>(origin_size_in_bytes);
        double ratio2 = static_cast<double>(compact_size_in_bytes) / static_cast<double>(packed_size_in_bytes);
        std::cout << "ratio1: " << ratio1 * 100.0 << "%\n"
                  << "ratio2: " << ratio2 * 100.0 << "%\n";
    }

    SECTION("lower_bound and upper_bound") {
        using int_type = int32_t;
        yaef::utils::uniform_int_generator<int_type> gen{std::numeric_limits<int_type>::min(),
                                                         std::numeric_limits<int_type>::max(),
                                                         yaef::utils::make_random_seed()};
        auto ints = gen.make_sorted_list(100000);
        
        yaef::eliasfano_list<int_type> list{yaef::from_sorted, ints.begin(), ints.end()};
        REQUIRE(list.size() == ints.size());

        for (size_t i = 0; i < 1000; ++i) {
            auto target = yaef::utils::random<int_type>(gen.min(), gen.max());

            auto lower_bound_expected_iter = std::lower_bound(ints.begin(), ints.end(), target);
            auto lower_bound_actual_iter = list.lower_bound(target);
            REQUIRE(*lower_bound_expected_iter == *lower_bound_actual_iter);

            size_t lower_bound_expected_index = std::distance(ints.begin(), lower_bound_expected_iter);
            size_t lower_bound_actual_index = std::distance(list.begin(), lower_bound_actual_iter);
            REQUIRE(lower_bound_expected_index == lower_bound_actual_index);
            
            auto upper_bound_expected_iter = std::upper_bound(ints.begin(), ints.end(), target);
            auto upper_bound_actual_iter = list.upper_bound(target);
            REQUIRE(*upper_bound_expected_iter == *upper_bound_actual_iter);

            size_t upper_bound_expected_index = std::distance(ints.begin(), upper_bound_expected_iter);
            size_t upper_bound_actual_index = std::distance(list.begin(), upper_bound_actual_iter);
            REQUIRE(upper_bound_expected_index == upper_bound_actual_index);
        }
    }

    SECTION("iterate") {
        using int_type = uint32_t;
        yaef::utils::uniform_int_generator<uint32_t> gen{std::numeric_limits<int_type>::min(),
                                                         std::numeric_limits<int_type>::max(),
                                                         yaef::utils::make_random_seed()};
        auto ints = gen.make_sorted_list(1000000 / 2);
        yaef::eliasfano_list<int_type> list{yaef::from_sorted, ints.begin(), ints.end()};

        size_t i = 0;
        for (auto iter = list.begin(); iter != list.end(); ++iter, ++i) {
            REQUIRE(*iter == ints[i]);
        }

        i = 20;
        for (auto iter = list.iter(i); iter != list.end(); ++iter, ++i) {
            REQUIRE(*iter == ints[i]);
        }
    }

    SECTION("serialize/deserialize to memory buffer") {
        using int_type = uint32_t;
        yaef::utils::uniform_int_generator<int_type> gen{std::numeric_limits<int_type>::min(), 
                                                         std::numeric_limits<int_type>::max(),
                                                         yaef::utils::make_random_seed()};
        auto ints = gen.make_sorted_list(80000);

        yaef::eliasfano_list<int_type> list(yaef::from_sorted, ints.begin(), ints.end());

        const size_t bytes_mem_size = 2 * 1024 * 1024;
        std::unique_ptr<uint8_t []> bytes_mem(new uint8_t[bytes_mem_size]);
        
        REQUIRE(yaef::serialize_to_buf(list, bytes_mem.get(), bytes_mem_size) == yaef::error_code::success);
        {
            yaef::eliasfano_list<int_type> deserialized_list;
            REQUIRE(yaef::deserialize_from_buf(deserialized_list, bytes_mem.get(), bytes_mem_size) == yaef::error_code::success);

            REQUIRE(deserialized_list.size() == list.size());
            for (size_t i = 0; i < list.size(); ++i) {
                REQUIRE(deserialized_list.at(i) == list.at(i));
            }
        }
    }

    SECTION("serialize/deserialize to file") {
        using int_type = uint32_t;
        yaef::utils::uniform_int_generator<int_type> gen{std::numeric_limits<int_type>::min(), 
                                                         std::numeric_limits<int_type>::max(),
                                                         yaef::utils::make_random_seed()};
        auto ints = gen.make_sorted_list(80000);

        yaef::eliasfano_list<int_type> list(yaef::from_sorted, ints.begin(), ints.end());

        REQUIRE(yaef::serialize_to_file(list, "tmp.yaef") == yaef::error_code::success);
        {
            yaef::eliasfano_list<int_type> deserialized_list;
            REQUIRE(yaef::deserialize_from_file(deserialized_list, "tmp.yaef") == yaef::error_code::success);

            REQUIRE(deserialized_list.size() == list.size());
            for (size_t i = 0; i < list.size(); ++i) {
                REQUIRE(deserialized_list.at(i) == list.at(i));
            }
        }
    }
}