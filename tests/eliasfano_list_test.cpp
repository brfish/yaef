#include "catch2/generators/catch_generators.hpp"
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

        for (size_t i = 0; i < ints.size(); ++i) {
            uint64_t expected = ints[i];
            uint64_t actual = list.at(i);
            REQUIRE(expected == actual);
        }
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
    }

    SECTION("construct from small sorted unsigned integer list and random access") {
        const size_t num_ints = GENERATE(1, 2, 5, 64, 65, 128, 4095);

        using int_type = uint32_t;
        yaef::utils::uniform_int_generator<int_type> gen{std::numeric_limits<int_type>::min(), 
                                                         std::numeric_limits<int_type>::max(),
                                                         yaef::utils::make_random_seed()};
        auto ints = gen.make_sorted_list(num_ints);

        yaef::eliasfano_list<int_type> list(yaef::from_sorted, ints.begin(), ints.end());
        REQUIRE(list.size() == ints.size());

        for (size_t i = 0; i < ints.size(); ++i) {
            int64_t expected = ints[i];
            int64_t actual = list.at(i);
            REQUIRE(expected == actual);
        }
    }

    SECTION("lower_bound and upper_bound") {
        using int_type = int32_t;
        yaef::utils::uniform_int_generator<int_type> gen{std::numeric_limits<int_type>::min() + 10,
                                                         std::numeric_limits<int_type>::max() - 10,
                                                         yaef::utils::make_random_seed()};
        auto ints = gen.make_sorted_list(100000);
        
        yaef::eliasfano_list<int_type> list{yaef::from_sorted, ints.begin(), ints.end()};
        REQUIRE(list.size() == ints.size());

        auto test_lower_bound = [&](int_type target) {
            auto expected_iter = std::lower_bound(ints.begin(), ints.end(), target);
            auto actual_iter = list.lower_bound(target);

            if (expected_iter == ints.end()) {
                REQUIRE(actual_iter == list.end());
            } else {
                REQUIRE(*expected_iter == *actual_iter);
            }

            size_t expected_index = std::distance(ints.begin(), expected_iter);
            size_t actual_index = std::distance(list.begin(), actual_iter);
            REQUIRE(expected_index == actual_index);

            auto actual_index_of = list.index_of_lower_bound(target);
            REQUIRE(expected_index == actual_index_of);
        };

        auto test_upper_bound = [&](int_type target) {
            auto expected_iter = std::upper_bound(ints.begin(), ints.end(), target);
            auto actual_iter = list.upper_bound(target);

            if (expected_iter == ints.end()) {
                REQUIRE(actual_iter == list.end());
            } else {
                REQUIRE(*expected_iter == *actual_iter);
            }

            size_t expected_index = std::distance(ints.begin(), expected_iter);
            size_t actual_index = std::distance(list.begin(), actual_iter);
            REQUIRE(expected_index == actual_index);

            auto actual_index_of = list.index_of_upper_bound(target);
            REQUIRE(expected_index == actual_index_of);
        };

        test_lower_bound(list.min() - 2);
        test_lower_bound(list.max() + 2);
        test_upper_bound(list.min() - 2);
        test_upper_bound(list.max() + 2);

        for (size_t i = 0; i < 1000; ++i) {
            auto target = yaef::utils::random<int_type>(gen.min(), gen.max());
            test_lower_bound(target);
            test_upper_bound(target);
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
        bool has_duplicates = list.has_duplicates();

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
            REQUIRE(deserialized_list.has_duplicates() == has_duplicates);
        }
    }

    SECTION("serialize/deserialize to file") {
        using int_type = uint32_t;
        yaef::utils::uniform_int_generator<int_type> gen{std::numeric_limits<int_type>::min(), 
                                                         std::numeric_limits<int_type>::max(),
                                                         yaef::utils::make_random_seed()};
        auto ints = gen.make_sorted_list(80000);

        yaef::eliasfano_list<int_type> list(yaef::from_sorted, ints.begin(), ints.end());
        bool has_duplicates = list.has_duplicates();

        REQUIRE(yaef::serialize_to_file(list, "tmp.yaef", true) == yaef::error_code::success);
        {
            yaef::eliasfano_list<int_type> deserialized_list;
            REQUIRE(yaef::deserialize_from_file(deserialized_list, "tmp.yaef") == yaef::error_code::success);

            REQUIRE(deserialized_list.size() == list.size());
            for (size_t i = 0; i < list.size(); ++i) {
                REQUIRE(deserialized_list.at(i) == list.at(i));
            }
            REQUIRE(deserialized_list.has_duplicates() == has_duplicates);
        }
    }

    SECTION("check if list contains duplicates") {
        yaef::eliasfano_list<uint32_t> list{1, 2, 3, 4, 5};
        REQUIRE(!list.has_duplicates());
        yaef::eliasfano_list<uint32_t> dup_list{1, 2, 2, 3, 3, 5};
        REQUIRE(dup_list.has_duplicates());
    }
}