#include "catch2/catch_template_test_macros.hpp"

#include "yaef/yaef.hpp"
#include "utils/int_generator.hpp"

using cardinality_uint32 = yaef::sparse_sampled_list<uint32_t, yaef::sample_strategy::cardinality>;
using universe_uint32    = yaef::sparse_sampled_list<uint32_t, yaef::sample_strategy::universe>;

TEMPLATE_TEST_CASE("sparse_sampled_list_test", "[public]", cardinality_uint32, universe_uint32) {
    using list_type = TestType;

    std::vector<uint32_t> data;
    for(uint32_t i = 1; i < 500; ++i) {
        data.push_back(i * 3); // 3, 6, ..., 1497
    }

    SECTION("constructors") {
        SECTION("default constructor") {
            list_type sl;
            REQUIRE(sl.empty());
            REQUIRE(sl.size() == 0);
            REQUIRE(sl.num_samples() == 0);
        }

        SECTION("iterator range constructor") {
            list_type sl(data.begin(), data.end());
            REQUIRE_FALSE(sl.empty());
            REQUIRE(sl.size() == data.size());
            REQUIRE(sl.min() == data.front());
            REQUIRE(sl.max() == data.back());
            REQUIRE(sl.num_samples() > 0);
        }
        
        SECTION("iterator constructor with from_sorted_t") {
            list_type sl(yaef::from_sorted, data.begin(), data.end());
            REQUIRE(sl.size() == data.size());
            REQUIRE(sl[10] == data[10]);
        }

        SECTION("initializer list constructor") {
            list_type sl = {0, 10, 20, 30, 40, 50};
            REQUIRE(sl.size() == 6);
            REQUIRE(sl.min() == 0);
            REQUIRE(sl.max() == 50);
            REQUIRE(sl[2] == 20);
        }

        SECTION("copy constructor") {
            list_type original(data.begin(), data.end());
            list_type copy(original);
            REQUIRE(copy.size() == original.size());
            REQUIRE(copy.num_samples() == original.num_samples());
            REQUIRE(copy.min() == original.min());
            REQUIRE(copy.max() == original.max());
            for (size_t i = 0; i < data.size(); ++i) {
                REQUIRE(copy[i] == original[i]);
            }
        }
        
        SECTION("move constructor") {
            list_type original(data.begin(), data.end());
            size_t original_size = original.size();
            size_t original_samples = original.num_samples();
            list_type moved(std::move(original));
            REQUIRE(moved.size() == original_size);
            REQUIRE(moved.num_samples() == original_samples);
            REQUIRE(moved.min() == data.front());
            REQUIRE(moved.max() == data.back());
            REQUIRE(original.empty());
            REQUIRE(original.size() == 0);
        }

        SECTION("constructor throws on unsorted data") {
            std::vector<uint32_t> unsorted_data = {5, 3, 8, 1};
            REQUIRE_THROWS_AS(list_type(unsorted_data.begin(), unsorted_data.end()), std::invalid_argument);
        }
    }

    SECTION("assignment") {
        list_type sl1(data.begin(), data.end());
        list_type sl2 = {1, 2, 3};

        SECTION("copy assignment") {
            sl2 = sl1;
            REQUIRE(sl2.size() == sl1.size());
            REQUIRE(sl2.min() == sl1.min());
            REQUIRE(sl2.max() == sl1.max());
            REQUIRE(sl2[100] == sl1[100]);
        }

        SECTION("move assignment") {
            size_t original_size = sl1.size();
            sl2 = std::move(sl1);
            
            REQUIRE(sl2.size() == original_size);
            REQUIRE(sl2.min() == 3);
            REQUIRE(sl1.empty());
        }

        SECTION("assign method") {
            list_type sl;
            std::vector<uint32_t> new_data = {100, 200, 300};
            sl.assign(new_data.begin(), new_data.end());
            REQUIRE(sl.size() == 3);
            REQUIRE(sl.min() == 100);
            REQUIRE(sl.max() == 300);
        }
    }

    SECTION("accessors and properties") {
        list_type empty_sl;
        REQUIRE_THROWS_AS(empty_sl.min(), std::out_of_range);
        REQUIRE_THROWS_AS(empty_sl.max(), std::out_of_range);
        REQUIRE_THROWS_AS(empty_sl.at(0), std::out_of_range);
        list_type sl(data.begin(), data.end());
        REQUIRE(sl.size() == data.size());
        REQUIRE(sl.min() == data.front());
        REQUIRE(sl.max() == data.back());
        REQUIRE(sl.at(10) == data[10]);
        REQUIRE(sl[10] == data[10]);
        REQUIRE_THROWS_AS(sl.at(data.size()), std::out_of_range);
    }
    
    SECTION("swap") {
        list_type sl1(data.begin(), data.end());
        list_type sl2 = {1, 2, 3};
        size_t size1 = sl1.size();
        int32_t max1 = sl1.max();
        size_t size2 = sl2.size();
        int32_t max2 = sl2.max();
        sl1.swap(sl2);
        REQUIRE(sl1.size() == size2);
        REQUIRE(sl1.max() == max2);
        REQUIRE(sl2.size() == size1);
        REQUIRE(sl2.max() == max1);
    }

    SECTION("search functionality") {
        // data: 3, 6, 9, 12, 15, ..., 1497
        list_type sl(yaef::from_sorted, data.begin(), data.end());

        SECTION("lower bound") {
            // value exists
            REQUIRE(sl.index_of_lower_bound(9) == 2);
            REQUIRE(*sl.lower_bound(9) == 9);
            
            // value does not exist, should point to next element
            REQUIRE(sl.index_of_lower_bound(10) == 3);
            REQUIRE(*sl.lower_bound(10) == 12);
            
            // value is the minimum
            REQUIRE(sl.index_of_lower_bound(3) == 0);
            REQUIRE(*sl.lower_bound(3) == 3);
            
            // value is smaller than minimum
            REQUIRE(sl.index_of_lower_bound(0) == 0);

            // value is the maximum
            REQUIRE(sl.index_of_lower_bound(1497) == 498);
            REQUIRE(*sl.lower_bound(1497) == 1497);
            
            // value is larger than maximum
            REQUIRE(sl.index_of_lower_bound(2000) == data.size());
            REQUIRE(sl.lower_bound(2000) == sl.data() + sl.size()); // end iterator
        }

        SECTION("upper bound") {
            // value exists
            REQUIRE(sl.index_of_upper_bound(9) == 3);
            REQUIRE(*sl.upper_bound(9) == 12);

            // value does not exist
            REQUIRE(sl.index_of_upper_bound(10) == 3);
            REQUIRE(*sl.upper_bound(10) == 12);
            
            // value is the minimum
            REQUIRE(sl.index_of_upper_bound(3) == 1);
            REQUIRE(*sl.upper_bound(3) == 6);
            
            // value is smaller than minimum
            REQUIRE(sl.index_of_upper_bound(0) == 0);
            
            // value is the maximum
            REQUIRE(sl.index_of_upper_bound(1497) == sl.size());
            REQUIRE(sl.upper_bound(1497) == sl.data() + sl.size()); // end iterator
            
            // value is larger than maximum
            REQUIRE(sl.index_of_upper_bound(2000) == sl.size());
        }

        SECTION("search with duplicates") {
            std::vector<uint32_t> dup_data = {10, 20, 20, 20, 30, 40, 40, 50};
            list_type sl_dup(yaef::from_sorted, dup_data.begin(), dup_data.end());

            // lower bound of a duplicate should be the first occurrence
            REQUIRE(sl_dup.index_of_lower_bound(20) == 1);
            REQUIRE(sl_dup.index_of_lower_bound(40) == 5);

            // upper bound of a duplicate should be after the last occurrence
            REQUIRE(sl_dup.index_of_upper_bound(20) == 4);
            REQUIRE(sl_dup.index_of_upper_bound(40) == 7);
        }

        SECTION("search in random data") {
            constexpr size_t DATA_SIZE = 500000;

            yaef::test_utils::uniform_int_generator<uint32_t> gen(0, std::numeric_limits<uint32_t>::max());
            for (uint32_t trial = 0; trial < 10; ++trial) {
                auto random_data = gen.make_sorted_list(DATA_SIZE);

                list_type sl(yaef::from_sorted, random_data.begin(), random_data.end());
                REQUIRE(sl.size() == random_data.size());

                const uint32_t min_val = random_data.front();
                const uint32_t max_val = random_data.back();

                const uint32_t rnd_min = min_val > 50 ? min_val - 50 : 0;
                const uint32_t rnd_max = max_val <= std::numeric_limits<uint32_t>::max() - 50 
                    ? max_val + 50 : std::numeric_limits<uint32_t>::max();

                for (uint32_t query = 0; query < 200; ++query) {
                    uint32_t target = yaef::test_utils::random(rnd_min, rnd_max);

                    size_t expected_lower_idx = std::distance(random_data.begin(), 
                        std::lower_bound(random_data.begin(), random_data.end(), target));
                    
                    size_t actual_lower_idx = sl.index_of_lower_bound(target);
                    if (expected_lower_idx != actual_lower_idx) {
                        size_t _ = sl.index_of_lower_bound(target);
                    }

                    REQUIRE(actual_lower_idx == expected_lower_idx);
                    REQUIRE(sl.data() + expected_lower_idx == sl.lower_bound(target));

                    size_t expected_upper_idx = std::distance(random_data.begin(), 
                        std::upper_bound(random_data.begin(), random_data.end(), target));
                    size_t actual_upper_idx = sl.index_of_upper_bound(target);
                    if (expected_upper_idx != actual_upper_idx) {
                        size_t _ = sl.index_of_upper_bound(target);
                    }

                    REQUIRE(actual_upper_idx == expected_upper_idx);
                    REQUIRE(sl.data() + expected_upper_idx == sl.upper_bound(target));
                }
            }
        }
    }
}