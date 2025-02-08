#include "catch2/catch_test_macros.hpp"

#include "yaef/utils/int_generator.hpp"
#include "yaef/yaef.hpp"

TEST_CASE("eliasfano_sequence", "[public]") {
    SECTION("construct") {
        using int_type = uint16_t;
        yaef::utils::uniform_int_generator<int_type> gen;
        auto ints = gen.make_sorted_list(50000);

        yaef::eliasfano_sequence<int_type> seq{yaef::from_sorted, ints.begin(), ints.end()};
        REQUIRE(seq.size() == ints.size());
        REQUIRE(seq.min() == ints.front());
        REQUIRE(seq.max() == ints.back());
    }

    SECTION("forward traverse") {
        using int_type = uint32_t;
        yaef::utils::uniform_int_generator<int_type> gen;
        auto ints = gen.make_sorted_list(80000);

        yaef::eliasfano_sequence<int_type> seq{yaef::from_sorted, ints.begin(), ints.end()};
        size_t i = 0;
        for (auto x : seq) {
            REQUIRE(x == ints[i]);
            ++i;
        }
    }

    SECTION("serialize/deserialize to memory buffer") {
        using int_type = uint32_t;
        yaef::utils::uniform_int_generator<int_type> gen;
        auto ints = gen.make_sorted_list(80000);

        yaef::eliasfano_sequence<int_type> seq{yaef::from_sorted, ints.begin(), ints.end()};

        const size_t bytes_mem_size = 2 * 1024 * 1024;
        std::unique_ptr<uint8_t []> bytes_mem(new uint8_t[bytes_mem_size]);
        
        REQUIRE(yaef::serialize_to_buf(seq, bytes_mem.get(), bytes_mem_size) == yaef::error_code::success);
        {
            yaef::eliasfano_sequence<int_type> deserialized_seq;
            REQUIRE(yaef::deserialize_from_buf(deserialized_seq, bytes_mem.get(), bytes_mem_size) == yaef::error_code::success);

            REQUIRE(deserialized_seq.size() == seq.size());
            auto iter1 = seq.begin();
            auto iter2 = deserialized_seq.begin();
            for (; iter1 != seq.end() && iter2 != deserialized_seq.end(); ++iter1, ++iter2) {
                REQUIRE(*iter1 == *iter2);
            }
        }
    }

    SECTION("serialize/deserialize to file") {
        using int_type = uint32_t;
        yaef::utils::uniform_int_generator<int_type> gen;
        auto ints = gen.make_sorted_list(80000);

        yaef::eliasfano_sequence<int_type> seq{yaef::from_sorted, ints.begin(), ints.end()};
        
        REQUIRE(yaef::serialize_to_file(seq, "tmp_seq.yaef", true) == yaef::error_code::success);
        {
            yaef::eliasfano_sequence<int_type> deserialized_seq;
            REQUIRE(yaef::deserialize_from_file(deserialized_seq, "tmp_seq.yaef") == yaef::error_code::success);

            REQUIRE(deserialized_seq.size() == seq.size());
            auto iter1 = seq.begin();
            auto iter2 = deserialized_seq.begin();
            for (; iter1 != seq.end() && iter2 != deserialized_seq.end(); ++iter1, ++iter2) {
                REQUIRE(*iter1 == *iter2);
            }
        }
    }

    SECTION("check if list contains duplicates") {
        yaef::eliasfano_sequence<uint32_t> list{1, 2, 3, 4, 5};
        REQUIRE(!list.has_duplicates());
        yaef::eliasfano_sequence<uint32_t> dup_list{1, 2, 2, 3, 3, 5};
        REQUIRE(dup_list.has_duplicates());
    }
}