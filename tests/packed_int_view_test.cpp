#include "catch2/catch_test_macros.hpp"

#include "yaef/yaef.hpp"

#include "utils/int_generator.hpp"
#include "utils/defer_guard.hpp"

TEST_CASE("packed_int_view_test", "[private]") {
    using yaef::details::bits64::packed_int_view;
    std::allocator<uint8_t> alloc;

    SECTION("allocate and deallocate") {
        constexpr size_t NUM_INTS = 10000;
        constexpr uint32_t VAL_WIDTH = 23;

        auto ints = yaef::details::allocate_packed_ints(alloc, VAL_WIDTH, NUM_INTS);
        REQUIRE(ints.size() == NUM_INTS);
        REQUIRE_NOTHROW(yaef::details::deallocate_packed_ints(alloc, ints));
    }

    SECTION("random access (get/set)") {
        constexpr size_t NUM_INTS = 10000;
        constexpr uint32_t MIN_INT = 10;
        constexpr uint32_t MAX_INT = 100000;

        yaef::test_utils::uniform_int_generator<uint32_t> gen{MIN_INT, MAX_INT};
        auto gen_result = gen.make_list(NUM_INTS);
        const uint32_t width = yaef::details::bits64::bit_width(*std::max_element(gen_result.begin(), gen_result.end()));

        auto ints = yaef::details::allocate_uninit_packed_ints(alloc, width, NUM_INTS);
        YAEF_DEFER { yaef::details::deallocate_packed_ints(alloc, ints); };
        for (size_t i = 0; i < ints.size(); ++i) {
            ints.set_value(i, gen_result[i]);
        }
        
        ints.prefetch_for_read(0, ints.size());
        for (size_t i = 0; i < ints.size(); ++i) {
            uint32_t actual = ints.get_value(i);
            uint32_t expected = gen_result[i];
            REQUIRE(actual == expected);
        }
    }

    SECTION("duplicate") {
        constexpr size_t NUM_INTS = 10000;
        constexpr uint32_t MIN_INT = 10;
        constexpr uint32_t MAX_INT = 100000;

        yaef::test_utils::uniform_int_generator<uint32_t> gen{MIN_INT, MAX_INT};
        auto gen_result = gen.make_list(NUM_INTS);
        const uint32_t width = yaef::details::bits64::bit_width(*std::max_element(gen_result.begin(), gen_result.end()));

        auto ints = yaef::details::allocate_uninit_packed_ints(alloc, width, NUM_INTS);
        YAEF_DEFER { yaef::details::deallocate_packed_ints(alloc, ints); };
        for (size_t i = 0; i < ints.size(); ++i) {
            ints.set_value(i, gen_result[i]);
        }

        auto copy = yaef::details::duplicate_packed_ints(alloc, ints);
        YAEF_DEFER { yaef::details::deallocate_packed_ints(alloc, copy); };
        
        REQUIRE(ints.size() == copy.size());
        for (size_t i = 0; i < ints.size(); ++i)
            REQUIRE(ints.get_value(i) == copy.get_value(i));
    }

    SECTION("eqaul") {
        constexpr size_t NUM_INTS = 10000;
        constexpr uint32_t MIN_INT = 10;
        constexpr uint32_t MAX_INT = 100000;

        yaef::test_utils::uniform_int_generator<uint32_t> gen{MIN_INT, MAX_INT};
        auto gen_result = gen.make_list(NUM_INTS);
        const uint32_t width = yaef::details::bits64::bit_width(*std::max_element(gen_result.begin(), gen_result.end()));

        auto ints = yaef::details::allocate_uninit_packed_ints(alloc, width, NUM_INTS);
        YAEF_DEFER { yaef::details::deallocate_packed_ints(alloc, ints); };
        for (size_t i = 0; i < ints.size(); ++i) {
            ints.set_value(i, gen_result[i]);
        }
        REQUIRE(ints == ints);

        auto copy = yaef::details::duplicate_packed_ints(alloc, ints);
        YAEF_DEFER { yaef::details::deallocate_packed_ints(alloc, copy); };

        REQUIRE(ints == copy);
        
        copy.set_value(0, copy.get_value(0) + 1);
        REQUIRE(ints != copy);
    }
    
    SECTION("set/clear all bits") {
        using block_type = packed_int_view::block_type;
        constexpr uint32_t BLOCK_WIDTH = packed_int_view::BLOCK_WIDTH;
        constexpr size_t NUM_INTS = 10000;
        constexpr uint32_t VAL_WIDTH = 13;

        auto ints = yaef::details::allocate_uninit_packed_ints(alloc, VAL_WIDTH, NUM_INTS);
        YAEF_DEFER { yaef::details::deallocate_packed_ints(alloc, ints); }; 

        const size_t num_blocks = ints.num_blocks();
        const auto *blocks = ints.blocks();

        ints.clear_all_bits();
        for (size_t i = 0; i < num_blocks; ++i) {
            REQUIRE(blocks[i] == 0);
        }

        ints.set_all_bits();
        for (size_t i = 0; i < num_blocks - 1; ++i) {
            REQUIRE(blocks[i] == std::numeric_limits<block_type>::max());
        }
        const size_t num_residual_bits = NUM_INTS * VAL_WIDTH - (num_blocks - 1) * BLOCK_WIDTH;
        REQUIRE(blocks[num_blocks - 1] == yaef::details::bits64::make_mask_lsb1(num_residual_bits));
    }
}