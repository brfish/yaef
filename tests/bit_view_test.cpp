#include <unordered_set>

#include "catch2/catch_test_macros.hpp"

#include "yaef/yaef.hpp"

#include "utils/bit_generator.hpp"
#include "utils/defer_guard.hpp"

TEST_CASE("bit_view_test", "[private]") {
    using yaef::details::bits64::bit_view;

    SECTION("allocate and deallocate") {
        constexpr size_t NUM_BITS = 10000;

        std::allocator<uint8_t> alloc;
        auto bits = yaef::details::allocate_bits(alloc, NUM_BITS);
        REQUIRE(bits.size() == NUM_BITS);
        REQUIRE_NOTHROW(yaef::details::deallocate_bits(alloc, bits));
    }

    SECTION("random access (get/set)") {
        constexpr size_t NUM_BITS = 10000;
        
        yaef::test_utils::bit_generator gen;
        auto gen_result = gen.make_uninit_bits(NUM_BITS);
        gen_result.view.clear_all_bits();
        
        yaef::test_utils::uniform_int_generator<size_t> indices_gen{0, NUM_BITS - 1};
        auto indices = indices_gen.make_list(NUM_BITS / 2);
        for (auto index : indices) {
            gen_result.view.set_bit(index);
            REQUIRE(gen_result.view.get_bit(index) == true);
        }
        gen_result.view.prefetch_for_read(0, NUM_BITS);
        std::unordered_set<size_t> index_set{indices.begin(), indices.end()};
        for (size_t i = 0; i < NUM_BITS; ++i) {
            bool actual_bit = gen_result.view.get_bit(i);
            bool expected_bit = index_set.find(i) != index_set.end();
            REQUIRE(actual_bit == expected_bit);
        }
    }

    SECTION("duplicate") {
        constexpr size_t NUM_BITS = 10000;
        
        yaef::test_utils::bit_generator gen;
        auto gen_result = gen.make_bits_with_one_indices(
            yaef::test_utils::bit_generator::param::by_one_density(NUM_BITS, 0.5));

        std::allocator<uint8_t> alloc;
        auto copy = yaef::details::duplicate_bits(alloc, gen_result.view);
        YAEF_DEFER { yaef::details::deallocate_bits(alloc, copy); };

        REQUIRE(copy.size() == NUM_BITS);
        for (size_t i = 0; i < NUM_BITS; ++i)
            REQUIRE(gen_result.view.get_bit(i) == copy.get_bit(i));
    }

    SECTION("eqaul") {
        constexpr size_t NUM_BITS = 10000;
        
        yaef::test_utils::bit_generator gen;
        auto gen_result = gen.make_bits_with_one_indices(
            yaef::test_utils::bit_generator::param::by_one_density(NUM_BITS, 0.5));
        REQUIRE(gen_result.view == gen_result.view);

        std::allocator<uint8_t> alloc;
        auto copy = yaef::details::duplicate_bits(alloc, gen_result.view);
        YAEF_DEFER { yaef::details::deallocate_bits(alloc, copy); };
        REQUIRE(gen_result.view == copy);
        
        copy.clear_bit(gen_result.one_indices.front());
        REQUIRE(gen_result.view != copy);
    }

    SECTION("set/clear all bits") {
        using bits_block_type = bit_view::block_type;
        constexpr uint32_t BITS_BLOCK_WIDTH = bit_view::BLOCK_WIDTH;
        constexpr size_t NUM_BITS = 10000;

        yaef::test_utils::bit_generator gen;
        auto gen_result = gen.make_uninit_bits(NUM_BITS);
        const size_t num_blocks = gen_result.view.num_blocks();
        const bits_block_type *blocks = gen_result.view.blocks();

        gen_result.view.clear_all_bits();
        for (size_t i = 0; i < num_blocks; ++i)
            REQUIRE(blocks[i] == 0);

        gen_result.view.set_all_bits();
        for (size_t i = 0; i < num_blocks - 1; ++i)
            REQUIRE(blocks[i] == std::numeric_limits<bits_block_type>::max());
        const size_t num_residual_bits = NUM_BITS - (num_blocks - 1) * BITS_BLOCK_WIDTH;
        REQUIRE(blocks[num_blocks - 1] == yaef::details::bits64::make_mask_lsb1(num_residual_bits));
    }
}