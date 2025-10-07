#include "catch2/catch_test_macros.hpp"

#include "yaef/yaef.hpp"

#include "utils/bit_generator.hpp"

TEST_CASE("bitset_foreach_test", "[private]") {
    using yaef::details::bits64::bitmap_foreach_onebit_cursor;
    using yaef::details::bits64::bitmap_foreach_zerobit_cursor;
    constexpr size_t NUM_BITS  = 1000000;
    constexpr size_t NUM_ONES  = 420000;
    constexpr size_t NUM_ZEROS = NUM_BITS - NUM_ONES;

    using bit_gen_param = yaef::test_utils::bit_generator::param;
    yaef::test_utils::bit_generator gen;
    auto gen_res = gen.make_bits(bit_gen_param::by_size(NUM_ZEROS, NUM_ONES));
    const auto &bits = gen_res.view;

    SECTION("foreach ones forward") {
        bitmap_foreach_onebit_cursor cursor{bits.blocks(), bits.num_blocks()};
        size_t popcnt = 0;
        auto bits_ref_res = gen.make_uninit_bits(NUM_BITS);
        auto &bits_ref = bits_ref_res.view;
        bits_ref.clear_all_bits();
        for (; cursor.is_valid(); cursor.next()) {
            size_t index = cursor.current();
            bits_ref.set_bit(index);
            ++popcnt;
        }
        REQUIRE(bits == bits_ref);
        REQUIRE(popcnt == NUM_ONES);

        bits_ref.clear_all_bits();
        popcnt = 0;
        for (cursor.prev(); cursor.is_valid(); cursor.prev()) {
            size_t index = cursor.current();
            bits_ref.set_bit(index);
            ++popcnt;
        }
        REQUIRE(bits == bits_ref);
        REQUIRE(popcnt == NUM_ONES);
    }
    
    SECTION("foreach zeros") {
        bitmap_foreach_zerobit_cursor cursor{bits.blocks(), bits.num_blocks()};
        size_t popcnt = 0;
        auto bits_ref_res = gen.make_uninit_bits(NUM_BITS);
        auto &bits_ref = bits_ref_res.view;
        bits_ref.set_all_bits();
        for (; cursor.is_valid(); cursor.next()) {
            size_t index = cursor.current();
            bits_ref.clear_bit(index);
            ++popcnt;
        }
        REQUIRE(bits == bits_ref);
        REQUIRE(popcnt == NUM_ZEROS);

        bits_ref.set_all_bits();
        popcnt = 0;
        for (cursor.prev(); cursor.is_valid(); cursor.prev()) {
            size_t index = cursor.current();
            bits_ref.clear_bit(index);
            ++popcnt;
        }
        REQUIRE(bits == bits_ref);
        REQUIRE(popcnt == NUM_ZEROS);
    }

    SECTION("foreach ones with offset") {
        constexpr uint32_t OFFSET = 7733;
        bitmap_foreach_onebit_cursor cursor{bits.blocks(), bits.num_blocks(), OFFSET};
        size_t popcnt = 0;
        auto bits_ref_res = gen.make_uninit_bits(NUM_BITS);
        auto &bits_ref = bits_ref_res.view;
        bits_ref.clear_all_bits();
        
        for (; cursor.is_valid(); cursor.next()) {
            size_t index = cursor.current();
            bits_ref.set_bit(index);
            ++popcnt;
        }

        size_t skipped_popcnt = 0;
        for (size_t i = 0; i < OFFSET; ++i) {
            skipped_popcnt += static_cast<size_t>(bits.get_bit(i));
            bits_ref.set_bit(i, bits.get_bit(i));
        }

        REQUIRE(bits == bits_ref);
        REQUIRE(popcnt == NUM_ONES - skipped_popcnt);
    }
    
    SECTION("foreach zeros with offset") {
        constexpr uint32_t OFFSET = 27;
        bitmap_foreach_zerobit_cursor cursor{bits.blocks(), bits.num_blocks(), OFFSET};
        size_t popcnt = 0;
        auto bits_ref_res = gen.make_uninit_bits(NUM_BITS);
        auto &bits_ref = bits_ref_res.view;
        bits_ref.set_all_bits();
        for (; cursor.is_valid(); cursor.next()) {
            size_t index = cursor.current();
            bits_ref.clear_bit(index);
            ++popcnt;
        }

        size_t skipped_popcnt = 0;
        for (size_t i = 0; i < OFFSET; ++i) {
            skipped_popcnt += static_cast<size_t>(!bits.get_bit(i));
            bits_ref.set_bit(i, bits.get_bit(i));
        }

        REQUIRE(bits == bits_ref);
        REQUIRE(popcnt == NUM_ZEROS - skipped_popcnt);
    }
}