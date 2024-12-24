#include "catch2/generators/catch_generators.hpp"
#include "catch2/catch_test_macros.hpp"

#include "yaef/utils/bit_generator.hpp"
#include "yaef/utils/defer_guard.hpp"
#include "yaef/utils/random.hpp"
#include "yaef/yaef.hpp"

TEST_CASE("selectable_dense_bits_test", "[private]") {
    SECTION("select bit-one positions") {
        const size_t num_bits = GENERATE(1024, 8192, 9876, 10000, 60000);
        const double one_density = GENERATE(0.01, 0.1, 0.5, 0.7, 0.9, 0.99);

        using gen_param = yaef::utils::bit_generator::param;
        yaef::utils::bit_generator gen{yaef::utils::make_random_seed()};
        auto gen_result = gen.make_bits_with_one_indices(
            gen_param::by_one_density(num_bits, one_density));
        auto bits = gen_result.view;
        const auto &one_indices = gen_result.one_indices;

        auto stats = yaef::details::bits64::stats_bits(bits);
        REQUIRE(stats.num_ones() == one_indices.size());

        std::allocator<uint8_t> alloc;
        yaef::details::selectable_dense_bits selectable_bits{alloc, bits, stats};
        gen_result.mem.release(); // Ownership is transferred to selectable_dense_bits.
        YAEF_DEFER {
            selectable_bits.deallocate(alloc);
        };

        for (size_t i = 0; i < one_indices.size(); ++i) {
            auto actual = selectable_bits.select_one(i);
            auto expected = one_indices[i];
            REQUIRE(actual == expected);
        }
    }

    SECTION("select bit-zero positions") {
        const size_t num_bits = GENERATE(1024, 8192, 9876, 10000, 60000);
        const double zero_density = GENERATE(0.01, 0.1, 0.5, 0.7, 0.9, 0.99);

        using gen_param = yaef::utils::bit_generator::param;
        yaef::utils::bit_generator gen{yaef::utils::make_random_seed()};
        auto gen_result = gen.make_bits_with_zero_indices(
            gen_param::by_zero_density(num_bits, zero_density));
        auto bits = gen_result.view;
        const auto &one_indices = gen_result.zero_indices;

        auto stats = yaef::details::bits64::stats_bits(bits);
        REQUIRE(stats.num_zeros() == one_indices.size());

        std::allocator<uint8_t> alloc;
        yaef::details::selectable_dense_bits selectable_bits{alloc, bits, stats};
        gen_result.mem.release(); // Ownership is transferred to selectable_dense_bits.
        YAEF_DEFER {
            selectable_bits.deallocate(alloc);
        };
        
        for (size_t i = 0; i < one_indices.size(); ++i) {
            auto actual = selectable_bits.select_zero(i);
            auto expected = one_indices[i];
            REQUIRE(actual == expected);
        }
    }

    SECTION("select bit-one positions when bitmap is small") {
        const size_t num_zeros = 1000;
        const size_t num_ones = GENERATE(1, 2, 5, 64, 65, 128, 4095, 4096, 4097, 8192);

        using gen_param = yaef::utils::bit_generator::param;
        yaef::utils::bit_generator gen{yaef::utils::make_random_seed()};
        auto gen_result = gen.make_bits_with_one_indices(gen_param::by_size(num_zeros, num_ones));
        auto bits = gen_result.view;
        const auto &one_indices = gen_result.one_indices;

        auto stats = yaef::details::bits64::stats_bits(bits);
        REQUIRE(stats.num_ones() == one_indices.size());

        std::allocator<uint8_t> alloc;
        yaef::details::selectable_dense_bits selectable_bits{alloc, bits, stats};
        gen_result.mem.release(); // Ownership is transferred to selectable_dense_bits.
        YAEF_DEFER {
            selectable_bits.deallocate(alloc);
        };

        for (size_t i = 0; i < one_indices.size(); ++i) {
            auto actual = selectable_bits.select_one(i);
            auto expected = one_indices[i];
            REQUIRE(actual == expected);
        }
    }
}
