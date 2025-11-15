#include "catch2/catch_test_macros.hpp"

#include "yaef/yaef.hpp"

#include "utils/int_generator.hpp"

using hyb_list = yaef::hybrid_list<uint32_t, yaef::details::aligned_allocator<uint8_t, 32>,
    yaef::hybrid_methods::linear,
    yaef::hybrid_methods::bitmap,
    yaef::hybrid_methods::fixed,
    yaef::hybrid_methods::eliasfano,
    yaef::hybrid_methods::eliasgamma_unique_gap
>;

TEST_CASE("hybrid_list_test", "[public]") {
    yaef::test_utils::uniform_int_generator<uint32_t> int_gen(0, 25000 * 80, 114514);
    auto data = int_gen.make_sorted_set(25000 * 20);

    for (size_t i = 0; i < 256; ++i) {
        data[i] = i;
    }
    for (size_t i = 256; i < 512; ++i) {
        data[i] = 256 + (i - 256) * 2;
    }
    for (size_t i = 512; i < data.size(); ++i) {
        data[i] += 1024;
    }
    
    SECTION("random access") {
        auto list = hyb_list(yaef::from_sorted, data.begin(), data.end());

        for (size_t i = 0; i < data.size(); ++i) {
            uint32_t expected = data[i];
            uint32_t actual = list[i];
            REQUIRE(expected == actual);
        }
    }

    SECTION("index of lower bound") {
        auto list = hyb_list(yaef::from_sorted, data.begin(), data.end());

        uint32_t rnd_min = data.front() >= 50 ? data.front() - 50 : 0;
        uint32_t rnd_max = data.back() <= std::numeric_limits<uint32_t>::max() - 50 ? data.back() + 50 :
            std::numeric_limits<uint32_t>::max();

        for (size_t i = 0; i < data.size(); ++i) {
            uint32_t target = yaef::test_utils::random(rnd_min, rnd_max);
            size_t expected = std::lower_bound(data.begin(), data.end(), target) - data.begin();
            size_t actual = list.index_of_lower_bound(target);
            REQUIRE(expected == actual);
        }
    }
}