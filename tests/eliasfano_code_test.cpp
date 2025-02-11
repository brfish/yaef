#include "catch2/catch_test_macros.hpp"

#include "yaef/yaef.hpp"

#include "utils/int_generator.hpp"

#if 0
template<typename T>
struct eliasfano_stat_info {
    T        min;
    T        max;
    size_t   num;
    uint32_t opt_low_width;
    size_t   num_low_bits;
    size_t   num_high_zeros;
    size_t   num_high_ones;
    size_t   num_high_bits;
};

template<typename RandAccessIterT>
eliasfano_stat_info<typename std::iterator_traits<RandAccessIterT>::value_type>
create_eliasfano_stat_info(RandAccessIterT first, RandAccessIterT last) {
    using info_type = eliasfano_stat_info<typename std::iterator_traits<RandAccessIterT>::value_type>;
    info_type info;
    info.min = *first;
    info.max = *std::prev(last);
    info.num = std::distance(first, last);
    info.opt_low_width = yaef::details::bits64::bit_width((info.max - info.min) / info.num);
    info.num_low_bits = info.opt_low_width * info.num;
    info.num_high_zeros = ((info.max - info.min) >> info.opt_low_width) + 1;
    info.num_high_ones = info.num;
    info.num_high_bits = info.num_high_zeros + info.num_high_ones;
    return info; 
}

TEST_CASE("elias-fano encoding", "[public]") {
    SECTION("encode unsigned integers") {
        yaef::utils::uniform_int_generator<uint64_t> gen{std::numeric_limits<uint16_t>::max(), 
                                                       std::numeric_limits<uint32_t>::max(),
                                                       yaef::utils::make_random_seed()};
        auto ints = gen.make_sorted_list(80000);
        std::unique_ptr<uint64_t> low_bits_buf, high_bits_buf;
        auto res = yaef::encode_eliasfano(ints.begin(), ints.end(), high_bits_buf.get(), 0, low_bits_buf.get(), 0);
        REQUIRE(res);

        auto stat_info = create_eliasfano_stat_info(ints.begin(), ints.end());
        REQUIRE(stat_info.num_high_bits == res.num_high_bits);
        REQUIRE(stat_info.num_low_bits == res.num_low_bits);
    }
    SECTION("encode signed integers") {
        yaef::utils::uniform_int_generator<int64_t> gen{std::numeric_limits<int16_t>::max(), 
                                                      std::numeric_limits<int32_t>::max(),
                                                      yaef::utils::make_random_seed()};
        auto ints = gen.make_sorted_list(80000);
        std::unique_ptr<uint64_t> low_bits_buf, high_bits_buf;
        auto res = yaef::encode_eliasfano(ints.begin(), ints.end(), high_bits_buf.get(), 0, low_bits_buf.get(), 0);
        REQUIRE(res);

        auto stat_info = create_eliasfano_stat_info(ints.begin(), ints.end());
        REQUIRE(stat_info.num_high_bits == res.num_high_bits);
        REQUIRE(stat_info.num_low_bits == res.num_low_bits);
    }
}

TEST_CASE("elias-fano decoding", "[public]") {

}
#endif