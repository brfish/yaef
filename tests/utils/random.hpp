#ifndef __YAEF_UTILS_RANDOM_HPP__
#define __YAEF_UTILS_RANDOM_HPP__
#pragma once

#include <limits>
#include <random>
#include <type_traits>

namespace yaef {
namespace test_utils {

[[nodiscard]] inline uint64_t make_random_seed() {
    static thread_local std::random_device rd;
    return rd();
}

namespace details{

template<typename T>
using integer_dist_is_well_defined = std::integral_constant<bool,
    std::is_same<typename std::make_unsigned<T>::type, unsigned short>::value ||
    std::is_same<typename std::make_unsigned<T>::type, unsigned int>::value ||
    std::is_same<typename std::make_unsigned<T>::type, unsigned long>::value ||
    std::is_same<typename std::make_unsigned<T>::type, unsigned long long>::value
>;

template<typename T>
using uniform_dist_type = typename std::conditional<integer_dist_is_well_defined<T>::value,
    std::uniform_int_distribution<T>,
    std::uniform_int_distribution<
        typename std::conditional<std::is_unsigned<T>::value, unsigned long long, long long>::type
    >>::type;

template<typename T>
using discrete_dist_type = typename std::conditional<integer_dist_is_well_defined<T>::value,
    std::discrete_distribution<T>,
    std::discrete_distribution<
        typename std::conditional<std::is_unsigned<T>::value, unsigned long long, long long>::type
    >>::type;

}

template<typename T>
inline T random(T min = std::numeric_limits<T>::min(), T max = std::numeric_limits<T>::max()) {
    using dist_impl_type = test_utils::details::uniform_dist_type<T>;
    using dist_result_type = typename dist_impl_type::result_type;
    using param = typename dist_impl_type::param_type;
    
    static std::mt19937_64 rng{make_random_seed()};
    static dist_impl_type dist;
    return dist(rng, param{static_cast<dist_result_type>(min), 
                           static_cast<dist_result_type>(max)});
}

template<typename T>
inline T safe_random(T min = std::numeric_limits<T>::min(), T max = std::numeric_limits<T>::max()) {
    using dist_impl_type = test_utils::details::uniform_dist_type<T>;
    using dist_result_type = typename dist_impl_type::result_type;
    using param = typename dist_impl_type::param_type;

    static thread_local std::mt19937_64 rng{make_random_seed()};
    static thread_local dist_impl_type dist;
    return dist(rng, param{static_cast<dist_result_type>(min), 
                           static_cast<dist_result_type>(max)});
}

} // namespace utils
} // namespace yaef

#endif