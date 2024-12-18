#pragma once

#include <map>
#include "common.hpp"

#include "yaef/yaef.hpp"

template<typename IntT>
class eliasfano_list_benchmark : public benchmark<IntT, eliasfano_list_benchmark<IntT>> {
    using base_type = benchmark<IntT, eliasfano_list_benchmark<IntT>>;
public:
    using typename base_type::int_type;
    using typename base_type::size_type;

public:
    const char *name() const noexcept {
        return "eliasfano_list";
    }

    size_type size_in_bytes() const noexcept {
        return list_.space_usage_in_bytes();
    }

    void build(const int_type *values, size_type size) {
        list_ = yaef::eliasfano_list<int_type>{yaef::from_sorted, values, values + size};
        /*auto stats = yaef::details::bits64::stats_bits(list_.get_high_bits().get_bits());
        std::cout << "ones: " << stats.num_ones() << "\n"
                  << "zeros: " << stats.num_zeros() << "\n"
                  << "one_density: " << stats.one_density() * 100.0 << "%\n";
        auto bits = list_.get_high_bits().get_bits();
        std::map<uint32_t, size_t> hist;
        for (size_t i = 0; i < bits.num_blocks(); ++i) {
            ++hist[std::popcount(bits.blocks()[i])];
        }
        for (const auto &[k, v] : hist) {
            std::cout << "popcnt = " << k << ": " << v << "\n";
        }*/
    }

    void random_access(const size_type *indices, size_type size) {
        for (size_type i = 0; i < size; ++i) {
            int_type val = list_[indices[i]];
            dont_optimize(val);
        }
    }
    
    void sequentially_access() {
        auto iter = list_.begin();
        for (size_type i = 0; i < list_.size(); ++i, ++iter) {
            int_type val = *iter;
            dont_optimize(val);
        }
    }

    void lower_bound(const int_type *targets, size_type size) {
        for (size_type i = 0; i < size; ++i) {
            auto iter = list_.lower_bound(targets[i]);
            dont_optimize(iter);
        }
    }

    void upper_bound(const int_type *targets, size_type size) {
        for (size_type i = 0; i < size; ++i) {
            auto iter = list_.upper_bound(targets[i]);
            dont_optimize(iter);
        }
    }

private:
    yaef::eliasfano_list<int_type> list_;
};
