#pragma once

#include "common.hpp"

#include "yaef/yaef.hpp"

template<typename IntT>
class eliasfano_sequence_benchmark : public benchmark<IntT, eliasfano_sequence_benchmark<IntT>> {
    using base_type = benchmark<IntT, eliasfano_sequence_benchmark<IntT>>;
public:
    using typename base_type::int_type;
    using typename base_type::size_type;

public:
    const char *name() const noexcept {
        return "eliasfano_sequence";
    }

    size_type size_in_bytes() const noexcept {
        return seq_.space_usage_in_bytes();
    }

    void build(const int_type *values, size_type size) {
        seq_ = yaef::eliasfano_sequence<int_type>{yaef::from_sorted, values, values + size};
    }
    
    void sequentially_access() {
        auto iter = seq_.begin();
        for (size_type i = 0; i < seq_.size(); ++i, ++iter) {
            int_type val = *iter;
            dont_optimize(val);
        }
    }

private:
    yaef::eliasfano_sequence<int_type> seq_;
};
