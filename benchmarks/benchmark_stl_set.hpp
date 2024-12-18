#pragma once

#include <set>

#include "common.hpp"

template<typename IntT>
class stl_set_benchmark : public benchmark<IntT, stl_set_benchmark<IntT>> {
    using base_type = benchmark<IntT, stl_set_benchmark<IntT>>;
public:
    using typename base_type::int_type;
    using typename base_type::size_type;

public:
    const char *name() const noexcept {
        return "stl_set";
    }

    // Assume that each node of an STL implementation of a Red-Black Tree contains 3 pointers and a bool variable.
    size_type size_in_bytes() const noexcept {
        const size_type node_size = sizeof(void *) * 3 + sizeof(bool) + sizeof(int_type);
        return node_size * set_.size();
    }

    void build(const int_type *values, size_type size) {
        set_ = std::multiset<int_type>{values, values + size};
    }
    
    void sequentially_access() {
        for (auto iter = set_.begin(); iter != set_.end(); ++iter) {
            int_type val = *iter;
            dont_optimize(val);
        }
    }

    void lower_bound(const int_type *targets, size_type size) {
        for (size_type i = 0; i < size; ++i) {
            auto iter = set_.lower_bound(targets[i]);
            dont_optimize(iter);
        }
    }

    void upper_bound(const int_type *targets, size_type size) {
        for (size_type i = 0; i < size; ++i) {
            auto iter = set_.upper_bound(targets[i]);
            dont_optimize(iter);
        }
    }

private:
    std::multiset<int_type> set_;
};