#pragma once

#include "common.hpp"

#include "yaef/yaef.hpp"

template<typename IntT>
class cardinality_sparse_sampled_list_benchmark 
    : public benchmark<IntT, cardinality_sparse_sampled_list_benchmark<IntT>> {
    using base_type = benchmark<IntT, cardinality_sparse_sampled_list_benchmark<IntT>>;

    using list_type = yaef::sparse_sampled_list<IntT,
        yaef::sample_strategy::cardinality
    >;
public:
    using typename base_type::int_type;
    using typename base_type::size_type;

public:
    const char *name() const noexcept {
        return "cardinality_sparse_sampled_list";
    }

    size_type size_in_bytes() const noexcept {
        return list_.space_usage_in_bytes();
    }

    void build(const int_type *values, size_type size) {
        list_ = list_type{yaef::from_sorted, values, values + size};
    }

    void random_access(const size_type *indices, size_type size) {
        for (size_type i = 0; i < size; ++i) {
            int_type val = list_[indices[i]];
            dont_optimize(val);
        }
    }
    
    void sequentially_access() {
        auto iter = list_.data();
        for (size_type i = 0; i < list_.size(); ++i, ++iter) {
            int_type val = *iter;
            dont_optimize(val);
        }
    }

    void lower_bound(const int_type *targets, size_type size) {
        for (size_type i = 0; i < size; ++i) {
            auto iter = list_.index_of_lower_bound(targets[i]);
            dont_optimize(iter);
        }
    }

    void upper_bound(const int_type *targets, size_type size) {
        for (size_type i = 0; i < size; ++i) {
            auto iter = list_.index_of_upper_bound(targets[i]);
            dont_optimize(iter);
        }
    }

private:
    list_type list_;
};

template<typename IntT>
class universe_sparse_sampled_list_benchmark 
    : public benchmark<IntT, universe_sparse_sampled_list_benchmark<IntT>> {
    using base_type = benchmark<IntT, universe_sparse_sampled_list_benchmark<IntT>>;

    using list_type = yaef::sparse_sampled_list<IntT,
        yaef::sample_strategy::universe
    >;
public:
    using typename base_type::int_type;
    using typename base_type::size_type;

public:
    const char *name() const noexcept {
        return "universe_sparse_sampled_list";
    }

    size_type size_in_bytes() const noexcept {
        return list_.space_usage_in_bytes();
    }

    void build(const int_type *values, size_type size) {
        list_ = list_type{yaef::from_sorted, values, values + size};
    }

    void random_access(const size_type *indices, size_type size) {
        for (size_type i = 0; i < size; ++i) {
            int_type val = list_[indices[i]];
            dont_optimize(val);
        }
    }
    
    void sequentially_access() {
        auto iter = list_.data();
        for (size_type i = 0; i < list_.size(); ++i, ++iter) {
            int_type val = *iter;
            dont_optimize(val);
        }
    }

    void lower_bound(const int_type *targets, size_type size) {
        for (size_type i = 0; i < size; ++i) {
            auto iter = list_.index_of_lower_bound(targets[i]);
            dont_optimize(iter);
        }
    }

    void upper_bound(const int_type *targets, size_type size) {
        for (size_type i = 0; i < size; ++i) {
            auto iter = list_.index_of_upper_bound(targets[i]);
            dont_optimize(iter);
        }
    }

private:
    list_type list_;
};
