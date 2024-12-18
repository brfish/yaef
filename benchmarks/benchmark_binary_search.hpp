#pragma once

#include <algorithm>
#include <memory>

#include "common.hpp"

template<typename IntT>
class binary_search_benchmark : public benchmark<IntT, binary_search_benchmark<IntT>> {
    using base_type = benchmark<IntT, binary_search_benchmark<IntT>>;
public:
    using typename base_type::int_type;
    using typename base_type::size_type;

public:
    const char *name() const noexcept {
        return "binary_search";
    }

    size_type size_in_bytes() const noexcept {
        return size_ * sizeof(int_type);
    }

    void build(const int_type *values, size_type size) {
        values_ = std::unique_ptr<int_type[]>(new int_type[size]);
        size_ = size;
        std::copy_n(values, size, values_.get());
    }

    void random_access(const size_type *indices, size_type size) {
        for (size_type i = 0; i < size; ++i) {
            int_type val = values_[indices[i]];
            dont_optimize(val);
        }
    }
    
    void sequentially_access() {
        for (size_type i = 0; i < size_; ++i) {
            int_type val = values_[i];
            dont_optimize(val);
        }
    }

    void lower_bound(const int_type *targets, size_type size) {
        for (size_type i = 0; i < size; ++i) {
            auto iter = std::lower_bound(values_.get(), values_.get() + size_, targets[i]);
            dont_optimize(iter);
        }
    }

    void upper_bound(const int_type *targets, size_type size) {
        for (size_type i = 0; i < size; ++i) {
            auto iter = std::upper_bound(values_.get(), values_.get() + size_, targets[i]);
            dont_optimize(iter);
        }
    }

private:
    std::unique_ptr<int_type[]> values_;
    size_type                   size_;
};

template<typename IntT>
class branchless_binary_search_benchmark : public benchmark<IntT, branchless_binary_search_benchmark<IntT>> {
    using base_type = benchmark<IntT, branchless_binary_search_benchmark<IntT>>;
public:
    using typename base_type::int_type;
    using typename base_type::size_type;

public:
    const char *name() const noexcept {
        return "branchless_binary_search";
    }

    size_type size_in_bytes() const noexcept {
        return size_ * sizeof(int_type);
    }

    void build(const int_type *values, size_type size) {
        values_ = std::unique_ptr<int_type[]>(new int_type[size]);
        size_ = size;
        std::copy_n(values, size, values_.get());
    }

    void random_access(const size_type *indices, size_type size) {
        for (size_type i = 0; i < size; ++i) {
            int_type val = values_[indices[i]];
            dont_optimize(val);
        }
    }
    
    void sequentially_access() {
        for (size_type i = 0; i < size_; ++i) {
            int_type val = values_[i];
            dont_optimize(val);
        }
    }

    void lower_bound(const int_type *targets, size_type size) {
        for (size_type i = 0; i < size; ++i) {
            auto iter = branchless_lower_bound(targets[i]);
            dont_optimize(iter);
        }
    }

    void upper_bound(const int_type *targets, size_type size) {
        for (size_type i = 0; i < size; ++i) {
            auto iter = branchless_upper_bound(targets[i]);
            dont_optimize(iter);
        }
    }

private:
    std::unique_ptr<int_type[]> values_;
    size_type                   size_;

    int_type *branchless_lower_bound(int_type target) const {
        int_type *base = values_.get();
        size_type len = size_;
        while (len > 0) {
            size_type half = len / 2;
            //len -= half;
            //__builtin_prefetch(&base[len / 2 - 1]);
            //__builtin_prefetch(&base[len / 2 - 1 + half]);
            base += (base[half] < target) * (len - half);
            len = half;
        }
        return base;
    }

    int_type *branchless_upper_bound(int_type target) const {
        int_type *base = values_.get();
        size_type len = size_;
        while (len > 0) {
            size_type half = len / 2;
            //len -= half;
            //__builtin_prefetch(&base[len / 2 - 1]);
            //__builtin_prefetch(&base[len / 2 - 1 + half]);
            base += (base[half] <= target) * (len - half);
            len = half;
        }
        return base;
    }
};