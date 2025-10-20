#pragma once

#include <algorithm>

#include "common.hpp"
#include "yaef/yaef.hpp"

template<typename IntT>
class packed_binary_search_benchmark : public benchmark<IntT, packed_binary_search_benchmark<IntT>> {
    using base_type = benchmark<IntT, packed_binary_search_benchmark<IntT>>;
public:
    using typename base_type::int_type;
    using typename base_type::size_type;

public:
    const char *name() const noexcept {
        return "packed_binary_search";
    }

    size_type size_in_bytes() const noexcept {
        return buf_.space_usage_in_bytes();
    }

    void build(const int_type *values, size_type size) {
        int_type max_val = *std::max_element(values, values + size);
        size_type width = yaef::details::bits64::bit_width(max_val);

        buf_ = yaef::packed_int_buffer<>(width, size);
        for (size_type i = 0; i < size; ++i) {
            buf_.set_value(i, values[i]);
        }
    }

    void random_access(const size_type *indices, size_type size) {
        for (size_type i = 0; i < size; ++i) {
            int_type val = buf_[indices[i]];
            dont_optimize(val);
        }
    }
    
    void sequentially_access() {
        for (size_type i = 0; i < buf_.size(); ++i) {
            int_type val = buf_[i];
            dont_optimize(val);
        }
    }

    void lower_bound(const int_type *targets, size_type size) {
        for (size_type i = 0; i < size; ++i) {
            auto iter = do_lower_bound(targets[i]);
            dont_optimize(iter);
        }
    }

    void upper_bound(const int_type *targets, size_type size) {
        for (size_type i = 0; i < size; ++i) {
            auto iter = do_upper_bound(targets[i]);
            dont_optimize(iter);
        }
    }

private:
    yaef::packed_int_buffer<> buf_;

    size_type do_lower_bound(int_type target) const {
        size_type iter, first = 0;
        ssize_t count = buf_.size(), step;
        while (count > 0) {
            iter = first;
            step = count / 2;
            iter += step;
            if (buf_.get_value(iter) < target) {
                first = ++iter;
                count -= step + 1;
            } else {
                count = step;
            }
        }
        return first;
    }

    size_type do_upper_bound(int_type target) const {
        size_type iter, first = 0;
        ssize_t count = buf_.size(), step;
        while (count > 0) {
            iter = first;
            step = count / 2;
            iter += step;
            if (target >= buf_.get_value(iter)) {
                first = ++iter;
                count -= step + 1;
            } else {
                count = step;
            }
        }
        return first;
    }
};

template<typename IntT>
class branchless_packed_binary_search_benchmark 
    : public benchmark<IntT, branchless_packed_binary_search_benchmark<IntT>> {
    using base_type = benchmark<IntT, branchless_packed_binary_search_benchmark<IntT>>;
public:
    using typename base_type::int_type;
    using typename base_type::size_type;

public:
    const char *name() const noexcept {
        return "branchless_packed_binary_search";
    }

    size_type size_in_bytes() const noexcept {
        return buf_.space_usage_in_bytes();
    }

    void build(const int_type *values, size_type size) {
        int_type max_val = *std::max_element(values, values + size);
        size_type width = yaef::details::bits64::bit_width(max_val);

        buf_ = yaef::packed_int_buffer<>(width, size);
        for (size_type i = 0; i < size; ++i) {
            buf_.set_value(i, values[i]);
        }
    }

    void random_access(const size_type *indices, size_type size) {
        for (size_type i = 0; i < size; ++i) {
            int_type val = buf_[indices[i]];
            dont_optimize(val);
        }
    }
    
    void sequentially_access() {
        for (size_type i = 0; i < buf_.size(); ++i) {
            int_type val = buf_[i];
            dont_optimize(val);
        }
    }

    void lower_bound(const int_type *targets, size_type size) {
        for (size_type i = 0; i < size; ++i) {
            auto idx = branchless_lower_bound(targets[i]);
            dont_optimize(idx);
        }
    }

    void upper_bound(const int_type *targets, size_type size) {
        for (size_type i = 0; i < size; ++i) {
            auto idx = branchless_upper_bound(targets[i]);
            dont_optimize(idx);
        }
    }

private:
    yaef::packed_int_buffer<> buf_;

    size_type branchless_lower_bound(int_type target) const {
        size_type base = 0;
        size_type len = buf_.size();
        while (len > 0) {
            size_type half = len / 2;
            base += (buf_.get_value(base + half) < target) * (len - half);
            len = half;
        }
        return base;
    }

    size_type branchless_upper_bound(int_type target) const {
        size_type base = 0;
        size_type len = buf_.size();
        while (len > 0) {
            size_type half = len / 2;
            base += (buf_.get_value(base + half) <= target) * (len - half);
            len = half;
        }
        return base;
    }
};