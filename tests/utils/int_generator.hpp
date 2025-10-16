#ifndef __YAEF_UTILS_INT_GENERATOR_HPP__
#define __YAEF_UTILS_INT_GENERATOR_HPP__
#pragma once

#include <algorithm>
#include <cstdint>
#include <stdexcept>
#include <unordered_set>
#include <vector>

#include "random.hpp"

namespace yaef {
namespace test_utils {

template<typename IntT>
class int_generator {
public:
    using value_type    = IntT;
    using size_type     = size_t;
    using random_engine = std::mt19937_64;

public:
    int_generator(uint64_t seed = 114514)
        : rng_(seed), seed_(seed) { }

    virtual ~int_generator() = default;

    virtual value_type min() const = 0;
    virtual value_type max() const = 0;
    uint64_t seed() const noexcept { return seed_; }
    void set_seed(uint64_t s) { seed_ = s; rng_.seed(s); } 

    virtual std::vector<value_type> make_list(size_type num) = 0;
    virtual std::vector<value_type> make_set(size_type num) = 0;

    virtual std::vector<value_type> make_sorted_list(size_type num) {
        auto result = make_list(num);
        std::sort(result.begin(), result.end());
        return result;
    }

    virtual std::vector<value_type> make_sorted_set(size_type num) {
        auto result = make_set(num);
        std::sort(result.begin(), result.end());
        return result;    
    }

protected:
    random_engine rng_;
    uint64_t      seed_;
};

template<typename IntT>
class iterative_int_generator : public int_generator<IntT> {
    using base_type = int_generator<IntT>;
public:
    using typename base_type::value_type;
    using typename base_type::size_type;
    using typename base_type::random_engine;
public:
    iterative_int_generator(uint64_t seed = 114514)
        : base_type(seed) { }

    std::vector<value_type> make_list(size_type num) override {
        std::vector<value_type> result;
        result.resize(num);
        for (size_type i = 0; i < result.size(); ++i) {
            result[i] = do_next_value(this->rng_);
        }
        return result;
    }

    std::vector<value_type> make_set(size_type num) override {
        const size_type num_candidates = this->max() - this->min() + 1;
        if (num_candidates < num) {
            throw std::invalid_argument{
                "The size of the set must be less than or equal to the size of the value range."};
        }

        std::unordered_set<value_type> flags;
        std::vector<value_type> result;
        result.resize(num);
        for (size_type i = 0; i < result.size(); ++i) {
            value_type val = do_next_value(this->rng_);
            while (flags.find(val) != flags.end()) {
                val = do_next_value(this->rng_);
            }
            flags.insert(val);
            result[i] = val;
        }
        return result;
    }
protected:
    virtual value_type do_next_value(random_engine &rng) = 0;
};

template<typename IntT>
class uniform_int_generator : public iterative_int_generator<IntT> {
    using base_type = iterative_int_generator<IntT>;
public:
    using typename base_type::value_type;
    using typename base_type::size_type;
    using typename base_type::random_engine;

public:
    uniform_int_generator(value_type minv, value_type maxv, uint64_t seed)
        : base_type(seed), dist_impl_(minv, maxv) { }

    uniform_int_generator(value_type minv = std::numeric_limits<value_type>::min(), 
                          value_type maxv = std::numeric_limits<value_type>::max())
        : dist_impl_(minv, maxv) { }

    value_type min() const override { return dist_impl_.a(); }
    value_type max() const override { return dist_impl_.b(); }

    std::vector<value_type> make_permutation(size_type num) {
        using ssize_type = typename std::make_signed<size_type>::type;
        using param = typename dist_impl_type::param_type;
        dist_impl_type dist;

        std::vector<value_type> result(num);
        for (size_type i = 0; i < num; ++i) {
            result[i] = i;
        }

        for (ssize_type i = num - 1; i > 0; --i) {
            std::swap(result[i], result[dist(this->rng_, param(0, i))]);
        }
        return result;
    }

private:
    using dist_impl_type = test_utils::details::uniform_dist_type<value_type>;
    dist_impl_type dist_impl_;

    value_type do_next_value(random_engine &rng) override {
        return dist_impl_(rng);
    }
};

template<typename IntT>
class probability_sampled_int_generator : public iterative_int_generator<IntT> {
    using base_type = iterative_int_generator<IntT>;
public:
    using typename base_type::value_type;
    using typename base_type::size_type;
    using typename base_type::random_engine;

public:
    probability_sampled_int_generator(value_type min, value_type max)
        : min_(min), max_(max) { }
    
    probability_sampled_int_generator(value_type min, value_type max, uint64_t seed)
        : base_type(seed), min_(min), max_(max) { }


    value_type min() const override { return min_; }
    value_type max() const override { return max_; }

protected:
    virtual double cdf(double x) const = 0;

    void build_samples(double delta) {
        size_t n = max() - min() + 1;
        std::vector<double> weights(n);
        
        double start = delta * n + delta;
        for (size_t i = 0; i < n; ++i) {
            weights[i] = start + delta * i * 2.0;
        }
        for (size_t i = 0; i < n; ++i) {
            weights[i] = sample_p(weights[i], delta);
        }
        dist_impl_ = dist_type{weights.begin(), weights.end()};
    }

private:
    using dist_type = test_utils::details::discrete_dist_type<value_type>;

    dist_type dist_impl_;
    value_type min_, max_;

    value_type do_next_value(random_engine &rng) override {
        return dist_impl_(rng) + min();
    }

    double sample_p(double x, double delta) {
        return cdf(x + delta) - cdf(x - delta);
    }
};

template<typename IntT>
class normal_int_generator : public probability_sampled_int_generator<IntT> {
    using base_type = probability_sampled_int_generator<IntT>;
public:
    using typename base_type::value_type;
    using typename base_type::size_type;
    using typename base_type::random_engine;
public:
    normal_int_generator(value_type minv, value_type maxv, double stddev, uint64_t seed)
        : base_type(minv, maxv, seed), stddev_(stddev) {
        init();
    }
    
    normal_int_generator(value_type minv, value_type maxv, double stddev = 1.0)
        : base_type(minv, maxv), stddev_(stddev) {
        init();
    }

    double stddev() const noexcept { return stddev_; }

private:
    double stddev_;

    double cdf(double x) const override {
        const double INV_SQRT2 = 1.0 / std::sqrt(2.0);
        double v = INV_SQRT2 * (x - static_cast<double>(this->max() - this->min() + 1) * 0.5) / stddev();
        return 0.5 * (1.0 + std::erf(v));
    }

    void init() {
        size_type n = this->max() - this->min() + 1;
        double mean = static_cast<double>(n) * 0.5;
        double left = mean - static_cast<double>(stddev()) * 5;
        double right = mean + static_cast<double>(stddev()) * 5;
        double delta = (right - left) / static_cast<double>(n) * 0.5;
        this->build_samples(delta);
    }
};

template<typename IntT>
class zipf_int_generator : public iterative_int_generator<IntT> {
    using base_type = iterative_int_generator<IntT>;
public:
    using typename base_type::value_type;
    using typename base_type::size_type;
    using typename base_type::random_engine;
public:

private:
    value_type do_next_value(random_engine &rng) override;
};

} // namespace utils
} // namespace yaef

#endif