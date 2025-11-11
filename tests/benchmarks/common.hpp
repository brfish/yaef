#ifndef __EF_BENCHMARK_COMMON_HPP__
#define __EF_BENCHMARK_COMMON_HPP__
#pragma once

#include <chrono>
#include <iostream>
#include <iomanip>
#include <ratio>

#include "utils/int_generator.hpp"

#if defined(_MSC_VER) && !defined(__clang__) // exclude clang-cl
#pragma optimize("", off)
inline void dont_optimize_away(const void *) noexcept { }
#pragma optimize("", on)

template<typename T>
inline void dont_optimize(T &value) noexcept {
    dont_optimize_away(&value);
}
#else
template<typename T>
inline void dont_optimize(T &value) noexcept {
#if defined(__clang__)
    asm volatile("" : "+r,m"(value) : : "memory");
#else
    asm volatile("" : "+m,r"(value) : : "memory");
#endif
}
#endif

enum class timeunit {
    nanoseconds,
    milliseconds,
    seconds
};

inline const char *timeunit_suffix(timeunit unit) {
    switch (unit) {
    default                     :
    case timeunit::nanoseconds  : return "ns";
    case timeunit::milliseconds : return "ms";
    case timeunit::seconds      : return "s";
    }
}

template<typename IntT>
struct benchmark_inputs {
    using int_type  = IntT;
    using size_type = size_t;

    int_type               min, max;
    std::vector<int_type>  values;
    std::vector<size_type> random_indices;
    std::vector<size_type> shuffled_indices;
    std::vector<int_type>  search_targets;

    static benchmark_inputs from_values(std::vector<int_type> &&values, uint64_t seed) {
        const size_type num = values.size();
        const int_type min = values.front(), max = values.back();

        yaef::test_utils::uniform_int_generator<size_type> random_indices_gen{0, num - 1, seed};
        auto random_indices = random_indices_gen.make_list(num);
        
        std::mt19937_64 tmp_rng(seed);
        std::vector<size_type> shuffled_indices(num);
        std::iota(shuffled_indices.begin(), shuffled_indices.end(), 0);
        std::shuffle(shuffled_indices.begin(), shuffled_indices.end(), tmp_rng);

        yaef::test_utils::uniform_int_generator<int_type> search_targets_gen{min, max, seed};
        auto search_targets = search_targets_gen.make_list(num / 2);
        return benchmark_inputs{min, max, std::move(values), std::move(random_indices), 
                                std::move(shuffled_indices), std::move(search_targets)};
    }

    static benchmark_inputs from_values(std::vector<int_type> &&values) {
        return from_values(std::move(values), yaef::test_utils::make_random_seed());
    }

    static benchmark_inputs from_values(const std::vector<int_type> &values, uint64_t seed) {
        return from_values(std::vector<int_type>{values}, seed);
    }

    static benchmark_inputs from_values(const std::vector<int_type> &values) {
        return from_values(std::vector<int_type>{values});
    }

    static benchmark_inputs from_datagen(int_type min, int_type max, size_type num, uint64_t seed) {
        yaef::test_utils::uniform_int_generator<int_type> values_gen{min, max, seed};
        auto values = values_gen.make_sorted_list(num);
        return from_values(std::move(values));
    }

    static benchmark_inputs from_datagen(int_type min, int_type max, size_type num) {
        return from_datagen(min, max, num, yaef::test_utils::make_random_seed());
    }

    static benchmark_inputs from_dategen_unique(int_type min, int_type max, size_type num, uint64_t seed) {
        yaef::test_utils::uniform_int_generator<int_type> values_gen{min, max, seed};
        auto values = values_gen.make_sorted_set(num);
        return from_values(std::move(values));
    }

    static benchmark_inputs from_datagen_unique(int_type min, int_type max, size_type num) {
        return from_datagen_unique(min, max, num, yaef::test_utils::make_random_seed());
    }
};

template<typename IntT, typename Impl>
class benchmark {
    using clock_type = std::chrono::steady_clock;
    using time_point = clock_type::time_point;
    using duration   = clock_type::duration;
public:
    using int_type   = IntT;
    using size_type  = size_t;

public:
    void run(const benchmark_inputs<int_type> &data) {
        build_.name = "build";
        measure_time(build_, data.values.size(), [&]() {
            static_cast<Impl *>(this)->build(data.values.data(), data.values.size());
        });
        
        random_access_.name = "random_access";
        measure_time(random_access_, data.random_indices.size(), [&]() {
            static_cast<Impl *>(this)->random_access(data.random_indices.data(), data.random_indices.size());
        });

        sequentially_access_.name = "sequentially_access";
        measure_time(sequentially_access_, data.values.size(), [&]() {
            static_cast<Impl *>(this)->sequentially_access();
        });
        
        lower_bound_.name = "lower_bound";
        measure_time(lower_bound_, data.search_targets.size(), [&]() {
            static_cast<Impl *>(this)->lower_bound(data.search_targets.data(), data.search_targets.size());
        });

        upper_bound_.name = "upper_bound";
        measure_time(upper_bound_, data.search_targets.size(), [&]() {
            static_cast<Impl *>(this)->upper_bound(data.search_targets.data(), data.search_targets.size());
        });

        size_in_bytes_ = static_cast<const Impl *>(this)->size_in_bytes();
    }

    void report() const {
        std::cout << "===================================\n"
                  << "[" << static_cast<const Impl *>(this)->name() << "]\n";
        report_print_result(build_, timeunit::nanoseconds, "int");
        report_print_result(random_access_, timeunit::nanoseconds, "int");
        report_print_result(sequentially_access_, timeunit::nanoseconds, "int");
        report_print_result(lower_bound_, timeunit::nanoseconds, "target");
        report_print_result(upper_bound_, timeunit::nanoseconds, "target");

        const double compression_ratio = static_cast<double>(size_in_bytes_) / 
                                         (sizeof(int_type) * sequentially_access_.num) * 100.0;
        report_print_row("size_in_bytes", size_in_bytes_, "B");
        report_print_row("compression_ratio", compression_ratio, "%");
        report_print_row("bps", static_cast<double>(size_in_bytes_ * 8) / sequentially_access_.num, "bits/int");
    }

    void random_access(const size_type *, size_type) {
        random_access_.enabled = false;
    }
    
    void sequentially_access() {
        sequentially_access_.enabled = false;
    }

    void lower_bound(const int_type *, size_type) {
        lower_bound_.enabled = false;
    }

    void upper_bound(const int_type *, size_type) {
        upper_bound_.enabled = false;
    }

private:
    struct benchmark_result {
        bool        enabled = true;
        const char *name;
        size_type   num;
        duration    total_time;

        double average_time(timeunit unit) const noexcept {
            switch (unit) {
            default                     :
            case timeunit::nanoseconds  : return average_time_impl<std::nano>();
            case timeunit::milliseconds : return average_time_impl<std::milli>();
            case timeunit::seconds      : return average_time_impl<std::ratio<1>>();
            }
        }

    private:
        template<typename R>
        double average_time_impl() const noexcept {
            using duration_type = std::chrono::duration<double, R>;
            return std::chrono::duration_cast<duration_type>(total_time).count() / static_cast<double>(num);
        }
    };

    benchmark_result build_;
    benchmark_result random_access_;
    benchmark_result sequentially_access_;
    benchmark_result lower_bound_;
    benchmark_result upper_bound_;
    size_type        size_in_bytes_;

    template<typename F>
    static void measure_time(benchmark_result &out, size_type num, F f) {
        time_point start = clock_type::now();
        f();
        time_point end = clock_type::now();
        out.num = num;
        out.total_time = end - start;
    }

    template<typename T>
    static void report_print_row(const char *name, T val, const char *suffix) {
        std::cout << std::left << std::setw(22) << name;
        if (std::is_floating_point<T>::value) {
            std::cout << std::fixed << std::setprecision(3) << val << ' ' << suffix << '\n';
        } else {
            std::cout << val << ' ' << suffix << '\n';
        }
    }

    static void report_print_result(const benchmark_result &res, timeunit unit, const char *denom_str) {
        if (res.enabled) {
            std::string suffix = std::string{timeunit_suffix(unit)} + "/" + denom_str;
            report_print_row(res.name, res.average_time(unit), suffix.c_str());
        } else {
            report_print_row(res.name, "not_supported", "");
        }
    }
};

#endif