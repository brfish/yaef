#include "yaef/yaef.hpp"

#include "common.hpp"

#define ENABLE_RANDOM_ACCESS     1
#define ENABLE_SEQ_ACCESS        1
#define ENABLE_LOWER_BOUND       1
#define ENABLE_UPPER_BOUND       1
#define ENABLE_LOWER_BOUND_INDEX 1
#define ENABLE_UPPER_BOUND_INDEX 1

constexpr size_t NUM_REPEATS = 20;

template<typename IntT>
benchmark_inputs<IntT> generate_dense(size_t num) {
    std::vector<IntT> vec(num);
    std::iota(vec.begin(), vec.end(), std::numeric_limits<IntT>::min());
    return benchmark_inputs<IntT>::from_values(vec, 114514);
}

template<typename IntT>
benchmark_inputs<IntT> generate_random(size_t num, IntT min = std::numeric_limits<IntT>::min(),
                                       IntT max = std::numeric_limits<IntT>::max()) {
    return benchmark_inputs<IntT>::from_datagen(min, max, num);
}

template<size_t NUM_INTS, typename IntT>
void run_benchmark(const benchmark_inputs<IntT> &inputs) {
    using int_type = IntT;
    std::chrono::steady_clock::time_point timer_beg, timer_end;

    timer_beg = std::chrono::steady_clock::now();
    yaef::eliasfano_list<int_type> list{yaef::from_sorted, inputs.values.begin(), inputs.values.end()};
    timer_end = std::chrono::steady_clock::now();
    double build_time = std::chrono::duration_cast<std::chrono::nanoseconds>(timer_end - timer_beg).count();
    std::cout << "build_time: " << std::fixed << std::setprecision(3) 
              << build_time / NUM_INTS << "ns/int\n";

    std::cout << "compression_ratio: " << std::fixed << std::setprecision(3)
              << static_cast<double>(list.space_usage_in_bytes()) / (sizeof(int_type) * NUM_INTS) * 100.0 << "%\n";

#if ENABLE_RANDOM_ACCESS
    {
        double time = 0.0;
        for (size_t repeat = 0; repeat < NUM_REPEATS; ++repeat) {
            timer_beg = std::chrono::steady_clock::now();
            int_type dummy_sum = 0;
            for (size_t i = 0; i < inputs.shuffled_indices.size(); ++i) {
                dummy_sum += list[inputs.shuffled_indices[i]];
            }
            dont_optimize(dummy_sum);
            timer_end = std::chrono::steady_clock::now();
            time += std::chrono::duration_cast<std::chrono::nanoseconds>(timer_end - timer_beg).count();
        }
        time /= NUM_REPEATS;
        time /= inputs.shuffled_indices.size();
        std::cout << std::fixed << std::setprecision(3) << "random_access: " << time << " ns/int\n";
    }
#endif

#if ENABLE_SEQ_ACCESS
    {
        double time = 0.0;
        for (size_t repeat = 0; repeat < NUM_REPEATS; ++repeat) {
            timer_beg = std::chrono::steady_clock::now();
            int_type dummy_sum = 0;
            auto iter = list.begin();
            auto end = list.end();
            for (; iter != end; ++iter) {
                dummy_sum += *iter;
            }
            dont_optimize(dummy_sum);
            timer_end = std::chrono::steady_clock::now();
            time += std::chrono::duration_cast<std::chrono::nanoseconds>(timer_end - timer_beg).count();
        }
        time /= NUM_REPEATS;
        time /= list.size();
        std::cout << std::fixed << std::setprecision(3) << "sequentially_access: " << time << " ns/int\n";
    }
#endif

#if ENABLE_LOWER_BOUND
    {
        double time = 0.0;
        for (size_t repeat = 0; repeat < NUM_REPEATS; ++repeat) {
            timer_beg = std::chrono::steady_clock::now();
            int_type dummy_sum = 0;
            for (size_t i = 0; i < inputs.search_targets.size(); ++i) {
                dummy_sum += *list.lower_bound(inputs.search_targets[i]);
            }
            dont_optimize(dummy_sum);
            timer_end = std::chrono::steady_clock::now();
            time += std::chrono::duration_cast<std::chrono::nanoseconds>(timer_end - timer_beg).count();
        }
        time /= NUM_REPEATS;
        time /= inputs.search_targets.size();
        std::cout << std::fixed << std::setprecision(3) << "lower_bound: " << time << " ns/int\n";
    }
#endif

#if ENABLE_UPPER_BOUND
    {
        double time = 0.0;
        for (size_t repeat = 0; repeat < NUM_REPEATS; ++repeat) {
            timer_beg = std::chrono::steady_clock::now();
            int_type dummy_sum = 0;
            for (size_t i = 0; i < inputs.search_targets.size(); ++i) {
                dummy_sum += *list.upper_bound(inputs.search_targets[i]);
            }
            dont_optimize(dummy_sum);
            timer_end = std::chrono::steady_clock::now();
            time += std::chrono::duration_cast<std::chrono::nanoseconds>(timer_end - timer_beg).count();
        }
        time /= NUM_REPEATS;
        time /= inputs.search_targets.size();
        std::cout << std::fixed << std::setprecision(3) << "upper_bound: " << time << " ns/int\n";
    }
#endif

#if ENABLE_LOWER_BOUND_INDEX
    {
        double time = 0.0;
        for (size_t repeat = 0; repeat < NUM_REPEATS; ++repeat) {
            timer_beg = std::chrono::steady_clock::now();
            int_type dummy_sum = 0;
            for (size_t i = 0; i < inputs.search_targets.size(); ++i) {
                dummy_sum += list.index_of_lower_bound(inputs.search_targets[i]);
            }
            dont_optimize(dummy_sum);
            timer_end = std::chrono::steady_clock::now();
            time += std::chrono::duration_cast<std::chrono::nanoseconds>(timer_end - timer_beg).count();
        }
        time /= NUM_REPEATS;
        time /= inputs.search_targets.size();
        std::cout << std::fixed << std::setprecision(3) << "lower_bound_index: " << time << " ns/int\n";
    }
#endif

#if ENABLE_UPPER_BOUND_INDEX
    {
        double time = 0.0;
        for (size_t repeat = 0; repeat < NUM_REPEATS; ++repeat) {
            timer_beg = std::chrono::steady_clock::now();
            int_type dummy_sum = 0;
            for (size_t i = 0; i < inputs.search_targets.size(); ++i) {
                dummy_sum += list.index_of_upper_bound(inputs.search_targets[i]);
            }
            dont_optimize(dummy_sum);
            timer_end = std::chrono::steady_clock::now();
            time += std::chrono::duration_cast<std::chrono::nanoseconds>(timer_end - timer_beg).count();
        }
        time /= NUM_REPEATS;
        time /= inputs.search_targets.size();
        std::cout << std::fixed << std::setprecision(3) << "upper_bound_index: " << time << " ns/int\n";
    }
#endif
}

int main(void) {
    using int_type = uint64_t;
    constexpr size_t NUM_INTS = 5000000;

    std::cout << "============== random ==============\n";
    auto random_inputs = generate_random<int_type>(NUM_INTS, 0, std::numeric_limits<int_type>::max() / 100);
    run_benchmark<NUM_INTS>(random_inputs);

    std::cout << "============== dense ==============\n";
    auto dense_inputs = generate_dense<int_type>(NUM_INTS);
    run_benchmark<NUM_INTS>(dense_inputs);

    return 0;
}