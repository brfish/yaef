#include <numeric>

#include "benchmark_binary_search.hpp"
#include "benchmark_eliasfano_list.hpp"
#include "benchmark_eliasfano_sequence.hpp"
#include "benchmark_packed_binary_search.hpp"
#include "benchmark_stl_set.hpp"

template<typename IntT>
benchmark_inputs<IntT> generate_dense(size_t num) {
    std::vector<IntT> vec(num);
    std::iota(vec.begin(), vec.end(), std::numeric_limits<IntT>::min());
    return benchmark_inputs<IntT>::from_values(vec);
}

template<typename IntT>
benchmark_inputs<IntT> generate_random(size_t num, IntT min = std::numeric_limits<IntT>::min(),
                                       IntT max = std::numeric_limits<IntT>::max()) {
    return benchmark_inputs<IntT>::from_datagen(min, max, num);
}

template<typename IntT>
void run_benchmarks(const benchmark_inputs<IntT> &inputs) {
#define REPORT_BENCHMARK(_name) \
    { _name<IntT> b; b.run(inputs); b.report(); }

    REPORT_BENCHMARK(binary_search_benchmark);
    REPORT_BENCHMARK(branchless_binary_search_benchmark);
    REPORT_BENCHMARK(packed_binary_search_benchmark);
    REPORT_BENCHMARK(branchless_packed_binary_search_benchmark);
    REPORT_BENCHMARK(eliasfano_list_benchmark);
    REPORT_BENCHMARK(eliasfano_sequence_benchmark);
    REPORT_BENCHMARK(stl_set_benchmark);
#undef REPORT_BENCHMARK
}

int main(int argc, char *argv[]) {
    (void)(argc);
    (void)(argv);
    using int_type = uint64_t;
    constexpr size_t NUM_INTS = 5000000;

    std::cout << "<<<<<<<<<< random >>>>>>>>>>\n";
    auto random_inputs = generate_random<int_type>(NUM_INTS, 0, NUM_INTS * 32);
    run_benchmarks(random_inputs);

    std::cout << "<<<<<<<<<< dense >>>>>>>>>>\n";
    auto dense_inputs = generate_dense<int_type>(NUM_INTS);
    run_benchmarks(dense_inputs);
}