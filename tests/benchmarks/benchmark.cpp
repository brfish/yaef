#include <numeric>

#include "benchmark_eliasfano_list.hpp"
#include "benchmark_eliasfano_sequence.hpp"
#include "benchmark_hybrid_list.hpp"
#include "benchmark_packed_array.hpp"
#include "benchmark_plain_array.hpp"
#include "benchmark_sparse_sampled_list.hpp"
#include "benchmark_stl_set.hpp"

template<typename IntT>
benchmark_inputs<IntT> generate_dense(size_t num) {
    std::vector<IntT> vec(num);
    std::iota(vec.begin(), vec.end(), std::numeric_limits<IntT>::min());
    return benchmark_inputs<IntT>::from_values(vec);
}

template<typename IntT>
benchmark_inputs<IntT> generate_random(size_t num, IntT min = std::numeric_limits<IntT>::min(),
                                       IntT max = std::numeric_limits<IntT>::max(), bool unique = true) {
    if (unique) {
        return benchmark_inputs<IntT>::from_datagen_unique(min, max, num);
    }
    return benchmark_inputs<IntT>::from_datagen(min, max, num);
}

template<typename IntT>
void run_benchmarks(const benchmark_inputs<IntT> &inputs) {
#define REPORT_BENCHMARK(_name) \
    { _name<IntT> b; b.run(inputs); b.report(); }

    REPORT_BENCHMARK(plain_array_search_benchmark);
    REPORT_BENCHMARK(plain_array_branchless_search_benchmark);
    if (inputs.values.size() <= 32) {
        REPORT_BENCHMARK(plain_array_seq_search_benchmark);
    } 

    REPORT_BENCHMARK(packed_array_search_benchmark);
    REPORT_BENCHMARK(packed_branchless_search_benchmark);
    if (inputs.values.size() <= 32) {
        REPORT_BENCHMARK(packed_array_seq_search_benchmark);
    }

    REPORT_BENCHMARK(eliasfano_list_benchmark);
    REPORT_BENCHMARK(eliasfano_sequence_benchmark);
    REPORT_BENCHMARK(hybrid_list_benchmark);

    REPORT_BENCHMARK(cardinality_sparse_sampled_list_benchmark);
    REPORT_BENCHMARK(universe_sparse_sampled_list_benchmark);

    REPORT_BENCHMARK(stl_set_benchmark);
#undef REPORT_BENCHMARK
}

int main(int argc, char *argv[]) {
    (void)(argc); (void)(argv);
    using int_type = uint32_t;
    constexpr size_t NUM_INTS = 500000;

    std::cout << "<<<<<<<<<< random >>>>>>>>>>\n";
    auto random_inputs = generate_random<int_type>(NUM_INTS, 0, NUM_INTS * 500);
    run_benchmarks(random_inputs);

    std::cout << "<<<<<<<<<< dense >>>>>>>>>>\n";
    auto dense_inputs = generate_dense<int_type>(NUM_INTS);
    run_benchmarks(dense_inputs);
}