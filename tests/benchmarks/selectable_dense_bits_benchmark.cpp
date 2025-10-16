#include <chrono>
#include <iomanip>
#include <iostream>

#include "yaef/yaef.hpp"

#include "utils/bit_generator.hpp"
#include "common.hpp"

using yaef::details::selectable_dense_bits;
using yaef::details::aligned_allocator;
using clock_type = std::chrono::steady_clock;
using f64nanos = std::chrono::duration<double, std::nano>;
using f64millis = std::chrono::duration<double, std::milli>;

void benchmark_select_one(const selectable_dense_bits &bits, size_t num_ones, 
                          const std::vector<size_t> &rand_indices) {
  auto bench_seq_start = clock_type::now();
  for (size_t i = 0; i < num_ones; ++i) {
    size_t index = bits.select_one(i);
    dont_optimize(index);
  }
  auto bench_seq_end = clock_type::now();
  auto bench_seq_nanos = std::chrono::duration_cast<f64nanos>(bench_seq_end - bench_seq_start);
  auto bench_seq_millis = std::chrono::duration_cast<f64millis>(bench_seq_end - bench_seq_start);

  auto bench_rand_start = clock_type::now();
  for (size_t i = 0; i < num_ones; ++i) {
    size_t index = bits.select_one(rand_indices[i]);
    dont_optimize(index);
  }
  auto bench_rand_end = clock_type::now();
  auto bench_rand_nanos = std::chrono::duration_cast<f64nanos>(bench_rand_end - bench_rand_start);
  auto bench_rand_millis = std::chrono::duration_cast<f64millis>(bench_rand_end - bench_rand_start);

  size_t space = bits.space_usage_in_bytes();
  std::cout << std::fixed << std::setprecision(3)
            << "benchmark for select_one: \n"
            << "space               : " << space << "B\n"
            << "compression ratio   : " << static_cast<double>(space * 8) / bits.size() * 100.0 << "%\n" 
            << "sequentially(total) : " << bench_seq_millis.count() << "ms\n"
            << "sequentially(ops)   : " << bench_seq_nanos.count() / num_ones << "ns/int\n"
            << "randomly(total)     : " << bench_rand_millis.count() << "ms\n"
            << "randomly(ops)       : " << bench_rand_nanos.count() / num_ones << "ns/int\n";
}

void benchmark_select_zero(const selectable_dense_bits &bits, size_t num_zeros, 
                          const std::vector<size_t> &rand_indices) {

  auto bench_seq_start = clock_type::now();
  for (size_t i = 0; i < num_zeros; ++i) {
    size_t index = bits.select_zero(i);
    dont_optimize(index);
  }
  auto bench_seq_end = clock_type::now();
  auto bench_seq_nanos = std::chrono::duration_cast<f64nanos>(bench_seq_end - bench_seq_start);
  auto bench_seq_millis = std::chrono::duration_cast<f64millis>(bench_seq_end - bench_seq_start);

  auto bench_rand_start = clock_type::now();
  for (size_t i = 0; i < num_zeros; ++i) {
    size_t index = bits.select_zero(rand_indices[i]);
    dont_optimize(index);
  }
  auto bench_rand_end = clock_type::now();
  auto bench_rand_nanos = std::chrono::duration_cast<f64nanos>(bench_rand_end - bench_rand_start);
  auto bench_rand_millis = std::chrono::duration_cast<f64millis>(bench_rand_end - bench_rand_start);

  size_t space = bits.space_usage_in_bytes();
  std::cout << std::fixed << std::setprecision(3)
            << "benchmark for select_zero: \n"
            << "space               : " << space << "B\n"
            << "compression ratio   : " << static_cast<double>(space * 8) / bits.size() * 100.0 << "%\n" 
            << "sequentially(total) : " << bench_seq_millis.count() << "ms\n"
            << "sequentially(ops)   : " << bench_seq_nanos.count() / num_zeros << "ns/int\n"
            << "randomly(total)     : " << bench_rand_millis.count() << "ms\n"
            << "randomly(ops)       : " << bench_rand_nanos.count() / num_zeros << "ns/int\n";
}

int main() {
  constexpr size_t NUM_BITS = 5000000;
  constexpr double ONE_DENSITY = 0.5;

  aligned_allocator<uint8_t, 64> alloc;
  yaef::test_utils::bit_generator bitgen;
  yaef::test_utils::uniform_int_generator<size_t> intgen;

  auto bitgen_param = yaef::test_utils::bit_generator::param::by_one_density(NUM_BITS, ONE_DENSITY);
  auto raw_bits = bitgen.make_bits(bitgen_param);
  selectable_dense_bits bits(alloc, raw_bits.view);
  size_t num_ones = bitgen_param.num_ones();
  size_t num_zeros = bitgen_param.num_zeros();
  auto one_rand_list = intgen.make_permutation(num_ones);
  auto zero_rand_list = intgen.make_permutation(num_zeros);

  benchmark_select_one(bits, num_ones, one_rand_list);
  std::cout << '\n';
  benchmark_select_zero(bits, num_zeros, zero_rand_list);

  return 0;
}