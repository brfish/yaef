#ifndef __YAEF_UTILS_BIT_GENERATOR_HPP__
#define __YAEF_UTILS_BIT_GENERATOR_HPP__
#pragma once

#include <memory>
#include <random>

#include "yaef/utils/int_generator.hpp"
#include "yaef/yaef.hpp"

namespace yaef {
namespace utils {

class bit_generator {
    using bits_block_type = yaef::details::bits64::bit_view::block_type;
public:
    using value_type    = bool;
    using size_type     = size_t;
    using random_engine = std::mt19937_64;

    struct result {
        std::unique_ptr<bits_block_type[]> mem;
        yaef::details::bits64::bit_view    view;
    };
    struct result_with_both_indices {
        std::unique_ptr<bits_block_type[]> mem;
        yaef::details::bits64::bit_view    view;
        std::vector<size_type>             zero_indices;
        std::vector<size_type>             one_indices;
    };
    struct result_with_one_indices {
        std::unique_ptr<bits_block_type[]> mem;
        yaef::details::bits64::bit_view    view;
        std::vector<size_type>             one_indices;
    };
    struct result_with_zero_indices {
        std::unique_ptr<bits_block_type[]> mem;
        yaef::details::bits64::bit_view    view;
        std::vector<size_type>             zero_indices;
    };

    struct param {
        _YAEF_ATTR_NODISCARD size_type num_bits() const noexcept { return num_zeros_ + num_ones_; }
        _YAEF_ATTR_NODISCARD size_type num_zeros() const noexcept { return num_zeros_; }
        _YAEF_ATTR_NODISCARD size_type num_ones() const noexcept { return num_ones_; }
        _YAEF_ATTR_NODISCARD double zero_density() const noexcept { 
            return static_cast<double>(num_zeros()) / static_cast<double>(num_bits()); 
        }
        _YAEF_ATTR_NODISCARD double one_density() const noexcept { 
            return static_cast<double>(num_ones()) / static_cast<double>(num_bits()); 
        }

        _YAEF_ATTR_NODISCARD static param by_size(size_type num_zeros, size_type num_ones) noexcept {
            return param{num_zeros, num_ones};
        }
        _YAEF_ATTR_NODISCARD static param by_one_density(size_type num_bits, double one_density) noexcept {
            one_density = std::min(1.0, one_density);
            size_type num_ones = static_cast<size_type>(static_cast<double>(num_bits) * one_density);
            size_type num_zeros = num_bits - num_ones;
            return by_size(num_zeros, num_ones);
        }
        _YAEF_ATTR_NODISCARD static param by_zero_density(size_type num_bits, double zero_density) {
            zero_density = std::min(1.0, zero_density);
            size_type num_zeros = static_cast<size_type>(static_cast<double>(num_bits) * zero_density);
            size_type num_ones = num_bits - num_zeros;
            return by_size(num_zeros, num_ones);
        }
    private:
        size_type num_zeros_;
        size_type num_ones_;

        param(size_type num_zeros, size_type num_ones) noexcept
            : num_zeros_(num_zeros), num_ones_(num_ones) { }
    };

public:
    bit_generator(uint64_t seed = 114514) noexcept
        : seed_(seed) { }

    _YAEF_ATTR_NODISCARD result make_uninit_bits(size_type num_bits) const {
        constexpr uint32_t BITS_BLOCK_WIDTH = yaef::details::bits64::bit_view::BLOCK_WIDTH;
        const size_type num_blocks = yaef::details::bits64::idiv_ceil(num_bits, BITS_BLOCK_WIDTH);

#if __cpp_lib_smart_ptr_for_overwrite >= 202002L
        auto mem = std::make_unique_for_overwrite<bits_block_type[]>(num_blocks);
#else
        auto mem = std::unique_ptr<bits_block_type[]>(new bits_block_type[num_blocks]);
#endif
        auto view = yaef::details::bits64::bit_view{mem.get(), num_bits};
        return result{std::move(mem), view};
    }

    _YAEF_ATTR_NODISCARD result make_bits(param p) const {
        uniform_int_generator<size_type> indices_gen{0, p.num_bits() - 1, seed_};
        result res = make_uninit_bits(p.num_bits());

        if (p.num_ones() <= p.num_zeros()) {
            auto indices = indices_gen.make_set(p.num_ones());
            res.view.clear_all_bits();
            for (size_type i = 0; i < indices.size(); ++i)
                res.view.set_bit(indices[i]);
        } else {
            auto indices = indices_gen.make_set(p.num_zeros());
            res.view.set_all_bits();
            for (size_type i = 0; i < indices.size(); ++i)
                res.view.clear_bit(indices[i]);
        }
        return res;
    }

    _YAEF_ATTR_NODISCARD result_with_both_indices make_bits_with_both_indices(param p) const {
        uniform_int_generator<size_type> indices_gen{0, p.num_bits() - 1, seed_};
        result res = make_uninit_bits(p.num_bits());

        auto one_indices = indices_gen.make_sorted_set(p.num_ones());
        res.view.clear_all_bits();
        for (size_type i = 0; i < one_indices.size(); ++i)
            res.view.set_bit(one_indices[i]);
        std::vector<size_type> zero_indices;
        zero_indices.resize(p.num_zeros());
        for (size_type i = 0, p = 0; i < res.view.size(); ++i) {
            if (res.view.get_bit(i) == false)
                zero_indices[p++] = i;
        }
        return result_with_both_indices{std::move(res.mem), res.view, 
            std::move(zero_indices), std::move(one_indices)};
    }

    _YAEF_ATTR_NODISCARD result_with_one_indices make_bits_with_one_indices(param p) const {
        uniform_int_generator<size_type> indices_gen{0, p.num_bits() - 1, seed_};
        result res = make_uninit_bits(p.num_bits());

        auto indices = indices_gen.make_sorted_set(p.num_ones());
        res.view.clear_all_bits();
        for (size_type i = 0; i < indices.size(); ++i)
            res.view.set_bit(indices[i]);
        return result_with_one_indices{std::move(res.mem), res.view, std::move(indices)};
    }

    _YAEF_ATTR_NODISCARD result_with_zero_indices make_bits_with_zero_indices(param p) const {
        uniform_int_generator<size_type> indices_gen{0, p.num_bits() - 1, seed_};
        result res = make_uninit_bits(p.num_bits());

        auto indices = indices_gen.make_sorted_set(p.num_zeros());
        res.view.set_all_bits();
        for (size_type i = 0; i < indices.size(); ++i)
            res.view.clear_bit(indices[i]);
        return result_with_zero_indices{std::move(res.mem), res.view, std::move(indices)};
    }

private:
    uint64_t seed_;
};

} // namespace utils
} // namespace yaef

#endif