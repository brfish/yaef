// Copyright (c) 2025 brfish
// 
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
// 
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#ifndef __YAEF_HPP__
#define __YAEF_HPP__
#pragma once

#include <algorithm>
#include <array>
#include <climits>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <initializer_list>
#include <istream>
#include <iterator>
#include <limits>
#include <memory>
#include <ostream>
#include <type_traits>
#include <utility>

#include <immintrin.h>

#ifdef _MSC_VER
#   include <intrin.h>
#   pragma intrinsic(_mm_prefetch)
#endif

#if __cplusplus >= 202002L
#   include <version>
#endif

#if __cpp_lib_bitops >= 201907L
#   include <bit> // for bitops
#   define _YAEF_USE_STL_BITOPS_IMPL 1
#endif

#if __cplusplus >= 202002L
#   include <compare> // for std::strong_ordering
#endif

#if __cpp_concepts >= 201907L
#   include <concepts>
#   define _YAEF_USE_CXX_CONCEPTS 1
#else
#   include <vector> // for is_contiguous_iter
#   include <string> // TODO: use forward declaration?
#endif

#if __cplusplus >= 201703L
#   include <new> // for std::aligned_val_t
#endif

#if __cplusplus >= 201703L
#   include <filesystem>
#   if __cpp_lib_filesystem >= 201703L
#       define _YAEF_USE_STL_FILESYSTEM 1
#       include <fstream>
#   endif
#endif

#if __cplusplus >= 202002L
#   include <ranges>
#   if __cpp_lib_ranges >= 201911L
#       define _YAEF_USE_STL_RANGES_ALG 1
#   endif
#endif

#if __cpp_lib_span >= 202002L
#   include <span>
#   define _YAEF_USE_STL_SPAN 1
#endif

#ifndef YAEF_OPTS_NO_EXCEPTION
#   include <stdexcept>
#endif

#if __cplusplus >= 201703L
#   define _YAEF_CXX17_CONSTEXPR constexpr
#else
#   define _YAEF_CXX17_CONSTEXPR
#endif

#ifdef __has_cpp_attribute
#   define _YAEF_HAS_CPP_ATTRIBUTE(_x) __has_cpp_attribute(_x)
#else
#   define _YAEF_HAS_CPP_ATTRIBUTE(_x) (false)
#endif

#ifdef __has_builtin
#   define _YAEF_HAS_BUILTIN(_x) __has_builtin(_x)
#else
#   define _YAEF_HAS_BUILTIN(_x) (false)
#endif

#if _YAEF_HAS_CPP_ATTRIBUTE(nodiscard)
#   define _YAEF_ATTR_NODISCARD [[nodiscard]]
#else
#   define _YAEF_ATTR_NODISCARD
#endif

#if _YAEF_HAS_CPP_ATTRIBUTE(clang::noinline)
#   define _YAEF_ATTR_NOINLINE [[clang::noinline]]
#elif _YAEF_HAS_CPP_ATTRIBUTE(gnu::noinline)
#   define _YAEF_ATTR_NOINLINE [[gnu::noinline]]
#elif defined(_MSC_VER)
#   define _YAEF_ATTR_NOINLINE __declspec(noinline)
#endif

#if _YAEF_HAS_CPP_ATTRIBUTE(clang::always_inline)
#   define _YAEF_ATTR_FORCEINLINE [[clang::always_inline]] inline
#elif _YAEF_HAS_CPP_ATTRIBUTE(gnu::always_inline)
#   define _YAEF_ATTR_FORCEINLINE [[gnu::always_inline]] inline
#elif defined(_MSC_VER)
#   define _YAEF_ATTR_FORCEINLINE __forceinline
#endif

#if _YAEF_HAS_BUILTIN(__builtin_expect)
#   define _YAEF_LIKELY(_expr) __builtin_expect(!!(_expr), 1)
#   define _YAEF_UNLIKELY(_expr) __builtin_expect(!!(_expr), 0)
#else
#   define _YAEF_LIKELY(_expr) (!!(_expr))
#   define _YAEF_UNLIKELY(_expr) (!!(_expr))
#endif

#if _YAEF_HAS_BUILTIN(__builtin_unreachable)
#   define _YAEF_UNREACHABLE() __builtin_unreachable()
#elif defined(_MSC_VER)
#   define _YAEF_UNREACHABLE() __assume(0)
#else
#   define _YAEF_UNREACHABLE() ::abort()
#endif

#if _YAEF_HAS_BUILTIN(__builtin_assume) // provided by clang
#   define _YAEF_ASSUME(...) __builtin_assume(__VA_ARGS__)
#elif defined(__GNUC__)
#   if __GNUC__ >= 13
#       define _YAEF_ASSUME(...) __attribute__((assume(__VA_ARGS__)))
#   else
#       define _YAEF_ASSUME(...) do { if (!static_cast<bool>(__VA_ARGS__)) { _YAEF_UNREACHABLE(); } } while (false)
#   endif
#elif defined(_MSC_VER)
#   define _YAEF_ASSUME(...) __assume(__VA_ARGS__)
#else
#   define _YAEF_ASSUME(...)
#endif

#if __cplusplus < 201703L
#   define _YAEF_STATIC_ASSERT_NOMSG(...) static_assert(__VA_ARGS__, "")
#else
#   define _YAEF_STATIC_ASSERT_NOMSG(...) static_assert(__VA_ARGS__)
#endif

#if __cplusplus <= 201402L
#   define _YAEF_TRAIT_VAR(_trait, ...) _trait<__VA_ARGS__>::value
#else
#   define _YAEF_TRAIT_VAR(_trait, ...) _trait ## _v <__VA_ARGS__>
#endif

#ifdef __clang__
#   if _YAEF_HAS_BUILTIN(__builtin_debugtrap)
#       define _YAEF_DEBUGBREAK() __builtin_debugtrap()
#   else
#       define _YAEF_DEBUGBREAK() asm("int3")
#   endif
#elif defined(__GNUC__)
#   if _YAEF_HAS_BUILTIN(__builtin_trap)
#       define _YAEF_DEBUGBREAK() __builtin_trap()
#   else
#       define _YAEF_DEBUGBREAK() asm("int3")
#   endif
#elif defined(_MSC_VER)
#   define _YAEF_DEBUGBREAK() __debugbreak()
#else
#   error "cannot find a suitable implementation for DEBUGBREAK"
#   define _YAEF_DEBUGBREAK()
#endif

#ifdef NDEBUG
#   define _YAEF_ASSERT(...)
#else
#   define _YAEF_ASSERT(...) do { if (!static_cast<bool>(__VA_ARGS__)) { \
        ::yaef::details::raise_assertion(__FILE__, __LINE__, _YAEF_STRINGIFY(__VA_ARGS__)); \
        _YAEF_DEBUGBREAK(); } } while (false)
#endif

#ifdef __BMI2__
#   define _YAEF_INTRINSICS_HAVE_BMI2 1
#elif defined(_MSC_VER) && defined(__AVX2__)
#   define _YAEF_INTRINSICS_HAVE_BMI2 1
#endif

#ifdef __AVX2__
#   define _YAEF_INTRINSICS_HAVE_AVX2 1
#endif

#ifndef YAEF_OPTS_NO_EXCEPTION
#   define _YAEF_THROW(...) throw (__VA_ARGS__)
#   define _YAEF_MAYBE_NOEXCEPT 
#else
#   define _YAEF_THROW(...) ::abort()
#   define _YAEF_MAYBE_NOEXCEPT noexcept
#endif

#define _YAEF_UNUSED(_x) ((void)(_x))

#define _YAEF_CONCAT_IMPL(_a, _b) _a##_b
#define _YAEF_CONCAT(_a, _b) _YAEF_CONCAT_IMPL(_a, _b)
#define _YAEF_STRINGIFY_IMPL(...) #__VA_ARGS__
#define _YAEF_STRINGIFY(...) _YAEF_STRINGIFY_IMPL(__VA_ARGS__)

#define _YAEF_JOIN_3(_sep, _a, _b, _c) \
    _YAEF_CONCAT(_YAEF_CONCAT(_YAEF_CONCAT(_a, _sep), _YAEF_CONCAT(_b, _sep)), _c)

#define YAEF_VERSION_MAJOR 0
#define YAEF_VERSION_MINOR 1
#define YAEF_VERSION_PATCH 0

#define YAEF_VERSION_NUM (((YAEF_VERSION_MAJOR) << 22) | ((YAEF_VERSION_MINOR) << 12) | (YAEF_VERSION_PATCH))
#define YAEF_VERSION_STR _YAEF_STRINGIFY(_YAEF_JOIN_3(., YAEF_VERSION_MAJOR, YAEF_VERSION_MINOR, YAEF_VERSION_PATCH))

#define _YAEF_VERSION_NAMESPACE _YAEF_CONCAT(v, _YAEF_JOIN_3(_, YAEF_VERSION_MAJOR, YAEF_VERSION_MINOR, YAEF_VERSION_PATCH))

#define _YAEF_NAMESPACE_BEGIN namespace yaef { inline namespace _YAEF_VERSION_NAMESPACE {
#define _YAEF_NAMESPACE_END } }

_YAEF_NAMESPACE_BEGIN

enum class error_code : uint32_t {
    success = 0,
    invalid_argument,
    out_of_memory,
    buf_too_small,
    not_sorted,
    serialize_io,
    deserialize_invalid_format,
    deserialize_io
};

#if __cplusplus >= 202002L
#   define _YAEF_RETURN_ERR_IF_FAIL(...) \
        if (::yaef::error_code ec = (__VA_ARGS__); ec != ::yaef::error_code::success) { return ec; }
#else
#   define _YAEF_RETURN_ERR_IF_FAIL(...) \
        do { \
            ::yaef::error_code ec = (__VA_ARGS__); \
            if (ec != ::yaef::error_code::success) { return ec; } \
        } while (false);
#endif

template<uint32_t W>
struct assumed_width_t : std::integral_constant<uint32_t, W> { };

#if __cplusplus >= 201703L
template<uint32_t W>
inline constexpr assumed_width_t<W> assumed_width;
#endif

namespace details {

inline void raise_assertion(const char *filename, int line, const char *expr) {
    ::fprintf(stderr, "[%s:%d] assertion `%s` failed.", filename, line, expr);
}

enum class prefetch_hint : int32_t {
#if _YAEF_HAS_BUILTIN(__builtin_prefetch)
    nta   = 0, 
    tier2 = 1, 
    tier1 = 2, 
    tier0 = 3
#else
    nta   = _MM_HINT_NTA, 
    tier2 = _MM_HINT_T2, 
    tier1 = _MM_HINT_T1, 
    tier0 = _MM_HINT_T0
#endif
};

template<prefetch_hint Hint = prefetch_hint::tier0>
_YAEF_ATTR_FORCEINLINE void prefetch_read(const void *p) noexcept {
#if _YAEF_HAS_BUILTIN(__builtin_prefetch)
    __builtin_prefetch(p, 0, static_cast<int>(Hint));
#elif defined(_MSC_VER)
    _mm_prefetch(const_cast<char *>(reinterpret_cast<const char *>(p)),
                 static_cast<int>(Hint));
#else
    _mm_prefetch(p, static_cast<int>(Hint));
#endif
}

template<prefetch_hint Hint = prefetch_hint::tier0>
_YAEF_ATTR_FORCEINLINE void prefetch_write(void *p) noexcept {
    _YAEF_STATIC_ASSERT_NOMSG(Hint == prefetch_hint::tier0 ||
                              Hint == prefetch_hint::tier1);
#if _YAEF_HAS_BUILTIN(__builtin_prefetch)
    __builtin_prefetch(p, 1, static_cast<int>(Hint));
#elif defined(_MSC_VER)
    // do nothing in MSVC compilers.
#else
    constexpr int REAL_HINT = Hint == prefetch_hint::tier0 ? _MM_HINT_ET0 : _MM_HINT_ET1;
    _mm_prefetch(p, REAL_HINT);
#endif
}

#if !_YAEF_USE_CXX_CONCEPTS
#   if __cpp_lib_void_t >= 201411L
template<typename ...Ts>
using void_t = std::void_t<Ts...>;
#   else
template<typename ...>
using void_t = void;
#   endif

#   if __cpp_lib_remove_cvref >= 201711L
template<typename T>
using remove_cvref = std::remove_cvref<T>;
#   else
template<typename T>
using remove_cvref = std::remove_cv<typename std::remove_reference<T>::type>;
#   endif

template<typename T, typename = void>
struct is_forward_iter : std::false_type { };

template<typename T>
struct is_forward_iter<T, details::void_t<typename std::iterator_traits<T>::iterator_category>>
    : std::is_base_of<std::forward_iterator_tag,
                      typename std::iterator_traits<T>::iterator_category> { };

template<typename T, typename = void>
struct is_bidirectional_iter : std::false_type { };

template<typename T>
struct is_bidirectional_iter<T, details::void_t<typename std::iterator_traits<T>::iterator_category>>
    : std::is_base_of<std::bidirectional_iterator_tag,
                      typename std::iterator_traits<T>::iterator_category> { };

template<typename T, typename = void>
struct is_random_access_iter : std::false_type { };

template<typename T>
struct is_random_access_iter<T, details::void_t<typename std::iterator_traits<T>::iterator_category>> 
    : std::is_base_of<std::random_access_iterator_tag, 
                      typename std::iterator_traits<T>::iterator_category> { };

template<typename Cond1, typename Cond2>
struct traits_or : std::integral_constant<bool, Cond1::value || Cond2::value> { };

template<typename T, typename = void>
struct is_contiguous_container_iter : std::false_type { };

template<typename T>
struct is_contiguous_container_iter<T, details::void_t<typename std::iterator_traits<T>::value_type>>
    : traits_or<
        traits_or<
            std::is_same<typename std::vector<typename std::iterator_traits<T>::value_type>::iterator, T>,
            std::is_same<typename std::vector<typename std::iterator_traits<T>::value_type>::const_iterator, T>
        >,
        traits_or<
            std::is_same<typename std::basic_string<typename std::iterator_traits<T>::value_type>::iterator, T>,
            std::is_same<typename std::basic_string<typename std::iterator_traits<T>::value_type>::const_iterator, T>
        >
      > { };

template<typename T, typename = void>
struct is_contiguous_iter : std::false_type { };

template<typename T>
struct is_contiguous_iter<T, typename std::enable_if<std::is_pointer<T>::value>::type> : std::true_type { };

template<typename T>
struct is_contiguous_iter<T, typename std::enable_if<is_contiguous_container_iter<T>::value>::type> 
    : std::true_type { };

#endif

template<typename T, template<typename> class Constraint, typename = void>
struct iterator_value_type_constraint : std::false_type { };

template<typename T, template<typename> class Constraint>
struct iterator_value_type_constraint<T, Constraint, void_t<typename std::iterator_traits<T>::value_type>> 
    : std::integral_constant<bool, Constraint<typename std::iterator_traits<T>::value_type>::value> { };

#if _YAEF_USE_CXX_CONCEPTS
#   define _YAEF_REQUIRES_FORWARD_ITER(_iter, _sent, _val_constraint) \
    template<std::forward_iterator _iter, \
             std::sized_sentinel_for<_iter> _sent> \
    requires (_val_constraint<std::iter_value_t<_iter>>::value)

#   define _YAEF_REQUIRES_BIDIR_ITER(_iter, _sent, _val_constraint) \
    template<std::bidirectional_iterator _iter, \
             std::sized_sentinel_for<_iter> _sent> \
    requires (_val_constraint<std::iter_value_t<_iter>>::value)

#   define _YAEF_REQUIRES_RANDOM_ACCESS_ITER(_iter, _sent, _val_constraint) \
    template<std::random_access_iterator _iter, \
             std::sized_sentinel_for<_iter> _sent> \
    requires (_val_constraint<std::iter_value_t<_iter>>::value)
#else
#   define _YAEF_REQUIRES_FORWARD_ITER(_iter, _sent, _val_constraint) \
    template<typename _iter, typename _sent, \
             typename = typename std::enable_if< \
                details::is_forward_iter<_iter>::value && \
                std::is_same<_iter, _sent>::value && \
                details::iterator_value_type_constraint<_iter, _val_constraint>::value>::type>

#   define _YAEF_REQUIRES_BIDIR_ITER(_iter, _sent, _val_constraint) \
    template<typename _iter, typename _sent, \
             typename = typename std::enable_if< \
                details::is_bidirectional_iter<_iter>::value && \
                std::is_same<_iter, _sent>::value && \
                details::iterator_value_type_constraint<_iter, _val_constraint>::value>::type>

#   define _YAEF_REQUIRES_RANDOM_ACCESS_ITER(_iter, _sent, _val_constraint) \
    template<typename _iter, typename _sent, \
             typename = typename std::enable_if< \
                details::is_random_access_iter<_iter>::value && \
                std::is_same<_iter, _sent>::value && \
                details::iterator_value_type_constraint<_iter, _val_constraint>::value>::type>
#endif

template<typename InputIterT, typename SentIterT>
_YAEF_ATTR_NODISCARD inline ptrdiff_t iter_distance(InputIterT first, SentIterT last) {
#if _YAEF_USE_STL_RANGES_ALG
    return std::ranges::distance(first, last);
#else
    _YAEF_STATIC_ASSERT_NOMSG(std::is_same<typename details::remove_cvref<InputIterT>::type,
                                           typename details::remove_cvref<SentIterT>::type>::value);
    return std::distance(first, last);
#endif
}

template<typename InputIterT, typename SentIterT>
_YAEF_ATTR_NODISCARD inline typename std::iterator_traits<InputIterT>::value_type
find_max_value(InputIterT first, SentIterT last) {
#if _YAEF_USE_STL_RANGES_ALG
    return *std::ranges::max_element(first, last);
#else
    _YAEF_STATIC_ASSERT_NOMSG(std::is_same<typename details::remove_cvref<InputIterT>::type,
                                           typename details::remove_cvref<SentIterT>::type>::value);
    return *std::max_element(first, last);
#endif
}

template<typename ForwardIterT, typename SentIterT>
_YAEF_ATTR_NODISCARD inline bool is_sorted(ForwardIterT first, SentIterT last) {
#if _YAEF_USE_STL_RANGES_ALG
    return std::ranges::is_sorted(first, last);
#else
    _YAEF_STATIC_ASSERT_NOMSG(std::is_same<typename details::remove_cvref<ForwardIterT>::type,
                                           typename details::remove_cvref<SentIterT>::type>::value);
    return std::is_sorted(first, last);
#endif
}

template<typename RandomAccessIter, typename T>
_YAEF_ATTR_NODISCARD inline RandomAccessIter branchless_lower_bound(RandomAccessIter first, size_t n, T target) {
    RandomAccessIter base = first;
    size_t len = n;
    while (len > 0) {
        size_t half = len / 2;
        base += (base[half] < target) * (len - half);
        len = half;
    }
    return base;
}

template<typename RandomAccessIter, typename T>
_YAEF_ATTR_NODISCARD inline RandomAccessIter branchless_upper_bound(RandomAccessIter first, size_t n, T target) {
    RandomAccessIter base = first;
    size_t len = n;
    while (len > 0) {
        size_t half = len / 2;
        base += (base[half] <= target) * (len - half);
        len = half;
    }
    return base;
}

template<typename AllocT>
void checked_swap_alloc_impl(AllocT &lhs, AllocT &rhs, std::true_type) noexcept {
    std::swap(lhs, rhs);
}

template<typename AllocT>
void checked_swap_alloc_impl(AllocT &lhs, AllocT &rhs, std::false_type) 
    noexcept(std::allocator_traits<AllocT>::is_always_equal::value) {
#ifdef NDEBUG
    _YAEF_UNUSED(lhs);
    _YAEF_UNUSED(rhs);
#endif
    _YAEF_ASSERT(lhs == rhs);
}

template<typename AllocT>
void checked_swap_alloc(AllocT &lhs, AllocT &rhs) {
    using alloc_traits = std::allocator_traits<AllocT>;
    checked_swap_alloc_impl(lhs, rhs, typename alloc_traits::propagate_on_container_swap{});
}

template<typename T, typename ...ArgTs>
_YAEF_ATTR_NODISCARD inline std::unique_ptr<T> make_unique_obj(ArgTs &&...args) {
#if __cplusplus < 201402L
    return std::unique_ptr<T>(new T(std::forward<ArgTs>(args)...));
#else
    return std::make_unique<T>(std::forward<ArgTs>(args)...);
#endif
}

template<typename T>
_YAEF_ATTR_NODISCARD inline std::unique_ptr<T []> make_unique_array(size_t num) {
#if __cplusplus < 201402L
    return std::unique_ptr<T []>(new T[num]());
#else
    return std::make_unique<T []>(num);
#endif
}

class reader_context {
public:
    virtual ~reader_context() = default;
    virtual size_t read(uint8_t *buf, size_t size) = 0;
};

class writer_context {
public:
    virtual ~writer_context() = default;
    virtual size_t write(const uint8_t *buf, size_t size) = 0;
};

class cfile_reader_context : public reader_context {
public:
    cfile_reader_context(FILE *file) noexcept
        : file_(file) { }

    size_t read(uint8_t *buf, size_t size) override {
        return ::fread(buf, 1, size, file_);
    }

private:
    FILE *file_;
};

class cfile_writer_context : public writer_context {
public:
    cfile_writer_context(FILE *file) noexcept
        : file_(file) { }

    size_t write(const uint8_t *buf, size_t size) override {
        return ::fwrite(buf, 1, size, file_);
    }

private:
    FILE *file_;
};

class istream_reader_context : public reader_context {
public:
    istream_reader_context(std::istream &stream) noexcept
        : stream_(std::addressof(stream)) { }

    size_t read(uint8_t *buf, size_t size) override {
        size_t cur_pos = stream_->tellg();
        stream_->read(reinterpret_cast<char *>(buf), size);
        size_t new_pos = stream_->tellg();
        return new_pos - cur_pos;
    }

private:
    std::istream *stream_;
};

class ostream_writer_context : public writer_context {
public:
    ostream_writer_context(std::ostream &stream) noexcept
        : stream_(std::addressof(stream)) { }

    size_t write(const uint8_t *buf, size_t size) override {
        size_t cur_pos = stream_->tellp();
        stream_->write(reinterpret_cast<const char *>(buf), size);
        size_t new_pos = stream_->tellp();
        return new_pos - cur_pos;
    }

private:
    std::ostream *stream_;
};

class membuf_reader_context : public reader_context {
public:
    membuf_reader_context(const uint8_t *buf, size_t buf_size) noexcept
        : buf_(buf), size_(buf_size), cur_(0) { }

    size_t read(uint8_t *buf, size_t size) override {
        size_t actual_size = std::min(size, size_ - cur_);
        if (actual_size == 0) {
            return 0;
        }
        ::memcpy(buf, buf_ + cur_, actual_size);
        cur_ += actual_size;
        return actual_size;
    }

private:
    const uint8_t *buf_;
    size_t         size_;
    size_t         cur_;
};

class membuf_writer_context : public writer_context {
public:
    membuf_writer_context(uint8_t *buf, size_t buf_size) noexcept
        : buf_(buf), size_(buf_size), cur_(0) { }

    size_t write(const uint8_t *buf, size_t size) override {
        size_t actual_size = std::min(size, size_ - cur_);
        if (actual_size == 0) {
            return 0;
        }
        ::memcpy(buf_ + cur_, buf, actual_size);
        cur_ += actual_size;
        return actual_size;
    }

private:
    uint8_t *buf_;
    size_t   size_;
    size_t   cur_;
};

class serializer {
public:
    serializer(std::unique_ptr<writer_context> &&ctx) noexcept
        : ctx_(std::move(ctx)) { }
    
    _YAEF_ATTR_NODISCARD const writer_context &context() const { return *ctx_; }
    _YAEF_ATTR_NODISCARD writer_context &context() { return *ctx_; }
    
    template<typename T>
    bool write(const T &val) {
        _YAEF_STATIC_ASSERT_NOMSG(_YAEF_TRAIT_VAR(std::is_trivial, T));
        _YAEF_STATIC_ASSERT_NOMSG(_YAEF_TRAIT_VAR(std::is_trivially_copyable, T));
        size_t actual_write = ctx_->write(reinterpret_cast<const uint8_t *>(std::addressof(val)), sizeof(T));
        return actual_write == sizeof(T);
    }

    bool write_bytes(const uint8_t *buf, size_t size) {
        size_t actual_write = ctx_->write(buf, size);
        return actual_write == size;
    }

private:
    std::unique_ptr<writer_context> ctx_;
};

class deserializer {
public:
    deserializer(std::unique_ptr<reader_context> &&ctx) noexcept
        : ctx_(std::move(ctx)) { }
    
    _YAEF_ATTR_NODISCARD const reader_context &context() const { return *ctx_; }
    _YAEF_ATTR_NODISCARD reader_context &context() { return *ctx_; }
    
    template<typename T>
    bool read(T &val) {
        _YAEF_STATIC_ASSERT_NOMSG(_YAEF_TRAIT_VAR(std::is_trivial, T));
        _YAEF_STATIC_ASSERT_NOMSG(_YAEF_TRAIT_VAR(std::is_trivially_copyable, T));
        size_t actual_read = ctx_->read(reinterpret_cast<uint8_t *>(std::addressof(val)), sizeof(T));
        return actual_read == sizeof(T);
    }

    bool read_bytes(uint8_t *buf, size_t size) {
        size_t actual_read = ctx_->read(buf, size);
        return actual_read == size;
    }

private:
    std::unique_ptr<reader_context> ctx_;
};

struct serialize_friend_access {
    template<typename T>
    static error_code serialize(const T &x, serializer &ser) {
        return x.do_serialize(ser);
    }

    template<typename T>
    static error_code deserialize(T &x, deserializer &deser) {
        return x.do_deserialize(deser);
    }
};

template<typename T, typename U = T>
_YAEF_ATTR_NODISCARD inline T exchange(T &obj, U &&new_val) 
    noexcept(std::is_nothrow_move_constructible<T>::value &&
             std::is_nothrow_assignable<T &, U>::value) {
#if __cpp_lib_exchange_function >= 201304L
    return std::exchange(obj, std::forward<U>(new_val));
#else
    T old_val = std::move(obj);
    obj = std::forward<U>(new_val);
    return old_val;
#endif
}

namespace bits64 {

template<uint64_t M>
_YAEF_ATTR_NODISCARD inline uint64_t align_to(uint64_t val) {
    return (val + M - 1) & ~(M - 1);
}

// a bit-parallel implementation
_YAEF_ATTR_NODISCARD inline uint32_t popcount_fallback(uint64_t block) noexcept {
    constexpr uint64_t MASK1 = 0x5555555555555555ULL;
    constexpr uint64_t MASK2 = 0x3333333333333333ULL;
    constexpr uint64_t MASK4 = 0x0F0F0F0F0F0F0F0FULL;
    constexpr uint64_t H01 = 0x0101010101010101ULL;

    uint64_t x = block;
    x -= (x >> 1) & MASK1;
    x = (x & MASK2) + ((x >> 2) & MASK2);
    x = (x + (x >> 4)) & MASK4;
    return static_cast<uint32_t>((x * H01) >> 56);   
}

_YAEF_ATTR_NODISCARD inline uint32_t popcount(uint64_t block) noexcept {
#if _YAEF_USE_STL_BITOPS_IMPL
    return std::popcount(block);
#elif _YAEF_HAS_BUILTIN(__builtin_popcountll)
    return __builtin_popcountll(block);
#elif defined(_MSC_VER)
    return __popcnt64(block);
#else
    return popcount_fallback(block);
#endif
}

// a simple implementation based on binary searching
_YAEF_ATTR_NODISCARD inline uint32_t count_leading_zero_fallback(uint64_t block) noexcept {
    if (_YAEF_UNLIKELY(block == 0)) { return 64; }
    uint32_t result = 0;
    if ((block & 0xFFFFFFFF00000000ULL) == 0) { result += 32; block <<= 32; }
    if ((block & 0xFFFF000000000000ULL) == 0) { result += 16; block <<= 16; }
    if ((block & 0xFF00000000000000ULL) == 0) { result += 8; block <<= 8; }
    if ((block & 0xF000000000000000ULL) == 0) { result += 4; block <<= 4; }
    if ((block & 0xC000000000000000ULL) == 0) { result += 2; block <<= 2; }
    if ((block & 0x8000000000000000ULL) == 0) { result += 1; }
    return result;
}

_YAEF_ATTR_NODISCARD inline uint32_t count_leading_zero(uint64_t block) noexcept {
#if _YAEF_USE_STL_BITOPS_IMPL
    return std::countl_zero(block);
#elif _YAEF_HAS_BUILTIN(__builtin_clzll)
    return __builtin_clzll(block);
#elif defined(_MSC_VER)
#   ifdef __AVX2__
    return __lzcnt64(block);
#   else
    unsigned long index;
    unsigned char found = _BitScanForward64(&index, block);
    return found ? index : 64;
#   endif
#else
    return count_leading_zero_fallback(block);
#endif
}

_YAEF_ATTR_NODISCARD inline uint32_t count_leading_one_fallback(uint64_t block) noexcept {
    return count_leading_zero_fallback(~block);
}

_YAEF_ATTR_NODISCARD inline uint32_t count_leading_one(uint64_t block) noexcept {
    return count_leading_zero(~block);
}

// a simple implementation based on binary searching
_YAEF_ATTR_NODISCARD inline uint32_t count_trailing_zero_fallback(uint64_t block) noexcept {
    if (_YAEF_UNLIKELY(block == 0)) { return 64; }
    uint32_t result = 0;
    if ((block & 0x00000000FFFFFFFFULL) == 0) { result += 32; block >>= 32; }
    if ((block & 0x000000000000FFFFULL) == 0) { result += 16; block >>= 16; }
    if ((block & 0x00000000000000FFULL) == 0) { result += 8; block >>= 8; }
    if ((block & 0x000000000000000FULL) == 0) { result += 4; block >>= 4; }
    if ((block & 0x0000000000000003ULL) == 0) { result += 2; block >>= 2; }
    if ((block & 0x0000000000000001ULL) == 0) { result += 1; }
    return result;
}

_YAEF_ATTR_NODISCARD inline uint32_t count_trailing_zero(uint64_t block) noexcept {
#if _YAEF_USE_STL_BITOPS_IMPL
    return std::countr_zero(block);
#elif _YAEF_HAS_BUILTIN(__builtin_ctzll)
    return __builtin_ctzll(block);
#elif defined(_MSC_VER)
#   ifdef __AVX2__
    return _tzcnt_u64(block);
#   else
    unsigned long index;
    unsigned char found = _BitScanForward64(&index, block);
    return found ? index : 64;
#   endif
#else
    return count_trailing_zero_fallback(block);
#endif
}

_YAEF_ATTR_NODISCARD inline uint32_t count_trailing_one_fallback(uint64_t block) noexcept {
    return count_trailing_zero_fallback(~block);
}

_YAEF_ATTR_NODISCARD inline uint32_t count_trailing_one(uint64_t block) noexcept {
    return count_trailing_zero(~block);
}

_YAEF_ATTR_NODISCARD inline uint64_t rotate_left_fallback(uint64_t block, uint32_t shift) noexcept {
    return (block << (shift % 64U)) | (block >> ((-shift) % 64U));
}

_YAEF_ATTR_NODISCARD inline uint64_t rotate_left(uint64_t block, uint32_t shift) noexcept {
#if _YAEF_USE_STL_BITOPS_IMPL
    return std::rotl(block, shift);
#elif _YAEF_HAS_BUILTIN(__builtin_rotateleft64)
    return __builtin_rotateleft64(block, shift);
#elif defined(_MSC_VER)
    return _rotl64(block, shift);
#else
    return rotate_left_fallback(block, shift);
#endif
}

_YAEF_ATTR_NODISCARD inline uint64_t rotate_right_fallback(uint64_t block, uint32_t shift) noexcept {
    return (block >> (shift % 64U)) | (block << ((-shift) % 64U));
}

_YAEF_ATTR_NODISCARD inline uint64_t rotate_right(uint64_t block, uint32_t shift) noexcept {
#if _YAEF_USE_STL_BITOPS_IMPL
    return std::rotr(block, shift);
#elif _YAEF_HAS_BUILTIN(__builtin_rotateright64)
    return __builtin_rotateright64(block, shift);
#elif defined(_MSC_VER)
    return _rotr64(block, shift);
#else
    return rotate_right_fallback(block, shift);
#endif
}

_YAEF_ATTR_NODISCARD inline uint32_t bit_width(uint64_t x) noexcept {
#if _YAEF_USE_STL_BITOPS_IMPL
    return std::bit_width(x);
#else
    return 64 - count_leading_zero(x);
#endif
}

constexpr uint32_t constexpr_bit_width_impl(uint64_t x) {
    return x == 0 ? 0 : 1 + constexpr_bit_width_impl(x >> 1);
}

#if _YAEF_USE_STL_BITOPS_IMPL
template<uint64_t Num>
struct constexpr_bit_width : std::integral_constant<uint32_t, std::bit_width(Num)> { };
#else
template<uint64_t Num>
struct constexpr_bit_width : std::integral_constant<uint32_t, constexpr_bit_width_impl(Num)> { };
#endif

static constexpr uint64_t LSB_MASK_LUT64[65] = {
    0x0ULL,
    0x1ULL, 0x3ULL, 0x7ULL, 0xFULL, 0x1FULL, 0x3FULL, 0x7FULL, 0xFFULL,
    0x1FFULL, 0x3FFULL, 0x7FFULL, 0xFFFULL, 0x1FFFULL, 0x3FFFULL, 0x7FFFULL, 0xFFFFULL,
    0x1FFFFULL, 0x3FFFFULL, 0x7FFFFULL, 0xFFFFFULL, 0x1FFFFFULL, 0x3FFFFFULL, 0x7FFFFFULL, 0xFFFFFFULL,
    0x1FFFFFFULL, 0x3FFFFFFULL, 0x7FFFFFFULL, 0xFFFFFFFULL, 0x1FFFFFFFULL, 0x3FFFFFFFULL, 0x7FFFFFFFULL, 0xFFFFFFFFULL,
    0x1FFFFFFFFULL, 0x3FFFFFFFFULL, 0x7FFFFFFFFULL, 0xFFFFFFFFFULL, 0x1FFFFFFFFFULL, 0x3FFFFFFFFFULL, 0x7FFFFFFFFFULL, 0xFFFFFFFFFFULL,
    0x1FFFFFFFFFFULL, 0x3FFFFFFFFFFULL, 0x7FFFFFFFFFFULL, 0xFFFFFFFFFFFULL, 0x1FFFFFFFFFFFULL, 0x3FFFFFFFFFFFULL, 0x7FFFFFFFFFFFULL, 0xFFFFFFFFFFFFULL,
    0x1FFFFFFFFFFFFULL, 0x3FFFFFFFFFFFFULL, 0x7FFFFFFFFFFFFULL, 0xFFFFFFFFFFFFFULL, 0x1FFFFFFFFFFFFFULL, 0x3FFFFFFFFFFFFFULL, 0x7FFFFFFFFFFFFFULL, 0xFFFFFFFFFFFFFFULL,
    0x1FFFFFFFFFFFFFFULL, 0x3FFFFFFFFFFFFFFULL, 0x7FFFFFFFFFFFFFFULL, 0xFFFFFFFFFFFFFFFULL, 0x1FFFFFFFFFFFFFFFULL, 0x3FFFFFFFFFFFFFFFULL, 0x7FFFFFFFFFFFFFFFULL, 0xFFFFFFFFFFFFFFFFULL
};

static constexpr uint64_t MSB_MASK_LUT64[65] = {
    0x0ULL,
    0x8000000000000000ULL, 0xC000000000000000ULL, 0xE000000000000000ULL, 0xF000000000000000ULL, 0xF800000000000000ULL, 0xFC00000000000000ULL, 0xFE00000000000000ULL, 0xFF00000000000000ULL,
    0xFF80000000000000ULL, 0xFFC0000000000000ULL, 0xFFE0000000000000ULL, 0xFFF0000000000000ULL, 0xFFF8000000000000ULL, 0xFFFC000000000000ULL, 0xFFFE000000000000ULL, 0xFFFF000000000000ULL,
    0xFFFF800000000000ULL, 0xFFFFC00000000000ULL, 0xFFFFE00000000000ULL, 0xFFFFF00000000000ULL, 0xFFFFF80000000000ULL, 0xFFFFFC0000000000ULL, 0xFFFFFE0000000000ULL, 0xFFFFFF0000000000ULL,
    0xFFFFFF8000000000ULL, 0xFFFFFFC000000000ULL, 0xFFFFFFE000000000ULL, 0xFFFFFFF000000000ULL, 0xFFFFFFF800000000ULL, 0xFFFFFFFC00000000ULL, 0xFFFFFFFE00000000ULL, 0xFFFFFFFF00000000ULL,
    0xFFFFFFFF80000000ULL, 0xFFFFFFFFC0000000ULL, 0xFFFFFFFFE0000000ULL, 0xFFFFFFFFF0000000ULL, 0xFFFFFFFFF8000000ULL, 0xFFFFFFFFFC000000ULL, 0xFFFFFFFFFE000000ULL, 0xFFFFFFFFFF000000ULL,
    0xFFFFFFFFFF800000ULL, 0xFFFFFFFFFFC00000ULL, 0xFFFFFFFFFFE00000ULL, 0xFFFFFFFFFFF00000ULL, 0xFFFFFFFFFFF80000ULL, 0xFFFFFFFFFFFC0000ULL, 0xFFFFFFFFFFFE0000ULL, 0xFFFFFFFFFFFF0000ULL,
    0xFFFFFFFFFFFF8000ULL, 0xFFFFFFFFFFFFC000ULL, 0xFFFFFFFFFFFFE000ULL, 0xFFFFFFFFFFFFF000ULL, 0xFFFFFFFFFFFFF800ULL, 0xFFFFFFFFFFFFFC00ULL, 0xFFFFFFFFFFFFFE00ULL, 0xFFFFFFFFFFFFFF00ULL,
    0xFFFFFFFFFFFFFF80ULL, 0xFFFFFFFFFFFFFFC0ULL, 0xFFFFFFFFFFFFFFE0ULL, 0xFFFFFFFFFFFFFFF0ULL, 0xFFFFFFFFFFFFFFF8ULL, 0xFFFFFFFFFFFFFFFCULL, 0xFFFFFFFFFFFFFFFEULL, 0xFFFFFFFFFFFFFFFFULL
};

_YAEF_ATTR_NODISCARD _YAEF_ATTR_FORCEINLINE uint64_t make_mask_lsb1_lut(uint32_t n) noexcept {
    _YAEF_ASSERT(n <= 64);
    return LSB_MASK_LUT64[n];
}

_YAEF_ATTR_NODISCARD _YAEF_ATTR_FORCEINLINE uint64_t make_mask_msb1_lut(uint32_t n) noexcept {
    _YAEF_ASSERT(n <= 64);
    return MSB_MASK_LUT64[n];
}

_YAEF_ATTR_NODISCARD _YAEF_ATTR_FORCEINLINE uint64_t make_mask_lsb1(uint32_t n) noexcept {
    _YAEF_ASSERT(n <= 64);

#if _YAEF_INTRINSICS_HAVE_BMI2
    return _bzhi_u64(~static_cast<uint64_t>(0), n);
#else
    return make_mask_lsb1_lut(n);
#endif
}

_YAEF_ATTR_NODISCARD _YAEF_ATTR_FORCEINLINE uint64_t make_mask_msb1(uint32_t n) noexcept {
    _YAEF_ASSERT(n <= 64);

#if _YAEF_INTRINSICS_HAVE_BMI2
    return ~_bzhi_u64(~static_cast<uint64_t>(0), 64 - n);
#else
    return make_mask_msb1_lut(n);
#endif
}

_YAEF_ATTR_NODISCARD inline bool get_bit(uint64_t block, uint32_t index) noexcept {
    _YAEF_ASSERT(index < 64);
#ifdef _MSC_VER
    return _bittest64(reinterpret_cast<__int64 *>(&block), index);
#else
    return (block >> index) & static_cast<uint64_t>(1);
#endif
}

_YAEF_ATTR_NODISCARD inline uint64_t set_bit(uint64_t block, uint32_t index) noexcept {
    _YAEF_ASSERT(index < 64);
    return block | (static_cast<uint64_t>(1) << index);
}

_YAEF_ATTR_NODISCARD inline uint64_t clear_bit(uint64_t block, uint32_t index) noexcept {
    _YAEF_ASSERT(index < 64);
    return block & ~(static_cast<uint64_t>(1) << index);
}

_YAEF_ATTR_NODISCARD inline uint64_t set_bit(uint64_t block, uint32_t index, bool value) noexcept {
    _YAEF_ASSERT(index < 64);
    return (block & ~(static_cast<uint64_t>(1) << index)) | 
           (static_cast<uint64_t>(value) << index);
}

_YAEF_ATTR_NODISCARD inline uint64_t extract_bits(uint64_t block, uint32_t first, uint32_t last) noexcept {
    _YAEF_ASSERT(first < 64);
    _YAEF_ASSERT(last <= 64);
    _YAEF_ASSERT(first <= last);
    return (block >> first) & make_mask_lsb1(last - first);
}

_YAEF_ATTR_NODISCARD inline uint64_t extract_first_bits(uint64_t block, uint32_t n) noexcept {
    _YAEF_ASSERT(n <= 64);
    return block & make_mask_lsb1(n);
}

_YAEF_ATTR_NODISCARD inline uint64_t extract_last_bits(uint64_t block, uint32_t n) noexcept {
    _YAEF_ASSERT(n <= 64);
    return rotate_left(block, n) & make_mask_lsb1(n);
}

_YAEF_ATTR_NODISCARD inline uint64_t set_bits(uint64_t block, uint32_t offset, uint64_t value, uint32_t n) noexcept {
    _YAEF_ASSERT(offset < 64);
    _YAEF_ASSERT(n <= 64);
    _YAEF_ASSERT(offset + n <= 64);
    uint64_t bits = extract_first_bits(value, n);
    uint64_t block_mask = ((~make_mask_lsb1(n)) << offset) | make_mask_lsb1(offset);
    return (block & block_mask) | (bits << offset);
}

_YAEF_ATTR_NODISCARD inline uint64_t set_first_bits(uint64_t block, uint64_t value, uint32_t n) noexcept {
    _YAEF_ASSERT(n <= 64);
    return (block & (~make_mask_lsb1(n))) | extract_first_bits(value, n);
}

_YAEF_ATTR_NODISCARD inline uint64_t set_last_bits(uint64_t block, uint64_t value, uint32_t n) noexcept {
    _YAEF_ASSERT(n <= 64);
    return (block & (~make_mask_msb1(n))) | rotate_right(extract_first_bits(value, n), n);
}

// adapt from the implementation used in Sux project
// reference: https://github.com/vigna/sux
_YAEF_ATTR_NODISCARD inline uint32_t select_one_fallback(uint64_t block, uint32_t k) noexcept {
    static constexpr uint8_t SELECT_IN_BYTE_LUT[2048] = {
        8, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        7, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        8, 8, 8, 1, 8, 2, 2, 1, 8, 3, 3, 1, 3, 2, 2, 1, 8, 4, 4, 1, 4, 2, 2, 1, 4, 3, 3, 1, 3, 2, 2, 1, 8, 5, 5, 1, 5, 2, 2, 1, 5, 3, 3, 1, 3, 2, 2, 1, 5, 4, 4, 1, 4, 2, 2, 1, 4, 3, 3, 1, 3, 2, 2, 1,
        8, 6, 6, 1, 6, 2, 2, 1, 6, 3, 3, 1, 3, 2, 2, 1, 6, 4, 4, 1, 4, 2, 2, 1, 4, 3, 3, 1, 3, 2, 2, 1, 6, 5, 5, 1, 5, 2, 2, 1, 5, 3, 3, 1, 3, 2, 2, 1, 5, 4, 4, 1, 4, 2, 2, 1, 4, 3, 3, 1, 3, 2, 2, 1,
        8, 7, 7, 1, 7, 2, 2, 1, 7, 3, 3, 1, 3, 2, 2, 1, 7, 4, 4, 1, 4, 2, 2, 1, 4, 3, 3, 1, 3, 2, 2, 1, 7, 5, 5, 1, 5, 2, 2, 1, 5, 3, 3, 1, 3, 2, 2, 1, 5, 4, 4, 1, 4, 2, 2, 1, 4, 3, 3, 1, 3, 2, 2, 1,
        7, 6, 6, 1, 6, 2, 2, 1, 6, 3, 3, 1, 3, 2, 2, 1, 6, 4, 4, 1, 4, 2, 2, 1, 4, 3, 3, 1, 3, 2, 2, 1, 6, 5, 5, 1, 5, 2, 2, 1, 5, 3, 3, 1, 3, 2, 2, 1, 5, 4, 4, 1, 4, 2, 2, 1, 4, 3, 3, 1, 3, 2, 2, 1,
        8, 8, 8, 8, 8, 8, 8, 2, 8, 8, 8, 3, 8, 3, 3, 2, 8, 8, 8, 4, 8, 4, 4, 2, 8, 4, 4, 3, 4, 3, 3, 2, 8, 8, 8, 5, 8, 5, 5, 2, 8, 5, 5, 3, 5, 3, 3, 2, 8, 5, 5, 4, 5, 4, 4, 2, 5, 4, 4, 3, 4, 3, 3, 2,
        8, 8, 8, 6, 8, 6, 6, 2, 8, 6, 6, 3, 6, 3, 3, 2, 8, 6, 6, 4, 6, 4, 4, 2, 6, 4, 4, 3, 4, 3, 3, 2, 8, 6, 6, 5, 6, 5, 5, 2, 6, 5, 5, 3, 5, 3, 3, 2, 6, 5, 5, 4, 5, 4, 4, 2, 5, 4, 4, 3, 4, 3, 3, 2,
        8, 8, 8, 7, 8, 7, 7, 2, 8, 7, 7, 3, 7, 3, 3, 2, 8, 7, 7, 4, 7, 4, 4, 2, 7, 4, 4, 3, 4, 3, 3, 2, 8, 7, 7, 5, 7, 5, 5, 2, 7, 5, 5, 3, 5, 3, 3, 2, 7, 5, 5, 4, 5, 4, 4, 2, 5, 4, 4, 3, 4, 3, 3, 2,
        8, 7, 7, 6, 7, 6, 6, 2, 7, 6, 6, 3, 6, 3, 3, 2, 7, 6, 6, 4, 6, 4, 4, 2, 6, 4, 4, 3, 4, 3, 3, 2, 7, 6, 6, 5, 6, 5, 5, 2, 6, 5, 5, 3, 5, 3, 3, 2, 6, 5, 5, 4, 5, 4, 4, 2, 5, 4, 4, 3, 4, 3, 3, 2,
        8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 3, 8, 8, 8, 8, 8, 8, 8, 4, 8, 8, 8, 4, 8, 4, 4, 3, 8, 8, 8, 8, 8, 8, 8, 5, 8, 8, 8, 5, 8, 5, 5, 3, 8, 8, 8, 5, 8, 5, 5, 4, 8, 5, 5, 4, 5, 4, 4, 3,
        8, 8, 8, 8, 8, 8, 8, 6, 8, 8, 8, 6, 8, 6, 6, 3, 8, 8, 8, 6, 8, 6, 6, 4, 8, 6, 6, 4, 6, 4, 4, 3, 8, 8, 8, 6, 8, 6, 6, 5, 8, 6, 6, 5, 6, 5, 5, 3, 8, 6, 6, 5, 6, 5, 5, 4, 6, 5, 5, 4, 5, 4, 4, 3,
        8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 7, 3, 8, 8, 8, 7, 8, 7, 7, 4, 8, 7, 7, 4, 7, 4, 4, 3, 8, 8, 8, 7, 8, 7, 7, 5, 8, 7, 7, 5, 7, 5, 5, 3, 8, 7, 7, 5, 7, 5, 5, 4, 7, 5, 5, 4, 5, 4, 4, 3,
        8, 8, 8, 7, 8, 7, 7, 6, 8, 7, 7, 6, 7, 6, 6, 3, 8, 7, 7, 6, 7, 6, 6, 4, 7, 6, 6, 4, 6, 4, 4, 3, 8, 7, 7, 6, 7, 6, 6, 5, 7, 6, 6, 5, 6, 5, 5, 3, 7, 6, 6, 5, 6, 5, 5, 4, 6, 5, 5, 4, 5, 4, 4, 3,
        8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 4, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 5, 8, 8, 8, 8, 8, 8, 8, 5, 8, 8, 8, 5, 8, 5, 5, 4,
        8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 6, 8, 8, 8, 8, 8, 8, 8, 6, 8, 8, 8, 6, 8, 6, 6, 4, 8, 8, 8, 8, 8, 8, 8, 6, 8, 8, 8, 6, 8, 6, 6, 5, 8, 8, 8, 6, 8, 6, 6, 5, 8, 6, 6, 5, 6, 5, 5, 4,
        8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 7, 4, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 7, 5, 8, 8, 8, 7, 8, 7, 7, 5, 8, 7, 7, 5, 7, 5, 5, 4,
        8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 7, 6, 8, 8, 8, 7, 8, 7, 7, 6, 8, 7, 7, 6, 7, 6, 6, 4, 8, 8, 8, 7, 8, 7, 7, 6, 8, 7, 7, 6, 7, 6, 6, 5, 8, 7, 7, 6, 7, 6, 6, 5, 7, 6, 6, 5, 6, 5, 5, 4,
        8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 5,
        8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 6, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 6, 8, 8, 8, 8, 8, 8, 8, 6, 8, 8, 8, 6, 8, 6, 6, 5,
        8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 7, 5,
        8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 7, 6, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 7, 6, 8, 8, 8, 7, 8, 7, 7, 6, 8, 7, 7, 6, 7, 6, 6, 5,
        8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
        8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 6,
        8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7,
        8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 7, 6,
        8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
        8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
        8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
        8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7
    };

    constexpr uint64_t ONE_STEP4 = 0x1111111111111111ULL;
    constexpr uint64_t ONE_STEP8 = 0x0101010101010101ULL;
    constexpr uint64_t MSB_STEP8 = 0x80ULL * ONE_STEP8;

    uint64_t s = block;
    s = s - ((s & 0xA * ONE_STEP4) >> 1);
    s = (s & 0x3 * ONE_STEP4) + ((s >> 2) & 0x3 * ONE_STEP4);
    s = (s + (s >> 4)) & 0xF * ONE_STEP8;
    uint64_t byte_sums = s * ONE_STEP8;

    uint64_t place = bits64::popcount((((k * ONE_STEP8) | MSB_STEP8) - byte_sums) & MSB_STEP8) * 8;

    uint64_t byte_rank = k - (((byte_sums << 8) >> place) & static_cast<uint64_t>(0xFF));
    return place + SELECT_IN_BYTE_LUT[((block >> place) & 0xFF) | (byte_rank << 8)];
}

_YAEF_ATTR_NODISCARD inline uint32_t select_one(uint64_t block, uint32_t k) noexcept {
    _YAEF_ASSERT(k < 64);
#if _YAEF_INTRINSICS_HAVE_BMI2
    return count_trailing_zero(_pdep_u64(static_cast<uint64_t>(1) << k, block));
#else
    return select_one_fallback(block, k);
#endif
}

_YAEF_ATTR_NODISCARD inline uint32_t select_zero(uint64_t block, uint32_t k) noexcept {
    _YAEF_ASSERT(k < 64);
    return select_one(~block, k);
}

_YAEF_ATTR_NODISCARD inline uint64_t idiv_ceil(uint64_t lhs, uint64_t rhs) noexcept {
    return lhs == 0 ? 0 : (lhs - 1) / rhs + 1;
}

// `lhs` should never be 0
_YAEF_ATTR_NODISCARD inline uint64_t idiv_ceil_nzero(uint64_t lhs, uint64_t rhs) noexcept {
    _YAEF_ASSERT(lhs > 0);
    _YAEF_ASSUME(lhs > 0);
    return (lhs - 1) / rhs + 1;
}

// return count of 1s in preceding k bits 
_YAEF_ATTR_NODISCARD inline size_t 
popcount_blocks(const uint64_t *blocks, size_t num_blocks, size_t k) {
    constexpr size_t BLOCK_WIDTH = sizeof(uint64_t) * CHAR_BIT;

    k = std::min(k, num_blocks * BLOCK_WIDTH);
    const size_t num_skipped_blocks = k / BLOCK_WIDTH,
                 num_rem_bits = k % BLOCK_WIDTH;
    
    size_t num_ones = 0;
    for (size_t i = 0; i < num_skipped_blocks; ++i) {
        num_ones += popcount(blocks[i]);
    }
    if (num_rem_bits != 0) {
        num_ones += popcount(extract_first_bits(blocks[num_skipped_blocks], num_rem_bits));
    }
    return num_ones;
}

#if _YAEF_INTRINSICS_HAVE_AVX512
template<bool Aligned = false>
_YAEF_ATTR_NODISCARD inline size_t
popcount_blocks_512_avx512(const uint64_t *blocks, size_t num_blocks, size_t k) {
    constexpr size_t BLOCK_WIDTH = sizeof(uint64_t) * CHAR_BIT;
    const __m512i ZERO = _mm512_setzero_si512();

    k = std::min(k, num_blocks * BLOCK_WIDTH);
    
    __m512i loaded_vec;
    if _YAEF_CXX17_CONSTEXPR (Aligned) {
        loaded_vec = _mm512_load_si512(blocks);
    } else {
        loaded_vec = _mm512_loadu_si512(blocks);
    }

    __m512i popcnts_vec =  _mm512_popcnt_epi64(loaded_vec);
    popcnts_vec = _mm512_add_epi64(popcnts_vec, _mm512_alignr_epi64(popcnts_vec, ZERO, 8 - 1));
    popcnts_vec = _mm512_add_epi64(popcnts_vec, _mm512_alignr_epi64(popcnts_vec, ZERO, 8 - 2));
    popcnts_vec = _mm512_add_epi64(popcnts_vec, _mm512_alignr_epi64(popcnts_vec, ZERO, 8 - 4));

    const size_t last_block_index = k / BLOCK_WIDTH, 
                 last_rem_bits = k % BLOCK_WIDTH;

    size_t res = 0;
    if (_YAEF_LIKELY(last_block_index != num_blocks)) {
        const uint64_t last_block = extract_first_bits(blocks[last_block_index], last_rem_bits);
        res += popcount(last_block);    
    }

    const uint64_t prv_num_ones = _mm512_cvtsi512_si32(_mm512_maskz_permutexvar_epi64(
        last_block_index != 0, _mm512_set1_epi64(last_block_index - 1), popcnts_vec));
    res += prv_num_ones;

    return res;
}

template<bool Aligned = false>
_YAEF_ATTR_NODISCARD inline size_t
popcount_blocks_1024_avx512(const uint64_t *blocks, size_t num_blocks, size_t k) {
    constexpr size_t BLOCK_WIDTH = sizeof(uint64_t) * CHAR_BIT;
    const __m512i ZERO = _mm512_setzero_si512();

    k = std::min(k, num_blocks * BLOCK_WIDTH);
    
    __m512i loaded_vecs[2], popcnts_vecs[2];
    if _YAEF_CXX17_CONSTEXPR (Aligned) {
        loaded_vecs[0] = _mm512_load_si512(blocks);
        loaded_vecs[1] = _mm512_load_si512(blocks + 8);
    } else {
        loaded_vecs[0] = _mm512_loadu_si512(blocks);
        loaded_vecs[1] = _mm512_loadu_si512(blocks + 8);
    }

    popcnts_vecs[0] = _mm512_popcnt_epi64(loaded_vecs[0]);
    popcnts_vecs[1] = _mm512_popcnt_epi64(loaded_vecs[1]);

    popcnts_vecs[0] = _mm512_add_epi64(popcnts_vecs[0], _mm512_alignr_epi64(popcnts_vecs[0], ZERO, 8 - 1));
    popcnts_vecs[0] = _mm512_add_epi64(popcnts_vecs[0], _mm512_alignr_epi64(popcnts_vecs[0], ZERO, 8 - 2));
    popcnts_vecs[0] = _mm512_add_epi64(popcnts_vecs[0], _mm512_alignr_epi64(popcnts_vecs[0], ZERO, 8 - 4));

    __m512i prv_sum = _mm512_permutexvar_epi64(_mm512_set1_epi64(7), popcnts_vecs[0]);
    popcnts_vecs[1] = _mm512_add_epi64(popcnts_vecs[1], _mm512_alignr_epi64(popcnts_vecs[1], ZERO, 8 - 1));
    popcnts_vecs[1] = _mm512_add_epi64(popcnts_vecs[1], _mm512_alignr_epi64(popcnts_vecs[1], ZERO, 8 - 2));
    popcnts_vecs[1] = _mm512_add_epi64(popcnts_vecs[1], _mm512_alignr_epi64(popcnts_vecs[1], ZERO, 8 - 4));
    popcnts_vecs[1] = _mm512_add_epi64(popcnts_vecs[1], prv_sum);

    const size_t last_block_index = k / BLOCK_WIDTH, 
                 last_rem_bits = k % BLOCK_WIDTH;
    
    size_t res = 0;
    if (_YAEF_LIKELY(last_block_index != num_blocks)) {
        uint64_t loaded[16];
        memcpy(loaded, &loaded_vecs, sizeof(loaded_vecs));
        uint64_t last_block = extract_first_bits(loaded[last_block_index], last_rem_bits);
        res += popcount(last_block);
    }
    
    uint64_t popcnts[16];
    memcpy(popcnts, &popcnts_vecs, sizeof(popcnts_vecs));
    res += last_block_index == 0 ? 0 : popcnts[last_block_index - 1];

    return res;
}
#endif

_YAEF_ATTR_NODISCARD inline size_t
select_one_blocks(const uint64_t *blocks, size_t num_blocks, size_t k) {
    constexpr size_t BLOCK_WIDTH = sizeof(uint64_t) * CHAR_BIT;

    size_t num_ones = k + 1;
    const uint64_t *last_block = nullptr;

    if (_YAEF_UNLIKELY(num_ones == 0)) {
        return static_cast<size_t>(-1);
    }

    for (size_t i = 0; i < num_blocks; ++i) {
        uint32_t block_num_ones = popcount(blocks[i]);
        if (num_ones > block_num_ones) {
            num_ones -= block_num_ones;
        } else {
            last_block = blocks + i;
            break;
        }
    }
    if (_YAEF_UNLIKELY(last_block == nullptr)) {
        return num_blocks * BLOCK_WIDTH;
    }

    const size_t num_skipped_blocks = last_block - blocks;
    return num_skipped_blocks * BLOCK_WIDTH + select_one(*last_block, num_ones - 1);
}

_YAEF_ATTR_NODISCARD inline size_t
select_zero_blocks(const uint64_t *blocks, size_t num_blocks, size_t k) {
    constexpr size_t BLOCK_WIDTH = sizeof(uint64_t) * CHAR_BIT;

    size_t num_zeros = k + 1;
    const uint64_t *last_block = nullptr;

    if (_YAEF_UNLIKELY(num_zeros == 0)) {
        return static_cast<size_t>(-1);
    }

    for (size_t i = 0; i < num_blocks; ++i) {
        uint32_t block_num_zeros = popcount(~blocks[i]);
        if (num_zeros > block_num_zeros) {
            num_zeros -= block_num_zeros;
        } else {
            last_block = blocks + i;
            break;
        }
    }
    if (_YAEF_UNLIKELY(last_block == nullptr)) {
        return num_blocks * BLOCK_WIDTH;
    }

    const size_t num_skipped_blocks = last_block - blocks;
    return num_skipped_blocks * BLOCK_WIDTH + select_zero(*last_block, num_zeros - 1);
}

#if _YAEF_INTRINSICS_HAVE_AVX512
template<bool Aligned = false>
_YAEF_ATTR_NODISCARD inline size_t
select_one_blocks_512_avx512(const uint64_t *blocks, size_t num_blocks, size_t k) {
    constexpr size_t BLOCK_WIDTH = sizeof(uint64_t) * CHAR_BIT;
    const __m512i ZERO = _mm512_setzero_si512();

    if (_YAEF_UNLIKELY(k + 1 == 0)) {
        return static_cast<size_t>(-1);
    }
    
    __m512i loaded_vec;
    if _YAEF_CXX17_CONSTEXPR (Aligned) {
        loaded_vec = _mm512_load_si512(blocks);
    } else {
        loaded_vec = _mm512_loadu_si512(blocks);
    }
 
    __m512i popcnts_vec =  _mm512_popcnt_epi64(loaded_vec); 
    popcnts_vec = _mm512_add_epi64(popcnts_vec, _mm512_alignr_epi64(popcnts_vec, ZERO, 8 - 1));
    popcnts_vec = _mm512_add_epi64(popcnts_vec, _mm512_alignr_epi64(popcnts_vec, ZERO, 8 - 2));
    popcnts_vec = _mm512_add_epi64(popcnts_vec, _mm512_alignr_epi64(popcnts_vec, ZERO, 8 - 4));
    
    const __m512i index_vec = _mm512_set1_epi64(k + 1);
    const __mmask8 cmp_mask = _mm512_cmp_epu64_mask(index_vec, popcnts_vec, _MM_CMPINT_LE);
    const uint32_t lane_idx = count_trailing_zero(cmp_mask);
    if (_YAEF_UNLIKELY(lane_idx >= 8)) {
        return num_blocks * BLOCK_WIDTH;
    }

    const uint64_t prv_num_ones = _mm512_cvtsi512_si32(
        _mm512_maskz_permutexvar_epi64(lane_idx != 0, _mm512_set1_epi64(lane_idx - 1), popcnts_vec));

    const uint64_t local_idx = k - prv_num_ones;
    const uint64_t block = blocks[lane_idx];
    size_t pos_in_block = select_one(block, local_idx);
    return lane_idx * 64 + pos_in_block;
}

template<bool Aligned = false>
_YAEF_ATTR_NODISCARD inline size_t
select_zero_blocks_512_avx512(const uint64_t *blocks, size_t num_blocks, size_t k) {
    constexpr size_t BLOCK_WIDTH = sizeof(uint64_t) * CHAR_BIT;
    const __m512i ZERO = _mm512_setzero_si512();

    if (_YAEF_UNLIKELY(k + 1 == 0)) {
        return static_cast<size_t>(-1);
    }
    
    __m512i loaded_vec;
    if _YAEF_CXX17_CONSTEXPR (Aligned) {
        loaded_vec = _mm512_load_si512(blocks);
    } else {
        loaded_vec = _mm512_loadu_si512(blocks);
    }
    loaded_vec = ~loaded_vec;
 
    __m512i popcnts_vec =  _mm512_popcnt_epi64(loaded_vec); 
    popcnts_vec = _mm512_add_epi64(popcnts_vec, _mm512_alignr_epi64(popcnts_vec, ZERO, 8 - 1));
    popcnts_vec = _mm512_add_epi64(popcnts_vec, _mm512_alignr_epi64(popcnts_vec, ZERO, 8 - 2));
    popcnts_vec = _mm512_add_epi64(popcnts_vec, _mm512_alignr_epi64(popcnts_vec, ZERO, 8 - 4));
    
    const __m512i index_vec = _mm512_set1_epi64(k + 1);
    const __mmask8 cmp_mask = _mm512_cmp_epu64_mask(index_vec, popcnts_vec, _MM_CMPINT_LE);
    const uint32_t lane_idx = count_trailing_zero(cmp_mask);
    if (_YAEF_UNLIKELY(lane_idx >= 8)) {
        return num_blocks * BLOCK_WIDTH;
    }

    const uint64_t prv_num_ones = _mm512_cvtsi512_si32(
        _mm512_maskz_permutexvar_epi64(lane_idx != 0, _mm512_set1_epi64(lane_idx - 1), popcnts_vec));

    const uint64_t local_idx = k - prv_num_ones;
    const uint64_t block = ~blocks[lane_idx];
    size_t pos_in_block = select_one(block, local_idx);
    return lane_idx * BLOCK_WIDTH + pos_in_block;
}

template<bool Aligned = false>
_YAEF_ATTR_NODISCARD inline size_t
select_one_blocks_1024_avx512(const uint64_t *blocks, size_t num_blocks, size_t k) {
    constexpr size_t BLOCK_WIDTH = sizeof(uint64_t) * CHAR_BIT;
    const __m512i ZERO = _mm512_setzero_si512();

    if (_YAEF_UNLIKELY(k + 1 == 0)) {
        return static_cast<size_t>(-1);
    }

    __m512i loaded_vecs[2], popcnts_vecs[2];

    if _YAEF_CXX17_CONSTEXPR (Aligned) {
        loaded_vecs[0] = _mm512_load_si512(blocks);
        loaded_vecs[1] = _mm512_load_si512(blocks + 8);
    } else {
        loaded_vecs[0] = _mm512_loadu_si512(blocks);
        loaded_vecs[1] = _mm512_loadu_si512(blocks + 8);
    }

    popcnts_vecs[0] = _mm512_popcnt_epi64(loaded_vecs[0]);
    popcnts_vecs[1] = _mm512_popcnt_epi64(loaded_vecs[1]);

    popcnts_vecs[0] = _mm512_add_epi64(popcnts_vecs[0], _mm512_alignr_epi64(popcnts_vecs[0], ZERO, 8 - 1));
    popcnts_vecs[0] = _mm512_add_epi64(popcnts_vecs[0], _mm512_alignr_epi64(popcnts_vecs[0], ZERO, 8 - 2));
    popcnts_vecs[0] = _mm512_add_epi64(popcnts_vecs[0], _mm512_alignr_epi64(popcnts_vecs[0], ZERO, 8 - 4));

    __m512i prv_sum = _mm512_permutexvar_epi64(_mm512_set1_epi64(7), popcnts_vecs[0]);
    popcnts_vecs[1] = _mm512_add_epi64(popcnts_vecs[1], _mm512_alignr_epi64(popcnts_vecs[1], ZERO, 8 - 1));
    popcnts_vecs[1] = _mm512_add_epi64(popcnts_vecs[1], _mm512_alignr_epi64(popcnts_vecs[1], ZERO, 8 - 2));
    popcnts_vecs[1] = _mm512_add_epi64(popcnts_vecs[1], _mm512_alignr_epi64(popcnts_vecs[1], ZERO, 8 - 4));
    popcnts_vecs[1] = _mm512_add_epi64(popcnts_vecs[1], prv_sum);

    const __m512i index_vec = _mm512_set1_epi64(k + 1);
    const __mmask8 cmp1_mask = _mm512_cmp_epu64_mask(index_vec, popcnts_vecs[0], _MM_CMPINT_LE);
    const __mmask8 cmp2_mask = _mm512_cmp_epu64_mask(index_vec, popcnts_vecs[1], _MM_CMPINT_LE);
    const __mmask16 cmp_mask = (static_cast<__mmask16>(cmp2_mask) << 8) | cmp1_mask;

    const uint32_t lane_idx = count_trailing_zero(cmp_mask);
    if (_YAEF_UNLIKELY(lane_idx >= 16)) {
        return num_blocks * BLOCK_WIDTH;
    }

    uint64_t loaded[16], popcnts[16];
    memcpy(loaded, &loaded_vecs, sizeof(loaded_vecs));
    memcpy(popcnts, &popcnts_vecs, sizeof(popcnts_vecs));

    const uint64_t prev_num_ones = lane_idx == 0 ? 0 : popcnts[lane_idx - 1];
    const uint64_t local_idx = k - prev_num_ones;
    const uint64_t block = loaded[lane_idx];
    size_t pos_in_block = select_one(block, local_idx);
    return lane_idx * BLOCK_WIDTH + pos_in_block;
}

template<bool Aligned = false>
_YAEF_ATTR_NODISCARD inline size_t
select_zero_blocks_1024_avx512(const uint64_t *blocks, size_t num_blocks, size_t k) {
    constexpr size_t BLOCK_WIDTH = sizeof(uint64_t) * CHAR_BIT;
    const __m512i ZERO = _mm512_setzero_si512();

    if (_YAEF_UNLIKELY(k + 1 == 0)) {
        return static_cast<size_t>(-1);
    }

    __m512i loaded_vecs[2], popcnts_vecs[2];

    if _YAEF_CXX17_CONSTEXPR (Aligned) {
        loaded_vecs[0] = _mm512_load_si512(blocks);
        loaded_vecs[1] = _mm512_load_si512(blocks + 8);
    } else {
        loaded_vecs[0] = _mm512_loadu_si512(blocks);
        loaded_vecs[1] = _mm512_loadu_si512(blocks + 8);
    }
    loaded_vecs[0] = ~loaded_vecs[0];
    loaded_vecs[1] = ~loaded_vecs[1];

    popcnts_vecs[0] = _mm512_popcnt_epi64(loaded_vecs[0]);
    popcnts_vecs[1] = _mm512_popcnt_epi64(loaded_vecs[1]);

    popcnts_vecs[0] = _mm512_add_epi64(popcnts_vecs[0], _mm512_alignr_epi64(popcnts_vecs[0], ZERO, 8 - 1));
    popcnts_vecs[0] = _mm512_add_epi64(popcnts_vecs[0], _mm512_alignr_epi64(popcnts_vecs[0], ZERO, 8 - 2));
    popcnts_vecs[0] = _mm512_add_epi64(popcnts_vecs[0], _mm512_alignr_epi64(popcnts_vecs[0], ZERO, 8 - 4));

    __m512i prv_sum = _mm512_permutexvar_epi64(_mm512_set1_epi64(7), popcnts_vecs[0]);
    popcnts_vecs[1] = _mm512_add_epi64(popcnts_vecs[1], _mm512_alignr_epi64(popcnts_vecs[1], ZERO, 8 - 1));
    popcnts_vecs[1] = _mm512_add_epi64(popcnts_vecs[1], _mm512_alignr_epi64(popcnts_vecs[1], ZERO, 8 - 2));
    popcnts_vecs[1] = _mm512_add_epi64(popcnts_vecs[1], _mm512_alignr_epi64(popcnts_vecs[1], ZERO, 8 - 4));
    popcnts_vecs[1] = _mm512_add_epi64(popcnts_vecs[1], prv_sum);

    const __m512i index_vec = _mm512_set1_epi64(k + 1);
    const __mmask8 cmp1_mask = _mm512_cmp_epu64_mask(index_vec, popcnts_vecs[0], _MM_CMPINT_LE);
    const __mmask8 cmp2_mask = _mm512_cmp_epu64_mask(index_vec, popcnts_vecs[1], _MM_CMPINT_LE);
    const __mmask16 cmp_mask = (static_cast<__mmask16>(cmp2_mask) << 8) | cmp1_mask;

    const uint32_t lane_idx = count_trailing_zero(cmp_mask);
    if (_YAEF_UNLIKELY(lane_idx >= 16)) {
        return num_blocks * BLOCK_WIDTH;
    }

    uint64_t loaded[16], popcnts[16];
    memcpy(loaded, &loaded_vecs, sizeof(loaded_vecs));
    memcpy(popcnts, &popcnts_vecs, sizeof(popcnts_vecs));

    const uint64_t prev_num_ones = lane_idx == 0 ? 0 : popcnts[lane_idx - 1];
    const uint64_t local_idx = k - prev_num_ones;
    const uint64_t block = loaded[lane_idx];
    size_t pos_in_block = select_one(block, local_idx);
    return lane_idx * BLOCK_WIDTH + pos_in_block;
}
#endif

class bit_view;
class packed_int_view;

class packed_int_view {
public:
    using value_type = uint64_t;
    using block_type = uint64_t;
    using size_type  = size_t;
    static constexpr uint32_t BLOCK_WIDTH = sizeof(block_type) * CHAR_BIT;

public:
    packed_int_view() noexcept
        : blocks_(nullptr), num_elems_(0), width_(0) { }

    packed_int_view(const packed_int_view &) = default;

    packed_int_view(uint32_t width, block_type *blocks, size_type num_elems) noexcept
        : blocks_(blocks), num_elems_(num_elems), width_(width) { }

    packed_int_view &operator=(const packed_int_view &) = default;

    _YAEF_ATTR_NODISCARD size_type size() const noexcept { return num_elems_; }
    _YAEF_ATTR_NODISCARD bool empty() const noexcept { return size() == 0; }
    _YAEF_ATTR_NODISCARD uint32_t width() const noexcept { return width_; }
    _YAEF_ATTR_NODISCARD const block_type *blocks() const noexcept { return blocks_; }
    _YAEF_ATTR_NODISCARD block_type *blocks() noexcept { return blocks_; }

    _YAEF_ATTR_NODISCARD size_type num_blocks() const noexcept { 
        return bits64::idiv_ceil(num_elems_ * width_, BLOCK_WIDTH); 
    }

    _YAEF_ATTR_NODISCARD value_type limit_min() const { return 0; }
    
    _YAEF_ATTR_NODISCARD value_type limit_max() const {
        if (_YAEF_UNLIKELY(width() == sizeof(value_type) * CHAR_BIT)) { 
            return std::numeric_limits<value_type>::max(); 
        }
        return (static_cast<value_type>(1) << width()) - 1;
    }

    _YAEF_ATTR_NODISCARD size_type space_usage_in_bytes() const noexcept {
        return sizeof(block_type) * num_blocks();
    }

    void fill(value_type value) {
        if (_YAEF_UNLIKELY(value == limit_min())) {
            clear_all_bits();
            return;
        }
        if (_YAEF_UNLIKELY(value == limit_max())) {
            set_all_bits();
            return;
        }
        for (size_type i = 0; i < size(); ++i) {
            set_value(i, value);
        }
    }

    void prefetch_for_read(size_type first, size_type last) const noexcept {
        _YAEF_ASSERT(first <= last);
        _YAEF_ASSERT(last <= size());

        const size_type first_block_index = first * width() / BLOCK_WIDTH;
        const size_type last_block_index = last * width() / BLOCK_WIDTH;
        const size_type num_blocks = last_block_index - first_block_index + 1;
        constexpr size_type STEP = 64 / sizeof(block_type);
        for (size_type i = 0; i < num_blocks; i += STEP) {
            prefetch_read(blocks() + first_block_index + i);
        }
    }

    void prefetch_for_write(size_type first, size_type last) noexcept {
        _YAEF_ASSERT(first <= last);
        _YAEF_ASSERT(last <= size());

        const size_type first_block_index = first * width() / BLOCK_WIDTH;
        const size_type last_block_index = last * width() / BLOCK_WIDTH;
        const size_type num_blocks = last_block_index - first_block_index + 1;
        constexpr size_type STEP = 64 / sizeof(block_type);
        for (size_type i = 0; i < num_blocks; i += STEP) {
            prefetch_write(blocks() + first_block_index + i);
        }
    }

    _YAEF_ATTR_NODISCARD bit_view to_bit_view() noexcept;

    _YAEF_ATTR_NODISCARD value_type get_value(size_type index) const noexcept {
        _YAEF_ASSERT(index < size());
        const size_type bit_index = index * width();
        const size_type block_index = bit_index / BLOCK_WIDTH, 
                        block_offset = bit_index % BLOCK_WIDTH;
    
#if _YAEF_INTRINSICS_HAVE_AVX2 && defined(__SIZEOF_INT128__) && __SIZEOF_INT128__ == 16
        __uint128_t combined;
        memcpy(&combined, blocks_ + block_index, sizeof(combined));
        uint64_t result = static_cast<uint64_t>(combined >> block_offset);
        return result & make_mask_lsb1(width_);
#elif _YAEF_INTRINSICS_HAVE_AVX2
        __m128i words = _mm_loadu_si128(reinterpret_cast<const __m128i *>(blocks_ + block_index));
        __m128i shifted = _mm_srli_epi64(words, block_offset);
        __m128i carry = _mm_bsrli_si128(_mm_slli_epi64(words, BLOCK_WIDTH - block_offset), 8);
        words = _mm_or_si128(shifted, carry);
        return _mm_cvtsi128_si64(words) & make_mask_lsb1(width());
#else
        if (block_offset + width() > BLOCK_WIDTH) {
            const uint32_t num_low_bits = BLOCK_WIDTH - block_offset;
            block_type low = extract_last_bits(blocks_[block_index], num_low_bits);
            block_type high = extract_first_bits(blocks_[block_index + 1], width() - num_low_bits);
            return (high << num_low_bits) | low;
        } else {
            return extract_bits(blocks_[block_index], block_offset, block_offset + width());
        }
#endif
    }

    void set_value(size_type index, value_type value) noexcept {
        _YAEF_ASSERT(index < size());
        const size_type bit_index = index * width();
        const size_type block_index = bit_index / BLOCK_WIDTH, block_offset = bit_index % BLOCK_WIDTH;

        block_type val = value;
        if (block_offset + width() > BLOCK_WIDTH) {
            const uint32_t num_low_bits = BLOCK_WIDTH - block_offset;
            block_type &block0 = blocks_[block_index], &block1 = blocks_[block_index + 1];
            block0 = set_last_bits(block0, val, num_low_bits);
            block1 = set_first_bits(block1, val >> num_low_bits, width() - num_low_bits);
        } else {
            block_type &block = blocks_[block_index];
            block = set_bits(block, block_offset, val, width());
        }
    }

    _YAEF_ATTR_NODISCARD bool get_bit(size_type index) const noexcept {
        _YAEF_ASSERT(index < size() * width());
        auto info = locate_block(index);
        return bits64::get_bit(*info.first, info.second);
    }

    void set_bit(size_type index, bool value) noexcept {
        _YAEF_ASSERT(index < size() * width());
        auto info = locate_block(index);
        *info.first = bits64::set_bit(*info.first, info.second, value);
    }

    void set_bit(size_type index) noexcept {
        _YAEF_ASSERT(index < size() * width());
        auto info = locate_block(index);
        *info.first = bits64::set_bit(*info.first, info.second);
    }

    void clear_bit(size_type index) noexcept {
        _YAEF_ASSERT(index < size() * width());
        auto info = locate_block(index);
        *info.first = bits64::clear_bit(*info.first, info.second);
    }

    void clear_all_bits() {
        std::fill_n(blocks_, num_blocks(), 0);
    }

    void set_all_bits() {
        if (_YAEF_UNLIKELY(num_blocks() == 0)) {
            return;
        }
        std::fill_n(blocks_, num_blocks() - 1, std::numeric_limits<block_type>::max());
        blocks_[num_blocks() - 1] = make_mask_lsb1(size() * width() - (num_blocks() - 1) * BLOCK_WIDTH);
    }

    void swap(packed_int_view &other) noexcept {
        std::swap(blocks_, other.blocks_);
        std::swap(num_elems_, other.num_elems_);
        std::swap(width_, other.width_);
    }

    error_code serialize(serializer &ser) const;

    template<typename AllocT>
    error_code deserialize(AllocT &alloc, deserializer &deser);

private:
    block_type *blocks_;
    size_type   num_elems_;
    uint32_t    width_;

    _YAEF_ATTR_NODISCARD std::pair<const block_type *, uint32_t> locate_block(size_type bit_index) const noexcept {
        const size_type block_index = bit_index / BLOCK_WIDTH, block_offset = bit_index % BLOCK_WIDTH;
        return std::make_pair(blocks_ + block_index, static_cast<uint32_t>(block_offset));
    }

    _YAEF_ATTR_NODISCARD std::pair<block_type *, uint32_t> locate_block(size_type bit_index) noexcept {
        const size_type block_index = bit_index / BLOCK_WIDTH, block_offset = bit_index % BLOCK_WIDTH;
        return std::make_pair(blocks_ + block_index, static_cast<uint32_t>(block_offset));
    }
};

_YAEF_ATTR_NODISCARD inline bool operator==(const packed_int_view &lhs, const packed_int_view &rhs) noexcept {
    if (_YAEF_UNLIKELY(std::addressof(lhs) == std::addressof(rhs))) {
        return true;
    }
    if (lhs.width() != rhs.width() || lhs.size() != rhs.size()) {
        return false;
    }
    return std::equal(lhs.blocks(), lhs.blocks() + lhs.num_blocks(), rhs.blocks());
}

#if __cplusplus < 202002L
_YAEF_ATTR_NODISCARD inline bool operator!=(const packed_int_view &lhs, const packed_int_view &rhs) noexcept {
    return !(lhs == rhs);
}
#endif

class bits_reader {
public:
    bits_reader(const uint64_t *blocks, size_t num_blocks)
        : blocks_(blocks), num_blocks_(num_blocks) {
        refill();
    }

    _YAEF_ATTR_NODISCARD uint64_t read_bits(uint32_t width) {
        if (_YAEF_UNLIKELY(width > buf_size_)) {
            refill();
        }
        uint64_t val = extract_first_bits(static_cast<uint64_t>(buf_), width);
        buf_ >>= width;
        buf_size_ -= width;
        return val;
    }

    _YAEF_ATTR_NODISCARD std::array<uint16_t, 4> read_bits_4x16(std::array<uint16_t, 4> width4) {
        uint16_t total_bits = width4[0] + width4[1] + width4[2] + width4[3];
        if (_YAEF_UNLIKELY(total_bits > buf_size_)) {
            refill();
        }

        uint64_t width_packed;
        memcpy(&width_packed, width4.data(), sizeof(width4));
        __m128i counts_vec = _mm_cvtsi64_si128(width_packed);
        __m128i ones_vec = _mm_set1_epi16(1);
        __m128i shifted = _mm_sllv_epi16(ones_vec, counts_vec);
        uint64_t mask = _mm_cvtsi128_si64(_mm_sub_epi16(shifted, ones_vec));

        uint64_t packed_data = _pdep_u64(static_cast<uint64_t>(buf_), mask);
        std::array<uint16_t, 4> res;
        memcpy(res.data(), &packed_data, sizeof(packed_data));
        return res;
    }

    _YAEF_ATTR_NODISCARD uint64_t peek_bits(uint32_t width) {
        if (_YAEF_UNLIKELY(width > buf_size_)) {
            refill();
        }
        uint64_t val = extract_first_bits(static_cast<uint64_t>(buf_), width);
        return val;
    }

private:
    const uint64_t *blocks_;
    size_t          num_blocks_;
    __uint128_t     buf_       = 0;
    uint32_t        buf_size_  = 0;
    size_t          block_idx_ = 0;

    void refill() {
#ifdef NDEBUG
        _YAEF_UNUSED(num_blocks_);
#endif
        _YAEF_ASSERT(block_idx_ < num_blocks_);
        uint64_t new_block = blocks_[block_idx_++];
        buf_ |= static_cast<__uint128_t>(new_block) << buf_size_;
        buf_size_ += 64;
    }
};

class bit_view {
public:
    using value_type = bool;
    using size_type  = size_t;
    using block_type = uint64_t;
    static constexpr uint32_t BLOCK_WIDTH = sizeof(block_type) * CHAR_BIT;

    struct dont_care_size_t { };
    static constexpr dont_care_size_t dont_care_size{};

public:
    bit_view() noexcept
        : blocks_(nullptr), num_bits_(0) { }

    bit_view(const bit_view &) = default;

    bit_view(block_type *blocks, size_type num_bits) noexcept
        : blocks_(blocks), num_bits_(num_bits) { }
    
    bit_view(block_type *blocks, dont_care_size_t) noexcept
        : blocks_(blocks), num_bits_(std::numeric_limits<size_type>::max()) { }

    bit_view &operator=(const bit_view &) = default;

    _YAEF_ATTR_NODISCARD size_type size() const noexcept { return num_bits_; }
    _YAEF_ATTR_NODISCARD bool empty() const noexcept { return size() == 0; }
    _YAEF_ATTR_NODISCARD const block_type *blocks() const noexcept { return blocks_; }
    _YAEF_ATTR_NODISCARD block_type *blocks() noexcept { return blocks_; }

    _YAEF_ATTR_NODISCARD size_type num_blocks() const noexcept { 
        return bits64::idiv_ceil(num_bits_, BLOCK_WIDTH); 
    }
   
    _YAEF_ATTR_NODISCARD size_type space_usage_in_bytes() const noexcept {
        return sizeof(block_type) * num_blocks();
    }

    void prefetch_for_read(size_type first, size_type last) const noexcept {
        _YAEF_ASSERT(first <= last);
        _YAEF_ASSERT(last <= size());

        const size_type first_block_index = first / BLOCK_WIDTH;
        const size_type last_block_index = last / BLOCK_WIDTH;
        const size_type num_blocks = last_block_index - first_block_index + 1;
        constexpr size_type STEP = 64 / sizeof(block_type);
        for (size_type i = 0; i < num_blocks; i += STEP) {
            prefetch_read(blocks() + first_block_index + i);
        }
    }

    void prefetch_for_write(size_type first, size_type last) noexcept {
        _YAEF_ASSERT(first <= last);
        _YAEF_ASSERT(last <= size());

        const size_type first_block_index = first / BLOCK_WIDTH;
        const size_type last_block_index = last / BLOCK_WIDTH;
        const size_type num_blocks = last_block_index - first_block_index + 1;
        constexpr size_type STEP = 64 / sizeof(block_type);
        for (size_type i = 0; i < num_blocks; i += STEP) {
            prefetch_write(blocks() + first_block_index + i);
        }
    }

    _YAEF_ATTR_NODISCARD packed_int_view to_packed_int_view(uint32_t w) _YAEF_MAYBE_NOEXCEPT;

    _YAEF_ATTR_NODISCARD value_type get_bit(size_type index) const noexcept {
        _YAEF_ASSERT(index < size());
        auto info = locate_block(index);
        return bits64::get_bit(*info.first, info.second);
    }

    void set_bit(size_type index, value_type value) noexcept {
        _YAEF_ASSERT(index < size());
        auto info = locate_block(index);
        *info.first = bits64::set_bit(*info.first, info.second, value);
    }

    void set_bit(size_type index) noexcept {
        _YAEF_ASSERT(index < size());
        auto info = locate_block(index);
        *info.first = bits64::set_bit(*info.first, info.second);
    }

    void clear_bit(size_type index) noexcept {
        _YAEF_ASSERT(index < size());
        auto info = locate_block(index);
        *info.first = bits64::clear_bit(*info.first, info.second);
    }

    void set_all_bits() {
        if (_YAEF_UNLIKELY(num_blocks() == 0)) { return; }
        std::fill_n(blocks_, num_blocks() - 1, std::numeric_limits<block_type>::max());
        blocks_[num_blocks() - 1] = make_mask_lsb1(size() - (num_blocks() - 1) * BLOCK_WIDTH);
    }

    void clear_all_bits() {
        std::fill_n(blocks_, num_blocks(), 0);
    }

    void set_all_bits(size_type offset, size_type len) {
        _YAEF_ASSERT(offset < size());
        _YAEF_ASSERT(offset + len <= size());
        do_modify_bits<true>(offset, len);
    }

    void clear_all_bits(size_type offset, size_type len) {
        _YAEF_ASSERT(offset < size());
        _YAEF_ASSERT(offset + len <= size());
        do_modify_bits<false>(offset, len);
    }

    _YAEF_ATTR_NODISCARD uint64_t get_bits(size_type index, uint32_t w) const {
        _YAEF_ASSERT(index < size());
        _YAEF_ASSERT(index + w <= size());
        _YAEF_ASSERT(w > 0 && w <= 64);
        _YAEF_ASSUME(w > 0 && w <= 64);
        const size_type block_index = index / BLOCK_WIDTH,
                        bit_offset = index % BLOCK_WIDTH;
        alignas(16) __uint128_t combined;
        memcpy(&combined, blocks_ + block_index, sizeof(combined));
        
        uint64_t result = static_cast<uint64_t>(combined >> bit_offset);
        const uint64_t mask = make_mask_lsb1(w);
        return result & mask;
    }

    void set_bits(size_type index, uint32_t w, uint64_t bits) {
        _YAEF_ASSERT(index < size());
        _YAEF_ASSERT(index + w <= size());
        _YAEF_ASSERT(w > 0 && w <= 64);
        _YAEF_ASSUME(w > 0 && w <= 64);
        const size_type block_index = index / BLOCK_WIDTH,
                        bit_offset = index % BLOCK_WIDTH;

        block_type val = bits;
        if (bit_offset + w > BLOCK_WIDTH) {
            const uint32_t num_low_bits = BLOCK_WIDTH - bit_offset;
            block_type &block0 = blocks_[block_index], &block1 = blocks_[block_index + 1];
            block0 = set_last_bits(block0, val, num_low_bits);
            block1 = set_first_bits(block1, val >> num_low_bits, w - num_low_bits);
        } else {
            block_type &block = blocks_[block_index];
            block = bits64::set_bits(block, bit_offset, val, w);
        }
    }

    _YAEF_ATTR_NODISCARD bits_reader new_reader() const {
        return bits_reader(blocks_, num_blocks());
    }

    void swap(bit_view &other) noexcept {
        std::swap(blocks_, other.blocks_);
        std::swap(num_bits_, other.num_bits_);
    }

    error_code serialize(serializer &ser) const;

    template<typename AllocT>
    error_code deserialize(AllocT &alloc, deserializer &deser);

private:
    block_type *blocks_;
    size_type   num_bits_;

    _YAEF_ATTR_NODISCARD std::pair<const block_type *, uint32_t> locate_block(size_type index) const noexcept {
        const size_type block_index = index / BLOCK_WIDTH, block_offset = index % BLOCK_WIDTH;
        return std::make_pair(blocks_ + block_index, static_cast<uint32_t>(block_offset));
    }

    _YAEF_ATTR_NODISCARD std::pair<block_type *, uint32_t> locate_block(size_type index) noexcept {
        const size_type block_index = index / BLOCK_WIDTH, block_offset = index % BLOCK_WIDTH;
        return std::make_pair(blocks_ + block_index, static_cast<uint32_t>(block_offset));
    }

    template<bool Op>
    void do_modify_bits(size_t pos, size_t len) {
        if (_YAEF_UNLIKELY(len == 0)) {
            return;
        }

        if (pos % CHAR_BIT == 0 && len % CHAR_BIT == 0) {
            uint8_t *start_addr = reinterpret_cast<uint8_t *>(blocks_) + (pos / CHAR_BIT);
            size_type num_bytes = len / CHAR_BIT;
            if _YAEF_CXX17_CONSTEXPR (Op) {
                memset(start_addr, 0xFF, num_bytes);
            } else {
                memset(start_addr, 0, num_bytes);
            }
            return;
        }
        
        const size_t start_block_index = pos / BLOCK_WIDTH,
                     start_block_offset = pos % BLOCK_WIDTH;
        const size_t end_pos = pos + len - 1;
        const size_t end_block_index = end_pos / BLOCK_WIDTH,
                     end_block_offset = end_pos % BLOCK_WIDTH;

        if (start_block_index == end_block_index) {
            const uint64_t mask = make_mask_lsb1(len) << start_block_offset;
            if _YAEF_CXX17_CONSTEXPR (Op) {
                blocks_[start_block_index] |= mask;
            } else {
                blocks_[start_block_index] &= ~mask;
            }
        } else {
            const block_type head_mask = make_mask_msb1(BLOCK_WIDTH - start_block_offset);
            if _YAEF_CXX17_CONSTEXPR (Op) {
                blocks_[start_block_index] |= head_mask;
            } else {
                blocks_[start_block_index] &= ~head_mask;
            }

            const uint64_t middle_val = Op ? ~0ULL : 0ULL;
            for (size_t i = start_block_index + 1; i < end_block_index; ++i) {
                blocks_[i] = middle_val;
            }
            const uint64_t tail_mask = make_mask_lsb1(end_block_offset + 1);
            if _YAEF_CXX17_CONSTEXPR (Op) {
                blocks_[end_block_index] |= tail_mask;
            } else {
                blocks_[end_block_index] &= ~tail_mask;
            }
        }
    }
};

_YAEF_ATTR_NODISCARD inline bool operator==(const bit_view &lhs, const bit_view &rhs) noexcept {
    if (_YAEF_UNLIKELY(std::addressof(lhs) == std::addressof(rhs))) {
        return true;
    }
    if (lhs.size() != rhs.size()) {
        return false;
    }
    return std::equal(lhs.blocks(), lhs.blocks() + lhs.num_blocks(), rhs.blocks());
}

#if __cplusplus < 202002L
_YAEF_ATTR_NODISCARD inline bool operator!=(const bit_view &lhs, const bit_view &rhs) noexcept {
    return !(lhs == rhs);
}
#endif

inline bit_view packed_int_view::to_bit_view() noexcept {
    return bit_view(blocks_, num_elems_ * width_);
}

inline packed_int_view bit_view::to_packed_int_view(uint32_t w) _YAEF_MAYBE_NOEXCEPT {
    if (_YAEF_UNLIKELY(num_bits_ % w != 0)) {
        _YAEF_THROW(std::invalid_argument("the number of bits must be a multiple of `w`"));
    }
    return packed_int_view(w, blocks_, num_bits_ / w);
}

class bits_stat_info;
bits_stat_info stats_bits(const bit_view &) noexcept;

class bits_stat_info {
    friend bits_stat_info stats_bits(const bit_view &) noexcept;
public:
    using size_type = size_t;

    _YAEF_ATTR_NODISCARD size_type size() const noexcept { return size_; }
    _YAEF_ATTR_NODISCARD size_type num_ones() const noexcept { return num_ones_; }
    _YAEF_ATTR_NODISCARD size_type num_zeros() const noexcept { return size_ - num_ones_; }

    _YAEF_ATTR_NODISCARD double one_density() const noexcept {
        return static_cast<double>(num_ones()) / static_cast<double>(size());
    }
    
    _YAEF_ATTR_NODISCARD double zero_density() const noexcept {
        return static_cast<double>(num_zeros()) / static_cast<double>(size());
    }

private:
    size_type size_;
    size_type num_ones_;
};

_YAEF_ATTR_NODISCARD inline bits_stat_info stats_bits(const bit_view &bits) noexcept {
    using size_type = bit_view::size_type;
    constexpr size_type BLOCK_WIDTH = bit_view::BLOCK_WIDTH;

    const size_type num_bits = bits.size();
    const size_type *blocks = bits.blocks();
    const size_type num_full_blocks = num_bits / BLOCK_WIDTH,
                    num_residual_bits = num_bits % BLOCK_WIDTH;
    bits_stat_info info;
    info.size_ = num_bits;
    info.num_ones_ = 0;
    for (size_type i = 0; i < num_full_blocks; ++i) {
        info.num_ones_ += popcount(blocks[i]);
    }
    if (num_residual_bits != 0) {
        info.num_ones_ += popcount(extract_first_bits(blocks[num_full_blocks], num_residual_bits));
    }
    return info;
}

template<bool Cond>
struct conditional_bitwise_not;

template<>
struct conditional_bitwise_not<true> {
    _YAEF_ATTR_NODISCARD uint64_t operator()(uint64_t b) const noexcept { return ~b; }
};

template<>
struct conditional_bitwise_not<false> {
    _YAEF_ATTR_NODISCARD uint64_t operator()(uint64_t b) const noexcept { return b; }
};

template<typename F>
inline size_t bitmap_foreach_onebit(uint64_t block, const F &f, size_t index_offset = 0) {
    size_t popcnt = 0;
    while (block != 0) {
        uint64_t t = block & -block;
        size_t offset = index_offset + count_trailing_zero(block);
        block ^= t;
        f(offset);
        ++popcnt;
    }
    return popcnt;
}

template<typename F>
inline size_t bitmap_foreach_zerobit(uint64_t block, const F &f, size_t index_offset = 0) {
    return bitmap_foreach_onebit(~block, f, index_offset);
}

template<bool BitType, typename F>
inline size_t bitmap_multiblocks_foreach_impl(const uint64_t *blocks, size_t num_blocks, const F &f) {
    using block_handler = conditional_bitwise_not<!BitType>;
    size_t index_offset = 0;
    size_t popcnt = 0;
    for (size_t i = 0; i < num_blocks; ++i) {
        popcnt += bitmap_foreach_onebit(block_handler{}(blocks[i]), f, index_offset);
        index_offset += sizeof(uint64_t) * CHAR_BIT;
    }
    return popcnt;
}

template<typename F>
inline size_t bitmap_foreach_onebit(const uint64_t *blocks, size_t num_blocks, const F &f) {
    return bitmap_multiblocks_foreach_impl<true>(blocks, num_blocks, f);
}

template<typename F>
inline size_t bitmap_foreach_zerobit(const uint64_t *blocks, size_t num_blocks, const F &f) {
    return bitmap_multiblocks_foreach_impl<false>(blocks, num_blocks, f);
}

template<bool BitType>
class bitmap_foreach_cursor {
public:
    using size_type     = size_t;
    using block_type    = uint64_t;
    static constexpr bool BIT_TYPE = BitType;
    static constexpr uint32_t BLOCK_WIDTH = sizeof(block_type) * CHAR_BIT;

    struct nocheck_tag { };

private:
    using block_handler = conditional_bitwise_not<!BitType>;

public:
    bitmap_foreach_cursor() noexcept
        : blocks_beg_(nullptr), blocks_end_(nullptr), cached_(0) { }

    bitmap_foreach_cursor(const bitmap_foreach_cursor &other)
        : blocks_beg_(other.blocks_beg_), blocks_end_(other.blocks_end_), cached_(other.cached_) { }

    bitmap_foreach_cursor(const uint64_t *blocks, size_type num_blocks) noexcept
        : blocks_beg_(blocks), blocks_end_(blocks + num_blocks), cached_(0) {
        _YAEF_ASSERT(num_blocks != 0);
        for (const block_type *ptr = blocks_beg_; ptr < blocks_end_; ++ptr) {
            const block_type block = block_handler{}(*ptr);
            if (_YAEF_LIKELY(block != 0)) {
                const auto block_idx = static_cast<size_t>(ptr - blocks_beg_);
                cached_ = block_idx * BLOCK_WIDTH + count_trailing_zero(block);
                return;
            }
        }
        cached_ = num_blocks * BLOCK_WIDTH;
    }

    bitmap_foreach_cursor(const uint64_t *blocks, size_t num_blocks, size_type cached, nocheck_tag) noexcept
        : blocks_beg_(blocks), blocks_end_(blocks + num_blocks), cached_(cached) { }

    bitmap_foreach_cursor(const uint64_t *blocks, size_type num_blocks, size_type num_skipped_bits) noexcept
        : blocks_beg_(blocks), blocks_end_(blocks + num_blocks), cached_(0) {
        _YAEF_ASSERT(num_blocks != 0);
        _YAEF_ASSERT(idiv_ceil(num_skipped_bits, BLOCK_WIDTH) <= num_blocks);
        
        const size_type num_full_blocks = num_skipped_bits / BLOCK_WIDTH,
                        num_residual_bits = num_skipped_bits % BLOCK_WIDTH;
        const block_type mask = ~bits64::make_mask_lsb1(num_residual_bits);
        const block_type block = block_handler{}(blocks_beg_[num_full_blocks]) & mask;

        if (_YAEF_LIKELY(block != 0)) {
            cached_ = num_full_blocks * BLOCK_WIDTH + count_trailing_zero(block);
            return;
        }
        for (const block_type *ptr = blocks_beg_ + num_full_blocks + 1; ptr < blocks_end_; ++ptr) {
            const block_type block = block_handler{}(*ptr);
            if (block != 0) {
                const auto block_idx = static_cast<size_t>(ptr - blocks_beg_);
                cached_ = block_idx * BLOCK_WIDTH + count_trailing_zero(block);
                return;
            }
        }
        cached_ = num_blocks * BLOCK_WIDTH;
    }

    bitmap_foreach_cursor(const bit_view &bits) noexcept
        : bitmap_foreach_cursor(bits.blocks(), bits.num_blocks()) { }
    
    bitmap_foreach_cursor(const bit_view &bits, size_type num_skipped_bits) noexcept
        : bitmap_foreach_cursor(bits.blocks(), bits.num_blocks(), num_skipped_bits) { }

    bitmap_foreach_cursor(const bit_view &bits, size_type cached, nocheck_tag) noexcept
        : bitmap_foreach_cursor(bits.blocks(), bits.num_blocks(), cached, nocheck_tag{}) { }

    _YAEF_ATTR_NODISCARD size_type current() const noexcept {
        return cached_;
    }

    _YAEF_ATTR_NODISCARD bool is_valid() const noexcept {
        return cached_ != num_blocks() * BLOCK_WIDTH;
    }

    void next() {
        _YAEF_ASSERT(cached_ < num_blocks() * BLOCK_WIDTH);

        const size_type block_idx = cached_ / BLOCK_WIDTH,
                        bit_offset = cached_ % BLOCK_WIDTH;
        const block_type mask = ~bits64::make_mask_lsb1(bit_offset + 1);

        const block_type block = block_handler{}(blocks_beg_[block_idx]) & mask;

        if (_YAEF_UNLIKELY(block != 0)) {
            cached_ = block_idx * BLOCK_WIDTH + count_trailing_zero(block);
            return;
        }

        for (const block_type *ptr = blocks_beg_ + block_idx + 1; ptr < blocks_end_; ++ptr) {
            const block_type block = block_handler{}(*ptr);
            if (_YAEF_LIKELY(block != 0)) {
                const auto block_idx = static_cast<size_t>(ptr - blocks_beg_);
                cached_ = block_idx * BLOCK_WIDTH + count_trailing_zero(block);
                return;
            }
        }
        cached_ = num_blocks() * BLOCK_WIDTH;
    }

    void prev() {
        auto msb = [](block_type b) -> uint32_t {
            _YAEF_ASSERT(b != 0);
            return BLOCK_WIDTH - count_leading_zero(b) - 1;
        };

        if (_YAEF_UNLIKELY(cached_ == num_blocks() * BLOCK_WIDTH)) {
            for (const block_type *ptr = blocks_end_ - 1; ptr >= blocks_beg_; --ptr) {
                const block_type block = block_handler{}(*ptr);
                if (_YAEF_LIKELY(block != 0)) {
                    const auto block_idx = static_cast<size_t>(ptr - blocks_beg_);
                    cached_ = block_idx * BLOCK_WIDTH + msb(block);
                    return;
                }
            }
        }

        const uint64_t block_idx = cached_ / BLOCK_WIDTH,
                       bit_offset = cached_ % BLOCK_WIDTH;
        const block_type mask = make_mask_lsb1(bit_offset);
        const block_type block = block_handler{}(blocks_beg_[block_idx]) & mask;

        if (_YAEF_LIKELY(block != 0)) {
            cached_ = block_idx * BLOCK_WIDTH + msb(block);
            return;
        }

        for (const block_type *ptr = blocks_beg_ + block_idx - 1; ptr >= blocks_beg_; --ptr) {
            const block_type block = block_handler{}(*ptr);
            if (_YAEF_LIKELY(block != 0)) {
                const size_t block_idx = ptr - blocks_beg_;
                cached_ = block_idx * BLOCK_WIDTH + msb(block);
                return;
            }
        }
        cached_ = num_blocks() * BLOCK_WIDTH;
    }

private:
    const block_type *blocks_beg_;
    const block_type *blocks_end_;
    size_t            cached_;

    _YAEF_ATTR_NODISCARD size_t num_blocks() const {
        return blocks_end_ - blocks_beg_;
    }
};

using bitmap_foreach_onebit_cursor  = bitmap_foreach_cursor<true>;
using bitmap_foreach_zerobit_cursor = bitmap_foreach_cursor<false>;

} // namespace bits64

template<typename T, size_t Alignment = alignof(T)>
class aligned_allocator {
    static_assert((Alignment & (Alignment - 1)) == 0, "the alignment must be the power of two");
public:
    using value_type      = T;
    using size_type       = size_t;
    using difference_type = ptrdiff_t;
    using propagate_on_container_move_assignment = std::true_type;
#if __cplusplus < 201703L
    using pointer         = T *;
    using const_pointer   = const T *;
    using reference       = T &;
    using const_reference = const T &;

    template<typename U, size_t A = Alignment>
    struct rebind { using other = aligned_allocator<U, A>; };
#endif

#if __cplusplus < 202302L
    using is_always_equal = std::true_type;
#endif
public:
    aligned_allocator() = default;
    aligned_allocator(const aligned_allocator &) = default;

    template<typename U, size_t A>
    aligned_allocator(aligned_allocator<U, A>) { }

    _YAEF_ATTR_NODISCARD T *allocate(size_type n) _YAEF_MAYBE_NOEXCEPT {
        if (_YAEF_UNLIKELY(n == 0)) {
            return nullptr;
        }
#if __cplusplus >= 201703L
#   ifdef YAEF_OPTS_NO_EXCEPTION
        return static_cast<T *>(::operator new[](sizeof(T) * n, std::align_val_t{Alignment}, std::nothrow));
#   else
        return static_cast<T *>(::operator new[](sizeof(T) * n, std::align_val_t{Alignment}));
#   endif
#elif (defined(_WIN32) || defined(_WIN64))
        T *ptr = static_cast<T *>(::_aligned_malloc(sizeof(T) * n, Alignment));
#   ifndef YAEF_OPTS_NO_EXCEPTION
        if (_YAEF_UNLIKELY(ptr == nullptr)) {
            throw std::bad_alloc{};
        }
#   endif
        return ptr;
#elif _POSIX_C_SOURCE >= 200112L
        void *ptr = nullptr;
        int ret = ::posix_memalign(&ptr, Alignment, sizeof(T) * n);
#   ifndef YAEF_OPTS_NO_EXCEPTION
        if (_YAEF_UNLIKELY(ret != 0)) {
            throw std::bad_alloc{};
        }
#   endif
        return static_cast<T *>(ptr);
#else
#   error "cannot find a suitable aligned allocate implementation"
#endif
    }

    void deallocate(T *p, size_type n) {
        _YAEF_UNUSED(n);
        if (_YAEF_UNLIKELY(p == nullptr || n == 0)) {
            return;
        }
#if __cplusplus >= 201703L
        ::operator delete[](static_cast<void *>(p), std::align_val_t{Alignment});
#elif (defined(_WIN32) || defined(_WIN64))
        ::_aligned_free(static_cast<void *>(p));
#elif _POSIX_C_SOURCE >= 200112L
        ::free(p);
#else
#   error "cannot find a suitable aligned allocate implementation"
#endif
    }
};

template<typename T1, size_t A1, typename T2, size_t A2>
_YAEF_ATTR_NODISCARD inline bool operator==(const aligned_allocator<T1, A1> &lhs, 
                                            const aligned_allocator<T2, A2> &rhs) noexcept {
    _YAEF_UNUSED(lhs);
    _YAEF_UNUSED(rhs);
    return true;
}

#if __cplusplus < 202002L
template<typename T1, size_t A1, typename T2, size_t A2>
_YAEF_ATTR_NODISCARD inline bool operator!=(const aligned_allocator<T1, A1> &lhs, 
                                            const aligned_allocator<T2, A2> &rhs) noexcept {
    _YAEF_UNUSED(lhs);
    _YAEF_UNUSED(rhs);
    return false;
}
#endif

template<typename AllocT>
_YAEF_ATTR_NODISCARD inline bits64::packed_int_view
allocate_uninit_packed_ints(AllocT &alloc, uint32_t width, size_t num_elems) {
    _YAEF_STATIC_ASSERT_NOMSG(std::is_same<typename std::allocator_traits<AllocT>::value_type, uint8_t>::value);
    using bits_block_type = bits64::packed_int_view::block_type;
    constexpr uint32_t BITS_BLOCK_WIDTH = bits64::packed_int_view::BLOCK_WIDTH;
    const size_t size_in_bytes = bits64::idiv_ceil(width * num_elems, BITS_BLOCK_WIDTH) *
                                                   sizeof(bits_block_type);
    uint8_t *mem = std::allocator_traits<AllocT>::allocate(alloc, size_in_bytes);
    auto *blocks = reinterpret_cast<bits_block_type *>(mem);
    return bits64::packed_int_view{width, blocks, num_elems};
}

template<typename AllocT>
_YAEF_ATTR_NODISCARD inline bits64::packed_int_view 
allocate_packed_ints(AllocT &alloc, uint32_t width, size_t num_elems) {
    auto result = allocate_uninit_packed_ints(alloc, width, num_elems);
    std::uninitialized_fill_n(result.blocks(), result.num_blocks(), 0);
    return result;
}

template<typename AllocT>
inline void deallocate_packed_ints(AllocT &alloc, bits64::packed_int_view &ints) {
    _YAEF_STATIC_ASSERT_NOMSG(std::is_same<typename std::allocator_traits<AllocT>::value_type, uint8_t>::value);
    using bits_block_type = bits64::packed_int_view::block_type;
    constexpr uint32_t BITS_BLOCK_WIDTH = bits64::packed_int_view::BLOCK_WIDTH;
    const size_t size_in_bytes = bits64::idiv_ceil(ints.width() * ints.size(), BITS_BLOCK_WIDTH) *
                                                   sizeof(bits_block_type);
    if (_YAEF_UNLIKELY(ints.blocks() == nullptr || size_in_bytes == 0)) {
        return;
    }
    uint8_t *mem = reinterpret_cast<uint8_t *>(ints.blocks());
    std::allocator_traits<AllocT>::deallocate(alloc, mem, size_in_bytes);
}

template<typename AllocT>
_YAEF_ATTR_NODISCARD inline bits64::packed_int_view 
duplicate_packed_ints(AllocT &alloc, const bits64::packed_int_view &ints) {
    auto result = allocate_uninit_packed_ints(alloc, ints.width(), ints.size());
    std::uninitialized_copy_n(ints.blocks(), ints.num_blocks(), result.blocks());
    return result;
}

template<typename AllocT>
_YAEF_ATTR_NODISCARD inline bits64::bit_view 
allocate_uninit_bits(AllocT &alloc, size_t num_elems) {
    _YAEF_STATIC_ASSERT_NOMSG(std::is_same<typename std::allocator_traits<AllocT>::value_type, uint8_t>::value);
    using bits_block_type = bits64::bit_view::block_type;
    constexpr uint32_t BITS_BLOCK_WIDTH = bits64::bit_view::BLOCK_WIDTH;
    const size_t size_in_bytes = bits64::idiv_ceil(num_elems, BITS_BLOCK_WIDTH) *
                                                   sizeof(bits_block_type);
    uint8_t *mem = std::allocator_traits<AllocT>::allocate(alloc, size_in_bytes);
    auto *blocks = reinterpret_cast<bits_block_type *>(mem);
    return bits64::bit_view{blocks, num_elems};
}

template<typename AllocT>
_YAEF_ATTR_NODISCARD inline bits64::bit_view 
allocate_bits(AllocT &alloc, size_t num_elems, bool init_value = false) {
    auto result = allocate_uninit_bits(alloc, num_elems);
    if (init_value == true) {
        result.set_all_bits();
    } else {
        result.clear_all_bits();
    }
    return result;
}

template<typename AllocT>
inline void deallocate_bits(AllocT &alloc, bits64::bit_view bits) {
    _YAEF_STATIC_ASSERT_NOMSG(std::is_same<typename std::allocator_traits<AllocT>::value_type, uint8_t>::value);
    using bits_block_type = bits64::bit_view::block_type;
    constexpr uint32_t BITS_BLOCK_WIDTH = bits64::bit_view::BLOCK_WIDTH;
    const size_t size_in_bytes = bits64::idiv_ceil(bits.size(), BITS_BLOCK_WIDTH) *
                                                   sizeof(bits_block_type);
    if (_YAEF_UNLIKELY(bits.blocks() == nullptr || size_in_bytes == 0)) {
        return;
    }
    uint8_t *mem = reinterpret_cast<uint8_t *>(bits.blocks());
    std::allocator_traits<AllocT>::deallocate(alloc, mem, size_in_bytes);
}

template<typename AllocT>
_YAEF_ATTR_NODISCARD inline bits64::bit_view duplicate_bits(AllocT &alloc, const bits64::bit_view &ints) {
    auto result = allocate_bits(alloc, ints.size());
    std::uninitialized_copy_n(ints.blocks(), ints.num_blocks(), result.blocks());
    return result;
}

namespace bits64 {

inline error_code packed_int_view::serialize(serializer &ser) const {
    if (!ser.write(width_)) { return error_code::serialize_io; }
    if (!ser.write(num_elems_)) { return error_code::serialize_io; }
    if (!ser.write_bytes(reinterpret_cast<uint8_t *>(blocks_), num_blocks() * sizeof(blocks_))) {
        return error_code::serialize_io;
    }
    return error_code::success;
}

template<typename AllocT>
inline error_code packed_int_view::deserialize(AllocT &alloc, deserializer &deser) {
    uint32_t width;
    if (!deser.read(width)) { return error_code::deserialize_io; }
    
    size_t num_elems;
    if (!deser.read(num_elems)) { return error_code::deserialize_io; }

    auto tmp = yaef::details::allocate_uninit_packed_ints(alloc, width, num_elems);
    if (!deser.read_bytes(reinterpret_cast<uint8_t *>(tmp.blocks_), sizeof(block_type) * tmp.num_blocks())) {
        yaef::details::deallocate_packed_ints(alloc, tmp);
        return error_code::deserialize_io;
    }
    *this = std::move(tmp);
    return error_code::success;
}

inline error_code bit_view::serialize(serializer &ser) const {
    if (!ser.write(num_bits_)) { return error_code::serialize_io; }
    if (!ser.write_bytes(reinterpret_cast<uint8_t *>(blocks_), num_blocks() * sizeof(blocks_))) {
        return error_code::serialize_io;
    }
    return error_code::success;
}

template<typename AllocT>
inline error_code bit_view::deserialize(AllocT &alloc, deserializer &deser) {
    size_t num_bits;
    if (!deser.read(num_bits)) { return error_code::deserialize_io; }

    auto tmp = yaef::details::allocate_uninit_bits(alloc, num_bits);
    if (!deser.read_bytes(reinterpret_cast<uint8_t *>(tmp.blocks_), sizeof(block_type) * tmp.num_blocks())) {
        yaef::details::deallocate_bits(alloc, tmp);
        return error_code::deserialize_io;
    }
    *this = std::move(tmp);
    return error_code::success;
}

}

template<typename ForwardIterT, typename SentIterT>
_YAEF_ATTR_NODISCARD static bool check_duplicate(ForwardIterT first, SentIterT last) {
    auto prv_iter = first;
    auto iter = std::next(first);
    for (; iter != last; ++iter) {
        if (*iter == *prv_iter) {
            return true;
        }
        prv_iter = iter;
    }
    return false;
}

template<typename T, typename InputIterT, typename SentIterT>
class eliasfano_encoder_scalar_impl {
public:
    using value_type = T;
    using size_type  = size_t;

public:
    eliasfano_encoder_scalar_impl(InputIterT first, SentIterT last, size_type num,
                                  value_type min, value_type max, uint32_t low_width) noexcept
        : first_(first), last_(last), size_(num), 
          min_(min), max_(max), low_width_(low_width) {
        _YAEF_ASSERT(low_width_ > 0 && low_width_ <= 64);
    }
    
    eliasfano_encoder_scalar_impl(InputIterT first, InputIterT last, size_type num, 
                                  value_type min, value_type max) noexcept
        : first_(first), last_(last), size_(num), 
          min_(min), max_(max), low_width_(0) {
        const uint64_t u = to_stored_value(max_);
        low_width_ = std::max<uint32_t>(1, bits64::bit_width(u / size_));
    }

#if _YAEF_USE_CXX_CONCEPTS
    template<std::bidirectional_iterator IterT = InputIterT>
#else
    template<typename IterT = InputIterT, 
             typename = typename std::enable_if<is_bidirectional_iter<IterT>::value>::type>
#endif
    eliasfano_encoder_scalar_impl(IterT first, IterT last, size_type num)
        : first_(first), last_(last), size_(num),
          min_(std::numeric_limits<value_type>::max()), 
          max_(std::numeric_limits<value_type>::min()),
          low_width_(0) {
        if (size_ != 0) {
            min_ = *first;
            max_ = *std::prev(last);
            const uint64_t u = to_stored_value(max_);
            low_width_ = std::max<uint32_t>(1, bits64::bit_width(u / size_));
        }
    }

    _YAEF_ATTR_NODISCARD size_type size() const noexcept { return size_; }
    _YAEF_ATTR_NODISCARD value_type min() const noexcept { return min_; }
    _YAEF_ATTR_NODISCARD value_type max() const noexcept { return max_; }
    _YAEF_ATTR_NODISCARD uint32_t low_width() const noexcept { return low_width_; }

    _YAEF_ATTR_NODISCARD size_type estimate_low_size_in_bits() const noexcept {
        return low_width_ * size_;
    }

    _YAEF_ATTR_NODISCARD size_type estimate_high_size_in_bits() const noexcept {
        const uint64_t u = static_cast<uint64_t>(max_) - static_cast<uint64_t>(min_);
        const size_type num_ones = size_;
        const size_type num_buckets = u >> low_width_;
        const size_type num_zeros = num_buckets + 1;
        return num_zeros + num_ones;
    }

    _YAEF_ATTR_NODISCARD size_type estimate_low_size_in_bytes() const noexcept {
        return bits64::idiv_ceil(estimate_low_size_in_bits(), sizeof(uint64_t) * CHAR_BIT) * sizeof(uint64_t);
    }

    _YAEF_ATTR_NODISCARD size_type estimate_high_size_in_bytes() const noexcept {
        return bits64::idiv_ceil(estimate_high_size_in_bits(), sizeof(uint64_t) * CHAR_BIT) * sizeof(uint64_t);
    }

    void unchecked_enocde_low_bits(uint64_t *buf_out) const {
        bits64::packed_int_view view{low_width_, buf_out, size_};
        view.clear_all_bits();

        auto iter = first_;
        for (size_type i = 0; i < size_; ++i, ++iter) {
            view.set_value(i, to_stored_value(*iter));
        }
    }

    void unchecked_encode_high_bits(uint64_t *buf_out) const {
        const uint64_t u = static_cast<uint64_t>(max_) - static_cast<uint64_t>(min_);
        const size_type num_ones = size_;
        const size_type num_buckets = u >> low_width_;
        const size_type num_zeros = num_buckets + 1;

        bits64::bit_view view{buf_out, num_zeros + num_ones};
        view.set_all_bits();
        view.clear_bit(0);

        auto iter = first_;
        for (size_type i = 0, zero_index = 1; i < num_buckets; ++i, ++zero_index) {
            while ((to_stored_value(*iter) >> low_width_) == i) {
                ++iter;
                ++zero_index;
            }
            view.clear_bit(zero_index);
        }
    }

private:
    InputIterT first_;
    SentIterT  last_;
    size_type  size_;
    value_type min_;
    value_type max_;
    uint32_t   low_width_;

    _YAEF_ATTR_NODISCARD uint64_t to_stored_value(value_type val) const noexcept {
        return static_cast<uint64_t>(val) - static_cast<uint64_t>(min_);
    }
};

template<typename T>
class eliasfano_decoder_scalar_impl {
public:
};

// a compressed pair to store a value with an allocator (in most situation, allocator is an empty object)
template<typename T, typename AllocT>
class value_with_allocator_pair : private AllocT {
public:
    value_with_allocator_pair() = default;

    value_with_allocator_pair(const value_with_allocator_pair &other)
        : AllocT(static_cast<const AllocT>(other)), value_(other.value()) { }

    value_with_allocator_pair(value_with_allocator_pair &&other) noexcept
        : AllocT(static_cast<AllocT &&>(std::move(other))), value_(std::move(other.value())) { }

    template<typename U = T, typename AllocU = AllocT>
    value_with_allocator_pair(U &&v, AllocU &&a)
        : AllocT(std::forward<AllocU>(a)), value_(std::forward<U>(v)) { }

    value_with_allocator_pair &operator=(const value_with_allocator_pair &other) {
        alloc() = other.alloc();
        value_ = std::move(other.value_);
        return *this;
    }

    value_with_allocator_pair &operator=(value_with_allocator_pair &&other) {
        alloc() = std::move(other.alloc());
        value_ = std::move(other.value_);
        return *this;
    }

    _YAEF_ATTR_NODISCARD const T &value() const noexcept { return value_; }
    _YAEF_ATTR_NODISCARD T &value() noexcept { return value_; }
    _YAEF_ATTR_NODISCARD const AllocT &alloc() const noexcept { return static_cast<const AllocT &>(*this); }
    _YAEF_ATTR_NODISCARD AllocT &alloc() noexcept { return static_cast<AllocT &>(*this); }  

private:
    T value_;
};

class selectable_dense_bits {
public:
    using size_type = size_t;

public:
    selectable_dense_bits() = default;

    template<typename AllocT>
    selectable_dense_bits(AllocT &alloc, bits64::bit_view bits, bits64::bits_stat_info stat_info)   
        : bits_(bits) {
        _YAEF_STATIC_ASSERT_NOMSG(std::is_same<typename std::allocator_traits<AllocT>::value_type, uint8_t>::value);
        
        // `sampler` is used to handle primary samples and allocate memory for subsamples.
        struct sampler {
            sampler(AllocT &alloc, size_type num_bits, size_type num_zeros_or_ones)
                : alloc_(alloc), num_zeros_or_ones_(num_zeros_or_ones), num_scanned_(0), 
                  last_sample_(0), max_uniform_subsample_(0), max_each_one_subsample_(0) {
                if (num_zeros_or_ones != 0) {
                    size_t get_num_samples = bits64::idiv_ceil_nzero(num_zeros_or_ones, position_samples::SAMPLE_RATE) + 1;
                    const uint32_t width = bits64::bit_width(num_bits);
                    samples_store_ = allocate_packed_ints(alloc, width, get_num_samples);
                }
            }

            void try_sample(size_type pos) {
                _YAEF_ASSERT(num_zeros_or_ones_ != 0);

                constexpr size_type SAMPLE_RATE = position_samples::SAMPLE_RATE;
                const size_type block_index = num_scanned_ / SAMPLE_RATE, block_offset = num_scanned_ % SAMPLE_RATE;
                if (block_offset == 0) {
                    samples_store_.set_value(block_index, pos);
                } else {
                    size_type ref_delta = pos - samples_store_.get_value(block_index);
                    if (num_scanned_ % position_samples::UNIFORM_SUBSAMPLE_RATE == 0) {
                        max_uniform_subsample_ = std::max(max_uniform_subsample_, ref_delta);
                    }
                    max_each_one_subsample_ = std::max(max_each_one_subsample_, ref_delta);
                }
                ++num_scanned_;
                last_sample_ = pos;
            }

            _YAEF_ATTR_NODISCARD position_samples finish() {
                if (num_zeros_or_ones_ == 0) {
                    return position_samples{};
                }

                samples_store_.set_value(samples_store_.size() - 1, last_sample_);

                size_type num_uniform_sample_blocks = 0;
                size_type num_each_one_sample_blocks = 0;
                for (size_type i = 1; i < samples_store_.size(); ++i) {
                    const size_type prv_sample = samples_store_.get_value(i - 1),
                                    cur_sample = samples_store_.get_value(i);
                    if (cur_sample - prv_sample >= position_samples::EACH_ONE_SUBSAMPLE_MIN_LEN) {
                        ++num_each_one_sample_blocks;
                    } else {
                        ++num_uniform_sample_blocks;
                    }
                }

                // allocate and initialize `subsample_lut`.
                auto subsample_info = [&]() {
                    const uint32_t width = 1 + std::max(
                        bits64::bit_width(std::max<size_type>(2, num_uniform_sample_blocks) - 1), 
                        bits64::bit_width(std::max<size_type>(2, num_each_one_sample_blocks) - 1)
                    );
                    auto lut = details::allocate_packed_ints(alloc_, width, samples_store_.size() - 1);
                    size_type uniform_subsample_start = 0;
                    size_type each_one_subsample_start = 0;
                    for (size_type i = 1; i < samples_store_.size(); ++i) {
                        const size_type prv_sample = samples_store_.get_value(i - 1),
                                        cur_sample = samples_store_.get_value(i);
                        uint64_t entry = 0;
                        if (cur_sample - prv_sample >= position_samples::EACH_ONE_SUBSAMPLE_MIN_LEN) {
                            entry = each_one_subsample_start | (1 << (width - 1));
                            ++each_one_subsample_start;
                        } else {
                            entry = uniform_subsample_start;
                            ++uniform_subsample_start;
                        }
                        lut.set_value(i - 1, entry);
                    }

                    return lut;
                }();

                // allocate uniform_subsamples.
                auto uniform_subsamples = details::allocate_packed_ints(
                    alloc_,
                    bits64::bit_width(max_uniform_subsample_), 
                    num_uniform_sample_blocks * (position_samples::UNIFORM_SUBSAMPLE_BLOCK_NUM_ELEMS - 1)
                );

                // allocate each_one_subsamples.
                auto each_one_subsamples = details::allocate_packed_ints(
                    alloc_,
                    bits64::bit_width(max_each_one_subsample_), 
                    num_each_one_sample_blocks * (position_samples::EACH_ONE_SUBSAMPLE_BLOCK_NUM_ELEMS - 1)
                );

                return position_samples{samples_store_, uniform_subsamples, 
                                        each_one_subsamples, subsample_info};
            }
        
        private:
            AllocT                  &alloc_;
            bits64::packed_int_view  samples_store_;
            size_type                num_zeros_or_ones_;
            size_type                num_scanned_;
            size_type                last_sample_;
            size_type                max_uniform_subsample_;
            size_type                max_each_one_subsample_;
        };

        const size_type num_ones = stat_info.num_ones();
        const size_type num_zeros = stat_info.num_zeros();

        sampler one_sampler{alloc, bits.size(), num_ones};
        sampler zero_sampler{alloc, bits.size(), num_zeros};

        // sample bit-1 and bit-0 respectively.
        for (size_type i = 0; i < bits.size(); ++i) {
            if (bits.get_bit(i) == true) {
                one_sampler.try_sample(i);
            } else { 
                zero_sampler.try_sample(i);
            }
        }
        
        one_samples_ = one_sampler.finish();
        zero_samples_ = zero_sampler.finish();

        // `subsampler` is used to complete the remaining subsampling.
        struct subsampler {
            subsampler(position_samples samples) noexcept
                : num_scanned_(0), uniform_writer_index_(0), 
                  each_one_writer_index_(0), samples_(samples) { }

            void try_sample(size_type pos) {
                constexpr size_type SAMPLE_RATE = position_samples::SAMPLE_RATE;
                constexpr size_type UNIFORM_SAMPLE_RATE = position_samples::UNIFORM_SUBSAMPLE_RATE;
                
                const size_type block_index = num_scanned_ / SAMPLE_RATE,
                                block_offset = num_scanned_ % SAMPLE_RATE;
                auto type = samples_.get_subsample_block_info(block_index).first;

                if (block_offset != 0) {
                    auto &subsamples = samples_.get_subsamples(type);
                    size_type sample = samples_.get_samples().get_value(block_index);
                    size_type ref_delta = pos - sample;
                    if (type == position_samples::subsampler_type::uniform) {
                        if (num_scanned_ % UNIFORM_SAMPLE_RATE == 0) {
                            subsamples.set_value(uniform_writer_index_++, ref_delta);
                        }
                    } else {
                        subsamples.set_value(each_one_writer_index_++, ref_delta);
                    }
                }
                ++num_scanned_;
            }

            _YAEF_ATTR_NODISCARD position_samples finish() {
                return samples_;
            }

        private:
            size_type        num_scanned_;
            size_type        uniform_writer_index_;
            size_type        each_one_writer_index_;
            position_samples samples_;
        };
        
        subsampler one_subsampler{one_samples_};
        subsampler zero_subsampler{zero_samples_};
        bool need_init_one_subsamples = !one_samples_.get_subsample_block_infos().empty();
        bool need_init_zero_subsamples = !zero_samples_.get_subsample_block_infos().empty();

        for (size_type i = 0; i < bits.size(); ++i) {
            if (need_init_one_subsamples && bits.get_bit(i) == true) {
                one_subsampler.try_sample(i);
            } else if (need_init_zero_subsamples) {
                zero_subsampler.try_sample(i);
            }
        }
        one_samples_ = one_subsampler.finish();
        zero_samples_ = zero_subsampler.finish();
    }

    template<typename AllocT>
    selectable_dense_bits(AllocT &alloc, bits64::bit_view bits)
        : selectable_dense_bits(alloc, bits, bits64::stats_bits(bits)) { }

    template<typename AllocT>
    void deallocate(AllocT &alloc) {
        deallocate_bits(alloc, bits_);
        zero_samples_.deallocate(alloc);
        one_samples_.deallocate(alloc);
    }

    template<typename AllocT>
    _YAEF_ATTR_NODISCARD selectable_dense_bits duplicate(AllocT &alloc) const {
        auto new_bits = duplicate_bits(alloc, bits_);
        auto new_zero_samples = zero_samples_.duplicate(alloc);
        auto new_one_samples = one_samples_.duplicate(alloc);
        return selectable_dense_bits{new_bits, new_zero_samples, new_one_samples};
    }

    _YAEF_ATTR_NODISCARD size_type size() const noexcept { return bits_.size(); }

    _YAEF_ATTR_NODISCARD const bits64::bit_view &get_bits() const noexcept { return bits_; }

    _YAEF_ATTR_NODISCARD size_type space_usage_in_bytes() const noexcept {
        return bits_.space_usage_in_bytes() +
               zero_samples_.space_usage_in_bytes() +
               one_samples_.space_usage_in_bytes();
    }

    _YAEF_ATTR_NODISCARD size_type select_one(size_type rank) const noexcept {
        return select_impl<true>(rank);
    }

    _YAEF_ATTR_NODISCARD size_type select_zero(size_type rank) const noexcept {
        return select_impl<false>(rank);
    }

    void swap(selectable_dense_bits &other) noexcept {
        bits_.swap(other.bits_);
        zero_samples_.swap(other.zero_samples_);
        one_samples_.swap(other.one_samples_);
    }

    friend bool operator==(const selectable_dense_bits &lhs, const selectable_dense_bits &rhs) noexcept;
#if __cplusplus < 202002L
    friend bool operator!=(const selectable_dense_bits &lhs, const selectable_dense_bits &rhs) noexcept;
#endif

    error_code serialize(serializer &ser) const {
        _YAEF_RETURN_ERR_IF_FAIL(bits_.serialize(ser));
        _YAEF_RETURN_ERR_IF_FAIL(zero_samples_.serialize(ser));
        _YAEF_RETURN_ERR_IF_FAIL(one_samples_.serialize(ser));
        return error_code::success;
    }

    template<typename AllocT>
    error_code deserialize(AllocT &alloc, deserializer &deser) {
        _YAEF_RETURN_ERR_IF_FAIL(bits_.deserialize(alloc, deser));
        _YAEF_RETURN_ERR_IF_FAIL(zero_samples_.deserialize(alloc, deser));
        _YAEF_RETURN_ERR_IF_FAIL(one_samples_.deserialize(alloc, deser));
        return error_code::success;
    }

private:
    struct position_samples {
        enum class subsampler_type {
            uniform = 0, each_one = 1
        };

        static constexpr size_type SAMPLE_RATE                        = static_cast<size_type>(1) << 12;
        static constexpr size_type UNIFORM_SUBSAMPLE_RATE             = 64;
        static constexpr size_type UNIFORM_SUBSAMPLE_BLOCK_NUM_ELEMS  = SAMPLE_RATE / UNIFORM_SUBSAMPLE_RATE;
        static constexpr size_type EACH_ONE_SUBSAMPLE_MIN_LEN         = static_cast<size_type>(1) << 16;
        static constexpr size_type EACH_ONE_SUBSAMPLE_BLOCK_NUM_ELEMS = SAMPLE_RATE; 

        struct sample_find_result {
            size_type rank_distance;
            size_type position;
        };

        position_samples() = default;

        position_samples(bits64::packed_int_view samples, bits64::packed_int_view uniform_subsamples, 
                         bits64::packed_int_view each_one_subsamples, bits64::packed_int_view subsample_info) noexcept
            : samples_(samples), subsamples_{uniform_subsamples, each_one_subsamples}, 
              subsample_info_(subsample_info) { }

        template<typename AllocT>
        void deallocate(AllocT &alloc) {
            deallocate_packed_ints(alloc, samples_);
            deallocate_packed_ints(alloc, subsamples_[0]);
            deallocate_packed_ints(alloc, subsamples_[1]);
            deallocate_packed_ints(alloc, subsample_info_);
        }

        template<typename AllocT>
        _YAEF_ATTR_NODISCARD position_samples duplicate(AllocT &alloc) const {
            auto new_samples = duplicate_packed_ints(alloc, samples_);
            auto new_uniform_subsamples = duplicate_packed_ints(alloc, get_subsamples(subsampler_type::uniform));
            auto new_each_one_subsamples = duplicate_packed_ints(alloc, get_subsamples(subsampler_type::each_one));
            auto new_subsample_lut = duplicate_packed_ints(alloc, subsample_info_);
            return position_samples{new_samples, new_uniform_subsamples, new_each_one_subsamples, new_subsample_lut};
        }

        _YAEF_ATTR_NODISCARD const bits64::packed_int_view &get_samples() const noexcept { return samples_; }
        _YAEF_ATTR_NODISCARD bits64::packed_int_view &get_samples() noexcept { return samples_; }

        _YAEF_ATTR_NODISCARD const bits64::packed_int_view &get_subsamples(subsampler_type type) const noexcept {
            return subsamples_[static_cast<size_type>(type)];
        }

        _YAEF_ATTR_NODISCARD bits64::packed_int_view &get_subsamples(subsampler_type type) noexcept {
            return subsamples_[static_cast<size_type>(type)];
        }
        
        _YAEF_ATTR_NODISCARD size_type space_usage_in_bytes() const noexcept {
            return samples_.space_usage_in_bytes() +
                   subsamples_[0].space_usage_in_bytes() +
                   subsamples_[1].space_usage_in_bytes() +
                   subsample_info_.space_usage_in_bytes();
        }

        _YAEF_ATTR_NODISCARD std::pair<subsampler_type, size_type> 
        get_subsample_block_info(size_type block_index) const noexcept {            
            const uint32_t lut_entry_width = subsample_info_.width();
            const uint64_t lut_entry_mask = 1 << (lut_entry_width - 1);

            uint64_t lut_entry = subsample_info_.get_value(block_index);
            subsampler_type type = static_cast<subsampler_type>(static_cast<bool>(lut_entry & lut_entry_mask));
            uint64_t subsample_start = lut_entry & (~lut_entry_mask);
            return std::make_pair(type, subsample_start);
        }

        _YAEF_ATTR_NODISCARD const bits64::packed_int_view &get_subsample_block_infos() const noexcept {
            return subsample_info_;
        }

        _YAEF_ATTR_NODISCARD bits64::packed_int_view &get_subsample_block_infos() noexcept {
            return subsample_info_;
        }

        _YAEF_ATTR_NODISCARD sample_find_result 
        lookup_subsample(size_type block_index, size_type block_offset) const noexcept {
            auto subsample_block_info = get_subsample_block_info(block_index);
            subsampler_type type = subsample_block_info.first;
            size_type subsample_index = subsample_block_info.second;
            const bits64::packed_int_view &subsamples = get_subsamples(type);
            
            size_type subsample_rank_distance = 0;
            if (type == subsampler_type::uniform) {
                const size_type mini_block_index = block_offset / UNIFORM_SUBSAMPLE_RATE,
                                mini_block_offset = block_offset % UNIFORM_SUBSAMPLE_RATE;
                if (_YAEF_UNLIKELY(mini_block_index == 0)) {
                    return sample_find_result{mini_block_offset, 0};
                }
                subsample_index = subsample_index * (UNIFORM_SUBSAMPLE_BLOCK_NUM_ELEMS - 1) + mini_block_index - 1;
                subsample_rank_distance = mini_block_offset;
            } else /*if (type == subsampler_type::each_one)*/ {
                subsample_index = subsample_index * (EACH_ONE_SUBSAMPLE_BLOCK_NUM_ELEMS - 1) + block_offset - 1;
            }
            return sample_find_result{subsample_rank_distance, subsamples.get_value(subsample_index)};
        }

        _YAEF_ATTR_NODISCARD sample_find_result find_nearest_sample(size_type rank) const noexcept {
            const size_type block_index = rank / SAMPLE_RATE, 
                            block_offset = rank % SAMPLE_RATE;
            uint64_t sample = samples_.get_value(block_index);
            if (block_offset == 0) {
                return sample_find_result{0, sample};
            }
            auto result = lookup_subsample(block_index, block_offset);
            result.position += sample;
            return result;
        }

        void swap(position_samples &other) noexcept {
            samples_.swap(other.samples_);
            subsamples_[0].swap(other.subsamples_[0]);
            subsamples_[1].swap(other.subsamples_[1]);
            subsample_info_.swap(other.subsample_info_);
        }

        _YAEF_ATTR_NODISCARD friend bool operator==(const position_samples &lhs, const position_samples &rhs) noexcept {
            if (_YAEF_UNLIKELY(std::addressof(lhs) == std::addressof(rhs))) {
                return true;
            }
            return lhs.samples_ == rhs.samples_ &&
                   lhs.subsamples_[0] == rhs.subsamples_[0] &&
                   lhs.subsamples_[1] == rhs.subsamples_[1] &&
                   lhs.subsample_info_ == rhs.subsample_info_;
        }

#if __cplusplus < 202002L
        _YAEF_ATTR_NODISCARD friend bool operator!=(const position_samples &lhs, const position_samples &rhs) noexcept {
            return !(lhs == rhs);
        }
#endif
        
        error_code serialize(serializer &ser) const {
            _YAEF_RETURN_ERR_IF_FAIL(samples_.serialize(ser));
            _YAEF_RETURN_ERR_IF_FAIL(subsamples_[0].serialize(ser));
            _YAEF_RETURN_ERR_IF_FAIL(subsamples_[1].serialize(ser));
            _YAEF_RETURN_ERR_IF_FAIL(subsample_info_.serialize(ser));
            return error_code::success;
        }
        
        template<typename AllocT>
        error_code deserialize(AllocT &alloc, deserializer &deser) {
            _YAEF_RETURN_ERR_IF_FAIL(samples_.deserialize(alloc, deser));
            _YAEF_RETURN_ERR_IF_FAIL(subsamples_[0].deserialize(alloc, deser));
            _YAEF_RETURN_ERR_IF_FAIL(subsamples_[1].deserialize(alloc, deser));
            _YAEF_RETURN_ERR_IF_FAIL(subsample_info_.deserialize(alloc, deser));
            return error_code::success;
        }

    private:
        bits64::packed_int_view samples_;
        bits64::packed_int_view subsamples_[2];
        bits64::packed_int_view subsample_info_;
    };

    bits64::bit_view bits_;
    position_samples zero_samples_;
    position_samples one_samples_;

    selectable_dense_bits(bits64::bit_view bits, const position_samples &zero_samples,
                          const position_samples &one_samples)
        : bits_(bits), zero_samples_(zero_samples), one_samples_(one_samples) { }

    _YAEF_ATTR_NODISCARD const position_samples &get_samples_impl(std::true_type) const noexcept { 
        return one_samples_; 
    }

    _YAEF_ATTR_NODISCARD const position_samples &get_samples_impl(std::false_type) const noexcept { 
        return zero_samples_; 
    }
    
    template<bool BitType>
    _YAEF_ATTR_NODISCARD const position_samples &get_samples() const noexcept {
        return get_samples_impl(std::integral_constant<bool, BitType>{});
    }

    struct memory_access_stats {
        size_type num_popcount = 0;
        size_type num_select = 0;

        memory_access_stats() = default;

        memory_access_stats(size_type p, size_type s) noexcept
            : num_popcount(p), num_select(s) { }
    };

    // !!!(dev-only) this method is used to count the number of memory accesses 
    // and popcounts/select_in_word performed during a `select` operation
    template<bool BitType>
    _YAEF_ATTR_NODISCARD memory_access_stats select_impl_scan_stats(size_type rank) const noexcept {        
        const auto &samples = BitType ? one_samples_ : zero_samples_;
        
        auto find = samples.find_nearest_sample(rank);
        if (find.rank_distance == 0) {
            return memory_access_stats{0, 0};
        }
        
        using bits_block_type = bits64::bit_view::block_type;
        constexpr size_type BITS_BLOCK_WIDTH = bits64::bit_view::BLOCK_WIDTH;
        const size_type bits_block_index = (find.position + 1) / BITS_BLOCK_WIDTH,
                        bits_block_offset = (find.position + 1) % BITS_BLOCK_WIDTH;
        const size_type num_bits_block = bits_.num_blocks();

        auto bitwise_not_if_select_zero = [](bits_block_type b) -> bits_block_type {
            if _YAEF_CXX17_CONSTEXPR (BitType) { 
                return b;
            } else { 
                return ~b;
            }
        };

        bits_block_type bits_block = bitwise_not_if_select_zero(bits_.blocks()[bits_block_index]) >> bits_block_offset;

        memory_access_stats stats;
        {
            uint32_t popcnt = bits64::popcount(bits_block);
            ++stats.num_popcount;
            if (popcnt < find.rank_distance) {
                find.rank_distance -= popcnt;
            } else {
                ++stats.num_select;
                return stats;
            }
        }
        for (size_type i = bits_block_index + 1; i < num_bits_block; ++i) {
            bits_block = bitwise_not_if_select_zero(bits_.blocks()[i]);
            uint32_t popcnt = bits64::popcount(bits_block);
            if (popcnt < find.rank_distance) {
                find.rank_distance -= popcnt;
                ++stats.num_popcount;
            } else {
                ++stats.num_select;
                break;
            }
        }
        return stats;
    }

    template<bool BitType>
    _YAEF_ATTR_NODISCARD size_type select_impl(size_type rank) const noexcept {
        using block_handler = bits64::conditional_bitwise_not<!BitType>;
        using bits_block_type = bits64::bit_view::block_type;
        
        auto sample = get_samples<BitType>().find_nearest_sample(rank);
        if (_YAEF_UNLIKELY(sample.rank_distance == 0)) {
            return sample.position;
        }

        constexpr size_type BITS_BLOCK_WIDTH = bits64::bit_view::BLOCK_WIDTH;
        const size_type bits_block_index = (sample.position + 1) / BITS_BLOCK_WIDTH,
                        bits_block_offset = (sample.position + 1) % BITS_BLOCK_WIDTH;
        
        size_type result = sample.position + 1;

        auto scan_single_block = [&sample](bits_block_type block, uint32_t width) -> std::pair<bool, size_type> {
            uint32_t popcnt = bits64::popcount(block);
            if (popcnt < sample.rank_distance) {
                sample.rank_distance -= popcnt;
                return std::make_pair(false, width);
            } else {
                return std::make_pair(true, bits64::select_one(block, sample.rank_distance - 1));
            }
        };

        {
            bits_block_type bits_block = block_handler{}(bits_.blocks()[bits_block_index]) >> bits_block_offset;
            auto scan_res = scan_single_block(bits_block, BITS_BLOCK_WIDTH - bits_block_offset);
            bool stop = scan_res.first;
            uint32_t step = scan_res.second;

            result += step;
            if (stop) {
                return result;
            }
        }

        const size_type num_bits_block = bits_.num_blocks();

#ifdef __clang__
#   pragma clang loop unroll_count(4)
#elif defined(__GNUC__)
#   pragma GCC unroll 4
#endif
        for (size_type i = bits_block_index + 1; i < num_bits_block; ++i) {
            bits_block_type bits_block = block_handler{}(bits_.blocks()[i]);
            auto scan_res = scan_single_block(bits_block, BITS_BLOCK_WIDTH);
            bool stop = scan_res.first;
            uint32_t step = scan_res.second;

            result += step;
            if (stop) { break; }
        }
        return result;
    }

    _YAEF_ATTR_NODISCARD memory_access_stats select_one_scan_stats(size_type rank) const noexcept {
        return select_impl_scan_stats<true>(rank);
    }

    _YAEF_ATTR_NODISCARD memory_access_stats  select_zero_scan_stats(size_type rank) const noexcept {
        return select_impl_scan_stats<false>(rank);
    }
};

_YAEF_ATTR_NODISCARD inline bool 
operator==(const selectable_dense_bits &lhs, const selectable_dense_bits &rhs) noexcept {
    if (_YAEF_UNLIKELY(std::addressof(lhs) == std::addressof(rhs))) {
        return true;
    }
    return lhs.bits_ == rhs.bits_ &&
           lhs.zero_samples_ == rhs.zero_samples_ &&
           lhs.one_samples_ == rhs.one_samples_;
}

#if __cplusplus < 202002L
_YAEF_ATTR_NODISCARD inline bool 
operator!=(const selectable_dense_bits &lhs, const selectable_dense_bits &rhs) noexcept {
    return !(lhs == rhs);
}
#endif

template<typename T>
class eliasfano_bidirectional_iterator {
public:
    using iterator_category = std::bidirectional_iterator_tag;
    using value_type        = T;
    using difference_type   = ptrdiff_t;
    using pointer           = value_type *;
    using reference         = value_type;

public:
    eliasfano_bidirectional_iterator() noexcept
        : index_(0), min_(0) { }

    eliasfano_bidirectional_iterator(const eliasfano_bidirectional_iterator &other) = default;

    eliasfano_bidirectional_iterator(const bits64::bitmap_foreach_onebit_cursor &high_bits_cursor, 
                               const bits64::packed_int_view &low_bits,
                               value_type min, bits64::packed_int_view::size_type index)
        : high_bits_cursor_(high_bits_cursor), low_bits_(low_bits),
          min_(min), index_(index) { }

    _YAEF_ATTR_NODISCARD value_type operator*() const noexcept {
        _YAEF_ASSERT(low_bits_.blocks() != nullptr);
        uint64_t high = high_bits_cursor_.current() - index_ - 1;
        uint64_t low = low_bits_.get_value(index_);
        uint64_t merged = static_cast<uint64_t>((high << low_bits_.width()) | low);
        return static_cast<value_type>(static_cast<uint64_t>(min_) + merged);
    }

    eliasfano_bidirectional_iterator &operator++() noexcept {
        ++index_;
        high_bits_cursor_.next();
        return *this;
    }

    eliasfano_bidirectional_iterator operator++(int) noexcept {
        eliasfano_bidirectional_iterator old{*this};
        ++index_;
        high_bits_cursor_.next();
        return old;
    }

    eliasfano_bidirectional_iterator &operator--() noexcept {
        --index_;
        high_bits_cursor_.prev();
        return *this;
    }

    eliasfano_bidirectional_iterator operator--(int) noexcept {
        eliasfano_bidirectional_iterator old{*this};
        --index_;
        high_bits_cursor_.prev();
        return old;
    }

    _YAEF_ATTR_NODISCARD bits64::packed_int_view::size_type to_index() const noexcept {
        return index_;
    }

    template<typename U>
    friend bool operator==(const eliasfano_bidirectional_iterator<U> &lhs, 
                           const eliasfano_bidirectional_iterator<U> &rhs) noexcept;

#if __cplusplus < 202002L
    template<typename U>
    friend bool operator!=(const eliasfano_bidirectional_iterator<U> &lhs, 
                           const eliasfano_bidirectional_iterator<U> &rhs) noexcept;
#endif
private:
    bits64::bitmap_foreach_onebit_cursor high_bits_cursor_;
    bits64::packed_int_view              low_bits_;
    value_type                           min_;
    bits64::packed_int_view::size_type   index_;
};

template<typename T>
_YAEF_ATTR_NODISCARD inline bool operator==(const eliasfano_bidirectional_iterator<T> &lhs, 
                                            const eliasfano_bidirectional_iterator<T> &rhs) noexcept {
    return lhs.index_ == rhs.index_ && lhs.low_bits_.blocks() == rhs.low_bits_.blocks();
}

#if __cplusplus < 202002L
template<typename U>
_YAEF_ATTR_NODISCARD inline bool operator!=(const eliasfano_bidirectional_iterator<U> &lhs, 
                                            const eliasfano_bidirectional_iterator<U> &rhs) noexcept {
    return !(lhs == rhs);
}
#endif

} // namespace details

struct from_sorted_t { };

#if __cplusplus < 201703L
static constexpr from_sorted_t from_sorted{};
#else
inline constexpr from_sorted_t from_sorted{};
#endif

#if _YAEF_USE_CXX_CONCEPTS
template<std::integral T, typename AllocT = details::aligned_allocator<uint8_t, 32>>
#else
template<typename T, typename AllocT = details::aligned_allocator<uint8_t, 32>>
#endif
class eliasfano_list {
#if !_YAEF_USE_CXX_CONCEPTS
    _YAEF_STATIC_ASSERT_NOMSG(std::is_integral<T>::value);
#endif
    template<typename>
    friend class details::eliasfano_bidirectional_iterator;

    template<bool, typename>
    friend class eliasfano_sparse_bitmap;

    friend struct details::serialize_friend_access;

    using high_bits_type      = details::selectable_dense_bits;
    using low_bits_type       = details::bits64::packed_int_view;
    using alloc_traits        = std::allocator_traits<AllocT>;
    using unsigned_value_type = uint64_t;
public:
    using value_type          = T;
    using size_type           = size_t;
    using difference_type     = ptrdiff_t;
    using const_reference     = const value_type &;
    using reference           = const_reference;
    using const_pointer       = const value_type *;
    using pointer             = const_pointer;
    using const_iterator      = details::eliasfano_bidirectional_iterator<value_type>;
    using iterator            = const_iterator;
    using allocator_type      = AllocT;

public:
    eliasfano_list() = default;

    eliasfano_list(const allocator_type &alloc)
        : low_bits_with_alloc_(details::bits64::packed_int_view{}, alloc) { }

    eliasfano_list(const eliasfano_list &other)
        : eliasfano_list(alloc_traits::select_on_container_copy_construction(other.get_alloc())) {
        high_bits_ = other.high_bits_.duplicate(get_alloc());
        get_low_bits() = details::duplicate_packed_ints(get_alloc(), other.get_low_bits());
        min_ = other.min_;
        max_ = other.max_;
        has_duplicates_ = other.has_duplicates_;
    }

    eliasfano_list(eliasfano_list &&other) noexcept {
        high_bits_ = details::exchange(other.high_bits_, high_bits_type{});
        get_low_bits() = details::exchange(other.get_low_bits(), low_bits_type{});
        details::checked_swap_alloc(get_alloc(), other.get_alloc());
        min_ = details::exchange(other.min_, std::numeric_limits<value_type>::max());
        max_ = details::exchange(other.max_, std::numeric_limits<value_type>::min());
        has_duplicates_ = details::exchange(other.has_duplicates_, false);
    }

    eliasfano_list(const eliasfano_list &other, const allocator_type &alloc)
        : eliasfano_list(alloc) {
        high_bits_ = other.high_bits_.duplicate(get_alloc());
        get_low_bits() = details::duplicate_packed_ints(get_alloc(), other.get_low_bits());
        min_ = other.min_;
        max_ = other.max_;
        has_duplicates_ = other.has_duplicates_;
    }

    eliasfano_list(eliasfano_list &&other, const allocator_type &alloc)
        : eliasfano_list(alloc) {
        if (get_alloc() == other.get_alloc()) {
            high_bits_ = details::exchange(other.high_bits_, high_bits_type{}); 
            get_low_bits() = details::exchange(other.get_low_bits(), low_bits_type{}); 
        } else {
            high_bits_ = other.high_bits_.duplicate(get_alloc());
            get_low_bits() = details::duplicate_packed_ints(get_alloc(), other.get_low_bits());
        }
        min_ = details::exchange(other.min_, std::numeric_limits<value_type>::max());
        max_ = details::exchange(other.max_, std::numeric_limits<value_type>::min());
        has_duplicates_ = details::exchange(other.has_duplicates_, false);
    }

    _YAEF_REQUIRES_RANDOM_ACCESS_ITER(RandomAccessIterT, SentIterT, std::is_integral)
    eliasfano_list(RandomAccessIterT first, SentIterT last, 
                   const allocator_type &alloc = allocator_type{})
        : eliasfano_list(alloc) {
        auto sorted_info = sorted_seq_info::create(first, last);
        if (!sorted_info.valid) {
            _YAEF_THROW(std::invalid_argument{"eliasfano_list::eliasfano_list: the input data is not sorted"});
        }
        if (sorted_info.num == 0) {
            return;
        }

        auto u = static_cast<unsigned_value_type>(sorted_info.max) - 
                 static_cast<unsigned_value_type>(sorted_info.min);
        const uint32_t low_width = details::bits64::bit_width(u / sorted_info.num);
        unchecked_init_with_low_width(first, last, sorted_info, std::max<uint32_t>(low_width, 1));
    }

    _YAEF_REQUIRES_RANDOM_ACCESS_ITER(RandomAccessIterT, SentIterT, std::is_integral)
    eliasfano_list(from_sorted_t, RandomAccessIterT first, SentIterT last, 
                   const allocator_type &alloc = allocator_type{})
        : eliasfano_list(alloc) {
        auto sorted_info = sorted_seq_info::unchecked_create(first, last);
        auto u = static_cast<unsigned_value_type>(sorted_info.max) - 
                 static_cast<unsigned_value_type>(sorted_info.min);
        const uint32_t low_width = details::bits64::bit_width(u / sorted_info.num);
        unchecked_init_with_low_width(first, last, sorted_info, std::max<uint32_t>(low_width, 1));
    }

    eliasfano_list(std::initializer_list<value_type> initlist)
        : eliasfano_list(initlist.begin(), initlist.end()) { }
    
    eliasfano_list(from_sorted_t, std::initializer_list<value_type> initlist)
        : eliasfano_list(from_sorted, initlist.begin(), initlist.end()) { }

    ~eliasfano_list() {
        high_bits_.deallocate(get_alloc());
        details::deallocate_packed_ints(get_alloc(), get_low_bits());
    }

    eliasfano_list &operator=(const eliasfano_list &other) {
        if (_YAEF_UNLIKELY(this == std::addressof(other))) {
            return *this;
        }
        high_bits_ = other.high_bits_.duplicate(get_alloc());
        get_low_bits() = details::duplicate_packed_ints(get_alloc(), other.get_low_bits());
        min_ = other.min_;
        max_ = other.max_;
        has_duplicates_ = other.has_duplicates_;
        return *this;
    }

    eliasfano_list &operator=(eliasfano_list &&other) noexcept {
        if (_YAEF_UNLIKELY(this == std::addressof(other))) {
            return *this;
        }
        high_bits_ = other.high_bits_; 
        other.high_bits_ = high_bits_type{};
        get_low_bits() = other.get_low_bits(); 
        other.get_low_bits() = low_bits_type{};
        min_ = details::exchange(other.min_, std::numeric_limits<value_type>::max());
        max_ = details::exchange(other.max_, std::numeric_limits<value_type>::min());
        has_duplicates_ = details::exchange(other.has_duplicates_, false);
        return *this;
    }

    _YAEF_ATTR_NODISCARD size_type size() const noexcept { return get_low_bits().size(); }
    _YAEF_ATTR_NODISCARD bool empty() const noexcept { return size() == 0; }
    _YAEF_ATTR_NODISCARD allocator_type get_allocator() const noexcept { return get_alloc(); }
    _YAEF_ATTR_NODISCARD bool has_duplicates() const { return has_duplicates_; }

    _YAEF_ATTR_NODISCARD size_type space_usage_in_bytes() const noexcept {
        return high_bits_.space_usage_in_bytes() +
               get_low_bits().space_usage_in_bytes();
    }

    _YAEF_ATTR_NODISCARD const_iterator begin() const noexcept { 
        return const_iterator{details::bits64::bitmap_foreach_onebit_cursor{high_bits_.get_bits()},
                              get_low_bits(), min(), 0};
    }

    _YAEF_ATTR_NODISCARD const_iterator end() const noexcept {
        using cursor = details::bits64::bitmap_foreach_onebit_cursor;
        const size_t endpos = high_bits_.get_bits().num_blocks() * details::bits64::bit_view::BLOCK_WIDTH;
        return const_iterator{cursor{high_bits_.get_bits(), endpos, cursor::nocheck_tag{}},
                              get_low_bits(), min(), size()};
    }

    _YAEF_ATTR_NODISCARD const_iterator cbegin() const noexcept { return begin(); }
    _YAEF_ATTR_NODISCARD const_iterator cend() const noexcept { return end(); }

    _YAEF_ATTR_NODISCARD const_iterator iter(size_type index) const _YAEF_MAYBE_NOEXCEPT {
        _YAEF_ASSERT(index < size());
        if (_YAEF_UNLIKELY(index >= size())) {
            _YAEF_THROW(std::out_of_range{"eliasfano_list::iter: index is out of range"});
        }
        return make_iter(high_bits_.select_one(index), index);
    }

    _YAEF_ATTR_NODISCARD value_type front() const _YAEF_MAYBE_NOEXCEPT {
        _YAEF_ASSERT(!empty());
        if (_YAEF_UNLIKELY(empty())) {
            _YAEF_THROW(std::out_of_range{"eliasfano_list::front: the list is empty"});
        }
        return min_;
    }

    _YAEF_ATTR_NODISCARD value_type back() const _YAEF_MAYBE_NOEXCEPT {
        _YAEF_ASSERT(!empty());
        if (_YAEF_UNLIKELY(empty())) {
            _YAEF_THROW(std::out_of_range{"eliasfano_list::back: the list is empty"});
        }
        return max_;
    }

    _YAEF_ATTR_NODISCARD value_type min() const _YAEF_MAYBE_NOEXCEPT { 
        _YAEF_ASSERT(!empty());
        if (_YAEF_UNLIKELY(empty())) {
            _YAEF_THROW(std::out_of_range{"eliasfano_list::min: the list is empty"});
        }
        return min_; 
    }

    _YAEF_ATTR_NODISCARD value_type max() const _YAEF_MAYBE_NOEXCEPT { 
        _YAEF_ASSERT(!empty());
        if (_YAEF_UNLIKELY(empty())) {
            _YAEF_THROW(std::out_of_range{"eliasfano_list::max: the list is empty"});
        }
        return max_;
    }

    _YAEF_ATTR_NODISCARD value_type at(size_type index) const _YAEF_MAYBE_NOEXCEPT {
        _YAEF_ASSERT(index < size());
        if (_YAEF_UNLIKELY(index >= size())) {
            _YAEF_THROW(std::out_of_range{"eliasfano_list::at: index is out of range"});
        }
        unsigned_value_type h = high_bits_.select_one(index) - index - 1;
        unsigned_value_type l = get_low_bits().get_value(index);
        return to_actual_value(merge_bits(h, l));
    }

    _YAEF_ATTR_NODISCARD value_type operator[](size_type index) const _YAEF_MAYBE_NOEXCEPT {
        return at(index);
    }

    _YAEF_ATTR_NODISCARD const_iterator lower_bound(value_type target) const noexcept {
        return search_iter_impl(target, [](value_type elem, value_type t) -> bool {
            return elem < t;
        });
    }

    _YAEF_ATTR_NODISCARD const_iterator upper_bound(value_type target) const noexcept {
        return search_iter_impl(target, [](value_type elem, value_type t) -> bool {
            return elem <= t;
        });
    }

    _YAEF_ATTR_NODISCARD size_type index_of_lower_bound(value_type target) const noexcept {
        return search_index_impl(target, [](value_type elem, value_type t) -> bool {
            return elem < t;
        });
    }

    _YAEF_ATTR_NODISCARD size_type index_of_upper_bound(value_type target) const noexcept {
        return search_index_impl(target, [](value_type elem, value_type t) -> bool {
            return elem <= t;
        });
    }

    _YAEF_ATTR_NODISCARD bool contains(value_type target) const noexcept {
        auto iter = lower_bound(target);
        return iter != end() && *iter == target;
    }

    eliasfano_list &assign(std::initializer_list<value_type> initlist) {
        eliasfano_list<value_type> new_list(initlist);
        swap(new_list);
        return *this;
    }

    eliasfano_list &assign(from_sorted_t, std::initializer_list<value_type> initlist) {
        eliasfano_list<value_type> new_list(from_sorted, initlist);
        swap(new_list);
        return *this;
    }

    _YAEF_REQUIRES_RANDOM_ACCESS_ITER(RandomAccessIterT, SentIterT, std::is_integral)
    eliasfano_list &assign(RandomAccessIterT first, SentIterT last) {
        eliasfano_list<value_type> new_list(first, last);
        swap(new_list);
        return *this;
    }

    _YAEF_REQUIRES_RANDOM_ACCESS_ITER(RandomAccessIterT, SentIterT, std::is_integral)
    eliasfano_list &assign(from_sorted_t, RandomAccessIterT first, SentIterT last) {
        eliasfano_list<value_type> new_list(from_sorted, first, last);
        swap(new_list);
        return *this;
    }

    void swap(eliasfano_list &other) 
        noexcept(alloc_traits::propagate_on_container_swap::value ||
                 alloc_traits::is_always_equal::value) {
        if (_YAEF_UNLIKELY(this == std::addressof(other))) {
            return;
        }
        high_bits_.swap(other.high_bits_);
        get_low_bits().swap(other.get_low_bits());
        details::checked_swap_alloc(get_alloc(), other.get_alloc());
        std::swap(min_, other.min_);
        std::swap(max_, other.max_);
        std::swap(has_duplicates_, other.has_duplicates_);
    }

    template<typename U, typename AllocU>
    friend bool operator==(const eliasfano_list<U, AllocU> &lhs, 
                           const eliasfano_list<U, AllocU> &rhs);

#if __cplusplus < 202002L
    template<typename U, typename AllocU>
    friend bool operator!=(const eliasfano_list<U, AllocU> &lhs, 
                           const eliasfano_list<U, AllocU> &rhs);
#endif

protected:
    error_code do_serialize(details::serializer &ser) const {
        _YAEF_RETURN_ERR_IF_FAIL(high_bits_.serialize(ser));
        _YAEF_RETURN_ERR_IF_FAIL(get_low_bits().serialize(ser));
        if (!ser.write(min_)) { return error_code::serialize_io; }
        if (!ser.write(max_)) { return error_code::serialize_io; }
        if (!ser.write(has_duplicates_)) { return error_code::serialize_io; }
        return error_code::success;
    }

    error_code do_deserialize(details::deserializer &deser) {
        _YAEF_RETURN_ERR_IF_FAIL(high_bits_.deserialize(get_alloc(), deser));
        _YAEF_RETURN_ERR_IF_FAIL(get_low_bits().deserialize(get_alloc(), deser));
        if (!deser.read(min_)) { return error_code::deserialize_io; }
        if (!deser.read(max_)) { return error_code::deserialize_io; }
        if (!deser.read(has_duplicates_)) { return error_code::deserialize_io; }
        return error_code::success;
    }

private:
    using low_bits_with_alloc_type = details::value_with_allocator_pair<low_bits_type, allocator_type>;
    high_bits_type           high_bits_;
    low_bits_with_alloc_type low_bits_with_alloc_;
    value_type               min_ = std::numeric_limits<value_type>::max();
    value_type               max_ = std::numeric_limits<value_type>::min();
    bool                     has_duplicates_ = false;

    struct sorted_seq_info {
        bool       valid = false;
        bool       has_duplicates = false;
        size_type  num = 0;
        value_type min = std::numeric_limits<value_type>::max();
        value_type max = std::numeric_limits<value_type>::min();

        template<typename RandomAccessIterT, typename SentIterT>
        _YAEF_ATTR_NODISCARD static sorted_seq_info 
        create(RandomAccessIterT first, SentIterT last) {
            if (first > last) {
                return sorted_seq_info{};
            }
            if (first == last) {
                return sorted_seq_info{true, false, 0, std::numeric_limits<value_type>::max(), 
                                       std::numeric_limits<value_type>::min()};
            }

            if (!details::is_sorted(first, last)) {
                return sorted_seq_info{};
            }

            return sorted_seq_info{
                true,
                details::check_duplicate(first, last),
                static_cast<size_type>(details::iter_distance(first, last)),
                static_cast<value_type>(*first),
                static_cast<value_type>(*std::prev(last))
            };
        }

        template<typename RandomAccessIterT, typename SentIterT>
        _YAEF_ATTR_NODISCARD static sorted_seq_info 
        unchecked_create(RandomAccessIterT first, SentIterT last) {
            _YAEF_ASSERT(first <= last);
            _YAEF_ASSERT(details::is_sorted(first, last));

            return sorted_seq_info{
                true,
                details::check_duplicate(first, last),
                static_cast<size_type>(details::iter_distance(first, last)),
                static_cast<value_type>(*first),
                static_cast<value_type>(*std::prev(last))
            };
        }
    
    private:
        sorted_seq_info() = default;

        sorted_seq_info(bool valid, bool has_duplicates, size_type num, value_type min, value_type max) noexcept
            : valid(valid), has_duplicates(has_duplicates), num(num),
              min(min), max(max) { }
    };

    template<typename RandomAccessIterT, typename SentIterT>
    void unchecked_init_with_low_width(RandomAccessIterT first, SentIterT last, 
                                       sorted_seq_info sorted_info, uint32_t low_width) {
        _YAEF_ASSERT(low_width > 0);
        min_ = sorted_info.min;
        max_ = sorted_info.max;
        has_duplicates_ = sorted_info.has_duplicates;
        size_type num_elems = details::iter_distance(first, last);

        using encoder_type = details::eliasfano_encoder_scalar_impl<value_type, RandomAccessIterT, SentIterT>;
        encoder_type encoder{first, last, num_elems, sorted_info.min, sorted_info.max, low_width};
        
        get_low_bits() = details::allocate_packed_ints(get_alloc(), low_width, sorted_info.num);
        encoder.unchecked_enocde_low_bits(get_low_bits().blocks());

        auto raw_high_bits = details::allocate_bits(get_alloc(), encoder.estimate_high_size_in_bits());
        encoder.unchecked_encode_high_bits(raw_high_bits.blocks());
        high_bits_ = high_bits_type{get_alloc(), raw_high_bits};
    }

    _YAEF_ATTR_NODISCARD const high_bits_type &get_high_bits() const noexcept { 
        return high_bits_; 
    }

    _YAEF_ATTR_NODISCARD const low_bits_type &get_low_bits() const noexcept { 
        return low_bits_with_alloc_.value(); 
    }

    _YAEF_ATTR_NODISCARD low_bits_type &get_low_bits() noexcept {
        return low_bits_with_alloc_.value();
    }

    _YAEF_ATTR_NODISCARD const allocator_type &get_alloc() const noexcept {
        return low_bits_with_alloc_.alloc();
    }

    _YAEF_ATTR_NODISCARD allocator_type &get_alloc() noexcept {
        return low_bits_with_alloc_.alloc();
    }

    _YAEF_ATTR_NODISCARD unsigned_value_type split_high_bits(unsigned_value_type v) const noexcept {
        return v >> get_low_bits().width();
    }

    _YAEF_ATTR_NODISCARD unsigned_value_type split_low_bits(unsigned_value_type v) const noexcept {
        return v & details::bits64::make_mask_lsb1(get_low_bits().width());
    }

    _YAEF_ATTR_NODISCARD unsigned_value_type 
    merge_bits(unsigned_value_type high, unsigned_value_type low) const noexcept {
        return (high << get_low_bits().width()) | low;
    }

    _YAEF_ATTR_NODISCARD value_type to_actual_value(unsigned_value_type v) const noexcept {
        return v + min_;
    }

    _YAEF_ATTR_NODISCARD unsigned_value_type to_stored_value(value_type v) const noexcept {
        return static_cast<unsigned_value_type>(v) - static_cast<unsigned_value_type>(min_);
    }

    _YAEF_ATTR_NODISCARD const_iterator make_iter(size_type high_bit_offset, size_type index) const noexcept {
        if (_YAEF_UNLIKELY(index == size())) { return end(); }
        details::bits64::bitmap_foreach_onebit_cursor high_bits_cursor{high_bits_.get_bits(), high_bit_offset};
        return const_iterator{high_bits_cursor, get_low_bits(), min(), index};
    }

    struct search_result { 
        size_type  num_skipped_zeros; 
        size_type  index;
    };

    template<typename CmpElemWithTargetT>
    _YAEF_ATTR_NODISCARD search_result
    search_impl(value_type target, CmpElemWithTargetT cmp) const noexcept {
        if (_YAEF_UNLIKELY(!cmp(min(), target))) {
            return search_result{0, 0};
        }
        if (_YAEF_UNLIKELY(cmp(max(), target))) {
            return search_result{0, size()};
        }

        const size_type num_zeros = high_bits_.size() - size();
        const unsigned_value_type t = to_stored_value(target);
        const unsigned_value_type h = split_high_bits(t);
        const unsigned_value_type l = split_low_bits(t);
        const size_type start = high_bits_.select_zero(h) - h;
        const size_type end = h + 1 == num_zeros ? size() : high_bits_.select_zero(h + 1) - h - 1;
        size_type len = end - start;

        size_type result = end;

        auto &low_bits = get_low_bits();
        size_type base = start;
        while (len > 0) {
            size_type half = len / 2;
            base += (cmp(low_bits.get_value(base + half), l)) * (len - half);
            len = half;
        }
        result = base;
        const size_type num_skipped_zeros = h + 1;
        return search_result{num_skipped_zeros, result};
    }

    template<typename CmpElemWithTargetT>
    _YAEF_ATTR_NODISCARD size_type 
    search_index_impl(value_type target, CmpElemWithTargetT cmp) const noexcept {
        auto result = search_impl(target, cmp);
        return result.index;
    }

    template<typename CmpElemWithTargetT>
    _YAEF_ATTR_NODISCARD const_iterator 
    search_iter_impl(value_type target, CmpElemWithTargetT cmp) const noexcept {
        auto result = search_impl(target, cmp);
        return make_iter(result.index + result.num_skipped_zeros, result.index);
    }
};

template<typename T, typename AllocT>
_YAEF_ATTR_NODISCARD inline bool 
operator==(const eliasfano_list<T, AllocT> &lhs, const eliasfano_list<T, AllocT> &rhs) {
    if (_YAEF_UNLIKELY(std::addressof(lhs) == std::addressof(rhs))) {
        return true;
    }
    return lhs.get_low_bits() == rhs.get_low_bits() && lhs.high_bits_ == rhs.high_bits_;
}

#if __cplusplus >= 202002L
template<typename T, typename AllocT>
_YAEF_ATTR_NODISCARD inline std::strong_ordering
operator<=>(const eliasfano_list<T, AllocT> &lhs, const eliasfano_list<T, AllocT> &rhs) {
    if (_YAEF_UNLIKELY(std::addressof(lhs) == std::addressof(rhs))) {
        return std::strong_ordering::equivalent;
    }
    auto num_common = std::min(lhs.size(), rhs.size());
    auto lhs_iter = lhs.begin(), rhs_iter = rhs.begin();
    for (auto i = 0; i < num_common; ++i, ++lhs_iter, ++rhs_iter) {
        T lhs_val = *lhs_iter, rhs_val = *rhs_iter;
        if (lhs_val == rhs_val) {
            continue;
        }
        return lhs_val < rhs_val ? std::strong_ordering::less : std::strong_ordering::greater;
    }
    if (lhs_iter == lhs.end() && rhs_iter == rhs.end()) {
        return std::strong_ordering::equivalent;
    } else if (lhs_iter != lhs.end()) {
        return std::strong_ordering::greater;
    } else {
        return std::strong_ordering::less;
    }
}
#else
template<typename T, typename AllocT>
_YAEF_ATTR_NODISCARD inline bool operator!=(const eliasfano_list<T, AllocT> &lhs, 
                                            const eliasfano_list<T, AllocT> &rhs) {
    return !(lhs == rhs);
}

template<typename T, typename AllocT>
_YAEF_ATTR_NODISCARD inline bool operator<(const eliasfano_list<T, AllocT> &lhs, 
                                           const eliasfano_list<T, AllocT> &rhs) {
    if (_YAEF_UNLIKELY(std::addressof(lhs) == std::addressof(rhs))) {
        return false;
    }
    auto num_common = std::min(lhs.size(), rhs.size());
    auto lhs_iter = lhs.begin(), rhs_iter = rhs.begin();
    for (auto i = 0; i < num_common; ++i, ++lhs_iter, ++rhs_iter) {
        T lhs_val = *lhs_iter, rhs_val = *rhs_iter;
        if (lhs_val == rhs_val)
            continue;
        return lhs_val < rhs_val;
    }
    return lhs.size() < rhs.size();
}

template<typename T, typename AllocT>
_YAEF_ATTR_NODISCARD inline bool
operator>(const eliasfano_list<T, AllocT> &lhs, const eliasfano_list<T, AllocT> &rhs) {
    return rhs < lhs;
}

template<typename T, typename AllocT>
_YAEF_ATTR_NODISCARD inline bool
operator<=(const eliasfano_list<T, AllocT> &lhs, const eliasfano_list<T, AllocT> &rhs) {
    return !(rhs < lhs);
}

template<typename T, typename AllocT>
_YAEF_ATTR_NODISCARD inline bool
operator>=(const eliasfano_list<T, AllocT> &lhs, const eliasfano_list<T, AllocT> &rhs) {
    return !(lhs < rhs);
}
#endif

template<typename T, typename AllocT = details::aligned_allocator<uint8_t, 32>>
class eliasfano_sequence {
    friend struct details::serialize_friend_access;

    using alloc_traits    = std::allocator_traits<AllocT>;
public:
    using value_type      = T;
    using size_type       = size_t;
    using difference_type = ptrdiff_t;
    using const_iterator  = details::eliasfano_bidirectional_iterator<T>;
    using iterator        = const_iterator;
    using allocator_type  = AllocT;

public:
    eliasfano_sequence()
        : size_(0), high_bits_mem_(nullptr), low_bits_mem_(nullptr),
          low_width_(0), num_buckets_(0), has_duplicates_(false) { }
    
    eliasfano_sequence(const allocator_type &alloc)
        : size_(0), high_bits_mem_(nullptr), low_bits_mem_(nullptr),
          low_width_(0), num_buckets_(0),
          min_max_and_alloc_(std::pair<value_type, value_type>{}, alloc),
          has_duplicates_(false) { }
    
    eliasfano_sequence(const eliasfano_sequence &other)
        : eliasfano_sequence(other, alloc_traits::select_on_container_copy_construction(other.get_alloc())) { }

    eliasfano_sequence(eliasfano_sequence &&other) noexcept {
        size_ = details::exchange(other.size_, 0);
        high_bits_mem_ = details::exchange(other.high_bits_mem_, nullptr);
        low_bits_mem_ = details::exchange(other.low_bits_mem_, nullptr);
        low_width_ = num_buckets_ = 0;
        swap_low_width_and_num_buckets_impl(other);
        min_max_and_alloc_.value() = details::exchange(
            other.min_max_and_alloc_.value(), std::pair<value_type, value_type>{});
        details::checked_swap_alloc(get_alloc(), other.get_alloc());
        has_duplicates_ = details::exchange(other.has_duplicates_, false);
    }

    eliasfano_sequence(const eliasfano_sequence &other, const allocator_type &alloc)
        : eliasfano_sequence(alloc) {
        init_copy_impl(other);
    }

    eliasfano_sequence(eliasfano_sequence &&other, const allocator_type &alloc)
        : eliasfano_sequence(alloc) {
        if (get_alloc() == other.get_alloc()) {
            size_ = details::exchange(other.size_, 0);
            high_bits_mem_ = details::exchange(other.high_bits_mem_, nullptr);
            low_bits_mem_ = details::exchange(other.low_bits_mem_, nullptr);
            low_width_ = num_buckets_ = 0;
            swap_low_width_and_num_buckets_impl(other);
            min_max_and_alloc_.value() = details::exchange(
                other.min_max_and_alloc_.value(), std::pair<value_type, value_type>{});
            has_duplicates_ = details::exchange(other.has_duplicates_, false);
        } else {
            init_copy_impl(other);
        }
    }

    _YAEF_REQUIRES_RANDOM_ACCESS_ITER(RandomAccessIterT, SentIterT, std::is_integral)
    eliasfano_sequence(from_sorted_t, RandomAccessIterT first, SentIterT last, 
                       const allocator_type &alloc = allocator_type{})
        : eliasfano_sequence(alloc) {
        if (first > last) {
            _YAEF_THROW(std::invalid_argument{"eliasfano_sequence::eliasfano_sequence: the iterators are invalid"});
        }
        if (first != last) {
            unchecked_init(first, last);
        }       
    }

    _YAEF_REQUIRES_RANDOM_ACCESS_ITER(RandomAccessIterT, SentIterT, std::is_integral)
    eliasfano_sequence(RandomAccessIterT first, SentIterT last,
                       const allocator_type &alloc = allocator_type{})
        : eliasfano_sequence(alloc) {
        if (first > last) {
            _YAEF_THROW(std::invalid_argument{"eliasfano_sequence::eliasfano_sequence: the iterators are invalid"});
        }

        if (!details::is_sorted(first, last)) {
            _YAEF_THROW(std::invalid_argument{"eliasfano_sequence::eliasfano_sequence: the input data is not sorted"});
        }

        if (first != last) {
            unchecked_init(first, last);
        }
    }

    eliasfano_sequence(std::initializer_list<value_type> initlist)
        : eliasfano_sequence(initlist.begin(), initlist.end()) { }
    
    eliasfano_sequence(from_sorted_t, std::initializer_list<value_type> initlist)
        : eliasfano_sequence(from_sorted, initlist.begin(), initlist.end()) { }

    ~eliasfano_sequence() {
        destroy_impl();
    }

    eliasfano_sequence &operator=(const eliasfano_sequence &other) {
        if (_YAEF_UNLIKELY(this == std::addressof(other))) {
            return *this;
        }
        destroy_impl();
        init_copy_impl(other);
        return *this;
    }

    eliasfano_sequence &operator=(eliasfano_sequence &&other) noexcept {
        if (_YAEF_UNLIKELY(this == std::addressof(other))) {
            return *this;
        }
        destroy_impl();
        swap(other);
        return *this;
    }

    _YAEF_ATTR_NODISCARD size_type size() const noexcept { return size_; }
    _YAEF_ATTR_NODISCARD bool empty() const noexcept { return size() == 0; }
    _YAEF_ATTR_NODISCARD bool has_duplicates() const noexcept { return has_duplicates_; }

    _YAEF_ATTR_NODISCARD size_type space_usage_in_bytes() const noexcept {
        constexpr size_t BLOCK_WIDTH = sizeof(uint64_t) * CHAR_BIT;
        const size_type num_high_bits = size_ + num_buckets_ + 1;
        const size_type num_low_bits = size_ * low_width_;
        return details::bits64::idiv_ceil_nzero(num_high_bits, BLOCK_WIDTH) * sizeof(uint64_t) +
               details::bits64::idiv_ceil(num_low_bits, BLOCK_WIDTH) * sizeof(uint64_t);
    }

    _YAEF_ATTR_NODISCARD allocator_type get_allocator() const noexcept { return allocator_type{get_alloc()}; }
    _YAEF_ATTR_NODISCARD value_type min() const noexcept { return min_max_and_alloc_.value().first; }
    _YAEF_ATTR_NODISCARD value_type max() const noexcept { return min_max_and_alloc_.value().second; }
    _YAEF_ATTR_NODISCARD value_type front() const noexcept { return min(); }
    _YAEF_ATTR_NODISCARD value_type back() const noexcept { return max(); }

    _YAEF_ATTR_NODISCARD const_iterator begin() const noexcept {
        constexpr size_t BLOCK_WIDTH = sizeof(uint64_t) * CHAR_BIT;
        const size_type num_high_bits = size_ + num_buckets_ + 1;
        const size_type num_high_blocks = details::bits64::idiv_ceil_nzero(num_high_bits, BLOCK_WIDTH);

        details::bits64::bitmap_foreach_onebit_cursor high_bits_cursor{high_bits_mem_, num_high_blocks};
        details::bits64::packed_int_view low_bits{static_cast<uint32_t>(low_width_), low_bits_mem_, size_};
        return const_iterator{high_bits_cursor, low_bits, min(), 0};
    }

    _YAEF_ATTR_NODISCARD const_iterator end() const noexcept {
        details::bits64::packed_int_view low_bits{static_cast<uint32_t>(low_width_), low_bits_mem_, size_};
        return const_iterator{details::bits64::bitmap_foreach_onebit_cursor{}, low_bits, min(), size_};
    }

    _YAEF_ATTR_NODISCARD const_iterator cbegin() const noexcept { return begin(); }
    _YAEF_ATTR_NODISCARD const_iterator cend() const noexcept { return end(); }

    eliasfano_sequence &assign(std::initializer_list<value_type> initlist) {
        eliasfano_sequence<value_type> new_list(initlist);
        swap(new_list);
        return *this;
    }

    eliasfano_sequence &assign(from_sorted_t, std::initializer_list<value_type> initlist) {
        eliasfano_sequence<value_type> new_list(from_sorted, initlist);
        swap(new_list);
        return *this;
    }

    _YAEF_REQUIRES_RANDOM_ACCESS_ITER(RandomAccessIterT, SentIterT, std::is_integral)
    eliasfano_sequence &assign(RandomAccessIterT first, SentIterT last) {
        eliasfano_sequence<value_type> new_list(first, last);
        swap(new_list);
        return *this;
    }

    _YAEF_REQUIRES_RANDOM_ACCESS_ITER(RandomAccessIterT, SentIterT, std::is_integral)
    eliasfano_sequence &assign(from_sorted_t, RandomAccessIterT first, SentIterT last) {
        eliasfano_sequence<value_type> new_list(from_sorted, first, last);
        swap(new_list);
        return *this;
    }

    void swap(eliasfano_sequence &other) noexcept {
        if (_YAEF_UNLIKELY(this == std::addressof(other))) {
            return;
        }
        std::swap(size_, other.size_);
        std::swap(high_bits_mem_, other.high_bits_mem_);
        std::swap(low_bits_mem_, other.low_bits_mem_);
        swap_low_width_and_num_buckets_impl(other);
        std::swap(min_max_and_alloc_.value(), other.min_max_and_alloc_.value());
        details::checked_swap_alloc(get_alloc(), other.get_alloc());
        std::swap(has_duplicates_, other.has_duplicates_);
    }

    template<typename U, typename AllocU>
    friend bool operator==(const eliasfano_sequence<U, AllocU> &lhs, 
                           const eliasfano_sequence<U, AllocU> &rhs);

#if __cplusplus < 202002L
    template<typename U, typename AllocU>
    friend bool operator!=(const eliasfano_sequence<U, AllocU> &lhs, 
                           const eliasfano_sequence<U, AllocU> &rhs);
#endif

protected:
    error_code do_serialize(details::serializer &ser) const {
        constexpr size_t BLOCK_WIDTH = sizeof(uint64_t) * CHAR_BIT;
        const size_type num_high_bits = size_ + num_buckets_ + 1;
        const size_type num_low_bits = size_ * low_width_;
        const size_t num_high_bytes = details::bits64::idiv_ceil_nzero(num_high_bits, BLOCK_WIDTH) * sizeof(uint64_t);
        const size_t num_low_bytes = details::bits64::idiv_ceil(num_low_bits, BLOCK_WIDTH) * sizeof(uint64_t);

        if (!ser.write(size_)) { return error_code::serialize_io; }
        uint64_t low_width = low_width_;
        uint64_t num_buckets = num_buckets_;
        if (!ser.write(low_width)) { return error_code::serialize_io; }
        if (!ser.write(num_buckets)) { return error_code::serialize_io; }

        if (!ser.write(min_max_and_alloc_.value().first)) { return error_code::serialize_io; }
        if (!ser.write(min_max_and_alloc_.value().second)) { return error_code::serialize_io; }
        if (!ser.write(has_duplicates_)) { return error_code::serialize_io; }

        if (!ser.write_bytes(reinterpret_cast<const uint8_t *>(high_bits_mem_), num_high_bytes)) {
            return error_code::serialize_io;
        }
        if (!ser.write_bytes(reinterpret_cast<const uint8_t *>(low_bits_mem_), num_low_bytes)) {
            return error_code::serialize_io;
        }
        return error_code::success;
    }
    
    error_code do_deserialize(details::deserializer &deser) {
        if (!deser.read(size_)) { return error_code::serialize_io; }

        uint64_t low_width;
        if (!deser.read(low_width)) { return error_code::serialize_io; }
        if (low_width >= (static_cast<uint64_t>(1) << 6)) {
            return error_code::deserialize_invalid_format;
        }
        low_width_ = low_width;

        uint64_t num_buckets;
        if (!deser.read(num_buckets)) { return error_code::serialize_io; }
        if (num_buckets >= (static_cast<uint64_t>(1) << 58)) {
            return error_code::deserialize_invalid_format;
        }
        num_buckets_ = num_buckets;

        if (!deser.read(min_max_and_alloc_.value().first)) { return error_code::deserialize_io; }
        if (!deser.read(min_max_and_alloc_.value().second)) { return error_code::deserialize_io; }
        if (!deser.read(has_duplicates_)) { return error_code::deserialize_io; }

        constexpr size_t BLOCK_WIDTH = sizeof(uint64_t) * CHAR_BIT;
        const size_type num_high_bits = size_ + num_buckets_ + 1;
        const size_type num_low_bits = size_ * low_width_;
        const size_t num_high_bytes = details::bits64::idiv_ceil_nzero(num_high_bits, BLOCK_WIDTH) * sizeof(uint64_t);
        const size_t num_low_bytes = details::bits64::idiv_ceil(num_low_bits, BLOCK_WIDTH) * sizeof(uint64_t);

        high_bits_mem_ = reinterpret_cast<uint64_t *>(alloc_traits::allocate(get_alloc(), num_high_bytes));
        if (!deser.read_bytes(reinterpret_cast<uint8_t *>(high_bits_mem_), num_high_bytes)) {
            return error_code::deserialize_io;
        }

        low_bits_mem_ = reinterpret_cast<uint64_t *>(alloc_traits::allocate(get_alloc(), num_low_bytes));
        if (!deser.read_bytes(reinterpret_cast<uint8_t *>(low_bits_mem_), num_low_bytes)) {
            return error_code::deserialize_io;
        }
        return error_code::success;
    }

private:
    using data_pair = details::value_with_allocator_pair<
        std::pair<value_type, value_type>, allocator_type>;

    size_type  size_ = 0;
    uint64_t  *high_bits_mem_ = nullptr;
    uint64_t  *low_bits_mem_ = nullptr;
    uint64_t   low_width_   : 6;
    uint64_t   num_buckets_ : 58;
    data_pair  min_max_and_alloc_;
    bool       has_duplicates_ = false;

    _YAEF_ATTR_NODISCARD const allocator_type &get_alloc() const noexcept { return min_max_and_alloc_.alloc(); }
    _YAEF_ATTR_NODISCARD allocator_type &get_alloc() noexcept { return min_max_and_alloc_.alloc(); }

    template<typename RandomAccessIterT, typename SentIterT>
    void unchecked_init(RandomAccessIterT first, SentIterT last) {
        using encoder_type = details::eliasfano_encoder_scalar_impl<value_type, RandomAccessIterT, SentIterT>;
        encoder_type encoder{first, last, static_cast<size_type>(details::iter_distance(first, last))};
        size_ = encoder.size();
        low_width_ = encoder.low_width();
        num_buckets_ = (static_cast<uint64_t>(encoder.max()) - static_cast<uint64_t>(encoder.min())) >> low_width_;
        min_max_and_alloc_.value().first = encoder.min();
        min_max_and_alloc_.value().second = encoder.max();
        has_duplicates_ = details::check_duplicate(first, last);

        constexpr size_t BLOCK_WIDTH = sizeof(uint64_t) * CHAR_BIT;
        const size_type num_high_bits = size_ + num_buckets_ + 1;
        const size_type num_low_bits = size_ * low_width_;

        const size_type num_high_bytes = 
            details::bits64::idiv_ceil_nzero(num_high_bits, BLOCK_WIDTH) * sizeof(uint64_t);
        const size_type num_low_bytes = details::bits64::idiv_ceil(num_low_bits, BLOCK_WIDTH) * sizeof(uint64_t);

        high_bits_mem_ = reinterpret_cast<uint64_t *>(alloc_traits::allocate(get_alloc(), num_high_bytes));
        low_bits_mem_ = reinterpret_cast<uint64_t *>(alloc_traits::allocate(get_alloc(), num_low_bytes));
        
        encoder.unchecked_encode_high_bits(high_bits_mem_);
        encoder.unchecked_enocde_low_bits(low_bits_mem_);
    }

    void init_copy_impl(const eliasfano_sequence &other) {
        constexpr size_t BLOCK_WIDTH = sizeof(uint64_t) * CHAR_BIT;

        size_ = other.size_;
        low_width_ = other.low_width_;
        num_buckets_= other.num_buckets_;
        has_duplicates_ = other.has_duplicates_;

        const size_type num_high_bits = size_ + num_buckets_ + 1;
        const size_type num_low_bits = size_ * low_width_;
        const size_type num_high_bytes = details::bits64::idiv_ceil_nzero(num_high_bits, BLOCK_WIDTH) * sizeof(uint64_t);
        const size_type num_low_bytes = details::bits64::idiv_ceil(num_low_bits, BLOCK_WIDTH) * sizeof(uint64_t);

        high_bits_mem_ = reinterpret_cast<uint64_t *>(alloc_traits::allocate(get_alloc(), num_high_bytes));
        low_bits_mem_ = reinterpret_cast<uint64_t *>(alloc_traits::allocate(get_alloc(), num_low_bytes));

        ::memcpy(high_bits_mem_, other.high_bits_mem_, num_high_bytes);
        ::memcpy(low_bits_mem_, other.low_bits_mem_, num_low_bytes);
    }

    void destroy_impl() {
        constexpr size_t BLOCK_WIDTH = sizeof(uint64_t) * CHAR_BIT;
        const size_type num_high_bits = size_ + num_buckets_ + 1;
        const size_type num_low_bits = size_ * low_width_;
        const size_type num_high_bytes = details::bits64::idiv_ceil_nzero(num_high_bits, BLOCK_WIDTH) * sizeof(uint64_t);
        const size_type num_low_bytes = details::bits64::idiv_ceil(num_low_bits, BLOCK_WIDTH) * sizeof(uint64_t);

        alloc_traits::deallocate(get_alloc(), reinterpret_cast<uint8_t *>(high_bits_mem_), num_high_bytes);
        alloc_traits::deallocate(get_alloc(), reinterpret_cast<uint8_t *>(low_bits_mem_), num_low_bytes);
        
        size_ = 0;
        high_bits_mem_ = low_bits_mem_ = nullptr;
        low_width_ = num_buckets_ = 0;
        min_max_and_alloc_.value() = std::pair<value_type, value_type>{};
    }

    // because bitfields cannot be bound to reference, std::swap cannot be used
    void swap_low_width_and_num_buckets_impl(eliasfano_sequence &other) noexcept {
        uint64_t low_width_tmp = low_width_;
        low_width_ = other.low_width_;
        other.low_width_ = low_width_tmp;
        
        uint64_t num_buckets_tmp = num_buckets_;
        num_buckets_ = other.num_buckets_;
        other.num_buckets_ = num_buckets_tmp; 
    }
};

template<typename T, typename AllocT>
_YAEF_ATTR_NODISCARD inline bool operator==(const eliasfano_sequence<T, AllocT> &lhs, 
                                            const eliasfano_sequence<T, AllocT> &rhs) {
    if (_YAEF_UNLIKELY(std::addressof(lhs) == std::addressof(rhs))) {
        return true;
    }
    if (lhs.size() != rhs.size()) {
        return false;
    }
    auto lhs_iter = lhs.begin(), lhs_end = lhs.end();
    auto rhs_iter = rhs.begin();

    for (; lhs_iter != lhs_end; ++lhs_iter, ++rhs_iter) {
        if (*lhs_iter != *rhs_iter)
            return false;
    }
    return true;
}

#if __cplusplus >= 202002L
template<typename T, typename AllocT>
_YAEF_ATTR_NODISCARD inline std::strong_ordering operator<=>(const eliasfano_sequence<T, AllocT> &lhs, 
                                                             const eliasfano_sequence<T, AllocT> &rhs) {
    if (_YAEF_UNLIKELY(std::addressof(lhs) == std::addressof(rhs))) {
        return std::strong_ordering::equivalent;
    }
    auto num_common = std::min(lhs.size(), rhs.size());
    auto lhs_iter = lhs.begin(), rhs_iter = rhs.begin();
    for (auto i = 0; i < num_common; ++i, ++lhs_iter, ++rhs_iter) {
        T lhs_val = *lhs_iter, rhs_val = *rhs_iter;
        if (lhs_val == rhs_val) {
            continue;
        }
        return lhs_val < rhs_val ? std::strong_ordering::less : std::strong_ordering::greater;
    }
    if (lhs_iter == lhs.end() && rhs_iter == rhs.end()) {
        return std::strong_ordering::equivalent;
    } else if (lhs_iter != lhs.end()) {
        return std::strong_ordering::greater;
    } else {
        return std::strong_ordering::less;
    }
}

#else
template<typename T, typename AllocT>
_YAEF_ATTR_NODISCARD inline bool operator!=(const eliasfano_sequence<T, AllocT> &lhs, 
                                            const eliasfano_sequence<T, AllocT> &rhs) {
    return !(lhs == rhs);
}

template<typename T, typename AllocT>
_YAEF_ATTR_NODISCARD inline bool operator<(const eliasfano_sequence<T, AllocT> &lhs, 
                                           const eliasfano_sequence<T, AllocT> &rhs) {
    if (_YAEF_UNLIKELY(std::addressof(lhs) == std::addressof(rhs))) {
        return false;
    }
    auto num_common = std::min(lhs.size(), rhs.size());
    auto lhs_iter = lhs.begin(), rhs_iter = rhs.begin();
    for (auto i = 0; i < num_common; ++i, ++lhs_iter, ++rhs_iter) {
        T lhs_val = *lhs_iter, rhs_val = *rhs_iter;
        if (lhs_val == rhs_val) {
            continue;
        }
        return lhs_val < rhs_val;
    }
    return lhs.size() < rhs.size();
}

template<typename T, typename AllocT>
_YAEF_ATTR_NODISCARD inline bool operator>(const eliasfano_sequence<T, AllocT> &lhs, 
                                           const eliasfano_sequence<T, AllocT> &rhs) {
    return rhs < lhs;
}

template<typename T, typename AllocT>
_YAEF_ATTR_NODISCARD inline bool operator<=(const eliasfano_sequence<T, AllocT> &lhs, 
                                            const eliasfano_sequence<T, AllocT> &rhs) {
    return !(rhs < lhs);
}

template<typename T, typename AllocT>
_YAEF_ATTR_NODISCARD inline bool operator>=(const eliasfano_sequence<T, AllocT> &lhs, 
                                            const eliasfano_sequence<T, AllocT> &rhs) {
    return !(lhs < rhs);
}
#endif

template<bool IndexedBitType, typename AllocT = details::aligned_allocator<uint8_t, 32>>
class eliasfano_sparse_bitmap {
    friend struct details::serialize_friend_access;

    using alloc_traits    = std::allocator_traits<AllocT>;
public:
    using value_type      = bool;
    using size_type       = size_t;
    using difference_type = ptrdiff_t;
    using allocator_type  = AllocT;
    using base_list_type  = eliasfano_list<size_type, allocator_type>;
    static constexpr bool INDEXED_BIT_TYPE = IndexedBitType;

public:
    eliasfano_sparse_bitmap()
        : num_bits_(0) { }
    
    eliasfano_sparse_bitmap(const allocator_type &alloc)
        : pos_list_(alloc), num_bits_(0) { }

    eliasfano_sparse_bitmap(const eliasfano_sparse_bitmap &other)
        : pos_list_(other.pos_list_), num_bits_(other.num_bits_) { }
    
    eliasfano_sparse_bitmap(eliasfano_sparse_bitmap &&other) noexcept
        : pos_list_(std::move(other.pos_list_)), num_bits_(details::exchange(other.num_bits_, 0)) { }

    eliasfano_sparse_bitmap(const eliasfano_sparse_bitmap &other, const allocator_type &alloc)
        : pos_list_(other, alloc), num_bits_(other.num_bits_) { }
    
    eliasfano_sparse_bitmap(eliasfano_sparse_bitmap &&other, const allocator_type &alloc)
        : pos_list_(std::move(other), alloc), num_bits_(details::exchange(other.num_bits_, 0)) { }

    // construct from the data of a plain bitmap (the number of indexed bits is unknown)
    eliasfano_sparse_bitmap(const uint64_t *blocks, size_type num_bits,
                            const allocator_type &alloc = allocator_type{})
        : num_bits_(num_bits) {
        constexpr size_type BLOCK_WIDTH = sizeof(uint64_t) * CHAR_BIT;
        const size_type num_full_blocks = num_bits / BLOCK_WIDTH,
                        num_rem_bits = num_bits % BLOCK_WIDTH;
        const size_type num_blocks = num_full_blocks + (num_rem_bits > 0 ? 1 : 0);

        size_t num_indexed_bits = 0;

        for (size_t i = 0; i < num_full_blocks; ++i) {
            uint64_t block = blocks[i];
            if _YAEF_CXX17_CONSTEXPR (!INDEXED_BIT_TYPE) {
                block = ~block;
            }
            num_indexed_bits += details::bits64::popcount(block);
        }
        
        // handle last block if need
        if (num_rem_bits != 0) {
            uint64_t block = blocks[num_full_blocks];
            if _YAEF_CXX17_CONSTEXPR (!INDEXED_BIT_TYPE) {
                block = ~block;
            }
            block = details::bits64::extract_first_bits(block, num_rem_bits);
            num_indexed_bits += details::bits64::popcount(block);
        }

        auto indices = details::make_unique_array<size_type>(num_indexed_bits);
        size_type indice_writer = 0;

        details::bits64::bitmap_multiblocks_foreach_impl<INDEXED_BIT_TYPE>(blocks, num_blocks, [&](size_type index) {
            indices[indice_writer++] = index;
        });
        pos_list_ = eliasfano_list<size_type, allocator_type>(indices.get(), indices.get() + num_indexed_bits, alloc);
    }

    // construct from the data of a plain bitmap (the number of indexed bits is known)
    eliasfano_sparse_bitmap(const uint64_t *blocks, size_type num_bits, 
                            size_type num_indexed_bits, const allocator_type &alloc = allocator_type{})
        : num_bits_(num_bits) {
        const size_type num_blocks = details::bits64::idiv_ceil(num_bits, sizeof(uint64_t) * CHAR_BIT);
        auto indices = details::make_unique_array<size_type>(num_indexed_bits);
        size_type indice_writer = 0;

        details::bits64::bitmap_multiblocks_foreach_impl<INDEXED_BIT_TYPE>(blocks, num_blocks, [&](size_type index) {
            if (indice_writer > num_indexed_bits) {
                _YAEF_THROW(std::out_of_range{
                    "eliasfano_sparse_bitmap::eliasfano_sparse_bitmap: "
                    "the number of indexed bits exceeds the parameter `num_indexed_bits`."});
            }
            indices[indice_writer++] = index;
        });
        pos_list_ = eliasfano_list<size_type, allocator_type>{indices.get(), indices.get() + num_indexed_bits, alloc};
    }

    _YAEF_REQUIRES_RANDOM_ACCESS_ITER(RandomAccessIterT, SentIterT, std::is_integral)
    eliasfano_sparse_bitmap(size_t num_bits, RandomAccessIterT indices_first, SentIterT indices_last,
                            const allocator_type &alloc = allocator_type{})
        : num_bits_(num_bits) {
        pos_list_ = eliasfano_list<size_type, allocator_type>{indices_first, indices_last, alloc};
    }

    _YAEF_REQUIRES_RANDOM_ACCESS_ITER(RandomAccessIterT, SentIterT, std::is_integral)
    eliasfano_sparse_bitmap(from_sorted_t, size_t num_bits, 
                            RandomAccessIterT indices_first, SentIterT indices_last,
                            const allocator_type &alloc = allocator_type{})
        : num_bits_(num_bits) {
        pos_list_ = eliasfano_list<size_type, allocator_type>{from_sorted, indices_first, indices_last, alloc};
    }

    // construct from the indices of indexed bits
    eliasfano_sparse_bitmap(size_t num_bits, const size_type *indices, size_t num_indexed_bits,
                            const allocator_type &alloc = allocator_type{})
        : num_bits_(num_bits) {
        pos_list_ = eliasfano_list<size_type, allocator_type>{indices, indices + num_indexed_bits, alloc};
    }

    // construct from the indices of indexed bits and indices are aussumed to be sorted
    eliasfano_sparse_bitmap(from_sorted_t, size_t num_bits, const size_type *indices, size_t num_indexed_bits,
                            const allocator_type &alloc = allocator_type{})
        : num_bits_(num_bits) {
        pos_list_ = eliasfano_list<size_type, allocator_type>{from_sorted, indices, indices + num_indexed_bits, alloc};
    }

#if _YAEF_USE_STL_SPAN
    eliasfano_sparse_bitmap(size_t num_bits, std::span<const size_t> indices,
                            const allocator_type &alloc = allocator_type{})
        : num_bits_(num_bits) {
        pos_list_ = eliasfano_list<size_type>{indices.data(), indices.data() + indices.size(), alloc};
    }

    eliasfano_sparse_bitmap(from_sorted_t, size_t num_bits, std::span<const size_t> indices,
                            const allocator_type &alloc = allocator_type{})
        : num_bits_(num_bits) {
        pos_list_ = eliasfano_list<size_type>{from_sorted, indices.data(), indices.data() + indices.size(), alloc};
    }
#endif

    eliasfano_sparse_bitmap &operator=(const eliasfano_sparse_bitmap &other) {
        pos_list_ = other.pos_list_;
        num_bits_ = other.num_bits_;
        return *this;
    }

    eliasfano_sparse_bitmap &operator=(eliasfano_sparse_bitmap &&other) noexcept {
        pos_list_ = std::move(other.pos_list_);
        num_bits_ = details::exchange(other.num_bits_, 0);
        return *this;
    }

    _YAEF_ATTR_NODISCARD const base_list_type &base_list() const noexcept { return pos_list_; }

    _YAEF_ATTR_NODISCARD size_type size() const noexcept { return num_bits_; }
    _YAEF_ATTR_NODISCARD bool empty() const noexcept { return size() == 0; }

    _YAEF_ATTR_NODISCARD size_type space_usage_in_bytes() const noexcept {
        return pos_list_.space_usage_in_bytes();
    }

    _YAEF_ATTR_NODISCARD value_type at(size_type index) const noexcept {
        _YAEF_ASSERT(index < size());
        return index_related_impl{this}.at(index);
    }

    _YAEF_ATTR_NODISCARD value_type operator[](size_type index) const noexcept {
        return at(index);
    }

    _YAEF_ATTR_NODISCARD size_type count_one() const noexcept { 
        return index_related_impl{this}.count_one();
    }

    _YAEF_ATTR_NODISCARD size_type count_zero() const noexcept {
        return size() - count_one();
    }

    _YAEF_ATTR_NODISCARD size_type rank_one(size_type index) const noexcept {
        _YAEF_ASSERT(index <= size());
        return index_related_impl{this}.rank_one(index);
    }

    _YAEF_ATTR_NODISCARD size_type rank_one(size_type index, bool &bit_out) const noexcept {
        _YAEF_ASSERT(index <= size());
        return index_related_impl{this}.rank_one(index, &bit_out);
    }

    _YAEF_ATTR_NODISCARD size_type rank_zero(size_type index) const noexcept {
        return index - rank_one(index);
    }

    _YAEF_ATTR_NODISCARD size_type rank_zero(size_type index, bool &bit_out) const noexcept {
        return index - rank_one(index, bit_out);
    }

    _YAEF_ATTR_NODISCARD size_type select(size_type rank) const _YAEF_MAYBE_NOEXCEPT {
        _YAEF_ASSERT(rank < pos_list_.size());
        return pos_list_.at(rank);
    }
    
    _YAEF_ATTR_NODISCARD typename base_list_type::iterator 
    select_iter(size_type rank) const _YAEF_MAYBE_NOEXCEPT {
        _YAEF_ASSERT(rank < pos_list_.size());
        return pos_list_.iter(rank);
    }

    _YAEF_ATTR_NODISCARD size_type find_first() const noexcept {
        return pos_list_.front();
    }

    _YAEF_ATTR_NODISCARD size_type find_last() const noexcept {
        return pos_list_.back();
    }

    void swap(eliasfano_sparse_bitmap &other) noexcept {
        pos_list_.swap(other.pos_list_);
        std::swap(num_bits_, other.num_bits_);
    }

    template<bool B, typename AllocU>
    friend bool operator==(const eliasfano_sparse_bitmap<B, AllocU> &lhs, 
                           const eliasfano_sparse_bitmap<B, AllocU> &rhs);

#if __cplusplus < 202002L
    template<bool B, typename AllocU>
    friend bool operator!=(const eliasfano_sparse_bitmap<B, AllocU> &lhs, 
                           const eliasfano_sparse_bitmap<B, AllocU> &rhs);
#endif

protected:
    error_code do_serialize(details::serializer &ser) const {
        _YAEF_RETURN_ERR_IF_FAIL(pos_list_.do_serialize(ser));
        if (!ser.write(num_bits_)) { return error_code::serialize_io; }
        return error_code::success;
    }

    error_code do_deserialize(details::deserializer &deser) {
        _YAEF_RETURN_ERR_IF_FAIL(pos_list_.do_deserialize(deser));
        if (!deser.read(num_bits_)) { return error_code::deserialize_io; }
        return error_code::success;
    }

private:
    base_list_type pos_list_;
    size_type      num_bits_;

    class index_related_impl {
    public:
        index_related_impl(const eliasfano_sparse_bitmap *parent) noexcept
            : parent_(parent) { }

        _YAEF_ATTR_NODISCARD value_type at(size_type index) const noexcept {
            return at_impl(index, std::integral_constant<bool, INDEXED_BIT_TYPE>{});
        }

        _YAEF_ATTR_NODISCARD size_type count_one() const noexcept {
            return count_one_impl(std::integral_constant<bool, INDEXED_BIT_TYPE>{});
        }

        _YAEF_ATTR_NODISCARD size_type rank_one(size_type index) const noexcept {
            return rank_one_impl(index, std::integral_constant<bool, INDEXED_BIT_TYPE>{});
        }

        _YAEF_ATTR_NODISCARD size_type rank_one(size_type index, bool *bit_out) const noexcept {
            return rank_one_impl(index, bit_out, std::integral_constant<bool, INDEXED_BIT_TYPE>{});
        }

    private:
        const eliasfano_sparse_bitmap *parent_;

        _YAEF_ATTR_NODISCARD value_type at_impl(size_type index, std::true_type) const noexcept {
            return parent_->pos_list_.contains(index);
        }

        _YAEF_ATTR_NODISCARD value_type at_impl(size_type index, std::false_type) const noexcept {
            return !parent_->pos_list_.contains(index);
        }

        _YAEF_ATTR_NODISCARD size_type count_one_impl(std::true_type) const noexcept {
            return parent_->pos_list_.size();
        }

        _YAEF_ATTR_NODISCARD size_type count_one_impl(std::false_type) const noexcept {
            return parent_->size() - parent_->pos_list_.size();
        }
    
        _YAEF_ATTR_NODISCARD size_type rank_one_impl(size_type index, std::true_type) const noexcept {
            return parent_->pos_list_.lower_bound(index).to_index();
        }

        _YAEF_ATTR_NODISCARD size_type rank_one_impl(size_type index, std::false_type) const noexcept {
            return parent_->size() - parent_->pos_list_.lower_bound(index).to_index();
        }

        _YAEF_ATTR_NODISCARD size_type rank_one_impl(size_type index, bool *bit_out, std::true_type) const noexcept {
            auto iter = parent_->pos_list_.lower_bound(index);
            *bit_out = *iter == index ? INDEXED_BIT_TYPE : !INDEXED_BIT_TYPE;
            return iter.to_index();
        }

        _YAEF_ATTR_NODISCARD size_type rank_one_impl(size_type index, bool *bit_out, std::false_type) const noexcept {
            auto iter = parent_->pos_list_.lower_bound(index);
            *bit_out = *iter == index ? INDEXED_BIT_TYPE : !INDEXED_BIT_TYPE;
            return parent_->size() - iter.to_index();
        }

    };
};

template<bool IndexedBitType, typename AllocT>
inline bool operator==(const eliasfano_sparse_bitmap<IndexedBitType, AllocT> &lhs,
                       const eliasfano_sparse_bitmap<IndexedBitType, AllocT> &rhs) {
    return lhs.pos_list_ == rhs.pos_list_;
}

#if __cplusplus < 202002L
template<bool IndexedBitType, typename AllocT>
inline bool operator!=(const eliasfano_sparse_bitmap<IndexedBitType, AllocT> &lhs,
                       const eliasfano_sparse_bitmap<IndexedBitType, AllocT> &rhs) {
    return lhs.pos_list_ != rhs.pos_list_;
}
#endif

template<typename AllocT = details::aligned_allocator<uint8_t, 32>>
class bit_buffer {
    using view_type       = details::bits64::bit_view;
    using inner_type      = details::value_with_allocator_pair<view_type, AllocT>;

    friend struct details::serialize_friend_access;
public:
    using size_type       = typename view_type::size_type;
    using difference_type = ptrdiff_t;
    using value_type      = bool;
    using block_type      = typename view_type::block_type;
    using allocator_type  = AllocT;
    static constexpr size_type BLOCK_WIDTH = view_type::BLOCK_WIDTH; 

public:
    bit_buffer() = default;

    bit_buffer(const bit_buffer &other) {
        get_view() = details::duplicate_bits(get_alloc(), other.get_view());
    }

    bit_buffer(bit_buffer &&other) noexcept {
        get_view() = other.get_view();
        other.get_view() = view_type{};
    }

    explicit bit_buffer(size_type size) {
        get_view() = details::allocate_bits(get_alloc(), size);
    }

    bit_buffer(std::initializer_list<bool> initlist)
        : bit_buffer(initlist.size()) {
        size_type i = 0;
        for (bool val : initlist) {
            set_bit(i++, val);
        }
    }

    _YAEF_REQUIRES_RANDOM_ACCESS_ITER(RandomAccessIterT, SentIterT, std::is_unsigned)
    bit_buffer(RandomAccessIterT block_first, SentIterT block_last) {
        using input_type = typename std::iterator_traits<RandomAccessIterT>::value_type;

        _YAEF_STATIC_ASSERT_NOMSG(sizeof(input_type) == sizeof(block_type));
        size_type num_blocks = details::iter_distance(block_first, block_last);
        get_view() = details::allocate_bits(get_alloc(), sizeof(input_type) * CHAR_BIT * num_blocks);
        block_type *block_arr = get_view().blocks();
        for (size_type i = 0; i < num_blocks; ++i) {
            block_arr[i] = *block_first++;
        }
    }

    ~bit_buffer() {
        details::deallocate_bits(get_alloc(), get_view());
    }

    bit_buffer &operator=(const bit_buffer &other) {
        get_view() = details::duplicate_bits(get_alloc(), other.get_view());
        return *this;
    }

    bit_buffer &operator=(bit_buffer &&other) noexcept {
        get_view() = other.get_view();
        other.get_view() = view_type{};
        return *this;
    }

    _YAEF_ATTR_NODISCARD size_type space_usage_in_bytes() const noexcept { return get_view().space_usage_in_bytes(); }
    _YAEF_ATTR_NODISCARD size_type size() const noexcept { return get_view().size(); }
    _YAEF_ATTR_NODISCARD bool empty() const noexcept { return size() == 0; }
    _YAEF_ATTR_NODISCARD const block_type *block_data() const noexcept { return get_view().blocks(); }
    _YAEF_ATTR_NODISCARD block_type *block_data() noexcept { return get_view().blocks(); }
    _YAEF_ATTR_NODISCARD size_type num_blocks() const noexcept { return get_view().num_blocks(); }

    bit_buffer &assign(std::initializer_list<bool> initlist) {
        if (initlist.size() != size()) {
            details::deallocate_bits(get_alloc(), get_view());
            get_view() = details::allocate_bits(get_alloc(), initlist.size());
        }
        size_type i = 0;
        for (bool val : initlist) {
            set_bit(i++, val);
        }
        return *this;
    }

    _YAEF_REQUIRES_RANDOM_ACCESS_ITER(RandomAccessIterT, SentIterT, std::is_unsigned)
    bit_buffer &assign(RandomAccessIterT block_first, SentIterT block_last) {
        using input_type = typename std::iterator_traits<RandomAccessIterT>::value_type;

        _YAEF_STATIC_ASSERT_NOMSG(sizeof(input_type) == sizeof(block_type));
        size_type num_blocks = details::iter_distance(block_first, block_last);
        if (num_blocks != this->num_blocks()) {
            details::deallocate_packed_ints(get_alloc(), get_view());
            get_view() = details::allocate_bits(get_alloc(), sizeof(input_type) * CHAR_BIT * num_blocks);
        }
        block_type *block_arr = get_view().blocks();
        for (size_type i = 0; i < num_blocks; ++i) {
            block_arr[i] = *block_first++;
        }
    }

    _YAEF_ATTR_NODISCARD value_type get_bit(size_type index) const { 
        return get_view().get_bit(index); 
    }

    void set_bit(size_type index, value_type value) { 
        get_view().set_bit(index, value); 
    }

    _YAEF_ATTR_NODISCARD value_type operator[](size_type index) const { 
        return get_view().get_bit(index); 
    }

    void prefetch_for_read(size_type first, size_type last) const {
        get_view().prefetch_for_read(first, last);
    }

    void prefetch_for_write(size_type first, size_type last) {
        get_view().prefetch_for_write(first, last);
    }

    void set_all_bits() {
        get_view().set_all_bits();
    }

    void clear_all_bits() {
        get_view().clear_all_bits();
    }

    void reset() {
        details::deallocate_bits(get_alloc(), get_view());
        get_view() = view_type{};
    }

    void resize(size_t new_size) {
        if (_YAEF_UNLIKELY(new_size == size())) { return; }
        if (_YAEF_UNLIKELY(new_size == 0)) {
            reset();
            return;
        }

        auto new_vec = details::allocate_bits(get_alloc(), new_size);
        for (size_t i = 0; i < size(); ++i) {
            new_vec.set_bit(i, get_bit(i));
        }
        details::deallocate_bits(get_alloc(), get_view());
        get_view() = new_vec;
    }

    void swap(bit_buffer &other) noexcept {
        get_view().swap(other.get_view());
    }

    template<typename AllocU, typename AllocV>
    friend bool operator==(const bit_buffer<AllocU> &lhs, const bit_buffer<AllocV> &rhs);

#if __cplusplus < 202002L
    template<typename AllocU, typename AllocV>
    friend bool operator!=(const bit_buffer<AllocU> &lhs, const bit_buffer<AllocV> &rhs);
#endif

private:
    inner_type inner_;

    _YAEF_ATTR_NODISCARD const allocator_type &get_alloc() const { return inner_.alloc(); }
    _YAEF_ATTR_NODISCARD allocator_type &get_alloc() { return inner_.alloc(); }
    _YAEF_ATTR_NODISCARD const view_type &get_view() const { return inner_.value(); }
    _YAEF_ATTR_NODISCARD view_type &get_view() { return inner_.value(); }

    error_code do_serialize(details::serializer &ser) const {
        return get_view().serialize(ser);
    }

    error_code do_deserialize(details::deserializer &deser) {
        return get_view().deserialize(get_alloc(), deser);        
    }
};

template<typename AllocT, typename AllocU>
_YAEF_ATTR_NODISCARD inline bool operator==(const bit_buffer<AllocT> &lhs, 
                                            const bit_buffer<AllocU> &rhs) {
    return lhs.inner_ == rhs.inner_;
}

#if __cplusplus < 202002L
template<typename AllocT, typename AllocU>
_YAEF_ATTR_NODISCARD inline bool operator!=(const bit_buffer<AllocT> &lhs, 
                                            const bit_buffer<AllocU> &rhs) {
    return lhs.inner_ != rhs.inner_;
}
#endif

template<typename AllocT = details::aligned_allocator<uint8_t, 32>>
class packed_int_buffer {
public:
    using view_type       = details::bits64::packed_int_view;
    using inner_type      = details::value_with_allocator_pair<view_type, AllocT>;

    friend struct details::serialize_friend_access;
public:
    using size_type       = typename view_type::size_type;
    using difference_type = ptrdiff_t;
    using value_type      = uint64_t;
    using block_type      = typename view_type::block_type;
    using allocator_type  = AllocT;
    static constexpr size_type BLOCK_WIDTH = view_type::BLOCK_WIDTH; 

public:
    packed_int_buffer() = default;

    packed_int_buffer(const packed_int_buffer &other) {
        get_view() = details::duplicate_packed_ints(get_alloc(), other.get_view());
    }

    packed_int_buffer(packed_int_buffer &&other) noexcept {
        get_view() = other.get_view();
        other.get_view() = view_type{};
    }

    packed_int_buffer(uint32_t width, size_type size) {
        if (_YAEF_UNLIKELY(width > 64)) {
            _YAEF_THROW(std::runtime_error{
                "packed_int_buffer::packed_int_buffer: the width of packed_int_buffer should be between 0 and 64."});
        }
        get_view() = details::allocate_packed_ints(get_alloc(), width, size);
    }

    _YAEF_REQUIRES_RANDOM_ACCESS_ITER(RandomAccessIterT, SentIterT, std::is_unsigned)
    packed_int_buffer(RandomAccessIterT first, SentIterT last) {
        auto max_val = details::find_max_value(first, last);
        uint32_t width = std::max<uint32_t>(1, details::bits64::bit_width(max_val));
        size_type size = details::iter_distance(first, last);
        get_view() = details::allocate_packed_ints(get_alloc(), width, size);
        for (size_type i = 0; i < size; ++i) {
            set_value(i, *first++);
        }
    }

    _YAEF_REQUIRES_RANDOM_ACCESS_ITER(RandomAccessIterT, SentIterT, std::is_unsigned)
    packed_int_buffer(RandomAccessIterT first, SentIterT last, uint32_t width) {
        size_type size = details::iter_distance(first, last);
        get_view() = details::allocate_packed_ints(get_alloc(), width, size);
        for (size_type i = 0; i < size; ++i) {
            set_value(i, *first++);
        }
    }

    packed_int_buffer(std::initializer_list<value_type> initlist)
        : packed_int_buffer(initlist.begin(), initlist.end()) { }

    packed_int_buffer(std::initializer_list<value_type> initlist, uint32_t width)
        : packed_int_buffer(initlist.begin(), initlist.end(), width) { }

    ~packed_int_buffer() {
        details::deallocate_packed_ints(get_alloc(), get_view());
    }

    packed_int_buffer &operator=(const packed_int_buffer &other) {
        get_view() = details::duplicate_packed_ints(get_alloc(), other.get_view());
        return *this;
    }

    packed_int_buffer &operator=(packed_int_buffer &&other) noexcept {
        get_view() = other.get_view();
        other.get_view() = view_type{};
        return *this;
    }

    _YAEF_ATTR_NODISCARD size_type space_usage_in_bytes() const noexcept { return get_view().space_usage_in_bytes(); }
    _YAEF_ATTR_NODISCARD size_type size() const noexcept { return get_view().size(); }
    _YAEF_ATTR_NODISCARD bool empty() const noexcept { return size() == 0; }
    _YAEF_ATTR_NODISCARD uint32_t width() const noexcept { return get_view().width(); }
    _YAEF_ATTR_NODISCARD value_type limit_min() const { return get_view().limit_min(); }
    _YAEF_ATTR_NODISCARD value_type limit_max() const { return get_view().limit_max(); }

    _YAEF_ATTR_NODISCARD const block_type *block_data() const noexcept { return get_view().blocks(); }
    _YAEF_ATTR_NODISCARD block_type *block_data() noexcept { return get_view().blocks(); }
    _YAEF_ATTR_NODISCARD size_type num_blocks() const noexcept { return get_view().num_blocks(); }

    _YAEF_REQUIRES_RANDOM_ACCESS_ITER(RandomAccessIterT, SentIterT, std::is_unsigned)
    packed_int_buffer &assign(RandomAccessIterT first, SentIterT last) {
        auto max_val = details::find_max_value(first, last);
        uint32_t width = std::max<uint32_t>(1, details::bits64::bit_width(max_val));
        size_type size = details::iter_distance(first, last);
        if (this->size() != size || this->width() != width) {
            details::deallocate_packed_ints(get_alloc(), get_view());
            get_view() = details::allocate_packed_ints(get_alloc(), width, size);
        }

        for (size_type i = 0; i < size; ++i) {
            set_value(i, *first++);
        }
        return *this;
    }

    _YAEF_REQUIRES_RANDOM_ACCESS_ITER(RandomAccessIterT, SentIterT, std::is_unsigned)
    packed_int_buffer &assign(RandomAccessIterT first, SentIterT last, uint32_t width) {
        size_type size = details::iter_distance(first, last);
        if (this->size() != size || this->width() != width) {
            details::deallocate_packed_ints(get_alloc(), get_view());
            get_view() = details::allocate_packed_ints(get_alloc(), width, size);
        }
        for (size_type i = 0; i < size; ++i) {
            set_value(i, *first++);
        }
        return *this;
    }

    packed_int_buffer &assign(std::initializer_list<value_type> initlist) {
        return assign(initlist.begin(), initlist.end());
    }

    packed_int_buffer &assign(std::initializer_list<value_type> initlist, uint32_t width) {
        return assign(initlist.begin(), initlist.end(), width);
    }

    _YAEF_ATTR_NODISCARD value_type get_value(size_type index) const noexcept { 
        return get_view().get_value(index); 
    }

    void set_value(size_type index, value_type value) noexcept { 
        get_view().set_value(index, value); 
    }

    void prefetch_for_read(size_type first, size_type last) const {
        get_view().prefetch_for_read(first, last);
    }

    void prefetch_for_write(size_type first, size_type last) {
        get_view().prefetch_for_write(first, last);
    }

    _YAEF_ATTR_NODISCARD value_type operator[](size_type index) const noexcept {
        return get_view().get_value(index);
    }

    void fill_min_values() {
        get_view().clear_all_bits();
    }
    
    void fill_max_values() {
        get_view().set_all_bits();
    }

    void fill(value_type val) {
        get_view().fill(val);
    }

    void reset() {
        details::deallocate_packed_ints(get_alloc(), get_view());
        get_view() = view_type{};
    }

    void resize(size_t new_size) {
        _YAEF_ASSERT(width() != 0);

        if (_YAEF_UNLIKELY(new_size == size())) { return; }
        if (_YAEF_UNLIKELY(new_size == 0)) { 
            reset(); 
            return;
        }

        auto new_vec = details::allocate_packed_ints(get_alloc(), width(), new_size);
        for (size_t i = 0; i < size(); ++i) {
            new_vec.set_value(i, get_value(i));
        }
        details::deallocate_packed_ints(get_alloc(), get_view());
        get_view() = new_vec;
    }

    void swap(packed_int_buffer &other) noexcept {
        get_view().swap(other.get_view());
    }

    template<typename AllocU, typename AllocV>
    friend bool operator==(const packed_int_buffer<AllocU> &lhs, const packed_int_buffer<AllocV> &rhs);

#if __cplusplus < 202002L
    template<typename AllocU, typename AllocV>
    friend bool operator!=(const packed_int_buffer<AllocU> &lhs, const packed_int_buffer<AllocV> &rhs);
#endif

private:
    inner_type inner_;

    _YAEF_ATTR_NODISCARD const allocator_type &get_alloc() const { return inner_.alloc(); }
    _YAEF_ATTR_NODISCARD allocator_type &get_alloc() { return inner_.alloc(); }
    _YAEF_ATTR_NODISCARD const view_type &get_view() const { return inner_.value(); }
    _YAEF_ATTR_NODISCARD view_type &get_view() { return inner_.value(); }

    error_code do_serialize(details::serializer &ser) const {
        return get_view().serialize(ser);
    }

    error_code do_deserialize(details::deserializer &deser) {
        return get_view().deserialize(get_alloc(), deser);        
    }
};

template<typename AllocT, typename AllocU>
_YAEF_ATTR_NODISCARD inline bool operator==(const packed_int_buffer<AllocT> &lhs, 
                                            const packed_int_buffer<AllocU> &rhs) {
    return lhs.inner_ == rhs.inner_;
}

#if __cplusplus < 202002L
template<typename AllocT, typename AllocU>
_YAEF_ATTR_NODISCARD inline bool operator!=(const packed_int_buffer<AllocT> &lhs, 
                                            const packed_int_buffer<AllocU> &rhs) {
    return lhs.inner_ != rhs.inner_;
}
#endif

enum class sample_strategy {
    cardinality, universe
};

template<sample_strategy Strategy>
struct defaule_sample_rate;

template<>
struct defaule_sample_rate<sample_strategy::cardinality> 
    : std::integral_constant<size_t, 256> { };

template<>
struct defaule_sample_rate<sample_strategy::universe> 
    : std::integral_constant<size_t, std::numeric_limits<uint16_t>::max()> { };

template<typename T, sample_strategy Strategy = sample_strategy::cardinality, 
         size_t SampleRate = defaule_sample_rate<Strategy>::value,
         typename AllocT = details::aligned_allocator<T, 32>>
class sparse_sampled_list {
    friend struct details::serialize_friend_access;

    using alloc_traits       = std::allocator_traits<AllocT>;
    using byte_alloc         = typename alloc_traits::template rebind_alloc<uint8_t>;
    using byte_alloc_traits  = std::allocator_traits<byte_alloc>;
    using is_cardinality_tag = std::integral_constant<sample_strategy, sample_strategy::cardinality>;
    using is_universe_tag    = std::integral_constant<sample_strategy, sample_strategy::universe>;
    using strategy_tag       = std::integral_constant<sample_strategy, Strategy>;
public:
    using value_type      = T;
    using size_type       = size_t;
    using const_pointer   = const T *;
    using pointer         = T *;
    using const_reference = const T &;
    using reference       = T &;
    using const_iterator  = const T *;
    using iterator        = const_iterator;
    using allocator_type  = AllocT;

    static constexpr sample_strategy SAMPLE_STRATEGY = Strategy;
    static constexpr size_type SAMPLE_RATE = SampleRate;

public:
    sparse_sampled_list()
        : num_samples_and_alloc_(0, allocator_type{}),
          samples_(nullptr), size_(0), data_(nullptr) { }

    sparse_sampled_list(const sparse_sampled_list &other)
        : num_samples_and_alloc_(other.get_num_samples(), other.get_alloc()),
          samples_(nullptr), size_(other.size_), data_(nullptr) {
        data_ = alloc_traits::allocate(get_alloc(), size_);
        std::uninitialized_copy_n(other.data_, size_, data_);
        copy_samples_from(other, strategy_tag{});
    }

    sparse_sampled_list(sparse_sampled_list &&other) noexcept
        : num_samples_and_alloc_(details::exchange(other.get_num_samples(), 0), allocator_type{}),
          samples_(details::exchange(other.samples_, nullptr)),
          size_(details::exchange(other.size_, 0)),
          data_(details::exchange(other.data_, nullptr)) {
        details::checked_swap_alloc(get_alloc(), other.get_alloc());
    }

    _YAEF_REQUIRES_FORWARD_ITER(ForwardIter, SentIterT, std::is_integral)
    sparse_sampled_list(ForwardIter first, SentIterT last) {
        if (_YAEF_UNLIKELY(!details::is_sorted(first, last))) {
            _YAEF_THROW(std::invalid_argument(
                "sparse_sampled_list::sparse_sampled_list: the input data is not sorted"));
        }

        size_ = details::iter_distance(first, last);
        data_ = alloc_traits::allocate(get_alloc(), size_);
        std::uninitialized_copy_n(first, size_, data_);
        build_samples(strategy_tag{});
    }

    _YAEF_REQUIRES_FORWARD_ITER(ForwardIter, SentIterT, std::is_integral)
    sparse_sampled_list(from_sorted_t, ForwardIter first, SentIterT last) {
        size_ = details::iter_distance(first, last);
        data_ = alloc_traits::allocate(get_alloc(), size_);
        std::uninitialized_copy_n(first, size_, data_);
        build_samples(strategy_tag{});
    }

    sparse_sampled_list(std::initializer_list<value_type> initlist)
        : sparse_sampled_list(initlist.begin(), initlist.end()) { }

    sparse_sampled_list(from_sorted_t, std::initializer_list<value_type> initlist)
        : sparse_sampled_list(from_sorted, initlist.begin(), initlist.end()) { }

    ~sparse_sampled_list() {
        do_deallocate();
    }

    sparse_sampled_list &operator=(const sparse_sampled_list &other) {
        do_deallocate();
        copy_from(other);
        return *this;
    }

    sparse_sampled_list &operator=(sparse_sampled_list &&other) noexcept {
        samples_ = details::exchange(other.samples_, nullptr);
        get_num_samples() = details::exchange(other.get_num_samples(), 0);
        data_ = details::exchange(other.data_, nullptr);
        size_ = details::exchange(other.size_, 0);
        return *this;
    }

    _YAEF_ATTR_NODISCARD size_type size() const noexcept { return size_; }
    _YAEF_ATTR_NODISCARD bool empty() const noexcept { return size_ == 0; }
    _YAEF_ATTR_NODISCARD size_type num_samples() const noexcept { return get_num_samples(); }
    _YAEF_ATTR_NODISCARD const_pointer data() const noexcept { return data_; }
    _YAEF_ATTR_NODISCARD allocator_type get_allocator() const { return allocator_type(get_alloc()); }

    _YAEF_ATTR_NODISCARD size_type space_usage_in_bytes() const {
        return sizeof(value_type) * get_num_samples() + sizeof(value_type) * size_;
    }

    _YAEF_ATTR_NODISCARD value_type min() const _YAEF_MAYBE_NOEXCEPT {
        if (_YAEF_UNLIKELY(size_ == 0)) {
            _YAEF_THROW(std::out_of_range("sparse_sampled_list::max: the list is empty"));
        }
        return data_[0];
    }

    _YAEF_ATTR_NODISCARD value_type max() const _YAEF_MAYBE_NOEXCEPT {
        if (_YAEF_UNLIKELY(size_ == 0)) {
            _YAEF_THROW(std::out_of_range("sparse_sampled_list::max: the list is empty"));
        }
        return data_[size_ - 1];
    }

    _YAEF_ATTR_NODISCARD value_type at(size_type index) const _YAEF_MAYBE_NOEXCEPT {
        if (_YAEF_UNLIKELY(index >= size_)) {
            _YAEF_THROW(std::out_of_range("sparse_sampled_list::at: the index is out of range"));
        }
        return data_[index];
    }

    _YAEF_ATTR_NODISCARD value_type operator[](size_type index) const _YAEF_MAYBE_NOEXCEPT { 
        return at(index); 
    }

    _YAEF_ATTR_NODISCARD const_iterator begin() const { return cbegin(); }
    _YAEF_ATTR_NODISCARD const_iterator end() const { return cend(); }
    _YAEF_ATTR_NODISCARD const_iterator cbegin() const { return data_; }
    _YAEF_ATTR_NODISCARD const_iterator cend() const { return size_ == 0 ? data_ : data_ + size_; }

    _YAEF_ATTR_NODISCARD const_iterator lower_bound(value_type target) const {
        return data_ + index_of_lower_bound(target);
    }

    _YAEF_ATTR_NODISCARD const_iterator upper_bound(value_type target) const {
        return data_ + index_of_upper_bound(target);
    }

    _YAEF_ATTR_NODISCARD size_type index_of_lower_bound(value_type target) const {
        if (_YAEF_UNLIKELY(target > max())) {
            return size_;
        }
        if (_YAEF_UNLIKELY(target <= min())) {
            return 0;
        }

        return do_search_lower_bound(target, strategy_tag{});
    }

    _YAEF_ATTR_NODISCARD size_type index_of_upper_bound(value_type target) const {
        if (_YAEF_UNLIKELY(target >= max())) {
            return size_;
        }
        if (_YAEF_UNLIKELY(target < min())) {
            return 0;
        }

        return do_search_upper_bound(target, strategy_tag{});
    }

    _YAEF_REQUIRES_FORWARD_ITER(ForwardIter, SentIterT, std::is_integral)
    sparse_sampled_list &assign(ForwardIter first, SentIterT last) {
        if (!_YAEF_UNLIKELY(details::is_sorted(first, last))) {
            _YAEF_THROW(std::invalid_argument(
                "sparse_sampled_list::assign: the input data is not sorted"));
        }

        do_deallocate();
        size_ = details::iter_distance(first, last);
        data_ = alloc_traits::allocate(get_alloc(), size_);
        std::uninitialized_copy_n(first, size_, data_);
        build_samples(strategy_tag{});
        return *this;
    }

    _YAEF_REQUIRES_FORWARD_ITER(ForwardIter, SentIterT, std::is_integral)
    sparse_sampled_list &assign(from_sorted_t, ForwardIter first, SentIterT last) {
        do_deallocate();
        size_ = details::iter_distance(first, last);
        data_ = alloc_traits::allocate(get_alloc(), size_);
        std::uninitialized_copy_n(first, size_, data_);
        build_samples(strategy_tag{});
        return *this;
    }

    sparse_sampled_list &assign(std::initializer_list<value_type> initlist) {
        return assign(initlist.begin(), initlist.end());
    }

    sparse_sampled_list &assign(from_sorted_t, std::initializer_list<value_type> initlist) {
        return assign(from_sorted, initlist.begin(), initlist.end());
    }

    void swap(sparse_sampled_list &other) noexcept {
        std::swap(get_num_samples(), other.get_num_samples());
        details::checked_swap_alloc(get_alloc(), get_alloc());
        std::swap(samples_, other.samples_);
        std::swap(data_, other.data_);
        std::swap(size_, other.size_);
    }

private:
    using num_samples_and_alloc = details::value_with_allocator_pair<size_type, allocator_type>;

    num_samples_and_alloc  num_samples_and_alloc_;
    uint8_t               *samples_;
    size_type              size_;
    pointer                data_;

    _YAEF_ATTR_NODISCARD const size_type &get_num_samples() const noexcept { return num_samples_and_alloc_.value(); }
    _YAEF_ATTR_NODISCARD size_type &get_num_samples() { return num_samples_and_alloc_.value(); }
    _YAEF_ATTR_NODISCARD const allocator_type &get_alloc() const { return num_samples_and_alloc_.alloc(); }
    _YAEF_ATTR_NODISCARD allocator_type &get_alloc() { return num_samples_and_alloc_.alloc(); }

    error_code do_serialize(details::serializer &ser) const {
        if (!ser.write(get_num_samples())) { return error_code::serialize_io; }
        
        if _YAEF_CXX17_CONSTEXPR (SAMPLE_STRATEGY == sample_strategy::cardinality) {
            if (!ser.write_bytes(samples_, sizeof(value_type) * get_num_samples())) { return error_code::serialize_io; }
        } else if _YAEF_CXX17_CONSTEXPR (SAMPLE_STRATEGY == sample_strategy::universe) {
            if (!ser.write_bytes(samples_, sizeof(size_type) * get_num_samples())) { return error_code::serialize_io; }
        }

        if (!ser.write(size_)) { return error_code::serialize_io; }
        if (!ser.write_bytes(reinterpret_cast<const uint8_t *>(data_), sizeof(value_type) * size_)) {
            return error_code::serialize_io;
        }
        return error_code::success;
    }
    
    error_code do_deserialize(details::deserializer &deser) {
        if (!deser.read(get_num_samples())) { return error_code::deserialize_io; }
    
        byte_alloc balloc(get_alloc());
        if _YAEF_CXX17_CONSTEXPR (SAMPLE_STRATEGY == sample_strategy::cardinality) {
            samples_ = byte_alloc_traits::allocate(balloc, sizeof(value_type) * get_num_samples());
            if (!deser.read_bytes(samples_, sizeof(value_type) * get_num_samples())) { return error_code::serialize_io; }
        } else if _YAEF_CXX17_CONSTEXPR (SAMPLE_STRATEGY == sample_strategy::universe) {
            samples_ = byte_alloc_traits::allocate(balloc, sizeof(size_type) * get_num_samples());
            if (!deser.read_bytes(samples_, sizeof(size_type) * get_num_samples())) { return error_code::serialize_io; }
        }

        if (!deser.read(size_)) { return error_code::serialize_io; }
        data_ = alloc_traits::allocate(get_alloc(), size_);
        if (!deser.read_bytes(reinterpret_cast<uint8_t *>(data_), sizeof(value_type) * size_)) {
            return error_code::serialize_io;
        }
        return error_code::success;
    }

    void do_deallocate() {
        if (samples_) {
            byte_alloc balloc(get_alloc());
            if (SAMPLE_STRATEGY == sample_strategy::cardinality) {
                byte_alloc_traits::deallocate(balloc, samples_, sizeof(value_type) * get_num_samples());
            } else {
                byte_alloc_traits::deallocate(balloc, samples_, sizeof(size_type) * get_num_samples());                
            }
            samples_ = nullptr;
            get_num_samples() = 0;
        }
        if (data_) {
            alloc_traits::deallocate(get_alloc(), data_, size_);
            data_ = nullptr;
            size_ = 0;
        }
    }

    void copy_samples_from(const sparse_sampled_list &other, is_cardinality_tag) {
        byte_alloc balloc(get_alloc());
        samples_ = byte_alloc_traits::allocate(balloc, sizeof(value_type) * get_num_samples());
        std::uninitialized_copy_n(other.samples_, sizeof(value_type) * get_num_samples(), samples_);
    }

    void copy_samples_from(const sparse_sampled_list &other, is_universe_tag) {
        byte_alloc balloc(get_alloc());
        samples_ = byte_alloc_traits::allocate(balloc, sizeof(size_type) * get_num_samples());
        std::uninitialized_copy_n(other.samples_, sizeof(size_type) * get_num_samples(), samples_);
    }

    void copy_from(const sparse_sampled_list &other) {
        get_num_samples() = other.get_num_samples();
        size_ = other.size_;
        get_alloc() = other.get_alloc();

        data_ = alloc_traits::allocate(get_alloc(), size_);
        std::uninitialized_copy_n(other.data_, size_, data_);

        copy_samples_from(other, strategy_tag{});
    }

    void build_eytzinger(value_type *out, size_type &reader_idx, size_type writer_idx) {
        if (writer_idx >= get_num_samples()) {
            return;
        }
        size_type left_child = 2 * writer_idx + 1;
        if (left_child < get_num_samples()) {
            build_eytzinger(out, reader_idx, left_child);
        }
        out[writer_idx] = data_[reader_idx * SAMPLE_RATE]; 
        ++reader_idx;
        size_type right_child = 2 * writer_idx + 2;
        if (right_child < get_num_samples()) {
            build_eytzinger(out, reader_idx, right_child);
        }
    }

    void build_samples(is_cardinality_tag) {
        get_num_samples() = details::bits64::idiv_ceil(size_, SAMPLE_RATE);
        
        byte_alloc balloc(get_alloc());
        samples_ = byte_alloc_traits::allocate(balloc, sizeof(value_type) * get_num_samples());
        value_type *samples = reinterpret_cast<value_type *>(samples_);
        for (size_type i = 0; i < get_num_samples(); ++i) {
            samples[i] = data_[i * SAMPLE_RATE];
        }
    }

    void build_samples(is_universe_tag) {
        const value_type u = max() - min();
        get_num_samples() = details::bits64::idiv_ceil(u, SAMPLE_RATE) + 1;

        byte_alloc balloc(get_alloc());
        samples_ = byte_alloc_traits::allocate(balloc, sizeof(size_type) * get_num_samples());
        size_type *samples = reinterpret_cast<size_type *>(samples_);

        std::uninitialized_fill_n(samples, get_num_samples(), 0);
        for (size_type i = 0; i < size_; ++i) {
            ++samples[(data_[i] - min()) / SAMPLE_RATE];
        }
        samples[get_num_samples() - 1] = size_;

        size_type prefix = 0;
        for (size_type i = 0; i < get_num_samples() - 1; ++i) {
            size_type cnt = samples[i];
            samples[i] = prefix;
            prefix += cnt;
        }
    }

    // search lower bound with cardinality partitioned samples
    _YAEF_ATTR_NODISCARD size_type do_search_lower_bound(value_type target, is_cardinality_tag) const {
        const value_type *samples = reinterpret_cast<const value_type *>(samples_);

        const value_type *iter = details::branchless_upper_bound(samples, get_num_samples(), target) - 1;
        size_type sample_index = iter - samples;

        const size_type partition_size = size_ - sample_index * SAMPLE_RATE;
        iter = details::branchless_lower_bound(data_ + sample_index * SAMPLE_RATE, partition_size, target);
        return iter - data_;
    }

    // search lower bound with cardinality partitioned samples
    _YAEF_ATTR_NODISCARD size_type do_search_upper_bound(value_type target, is_cardinality_tag) const {
        const value_type *samples = reinterpret_cast<const value_type *>(samples_);

        const value_type *iter = details::branchless_upper_bound(samples, get_num_samples(), target) - 1;
        size_type sample_index = iter - samples;

        const size_type partition_size = size_ - sample_index * SAMPLE_RATE;
        iter = details::branchless_upper_bound(data_ + sample_index * SAMPLE_RATE, partition_size, target);
        return iter - data_;
    }

    // search lower bound with universe partitioned samples
    _YAEF_ATTR_NODISCARD size_type do_search_lower_bound(value_type target, is_universe_tag) const {
        size_type sample_index = (target - min()) / SAMPLE_RATE;
        
        const size_type *samples = reinterpret_cast<const size_type *>(samples_);
        size_type first = samples[sample_index], 
                  last = samples[sample_index + 1];
        value_type *iter = details::branchless_lower_bound(data_ + first, last - first, target);
        size_type idx = iter - data_;
        return idx;
    }

    // search upper bound with universe partitioned samples
    _YAEF_ATTR_NODISCARD size_type do_search_upper_bound(value_type target, is_universe_tag) const {
        size_type sample_index = (target - min()) / SAMPLE_RATE;
        
        const size_type *samples = reinterpret_cast<const size_type *>(samples_);
        size_type first = samples[sample_index], 
                  last = samples[sample_index + 1];
        value_type *iter = details::branchless_upper_bound(data_ + first, last - first, target);
        size_type idx = iter - data_;
        return idx;
    }
};

namespace details {

struct hybrid_dispatch_name {
    template<typename T>
    struct caller {
        void operator()(std::string *name_out) {
            T::name(name_out);
        }
    };
};

struct hybrid_dispatch_at {
    template<typename T>
    struct caller {
        void operator()(size_t offset, const uint8_t *data, uint8_t ext_meta, uint64_t *res_out) {
            T::at(offset, data, ext_meta, res_out);
        }
    };
};

struct hybrid_dispatch_index_of_lower_bound {
    template<typename T>
    struct caller {
        void operator()(uint64_t target, const uint8_t *data, uint8_t ext_meta, size_t *res_out) {
            T::index_of_lower_bound(target, data, ext_meta, res_out);
        }
    };
};

template<size_t Alternatives, typename MethodTup>
struct hybrid_method_dispatcher;

#define _YAEF_DISPATCH_CALL(_idx) \
    typename Fn::template caller<typename std::tuple_element<_idx, MethodTup>::type>()(std::forward<ArgTs>(args)...);

template<typename MethodTup>
struct hybrid_method_dispatcher<1, MethodTup> {
    template<typename Fn, typename ...ArgTs>
    static void execute(size_t idx, ArgTs &&...args) {
        _YAEF_ASSERT(idx == 0);
#ifdef NDEBUG
        _YAEF_UNUSED(idx);
#endif
        _YAEF_DISPATCH_CALL(0);
    }
};

template<typename MethodTup>
struct hybrid_method_dispatcher<2, MethodTup> {
    template<typename Fn, typename ...ArgTs>
    static void execute(size_t idx, ArgTs &&...args) {
        _YAEF_ASSERT(idx == 0 || idx == 1);
        if (idx == 0) {
            _YAEF_DISPATCH_CALL(0);
        } else {
            _YAEF_DISPATCH_CALL(1);
        }
    }
};

template<typename MethodTup>
struct hybrid_method_dispatcher<3, MethodTup> {
    template<typename Fn, typename ...ArgTs>
    static void execute(size_t idx, ArgTs &&...args) {
        switch (idx) {
        case 0  : _YAEF_DISPATCH_CALL(0); break;
        case 1  : _YAEF_DISPATCH_CALL(1); break;
        case 2  : _YAEF_DISPATCH_CALL(2); break;
        default : _YAEF_UNREACHABLE();
        }
    }
};

template<typename MethodTup>
struct hybrid_method_dispatcher<4, MethodTup> {
    template<typename Fn, typename ...ArgTs>
    static void execute(size_t idx, ArgTs &&...args) {
        switch (idx) {
        case 0  : _YAEF_DISPATCH_CALL(0); break;
        case 1  : _YAEF_DISPATCH_CALL(1); break;
        case 2  : _YAEF_DISPATCH_CALL(2); break;
        case 3  : _YAEF_DISPATCH_CALL(3); break;
        default : _YAEF_UNREACHABLE();
        }
    }
};

template<typename MethodTup>
struct hybrid_method_dispatcher<5, MethodTup> {
    template<typename Fn, typename ...ArgTs>
    static void execute(size_t idx, ArgTs &&...args) {
        switch (idx) {
        case 0  : _YAEF_DISPATCH_CALL(0); break;
        case 1  : _YAEF_DISPATCH_CALL(1); break;
        case 2  : _YAEF_DISPATCH_CALL(2); break;
        case 3  : _YAEF_DISPATCH_CALL(3); break;
        case 4  : _YAEF_DISPATCH_CALL(4); break;
        default : _YAEF_UNREACHABLE();
        }
    }
};

template<typename MethodTup>
struct hybrid_method_dispatcher<6, MethodTup> {
    template<typename Fn, typename ...ArgTs>
    static void execute(size_t idx, ArgTs &&...args) {
        switch (idx) {
        case 0  : _YAEF_DISPATCH_CALL(0); break;
        case 1  : _YAEF_DISPATCH_CALL(1); break;
        case 2  : _YAEF_DISPATCH_CALL(2); break;
        case 3  : _YAEF_DISPATCH_CALL(3); break;
        case 4  : _YAEF_DISPATCH_CALL(4); break;
        case 5  : _YAEF_DISPATCH_CALL(5); break;
        default : _YAEF_UNREACHABLE();
        }
    }
};

template<typename MethodTup>
struct hybrid_method_dispatcher<7, MethodTup> {
    template<typename Fn, typename ...ArgTs>
    static void execute(size_t idx, ArgTs &&...args) {
        switch (idx) {
        case 0  : _YAEF_DISPATCH_CALL(0); break;
        case 1  : _YAEF_DISPATCH_CALL(1); break;
        case 2  : _YAEF_DISPATCH_CALL(2); break;
        case 3  : _YAEF_DISPATCH_CALL(3); break;
        case 4  : _YAEF_DISPATCH_CALL(4); break;
        case 5  : _YAEF_DISPATCH_CALL(5); break;
        case 6  : _YAEF_DISPATCH_CALL(6); break;
        default : _YAEF_UNREACHABLE();
        }
    }
};

template<typename MethodTup>
struct hybrid_method_dispatcher<8, MethodTup> {
    template<typename Fn, typename ...ArgTs>
    static void execute(size_t idx, ArgTs &&...args) {
        switch (idx) {
        case 0  : _YAEF_DISPATCH_CALL(0); break;
        case 1  : _YAEF_DISPATCH_CALL(1); break;
        case 2  : _YAEF_DISPATCH_CALL(2); break;
        case 3  : _YAEF_DISPATCH_CALL(3); break;
        case 4  : _YAEF_DISPATCH_CALL(4); break;
        case 5  : _YAEF_DISPATCH_CALL(5); break;
        case 6  : _YAEF_DISPATCH_CALL(6); break;
        case 7  : _YAEF_DISPATCH_CALL(7); break;
        default : _YAEF_UNREACHABLE();
        }
    }
};
#undef _YAEF_DISPATCH_CALL

}

class hybrid_method_encoder {
public:
    virtual ~hybrid_method_encoder() = default;

    virtual size_t estimated_bits() const = 0;
    virtual size_t required_bits() const = 0;
    virtual void encode(uint8_t *buf_out, uint8_t &ext_meta) const = 0;
};

struct hybrid_method_stat_entry {
    uint32_t    id = -1;
    std::string name;
    size_t      encoded_elements = 0;
    size_t      num_partitions = 0;
    size_t      space_usage_in_bytes = 0;
    double      encoding_ratio = 0;
    double      space_usage_ratio = 0;
};

namespace details {

static constexpr size_t DEFAULT_HYBRID_PARTITION_SIZE = 256;

template<typename ...Methods>
struct hybrid_method_type_list {
    using tuple_type = std::tuple<Methods...>;
    using dispatcher = details::hybrid_method_dispatcher<sizeof...(Methods), tuple_type>;

    template<size_t Idx>
    using at = typename std::tuple_element<Idx, tuple_type>::type;

    template<size_t Idx, typename IterT>
    using encoder_type_of = typename at<Idx>::template encoder_type<IterT>;

    template<typename IterT>
    using encoders_tuple = std::tuple<typename Methods::template encoder_type<IterT>...>;

    template<size_t Idx, typename RandomAccessIterT>
    _YAEF_ATTR_NODISCARD static std::unique_ptr<hybrid_method_encoder>
    new_encoder(RandomAccessIterT first, size_t n) {
        return std::unique_ptr<encoder_type_of<Idx, RandomAccessIterT>>(
            new encoder_type_of<Idx, RandomAccessIterT>(first, n)
        );
    }

    _YAEF_ATTR_NODISCARD static std::string dispatch_name(size_t method_idx) {
        std::string name;
        dispatcher::template execute<details::hybrid_dispatch_name>(method_idx, &name);
        return name;
    }

    _YAEF_ATTR_NODISCARD static uint64_t 
    dispatch_at(size_t method_idx, size_t offset, const uint8_t *data, uint8_t ext_meta) {
        uint64_t res = 0;
        dispatcher::template execute<details::hybrid_dispatch_at>(method_idx, offset, data, ext_meta, &res);
        return res;
    }

    _YAEF_ATTR_NODISCARD static size_t 
    dispatch_index_of_lower_bound(size_t method_idx, size_t offset, const uint8_t *data, uint8_t ext_meta) {
        size_t res = 0;
        dispatcher::template execute<details::hybrid_dispatch_index_of_lower_bound>(
            method_idx, offset, data, ext_meta, &res);
        return res;
    }
};

template<typename MethodTup, size_t N>
struct hybrid_method_selector;

template<typename MethodTup>
struct hybrid_method_selector<MethodTup, 1> {
    template<typename RandomAccessIterT>
    static std::pair<std::unique_ptr<hybrid_method_encoder>, size_t> 
    select(RandomAccessIterT first, size_t n) {
        return std::make_pair(std::tuple_element<0, MethodTup>::type::new_encoder(first, n), 0);
    }
};

template<typename MethodTup>
struct hybrid_method_selector<MethodTup, 2> {
    template<typename RandomAccessIterT>
    static std::pair<std::unique_ptr<hybrid_method_encoder>, size_t> 
    select(RandomAccessIterT first, size_t n) {
        auto encoder1 = std::tuple_element<0, MethodTup>::type::new_encoder(first, n);
        auto encoder2 = std::tuple_element<1, MethodTup>::type::new_encoder(first, n);
        if (encoder1->estimated_bits() <= encoder2->estimated_bits()) {
            return std::make_pair(std::move(encoder1), 0);
        } else {
            return std::make_pair(std::move(encoder2), 1);
        }
    }
};

template<size_t N>
_YAEF_ATTR_NODISCARD std::pair<std::unique_ptr<hybrid_method_encoder>, size_t> 
find_best_hybrid_encoder(std::array<std::unique_ptr<hybrid_method_encoder>, N> &arr) {
    size_t best_bits = arr[0]->estimated_bits();
    size_t best_idx = 0;
    for (size_t i = 1; i < N; ++i) {
        size_t estimated = arr[i]->estimated_bits();
        if (estimated < best_bits) {
            best_bits = estimated;
            best_idx = i;
        }
    }
    return std::make_pair(std::move(arr[best_idx]), best_idx);
}

template<typename MethodTup>
struct hybrid_method_selector<MethodTup, 3> {
    template<typename RandomAccessIterT>
    static std::pair<std::unique_ptr<hybrid_method_encoder>, size_t> 
    select(RandomAccessIterT first, size_t n) {
        std::array<std::unique_ptr<hybrid_method_encoder>, 3> cands;
        cands[0] = std::tuple_element<0, MethodTup>::type::new_encoder(first, n);
        cands[1] = std::tuple_element<1, MethodTup>::type::new_encoder(first, n);
        cands[2] = std::tuple_element<2, MethodTup>::type::new_encoder(first, n);
        return find_best_hybrid_encoder(cands);
    }
};

template<typename MethodTup>
struct hybrid_method_selector<MethodTup, 4> {
    template<typename RandomAccessIterT>
    static std::pair<std::unique_ptr<hybrid_method_encoder>, size_t> 
    select(RandomAccessIterT first, size_t n) {
        std::array<std::unique_ptr<hybrid_method_encoder>, 4> cands;
        cands[0] = std::tuple_element<0, MethodTup>::type::new_encoder(first, n);
        cands[1] = std::tuple_element<1, MethodTup>::type::new_encoder(first, n);
        cands[2] = std::tuple_element<2, MethodTup>::type::new_encoder(first, n);
        cands[3] = std::tuple_element<3, MethodTup>::type::new_encoder(first, n);
        return find_best_hybrid_encoder(cands);
    }
};

template<typename MethodTup>
struct hybrid_method_selector<MethodTup, 5> {
    template<typename RandomAccessIterT>
    static std::pair<std::unique_ptr<hybrid_method_encoder>, size_t> 
    select(RandomAccessIterT first, size_t n) {
        std::array<std::unique_ptr<hybrid_method_encoder>, 5> cands;
        cands[0] = std::tuple_element<0, MethodTup>::type::new_encoder(first, n);
        cands[1] = std::tuple_element<1, MethodTup>::type::new_encoder(first, n);
        cands[2] = std::tuple_element<2, MethodTup>::type::new_encoder(first, n);
        cands[3] = std::tuple_element<3, MethodTup>::type::new_encoder(first, n);
        cands[4] = std::tuple_element<4, MethodTup>::type::new_encoder(first, n);
        return find_best_hybrid_encoder(cands);
    }
};

template<typename MethodTup>
struct hybrid_method_selector<MethodTup, 6> {
    template<typename RandomAccessIterT>
    static std::pair<std::unique_ptr<hybrid_method_encoder>, size_t> 
    select(RandomAccessIterT first, size_t n) {
        std::array<std::unique_ptr<hybrid_method_encoder>, 6> cands;
        cands[0] = std::tuple_element<0, MethodTup>::type::new_encoder(first, n);
        cands[1] = std::tuple_element<1, MethodTup>::type::new_encoder(first, n);
        cands[2] = std::tuple_element<2, MethodTup>::type::new_encoder(first, n);
        cands[3] = std::tuple_element<3, MethodTup>::type::new_encoder(first, n);
        cands[4] = std::tuple_element<4, MethodTup>::type::new_encoder(first, n);
        cands[5] = std::tuple_element<5, MethodTup>::type::new_encoder(first, n);
        return find_best_hybrid_encoder(cands);
    }
};

template<typename MethodTup>
struct hybrid_method_selector<MethodTup, 7> {
    template<typename RandomAccessIterT>
    static std::pair<std::unique_ptr<hybrid_method_encoder>, size_t> 
    select(RandomAccessIterT first, size_t n) {
        std::array<std::unique_ptr<hybrid_method_encoder>, 7> cands;
        cands[0] = std::tuple_element<0, MethodTup>::type::new_encoder(first, n);
        cands[1] = std::tuple_element<1, MethodTup>::type::new_encoder(first, n);
        cands[2] = std::tuple_element<2, MethodTup>::type::new_encoder(first, n);
        cands[3] = std::tuple_element<3, MethodTup>::type::new_encoder(first, n);
        cands[4] = std::tuple_element<4, MethodTup>::type::new_encoder(first, n);
        cands[5] = std::tuple_element<5, MethodTup>::type::new_encoder(first, n);
        cands[6] = std::tuple_element<6, MethodTup>::type::new_encoder(first, n);
        return find_best_hybrid_encoder(cands);
    }
};

template<typename MethodTup>
struct hybrid_method_selector<MethodTup, 8> {
    template<typename RandomAccessIterT>
    static std::pair<std::unique_ptr<hybrid_method_encoder>, size_t> 
    select(RandomAccessIterT first, size_t n) {
        std::array<std::unique_ptr<hybrid_method_encoder>, 8> cands;
        cands[0] = std::tuple_element<0, MethodTup>::type::new_encoder(first, n);
        cands[1] = std::tuple_element<1, MethodTup>::type::new_encoder(first, n);
        cands[2] = std::tuple_element<2, MethodTup>::type::new_encoder(first, n);
        cands[3] = std::tuple_element<3, MethodTup>::type::new_encoder(first, n);
        cands[4] = std::tuple_element<4, MethodTup>::type::new_encoder(first, n);
        cands[5] = std::tuple_element<5, MethodTup>::type::new_encoder(first, n);
        cands[6] = std::tuple_element<6, MethodTup>::type::new_encoder(first, n);
        cands[7] = std::tuple_element<7, MethodTup>::type::new_encoder(first, n);
        return find_best_hybrid_encoder(cands);
    }
};

template<typename RandomAccessIterT>
class hybrid_encoding_iterator_adaptor {
public:
    using difference_type   = typename std::iterator_traits<RandomAccessIterT>::difference_type;
    using value_type        = uint64_t;
    using pointer           = const uint64_t*;
    using reference         = uint64_t;
    using iterator_category = std::random_access_iterator_tag;

    hybrid_encoding_iterator_adaptor() = default;

    explicit hybrid_encoding_iterator_adaptor(
        RandomAccessIterT iter, 
        typename std::iterator_traits<RandomAccessIterT>::value_type min,
        uint64_t sample)
        : cur_(iter), min_(static_cast<uint64_t>(min)), sample_(sample) {}

    reference operator*() const {
        return static_cast<uint64_t>(*cur_) - min_ - sample_;
    }

    pointer operator->() const {
        holder_ = operator*();
        return &holder_;
    }

    bool operator==(const hybrid_encoding_iterator_adaptor& other) const {
        return cur_ == other.cur_;
    }

    bool operator!=(const hybrid_encoding_iterator_adaptor& other) const {
        return !(*this == other);
    }

    bool operator<(const hybrid_encoding_iterator_adaptor& other) const {
        return cur_ < other.cur_;
    }

    bool operator>(const hybrid_encoding_iterator_adaptor& other) const {
        return other < *this;
    }

    bool operator<=(const hybrid_encoding_iterator_adaptor& other) const {
        return !(other < *this);
    }

    bool operator>=(const hybrid_encoding_iterator_adaptor& other) const {
        return !(*this < other);
    }

    hybrid_encoding_iterator_adaptor& operator++() {
        ++cur_;
        return *this;
    }

    hybrid_encoding_iterator_adaptor operator++(int) {
        hybrid_encoding_iterator_adaptor temp = *this;
        ++cur_;
        return temp;
    }

    hybrid_encoding_iterator_adaptor& operator--() {
        --cur_;
        return *this;
    }

    hybrid_encoding_iterator_adaptor operator--(int) {
        hybrid_encoding_iterator_adaptor temp = *this;
        --cur_;
        return temp;
    }

    hybrid_encoding_iterator_adaptor operator+(difference_type n) const {
        return hybrid_encoding_iterator_adaptor(cur_ + n, min_, sample_);
    }

    hybrid_encoding_iterator_adaptor operator-(difference_type n) const {
        return hybrid_encoding_iterator_adaptor(cur_ - n);
    }

    difference_type operator-(const hybrid_encoding_iterator_adaptor& other) const {
        return cur_ - other.cur_;
    }

    hybrid_encoding_iterator_adaptor& operator+=(difference_type n) {
        cur_ += n;
        return *this;
    }

    hybrid_encoding_iterator_adaptor& operator-=(difference_type n) {
        cur_ -= n;
        return *this;
    }

    reference operator[](difference_type n) const {
        return static_cast<uint64_t>(cur_[n]) - min_ - sample_;
    }

    const RandomAccessIterT& base() const {
        return cur_;
    }
private:
    RandomAccessIterT cur_;
    uint64_t          min_ = 0;
    uint64_t          sample_ = 0;
    mutable uint64_t  holder_ = 0;
};

template<typename RandomAccessIterT>
inline hybrid_encoding_iterator_adaptor<RandomAccessIterT> operator+(
    typename hybrid_encoding_iterator_adaptor<RandomAccessIterT>::difference_type n,
    const hybrid_encoding_iterator_adaptor<RandomAccessIterT>& iter) {
    return iter + n;
}

}

namespace hybrid_methods {

class fixed {
public:
    template<typename RandomAccessIterT>
    struct encoder_type : hybrid_method_encoder {
        encoder_type(RandomAccessIterT first, size_t n)
            : first_(first), num_(n) {
            auto maxval = first_[num_ - 1];
            width_ = std::max<uint32_t>(1, details::bits64::bit_width(maxval));
        }

        _YAEF_ATTR_NODISCARD size_t estimated_bits() const override {
            return num_ * width_;
        }

        _YAEF_ATTR_NODISCARD size_t required_bits() const override {
            return num_ * width_;
        }

        void encode(uint8_t *buf_out, uint8_t &ext_meta) const override {
            using details::bits64::bit_view;
            bit_view bv(reinterpret_cast<uint64_t *>(buf_out), bit_view::dont_care_size);
            ext_meta = width_;

            for (size_t i = 0, bit_offset = 0; i < num_; ++i, bit_offset += width_) {
                uint64_t bits = first_[i];
                bv.set_bits(bit_offset, width_, bits);
            }
        }
    
    private:
        RandomAccessIterT first_;
        size_t            num_;
        uint32_t          width_;
    };

public:
    template<typename RandomAccessIterT>
    static std::unique_ptr<encoder_type<RandomAccessIterT>>
    new_encoder(RandomAccessIterT first, size_t n) {
        return std::unique_ptr<encoder_type<RandomAccessIterT>>(
            new encoder_type<RandomAccessIterT>(first, n)
        );
    }

    static void name(std::string *out) { *out = "fixed"; }

    static void at(size_t offset, const uint8_t *data, uint8_t ext_meta, uint64_t *res_out) {
        using details::bits64::bit_view;
        bit_view bv(reinterpret_cast<uint64_t *>(const_cast<uint8_t *>(data)), bit_view::dont_care_size);
        const uint32_t width = ext_meta;

        *res_out = bv.get_bits(width * offset, width);
    }

    static void index_of_lower_bound(size_t target, const uint8_t *data, uint8_t ext_meta, size_t *res_out) {
        using details::bits64::bit_view;
        bit_view bv(reinterpret_cast<uint64_t *>(const_cast<uint8_t *>(data)), bit_view::dont_care_size);
        const uint32_t width = ext_meta;

        for (size_t i = 0, bit_offset = 0; i < details::DEFAULT_HYBRID_PARTITION_SIZE; ++i, bit_offset += width) {
           if (bv.get_bits(bit_offset, width) >= target) {
                *res_out = i;
                return;
            }
        }
        *res_out = details::DEFAULT_HYBRID_PARTITION_SIZE;
    }
};

class fixed_gap {
public:
    template<typename RandomAccessIterT>
    struct encoder_type : hybrid_method_encoder {
        encoder_type(RandomAccessIterT first, size_t n)
            : first_(first), num_(n) {
            width_ = 1;
            for (size_t i = 1; i < n; ++i) {
                uint64_t gap = first[i] - first[i - 1];
                width_ = std::max(width_, details::bits64::bit_width(gap));
            }
        }

        _YAEF_ATTR_NODISCARD size_t estimated_bits() const override {
            return width_ * (num_ - 1);
        }

        _YAEF_ATTR_NODISCARD size_t required_bits() const override {
            return width_ * (num_ - 1);
        }

        void encode(uint8_t *buf_out, uint8_t &ext_meta) const override {
            using details::bits64::bit_view;
            bit_view bv(reinterpret_cast<uint64_t *>(buf_out), bit_view::dont_care_size);
            ext_meta = static_cast<uint8_t>(width_);

            for (size_t i = 1, bit_offset = 0; i < num_; ++i, bit_offset += width_) {
                uint64_t gap = first_[i] - first_[i - 1];
                bv.set_bits(bit_offset, width_, gap);
            }
        }
    
    private:
        RandomAccessIterT first_;
        size_t            num_;
        uint32_t          width_;
    };

public:
    template<typename RandomAccessIterT>
    static std::unique_ptr<encoder_type<RandomAccessIterT>>
    new_encoder(RandomAccessIterT first, size_t n) {
        return std::unique_ptr<encoder_type<RandomAccessIterT>>(
            new encoder_type<RandomAccessIterT>(first, n)
        );
    }

    static void name(std::string *out) { *out = "fixed_gap"; }

    static void at(size_t offset, const uint8_t *data, uint8_t ext_meta, uint64_t *res_out) {
        using details::bits64::bit_view;
        bit_view bv(reinterpret_cast<uint64_t *>(const_cast<uint8_t *>(data)), bit_view::dont_care_size);
        const uint32_t width = ext_meta;

        uint64_t res = 0;
        for (size_t i = 0, bit_offset = 0; i < offset; ++i, bit_offset += width) {
            uint64_t gap = bv.get_bits(bit_offset, width);
            res += gap;
        }
        *res_out = res;
    }

    static void index_of_lower_bound(size_t target, const uint8_t *data, uint8_t ext_meta, size_t *res_out) {
        using details::bits64::bit_view;
        bit_view bv(reinterpret_cast<uint64_t *>(const_cast<uint8_t *>(data)), bit_view::dont_care_size);
        const uint32_t width = ext_meta;

        uint64_t val = 0;
        for (size_t i = 0, bit_offset = 0; i < details::DEFAULT_HYBRID_PARTITION_SIZE - 1; ++i, bit_offset += width) {
            uint64_t gap = bv.get_bits(bit_offset, width);
            val += gap;
            if (val >= target) {
                *res_out = i + 1;
                return;
            }
        }
        *res_out = details::DEFAULT_HYBRID_PARTITION_SIZE;
    }
};

class eliasgamma_unique_gap {
public:
    template<typename RandomAccessIterT>
    struct encoder_type : hybrid_method_encoder {
        encoder_type(RandomAccessIterT first, size_t n)
            : first_(first), num_(n) {
            for (size_t i = 1; i < n; ++i) {
                uint64_t gap = first[i] - first[i - 1];
                if (_YAEF_UNLIKELY(gap == 0)) {
                    estimated_bits_ = std::numeric_limits<size_t>::max();
                    return;
                }

                uint32_t width = std::max<uint32_t>(1, details::bits64::bit_width(gap));
                estimated_bits_ += 2 * width - 1;
                required_unary_bits_ += width;
                required_body_bits_ += width - 1;
            }
            required_unary_bits_ = details::bits64::align_to<64>(required_unary_bits_);
            required_body_bits_ = details::bits64::align_to<64>(required_body_bits_);
        }

        _YAEF_ATTR_NODISCARD size_t estimated_bits() const override {
            return estimated_bits_;
        }

        _YAEF_ATTR_NODISCARD size_t required_bits() const override {
            return required_unary_bits_ + required_body_bits_;
        }

        void encode(uint8_t *buf_out, uint8_t &ext_meta) const override {
            using details::bits64::bit_view;

            size_t num_unary_blocks = details::bits64::align_to<64>(required_unary_bits_) / bit_view::BLOCK_WIDTH;
            _YAEF_ASSERT(num_unary_blocks <= 256);
            ext_meta = static_cast<uint8_t>(num_unary_blocks - 1);

            auto unary_view = bit_view(reinterpret_cast<uint64_t *>(buf_out), bit_view::dont_care_size);
            auto body_view = bit_view(
                reinterpret_cast<uint64_t *>(buf_out) + num_unary_blocks, bit_view::dont_care_size);

            unary_view.clear_all_bits(0, details::bits64::align_to<64>(required_unary_bits_));
            body_view.clear_all_bits(0, required_body_bits_);

            for (size_t i = 1, unary_bit_offset = 0, body_bit_offset = 0; i < num_; ++i) {
                uint64_t gap = first_[i] - first_[i - 1];
                uint32_t width = details::bits64::bit_width(gap);
                unary_view.set_bit(unary_bit_offset + width - 1);
                unary_bit_offset += width;
                if (width > 1) {
                    body_view.set_bits(body_bit_offset, width - 1, gap);
                }
                body_bit_offset += width - 1;
            }
        }
    
    private:
        RandomAccessIterT first_;
        size_t            num_;
        size_t            estimated_bits_      = 0;
        size_t            required_unary_bits_ = 0;
        size_t            required_body_bits_  = 0;
    };

public:
    template<typename RandomAccessIterT>
    static std::unique_ptr<encoder_type<RandomAccessIterT>>
    new_encoder(RandomAccessIterT first, size_t n) {
        return std::unique_ptr<encoder_type<RandomAccessIterT>>(
            new encoder_type<RandomAccessIterT>(first, n)
        );
    }

    static void name(std::string *out) { *out = "eliasgamma_gap"; }

    static void at(size_t offset, const uint8_t *data, uint8_t ext_meta, uint64_t *res_out) {
        using details::bits64::bit_view;
        
        auto unary_data = reinterpret_cast<uint64_t *>(const_cast<uint8_t *>(data));
        const size_t num_unary_blocks = static_cast<size_t>(ext_meta) + 1;

        auto body_view = bit_view(
            reinterpret_cast<uint64_t *>(const_cast<uint8_t *>(data)) + num_unary_blocks, bit_view::dont_care_size);
        auto body_reader = body_view.new_reader();
        
        uint8_t prv_zeros = 0;
        uint64_t res = 0;
        for (size_t i = 0, j = 0; j < offset && i < num_unary_blocks; ++i) {
            uint64_t block = unary_data[i];
            uint8_t unconsumed_bits = 64;
            while (j < offset && block != 0) {
                uint64_t num_zeros = details::bits64::count_trailing_zero(block);
                uint8_t w = num_zeros + prv_zeros;
                prv_zeros = 0;
                block >>= num_zeros + 1;
                unconsumed_bits -= num_zeros + 1;
                uint64_t body = body_reader.read_bits(w);
                uint64_t gap = details::bits64::set_bit(body, w);
                res += gap;
                ++j;
            }
            prv_zeros = unconsumed_bits;
        }
        *res_out = res;
    }

    static void index_of_lower_bound(size_t target, const uint8_t *data, uint8_t ext_meta, size_t *res_out) {
        using details::bits64::bit_view;

        auto unary_data = reinterpret_cast<uint64_t *>(const_cast<uint8_t *>(data));
        const size_t num_unary_blocks = static_cast<size_t>(ext_meta) + 1;

        auto body_view = bit_view(
            reinterpret_cast<uint64_t *>(const_cast<uint8_t *>(data)) + num_unary_blocks, bit_view::dont_care_size);
        auto body_reader = body_view.new_reader();
        
        uint16_t prv_zeros = 0;
        uint64_t val = 0;
        for (size_t i = 0, j = 0; j < details::DEFAULT_HYBRID_PARTITION_SIZE - 1 && i < num_unary_blocks; ++i) {
            uint64_t block = unary_data[i];
            uint8_t unconsumed_bits = 64;
            while (block != 0) {
                uint64_t num_zeros = details::bits64::count_trailing_zero(block);;
                uint8_t w = num_zeros + prv_zeros;
                prv_zeros = 0;
                block >>= num_zeros + 1;
                unconsumed_bits -= num_zeros + 1;
                uint64_t body = body_reader.read_bits(w);
                uint64_t gap = details::bits64::set_bit(body, w);
                val += gap;
                if (val >= target) {
                    *res_out = j + 1;
                    return;
                }
                ++j;
            }
            prv_zeros = unconsumed_bits;
        }
        *res_out = details::DEFAULT_HYBRID_PARTITION_SIZE;
    }
};

class linear {
public:
    template<typename RandomAccessIterT>
    struct encoder_type : hybrid_method_encoder {
        encoder_type(RandomAccessIterT first, size_t n) {
            uint64_t u = first[n - 1] - first[0];
            usable_ = u == (n - 1);
        }

        _YAEF_ATTR_NODISCARD size_t estimated_bits() const override {
            return usable_ ? 0 : std::numeric_limits<size_t>::max();
        }

        _YAEF_ATTR_NODISCARD size_t required_bits() const override {
            return usable_ ? 0 : std::numeric_limits<size_t>::max();
        }

        void encode(uint8_t *buf_out, uint8_t &ext_meta) const override {
            _YAEF_UNUSED(buf_out);
            _YAEF_UNUSED(ext_meta);
        }
    
    private:
        bool usable_ = false;
    };

public:
    template<typename RandomAccessIterT>
    static std::unique_ptr<encoder_type<RandomAccessIterT>>
    new_encoder(RandomAccessIterT first, size_t n) {
        return std::unique_ptr<encoder_type<RandomAccessIterT>>(
            new encoder_type<RandomAccessIterT>(first, n)
        );
    }

    static void name(std::string *out) { *out = "linear"; }

    static void at(size_t offset, const uint8_t *data, uint8_t ext_meta, uint64_t *res_out) {
        _YAEF_UNUSED(data);
        _YAEF_UNUSED(ext_meta);
        *res_out = offset;
    }

    static void index_of_lower_bound(size_t target, const uint8_t *data, uint8_t ext_meta, size_t *res_out) {
        _YAEF_UNUSED(data);
        _YAEF_UNUSED(ext_meta);
        *res_out = std::min<size_t>(details::DEFAULT_HYBRID_PARTITION_SIZE, target);
    }
};

class bitmap {
public:
    template<typename RandomAccessIterT>
    struct encoder_type : hybrid_method_encoder {
        encoder_type(RandomAccessIterT first, size_t n)
            : first_(first), num_(n) {
            for (size_t i = 1; i < n; ++i) {
                if (first[i] == first[i - 1]) {
                    requierd_bits_ = std::numeric_limits<size_t>::max();
                    return;
                }
            }
            const uint64_t u = first[n - 1];
            if (u + 1 >= details::DEFAULT_HYBRID_PARTITION_SIZE * (sizeof(uint64_t) * CHAR_BIT)) {
                requierd_bits_ = std::numeric_limits<size_t>::max();
            } else {
                requierd_bits_ = details::bits64::align_to<64>(u + 1);
            }
        }

        _YAEF_ATTR_NODISCARD size_t estimated_bits() const override {
            return requierd_bits_;
        }

        _YAEF_ATTR_NODISCARD size_t required_bits() const override {
            return requierd_bits_;
        }

        void encode(uint8_t *buf_out, uint8_t &ext_meta) const override {
            using details::bits64::bit_view;
            bit_view bv(reinterpret_cast<uint64_t *>(buf_out), bit_view::dont_care_size);
            bv.clear_all_bits(0, requierd_bits_);
            for (size_t i = 0; i < num_; ++i) {
                bv.set_bit(first_[i]);
            }
            ext_meta = details::bits64::idiv_ceil(first_[num_ - 1] + 1, 64) - 1;
        }
    
    private:
        RandomAccessIterT first_;
        size_t            num_;
        size_t            requierd_bits_;
    };

public:
    template<typename RandomAccessIterT>
    static std::unique_ptr<encoder_type<RandomAccessIterT>>
    new_encoder(RandomAccessIterT first, size_t n) {
        return std::unique_ptr<encoder_type<RandomAccessIterT>>(
            new encoder_type<RandomAccessIterT>(first, n)
        );
    }

    static void name(std::string *out) { *out = "bitmap"; }

    static void at(size_t offset, const uint8_t *data, uint8_t ext_meta, uint64_t *res_out) {
        const uint64_t *data64 = reinterpret_cast<const uint64_t *>(data);
        *res_out = details::bits64::select_one_blocks(data64, static_cast<size_t>(ext_meta) + 1, offset);
    }

    static void index_of_lower_bound(size_t target, const uint8_t *data, uint8_t ext_meta, size_t *res_out) { 
        constexpr size_t BLOCK_WIDTH = sizeof(uint64_t) * CHAR_BIT;
        const uint64_t *data64 = reinterpret_cast<const uint64_t *>(data);
        if (target / BLOCK_WIDTH > static_cast<size_t>(ext_meta) + 1) {
            *res_out = details::DEFAULT_HYBRID_PARTITION_SIZE;
            return;
        }
        *res_out = details::bits64::popcount_blocks(data64, static_cast<size_t>(ext_meta) + 1, target);
    }
};

class eliasfano {
public:
    template<typename RandomAccessIterT>
    struct encoder_type : hybrid_method_encoder {
        encoder_type(RandomAccessIterT first, size_t n)
            : first_(first), num_(n) {
            const uint64_t u = first[n - 1];
            lower_width_ = std::max<uint32_t>(1, details::bits64::bit_width(u / n));
            required_lower_bits_ = lower_width_ * details::DEFAULT_HYBRID_PARTITION_SIZE;
            required_upper_bits_ = n + (u >> lower_width_) + 1;
        }

        _YAEF_ATTR_NODISCARD size_t estimated_bits() const override {
            return lower_width_ * num_ + required_upper_bits_;
        }

        _YAEF_ATTR_NODISCARD size_t required_bits() const override {
            return required_lower_bits_ + required_upper_bits_;
        }

        void encode(uint8_t *buf_out, uint8_t &ext_meta) const override {
            using details::bits64::bit_view;

            ext_meta = static_cast<uint8_t>(lower_width_);
            
            bit_view lower_view(reinterpret_cast<uint64_t *>(buf_out), bit_view::dont_care_size);
            for (size_t i = 0, offset = 0; i < num_; ++i, offset += lower_width_) {
                uint64_t val = first_[i];
                lower_view.set_bits(offset, lower_width_, val);
            }

            const size_t lower_bytes = required_lower_bits_ / CHAR_BIT;
            bit_view upper_view(reinterpret_cast<uint64_t *>(buf_out + lower_bytes), bit_view::dont_care_size);
            upper_view.set_all_bits(0, required_upper_bits_);
            
            const size_t num_buckets = first_[num_ - 1] >> lower_width_;
            auto iter = first_;
            auto end_iter = first_ + num_;
            for (size_t i = 0, zero_index = 0; i <= num_buckets; ++i, ++zero_index) {
                while (iter < end_iter && ((*iter) >> lower_width_) == i) {
                    ++iter;
                    ++zero_index;
                }
                upper_view.clear_bit(zero_index);
            }
        }
    
    private:
        RandomAccessIterT first_;
        size_t            num_;
        size_t            required_lower_bits_;
        size_t            required_upper_bits_;
        uint32_t          lower_width_;
    };

public:
    static constexpr size_t MAX_NUM_BLOCKS = 
        details::DEFAULT_HYBRID_PARTITION_SIZE * 2 / (sizeof(uint64_t) * CHAR_BIT);

    template<typename RandomAccessIterT>
    static std::unique_ptr<encoder_type<RandomAccessIterT>>
    new_encoder(RandomAccessIterT first, size_t n) {
        return std::unique_ptr<encoder_type<RandomAccessIterT>>(
            new encoder_type<RandomAccessIterT>(first, n)
        );
    }

    static void name(std::string *out) { *out = "eliasfano"; }

    static void at(size_t offset, const uint8_t *data, uint8_t ext_meta, uint64_t *res_out) {
        using details::bits64::bit_view;

        uint32_t lower_width = ext_meta;
        auto lower_view = bit_view(reinterpret_cast<uint64_t *>(const_cast<uint8_t *>(data)), bit_view::dont_care_size);
        uint64_t lo = lower_view.get_bits(offset * lower_width, lower_width);
        
        const uint64_t *upper_addr = reinterpret_cast<const uint64_t *>(
            data + details::DEFAULT_HYBRID_PARTITION_SIZE * lower_width / CHAR_BIT);
        uint64_t hi = details::bits64::select_one_blocks(upper_addr, MAX_NUM_BLOCKS, offset) - offset;
        *res_out = (hi << lower_width) | lo;
    }

    static void index_of_lower_bound(size_t target, const uint8_t *data, uint8_t ext_meta, size_t *res_out) {
        using details::bits64::bit_view;

        const uint32_t lower_width = ext_meta;
        auto lower_view = bit_view(reinterpret_cast<uint64_t *>(const_cast<uint8_t *>(data)), bit_view::dont_care_size);

        uint64_t hi = target >> lower_width,
                 lo = target & (static_cast<uint64_t>(1 << lower_width) - 1);
        
        if (_YAEF_UNLIKELY(hi >= details::DEFAULT_HYBRID_PARTITION_SIZE)) {
            *res_out = details::DEFAULT_HYBRID_PARTITION_SIZE;
            return;
        }

        const uint64_t *upper_addr = reinterpret_cast<const uint64_t *>(
            data + details::DEFAULT_HYBRID_PARTITION_SIZE * lower_width / CHAR_BIT);

        size_t start = details::bits64::select_zero_blocks(upper_addr, MAX_NUM_BLOCKS, hi - 1) - hi + 1;
        size_t end = details::bits64::select_zero_blocks(upper_addr, MAX_NUM_BLOCKS, hi) - hi;
        start = std::min(start, details::DEFAULT_HYBRID_PARTITION_SIZE);
        end = std::min(end, details::DEFAULT_HYBRID_PARTITION_SIZE);
        size_t len = end - start;

        size_t base = start;
        while (len > 0) {
            size_t half = len / 2;
            uint64_t val = lower_view.get_bits((base + half) * lower_width, lower_width);
            base += static_cast<size_t>(val < lo) * (len - half);
            len = half;
        }
        *res_out = base;
    }
};

}

template<typename T, typename AllocT = details::aligned_allocator<uint8_t, 64>, 
         typename ...Methods>
class hybrid_list {
    static constexpr uint32_t METHOD_WIDTH = details::bits64::constexpr_bit_width<sizeof...(Methods)>::value;
    static constexpr uint64_t METHOD_MASK = (static_cast<uint64_t>(1) << METHOD_WIDTH) - 1;
    static constexpr size_t   PARTITION_DESC_BYTES = 6;
    static constexpr uint64_t PARTITION_DESC_MASK = 
        (static_cast<uint64_t>(1) << (PARTITION_DESC_BYTES * CHAR_BIT)) - 1;
    static constexpr uint64_t PARTITION_DESC_OFFSET_MASK =
        (static_cast<uint64_t>(1) << (32 - METHOD_WIDTH)) - 1;

    using unsigned_value_type = uint64_t;
    using method_types        = details::hybrid_method_type_list<Methods...>;
    using alloc_traits        = std::allocator_traits<AllocT>;
public:
    using value_type       = T;
    using size_type        = size_t;
    using difference_type  = ptrdiff_t;
    using const_reference  = const value_type &;
    using reference        = const_reference;
    using const_pointer    = const value_type *;
    using pointer          = const_pointer;
    using allocator_type   = AllocT;
    using sample_list      = sparse_sampled_list<value_type, sample_strategy::universe>;

    static constexpr size_t NUM_METHODS    = sizeof...(Methods);
    static constexpr size_t PARTITION_SIZE = details::DEFAULT_HYBRID_PARTITION_SIZE;

public:
    hybrid_list()
        : meta_with_alloc_(meta_data(), allocator_type{}) { }

    hybrid_list(const hybrid_list &other)
        : hybrid_list() {
        get_meta() = other.get_meta();
        partition_samples_ = other.partition_samples_;
        
        const size_type num_partitions = details::bits64::idiv_ceil(size(), PARTITION_SIZE);
        partition_descs_ = alloc_traits::allocate(get_alloc(), 
            details::bits64::align_to<8>(PARTITION_DESC_BYTES * num_partitions));
        std::uninitialized_copy_n(other.partition_descs_, PARTITION_DESC_BYTES * num_partitions, partition_descs_);
        data_ = alloc_traits::allocate(get_alloc(), get_data_bytes());
        std::uninitialized_copy_n(other.data_, get_data_bytes(), data_);
    }

    hybrid_list(hybrid_list &&other) noexcept
        : meta_with_alloc_(std::move(other.meta_with_alloc_)),
          partition_samples_(std::move(other.partition_samples_)),
          partition_descs_(details::exchange(other.partition_descs_, nullptr)),
          data_(details::exchange(other.data_, other.data_)) { }
    
    _YAEF_REQUIRES_RANDOM_ACCESS_ITER(RandomAccessIterT, SentIterT, std::is_integral)
    hybrid_list(RandomAccessIterT first, SentIterT last)
        : hybrid_list() {
        if (!details::is_sorted(first, last)) {
            throw std::invalid_argument("hybrid_list::hybrid_list: the input data is not sorted");
        }
        size_type num = details::iter_distance(first, last);
        unchecked_init(first, num);
    }

    _YAEF_REQUIRES_RANDOM_ACCESS_ITER(RandomAccessIterT, SentIterT, std::is_integral)
    hybrid_list(from_sorted_t, RandomAccessIterT first, SentIterT last)
        : hybrid_list() {
        size_type num = details::iter_distance(first, last);
        unchecked_init(first, num);
    }

    hybrid_list(std::initializer_list<value_type> initlist)
        : hybrid_list(initlist.begin(), initlist.end()) { }

    hybrid_list(from_sorted_t, std::initializer_list<value_type> initlist)
        : hybrid_list(from_sorted, initlist.begin(), initlist.end()) { }

    ~hybrid_list() {
        if (partition_descs_) {
            alloc_traits::deallocate(get_alloc(), partition_descs_, 
                PARTITION_DESC_BYTES * details::bits64::idiv_ceil(size(), PARTITION_SIZE));
            partition_descs_ = nullptr;
        }
        if (data_) {
            alloc_traits::deallocate(get_alloc(), data_, get_data_bytes());
            data_ = nullptr;
        }
    }

    hybrid_list &operator=(const hybrid_list &other) {
        hybrid_list cpy(other);
        swap(cpy);
        return *this;
    }

    hybrid_list &operator=(hybrid_list &&other) noexcept {
        meta_with_alloc_.value() = other.meta_with_alloc_.value();
        partition_samples_ = std::move(other.partition_samples_);
        partition_descs_ = details::exchange(other.partition_descs_, nullptr);
        data_ = details::exchange(other.data_, nullptr);
        return *this;
    }

    _YAEF_ATTR_NODISCARD size_type size() const noexcept { return get_meta().size; }
    _YAEF_ATTR_NODISCARD bool empty() const noexcept { return size() == 0; }

    _YAEF_ATTR_NODISCARD size_type space_usage_in_bytes() const noexcept {
        const size_type num_partitions = details::bits64::idiv_ceil(size(), PARTITION_SIZE);
        return partition_samples_.space_usage_in_bytes() +
               PARTITION_DESC_BYTES * num_partitions +
               get_meta().data_bytes;
    }

    _YAEF_ATTR_NODISCARD std::vector<hybrid_method_stat_entry> method_stats() const {
        std::vector<hybrid_method_stat_entry> res(NUM_METHODS);
        for (size_type i = 0; i < NUM_METHODS; ++i) {
            res[i].id = i;
            res[i].name = method_types::dispatch_name(i);
        }
        
        const size_type num_partitions = details::bits64::idiv_ceil(size(), PARTITION_SIZE);
        for (size_type i = 0; i < num_partitions - 1; ++i) {
            const uint64_t partition_desc = *reinterpret_cast<const uint64_t *>(
                partition_descs_ + PARTITION_DESC_BYTES * i) & PARTITION_DESC_MASK;
            const uint64_t next_partition_desc = *reinterpret_cast<const uint64_t *>(
                partition_descs_ + PARTITION_DESC_BYTES * (i + 1)) & PARTITION_DESC_MASK;

            const size_type method_idx = partition_desc & METHOD_MASK;
            const size_type buf_offset = (partition_desc >> METHOD_WIDTH) & PARTITION_DESC_OFFSET_MASK;
            const size_type next_buf_offset = (next_partition_desc >> METHOD_WIDTH) & PARTITION_DESC_OFFSET_MASK;

            const size_type buf_size = (next_buf_offset - buf_offset) * 8;
            res[method_idx].encoded_elements += PARTITION_SIZE;
            res[method_idx].num_partitions++;
            res[method_idx].space_usage_in_bytes += buf_size;
        }

        {
            const uint64_t last_partition_desc = *reinterpret_cast<const uint64_t *>(
                partition_descs_ + PARTITION_DESC_BYTES * (num_partitions - 1)) & PARTITION_DESC_MASK;
            const size_type method_idx = last_partition_desc & METHOD_MASK;
            const size_type buf_offset = (last_partition_desc >> METHOD_WIDTH) & PARTITION_DESC_OFFSET_MASK;
            const size_type buf_size = (get_meta().data_bytes - buf_offset * 8);
            res[method_idx].encoded_elements += size() - (num_partitions - 1) * PARTITION_SIZE;
            res[method_idx].num_partitions++;
            res[method_idx].space_usage_in_bytes += buf_size;
        }

        const size_type total_space_usage = space_usage_in_bytes();
        for (size_type i = 0; i < NUM_METHODS; ++i) {
            res[i].encoding_ratio = static_cast<double>(res[i].encoded_elements) /
                                    static_cast<double>(size());
            res[i].space_usage_ratio = static_cast<double>(res[i].space_usage_in_bytes) /
                                       static_cast<double>(total_space_usage);
        }
        return res;
    }

    _YAEF_ATTR_NODISCARD value_type at(size_type index) const {
        _YAEF_ASSERT(index < size());

        const size_type partition_index = index / PARTITION_SIZE,
                        partition_offset = index % PARTITION_SIZE;
        value_type sample = partition_samples_.at(partition_index);
        if (_YAEF_UNLIKELY(partition_offset == 0)) {
            return to_actual_value(sample);
        }

        const uint64_t partition_desc = *reinterpret_cast<const uint64_t *>(
            partition_descs_ + PARTITION_DESC_BYTES * partition_index) & PARTITION_DESC_MASK;

        const size_type method_idx = partition_desc & METHOD_MASK;
        const size_type buf_offset = (partition_desc >> METHOD_WIDTH) & PARTITION_DESC_OFFSET_MASK;
        const uint8_t ext_meta = partition_desc >> 40;
        const uint8_t *buf = data_ + (buf_offset * 8);

        uint64_t inner_val = method_types::dispatch_at(method_idx, partition_offset, buf, ext_meta);
        return to_actual_value(sample + inner_val);
    }

    _YAEF_ATTR_NODISCARD value_type operator[](size_type index) const {
        return at(index);
    }
    
    _YAEF_ATTR_NODISCARD value_type min() const {
        _YAEF_ASSERT(!empty());
        return get_meta().min;
    }

    _YAEF_ATTR_NODISCARD value_type max() const {
        _YAEF_ASSERT(!empty());
        return get_meta().max;
    }

    _YAEF_ATTR_NODISCARD value_type front() const { return min(); }
    _YAEF_ATTR_NODISCARD value_type back() const { return max(); }

    _YAEF_ATTR_NODISCARD size_type index_of_lower_bound(value_type target) const {
        if (_YAEF_UNLIKELY(target <= min())) { return 0; }
        if (_YAEF_UNLIKELY(target > max())) { return size(); }

        unsigned_value_type t = to_stored_value(target);
        
        auto sample_iter = partition_samples_.upper_bound(t) - 1;
        unsigned_value_type sample = *sample_iter;
        size_type partition_index = std::distance(partition_samples_.begin(), sample_iter);
        
        const uint64_t partition_desc = *reinterpret_cast<const uint64_t *>(
            partition_descs_ + PARTITION_DESC_BYTES * partition_index) & PARTITION_DESC_MASK;
        const size_type method_idx = partition_desc & METHOD_MASK;
        const size_type buf_offset = ((partition_desc >> METHOD_WIDTH) & PARTITION_DESC_OFFSET_MASK) * 8;
        const uint8_t ext_meta = partition_desc >> 40;
        const uint8_t *buf = data_ + buf_offset;

        t -= sample;
        if (_YAEF_UNLIKELY(t == 0)) {
            return std::min(partition_index * PARTITION_SIZE, size() - 1);
        }

        uint64_t idx = method_types::dispatch_index_of_lower_bound(method_idx, t, buf, ext_meta);
        return partition_index * PARTITION_SIZE + idx;
    }

    void swap(hybrid_list &other) noexcept {
        get_meta().swap(other.get_meta());
        details::checked_swap_alloc(get_alloc(), other.get_alloc());
        partition_samples_.swap(other.partition_samples_);
        std::swap(partition_descs_, other.partition_descs_);
        std::swap(data_, other.data_);
    }

private:
    struct meta_data {
        value_type min  = std::numeric_limits<value_type>::max();
        value_type max  = std::numeric_limits<value_type>::min();
        size_type  size = 0;
        size_type  data_bytes = 0;
        bool       has_duplicates = false;

        meta_data() = default;

        meta_data(const meta_data &other)
            : min(other.min), max(other.max), size(other.size),
              data_bytes(other.data_bytes), has_duplicates(other.has_duplicates) { }

        meta_data &operator=(const meta_data &other) {
            min = other.min;
            max = other.max;
            size = other.size;
            data_bytes = other.data_bytes;
            has_duplicates = other.has_duplicates;
            return *this;
        }

        error_code serialize(details::serializer &ser) const {
            if (!ser.write(min)) { return error_code::serialize_io; }
            if (!ser.write(max)) { return error_code::serialize_io; }
            if (!ser.write(size)) { return error_code::serialize_io; }
            if (!ser.write(data_bytes)) { return error_code::serialize_io; }
            if (!ser.write(has_duplicates)) { return error_code::serialize_io; }
            return error_code::success;
        }
        
        error_code deserialize(details::deserializer &deser) {
            if (!deser.read(min)) { return error_code::deserialize_io; }
            if (!deser.read(max)) { return error_code::deserialize_io; }
            if (!deser.read(size)) { return error_code::deserialize_io; }
            if (!deser.read(data_bytes)) { return error_code::deserialize_io; }
            if (!deser.read(has_duplicates)) { return error_code::deserialize_io; }
            return error_code::success;
        }

        void swap(meta_data &other) {
            std::swap(min, other.min);
            std::swap(max, other.max);
            std::swap(size, other.size);
            std::swap(data_bytes, other.data_bytes);
            std::swap(has_duplicates, other.has_duplicates);
        }
    };

    using meta_with_alloc = details::value_with_allocator_pair<meta_data, allocator_type>;

    meta_with_alloc    meta_with_alloc_;
    sample_list        partition_samples_;
    uint8_t           *partition_descs_ = nullptr;
    uint8_t           *data_            = nullptr;

    _YAEF_ATTR_NODISCARD const meta_data &get_meta() const { return meta_with_alloc_.value(); }
    _YAEF_ATTR_NODISCARD meta_data &get_meta() { return meta_with_alloc_.value(); }
    _YAEF_ATTR_NODISCARD allocator_type &get_alloc() { return meta_with_alloc_.alloc(); }
    _YAEF_ATTR_NODISCARD const allocator_type &get_alloc() const { return meta_with_alloc_.alloc(); }

    _YAEF_ATTR_NODISCARD size_type get_data_bytes() const noexcept {
        return details::bits64::align_to<32>(get_meta().data_bytes);
    }

    _YAEF_ATTR_NODISCARD value_type to_actual_value(unsigned_value_type v) const noexcept {
        return static_cast<value_type>(v + min());
    }

    _YAEF_ATTR_NODISCARD unsigned_value_type to_stored_value(value_type v) const noexcept {
        return static_cast<unsigned_value_type>(v) - static_cast<unsigned_value_type>(min());
    }

    error_code do_serialize(details::serializer &ser) const {
        error_code ec = get_meta().serialize(ser);
        if (ec != error_code::success) { return ec; }
        ec = details::serialize_friend_access::serialize(partition_samples_, ser);
        if (ec != error_code::success) { return ec; }
        const size_type num_partitions = details::bits64::idiv_ceil(size(), PARTITION_SIZE);
        if (!ser.write_bytes(partition_descs_, PARTITION_DESC_BYTES * num_partitions)) {
            return error_code::serialize_io;
        }
        if (!ser.write_bytes(data_, get_meta().data_bytes)) {
            return error_code::serialize_io;
        }
        return error_code::success;
    }

    error_code do_deserialize(details::deserializer &deser) {
        error_code ec = get_meta().deserialize(deser);
        if (ec != error_code::success) { return ec; }
        ec = details::serialize_friend_access::deserialize(partition_samples_, deser);
        if (ec != error_code::success) { return ec; }
        const size_type num_partitions = details::bits64::idiv_ceil(size(), PARTITION_SIZE);
        partition_descs_ = alloc_traits::allocate(get_alloc(), 
            details::bits64::align_to<8>(PARTITION_DESC_BYTES * num_partitions));
        if (!deser.read_bytes(partition_descs_, PARTITION_DESC_BYTES * num_partitions)) {
            return error_code::deserialize_io;
        }
        data_ = alloc_traits::allocate(get_alloc(), get_data_bytes());
        if (!deser.read_bytes(data_, get_meta().data_bytes)) {
            return error_code::deserialize_io;
        }
        return error_code::success;
    }

    template<typename RandomAccessIterT>
    void unchecked_init(RandomAccessIterT first, size_type n) {
        using iter_adaptor = details::hybrid_encoding_iterator_adaptor<RandomAccessIterT>;

        if (_YAEF_UNLIKELY(n == 0)) {
            return;
        }

        meta_data &meta = get_meta();
        meta.min = first[0];
        meta.max = first[n - 1];
        meta.size = n;

        for (size_type i = 1; i < n; ++i) {
            if (first[i] == first[i - 1]) {
                meta.has_duplicates = true;
                break;
            }
        }

        const size_type num_partitions = details::bits64::idiv_ceil(n, PARTITION_SIZE);

        std::vector<unsigned_value_type> samples(num_partitions + 1);
        for (size_type i = 0; i < n; i += PARTITION_SIZE) {
            samples[i / PARTITION_SIZE] = to_stored_value(first[i]);
        }
        samples[num_partitions] = to_stored_value(max());
        partition_samples_ = sample_list(from_sorted, samples.begin(), samples.end());

        const size_type partition_desc_space_bytes = 
            details::bits64::align_to<8>(PARTITION_DESC_BYTES * num_partitions);
        partition_descs_ = alloc_traits::allocate(get_alloc(), partition_desc_space_bytes);
        std::uninitialized_fill_n(partition_descs_, partition_desc_space_bytes, 0);

        std::vector<std::unique_ptr<hybrid_method_encoder>> encoders(num_partitions);
        using method_selector = details::hybrid_method_selector<typename method_types::tuple_type, NUM_METHODS>;

        size_type code_offset = 0;
        size_type required_bits = 0;
        for (size_type i = 0; i < num_partitions; ++i) {
            const unsigned_value_type sample = samples[i];
            const size_type partition_size = std::min<size_type>(n - i * PARTITION_SIZE, (size_type) PARTITION_SIZE);

            iter_adaptor iter(first + i * PARTITION_SIZE, min(), sample);
            auto select_res = method_selector::select(iter, partition_size);
            encoders[i] = std::move(std::get<0>(select_res));
            size_type method_idx = std::get<1>(select_res);
            size_type encode_bits = details::bits64::align_to<64>(encoders[i]->required_bits());
            
            uint64_t partition_desc = static_cast<uint64_t>(method_idx) | (code_offset << METHOD_WIDTH);
            (*reinterpret_cast<uint64_t *>(partition_descs_ + PARTITION_DESC_BYTES * i)) |= partition_desc;

            code_offset += encode_bits / 64;
            required_bits += encode_bits;    
        }

        size_type required_bytes = details::bits64::idiv_ceil(required_bits, CHAR_BIT);
        meta.data_bytes = required_bytes;
        data_ = alloc_traits::allocate(get_alloc(), get_data_bytes());

        for (size_type i = 0; i < num_partitions; ++i) {
            const uint64_t partition_desc = 
                *reinterpret_cast<uint64_t *>(partition_descs_ + PARTITION_DESC_BYTES * i) & PARTITION_DESC_MASK;
            const size_type byte_offset = ((partition_desc >> METHOD_WIDTH) & PARTITION_DESC_OFFSET_MASK) * 8;
            uint8_t *buf = data_ + byte_offset;
            uint8_t *ext_meta_byte_ptr = partition_descs_ + PARTITION_DESC_BYTES * i + 5;
            encoders[i]->encode(buf, *ext_meta_byte_ptr);
        }
    }
};

template<typename T>
inline error_code serialize_to_buf(const T &x, uint8_t *buf, size_t buf_size) {
    auto ser = details::serializer{details::make_unique_obj<details::membuf_writer_context>(buf, buf_size)};
    return details::serialize_friend_access::serialize(x, ser);
}

template<typename T>
inline error_code serialize_to_file(const T &x, FILE *file) {
    auto ser = details::serializer{details::make_unique_obj<details::cfile_writer_context>(file)};
    return details::serialize_friend_access::serialize(x, ser);
}

template<typename T>
inline error_code serialize_to_file(const T &x, const char *path, bool overwrite) {
    FILE *file = nullptr;
    if (overwrite) {
        file = ::fopen(path, "wb");
    } else {
        file = ::fopen(path, "ab");
    }
    if (file == nullptr) {
        return error_code::serialize_io;
    }
    error_code ec = serialize_to_file(x, file);
    ::fclose(file);
    return ec;
}

template<typename T>
inline error_code serialize_to_file(const T &x, const std::string &path, bool overwrite) {
    return serialize_to_file(x, path.c_str(), overwrite);
}

template<typename T>
inline error_code serialize_to_stream(const T &x, std::ostream &stream) {
    auto ser = details::serializer{details::make_unique_obj<details::ostream_writer_context>(stream)};
    return details::serialize_friend_access::serialize(x, ser);
}

#if _YAEF_USE_STL_SPAN
template<typename T>
inline error_code serialize_to_buf(const T &x, std::span<uint8_t> buf) {
    return serialize_to_buf(x, buf.data(), buf.size());
}
#endif

#if _YAEF_USE_STL_FILESYSTEM
template<typename T>
inline error_code serialize_to_file(const T &x, const std::filesystem::path &path, bool overwrite) {
    auto open_flags = std::ios::out | std::ios::binary;
    if (!overwrite) {
        open_flags |= std::ios::app;
    }
    std::ofstream out(path, open_flags);
    if (out.fail()) {
        return error_code::serialize_io;
    }
    return serialize_to_stream(x, out);
}
#endif

template<typename T>
inline error_code deserialize_from_buf(T &x, const uint8_t *buf, size_t buf_size) {
    auto deser = details::deserializer{details::make_unique_obj<details::membuf_reader_context>(buf, buf_size)};
    return details::serialize_friend_access::deserialize(x, deser);
}

template<typename T>
inline error_code deserialize_from_file(T &x, FILE *file) {
    auto deser = details::deserializer{details::make_unique_obj<details::cfile_reader_context>(file)};
    return details::serialize_friend_access::deserialize(x, deser);
}

template<typename T>
inline error_code deserialize_from_file(T &x, const char *path) {
    FILE *file = ::fopen(path, "rb");
    if (file == nullptr) {
        return error_code::deserialize_io;
    }
    error_code ec = deserialize_from_file(x, file);
    ::fclose(file);
    return ec;
}

template<typename T>
inline error_code deserialize_from_stream(T &x, std::istream &stream) {
    auto deser = details::deserializer{details::make_unique_obj<details::istream_reader_context>(stream)};
    return details::serialize_friend_access::deserialize(x, deser);
}

#if _YAEF_USE_STL_SPAN
template<typename T>
inline error_code deserialize_from_buf(const T &x, std::span<const uint8_t> buf) {
    return deserialize_from_buf(x, buf.data(), buf.size());
}
#endif

#if _YAEF_USE_STL_FILESYSTEM
template<typename T>
inline error_code deserialize_from_file(const T &x, const std::filesystem::path &path) {
    auto open_flags = std::ios::in | std::ios::binary;
    std::ifstream in(path, open_flags);
    if (in.fail()) {
        return error_code::deserialize_io;
    }
    return deserialize_from_stream(x, in);
}
#endif

// not available now
#if 0
struct encode_result {
    size_t      num_low_bits;
    size_t      num_high_bits;
    uint64_t   *low_bits_ptr;
    uint64_t   *high_bits_ptr;
    error_code  ec;

    _YAEF_ATTR_NODISCARD explicit operator bool() const noexcept {
        return ec == error_code::success;
    }
};

namespace details {

_YAEF_ATTR_NODISCARD inline encode_result 
make_encode_err(uint64_t *low_ptr, uint64_t *high_ptr, error_code ec) noexcept {
    encode_result res;
    res.num_high_bits = 0;
    res.num_high_bits = 0;
    res.low_bits_ptr = low_ptr;
    res.high_bits_ptr = high_ptr;
    res.ec = ec;
    return res;
}

}

#if _YAEF_USE_CXX_CONCEPTS
template<std::random_access_iterator RandomAccessIterT>
#else
template<typename RandomAccessIterT, 
         typename = typename std::enable_if<details::is_random_access_iter<RandomAccessIterT>::value>::type>
#endif
_YAEF_ATTR_NODISCARD inline encode_result 
encode_eliasfano(RandomAccessIterT first, RandomAccessIterT last, uint32_t low_width,
                 uint64_t *high_bits_buf, size_t high_bits_buf_size, 
                 uint64_t *low_bits_buf, size_t low_bits_buf_size) {
    if (_YAEF_UNLIKELY(first > last)) {
        return details::make_encode_err(low_bits_buf, high_bits_buf, error_code::invalid_argument);
    }
    if (_YAEF_UNLIKELY(!details::is_sorted(first, last))) {
        return details::make_encode_err(low_bits_buf, high_bits_buf, error_code::not_sorted);
    }
    if (_YAEF_UNLIKELY(low_width > 64)) {
        return details::make_encode_err(low_bits_buf, high_bits_buf, error_code::invalid_argument);
    }

    auto min = *first;
    uint64_t max = *std::prev(last) - min;
    size_t num = static_cast<size_t>(std::distance(first, last));

    encode_result res;
    res.num_low_bits = num * low_width;
    res.num_high_bits = num + (max >> low_width) + 1;

    const size_t num_low_blocks = details::bits64::idiv_ceil(res.num_low_bits, sizeof(uint64_t) * CHAR_BIT);
    const size_t num_high_blocks = details::bits64::idiv_ceil(res.num_high_bits, sizeof(uint64_t) * CHAR_BIT);

    if (num_low_blocks > low_bits_buf_size || num_high_blocks > high_bits_buf_size)
        return details::make_encode_err(low_bits_buf, high_bits_buf, error_code::buf_too_small);
    
    res.low_bits_ptr = low_bits_buf + num_low_blocks;
    res.high_bits_ptr = high_bits_buf + num_high_blocks;

    details::bits64::packed_int_view low_bits{low_width, low_bits_buf, num_low_blocks};
    details::bits64::bit_view high_bits{high_bits_buf, num_high_blocks};

    low_bits.clear_all_bits();
    for (size_t i = 0; i < num; ++i) {
        low_bits.set_value(i, first[i] - min);
    }
    
    high_bits.set_all_bits();
    high_bits.clear_bit(0);
    for (size_t i = 0, zero_index = 1; i < num_high_blocks; ++i, ++zero_index) {
        while ((static_cast<uint64_t>(first[i] - min) >> low_width) == i) {
            ++first;
            ++zero_index;
        }
        high_bits.clear_bit(zero_index);
    }

    res.ec = error_code::success;
    return res;
}

#if _YAEF_USE_CXX_CONCEPTS
template<std::random_access_iterator RandomAccessIterT>
#else
template<typename RandomAccessIterT, 
         typename = typename std::enable_if<details::is_random_access_iter<RandomAccessIterT>::value>::type>
#endif
_YAEF_ATTR_NODISCARD inline encode_result 
encode_eliasfano(RandomAccessIterT first, RandomAccessIterT last,
                 uint64_t *high_bits_buf, size_t high_bits_buf_size, 
                 uint64_t *low_bits_buf, size_t low_bits_buf_size) {
    auto min = *first;
    uint64_t max = *std::prev(last) - min;
    size_t num = static_cast<size_t>(std::distance(first, last));
    uint32_t low_width = details::bits64::bit_width(max / num);
    return encode_eliasfano(first, last, low_width, high_bits_buf, high_bits_buf_size,
                            low_bits_buf, low_bits_buf_size);
}

template<typename BidirIterT>
_YAEF_ATTR_NODISCARD inline size_t encoded_size_in_bytes(BidirIterT first, BidirIterT last) {
    using value_type = typename std::iterator_traits<BidirIterT>::value_type;
    value_type min = *first;
    value_type max = *std::prev(last);
    uint64_t u = static_cast<uint64_t>(max) - static_cast<uint64_t>(min);
    size_t n = static_cast<size_t>(std::distance(first, last));
    uint32_t low_width = std::max<uint32_t>(1, details::bits64::bit_width(u / n));
    
    size_t low_bits_size = details::bits64::idiv_ceil(low_width * n, CHAR_BIT);
    size_t high_bits_size = details::bits64::idiv_ceil(n + (u >> low_width) + 1, CHAR_BIT);
    return low_bits_size + high_bits_size;
}

template<typename BidirIterT>
_YAEF_ATTR_NODISCARD inline encode_result
encode(BidirIterT first, BidirIterT last, uint8_t *buf_out, size_t buf_size);

#if _YAEF_USE_STL_SPAN
template<std::random_access_iterator RandomAccessIterT>
_YAEF_ATTR_NODISCARD inline encode_result
encode_eliasfano(RandomAccessIterT first, RandomAccessIterT last, uint32_t low_width,
                 std::span<uint64_t> high_bits_buf, std::span<uint64_t> low_bits_buf) {
    return encode_eliasfano(first, last, low_width, high_bits_buf.data(), high_bits_buf.size(),
                            low_bits_buf.data(), low_bits_buf.size());
}

template<std::random_access_iterator RandomAccessIterT>
_YAEF_ATTR_NODISCARD inline encode_result
encode_eliasfano(RandomAccessIterT first, RandomAccessIterT last,
                 std::span<uint64_t> high_bits_buf, std::span<uint64_t> low_bits_buf) {
    return encode_eliasfano(first, last, high_bits_buf.data(), high_bits_buf.size(),
                            low_bits_buf.data(), low_bits_buf.size());
}
#endif

struct decode_result {
    error_code ec;
};

template<typename OutputIterT>
_YAEF_ATTR_NODISCARD inline decode_result 
decode_eliasfano(OutputIterT out_first, size_t num_elems, uint32_t low_width, 
                 const uint64_t *high_bits, size_t num_high_bits, 
                 const uint64_t *low_bits, size_t num_low_bits) {
    const size_t num_high_blocks = details::bits64::idiv_ceil(num_high_bits, sizeof(uint64_t) * CHAR_BIT);
    const size_t num_low_blocks = details::bits64::idiv_ceil(num_low_bits, sizeof(uint64_t) * CHAR_BIT);

    details::bits64::packed_int_view low_bits_view{low_width, const_cast<uint64_t *>(low_bits), num_low_blocks};

    size_t index = 0;
    details::bits64::bitmap_foreach_onebit(high_bits, num_high_blocks, [&](size_t pos) {
        uint64_t high = pos - index;
        uint64_t low = low_bits_view.get_value(index);
        uint64_t val = (high << low_width) | low;
        *out_first++ = val;
        ++index;
    });

    decode_result res;
    return res;
}
#endif

_YAEF_NAMESPACE_END

#endif