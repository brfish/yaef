// Copyright (c) 2024 brfish
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
#endif

#if __cplusplus >= 201703L
#   include <new> // for std::aligned_val_t
#endif

#if __cplusplus >= 201703L
#   include <filesystem>
#   if __cpp_lib_filesystem	>= 201703L
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

#if _YAEF_HAS_BUILTIN(__builtin_assume)
#   define _YAEF_ASSUME(_cond) __builtin_assume(_cond)
#elif defined(__GNUC__)
#   define _YAEF_ASSUME(_cond) __attribute__((assume(_cond)))
#elif defined(_MSC_VER)
#   define _YAEF_ASSUME(_cond) __assume(_cond)
#else
#   define _YAEF_ASSUME(_cond)
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
#   define _YAEF_ASSERT(_expr)
#else
#   define _YAEF_ASSERT(_expr) do { if (!(_expr)) { \
    ::yaef::details::raise_assertion(__FILE__, __LINE__, #_expr); _YAEF_DEBUGBREAK(); } } while (false)
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

#if __cpp_lib_void_t >= 201411L
template<typename ...Ts>
using void_t = std::void_t<Ts...>;
#else
template<typename ...>
using void_t = void;
#endif

#if __cpp_lib_remove_cvref >= 201711L
template<typename T>
using remove_cvref = std::remove_cvref<T>;
#else
template<typename T>
using remove_cvref = std::remove_cv<typename std::remove_reference<T>::type>;
#endif

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
    : public std::is_base_of<std::random_access_iterator_tag, 
                             typename std::iterator_traits<T>::iterator_category> { };
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
        for (size_type i = 0; i < size(); ++i)
            set_value(i, value);
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

    _YAEF_ATTR_NODISCARD value_type get_value(size_type index) const noexcept {
        _YAEF_ASSERT(index < size());
        const size_type bit_index = index * width();
        const size_type block_index = bit_index / BLOCK_WIDTH, 
                        block_offset = bit_index % BLOCK_WIDTH;

#if _YAEF_INTRINSICS_HAVE_AVX2
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

class bit_view {
public:
    using value_type = bool;
    using size_type  = size_t;
    using block_type = uint64_t;
    static constexpr uint32_t BLOCK_WIDTH = sizeof(block_type) * CHAR_BIT;

public:
    bit_view() noexcept
        : blocks_(nullptr), num_bits_(0) { }

    bit_view(const bit_view &) = default;

    bit_view(block_type *blocks, size_type num_bits) noexcept
        : blocks_(blocks), num_bits_(num_bits) { }

    bit_view &operator=(const bit_view &) = default;

    _YAEF_ATTR_NODISCARD size_type size() const noexcept { return num_bits_; }
    _YAEF_ATTR_NODISCARD bool empty() const noexcept { return size() == 0; }
    _YAEF_ATTR_NODISCARD block_type *blocks() const noexcept { return blocks_; }

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

    void clear_all_bits() {
        std::fill_n(blocks_, num_blocks(), 0);
    }

    void set_all_bits() {
        if (_YAEF_UNLIKELY(num_blocks() == 0)) { return; }
        std::fill_n(blocks_, num_blocks() - 1, std::numeric_limits<block_type>::max());
        blocks_[num_blocks() - 1] = make_mask_lsb1(size() - (num_blocks() - 1) * BLOCK_WIDTH);
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
inline size_t bitset_foreach_one(uint64_t block, F f, size_t index_offset = 0) {
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
inline size_t bitset_foreach_zero(uint64_t block, F f, size_t index_offset = 0) {
    return bitset_foreach_one(~block, f, index_offset);
}

template<bool BitType, typename F>
inline size_t large_bitset_foreach_impl(const uint64_t *blocks, size_t num_blocks, F f) {
    using block_handler = conditional_bitwise_not<!BitType>;
    size_t index_offset = 0;
    size_t popcnt = 0;
    for (size_t i = 0; i < num_blocks; ++i) {
        popcnt += bitset_foreach_one(block_handler{}(blocks[i]), f, index_offset);
        index_offset += sizeof(uint64_t) * CHAR_BIT;
    }
    return popcnt;
}

template<typename F>
inline size_t bitset_foreach_one(const uint64_t *blocks, size_t num_blocks, F f) {
    return large_bitset_foreach_impl<true>(blocks, num_blocks, f);
}

template<typename F>
inline size_t bitset_foreach_zero(const uint64_t *blocks, size_t num_blocks, F f) {
    return large_bitset_foreach_impl<false>(blocks, num_blocks, f);
}

template<bool BitType>
class bitset_foreach_cursor {
public:
    using size_type     = size_t;
    using block_type    = uint64_t;
    static constexpr bool BIT_TYPE = BitType;
    static constexpr uint32_t BLOCK_WIDTH = sizeof(block_type) * CHAR_BIT;

private:
    using block_handler = conditional_bitwise_not<!BitType>;

public:
    bitset_foreach_cursor() noexcept
        : blocks_beg_(nullptr), blocks_end_(nullptr), cur_block_(0), 
          skipped_blocks_(0), cached_(0) { }

    bitset_foreach_cursor(const uint64_t *blocks, size_type num_blocks) noexcept
        : blocks_beg_(blocks), blocks_end_(blocks + num_blocks), cur_block_(0), 
          skipped_blocks_(0), cached_(0) {
        _YAEF_ASSERT(num_blocks != 0);
        move_to_next_non_empty_block();
        update_cache();
    }

    bitset_foreach_cursor(const uint64_t *blocks, size_type num_blocks, size_type num_skipped_bits) noexcept
        : blocks_beg_(blocks), blocks_end_(blocks + num_blocks), cur_block_(0), 
          skipped_blocks_(0), cached_(0) {
        _YAEF_ASSERT(num_blocks != 0);
        _YAEF_ASSERT(idiv_ceil(num_skipped_bits, BLOCK_WIDTH) <= num_blocks);

        const size_type num_full_blocks = num_skipped_bits / BLOCK_WIDTH,
                        num_residual_bits = num_skipped_bits % BLOCK_WIDTH;
        
        skipped_blocks_ += num_full_blocks;

        cur_block_ = block_handler{}(*get_cur_block_ptr()) & (~bits64::make_mask_lsb1(num_residual_bits));
        if (cur_block_ == 0) {
            ++skipped_blocks_;
            move_to_next_non_empty_block();
        }
        update_cache();
    }

    bitset_foreach_cursor(const bit_view &bits) noexcept
        : bitset_foreach_cursor(bits.blocks(), bits.num_blocks()) { }
    
    bitset_foreach_cursor(const bit_view &bits, size_type num_skipped_bits) noexcept
        : bitset_foreach_cursor(bits.blocks(), bits.num_blocks(), num_skipped_bits) { }
    
    _YAEF_ATTR_NODISCARD size_type current() const noexcept {
        _YAEF_ASSERT(is_valid());
        return skipped_blocks_ * BLOCK_WIDTH + cached_;
    }

    _YAEF_ATTR_NODISCARD bool is_valid() const noexcept { return get_cur_block_ptr() != blocks_end_; }

    void next() {
        if (_YAEF_UNLIKELY(cur_block_ == 0)) {
            ++skipped_blocks_;
            move_to_next_non_empty_block();
        }
        update_cache();
    }

private:
    const block_type *blocks_beg_;
    const block_type *blocks_end_;
    block_type        cur_block_;
    uint64_t          skipped_blocks_;
    uint32_t          cached_;

    _YAEF_ATTR_NODISCARD const block_type *get_cur_block_ptr() const {
        return blocks_beg_ + skipped_blocks_;
    }

    void move_to_next_non_empty_block() {
        while (get_cur_block_ptr() != blocks_end_ && block_handler{}(*get_cur_block_ptr()) == 0) {
            ++skipped_blocks_;
        }
        cur_block_ = block_handler{}(*get_cur_block_ptr());
    }

    void update_cache() {
        cached_ = count_trailing_zero(cur_block_);
        block_type tmp = cur_block_ & -cur_block_;
        cur_block_ ^= tmp;
    }
};

using bitset_foreach_one_cursor  = bitset_foreach_cursor<true>;
using bitset_foreach_zero_cursor = bitset_foreach_cursor<false>;

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

template<typename RandomAccessIterT, typename SentIterT>
_YAEF_ATTR_NODISCARD static bool check_duplicate(RandomAccessIterT first, SentIterT last) {
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
                                  value_type min, value_type max, uint32_t low_width)
        : first_(first), last_(last), size_(num), 
          min_(min), max_(max), low_width_(low_width) {
        _YAEF_ASSERT(low_width_ > 0 && low_width_ <= 64);
    }
    
    eliasfano_encoder_scalar_impl(InputIterT first, InputIterT last, size_type num, 
                                  value_type min, value_type max)
        : first_(first), last_(last), size_(num), 
          min_(min), max_(max), low_width_(0) {
        const uint64_t u = to_stored_value(max_);
        low_width_ = std::max<uint32_t>(1, bits64::bit_width(u / size_));
    }

#if _YAEF_USE_CXX_CONCEPTS
    template<std::bidirectional_iterator IterT = InputIterT>
#else
    template<typename IterT = InputIterT, typename = typename std::enable_if<is_bidirectional_iter<IterT>::value>::type>
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

    template<typename U = T, typename AllocU = AllocT>
    value_with_allocator_pair(U &&v, AllocU &&a)
        : AllocT(std::forward<AllocU>(a)), value_(std::forward<U>(v)) { }

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
                    size_t num_samples = bits64::idiv_ceil_nzero(num_zeros_or_ones, position_samples::SAMPLE_RATE) + 1;
                    const uint32_t width = bits64::bit_width(num_bits);
                    samples_store_ = allocate_packed_ints(alloc, width, num_samples);
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

        prefetch_read(bits_.blocks() + bits_block_index);
        
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
class eliasfano_forward_iterator {
public:
    using iterator_category = std::forward_iterator_tag;
    using value_type        = T;
    using difference_type   = ptrdiff_t;
    using pointer           = value_type *;
    using reference         = value_type;

public:
    eliasfano_forward_iterator() noexcept
        : index_(0), min_(0) { }

    eliasfano_forward_iterator(const eliasfano_forward_iterator &other) = default;

    eliasfano_forward_iterator(const bits64::bitset_foreach_one_cursor &high_bits_cursor, 
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

    eliasfano_forward_iterator &operator++() noexcept {
        ++index_;
        high_bits_cursor_.next();
        return *this;
    }

    eliasfano_forward_iterator operator++(int) noexcept {
        eliasfano_forward_iterator old{*this};
        ++index_;
        high_bits_cursor_.next();
        return old;
    }

    _YAEF_ATTR_NODISCARD bits64::packed_int_view::size_type to_index() const noexcept {
        return index_;
    }

    template<typename U>
    friend bool operator==(const eliasfano_forward_iterator<U> &lhs, 
                           const eliasfano_forward_iterator<U> &rhs) noexcept;

#if __cplusplus < 202002L
    template<typename U>
    friend bool operator!=(const eliasfano_forward_iterator<U> &lhs, 
                           const eliasfano_forward_iterator<U> &rhs) noexcept;
#endif
private:
    bits64::bitset_foreach_one_cursor  high_bits_cursor_;
    bits64::packed_int_view            low_bits_;
    value_type                         min_;
    bits64::packed_int_view::size_type index_;
};

template<typename T>
_YAEF_ATTR_NODISCARD inline bool operator==(const eliasfano_forward_iterator<T> &lhs, 
                                            const eliasfano_forward_iterator<T> &rhs) noexcept {
    return lhs.index_ == rhs.index_ && lhs.low_bits_.blocks() == rhs.low_bits_.blocks();
}

#if __cplusplus < 202002L
template<typename U>
_YAEF_ATTR_NODISCARD inline bool operator!=(const eliasfano_forward_iterator<U> &lhs, 
                                            const eliasfano_forward_iterator<U> &rhs) noexcept {
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
    friend class details::eliasfano_forward_iterator;

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
    using const_iterator      = details::eliasfano_forward_iterator<value_type>;
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
        swap_alloc_impl(other.get_alloc(), typename alloc_traits::propagate_on_container_swap{});
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

#if _YAEF_USE_CXX_CONCEPTS
    template<std::random_access_iterator RandomAccessIterT,
             std::sized_sentinel_for<RandomAccessIterT> SentIterT>
#else
    template<typename RandomAccessIterT, typename SentIterT,
             typename = typename std::enable_if<
                details::is_random_access_iter<RandomAccessIterT>::value &&
                std::is_same<RandomAccessIterT, SentIterT>::value>::type>
#endif
    eliasfano_list(RandomAccessIterT first, SentIterT last, 
                   const allocator_type &alloc = allocator_type{})
        : eliasfano_list(alloc) {
        auto sorted_info = sorted_seq_info::create(first, last);
        if (!sorted_info.valid) {
            _YAEF_THROW(std::invalid_argument{"eliasfano_list::eliasfano_list: the input data is not sorted"});
        }
        auto u = static_cast<unsigned_value_type>(sorted_info.max) - 
                 static_cast<unsigned_value_type>(sorted_info.min);
        const uint32_t low_width = details::bits64::bit_width(u / sorted_info.num);
        unchecked_init_with_low_width(first, last, sorted_info, std::max<uint32_t>(low_width, 1));
    }

#if _YAEF_USE_CXX_CONCEPTS
    template<std::random_access_iterator RandomAccessIterT,
             std::sized_sentinel_for<RandomAccessIterT> SentIterT>
#else
    template<typename RandomAccessIterT, typename SentIterT,
             typename = typename std::enable_if<
                details::is_random_access_iter<RandomAccessIterT>::value &&
                std::is_same<RandomAccessIterT, SentIterT>::value>::type>
#endif
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
        return const_iterator{details::bits64::bitset_foreach_one_cursor{high_bits_.get_bits()},
                              get_low_bits(), min(), 0};
    }

    _YAEF_ATTR_NODISCARD const_iterator end() const noexcept {
        return const_iterator{details::bits64::bitset_foreach_one_cursor{},
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

    void swap(eliasfano_list &other) 
        noexcept(alloc_traits::propagate_on_container_swap::value ||
                 alloc_traits::is_always_equal::value) {
        if (_YAEF_UNLIKELY(this == std::addressof(other))) {
            return;
        }
        high_bits_.swap(other.high_bits_);
        get_low_bits().swap(other.get_low_bits());
        swap_alloc_impl(other.get_alloc(), typename alloc_traits::propagate_on_container_swap{});
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

#if _YAEF_USE_STL_RANGES_ALG
            if (!std::ranges::is_sorted(first, last)) {
                return sorted_seq_info{};
            }
#else
            if (!std::is_sorted(first, last)) {
                return sorted_seq_info{};
            }
#endif
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
#if _YAEF_USE_STL_RANGES_ALG
            _YAEF_ASSERT(std::ranges::is_sorted(first, last));
#else
            _YAEF_ASSERT(std::is_sorted(first, last));
#endif
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

    void swap_alloc_impl(allocator_type &other_alloc, std::true_type) noexcept {
        std::swap(get_alloc(), other_alloc);        
    }

    void swap_alloc_impl(allocator_type &other_alloc, std::false_type) 
        noexcept(alloc_traits::is_always_equal::value) {
#ifdef NDEBUG
        _YAEF_UNUSED(other_alloc);
#endif
        _YAEF_ASSERT(get_alloc() == other_alloc);
    }

    _YAEF_ATTR_NODISCARD const_iterator make_iter(size_type high_bit_offset, size_type index) const noexcept {
        if (_YAEF_UNLIKELY(index == size())) { return end(); }
        details::bits64::bitset_foreach_one_cursor high_bits_cursor{high_bits_.get_bits(), high_bit_offset};
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

        get_low_bits().prefetch_for_read(start, end);

        size_type base = start;
        while (len > 0) {
            size_type half = len / 2;
            base += (cmp(get_low_bits().get_value(base + half), l)) * (len - half);
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
    using const_iterator  = details::eliasfano_forward_iterator<T>;
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
        swap_alloc_impl(typename alloc_traits::propagate_on_container_swap{});
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

#if _YAEF_USE_CXX_CONCEPTS
    template<std::random_access_iterator RandomAccessIterT>
#else
    template<typename RandomAccessIterT, 
             typename = typename std::enable_if<details::is_random_access_iter<RandomAccessIterT>::value>::type>
#endif
    eliasfano_sequence(from_sorted_t, RandomAccessIterT first, RandomAccessIterT last, 
                       const allocator_type &alloc = allocator_type{})
        : min_max_and_alloc_(std::pair<value_type, value_type>{}, alloc) {
        unchecked_init(first, last);
    }

#if _YAEF_USE_CXX_CONCEPTS
    template<std::random_access_iterator RandomAccessIterT,
             std::sized_sentinel_for<RandomAccessIterT> SentIterT>
#else
    template<typename RandomAccessIterT, typename SentIterT,
             typename = typename std::enable_if<
                details::is_random_access_iter<RandomAccessIterT>::value &&
                std::is_same<RandomAccessIterT, SentIterT>::value>::type>
#endif
    eliasfano_sequence(RandomAccessIterT first, SentIterT last,
                       const allocator_type &alloc = allocator_type{})
        : min_max_and_alloc_(std::pair<value_type, value_type>{}, alloc) {
        if (!std::is_sorted(first, last)) {
            _YAEF_THROW(std::invalid_argument{
                "eliasfano_sequence::eliasfano_sequence: the input data is not sorted"});
        }
        unchecked_init(first, last);
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

        details::bits64::bitset_foreach_one_cursor high_bits_cursor{high_bits_mem_, num_high_blocks};
        details::bits64::packed_int_view low_bits{static_cast<uint32_t>(low_width_), low_bits_mem_, size_};
        return const_iterator{high_bits_cursor, low_bits, min(), 0};
    }

    _YAEF_ATTR_NODISCARD const_iterator end() const noexcept {
        details::bits64::packed_int_view low_bits{static_cast<uint32_t>(low_width_), low_bits_mem_, size_};
        return const_iterator{details::bits64::bitset_foreach_one_cursor{}, low_bits, min(), size_};
    }

    _YAEF_ATTR_NODISCARD const_iterator cbegin() const noexcept { return begin(); }
    _YAEF_ATTR_NODISCARD const_iterator cend() const noexcept { return end(); }

    void swap(eliasfano_sequence &other) noexcept {
        if (_YAEF_UNLIKELY(this == std::addressof(other))) {
            return;
        }
        std::swap(size_, other.size_);
        std::swap(high_bits_mem_, other.high_bits_mem_);
        std::swap(low_bits_mem_, other.low_bits_mem_);
        swap_low_width_and_num_buckets_impl(other);
        std::swap(min_max_and_alloc_.value(), other.min_max_and_alloc_.value());
        swap_alloc_impl(other.get_alloc(), typename alloc_traits::propagate_on_container_swap{});
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

    void swap_alloc_impl(allocator_type &other_alloc, std::true_type) noexcept {
        std::swap(get_alloc(), other_alloc);        
    }
    
    void swap_alloc_impl(allocator_type &other_alloc, std::false_type) 
        noexcept(alloc_traits::is_always_equal::value) {
#ifdef NDEBUG
        _YAEF_UNUSED(other_alloc);
#endif
        _YAEF_ASSERT(get_alloc() == other_alloc);
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

        details::bits64::large_bitset_foreach_impl<INDEXED_BIT_TYPE>(blocks, num_blocks, [&](size_type index) {
            indices[indice_writer++] = index;
        });
        pos_list_ = eliasfano_list<size_type, allocator_type>{indices.get(), indices.get() + num_indexed_bits, alloc};
    }

    // construct from the data of a plain bitmap (the number of indexed bits is known)
    eliasfano_sparse_bitmap(const uint64_t *blocks, size_type num_bits, 
                            size_type num_indexed_bits, const allocator_type &alloc = allocator_type{})
        : num_bits_(num_bits) {
        const size_type num_blocks = details::bits64::idiv_ceil(num_bits, sizeof(uint64_t) * CHAR_BIT);
        auto indices = details::make_unique_array<size_type>(num_indexed_bits);
        size_type indice_writer = 0;

        details::bits64::large_bitset_foreach_impl<INDEXED_BIT_TYPE>(blocks, num_blocks, [&](size_type index) {
            if (indice_writer > num_indexed_bits) {
                _YAEF_THROW(std::out_of_range{
                    "eliasfano_sparse_bitmap::eliasfano_sparse_bitmap: "
                    "the number of indexed bits exceeds the parameter `num_indexed_bits`."});
            }
            indices[indice_writer++] = index;
        });
        pos_list_ = eliasfano_list<size_type, allocator_type>{indices.get(), indices.get() + num_indexed_bits, alloc};
    }

#if _YAEF_USE_CXX_CONCEPTS
    template<std::random_access_iterator RandomAccessIterT>
#else
    template<typename RandomAccessIterT, 
             typename = typename std::enable_if<details::is_random_access_iter<RandomAccessIterT>::value>::type>
#endif
    eliasfano_sparse_bitmap(size_t num_bits, RandomAccessIterT indices_first, RandomAccessIterT indices_last,
                            const allocator_type &alloc = allocator_type{})
        : num_bits_(num_bits) {
        pos_list_ = eliasfano_list<size_type, allocator_type>{indices_first, indices_last, alloc};
    }

#if _YAEF_USE_CXX_CONCEPTS
    template<std::random_access_iterator RandomAccessIterT>
#else
    template<typename RandomAccessIterT, 
             typename = typename std::enable_if<details::is_random_access_iter<RandomAccessIterT>::value>::type>
#endif
    eliasfano_sparse_bitmap(from_sorted_t, size_t num_bits, 
                            RandomAccessIterT indices_first, RandomAccessIterT indices_last,
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

    bit_buffer(size_type size) {
        get_view() = details::allocate_bits(get_alloc(), size);
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
    if (_YAEF_UNLIKELY(!std::is_sorted(first, last))) {
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
    details::bits64::bitset_foreach_one(high_bits, num_high_blocks, [&](size_t pos) {
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