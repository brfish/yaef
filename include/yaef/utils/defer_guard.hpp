#ifndef __YAEF_UTILS_DEFER_GUARD_HPP__
#define __YAEF_UTILS_DEFER_GUARD_HPP__
#pragma once

#include <type_traits>
#include <utility>

#include "yaef/yaef.hpp"

namespace yaef {
namespace utils {

template<typename CallbackT>
class defer_guard final {
public:
    using callback_type = CallbackT;
public:
    defer_guard() = delete;
    defer_guard(const defer_guard &) = delete;

    defer_guard(defer_guard &&other)
        : active_(std::exchange(other.active_, false)),
          callback_(std::move(other.callback_)) { }

    template<typename F = callback_type>
    defer_guard(F &&f)
        : active_(true), callback_(std::forward<F>(f)) { }

    ~defer_guard() {
        if (active_)
            callback_();
    }

private:
    bool          active_;
    callback_type callback_;
};

namespace util_details {

struct defer_guard_builder {
    template<typename CallbackT>
    defer_guard<std::decay_t<CallbackT>> operator%(CallbackT &&callback) {
        return defer_guard<std::decay_t<CallbackT>>{std::forward<CallbackT>(callback)};
    }
};

}

#define _YAEF_DEFER_IMPL_NAME() _YAEF_CONCAT(__ef_anonymous_defer_guard_, __LINE__)

#define YAEF_DEFER auto _YAEF_DEFER_IMPL_NAME() = \
    ::yaef::utils::util_details::defer_guard_builder{} % [&]() -> void

}
}

#endif