/**
 * @file dw_result.h
 * @brief 操作结果（内部版本）+ JSON 序列化 + ABI 包装。
 */

#pragma once

#include "download_wrapper/download_wrapper.h"
#include "utils/string_util.h"

#include <cstdint>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>

#include <boost/json.hpp>

namespace dw {
    template <typename T = std::monostate>
    struct dw_result {
        dw_reason_t code = DW_REASON_NONE;
        std::string message;
        T data{};

        /// 构造成功结果
        static dw_result success() { return {}; }

        /// 构造失败结果
        static dw_result failure(const dw_reason_t reason = DW_REASON_ERROR, std::string msg = {}) {
            dw_result r;
            r.code = reason;
            r.message = std::move(msg);
            return r;
        }
    };

    template <typename T>
    boost::json::object to_json(const dw_result<T> &result) {
        boost::json::object response;
        response["code"] = static_cast<int>(result.code);
        if (result.code != DW_REASON_NONE) {
            response["message"] = result.message.empty() ? "操作失败" : result.message;
        } else if constexpr (!std::is_same_v<T, std::monostate>) {
            if constexpr (requires { to_json(result.data); }) {
                response["data"] = to_json(result.data);
            } else {
                response["data"] = boost::json::value_from(result.data);
            }
        }
        return response;
    }

    template <typename T>
    char *result_to_json(const dw_result<T> &result) {
        return utils::dup_cstr(boost::json::serialize(to_json(result)));
    }
} // namespace dw
