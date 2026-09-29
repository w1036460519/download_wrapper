/**
 * @file json_util.h
 * @brief JSON 字段提取工具：类型匹配则填充，否则跳过。
 */

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <boost/json.hpp>

namespace dw::json_util {
    inline void extract(const boost::json::object &obj, const char *key, std::string &out) {
        if (auto *v = obj.if_contains(key); v && v->is_string()) {
            out = v->as_string().c_str();
        }
    }

    inline void extract(const boost::json::object &obj, const char *key, int32_t &out) {
        if (auto *v = obj.if_contains(key); v && v->is_int64()) {
            out = static_cast<int32_t>(v->as_int64());
        }
    }

    inline void extract(const boost::json::object &obj, const char *key, int64_t &out) {
        if (auto *v = obj.if_contains(key); v && v->is_int64()) {
            out = v->as_int64();
        }
    }

    inline void extract(const boost::json::object &obj, const char *key, bool &out) {
        if (auto *v = obj.if_contains(key); v && v->is_bool()) {
            out = v->as_bool();
        }
    }

    inline void extract(const boost::json::object &obj, const char *key, std::vector<int32_t> &out) {
        if (auto *v = obj.if_contains(key); v && v->is_array()) {
            out.clear();
            for (const auto &elem: v->as_array()) {
                if (elem.is_int64()) {
                    out.push_back(static_cast<int32_t>(elem.as_int64()));
                }
            }
        }
    }

    inline void extract(const boost::json::object &obj, const char *key, std::vector<std::string> &out) {
        if (auto *v = obj.if_contains(key); v && v->is_array()) {
            out.clear();
            for (const auto &elem: v->as_array()) {
                if (elem.is_string()) {
                    out.push_back(elem.as_string().c_str());
                }
            }
        }
    }
} // namespace dw::json_util
