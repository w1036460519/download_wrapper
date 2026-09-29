/**
 * @file task_identity.h
 * @brief 任务三要素（基类）+ JSON 序列化/反序列化。
 */

#pragma once

#include "download_wrapper/download_wrapper.h"
#include "internal/json_util.h"

#include <string>

#include <boost/json.hpp>

namespace dw {
    /**
     * 任务三要素（基类）：client_id + protocol + natural_key。
     * 所有任务参数的公共标识字段。
     */
    struct TaskIdentity {
        std::string client_id;
        dw_protocol_t protocol = DW_PROTOCOL_HTTP;
        std::string natural_key;

        /// 复合键：client_id|protocol|natural_key
        std::string union_id() const {
            return client_id + "|" + std::to_string(static_cast<int>(protocol)) + "|" + natural_key;
        }
    };

    // ---- C++ 序列化（内部消费） ----

    inline boost::json::object to_json(const TaskIdentity &t) {
        boost::json::object obj;
        obj["client_id"] = t.client_id;
        obj["protocol"] = static_cast<int>(t.protocol);
        obj["natural_key"] = t.natural_key;
        return obj;
    }

    inline void from_json(const boost::json::object &obj, TaskIdentity &t) {
        json_util::extract(obj, "client_id", t.client_id);
        if (auto *v = obj.if_contains("protocol"); v && v->is_int64()) {
            t.protocol = static_cast<dw_protocol_t>(v->as_int64());
        }
        json_util::extract(obj, "natural_key", t.natural_key);
    }

    // ---- ABI 包装（FFI 层消费） ----

    /// JSON 字符串 → TaskIdentity，成功返回 0，解析失败返回 -1
    inline int parse_identity(const std::string &json, TaskIdentity &out) {
        try {
            auto obj = boost::json::parse(json).as_object();
            from_json(obj, out);
            return 0;
        } catch (const std::exception &) {
            return -1;
        }
    }
} // namespace dw
