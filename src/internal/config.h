/**
 * @file config.h
 * @brief 配置结构体 + JSON 序列化/反序列化 + ABI 包装。
 */

#pragma once

#include "download_wrapper/download_wrapper.h"
#include "internal/json_util.h"

#include <cstdint>
#include <string>
#include <vector>

#include <boost/json.hpp>

namespace dw {
    /**
     * 配置结构体（内部 C++ 版本）：用于 FFI 边界的 JSON 序列化/反序列化。
     */
    struct Config {
        // HTTP 配置
        int32_t connect_timeout_seconds = 30;
        int32_t request_timeout_seconds = 300;
        int64_t low_speed_limit_bps = 1024;
        int32_t low_speed_time = 60;
        int32_t max_redirect = 5;
        std::string proxy;
        std::string proxy_username;
        std::string proxy_password;
        std::string user_agent;
        bool verify_ssl = true;
        std::string ca_bundle;
        int32_t max_retries = 3;
        int32_t default_parts = 4;
        int64_t min_size_for_split = 1024 * 1024;

        // BT 配置
        int32_t listen_port = 6881;
        int32_t max_concurrent_downloads = 5;
        int64_t download_rate_limit = 0;
        int64_t upload_rate_limit = 0;
        double seed_ratio_limit = 1.0;

        // 通用配置
        dw_log_level_t log_level = DW_LOG_INFO;
        std::string work_dir;
        std::string client_id;
        std::string save_path = "."; // 默认保存目录：当前目录
        std::vector<std::string> trackers;

        // 网络控制
        bool allow_mobile_data = false; // 应用设置：是否允许使用移动数据
        int32_t network_type = 1; // 网络状态：0=无网络, 1=WiFi, 2=移动数据
    };

    // ---- C++ 序列化（内部消费） ----

    inline boost::json::object to_json(const Config &c) {
        boost::json::object obj;
        obj["connect_timeout"] = c.connect_timeout_seconds;
        obj["request_timeout"] = c.request_timeout_seconds;
        obj["low_speed_limit"] = c.low_speed_limit_bps;
        obj["low_speed_time"] = c.low_speed_time;
        obj["max_redirect"] = c.max_redirect;
        obj["proxy"] = c.proxy;
        obj["proxy_username"] = c.proxy_username;
        obj["proxy_password"] = c.proxy_password;
        obj["user_agent"] = c.user_agent;
        obj["verify_ssl"] = c.verify_ssl;
        obj["ca_bundle"] = c.ca_bundle;
        obj["max_retries"] = c.max_retries;
        obj["default_parts"] = c.default_parts;
        obj["min_size_for_split"] = c.min_size_for_split;
        obj["listen_port"] = c.listen_port;
        obj["max_concurrent"] = c.max_concurrent_downloads;
        obj["dl_rate_limit"] = c.download_rate_limit;
        obj["ul_rate_limit"] = c.upload_rate_limit;
        obj["seed_ratio"] = c.seed_ratio_limit;
        obj["log_level"] = static_cast<int>(c.log_level);
        obj["work_dir"] = c.work_dir;
        obj["client_id"] = c.client_id;
        obj["save_path"] = c.save_path;
        boost::json::array trackers_arr;
        for (const auto &t: c.trackers) trackers_arr.emplace_back(t);
        obj["trackers"] = std::move(trackers_arr);
        obj["allow_mobile_data"] = c.allow_mobile_data;
        obj["network_type"] = c.network_type;
        return obj;
    }

    inline void from_json(const boost::json::object &obj, Config &c) {
        json_util::extract(obj, "connect_timeout", c.connect_timeout_seconds);
        json_util::extract(obj, "request_timeout", c.request_timeout_seconds);
        json_util::extract(obj, "low_speed_limit", c.low_speed_limit_bps);
        json_util::extract(obj, "low_speed_time", c.low_speed_time);
        json_util::extract(obj, "max_redirect", c.max_redirect);
        json_util::extract(obj, "proxy", c.proxy);
        json_util::extract(obj, "proxy_username", c.proxy_username);
        json_util::extract(obj, "proxy_password", c.proxy_password);
        json_util::extract(obj, "user_agent", c.user_agent);
        if (auto *v = obj.if_contains("verify_ssl"); v && v->is_bool()) {
            c.verify_ssl = v->as_bool();
        }
        json_util::extract(obj, "ca_bundle", c.ca_bundle);
        json_util::extract(obj, "max_retries", c.max_retries);
        json_util::extract(obj, "default_parts", c.default_parts);
        json_util::extract(obj, "min_size_for_split", c.min_size_for_split);
        json_util::extract(obj, "listen_port", c.listen_port);
        json_util::extract(obj, "max_concurrent", c.max_concurrent_downloads);
        json_util::extract(obj, "dl_rate_limit", c.download_rate_limit);
        json_util::extract(obj, "ul_rate_limit", c.upload_rate_limit);
        if (auto *v = obj.if_contains("seed_ratio"); v && v->is_double()) {
            c.seed_ratio_limit = v->as_double();
        }
        if (auto *v = obj.if_contains("log_level"); v && v->is_int64()) {
            c.log_level = static_cast<dw_log_level_t>(v->as_int64());
        }
        json_util::extract(obj, "work_dir", c.work_dir);
        json_util::extract(obj, "client_id", c.client_id);
        json_util::extract(obj, "save_path", c.save_path);
        json_util::extract(obj, "trackers", c.trackers);
        if (auto *v = obj.if_contains("allow_mobile_data"); v && v->is_bool()) {
            c.allow_mobile_data = v->as_bool();
        }
        json_util::extract(obj, "network_type", c.network_type);
    }

    // ---- ABI 包装（FFI 层消费） ----

    /// JSON 字符串 → Config，成功返回 0，解析失败返回 -1
    inline int parse_config(const std::string &json, Config &out) {
        try {
            auto obj = boost::json::parse(json).as_object();
            from_json(obj, out);
            return 0;
        } catch (const std::exception &) {
            return -1;
        }
    }

    /// Config → JSON 字符串
    inline std::string to_json_string(const Config &c) {
        return boost::json::serialize(to_json(c));
    }
} // namespace dw
