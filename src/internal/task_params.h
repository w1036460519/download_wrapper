/**
 * @file task_params.h
 * @brief 任务参数（继承三要素）+ JSON 序列化/反序列化 + ABI 包装。
 */

#pragma once

#include "internal/task_identity.h"

#include <charconv>
#include <cstdint>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <boost/json.hpp>

namespace dw {

    struct TaskParams {
        std::string client_id;
        std::string save_path;
        std::string url;
        std::string info_hash;
        std::string magnet_link;
        std::string torrent_file;
        std::vector<int32_t> file_indexes;
        int32_t file_priority = 4; // 添加 BT 任务时，file_indexes 共用的下载优先级。
        std::map<int32_t, int32_t> file_priorities; // 文件索引 -> 下载优先级。
        std::vector<std::string> url_seeds;
        std::vector<uint8_t> resume_data; // 断点续传数据
        std::optional<int32_t> queue_priority; // 添加任务时的队列优先级。
        bool delete_files = false; // 删除任务时是否同时删除本地文件
        bool force = false; // 恢复时强制准入（调度器暂停最慢任务让行）
    };

    // ---- C++ 序列化（内部消费） ----

    inline boost::json::object to_json(const TaskParams &p) {
        auto obj = to_json(static_cast<const TaskIdentity &>(p));
        if (!p.save_path.empty()) obj["save_path"] = p.save_path;
        if (!p.url.empty()) obj["url"] = p.url;
        if (!p.info_hash.empty()) obj["info_hash"] = p.info_hash;
        if (!p.magnet_link.empty()) obj["magnet_link"] = p.magnet_link;
        if (!p.torrent_file.empty()) obj["torrent_file"] = p.torrent_file;
        if (p.queue_priority) obj["priority"] = *p.queue_priority;
        if (p.delete_files) obj["delete_files"] = true;
        if (p.force) obj["force"] = true;
        if (!p.file_indexes.empty() || p.file_priority != 4) obj["file_priority"] = p.file_priority;

        if (!p.file_indexes.empty()) {
            boost::json::array fi;
            for (auto x: p.file_indexes) fi.push_back(x);
            obj["file_indexes"] = std::move(fi);
        }

        if (!p.file_priorities.empty()) {
            boost::json::object priorities;
            for (const auto &[index, priority]: p.file_priorities) {
                priorities.emplace(std::to_string(index), priority);
            }
            obj["file_priorities"] = std::move(priorities);
        }

        if (!p.url_seeds.empty()) {
            boost::json::array seeds;
            for (const auto &s: p.url_seeds) seeds.emplace_back(s);
            obj["url_seeds"] = std::move(seeds);
        }

        return obj;
    }

    inline void from_json(const boost::json::object &obj, TaskParams &p) {
        // 基类字段
        from_json(obj, static_cast<TaskIdentity &>(p));
        // 派生类字段（宽松提取）
        json_util::extract(obj, "save_path", p.save_path);
        json_util::extract(obj, "url", p.url);
        json_util::extract(obj, "info_hash", p.info_hash);
        json_util::extract(obj, "magnet_link", p.magnet_link);
        json_util::extract(obj, "torrent_file", p.torrent_file);
        if (const auto *value = obj.if_contains("priority");
            value && value->is_int64() && std::in_range<int32_t>(value->as_int64())) {
            p.queue_priority = static_cast<int32_t>(value->as_int64());
        }
        json_util::extract(obj, "delete_files", p.delete_files);
        json_util::extract(obj, "force", p.force);
        p.file_priority = 4;
        if (const auto *value = obj.if_contains("file_priority")) {
            if (!value->is_int64() || value->as_int64() < 0 || value->as_int64() > 7) {
                throw std::invalid_argument("file_priority must be an integer from 0 to 7");
            }
            p.file_priority = static_cast<int32_t>(value->as_int64());
        }
        if (const auto *indexes = obj.if_contains("file_indexes"); indexes && indexes->is_array()) {
            p.file_indexes.clear();
            for (const auto &index: indexes->as_array()) {
                if (!index.is_int64() || index.as_int64() < 0 || !std::in_range<int32_t>(index.as_int64())) {
                    throw std::invalid_argument("file_indexes must contain non-negative int32 indexes");
                }
                p.file_indexes.push_back(static_cast<int32_t>(index.as_int64()));
            }
        }
        if (const auto *priorities = obj.if_contains("file_priorities"); priorities && priorities->is_object()) {
            p.file_priorities.clear();
            for (const auto &entry: priorities->as_object()) {
                const auto key = entry.key();
                int32_t index = 0;
                const auto *end = key.data() + key.size();
                const auto [ptr, ec] = std::from_chars(key.data(), end, index);
                const auto &priority = entry.value();
                if (ec == std::errc{} && ptr == end && index >= 0 && priority.is_int64()) {
                    const auto level = priority.as_int64();
                    if (level >= 0 && level <= 7) {
                        p.file_priorities.insert_or_assign(index, static_cast<int32_t>(level));
                    }
                }
            }
        } else if (const auto *indexes = obj.if_contains("priority_file_indexes"); indexes && indexes->is_array()) {
            // 兼容旧请求中的索引数组和统一优先级。
            p.file_priorities.clear();
            const int32_t priority = p.queue_priority.value_or(4);
            const int32_t file_priority = priority < 4 ? 1 : (priority > 4 ? 7 : 4);
            for (const auto &index: indexes->as_array()) {
                if (index.is_int64() && index.as_int64() >= 0 && std::in_range<int32_t>(index.as_int64())) {
                    p.file_priorities.try_emplace(static_cast<int32_t>(index.as_int64()), file_priority);
                }
            }
        }
        json_util::extract(obj, "url_seeds", p.url_seeds);
    }

    // ---- ABI 包装（FFI 层消费） ----

    /// JSON 字符串 → TaskParams，成功返回 0，解析失败返回 -1
    inline int parse_task_params(const std::string &json, TaskParams &out) {
        try {
            auto obj = boost::json::parse(json).as_object();
            from_json(obj, out);
            return 0;
        } catch (const std::exception &) {
            return -1;
        }
    }

    /// TaskParams → JSON 字符串
    inline std::string to_json_string(const TaskParams &p) {
        return boost::json::serialize(to_json(p));
    }
} // namespace dw
