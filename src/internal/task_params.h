/**
 * @file task_params.h
 * @brief 任务参数（继承三要素）+ JSON 序列化/反序列化 + ABI 包装。
 */

#pragma once

#include "internal/task_identity.h"

#include <cstdint>
#include <string>
#include <vector>

#include <boost/json.hpp>

namespace dw {
    /**
     * 任务参数（继承三要素）：用于 FFI 边界的 JSON 序列化/反序列化。
     * 内部使用 C++ 特性（std::string, std::vector），无需跨边界内存管理。
     */
    struct TaskParams : TaskIdentity {
        std::string save_path;
        std::string url;
        std::string info_hash;
        std::string magnet_link;
        std::string torrent_file;
        std::vector<int32_t> file_indexes;
        std::vector<int32_t> priority_file_indexes;
        std::vector<std::string> url_seeds;
        std::vector<uint8_t> resume_data; // 断点续传数据
        int32_t priority = 0;
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
        if (p.priority != 0) obj["priority"] = p.priority;
        if (p.delete_files) obj["delete_files"] = true;
        if (p.force) obj["force"] = true;

        if (!p.file_indexes.empty()) {
            boost::json::array fi;
            for (auto x: p.file_indexes) fi.push_back(x);
            obj["file_indexes"] = std::move(fi);
        }

        if (!p.priority_file_indexes.empty()) {
            boost::json::array pfi;
            for (auto x: p.priority_file_indexes) pfi.push_back(x);
            obj["priority_file_indexes"] = std::move(pfi);
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
        json_util::extract(obj, "priority", p.priority);
        json_util::extract(obj, "delete_files", p.delete_files);
        json_util::extract(obj, "force", p.force);
        json_util::extract(obj, "file_indexes", p.file_indexes);
        json_util::extract(obj, "priority_file_indexes", p.priority_file_indexes);
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
