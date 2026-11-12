/**
 * @file task.h
 * @brief 统一任务结构，供引擎、任务管理和持久化使用。
 */
#pragma once

#include "protocol.h"
#include "task_file.h"
#include <cstdint>
#include <map>
#include <string>
#include <vector>
#include <boost/json.hpp>

namespace dw {
    struct Task {
        [[nodiscard]] std::string to_string() const;

        // ---- 持久化到 tasks 表 ----
        std::string task_id; // 任务主键，HTTP 为 URL，BT 为 info_hash。
        std::string client_id; // 客户端标识
        bool is_remote = false; // 远程标识
        std::string name; // 任务名称
        std::string save_path; // 保存路径
        std::string full_path; // 磁盘根实体全路径；占位空串，PARSED 回填
        bool file_type = true; // true=目录 false=文件
        std::string ext; // 文件后缀（不含点，如 "mp4"）；目录为空

        // 任务元数据（添加任务时回填，供恢复 / 重试 / 详情展示；按协议各自留空）
        std::string url; // HTTP 任务源地址；BT 任务为空
        std::string info_hash; // BT info hash（v1=40 位 / v2=64 位 hex）；HTTP 任务为空
        std::string magnet_link; // BT 磁力链接；种子文件任务为空
        std::string torrent_file; // BT 种子文件路径；磁力任务为空

        // 任务关联（无关联时 protocol=LOCAL、natural_key 为空）
        dw_protocol_t task_protocol = DW_PROTOCOL_LOCAL;

        // 任务状态和进度，由维护线程同步。
        int32_t status = 0; // 任务状态（dw_task_status_t）
        int64_t total_size = -1; // 总字节
        int64_t total_done = 0; // 已完成字节
        std::string message; // 错误文本 / 状态描述

        int64_t created_at = 0; // Unix 毫秒
        int64_t modified_at = 0; // Unix 毫秒
        bool is_delete = false; // 软删除标记
        bool is_read = false; // 已读标记

        // ---- 独立持久化到 task_file 表 ----
        std::vector<TaskFile> files; // 按 file_index 升序保存，同一索引只保留一条记录。

        // ---- 运行时字段，不写入 tasks 表 ----
        std::vector<uint8_t> resume_data;
        int32_t support_range = 0; // HTTP Range 支持，续传所需的值由 HTTP 续传数据保存。
        std::string etag; // HTTP 响应 ETag，续传所需的值由 HTTP 续传数据保存。
        bool force = false; // 本次调度的强制准入标记。
        bool dirty = false; // 任务有待保存和推送的变化。
        bool resume_dirty = false;

        double download_rate = 0.0; // 实时下载速率（B/s）。
        double upload_rate = 0.0; // 实时上传速率（B/s）。
    };

    inline boost::json::object to_json(const Task &r) {
        boost::json::object obj;
        obj["task_id"] = r.task_id;
        obj["client_id"] = r.client_id;
        obj["is_remote"] = r.is_remote;
        obj["name"] = r.name;
        obj["save_path"] = r.save_path;
        obj["full_path"] = r.full_path;
        obj["file_type"] = r.file_type;
        obj["ext"] = r.ext;
        obj["url"] = r.url;
        obj["info_hash"] = r.info_hash;
        obj["magnet_link"] = r.magnet_link;
        obj["torrent_file"] = r.torrent_file;
        obj["task_protocol"] = static_cast<int>(r.task_protocol);
        obj["status"] = r.status;
        obj["total_size"] = r.total_size;
        obj["total_done"] = r.total_done;
        // 运行时速率字段：引擎事件已回填（BT=torrent_status、HTTP=分片速率和），
        // 进度回调与列表查询依赖此字段展示实时速率，遗漏会导致上层恒取 -1。
        obj["download_rate"] = r.download_rate;
        obj["upload_rate"] = r.upload_rate;
        obj["message"] = r.message;
        obj["created_at"] = r.created_at;
        obj["modified_at"] = r.modified_at;
        obj["is_delete"] = r.is_delete;
        obj["is_read"] = r.is_read;
        obj["support_range"] = r.support_range;
        obj["etag"] = r.etag;
        obj["force"] = r.force;
        obj["dirty"] = r.dirty;
        return obj;
    }

    inline void tag_invoke(boost::json::value_from_tag, boost::json::value &value, const Task &task) {
        value = to_json(task);
    }

    inline std::string to_string(const Task &r) {
        return boost::json::serialize(to_json(r));
    }
} // namespace dw
