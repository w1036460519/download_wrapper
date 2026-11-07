/**
 * @file file_record.h
 * @brief 文件目录记录：任务状态持久化权威 + UI 渲染主表。
 *
 * 1 任务 = 1 行。status 列承载任务状态机（add=RESOLVING → PARSED=QUEUED →
 * 调度=DOWNLOADING → PAUSED/COMPLETED/ERROR 终态），重启恢复时据此重建运行态。
 * 添加任务时先以 url/infohash 推导占位，PARSED 后修正为真实根名与文件类型。
 * 本地文件也入此表（task_protocol 为 LOCAL，无任务关联时 protocol 取 DW_PROTOCOL_LOCAL）。
 */

#pragma once

#include "task.h"

#include <cstdint>
#include <string>
#include <vector>

#include <boost/json.hpp>

namespace dw {
    // 迁移期旧表保留旧关联字段，新 Task 不再携带这些字段。
    struct FileRecord : Task {
        dw_source_t type = DW_SOURCE_LOCAL_FILE;
        std::string task_natural_key;
        std::vector<int32_t> file_indexes;
    };

    /// FileRecord → JSON 对象（日志输出用）
    inline boost::json::object to_json(const FileRecord &r) {
        boost::json::object obj;
        obj["id"] = r.id;
        obj["client_id"] = r.client_id;
        obj["type"] = static_cast<int>(r.type);
        obj["is_remote"] = r.is_remote;
        obj["save_path"] = r.save_path;
        obj["original_root_name"] = r.original_root_name;
        obj["root_name"] = r.root_name;
        obj["full_path"] = r.full_path;
        obj["file_type"] = r.file_type;
        obj["ext"] = r.ext;
        obj["parsed"] = r.parsed;
        obj["url"] = r.url;
        obj["info_hash"] = r.info_hash;
        obj["magnet_link"] = r.magnet_link;
        obj["torrent_file"] = r.torrent_file;
        obj["task_protocol"] = static_cast<int>(r.task_protocol);
        obj["task_natural_key"] = r.task_natural_key;
        obj["status"] = r.status;
        obj["total_size"] = r.total_size;
        obj["total_done"] = r.total_done;
        // 运行时速率字段：引擎事件已回填（BT=torrent_status、HTTP=分片速率和），
        // 进度回调与列表查询依赖此字段展示实时速率，遗漏会导致上层恒取 -1。
        obj["download_rate"] = r.download_rate;
        obj["upload_rate"] = r.upload_rate;
        obj["priority"] = r.priority;
        obj["reason"] = r.reason;
        obj["message"] = r.message;
        obj["created_at"] = r.created_at;
        obj["modified_at"] = r.modified_at;
        obj["is_delete"] = r.is_delete;
        obj["is_read"] = r.is_read;
        return obj;
    }

    /// FileRecord 指针 → JSON 字符串（空指针返回 "null"）
    inline std::string to_json_string(const FileRecord *r) {
        if (!r) return "null";
        return boost::json::serialize(to_json(*r));
    }
} // namespace dw
