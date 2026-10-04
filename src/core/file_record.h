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

#include "protocol.h"

#include <cstdint>
#include <string>
#include <vector>

#include <boost/json.hpp>

namespace dw {
    struct FileRecord {
        int64_t id = 0; // 自增主键（0=未落库）
        std::string client_id; // 客户端标识
        dw_source_t type = DW_SOURCE_LOCAL_FILE; // 任务类型：0=文件任务 1=HTTP任务 2=BT任务
        bool is_remote = false; // 远程标识
        std::string save_path; // 保存路径
        std::string original_root_name; // 重名/包装前的原始目录/文件名（未重名时与 root_name 相同）
        std::string root_name; // 根目录/文件名（重名/包装后的最终名称）
        std::string full_path; // 磁盘根实体全路径（save_path/root_name）；占位空串，PARSED 回填
        bool file_type = true; // true=目录 false=文件
        std::string ext; // 文件后缀（不含点，如 "mp4"）；目录为空
        bool parsed = false; // 元数据已解析（PARSED 事件后为 true）

        // 任务关联（无关联时 protocol=LOCAL、natural_key 为空）
        dw_protocol_t task_protocol = DW_PROTOCOL_LOCAL;
        std::string task_natural_key; // 关联任务的 natural_key

        // 任务状态持久化（权威列：状态迁移即时写，进度经节流同步）
        int32_t status = 0; // 任务状态（dw_task_status_t）
        int64_t total_size = -1; // 总字节
        int64_t total_done = 0; // 已完成字节
        int32_t priority = 0; // 队列优先级（越大越优先）
        int32_t reason = 0; // 错误原因码（dw_reason_t）；仅 ERROR 态有效
        std::string message; // 错误文本 / 状态描述

        int64_t created_at = 0; // Unix 毫秒
        int64_t modified_at = 0; // Unix 毫秒

        // ---- 非持久化运行时字段（仅内存态，不入库） ----
        int32_t support_range = 0; // 服务端 Range 支持：0=不支持，1=支持
        std::string etag; // 响应 ETag
        std::string last_modified; // 响应 Last-Modified
        std::vector<int32_t> file_indexes; // 文件索引列表
        std::vector<int32_t> priority_file_indexes; // 优先下载文件索引
        bool force = false; // 强制准入标记（调度器让行后自动清除）
        bool dirty = false; // 数据已变更待同步（持久化 + 推送 app）
        bool is_delete = false;

        // 运行态遥测（不持久化）
        double download_rate = 0.0; // 下载速率（B/s）
        double upload_rate = 0.0; // 上传速率（B/s）；HTTP 恒为 0

        /// 是否关联了下载任务（非本地文件）。
        bool has_task() const {
            return task_protocol != DW_PROTOCOL_LOCAL && !task_natural_key.empty();
        }

        /// 任务主键 union_id（client_id|protocol|natural_key）
        std::string union_id() const {
            return client_id + '|' + to_string(task_protocol) + '|' + task_natural_key;
        }
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
        return obj;
    }

    /// FileRecord 指针 → JSON 字符串（空指针返回 "null"）
    inline std::string to_json_string(const FileRecord *r) {
        if (!r) return "null";
        return boost::json::serialize(to_json(*r));
    }
} // namespace dw
