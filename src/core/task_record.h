/**
 * @file task_record.h
 * @brief 任务数据结构：任务中枢（TaskManager）与持久化存储层（TaskStore）共享的内存态。
 *
 * TaskRuntime 为纯运行态结构，不直接落库；任务状态持久化权威为 file_records 表
 * （关键状态迁移点即时写，进度遥测经节流同步）。
 * 独立成头，供 task_manager.h 与 task_store.h 各自 include，避免二者相互包含形成循环依赖。
 */

#pragma once

#include "download_wrapper/download_wrapper.h"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include <boost/json.hpp>

namespace dw {
    /// dw_protocol_t 可读名（供 open_id / 日志使用）。
    inline const char *to_string(dw_protocol_t p) {
        switch (p) {
            case DW_PROTOCOL_HTTP: return "HTTP";
            case DW_PROTOCOL_TORRENT: return "BT";
            case DW_PROTOCOL_LOCAL: return "LOCAL";
            default: return "UNKNOWN";
        }
    }

    /**
     * 任务运行态：注册表内存态 = 来源参数（恢复/队列晋升重建用）+ 最新快照 + 调度元数据。
     *
     * 主键为 (client_id, protocol, natural_key)。
     * natural_key：HTTP=url / BT=info_hash，统一唯一标识。
     * union_id() 按 "client_id|type|natural_key" 格式派生，为 tasks_ map 键。
     * 状态迁移由 TaskManager 即时投影至 file_records.status；本结构不直接落库。
     */

    /**
     * 文件目录记录：任务状态持久化权威 + UI 渲染主表，1 任务 = 1 行。
     *
     * status 列承载任务状态机（add=RESOLVING → PARSED=QUEUED → 调度=DOWNLOADING →
     * PAUSED/COMPLETED/ERROR 终态），重启恢复时据此重建 TaskRuntime。
     * 添加任务时先以 url/infohash 推导占位，PARSED 后修正为真实根名与文件类型。
     * 本地文件也入此表（task_protocol 为 LOCAL，无任务关联时 protocol 取 DW_PROTOCOL_LOCAL）。
     */
    struct FileRecord {
        int64_t id = 0; // 自增主键（0=未落库）
        std::string client_id; // 客户端标识
        dw_source_t type = DW_SOURCE_LOCAL_FILE; // 任务类型：0=文件任务 1=HTTP任务 2=BT任务
        bool is_remote = false; // 远程标识
        std::string save_path; // 保存路径
        std::string original_root_name; // 重名/包装前的原始目录/文件名（未重名时与 root_name 相同）
        std::string root_name; // 根目录/文件名（重名/包装后的最终名称）
        std::string full_path; // 磁盘根实体全路径（save_path/root_name，目录与单文件统一公式）；占位空串，PARSED 回填
        bool file_type = true; // 0=文件 1=目录
        std::string ext; // 文件后缀（不含点，如 "mp4"）；目录为空；add 占位空串，PARSED 回填
        bool parsed = false; // 元数据已解析（PARSED 事件后为 true）；引擎据此跳过重复重名检测

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
        // HTTP 续传元数据（已在 resume_data 表持久化，此处仅内存态供进度回调）
        int32_t support_range = 0; // 服务端 Range 支持：0=不支持，1=支持
        std::string etag; // 响应 ETag
        std::string last_modified; // 响应 Last-Modified
        std::vector<int32_t> file_indexes; // 文件索引列表
        std::vector<int32_t> priority_file_indexes; // 优先下载文件索引
        bool force = false; // 强制准入标记（调度器让行后自动清除）
        bool dirty = false; // 数据已变更待同步（持久化 + 推送 app）
        bool is_delete = false;

        // 运行态遥测（不持久化）：引擎线程经 on_progress 推入，A 线程节拍读取并转发
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
        obj["priority"] = r.priority;
        obj["reason"] = r.reason;
        obj["message"] = r.message;
        obj["created_at"] = r.created_at;
        obj["modified_at"] = r.modified_at;
        return obj;
    }

    /// FileRecord 指针 → JSON 字符串（空指针返回 "null"）
    inline std::string to_json_string(const FileRecord *r) {
        if (!r) return "null";
        return boost::json::serialize(to_json(*r));
    }
} // namespace dw
