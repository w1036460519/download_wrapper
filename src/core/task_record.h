/**
 * @file task_record.h
 * @brief 任务记录数据结构：任务中枢（TaskManager）与持久化存储层（TaskStore）共享的内存态。
 *
 * 独立成头，供 task_manager.h 与 task_store.h 各自 include，避免二者相互包含形成循环依赖。
 */

#pragma once

#include "download_wrapper/download_wrapper.h"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <vector>

namespace dw {

/// dw_protocol_t 可读名（供 open_id / 日志使用）。
inline const char* to_string(dw_protocol_t p) {
    switch (p) {
    case DW_PROTOCOL_HTTP:    return "HTTP";
    case DW_PROTOCOL_TORRENT: return "BT";
    case DW_PROTOCOL_LOCAL:   return "LOCAL";
    default:                  return "UNKNOWN";
    }
}

/**
 * 任务记录：注册表内存态 = 来源参数（恢复/队列晋升重建用）+ 最新快照 + 队列元数据。
 *
 * 主键为 (client_id, protocol, natural_key)。
 * natural_key：HTTP=url / BT=info_hash，统一唯一标识。
 * LOCAL 任务不进 tasks 表，仅存 file_records。
 * union_id() 按 "client_id|type|natural_key" 格式派生，为 tasks_ map 键。
 */
struct TaskRecord {
    // 主键（DB PK = client_id + protocol 列 + natural_key 列）
    std::string   client_id;    // App 启动时注入（UUIDv4）；本机任务恒 = TaskManager::client_id_
    dw_protocol_t protocol = DW_PROTOCOL_HTTP; // 协议类型
    std::string   natural_key;  // 唯一标识：HTTP=url / BT=info_hash

    // 任务全局唯一标识
    std::string union_id() const {
        return client_id + '|' + to_string(protocol) + '|' + natural_key;
    }

    // 来源参数（恢复 / 队列晋升时重建引擎任务）
    std::string              save_path;     // 用户指定的保存目录（固定不变）
    std::string              content_root;  // save_path 下的磁盘根文件/目录名（恒非空，取判重后根名）
    std::string              magnet_link;   // BT
    std::string              torrent_file;  // BT
    std::string              name;          // 任务显示名称（种子原始名 / HTTP 原始文件名；STATUS_UPDATE 可更新）

    std::vector<int32_t>     file_indexes;
    std::vector<int32_t>     priority_file_indexes; // 优先下载文件索引（不持久化）

    // 队列元数据
    int64_t     created_at  = 0;   // Unix 毫秒
    int64_t     modified_at = 0;   // Unix 毫秒：每次 store_.update 落库自动刷新；事件驱动迁态时同步更新
    int32_t     priority    = 0;   // 越大越优先

    // 最新快照（用于 dw_list_tasks 与断点恢复展示）
    dw_task_status_t status       = DW_TASK_STATUS_QUEUED;
    dw_source_t      source       = DW_SOURCE_TASK_FILE;  // 来源枚举
    bool             dup_checked  = false; // 首次解析的重名判定已完成（持久化）；
                                           // PARSED 事件快路依据，避免重复磁盘冲突检测误判包装
    int64_t          total_size   = -1;
    int64_t          total_done   = -1;
    double           progress     = -1.0;
    int32_t          support_range = 0;  // 服务端 Range 支持：0=不支持（200，单分片全量），1=支持（206，可分片并发/续传）
    std::string      etag;
    std::string      last_modified;

    // 运行态标记（不持久化）
    bool dirty          = false;  // 进度待刷库
    bool synth_notified = false;  // 引擎无 ctx 的合成态（QUEUED/PAUSED）已向上层合成过一帧回调；状态跃迁时复位
    std::string pending_resume;   // 引擎线程经 on_resume_data 暂存的待落库续传数据；B 线程持久化后清空（两引擎异步 resume 通道）

    // 运行态遥测（不持久化）：引擎线程经 on_progress 推入，A 线程节拍读取并转发；
    // 任务离开活跃态转 PAUSED/QUEUED 时归零，避免合成帧残留旧值。
    double      download_rate = 0.0;            // 下载速率（B/s）
    double      upload_rate   = 0.0;            // 上传速率（B/s）；HTTP 恒为 0
    int64_t     total_upload  = 0;              // 累计上传字节（bytes）；HTTP 恒为 0，BT 取自 all_time_upload
    dw_reason_t reason        = DW_REASON_NONE; // 采集到的原因码；仅终态 ERROR 有意义
    std::string message;                        // 采集到的状态 / 错误文本

    // 播放提优信号（dw_set_playing_file 写入，调度器消费；不持久化）
    int32_t playing_file_index  = -1;  // ≥0=待设置 piece deadline 的文件索引
    int64_t playing_byte_offset = 0;   // 播放起始偏移

    // 引擎终态信号（推入侧写，A 线程消费后迁权威态；不持久化）
    dw_task_status_t pending_engine_status = DW_TASK_STATUS_QUEUED; // QUEUED=无终态待消费
};

/**
 * 文件目录记录：UI 渲染主表，1 任务 = 1 行。
 *
 * 存储下载内容的根目录/文件条目，可选关联任务表获取状态与进度。
 * 添加任务时先以 url/infohash 推导占位，PARSED 后修正为真实根名与文件类型。
 * 本地文件也入此表（task_protocol 为 LOCAL，无任务关联时 protocol 取 DW_PROTOCOL_LOCAL）。
 */
struct FileRecord {
    int64_t     id = 0;             // 自增主键（0=未落库）
    std::string client_id;          // 客户端标识
    dw_source_t type = DW_SOURCE_LOCAL_FILE;  // 来源枚举（与 TaskRecord.source 对齐）
    bool        is_remote = false;  // 远程标识
    std::string save_path;          // 保存路径
    std::string root_name;          // 根目录/文件名（占位→修正）
    std::string full_path;          // 磁盘根实体全路径（save_path/root_name，目录与单文件统一公式）；占位空串，PARSED 回填
    bool        file_type = true;   // 0=文件 1=目录
    std::string ext;                // 文件后缀（不含点，如 "mp4"）；目录为空；add 占位空串，PARSED 回填

    // 任务关联（无关联时 protocol=LOCAL、natural_key 为空）
    dw_protocol_t task_protocol = DW_PROTOCOL_LOCAL;
    std::string   task_natural_key; // 关联任务的 natural_key

    // 冗余自 tasks（UI 渲染免 JOIN）
    int32_t status      = 0;        // 任务状态（dw_task_status_t）
    int64_t total_size  = -1;       // 总字节
    int64_t total_done  = 0;        // 已完成字节

    int64_t created_at  = 0;        // Unix 毫秒
    int64_t modified_at = 0;        // Unix 毫秒

    /// 是否关联了下载任务（非本地文件）。
    bool has_task() const {
        return task_protocol != DW_PROTOCOL_LOCAL && !task_natural_key.empty();
    }
};

} // namespace dw
