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

/**
 * 任务键类型：决定 natural_key 的语义。同步用作 SQLite 库内 key_type 列。
 */
enum class TaskKeyType : int32_t {
    HTTP  = 0,  // natural_key = url
    BT    = 1,  // natural_key = info_hash
    LOCAL = 2,  // natural_key = content_root（本地文件任务；filepath 不变只删不挪）
};

inline const char* to_string(TaskKeyType t) {
    switch (t) {
    case TaskKeyType::HTTP:  return "HTTP";
    case TaskKeyType::BT:    return "BT";
    case TaskKeyType::LOCAL: return "LOCAL";
    }
    return "UNKNOWN";
}

/**
 * 任务唯一键：(clientId, key_type, natural_key) 三元组。
 *
 * - clientId：App 启动时注入（UUIDv4，SharedPreferences 持久化）。
 *   本地任务也强制填写本机 clientId；远程同步场景下保留远端 clientId 用于多客户端隔离。
 * - key_type：协议 / 来源类别，决定 natural_key 字段语义。
 * - natural_key：业务自然键。HTTP 取 url，BT 取 info_hash，本地任务取 content_root。
 *
 * 作为 std::unordered_map 的 key，必须定义 hash + 相等。
 */
struct TaskKey {
    std::string client_id;
    TaskKeyType key_type = TaskKeyType::HTTP;
    std::string natural_key;

    bool operator==(const TaskKey& o) const noexcept {
        return key_type == o.key_type &&
               client_id == o.client_id &&
               natural_key == o.natural_key;
    }
};

struct TaskKeyHash {
    size_t operator()(const TaskKey& k) const noexcept {
        // 三段独立 hash 后折叠，避免单段被覆盖到同一桶
        const size_t h1 = std::hash<std::string>{}(k.client_id);
        const size_t h2 = std::hash<int>{}(static_cast<int>(k.key_type));
        const size_t h3 = std::hash<std::string>{}(k.natural_key);
        size_t h = h1;
        h ^= h2 + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
        h ^= h3 + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
        return h;
    }
};

/**
 * 任务记录：注册表内存态 = 来源参数（恢复/晋升重建用）+ 最新快照 + 队列元数据。
 *
 * 主键为 TaskKey，原 task_id 字段已移除。
 */
struct TaskRecord {
    // 主键（同时为 PK 与内存 map 的 key）
    TaskKey     task_key;

    // 协议标识（冗余 key_type 但便于 switch；HTTP/BT 由 key_type 决定）
    dw_protocol_t protocol = DW_PROTOCOL_HTTP;

    // 来源参数（恢复 / 队列晋升时重建引擎任务）
    std::string              save_path;     // 用户指定的保存目录（固定不变）
    std::string              content_root;  // save_path 下的实际根目录名（物理路径 = save_path/content_root）
                                             // BT PARSED 时由文件结构分析确定；HTTP 由定名流程写入。
                                             // 空 = 尚未定名（PARSED 前）。
                                             // 本地任务的 natural_key 也指向此字段。
    std::string              filename;      // content_root 内的主文件名（含后缀），BT 多文件任务可空
    std::string              url;           // HTTP 身份 + 下载地址（同时为 natural_key）
    std::string              info_hash;     // BT 身份（同时为 natural_key），HTTP 该字段为空
    std::string              magnet_link;   // BT
    std::string              torrent_file;  // BT
    std::string              name;          // 任务显示名称（种子原始名 / HTTP 原始文件名；STATUS_UPDATE 可更新）

    std::vector<std::string> trackers;
    std::vector<int32_t>     file_indexes;

    // 队列元数据
    int64_t     created_at  = 0;   // Unix 毫秒
    int64_t     modified_at = 0;   // Unix 毫秒：每次 store_.update 落库自动刷新；事件驱动迁态时同步更新
    int32_t     priority    = 0;   // 越大越优先

    // 最新快照（用于 dw_list_tasks 与断点恢复展示）
    dw_task_status_t status       = DW_TASK_STATUS_QUEUED;
    int32_t          source       = 0;  // 0=本地任务 1=本地文件 2=远程文件
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

    // BT 扩展遥测（引擎推入，A 线程校验拍判断用；不持久化）
    bool bt_metadata_ready = false; // 元数据是否就绪（has_metadata）
    bool bt_multi_file     = false; // 多文件 torrent（name 为根目录名）

    // 播放提优信号（dw_set_playing_file 写入，调度器消费；不持久化）
    int32_t playing_file_index  = -1;  // ≥0=待设置 piece deadline 的文件索引
    int64_t playing_byte_offset = 0;   // 播放起始偏移

    // 引擎终态信号（推入侧写，A 线程消费后迁权威态；不持久化）
    dw_task_status_t pending_engine_status = DW_TASK_STATUS_QUEUED; // QUEUED=无终态待消费
};

} // namespace dw


