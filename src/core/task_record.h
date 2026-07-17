/**
 * @file task_record.h
 * @brief 任务记录数据结构：任务中枢（TaskManager）与持久化存储层（TaskStore）共享的内存态。
 *
 * 独立成头，供 task_manager.h 与 task_store.h 各自 include，避免二者相互包含形成循环依赖。
 */

#ifndef DW_TASK_RECORD_H
#define DW_TASK_RECORD_H

#include "download_wrapper/download_wrapper.h"

#include <cstdint>
#include <string>
#include <vector>

namespace dw {

/**
 * 任务记录：注册表内存态 = 来源参数（恢复/晋升重建用）+ 最新快照 + 队列元数据。
 */
struct TaskRecord {
    // 标识
    std::string   task_id;
    dw_protocol_t protocol = DW_PROTOCOL_HTTP;

    // 来源参数（恢复 / 队列晋升时重建引擎任务）
    std::string              save_path;
    std::string              filename;
    std::string              url;           // HTTP
    std::string              magnet_link;   // BT
    std::string              torrent_file;  // BT
    std::vector<std::string> trackers;
    std::vector<int32_t>     file_indexes;
    std::vector<dw_part_state_t> parts;      // HTTP 分片续传态（仅 index/start/end/done 持久化）

    // 队列元数据
    int64_t created_at = 0;  // Unix 毫秒
    int64_t id         = 0;  // 数据库自增主键；同优先级下按 id 升序即 FIFO；0=尚未落库
    int32_t priority   = 0;  // 越大越优先

    // 最新快照（用于 dw_list_tasks 与断点恢复展示）
    dw_task_status_t status       = DW_TASK_STATUS_QUEUED;
    std::string      name;
    int64_t          total_size   = -1;
    int64_t          total_done   = -1;
    double           progress     = -1.0;
    int32_t          support_range = 0;
    std::string      etag;
    std::string      last_modified;

    // 运行态标记（不持久化）
    bool dirty          = false;  // 进度待刷库
    bool queued_notified = false; // 已向上层合成过 QUEUED 回调
};

} // namespace dw

#endif /* DW_TASK_RECORD_H */
