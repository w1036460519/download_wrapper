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
    std::string   info_hash;               // BT 身份（info_hash）；HTTP 该字段为空，身份见 url
    dw_protocol_t protocol = DW_PROTOCOL_HTTP;

    // 来源参数（恢复 / 队列晋升时重建引擎任务）
    std::string              save_path;     // 最终目录（引擎下载到临时目录，完成后移出到此）
    std::string              filename;
    std::string              url;           // HTTP 身份 + 下载地址
    std::string              magnet_link;   // BT
    std::string              torrent_file;  // BT

    std::vector<std::string> trackers;
    std::vector<int32_t>     file_indexes;

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
    bool synth_notified = false;  // 引擎无 ctx 的合成态（QUEUED/PAUSED）已向上层合成过一帧回调；状态跃迁时复位
    std::string pending_resume;   // 引擎线程经 on_resume_data 暂存的待落库续传数据；B 线程持久化后清空（两引擎异步 resume 通道）

    // 运行态遥测（不持久化）：A 线程每次采集写入，转发回调直接投影；
    // 任务离开活跃态转 PAUSED/QUEUED 时归零，避免合成帧残留旧值。
    std::string output_path;                    // 引擎回报的当前物理目录（下载期=临时目录，移出后=最终目录）；空则回退 save_path
    double      download_rate = 0.0;            // 下载速率（B/s）
    double      upload_rate   = 0.0;            // 上传速率（B/s）；HTTP 恒为 0
    dw_reason_t reason        = DW_REASON_NONE; // 采集到的原因码；仅终态 ERROR 有意义
    std::string message;                        // 采集到的状态 / 错误文本
};

} // namespace dw

#endif /* DW_TASK_RECORD_H */
