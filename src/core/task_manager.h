/**
 * @file task_manager.h
 * @brief 库内任务中枢：SQLite 持久化 + 内存注册表 + 优先级就绪队列 + 事件驱动调度。
 *
 * 设计要点：
 *   - 注册表持有全部任务（含暂停/完成/排队），引擎仅持有当前活跃任务；
 *   - 所有状态经 dw::post_progress 汇入 on_progress，断点续传经 on_resume_data 汇入，
 *     是持久化的天然拦截点，引擎内部零改动；
 *   - 并发准入：活跃任务数 < max_concurrent_downloads 才准入下载，其余置 QUEUED；
 *   - 调度纯事件驱动：状态跃迁释放许可 / 新增 / 恢复 / 调整优先级时唤醒调度线程，
 *     调度线程另按固定周期刷写脏进度到库（持久化与调度职责分离）；
 *   - 引擎启动动作统一在调度线程执行，规避回调线程重入。
 */

#ifndef DW_TASK_MANAGER_H
#define DW_TASK_MANAGER_H

#include "download_wrapper/download_wrapper.h"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

struct sqlite3;

namespace dw {

class HttpEngine;
class TorrentEngine;

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
    int32_t                  auto_start = 1;

    // 队列元数据
    int64_t created_at = 0;  // Unix 毫秒
    int64_t submit_seq = 0;  // 单调递增，同优先级下的 FIFO 次序
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

/**
 * 任务中枢单例（由 dw_downloader 持有）。
 */
class TaskManager {
public:
    TaskManager() = default;
    ~TaskManager();

    TaskManager(const TaskManager&)            = delete;
    TaskManager& operator=(const TaskManager&) = delete;

    /// 注入引擎（由 download_wrapper.cpp 在 init 时提供）。
    void set_engines(HttpEngine* http, TorrentEngine* torrent);
    /// 设置合成态进度回调（用于 QUEUED/PAUSED 等库自身管理的状态推送）。
    void set_progress_cb(dw_progress_cb cb);

    /// 打开 DB、建表、加载注册表、启动调度线程；恢复既有任务由调度线程按并发上限重新准入。
    int32_t start(const dw_config_t& cfg);
    /// 停止调度线程、最终刷库、关闭 DB。
    void stop();

    // ---- 控制操作（C ABI 转发到此） ----
    int32_t add(dw_protocol_t proto, const dw_task_params_t* params, dw_submit_result_t* out);
    int32_t pause(dw_protocol_t proto, const char* id, dw_submit_result_t* out);
    int32_t resume(dw_protocol_t proto, const dw_task_params_t* params, dw_submit_result_t* out);
    int32_t remove(dw_protocol_t proto, const char* id, dw_submit_result_t* out);
    int32_t set_priority(const char* id, int32_t priority);

    // ---- 回调拦截（post_progress / post_resume_data 调用） ----
    void on_progress(const dw_progress_t* p);
    void on_resume_data(const char* id, dw_protocol_t proto, const uint8_t* data, size_t size);

    // ---- 快照查询 ----
    int32_t list(dw_task_snapshot_t** out_tasks, int32_t* out_count);

private:
    // 调度线程主循环
    void scheduler_loop();
    // 准入队列中任务直到占满并发额度（在调度线程，准入 / 合成回调均在释锁后执行）
    void run_schedule(std::unique_lock<std::mutex>& lock);
    // 在引擎启动一个任务（不持 mtx_）；BT 携带 resume_data
    bool start_engine_task(const TaskRecord& r, const std::vector<uint8_t>& resume);
    // 向上层推送一次合成进度（用于 QUEUED/PAUSED）
    void emit_synthetic(const TaskRecord& r);

    // ---- DB 辅助（均在持有 mtx_ 时调用） ----
    bool db_open(const std::string& path);
    void db_close();
    void db_init_schema();
    void db_load_all();                       // 载入注册表
    void db_upsert(const TaskRecord& r);      // 全量 upsert
    void db_update_progress(const TaskRecord& r); // 仅进度/状态快照
    void db_delete(const std::string& task_id);
    void db_save_resume(const std::string& task_id, const uint8_t* data, size_t size);
    std::vector<uint8_t> db_load_resume(const std::string& task_id);

    // ---- 内部工具 ----
    int32_t active_count_locked() const;      // 占用下载额度的任务数

    std::mutex              mtx_;
    std::condition_variable cv_;
    std::map<std::string, TaskRecord> tasks_;

    sqlite3*         db_ = nullptr;
    std::thread      worker_;
    std::atomic<bool> running_{false};
    bool             schedule_needed_ = false;
    int64_t          seq_counter_     = 0;
    int32_t          max_concurrent_  = 3;
    int32_t          flush_interval_ms_ = 1000;

    HttpEngine*      http_    = nullptr;
    TorrentEngine*   torrent_ = nullptr;
    dw_progress_cb   progress_cb_ = nullptr;
};

} // namespace dw

#endif /* DW_TASK_MANAGER_H */
