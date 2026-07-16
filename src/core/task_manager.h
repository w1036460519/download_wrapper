/**
 * @file task_manager.h
 * @brief 库内任务中枢：SQLite 持久化 + 内存注册表 + 优先级就绪队列 + 事件驱动调度。
 *
 * 设计要点：
 *   - 注册表仅常驻排队 / 活跃任务（DOWNLOADING/QUEUED/PARSING/PARSED），
 *     暂停 / 完成 / 错误任务落库后从内存移除，按需经 add/resume/list 回读；
 *     引擎仅持有当前活跃任务的运行时句柄；
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
#include "task_record.h"
#include "task_store.h"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace dw {

class HttpEngine;
class TorrentEngine;

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
    int32_t pause(dw_protocol_t proto, int64_t id, dw_submit_result_t* out);
    int32_t resume(dw_protocol_t proto, int64_t id, dw_submit_result_t* out);
    int32_t remove(dw_protocol_t proto, int64_t id, dw_submit_result_t* out);
    int32_t set_priority(int64_t id, int32_t priority);

    /// 按自增 id 翻译得业务键 task_id（低频；供 BT 工具函数转 info_hash 用）。命中返回 true。
    bool task_id_of(int64_t id, std::string& out_task_id);

    // ---- 回调拦截（post_progress / post_resume_data 调用） ----
    void on_progress(const dw_progress_t* p);
    /// 持久化 resume_data；命中记录返回其自增 id（供上层回调），未命中返回 0。
    int64_t on_resume_data(const char* task_id, dw_protocol_t proto, const uint8_t* data, size_t size);

    // ---- 快照查询 ----
    int32_t list(dw_task_snapshot_t** out_tasks, int32_t* out_count);

    /// 设置流量闸门：allowed=false 时挂起 torrent session 并将活跃 HTTP 任务回落 QUEUED，
    /// 调度线程不再准入新任务；true 时恢复 session 并唤醒调度自动重启排队任务。
    void set_network_allowed(bool allowed);

private:
    // 调度线程主循环
    void scheduler_loop();
    // 准入队列中任务直到占满并发额度（在调度线程，准入 / 合成回调均在释锁后执行）
    void run_schedule(std::unique_lock<std::mutex>& lock);
    // 在引擎启动一个任务（不持 mtx_）；BT 携带 resume_data
    bool start_engine_task(const TaskRecord& r, const std::vector<uint8_t>& resume);
    // 向上层推送一次合成进度（用于 QUEUED/PAUSED）
    void emit_synthetic(const TaskRecord& r);

    // ---- 内部工具 ----
    int32_t active_count_locked() const;      // 占用下载额度的任务数

    std::mutex              mtx_;
    std::condition_variable cv_;
    std::map<std::string, TaskRecord> tasks_;

    TaskStore        store_;                  // 持久化存储层（持有 sqlite3 连接，析构自动关闭）
    std::thread      worker_;
    std::atomic<bool> running_{false};
    bool             schedule_needed_ = false;
    bool             net_allowed_     = true;  // 流量闸门：false=关闭（不准入新任务）；默认开启，不持久化，由调用方重启后重新下发
    int32_t          max_concurrent_  = 3;
    int32_t          flush_interval_ms_ = 1000;

    HttpEngine*      http_    = nullptr;
    TorrentEngine*   torrent_ = nullptr;
    dw_progress_cb   progress_cb_ = nullptr;
};

} // namespace dw

#endif /* DW_TASK_MANAGER_H */