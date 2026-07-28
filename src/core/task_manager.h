/**
 * @file task_manager.h
 * @brief 库内任务中枢：SQLite 持久化 + 内存注册表 + 优先级就绪队列 + 事件驱动调度。
 *
 * 设计要点：
 *   - 注册表仅常驻排队 / 活跃任务（DOWNLOADING/QUEUED），
 *     暂停 / 完成 / 错误任务落库后从内存移除，按需经 add/resume/list 回读；
 *     引擎仅持有当前活跃任务的运行时句柄；
 *   - 进度拉模型：调度线程按固定周期经 query_progress 主动拉取各引擎进度，
 *     应用到记录后统一转发上层；断点续传经 on_resume_data 汇入持久化（触发权下沉引擎：
 *     HTTP 在 query_progress 被采集时自触发，BT 由 save_resume_data_alert 事件驱动）；
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
#include <unordered_map>
#include <vector>

namespace dw {
    class IDownloadEngine;

    /**
     * 任务中枢单例（由 dw_downloader 持有）。
     */
    class TaskManager {
    public:
        TaskManager() = default;

        ~TaskManager();

        TaskManager(const TaskManager &) = delete;

        TaskManager &operator=(const TaskManager &) = delete;

        /// 注入引擎（由 download_wrapper.cpp 在 init 时提供，经统一接口分发）。
        void set_engines(IDownloadEngine *http, IDownloadEngine *torrent);

        /// 设置合成态进度回调（用于 QUEUED/PAUSED 等库自身管理的状态推送）。
        void set_progress_cb(dw_progress_cb cb);

        /// 打开 DB、建表、加载注册表、启动调度线程；恢复既有任务由调度线程按并发上限重新准入。
        int32_t start(const dw_config_t &cfg);

        /// 停止调度线程、最终刷库、关闭 DB。
        void stop();

        // ---- 控制操作（C ABI 转发到此） ----
        int32_t add(dw_protocol_t proto, const dw_task_params_t *params, dw_submit_result_t *out);

        int32_t pause(dw_protocol_t proto, int64_t id, dw_submit_result_t *out);

        int32_t resume(dw_protocol_t proto, int64_t id, dw_submit_result_t *out);

        int32_t remove(dw_protocol_t proto, int64_t id, dw_submit_result_t *out);

        int32_t set_priority(int64_t id, int32_t priority);

        /// 确认文件选择（BT 专用；HTTP 恒视为已确认直接成功）。
        /// file_indexes=NULL 或 count<=0 表示下载全部文件。写选择并落库；
        /// 任务处于 DOWNLOADING（已就绪）时锁外立即应用选择，RESOLVING/QUEUED
        /// 仅落库交调度节拍处理，PAUSED（待选择）/ERROR 重新入队等待准入。
        int32_t confirm_file_selection(int64_t id, const int32_t *file_indexes,
                                       int32_t count, dw_submit_result_t *out);

        /// 按自增 id 回读引擎识别键（HTTP=url，BT=info_hash）：先查常驻内存，未命中回落 DB。
        /// 供低频 BT 工具函数（磁力/优先级/文件列表）定位引擎句柄。命中返回 true。
        bool engine_key_of(int64_t id, std::string &out_key);

        /// 按自增 id 回读引擎识别键与协议：先查常驻内存，未命中回落 DB。命中返回 true。
        /// 供边下边播多协议分发（区间查询 / 提优）定位目标引擎。
        bool engine_ref_of(int64_t id, std::string &out_key, dw_protocol_t &out_proto);

        // ---- 边下边播缓存（直落 task_store，与协议无关） ----

        /// 写入 / 覆盖文件播放进度（毫秒）。
        void set_play_position(int64_t id, int32_t file_index, int64_t position_ms);

        /// 读取文件播放进度（毫秒）；无记录返回 0。
        int64_t get_play_position(int64_t id, int32_t file_index);

        /// 读取某文件已下载区间快照（任务未加载进引擎时的播放兜底）；无记录返回空 vector。
        std::vector<dw_byte_range_t> load_segments(int64_t id, int32_t file_index);

        // ---- 回调拦截（post_resume_data 调用） ----
        /// 持久化 resume_data；命中内存记录返回其自增 id（供上层回调），非激活任务丢弃返回 0。
        int64_t on_resume_data(const char *engine_key, dw_protocol_t proto, const uint8_t *data, size_t size);

        // ---- 回调拦截（post_task_files 调用） ----
        /// 引擎元数据就绪推送的节点树：按 engine_key 定位 id 后全量落库。
        void on_task_files(const char *engine_key, dw_protocol_t proto,
                           const dw_file_info_t *files, int32_t count);

        // ---- 唯一名定名（add 内部与引擎 request_unique_name 上调共用） ----
        /// 持 mtx_ 取占用名集（磁盘 ∪ tasks 表同 save_path）→ 解唯一名 → 回写任务
        /// name/filename 并立即落库（持久预留）。返回定名后的 basename；
        /// 任务未知时仅解唯一名返回不落库。
        std::string resolve_and_record_name(const char *engine_key, dw_protocol_t proto,
                                            const std::string &dir, const std::string &name);

        // ---- 快照查询 ----
        int32_t list(dw_task_snapshot_t **out_tasks, int32_t *out_count);

        /// 设置流量闸门：allowed=false 时逐任务暂停所有活跃下载（BT/HTTP）并回落 QUEUED，
        /// 调度线程不再准入新任务；true 时唤醒调度按 QUEUED→准入路径自动重启。
        void set_network_allowed(bool allowed);

        // ---- 任务文件持久化 ----

        /// 保存任务的文件信息到数据库（key 为自增 id）。
        void save_files(int64_t id, const std::vector<dw_file_info_t> &files);

        /// 从数据库加载任务的文件列表（key 为自增 id）。
        std::vector<dw_file_info_t> load_files(int64_t id);

    private:
        // RESOLVING 校验动作（下载前统一关卡）：A 线程锁内收集，锁外执行引擎调用，
        // 通过后回锁迁 DOWNLOADING（未通过保持 RESOLVING 下拍再查）。
        struct ResolveAction {
            TaskRecord rec; // 锁内记录快照（HTTP 已含定名后 filename，供 start_engine_task）
            bool naming_done; // 文件表已有记录（判重定名凭证）：BT 跳过 finalize_naming
            std::vector<uint8_t> resume; // HTTP 续传存档（BT 引擎侧 add 时已装载，恒空）
        };

        // HTTP 完成拍改名动作：服务器建议名与定名不一致，判重后锁外 fs::rename，
        // 回锁按结果落库（成功用新名，失败保持原名）并补写文件表凭证。
        struct RenameAction {
            int64_t id;
            std::string dir; // 文件所在目录（save_path）
            std::string old_name; // 当前定名
            std::string new_name; // 服务器建议名判重后的唯一名
        };

        // A 线程（轻量）：周期采集。仅 query（纯读）+ 改内存 + 转发回调，不落库 / 不快照 / 不移除 / 不 sweep。
        void scheduler_loop();

        // A 线程采集（持 mtx_，单段）：遍历下载中/解析中任务，query_progress 纯读可持锁直接调用；
        // 回填内存进度/元数据/遥测、暂存续传至 pending_resume、置终态权威态，并收集待转发记录（已含遥测）；
        // 另收集 RESOLVING 校验动作与 HTTP 完成拍改名动作，由 scheduler_loop 锁外执行。
        // 落库 / 区间快照 / 注销均延后到 B 线程。
        void collect_progress_locked(std::vector<TaskRecord> &fwd_records,
                                     std::vector<ResolveAction> &resolve_actions,
                                     std::vector<RenameAction> &rename_actions);

        // B 线程（重载）：较长节拍或被 schedule 唤醒，持锁完成持久化 / 区间快照 / 终态注销 / 准入，随后锁外 sweep。
        void maintenance_loop();

        // B 线程持锁持久化：为下载中 / 终态任务落区间快照（引擎 ctx 尚在），刷写脏进度与暂存续传，最后注销终态任务。
        void maintenance_persist_locked();

        // 统一进度转发：由记录（权威态 + 采集遥测）构造 dw_progress_t 发上层（不持 mtx_）；
        // remaining/eta 由 total_size/total_done/download_rate 现算。QUEUED/PAUSED 合成帧同走此路径，
        // 遥测字段已在状态迁移时归零。
        void emit_progress(dw_progress_cb cb, const TaskRecord &rec);

        // 准入队列中任务直到占满并发额度（在调度线程，准入操作均在释锁后执行）
        void run_schedule(std::unique_lock<std::mutex> &lock);

        // 在引擎启动一个任务（不持 mtx_）；BT 携带 resume_data
        bool start_engine_task(const TaskRecord &task_record, const std::vector<uint8_t> &resume);

        // 复位运行态遥测（速率/探测/原因/消息）：任务离开活跃态转 PAUSED/QUEUED 时调用，避免合成帧残留旧速率。
        static void reset_live_telemetry(TaskRecord &rec);

        // ---- 内部工具 ----
        int32_t active_count_locked() const; // 占用下载额度的任务数
        void flush_dirty_locked(); // 刷写全部脏任务进度 + A 线程暂存续传到库（假定已持 mtx_）

        // 按协议取引擎（统一接口分发点；HTTP/BT 之外无其他协议）
        IDownloadEngine *engine_of(dw_protocol_t proto) const;

        // 定名落库（假定已持 mtx_）：取占用名集 → 解唯一名 → 回写 rec.name/filename 并 update。
        std::string resolve_and_record_name_locked(TaskRecord &rec,
                                                   const std::string &dir,
                                                   const std::string &name);

        // HTTP 单文件任务的文件表凭证（假定已持 mtx_）：写单条文件节点（index=0，根层）。
        // 存在记录即已判重定名，RESOLVING 校验拍据此跳过定名环节。
        void save_http_file_entry_locked(int64_t id, const std::string &filename,
                                         int64_t total_size, int32_t status);

        // 内存索引维护（tasks_ 以自增 id 为键，另维护两张自然键反查表，增删三者严格同步防悬置）
        void register_task(TaskRecord task_record); // 登记：写 tasks_ 并按协议登记 url→id / info_hash→id
        void unregister_task(int64_t id); // 注销：清 tasks_ 及其反查表项
        // 引擎回调自然键 → 自增 id：HTTP 查 url_index_，BT 查 info_hash_index_；未命中返回 0（须持锁调用）
        int64_t id_of_engine_key(dw_protocol_t proto, const std::string &key) const;

        // 错误任务重新入队前的残留态清理：两协议均清 resume_data + file_segments；
        // HTTP 额外复位进度令其从零重下并避免 UI 残留旧进度。非错误态直接跳过。
        void reset_error_task_for_restart(TaskRecord &task_record);

        // 将活引擎的已下载连续区间快照落库（HTTP 单文件 index=0，BT 遍历已选 file_indexes）；
        // 供任务未加载进引擎时的播放兜底。假定已持 mtx_，仅短暂访问引擎自有锁，无死锁。
        void snapshot_segments_locked(const TaskRecord &task_record);

        std::mutex mtx_;
        std::condition_variable cv_;
        std::map<int64_t, TaskRecord> tasks_; // 注册表：自增 id → 记录（仅常驻活跃/排队任务）
        std::unordered_map<std::string, int64_t> url_index_; // HTTP 自然键反查：url → id
        std::unordered_map<std::string, int64_t> info_hash_index_; // BT 自然键反查：info_hash → id

        TaskStore store_; // 持久化存储层（持有 sqlite3 连接，析构自动关闭）
        std::thread worker_; // A 线程：轻量采集 + 回调
        std::thread maintenance_; // B 线程：持久化 + 区间快照 + 终态注销 + 准入 + sweep
        std::atomic<bool> running_{false};
        bool schedule_needed_ = false; // 调度线程需被唤醒
        bool net_allowed_ = true; // 流量闸门：false=关闭（不准入新任务）；默认开启，不持久化，由调用方重启后重新下发
        int32_t max_concurrent_ = 3;
        int32_t flush_interval_ms_ = 1000; // 数据采集/回调节拍
        int32_t maintenance_interval_ms_ = 2000; // 内存数据持久化节拍
        int64_t resume_checkpoint_interval_ms_ = 5000;  // 恢复数据采集节拍
        int64_t last_resume_checkpoint_ms_ = 0;

        IDownloadEngine *http_ = nullptr;
        IDownloadEngine *torrent_ = nullptr;
        dw_progress_cb progress_cb_ = nullptr;
    };
} // namespace dw

#endif /* DW_TASK_MANAGER_H */
