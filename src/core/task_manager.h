/**
 * @file task_manager.h
 * @brief 库内任务中枢：SQLite 持久化 + 内存注册表 + 优先级就绪队列 + 事件驱动调度。
 *
 * 设计要点：
 *   - 注册表仅常驻排队 / 活跃任务（DOWNLOADING/QUEUED），
 *     暂停 / 完成 / 错误任务落库后从内存移除，按需经 add/resume/list 回读；
 *     引擎仅持有当前活跃任务的运行时句柄；
 *   - 任务主键为 TaskKey（client_id + key_type + natural_key），注册表与持久化层共享同一 key；
 *   - 事件驱动模型：引擎经 post_engine_event 投递事件（STATUS_UPDATE/PARSED/DOWNLOAD_FAILED/
 *     DOWNLOAD_COMPLETED），B 线程消费并更新 TaskRecord 内存，A 线程按固定周期读取并转发上层；
 *   - 断点续传经 on_resume_data 汇入持久化（HTTP worker 线程自推 / BT save_resume_data_alert 事件驱动）；
 *   - 并发准入：活跃任务数 < max_concurrent_downloads 才准入下载，其余置 QUEUED；
 *   - 调度纯事件驱动：状态跃迁释放许可 / 新增 / 恢复 / 调整优先级时唤醒调度线程；
 *   - 引擎启动动作统一在调度线程执行，规避回调线程重入。
 */

#pragma once

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

#include <boost/asio.hpp>

namespace dw {
    class IDownloadEngine;
    struct EngineEvent;

    /**
     * 任务中枢单例（由 dw_downloader 持有）。
     */
    class TaskManager {
    public:
        TaskManager() = default;

        ~TaskManager();

        TaskManager(const TaskManager &) = delete;

        TaskManager &operator=(const TaskManager) = delete;

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

        int32_t pause(dw_protocol_t proto, const TaskKey &key, dw_submit_result_t *out);

        int32_t resume(dw_protocol_t proto, const TaskKey &key, dw_submit_result_t *out);

        /// 删除任务：移除记录 / 注销内存 / 引擎释放运行时资源；delete_files 非 0 时
        /// 登记待删文件项，由 B 线程在引擎确认资源释放（task_released）后删除落盘
        /// 产物（仅尝试一次，成败均不影响任务删除本身）。
        int32_t remove(dw_protocol_t proto, const TaskKey &key, int32_t delete_files,
                       dw_submit_result_t *out);

        int32_t set_priority(const TaskKey &key, int32_t priority);

        /// 设置播放提优标识：写入 playing_file_index/byte_offset，唤醒调度器。
        /// 调度器负责任务准入与 piece deadline 设置，API 层不直接操作引擎。
        /// 任务不在内存时从 DB 加载并重新登记；已完成任务拒绝（返回 false）。
        /// 非 DOWNLOADING 态任务转 QUEUED 等待调度器准入。
        /// @return true=成功设置，false=任务不存在或已完成。
        bool set_playing(const TaskKey &key, int32_t file_index, int64_t byte_offset);

        /// 按 TaskKey 回读引擎识别键（HTTP=url，BT=info_hash）：先查常驻内存，未命中回落 DB。
        /// 供低频 BT 工具函数（磁力/优先级/文件列表）定位引擎句柄。命中返回 true。
        bool engine_key_of(const TaskKey &key, std::string &out_key);

        /// 按 TaskKey 回读引擎识别键与协议：先查常驻内存，未命中回落 DB。命中返回 true。
        /// 供边下边播多协议分发（区间查询 / 提优）定位目标引擎。
        bool engine_ref_of(const TaskKey &key, std::string &out_key, dw_protocol_t &out_proto);

        // ---- 边下边播缓存（直落 task_store，与协议无关） ----

        /// 写入 / 覆盖文件播放进度（毫秒）。
        void set_play_position(const TaskKey &key, int32_t file_index, int64_t position_ms);

        /// 读取文件播放进度（毫秒）；无记录返回 0。
        int64_t get_play_position(const TaskKey &key, int32_t file_index);

        /// 读取某文件已下载区间快照（任务未加载进引擎时的播放兜底）；无记录返回空 vector。
        std::vector<dw_byte_range_t> load_segments(const TaskKey &key, int32_t file_index);

        // ---- 分段内存缓存（STATUS_UPDATE 事件时从引擎拉取并缓存） ----

        /// 读取缓存的已下载区间（代理热路径，无需查引擎或 DB）；未缓存返回空 vector。
        std::vector<dw_byte_range_t> get_cached_segments(const TaskKey &key, int32_t file_index);

        /// 查询任务当前状态（mtx_ 保护）；任务不存在返回 -1。
        int32_t get_task_status(const TaskKey &key);

        // ---- 回调拦截（post_resume_data 调用） ----
        /// 持久化 resume_data；命中内存记录返回其 TaskKey（供上层回调），非激活任务丢弃返回空 TaskKey。
        TaskKey on_resume_data(const char *engine_key, dw_protocol_t proto, const uint8_t *data, size_t size);

        // ---- 回调拦截（post_task_files 调用） ----
        /// 引擎元数据就绪推送的节点树：按 engine_key 定位 TaskKey 后全量落库。
        void on_task_files(const char *engine_key, dw_protocol_t proto,
                           const dw_file_info_t *files, int32_t count);

        // ---- 回调拦截（post_task_file_update 调用） ----
        /// 引擎按需推送单文件进度：按 engine_key 定位 TaskKey 后单条 upsert task_files。
        /// 元数据（name/ext/offset）保持空，全量 save_task_files 时由引擎补齐。
        void on_task_file_update(const char *engine_key, dw_protocol_t proto,
                                 int32_t file_index, int64_t downloaded_bytes, int64_t total_size);

        // ---- 引擎事件消费（Boost.Asio 事件投递入口） ----
        /// 引擎 alert 经 Boost.Asio io_context::post 投递到此，B 线程消费。
        /// 事件经值语义拷贝后投递，线程安全。
        void on_engine_event(EngineEvent event);

        // ---- 唯一名定名（add 内部与引擎 request_unique_name 上调共用） ----
        /// 持 mtx_ 抢占唯一 wrapper 名：以磁盘为唯一判重真相源（不查库），候选名未
        /// 被占用即立即创建 wrapper 目录物化占位（"定名即持有"），冲突则整名尾部
        /// 自增 (n) 作为 wrapper 名。随后回写 name=wrapper、save_path=原 dir（不变）、
        /// filename=inner_name（仅在 !multi_file 时有值）并落库。
        /// 返回 wrapper 名（可能含 (n) 后缀）；任务未知时仅抢名返回不落库。
        /// @param wrapper_name 期望的 wrapper 名（basename，去后缀）。
        /// @param inner_name   内部文件名（HTTP/BT 单文件，含后缀）；BT 多文件传空。
        /// @param multi_file   是否多文件 BT（仅决定 filename 是否被记录）。
        std::string resolve_and_record_name(const char *engine_key, dw_protocol_t proto,
                                            const std::string &dir, const std::string &wrapper_name,
                                            const std::string &inner_name, bool multi_file);

        // ---- 快照查询 ----
        int32_t list(dw_task_snapshot_t **out_tasks, int32_t *out_count);

        /// 设置流量闸门：allowed=false 时逐任务暂停所有活跃下载（BT/HTTP）并回落 QUEUED，
        /// 调度线程不再准入新任务；true 时唤醒调度按 QUEUED→准入路径自动重启。
        void set_network_allowed(bool allowed);

        // ---- 任务文件持久化 ----

        /// 从数据库加载任务的文件列表（key 为 TaskKey）。
        std::vector<dw_file_info_t> load_files(const TaskKey &key);

        // ---- 本地文件浏览与管理 ----

        /// 增量扫描本地文件任务：扫描目录，仅添加新文件（不删除旧记录），返回新增任务快照。
        int32_t scan_local_tasks(const std::string &save_path,
                                  dw_task_snapshot_t **out_tasks,
                                  int32_t *out_count);

        /// 校验本地文件任务的存在性：物理文件不存在则标记为 INVALIDATED。
        int32_t validate_local_tasks(const std::string &save_path,
                                      int32_t *out_invalidated_count);

        /// 全量清理指定 save_path 下的非下载任务（source IN (1,2)）：DB + 物理文件。
        int32_t clear_local_tasks(const std::string &save_path);

        /// 删除单个本地文件任务（source=1）：仅 DB + 磁盘清理，不涉及 engine 层。
        /// 下载任务（source=0）拒绝，应走 dw_delete_task。
        int32_t delete_local_entry(const TaskKey &key);

        // ---- 路径与展示辅助（静态，不依赖实例态） ----

        /// 根据 TaskRecord 计算磁盘根路径。
        /// 统一为 save_path / name（name 即 wrapper 名，去后缀，冲突时含 (n) 后缀）。
        static std::string disk_root_path(const TaskRecord &rec);

        /// 根据 TaskRecord 计算展示名：name（已含可能的去重后缀）。
        static std::string display_name(const TaskRecord &rec);

        /// 当前本机 clientId（init 注入，跨所有方法使用）。
        const std::string& client_id() const { return client_id_; }

        // ---- 内部访问器（供同库模块经持锁快照访问持久化层） ----

        /// 返回内部互斥锁引用，供调用方持锁期间安全访问 store_。
        std::mutex& get_mutex() { return mtx_; }
        /// 返回持久化存储层引用（调用方须持 mtx_ 保证线程安全）。
        TaskStore& get_store() { return store_; }

    private:
        // 校验动作（下载前统一关卡）：A 线程锁内收集，锁外执行引擎调用，
        // 通过后回锁迁 DOWNLOADING（未通过保持原态下拍再查）。
        struct ResolveAction {
            TaskRecord rec; // 锁内记录快照
            std::vector<uint8_t> resume; // HTTP 续传存档（BT 引擎侧 add 时已装载，恒空）
        };

        // A 线程采集（持 mtx_，单段）：遍历下载中/解析中任务，直接读 TaskRecord 已被引擎推入
        // 的进度字段；判终态 → 置 schedule_needed_ → 收集 RESOLVING 校验动作 → push 转发记录。
        // 落库 / 区间快照 / 注销均延后到 B 线程。
        void collect_progress_locked(std::vector<TaskRecord> &fwd_records,
                                     std::vector<ResolveAction> &resolve_actions);
        // A 线程（轻量）：周期遍历内存 + 转发回调，不落库 / 不快照 / 不移除 / 不 sweep。
        void scheduler_loop();

        // B 线程（重载）：较长节拍或被 schedule 唤醒，持锁完成持久化 / 区间快照 / 终态注销 / 准入，随后锁外 sweep。
        void maintenance_loop();

        // B 线程消费单个引擎事件（PARSED/STORAGE_MOVED/STORAGE_MOVE_FAILED）。
        void consume_engine_event(EngineEvent event);

        // 待删文件项：remove 登记，B 线程在引擎确认资源释放后删除。
        struct PendingFileDelete {
            dw_protocol_t proto; // 释放确认走对应引擎的 task_released
            std::string   path;  // 待删根路径（save_path/filename），单文件与目录树均适用
            bool placeholder_only; // true=仅回收空占位（delete_files=0 时保留用户数据）
        };

        // B 线程锁外调用：对每个待删项确认引擎资源已释放后 remove_all（仅尝试一次，
        // 成败均出队）；force=true 跳过释放确认（停机兜底尽力删除）。
        void process_pending_deletes(bool force = false);

        // B 线程持锁持久化：为下载中 / 终态任务落区间快照（引擎 ctx 尚在），刷写脏进度与暂存续传，最后注销终态任务。
        void maintenance_persist_locked();

        // 统一进度转发：由记录（权威态 + 采集遥测）构造 dw_progress_t 发上层（不持 mtx_）；
        // remaining/eta 由 total_size/total_done/download_rate 现算。QUEUED/PAUSED 合成帧同走此路径，
        // 遥测字段已在状态迁移时归零。
        void emit_progress(dw_progress_cb cb, const TaskRecord &rec);

        // 播放提优动作：run_schedule 收集，maintenance_loop 锁外执行引擎调用。
        struct PlayingAction {
            TaskKey       task_key;
            dw_protocol_t protocol;
            std::string   key;       // 引擎键（BT=info_hash）
            int32_t       file_index;
            int64_t       byte_offset;
        };

        // 准入队列中任务直到占满并发额度（在调度线程，准入操作均在释锁后执行）。
        // 同时处理播放提优：收集 piece deadline 动作交调用方锁外执行。
        void run_schedule(std::unique_lock<std::mutex> &lock,
                          std::vector<PlayingAction> &playing_actions);

        // 播放提优：暂停下载速率最低的 DOWNLOADING 任务释放名额（持 mtx_）。
        // 返回被暂停任务的引擎键与协议，供调用方锁外调 pause_task；无活跃任务返回 false。
        bool pause_slowest_downloading_locked(std::string &out_key, dw_protocol_t &out_proto);

        // 在引擎启动一个任务（不持 mtx_）；BT 携带 resume_data
        bool start_engine_task(const TaskRecord &task_record, const std::vector<uint8_t> &resume);

        // 复位运行态遥测（速率/探测/原因/消息）：任务离开活跃态转 PAUSED/QUEUED 时调用，避免合成帧残留旧速率。
        static void reset_live_telemetry(TaskRecord &rec);

        // ---- 内部工具 ----
        int32_t active_count_locked() const; // 占用下载额度的任务数
        void flush_dirty_locked(); // 刷写全部脏任务进度 + A 线程暂存续传到库（假定已持 mtx_）

        // 按协议取引擎（统一接口分发点；HTTP/BT 之外无其他协议）
        IDownloadEngine *engine_of(dw_protocol_t proto) const;

        // 定名落库（假定已持 mtx_）：抢占唯一 wrapper 名并创建 wrapper 目录占位 →
        // 回写 name=wrapper（可能含 (n) 后缀）、save_path=原 dir（不变）、filename=inner_name
        // （仅在 !multi_file 时记录）并 update。返回 wrapper 名（可能含 (n) 后缀），
        // 调用方据此与原名比较判是否发生去重包层。
        // wrapper_name 重复（同 wrapper 已定名）时为幂等重入：补齐占位物化并原样返回。
        std::string resolve_and_record_name_locked(TaskRecord &rec,
                                                   const std::string &dir,
                                                   const std::string &wrapper_name,
                                                   const std::string &inner_name,
                                                   bool multi_file);

        // 内存注册：仅写 tasks_ 一张 map（key=TaskKey），无冗余索引。
        void register_task(TaskRecord task_record);
        // 注销：清 tasks_ + segment_cache_，统一由 TaskKey 定位。
        void unregister_task(const TaskKey &key);
        // 引擎回调自然键 → TaskKey：HTTP 走 KEY_TYPE_HTTP，BT 走 KEY_TYPE_BT；
        // 用本机 clientId 拼接后 O(1) 查 tasks_，未命中返回空 TaskKey（须持锁调用）。
        TaskKey task_key_of_engine_key(dw_protocol_t proto, const std::string &key) const;

        // 错误任务重新入队前的残留态清理：两协议均清 resume_data + file_segments；
        // HTTP 额外复位进度令其从零重下并避免 UI 残留旧进度。非错误态直接跳过。
        void reset_error_task_for_restart(TaskRecord &task_record);

        // 将活引擎的已下载连续区间快照落库（HTTP 单文件 index=0，BT 遍历已选 file_indexes）；
        // 供任务未加载进引擎时的播放兜底。假定已持 mtx_，仅短暂访问引擎自有锁，无死锁。
        // 同时更新 segment_cache_ 内存缓存，并检查文件/任务级完成条件。
        void snapshot_segments_locked(TaskRecord &task_record);

        std::mutex mtx_;
        std::condition_variable cv_;
        // 任务主表：TaskKey → TaskRecord。仅常驻活跃/排队任务，暂停/完成/错误
        // 落库后由 unregister_task 清出。
        std::unordered_map<TaskKey, TaskRecord, TaskKeyHash> tasks_;
        // 待删文件：engine key → 项（mtx_ 保护）
        std::unordered_map<std::string, PendingFileDelete> pending_deletes_;

        // Boost.Asio 事件队列：引擎 alert 经此投递，B 线程 maintenance_loop 中 poll 消费。
        boost::asio::io_context event_ioc_;

        // 分段内存缓存：TaskKey → (file_index → ranges)。STATUS_UPDATE 事件时从引擎拉取更新，
        // dw_get_file_ranges 优先读此缓存（下载中任务不回退 DB，避免过时快照误导代理）。
        std::unordered_map<TaskKey, std::map<int32_t, std::vector<dw_byte_range_t>>, TaskKeyHash> segment_cache_;

        TaskStore store_; // 持久化存储层（持有 sqlite3 连接，析构自动关闭）
        std::thread worker_; // A 线程：轻量采集 + 回调
        std::thread maintenance_; // B 线程：持久化 + 区间快照 + 终态注销 + 准入 + sweep
        std::atomic<bool> running_{false};
        bool schedule_needed_ = false; // 调度线程需被唤醒
        bool net_allowed_ = true; // 流量闸门：false=关闭（不准入新任务）；默认开启，不持久化，由调用方重启后重新下发
        int32_t max_concurrent_ = 3;
        int32_t flush_interval_ms_ = 1000; // 数据采集/回调节拍
        int32_t maintenance_interval_ms_ = 2000; // 内存数据持久化节拍

        IDownloadEngine *http_ = nullptr;
        IDownloadEngine *torrent_ = nullptr;
        dw_progress_cb progress_cb_ = nullptr;
        std::string client_id_; // App 启动时注入的 UUIDv4
    };
} // namespace dw


