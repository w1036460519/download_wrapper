/**
 * @file task_manager.cpp
 * @brief 库内任务中枢实现：SQLite 持久化 + 优先级就绪队列 + 事件驱动准入调度。
 *
 * 并发模型（双线程职责分离）：
 *   - mtx_ 保护注册表 tasks_ 与 DB（sqlite3 串行化模式，读写均在持锁期间）；
 *   - A 线程（scheduler_loop，快节拍）：仅 query 纯读 + 改内存 + 转发回调，持锁期间只做内存操作；
 *   - B 线程（maintenance_loop，慢节拍）：唯一的落库 / 区间快照 / 终态注销 / 准入与引擎 sweep 执行方；
 *   - 引擎启动 / 合成回调一律在释放 mtx_ 后执行，规避回调线程重入。
 */

#include "task_manager.h"

#include "internal/downloader_internal.h"
#include "internal/engine_interface.h"
#include "http/http_engine_internal.h" // extract_filename_from_url（RESOLVING 校验拍定名）
#include "utils/time_util.h"
#include "utils/unique_name.h"

#include <chrono>
#include <filesystem>
#include <optional>
#include <ranges>
#include <unordered_set>

namespace dw {
    using utils::now_unix_ms;

    namespace {
        /// 占用下载额度的状态集合：活跃态 {DOWNLOADING, RESOLVING}。
        /// RESOLVING（BT 等待元数据/定名）已由调度准入引擎，占用名额防止超发；
        /// 其余状态（QUEUED / PAUSED / COMPLETED / ERROR）均不占额度。
        bool status_occupies_slot(dw_task_status_t s) {
            return s == DW_TASK_STATUS_DOWNLOADING || s == DW_TASK_STATUS_RESOLVING;
        }

        /// std::string 拷贝为堆分配 C 字符串（供快照数组使用，调用方 free）。
        char *dup_cstr(const std::string &s) {
            char *p = static_cast<char *>(std::malloc(s.size() + 1));
            if (p) std::memcpy(p, s.c_str(), s.size() + 1);
            return p;
        }

        /// 引擎识别键：HTTP 取 url，BT 取 info_hash。仅用于向引擎派发 / 匹配回调。
        const std::string &engine_key(const TaskRecord &task_record) {
            return task_record.protocol == DW_PROTOCOL_HTTP ? task_record.url : task_record.info_hash;
        }
    } // namespace

    TaskManager::~TaskManager() {
        stop();
    }

    void TaskManager::set_engines(IDownloadEngine *http, IDownloadEngine *torrent) {
        http_ = http;
        torrent_ = torrent;
    }

    void TaskManager::set_progress_cb(dw_progress_cb cb) {
        std::lock_guard<std::mutex> lock(mtx_);
        progress_cb_ = cb;
    }

    /* ================================================================== */
    /*                          生命周期                                  */
    /* ================================================================== */

    int32_t TaskManager::start(const dw_config_t &cfg) {
        max_concurrent_ = cfg.max_concurrent_downloads > 0
                              ? cfg.max_concurrent_downloads
                              : 3;
        flush_interval_ms_ = cfg.status_callback_interval_ms > 0
                                 ? cfg.status_callback_interval_ms
                                 : 1000;
        maintenance_interval_ms_ = flush_interval_ms_ * 2;
        resume_checkpoint_interval_ms_ = flush_interval_ms_ * 5;

        const std::string dir = cfg.work_dir && cfg.work_dir[0] ? cfg.work_dir : ".";
        const std::string story_path = dir + "/leopard_tasks.db";

        std::lock_guard<std::mutex> lock(mtx_);
        if (!store_.open(story_path)) {
            DW_LOGF(DW_LOG_ERROR, "", "[ERROR] TaskManager 打开数据库失败: %s",
                    story_path.c_str());
            return -1;
        }
        store_.init_schema();
        for (auto &task_record: store_.load_active()) {
            register_task(std::move(task_record)); // 登记到 id 注册表并建立自然键反查
        }

        // 首次加载时将下载中/解析中的任务设置为队列中等待调度
        for (auto &task_record: tasks_ | std::views::values) {
            if (task_record.status == DW_TASK_STATUS_DOWNLOADING ||
                task_record.status == DW_TASK_STATUS_RESOLVING) {
                task_record.status = DW_TASK_STATUS_QUEUED;
                task_record.synth_notified = false;
                store_.update(task_record);
            }
        }

        running_.store(true);
        schedule_needed_ = true;
        worker_ = std::thread(&TaskManager::scheduler_loop, this);
        maintenance_ = std::thread(&TaskManager::maintenance_loop, this);

        DW_LOGF(DW_LOG_INFO, "", "[EVENT] TaskManager 启动 tasks=%zu concurrent=%d",
                tasks_.size(), max_concurrent_);
        return 0;
    }

    void TaskManager::stop() {
        if (!running_.exchange(false)) {
            return;
        }
        cv_.notify_all();
        if (worker_.joinable()) {
            worker_.join();
        }
        if (maintenance_.joinable()) {
            maintenance_.join();
        }

        std::lock_guard<std::mutex> lock(mtx_);
        flush_dirty_locked();
        store_.close();
        DW_LOG(DW_LOG_INFO, "[CLEANUP] TaskManager 已停止", "");
    }

    /* ================================================================== */
    /*                          控制操作                                  */
    /* ================================================================== */

    int32_t TaskManager::add(const dw_protocol_t proto, const dw_task_params_t *params,
                             dw_submit_result_t *out) {
        if (!params || !out) {
            if (out) {
                out->code = DW_REASON_ERROR;
                out->message = nullptr;
            }
            return -1;
        }
        const char *raw_key = proto == DW_PROTOCOL_HTTP ? params->url : params->info_hash;
        if (!raw_key || !raw_key[0]) {
            out->code = DW_REASON_ERROR;
            out->message = nullptr;
            return -1;
        }

        int64_t task_id = 0;
        // BT 待选择任务：add 返回前锁外入引擎（metadata-only），供弹窗轮询文件列表。
        std::optional<TaskRecord> engine_start;
        {
            std::lock_guard<std::mutex> lock(mtx_);
            const std::string key = raw_key;

            if (const int64_t id = id_of_engine_key(proto, key); id != 0) {
                // 活跃任务：直接引用内存中的现有对象，错误态清理后重新入队
                TaskRecord &task_record = tasks_[id];
                if (!status_occupies_slot(task_record.status) &&
                    task_record.status != DW_TASK_STATUS_COMPLETED) {
                    reset_error_task_for_restart(task_record);
                    task_record.status = DW_TASK_STATUS_QUEUED;
                    task_record.synth_notified = false;
                    task_record.created_at = now_unix_ms();
                    store_.update(task_record);
                }
                task_id = task_record.id;
                schedule_needed_ = true;
            } else if (TaskRecord task_record;
                proto == DW_PROTOCOL_HTTP
                    ? store_.load_by_url(key, task_record)
                    : store_.load_by_info_hash(key, task_record)) {
                // 从库中取历史任务重新入队；错误任务清理残留态以强制重新校验 / 重下。
                task_id = task_record.id;
                if (task_record.status != DW_TASK_STATUS_COMPLETED) {
                    reset_error_task_for_restart(task_record);
                    task_record.status = DW_TASK_STATUS_QUEUED;
                    task_record.synth_notified = false;
                    task_record.created_at = now_unix_ms();
                    store_.update(task_record);
                    register_task(std::move(task_record));
                    schedule_needed_ = true;
                }
            } else {
                // 全新任务
                task_record.protocol = proto;
                if (proto == DW_PROTOCOL_TORRENT) task_record.info_hash = key;
                else task_record.url = key;
                task_record.save_path = params->save_path ? params->save_path : "";
                task_record.filename = params->filename ? params->filename : "";
                task_record.magnet_link = params->magnet_link ? params->magnet_link : "";
                task_record.torrent_file = params->torrent_file ? params->torrent_file : "";
                if (params->trackers && params->tracker_count > 0) {
                    for (int32_t i = 0; i < params->tracker_count; ++i) {
                        if (params->trackers[i]) task_record.trackers.emplace_back(params->trackers[i]);
                    }
                }
                if (params->file_indexes && params->file_index_size > 0) {
                    task_record.file_indexes.assign(params->file_indexes,
                                                    params->file_indexes + params->file_index_size);
                }
                task_record.priority = params->priority;
                task_record.created_at = now_unix_ms();
                task_record.name = task_record.filename.empty() ? key : task_record.filename;
                // BT 未携带文件选择：初始 PAUSED（不占调度名额），add 返回前锁外入引擎
                // metadata-only 拿元数据（磁力弹窗轮询文件列表渐进出现）；用户经
                // confirm_file_selection 确认或 resume 后转 QUEUED 进调度。
                // 其余（HTTP / BT 已带选择）直接排队。判重定名统一由调度 RESOLVING 校验拍处理。
                const bool await_selection = proto == DW_PROTOCOL_TORRENT &&
                                             task_record.file_indexes.empty();
                task_record.status = await_selection ? DW_TASK_STATUS_PAUSED
                                                     : DW_TASK_STATUS_QUEUED;
                task_record.synth_notified = false;
                store_.insert(task_record);
                task_id = task_record.id;
                if (await_selection) engine_start = task_record;
                register_task(std::move(task_record));
                if (!await_selection) schedule_needed_ = true;
            }
        }
        cv_.notify_all();

        // BT 待选择任务锁外入引擎（metadata-only：default_dont_download + auto_managed，
        // 接 swarm 拿元数据、零 payload 下载）。失败不阻塞 add：弹窗文件列表自然为空，
        // 用户确认/恢复后由调度冷启动重试。
        if (engine_start) {
            start_engine_task(*engine_start, {});
        }

        out->code = DW_REASON_NONE;
        out->message = nullptr;
        out->id = task_id;
        return 0;
    }

    int32_t TaskManager::pause(const dw_protocol_t proto, const int64_t id,
                               dw_submit_result_t *out) {
        if (!out) return -1;

        dw_task_status_t prev = DW_TASK_STATUS_QUEUED;
        std::string key;
        {
            std::lock_guard<std::mutex> lock(mtx_);
            // 仅活跃/排队任务（常驻内存）可暂停；暂停/终态任务视为无效操作。
            const auto it = tasks_.find(id);
            if (it == tasks_.end()) {
                out->code = DW_REASON_ERROR;
                out->message = nullptr;
                return -1;
            }
            key = engine_key(it->second);
            prev = it->second.status;
            // 暂停仅改内存态与标志：两引擎 pause 均非销毁（HTTP worker 自退 + ctx 待 sweep，
            // BT handle 常驻 session）。落库交 B 的 flush_dirty_locked，区间快照 / 内存逐出
            // 交 B 的 maintenance_persist_locked，PAUSED 帧回调交 A 的 collect_progress_locked 合成。
            it->second.status = DW_TASK_STATUS_PAUSED;
            it->second.synth_notified = false; // A 线程合成一次 PAUSED 帧后置位
            it->second.dirty = true; // 待 B flush 落库
            reset_live_telemetry(it->second); // 暂停帧不残留旧速率
        }

        dw_submit_result_t r{};
        if (IDownloadEngine *eng = engine_of(proto)) eng->pause_task(key.c_str(), &r);
        dw_submit_result_release(&r);

        {
            std::lock_guard<std::mutex> lock(mtx_);
            schedule_needed_ = true;
        }
        cv_.notify_all();

        out->code = DW_REASON_NONE;
        out->message = nullptr;
        out->id = id;
        return 0;
    }

    int32_t TaskManager::resume(dw_protocol_t /*proto*/, int64_t id,
                                dw_submit_result_t *out) {
        if (!out) return -1;
        {
            std::lock_guard<std::mutex> lock(mtx_);
            if (const auto it = tasks_.find(id);
                it != tasks_.end()) {
                // 常驻任务：恢复即重新入队，实际引擎启动交由调度线程按并发额度准入。
                it->second.status = DW_TASK_STATUS_QUEUED;
                it->second.synth_notified = false;
                store_.update(it->second);
            } else {
                // 已落库的暂停/错误任务：按 id 回读全字段（未命中即无效 id），重新入队并登记。
                TaskRecord task_record;
                if (!store_.load_by_id(id, task_record)) {
                    out->code = DW_REASON_ERROR;
                    out->message = nullptr;
                    return -1;
                }
                task_record.status = DW_TASK_STATUS_QUEUED;
                task_record.synth_notified = false;
                store_.update(task_record);
                register_task(std::move(task_record));
            }
            schedule_needed_ = true;
        }
        cv_.notify_all();

        out->code = DW_REASON_NONE;
        out->message = nullptr;
        out->id = id;
        return 0;
    }

    int32_t TaskManager::remove(const dw_protocol_t proto, const int64_t id,
                                dw_submit_result_t *out) {
        if (!out) return -1;

        std::string key;
        {
            std::lock_guard<std::mutex> lock(mtx_);
            if (const auto it = tasks_.find(id);
                it != tasks_.end()) {
                key = engine_key(it->second);
                unregister_task(id);
            } else {
                TaskRecord task_record;
                if (!store_.load_by_id(id, task_record)) {
                    out->code = DW_REASON_ERROR;
                    out->message = nullptr;
                    return -1;
                }
                key = engine_key(task_record);
            }
            store_.remove(id);
        }

        dw_submit_result_t r{};
        if (IDownloadEngine *eng = engine_of(proto)) eng->delete_task(key.c_str(), &r);
        dw_submit_result_release(&r);

        {
            std::lock_guard<std::mutex> lock(mtx_);
            schedule_needed_ = true;
        }
        cv_.notify_all();

        out->code = DW_REASON_NONE;
        out->message = nullptr;
        out->id = id;
        return 0;
    }

    int32_t TaskManager::set_priority(const int64_t id, const int32_t priority) {
        {
            std::lock_guard<std::mutex> lock(mtx_);
            if (const auto it = tasks_.find(id);
                it != tasks_.end()) {
                it->second.priority = priority;
                store_.update(it->second);
            } else {
                TaskRecord task_record;
                if (!store_.load_by_id(id, task_record)) return -1;
                task_record.priority = priority;
                store_.update(task_record);
            }
            schedule_needed_ = true;
        }
        cv_.notify_all();
        return 0;
    }

    int32_t TaskManager::confirm_file_selection(const int64_t id, const int32_t *file_indexes,
                                                const int32_t count, dw_submit_result_t *out) {
        if (!out) return -1;

        std::string apply_key; // 非空表示任务已在 DOWNLOADING，锁外立即应用选择
        std::vector<int32_t> apply_indexes;
        {
            std::lock_guard<std::mutex> lock(mtx_);
            TaskRecord loaded;
            TaskRecord *rec = nullptr;
            if (const auto it = tasks_.find(id); it != tasks_.end()) {
                rec = &it->second;
            } else if (store_.load_by_id(id, loaded)) {
                rec = &loaded;
            } else {
                out->code = DW_REASON_ERROR;
                out->message = nullptr;
                return -1;
            }

            // HTTP 无文件选择环节：恒视为已确认，幂等成功。
            if (rec->protocol == DW_PROTOCOL_HTTP) {
                out->code = DW_REASON_NONE;
                out->message = nullptr;
                out->id = id;
                return 0;
            }

            if (file_indexes && count > 0) {
                rec->file_indexes.assign(file_indexes, file_indexes + count);
            } else {
                rec->file_indexes.clear(); // 空 = 下载全部
            }

            switch (rec->status) {
                case DW_TASK_STATUS_DOWNLOADING:
                    // 已在下载中（如确认前已全量开下后改选）：锁外立即应用新选择。
                    apply_key = engine_key(*rec);
                    apply_indexes = rec->file_indexes;
                    store_.update(*rec);
                    break;
                case DW_TASK_STATUS_RESOLVING:
                case DW_TASK_STATUS_QUEUED:
                    // 解析中/排队中：仅落库选择，校验拍 apply 时取最新选择。
                    store_.update(*rec);
                    break;
                case DW_TASK_STATUS_COMPLETED:
                    // 终态仅记录选择，不重启任务。
                    store_.update(*rec);
                    break;
                default:
                    // PAUSED（待选择）/ ERROR：确认即入队等待准入（状态即语义：QUEUED+=已确认）。
                    reset_error_task_for_restart(*rec);
                    rec->status = DW_TASK_STATUS_QUEUED;
                    rec->synth_notified = false;
                    store_.update(*rec);
                    if (rec == &loaded) register_task(std::move(loaded));
                    schedule_needed_ = true;
                    break;
            }
        }
        cv_.notify_all();

        // 锁外应用（引擎内部有自有锁，不可持 mtx_ 调用）。
        if (!apply_key.empty() && torrent_) {
            torrent_->apply_file_selection(apply_key.c_str(),
                                           apply_indexes.empty() ? nullptr : apply_indexes.data(),
                                           static_cast<int32_t>(apply_indexes.size()));
        }

        DW_LOGF(DW_LOG_INFO, "", "[EVENT] 文件选择已确认 id=%lld count=%d",
                static_cast<long long>(id), count > 0 ? count : 0);
        out->code = DW_REASON_NONE;
        out->message = nullptr;
        out->id = id;
        return 0;
    }

    bool TaskManager::engine_key_of(const int64_t id, std::string &out_key) {
        std::lock_guard<std::mutex> lock(mtx_);
        if (const auto it = tasks_.find(id); it != tasks_.end()) {
            out_key = engine_key(it->second);
            return true;
        }
        TaskRecord task_record;
        if (!store_.load_by_id(id, task_record)) return false;
        out_key = engine_key(task_record);
        return true;
    }

    bool TaskManager::engine_ref_of(const int64_t id, std::string &out_key, dw_protocol_t &out_proto) {
        std::lock_guard<std::mutex> lock(mtx_);
        if (const auto it = tasks_.find(id); it != tasks_.end()) {
            out_key = engine_key(it->second);
            out_proto = it->second.protocol;
            return true;
        }
        TaskRecord task_record;
        if (!store_.load_by_id(id, task_record)) return false;
        out_key = engine_key(task_record);
        out_proto = task_record.protocol;
        return true;
    }

    void TaskManager::set_play_position(const int64_t id, const int32_t file_index, const int64_t position_ms) {
        std::lock_guard<std::mutex> lock(mtx_);
        store_.set_play_position(id, file_index, position_ms);
    }

    int64_t TaskManager::get_play_position(const int64_t id, const int32_t file_index) {
        std::lock_guard<std::mutex> lock(mtx_);
        return store_.get_play_position(id, file_index);
    }

    std::vector<dw_byte_range_t> TaskManager::load_segments(const int64_t id, const int32_t file_index) {
        std::lock_guard<std::mutex> lock(mtx_);
        return store_.load_segments(id, file_index);
    }

    /* ================================================================== */
    /*                          回调拦截                                  */
    /* ================================================================== */

    int64_t TaskManager::on_resume_data(const char *engine_key, const dw_protocol_t proto,
                                        const uint8_t *data, const size_t size) {
        if (!engine_key || !data || size == 0) return 0;
        std::lock_guard<std::mutex> lock(mtx_);
        const int64_t id = id_of_engine_key(proto, engine_key);
        if (id != 0) {
            if (const auto it = tasks_.find(id); it != tasks_.end()) {
                // 常规路径：任务在内存中，暂存 pending_resume，由 B 线程 flush_dirty_locked 落库。
                it->second.pending_resume.assign(reinterpret_cast<const char *>(data), size);
                return id;
            }
        }
        // 兜底路径：任务已从内存逐出（如完成移出后 resume 晚到）。不重新加载入内存，
        // 查库确认记录仍存在则直接落库；库中也不存在（已删除）则丢弃不处理。
        TaskRecord task_record;
        const bool found = (proto == DW_PROTOCOL_HTTP)
                               ? store_.load_by_url(engine_key, task_record)
                               : store_.load_by_info_hash(engine_key, task_record);
        if (!found) return 0;
        store_.save_resume(task_record.id, data, size);
        return task_record.id;
    }

    void TaskManager::on_task_files(const char *engine_key, const dw_protocol_t proto,
                                    const dw_file_info_t *files, const int32_t count) {
        if (!engine_key || !files || count <= 0) return;
        std::lock_guard<std::mutex> lock(mtx_);
        int64_t id = id_of_engine_key(proto, engine_key);
        if (id == 0) {
            TaskRecord task_record;
            const bool found = (proto == DW_PROTOCOL_HTTP)
                                   ? store_.load_by_url(engine_key, task_record)
                                   : store_.load_by_info_hash(engine_key, task_record);
            if (found) id = task_record.id;
        }
        if (id == 0) return; // 非已知任务，丢弃
        // 浅拷贝为 vector（仅拷贝指针，字符串所有权仍属调用方），库内自行深拷落库。
        const std::vector<dw_file_info_t> file_vec(files, files + count);
        store_.save_task_files(id, file_vec);
    }

    /* ================================================================== */
    /*                          唯一名定名                                */
    /* ================================================================== */

    std::string TaskManager::resolve_and_record_name(const char *engine_key, const dw_protocol_t proto,
                                                     const std::string &dir, const std::string &name) {
        if (!engine_key || !engine_key[0] || name.empty()) return name;
        std::lock_guard<std::mutex> lock(mtx_);
        if (const int64_t id = id_of_engine_key(proto, engine_key); id != 0) {
            if (const auto it = tasks_.find(id); it != tasks_.end()) {
                return resolve_and_record_name_locked(it->second, dir, name);
            }
        }
        // 任务未常驻内存（罕见：上调早于登记 / 已被逐出）：回落库定位记录后仍走定名落库。
        TaskRecord task_record;
        const bool found = (proto == DW_PROTOCOL_HTTP)
                               ? store_.load_by_url(engine_key, task_record)
                               : store_.load_by_info_hash(engine_key, task_record);
        if (found) {
            return resolve_and_record_name_locked(task_record, dir, name);
        }
        // 未知任务：仅解唯一名返回，不落库（无持久预留）。
        std::unordered_set<std::string> taken;
        for (const auto &n: store_.load_names_by_save_path(dir, 0)) taken.insert(n);
        return utils::resolve_unique_name(dir, name, taken);
    }

    std::string TaskManager::resolve_and_record_name_locked(TaskRecord &rec,
                                                            const std::string &dir,
                                                            const std::string &name) {
        // 占用名集 = tasks 表同 save_path 其他任务的 name/filename（磁盘存在性由工具内部判定）。
        std::unordered_set<std::string> taken;
        for (const auto &n: store_.load_names_by_save_path(dir, rec.id)) taken.insert(n);
        const std::string unique = utils::resolve_unique_name(dir, name, taken);
        rec.name = unique;
        rec.filename = unique;
        store_.update(rec); // 落库即完成持久预留（跨重启有效）
        if (unique != name) {
            DW_LOGF(DW_LOG_INFO, "", "[EVENT] 唯一名定名: '%s' -> '%s' (dir=%s id=%lld)",
                    name.c_str(), unique.c_str(), dir.c_str(),
                    static_cast<long long>(rec.id));
        }
        return unique;
    }

    /* ================================================================== */
    /*                          快照查询                                  */
    /* ================================================================== */

    int32_t TaskManager::list(dw_task_snapshot_t **out_tasks, int32_t *out_count) {
        if (!out_tasks || !out_count) return -1;

        std::lock_guard<std::mutex> lock(mtx_);

        // 先刷写活跃任务脏进度，使快照反映最新内存态（暂停/完成/错误已在状态迁移时落库）。
        flush_dirty_locked();

        // 全量任务来自库（含未常驻内存的暂停/完成/错误任务）。
        const std::vector<TaskRecord> all = store_.load_all();
        const auto n = static_cast<int32_t>(all.size());
        if (n == 0) {
            *out_tasks = nullptr;
            *out_count = 0;
            return 0;
        }

        auto *arr = static_cast<dw_task_snapshot_t *>(
            std::calloc(n, sizeof(dw_task_snapshot_t)));
        if (!arr) {
            *out_tasks = nullptr;
            *out_count = 0;
            return -1;
        }

        // 逐条从 TaskRecord 投影为 C ABI 快照（字符串堆分配，调用方经 dw_free_task_list 释放）。
        for (int32_t i = 0; i < n; ++i) {
            const TaskRecord &task_record = all[i];
            dw_task_snapshot_t s{};
            s.id = task_record.id;
            s.url = dup_cstr(task_record.url);
            s.info_hash = dup_cstr(task_record.info_hash);
            s.protocol = task_record.protocol;
            s.name = dup_cstr(task_record.name);
            s.save_path = dup_cstr(task_record.save_path);
            s.filename = dup_cstr(task_record.filename);
            s.status = task_record.status;
            s.progress = task_record.progress;
            s.total_size = task_record.total_size;
            s.total_done = task_record.total_done;
            s.priority = task_record.priority;
            s.created_at = task_record.created_at;
            arr[i] = s;
        }
        *out_tasks = arr;
        *out_count = n;
        return 0;
    }

    /* ================================================================== */
    /*                          调度线程                                  */
    /* ================================================================== */

    void TaskManager::scheduler_loop() {
        // 采集后同步到内存和调用回调
        while (running_.load()) {
            std::vector<TaskRecord> fwd_records;
            std::vector<ResolveAction> resolve_actions;
            std::vector<RenameAction> rename_actions;
            dw_progress_cb cb = nullptr;

            {
                std::unique_lock<std::mutex> lock(mtx_);
                cv_.wait_for(lock, std::chrono::milliseconds(flush_interval_ms_),
                             [this] { return !running_.load(); });
                if (!running_.load()) break;

                // 采集数据
                collect_progress_locked(fwd_records, resolve_actions, rename_actions);
                cb = progress_cb_;
            } // ← 作用域退出，自动释放 mtx_

            // 触发引擎推送（锁外，绝不持 mtx_）：统一轮询各引擎钩子。post_updates 让 Torrent
            // 引擎线程刷新进度快照；request_resume_checkpoint 低频触发 BT 续传检查点。
            // 两者产生的回调经引擎线程回来，与 A 持 mtx_ 的采集天然错开，无自死锁路径。
            // HTTP 钩子为默认空实现（状态 / resume 由 worker 线程自推）。
            const int64_t now_ms = now_unix_ms();
            const bool checkpoint_due =
                    now_ms - last_resume_checkpoint_ms_ >= resume_checkpoint_interval_ms_;
            if (checkpoint_due) last_resume_checkpoint_ms_ = now_ms;
            for (IDownloadEngine *eng: {http_, torrent_}) {
                if (!eng) continue;
                eng->post_updates();
                if (checkpoint_due) eng->request_resume_checkpoint();
            }

            // RESOLVING 校验执行（锁外引擎调用，回锁校验后定态；期间可能已被暂停/删除，
            // 仅 RESOLVING 才迁移）：
            //   BT：无凭证先 finalize_naming（根名判重；重名发起引擎改名返回 1，下拍再查；
            //       rc==0 已同步构树落文件表）→ apply_file_selection 定型开下 → DOWNLOADING；
            //   HTTP：定名已在采集拍锁内完成，此处入引擎（携续传存档）→ 成功迁 DOWNLOADING，
            //       失败迁 ERROR 释放名额。
            bool slot_released = false;
            for (const auto &act: resolve_actions) {
                if (act.rec.protocol == DW_PROTOCOL_TORRENT) {
                    if (!torrent_) continue;
                    const std::string &key = act.rec.info_hash;
                    if (!act.naming_done && torrent_->finalize_naming(key.c_str()) != 0) {
                        continue; // 改名进行中 / 元数据未就绪，保持 RESOLVING 下拍再查
                    }
                    if (torrent_->apply_file_selection(
                            key.c_str(),
                            act.rec.file_indexes.empty() ? nullptr : act.rec.file_indexes.data(),
                            static_cast<int32_t>(act.rec.file_indexes.size())) != 0) {
                        continue;
                    }
                    std::lock_guard<std::mutex> lock(mtx_);
                    if (const auto it = tasks_.find(act.rec.id);
                        it != tasks_.end() && it->second.status == DW_TASK_STATUS_RESOLVING) {
                        it->second.status = DW_TASK_STATUS_DOWNLOADING;
                        it->second.dirty = true; // 落库交 B 线程 flush
                        DW_LOGF(DW_LOG_INFO, "", "[EVENT] 任务校验通过转下载 id=%lld",
                                static_cast<long long>(act.rec.id));
                    }
                } else {
                    const bool ok = start_engine_task(act.rec, act.resume);
                    std::lock_guard<std::mutex> lock(mtx_);
                    if (const auto it = tasks_.find(act.rec.id);
                        it != tasks_.end() && it->second.status == DW_TASK_STATUS_RESOLVING) {
                        if (ok) {
                            it->second.status = DW_TASK_STATUS_DOWNLOADING;
                            it->second.dirty = true;
                            DW_LOGF(DW_LOG_INFO, "", "[EVENT] 任务校验通过转下载 id=%lld",
                                    static_cast<long long>(act.rec.id));
                        } else {
                            it->second.status = DW_TASK_STATUS_ERROR;
                            store_.update(it->second);
                            schedule_needed_ = true; // 名额释放，唤醒调度准入后续任务
                            slot_released = true;
                            DW_LOGF(DW_LOG_ERROR, "", "[ERROR] 任务引擎启动失败 id=%lld",
                                    static_cast<long long>(act.rec.id));
                        }
                    }
                }
            }

            // HTTP 完成拍改名执行（锁外 fs::rename，失败保持原名不影响完成态；
            // 成功回锁落新名并重写文件表凭证——任务可能已被 B 线程终态逐出，回落库直改）。
            for (const auto &act: rename_actions) {
                std::error_code ec;
                std::filesystem::rename(std::filesystem::path(act.dir) / act.old_name,
                                        std::filesystem::path(act.dir) / act.new_name, ec);
                if (ec) {
                    DW_LOGF(DW_LOG_ERROR, "", "[ERROR] 完成拍改名失败 id=%lld '%s'->'%s': %s",
                            static_cast<long long>(act.id), act.old_name.c_str(),
                            act.new_name.c_str(), ec.message().c_str());
                    continue;
                }
                std::lock_guard<std::mutex> lock(mtx_);
                if (const auto it = tasks_.find(act.id); it != tasks_.end()) {
                    it->second.name = act.new_name;
                    it->second.filename = act.new_name;
                    it->second.dirty = true;
                    save_http_file_entry_locked(act.id, act.new_name,
                                                it->second.total_size, 2);
                } else if (TaskRecord rec; store_.load_by_id(act.id, rec)) {
                    rec.name = act.new_name;
                    rec.filename = act.new_name;
                    store_.update(rec);
                    save_http_file_entry_locked(act.id, act.new_name, rec.total_size, 2);
                }
                DW_LOGF(DW_LOG_INFO, "", "[OK] 完成拍改名 id=%lld '%s'->'%s'",
                        static_cast<long long>(act.id), act.old_name.c_str(),
                        act.new_name.c_str());
            }
            if (slot_released) cv_.notify_all();

            // 锁外转发（周期节奏）：只读锁内已拷出的本地副本（已含遥测），避免与上层回调重入交叉。
            for (const auto &rec: fwd_records) {
                emit_progress(cb, rec);
            }
        }
    }

    void TaskManager::collect_progress_locked(std::vector<TaskRecord> &fwd_records,
                                              std::vector<ResolveAction> &resolve_actions,
                                              std::vector<RenameAction> &rename_actions) {
        for (auto &task_record: tasks_ | std::views::values) {
            // 引擎无 ctx 的合成态（QUEUED/PAUSED）：直接从记录投影合成一帧，一次性去重；
            // 回调唯一出口收归 A 线程，pause()/run_schedule（B 线程）仅置态，不再直接发射。
            if ((task_record.status == DW_TASK_STATUS_QUEUED ||
                 task_record.status == DW_TASK_STATUS_PAUSED) && !task_record.synth_notified) {
                task_record.synth_notified = true;
                fwd_records.push_back(task_record);
                continue;
            }
            if (task_record.status != DW_TASK_STATUS_DOWNLOADING &&
                task_record.status != DW_TASK_STATUS_RESOLVING)
                continue;
            const std::string key = engine_key(task_record);

            // HTTP RESOLVING 校验拍：任务尚未入引擎（无 ctx 可查），定名判重锁内完成，
            // 引擎启动由 scheduler_loop 锁外执行。文件表有记录即已判重定名（恢复/重试），
            // 直接放行续传；URL 推断不出名则跳过定名，交引擎 probing 经
            // request_unique_name 上调兜底，完成拍补写文件表凭证。
            if (task_record.status == DW_TASK_STATUS_RESOLVING &&
                task_record.protocol == DW_PROTOCOL_HTTP) {
                if (!store_.has_task_files(task_record.id)) {
                    const std::string base = !task_record.filename.empty()
                                                 ? task_record.filename
                                                 : http_engine::internal::extract_filename_from_url(
                                                     task_record.url);
                    if (!base.empty()) {
                        const std::string unique = resolve_and_record_name_locked(
                            task_record, task_record.save_path, base);
                        save_http_file_entry_locked(task_record.id, unique,
                                                    task_record.total_size, 0);
                    }
                }
                resolve_actions.push_back({task_record, true, store_.load_resume(task_record.id)});
                continue;
            }

            EngineProgress ep;
            IDownloadEngine *eng = engine_of(task_record.protocol);
            const bool ok = eng && eng->query_progress(key.c_str(), ep);
            if (!ok || !ep.valid) continue;

            task_record.progress = ep.progress;
            task_record.total_size = ep.total_size;
            task_record.total_done = ep.total_done;
            // 定名守卫：filename 非空即已经唯一名定名（校验拍定名或引擎上调定名），
            // 不再用 ep.name 覆盖，防止引擎回报的元数据原名把唯一名冲掉。
            if (!ep.name.empty() && task_record.filename.empty()) {
                task_record.name = ep.name;
                task_record.filename = ep.name;
            }
            if (!ep.output_path.empty()) {
                task_record.output_path = ep.output_path;
            }
            task_record.support_range = ep.support_range;
            if (!ep.etag.empty()) {
                task_record.etag = ep.etag;
            }
            if (!ep.last_modified.empty()) {
                task_record.last_modified = ep.last_modified;
            }
            // resume 不再随 query_progress 返回值携回：两引擎均由引擎线程
            // 经 post_resume_data → on_resume_data 异步写入 pending_resume，B 落库。
            // 运行态遥测：直接回填记录，转发时从记录投影（不再携带 EngineProgress）。
            task_record.download_rate = ep.download_rate;
            task_record.upload_rate = ep.upload_rate;
            task_record.reason = ep.reason;
            task_record.message = ep.message;
            task_record.dirty = true;

            if (ep.status == DW_TASK_STATUS_COMPLETED || ep.status == DW_TASK_STATUS_ERROR) {
                task_record.status = ep.status;
                schedule_needed_ = true;
                // HTTP 完成拍（DOWNLOADING→COMPLETED 收敛，仅此一拍）：
                //   1) 无文件表凭证（无名 probing 定名路径）→ 补写单条文件节点；
                //   2) 服务器建议名与定名不一致 → 锁内判重出唯一名，收集改名动作
                //      （fs::rename 由 scheduler_loop 锁外执行，失败保持原名不影响完成态）。
                if (ep.status == DW_TASK_STATUS_COMPLETED &&
                    task_record.protocol == DW_PROTOCOL_HTTP) {
                    if (!store_.has_task_files(task_record.id)) {
                        save_http_file_entry_locked(task_record.id, task_record.filename,
                                                    task_record.total_size, 2);
                    }
                    if (!ep.server_name.empty() && !task_record.filename.empty() &&
                        ep.server_name != task_record.filename) {
                        std::unordered_set<std::string> taken;
                        for (const auto &n: store_.load_names_by_save_path(
                                 task_record.save_path, task_record.id))
                            taken.insert(n);
                        const std::string unique = utils::resolve_unique_name(
                            task_record.save_path, ep.server_name, taken);
                        if (unique != task_record.filename) {
                            rename_actions.push_back({
                                task_record.id, task_record.save_path,
                                task_record.filename, unique
                            });
                        }
                    }
                }
            }

            // BT RESOLVING 校验拍：元数据就绪且无进行中改名后收集校验动作，
            // finalize_naming（判重定名）/ apply_file_selection（定型开下）由
            // scheduler_loop 锁外执行，回锁校验后迁 DOWNLOADING。
            // 置于终态判断之后：本拍已迁终态（如引擎报错）的任务不再收集。
            if (task_record.status == DW_TASK_STATUS_RESOLVING &&
                task_record.protocol == DW_PROTOCOL_TORRENT &&
                ep.metadata_ready && ep.naming_ready) {
                resolve_actions.push_back({
                    task_record, store_.has_task_files(task_record.id), {}
                });
            }

            fwd_records.push_back(task_record);
        }
    }

    void TaskManager::maintenance_loop() {
        while (running_.load()) {
            {
                std::unique_lock<std::mutex> lock(mtx_);
                cv_.wait_for(lock, std::chrono::milliseconds(maintenance_interval_ms_),
                             [this] { return !running_.load() || schedule_needed_; });
                if (!running_.load()) break;

                maintenance_persist_locked();

                if (schedule_needed_) {
                    schedule_needed_ = false;
                    run_schedule(lock);
                }
            }
            if (http_) http_->sweep();
            if (torrent_) torrent_->sweep();
        }
    }

    void TaskManager::maintenance_persist_locked() {
        std::vector<int64_t> to_remove;
        for (auto &[id, task_record]: tasks_) {
            const bool terminal = (task_record.status == DW_TASK_STATUS_COMPLETED ||
                                   task_record.status == DW_TASK_STATUS_ERROR);
            const bool paused = (task_record.status == DW_TASK_STATUS_PAUSED);
            if (task_record.status == DW_TASK_STATUS_DOWNLOADING || terminal || paused) {
                snapshot_segments_locked(task_record);
            }
            if (terminal) {
                // 任务级 0→2 传播：完成态任务把其文件节点统一置为完成正常。
                if (task_record.status == DW_TASK_STATUS_COMPLETED) {
                    store_.mark_task_files_completed(id);
                }
                to_remove.push_back(id);
            } else if (paused) {
                if (task_record.protocol == DW_PROTOCOL_HTTP) {
                    // HTTP 暂停态延迟逐出：待引擎 ctx 被 sweep 回收（query_progress 探测不到）后
                    // 再移出内存。此时 worker 已结束并经 post_resume_data 汇入 pending_resume，
                    // 由下方 flush_dirty_locked 落库，规避先逐出导致异步 resume 被 on_resume_data 丢弃。
                    EngineProgress probe;
                    if (!http_ || !http_->query_progress(engine_key(task_record).c_str(), probe)) {
                        to_remove.push_back(id);
                    }
                } else {
                    // BT：handle 常驻 session，无 ctx 回收信号；待记录已落库（!dirty）且
                    // pending_resume 已被 flush 清空后再逐出，尽力保住暂停时续传（晚到的检查点可能丢一次）。
                    if (!task_record.dirty && task_record.pending_resume.empty()) {
                        to_remove.push_back(id);
                    }
                }
            }
        }

        flush_dirty_locked();

        // 内存中移除任务
        for (const int64_t id: to_remove) {
            unregister_task(id);
        }
    }

    void TaskManager::emit_progress(const dw_progress_cb cb, const TaskRecord &rec) {
        if (!cb) return;
        dw_progress_t p{};
        p.id = rec.id;
        p.url = rec.url.c_str(); // HTTP 展示 / 识别；BT 为空串
        p.info_hash = rec.info_hash.c_str(); // BT 展示 / 识别；HTTP 为空串
        p.trace_id = "";
        p.protocol = rec.protocol;
        p.name = rec.name.c_str();
        // output_path 指向文件所在目录：引擎回报了则用之，否则回退 save_path（无 .tmp 隔离，两者一致）。
        p.output_path = rec.output_path.empty() ? rec.save_path.c_str() : rec.output_path.c_str();
        p.filename = rec.filename.c_str();
        p.total_size = rec.total_size;
        p.total_done = rec.total_done;
        p.remaining = (rec.total_size > 0 && rec.total_size >= rec.total_done)
                          ? (rec.total_size - rec.total_done)
                          : -1;
        p.progress = rec.progress;
        p.download_rate = rec.download_rate;
        p.eta = (rec.download_rate > 0.0 && p.remaining > 0)
                    ? static_cast<double>(p.remaining) / rec.download_rate
                    : -1.0;
        p.task_status = rec.status; // 权威态由 TaskManager 独占
        p.reason = rec.reason;
        p.message = rec.message.c_str();
        p.saved_at_unix_ms = now_unix_ms();
        p.support_range = rec.support_range;
        p.etag = rec.etag.c_str();
        p.last_modified = rec.last_modified.c_str();
        p.upload_rate = rec.upload_rate;
        cb(&p);
    }

    void TaskManager::reset_live_telemetry(TaskRecord &rec) {
        rec.download_rate = 0.0;
        rec.upload_rate = 0.0;
        rec.reason = DW_REASON_NONE;
        rec.message.clear();
    }

    void TaskManager::run_schedule(std::unique_lock<std::mutex> &lock) {
        // 调度队列中的任务
        while (running_.load() && net_allowed_) {
            if (const int32_t active = active_count_locked(); active >= max_concurrent_) {
                break;
            }

            TaskRecord *best = nullptr;
            for (auto &task_record: tasks_ | std::views::values) {
                if (task_record.status != DW_TASK_STATUS_QUEUED) continue;
                if (!best ||
                    task_record.priority > best->priority ||
                    (task_record.priority == best->priority && task_record.id < best->id)) {
                    best = &task_record;
                }
            }
            if (!best) {
                break;
            }

            // 两协议统一先进 RESOLVING（下载前校验关卡：判重定名 / 等元数据 / 文件表凭证），
            // 就绪后由 A 线程校验拍迁 DOWNLOADING。
            best->status = DW_TASK_STATUS_RESOLVING;
            best->dirty = false;
            store_.update(*best);

            TaskRecord copy = *best;

            // BT 此处即入引擎（metadata-only 等元数据；add 时已入引擎的待选择任务走
            // 引擎幂等分支 resume）；HTTP 引擎启动延后到校验拍（定名后才知最终 filename）。
            if (copy.protocol == DW_PROTOCOL_TORRENT) {
                std::vector<uint8_t> resume = store_.load_resume(copy.id);

                lock.unlock();
                const bool ok = start_engine_task(copy, resume);
                lock.lock();

                if (!ok) {
                    if (auto it = tasks_.find(copy.id); it != tasks_.end()) {
                        it->second.status = DW_TASK_STATUS_ERROR;
                        store_.update(it->second);
                    }
                }
            }
        }
    }

    void TaskManager::set_network_allowed(bool allowed) {
        // 需停传输的活跃任务：(自增 id, 协议, 引擎键)。引擎键锁外喂引擎，id 锁内回落。
        struct PendingPause {
            int64_t id;
            dw_protocol_t protocol;
            std::string key; // HTTP=url，BT=info_hash
        };
        std::vector<PendingPause> to_pause;
        {
            std::lock_guard<std::mutex> lock(mtx_);
            if (net_allowed_ == allowed) return; // 状态未变，幂等跳过
            net_allowed_ = allowed;
            if (!allowed) {
                // 闸门关闭：收集所有活跃任务（HTTP + BT），锁外逐任务暂停后回落 QUEUED。
                // 不再整会话 pause，仅停各任务的载荷传输；session 存活维持连接/心跳。
                for (const auto &task_record: tasks_ | std::views::values) {
                    if (!status_occupies_slot(task_record.status)) continue;
                    to_pause.push_back({
                        task_record.id, task_record.protocol,
                        task_record.protocol == DW_PROTOCOL_HTTP ? task_record.url : task_record.info_hash
                    });
                }
            } else {
                // 闸门开启：唤醒调度按 QUEUED→准入路径重启（BT 经 add_task 幂等分支 resume）。
                schedule_needed_ = true;
            }
        }
        if (allowed) {
            cv_.notify_all();
            return;
        }

        // 锁外逐任务停传输（pause_task 内部可能 join / 加引擎锁，不可持 mtx_ 调用以规避死锁）。
        for (const auto &pp: to_pause) {
            dw_submit_result_t res{};
            if (IDownloadEngine *eng = engine_of(pp.protocol)) {
                eng->pause_task(pp.key.c_str(), &res);
            }
            dw_submit_result_release(&res);
        }

        // 引擎已停，回落 QUEUED 待闸门开启后重启；遥测归零，置 schedule_needed_ 由调度线程单点发射 QUEUED 合成帧。
        {
            std::lock_guard<std::mutex> lock(mtx_);
            for (const auto &pp: to_pause) {
                auto it = tasks_.find(pp.id);
                if (it == tasks_.end()) continue;
                TaskRecord &task_record = it->second;
                task_record.status = DW_TASK_STATUS_QUEUED;
                task_record.synth_notified = false; // 待 run_schedule 尾部统一发射
                reset_live_telemetry(task_record);
                store_.update(task_record);
            }
            schedule_needed_ = true;
        }
        cv_.notify_all();
    }

    // ---- 任务文件持久化 ----

    void TaskManager::save_files(const int64_t id,
                                 const std::vector<dw_file_info_t> &files) {
        std::lock_guard<std::mutex> lock(mtx_);
        store_.save_task_files(id, files);
    }

    void TaskManager::save_http_file_entry_locked(const int64_t id, const std::string &filename,
                                                  const int64_t total_size, const int32_t status) {
        if (filename.empty()) return;
        // HTTP 单文件模型：单条根层文件节点（index=0）。save_task_files 全量重建 +
        // SQLITE_TRANSIENT 拷贝绑定，此处借用调用方字符串生命周期无需堆拷。
        dw_file_info_t f{};
        f.index = 0;
        f.type = 1;
        f.node_id = 1;
        f.parent_id = -1;
        f.prefix = const_cast<char *>("");
        f.temp_dir = nullptr;
        f.name = const_cast<char *>(filename.c_str());
        const size_t dot = filename.find_last_of('.');
        const std::string ext = (dot != std::string::npos && dot + 1 < filename.size())
                                    ? filename.substr(dot + 1)
                                    : std::string();
        f.ext = ext.empty() ? nullptr : const_cast<char *>(ext.c_str());
        f.size = total_size > 0 ? total_size : 0;
        f.status = status;
        f.created_at = now_unix_ms();
        store_.save_task_files(id, {f});
    }

    std::vector<dw_file_info_t> TaskManager::load_files(const int64_t id) {
        std::lock_guard<std::mutex> lock(mtx_);
        std::vector<dw_file_info_t> files = store_.load_task_files(id);
        if (files.empty()) return files;
        // 惰性删除检测：仅对完成态(2)文件 stat 物理路径，缺失则标记为已删除(1)。
        // 物理路径 = save_path + prefix + name（无 .tmp 隔离，下载期即最终位置）。
        std::string save_path;
        TaskRecord task_record;
        if (store_.load_by_id(id, task_record)) save_path = task_record.save_path;
        if (save_path.empty()) return files;
        for (auto &f: files) {
            if (f.type != 1 || f.status != 2) continue;
            std::filesystem::path p(save_path);
            if (f.prefix && f.prefix[0]) p /= f.prefix;
            if (f.name) p /= f.name;
            std::error_code ec;
            if (!std::filesystem::exists(p, ec)) f.status = 1;
        }
        return files;
    }

    bool TaskManager::start_engine_task(const TaskRecord &task_record,
                                        const std::vector<uint8_t> &resume) {
        dw_task_params_t p{};
        const std::string &key = engine_key(task_record); // 引擎标识（HTTP=url，BT=info_hash）；下方 c_str 依赖其存活
        p.save_path = task_record.save_path.c_str();
        p.filename = task_record.filename.empty() ? nullptr : task_record.filename.c_str();
        p.trace_id = key.c_str();
        p.priority = task_record.priority;

        std::vector<const char *> tk;
        if (task_record.protocol == DW_PROTOCOL_HTTP) {
            p.url = task_record.url.c_str();
        } else {
            p.info_hash = task_record.info_hash.c_str();
            if (!task_record.magnet_link.empty()) p.magnet_link = task_record.magnet_link.c_str();
            if (!task_record.torrent_file.empty()) p.torrent_file = task_record.torrent_file.c_str();
            if (!task_record.trackers.empty()) {
                for (auto &t: task_record.trackers) tk.push_back(t.c_str());
                p.trackers = tk.data();
                p.tracker_count = static_cast<int32_t>(tk.size());
            }
            if (!task_record.file_indexes.empty()) {
                p.file_indexes = task_record.file_indexes.data();
                p.file_index_size = static_cast<int32_t>(task_record.file_indexes.size());
            }
        }

        // 续传存档（两协议均适用）：HTTP 为分片续传 BLOB，BT 为 libtorrent 存档。
        if (!resume.empty()) {
            p.resume_data = resume.data();
            p.resume_data_size = resume.size();
        }

        dw_submit_result_t out{};
        IDownloadEngine *eng = engine_of(task_record.protocol);
        const int32_t rc = eng ? eng->add_task(&p, &out) : -1;
        dw_submit_result_release(&out);
        return rc == 0;
    }

    IDownloadEngine *TaskManager::engine_of(const dw_protocol_t proto) const {
        return proto == DW_PROTOCOL_HTTP ? http_ : torrent_;
    }

    int32_t TaskManager::active_count_locked() const {
        int32_t n = 0;
        for (const auto &task_record: tasks_ | std::views::values) {
            if (status_occupies_slot(task_record.status)) ++n;
        }
        return n;
    }

    void TaskManager::flush_dirty_locked() {
        for (auto &task_record: tasks_ | std::views::values) {
            // 保存续传数据
            if (!task_record.pending_resume.empty()) {
                store_.save_resume(task_record.id,
                                   reinterpret_cast<const uint8_t *>(task_record.pending_resume.data()),
                                   task_record.pending_resume.size());
                task_record.pending_resume.clear();
            }
            // 保存任务数据
            if (task_record.dirty) {
                store_.update(task_record);
                task_record.dirty = false;
            }
        }
    }

    void TaskManager::register_task(TaskRecord task_record) {
        const int64_t id = task_record.id;
        // 按协议登记自然键反查表，再入注册表（三者同步维护，避免悬置）。
        if (task_record.protocol == DW_PROTOCOL_HTTP) {
            if (!task_record.url.empty()) url_index_[task_record.url] = id;
        } else {
            if (!task_record.info_hash.empty()) info_hash_index_[task_record.info_hash] = id;
        }
        tasks_[id] = std::move(task_record);
    }

    void TaskManager::unregister_task(const int64_t id) {
        const auto it = tasks_.find(id);
        if (it == tasks_.end()) return;
        if (const TaskRecord &task_record = it->second;
            task_record.protocol == DW_PROTOCOL_HTTP) {
            url_index_.erase(task_record.url);
        } else {
            info_hash_index_.erase(task_record.info_hash);
        }
        tasks_.erase(it);
    }

    int64_t TaskManager::id_of_engine_key(const dw_protocol_t proto, const std::string &key) const {
        const auto &idx = (proto == DW_PROTOCOL_HTTP) ? url_index_ : info_hash_index_;
        const auto it = idx.find(key);
        return it != idx.end() ? it->second : 0;
    }

    void TaskManager::reset_error_task_for_restart(TaskRecord &task_record) {
        if (task_record.status == DW_TASK_STATUS_ERROR) {
            store_.clear_resume(task_record.id);
            store_.clear_segments(task_record.id);
            if (task_record.protocol == DW_PROTOCOL_HTTP) {
                task_record.total_done = -1;
                task_record.progress = -1.0;
            }
        }
    }

    void TaskManager::snapshot_segments_locked(const TaskRecord &task_record) {
        const std::string key = engine_key(task_record);
        if (key.empty()) return;
        if (task_record.protocol == DW_PROTOCOL_HTTP) {
            // HTTP 单文件模型：file_index 固定 0。引擎已回收 ctx 时返回空，不覆盖既有快照。
            if (!http_) return;
            if (const std::vector<dw_byte_range_t> ranges = http_->get_file_ranges(key.c_str(), 0);
                !ranges.empty()) {
                store_.save_segments(task_record.id, 0, ranges);
            }
        } else {
            // BT 多文件：仅对已选文件逐一快照；元数据未就绪 / 无数据的文件返回空则跳过。
            if (!torrent_) return;
            for (const int32_t idx: task_record.file_indexes) {
                if (std::vector<dw_byte_range_t> ranges = torrent_->get_file_ranges(key.c_str(), idx);
                    !ranges.empty()) {
                    store_.save_segments(task_record.id, idx, ranges);
                }
            }
        }
    }
} // namespace dw
