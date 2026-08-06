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
#include "utils/time_util.h"
#include "utils/unique_name.h"

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <optional>
#include <ranges>
#include <unordered_set>

#include <boost/asio.hpp>

namespace dw {
    using utils::now_unix_ms;

    namespace {
        /// 占用下载额度的状态集合：活跃态 {DOWNLOADING, RESOLVING, PARSED}。
        /// RESOLVING（BT 等待元数据）和 PARSED（元数据就绪待准入）均占用名额防止超发；
        /// 其余状态（QUEUED / PAUSED / COMPLETED / ERROR）均不占额度。
        bool status_occupies_slot(dw_task_status_t s) {
            return s == DW_TASK_STATUS_DOWNLOADING ||
                   s == DW_TASK_STATUS_RESOLVING ||
                   s == DW_TASK_STATUS_PARSED;
        }

        /// 已下载区间是否完全覆盖文件 [0, file_size-1]（区间须已合并且按 start 升序）。
        /// file_size <= 0 时无法判定，返回 false（HTTP chunked 无总长场景）。
        bool is_file_complete(const std::vector<dw_byte_range_t> &segs, int64_t file_size) {
            if (segs.empty() || file_size <= 0) return false;
            int64_t cursor = 0;
            for (const auto &s : segs) {
                if (s.start > cursor) return false; // 有间隙
                cursor = std::max(cursor, s.end + 1);
                if (cursor >= file_size) return true;
            }
            return false;
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

        // 停机兜底：强制处理残余待删项（跳过释放确认，尽力删除一次）。
        process_pending_deletes(true);

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
        {
            std::lock_guard<std::mutex> lock(mtx_);
            const std::string key = raw_key;

            // 同 key 重新添加：撤销尚未执行的待删文件意图，防旧删除项在新任务
            // 释放后误删其落盘产物。
            if (pending_deletes_.erase(key) > 0) {
                DW_LOGF(DW_LOG_INFO, "",
                        "[EVENT] 重新添加撤销待删文件意图 key=%s", key.c_str());
            }

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
                // filename 为定名凭证（非空即已判重定名并已物化磁盘占位），恒由定名链
                // 写入：HTTP 经引擎探测上调、BT 经 RESOLVING 校验拍。add 一律不预填，
                // 外部指定名不被接受——预填会产生无占位的凭证，令该名对判重不可见。
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
                // 初始显示名占位为识别键（HTTP url / BT info_hash / magnet link），
                // 待引擎回报真实名后经事件同步。
                task_record.name = key;
                // 所有任务直接入队（QUEUED），不等待文件选择。
                // BT 未携带 file_indexes 表示下载全部文件；torrent 文件由 App 先解析
                // 后携带 file_indexes 添加。判重定名统一由事件驱动处理。
                task_record.status = DW_TASK_STATUS_QUEUED;
                task_record.synth_notified = false;
                store_.insert(task_record);
                task_id = task_record.id;
                register_task(std::move(task_record));
                schedule_needed_ = true;
            }
        }
        cv_.notify_all();

        out->code = DW_REASON_NONE;
        out->message = nullptr;
        out->id = task_id;
        return 0;
    }

    int32_t TaskManager::pause(const dw_protocol_t proto, const int64_t id,
                               dw_submit_result_t *out) {
        if (!out) return -1;

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
                                const int32_t delete_files, dw_submit_result_t *out) {
        if (!out) return -1;

        std::string key;
        std::string save_path, filename; // 登记待删文件项所需的落盘路径要素
        {
            std::lock_guard<std::mutex> lock(mtx_);
            if (const auto it = tasks_.find(id);
                it != tasks_.end()) {
                key = engine_key(it->second);
                save_path = it->second.save_path;
                filename = it->second.filename;
                unregister_task(id);
            } else {
                TaskRecord task_record;
                if (!store_.load_by_id(id, task_record)) {
                    out->code = DW_REASON_ERROR;
                    out->message = nullptr;
                    return -1;
                }
                key = engine_key(task_record);
                save_path = task_record.save_path;
                filename = task_record.filename;
            }
            store_.remove(id);
            if (!filename.empty()) {
                // filename 为定名凭证：为空即从未定名占位，无需处理。
                // save_path 已含包层目录（冲突时直接追加），直接拼接 filename。
                // delete_files=1 删整棵产物；=0 仅回收仍为空的占位条目（放弃该名字的持有），
                // 已有真实数据则原样保留。两者均登记待删项，由 B 线程在引擎确认资源
                // 释放（task_released）后执行，避免删到引擎仍持有的路径。
                pending_deletes_[key] = PendingFileDelete{
                        proto, (std::filesystem::path(save_path) / filename).string(),
                        delete_files == 0};
            }
        }

        // 引擎仅释放运行时资源（0=已接管释放，1=未持有即已释放）；文件删除统一由
        // B 线程按上方登记项处理，此处无需按返回值分支。
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
                    // PAUSED / ERROR：确认即入队等待准入。
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

    /* ==================================================================
     *                          引擎事件消费                                */
    /* ================================================================== */

    void TaskManager::on_engine_event(EngineEvent event) {
        // 经 Boost.Asio io_context::post 投递到 B 线程消费，线程安全。
        // 事件经值语义拷贝后投递，调用方无需保持数据存活。
        boost::asio::post(event_ioc_, [this, ev = std::move(event)]() mutable {
            consume_engine_event(std::move(ev));
        });
        // 唤醒 B 线程及时处理事件（不等维护周期超时）。
        cv_.notify_all();
    }

    void TaskManager::consume_engine_event(EngineEvent event) {
        // B 线程消费单个引擎事件。事件消费在锁外进行磁盘检测，锁内更新状态。
        const std::string &key = event.engine_key;
        if (key.empty()) return;

        switch (event.type) {
        case EngineEventType::PARSED: {
            // 解析完成（含元数据+文件信息，或存储迁移完成）。
            // 若 name 非空：首次解析，冲突检测后定名；若 name 为空但 save_path 非空：存储迁移完成确认。
            int64_t task_id = 0;
            std::string task_name;
            std::string task_save_path;
            bool multi_file = false;
            bool is_storage_move = event.name.empty() && !event.save_path.empty();
            {
                std::lock_guard<std::mutex> lock(mtx_);
                task_id = id_of_engine_key(event.protocol, key);
                if (task_id == 0) return;
                const auto it = tasks_.find(task_id);
                if (it == tasks_.end()) return;
                
                if (is_storage_move) {
                    // 存储迁移完成：确认 PARSED
                    if (it->second.status != DW_TASK_STATUS_RESOLVING) return;
                    it->second.status = DW_TASK_STATUS_PARSED;
                    it->second.bt_naming_ready = 2; // 迁移完成
                    it->second.filename = it->second.name; // 定名凭证
                    it->second.dirty = true;
                    DW_LOGF(DW_LOG_INFO, "", "[EVENT] 存储迁移完成，转解析完成 id=%lld",
                            static_cast<long long>(task_id));
                    schedule_needed_ = true;
                    return;
                }
                
                // 首次解析：元数据就绪
                if (it->second.status != DW_TASK_STATUS_RESOLVING) return;
                task_name = event.name;
                task_save_path = event.save_path;
                multi_file = event.multi_file;
                // 回填任务名称（元数据就绪后才知道种子名）
                if (!task_name.empty()) {
                    it->second.name = task_name;
                    it->second.bt_metadata_ready = true;
                }
            }

            if (task_name.empty()) return;

            // 冲突检测：检查活跃任务（RESOLVING/PARSED/DOWNLOADING）+ 磁盘
            bool conflict = false;
            {
                std::lock_guard<std::mutex> lock(mtx_);
                for (const auto &[id, rec] : tasks_) {
                    if (id == task_id) continue;
                    if (rec.status != DW_TASK_STATUS_RESOLVING &&
                        rec.status != DW_TASK_STATUS_PARSED &&
                        rec.status != DW_TASK_STATUS_DOWNLOADING) continue;
                    // 拼接其他任务的有效路径（save_path 已含包层目录）
                    std::string other_path = rec.save_path;
                    if (!rec.filename.empty()) other_path += "/" + rec.filename;
                    else other_path += "/" + rec.name;
                    std::string target_path = task_save_path + "/" + task_name;
                    if (other_path == target_path) {
                        conflict = true;
                        break;
                    }
                }
            }
            // 磁盘检测（锁外）
            if (!conflict) {
                const std::filesystem::path target =
                    std::filesystem::path(task_save_path) / task_name;
                std::error_code ec;
                conflict = std::filesystem::exists(target, ec);
            }

            if (conflict) {
                // 有冲突：计算新 save_path（包层目录），调用引擎迁移。
                // acquire_unique_name 返回包层目录名（原名(n)），引擎 save_path 迁至该目录。
                const std::string wrap = utils::acquire_unique_name(
                    task_save_path, task_name, multi_file);
                const std::string effective =
                    (std::filesystem::path(task_save_path) / wrap).string();
                DW_LOGF(DW_LOG_INFO, "", "[EVENT] 解析完成但重名，迁移路径 id=%lld -> '%s'",
                        static_cast<long long>(task_id), effective.c_str());
                if (torrent_) {
                    torrent_->move_storage(key.c_str(), effective.c_str());
                    // 迁移进行中，状态保持 RESOLVING，等 storage_moved_alert
                    std::lock_guard<std::mutex> lock(mtx_);
                    if (auto it = tasks_.find(task_id); it != tasks_.end()) {
                        it->second.bt_naming_ready = 1; // 迁移进行中
                        // 回写 save_path（迁移完成后由 PARSED 事件确认）
                        it->second.save_path = effective;
                    }
                }
            } else {
                // 无冲突：直接 PARSED，持久化文件列表。
                DW_LOGF(DW_LOG_INFO, "", "[EVENT] 解析完成，无冲突 id=%lld name='%s'",
                        static_cast<long long>(task_id), task_name.c_str());
                {
                    std::lock_guard<std::mutex> lock(mtx_);
                    if (auto it = tasks_.find(task_id); it != tasks_.end()) {
                        it->second.status = DW_TASK_STATUS_PARSED;
                        it->second.filename = task_name; // 定名凭证
                        it->second.dirty = true;
                    }
                }
                // 文件列表落库（锁外）
                if (!event.files.empty()) {
                    store_.save_task_files(task_id, event.files);
                }
                schedule_needed_ = true;
            }
            break;
        }
        case EngineEventType::DOWNLOAD_FAILED: {
            // 下载失败（通用）：转 ERROR。
            int64_t task_id = 0;
            {
                std::lock_guard<std::mutex> lock(mtx_);
                task_id = id_of_engine_key(event.protocol, key);
                if (task_id == 0) return;
                const auto it = tasks_.find(task_id);
                if (it == tasks_.end()) return;
                it->second.status = DW_TASK_STATUS_ERROR;
                it->second.reason = event.reason;
                it->second.message = event.message;
                it->second.dirty = true;
            }
            DW_LOGF(DW_LOG_ERROR, "", "[ERROR] 下载失败 id=%lld msg=%s",
                    static_cast<long long>(task_id), event.message.c_str());
            schedule_needed_ = true;
            break;
        }
        case EngineEventType::DOWNLOAD_COMPLETED: {
            // 下载完成：迁 COMPLETED 状态。
            int64_t task_id = 0;
            {
                std::lock_guard<std::mutex> lock(mtx_);
                task_id = id_of_engine_key(event.protocol, key);
                if (task_id == 0) return;
                const auto it = tasks_.find(task_id);
                if (it == tasks_.end()) return;
                it->second.status = DW_TASK_STATUS_COMPLETED;
                it->second.dirty = true;
            }
            DW_LOGF(DW_LOG_INFO, "", "[OK] 下载完成 id=%lld", static_cast<long long>(task_id));
            schedule_needed_ = true;
            break;
        }
        case EngineEventType::STATUS_UPDATE: {
            // 状态+进度更新：写入 TaskRecord 内存字段。
            if (!event.valid) return;
            std::lock_guard<std::mutex> lock(mtx_);
            const int64_t id = id_of_engine_key(event.protocol, key);
            if (id == 0) return; // 任务不在内存中，静默丢弃
            const auto it = tasks_.find(id);
            if (it == tasks_.end()) return;
            TaskRecord &rec = it->second;

            // 进度数值
            rec.progress = event.progress;
            rec.total_size = event.total_size;
            rec.total_done = event.total_done;
            rec.download_rate = event.download_rate;
            rec.upload_rate = event.upload_rate;
            rec.support_range = event.support_range;
            rec.reason = event.reason;
            rec.message = event.message;

            // 元数据字段（仅非空时覆盖，防引擎早期帧冲刷已有值）
            if (!event.name.empty() && rec.filename.empty()) {
                rec.name = event.name;
                if (event.protocol == DW_PROTOCOL_HTTP) {
                    rec.filename = event.name;
                }
            }
            if (!event.etag.empty()) rec.etag = event.etag;
            if (!event.last_modified.empty()) rec.last_modified = event.last_modified;

            // BT 扩展
            rec.bt_multi_file = event.multi_file;
            
            // 终态信号：引擎报完成/错误时写入，A 线程消费后迁权威态
            if (event.status == DW_TASK_STATUS_COMPLETED || event.status == DW_TASK_STATUS_ERROR) {
                rec.pending_engine_status = event.status;
            }
            
            rec.dirty = true;
            break;
        }
        }
    }
    
    /* ==================================================================
     *                          唯一名定名                                *
     * ================================================================== */

    std::string TaskManager::resolve_and_record_name(const char *engine_key, const dw_protocol_t proto,
                                                     const std::string &dir, const std::string &name,
                                                     const bool multi_file) {
        if (!engine_key || !engine_key[0] || name.empty()) return name;
        std::lock_guard<std::mutex> lock(mtx_);
        if (const int64_t id = id_of_engine_key(proto, engine_key); id != 0) {
            if (const auto it = tasks_.find(id); it != tasks_.end()) {
                return resolve_and_record_name_locked(it->second, dir, name, multi_file);
            }
        }
        // 任务未常驻内存（罕见：上调早于登记 / 已被逐出）：回落库定位记录后仍走定名落库。
        TaskRecord task_record;
        const bool found = (proto == DW_PROTOCOL_HTTP)
                               ? store_.load_by_url(engine_key, task_record)
                               : store_.load_by_info_hash(engine_key, task_record);
        if (found) {
            return resolve_and_record_name_locked(task_record, dir, name, multi_file);
        }
        // 未知任务：仅抢名返回，不落库（磁盘占位已在抢名时物化）。
        return utils::acquire_unique_name(dir, name, multi_file);
    }

    std::string TaskManager::resolve_and_record_name_locked(TaskRecord &rec,
                                                            const std::string &dir,
                                                            const std::string &name,
                                                            const bool multi_file) {
        // 幂等重入：同一原名已定名（持久预留跨重启有效），直接沿用既有第一层条目名，
        // 避免自身包层目录/半成品文件被当作冲突源导致序号漂移或二次包层。
        // 同时幂等补齐占位——重启恢复后引擎会再次上调定名，此间占位若被外部清理，
        // 补回可维持名字持有的连续性，防其他任务判重时看不见该名。
        if (!rec.filename.empty() && rec.filename == name) {
            utils::ensure_placeholder(dir, rec.filename, multi_file);
            return rec.filename;
        }
        // 判重真相源为磁盘：抢名即物化占位（未冲突建原名条目，冲突建包层目录），
        // 不再查库取占用名集合。
        std::string place_err;
        const std::string unique = utils::acquire_unique_name(dir, name, multi_file, &place_err);
        if (!place_err.empty()) {
            DW_LOGF(DW_LOG_ERROR, "", "[ERROR] 定名占位创建失败: %s/%s (%s)",
                    dir.c_str(), unique.c_str(), place_err.c_str());
        }
        // 包层方案：冲突时名称一律不改，落盘位置包一层唯一目录。
        // save_path 直接包含包层目录（冲突时追加），filename = 原名快照兼定名凭证。
        rec.name = name;
        rec.filename = name;
        // 冲突时 save_path 追加包层目录
        if (unique != name) {
            rec.save_path = (std::filesystem::path(dir) / unique).string();
        } else {
            rec.save_path = dir;
        }
        store_.update(rec); // 落库即完成持久预留（跨重启有效）。
        // 文件表由引擎在事件点经 post_task_files 自主推送，此处不再代写。
        if (unique != name) {
            DW_LOGF(DW_LOG_INFO, "", "[EVENT] 重名包层定名: '%s' -> '%s/' (dir=%s id=%lld)",
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
            dw_progress_cb cb = nullptr;
            bool wake_schedule = false;

            {
                std::unique_lock<std::mutex> lock(mtx_);
                cv_.wait_for(lock, std::chrono::milliseconds(flush_interval_ms_),
                             [this] { return !running_.load(); });
                if (!running_.load()) break;

                // 采集数据
                collect_progress_locked(fwd_records, resolve_actions);
                cb = progress_cb_;
                // 采集拍产生终态（COMPLETED/ERROR）即置 schedule_needed_，记录到本地，
                // 锁外与 slot_released 合并 notify 立即唤醒 B 线程调度，不等维护周期超时。
                wake_schedule = schedule_needed_;
            } // ← 作用域退出，自动释放 mtx_

            // 触发引擎续传检查点（锁外，绝不持 mtx_）：post_updates 让 Torrent 引擎
            // 携变更门槛请求续传（无变化不产生 alert，去重下沉 libtorrent）。
            // 进度推送已由引擎经 STATUS_UPDATE 事件实时完成，此处仅驱动续传检查点。
            for (IDownloadEngine *eng: {http_, torrent_}) {
                if (eng) eng->post_updates();
            }

            // RESOLVING/PARSED 校验执行（锁外引擎调用，回锁校验后定态；期间可能已被暂停/删除，
            // 仅对应状态才迁移）：
            //   BT PARSED：事件驱动已完成冲突检测与文件落库，此处仅 apply_file_selection
            //       定型开下 → DOWNLOADING；
            //   HTTP RESOLVING：定名已在引擎 finalize_probing 完成，此处入引擎（携续传存档）→
            //       成功迁 DOWNLOADING，失败迁 ERROR 释放名额。
            bool slot_released = false;
            for (const auto &act: resolve_actions) {
                if (act.rec.protocol == DW_PROTOCOL_TORRENT) {
                    if (!torrent_) continue;
                    const std::string &key = act.rec.info_hash;
                    // BT PARSED：冲突检测已由事件完成，直接 apply_file_selection 开下载。
                    if (torrent_->apply_file_selection(
                            key.c_str(),
                            act.rec.file_indexes.empty() ? nullptr : act.rec.file_indexes.data(),
                            static_cast<int32_t>(act.rec.file_indexes.size())) != 0) {
                        continue;
                    }
                    std::lock_guard<std::mutex> lock(mtx_);
                    if (const auto it = tasks_.find(act.rec.id);
                        it != tasks_.end() && it->second.status == DW_TASK_STATUS_PARSED) {
                        it->second.status = DW_TASK_STATUS_DOWNLOADING;
                        it->second.dirty = true; // 落库交 B 线程 flush
                        DW_LOGF(DW_LOG_INFO, "", "[EVENT] 任务解析完成转下载 id=%lld",
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

            if (slot_released || wake_schedule) cv_.notify_all();

            // 锁外转发（周期节奏）：只读锁内已拷出的本地副本（已含遥测），避免与上层回调重入交叉。
            for (const auto &rec: fwd_records) {
                emit_progress(cb, rec);
            }
        }
    }

    void TaskManager::collect_progress_locked(std::vector<TaskRecord> &fwd_records,
                                              std::vector<ResolveAction> &resolve_actions) {
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
                task_record.status != DW_TASK_STATUS_RESOLVING &&
                task_record.status != DW_TASK_STATUS_PARSED)
                continue;

            // HTTP RESOLVING 校验拍：直接收集启动动作交 scheduler_loop 锁外入引擎。
            // 定名下放引擎 finalize_probing 单点（首个响应到达时优先级链取名，
            // 经 request_unique_name 上调判重定名并落库+写文件表凭证）。
            if (task_record.status == DW_TASK_STATUS_RESOLVING &&
                task_record.protocol == DW_PROTOCOL_HTTP) {
                resolve_actions.push_back({
                    task_record, store_.load_resume(task_record.id)
                });
                continue;
            }

            // 推模型：进度字段已由引擎线程经 on_progress 实时写入 TaskRecord，
            // 此处仅判终态与收集校验动作，不再调 query_progress。

            // 终态消费：引擎推入 COMPLETED/ERROR 后 pending_engine_status 非 QUEUED，
            // 消费一次迁权威态并释放名额。
            if (task_record.pending_engine_status == DW_TASK_STATUS_COMPLETED ||
                task_record.pending_engine_status == DW_TASK_STATUS_ERROR) {
                task_record.status = task_record.pending_engine_status;
                task_record.pending_engine_status = DW_TASK_STATUS_QUEUED; // 消费后复位
                schedule_needed_ = true;
            }

            fwd_records.push_back(task_record);

            // BT PARSED 校验拍：元数据就绪且冲突检测已通过（事件驱动），
            // 收集动作交 scheduler_loop 锁外 apply_file_selection 后迁 DOWNLOADING。
            // 置于终态判断之后：本拍已迁终态的任务不再收集。
            if (task_record.status == DW_TASK_STATUS_PARSED &&
                task_record.protocol == DW_PROTOCOL_TORRENT) {
                resolve_actions.push_back({
                    task_record, {}
                });
            }
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
            // B 线程消费引擎事件（Boost.Asio io_context 投递的 PARSED/STORAGE_MOVED 等）。
            // poll 非阻塞，处理所有待消费事件后立即返回。
            event_ioc_.poll();

            if (http_) http_->sweep();
            if (torrent_) torrent_->sweep();
            process_pending_deletes();
        }
    }

    void TaskManager::process_pending_deletes(const bool force) {
        // 锁内快照待删项，释放确认与文件删除均在锁外执行（引擎调用 / 磁盘 IO 不占 mtx_）。
        std::vector<std::pair<std::string, PendingFileDelete>> snapshot;
        {
            std::lock_guard<std::mutex> lock(mtx_);
            if (pending_deletes_.empty()) return;
            snapshot.assign(pending_deletes_.begin(), pending_deletes_.end());
        }

        for (auto &[key, item]: snapshot) {
            if (!force) {
                IDownloadEngine *eng = engine_of(item.proto);
                if (eng && !eng->task_released(key.c_str())) continue; // 资源未释放，下拍再查
            }
            {
                // 回锁出队；erase==0 说明该项已被同 key 重新添加撤销，跳过删除。
                std::lock_guard<std::mutex> lock(mtx_);
                if (pending_deletes_.erase(key) == 0) continue;
            }
            if (item.placeholder_only) {
                // 仅回收空占位：release_placeholder 只删空目录 / 零字节文件，
                // 已有真实数据一律不动（delete_files=0 语义：保留文件，只放弃名字）。
                const std::filesystem::path p(item.path);
                if (utils::release_placeholder(p.parent_path().string(),
                                              p.filename().string())) {
                    DW_LOGF(DW_LOG_INFO, "", "[CLEANUP] 回收空占位完成 path=%s",
                            item.path.c_str());
                }
                continue;
            }
            // 仅尝试一次，成败均不重试（POSIX 下占用文件 unlink 立即成功，
            // inode 随最后一个 fd 关闭回收；目标不存在时 remove_all 幂等无错）。
            std::error_code ec;
            std::filesystem::remove_all(item.path, ec);
            if (ec) {
                DW_LOGF(DW_LOG_ERROR, "", "[ERROR] 删除落盘产物失败 path=%s err=%s",
                        item.path.c_str(), ec.message().c_str());
            } else {
                DW_LOGF(DW_LOG_INFO, "", "[CLEANUP] 删除落盘产物完成 path=%s",
                        item.path.c_str());
            }
        }
    }

    void TaskManager::maintenance_persist_locked() {
        std::vector<int64_t> to_remove;
        for (auto &[id, task_record]: tasks_) {
            const bool paused = (task_record.status == DW_TASK_STATUS_PAUSED);
            // 初判终态（含引擎驱动的 pending_engine_status 信号）
            const bool terminal = (task_record.status == DW_TASK_STATUS_COMPLETED ||
                                   task_record.status == DW_TASK_STATUS_ERROR ||
                                   task_record.pending_engine_status == DW_TASK_STATUS_COMPLETED ||
                                   task_record.pending_engine_status == DW_TASK_STATUS_ERROR);
            if (task_record.status == DW_TASK_STATUS_DOWNLOADING || terminal || paused) {
                snapshot_segments_locked(task_record);
            }
            // snapshot 可能经数据驱动置 pending_engine_status=COMPLETED，重判终态
            const bool now_terminal = (task_record.status == DW_TASK_STATUS_COMPLETED ||
                                       task_record.status == DW_TASK_STATUS_ERROR ||
                                       task_record.pending_engine_status == DW_TASK_STATUS_COMPLETED ||
                                       task_record.pending_engine_status == DW_TASK_STATUS_ERROR);
            if (now_terminal) {
                // 任务级 0→2 传播：完成态任务把其文件节点统一置为完成正常。
                if (task_record.status == DW_TASK_STATUS_COMPLETED ||
                    task_record.pending_engine_status == DW_TASK_STATUS_COMPLETED) {
                    store_.mark_task_files_completed(id);
                }
                to_remove.push_back(id);
            } else if (paused) {
                if (task_record.protocol == DW_PROTOCOL_HTTP) {
                    // HTTP 暂停态延迟逐出：待引擎 ctx 被 sweep 回收（task_released 确认）后
                    // 再移出内存。此时 worker 已结束并经 post_resume_data 汇入 pending_resume，
                    // 由下方 flush_dirty_locked 落库，规避先逐出导致异步 resume 被 on_resume_data 丢弃。
                    if (!http_ || http_->task_released(engine_key(task_record).c_str())) {
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
        p.protocol = rec.protocol;
        p.name = rec.name.c_str();
        // output_path 为权威 save_path（已含包层目录，冲突时直接追加）。
        p.output_path = rec.save_path.c_str();
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

            // BT 入引擎等元数据；HTTP 延后到定名后入引擎。
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

    std::vector<dw_file_info_t> TaskManager::load_files(const int64_t id) {
        std::lock_guard<std::mutex> lock(mtx_);
        std::vector<dw_file_info_t> files = store_.load_task_files(id);
        if (files.empty()) return files;
        // 惰性删除检测：仅对完成态(2)文件 stat 物理路径，缺失则标记为已删除(1)。
        // 物理路径 = save_path + name（save_path 已含包层目录，name 已含完整相对路径）。
        TaskRecord task_record;
        if (!store_.load_by_id(id, task_record) || task_record.save_path.empty()) return files;
        std::filesystem::path base(task_record.save_path);
        for (auto &f: files) {
            if (f.status != 2) continue;
            std::filesystem::path p(base);
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
        segment_cache_.erase(id); // 同步清理分段缓存
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
            segment_cache_.erase(task_record.id); // 同步清理内存缓存
            if (task_record.protocol == DW_PROTOCOL_HTTP) {
                task_record.total_done = -1;
                task_record.progress = -1.0;
            }
        }
    }

    void TaskManager::snapshot_segments_locked(TaskRecord &task_record) {
        const std::string key = engine_key(task_record);
        if (key.empty()) return;

        // 收集每个已选文件的最新分段，同时更新内存缓存与 DB 快照。
        // HTTP 单文件 file_index=0；BT 遍历已选 file_indexes。
        struct file_result { int32_t file_index; std::vector<dw_byte_range_t> ranges; };
        std::vector<file_result> results;

        if (task_record.protocol == DW_PROTOCOL_HTTP) {
            if (!http_) return;
            auto ranges = http_->get_file_ranges(key.c_str(), 0);
            if (!ranges.empty()) {
                results.push_back({0, std::move(ranges)});
            }
        } else {
            if (!torrent_) return;
            for (const int32_t idx : task_record.file_indexes) {
                auto ranges = torrent_->get_file_ranges(key.c_str(), idx);
                if (!ranges.empty()) {
                    results.push_back({idx, std::move(ranges)});
                }
            }
        }

        // 写入缓存 + DB
        for (const auto &fr : results) {
            segment_cache_[task_record.id][fr.file_index] = fr.ranges;
            store_.save_segments(task_record.id, fr.file_index, fr.ranges);
        }

        // ---- 数据驱动完成检测（仅 DOWNLOADING 态） ----
        if (task_record.status != DW_TASK_STATUS_DOWNLOADING) return;

        // 从 task_files 表一次性加载文件大小（B 线程持 mtx_，DB 访问安全）。
        // file_size <= 0 的文件跳过完成判定（HTTP chunked 无总长场景）。
        const auto all_files = store_.load_task_files(task_record.id);

        bool all_complete = true;
        bool any_checkable = false; // 是否有可判定的文件（file_size > 0）
        for (const auto &fr : results) {
            // 从已加载的文件列表查当前文件大小
            int64_t file_size = -1;
            for (const auto &f : all_files) {
                if (f.index == fr.file_index) {
                    file_size = f.size;
                    break;
                }
            }
            if (file_size <= 0) {
                all_complete = false;
                continue; // 不可判定（HTTP chunked 无总长），不阻塞整体判定
            }
            any_checkable = true;
            if (is_file_complete(fr.ranges, file_size)) {
                store_.mark_file_completed(task_record.id, fr.file_index);
            } else {
                all_complete = false;
            }
        }

        // 所有可判定文件均完成 → 数据驱动任务完成（设 pending_engine_status 交 A 线程消费）
        if (any_checkable && all_complete) {
            task_record.pending_engine_status = DW_TASK_STATUS_COMPLETED;
            DW_LOGF(DW_LOG_INFO, "", "[OK] 数据驱动任务完成 id=%lld",
                    (long long)task_record.id);
        }
    }

    std::vector<dw_byte_range_t> TaskManager::get_cached_segments(int64_t id, int32_t file_index) {
        std::lock_guard<std::mutex> lock(mtx_);
        const auto it = segment_cache_.find(id);
        if (it == segment_cache_.end()) return {};
        const auto fit = it->second.find(file_index);
        if (fit == it->second.end()) return {};
        return fit->second;
    }

    int32_t TaskManager::get_task_status(int64_t id) {
        std::lock_guard<std::mutex> lock(mtx_);
        const auto it = tasks_.find(id);
        if (it == tasks_.end()) return -1;
        return static_cast<int32_t>(it->second.status);
    }
} // namespace dw
