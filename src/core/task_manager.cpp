/**
 * @file task_manager.cpp
 * @brief 库内任务中枢实现：SQLite 持久化 + 优先级就绪队列 + 事件驱动准入调度。
 *
 * 并发模型：
 *   - mtx_ 保护注册表 tasks_ 与 DB（sqlite3 串行化模式，读写均在持锁期间）；
 *   - 调度线程是唯一调用引擎"启动动作"与合成回调的线程，规避回调线程重入；
 *   - 引擎启动 / 合成回调一律在释放 mtx_ 后执行，仅在持锁期间读写内存与库。
 */

#include "task_manager.h"

#include "internal/downloader_internal.h"
#include "http/http_engine.h"
#include "torrent/torrent_engine.h"
#include "utils/time_util.h"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ranges>

namespace dw {

using utils::now_unix_ms;

namespace {

/// 占用下载额度的状态集合：明确列举活跃态 {DOWNLOADING, PARSING, PARSED}。
/// 其余状态（QUEUED / PAUSED / COMPLETED / ERROR）均不占额度。
bool status_occupies_slot(dw_task_status_t s) {
    return s == DW_TASK_STATUS_DOWNLOADING ||
           s == DW_TASK_STATUS_PARSING ||
           s == DW_TASK_STATUS_PARSED;
}

/// std::string 拷贝为堆分配 C 字符串（供快照数组使用，调用方 free）。
char* dup_cstr(const std::string& s) {
    char* p = static_cast<char*>(std::malloc(s.size() + 1));
    if (p) std::memcpy(p, s.c_str(), s.size() + 1);
    return p;
}

} // namespace

TaskManager::~TaskManager() {
    stop();
}

void TaskManager::set_engines(HttpEngine* http, TorrentEngine* torrent) {
    http_    = http;
    torrent_ = torrent;
}

void TaskManager::set_progress_cb(dw_progress_cb cb) {
    std::lock_guard<std::mutex> lock(mtx_);
    progress_cb_ = cb;
}

/* ================================================================== */
/*                          生命周期                                  */
/* ================================================================== */

int32_t TaskManager::start(const dw_config_t& cfg) {
    max_concurrent_    = cfg.max_concurrent_downloads > 0
                             ? cfg.max_concurrent_downloads : 3;
    flush_interval_ms_ = cfg.status_callback_interval_ms > 0
                             ? cfg.status_callback_interval_ms : 1000;

    std::string dir  = cfg.work_dir && cfg.work_dir[0] ? cfg.work_dir : ".";
    std::string path = dir + "/leopard_tasks.db";

    std::lock_guard<std::mutex> lock(mtx_);
    if (!store_.open(path)) {
        DW_LOGF(DW_LOG_ERROR, "", "[ERROR] TaskManager 打开数据库失败: %s",
                path.c_str());
        return -1;
    }
    store_.init_schema();
    for (auto& r : store_.load_active()) tasks_[r.task_id] = std::move(r);

    // 重启归一化：既有 downloading/parsing/parsed 状态无引擎支撑，退回 QUEUED 重新准入。
    for (auto &val: tasks_ | std::views::values) {
        if (TaskRecord& r = val; status_occupies_slot(r.status)) {
            r.status          = DW_TASK_STATUS_QUEUED;
            r.queued_notified = false;
            store_.upsert(r);
        }
    }

    running_.store(true);
    schedule_needed_ = true; // 唤醒后立即准入恢复的排队任务
    worker_ = std::thread(&TaskManager::scheduler_loop, this);

    DW_LOGF(DW_LOG_INFO, "", "[EVENT] TaskManager 启动 tasks=%zu concurrent=%d",
            tasks_.size(), max_concurrent_);
    return 0;
}

void TaskManager::stop() {
    if (!running_.exchange(false)) return;
    cv_.notify_all();
    if (worker_.joinable()) worker_.join();

    std::lock_guard<std::mutex> lock(mtx_);
    // 最终刷写脏进度
    for (auto& kv : tasks_) {
        if (kv.second.dirty) {
            store_.upsert(kv.second);
            kv.second.dirty = false;
        }
    }
    store_.close();
    DW_LOG(DW_LOG_INFO, "[CLEANUP] TaskManager 已停止", "");
}

/* ================================================================== */
/*                          控制操作                                  */
/* ================================================================== */

int32_t TaskManager::add(dw_protocol_t proto, const dw_task_params_t* params,
                         dw_submit_result_t* out) {
    if (!params || !out) {
        if (out) { out->task_id = nullptr; out->code = DW_REASON_ERROR;
                   out->message = nullptr; }
        return -1;
    }
    // HTTP 允许以 url 兜底作为任务标识；BT 必须显式提供 info_hash。
    const char* raw_id = params->task_id;
    if ((!raw_id || !raw_id[0]) && proto == DW_PROTOCOL_HTTP) raw_id = params->url;
    if (!raw_id || !raw_id[0]) {
        out->task_id = nullptr; out->code = DW_REASON_ERROR;
        out->message = nullptr;
        return -1;
    }

    int64_t new_id = 0;    // 落库后回填给 out->id（新建或既有）
    {
        std::lock_guard<std::mutex> lock(mtx_);
        const std::string id = raw_id;

        // 幂等：已存在则提升到就绪并触发调度。
        auto it = tasks_.find(id);
        if (it != tasks_.end()) {
            TaskRecord& r = it->second;
            if (!status_occupies_slot(r.status) &&
                r.status != DW_TASK_STATUS_COMPLETED) {
                r.status          = DW_TASK_STATUS_QUEUED;
                r.queued_notified = false;
                store_.upsert(r);
            }
            new_id           = r.id;
            schedule_needed_ = true;
        } else if (TaskRecord existing; store_.load_by_task_id(id, existing)) {
            // 已落库任务（暂停/错误）重新入队；已完成任务保持不变（幂等 no-op）。
            new_id = existing.id;
            if (existing.status != DW_TASK_STATUS_COMPLETED) {
                existing.status          = DW_TASK_STATUS_QUEUED;
                existing.queued_notified = false;
                tasks_[id] = std::move(existing);
                store_.upsert(tasks_[id]);
                schedule_needed_ = true;
            }
        } else {
            TaskRecord r;
            r.task_id     = id;
            r.protocol    = proto;
            r.save_path   = params->save_path ? params->save_path : "";
            r.filename    = params->filename ? params->filename : "";
            r.url         = params->url ? params->url : "";
            r.magnet_link = params->magnet_link ? params->magnet_link : "";
            r.torrent_file = params->torrent_file ? params->torrent_file : "";
            if (params->trackers && params->tracker_count > 0) {
                for (int32_t i = 0; i < params->tracker_count; ++i) {
                    if (params->trackers[i]) r.trackers.emplace_back(params->trackers[i]);
                }
            }
            if (params->file_indexes && params->file_index_size > 0) {
                r.file_indexes.assign(params->file_indexes,
                                      params->file_indexes + params->file_index_size);
            }
            r.priority    = params->priority;
            r.created_at  = now_unix_ms();
            r.name        = r.filename.empty() ? id : r.filename;
            r.status      = DW_TASK_STATUS_QUEUED;
            r.queued_notified = false;
            store_.upsert(r);         // 先落库回填自增 id，再入内存注册表
            tasks_[id] = r;
            new_id           = r.id;
            schedule_needed_ = true;
        }
    }
    cv_.notify_one();

    out->task_id = raw_id;
    out->code    = DW_REASON_NONE;
    out->message = nullptr;
    out->id      = new_id;    // add 成功后回填任务自增 id（交互主键）
    return 0;
}

int32_t TaskManager::pause(dw_protocol_t proto, int64_t id,
                           dw_submit_result_t* out) {
    if (!out) return -1;

    dw_task_status_t prev = DW_TASK_STATUS_QUEUED;
    TaskRecord copy;
    std::string task_id;    // 按 id 翻译得业务键，供锁外喂引擎
    {
        std::lock_guard<std::mutex> lock(mtx_);
        // 低频控制操作：按 id 回读得 task_id（未命中即无效 id）。
        TaskRecord probe;
        if (!store_.load_by_id(id, probe)) {
            out->task_id = nullptr; out->code = DW_REASON_ERROR;
            out->message = nullptr;
            return -1;
        }
        task_id = probe.task_id;
        auto it = tasks_.find(task_id);
        if (it == tasks_.end()) {
            // 仅活跃/排队任务（常驻内存）可暂停；暂停/终态任务视为无效操作。
            out->task_id = nullptr; out->code = DW_REASON_ERROR;
            out->message = nullptr;
            return -1;
        }
        prev = it->second.status;
        it->second.status          = DW_TASK_STATUS_PAUSED;
        it->second.queued_notified = false;
        store_.upsert(it->second);
        copy = it->second;
        // 暂停任务落库后从内存移除，仅保留活跃/排队任务常驻。
        tasks_.erase(it);
    }

    // 仅对引擎持有的活跃任务下发暂停；排队 / 终态任务无需通知引擎。
    if (status_occupies_slot(prev)) {
        dw_submit_result_t r{};
        if (proto == DW_PROTOCOL_HTTP) http_->pause_task(task_id.c_str(), &r);
        else                           torrent_->pause_task(task_id.c_str(), &r);
        dw_submit_result_release(&r);
    }

    emit_synthetic(copy); // 引擎暂停后不推送 paused，需库内合成一次
    {
        std::lock_guard<std::mutex> lock(mtx_);
        schedule_needed_ = true; // 释放的额度供排队任务补位
    }
    cv_.notify_one();

    out->task_id = nullptr;
    out->code    = DW_REASON_NONE;
    out->message = nullptr;
    out->id      = id;
    return 0;
}

int32_t TaskManager::resume(dw_protocol_t /*proto*/, int64_t id,
                            dw_submit_result_t* out) {
    if (!out) return -1;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        // 低频控制操作：按 id 回读全字段（未命中即无效 id）。
        TaskRecord probe;
        if (!store_.load_by_id(id, probe)) {
            out->task_id = nullptr; out->code = DW_REASON_ERROR;
            out->message = nullptr;
            return -1;
        }
        const std::string task_id = probe.task_id;
        auto it = tasks_.find(task_id);
        if (it != tasks_.end()) {
            // 恢复即重新入队，实际引擎启动交由调度线程按并发额度准入。
            it->second.status          = DW_TASK_STATUS_QUEUED;
            it->second.queued_notified = false;
            store_.upsert(it->second);
        } else {
            // 已落库的暂停/错误任务：复用回读记录后重新入队。
            probe.status          = DW_TASK_STATUS_QUEUED;
            probe.queued_notified = false;
            tasks_[task_id] = std::move(probe);
            store_.upsert(tasks_[task_id]);
        }
        schedule_needed_ = true;
    }
    cv_.notify_one();

    out->task_id = nullptr;
    out->code    = DW_REASON_NONE;
    out->message = nullptr;
    out->id      = id;
    return 0;
}

int32_t TaskManager::remove(dw_protocol_t proto, int64_t id,
                            dw_submit_result_t* out) {
    if (!out) return -1;

    std::string task_id;    // 按 id 翻译得业务键，供锁外喂引擎
    {
        std::lock_guard<std::mutex> lock(mtx_);
        // 低频控制操作：按 id 回读得 task_id（未命中即无效 id）。
        TaskRecord probe;
        if (!store_.load_by_id(id, probe)) {
            out->task_id = nullptr; out->code = DW_REASON_ERROR;
            out->message = nullptr;
            return -1;
        }
        task_id = probe.task_id;
        auto it = tasks_.find(task_id);
        if (it != tasks_.end()) tasks_.erase(it);
        store_.remove(task_id);
    }

    dw_submit_result_t r{};
    if (proto == DW_PROTOCOL_HTTP) http_->delete_task(task_id.c_str(), &r);
    else                           torrent_->delete_task(task_id.c_str(), &r);
    dw_submit_result_release(&r);

    {
        std::lock_guard<std::mutex> lock(mtx_);
        schedule_needed_ = true;
    }
    cv_.notify_one();

    out->task_id = nullptr;
    out->code    = DW_REASON_NONE;
    out->message = nullptr;
    out->id      = id;
    return 0;
}

int32_t TaskManager::set_priority(int64_t id, int32_t priority) {
    {
        std::lock_guard<std::mutex> lock(mtx_);
        // 低频控制操作：按 id 回读得 task_id（未命中即无效 id）。
        TaskRecord probe;
        if (!store_.load_by_id(id, probe)) return -1;
        const std::string task_id = probe.task_id;
        auto it = tasks_.find(task_id);
        if (it != tasks_.end()) {
            it->second.priority = priority;
            store_.upsert(it->second);
        } else {
            // 已落库的暂停/错误任务：复用回读记录仅更新优先级并写回。
            probe.priority = priority;
            store_.upsert(probe);
        }
        schedule_needed_ = true; // 仅影响排队顺序，不打断下载中的任务
    }
    cv_.notify_one();
    return 0;
}

bool TaskManager::task_id_of(int64_t id, std::string& out_task_id) {
    std::lock_guard<std::mutex> lock(mtx_);
    TaskRecord r;
    if (!store_.load_by_id(id, r)) return false;
    out_task_id = r.task_id;
    return true;
}

/* ================================================================== */
/*                          回调拦截                                  */
/* ================================================================== */

void TaskManager::on_progress(const dw_progress_t* p) {
    if (!p || !p->task_id) return;

    bool           need_schedule = false;
    dw_progress_cb cb            = nullptr;
    int64_t        resolved_id   = 0;    // 命中记录的自增 id，转发前填入 dw_progress_t.id
    {
        std::lock_guard<std::mutex> lock(mtx_);
        cb = progress_cb_;
        auto it = tasks_.find(p->task_id);
        if (it != tasks_.end()) {
            TaskRecord& r = it->second;
            resolved_id = r.id;    // erase 前捕获（终态分支会移除记录）
            const dw_task_status_t before = r.status;

            r.status     = p->task_status;
            r.progress   = p->progress;
            r.total_size = p->total_size;
            r.total_done = p->total_done;
            if (p->name && p->name[0])          r.name          = p->name;
            if (p->filename && p->filename[0])  r.filename      = p->filename;
            r.support_range = p->support_range;
            if (p->etag && p->etag[0])          r.etag          = p->etag;
            if (p->last_modified && p->last_modified[0]) r.last_modified = p->last_modified;
            // HTTP 分片续传态随进度落库（BT 无分片，part_count=0）。
            if (r.protocol == DW_PROTOCOL_HTTP && p->part_count > 0 && p->part_states) {
                r.parts.assign(p->part_states, p->part_states + p->part_count);
            }
            r.dirty = true;

            // 任务由占额度状态跌落为释放额度状态时，唤醒调度补位。
            if (status_occupies_slot(before) && !status_occupies_slot(r.status)) {
                need_schedule = true;
            }

            // 终态（完成/错误）落库后即从内存移除，仅保留活跃/排队任务常驻。
            if (r.status == DW_TASK_STATUS_COMPLETED ||
                r.status == DW_TASK_STATUS_ERROR) {
                store_.upsert(r);        // erase 前落最终快照（r 引用此后失效）
                tasks_.erase(it);
            }
        }
    }

    // 转发前填入自增 id（p 为引擎构造，携带 task_id 作识别通道；id 由库内补齐）。
    if (cb) {
        dw_progress_t fwd = *p;    // 浅拷贝：同步转发期间指针字段仍有效
        fwd.id = resolved_id;
        cb(&fwd);
    }

    if (need_schedule) {
        {
            std::lock_guard<std::mutex> lock(mtx_);
            schedule_needed_ = true;
        }
        cv_.notify_one();
    }
}

int64_t TaskManager::on_resume_data(const char* task_id, dw_protocol_t /*proto*/,
                                    const uint8_t* data, size_t size) {
    if (!task_id || !data || size == 0) return 0;
    std::lock_guard<std::mutex> lock(mtx_);
    auto it = tasks_.find(task_id);
    if (it == tasks_.end()) return 0; // 已删除任务不再持久化
    store_.save_resume(task_id, data, size);
    return it->second.id;    // 返回自增 id 供上层回调
}

/* ================================================================== */
/*                          快照查询                                  */
/* ================================================================== */

int32_t TaskManager::list(dw_task_snapshot_t** out_tasks, int32_t* out_count) {
    if (!out_tasks || !out_count) return -1;

    std::lock_guard<std::mutex> lock(mtx_);

    // 先刷写活跃任务脏进度，使快照反映最新内存态（暂停/完成/错误已在状态迁移时落库）。
    for (auto& kv : tasks_) {
        if (kv.second.dirty) {
            store_.upsert(kv.second);
            kv.second.dirty = false;
        }
    }

    // 全量任务来自库（含未常驻内存的暂停/完成/错误任务）。
    std::vector<TaskRecord> all = store_.load_all();
    const int32_t n = static_cast<int32_t>(all.size());
    if (n == 0) { *out_tasks = nullptr; *out_count = 0; return 0; }

    auto* arr = static_cast<dw_task_snapshot_t*>(
        std::calloc(n, sizeof(dw_task_snapshot_t)));
    if (!arr) { *out_tasks = nullptr; *out_count = 0; return -1; }

    // 逐条从 TaskRecord 投影为 C ABI 快照（字符串堆分配，调用方经 dw_free_task_list 释放）。
    for (int32_t i = 0; i < n; ++i) {
        const TaskRecord& r = all[i];
        dw_task_snapshot_t s{};
        s.id         = r.id;
        s.task_id    = dup_cstr(r.task_id);
        s.protocol   = r.protocol;
        s.name       = dup_cstr(r.name);
        s.save_path  = dup_cstr(r.save_path);
        s.filename   = dup_cstr(r.filename);
        s.status     = r.status;
        s.progress   = r.progress;
        s.total_size = r.total_size;
        s.total_done = r.total_done;
        s.priority   = r.priority;
        s.created_at = r.created_at;
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
    std::unique_lock<std::mutex> lock(mtx_);
    while (running_.load()) {
        cv_.wait_for(lock, std::chrono::milliseconds(flush_interval_ms_),
                     [this] { return !running_.load() || schedule_needed_; });
        if (!running_.load()) break;

        // 周期刷写脏进度（持久化与调度职责分离）。
        for (auto& kv : tasks_) {
            if (kv.second.dirty) {
                store_.upsert(kv.second);
                kv.second.dirty = false;
            }
        }

        if (schedule_needed_) {
            schedule_needed_ = false;
            run_schedule(lock);
        }

        // 引擎周期性维护策略：HTTP 回收终态任务上下文、Torrent 释放已达分享率的做种任务。
        // 锁外调用，避免与引擎内部锁交叉等待。
        lock.unlock();
        if (http_)    http_->sweep();
        if (torrent_) torrent_->sweep();
        lock.lock();
    }
}

void TaskManager::run_schedule(std::unique_lock<std::mutex>& lock) {
    // 准入循环：额度未满且存在排队任务时，按 (priority, id) 取最优准入。
    // 流量闸门关闭时不准入任何新任务，仅保留下方的 QUEUED 合成通知。
    while (running_.load() && net_allowed_) {
        int32_t active = active_count_locked();
        if (active >= max_concurrent_) break;

        TaskRecord* best = nullptr;
        for (auto& kv : tasks_) {
            TaskRecord& r = kv.second;
            if (r.status != DW_TASK_STATUS_QUEUED) continue;
            if (!best ||
                r.priority > best->priority ||
                (r.priority == best->priority && r.id < best->id)) {
                best = &r;
            }
        }
        if (!best) break;

        // 乐观占位：先置为 DOWNLOADING 占用额度，真实状态由引擎进度回调修正。
        best->status = DW_TASK_STATUS_DOWNLOADING;
        best->dirty  = false;
        store_.upsert(*best);

        TaskRecord           copy   = *best;
        std::vector<uint8_t> resume;
        if (copy.protocol == DW_PROTOCOL_TORRENT) {
            resume = store_.load_resume(copy.task_id);
        }

        lock.unlock();
        bool ok = start_engine_task(copy, resume);
        lock.lock();

        if (!ok) {
            auto it = tasks_.find(copy.task_id);
            if (it != tasks_.end()) {
                it->second.status = DW_TASK_STATUS_ERROR;
                store_.upsert(it->second);
            }
        }
    }

    // 收集仍在排队、尚未通知过的任务，锁外合成 QUEUED 回调。
    std::vector<TaskRecord> to_notify;
    for (auto& kv : tasks_) {
        TaskRecord& r = kv.second;
        if (r.status == DW_TASK_STATUS_QUEUED && !r.queued_notified) {
            r.queued_notified = true;
            to_notify.push_back(r);
        }
    }
    if (!to_notify.empty()) {
        lock.unlock();
        for (auto& r : to_notify) emit_synthetic(r);
        lock.lock();
    }
}

void TaskManager::set_network_allowed(bool allowed) {
    std::vector<std::string> http_to_pause;   // 需停传输的活跃 HTTP 任务
    {
        std::lock_guard<std::mutex> lock(mtx_);
        if (net_allowed_ == allowed) return;   // 状态未变，幂等跳过
        net_allowed_ = allowed;
        if (!allowed) {
            // 闸门关闭：torrent 整体挂起（含做种流量），收集活跃 HTTP 任务待锁外停线程。
            if (torrent_) torrent_->set_network_paused(true);
            for (auto& kv : tasks_) {
                TaskRecord& r = kv.second;
                if (r.protocol == DW_PROTOCOL_HTTP && status_occupies_slot(r.status)) {
                    http_to_pause.push_back(r.task_id);
                }
            }
        } else {
            // 闸门开启：恢复 session，唤醒调度自动重启排队任务（HTTP 走既有 QUEUED→准入路径）。
            if (torrent_) torrent_->set_network_paused(false);
            schedule_needed_ = true;
        }
    }
    if (allowed) { cv_.notify_one(); return; }

    // 锁外停止 HTTP 传输线程（pause_task 内部 join，不可持 mtx_ 调用以规避死锁）。
    for (const auto& id : http_to_pause) {
        dw_submit_result_t res{};
        http_->pause_task(id.c_str(), &res);
        dw_submit_result_release(&res);
    }

    // 线程已停，回落 QUEUED 待闸门开启后重启；收集 QUEUED 合成通知。
    std::vector<TaskRecord> to_notify;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        for (const auto& id : http_to_pause) {
            auto it = tasks_.find(id);
            if (it == tasks_.end()) continue;
            TaskRecord& r = it->second;
            if (r.protocol != DW_PROTOCOL_HTTP) continue;
            r.status          = DW_TASK_STATUS_QUEUED;
            r.queued_notified = true;   // 已在此处合成，避免调度重复通知
            store_.upsert(r);
            to_notify.push_back(r);
        }
    }
    for (auto& r : to_notify) emit_synthetic(r);
}

// ---- 任务文件持久化 ----

void TaskManager::save_files(const std::string& task_id,
                             const std::vector<dw_file_info_t>& files) {
    std::lock_guard<std::mutex> lock(mtx_);
    store_.save_task_files(task_id, files);
}

std::vector<dw_file_info_t> TaskManager::load_files(const std::string& task_id) {
    std::lock_guard<std::mutex> lock(mtx_);
    return store_.load_task_files(task_id);
}

bool TaskManager::start_engine_task(const TaskRecord& r,
                                    const std::vector<uint8_t>& resume) {
    dw_task_params_t p{};
    p.task_id    = r.task_id.c_str();
    p.save_path  = r.save_path.c_str();
    p.filename   = r.filename.empty() ? nullptr : r.filename.c_str();
    p.trace_id   = r.task_id.c_str();
    p.priority   = r.priority;

    std::vector<const char*> tk;
    if (r.protocol == DW_PROTOCOL_HTTP) {
        p.url = r.url.empty() ? r.task_id.c_str() : r.url.c_str();
    } else {
        if (!r.magnet_link.empty())  p.magnet_link  = r.magnet_link.c_str();
        if (!r.torrent_file.empty()) p.torrent_file = r.torrent_file.c_str();
        if (!r.trackers.empty()) {
            for (auto& t : r.trackers) tk.push_back(t.c_str());
            p.trackers      = tk.data();
            p.tracker_count = static_cast<int32_t>(tk.size());
        }
        if (!r.file_indexes.empty()) {
            p.file_indexes    = r.file_indexes.data();
            p.file_index_size = static_cast<int32_t>(r.file_indexes.size());
        }
        if (!resume.empty()) {
            p.resume_data      = resume.data();
            p.resume_data_size = resume.size();
        }
    }

    dw_submit_result_t out{};
    int32_t rc;
    if (r.protocol == DW_PROTOCOL_HTTP) rc = http_->add_task(&p, &out);
    else                                rc = torrent_->add_task(&p, &out);
    dw_submit_result_release(&out);
    return rc == 0;
}

void TaskManager::emit_synthetic(const TaskRecord& r) {
    dw_progress_cb cb;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        cb = progress_cb_;
    }
    if (!cb) return;

    dw_progress_t p{};
    p.id               = r.id;
    p.task_id          = r.task_id.c_str();
    p.trace_id         = r.task_id.c_str();
    p.protocol         = r.protocol;
    p.name             = r.name.c_str();
    p.output_path      = r.save_path.c_str();
    p.filename         = r.filename.c_str();
    p.total_size       = r.total_size;
    p.total_done       = r.total_done;
    p.remaining        = -1;
    p.progress         = r.progress;
    p.download_rate    = 0;
    p.eta              = -1;
    p.task_status      = r.status;
    p.reason           = DW_REASON_NONE;
    p.message          = "";
    p.saved_at_unix_ms = now_unix_ms();
    p.support_range    = r.support_range;
    p.etag             = r.etag.c_str();
    p.last_modified    = r.last_modified.c_str();
    p.probing          = 0;
    p.upload_rate      = 0;
    p.total_upload     = 0;
    p.is_seeding       = 0;
    p.state            = 0;
    p.peers_count      = 0;
    p.part_states      = nullptr;
    p.part_count       = 0;
    cb(&p);
}

int32_t TaskManager::active_count_locked() const {
    int32_t n = 0;
    for (auto& kv : tasks_) {
        if (status_occupies_slot(kv.second.status)) ++n;
    }
    return n;
}

} // namespace dw
