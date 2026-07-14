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
#include "utils/string_util.h"
#include "utils/time_util.h"

#include <sqlite3.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace dw {

using utils::now_unix_ms;
using utils::join_lines;
using utils::split_lines;
using utils::join_ints;
using utils::split_ints;

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

/// SQLite text 列安全读取（NULL 返回空串）。
std::string col_text(sqlite3_stmt* st, int idx) {
    const unsigned char* t = sqlite3_column_text(st, idx);
    return t ? reinterpret_cast<const char*>(t) : std::string();
}

/// HTTP 分片续传态序列化：每行 "index,start,end,done"，仅存续传必需字段。
std::string serialize_parts(const std::vector<dw_part_state_t>& parts) {
    std::string out;
    for (const auto& p : parts) {
        if (!out.empty()) out.push_back('\n');
        out += std::to_string(p.index); out.push_back(',');
        out += std::to_string(p.start); out.push_back(',');
        out += std::to_string(p.end);   out.push_back(',');
        out += std::to_string(p.done);
    }
    return out;
}

/// 反序列化 HTTP 分片续传态；坏行跳过。size/status 等运行态由引擎续传时重算。
std::vector<dw_part_state_t> deserialize_parts(const std::string& s) {
    std::vector<dw_part_state_t> parts;
    for (const auto& line : split_lines(s)) {
        long long idx = 0, st = 0, en = 0, dn = 0;
        if (std::sscanf(line.c_str(), "%lld,%lld,%lld,%lld", &idx, &st, &en, &dn) != 4)
            continue;
        dw_part_state_t p{};
        p.index  = static_cast<int32_t>(idx);
        p.start  = static_cast<int64_t>(st);
        p.end    = static_cast<int64_t>(en);
        p.done   = static_cast<int64_t>(dn);
        p.size   = (p.end >= p.start) ? (p.end - p.start + 1) : 0;
        p.status = DW_TASK_STATUS_DOWNLOADING;
        p.reason = DW_REASON_NONE;
        parts.push_back(p);
    }
    return parts;
}

/// 从查询行填充 TaskRecord（db_load_all 与 db_load_by_id 共用，列序须与 SELECT 一致）。
void fill_record(sqlite3_stmt* st, TaskRecord& r) {
    r.task_id       = col_text(st, 0);
    r.protocol      = static_cast<dw_protocol_t>(sqlite3_column_int(st, 1));
    r.name          = col_text(st, 2);
    r.save_path     = col_text(st, 3);
    r.filename      = col_text(st, 4);
    r.url           = col_text(st, 5);
    r.magnet_link   = col_text(st, 6);
    r.torrent_file  = col_text(st, 7);
    r.trackers      = split_lines(col_text(st, 8));
    r.file_indexes  = split_ints(col_text(st, 9));
    r.auto_start    = sqlite3_column_int(st, 10);
    r.priority      = sqlite3_column_int(st, 11);
    r.status        = static_cast<dw_task_status_t>(sqlite3_column_int(st, 12));
    r.progress      = sqlite3_column_double(st, 13);
    r.total_size    = sqlite3_column_int64(st, 14);
    r.total_done    = sqlite3_column_int64(st, 15);
    r.support_range = sqlite3_column_int(st, 16);
    r.etag          = col_text(st, 17);
    r.last_modified = col_text(st, 18);
    r.created_at    = sqlite3_column_int64(st, 19);
    r.submit_seq    = sqlite3_column_int64(st, 20);
    r.parts         = deserialize_parts(col_text(st, 21));
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
    if (!db_open(path)) {
        DW_LOGF(DW_LOG_ERROR, "", "[ERROR] TaskManager 打开数据库失败: %s",
                path.c_str());
        return -1;
    }
    db_init_schema();
    db_load_active();
    // submit_seq 单调性需覆盖全量（含未载入的暂停/完成/错误任务），避免新任务序号冲突。
    seq_counter_ = db_max_submit_seq();

    // 重启归一化：既有 downloading/parsing/parsed 状态无引擎支撑，退回 QUEUED 重新准入。
    for (auto& kv : tasks_) {
        TaskRecord& r = kv.second;
        if (status_occupies_slot(r.status)) {
            r.status          = DW_TASK_STATUS_QUEUED;
            r.queued_notified = false;
            db_upsert(r);
        }
    }

    running_.store(true);
    // 初始流量闸门：wifi_only 且非 WiFi 时关闭（挂起 torrent session、不准入新任务）。
    net_allowed_ = !(cfg.wifi_only != 0 && cfg.is_wifi == 0);
    if (!net_allowed_ && torrent_) torrent_->set_network_paused(true);
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
            db_upsert(kv.second);
            kv.second.dirty = false;
        }
    }
    db_close();
    DW_LOG(DW_LOG_INFO, "[CLEANUP] TaskManager 已停止", "");
}

/* ================================================================== */
/*                          控制操作                                  */
/* ================================================================== */

int32_t TaskManager::add(dw_protocol_t proto, const dw_task_params_t* params,
                         dw_submit_result_t* out) {
    if (!params || !out) {
        if (out) { out->task_id = nullptr; out->code = DW_REASON_INVALID_INPUT;
                   out->message = nullptr; }
        return -1;
    }
    // HTTP 允许以 url 兜底作为任务标识；BT 必须显式提供 info_hash。
    const char* raw_id = params->task_id;
    if ((!raw_id || !raw_id[0]) && proto == DW_PROTOCOL_HTTP) raw_id = params->url;
    if (!raw_id || !raw_id[0]) {
        out->task_id = nullptr; out->code = DW_REASON_INVALID_INPUT;
        out->message = nullptr;
        return -1;
    }

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
                db_upsert(r);
            }
            schedule_needed_ = true;
        } else if (TaskRecord existing; db_load_by_id(id, existing)) {
            // 已落库任务（暂停/错误）重新入队；已完成任务保持不变（幂等 no-op）。
            if (existing.status != DW_TASK_STATUS_COMPLETED) {
                existing.status          = DW_TASK_STATUS_QUEUED;
                existing.queued_notified = false;
                tasks_[id] = std::move(existing);
                db_upsert(tasks_[id]);
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
            r.auto_start  = params->auto_start;
            r.priority    = params->priority;
            r.created_at  = now_unix_ms();
            r.submit_seq  = ++seq_counter_;
            r.name        = r.filename.empty() ? id : r.filename;
            r.status      = params->auto_start == 0 ? DW_TASK_STATUS_PAUSED
                                                    : DW_TASK_STATUS_QUEUED;
            r.queued_notified = false;
            tasks_[id] = r;
            db_upsert(r);
            schedule_needed_ = true;
        }
    }
    cv_.notify_one();

    out->task_id = raw_id;
    out->code    = DW_REASON_NONE;
    out->message = nullptr;
    return 0;
}

int32_t TaskManager::pause(dw_protocol_t proto, const char* id,
                           dw_submit_result_t* out) {
    if (!id || !out) {
        if (out) { out->task_id = nullptr; out->code = DW_REASON_INVALID_INPUT;
                   out->message = nullptr; }
        return -1;
    }

    dw_task_status_t prev = DW_TASK_STATUS_QUEUED;
    TaskRecord copy;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        auto it = tasks_.find(id);
        if (it == tasks_.end()) {
            out->task_id = nullptr; out->code = DW_REASON_INVALID_INPUT;
            out->message = nullptr;
            return -1;
        }
        prev = it->second.status;
        it->second.status          = DW_TASK_STATUS_PAUSED;
        it->second.queued_notified = false;
        db_upsert(it->second);
        copy = it->second;
        // 暂停任务落库后从内存移除，仅保留活跃/排队任务常驻。
        tasks_.erase(it);
    }

    // 仅对引擎持有的活跃任务下发暂停；排队 / 终态任务无需通知引擎。
    if (status_occupies_slot(prev)) {
        dw_submit_result_t r{};
        if (proto == DW_PROTOCOL_HTTP) http_->pause_task(id, &r);
        else                           torrent_->pause_task(id, &r);
        dw_submit_result_release(&r);
    }

    emit_synthetic(copy); // 引擎暂停后不推送 paused，需库内合成一次
    {
        std::lock_guard<std::mutex> lock(mtx_);
        schedule_needed_ = true; // 释放的额度供排队任务补位
    }
    cv_.notify_one();

    out->task_id = id;
    out->code    = DW_REASON_NONE;
    out->message = nullptr;
    return 0;
}

int32_t TaskManager::resume(dw_protocol_t /*proto*/, const dw_task_params_t* params,
                            dw_submit_result_t* out) {
    if (!params || !params->task_id || !out) {
        if (out) { out->task_id = nullptr; out->code = DW_REASON_INVALID_INPUT;
                   out->message = nullptr; }
        return -1;
    }
    {
        std::lock_guard<std::mutex> lock(mtx_);
        auto it = tasks_.find(params->task_id);
        if (it != tasks_.end()) {
            // 恢复即重新入队，实际引擎启动交由调度线程按并发额度准入。
            it->second.status          = DW_TASK_STATUS_QUEUED;
            it->second.queued_notified = false;
            db_upsert(it->second);
        } else {
            // 已落库的暂停/错误任务：回读全字段后重新入队。
            TaskRecord r;
            if (!db_load_by_id(params->task_id, r)) {
                out->task_id = nullptr; out->code = DW_REASON_INVALID_INPUT;
                out->message = nullptr;
                return -1;
            }
            r.status          = DW_TASK_STATUS_QUEUED;
            r.queued_notified = false;
            tasks_[params->task_id] = std::move(r);
            db_upsert(tasks_[params->task_id]);
        }
        schedule_needed_ = true;
    }
    cv_.notify_one();

    out->task_id = params->task_id;
    out->code    = DW_REASON_NONE;
    out->message = nullptr;
    return 0;
}

int32_t TaskManager::remove(dw_protocol_t proto, const char* id,
                            dw_submit_result_t* out) {
    if (!id || !out) {
        if (out) { out->task_id = nullptr; out->code = DW_REASON_INVALID_INPUT;
                   out->message = nullptr; }
        return -1;
    }
    {
        std::lock_guard<std::mutex> lock(mtx_);
        auto it = tasks_.find(id);
        const bool present = (it != tasks_.end());
        // 内存未命中时查库确认存在性，避免误删不存在任务仍返回成功。
        if (!present) {
            TaskRecord probe;
            if (!db_load_by_id(id, probe)) {
                out->task_id = nullptr; out->code = DW_REASON_INVALID_INPUT;
                out->message = nullptr;
                return -1;
            }
        } else {
            tasks_.erase(it);
        }
        db_delete(id);
    }

    dw_submit_result_t r{};
    if (proto == DW_PROTOCOL_HTTP) http_->delete_task(id, &r);
    else                           torrent_->delete_task(id, &r);
    dw_submit_result_release(&r);

    {
        std::lock_guard<std::mutex> lock(mtx_);
        schedule_needed_ = true;
    }
    cv_.notify_one();

    out->task_id = id;
    out->code    = DW_REASON_NONE;
    out->message = nullptr;
    return 0;
}

int32_t TaskManager::set_priority(const char* id, int32_t priority) {
    if (!id) return -1;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        auto it = tasks_.find(id);
        if (it != tasks_.end()) {
            it->second.priority = priority;
            db_upsert(it->second);
        } else {
            // 已落库的暂停/错误任务：回读后仅更新优先级并写回。
            TaskRecord r;
            if (!db_load_by_id(id, r)) return -1;
            r.priority = priority;
            db_upsert(r);
        }
        schedule_needed_ = true; // 仅影响排队顺序，不打断下载中的任务
    }
    cv_.notify_one();
    return 0;
}

/* ================================================================== */
/*                          回调拦截                                  */
/* ================================================================== */

void TaskManager::on_progress(const dw_progress_t* p) {
    if (!p || !p->task_id) return;

    bool           need_schedule = false;
    dw_progress_cb cb            = nullptr;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        cb = progress_cb_;
        auto it = tasks_.find(p->task_id);
        if (it != tasks_.end()) {
            TaskRecord& r = it->second;
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
                db_upsert(r);        // erase 前落最终快照（r 引用此后失效）
                tasks_.erase(it);
            }
        }
    }

    if (cb) cb(p); // 原样转发进度给上层

    if (need_schedule) {
        {
            std::lock_guard<std::mutex> lock(mtx_);
            schedule_needed_ = true;
        }
        cv_.notify_one();
    }
}

void TaskManager::on_resume_data(const char* id, dw_protocol_t /*proto*/,
                                 const uint8_t* data, size_t size) {
    if (!id || !data || size == 0) return;
    std::lock_guard<std::mutex> lock(mtx_);
    if (tasks_.find(id) == tasks_.end()) return; // 已删除任务不再持久化
    db_save_resume(id, data, size);
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
            db_upsert(kv.second);
            kv.second.dirty = false;
        }
    }

    // 全量任务来自库（含未常驻内存的暂停/完成/错误任务）。
    const char* sql =
        "SELECT task_id, protocol, name, save_path, filename, status,"
        "       progress, total_size, total_done, priority, created_at"
        " FROM tasks;";
    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) {
        *out_tasks = nullptr; *out_count = 0; return -1;
    }

    std::vector<dw_task_snapshot_t> rows;
    while (sqlite3_step(st) == SQLITE_ROW) {
        dw_task_snapshot_t s{};
        s.task_id    = dup_cstr(col_text(st, 0));
        s.protocol   = static_cast<dw_protocol_t>(sqlite3_column_int(st, 1));
        s.name       = dup_cstr(col_text(st, 2));
        s.save_path  = dup_cstr(col_text(st, 3));
        s.filename   = dup_cstr(col_text(st, 4));
        s.status     = static_cast<dw_task_status_t>(sqlite3_column_int(st, 5));
        s.progress   = sqlite3_column_double(st, 6);
        s.total_size = sqlite3_column_int64(st, 7);
        s.total_done = sqlite3_column_int64(st, 8);
        s.priority   = sqlite3_column_int(st, 9);
        s.created_at = sqlite3_column_int64(st, 10);
        rows.push_back(s);
    }
    sqlite3_finalize(st);

    const int32_t n = static_cast<int32_t>(rows.size());
    if (n == 0) { *out_tasks = nullptr; *out_count = 0; return 0; }

    auto* arr = static_cast<dw_task_snapshot_t*>(
        std::calloc(n, sizeof(dw_task_snapshot_t)));
    if (!arr) {
        // 分配失败：释放已 dup 的字符串，避免泄漏。
        for (auto& s : rows) {
            std::free(const_cast<char*>(s.task_id));
            std::free(const_cast<char*>(s.name));
            std::free(const_cast<char*>(s.save_path));
            std::free(const_cast<char*>(s.filename));
        }
        *out_tasks = nullptr; *out_count = 0; return -1;
    }
    for (int32_t i = 0; i < n; ++i) arr[i] = rows[i];
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
                db_upsert(kv.second);
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
    // 准入循环：额度未满且存在排队任务时，按 (priority, submit_seq) 取最优准入。
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
                (r.priority == best->priority && r.submit_seq < best->submit_seq)) {
                best = &r;
            }
        }
        if (!best) break;

        // 乐观占位：先置为 DOWNLOADING 占用额度，真实状态由引擎进度回调修正。
        best->status = DW_TASK_STATUS_DOWNLOADING;
        best->dirty  = false;
        db_upsert(*best);

        TaskRecord           copy   = *best;
        std::vector<uint8_t> resume;
        if (copy.protocol == DW_PROTOCOL_TORRENT) {
            resume = db_load_resume(copy.task_id);
        }

        lock.unlock();
        bool ok = start_engine_task(copy, resume);
        lock.lock();

        if (!ok) {
            auto it = tasks_.find(copy.task_id);
            if (it != tasks_.end()) {
                it->second.status = DW_TASK_STATUS_ERROR;
                db_upsert(it->second);
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
            db_upsert(r);
            to_notify.push_back(r);
        }
    }
    for (auto& r : to_notify) emit_synthetic(r);
}

bool TaskManager::start_engine_task(const TaskRecord& r,
                                    const std::vector<uint8_t>& resume) {
    dw_task_params_t p{};
    p.task_id    = r.task_id.c_str();
    p.save_path  = r.save_path.c_str();
    p.filename   = r.filename.empty() ? nullptr : r.filename.c_str();
    p.auto_start = 1;
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

/* ================================================================== */
/*                          SQLite 持久化                             */
/* ================================================================== */

bool TaskManager::db_open(const std::string& path) {
    if (sqlite3_open(path.c_str(), &db_) != SQLITE_OK) {
        db_ = nullptr;
        return false;
    }
    sqlite3_exec(db_, "PRAGMA journal_mode=WAL;", nullptr, nullptr, nullptr);
    sqlite3_exec(db_, "PRAGMA synchronous=NORMAL;", nullptr, nullptr, nullptr);
    return true;
}

void TaskManager::db_close() {
    if (db_) {
        sqlite3_close(db_);
        db_ = nullptr;
    }
}

void TaskManager::db_init_schema() {
    const char* sql =
        "CREATE TABLE IF NOT EXISTS tasks ("
        "  task_id TEXT PRIMARY KEY,"
        "  protocol INTEGER,"
        "  name TEXT,"
        "  save_path TEXT,"
        "  filename TEXT,"
        "  url TEXT,"
        "  magnet_link TEXT,"
        "  torrent_file TEXT,"
        "  trackers TEXT,"
        "  file_indexes TEXT,"
        "  auto_start INTEGER,"
        "  priority INTEGER,"
        "  status INTEGER,"
        "  progress REAL,"
        "  total_size INTEGER,"
        "  total_done INTEGER,"
        "  support_range INTEGER,"
        "  etag TEXT,"
        "  last_modified TEXT,"
        "  created_at INTEGER,"
        "  submit_seq INTEGER,"
        "  http_parts TEXT"
        ");"
        "CREATE TABLE IF NOT EXISTS resume_data ("
        "  task_id TEXT PRIMARY KEY,"
        "  data BLOB,"
        "  saved_at INTEGER"
        ");";
    sqlite3_exec(db_, sql, nullptr, nullptr, nullptr);
    // 兼容旧库：为既有 tasks 表补 http_parts 列（新库 CREATE 已含，重复 ADD 报错忽略）。
    sqlite3_exec(db_, "ALTER TABLE tasks ADD COLUMN http_parts TEXT;",
                 nullptr, nullptr, nullptr);
}

void TaskManager::db_load_active() {
    // 仅载入排队 / 活跃任务（DOWNLOADING=0, QUEUED=4, PARSING=5, PARSED=6）；
    // 暂停(1)/完成(2)/错误(3) 留库，按需经 add/resume/list 回读，减小常驻内存。
    const char* sql =
        "SELECT task_id, protocol, name, save_path, filename, url, magnet_link,"
        "       torrent_file, trackers, file_indexes, auto_start, priority,"
        "       status, progress, total_size, total_done, support_range, etag,"
        "       last_modified, created_at, submit_seq, http_parts FROM tasks"
        "  WHERE status IN (0,4,5,6);";
    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return;

    while (sqlite3_step(st) == SQLITE_ROW) {
        TaskRecord r;
        fill_record(st, r);
        tasks_[r.task_id] = std::move(r);
    }
    sqlite3_finalize(st);
}

int64_t TaskManager::db_max_submit_seq() {
    int64_t maxv = 0;
    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db_, "SELECT IFNULL(MAX(submit_seq),0) FROM tasks;",
                           -1, &st, nullptr) != SQLITE_OK) {
        return 0;
    }
    if (sqlite3_step(st) == SQLITE_ROW) maxv = sqlite3_column_int64(st, 0);
    sqlite3_finalize(st);
    return maxv;
}

bool TaskManager::db_load_by_id(const std::string& task_id, TaskRecord& out) {
    const char* sql =
        "SELECT task_id, protocol, name, save_path, filename, url, magnet_link,"
        "       torrent_file, trackers, file_indexes, auto_start, priority,"
        "       status, progress, total_size, total_done, support_range, etag,"
        "       last_modified, created_at, submit_seq, http_parts"
        " FROM tasks WHERE task_id=?;";
    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return false;
    sqlite3_bind_text(st, 1, task_id.c_str(), -1, SQLITE_TRANSIENT);
    bool found = false;
    if (sqlite3_step(st) == SQLITE_ROW) {
        fill_record(st, out);
        found = true;
    }
    sqlite3_finalize(st);
    return found;
}

void TaskManager::db_upsert(const TaskRecord& r) {
    const char* sql =
        "INSERT OR REPLACE INTO tasks (task_id, protocol, name, save_path,"
        " filename, url, magnet_link, torrent_file, trackers, file_indexes,"
        " auto_start, priority, status, progress, total_size, total_done,"
        " support_range, etag, last_modified, created_at, submit_seq,"
        " http_parts)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return;

    const std::string trackers = join_lines(r.trackers);
    const std::string indexes  = join_ints(r.file_indexes);
    const std::string parts    = serialize_parts(r.parts);

    sqlite3_bind_text(st, 1, r.task_id.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(st, 2, r.protocol);
    sqlite3_bind_text(st, 3, r.name.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 4, r.save_path.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 5, r.filename.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 6, r.url.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 7, r.magnet_link.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 8, r.torrent_file.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 9, trackers.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 10, indexes.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(st, 11, r.auto_start);
    sqlite3_bind_int(st, 12, r.priority);
    sqlite3_bind_int(st, 13, r.status);
    sqlite3_bind_double(st, 14, r.progress);
    sqlite3_bind_int64(st, 15, r.total_size);
    sqlite3_bind_int64(st, 16, r.total_done);
    sqlite3_bind_int(st, 17, r.support_range);
    sqlite3_bind_text(st, 18, r.etag.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 19, r.last_modified.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(st, 20, r.created_at);
    sqlite3_bind_int64(st, 21, r.submit_seq);
    sqlite3_bind_text(st, 22, parts.c_str(), -1, SQLITE_TRANSIENT);

    sqlite3_step(st);
    sqlite3_finalize(st);
}

void TaskManager::db_update_progress(const TaskRecord& r) {
    // 进度更新与全量 upsert 复用同一持久化路径（列不多，重写成本可忽略）。
    db_upsert(r);
}

void TaskManager::db_delete(const std::string& task_id) {
    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db_, "DELETE FROM tasks WHERE task_id=?;", -1, &st,
                           nullptr) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, task_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_step(st);
        sqlite3_finalize(st);
    }
    st = nullptr;
    if (sqlite3_prepare_v2(db_, "DELETE FROM resume_data WHERE task_id=?;", -1,
                           &st, nullptr) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, task_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_step(st);
        sqlite3_finalize(st);
    }
}

void TaskManager::db_save_resume(const std::string& task_id,
                                 const uint8_t* data, size_t size) {
    const char* sql =
        "INSERT OR REPLACE INTO resume_data (task_id, data, saved_at)"
        " VALUES (?,?,?);";
    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return;
    sqlite3_bind_text(st, 1, task_id.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_blob(st, 2, data, static_cast<int>(size), SQLITE_TRANSIENT);
    sqlite3_bind_int64(st, 3, now_unix_ms());
    sqlite3_step(st);
    sqlite3_finalize(st);
}

std::vector<uint8_t> TaskManager::db_load_resume(const std::string& task_id) {
    std::vector<uint8_t> out;
    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db_, "SELECT data FROM resume_data WHERE task_id=?;",
                           -1, &st, nullptr) != SQLITE_OK) {
        return out;
    }
    sqlite3_bind_text(st, 1, task_id.c_str(), -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW) {
        const void* blob = sqlite3_column_blob(st, 0);
        const int   n    = sqlite3_column_bytes(st, 0);
        if (blob && n > 0) {
            const auto* p = static_cast<const uint8_t*>(blob);
            out.assign(p, p + n);
        }
    }
    sqlite3_finalize(st);
    return out;
}

} // namespace dw
