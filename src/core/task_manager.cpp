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

#include <sqlite3.h>

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <sstream>

namespace dw {

namespace {

/// 当前 Unix 毫秒时间戳。
int64_t now_unix_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

/// 占用下载额度的状态集合：非 {QUEUED, PAUSED, COMPLETED, ERROR} 即占额度。
bool status_occupies_slot(dw_task_status_t s) {
    return s != DW_TASK_STATUS_QUEUED && s != DW_TASK_STATUS_PAUSED &&
           s != DW_TASK_STATUS_COMPLETED && s != DW_TASK_STATUS_ERROR;
}

/// std::string 拷贝为堆分配 C 字符串（供快照数组使用，调用方 free）。
char* dup_cstr(const std::string& s) {
    char* p = static_cast<char*>(std::malloc(s.size() + 1));
    if (p) std::memcpy(p, s.c_str(), s.size() + 1);
    return p;
}

/// 以 '\n' 连接字符串数组（trackers 序列化）。
std::string join_lines(const std::vector<std::string>& v) {
    std::string out;
    for (size_t i = 0; i < v.size(); ++i) {
        if (i) out.push_back('\n');
        out += v[i];
    }
    return out;
}

/// 按 '\n' 拆分（trackers 反序列化），忽略空行。
std::vector<std::string> split_lines(const std::string& s) {
    std::vector<std::string> out;
    std::string line;
    std::istringstream iss(s);
    while (std::getline(iss, line)) {
        if (!line.empty()) out.push_back(line);
    }
    return out;
}

/// 以 ',' 连接整型数组（file_indexes 序列化）。
std::string join_ints(const std::vector<int32_t>& v) {
    std::string out;
    for (size_t i = 0; i < v.size(); ++i) {
        if (i) out.push_back(',');
        out += std::to_string(v[i]);
    }
    return out;
}

/// 按 ',' 拆分整型数组（file_indexes 反序列化）。
std::vector<int32_t> split_ints(const std::string& s) {
    std::vector<int32_t> out;
    std::string tok;
    std::istringstream iss(s);
    while (std::getline(iss, tok, ',')) {
        if (!tok.empty()) out.push_back(std::atoi(tok.c_str()));
    }
    return out;
}

/// SQLite text 列安全读取（NULL 返回空串）。
std::string col_text(sqlite3_stmt* st, int idx) {
    const unsigned char* t = sqlite3_column_text(st, idx);
    return t ? reinterpret_cast<const char*>(t) : std::string();
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
    db_load_all();

    // 重启归一化：既有 downloading/parsing/parsed 状态无引擎支撑，退回 QUEUED 重新准入。
    for (auto& kv : tasks_) {
        TaskRecord& r = kv.second;
        if (status_occupies_slot(r.status)) {
            r.status          = DW_TASK_STATUS_QUEUED;
            r.queued_notified = false;
            db_upsert(r);
        }
        if (r.submit_seq > seq_counter_) seq_counter_ = r.submit_seq;
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
        if (it == tasks_.end()) {
            out->task_id = nullptr; out->code = DW_REASON_INVALID_INPUT;
            out->message = nullptr;
            return -1;
        }
        // 恢复即重新入队，实际引擎启动交由调度线程按并发额度准入。
        it->second.status          = DW_TASK_STATUS_QUEUED;
        it->second.queued_notified = false;
        db_upsert(it->second);
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
        if (it == tasks_.end()) {
            out->task_id = nullptr; out->code = DW_REASON_INVALID_INPUT;
            out->message = nullptr;
            return -1;
        }
        tasks_.erase(it);
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
        if (it == tasks_.end()) return -1;
        it->second.priority = priority;
        db_upsert(it->second);
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
            r.dirty = true;

            // 任务由占额度状态跌落为释放额度状态时，唤醒调度补位。
            if (status_occupies_slot(before) && !status_occupies_slot(r.status)) {
                need_schedule = true;
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
    const int32_t n = static_cast<int32_t>(tasks_.size());
    if (n == 0) { *out_tasks = nullptr; *out_count = 0; return 0; }

    auto* arr = static_cast<dw_task_snapshot_t*>(
        std::calloc(n, sizeof(dw_task_snapshot_t)));
    if (!arr) { *out_tasks = nullptr; *out_count = 0; return -1; }

    int32_t i = 0;
    for (auto& kv : tasks_) {
        const TaskRecord& r = kv.second;
        arr[i].task_id    = dup_cstr(r.task_id);
        arr[i].protocol   = r.protocol;
        arr[i].name       = dup_cstr(r.name);
        arr[i].save_path  = dup_cstr(r.save_path);
        arr[i].filename   = dup_cstr(r.filename);
        arr[i].status     = r.status;
        arr[i].progress   = r.progress;
        arr[i].total_size = r.total_size;
        arr[i].total_done = r.total_done;
        arr[i].priority   = r.priority;
        arr[i].created_at = r.created_at;
        ++i;
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
                db_upsert(kv.second);
                kv.second.dirty = false;
            }
        }

        if (schedule_needed_) {
            schedule_needed_ = false;
            run_schedule(lock);
        }
    }
}

void TaskManager::run_schedule(std::unique_lock<std::mutex>& lock) {
    // 准入循环：额度未满且存在排队任务时，按 (priority, submit_seq) 取最优准入。
    while (running_.load()) {
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
        "  submit_seq INTEGER"
        ");"
        "CREATE TABLE IF NOT EXISTS resume_data ("
        "  task_id TEXT PRIMARY KEY,"
        "  data BLOB,"
        "  saved_at INTEGER"
        ");";
    sqlite3_exec(db_, sql, nullptr, nullptr, nullptr);
}

void TaskManager::db_load_all() {
    const char* sql =
        "SELECT task_id, protocol, name, save_path, filename, url, magnet_link,"
        "       torrent_file, trackers, file_indexes, auto_start, priority,"
        "       status, progress, total_size, total_done, support_range, etag,"
        "       last_modified, created_at, submit_seq FROM tasks;";
    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return;

    while (sqlite3_step(st) == SQLITE_ROW) {
        TaskRecord r;
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
        tasks_[r.task_id] = std::move(r);
    }
    sqlite3_finalize(st);
}

void TaskManager::db_upsert(const TaskRecord& r) {
    const char* sql =
        "INSERT OR REPLACE INTO tasks (task_id, protocol, name, save_path,"
        " filename, url, magnet_link, torrent_file, trackers, file_indexes,"
        " auto_start, priority, status, progress, total_size, total_done,"
        " support_range, etag, last_modified, created_at, submit_seq)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return;

    const std::string trackers = join_lines(r.trackers);
    const std::string indexes  = join_ints(r.file_indexes);

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
