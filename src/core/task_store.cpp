/**
 * @file task_store.cpp
 * @brief 任务持久化存储层实现：SQLite 建表 / 读写与分片续传态序列化。
 *
 * 说明：本层不加锁、不涉及调度与内存注册表，仅围绕 sqlite3 连接完成 TaskRecord 与
 * resume_data 的存取。并发串行化由调用方（TaskManager 持有 mtx_）保证。
 */

#include "task_store.h"

#include "internal/downloader_internal.h"
#include "utils/string_util.h"
#include "utils/time_util.h"

#include <sqlite3.h>

#include <cstdio>

namespace dw {

using utils::now_unix_ms;
using utils::join_lines;
using utils::split_lines;
using utils::join_ints;
using utils::split_ints;

namespace {

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

/// 从查询行填充 TaskRecord（load_active / load_by_url / load_by_info_hash / load_by_id 共用，列序须与 SELECT 一致）。
void fill_record(sqlite3_stmt* st, TaskRecord& r) {
    r.id            = sqlite3_column_int64(st, 0);
    r.info_hash     = col_text(st, 1);
    r.protocol      = static_cast<dw_protocol_t>(sqlite3_column_int(st, 2));
    r.name          = col_text(st, 3);
    r.save_path     = col_text(st, 4);
    r.filename      = col_text(st, 5);
    r.url           = col_text(st, 6);
    r.magnet_link   = col_text(st, 7);
    r.torrent_file  = col_text(st, 8);
    r.trackers      = split_lines(col_text(st, 9));
    r.file_indexes  = split_ints(col_text(st, 10));
    r.priority      = sqlite3_column_int(st, 11);
    r.status        = static_cast<dw_task_status_t>(sqlite3_column_int(st, 12));
    r.progress      = sqlite3_column_double(st, 13);
    r.total_size    = sqlite3_column_int64(st, 14);
    r.total_done    = sqlite3_column_int64(st, 15);
    r.support_range = sqlite3_column_int(st, 16);
    r.etag          = col_text(st, 17);
    r.last_modified = col_text(st, 18);
    r.created_at    = sqlite3_column_int64(st, 19);
    r.parts         = deserialize_parts(col_text(st, 20));
}

} // namespace

TaskStore::~TaskStore() {
    close();
}

bool TaskStore::open(const std::string& path) {
    if (sqlite3_open(path.c_str(), &db_) != SQLITE_OK) {
        db_ = nullptr;
        return false;
    }
    sqlite3_exec(db_, "PRAGMA journal_mode=WAL;", nullptr, nullptr, nullptr);
    sqlite3_exec(db_, "PRAGMA synchronous=NORMAL;", nullptr, nullptr, nullptr);
    return true;
}

void TaskStore::close() {
    if (db_) {
        sqlite3_close(db_);
        db_ = nullptr;
    }
}

void TaskStore::init_schema() {
    // 表结构：自增主键 id（升序即提交次序）。身份为协议专属列——BT 存 info_hash，HTTP 用 url；
    // 不再设唯一约束，去重由业务层在 add 时按 url / info_hash 查重实现。
    // resume_data / task_files 的 task_id 列统一引用 tasks.id（整型自增主键）。
    const char* sql =
        "CREATE TABLE IF NOT EXISTS tasks ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  info_hash TEXT,"
        "  protocol INTEGER,"
        "  name TEXT,"
        "  save_path TEXT,"
        "  filename TEXT,"
        "  url TEXT,"
        "  magnet_link TEXT,"
        "  torrent_file TEXT,"
        "  trackers TEXT,"
        "  file_indexes TEXT,"
        "  priority INTEGER,"
        "  status INTEGER,"
        "  progress REAL,"
        "  total_size INTEGER,"
        "  total_done INTEGER,"
        "  support_range INTEGER,"
        "  etag TEXT,"
        "  last_modified TEXT,"
        "  created_at INTEGER,"
        "  http_parts TEXT"
        ");"
        // 去重查询加速：add 时按协议查 url / info_hash（非唯一索引）。
        "CREATE INDEX IF NOT EXISTS idx_tasks_info_hash ON tasks(info_hash);"
        "CREATE INDEX IF NOT EXISTS idx_tasks_url ON tasks(url);"
        "CREATE TABLE IF NOT EXISTS resume_data ("
        "  task_id INTEGER PRIMARY KEY,"
        "  data BLOB,"
        "  saved_at INTEGER"
        ");"
        "CREATE TABLE IF NOT EXISTS task_files ("
        "  task_id INTEGER NOT NULL,"
        "  file_index INTEGER NOT NULL,"
        "  name TEXT NOT NULL,"
        "  size INTEGER NOT NULL,"
        "  PRIMARY KEY (task_id, file_index)"
        ");"
        "CREATE INDEX IF NOT EXISTS idx_task_files_task_id"
        "  ON task_files(task_id);";
    sqlite3_exec(db_, sql, nullptr, nullptr, nullptr);
}

std::vector<TaskRecord> TaskStore::load_active() {
    // 载入排队 / 活跃任务（DOWNLOADING=0, QUEUED=4）；兼容旧库遗留的 5/6（原解析态），
    // 由上层重启归一化统一回落 QUEUED。暂停(1)/完成(2)/错误(3) 留库，按需回读，减小常驻内存。
    std::vector<TaskRecord> out;
    const char* sql =
        "SELECT id, info_hash, protocol, name, save_path, filename, url, magnet_link,"
        "       torrent_file, trackers, file_indexes, priority,"
        "       status, progress, total_size, total_done, support_range, etag,"
        "       last_modified, created_at, http_parts FROM tasks"
        "  WHERE status IN (0,4,5,6);";
    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return out;

    while (sqlite3_step(st) == SQLITE_ROW) {
        TaskRecord r;
        fill_record(st, r);
        out.push_back(std::move(r));
    }
    sqlite3_finalize(st);
    return out;
}

std::vector<TaskRecord> TaskStore::load_all() {
    // 全量任务（含暂停 / 完成 / 错误），供 dw_list_tasks 快照使用。列序与 fill_record 一致。
    std::vector<TaskRecord> out;
    const char* sql =
        "SELECT id, info_hash, protocol, name, save_path, filename, url, magnet_link,"
        "       torrent_file, trackers, file_indexes, priority,"
        "       status, progress, total_size, total_done, support_range, etag,"
        "       last_modified, created_at, http_parts FROM tasks;";
    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return out;

    while (sqlite3_step(st) == SQLITE_ROW) {
        TaskRecord r;
        fill_record(st, r);
        out.push_back(std::move(r));
    }
    sqlite3_finalize(st);
    return out;
}

namespace {
/// 按协议 + 指定列查询单条任务（列序与 fill_record 一致）。命中填充 out 返回 true。
/// col 为库内固定列名（"url" / "info_hash"），值走绑定参数，无注入风险。
bool load_one_by(sqlite3* db, dw_protocol_t protocol, const char* col,
                 const std::string& value, TaskRecord& out) {
    std::string sql =
        "SELECT id, info_hash, protocol, name, save_path, filename, url, magnet_link,"
        "       torrent_file, trackers, file_indexes, priority,"
        "       status, progress, total_size, total_done, support_range, etag,"
        "       last_modified, created_at, http_parts FROM tasks WHERE protocol=? AND ";
    sql += col;
    sql += "=? LIMIT 1;";
    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db, sql.c_str(), -1, &st, nullptr) != SQLITE_OK) return false;
    sqlite3_bind_int(st, 1, protocol);
    sqlite3_bind_text(st, 2, value.c_str(), -1, SQLITE_TRANSIENT);
    bool found = false;
    if (sqlite3_step(st) == SQLITE_ROW) {
        fill_record(st, out);
        found = true;
    }
    sqlite3_finalize(st);
    return found;
}
} // namespace

bool TaskStore::load_by_url(const std::string& url, TaskRecord& out) {
    // HTTP 按 url 查（url 列有非唯一索引）。业务层据此判重。
    return load_one_by(db_, DW_PROTOCOL_HTTP, "url", url, out);
}

bool TaskStore::load_by_info_hash(const std::string& info_hash, TaskRecord& out) {
    // BT 按 info_hash 查（info_hash 列有非唯一索引）。业务层据此判重。
    return load_one_by(db_, DW_PROTOCOL_TORRENT, "info_hash", info_hash, out);
}

bool TaskStore::load_by_id(int64_t id, TaskRecord& out) {
    const char* sql =
        "SELECT id, info_hash, protocol, name, save_path, filename, url, magnet_link,"
        "       torrent_file, trackers, file_indexes, priority,"
        "       status, progress, total_size, total_done, support_range, etag,"
        "       last_modified, created_at, http_parts"
        " FROM tasks WHERE id=?;";
    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return false;
    sqlite3_bind_int64(st, 1, id);
    bool found = false;
    if (sqlite3_step(st) == SQLITE_ROW) {
        fill_record(st, out);
        found = true;
    }
    sqlite3_finalize(st);
    return found;
}

void TaskStore::insert(TaskRecord& r) {
    // 新增任务：纯 INSERT，回填自增主键。去重已由调用方（add）预先按 url / info_hash 判定。
    const char* sql =
        "INSERT INTO tasks (info_hash, protocol, name, save_path,"
        " filename, url, magnet_link, torrent_file, trackers, file_indexes,"
        " priority, status, progress, total_size, total_done,"
        " support_range, etag, last_modified, created_at, http_parts)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return;

    const std::string trackers = join_lines(r.trackers);
    const std::string indexes  = join_ints(r.file_indexes);
    const std::string parts    = serialize_parts(r.parts);

    sqlite3_bind_text(st, 1, r.info_hash.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(st, 2, r.protocol);
    sqlite3_bind_text(st, 3, r.name.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 4, r.save_path.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 5, r.filename.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 6, r.url.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 7, r.magnet_link.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 8, r.torrent_file.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 9, trackers.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 10, indexes.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(st, 11, r.priority);
    sqlite3_bind_int(st, 12, r.status);
    sqlite3_bind_double(st, 13, r.progress);
    sqlite3_bind_int64(st, 14, r.total_size);
    sqlite3_bind_int64(st, 15, r.total_done);
    sqlite3_bind_int(st, 16, r.support_range);
    sqlite3_bind_text(st, 17, r.etag.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 18, r.last_modified.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(st, 19, r.created_at);
    sqlite3_bind_text(st, 20, parts.c_str(), -1, SQLITE_TRANSIENT);

    if (sqlite3_step(st) == SQLITE_DONE) {
        r.id = sqlite3_last_insert_rowid(db_); // 回填自增主键，供 (priority, id) 排序使用
    }
    sqlite3_finalize(st);
}

void TaskStore::update(const TaskRecord& r) {
    // 更新既有任务：按 id 原地 UPDATE 全字段（保留 rowid 与提交次序）。要求 r.id 有效。
    const char* sql =
        "UPDATE tasks SET info_hash=?, protocol=?, name=?, save_path=?,"
        " filename=?, url=?, magnet_link=?, torrent_file=?, trackers=?, file_indexes=?,"
        " priority=?, status=?, progress=?, total_size=?, total_done=?,"
        " support_range=?, etag=?, last_modified=?, created_at=?, http_parts=?"
        " WHERE id=?;";
    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return;

    const std::string trackers = join_lines(r.trackers);
    const std::string indexes  = join_ints(r.file_indexes);
    const std::string parts    = serialize_parts(r.parts);

    sqlite3_bind_text(st, 1, r.info_hash.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(st, 2, r.protocol);
    sqlite3_bind_text(st, 3, r.name.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 4, r.save_path.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 5, r.filename.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 6, r.url.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 7, r.magnet_link.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 8, r.torrent_file.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 9, trackers.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 10, indexes.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(st, 11, r.priority);
    sqlite3_bind_int(st, 12, r.status);
    sqlite3_bind_double(st, 13, r.progress);
    sqlite3_bind_int64(st, 14, r.total_size);
    sqlite3_bind_int64(st, 15, r.total_done);
    sqlite3_bind_int(st, 16, r.support_range);
    sqlite3_bind_text(st, 17, r.etag.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 18, r.last_modified.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(st, 19, r.created_at);
    sqlite3_bind_text(st, 20, parts.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(st, 21, r.id);

    sqlite3_step(st);
    sqlite3_finalize(st);
}

void TaskStore::remove(int64_t id) {
    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db_, "DELETE FROM tasks WHERE id=?;", -1, &st,
                           nullptr) == SQLITE_OK) {
        sqlite3_bind_int64(st, 1, id);
        sqlite3_step(st);
        sqlite3_finalize(st);
    }
    st = nullptr;
    if (sqlite3_prepare_v2(db_, "DELETE FROM resume_data WHERE task_id=?;", -1,
                           &st, nullptr) == SQLITE_OK) {
        sqlite3_bind_int64(st, 1, id);
        sqlite3_step(st);
        sqlite3_finalize(st);
    }
    // 联动删除任务关联的文件信息（子表统一以自增 id 为键）
    delete_task_files(id);
}

void TaskStore::save_resume(int64_t id,
                            const uint8_t* data, size_t size) {
    const char* sql =
        "INSERT OR REPLACE INTO resume_data (task_id, data, saved_at)"
        " VALUES (?,?,?);";
    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return;
    sqlite3_bind_int64(st, 1, id);
    sqlite3_bind_blob(st, 2, data, static_cast<int>(size), SQLITE_TRANSIENT);
    sqlite3_bind_int64(st, 3, now_unix_ms());
    sqlite3_step(st);
    sqlite3_finalize(st);
}

std::vector<uint8_t> TaskStore::load_resume(int64_t id) {
    std::vector<uint8_t> out;
    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db_, "SELECT data FROM resume_data WHERE task_id=?;",
                           -1, &st, nullptr) != SQLITE_OK) {
        return out;
    }
    sqlite3_bind_int64(st, 1, id);
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

// ---- 任务文件信息 ----

void TaskStore::save_task_files(int64_t id,
                                const std::vector<dw_file_info_t>& files) {
    if (files.empty()) return;

    // 事务包裹批量写入，保证原子性与性能
    sqlite3_exec(db_, "BEGIN;", nullptr, nullptr, nullptr);

    const char* sql =
        "INSERT OR REPLACE INTO task_files (task_id, file_index, name, size)"
        " VALUES (?,?,?,?);";
    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) {
        sqlite3_exec(db_, "ROLLBACK;", nullptr, nullptr, nullptr);
        return;
    }

    for (const auto& f : files) {
        sqlite3_reset(st);
        sqlite3_bind_int64(st, 1, id);
        sqlite3_bind_int(st, 2, f.index);
        sqlite3_bind_text(st, 3, f.name ? f.name : "", -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(st, 4, f.size);
        sqlite3_step(st);
    }
    sqlite3_finalize(st);
    sqlite3_exec(db_, "COMMIT;", nullptr, nullptr, nullptr);
}

std::vector<dw_file_info_t> TaskStore::load_task_files(int64_t id) {
    std::vector<dw_file_info_t> out;
    const char* sql =
        "SELECT file_index, name, size FROM task_files"
        " WHERE task_id=? ORDER BY file_index;";
    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return out;

    sqlite3_bind_int64(st, 1, id);
    while (sqlite3_step(st) == SQLITE_ROW) {
        dw_file_info_t f{};
        f.index = sqlite3_column_int(st, 0);
        const char* n = reinterpret_cast<const char*>(sqlite3_column_text(st, 1));
        if (n) {
            const size_t len = std::strlen(n);
            f.name = static_cast<char*>(std::malloc(len + 1));
            if (f.name) std::memcpy(f.name, n, len + 1);
        } else {
            f.name = nullptr;
        }
        f.size = sqlite3_column_int64(st, 2);
        out.push_back(f);
    }
    sqlite3_finalize(st);
    return out;
}

void TaskStore::delete_task_files(int64_t id) {
    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db_, "DELETE FROM task_files WHERE task_id=?;", -1,
                           &st, nullptr) == SQLITE_OK) {
        sqlite3_bind_int64(st, 1, id);
        sqlite3_step(st);
        sqlite3_finalize(st);
    }
}

} // namespace dw
