#pragma once

#include "core/sqlite_database.h"
#include "core/task.h"
#include <sqlite3.h>
#include <memory>
#include <mutex>

namespace dw {
    /// 处理 tasks 表；借用统一连接，不管理连接生命周期。
    class TaskStore {
    public:
        explicit TaskStore(sqlite3 *const &db) : db_(db) {
        }

        TaskStore(const TaskStore &) = delete;

        TaskStore &operator=(const TaskStore &) = delete;

        bool init_schema() const {
            std::scoped_lock lock(mutex_);
            const bool schema_ready = db_ && sqlite3_exec(
                                          db_,
                                          R"SQL(
                           CREATE TABLE IF NOT EXISTS tasks (
                               protocol INTEGER NOT NULL,
                               task_id TEXT NOT NULL PRIMARY KEY,
                               client_id TEXT NOT NULL,
                               is_remote INTEGER NOT NULL,
                               save_path TEXT NOT NULL,
                               original_root_name TEXT NOT NULL,
                               root_name TEXT NOT NULL,
                               full_path TEXT NOT NULL,
                               file_type INTEGER NOT NULL,
                               ext TEXT NOT NULL,
                               parsed INTEGER NOT NULL,
                               url TEXT NOT NULL,
                               info_hash TEXT NOT NULL,
                               magnet_link TEXT NOT NULL,
                               torrent_file TEXT NOT NULL,
                               status INTEGER NOT NULL,
                               total_size INTEGER NOT NULL,
                               total_done INTEGER NOT NULL,
                               priority INTEGER NOT NULL,
                               reason INTEGER NOT NULL,
                               message TEXT NOT NULL,
                               created_at INTEGER NOT NULL,
                               modified_at INTEGER NOT NULL,
                               is_delete INTEGER NOT NULL,
                               is_read INTEGER NOT NULL DEFAULT 0,
                               name TEXT NOT NULL DEFAULT '',
                               wrapper_name TEXT NOT NULL DEFAULT ''
                           );
                           CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(protocol, status);
                       )SQL",
                                          nullptr, nullptr, nullptr) == SQLITE_OK
                                      && ensure_sqlite_column(db_, "tasks", "is_delete",
                                                              "ALTER TABLE tasks ADD COLUMN is_delete INTEGER NOT NULL DEFAULT 0")
                                      && ensure_sqlite_column(db_, "tasks", "is_read",
                                                              "ALTER TABLE tasks ADD COLUMN is_read INTEGER NOT NULL DEFAULT 0")
                                      && ensure_sqlite_column(db_, "tasks", "name",
                                                              "ALTER TABLE tasks ADD COLUMN name TEXT NOT NULL DEFAULT ''")
                                      && ensure_sqlite_column(db_, "tasks", "wrapper_name",
                                                              "ALTER TABLE tasks ADD COLUMN wrapper_name TEXT NOT NULL DEFAULT ''");
            if (!schema_ready) return false;
            // 旧表的 dirty 列没有默认值，移除后新任务可直接写入。
            if (sqlite3_table_column_metadata(db_, nullptr, "tasks", "dirty",
                                              nullptr, nullptr, nullptr, nullptr, nullptr) != SQLITE_OK)
                return true;
            return sqlite3_exec(db_, "ALTER TABLE tasks DROP COLUMN dirty", nullptr, nullptr, nullptr) == SQLITE_OK;
        }

        // 重新添加时在同一事务中替换任务及其文件。调用方须持有 TaskManager 锁。
        bool replace_task(const Task &task) const {
            const auto &task_id = task.task_id;
            std::scoped_lock lock(mutex_);
            if (!db_ || task_id.empty()) return false;
            // HTTP 工作线程也会使用此连接保存续传数据，防止其写入混入本事务。
            auto *connection_mutex = sqlite3_db_mutex(db_);
            if (connection_mutex) sqlite3_mutex_enter(connection_mutex);
            const std::unique_ptr<sqlite3_mutex, decltype(&sqlite3_mutex_leave)>
                    connection_lock(connection_mutex, &sqlite3_mutex_leave);
            if (sqlite3_exec(db_, "SAVEPOINT replace_task", nullptr, nullptr, nullptr) != SQLITE_OK) return false;
            const bool replaced =
                    delete_for_readd("DELETE FROM tasks WHERE task_id=:task_id", task_id)
                    && delete_for_readd("DELETE FROM task_file WHERE task_id=:task_id", task_id)
                    && write_task(task);
            if (replaced && sqlite3_exec(db_, "RELEASE replace_task", nullptr, nullptr, nullptr) == SQLITE_OK) {
                return true;
            }
            sqlite3_exec(db_, "ROLLBACK TO replace_task; RELEASE replace_task", nullptr, nullptr, nullptr);
            return false;
        }

    private:
        bool delete_for_readd(const char *sql, const std::string &task_id) const {
            sqlite3_stmt *statement = nullptr;
            if (sqlite3_prepare_v2(db_, sql, -1, &statement, nullptr) != SQLITE_OK) return false;
            const bool success =
                    sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":task_id"),
                                      task_id.c_str(), -1, SQLITE_TRANSIENT) == SQLITE_OK &&
                    sqlite3_step(statement) == SQLITE_DONE;
            sqlite3_finalize(statement);
            return success;
        }

        bool write_task(const Task &task) const {
            constexpr auto sql = R"SQL(
                INSERT INTO tasks (
                    protocol, task_id, client_id, is_remote,
                    save_path, original_root_name, root_name, full_path, file_type,
                    ext, parsed, url, info_hash, magnet_link,
                    torrent_file, status, total_size, total_done, priority,
                    reason, message, created_at, modified_at, is_delete,
                    is_read, name, wrapper_name
                ) VALUES (
                    :protocol, :task_id, :client_id, :is_remote,
                    :save_path, :original_root_name, :root_name, :full_path, :file_type,
                    :ext, :parsed, :url, :info_hash, :magnet_link,
                    :torrent_file, :status, :total_size, :total_done, :priority,
                    :reason, :message, :created_at, :modified_at, :is_delete,
                    :is_read, :name, :wrapper_name
                )
                ON CONFLICT(task_id) DO UPDATE SET
                    protocol = excluded.protocol,
                    client_id = excluded.client_id,
                    is_remote = excluded.is_remote,
                    save_path = excluded.save_path,
                    original_root_name = excluded.original_root_name,
                    root_name = excluded.root_name,
                    full_path = excluded.full_path,
                    file_type = excluded.file_type,
                    ext = excluded.ext,
                    parsed = excluded.parsed,
                    url = excluded.url,
                    info_hash = excluded.info_hash,
                    magnet_link = excluded.magnet_link,
                    torrent_file = excluded.torrent_file,
                    status = excluded.status,
                    total_size = excluded.total_size,
                    total_done = excluded.total_done,
                    priority = excluded.priority,
                    reason = excluded.reason,
                    message = excluded.message,
                    created_at = excluded.created_at,
                    modified_at = excluded.modified_at,
                    is_delete = excluded.is_delete,
                    is_read = excluded.is_read,
                    name = excluded.name,
                    wrapper_name = excluded.wrapper_name
            )SQL";
            sqlite3_stmt *statement = nullptr;
            if (sqlite3_prepare_v2(db_, sql, -1, &statement, nullptr) != SQLITE_OK)
                return false;
            sqlite3_bind_int(statement, sqlite3_bind_parameter_index(statement, ":protocol"),
                             static_cast<int>(task.task_protocol));
            sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":task_id"), task.task_id.c_str(), -1,
                              SQLITE_TRANSIENT);
            sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":client_id"), task.client_id.c_str(),
                              -1,
                              SQLITE_TRANSIENT);
            sqlite3_bind_int64(statement, sqlite3_bind_parameter_index(statement, ":is_remote"), task.is_remote);
            sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":save_path"), task.save_path.c_str(),
                              -1,
                              SQLITE_TRANSIENT);
            sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":original_root_name"),
                              task.original_root_name.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":root_name"), task.root_name.c_str(),
                              -1,
                              SQLITE_TRANSIENT);
            sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":full_path"), task.full_path.c_str(),
                              -1,
                              SQLITE_TRANSIENT);
            sqlite3_bind_int64(statement, sqlite3_bind_parameter_index(statement, ":file_type"), task.file_type);
            sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":ext"), task.ext.c_str(), -1,
                              SQLITE_TRANSIENT);
            sqlite3_bind_int64(statement, sqlite3_bind_parameter_index(statement, ":parsed"), task.parsed);
            sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":url"), task.url.c_str(), -1,
                              SQLITE_TRANSIENT);
            sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":info_hash"), task.info_hash.c_str(),
                              -1,
                              SQLITE_TRANSIENT);
            sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":magnet_link"),
                              task.magnet_link.c_str(),
                              -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":torrent_file"),
                              task.torrent_file.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int64(statement, sqlite3_bind_parameter_index(statement, ":status"), task.status);
            sqlite3_bind_int64(statement, sqlite3_bind_parameter_index(statement, ":total_size"), task.total_size);
            sqlite3_bind_int64(statement, sqlite3_bind_parameter_index(statement, ":total_done"), task.total_done);
            sqlite3_bind_int64(statement, sqlite3_bind_parameter_index(statement, ":priority"), task.priority);
            sqlite3_bind_int(statement, sqlite3_bind_parameter_index(statement, ":reason"), task.reason);
            sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":message"), task.message.c_str(), -1,
                              SQLITE_TRANSIENT);
            sqlite3_bind_int64(statement, sqlite3_bind_parameter_index(statement, ":created_at"), task.created_at);
            sqlite3_bind_int64(statement, sqlite3_bind_parameter_index(statement, ":modified_at"), task.modified_at);
            sqlite3_bind_int64(statement, sqlite3_bind_parameter_index(statement, ":is_delete"), task.is_delete);
            sqlite3_bind_int(statement, sqlite3_bind_parameter_index(statement, ":is_read"), task.is_read);
            sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":name"),
                              task.name.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":wrapper_name"),
                              task.wrapper_name.c_str(), -1, SQLITE_TRANSIENT);
            const bool success = sqlite3_step(statement) == SQLITE_DONE;
            sqlite3_finalize(statement);
            return success;
        }

    public:
        bool save(const Task &task) const {
            std::scoped_lock lock(mutex_);
            return db_ && !task.task_id.empty() && write_task(task);
        }

        bool delete_task(const std::string &task_id) const {
            std::scoped_lock lock(mutex_);
            if (!db_ || task_id.empty()) return false;
            sqlite3_stmt *statement = nullptr;
            if (sqlite3_prepare_v2(
                    db_,
                    "UPDATE tasks SET is_delete=1 WHERE task_id=:task_id AND is_delete=0",
                    -1, &statement, nullptr) != SQLITE_OK)
                return false;
            sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":task_id"), task_id.c_str(), -1,
                              SQLITE_TRANSIENT);
            const bool success = sqlite3_step(statement) == SQLITE_DONE;
            sqlite3_finalize(statement);
            return success;
        }

        bool update_read(const std::string &task_id, bool is_read) const {
            std::scoped_lock lock(mutex_);
            sqlite3_stmt *statement = nullptr;
            if (!db_ || sqlite3_prepare_v2(db_,
                                           "UPDATE tasks SET is_read=:is_read WHERE task_id=:task_id AND is_delete=0",
                                           -1, &statement, nullptr) != SQLITE_OK)
                return false;
            const bool success =
                    sqlite3_bind_int(statement, sqlite3_bind_parameter_index(statement, ":is_read"), is_read) ==
                    SQLITE_OK &&
                    sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":task_id"),
                                      task_id.c_str(), -1, SQLITE_TRANSIENT) == SQLITE_OK &&
                    sqlite3_step(statement) == SQLITE_DONE;
            sqlite3_finalize(statement);
            return success;
        }

        /// 按任务标识查询，默认排除软删除记录。
        bool find_by_task_id(const std::string &task_id, Task &out, const bool include_deleted = false) const {
            std::scoped_lock lock(mutex_);
            out = {};
            if (!db_ || task_id.empty()) return false;
            sqlite3_stmt *statement = nullptr;
            if (sqlite3_prepare_v2(db_,
                                   "SELECT * FROM tasks WHERE task_id=:task_id AND (is_delete=0 OR :include_deleted)",
                                   -1, &statement, nullptr) != SQLITE_OK)
                return false;
            sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":task_id"), task_id.c_str(), -1,
                              SQLITE_TRANSIENT);
            sqlite3_bind_int(statement, sqlite3_bind_parameter_index(statement, ":include_deleted"), include_deleted);
            const bool found = sqlite3_step(statement) == SQLITE_ROW;
            if (found) out = read_task(statement);
            sqlite3_finalize(statement);
            return found;
        }

        /// 查询所有未删除的任务。
        std::vector<Task> find_all() const {
            std::scoped_lock lock(mutex_);
            std::vector<Task> tasks;
            sqlite3_stmt *statement = nullptr;
            if (!db_ || sqlite3_prepare_v2(db_,
                                           "SELECT * FROM tasks WHERE is_delete=0 ORDER BY modified_at DESC",
                                           -1, &statement, nullptr) != SQLITE_OK)
                return tasks;
            while (sqlite3_step(statement) == SQLITE_ROW) tasks.push_back(read_task(statement));
            sqlite3_finalize(statement);
            return tasks;
        }

        /// 按任务状态批量查询未删除的任务。
        std::vector<Task> find_by_status(const std::vector<dw_task_status_t> &statuses) const {
            std::scoped_lock lock(mutex_);
            std::vector<Task> tasks;
            if (!db_ || statuses.empty()) return tasks;

            std::string sql = "SELECT * FROM tasks WHERE is_delete=0 AND status IN (";
            std::vector<std::string> params;
            params.reserve(statuses.size());
            for (size_t index = 0; index < statuses.size(); ++index) {
                params.push_back(":status_" + std::to_string(index));
                if (index != 0) sql += ',';
                sql += params.back();
            }
            sql += ") ORDER BY modified_at DESC";

            sqlite3_stmt *statement = nullptr;
            if (sqlite3_prepare_v2(db_, sql.c_str(), -1, &statement, nullptr) != SQLITE_OK) return tasks;
            for (size_t index = 0; index < statuses.size(); ++index) {
                if (sqlite3_bind_int(statement, sqlite3_bind_parameter_index(statement, params[index].c_str()),
                                     static_cast<int>(statuses[index])) != SQLITE_OK) {
                    sqlite3_finalize(statement);
                    return tasks;
                }
            }
            while (sqlite3_step(statement) == SQLITE_ROW) tasks.push_back(read_task(statement));
            sqlite3_finalize(statement);
            return tasks;
        }

    private:
        static Task read_task(sqlite3_stmt *statement);

        sqlite3 *const &db_;
        mutable std::mutex mutex_;
    };

    // 暂保留尚未有新表承接的 HTTP 续传及播放进度。
    class LegacyTaskStore {
    public:
        explicit LegacyTaskStore(sqlite3 *const &db) : db_(db) {
        }

        ~LegacyTaskStore() = default;

        LegacyTaskStore(const LegacyTaskStore &) = delete;

        LegacyTaskStore &operator=(const LegacyTaskStore &) = delete;

        bool init_schema() const;

        /// 写入 / 覆盖断点续传数据。
        void save_resume(const std::string &client_id, dw_protocol_t protocol,
                         const std::string &natural_key, const uint8_t *data, size_t size) const;

        /// 恢复信息结构体：包含 resume data 二进制、magnet_link、torrent_file
        struct ResumeInfo {
            std::vector<uint8_t> data; ///< libtorrent resume 二进制数据
            std::string magnet_link; ///< 磁力链接
            std::string torrent_file; ///< 种子文件路径
        };

        /// 一次性加载全部恢复信息（data + magnet + torrent）
        ResumeInfo load_resume_info(const std::string &client_id, dw_protocol_t protocol,
                                    const std::string &natural_key) const;

        // ---- 播放进度（独立表 play_progress，以完整路径为键）----

        /// 写入 / 覆盖文件播放进度（毫秒）。完整路径由调用方（TaskManager）解析后直传。
        void set_play_position(const std::string &full_path, int64_t position_ms) const;

        /// 读取文件播放进度（毫秒）；无记录返回 0。
        int64_t get_play_position(const std::string &full_path) const;

    private:
        sqlite3 *const &db_;
    };
}
