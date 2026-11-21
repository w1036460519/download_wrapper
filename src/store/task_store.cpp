#include "task_store.h"

#include <filesystem>

#include <sqlite3.h>

#include <string>

namespace dw {
    namespace {
        /// SQLite text 列安全读取（NULL 返回空串）。
        std::string col_text(sqlite3_stmt *st, int idx) {
            const unsigned char *t = sqlite3_column_text(st, idx);
            return t ? reinterpret_cast<const char *>(t) : std::string();
        }

        // 查询列顺序须与 TaskColumn 一致。
        constexpr char task_select[] = R"SQL(
            SELECT task_id, client_id, is_remote, name, relative_full_path, file_type, ext,
                   url, info_hash, magnet_link, torrent_file, protocol, status, total_size, total_done,
                   message, created_at, modified_at, is_delete, is_read, storage_root_id, relative_save_path, rate_history
            FROM tasks
        )SQL";

        enum TaskColumn {
            col_task_id,
            col_client_id,
            col_is_remote,
            col_name,
            col_relative_full_path,
            col_file_type,
            col_ext,
            col_url,
            col_info_hash,
            col_magnet_link,
            col_torrent_file,
            col_protocol,
            col_status,
            col_total_size,
            col_total_done,
            col_message,
            col_created_at,
            col_modified_at,
            col_is_delete,
            col_is_read,
            col_storage_root_id,
            col_relative_save_path,
            col_rate_history,
        };

        Task read_task(sqlite3_stmt *statement) {
            Task task;
            task.task_id = col_text(statement, col_task_id);
            task.client_id = col_text(statement, col_client_id);
            task.is_remote = sqlite3_column_int(statement, col_is_remote) != 0;
            task.name = col_text(statement, col_name);
            task.storage_root_id = col_text(statement, col_storage_root_id);
            task.relative_save_path = col_text(statement, col_relative_save_path);
            task.full_path = col_text(statement, col_relative_full_path);
            task.file_type = sqlite3_column_int(statement, col_file_type) != 0;
            task.ext = col_text(statement, col_ext);
            task.url = col_text(statement, col_url);
            task.info_hash = col_text(statement, col_info_hash);
            task.magnet_link = col_text(statement, col_magnet_link);
            task.torrent_file = col_text(statement, col_torrent_file);
            task.task_protocol = static_cast<dw_protocol_t>(sqlite3_column_int(statement, col_protocol));
            task.status = sqlite3_column_int(statement, col_status);
            task.total_size = sqlite3_column_int64(statement, col_total_size);
            task.total_done = sqlite3_column_int64(statement, col_total_done);
            task.message = col_text(statement, col_message);
            task.created_at = sqlite3_column_int64(statement, col_created_at);
            task.modified_at = sqlite3_column_int64(statement, col_modified_at);
            task.is_delete = sqlite3_column_int(statement, col_is_delete) != 0;
            task.is_read = sqlite3_column_int(statement, col_is_read) != 0;
            boost::system::error_code error;
            const auto history = boost::json::parse(col_text(statement, col_rate_history), error);
            if (!error && history.is_array() && history.as_array().size() == 2) {
                const auto &rates = history.as_array();
                if (rates[0].is_array()) task.download_rate_history = rates[0].as_array();
                if (rates[1].is_array()) task.upload_rate_history = rates[1].as_array();
            }
            return task;
        }
    } // namespace

    TaskStore::TaskStore(sqlite3 *const &db) : db_(db) {}

    bool TaskStore::init_schema() const {
        std::scoped_lock lock(mutex_);
        constexpr auto sql = R"SQL(
            CREATE TABLE IF NOT EXISTS tasks (
                protocol INTEGER NOT NULL,
                task_id TEXT NOT NULL PRIMARY KEY,
                client_id TEXT NOT NULL,
                is_remote INTEGER NOT NULL,
                relative_full_path TEXT NOT NULL,
                file_type INTEGER NOT NULL,
                ext TEXT NOT NULL,
                url TEXT NOT NULL,
                info_hash TEXT NOT NULL,
                magnet_link TEXT NOT NULL,
                torrent_file TEXT NOT NULL,
                status INTEGER NOT NULL,
                total_size INTEGER NOT NULL,
                total_done INTEGER NOT NULL,
                message TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                modified_at INTEGER NOT NULL,
                is_delete INTEGER NOT NULL,
                is_read INTEGER NOT NULL DEFAULT 0,
                name TEXT NOT NULL DEFAULT '',
                storage_root_id TEXT NOT NULL DEFAULT '',
                relative_save_path TEXT NOT NULL DEFAULT '',
                rate_history TEXT NOT NULL DEFAULT '[[],[]]'
            );
        )SQL";
        if (!db_ || sqlite3_exec(db_, sql, nullptr, nullptr, nullptr) != SQLITE_OK) return false;

        // 已安装版本的任务表可能还没有历史列，原有任务保留为空历史。
        sqlite3_stmt *statement = nullptr;
        const int prepared = sqlite3_prepare_v2(db_, "SELECT rate_history FROM tasks LIMIT 0", -1, &statement, nullptr);
        sqlite3_finalize(statement);
        if (prepared == SQLITE_OK) return true;
        return sqlite3_exec(db_, "ALTER TABLE tasks ADD COLUMN rate_history TEXT NOT NULL DEFAULT '[[],[]]'",
                            nullptr, nullptr, nullptr) == SQLITE_OK;
    }

    bool TaskStore::write_task(const Task &task) const {
        constexpr auto sql = R"SQL(
            INSERT INTO tasks (
                protocol, task_id, client_id, is_remote,
                relative_full_path, file_type,
                ext, url, info_hash, magnet_link,
                torrent_file, status, total_size, total_done,
                message, created_at, modified_at, is_delete,
                is_read, name, storage_root_id, relative_save_path, rate_history
            ) VALUES (
                :protocol, :task_id, :client_id, :is_remote,
                :relative_full_path, :file_type,
                :ext, :url, :info_hash, :magnet_link,
                :torrent_file, :status, :total_size, :total_done,
                :message, :created_at, :modified_at, :is_delete,
                :is_read, :name, :storage_root_id, :relative_save_path, :rate_history
            )
            ON CONFLICT(task_id) DO UPDATE SET
                protocol = excluded.protocol,
                client_id = excluded.client_id,
                is_remote = excluded.is_remote,
                relative_full_path = excluded.relative_full_path,
                file_type = excluded.file_type,
                ext = excluded.ext,
                url = excluded.url,
                info_hash = excluded.info_hash,
                magnet_link = excluded.magnet_link,
                torrent_file = excluded.torrent_file,
                status = excluded.status,
                total_size = excluded.total_size,
                total_done = excluded.total_done,
                message = excluded.message,
                created_at = excluded.created_at,
                modified_at = excluded.modified_at,
                is_delete = excluded.is_delete,
                is_read = excluded.is_read,
                name = excluded.name,
                storage_root_id = excluded.storage_root_id,
                relative_save_path = excluded.relative_save_path,
                rate_history = excluded.rate_history
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
        sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":storage_root_id"),
                          task.storage_root_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":relative_save_path"),
                          task.relative_save_path.c_str(), -1, SQLITE_TRANSIENT);
        std::string relative_full_path;
        if (!task.full_path.empty() && !task.save_path.empty()) {
            const auto relative = std::filesystem::path(task.full_path).lexically_relative(task.save_path);
            if (!relative.empty() && !relative.has_root_path() && *relative.begin() != "..") {
                relative_full_path = relative.generic_string();
            }
        }
        sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":relative_full_path"),
                          relative_full_path.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(statement, sqlite3_bind_parameter_index(statement, ":file_type"), task.file_type);
        sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":ext"), task.ext.c_str(), -1,
                          SQLITE_TRANSIENT);
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
        sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":message"), task.message.c_str(), -1,
                          SQLITE_TRANSIENT);
        sqlite3_bind_int64(statement, sqlite3_bind_parameter_index(statement, ":created_at"), task.created_at);
        sqlite3_bind_int64(statement, sqlite3_bind_parameter_index(statement, ":modified_at"), task.modified_at);
        sqlite3_bind_int64(statement, sqlite3_bind_parameter_index(statement, ":is_delete"), task.is_delete);
        sqlite3_bind_int(statement, sqlite3_bind_parameter_index(statement, ":is_read"), task.is_read);
        sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":name"),
                          task.name.c_str(), -1, SQLITE_TRANSIENT);
        const auto history = boost::json::serialize(
            boost::json::array{task.download_rate_history, task.upload_rate_history});
        sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":rate_history"),
                          history.c_str(), -1, SQLITE_TRANSIENT);
        const bool success = sqlite3_step(statement) == SQLITE_DONE;
        sqlite3_finalize(statement);
        return success;
    }

    bool TaskStore::save(const Task &task) const {
        std::scoped_lock lock(mutex_);
        return db_ && !task.task_id.empty() && !task.storage_root_id.empty() && write_task(task);
    }

    bool TaskStore::find_by_task_id(const std::string &task_id, Task &out) const {
        std::scoped_lock lock(mutex_);
        out = {};
        if (!db_ || task_id.empty()) return false;
        sqlite3_stmt *statement = nullptr;
        const std::string sql = std::string(task_select) +
                                " WHERE task_id=:task_id";
        if (sqlite3_prepare_v2(db_, sql.c_str(),
                               -1, &statement, nullptr) != SQLITE_OK)
            return false;
        sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":task_id"), task_id.c_str(), -1,
                          SQLITE_TRANSIENT);
        const bool found = sqlite3_step(statement) == SQLITE_ROW;
        if (found) out = read_task(statement);
        sqlite3_finalize(statement);
        return found;
    }

    std::vector<Task> TaskStore::find_all() const {
        std::scoped_lock lock(mutex_);
        std::vector<Task> tasks;
        sqlite3_stmt *statement = nullptr;
        const std::string sql = std::string(task_select) + " WHERE is_delete=0 ORDER BY modified_at DESC";
        if (!db_ || sqlite3_prepare_v2(db_, sql.c_str(), -1, &statement, nullptr) != SQLITE_OK)
            return tasks;
        while (sqlite3_step(statement) == SQLITE_ROW) tasks.push_back(read_task(statement));
        sqlite3_finalize(statement);
        return tasks;
    }
} // namespace dw
