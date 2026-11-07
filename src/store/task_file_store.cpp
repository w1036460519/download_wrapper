#include "task_file_store.h"

#include "core/sqlite_database.h"
#include "internal/json_util.h"

namespace dw {
    namespace {
        std::string file_payload(const TaskFile &file) {
            auto payload = to_json(file);
            payload.erase("is_delete");
            payload.erase("is_read");
            return boost::json::serialize(payload);
        }

        bool read_task_file(sqlite3_stmt *statement, const std::string &task_id, TaskFile &file) {
            const auto *data = reinterpret_cast<const char *>(sqlite3_column_text(statement, 1));
            boost::system::error_code ec;
            const auto value = boost::json::parse(data ? data : "", ec);
            if (ec || !value.is_object()) return false;
            const auto &obj = value.as_object();
            file = {};
            file.task_id = task_id;
            file.file_index = sqlite3_column_int(statement, 0);
            file.is_read = sqlite3_column_int(statement, 2) != 0;
            json_util::extract(obj, "name", file.name);
            json_util::extract(obj, "path", file.path);
            json_util::extract(obj, "full_path", file.full_path);
            json_util::extract(obj, "total_size", file.total_size);
            json_util::extract(obj, "total_done", file.total_done);
            json_util::extract(obj, "extension", file.extension);
            json_util::extract(obj, "prioritize", file.prioritize);
            if (const auto *progress = obj.if_contains("progress"); progress && progress->is_number()) {
                file.progress = progress->to_number<double>();
            }
            if (const auto *segments = obj.if_contains("segments"); segments && segments->is_array()) {
                for (const auto &segment : segments->as_array()) {
                    if (!segment.is_array()) continue;
                    const auto &ends = segment.as_array();
                    if (ends.size() == 2 && ends[0].is_int64() && ends[1].is_int64()) {
                        file.segments.emplace_back(ends[0].as_int64(), ends[1].as_int64());
                    }
                }
            }
            return true;
        }
    }

    bool TaskFileStore::init_schema() const {
        std::scoped_lock lock(mutex_);
        return db_ && sqlite3_exec(db_,
            "CREATE TABLE IF NOT EXISTS task_file ("
            "task_id TEXT NOT NULL, file_index INTEGER NOT NULL, data TEXT NOT NULL, "
            "is_delete INTEGER NOT NULL DEFAULT 0, is_read INTEGER NOT NULL DEFAULT 0, "
            "PRIMARY KEY(task_id, file_index))",
            nullptr, nullptr, nullptr) == SQLITE_OK
            && ensure_sqlite_column(db_, "task_file", "is_delete",
                "ALTER TABLE task_file ADD COLUMN is_delete INTEGER NOT NULL DEFAULT 0")
            && ensure_sqlite_column(db_, "task_file", "is_read",
                "ALTER TABLE task_file ADD COLUMN is_read INTEGER NOT NULL DEFAULT 0");
    }

    bool TaskFileStore::init_task_triggers() const {
        std::scoped_lock lock(mutex_);
        return db_ && sqlite3_exec(db_, R"SQL(
            DROP TRIGGER IF EXISTS sync_bt_task_file_deletion;
            CREATE TRIGGER IF NOT EXISTS sync_task_file_deletion
            AFTER UPDATE OF is_delete ON tasks
            WHEN NEW.protocol IN (0, 1) AND NEW.is_delete = 1 AND OLD.is_delete = 0
            BEGIN
                UPDATE task_file
                SET is_delete = 1
                WHERE task_id = NEW.task_id;
            END;
            UPDATE task_file SET is_delete = 1
            WHERE is_delete = 0 AND EXISTS (
                SELECT 1 FROM tasks
                WHERE protocol IN (0, 1) AND tasks.is_delete = 1
                    AND task_file.task_id = tasks.task_id
            );
        )SQL", nullptr, nullptr, nullptr) == SQLITE_OK;
    }

    bool TaskFileStore::save(const std::vector<TaskFile> &files) const {
        std::scoped_lock lock(mutex_);
        if (!db_) return false;
        sqlite3_stmt *statement = nullptr;
        if (sqlite3_prepare_v2(db_,
                "INSERT INTO task_file(task_id,file_index,data,is_delete,is_read) VALUES(?,?,?,?,?) "
                "ON CONFLICT(task_id,file_index) DO UPDATE SET data=excluded.data,"
                "is_delete=excluded.is_delete,is_read=MAX(task_file.is_read,excluded.is_read) "
                "WHERE task_file.is_delete=0",
                -1, &statement, nullptr) != SQLITE_OK) return false;
        bool success = true;
        for (const auto &file : files) {
            if (file.task_id.empty() || file.file_index < 0) {
                success = false;
                break;
            }
            const auto data = file_payload(file);
            sqlite3_bind_text(statement, 1, file.task_id.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(statement, 2, file.file_index);
            sqlite3_bind_text(statement, 3, data.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(statement, 4, file.is_delete);
            sqlite3_bind_int(statement, 5, file.is_read);
            if (sqlite3_step(statement) != SQLITE_DONE) {
                success = false;
                break;
            }
            sqlite3_reset(statement);
            sqlite3_clear_bindings(statement);
        }
        sqlite3_finalize(statement);
        return success;
    }

    bool TaskFileStore::valid(const TaskFile &file) {
        if (file.task_id.empty() || file.file_index < 0 || file.total_size < 0
            || file.total_done < 0 || file.total_done > file.total_size)
            return false;
        if (file.prioritize != 0 && file.prioritize != 1
            && file.prioritize != 4 && file.prioritize != 7)
            return false;
        int64_t previous_end = -1;
        for (const auto &[start, end] : file.segments) {
            if (start < 0 || start <= previous_end
                || end < start || end >= file.total_size)
                return false;
            previous_end = end;
        }
        return true;
    }

    bool TaskFileStore::upsert(TaskFile file) {
        return valid(file) && save({std::move(file)});
    }

    std::vector<TaskFile> TaskFileStore::get(const std::string &task_id) const {
        std::scoped_lock lock(mutex_);
        std::vector<TaskFile> result;
        sqlite3_stmt *statement = nullptr;
        if (!db_ || sqlite3_prepare_v2(db_,
                "SELECT file_index,data,is_read FROM task_file WHERE task_id=? AND is_delete=0 ORDER BY file_index",
                -1, &statement, nullptr) != SQLITE_OK) return result;
        sqlite3_bind_text(statement, 1, task_id.c_str(), -1, SQLITE_TRANSIENT);
        while (sqlite3_step(statement) == SQLITE_ROW) {
            TaskFile file;
            if (read_task_file(statement, task_id, file)) result.push_back(std::move(file));
        }
        sqlite3_finalize(statement);
        return result;
    }

    bool TaskFileStore::find(const std::string &task_id, int32_t file_index, TaskFile &file) const {
        std::scoped_lock lock(mutex_);
        sqlite3_stmt *statement = nullptr;
        if (!db_ || sqlite3_prepare_v2(db_,
                "SELECT file_index,data,is_read FROM task_file WHERE task_id=? AND file_index=? AND is_delete=0",
                -1, &statement, nullptr) != SQLITE_OK) return false;
        sqlite3_bind_text(statement, 1, task_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(statement, 2, file_index);
        const bool found = sqlite3_step(statement) == SQLITE_ROW && read_task_file(statement, task_id, file);
        sqlite3_finalize(statement);
        return found;
    }

    bool TaskFileStore::update(const std::string &task_id,
                               int32_t file_index, TaskFile file) {
        file.task_id = task_id;
        file.file_index = file_index;
        if (!valid(file)) return false;
        const auto data = file_payload(file);
        std::scoped_lock lock(mutex_);
        sqlite3_stmt *statement = nullptr;
        if (!db_ || sqlite3_prepare_v2(db_,
                "UPDATE task_file SET data=?,is_delete=?,is_read=? WHERE task_id=? AND file_index=? AND is_delete=0",
                -1, &statement, nullptr) != SQLITE_OK) return false;
        sqlite3_bind_text(statement, 1, data.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(statement, 2, file.is_delete);
        sqlite3_bind_int(statement, 3, file.is_read);
        sqlite3_bind_text(statement, 4, task_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(statement, 5, file_index);
        const bool success = sqlite3_step(statement) == SQLITE_DONE && sqlite3_changes(db_) != 0;
        sqlite3_finalize(statement);
        return success;
    }

    std::size_t TaskFileStore::erase(const std::string &task_id) {
        std::scoped_lock lock(mutex_);
        sqlite3_stmt *statement = nullptr;
        if (!db_ || sqlite3_prepare_v2(db_, "UPDATE task_file SET is_delete=1 WHERE task_id=? AND is_delete=0",
                                      -1, &statement, nullptr) != SQLITE_OK) return 0;
        sqlite3_bind_text(statement, 1, task_id.c_str(), -1, SQLITE_TRANSIENT);
        const auto count = sqlite3_step(statement) == SQLITE_DONE ? sqlite3_changes(db_) : 0;
        sqlite3_finalize(statement);
        return static_cast<std::size_t>(count);
    }
} // namespace dw
