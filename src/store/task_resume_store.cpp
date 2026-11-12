#include "task_resume_store.h"

namespace dw {
    namespace {
        constexpr char task_select[] = R"SQL(
            SELECT resume_data
            FROM task_resume
        )SQL";

        enum TaskColumn { col_resume_data };
    }


    TaskResumeStore::TaskResumeStore(sqlite3 *const &db) : db_(db) {}

    bool TaskResumeStore::init_schema() const {
        std::scoped_lock lock(mutex_);
        return db_ && sqlite3_exec(db_, "CREATE TABLE IF NOT EXISTS task_resume ("
                                   "task_id TEXT PRIMARY KEY NOT NULL, resume_data BLOB NOT NULL)",
                                   nullptr, nullptr, nullptr) == SQLITE_OK;
    }

    bool TaskResumeStore::save(const TaskResume &record) const {
        std::scoped_lock lock(mutex_);
        if (!db_ || record.task_id.empty() || record.resume_data.empty()) return false;
        sqlite3_stmt *statement = nullptr;
        constexpr auto sql = R"SQL(
            INSERT INTO task_resume(task_id, resume_data)
            SELECT :task_id, :resume_data
            WHERE NOT EXISTS (SELECT 1 FROM tasks WHERE task_id = :task_id AND is_delete = 1)
            ON CONFLICT(task_id) DO UPDATE SET resume_data = excluded.resume_data
        )SQL";
        if (sqlite3_prepare_v2(db_, sql, -1, &statement, nullptr) != SQLITE_OK)
            return false;
        sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":task_id"),
                          record.task_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_blob64(statement, sqlite3_bind_parameter_index(statement, ":resume_data"),
                            record.resume_data.data(), record.resume_data.size(), SQLITE_TRANSIENT);
        const bool success = sqlite3_step(statement) == SQLITE_DONE;
        sqlite3_finalize(statement);
        return success;
    }

    bool TaskResumeStore::find(const std::string &task_id, TaskResume &out) const {
        std::scoped_lock lock(mutex_);
        out = {};
        if (!db_ || task_id.empty()) return false;
        sqlite3_stmt *statement = nullptr;
        const std::string sql = std::string(task_select) + " WHERE task_id = :task_id";
        if (sqlite3_prepare_v2(db_, sql.c_str(),
                               -1, &statement, nullptr) != SQLITE_OK)
            return false;
        sqlite3_bind_text(statement,
                          sqlite3_bind_parameter_index(statement, ":task_id"),
                          task_id.c_str(),
                          -1,
                          SQLITE_TRANSIENT);
        bool found = false;
        if (sqlite3_step(statement) == SQLITE_ROW) {
            const auto *data = static_cast<const uint8_t *>(sqlite3_column_blob(statement, col_resume_data));
            if (const auto size = sqlite3_column_bytes(statement, col_resume_data); data && size > 0) {
                out.task_id = task_id;
                out.resume_data.assign(data, data + size);
                found = true;
            }
        }
        sqlite3_finalize(statement);
        return found;
    }
} // namespace dw
