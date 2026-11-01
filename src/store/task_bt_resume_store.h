#pragma once

#include "core/task_bt_resume.h"
#include <sqlite3.h>
#include <mutex>

namespace dw {
    class TaskBtResumeStore {
    public:
        explicit TaskBtResumeStore(sqlite3 *const &db) : db_(db) {
        }

        TaskBtResumeStore(const TaskBtResumeStore &) = delete;

        TaskBtResumeStore &operator=(const TaskBtResumeStore &) = delete;

        bool init_schema() const {
            std::scoped_lock lock(mutex_);
            return db_ && sqlite3_exec(db_, "CREATE TABLE IF NOT EXISTS task_bt_resume ("
                                       "info_hash TEXT PRIMARY KEY NOT NULL, resume_data BLOB NOT NULL)",
                                       nullptr, nullptr, nullptr) == SQLITE_OK;
        }

        bool save(const TaskBtResume &record) const {
            std::scoped_lock lock(mutex_);
            if (!db_ || record.info_hash.empty() || record.resume_data.empty()) return false;
            sqlite3_stmt *statement = nullptr;
            if (sqlite3_prepare_v2(db_,
                                   "INSERT INTO task_bt_resume(info_hash,resume_data) VALUES(:info_hash,:resume_data) "
                                   "ON CONFLICT(info_hash) DO UPDATE SET resume_data=excluded.resume_data",
                                   -1, &statement, nullptr) != SQLITE_OK)
                return false;
            sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":info_hash"),
                              record.info_hash.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_blob64(statement, sqlite3_bind_parameter_index(statement, ":resume_data"),
                                record.resume_data.data(), record.resume_data.size(), SQLITE_TRANSIENT);
            const bool success = sqlite3_step(statement) == SQLITE_DONE;
            sqlite3_finalize(statement);
            return success;
        }

        /// 按 info_hash 删除；记录不存在也视为成功。
        bool remove(const std::string &info_hash) const {
            std::scoped_lock lock(mutex_);
            if (!db_ || info_hash.empty()) return false;
            sqlite3_stmt *statement = nullptr;
            if (sqlite3_prepare_v2(db_, "DELETE FROM task_bt_resume WHERE info_hash=:info_hash",
                -1, &statement, nullptr) != SQLITE_OK) return false;
            sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":info_hash"),
                info_hash.c_str(), -1, SQLITE_TRANSIENT);
            const bool success = sqlite3_step(statement) == SQLITE_DONE;
            sqlite3_finalize(statement);
            return success;
        }

        bool find(const std::string &info_hash, TaskBtResume &out) const {
            std::scoped_lock lock(mutex_);
            out = {};
            if (!db_ || info_hash.empty()) return false;
            sqlite3_stmt *statement = nullptr;
            if (sqlite3_prepare_v2(db_, "SELECT resume_data FROM task_bt_resume WHERE info_hash=:info_hash",
                                   -1, &statement, nullptr) != SQLITE_OK)
                return false;
            sqlite3_bind_text(statement,
                              sqlite3_bind_parameter_index(statement, ":info_hash"),
                              info_hash.c_str(),
                              -1,
                              SQLITE_TRANSIENT);
            bool found = false;
            if (sqlite3_step(statement) == SQLITE_ROW) {
                const auto *data = static_cast<const uint8_t *>(sqlite3_column_blob(statement, 0));
                if (const auto size = sqlite3_column_bytes(statement, 0); data && size > 0) {
                    out.info_hash = info_hash;
                    out.resume_data.assign(data, data + size);
                    found = true;
                }
            }
            sqlite3_finalize(statement);
            return found;
        }

    private:
        sqlite3 *const &db_;
        mutable std::mutex mutex_;
    };
}
