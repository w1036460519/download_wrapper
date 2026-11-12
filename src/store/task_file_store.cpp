#include "task_file_store.h"

#include <memory>

namespace dw {
    namespace {
        const char *column_text(sqlite3_stmt *statement, int column) {
            const auto *value = sqlite3_column_text(statement, column);
            return value ? reinterpret_cast<const char *>(value) : "";
        }

        constexpr char task_select[] = R"SQL(
            SELECT file_index, name, path, full_path, total_size, total_done,
                   progress, segments, extension, prioritize, is_delete, is_read, task_id
            FROM task_file
        )SQL";

        enum TaskColumn {
            col_file_index,
            col_name,
            col_path,
            col_full_path,
            col_total_size,
            col_total_done,
            col_progress,
            col_segments,
            col_extension,
            col_prioritize,
            col_is_delete,
            col_is_read,
            col_task_id,
        };

        TaskFile read_task_file(sqlite3_stmt *statement) {
            TaskFile file;
            file.task_id = column_text(statement, col_task_id);
            file.file_index = sqlite3_column_int(statement, col_file_index);
            file.name = column_text(statement, col_name);
            file.path = column_text(statement, col_path);
            file.full_path = column_text(statement, col_full_path);
            file.total_size = sqlite3_column_int64(statement, col_total_size);
            file.total_done = sqlite3_column_int64(statement, col_total_done);
            file.progress = sqlite3_column_double(statement, col_progress);
            file.extension = column_text(statement, col_extension);
            file.prioritize = sqlite3_column_int(statement, col_prioritize);
            file.is_delete = sqlite3_column_int(statement, col_is_delete) != 0;
            file.is_read = sqlite3_column_int(statement, col_is_read) != 0;

            boost::system::error_code ec;
            const auto segments = boost::json::parse(column_text(statement, col_segments), ec);
            if (!ec && segments.is_array()) {
                for (const auto &segment : segments.as_array()) {
                    if (const auto *entry = segment.if_object()) {
                        const auto *start = entry->if_contains("start");
                        const auto *end = entry->if_contains("end");
                        const auto *status = entry->if_contains("status");
                        if (!start || !end || !status || !start->is_int64() || !end->is_int64() ||
                            !status->is_int64()) continue;
                        const auto code = status->as_int64();
                        if (code < static_cast<int32_t>(SegmentStatus::NotDownloaded) ||
                            code > static_cast<int32_t>(SegmentStatus::Failed)) continue;
                        file.segments.push_back({start->as_int64(), end->as_int64(), static_cast<SegmentStatus>(code)});
                    } else if (const auto *ends = segment.if_array();
                               ends && ends->size() == 2 && (*ends)[0].is_int64() && (*ends)[1].is_int64()) {
                        // 旧数据中的区间均为已下载。
                        file.segments.push_back({(*ends)[0].as_int64(), (*ends)[1].as_int64(), SegmentStatus::Downloaded});
                    }
                }
            }
            return file;
        }

        bool bind_text(sqlite3_stmt *statement, const char *name, const std::string &value) {
            return sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, name),
                                     value.c_str(), -1, SQLITE_TRANSIENT) == SQLITE_OK;
        }

        bool bind_task_file(sqlite3_stmt *statement, const TaskFile &file) {
            const auto param = [statement](const char *name) {
                return sqlite3_bind_parameter_index(statement, name);
            };
            const auto segments = boost::json::serialize(boost::json::value_from(file.segments));
            return bind_text(statement, ":task_id", file.task_id) &&
                   sqlite3_bind_int(statement, param(":file_index"), file.file_index) == SQLITE_OK &&
                   bind_text(statement, ":name", file.name) &&
                   bind_text(statement, ":path", file.path) &&
                   bind_text(statement, ":full_path", file.full_path) &&
                   sqlite3_bind_int64(statement, param(":total_size"), file.total_size) == SQLITE_OK &&
                   sqlite3_bind_int64(statement, param(":total_done"), file.total_done) == SQLITE_OK &&
                   sqlite3_bind_double(statement, param(":progress"), file.progress) == SQLITE_OK &&
                   bind_text(statement, ":segments", segments) &&
                   bind_text(statement, ":extension", file.extension) &&
                   sqlite3_bind_int(statement, param(":prioritize"), file.prioritize) == SQLITE_OK &&
                   sqlite3_bind_int(statement, param(":is_delete"), file.is_delete) == SQLITE_OK &&
                   sqlite3_bind_int(statement, param(":is_read"), file.is_read) == SQLITE_OK;
        }
    }

    TaskFileStore::TaskFileStore(sqlite3 *const &db) : db_(db) {}

    bool TaskFileStore::init_schema() const {
        std::scoped_lock lock(mutex_);
        return db_ && sqlite3_exec(db_, R"SQL(
            CREATE TABLE IF NOT EXISTS task_file (
                task_id TEXT NOT NULL,
                file_index INTEGER NOT NULL,
                name TEXT NOT NULL DEFAULT '',
                path TEXT NOT NULL DEFAULT '',
                full_path TEXT NOT NULL DEFAULT '',
                total_size INTEGER NOT NULL DEFAULT 0,
                total_done INTEGER NOT NULL DEFAULT 0,
                progress REAL NOT NULL DEFAULT 0,
                segments TEXT NOT NULL DEFAULT '[]',
                extension TEXT NOT NULL DEFAULT '',
                prioritize INTEGER NOT NULL DEFAULT 4,
                is_delete INTEGER NOT NULL DEFAULT 0,
                is_read INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(task_id, file_index)
            );
        )SQL", nullptr, nullptr, nullptr) == SQLITE_OK
            && sqlite3_exec(db_, "CREATE INDEX IF NOT EXISTS idx_task_file_active_path "
                                "ON task_file(full_path) WHERE is_delete=0",
                            nullptr, nullptr, nullptr) == SQLITE_OK;
    }

    bool TaskFileStore::save(const TaskFile &file) const {
        return save_files(std::span<const TaskFile>{&file, 1});
    }

    bool TaskFileStore::save_files(const std::span<const TaskFile> files) const {
        std::scoped_lock lock(mutex_);
        if (!db_) return false;
        sqlite3_stmt *statement = nullptr;
        if (sqlite3_prepare_v2(db_,
                R"SQL(
                INSERT INTO task_file(task_id,file_index,name,path,full_path,total_size,total_done,
                                      progress,segments,extension,prioritize,is_delete,is_read)
                SELECT :task_id,:file_index,:name,:path,:full_path,:total_size,:total_done,
                       :progress,:segments,:extension,:prioritize,:is_delete,:is_read
                WHERE NOT EXISTS (SELECT 1 FROM tasks WHERE task_id=:task_id AND is_delete=1)
                ON CONFLICT(task_id,file_index) DO UPDATE SET
                    name=excluded.name, path=excluded.path, full_path=excluded.full_path,
                    total_size=excluded.total_size, total_done=excluded.total_done,
                    progress=excluded.progress, segments=excluded.segments,
                    extension=excluded.extension, prioritize=excluded.prioritize,
                    is_delete=excluded.is_delete, is_read=MAX(task_file.is_read,excluded.is_read)
                )SQL",
                -1, &statement, nullptr) != SQLITE_OK) return false;
        const std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)> statement_guard(statement, &sqlite3_finalize);
        bool success = true;
        for (const auto &file : files) {
            if (file.task_id.empty() || file.file_index < 0) {
                success = false;
                continue;
            }
            if (!bind_task_file(statement, file) || sqlite3_step(statement) != SQLITE_DONE) {
                success = false;
            }
            sqlite3_reset(statement);
            sqlite3_clear_bindings(statement);
        }
        return success;
    }

    std::vector<TaskFile> TaskFileStore::get(const std::string &task_id) const {
        std::scoped_lock lock(mutex_);
        std::vector<TaskFile> result;
        sqlite3_stmt *statement = nullptr;
        const std::string sql = std::string(task_select) +
                                " WHERE task_id = :task_id AND is_delete = 0 ORDER BY file_index";
        if (!db_ || sqlite3_prepare_v2(db_, sql.c_str(), -1, &statement, nullptr) != SQLITE_OK) return result;
        if (bind_text(statement, ":task_id", task_id)) {
            while (sqlite3_step(statement) == SQLITE_ROW) {
                result.push_back(read_task_file(statement));
            }
        }
        sqlite3_finalize(statement);
        return result;
    }

    bool TaskFileStore::find_by_path(const std::string &full_path, TaskFile &file) const {
        std::scoped_lock lock(mutex_);
        file = {};
        if (!db_ || full_path.empty()) return false;

        const std::string sql = std::string(task_select) + R"SQL(
            WHERE full_path = :full_path AND is_delete = 0
              AND EXISTS (
                  SELECT 1 FROM tasks
                  WHERE tasks.task_id = task_file.task_id AND tasks.is_delete = 0
              )
            ORDER BY total_done DESC, task_id, file_index
            LIMIT 1
        )SQL";
        sqlite3_stmt *statement = nullptr;
        if (sqlite3_prepare_v2(db_, sql.c_str(), -1, &statement, nullptr) != SQLITE_OK) return false;
        const std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)> statement_guard(statement, &sqlite3_finalize);
        if (!bind_text(statement, ":full_path", full_path) || sqlite3_step(statement) != SQLITE_ROW) return false;
        file = read_task_file(statement);
        return true;
    }

    bool TaskFileStore::delete_task(const std::string &task_id) const {
        std::scoped_lock lock(mutex_);
        sqlite3_stmt *statement = nullptr;
        if (!db_ || task_id.empty() ||
            sqlite3_prepare_v2(db_, "UPDATE task_file SET is_delete=1 WHERE task_id=:task_id AND is_delete=0",
                               -1, &statement, nullptr) != SQLITE_OK) return false;
        const bool success = bind_text(statement, ":task_id", task_id) &&
                             sqlite3_step(statement) == SQLITE_DONE;
        sqlite3_finalize(statement);
        return success;
    }
} // namespace dw
