#include "playback_progress_store.h"

#include <algorithm>
#include <filesystem>
#include <memory>

namespace dw {
    namespace {
        bool bind_file(sqlite3_stmt *statement, const std::string &full_path) {
            namespace fs = std::filesystem;
            if (full_path.empty()) return false;
            std::error_code error;
            const auto path = fs::absolute(fs::path(full_path), error).lexically_normal();
            if (error || path.filename().empty()) return false;
            // 使用实际文件名和父目录，任务名与目录列表中的显示名可以不同。
            const auto directory = path.parent_path().generic_string();
            const auto name = path.filename().generic_string();
            return sqlite3_bind_text(statement, 1, directory.c_str(), -1, SQLITE_TRANSIENT) == SQLITE_OK &&
                   sqlite3_bind_text(statement, 2, name.c_str(), -1, SQLITE_TRANSIENT) == SQLITE_OK;
        }
    }

    bool PlaybackProgressStore::init_schema() const {
        constexpr auto sql = R"SQL(
            CREATE TABLE IF NOT EXISTS playback_progress (
                directory_path TEXT NOT NULL,
                file_name TEXT NOT NULL,
                position_ms INTEGER NOT NULL,
                duration_ms INTEGER NOT NULL,
                PRIMARY KEY (directory_path, file_name)
            ) WITHOUT ROWID;
        )SQL";
        return db_ && sqlite3_exec(db_, sql, nullptr, nullptr, nullptr) == SQLITE_OK;
    }

    bool PlaybackProgressStore::save(const std::string &full_path, int64_t position_ms, int64_t duration_ms) const {
        if (!db_ || position_ms < 0 || duration_ms <= 0) return false;
        constexpr auto sql = R"SQL(
            INSERT INTO playback_progress (directory_path, file_name, position_ms, duration_ms)
            VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(directory_path, file_name) DO UPDATE SET
                position_ms = excluded.position_ms,
                duration_ms = excluded.duration_ms
        )SQL";
        sqlite3_stmt *raw = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &raw, nullptr) != SQLITE_OK) return false;
        const std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)> statement(raw, sqlite3_finalize);
        return bind_file(raw, full_path) &&
               sqlite3_bind_int64(raw, 3, std::min(position_ms, duration_ms)) == SQLITE_OK &&
               sqlite3_bind_int64(raw, 4, duration_ms) == SQLITE_OK &&
               sqlite3_step(raw) == SQLITE_DONE;
    }

    void PlaybackProgressStore::attach(std::vector<TaskFile> &files) const {
        if (!db_ || files.empty()) return;
        constexpr auto sql = R"SQL(
            SELECT position_ms, duration_ms
            FROM playback_progress
            WHERE directory_path = ?1 AND file_name = ?2
        )SQL";
        sqlite3_stmt *raw = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &raw, nullptr) != SQLITE_OK) return;
        const std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)> statement(raw, sqlite3_finalize);
        for (auto &file: files) {
            if (file.is_dir || file.full_path.empty()) continue;
            if (bind_file(raw, file.full_path) && sqlite3_step(raw) == SQLITE_ROW) {
                file.playback_position_ms = sqlite3_column_int64(raw, 0);
                file.playback_duration_ms = sqlite3_column_int64(raw, 1);
            }
            sqlite3_reset(raw);
            sqlite3_clear_bindings(raw);
        }
    }
}
