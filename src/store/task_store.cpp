#include "task_store.h"

#include "utils/time_util.h"

#include <sqlite3.h>

#include <string_view>
#include <unordered_map>

namespace dw {
    using utils::now_unix_ms;

    namespace {
        /// SQLite text 列安全读取（NULL 返回空串）。
        std::string col_text(sqlite3_stmt *st, int idx) {
            const unsigned char *t = sqlite3_column_text(st, idx);
            return t ? reinterpret_cast<const char *>(t) : std::string();
        }

        /// 列名→索引映射
        struct col_map {
            sqlite3_stmt *st;
            std::unordered_map<std::string_view, int> m;

            explicit col_map(sqlite3_stmt *s) : st(s) {
                const int n = sqlite3_column_count(s);
                for (int i = 0; i < n; ++i)
                    if (const char *name = sqlite3_column_name(s, i))
                        m[name] = i;
            }

            int idx(const std::string_view name) const { return m.at(name); }

            std::string getText(std::string_view name) const { return col_text(st, idx(name)); }
            int getInt(std::string_view name) const { return sqlite3_column_int(st, idx(name)); }
            int64_t getInt64(std::string_view name) const { return sqlite3_column_int64(st, idx(name)); }
        };

        /// 命名参数绑定辅助：SQL 使用 :name 占位符，按名绑定不依赖位置索引。
        int param_idx(sqlite3_stmt *st, const char *name) {
            return sqlite3_bind_parameter_index(st, name);
        }

        void bind_text(sqlite3_stmt *st, const char *name, const std::string &val) {
            sqlite3_bind_text(st, param_idx(st, name), val.c_str(), -1, SQLITE_TRANSIENT);
        }

        void bind_int(sqlite3_stmt *st, const char *name, int val) {
            sqlite3_bind_int(st, param_idx(st, name), val);
        }

        void bind_int64(sqlite3_stmt *st, const char *name, int64_t val) {
            sqlite3_bind_int64(st, param_idx(st, name), val);
        }

        void bind_blob(sqlite3_stmt *st, const char *name, const void *data, int size) {
            sqlite3_bind_blob(st, param_idx(st, name), data, size, SQLITE_TRANSIENT);
        }
    } // namespace

    Task TaskStore::read_task(sqlite3_stmt *statement) {
        const col_map columns(statement);
        Task task;
        task.task_id = columns.getText("task_id");
        task.client_id = columns.getText("client_id");
        task.is_remote = columns.getInt("is_remote") != 0;
        task.name = columns.getText("name");
        task.wrapper_name = columns.getText("wrapper_name");
        task.save_path = columns.getText("save_path");
        task.original_root_name = columns.getText("original_root_name");
        task.root_name = columns.getText("root_name");
        task.full_path = columns.getText("full_path");
        task.file_type = columns.getInt("file_type") != 0;
        task.ext = columns.getText("ext");
        task.parsed = columns.getInt("parsed") != 0;
        task.url = columns.getText("url");
        task.info_hash = columns.getText("info_hash");
        task.magnet_link = columns.getText("magnet_link");
        task.torrent_file = columns.getText("torrent_file");
        task.task_protocol = static_cast<dw_protocol_t>(columns.getInt("protocol"));
        task.status = columns.getInt("status");
        task.total_size = columns.getInt64("total_size");
        task.total_done = columns.getInt64("total_done");
        task.priority = columns.getInt("priority");
        task.reason = columns.getInt("reason");
        task.message = columns.getText("message");
        task.created_at = columns.getInt64("created_at");
        task.modified_at = columns.getInt64("modified_at");
        task.is_delete = columns.getInt("is_delete") != 0;
        task.is_read = columns.getInt("is_read") != 0;
        return task;
    }

    bool LegacyTaskStore::init_schema() const {
        constexpr auto sql = R"(
            CREATE TABLE IF NOT EXISTS resume_data (
                client_id   TEXT NOT NULL,
                protocol    INTEGER NOT NULL,
                natural_key TEXT NOT NULL,
                data BLOB,
                save_path TEXT,
                magnet_link TEXT,
                torrent_file TEXT,
                saved_at INTEGER,
                PRIMARY KEY (client_id, protocol, natural_key)
            );

            CREATE TABLE IF NOT EXISTS play_progress (
                full_path    TEXT NOT NULL PRIMARY KEY,
                position_ms  INTEGER NOT NULL DEFAULT 0,
                duration_ms  INTEGER NOT NULL DEFAULT 0,
                updated_at   INTEGER
            );
        )";
        if (!db_ || sqlite3_exec(db_, sql, nullptr, nullptr, nullptr) != SQLITE_OK) return false;
        // 存量库迁移：远古版本 resume_data 以 infoHash 为主键（缺 client_id 等
        // 全部新列），针对它的 INSERT/SELECT 均因 "no such column" 静默失败，
        // 导致任务来源丢失、find_handle 无法重建 handle、任务永远进不了
        // session。旧表数据格式已不可用，检测到缺列即整表重建；全新库表不
        // 存在时同样命中本分支，DROP 无害、CREATE 走幂等建表。
        {
            sqlite3_stmt *st = nullptr;
            if (sqlite3_prepare_v2(db_,
                                   "SELECT COUNT(*) FROM pragma_table_info('resume_data') WHERE name='client_id';",
                                   -1, &st, nullptr) == SQLITE_OK) {
                bool legacy = false;
                if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_int(st, 0) == 0) {
                    legacy = true;
                }
                sqlite3_finalize(st);
                if (legacy) {
                    sqlite3_exec(db_, "DROP TABLE IF EXISTS resume_data;", nullptr, nullptr, nullptr);
                    sqlite3_exec(db_,
                                 R"(CREATE TABLE IF NOT EXISTS resume_data (
                        client_id   TEXT NOT NULL,
                        protocol    INTEGER NOT NULL,
                        natural_key TEXT NOT NULL,
                        data BLOB,
                        save_path TEXT,
                        magnet_link TEXT,
                        torrent_file TEXT,
                        saved_at INTEGER,
                        PRIMARY KEY (client_id, protocol, natural_key)
                    );)",
                                 nullptr, nullptr, nullptr);
                }
            }
        }
        return true;
    }

    void LegacyTaskStore::save_resume(const std::string &client_id, dw_protocol_t protocol,
                                const std::string &natural_key,
                                const uint8_t *data, const size_t size) const {
        // 只更新续传数据和保存时间，保留旧记录中的来源字段。
        constexpr auto sql = R"(
            INSERT INTO resume_data (client_id, protocol, natural_key, data, saved_at)
            SELECT :client_id, :protocol, :natural_key, :data, :saved_at
            WHERE NOT EXISTS (SELECT 1 FROM tasks WHERE task_id=:natural_key AND is_delete=1)
            ON CONFLICT(client_id, protocol, natural_key) DO UPDATE SET
                data = excluded.data,
                saved_at = excluded.saved_at;
        )";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return;
        bind_text(st, ":client_id", client_id);
        bind_int(st, ":protocol", static_cast<int>(protocol));
        bind_text(st, ":natural_key", natural_key);
        bind_blob(st, ":data", data, static_cast<int>(size));
        bind_int64(st, ":saved_at", now_unix_ms());
        sqlite3_step(st);
        sqlite3_finalize(st);
    }

    LegacyTaskStore::ResumeInfo LegacyTaskStore::load_resume_info(const std::string &client_id, const dw_protocol_t protocol,
                                                      const std::string &natural_key) const {
        ResumeInfo info;
        constexpr auto sql =
                "SELECT data, magnet_link, torrent_file FROM resume_data WHERE client_id=:client_id AND protocol=:protocol AND natural_key=:natural_key;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return info;
        bind_text(st, ":client_id", client_id);
        bind_int(st, ":protocol", static_cast<int>(protocol));
        bind_text(st, ":natural_key", natural_key);
        if (sqlite3_step(st) == SQLITE_ROW) {
            // data (BLOB)
            if (const void *blob = sqlite3_column_blob(st, 0);
                blob && sqlite3_column_bytes(st, 0) > 0) {
                const auto *p = static_cast<const uint8_t *>(blob);
                info.data.assign(p, p + sqlite3_column_bytes(st, 0));
            }
            // magnet_link (TEXT)
            if (const char *m = reinterpret_cast<const char *>(sqlite3_column_text(st, 1)); m) {
                info.magnet_link = m;
            }
            // torrent_file (TEXT)
            if (const char *t = reinterpret_cast<const char *>(sqlite3_column_text(st, 2)); t) {
                info.torrent_file = t;
            }
        }
        sqlite3_finalize(st);
        return info;
    }

    // ---- 播放进度（独立表 play_progress，以完整路径为键）----

    void LegacyTaskStore::set_play_position(const std::string &full_path, const int64_t position_ms) const {
        if (full_path.empty()) return;
        constexpr auto sql = R"(
            INSERT INTO play_progress (full_path, position_ms, updated_at)
            VALUES (:full_path, :position_ms, :updated_at)
            ON CONFLICT(full_path) DO UPDATE SET position_ms=excluded.position_ms, updated_at=excluded.updated_at;
        )";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return;
        bind_text(st, ":full_path", full_path);
        bind_int64(st, ":position_ms", position_ms);
        bind_int64(st, ":updated_at", now_unix_ms());
        sqlite3_step(st);
        sqlite3_finalize(st);
    }

    int64_t LegacyTaskStore::get_play_position(const std::string &full_path) const {
        if (full_path.empty()) return 0;
        int64_t position_ms = 0;
        constexpr auto sql = "SELECT position_ms FROM play_progress WHERE full_path=:full_path;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return 0;
        bind_text(st, ":full_path", full_path);
        if (sqlite3_step(st) == SQLITE_ROW &&
            sqlite3_column_type(st, 0) != SQLITE_NULL) {
            position_ms = sqlite3_column_int64(st, 0);
        }
        sqlite3_finalize(st);
        return position_ms;
    }
} // namespace dw
