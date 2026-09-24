/**
 * @file task_store.cpp
 * @brief 任务持久化存储层实现：SQLite 建表 / 读写与分片续传态序列化。
 *
 * 说明：本层不加锁、不涉及调度与内存注册表，仅围绕 sqlite3 连接完成 file_records
 * （任务状态持久化权威）、resume_data 及进度缓存的存取。并发串行化由调用方
 * （TaskManager 持有 mtx_）保证。
 */

#include "task_store.h"

#include "internal/downloader_internal.h"
#include "utils/string_util.h"
#include "utils/time_util.h"

#include <sqlite3.h>

#include <filesystem>
#include <string_view>
#include <unordered_map>

namespace dw {
    using utils::now_unix_ms;
    using utils::join_ints;
    using utils::split_ints;

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
            double getDouble(std::string_view name) const { return sqlite3_column_double(st, idx(name)); }
        };

        /// 命名参数绑定辅助：SQL 使用 :name 占位符，按名绑定不依赖位置索引。
        int param_idx(sqlite3_stmt *st, const char *name) {
            return sqlite3_bind_parameter_index(st, name);
        }

        void bind_text(sqlite3_stmt *st, const char *name, const std::string &val) {
            sqlite3_bind_text(st, param_idx(st, name), val.c_str(), -1, SQLITE_TRANSIENT);
        }

        void bind_text_or_null(sqlite3_stmt *st, const char *name, const std::string &val) {
            const int idx = param_idx(st, name);
            if (val.empty()) sqlite3_bind_null(st, idx);
            else sqlite3_bind_text(st, idx, val.c_str(), -1, SQLITE_TRANSIENT);
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

    TaskStore::~TaskStore() {
        close();
    }

    bool TaskStore::open(const std::string &path) {
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

    void TaskStore::init_schema() const {
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

            CREATE TABLE IF NOT EXISTS file_records (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id       TEXT NOT NULL,
                type            INTEGER NOT NULL DEFAULT 0,
                is_remote       INTEGER DEFAULT 0,
                save_path       TEXT NOT NULL,
                original_root_name TEXT NOT NULL DEFAULT '',  -- 重名/包装前的原始名称
                root_name       TEXT NOT NULL,
                full_path       TEXT,
                file_type       INTEGER DEFAULT 1,
                ext             TEXT,
                parsed          INTEGER DEFAULT 0,             -- 元数据已解析标志
                task_protocol   INTEGER,
                task_natural_key TEXT,
                status          INTEGER DEFAULT 0,
                total_size      INTEGER DEFAULT -1,
                total_done      INTEGER DEFAULT 0,
                priority        INTEGER DEFAULT 0,
                reason          INTEGER DEFAULT 0,
                message         TEXT NOT NULL DEFAULT '',
                created_at      INTEGER,
                modified_at     INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_file_records_task ON file_records(client_id, task_protocol, task_natural_key);
            CREATE INDEX IF NOT EXISTS idx_file_records_path ON file_records(full_path);

            CREATE TABLE IF NOT EXISTS file_progress_cache (
                client_id        TEXT NOT NULL,
                protocol         INTEGER NOT NULL,
                natural_key      TEXT NOT NULL,
                file_index       INTEGER NOT NULL,
                full_path        TEXT NOT NULL,
                size             INTEGER DEFAULT 0,
                downloaded_bytes INTEGER DEFAULT 0,
                segments         TEXT NOT NULL,
                modified_at      INTEGER,
                PRIMARY KEY (client_id, protocol, natural_key, file_index)
            );
            CREATE INDEX IF NOT EXISTS idx_fpc_path ON file_progress_cache(full_path);

            CREATE TABLE IF NOT EXISTS play_progress (
                full_path    TEXT NOT NULL PRIMARY KEY,
                position_ms  INTEGER NOT NULL DEFAULT 0,
                duration_ms  INTEGER NOT NULL DEFAULT 0,
                updated_at   INTEGER
            );
        )";
        sqlite3_exec(db_, sql, nullptr, nullptr, nullptr);
    }

    void TaskStore::clear_local_tasks(const std::string &client_id, const std::string &save_path) const {
        // 仅清理本客户端、本目录下的本地文件条目（type=DW_SOURCE_LOCAL_FILE），
        // 不得跨 client_id 删除，也不得连带 HTTP / BT 下载任务的记录。
        constexpr auto sql =
                "DELETE FROM file_records WHERE client_id=:client_id AND save_path=:save_path AND type=0;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return;
        bind_text(st, ":client_id", client_id);
        bind_text(st, ":save_path", save_path);
        sqlite3_step(st);
        sqlite3_finalize(st);
    }

    void TaskStore::remove(const std::string &client_id, dw_protocol_t protocol, const std::string &natural_key) const {
        // resume_data / file_progress_cache 仍用 protocol + natural_key 列名
        auto del_by_key = [this, &client_id, &protocol, &natural_key](const char *table) {
            sqlite3_stmt *st = nullptr;
            const std::string sql = std::string("DELETE FROM ") + table +
                                    " WHERE client_id=:client_id AND protocol=:protocol AND natural_key=:natural_key;";
            if (sqlite3_prepare_v2(db_, sql.c_str(), -1, &st, nullptr) == SQLITE_OK) {
                bind_text(st, ":client_id", client_id);
                bind_int(st, ":protocol", static_cast<int>(protocol));
                bind_text(st, ":natural_key", natural_key);
                sqlite3_step(st);
                sqlite3_finalize(st);
            }
        };
        del_by_key("resume_data");
        del_by_key("file_progress_cache");
        // file_records 使用 task_protocol + task_natural_key 列名
        {
            sqlite3_stmt *st = nullptr;
            constexpr auto sql =
                    "DELETE FROM file_records WHERE client_id=:client_id AND task_protocol=:task_protocol AND task_natural_key=:task_natural_key;";
            if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) == SQLITE_OK) {
                bind_text(st, ":client_id", client_id);
                bind_int(st, ":task_protocol", static_cast<int>(protocol));
                bind_text(st, ":task_natural_key", natural_key);
                sqlite3_step(st);
                sqlite3_finalize(st);
            }
        }
    }

    void TaskStore::update_file_record_status(const std::string &client_id, const dw_protocol_t task_protocol,
                                              const std::string &task_natural_key,
                                              const dw_task_status_t status, const dw_reason_t reason,
                                              const std::string &message) const {
        // 任务状态迁移即时写（权威列），不携带进度。
        constexpr auto sql = R"(
            UPDATE file_records SET status=:status, reason=:reason, message=:message, modified_at=:modified_at
            WHERE client_id=:client_id AND task_protocol=:task_protocol AND task_natural_key=:task_natural_key;
        )";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return;
        bind_int(st, ":status", static_cast<int>(status));
        bind_int(st, ":reason", static_cast<int>(reason));
        bind_text(st, ":message", message);
        bind_int64(st, ":modified_at", now_unix_ms());
        bind_text(st, ":client_id", client_id);
        bind_int(st, ":task_protocol", static_cast<int>(task_protocol));
        bind_text(st, ":task_natural_key", task_natural_key);
        sqlite3_step(st);
        sqlite3_finalize(st);
    }

    void TaskStore::update_file_record(const FileRecord &rec) const {
        // 根据三要素（client_id, protocol, natural_key）更新全部字段
        constexpr auto sql = R"(
            UPDATE file_records SET
                type=:type, is_remote=:is_remote, save_path=:save_path,
                original_root_name=:original_root_name, root_name=:root_name,
                full_path=:full_path, file_type=:file_type, ext=:ext, parsed=:parsed,
                status=:status, total_size=:total_size, total_done=:total_done,
                priority=:priority, reason=:reason, message=:message,
                modified_at=:modified_at
            WHERE client_id=:client_id AND task_protocol=:task_protocol AND task_natural_key=:task_natural_key;
        )";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return;
        bind_int(st, ":type", static_cast<int>(rec.type));
        bind_int(st, ":is_remote", rec.is_remote ? 1 : 0);
        bind_text(st, ":save_path", rec.save_path);
        bind_text(st, ":original_root_name", rec.original_root_name);
        bind_text(st, ":root_name", rec.root_name);
        bind_text(st, ":full_path", rec.full_path);
        bind_int(st, ":file_type", rec.file_type ? 1 : 0);
        bind_text(st, ":ext", rec.ext);
        bind_int(st, ":parsed", rec.parsed ? 1 : 0);
        bind_int(st, ":status", rec.status);
        bind_int64(st, ":total_size", rec.total_size);
        bind_int64(st, ":total_done", rec.total_done);
        bind_int(st, ":priority", rec.priority);
        bind_int(st, ":reason", rec.reason);
        bind_text(st, ":message", rec.message);
        bind_int64(st, ":modified_at", now_unix_ms());
        bind_text(st, ":client_id", rec.client_id);
        bind_int(st, ":task_protocol", static_cast<int>(rec.task_protocol));
        bind_text(st, ":task_natural_key", rec.task_natural_key);
        sqlite3_step(st);
        sqlite3_finalize(st);
    }

    void TaskStore::save_resume(const std::string &client_id, dw_protocol_t protocol,
                                const std::string &natural_key,
                                const uint8_t *data, const size_t size) const {
        constexpr auto sql = R"(
            INSERT OR REPLACE INTO resume_data (client_id, protocol, natural_key, data, saved_at)
            VALUES (:client_id, :protocol, :natural_key, :data, :saved_at);
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

    void TaskStore::save_resume_source(const std::string &client_id, dw_protocol_t protocol,
                                       const std::string &natural_key,
                                       const std::string &save_path,
                                       const std::string &magnet_link,
                                       const std::string &torrent_file) const {
        // 仅写入来源字段（save_path / magnet_link / torrent_file），不影响 data 列；
        // 使用 INSERT ... ON CONFLICT DO UPDATE 保留已有 data。
        constexpr auto sql = R"(
            INSERT INTO resume_data (client_id, protocol, natural_key, save_path, magnet_link, torrent_file, saved_at)
            VALUES (:client_id, :protocol, :natural_key, :save_path, :magnet_link, :torrent_file, :saved_at)
            ON CONFLICT(client_id, protocol, natural_key) DO UPDATE SET
                save_path = excluded.save_path,
                magnet_link = excluded.magnet_link,
                torrent_file = excluded.torrent_file;
        )";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return;
        bind_text(st, ":client_id", client_id);
        bind_int(st, ":protocol", static_cast<int>(protocol));
        bind_text(st, ":natural_key", natural_key);
        bind_text(st, ":save_path", save_path);
        bind_text(st, ":magnet_link", magnet_link);
        bind_text(st, ":torrent_file", torrent_file);
        bind_int64(st, ":saved_at", now_unix_ms());
        sqlite3_step(st);
        sqlite3_finalize(st);
    }

    TaskStore::ResumeInfo TaskStore::load_resume_info(const std::string &client_id, const dw_protocol_t protocol,
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

    // ---- 文件目录表（file_records）----

    namespace {
        void fill_file_record(sqlite3_stmt *st, FileRecord &r) {
            const col_map cm(st);
            r.id = cm.getInt64("id");
            r.client_id = cm.getText("client_id");
            r.type = static_cast<dw_source_t>(cm.getInt("type"));
            r.is_remote = cm.getInt("is_remote") != 0;
            r.save_path = cm.getText("save_path");
            r.original_root_name = cm.getText("original_root_name");
            r.root_name = cm.getText("root_name");
            r.full_path = cm.getText("full_path");
            r.file_type = cm.getInt("file_type") != 0;
            r.ext = cm.getText("ext");
            r.parsed = cm.getInt("parsed") != 0;
            r.task_protocol = static_cast<dw_protocol_t>(cm.getInt("task_protocol"));
            r.task_natural_key = cm.getText("task_natural_key");
            r.status = cm.getInt("status");
            r.total_size = cm.getInt64("total_size");
            r.total_done = cm.getInt64("total_done");
            r.priority = cm.getInt("priority");
            r.reason = cm.getInt("reason");
            r.message = cm.getText("message");
            // support_range/etag/last_modified 不再从 file_records 读取（已在 resume_data 表中）
            r.created_at = cm.getInt64("created_at");
            r.modified_at = cm.getInt64("modified_at");
        }
    } // namespace

    bool TaskStore::has_file_record(const std::string &client_id, const dw_protocol_t task_protocol,
                                    const std::string &task_natural_key) const {
        constexpr auto sql =
                "SELECT 1 FROM file_records WHERE client_id=:client_id AND task_protocol=:task_protocol AND task_natural_key=:task_natural_key LIMIT 1;";
        sqlite3_stmt *st = nullptr;
        bool exists = false;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) == SQLITE_OK) {
            bind_text(st, ":client_id", client_id);
            bind_int(st, ":task_protocol", static_cast<int>(task_protocol));
            bind_text(st, ":task_natural_key", task_natural_key);
            exists = sqlite3_step(st) == SQLITE_ROW;
            sqlite3_finalize(st);
        }
        return exists;
    }

    bool TaskStore::find_file_record(const std::string &client_id, const dw_protocol_t task_protocol,
                                     const std::string &task_natural_key, FileRecord &out) const {
        constexpr auto sql =
                "SELECT * FROM file_records WHERE client_id=:client_id AND task_protocol=:task_protocol AND task_natural_key=:task_natural_key LIMIT 1;";
        sqlite3_stmt *st = nullptr;
        bool found = false;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) == SQLITE_OK) {
            bind_text(st, ":client_id", client_id);
            bind_int(st, ":task_protocol", static_cast<int>(task_protocol));
            bind_text(st, ":task_natural_key", task_natural_key);
            if (sqlite3_step(st) == SQLITE_ROW) {
                fill_file_record(st, out);
                found = true;
            }
            sqlite3_finalize(st);
        }
        return found;
    }

    void TaskStore::touch_file_record(const std::string &client_id, const dw_protocol_t task_protocol,
                                      const std::string &task_natural_key) const {
        constexpr auto sql =
                "UPDATE file_records SET modified_at=:modified_at WHERE client_id=:client_id AND task_protocol=:task_protocol AND task_natural_key=:task_natural_key;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) == SQLITE_OK) {
            bind_int64(st, ":modified_at", now_unix_ms());
            bind_text(st, ":client_id", client_id);
            bind_int(st, ":task_protocol", static_cast<int>(task_protocol));
            bind_text(st, ":task_natural_key", task_natural_key);
            sqlite3_step(st);
            sqlite3_finalize(st);
        }
    }

    void TaskStore::insert_file_record(FileRecord &r) const {
        const int64_t now = now_unix_ms();
        if (r.created_at == 0) r.created_at = now;
        if (r.modified_at == 0) r.modified_at = now;
        constexpr auto sql = R"(
            INSERT INTO file_records (client_id, type, is_remote, save_path, original_root_name, root_name,
                full_path, file_type, ext, parsed, task_protocol, task_natural_key, status, total_size, total_done,
                priority, reason, message, created_at, modified_at)
            VALUES (:client_id, :type, :is_remote, :save_path, :original_root_name, :root_name,
                :full_path, :file_type, :ext, :parsed, :task_protocol, :task_natural_key, :status, :total_size, :total_done,
                :priority, :reason, :message, :created_at, :modified_at);
        )";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return;
        bind_text(st, ":client_id", r.client_id);
        bind_int(st, ":type", r.type);
        bind_int(st, ":is_remote", r.is_remote ? 1 : 0);
        bind_text(st, ":save_path", r.save_path);
        bind_text(st, ":original_root_name", r.original_root_name);
        bind_text(st, ":root_name", r.root_name);
        bind_text_or_null(st, ":full_path", r.full_path);
        bind_int(st, ":file_type", r.file_type ? 1 : 0);
        bind_text_or_null(st, ":ext", r.ext);
        bind_int(st, ":parsed", r.parsed ? 1 : 0);
        if (r.has_task()) {
            bind_int(st, ":task_protocol", static_cast<int>(r.task_protocol));
            bind_text(st, ":task_natural_key", r.task_natural_key);
        } else {
            sqlite3_bind_null(st, sqlite3_bind_parameter_index(st, ":task_protocol"));
            sqlite3_bind_null(st, sqlite3_bind_parameter_index(st, ":task_natural_key"));
        }
        bind_int(st, ":status", r.status);
        bind_int64(st, ":total_size", r.total_size);
        bind_int64(st, ":total_done", r.total_done);
        bind_int(st, ":priority", r.priority);
        bind_int(st, ":reason", r.reason);
        bind_text(st, ":message", r.message);
        bind_int64(st, ":created_at", r.created_at);
        bind_int64(st, ":modified_at", r.modified_at);
        sqlite3_step(st);
        sqlite3_finalize(st);
        r.id = sqlite3_last_insert_rowid(db_);
    }

    std::vector<FileRecord> TaskStore::load_file_records(const std::string &client_id) const {
        std::vector<FileRecord> out;
        constexpr auto sql =
                "SELECT * FROM file_records WHERE client_id=:client_id ORDER BY modified_at DESC;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return out;
        bind_text(st, ":client_id", client_id);
        while (sqlite3_step(st) == SQLITE_ROW) {
            FileRecord r;
            fill_file_record(st, r);
            out.push_back(std::move(r));
        }
        sqlite3_finalize(st);
        return out;
    }

    std::vector<FileRecord> TaskStore::load_file_records(const std::string &client_id,
                                                         const std::vector<dw_task_status_t> &statuses) const {
        std::vector<FileRecord> out;
        if (statuses.empty()) return out;
        // 动态构建 IN 子句：status IN (?, ?, ...)
        std::string placeholders;
        for (size_t i = 0; i < statuses.size(); ++i) {
            if (i > 0) placeholders += ", ";
            placeholders += "?";
        }
        const std::string sql =
                "SELECT * FROM file_records WHERE client_id=? AND status IN (" + placeholders +
                ") ORDER BY modified_at DESC;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql.c_str(), -1, &st, nullptr) != SQLITE_OK) return out;
        sqlite3_bind_text(st, 1, client_id.c_str(), -1, SQLITE_TRANSIENT);
        for (size_t i = 0; i < statuses.size(); ++i) {
            sqlite3_bind_int(st, static_cast<int>(i + 2), static_cast<int>(statuses[i]));
        }
        while (sqlite3_step(st) == SQLITE_ROW) {
            FileRecord r;
            fill_file_record(st, r);
            out.push_back(std::move(r));
        }
        sqlite3_finalize(st);
        return out;
    }

    std::vector<FileRecord> TaskStore::load_file_records(const std::string &client_id,
                                                         const std::vector<dw_protocol_t> &protocols,
                                                         const std::vector<dw_task_status_t> &statuses) const {
        std::vector<FileRecord> out;
        if (protocols.empty() || statuses.empty()) return out;
        // 动态构建 IN 子句：protocol IN (?, ...) AND status IN (?, ...)
        std::string proto_placeholders, status_placeholders;
        for (size_t i = 0; i < protocols.size(); ++i) {
            if (i > 0) proto_placeholders += ", ";
            proto_placeholders += "?";
        }
        for (size_t i = 0; i < statuses.size(); ++i) {
            if (i > 0) status_placeholders += ", ";
            status_placeholders += "?";
        }
        const std::string sql =
                "SELECT * FROM file_records WHERE client_id=? AND protocol IN (" + proto_placeholders +
                ") AND status IN (" + status_placeholders + ") ORDER BY modified_at DESC;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql.c_str(), -1, &st, nullptr) != SQLITE_OK) return out;
        int idx = 1;
        sqlite3_bind_text(st, idx++, client_id.c_str(), -1, SQLITE_TRANSIENT);
        for (size_t i = 0; i < protocols.size(); ++i) {
            sqlite3_bind_int(st, idx++, static_cast<int>(protocols[i]));
        }
        for (size_t i = 0; i < statuses.size(); ++i) {
            sqlite3_bind_int(st, idx++, static_cast<int>(statuses[i]));
        }
        while (sqlite3_step(st) == SQLITE_ROW) {
            FileRecord r;
            fill_file_record(st, r);
            out.push_back(std::move(r));
        }
        sqlite3_finalize(st);
        return out;
    }

    std::vector<FileRecord> TaskStore::load_file_records_by_save_path(const std::string &client_id,
                                                                      const std::string &save_path) const {
        std::vector<FileRecord> out;
        constexpr auto sql =
                "SELECT * FROM file_records WHERE client_id=:client_id AND save_path=:save_path ORDER BY modified_at DESC;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return out;
        bind_text(st, ":client_id", client_id);
        bind_text(st, ":save_path", save_path);
        while (sqlite3_step(st) == SQLITE_ROW) {
            FileRecord r;
            fill_file_record(st, r);
            out.push_back(std::move(r));
        }
        sqlite3_finalize(st);
        return out;
    }

    void TaskStore::delete_file_record_by_name(const std::string &client_id, const std::string &save_path,
                                               const std::string &root_name) const {
        constexpr auto sql =
                "DELETE FROM file_records WHERE client_id=:client_id AND save_path=:save_path AND root_name=:root_name;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK)
            return;
        bind_text(st, ":client_id", client_id);
        bind_text(st, ":save_path", save_path);
        bind_text(st, ":root_name", root_name);
        sqlite3_step(st);
        sqlite3_finalize(st);
    }

    void TaskStore::update_file_record_status_by_name(const std::string &save_path, const std::string &root_name,
                                                      const int32_t status) const {
        constexpr auto sql =
                "UPDATE file_records SET status=:status, modified_at=:modified_at WHERE save_path=:save_path AND root_name=:root_name;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK)
            return;
        bind_int(st, ":status", status);
        bind_int64(st, ":modified_at", now_unix_ms());
        bind_text(st, ":save_path", save_path);
        bind_text(st, ":root_name", root_name);
        sqlite3_step(st);
        sqlite3_finalize(st);
    }

    void TaskStore::sync_file_record_progress(const dw_protocol_t task_protocol, const std::string &task_natural_key,
                                              const int32_t status, const int64_t total_size,
                                              const int64_t total_done, const int32_t reason,
                                              const std::string &message) const {
        constexpr auto sql = R"(
            UPDATE file_records SET status=:status, total_size=:total_size, total_done=:total_done,
                reason=:reason, message=:message, modified_at=:modified_at
            WHERE task_protocol=:task_protocol AND task_natural_key=:task_natural_key;
        )";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return;
        bind_int(st, ":status", status);
        bind_int64(st, ":total_size", total_size);
        bind_int64(st, ":total_done", total_done);
        bind_int(st, ":reason", reason);
        bind_text(st, ":message", message);
        bind_int64(st, ":modified_at", now_unix_ms());
        bind_int(st, ":task_protocol", static_cast<int>(task_protocol));
        bind_text(st, ":task_natural_key", task_natural_key);
        sqlite3_step(st);
        sqlite3_finalize(st);
    }

    void TaskStore::update_file_record_meta(const std::string &client_id,
                                            const dw_protocol_t task_protocol, const std::string &task_natural_key,
                                            const std::string &task_save_path,
                                            const std::string &original_root_name, const std::string &root_name,
                                            const std::string &full_path,
                                            const bool file_type, const std::string &ext) const {
        constexpr auto sql = R"(
            UPDATE file_records SET save_path=:save_path, original_root_name=:original_root_name, root_name=:root_name,
                full_path=:full_path, file_type=:file_type, ext=:ext,
                parsed=1, modified_at=:modified_at
            WHERE client_id=:client_id AND task_protocol=:task_protocol AND task_natural_key=:task_natural_key;
        )";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return;
        bind_text(st, ":save_path", task_save_path);
        bind_text(st, ":original_root_name", original_root_name);
        bind_text(st, ":root_name", root_name);
        bind_text(st, ":full_path", full_path);
        bind_int(st, ":file_type", file_type ? 1 : 0);
        bind_text_or_null(st, ":ext", ext);
        bind_int64(st, ":modified_at", now_unix_ms());
        bind_text(st, ":client_id", client_id);
        bind_int(st, ":task_protocol", static_cast<int>(task_protocol));
        bind_text(st, ":task_natural_key", task_natural_key);
        sqlite3_step(st);
        sqlite3_finalize(st);
    }

    // ---- 播放进度（独立表 play_progress，以完整路径为键）----

    void TaskStore::set_play_position(const std::string &full_path, const int64_t position_ms) const {
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

    int64_t TaskStore::get_play_position(const std::string &full_path) const {
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

    // ---- 文件下载进度缓存 ----

    void TaskStore::replace_file_progress(
        const std::string &client_id, const dw_protocol_t protocol, const std::string &natural_key,
        const std::vector<std::tuple<std::string, int32_t, std::vector<dw_byte_range_t> > > &file_ranges) const {
        if (file_ranges.empty()) return;
        sqlite3_exec(db_, "BEGIN;", nullptr, nullptr, nullptr);
        constexpr auto sql = R"(
            INSERT OR REPLACE INTO file_progress_cache
            (client_id, protocol, natural_key, file_index, full_path, segments, modified_at)
            VALUES (?,?,?,?,?,?,?);
        )";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) {
            sqlite3_exec(db_, "ROLLBACK;", nullptr, nullptr, nullptr);
            return;
        }
        for (const auto &[path, idx, ranges]: file_ranges) {
            if (ranges.empty()) continue;
            // 序列化区间集合为 JSON：[[start1,end1],[start2,end2],...]
            boost::json::array segments_arr;
            for (const auto &[start, end]: ranges) {
                boost::json::array interval;
                interval.push_back(start);
                interval.push_back(end);
                segments_arr.push_back(std::move(interval));
            }
            const std::string segments_json = boost::json::serialize(segments_arr);
            sqlite3_reset(st);
            bind_text(st, ":client_id", client_id);
            bind_int(st, ":protocol", static_cast<int>(protocol));
            bind_text(st, ":natural_key", natural_key);
            bind_int(st, ":file_index", idx);
            bind_text(st, ":full_path", path);
            bind_text(st, ":segments", segments_json);
            bind_int64(st, ":modified_at", now_unix_ms());
            sqlite3_step(st);
        }
        sqlite3_finalize(st);
        sqlite3_exec(db_, "COMMIT;", nullptr, nullptr, nullptr);
    }

    int64_t TaskStore::save_file_progress(const std::string &client_id, const dw_protocol_t protocol,
                                          const std::string &natural_key, const int32_t file_index,
                                          const std::string &full_path,
                                          const int64_t size, const int64_t downloaded_bytes,
                                          const std::string &segments_json) const {
        if (segments_json.empty()) return 0;
        constexpr auto sql = R"(
            INSERT OR REPLACE INTO file_progress_cache
            (client_id, protocol, natural_key, file_index, full_path, size, downloaded_bytes, segments, modified_at)
            VALUES (:client_id, :protocol, :natural_key, :file_index, :full_path, :size, :downloaded_bytes, :segments, :modified_at);
        )";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) == SQLITE_OK) {
            bind_text(st, ":client_id", client_id);
            bind_int(st, ":protocol", static_cast<int>(protocol));
            bind_text(st, ":natural_key", natural_key);
            bind_int(st, ":file_index", file_index);
            bind_text(st, ":full_path", full_path);
            bind_int64(st, ":size", size);
            bind_int64(st, ":downloaded_bytes", downloaded_bytes);
            bind_text(st, ":segments", segments_json);
            bind_int64(st, ":modified_at", now_unix_ms());
            sqlite3_step(st);
            sqlite3_finalize(st);
        }
        return downloaded_bytes;
    }

    void TaskStore::delete_file_progress_by_file(const std::string &client_id, const dw_protocol_t protocol,
                                                 const std::string &natural_key, const int32_t file_index) const {
        constexpr auto sql =
                "DELETE FROM file_progress_cache WHERE client_id=:client_id AND protocol=:protocol AND natural_key=:natural_key AND file_index=:file_index;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK)
            return;
        bind_text(st, ":client_id", client_id);
        bind_int(st, ":protocol", static_cast<int>(protocol));
        bind_text(st, ":natural_key", natural_key);
        bind_int(st, ":file_index", file_index);
        sqlite3_step(st);
        sqlite3_finalize(st);
    }

    void TaskStore::mark_file_complete(const std::string &client_id, const dw_protocol_t protocol,
                                       const std::string &natural_key, const int32_t file_index) const {
        // 更新 downloaded_bytes = size，segments = [[0, size-1]]
        constexpr auto sql = R"(
            UPDATE file_progress_cache
            SET downloaded_bytes = size,
                segments = '[[0,' || (size - 1) || ']]',
                modified_at = :modified_at
            WHERE client_id=:client_id AND protocol=:protocol AND natural_key=:natural_key AND file_index=:file_index;
        )";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK)
            return;
        bind_int64(st, ":modified_at", now_unix_ms());
        bind_text(st, ":client_id", client_id);
        bind_int(st, ":protocol", static_cast<int>(protocol));
        bind_text(st, ":natural_key", natural_key);
        bind_int(st, ":file_index", file_index);
        sqlite3_step(st);
        sqlite3_finalize(st);
    }

    void TaskStore::delete_file_progress_by_task(const std::string &client_id, const dw_protocol_t protocol,
                                                 const std::string &natural_key) const {
        constexpr auto sql =
                "DELETE FROM file_progress_cache WHERE client_id=:client_id AND protocol=:protocol AND natural_key=:natural_key;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK)
            return;
        bind_text(st, ":client_id", client_id);
        bind_int(st, ":protocol", static_cast<int>(protocol));
        bind_text(st, ":natural_key", natural_key);
        sqlite3_step(st);
        sqlite3_finalize(st);
    }

    int64_t TaskStore::sum_file_progress_by_file(const std::string &client_id, const dw_protocol_t protocol,
                                                 const std::string &natural_key, const int32_t file_index) const {
        constexpr auto sql =
                "SELECT segments FROM file_progress_cache WHERE client_id=:client_id AND protocol=:protocol AND natural_key=:natural_key AND file_index=:file_index;";
        sqlite3_stmt *st = nullptr;
        int64_t total = 0;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK)
            return total;
        bind_text(st, ":client_id", client_id);
        bind_int(st, ":protocol", static_cast<int>(protocol));
        bind_text(st, ":natural_key", natural_key);
        bind_int(st, ":file_index", file_index);
        if (sqlite3_step(st) == SQLITE_ROW) {
            const char *json_str = reinterpret_cast<const char *>(sqlite3_column_text(st, 0));
            if (json_str) {
                try {
                    auto json = boost::json::parse(json_str);
                    if (json.is_array()) {
                        for (const auto &interval: json.as_array()) {
                            if (interval.is_array() && interval.as_array().size() == 2) {
                                const int64_t start = interval.as_array()[0].as_int64();
                                const int64_t end = interval.as_array()[1].as_int64();
                                total += (end - start + 1);
                            }
                        }
                    }
                } catch (...) {
                    // JSON 解析失败，返回 0
                }
            }
        }
        sqlite3_finalize(st);
        return total;
    }

    std::vector<dw_byte_range_t> TaskStore::load_segments(const std::string &full_path, int32_t file_index) const {
        std::vector<dw_byte_range_t> out;
        // 按完整路径查询已下载区间（App 播放器按路径消费；file_index 辅助定位）。
        constexpr auto sql =
                "SELECT segments FROM file_progress_cache WHERE full_path=:full_path AND file_index=:file_index;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return out;
        bind_text(st, ":full_path", full_path);
        bind_int(st, ":file_index", file_index);
        if (sqlite3_step(st) == SQLITE_ROW) {
            const char *json_str = reinterpret_cast<const char *>(sqlite3_column_text(st, 0));
            if (json_str) {
                try {
                    auto json = boost::json::parse(json_str);
                    if (json.is_array()) {
                        for (const auto &interval: json.as_array()) {
                            if (interval.is_array() && interval.as_array().size() == 2) {
                                dw_byte_range_t seg{};
                                seg.start = interval.as_array()[0].as_int64();
                                seg.end = interval.as_array()[1].as_int64();
                                out.push_back(seg);
                            }
                        }
                    }
                } catch (...) {
                    // JSON 解析失败，返回空
                }
            }
        }
        sqlite3_finalize(st);
        return out;
    }

    std::unordered_map<std::string, std::pair<int64_t, std::string>>
    TaskStore::load_file_progress_by_paths(const std::vector<std::string> &paths) const {
        std::unordered_map<std::string, std::pair<int64_t, std::string>> result;
        if (paths.empty() || !db_) return result;
        // 构建 IN 子句
        std::string placeholders;
        for (size_t i = 0; i < paths.size(); ++i) {
            if (i > 0) placeholders += ",";
            placeholders += "?";
        }
        const std::string sql =
                "SELECT full_path, downloaded_bytes, segments FROM file_progress_cache WHERE full_path IN (" +
                placeholders + ")";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql.c_str(), -1, &st, nullptr) != SQLITE_OK) return result;
        for (size_t i = 0; i < paths.size(); ++i) {
            sqlite3_bind_text(st, static_cast<int>(i + 1), paths[i].c_str(), -1, SQLITE_TRANSIENT);
        }
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *path = reinterpret_cast<const char *>(sqlite3_column_text(st, 0));
            const int64_t downloaded_bytes = sqlite3_column_int64(st, 1);
            const char *segments = reinterpret_cast<const char *>(sqlite3_column_text(st, 2));
            if (path) {
                result[path] = {downloaded_bytes, segments ? segments : ""};
            }
        }
        sqlite3_finalize(st);
        return result;
    }
} // namespace dw
