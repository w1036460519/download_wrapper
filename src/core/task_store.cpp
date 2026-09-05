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

        void bind_double(sqlite3_stmt *st, const char *name, double val) {
            sqlite3_bind_double(st, param_idx(st, name), val);
        }

        void bind_blob(sqlite3_stmt *st, const char *name, const void *data, int size) {
            sqlite3_bind_blob(st, param_idx(st, name), data, size, SQLITE_TRANSIENT);
        }

        /// 从查询行填充 TaskRecord（列序无关，按名取值）。
        void fill_record(sqlite3_stmt *st, TaskRecord &r) {
            const col_map cm(st);
            r.client_id = cm.getText("client_id");
            r.protocol = static_cast<dw_protocol_t>(cm.getInt("protocol"));
            r.natural_key = cm.getText("natural_key");
            r.name = cm.getText("name");
            r.save_path = cm.getText("save_path");
            r.magnet_link = cm.getText("magnet_link");
            r.torrent_file = cm.getText("torrent_file");
            r.file_indexes = split_ints(cm.getText("file_indexes"));
            r.priority = cm.getInt("priority");
            r.status = static_cast<dw_task_status_t>(cm.getInt("status"));
            r.progress = cm.getDouble("progress");
            r.total_size = cm.getInt64("total_size");
            r.total_done = cm.getInt64("total_done");
            r.support_range = cm.getInt("support_range");
            r.etag = cm.getText("etag");
            r.last_modified = cm.getText("last_modified");
            r.created_at = cm.getInt64("created_at");
            r.modified_at = cm.getInt64("modified_at");
            r.source = static_cast<dw_source_t>(cm.getInt("source"));
            r.content_root = cm.getText("content_root");
            r.dup_checked = cm.getInt("dup_checked") != 0;
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
            CREATE TABLE IF NOT EXISTS tasks (
                client_id   TEXT NOT NULL,
                protocol    INTEGER NOT NULL,
                natural_key TEXT NOT NULL,
                name TEXT,
                save_path TEXT,
                magnet_link TEXT,
                torrent_file TEXT,
                trackers TEXT,
                file_indexes TEXT,
                priority INTEGER,
                status INTEGER,
                progress REAL,
                total_size INTEGER,
                total_done INTEGER,
                support_range INTEGER,
                etag TEXT,
                last_modified TEXT,
                created_at INTEGER,
                modified_at INTEGER,
                source INTEGER DEFAULT 0,
                content_root TEXT,
                dup_checked INTEGER DEFAULT 0,
                PRIMARY KEY (client_id, protocol, natural_key)
            );

            CREATE TABLE IF NOT EXISTS resume_data (
                client_id   TEXT NOT NULL,
                protocol    INTEGER NOT NULL,
                natural_key TEXT NOT NULL,
                data BLOB,
                saved_at INTEGER,
                PRIMARY KEY (client_id, protocol, natural_key)
            );

            CREATE TABLE IF NOT EXISTS file_records (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id       TEXT NOT NULL,
                type            INTEGER NOT NULL DEFAULT 0,
                is_remote       INTEGER DEFAULT 0,
                save_path       TEXT NOT NULL,
                root_name       TEXT NOT NULL,
                full_path       TEXT,
                file_type       INTEGER DEFAULT 1,
                ext             TEXT,
                protocol        INTEGER,
                natural_key     TEXT,
                status          INTEGER DEFAULT 0,
                total_size      INTEGER DEFAULT -1,
                total_done      INTEGER DEFAULT 0,
                created_at      INTEGER,
                modified_at     INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_file_records_task ON file_records(client_id, protocol, natural_key);
            CREATE INDEX IF NOT EXISTS idx_file_records_path ON file_records(full_path);

            CREATE TABLE IF NOT EXISTS file_progress_cache (
                client_id     TEXT NOT NULL,
                protocol      INTEGER NOT NULL,
                natural_key   TEXT NOT NULL,
                file_index    INTEGER NOT NULL,
                full_path     TEXT NOT NULL,
                intervals     TEXT NOT NULL,
                modified_at   INTEGER,
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

    std::vector<TaskRecord> TaskStore::load_active(const std::string &client_id) const {
        // 载入排队 / 活跃任务（DOWNLOADING=0, QUEUED=4, RESOLVING=5, PARSED=6）；
        // 暂停(1)/完成(2)/错误(3) 留库，按需回读，减小常驻内存。
        std::vector<TaskRecord> out;
        constexpr auto sql = "SELECT * FROM tasks WHERE client_id=:client_id AND status IN (0,4,5,6);";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return out;
        bind_text(st, ":client_id", client_id);

        while (sqlite3_step(st) == SQLITE_ROW) {
            TaskRecord r;
            fill_record(st, r);
            out.push_back(std::move(r));
        }
        sqlite3_finalize(st);
        return out;
    }

    std::vector<TaskRecord> TaskStore::load_all() const {
        std::vector<TaskRecord> out;
        constexpr auto sql = "SELECT * FROM tasks ORDER BY created_at DESC;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return out;

        while (sqlite3_step(st) == SQLITE_ROW) {
            TaskRecord r;
            fill_record(st, r);
            out.push_back(std::move(r));
        }
        sqlite3_finalize(st);
        return out;
    }

    bool TaskStore::load_by_natural_key(const std::string &client_id, const dw_protocol_t protocol,
                                        const std::string &natural_key, TaskRecord &out) const {
        constexpr auto sql =
                "SELECT * FROM tasks WHERE client_id=:client_id AND protocol=:protocol AND natural_key=:natural_key LIMIT 1;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return false;
        bind_text(st, ":client_id", client_id);
        bind_int(st, ":protocol", static_cast<int>(protocol));
        bind_text(st, ":natural_key", natural_key);
        bool found = false;
        if (sqlite3_step(st) == SQLITE_ROW) {
            fill_record(st, out);
            found = true;
        }
        sqlite3_finalize(st);
        return found;
    }

    void TaskStore::clear_local_tasks(const std::string &save_path) const {
        constexpr auto sql = "DELETE FROM file_records WHERE save_path=:save_path AND type IN (1,2);";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return;
        bind_text(st, ":save_path", save_path);
        sqlite3_step(st);
        sqlite3_finalize(st);
    }

    void TaskStore::insert(TaskRecord &r) const {
        const int64_t now = now_unix_ms();
        if (r.created_at == 0) r.created_at = now;
        if (r.modified_at == 0) r.modified_at = now;
        constexpr auto sql = R"(
            INSERT INTO tasks (client_id, protocol, natural_key,
                name, save_path,
                magnet_link, torrent_file, file_indexes,
                priority, status, progress, total_size, total_done,
                support_range, etag, last_modified, created_at, modified_at,
                source, content_root, dup_checked)
            VALUES (:client_id, :protocol, :natural_key,
                :name, :save_path,
                :magnet_link, :torrent_file, :file_indexes,
                :priority, :status, :progress, :total_size, :total_done,
                :support_range, :etag, :last_modified, :created_at, :modified_at,
                :source, :content_root, :dup_checked);
        )";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return;

        const std::string indexes = join_ints(r.file_indexes);

        bind_text(st, ":client_id", r.client_id);
        bind_int(st, ":protocol", static_cast<int>(r.protocol));
        bind_text(st, ":natural_key", r.natural_key);
        bind_text(st, ":name", r.name);
        bind_text(st, ":save_path", r.save_path);
        bind_text(st, ":magnet_link", r.magnet_link);
        bind_text(st, ":torrent_file", r.torrent_file);
        bind_text(st, ":file_indexes", indexes);
        bind_int(st, ":priority", r.priority);
        bind_int(st, ":status", r.status);
        bind_double(st, ":progress", r.progress);
        bind_int64(st, ":total_size", r.total_size);
        bind_int64(st, ":total_done", r.total_done);
        bind_int(st, ":support_range", r.support_range);
        bind_text(st, ":etag", r.etag);
        bind_text(st, ":last_modified", r.last_modified);
        bind_int64(st, ":created_at", r.created_at);
        bind_int64(st, ":modified_at", r.modified_at);
        bind_int(st, ":source", r.source);
        bind_text(st, ":content_root", r.content_root);
        bind_int(st, ":dup_checked", r.dup_checked ? 1 : 0);

        sqlite3_step(st);
        sqlite3_finalize(st);
    }

    void TaskStore::update(const TaskRecord &r) const {
        // 更新既有任务：按复合主键原地 UPDATE 全字段。
        // modified_at 自动刷为 now_unix_ms。
        const int64_t now = now_unix_ms();
        const int64_t modified_at = r.modified_at != 0 ? r.modified_at : now;
        constexpr auto sql = R"(
            UPDATE tasks SET protocol=:protocol, name=:name, save_path=:save_path,
                magnet_link=:magnet_link, torrent_file=:torrent_file, file_indexes=:file_indexes,
                priority=:priority, status=:status, progress=:progress, total_size=:total_size, total_done=:total_done,
                support_range=:support_range, etag=:etag, last_modified=:last_modified, created_at=:created_at, modified_at=:modified_at,
                source=:source, content_root=:content_root, dup_checked=:dup_checked
            WHERE client_id=:client_id AND protocol=:protocol AND natural_key=:natural_key;
        )";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return;

        const std::string indexes = join_ints(r.file_indexes);

        bind_int(st, ":protocol", static_cast<int>(r.protocol));
        bind_text(st, ":name", r.name);
        bind_text(st, ":save_path", r.save_path);
        bind_text(st, ":magnet_link", r.magnet_link);
        bind_text(st, ":torrent_file", r.torrent_file);
        bind_text(st, ":file_indexes", indexes);
        bind_int(st, ":priority", r.priority);
        bind_int(st, ":status", r.status);
        bind_double(st, ":progress", r.progress);
        bind_int64(st, ":total_size", r.total_size);
        bind_int64(st, ":total_done", r.total_done);
        bind_int(st, ":support_range", r.support_range);
        bind_text(st, ":etag", r.etag);
        bind_text(st, ":last_modified", r.last_modified);
        bind_int64(st, ":created_at", r.created_at);
        bind_int64(st, ":modified_at", modified_at);
        bind_int(st, ":source", r.source);
        bind_text(st, ":content_root", r.content_root);
        bind_int(st, ":dup_checked", r.dup_checked ? 1 : 0);
        bind_text(st, ":client_id", r.client_id);
        bind_text(st, ":natural_key", r.natural_key);

        sqlite3_step(st);
        sqlite3_finalize(st);
    }

    void TaskStore::update_status(const std::string &client_id, const dw_protocol_t protocol,
                                  const std::string &natural_key, const int32_t status) const {
        constexpr auto sql =
                "UPDATE tasks SET status=:status WHERE client_id=:client_id AND protocol=:protocol AND natural_key=:natural_key;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return;
        bind_int(st, ":status", status);
        bind_text(st, ":client_id", client_id);
        bind_int(st, ":protocol", static_cast<int>(protocol));
        bind_text(st, ":natural_key", natural_key);
        sqlite3_step(st);
        sqlite3_finalize(st);
    }

    void TaskStore::remove(const std::string &client_id, dw_protocol_t protocol, const std::string &natural_key) const {
        // 辅助 lambda：按复合键删除指定表的一行
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
        // 删除任务数据
        del_by_key("tasks");
        // 删除任务的恢复数据
        del_by_key("resume_data");
        // 删除任务的文件进度缓存
        del_by_key("file_progress_cache");
        // 删除文件记录
        del_by_key("file_records");
    }

    void TaskStore::reset_task_progress(const std::string &client_id, const dw_protocol_t protocol,
                                        const std::string &natural_key) const {
        // 清除 resume_data
        {
            constexpr auto sql = R"(
                DELETE FROM resume_data
                WHERE client_id=:client_id AND protocol=:protocol AND natural_key=:natural_key;
            )";
            sqlite3_stmt *st = nullptr;
            if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) == SQLITE_OK) {
                bind_text(st, ":client_id", client_id);
                bind_int(st, ":protocol", static_cast<int>(protocol));
                bind_text(st, ":natural_key", natural_key);
                sqlite3_step(st);
                sqlite3_finalize(st);
            }
        }
        // 清除 file_progress_cache
        {
            constexpr auto sql = R"(
                DELETE FROM file_progress_cache
                WHERE client_id=:client_id AND protocol=:protocol AND natural_key=:natural_key;
            )";
            sqlite3_stmt *st = nullptr;
            if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) == SQLITE_OK) {
                bind_text(st, ":client_id", client_id);
                bind_int(st, ":protocol", static_cast<int>(protocol));
                bind_text(st, ":natural_key", natural_key);
                sqlite3_step(st);
                sqlite3_finalize(st);
            }
        }
        // 重置 tasks 表进度字段
        {
            constexpr auto sql = R"(
                UPDATE tasks SET progress=0, total_size=0, total_done=0, status=:status, modified_at=:modified_at
                WHERE client_id=:client_id AND protocol=:protocol AND natural_key=:natural_key;
            )";
            sqlite3_stmt *st = nullptr;
            if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) == SQLITE_OK) {
                bind_int(st, ":status", DW_TASK_STATUS_QUEUED);
                bind_int64(st, ":modified_at", now_unix_ms());
                bind_text(st, ":client_id", client_id);
                bind_int(st, ":protocol", static_cast<int>(protocol));
                bind_text(st, ":natural_key", natural_key);
                sqlite3_step(st);
                sqlite3_finalize(st);
            }
        }
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

    std::vector<uint8_t> TaskStore::load_resume(const std::string &client_id, const dw_protocol_t protocol,
                                                const std::string &natural_key) const {
        std::vector<uint8_t> out;
        constexpr auto sql =
                "SELECT data FROM resume_data WHERE client_id=:client_id AND protocol=:protocol AND natural_key=:natural_key;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) {
            return out;
        }
        bind_text(st, ":client_id", client_id);
        bind_int(st, ":protocol", static_cast<int>(protocol));
        bind_text(st, ":natural_key", natural_key);
        if (sqlite3_step(st) == SQLITE_ROW) {
            const void *blob = sqlite3_column_blob(st, 0);
            const int n = sqlite3_column_bytes(st, 0);
            if (blob && n > 0) {
                const auto *p = static_cast<const uint8_t *>(blob);
                out.assign(p, p + n);
            }
        }
        sqlite3_finalize(st);
        return out;
    }

    void TaskStore::clear_resume(const std::string &client_id, const dw_protocol_t protocol,
                                 const std::string &natural_key) const {
        constexpr auto sql =
                "DELETE FROM resume_data WHERE client_id=:client_id AND protocol=:protocol AND natural_key=:natural_key;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) == SQLITE_OK) {
            bind_text(st, ":client_id", client_id);
            bind_int(st, ":protocol", static_cast<int>(protocol));
            bind_text(st, ":natural_key", natural_key);
            sqlite3_step(st);
            sqlite3_finalize(st);
        }
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
            r.root_name = cm.getText("root_name");
            r.full_path = cm.getText("full_path");
            r.file_type = cm.getInt("file_type") != 0;
            r.ext = cm.getText("ext");
            r.task_protocol = static_cast<dw_protocol_t>(cm.getInt("protocol"));
            r.task_natural_key = cm.getText("natural_key");
            r.status = cm.getInt("status");
            r.total_size = cm.getInt64("total_size");
            r.total_done = cm.getInt64("total_done");
            r.created_at = cm.getInt64("created_at");
            r.modified_at = cm.getInt64("modified_at");
        }
    } // namespace

    bool TaskStore::has_file_record(const std::string &client_id, const dw_protocol_t task_protocol,
                                    const std::string &task_natural_key) const {
        constexpr auto sql =
                "SELECT 1 FROM file_records WHERE client_id=:client_id AND protocol=:protocol AND natural_key=:natural_key LIMIT 1;";
        sqlite3_stmt *st = nullptr;
        bool exists = false;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) == SQLITE_OK) {
            bind_text(st, ":client_id", client_id);
            bind_int(st, ":protocol", static_cast<int>(task_protocol));
            bind_text(st, ":natural_key", task_natural_key);
            exists = sqlite3_step(st) == SQLITE_ROW;
            sqlite3_finalize(st);
        }
        return exists;
    }

    void TaskStore::insert_file_record(FileRecord &r) const {
        const int64_t now = now_unix_ms();
        if (r.created_at == 0) r.created_at = now;
        if (r.modified_at == 0) r.modified_at = now;
        constexpr auto sql = R"(
            INSERT INTO file_records (client_id, type, is_remote, save_path, root_name,
                full_path, file_type, ext, protocol, natural_key, status, total_size, total_done,
                created_at, modified_at)
            VALUES (:client_id, :type, :is_remote, :save_path, :root_name,
                :full_path, :file_type, :ext, :protocol, :natural_key, :status, :total_size, :total_done,
                :created_at, :modified_at);
        )";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return;
        bind_text(st, ":client_id", r.client_id);
        bind_int(st, ":type", r.type);
        bind_int(st, ":is_remote", r.is_remote ? 1 : 0);
        bind_text(st, ":save_path", r.save_path);
        bind_text(st, ":root_name", r.root_name);
        bind_text_or_null(st, ":full_path", r.full_path);
        bind_int(st, ":file_type", r.file_type ? 1 : 0);
        bind_text_or_null(st, ":ext", r.ext);
        if (r.has_task()) {
            bind_int(st, ":protocol", static_cast<int>(r.task_protocol));
            bind_text(st, ":natural_key", r.task_natural_key);
        } else {
            sqlite3_bind_null(st, sqlite3_bind_parameter_index(st, ":protocol"));
            sqlite3_bind_null(st, sqlite3_bind_parameter_index(st, ":natural_key"));
        }
        bind_int(st, ":status", r.status);
        bind_int64(st, ":total_size", r.total_size);
        bind_int64(st, ":total_done", r.total_done);
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
                                              const int64_t total_done) const {
        constexpr auto sql = R"(
            UPDATE file_records SET status=:status, total_size=:total_size, total_done=:total_done, modified_at=:modified_at
            WHERE protocol=:protocol AND natural_key=:natural_key;
        )";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return;
        bind_int(st, ":status", status);
        bind_int64(st, ":total_size", total_size);
        bind_int64(st, ":total_done", total_done);
        bind_int64(st, ":modified_at", now_unix_ms());
        bind_int(st, ":protocol", static_cast<int>(task_protocol));
        bind_text(st, ":natural_key", task_natural_key);
        sqlite3_step(st);
        sqlite3_finalize(st);
    }

    void TaskStore::update_file_record_meta(const std::string &client_id,
                                            const dw_protocol_t task_protocol, const std::string &task_natural_key,
                                            const std::string &root_name, const std::string &full_path,
                                            const bool file_type, const std::string &ext) const {
        constexpr auto sql = R"(
            UPDATE file_records SET root_name=:root_name, full_path=:full_path, file_type=:file_type, ext=:ext, modified_at=:modified_at
            WHERE client_id=:client_id AND protocol=:protocol AND natural_key=:natural_key;
        )";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return;
        bind_text(st, ":root_name", root_name);
        bind_text(st, ":full_path", full_path);
        bind_int(st, ":file_type", file_type ? 1 : 0);
        bind_text_or_null(st, ":ext", ext);
        bind_int64(st, ":modified_at", now_unix_ms());
        bind_text(st, ":client_id", client_id);
        bind_int(st, ":protocol", static_cast<int>(task_protocol));
        bind_text(st, ":natural_key", task_natural_key);
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
            (client_id, protocol, natural_key, file_index, full_path, intervals, modified_at)
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
            boost::json::array intervals_arr;
            for (const auto &[start, end]: ranges) {
                boost::json::array interval;
                interval.push_back(start);
                interval.push_back(end);
                intervals_arr.push_back(std::move(interval));
            }
            const std::string intervals_json = boost::json::serialize(intervals_arr);
            sqlite3_reset(st);
            bind_text(st, ":client_id", client_id);
            bind_int(st, ":protocol", static_cast<int>(protocol));
            bind_text(st, ":natural_key", natural_key);
            bind_int(st, ":file_index", idx);
            bind_text(st, ":full_path", path);
            bind_text(st, ":intervals", intervals_json);
            bind_int64(st, ":modified_at", now_unix_ms());
            sqlite3_step(st);
        }
        sqlite3_finalize(st);
        sqlite3_exec(db_, "COMMIT;", nullptr, nullptr, nullptr);
    }

    int64_t TaskStore::save_file_progress(const std::string &client_id, const dw_protocol_t protocol,
                                          const std::string &natural_key, const int32_t file_index,
                                          const std::string &full_path,
                                          const std::string &intervals_json) const {
        if (intervals_json.empty()) return 0;
        int64_t total = 0;
        constexpr auto sql = R"(
            INSERT OR REPLACE INTO file_progress_cache
            (client_id, protocol, natural_key, file_index, full_path, intervals, modified_at)
            VALUES (:client_id, :protocol, :natural_key, :file_index, :full_path, :intervals, :modified_at);
        )";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) == SQLITE_OK) {
            bind_text(st, ":client_id", client_id);
            bind_int(st, ":protocol", static_cast<int>(protocol));
            bind_text(st, ":natural_key", natural_key);
            bind_int(st, ":file_index", file_index);
            bind_text(st, ":full_path", full_path);
            bind_text(st, ":intervals", intervals_json);
            bind_int64(st, ":modified_at", now_unix_ms());
            sqlite3_step(st);
            sqlite3_finalize(st);
        }
        // 解析 JSON 计算累计已下载字节（供调用方判定文件完成）
        // 格式：[[start1,end1],[start2,end2],...]
        try {
            if (auto json = boost::json::parse(intervals_json); json.is_array()) {
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
        return total;
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
                "SELECT intervals FROM file_progress_cache WHERE client_id=:client_id AND protocol=:protocol AND natural_key=:natural_key AND file_index=:file_index;";
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
                "SELECT intervals FROM file_progress_cache WHERE full_path=:full_path AND file_index=:file_index;";
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
} // namespace dw
