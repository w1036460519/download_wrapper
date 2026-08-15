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

#include <cstdlib>
#include <cstring>

namespace dw {
    using utils::now_unix_ms;
    using utils::join_lines;
    using utils::split_lines;
    using utils::join_ints;
    using utils::split_ints;

    namespace {
        /// SQLite text 列安全读取（NULL 返回空串）。
        std::string col_text(sqlite3_stmt *st, int idx) {
            const unsigned char *t = sqlite3_column_text(st, idx);
            return t ? reinterpret_cast<const char *>(t) : std::string();
        }

        /// SQLite text 列堆拷贝（NULL 返回 nullptr），供 dw_file_info_t 字符串字段填充。
        char *dup_col_text(sqlite3_stmt *st, int idx) {
            const unsigned char *t = sqlite3_column_text(st, idx);
            if (!t) return nullptr;
            const auto *s = reinterpret_cast<const char *>(t);
            const size_t len = std::strlen(s);
            auto *p = static_cast<char *>(std::malloc(len + 1));
            if (p) std::memcpy(p, s, len + 1);
            return p;
        }

        /// 从查询行填充 TaskRecord（load_active / load_by_natural_key 共用，列序须与 SELECT 一致）。
        void fill_record(sqlite3_stmt *st, TaskRecord &r) {
            r.client_id  = col_text(st, 0);
            r.key_type   = static_cast<TaskKeyType>(sqlite3_column_int(st, 1));
            // 列 2 = natural_key（DB 列保留，内存不存；url/info_hash/content_root 已含同等数据）
            r.protocol = static_cast<dw_protocol_t>(sqlite3_column_int(st, 3));
            r.name = col_text(st, 4);
            r.save_path = col_text(st, 5);
            r.filename = col_text(st, 6);
            r.url = col_text(st, 7);
            r.info_hash = col_text(st, 8);
            r.magnet_link = col_text(st, 9);
            r.torrent_file = col_text(st, 10);
            r.trackers = split_lines(col_text(st, 11));
            r.file_indexes = split_ints(col_text(st, 12));
            r.priority = sqlite3_column_int(st, 13);
            r.status = static_cast<dw_task_status_t>(sqlite3_column_int(st, 14));
            r.progress = sqlite3_column_double(st, 15);
            r.total_size = sqlite3_column_int64(st, 16);
            r.total_done = sqlite3_column_int64(st, 17);
            r.support_range = sqlite3_column_int(st, 18);
            r.etag = col_text(st, 19);
            r.last_modified = col_text(st, 20);
            r.created_at = sqlite3_column_int64(st, 21);
            r.modified_at = sqlite3_column_int64(st, 22);
            r.source = sqlite3_column_int(st, 23);
            r.content_root = col_text(st, 24);
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

    void TaskStore::init_schema() {
        // 表结构：复合主键 (client_id, key_type, natural_key) 表示全局唯一。
        // - client_id：App 启动时注入的 UUIDv4。远程同步场景下保留远端 clientId 用于多客户端隔离。
        // - key_type：0=http(url) 1=bt(info_hash) 2=local(content_root)。
        // - natural_key：随 key_type 语义变化，URL / info_hash / content_root 三选一。
        // 同一客户端同 key_type 下 natural_key 冲突时按业务去重（add 路径预判重）。
        // resume_data / task_files / file_segments / file_cache 复制复合键（无 FK，靠应用层保证一致）。
        // 项目未上线：检测到旧 schema（task_id 列存在 / modified_at 列缺失）则 DROP 全部表重建。
        sqlite3_stmt *chk = nullptr;
        if (sqlite3_prepare_v2(db_,
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='tasks';",
                -1, &chk, nullptr) == SQLITE_OK) {
            bool need_rebuild = false;
            if (sqlite3_step(chk) == SQLITE_ROW) {
                std::string def = col_text(chk, 0);
                if (def.find("task_id TEXT PRIMARY KEY") != std::string::npos ||
                    def.find("INTEGER PRIMARY KEY AUTOINCREMENT") != std::string::npos ||
                    def.find("modified_at INTEGER") == std::string::npos) {
                    need_rebuild = true;
                }
            }
            sqlite3_finalize(chk);
            if (need_rebuild) {
                sqlite3_exec(db_, "DROP TABLE IF EXISTS file_segments;", nullptr, nullptr, nullptr);
                sqlite3_exec(db_, "DROP TABLE IF EXISTS file_cache;", nullptr, nullptr, nullptr);
                sqlite3_exec(db_, "DROP TABLE IF EXISTS task_files;", nullptr, nullptr, nullptr);
                sqlite3_exec(db_, "DROP TABLE IF EXISTS resume_data;", nullptr, nullptr, nullptr);
                sqlite3_exec(db_, "DROP TABLE IF EXISTS tasks;", nullptr, nullptr, nullptr);
            }
        }

        const char *sql =
                "CREATE TABLE IF NOT EXISTS tasks ("
                "  client_id   TEXT NOT NULL,"
                "  key_type    INTEGER NOT NULL,"
                "  natural_key TEXT NOT NULL,"
                "  protocol INTEGER,"
                "  name TEXT,"
                "  save_path TEXT,"
                "  filename TEXT,"
                "  url TEXT,"
                "  info_hash TEXT,"
                "  magnet_link TEXT,"
                "  torrent_file TEXT,"
                "  trackers TEXT,"
                "  file_indexes TEXT,"
                "  priority INTEGER,"
                "  status INTEGER,"
                "  progress REAL,"
                "  total_size INTEGER,"
                "  total_done INTEGER,"
                // 服务端 Range 支持：0=不支持（200，单分片全量），1=支持（206，可分片并发/续传）
                "  support_range INTEGER,"
                "  etag TEXT,"
                "  last_modified TEXT,"
                "  created_at INTEGER,"
                "  modified_at INTEGER,"  // 每次 update() 自动刷为 now_unix_ms
                "  source INTEGER DEFAULT 0,"
                "  content_root TEXT,"
                "  PRIMARY KEY (client_id, key_type, natural_key)"
                ");"
                // 按 save_path 查重名/扫本地任务
                "CREATE INDEX IF NOT EXISTS idx_tasks_save_path ON tasks(save_path);"
                // 本地任务按 content_root 反查（仅源本地任务占用）
                "CREATE INDEX IF NOT EXISTS idx_tasks_content_root ON tasks(content_root);"
                "CREATE TABLE IF NOT EXISTS resume_data ("
                "  client_id   TEXT NOT NULL,"
                "  key_type    INTEGER NOT NULL,"
                "  natural_key TEXT NOT NULL,"
                "  data BLOB,"
                "  saved_at INTEGER,"
                "  PRIMARY KEY (client_id, key_type, natural_key)"
                ");"
                // 扁平文件列表：每个文件一行，name 为相对路径（含目录），不再建文件夹节点。
                // 合并原 file_cache 的 downloaded_bytes / play_position_ms 到单行。
                "CREATE TABLE IF NOT EXISTS task_files ("
                "  client_id   TEXT NOT NULL,"
                "  key_type    INTEGER NOT NULL,"
                "  natural_key TEXT NOT NULL,"
                "  file_index INTEGER NOT NULL,"  // libtorrent 索引
                "  name TEXT NOT NULL,"           // 相对路径（含目录）
                "  ext TEXT,"                     // 后缀不含点
                "  size INTEGER NOT NULL,"        // 文件大小
                "  offset INTEGER DEFAULT 0,"     // 文件在 torrent 全局字节流的起始偏移（HTTP 恒 0）
                "  status INTEGER NOT NULL DEFAULT 0,"  // 0=下载中 1=已删除 2=完成
                "  downloaded_bytes INTEGER DEFAULT 0," // 已下载字节数
                "  play_position_ms INTEGER DEFAULT 0," // 播放进度（毫秒）
                "  PRIMARY KEY (client_id, key_type, natural_key, file_index)"
                ");"
                // 已下载连续字节区间快照：任务未加载进引擎时的播放兜底，多行 (复合键, file_index, seg_start)。
                "CREATE TABLE IF NOT EXISTS file_segments ("
                "  client_id   TEXT NOT NULL,"
                "  key_type    INTEGER NOT NULL,"
                "  natural_key TEXT NOT NULL,"
                "  file_index INTEGER NOT NULL,"
                "  seg_start INTEGER NOT NULL,"
                "  seg_end INTEGER NOT NULL,"
                "  PRIMARY KEY (client_id, key_type, natural_key, file_index, seg_start)"
                ");";
        sqlite3_exec(db_, sql, nullptr, nullptr, nullptr);
    }

    std::vector<TaskRecord> TaskStore::load_active(const std::string &client_id) {
        // 载入排队 / 活跃任务（DOWNLOADING=0, QUEUED=4, RESOLVING=5, PARSED=6）；
        // 暂停(1)/完成(2)/错误(3) 留库，按需回读，减小常驻内存。
        std::vector<TaskRecord> out;
        const char *sql =
                "SELECT client_id, key_type, natural_key, protocol, name, save_path, filename, url, info_hash, magnet_link,"
                "       torrent_file, trackers, file_indexes, priority,"
                "       status, progress, total_size, total_done, support_range, etag,"
                "       last_modified, created_at, modified_at, source, content_root FROM tasks"
                "  WHERE client_id=? AND status IN (0,4,5,6) AND source=0;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return out;
        sqlite3_bind_text(st, 1, client_id.c_str(), -1, SQLITE_TRANSIENT);

        while (sqlite3_step(st) == SQLITE_ROW) {
            TaskRecord r;
            fill_record(st, r);
            out.push_back(std::move(r));
        }
        sqlite3_finalize(st);
        return out;
    }

    std::vector<TaskRecord> TaskStore::load_all() {
        // 全量任务（含暂停 / 完成 / 错误），供 dw_list_tasks 快照使用。列序与 fill_record 一致。
        std::vector<TaskRecord> out;
        const char *sql =
                "SELECT client_id, key_type, natural_key, protocol, name, save_path, filename, url, info_hash, magnet_link,"
                "       torrent_file, trackers, file_indexes, priority,"
                "       status, progress, total_size, total_done, support_range, etag,"
                "       last_modified, created_at, modified_at, source, content_root FROM tasks"
                "  ORDER BY created_at DESC;";
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

    namespace {
        /// 按 client_id + key_type + 指定列查询单条任务（列序与 fill_record 一致）。
        /// 命中填充 out 返回 true。col 为库内固定列名（"url" / "info_hash" / "content_root"），
        /// 值走绑定参数，无注入风险。
        bool load_one_by(sqlite3 *db, const std::string &client_id, const TaskKeyType key_type,
                         const char *col, const std::string &value, TaskRecord &out) {
            std::string sql =
                    "SELECT client_id, key_type, natural_key, protocol, name, save_path, filename, url, info_hash, magnet_link,"
                    "       torrent_file, trackers, file_indexes, priority,"
                    "       status, progress, total_size, total_done, support_range, etag,"
                    "       last_modified, created_at, modified_at, source, content_root FROM tasks"
                    " WHERE client_id=? AND key_type=? AND ";
            sql += col;
            sql += "=? LIMIT 1;";
            sqlite3_stmt *st = nullptr;
            if (sqlite3_prepare_v2(db, sql.c_str(), -1, &st, nullptr) != SQLITE_OK) return false;
            sqlite3_bind_text(st, 1, client_id.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(st, 2, static_cast<int>(key_type));
            sqlite3_bind_text(st, 3, value.c_str(), -1, SQLITE_TRANSIENT);
            bool found = false;
            if (sqlite3_step(st) == SQLITE_ROW) {
                fill_record(st, out);
                found = true;
            }
            sqlite3_finalize(st);
            return found;
        }
    } // namespace

    bool TaskStore::load_by_natural_key(const std::string &client_id, const TaskKeyType key_type,
                                       const std::string &natural_key, TaskRecord &out) {
        // key_type 决定 natural_key 落库列：HTTP=url / BT=info_hash / LOCAL=content_root。
        const char *col = (key_type == TaskKeyType::HTTP)
                              ? "url"
                              : (key_type == TaskKeyType::BT) ? "info_hash" : "content_root";
        return load_one_by(db_, client_id, key_type, col, natural_key, out);
    }

    std::vector<TaskRecord> TaskStore::load_tasks_by_save_path(const std::string &save_path) {
        std::vector<TaskRecord> out;
        const char *sql =
                "SELECT client_id, key_type, natural_key, protocol, name, save_path, filename, url, info_hash, magnet_link,"
                "       torrent_file, trackers, file_indexes, priority,"
                "       status, progress, total_size, total_done, support_range, etag,"
                "       last_modified, created_at, modified_at, source, content_root"
                " FROM tasks WHERE save_path=?;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return out;
        sqlite3_bind_text(st, 1, save_path.c_str(), -1, SQLITE_TRANSIENT);
        while (sqlite3_step(st) == SQLITE_ROW) {
            TaskRecord r;
            fill_record(st, r);
            out.push_back(std::move(r));
        }
        sqlite3_finalize(st);
        return out;
    }

    void TaskStore::clear_local_tasks(const std::string &save_path) {
        const char *sql = "DELETE FROM tasks WHERE save_path=? AND source IN (1,2);";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return;
        sqlite3_bind_text(st, 1, save_path.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_step(st);
        sqlite3_finalize(st);
    }

    void TaskStore::clear_tasks_by_source(int source) {
        const char *sql = "DELETE FROM tasks WHERE source=?;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return;
        sqlite3_bind_int(st, 1, source);
        sqlite3_step(st);
        sqlite3_finalize(st);
    }

    void TaskStore::insert(TaskRecord &r) {
        // 新增任务：主键 (client_id, key_type, natural_key) 须预填；natural_key 由 raw_key() 派生。
        // 同一三元组重入则 PK 冲突报错；add 路径按 (client_id, key_type, url/info_hash/content_root) 预判重。
        // modified_at 默认随 created_at（新增即修改）。
        const int64_t now = now_unix_ms();
        if (r.created_at == 0) r.created_at = now;
        if (r.modified_at == 0) r.modified_at = now;
        const char *sql =
                "INSERT INTO tasks (client_id, key_type, natural_key,"
                " protocol, name, save_path, filename, url, info_hash,"
                " magnet_link, torrent_file, trackers, file_indexes,"
                " priority, status, progress, total_size, total_done,"
                " support_range, etag, last_modified, created_at, modified_at,"
                " source, content_root)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return;
    
        const std::string trackers = join_lines(r.trackers);
        const std::string indexes = join_ints(r.file_indexes);
    
        sqlite3_bind_text(st, 1, r.client_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 2, static_cast<int>(r.key_type));
        sqlite3_bind_text(st, 3, r.raw_key().c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 4, r.protocol);
        sqlite3_bind_text(st, 5, r.name.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 6, r.save_path.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 7, r.filename.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 8, r.url.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 9, r.info_hash.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 10, r.magnet_link.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 11, r.torrent_file.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 12, trackers.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 13, indexes.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 14, r.priority);
        sqlite3_bind_int(st, 15, r.status);
        sqlite3_bind_double(st, 16, r.progress);
        sqlite3_bind_int64(st, 17, r.total_size);
        sqlite3_bind_int64(st, 18, r.total_done);
        sqlite3_bind_int(st, 19, r.support_range);
        sqlite3_bind_text(st, 20, r.etag.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 21, r.last_modified.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(st, 22, r.created_at);
        sqlite3_bind_int64(st, 23, r.modified_at);
        sqlite3_bind_int(st, 24, r.source);
        sqlite3_bind_text(st, 25, r.content_root.c_str(), -1, SQLITE_TRANSIENT);
    
        sqlite3_step(st);
        sqlite3_finalize(st);
    }

    void TaskStore::update(const TaskRecord &r) {
        // 更新既有任务：按复合主键 (client_id, key_type, natural_key) 原地 UPDATE 全字段。
        // modified_at 自动刷为 now_unix_ms：调用方无需手动维护；上层若希望冻结时间，可直接读写 r.modified_at
        // （非零值将被保留）。
        const int64_t now = now_unix_ms();
        const int64_t modified_at = r.modified_at != 0 ? r.modified_at : now;
        const char *sql =
                "UPDATE tasks SET protocol=?, name=?, save_path=?,"
                " filename=?, url=?, info_hash=?, magnet_link=?, torrent_file=?, trackers=?, file_indexes=?,"
                " priority=?, status=?, progress=?, total_size=?, total_done=?,"
                " support_range=?, etag=?, last_modified=?, created_at=?, modified_at=?,"
                " source=?, content_root=?"
                " WHERE client_id=? AND key_type=? AND natural_key=?;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return;

        const std::string trackers = join_lines(r.trackers);
        const std::string indexes = join_ints(r.file_indexes);

        sqlite3_bind_int(st, 1, r.protocol);
        sqlite3_bind_text(st, 2, r.name.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 3, r.save_path.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 4, r.filename.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 5, r.url.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 6, r.info_hash.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 7, r.magnet_link.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 8, r.torrent_file.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 9, trackers.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 10, indexes.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 11, r.priority);
        sqlite3_bind_int(st, 12, r.status);
        sqlite3_bind_double(st, 13, r.progress);
        sqlite3_bind_int64(st, 14, r.total_size);
        sqlite3_bind_int64(st, 15, r.total_done);
        sqlite3_bind_int(st, 16, r.support_range);
        sqlite3_bind_text(st, 17, r.etag.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 18, r.last_modified.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(st, 19, r.created_at);
        sqlite3_bind_int64(st, 20, modified_at);
        sqlite3_bind_int(st, 21, r.source);
        sqlite3_bind_text(st, 22, r.content_root.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 23, r.client_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 24, static_cast<int>(r.key_type));
        sqlite3_bind_text(st, 25, r.raw_key().c_str(), -1, SQLITE_TRANSIENT);

        sqlite3_step(st);
        sqlite3_finalize(st);
    }

    void TaskStore::update_status(const std::string &client_id, TaskKeyType key_type,
                                   const std::string &natural_key, int32_t status) {
        const char *sql = "UPDATE tasks SET status=? WHERE client_id=? AND key_type=? AND natural_key=?;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return;
        sqlite3_bind_int(st, 1, status);
        sqlite3_bind_text(st, 2, client_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 3, static_cast<int>(key_type));
        sqlite3_bind_text(st, 4, natural_key.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_step(st);
        sqlite3_finalize(st);
    }

    void TaskStore::remove(const std::string &client_id, TaskKeyType key_type, const std::string &natural_key) {
        // 辅助 lambda：按复合键删除指定表的一行
        auto del_by_key = [this, &client_id, &key_type, &natural_key](const char *table) {
            sqlite3_stmt *st = nullptr;
            const std::string sql = std::string("DELETE FROM ") + table +
                                    " WHERE client_id=? AND key_type=? AND natural_key=?;";
            if (sqlite3_prepare_v2(db_, sql.c_str(), -1, &st, nullptr) == SQLITE_OK) {
                sqlite3_bind_text(st, 1, client_id.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_int(st, 2, static_cast<int>(key_type));
                sqlite3_bind_text(st, 3, natural_key.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_step(st);
                sqlite3_finalize(st);
            }
        };
        del_by_key("tasks");
        del_by_key("resume_data");
        del_by_key("task_files");
        del_by_key("file_segments");
    }

    void TaskStore::save_resume(const std::string &client_id, TaskKeyType key_type,
                                const std::string &natural_key,
                                const uint8_t *data, size_t size) {
        const char *sql =
                "INSERT OR REPLACE INTO resume_data (client_id, key_type, natural_key, data, saved_at)"
                " VALUES (?,?,?,?,?);";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return;
        sqlite3_bind_text(st, 1, client_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 2, static_cast<int>(key_type));
        sqlite3_bind_text(st, 3, natural_key.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_blob(st, 4, data, static_cast<int>(size), SQLITE_TRANSIENT);
        sqlite3_bind_int64(st, 5, now_unix_ms());
        sqlite3_step(st);
        sqlite3_finalize(st);
    }

    std::vector<uint8_t> TaskStore::load_resume(const std::string &client_id, TaskKeyType key_type,
                                              const std::string &natural_key) {
        std::vector<uint8_t> out;
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_,
                "SELECT data FROM resume_data WHERE client_id=? AND key_type=? AND natural_key=?;",
                -1, &st, nullptr) != SQLITE_OK) {
            return out;
        }
        sqlite3_bind_text(st, 1, client_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 2, static_cast<int>(key_type));
        sqlite3_bind_text(st, 3, natural_key.c_str(), -1, SQLITE_TRANSIENT);
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

    void TaskStore::clear_resume(const std::string &client_id, TaskKeyType key_type, const std::string &natural_key) {
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_,
                "DELETE FROM resume_data WHERE client_id=? AND key_type=? AND natural_key=?;",
                -1, &st, nullptr) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, client_id.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(st, 2, static_cast<int>(key_type));
            sqlite3_bind_text(st, 3, natural_key.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_step(st);
            sqlite3_finalize(st);
        }
    }

    // ---- 任务文件信息 ----

    void TaskStore::save_task_files(const std::string &client_id, TaskKeyType key_type,
                                    const std::string &natural_key,
                                    const std::vector<dw_file_info_t> &files) {
        if (files.empty()) return;

        // 先清旧节点再批量写入（全量重建），事务包裹保证原子性。
        sqlite3_exec(db_, "BEGIN;", nullptr, nullptr, nullptr);
        {
            sqlite3_stmt *del = nullptr;
            if (sqlite3_prepare_v2(db_,
                    "DELETE FROM task_files WHERE client_id=? AND key_type=? AND natural_key=?;",
                    -1, &del, nullptr) == SQLITE_OK) {
                sqlite3_bind_text(del, 1, client_id.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_int(del, 2, static_cast<int>(key_type));
                sqlite3_bind_text(del, 3, natural_key.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_step(del);
                sqlite3_finalize(del);
            }
        }

        const char *sql =
                "INSERT OR REPLACE INTO task_files"
                " (client_id, key_type, natural_key, file_index, name, ext, size, offset, status, downloaded_bytes)"
                " VALUES (?,?,?,?,?,?,?,?,?,?);";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) {
            sqlite3_exec(db_, "ROLLBACK;", nullptr, nullptr, nullptr);
            return;
        }

        for (const auto &f: files) {
            sqlite3_reset(st);
            sqlite3_bind_text(st, 1, client_id.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(st, 2, static_cast<int>(key_type));
            sqlite3_bind_text(st, 3, natural_key.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(st, 4, f.index);
            sqlite3_bind_text(st, 5, f.name ? f.name : "", -1, SQLITE_TRANSIENT);
            if (f.ext) {
                sqlite3_bind_text(st, 6, f.ext, -1, SQLITE_TRANSIENT);
            } else {
                sqlite3_bind_null(st, 6);
            }
            sqlite3_bind_int64(st, 7, f.size);
            sqlite3_bind_int64(st, 8, f.offset);
            sqlite3_bind_int(st, 9, f.status);
            sqlite3_bind_int64(st, 10, f.downloaded_bytes);
            sqlite3_step(st);
        }
        sqlite3_finalize(st);
        sqlite3_exec(db_, "COMMIT;", nullptr, nullptr, nullptr);
    }

    std::vector<dw_file_info_t> TaskStore::load_task_files(const std::string &client_id, TaskKeyType key_type,
                                                         const std::string &natural_key) {
        std::vector<dw_file_info_t> out;
        // 按 file_index 升序返回扁平文件列表。
        const char *sql =
                "SELECT file_index, name, ext, size, offset, status, downloaded_bytes FROM task_files"
                " WHERE client_id=? AND key_type=? AND natural_key=? ORDER BY file_index;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return out;

        sqlite3_bind_text(st, 1, client_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 2, static_cast<int>(key_type));
        sqlite3_bind_text(st, 3, natural_key.c_str(), -1, SQLITE_TRANSIENT);
        while (sqlite3_step(st) == SQLITE_ROW) {
            dw_file_info_t f{};
            f.index = sqlite3_column_int(st, 0);
            f.name = dup_col_text(st, 1);
            f.ext = dup_col_text(st, 2);
            f.size = sqlite3_column_int64(st, 3);
            f.offset = sqlite3_column_int64(st, 4);
            f.status = sqlite3_column_int(st, 5);
            f.downloaded_bytes = sqlite3_column_int64(st, 6);
            out.push_back(f);
        }
        sqlite3_finalize(st);
        return out;
    }

    void TaskStore::mark_task_files_completed(const std::string &client_id, TaskKeyType key_type,
                                            const std::string &natural_key) {
        // 任务级 0→2 传播：所有文件节点置完成，不触碰已删除(1)态。
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_,
                "UPDATE task_files SET status=2"
                " WHERE client_id=? AND key_type=? AND natural_key=? AND status<>1;",
                -1, &st, nullptr) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, client_id.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(st, 2, static_cast<int>(key_type));
            sqlite3_bind_text(st, 3, natural_key.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_step(st);
            sqlite3_finalize(st);
        }
    }

    void TaskStore::mark_file_completed(const std::string &client_id, TaskKeyType key_type,
                                      const std::string &natural_key, int32_t file_index) {
        // 单文件 0→2 标记：仅更新下载中态的文件节点，已删除(1) / 已完成(2) 不触碰（幂等）。
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_,
                "UPDATE task_files SET status=2"
                " WHERE client_id=? AND key_type=? AND natural_key=? AND file_index=? AND status=0;",
                -1, &st, nullptr) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, client_id.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(st, 2, static_cast<int>(key_type));
            sqlite3_bind_text(st, 3, natural_key.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(st, 4, file_index);
            sqlite3_step(st);
            sqlite3_finalize(st);
        }
    }

    // ---- 边下边播缓存（已合并到 task_files 表，play_position_ms 字段在同一行）----
    
    void TaskStore::set_play_position(const std::string &client_id, TaskKeyType key_type,
                                    const std::string &natural_key, int32_t file_index, int64_t position_ms) {
        // upsert：仅更新 play_position_ms，不触碰 downloaded_bytes（由引擎独立写）。
        const char *sql =
                "UPDATE task_files SET play_position_ms=?"
                " WHERE client_id=? AND key_type=? AND natural_key=? AND file_index=?;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return;
        sqlite3_bind_int64(st, 1, position_ms);
        sqlite3_bind_text(st, 2, client_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 3, static_cast<int>(key_type));
        sqlite3_bind_text(st, 4, natural_key.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 5, file_index);
        sqlite3_step(st);
        sqlite3_finalize(st);
    }
    
    int64_t TaskStore::get_play_position(const std::string &client_id, TaskKeyType key_type,
                                       const std::string &natural_key, int32_t file_index) {
        int64_t position_ms = 0;
        const char *sql =
                "SELECT play_position_ms FROM task_files"
                " WHERE client_id=? AND key_type=? AND natural_key=? AND file_index=?;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return position_ms;
        sqlite3_bind_text(st, 1, client_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 2, static_cast<int>(key_type));
        sqlite3_bind_text(st, 3, natural_key.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 4, file_index);
        if (sqlite3_step(st) == SQLITE_ROW &&
            sqlite3_column_type(st, 0) != SQLITE_NULL) {
            position_ms = sqlite3_column_int64(st, 0);
        }
        sqlite3_finalize(st);
        return position_ms;
    }
    
    // ---- 已下载区间快照（任务未加载进引擎时的播放兌底）----
    
    void TaskStore::save_segments(const std::string &client_id, TaskKeyType key_type,
                                 const std::string &natural_key, int32_t file_index,
                                 const std::vector<dw_byte_range_t> &segments) {
        // 委托批量接口：单文件区间即只含一个元素的向量
        save_segments_batch(client_id, key_type, natural_key, {{file_index, segments}});
    }
    
    void TaskStore::save_segments_batch(
            const std::string &client_id, TaskKeyType key_type,
            const std::string &natural_key,
            const std::vector<std::pair<int32_t, std::vector<dw_byte_range_t>>> &file_segments) {
        sqlite3_exec(db_, "BEGIN;", nullptr, nullptr, nullptr);
    
        // 删除该任务全部旧区间
        sqlite3_stmt *del = nullptr;
        if (sqlite3_prepare_v2(db_,
                "DELETE FROM file_segments WHERE client_id=? AND key_type=? AND natural_key=?;",
                -1, &del, nullptr) != SQLITE_OK) {
            sqlite3_exec(db_, "ROLLBACK;", nullptr, nullptr, nullptr);
            return;
        }
        sqlite3_bind_text(del, 1, client_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(del, 2, static_cast<int>(key_type));
        sqlite3_bind_text(del, 3, natural_key.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_step(del);
        sqlite3_finalize(del);
    
        if (!file_segments.empty()) {
            const char *sql =
                    "INSERT INTO file_segments (client_id, key_type, natural_key, file_index, seg_start, seg_end)"
                    " VALUES (?,?,?,?,?,?);";
            sqlite3_stmt *st = nullptr;
            if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) {
                sqlite3_exec(db_, "ROLLBACK;", nullptr, nullptr, nullptr);
                return;
            }
            for (const auto &[file_index, segments] : file_segments) {
                for (const auto &seg : segments) {
                    sqlite3_reset(st);
                    sqlite3_bind_text(st, 1, client_id.c_str(), -1, SQLITE_TRANSIENT);
                    sqlite3_bind_int(st, 2, static_cast<int>(key_type));
                    sqlite3_bind_text(st, 3, natural_key.c_str(), -1, SQLITE_TRANSIENT);
                    sqlite3_bind_int(st, 4, file_index);
                    sqlite3_bind_int64(st, 5, seg.start);
                    sqlite3_bind_int64(st, 6, seg.end);
                    sqlite3_step(st);
                }
            }
            sqlite3_finalize(st);
        }
        sqlite3_exec(db_, "COMMIT;", nullptr, nullptr, nullptr);
    }
    
    void TaskStore::update_downloaded_bytes(
            const std::string &client_id, TaskKeyType key_type,
            const std::string &natural_key,
            const std::vector<std::pair<int32_t, int64_t>> &file_bytes) {
        if (file_bytes.empty()) return;
        sqlite3_exec(db_, "BEGIN;", nullptr, nullptr, nullptr);
        const char *sql =
                "UPDATE task_files SET downloaded_bytes=?"
                " WHERE client_id=? AND key_type=? AND natural_key=? AND file_index=?;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) {
            sqlite3_exec(db_, "ROLLBACK;", nullptr, nullptr, nullptr);
            return;
        }
        for (const auto &[file_index, bytes] : file_bytes) {
            sqlite3_reset(st);
            sqlite3_bind_int64(st, 1, bytes);
            sqlite3_bind_text(st, 2, client_id.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(st, 3, static_cast<int>(key_type));
            sqlite3_bind_text(st, 4, natural_key.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(st, 5, file_index);
            sqlite3_step(st);
        }
        sqlite3_finalize(st);
        sqlite3_exec(db_, "COMMIT;", nullptr, nullptr, nullptr);
    }
    
    void TaskStore::upsert_task_file(const std::string &client_id, TaskKeyType key_type,
                                     const std::string &natural_key, int32_t file_index,
                                     int64_t downloaded_bytes, int64_t total_size) {
        // 懒创建 / 进度推送二合一：存在则更新下载量与 size（仅在 size>0 且原值较小时上提），
        // 不存在则插入一行占位（name=''、offset=0、status=0）。元数据（name/ext/offset）
        // 由 save_task_files 全量重写时补齐。
        // 该路径专为运行期按需落地设计，避免一次性写齐全部分片记录。
        const char *sql =
                "INSERT INTO task_files (client_id, key_type, natural_key, file_index, name, size, downloaded_bytes)"
                " VALUES (?,?,?,?,?,?,?)"
                " ON CONFLICT(client_id, key_type, natural_key, file_index) DO UPDATE SET"
                "   downloaded_bytes = MAX(downloaded_bytes, excluded.downloaded_bytes),"
                "   size = MAX(size, excluded.size);";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return;
        sqlite3_bind_text(st, 1, client_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 2, static_cast<int>(key_type));
        sqlite3_bind_text(st, 3, natural_key.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 4, file_index);
        sqlite3_bind_text(st, 5, "", -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(st, 6, total_size > 0 ? total_size : 0);
        sqlite3_bind_int64(st, 7, downloaded_bytes > 0 ? downloaded_bytes : 0);
        sqlite3_step(st);
        sqlite3_finalize(st);
    }
    
    std::vector<dw_byte_range_t> TaskStore::load_segments(const std::string &client_id, TaskKeyType key_type,
                                                        const std::string &natural_key, int32_t file_index) {
        std::vector<dw_byte_range_t> out;
        const char *sql =
                "SELECT seg_start, seg_end FROM file_segments"
                " WHERE client_id=? AND key_type=? AND natural_key=? AND file_index=? ORDER BY seg_start;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return out;
        sqlite3_bind_text(st, 1, client_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 2, static_cast<int>(key_type));
        sqlite3_bind_text(st, 3, natural_key.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 4, file_index);
        while (sqlite3_step(st) == SQLITE_ROW) {
            dw_byte_range_t seg{};
            seg.start = sqlite3_column_int64(st, 0);
            seg.end = sqlite3_column_int64(st, 1);
            out.push_back(seg);
        }
        sqlite3_finalize(st);
        return out;
    }
    
    void TaskStore::clear_segments(const std::string &client_id, TaskKeyType key_type, const std::string &natural_key) {
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_,
                "DELETE FROM file_segments WHERE client_id=? AND key_type=? AND natural_key=?;",
                -1, &st, nullptr) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, client_id.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(st, 2, static_cast<int>(key_type));
            sqlite3_bind_text(st, 3, natural_key.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_step(st);
            sqlite3_finalize(st);
        }
    }
} // namespace dw
