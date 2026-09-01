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
#include <filesystem>

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
            r.client_id = col_text(st, 0);
            r.protocol = static_cast<dw_protocol_t>(sqlite3_column_int(st, 1));
            // 列 2 = natural_key（DB 列保留，内存不存；url/info_hash/content_root 已含同等数据）
            r.protocol = static_cast<dw_protocol_t>(sqlite3_column_int(st, 3));
            r.name = col_text(st, 4);
            r.save_path = col_text(st, 5);
            r.url = col_text(st, 6);
            r.info_hash = col_text(st, 7);
            r.magnet_link = col_text(st, 8);
            r.torrent_file = col_text(st, 9);
            // 列 10 = trackers：不加载到 TaskRecord（数据量大，无持久化意义）。
            r.file_indexes = split_ints(col_text(st, 11));
            r.priority = sqlite3_column_int(st, 12);
            r.status = static_cast<dw_task_status_t>(sqlite3_column_int(st, 13));
            r.progress = sqlite3_column_double(st, 14);
            r.total_size = sqlite3_column_int64(st, 15);
            r.total_done = sqlite3_column_int64(st, 16);
            r.support_range = sqlite3_column_int(st, 17);
            r.etag = col_text(st, 18);
            r.last_modified = col_text(st, 19);
            r.created_at = sqlite3_column_int64(st, 20);
            r.modified_at = sqlite3_column_int64(st, 21);
            r.source = sqlite3_column_int(st, 22);
            r.content_root = col_text(st, 23);
            r.dup_checked = sqlite3_column_int(st, 24) != 0;
            r.trace_id = col_text(st, 25);
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
        // 表结构：复合主键 (client_id, protocol, natural_key) 表示全局唯一。
        // - client_id：App 启动时注入的 UUIDv4。远程同步场景下保留远端 clientId 用于多客户端隔离。
        // - key_type：0=http(url) 1=bt(info_hash) 2=local(content_root)。
        // - natural_key：随 key_type 语义变化，URL / info_hash / content_root 三选一。
        // 同一客户端同 key_type 下 natural_key 冲突时按业务去重（add 路径预判重）。
        // resume_data / task_files / file_segments / file_cache 复制复合键（无 FK，靠应用层保证一致）。
        // 项目未上线：检测到旧 schema（task_id 列存在 / modified_at、dup_checked、trace_id 列缺失）则 DROP 全部表重建。
        sqlite3_stmt *chk = nullptr;
        if (sqlite3_prepare_v2(db_,
                               "SELECT sql FROM sqlite_master WHERE type='table' AND name='tasks';",
                               -1, &chk, nullptr) == SQLITE_OK) {
            bool need_rebuild = false;
            if (sqlite3_step(chk) == SQLITE_ROW) {
                std::string def = col_text(chk, 0);
                // 注：std::string::contains 为 C++23 特性，此处维持 find 判存写法。
                if (def.find("task_id TEXT PRIMARY KEY") != std::string::npos ||
                    def.find("INTEGER PRIMARY KEY AUTOINCREMENT") != std::string::npos ||
                    def.find("modified_at INTEGER") == std::string::npos ||
                    def.find("dup_checked INTEGER") == std::string::npos ||
                    def.find("trace_id TEXT") == std::string::npos ||
                    def.find("is_directory INTEGER") != std::string::npos) {
                    need_rebuild = true;
                }
            }
            sqlite3_finalize(chk);
            if (need_rebuild) {
                sqlite3_exec(db_, "DROP TABLE IF EXISTS file_segments;", nullptr, nullptr, nullptr);
                sqlite3_exec(db_, "DROP TABLE IF EXISTS file_cache;", nullptr, nullptr, nullptr);
                sqlite3_exec(db_, "DROP TABLE IF EXISTS file_records;", nullptr, nullptr, nullptr);
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
                "  modified_at INTEGER," // 每次 update() 自动刷为 now_unix_ms
                "  source INTEGER DEFAULT 0,"
                "  content_root TEXT,"
                // 首次解析重名判定完成标记：1=已判定，PARSED 快路依据（防重复冲突检测误判）
                "  dup_checked INTEGER DEFAULT 0,"
                // 追踪 ID：add 时外部注入值优先，缺省为识别键本身（url / info_hash）；日志关联用
                "  trace_id TEXT,"
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
                // 文件目录表：UI 渲染主表，1 任务 = 1 行。
                // 冗余 status/total_size/total_done 免 JOIN tasks。
                "CREATE TABLE IF NOT EXISTS file_records ("
                "  id              INTEGER PRIMARY KEY AUTOINCREMENT,"
                "  client_id       TEXT NOT NULL,"
                "  type            INTEGER NOT NULL DEFAULT 0," // 0=本地文件 1=任务文件 2=远程文件
                "  is_remote       INTEGER DEFAULT 0,"
                "  save_path       TEXT NOT NULL,"
                "  root_name       TEXT NOT NULL,"
                "  file_type       INTEGER DEFAULT 1," // 0=文件 1=目录
                "  task_protocol   INTEGER," // 关联任务协议（NULL=本地文件）
                "  task_natural_key TEXT," // 关联任务 natural_key
                "  status          INTEGER DEFAULT 0,"
                "  total_size      INTEGER DEFAULT -1,"
                "  total_done      INTEGER DEFAULT 0,"
                "  created_at      INTEGER,"
                "  modified_at     INTEGER"
                ");"
                "CREATE INDEX IF NOT EXISTS idx_file_records_task ON file_records(task_protocol, task_natural_key);"
                "CREATE INDEX IF NOT EXISTS idx_file_records_save_path ON file_records(save_path);"
                // 扁平文件列表：每个文件一行，name 为相对路径（含目录），不再建文件夹节点。
                // physical_path 存完整物理路径（写入时算好），downloaded_bytes 跟踪下载进度。
                "CREATE TABLE IF NOT EXISTS task_files ("
                "  client_id   TEXT NOT NULL,"
                "  key_type    INTEGER NOT NULL,"
                "  natural_key TEXT NOT NULL,"
                "  file_index INTEGER NOT NULL," // libtorrent 索引
                "  physical_path TEXT NOT NULL DEFAULT ''," // 完整物理路径（写入时算好）
                "  name TEXT NOT NULL," // 相对路径（含目录，展示用）
                "  ext TEXT," // 后缀不含点
                "  size INTEGER NOT NULL," // 文件大小
                "  offset INTEGER DEFAULT 0," // 文件在 torrent 全局字节流的起始偏移（HTTP 恒 0）
                "  status INTEGER NOT NULL DEFAULT 0," // 0=下载中 1=已删除 2=完成
                "  downloaded_bytes INTEGER DEFAULT 0," // 已下载字节数
                "  PRIMARY KEY (client_id, key_type, natural_key, file_index)"
                ");"
                // 已下载连续字节区间快照：以物理路径为键，脱离任务生命周期。
                // 任务删除后仍保留，服务于播放兜底。
                "CREATE TABLE IF NOT EXISTS file_segments ("
                "  physical_path TEXT NOT NULL,"
                "  file_index    INTEGER NOT NULL DEFAULT 0,"
                "  seg_start     INTEGER NOT NULL,"
                "  seg_end       INTEGER NOT NULL,"
                "  PRIMARY KEY (physical_path, file_index, seg_start)"
                ");"
                "CREATE INDEX IF NOT EXISTS idx_file_segments_path ON file_segments(physical_path);"
                // 播放进度表：独立于任务生命周期，以物理路径为唯一标识。
                // 任务删除/完成后仍保留，服务于播放列表与进度恢复。
                "CREATE TABLE IF NOT EXISTS play_progress ("
                "  file_path    TEXT NOT NULL PRIMARY KEY,"
                "  position_ms  INTEGER NOT NULL DEFAULT 0,"
                "  duration_ms  INTEGER NOT NULL DEFAULT 0,"
                "  updated_at   INTEGER"
                ");";
        sqlite3_exec(db_, sql, nullptr, nullptr, nullptr);
    }

    std::vector<TaskRecord> TaskStore::load_active(const std::string &client_id) {
        // 载入排队 / 活跃任务（DOWNLOADING=0, QUEUED=4, RESOLVING=5, PARSED=6）；
        // 暂停(1)/完成(2)/错误(3) 留库，按需回读，减小常驻内存。
        std::vector<TaskRecord> out;
        const char *sql =
                "SELECT client_id, key_type, natural_key, protocol, name, save_path, url, info_hash, magnet_link,"
                "       torrent_file, trackers, file_indexes, priority,"
                "       status, progress, total_size, total_done, support_range, etag,"
                "       last_modified, created_at, modified_at, source, content_root, dup_checked, trace_id FROM tasks"
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
                "SELECT client_id, key_type, natural_key, protocol, name, save_path, url, info_hash, magnet_link,"
                "       torrent_file, trackers, file_indexes, priority,"
                "       status, progress, total_size, total_done, support_range, etag,"
                "       last_modified, created_at, modified_at, source, content_root, dup_checked, trace_id FROM tasks"
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
        bool load_one_by(sqlite3 *db, const std::string &client_id, const dw_protocol_t protocol,
                         const char *col, const std::string &value, TaskRecord &out) {
            std::string sql =
                    "SELECT client_id, key_type, natural_key, protocol, name, save_path, url, info_hash, magnet_link,"
                    "       torrent_file, trackers, file_indexes, priority,"
                    "       status, progress, total_size, total_done, support_range, etag,"
                    "       last_modified, created_at, modified_at, source, content_root, dup_checked, trace_id FROM tasks"
                    " WHERE client_id=? AND key_type=? AND ";
            sql += col;
            sql += "=? LIMIT 1;";
            sqlite3_stmt *st = nullptr;
            if (sqlite3_prepare_v2(db, sql.c_str(), -1, &st, nullptr) != SQLITE_OK) return false;
            sqlite3_bind_text(st, 1, client_id.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(st, 2, static_cast<int>(protocol));
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

    bool TaskStore::load_by_natural_key(const std::string &client_id, const dw_protocol_t protocol,
                                        const std::string &natural_key, TaskRecord &out) const {
        const char *col = nullptr;
        switch (protocol) {
            case DW_PROTOCOL_HTTP: col = "url";
                break;
            case DW_PROTOCOL_TORRENT: col = "info_hash";
                break;
            case DW_PROTOCOL_LOCAL: col = "content_root";
                break;
            default: return false;
        }
        return load_one_by(db_, client_id, protocol, col, natural_key, out);
    }

    std::vector<TaskRecord> TaskStore::load_tasks_by_save_path(const std::string &save_path) {
        std::vector<TaskRecord> out;
        const char *sql =
                "SELECT client_id, key_type, natural_key, protocol, name, save_path, url, info_hash, magnet_link,"
                "       torrent_file, trackers, file_indexes, priority,"
                "       status, progress, total_size, total_done, support_range, etag,"
                "       last_modified, created_at, modified_at, source, content_root, dup_checked, trace_id"
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

    void TaskStore::insert(TaskRecord &r) {
        // 新增任务：主键 (client_id, protocol, natural_key) 须预填；natural_key 由 raw_key() 派生。
        // 同一三元组重入则 PK 冲突报错；add 路径按 (client_id, protocol, url/info_hash/content_root) 预判重。
        // modified_at 默认随 created_at（新增即修改）。
        const int64_t now = now_unix_ms();
        if (r.created_at == 0) r.created_at = now;
        if (r.modified_at == 0) r.modified_at = now;
        const char *sql =
                "INSERT INTO tasks (client_id, key_type, natural_key,"
                " protocol, name, save_path, url, info_hash,"
                " magnet_link, torrent_file, trackers, file_indexes,"
                " priority, status, progress, total_size, total_done,"
                " support_range, etag, last_modified, created_at, modified_at,"
                " source, content_root, dup_checked, trace_id)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return;

        const std::string indexes = join_ints(r.file_indexes);

        sqlite3_bind_text(st, 1, r.client_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 2, static_cast<int>(r.protocol));
        sqlite3_bind_text(st, 3, r.raw_key().c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 4, r.protocol);
        sqlite3_bind_text(st, 5, r.name.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 6, r.save_path.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 7, r.url.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 8, r.info_hash.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 9, r.magnet_link.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 10, r.torrent_file.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 11, "", -1, SQLITE_TRANSIENT); // trackers：不持久化
        sqlite3_bind_text(st, 12, indexes.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 13, r.priority);
        sqlite3_bind_int(st, 14, r.status);
        sqlite3_bind_double(st, 15, r.progress);
        sqlite3_bind_int64(st, 16, r.total_size);
        sqlite3_bind_int64(st, 17, r.total_done);
        sqlite3_bind_int(st, 18, r.support_range);
        sqlite3_bind_text(st, 19, r.etag.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 20, r.last_modified.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(st, 21, r.created_at);
        sqlite3_bind_int64(st, 22, r.modified_at);
        sqlite3_bind_int(st, 23, r.source);
        sqlite3_bind_text(st, 24, r.content_root.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 25, r.dup_checked ? 1 : 0);
        sqlite3_bind_text(st, 26, r.trace_id.c_str(), -1, SQLITE_TRANSIENT);

        sqlite3_step(st);
        sqlite3_finalize(st);
    }

    void TaskStore::update(const TaskRecord &r) {
        // 更新既有任务：按复合主键 (client_id, protocol, natural_key) 原地 UPDATE 全字段。
        // modified_at 自动刷为 now_unix_ms：调用方无需手动维护；上层若希望冻结时间，可直接读写 r.modified_at
        // （非零值将被保留）。
        const int64_t now = now_unix_ms();
        const int64_t modified_at = r.modified_at != 0 ? r.modified_at : now;
        const char *sql =
                "UPDATE tasks SET protocol=?, name=?, save_path=?,"
                " url=?, info_hash=?, magnet_link=?, torrent_file=?, trackers=?, file_indexes=?,"
                " priority=?, status=?, progress=?, total_size=?, total_done=?,"
                " support_range=?, etag=?, last_modified=?, created_at=?, modified_at=?,"
                " source=?, content_root=?, dup_checked=?, trace_id=?"
                " WHERE client_id=? AND key_type=? AND natural_key=?;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return;

        const std::string indexes = join_ints(r.file_indexes);

        sqlite3_bind_int(st, 1, r.protocol);
        sqlite3_bind_text(st, 2, r.name.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 3, r.save_path.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 4, r.url.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 5, r.info_hash.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 6, r.magnet_link.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 7, r.torrent_file.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 8, "", -1, SQLITE_TRANSIENT); // trackers：不持久化
        sqlite3_bind_text(st, 9, indexes.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 10, r.priority);
        sqlite3_bind_int(st, 11, r.status);
        sqlite3_bind_double(st, 12, r.progress);
        sqlite3_bind_int64(st, 13, r.total_size);
        sqlite3_bind_int64(st, 14, r.total_done);
        sqlite3_bind_int(st, 15, r.support_range);
        sqlite3_bind_text(st, 16, r.etag.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 17, r.last_modified.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(st, 18, r.created_at);
        sqlite3_bind_int64(st, 19, modified_at);
        sqlite3_bind_int(st, 20, r.source);
        sqlite3_bind_text(st, 21, r.content_root.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 22, r.dup_checked ? 1 : 0);
        sqlite3_bind_text(st, 23, r.trace_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 24, r.client_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 25, static_cast<int>(r.protocol));
        sqlite3_bind_text(st, 26, r.raw_key().c_str(), -1, SQLITE_TRANSIENT);

        sqlite3_step(st);
        sqlite3_finalize(st);
    }

    void TaskStore::update_status(const std::string &client_id, dw_protocol_t protocol,
                                  const std::string &natural_key, int32_t status) {
        const char *sql = "UPDATE tasks SET status=? WHERE client_id=? AND key_type=? AND natural_key=?;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return;
        sqlite3_bind_int(st, 1, status);
        sqlite3_bind_text(st, 2, client_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 3, static_cast<int>(protocol));
        sqlite3_bind_text(st, 4, natural_key.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_step(st);
        sqlite3_finalize(st);
    }

    void TaskStore::remove(const std::string &client_id, dw_protocol_t protocol, const std::string &natural_key) {
        // 辅助 lambda：按复合键删除指定表的一行
        auto del_by_key = [this, &client_id, &protocol, &natural_key](const char *table) {
            sqlite3_stmt *st = nullptr;
            const std::string sql = std::string("DELETE FROM ") + table +
                                    " WHERE client_id=? AND key_type=? AND natural_key=?;";
            if (sqlite3_prepare_v2(db_, sql.c_str(), -1, &st, nullptr) == SQLITE_OK) {
                sqlite3_bind_text(st, 1, client_id.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_int(st, 2, static_cast<int>(protocol));
                sqlite3_bind_text(st, 3, natural_key.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_step(st);
                sqlite3_finalize(st);
            }
        };
        del_by_key("tasks");
        del_by_key("resume_data");
        del_by_key("task_files");
        // file_records 按任务关联键删除
        {
            sqlite3_stmt *st = nullptr;
            if (sqlite3_prepare_v2(db_,
                                   "DELETE FROM file_records WHERE task_protocol=? AND task_natural_key=?;",
                                   -1, &st, nullptr) == SQLITE_OK) {
                sqlite3_bind_int(st, 1, static_cast<int>(protocol));
                sqlite3_bind_text(st, 2, natural_key.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_step(st);
                sqlite3_finalize(st);
            }
        }
    }

    void TaskStore::save_resume(const std::string &client_id, dw_protocol_t protocol,
                                const std::string &natural_key,
                                const uint8_t *data, size_t size) {
        const char *sql =
                "INSERT OR REPLACE INTO resume_data (client_id, key_type, natural_key, data, saved_at)"
                " VALUES (?,?,?,?,?);";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return;
        sqlite3_bind_text(st, 1, client_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 2, static_cast<int>(protocol));
        sqlite3_bind_text(st, 3, natural_key.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_blob(st, 4, data, static_cast<int>(size), SQLITE_TRANSIENT);
        sqlite3_bind_int64(st, 5, now_unix_ms());
        sqlite3_step(st);
        sqlite3_finalize(st);
    }

    std::vector<uint8_t> TaskStore::load_resume(const std::string &client_id, dw_protocol_t protocol,
                                                const std::string &natural_key) {
        std::vector<uint8_t> out;
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_,
                               "SELECT data FROM resume_data WHERE client_id=? AND key_type=? AND natural_key=?;",
                               -1, &st, nullptr) != SQLITE_OK) {
            return out;
        }
        sqlite3_bind_text(st, 1, client_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 2, static_cast<int>(protocol));
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

    void TaskStore::clear_resume(const std::string &client_id, dw_protocol_t protocol, const std::string &natural_key) {
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_,
                               "DELETE FROM resume_data WHERE client_id=? AND key_type=? AND natural_key=?;",
                               -1, &st, nullptr) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, client_id.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(st, 2, static_cast<int>(protocol));
            sqlite3_bind_text(st, 3, natural_key.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_step(st);
            sqlite3_finalize(st);
        }
    }

    // ---- 文件目录表（file_records）----

    std::string TaskStore::load_file_physical_path(const std::string &client_id, dw_protocol_t protocol,
                                                    const std::string &natural_key, int32_t file_index) const {
        const char *sql =
                "SELECT physical_path FROM task_files"
                " WHERE client_id=? AND key_type=? AND natural_key=? AND file_index=? LIMIT 1;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return {};
        sqlite3_bind_text(st, 1, client_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 2, static_cast<int>(protocol));
        sqlite3_bind_text(st, 3, natural_key.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 4, file_index);
        std::string result;
        if (sqlite3_step(st) == SQLITE_ROW) {
            result = col_text(st, 0);
        }
        sqlite3_finalize(st);
        return result;
    }

    namespace {
        /// 从查询行填充 FileRecord（列序须与 SELECT 一致）。
        void fill_file_record(sqlite3_stmt *st, FileRecord &r) {
            r.id = sqlite3_column_int64(st, 0);
            r.client_id = col_text(st, 1);
            r.type = sqlite3_column_int(st, 2);
            r.is_remote = sqlite3_column_int(st, 3) != 0;
            r.save_path = col_text(st, 4);
            r.root_name = col_text(st, 5);
            r.file_type = sqlite3_column_int(st, 6) != 0;
            r.task_protocol = static_cast<dw_protocol_t>(sqlite3_column_int(st, 7));
            r.task_natural_key = col_text(st, 8);
            r.status = sqlite3_column_int(st, 9);
            r.total_size = sqlite3_column_int64(st, 10);
            r.total_done = sqlite3_column_int64(st, 11);
            r.created_at = sqlite3_column_int64(st, 12);
            r.modified_at = sqlite3_column_int64(st, 13);
        }
    } // namespace

    void TaskStore::insert_file_record(FileRecord &r) {
        const int64_t now = now_unix_ms();
        if (r.created_at == 0) r.created_at = now;
        if (r.modified_at == 0) r.modified_at = now;
        const char *sql =
                "INSERT INTO file_records (client_id, type, is_remote, save_path, root_name,"
                " file_type, task_protocol, task_natural_key, status, total_size, total_done,"
                " created_at, modified_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?);";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return;
        sqlite3_bind_text(st, 1, r.client_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 2, r.type);
        sqlite3_bind_int(st, 3, r.is_remote ? 1 : 0);
        sqlite3_bind_text(st, 4, r.save_path.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 5, r.root_name.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 6, r.file_type ? 1 : 0);
        if (r.has_task()) {
            sqlite3_bind_int(st, 7, static_cast<int>(r.task_protocol));
            sqlite3_bind_text(st, 8, r.task_natural_key.c_str(), -1, SQLITE_TRANSIENT);
        } else {
            sqlite3_bind_null(st, 7);
            sqlite3_bind_null(st, 8);
        }
        sqlite3_bind_int(st, 9, r.status);
        sqlite3_bind_int64(st, 10, r.total_size);
        sqlite3_bind_int64(st, 11, r.total_done);
        sqlite3_bind_int64(st, 12, r.created_at);
        sqlite3_bind_int64(st, 13, r.modified_at);
        sqlite3_step(st);
        sqlite3_finalize(st);
        // 回填自增 id
        r.id = sqlite3_last_insert_rowid(db_);
    }

    std::vector<FileRecord> TaskStore::load_file_records(const std::string &client_id) {
        std::vector<FileRecord> out;
        const char *sql =
                "SELECT id, client_id, type, is_remote, save_path, root_name, file_type,"
                " task_protocol, task_natural_key, status, total_size, total_done,"
                " created_at, modified_at FROM file_records"
                " WHERE client_id=? ORDER BY modified_at DESC;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return out;
        sqlite3_bind_text(st, 1, client_id.c_str(), -1, SQLITE_TRANSIENT);
        while (sqlite3_step(st) == SQLITE_ROW) {
            FileRecord r;
            fill_file_record(st, r);
            out.push_back(std::move(r));
        }
        sqlite3_finalize(st);
        return out;
    }

    void TaskStore::sync_file_record_progress(dw_protocol_t task_protocol, const std::string &task_natural_key,
                                              int32_t status, int64_t total_size, int64_t total_done) {
        const char *sql =
                "UPDATE file_records SET status=?, total_size=?, total_done=?, modified_at=?"
                " WHERE task_protocol=? AND task_natural_key=?;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return;
        sqlite3_bind_int(st, 1, status);
        sqlite3_bind_int64(st, 2, total_size);
        sqlite3_bind_int64(st, 3, total_done);
        sqlite3_bind_int64(st, 4, now_unix_ms());
        sqlite3_bind_int(st, 5, static_cast<int>(task_protocol));
        sqlite3_bind_text(st, 6, task_natural_key.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_step(st);
        sqlite3_finalize(st);
    }

    void TaskStore::update_file_record_meta(dw_protocol_t task_protocol, const std::string &task_natural_key,
                                            const std::string &root_name, bool file_type) {
        const char *sql =
                "UPDATE file_records SET root_name=?, file_type=?, modified_at=?"
                " WHERE task_protocol=? AND task_natural_key=?;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return;
        sqlite3_bind_text(st, 1, root_name.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 2, file_type ? 1 : 0);
        sqlite3_bind_int64(st, 3, now_unix_ms());
        sqlite3_bind_int(st, 4, static_cast<int>(task_protocol));
        sqlite3_bind_text(st, 5, task_natural_key.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_step(st);
        sqlite3_finalize(st);
    }

    // ---- 任务文件信息 ----

    void TaskStore::save_task_files(const std::string &client_id, dw_protocol_t protocol,
                                    const std::string &natural_key,
                                    const std::vector<dw_file_info_t> &files,
                                    const std::string &physical_path_prefix) {
        if (files.empty()) return;

        // 先清旧节点再批量写入（全量重建），事务包裹保证原子性。
        sqlite3_exec(db_, "BEGIN;", nullptr, nullptr, nullptr);
        {
            sqlite3_stmt *del = nullptr;
            if (sqlite3_prepare_v2(db_,
                                   "DELETE FROM task_files WHERE client_id=? AND key_type=? AND natural_key=?;",
                                   -1, &del, nullptr) == SQLITE_OK) {
                sqlite3_bind_text(del, 1, client_id.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_int(del, 2, static_cast<int>(protocol));
                sqlite3_bind_text(del, 3, natural_key.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_step(del);
                sqlite3_finalize(del);
            }
        }

        const char *sql =
                "INSERT OR REPLACE INTO task_files"
                " (client_id, key_type, natural_key, file_index, physical_path, name, ext, size, offset, status, downloaded_bytes)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?);";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) {
            sqlite3_exec(db_, "ROLLBACK;", nullptr, nullptr, nullptr);
            return;
        }

        for (const auto &f: files) {
            const std::string fname = f.name ? f.name : "";
            // 防御路径穿越：拒绝含 ".." 的路径段（引擎正常上报不应出现）。
            // 该路径将落库并用于后续文件 I/O，宁可缺一条记录也不放行越界路径。
            bool traversal = false;
            for (const auto &seg: std::filesystem::path(fname)) {
                if (seg == "..") {
                    traversal = true;
                    break;
                }
            }
            if (traversal) {
                log_e(natural_key.c_str(), "跳过含非法路径段的文件 name=%s", fname.c_str());
                continue;
            }
            sqlite3_reset(st);
            sqlite3_bind_text(st, 1, client_id.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(st, 2, static_cast<int>(protocol));
            sqlite3_bind_text(st, 3, natural_key.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(st, 4, f.index);
            // physical_path = prefix / name（经 filesystem 拼接，自动补分隔符）
            const std::string full_path = (std::filesystem::path(physical_path_prefix) / fname).string();
            sqlite3_bind_text(st, 5, full_path.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(st, 6, fname.c_str(), -1, SQLITE_TRANSIENT);
            if (f.ext) {
                sqlite3_bind_text(st, 7, f.ext, -1, SQLITE_TRANSIENT);
            } else {
                sqlite3_bind_null(st, 7);
            }
            sqlite3_bind_int64(st, 8, f.size);
            sqlite3_bind_int64(st, 9, f.offset);
            sqlite3_bind_int(st, 10, f.status);
            sqlite3_bind_int64(st, 11, f.downloaded_bytes);
            sqlite3_step(st);
        }
        sqlite3_finalize(st);
        sqlite3_exec(db_, "COMMIT;", nullptr, nullptr, nullptr);
    }

    std::vector<dw_file_info_t> TaskStore::load_task_files(const std::string &client_id, dw_protocol_t protocol,
                                                           const std::string &natural_key) {
        std::vector<dw_file_info_t> out;
        // 按 file_index 升序返回扁平文件列表。
        const char *sql =
                "SELECT file_index, physical_path, name, ext, size, offset, status, downloaded_bytes FROM task_files"
                " WHERE client_id=? AND key_type=? AND natural_key=? ORDER BY file_index;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return out;

        sqlite3_bind_text(st, 1, client_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 2, static_cast<int>(protocol));
        sqlite3_bind_text(st, 3, natural_key.c_str(), -1, SQLITE_TRANSIENT);
        while (sqlite3_step(st) == SQLITE_ROW) {
            dw_file_info_t f{};
            f.index = sqlite3_column_int(st, 0);
            f.physical_path = dup_col_text(st, 1);
            f.name = dup_col_text(st, 2);
            f.ext = dup_col_text(st, 3);
            f.size = sqlite3_column_int64(st, 4);
            f.offset = sqlite3_column_int64(st, 5);
            f.status = sqlite3_column_int(st, 6);
            f.downloaded_bytes = sqlite3_column_int64(st, 7);
            out.push_back(f);
        }
        sqlite3_finalize(st);
        return out;
    }

    void TaskStore::mark_task_files_completed(const std::string &client_id, dw_protocol_t protocol,
                                              const std::string &natural_key) {
        // 任务级 0→2 传播：所有文件节点置完成，不触碰已删除(1)态。
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_,
                               "UPDATE task_files SET status=2"
                               " WHERE client_id=? AND key_type=? AND natural_key=? AND status<>1;",
                               -1, &st, nullptr) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, client_id.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(st, 2, static_cast<int>(protocol));
            sqlite3_bind_text(st, 3, natural_key.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_step(st);
            sqlite3_finalize(st);
        }
    }

    void TaskStore::mark_file_completed(const std::string &client_id, dw_protocol_t protocol,
                                        const std::string &natural_key, int32_t file_index) {
        // 单文件 0→2 标记：仅更新下载中态的文件节点，已删除(1) / 已完成(2) 不触碰（幂等）。
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_,
                               "UPDATE task_files SET status=2"
                               " WHERE client_id=? AND key_type=? AND natural_key=? AND file_index=? AND status=0;",
                               -1, &st, nullptr) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, client_id.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(st, 2, static_cast<int>(protocol));
            sqlite3_bind_text(st, 3, natural_key.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(st, 4, file_index);
            sqlite3_step(st);
            sqlite3_finalize(st);
        }
    }

    // ---- 播放进度（独立表 play_progress，以物理路径为键）----

    void TaskStore::set_play_position(const std::string &client_id, dw_protocol_t protocol,
                                      const std::string &natural_key, int32_t file_index, int64_t position_ms) {
        std::string file_path = load_file_physical_path(client_id, protocol, natural_key, file_index);
        if (file_path.empty()) return;

        const char *sql =
                "INSERT INTO play_progress (file_path, position_ms, updated_at)"
                " VALUES (?, ?, strftime('%s','now') * 1000)"
                " ON CONFLICT(file_path) DO UPDATE SET position_ms=excluded.position_ms, updated_at=excluded.updated_at;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return;
        sqlite3_bind_text(st, 1, file_path.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(st, 2, position_ms);
        sqlite3_step(st);
        sqlite3_finalize(st);
    }

    int64_t TaskStore::get_play_position(const std::string &client_id, dw_protocol_t protocol,
                                         const std::string &natural_key, int32_t file_index) {
        std::string file_path = load_file_physical_path(client_id, protocol, natural_key, file_index);
        if (file_path.empty()) return 0;

        int64_t position_ms = 0;
        const char *sql =
                "SELECT position_ms FROM play_progress WHERE file_path=?;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return 0;
        sqlite3_bind_text(st, 1, file_path.c_str(), -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW &&
            sqlite3_column_type(st, 0) != SQLITE_NULL) {
            position_ms = sqlite3_column_int64(st, 0);
        }
        sqlite3_finalize(st);
        return position_ms;
    }

    // ---- 已下载区间快照（以物理路径为键）----

    void TaskStore::save_segments_batch(
        const std::vector<std::tuple<std::string, int32_t, std::vector<dw_byte_range_t>>> &file_segments) {
        sqlite3_exec(db_, "BEGIN;", nullptr, nullptr, nullptr);

        if (!file_segments.empty()) {
            // 先删除涉及的文件旧区间
            sqlite3_stmt *del = nullptr;
            if (sqlite3_prepare_v2(db_,
                                   "DELETE FROM file_segments WHERE physical_path=? AND file_index=?;",
                                   -1, &del, nullptr) != SQLITE_OK) {
                sqlite3_exec(db_, "ROLLBACK;", nullptr, nullptr, nullptr);
                return;
            }
            for (const auto &[path, idx, segs] : file_segments) {
                sqlite3_reset(del);
                sqlite3_bind_text(del, 1, path.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_int(del, 2, idx);
                sqlite3_step(del);
            }
            sqlite3_finalize(del);

            const char *sql =
                    "INSERT INTO file_segments (physical_path, file_index, seg_start, seg_end)"
                    " VALUES (?,?,?,?);";
            sqlite3_stmt *st = nullptr;
            if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) {
                sqlite3_exec(db_, "ROLLBACK;", nullptr, nullptr, nullptr);
                return;
            }
            for (const auto &[path, idx, segs] : file_segments) {
                for (const auto &seg: segs) {
                    sqlite3_reset(st);
                    sqlite3_bind_text(st, 1, path.c_str(), -1, SQLITE_TRANSIENT);
                    sqlite3_bind_int(st, 2, idx);
                    sqlite3_bind_int64(st, 3, seg.start);
                    sqlite3_bind_int64(st, 4, seg.end);
                    sqlite3_step(st);
                }
            }
            sqlite3_finalize(st);
        }
        sqlite3_exec(db_, "COMMIT;", nullptr, nullptr, nullptr);
    }

    std::vector<dw_byte_range_t> TaskStore::load_segments(const std::string &physical_path, int32_t file_index) {
        std::vector<dw_byte_range_t> out;
        const char *sql =
                "SELECT seg_start, seg_end FROM file_segments"
                " WHERE physical_path=? AND file_index=? ORDER BY seg_start;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return out;
        sqlite3_bind_text(st, 1, physical_path.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 2, file_index);
        while (sqlite3_step(st) == SQLITE_ROW) {
            dw_byte_range_t seg{};
            seg.start = sqlite3_column_int64(st, 0);
            seg.end = sqlite3_column_int64(st, 1);
            out.push_back(seg);
        }
        sqlite3_finalize(st);
        return out;
    }

    void TaskStore::update_downloaded_bytes(
        const std::string &client_id, dw_protocol_t protocol,
        const std::string &natural_key,
        const std::vector<std::pair<int32_t, int64_t> > &file_bytes) {
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
        for (const auto &[file_index, bytes]: file_bytes) {
            sqlite3_reset(st);
            sqlite3_bind_int64(st, 1, bytes);
            sqlite3_bind_text(st, 2, client_id.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(st, 3, static_cast<int>(protocol));
            sqlite3_bind_text(st, 4, natural_key.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(st, 5, file_index);
            sqlite3_step(st);
        }
        sqlite3_finalize(st);
        sqlite3_exec(db_, "COMMIT;", nullptr, nullptr, nullptr);
    }

    void TaskStore::upsert_task_file(const std::string &client_id, dw_protocol_t protocol,
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
        sqlite3_bind_int(st, 2, static_cast<int>(protocol));
        sqlite3_bind_text(st, 3, natural_key.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 4, file_index);
        sqlite3_bind_text(st, 5, "", -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(st, 6, total_size > 0 ? total_size : 0);
        sqlite3_bind_int64(st, 7, downloaded_bytes > 0 ? downloaded_bytes : 0);
        sqlite3_step(st);
        sqlite3_finalize(st);
    }

} // namespace dw
