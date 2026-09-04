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
            r.is_directory = sqlite3_column_int(st, 26) != 0;
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
        // resume_data / file_progress_cache / file_cache 复制复合键（无 FK，靠应用层保证一致）。
        // 项目未上线：检测到旧 schema（tasks 的 task_id 列存在 / modified_at、dup_checked、
        // trace_id、is_directory 列缺失，或 file_records 缺 full_path 列）则 DROP 全部表重建。
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
                    def.find("is_directory INTEGER") == std::string::npos) {
                    need_rebuild = true;
                }
            }
            sqlite3_finalize(chk);
            // file_records 旧 schema 检测：full_path 列缺失则重建（与 tasks 同款策略）。
            sqlite3_stmt *chk_fr = nullptr;
            if (sqlite3_prepare_v2(db_,
                                   "SELECT sql FROM sqlite_master WHERE type='table' AND name='file_records';",
                                   -1, &chk_fr, nullptr) == SQLITE_OK) {
                if (sqlite3_step(chk_fr) == SQLITE_ROW &&
                    col_text(chk_fr, 0).find("full_path TEXT") == std::string::npos) {
                    need_rebuild = true;
                }
                sqlite3_finalize(chk_fr);
            }
            if (need_rebuild) {
                sqlite3_exec(db_, "DROP TABLE IF EXISTS file_segments;", nullptr, nullptr, nullptr);
                sqlite3_exec(db_, "DROP TABLE IF EXISTS file_progress_cache;", nullptr, nullptr, nullptr);
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
                // 磁盘根实体形态：1=目录（包装/多根） 0=单文件；BT 首次 PARSED 落定，HTTP 恒 0
                "  is_directory INTEGER DEFAULT 1,"
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
                "  full_path       TEXT," // 磁盘根实体全路径（save_path/root_name）；占位 NULL，PARSED 回填
                "  file_type       INTEGER DEFAULT 1," // 0=文件 1=目录
                "  ext             TEXT," // 文件后缀不含点（如 "mp4"）；目录为 NULL
                "  key_type        INTEGER," // 关联任务协议（NULL=本地文件）
                "  natural_key     TEXT," // 关联任务 natural_key
                "  status          INTEGER DEFAULT 0,"
                "  total_size      INTEGER DEFAULT -1,"
                "  total_done      INTEGER DEFAULT 0,"
                "  created_at      INTEGER,"
                "  modified_at     INTEGER"
                ");"
                "CREATE INDEX IF NOT EXISTS idx_file_records_task ON file_records(key_type, natural_key);"
                "CREATE INDEX IF NOT EXISTS idx_file_records_save_path ON file_records(save_path);"
                // 文件下载进度缓存：已下载连续字节区间（闭区间），engine 按三要素 + file_index
                // 维护，App 按物理路径关联；文件完成即删（区别于旧 file_segments 的持久保留语义）。
                "CREATE TABLE IF NOT EXISTS file_progress_cache ("
                "  client_id     TEXT NOT NULL,"
                "  key_type      INTEGER NOT NULL,"
                "  natural_key   TEXT NOT NULL,"
                "  file_index    INTEGER NOT NULL,"
                "  physical_path TEXT NOT NULL,"
                "  offset_start  INTEGER NOT NULL,"
                "  offset_end    INTEGER NOT NULL,"
                "  modified_at   INTEGER,"
                "  PRIMARY KEY (client_id, key_type, natural_key, file_index, offset_start)"
                ");"
                "CREATE INDEX IF NOT EXISTS idx_fpc_path ON file_progress_cache(physical_path);"
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
                "       last_modified, created_at, modified_at, source, content_root, dup_checked, trace_id, is_directory FROM tasks"
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
                "       last_modified, created_at, modified_at, source, content_root, dup_checked, trace_id, is_directory FROM tasks"
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
                    "       last_modified, created_at, modified_at, source, content_root, dup_checked, trace_id, is_directory FROM tasks"
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
                "       last_modified, created_at, modified_at, source, content_root, dup_checked, trace_id, is_directory"
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
                " source, content_root, dup_checked, trace_id, is_directory)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
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
        sqlite3_bind_int(st, 27, r.is_directory ? 1 : 0);

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
                " source=?, content_root=?, dup_checked=?, trace_id=?, is_directory=?"
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
        sqlite3_bind_int(st, 24, r.is_directory ? 1 : 0);
        sqlite3_bind_text(st, 25, r.client_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 26, static_cast<int>(r.protocol));
        sqlite3_bind_text(st, 27, r.raw_key().c_str(), -1, SQLITE_TRANSIENT);

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
        // 进度缓存随任务级联删除（区别于旧 file_segments 持久保留语义：任务删除后文件
        // 是否留存由调用方决定，缓存区间不再有消费方）。
        del_by_key("file_progress_cache");
        // file_records 按任务关联三要素删除（含 client_id，多客户端共库时避免误删）
        del_by_key("file_records");
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

    namespace {
        /// 从查询行填充 FileRecord（列序须与 SELECT 一致）。
        void fill_file_record(sqlite3_stmt *st, FileRecord &r) {
            r.id = sqlite3_column_int64(st, 0);
            r.client_id = col_text(st, 1);
            r.type = sqlite3_column_int(st, 2);
            r.is_remote = sqlite3_column_int(st, 3) != 0;
            r.save_path = col_text(st, 4);
            r.root_name = col_text(st, 5);
            r.full_path = col_text(st, 6);
            r.file_type = sqlite3_column_int(st, 7) != 0;
            r.ext = col_text(st, 8);
            r.task_protocol = static_cast<dw_protocol_t>(sqlite3_column_int(st, 9));
            r.task_natural_key = col_text(st, 10);
            r.status = sqlite3_column_int(st, 11);
            r.total_size = sqlite3_column_int64(st, 12);
            r.total_done = sqlite3_column_int64(st, 13);
            r.created_at = sqlite3_column_int64(st, 14);
            r.modified_at = sqlite3_column_int64(st, 15);
        }
    } // namespace

    bool TaskStore::has_file_record(const std::string &client_id, dw_protocol_t task_protocol,
                                    const std::string &task_natural_key) const {
        sqlite3_stmt *st = nullptr;
        bool exists = false;
        if (sqlite3_prepare_v2(db_,
                               "SELECT 1 FROM file_records"
                               " WHERE client_id=? AND key_type=? AND natural_key=? LIMIT 1;",
                               -1, &st, nullptr) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, client_id.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(st, 2, static_cast<int>(task_protocol));
            sqlite3_bind_text(st, 3, task_natural_key.c_str(), -1, SQLITE_TRANSIENT);
            exists = sqlite3_step(st) == SQLITE_ROW;
            sqlite3_finalize(st);
        }
        return exists;
    }

    void TaskStore::insert_file_record(FileRecord &r) {
        const int64_t now = now_unix_ms();
        if (r.created_at == 0) r.created_at = now;
        if (r.modified_at == 0) r.modified_at = now;
        const char *sql =
                "INSERT INTO file_records (client_id, type, is_remote, save_path, root_name,"
                " full_path, file_type, ext, key_type, natural_key, status, total_size, total_done,"
                " created_at, modified_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return;
        sqlite3_bind_text(st, 1, r.client_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 2, r.type);
        sqlite3_bind_int(st, 3, r.is_remote ? 1 : 0);
        sqlite3_bind_text(st, 4, r.save_path.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 5, r.root_name.c_str(), -1, SQLITE_TRANSIENT);
        // full_path 列：空串按 NULL 入库（占位阶段未定名，与 ext 同款约定）。
        if (r.full_path.empty()) {
            sqlite3_bind_null(st, 6);
        } else {
            sqlite3_bind_text(st, 6, r.full_path.c_str(), -1, SQLITE_TRANSIENT);
        }
        sqlite3_bind_int(st, 7, r.file_type ? 1 : 0);
        // ext 列：空串按 NULL 入库（目录场景无后缀，各 ext 列统一约定）。
        if (r.ext.empty()) {
            sqlite3_bind_null(st, 8);
        } else {
            sqlite3_bind_text(st, 8, r.ext.c_str(), -1, SQLITE_TRANSIENT);
        }
        if (r.has_task()) {
            sqlite3_bind_int(st, 9, static_cast<int>(r.task_protocol));
            sqlite3_bind_text(st, 10, r.task_natural_key.c_str(), -1, SQLITE_TRANSIENT);
        } else {
            sqlite3_bind_null(st, 9);
            sqlite3_bind_null(st, 10);
        }
        sqlite3_bind_int(st, 11, r.status);
        sqlite3_bind_int64(st, 12, r.total_size);
        sqlite3_bind_int64(st, 13, r.total_done);
        sqlite3_bind_int64(st, 14, r.created_at);
        sqlite3_bind_int64(st, 15, r.modified_at);
        sqlite3_step(st);
        sqlite3_finalize(st);
        // 回填自增 id
        r.id = sqlite3_last_insert_rowid(db_);
    }

    std::vector<FileRecord> TaskStore::load_file_records(const std::string &client_id) {
        std::vector<FileRecord> out;
        const char *sql =
                "SELECT id, client_id, type, is_remote, save_path, root_name, full_path, file_type, ext,"
                " key_type, natural_key, status, total_size, total_done,"
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
                " WHERE key_type=? AND natural_key=?;";
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

    void TaskStore::update_file_record_meta(const std::string &client_id,
                                            dw_protocol_t task_protocol, const std::string &task_natural_key,
                                            const std::string &root_name, const std::string &full_path,
                                            bool file_type, const std::string &ext) {
        // 按三要素（client_id + key_type + natural_key）精准定位，与 has_file_record/remove 一致。
        const char *sql =
                "UPDATE file_records SET root_name=?, full_path=?, file_type=?, ext=?, modified_at=?"
                " WHERE client_id=? AND key_type=? AND natural_key=?;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return;
        sqlite3_bind_text(st, 1, root_name.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 2, full_path.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 3, file_type ? 1 : 0);
        // ext 空串按 NULL 入库（目录场景无后缀，各 ext 列统一约定）。
        if (ext.empty()) {
            sqlite3_bind_null(st, 4);
        } else {
            sqlite3_bind_text(st, 4, ext.c_str(), -1, SQLITE_TRANSIENT);
        }
        sqlite3_bind_int64(st, 5, now_unix_ms());
        sqlite3_bind_text(st, 6, client_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 7, static_cast<int>(task_protocol));
        sqlite3_bind_text(st, 8, task_natural_key.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_step(st);
        sqlite3_finalize(st);
    }

    // ---- 播放进度（独立表 play_progress，以物理路径为键）----

    void TaskStore::set_play_position(const std::string &file_path, int64_t position_ms) {
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

    int64_t TaskStore::get_play_position(const std::string &file_path) {
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

    // ---- 文件下载进度缓存 ----

    void TaskStore::replace_file_progress(
        const std::string &client_id, dw_protocol_t protocol, const std::string &natural_key,
        const std::vector<std::tuple<std::string, int32_t, std::vector<dw_byte_range_t>>> &file_ranges) {
        if (file_ranges.empty()) return;
        sqlite3_exec(db_, "BEGIN;", nullptr, nullptr, nullptr);
        // 先删除涉及文件的旧区间（按三要素 + file_index 精准清除），再全量插入。
        {
            sqlite3_stmt *del = nullptr;
            if (sqlite3_prepare_v2(db_,
                                   "DELETE FROM file_progress_cache"
                                   " WHERE client_id=? AND key_type=? AND natural_key=? AND file_index=?;",
                                   -1, &del, nullptr) != SQLITE_OK) {
                sqlite3_exec(db_, "ROLLBACK;", nullptr, nullptr, nullptr);
                return;
            }
            for (const auto &[path, idx, segs]: file_ranges) {
                if (segs.empty()) continue;
                sqlite3_reset(del);
                sqlite3_bind_text(del, 1, client_id.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_int(del, 2, static_cast<int>(protocol));
                sqlite3_bind_text(del, 3, natural_key.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_int(del, 4, idx);
                sqlite3_step(del);
            }
            sqlite3_finalize(del);
        }
        {
            const char *sql =
                    "INSERT INTO file_progress_cache"
                    " (client_id, key_type, natural_key, file_index, physical_path, offset_start, offset_end, modified_at)"
                    " VALUES (?,?,?,?,?,?,?,?);";
            sqlite3_stmt *st = nullptr;
            if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) {
                sqlite3_exec(db_, "ROLLBACK;", nullptr, nullptr, nullptr);
                return;
            }
            for (const auto &[path, idx, segs]: file_ranges) {
                for (const auto &seg: segs) {
                    sqlite3_reset(st);
                    sqlite3_bind_text(st, 1, client_id.c_str(), -1, SQLITE_TRANSIENT);
                    sqlite3_bind_int(st, 2, static_cast<int>(protocol));
                    sqlite3_bind_text(st, 3, natural_key.c_str(), -1, SQLITE_TRANSIENT);
                    sqlite3_bind_int(st, 4, idx);
                    sqlite3_bind_text(st, 5, path.c_str(), -1, SQLITE_TRANSIENT);
                    sqlite3_bind_int64(st, 6, seg.start);
                    sqlite3_bind_int64(st, 7, seg.end);
                    sqlite3_bind_int64(st, 8, now_unix_ms());
                    sqlite3_step(st);
                }
            }
            sqlite3_finalize(st);
        }
        sqlite3_exec(db_, "COMMIT;", nullptr, nullptr, nullptr);
    }

    int64_t TaskStore::upsert_file_progress(const std::string &client_id, dw_protocol_t protocol,
                                            const std::string &natural_key, int32_t file_index,
                                            const std::string &physical_path,
                                            int64_t offset_start, int64_t offset_end) {
        if (offset_start < 0 || offset_end < offset_start) return 0;
        int64_t total = 0;
        sqlite3_exec(db_, "BEGIN;", nullptr, nullptr, nullptr);
        // 合并语义：选出与新区间重叠或相邻（端点相接）的既有行，内存合并后删旧插新。
        sqlite3_stmt *sel = nullptr;
        int64_t merge_start = offset_start;
        int64_t merge_end = offset_end;
        if (sqlite3_prepare_v2(db_,
                               "SELECT offset_start, offset_end FROM file_progress_cache"
                               " WHERE client_id=? AND key_type=? AND natural_key=? AND file_index=?"
                               "   AND offset_start <= ? AND offset_end + 1 >= ?;",
                               -1, &sel, nullptr) == SQLITE_OK) {
            sqlite3_bind_text(sel, 1, client_id.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(sel, 2, static_cast<int>(protocol));
            sqlite3_bind_text(sel, 3, natural_key.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(sel, 4, file_index);
            // 重叠/相邻判定：旧行区间 [s,e] 与新区间 [ns,ne] 满足 s <= ne 且 e+1 >= ns。
            sqlite3_bind_int64(sel, 5, offset_end);
            sqlite3_bind_int64(sel, 6, offset_start);
            std::vector<std::pair<int64_t, int64_t>> olds;
            while (sqlite3_step(sel) == SQLITE_ROW) {
                const int64_t s = sqlite3_column_int64(sel, 0);
                const int64_t e = sqlite3_column_int64(sel, 1);
                olds.emplace_back(s, e);
                merge_start = std::min(merge_start, s);
                merge_end = std::max(merge_end, e);
            }
            sqlite3_finalize(sel);
            if (!olds.empty()) {
                sqlite3_stmt *del = nullptr;
                if (sqlite3_prepare_v2(db_,
                                       "DELETE FROM file_progress_cache"
                                       " WHERE client_id=? AND key_type=? AND natural_key=? AND file_index=?"
                                       "   AND offset_start >= ? AND offset_start <= ?;",
                                       -1, &del, nullptr) == SQLITE_OK) {
                    sqlite3_bind_text(del, 1, client_id.c_str(), -1, SQLITE_TRANSIENT);
                    sqlite3_bind_int(del, 2, static_cast<int>(protocol));
                    sqlite3_bind_text(del, 3, natural_key.c_str(), -1, SQLITE_TRANSIENT);
                    sqlite3_bind_int(del, 4, file_index);
                    // 删除范围：与合并区间有交集的行（起点落在合并区间内）。
                    sqlite3_bind_int64(del, 5, merge_start);
                    sqlite3_bind_int64(del, 6, merge_end);
                    sqlite3_step(del);
                    sqlite3_finalize(del);
                }
            }
        }
        {
            sqlite3_stmt *st = nullptr;
            if (sqlite3_prepare_v2(db_,
                                   "INSERT INTO file_progress_cache"
                                   " (client_id, key_type, natural_key, file_index, physical_path, offset_start, offset_end, modified_at)"
                                   " VALUES (?,?,?,?,?,?,?,?);",
                                   -1, &st, nullptr) == SQLITE_OK) {
                sqlite3_bind_text(st, 1, client_id.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_int(st, 2, static_cast<int>(protocol));
                sqlite3_bind_text(st, 3, natural_key.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_int(st, 4, file_index);
                sqlite3_bind_text(st, 5, physical_path.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_int64(st, 6, merge_start);
                sqlite3_bind_int64(st, 7, merge_end);
                sqlite3_bind_int64(st, 8, now_unix_ms());
                sqlite3_step(st);
                sqlite3_finalize(st);
            }
        }
        // 返回合并后该文件累计已下载字节（供调用方判定文件完成）。
        sqlite3_stmt *sum = nullptr;
        if (sqlite3_prepare_v2(db_,
                               "SELECT COALESCE(SUM(offset_end - offset_start + 1), 0) FROM file_progress_cache"
                               " WHERE client_id=? AND key_type=? AND natural_key=? AND file_index=?;",
                               -1, &sum, nullptr) == SQLITE_OK) {
            sqlite3_bind_text(sum, 1, client_id.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(sum, 2, static_cast<int>(protocol));
            sqlite3_bind_text(sum, 3, natural_key.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(sum, 4, file_index);
            if (sqlite3_step(sum) == SQLITE_ROW) total = sqlite3_column_int64(sum, 0);
            sqlite3_finalize(sum);
        }
        sqlite3_exec(db_, "COMMIT;", nullptr, nullptr, nullptr);
        return total;
    }

    void TaskStore::delete_file_progress_by_file(const std::string &client_id, dw_protocol_t protocol,
                                                  const std::string &natural_key, int32_t file_index) {
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_,
                               "DELETE FROM file_progress_cache"
                               " WHERE client_id=? AND key_type=? AND natural_key=? AND file_index=?;",
                               -1, &st, nullptr) != SQLITE_OK) return;
        sqlite3_bind_text(st, 1, client_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 2, static_cast<int>(protocol));
        sqlite3_bind_text(st, 3, natural_key.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 4, file_index);
        sqlite3_step(st);
        sqlite3_finalize(st);
    }

    void TaskStore::delete_file_progress_by_task(const std::string &client_id, dw_protocol_t protocol,
                                                  const std::string &natural_key) {
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_,
                               "DELETE FROM file_progress_cache"
                               " WHERE client_id=? AND key_type=? AND natural_key=?;",
                               -1, &st, nullptr) != SQLITE_OK) return;
        sqlite3_bind_text(st, 1, client_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 2, static_cast<int>(protocol));
        sqlite3_bind_text(st, 3, natural_key.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_step(st);
        sqlite3_finalize(st);
    }

    int64_t TaskStore::sum_file_progress_by_file(const std::string &client_id, dw_protocol_t protocol,
                                                 const std::string &natural_key, int32_t file_index) const {
        sqlite3_stmt *st = nullptr;
        int64_t total = 0;
        if (sqlite3_prepare_v2(db_,
                               "SELECT COALESCE(SUM(offset_end - offset_start + 1), 0) FROM file_progress_cache"
                               " WHERE client_id=? AND key_type=? AND natural_key=? AND file_index=?;",
                               -1, &st, nullptr) != SQLITE_OK) return total;
        sqlite3_bind_text(st, 1, client_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 2, static_cast<int>(protocol));
        sqlite3_bind_text(st, 3, natural_key.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 4, file_index);
        if (sqlite3_step(st) == SQLITE_ROW) total = sqlite3_column_int64(st, 0);
        sqlite3_finalize(st);
        return total;
    }

    std::vector<dw_byte_range_t> TaskStore::load_segments(const std::string &physical_path, int32_t file_index) const {
        std::vector<dw_byte_range_t> out;
        // 按物理路径查询已下载区间（App 播放器按物理路径消费；file_index 辅助定位）。
        const char *sql =
                "SELECT offset_start, offset_end FROM file_progress_cache"
                " WHERE physical_path=? AND file_index=? ORDER BY offset_start;";
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

} // namespace dw
