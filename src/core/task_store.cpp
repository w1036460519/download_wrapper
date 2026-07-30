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

#include <cstdio>

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

        /// 从查询行填充 TaskRecord（load_active / load_by_url / load_by_info_hash / load_by_id 共用，列序须与 SELECT 一致）。
        void fill_record(sqlite3_stmt *st, TaskRecord &r) {
            r.id = sqlite3_column_int64(st, 0);
            r.info_hash = col_text(st, 1);
            r.protocol = static_cast<dw_protocol_t>(sqlite3_column_int(st, 2));
            r.name = col_text(st, 3);
            r.save_path = col_text(st, 4);
            r.filename = col_text(st, 5);
            r.url = col_text(st, 6);
            r.magnet_link = col_text(st, 7);
            r.torrent_file = col_text(st, 8);
            r.trackers = split_lines(col_text(st, 9));
            r.file_indexes = split_ints(col_text(st, 10));
            r.priority = sqlite3_column_int(st, 11);
            r.status = static_cast<dw_task_status_t>(sqlite3_column_int(st, 12));
            r.progress = sqlite3_column_double(st, 13);
            r.total_size = sqlite3_column_int64(st, 14);
            r.total_done = sqlite3_column_int64(st, 15);
            r.support_range = sqlite3_column_int(st, 16);
            r.etag = col_text(st, 17);
            r.last_modified = col_text(st, 18);
            r.created_at = sqlite3_column_int64(st, 19);
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
        // 表结构：自增主键 id（升序即提交次序）。身份为协议专属列——BT 存 info_hash，HTTP 用 url；
        // 不再设唯一约束，去重由业务层在 add 时按 url / info_hash 查重实现。
        // resume_data / task_files 的 task_id 列统一引用 tasks.id（整型自增主键）。
        const char *sql =
                "CREATE TABLE IF NOT EXISTS tasks ("
                "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
                "  info_hash TEXT,"
                "  protocol INTEGER,"
                "  name TEXT,"
                "  save_path TEXT,"
                "  filename TEXT,"
                "  url TEXT,"
                "  magnet_link TEXT,"
                "  torrent_file TEXT,"
                "  trackers TEXT,"
                "  file_indexes TEXT,"
                "  priority INTEGER,"
                "  status INTEGER,"
                "  progress REAL,"
                "  total_size INTEGER,"
                "  total_done INTEGER,"
                "  support_range INTEGER,"
                "  etag TEXT,"
                "  last_modified TEXT,"
                "  created_at INTEGER"
                ");"
                // 去重查询加速：add 时按协议查 url / info_hash（非唯一索引）。
                "CREATE INDEX IF NOT EXISTS idx_tasks_info_hash ON tasks(info_hash);"
                "CREATE INDEX IF NOT EXISTS idx_tasks_url ON tasks(url);"
                "CREATE TABLE IF NOT EXISTS resume_data ("
                "  task_id INTEGER PRIMARY KEY,"
                "  data BLOB,"
                "  saved_at INTEGER"
                ");"
                "CREATE TABLE IF NOT EXISTS task_files ("
                "  task_id INTEGER NOT NULL,"
                "  node_id INTEGER NOT NULL,"      // 任务内节点序号
                "  parent_id INTEGER,"            // 父节点 node_id；根节点 NULL
                "  file_index INTEGER NOT NULL,"  // libtorrent 索引；文件夹 -1
                "  type INTEGER NOT NULL,"        // 0=文件夹 1=文件
                "  prefix TEXT,"                  // 父路径累积（含尾部分隔符）
                "  name TEXT NOT NULL,"           // 完整名含后缀 / 文件夹名
                "  ext TEXT,"                     // 后缀不含点；文件夹 NULL
                "  size INTEGER NOT NULL,"        // 文件实际大小 / 文件夹聚合和
                "  status INTEGER NOT NULL,"      // 文件 0下载中/1已删除/2完成；文件夹恒 0
                "  created_at INTEGER,"           // 仅目录内排序用（不建索引）
                "  PRIMARY KEY (task_id, node_id)"
                ");"
                "CREATE INDEX IF NOT EXISTS idx_task_files_task_id"
                "  ON task_files(task_id);"
                // 边下边播缓存：文件级下载/播放进度。downloaded_bytes 由引擎低频聚合写（本期暂不写），
                // play_position_ms 仅由 App 经 setter 写；精确已下载区间不入表（走实时接口）。
                "CREATE TABLE IF NOT EXISTS file_cache ("
                "  task_id INTEGER NOT NULL,"
                "  file_index INTEGER NOT NULL,"
                "  downloaded_bytes INTEGER,"
                "  play_position_ms INTEGER,"
                "  updated_at INTEGER,"
                "  PRIMARY KEY (task_id, file_index)"
                ");"
                "CREATE INDEX IF NOT EXISTS idx_file_cache_task_id"
                "  ON file_cache(task_id);"
                // 已下载连续字节区间快照：任务未加载进引擎时的播放兜底，多行 (task_id,file_index)。
                "CREATE TABLE IF NOT EXISTS file_segments ("
                "  task_id INTEGER NOT NULL,"
                "  file_index INTEGER NOT NULL,"
                "  seg_start INTEGER NOT NULL,"
                "  seg_end INTEGER NOT NULL"
                ");"
                "CREATE INDEX IF NOT EXISTS idx_file_segments_task"
                "  ON file_segments(task_id);";
        sqlite3_exec(db_, sql, nullptr, nullptr, nullptr);
    }

    std::vector<TaskRecord> TaskStore::load_active() {
        // 载入排队 / 活跃任务（DOWNLOADING=0, QUEUED=4, RESOLVING=5）；6 为旧库遗留值，
        // 由上层重启归一化统一回落 QUEUED。暂停(1)/完成(2)/错误(3) 留库，按需回读，减小常驻内存。
        std::vector<TaskRecord> out;
        const char *sql =
                "SELECT id, info_hash, protocol, name, save_path, filename, url, magnet_link,"
                "       torrent_file, trackers, file_indexes, priority,"
                "       status, progress, total_size, total_done, support_range, etag,"
                "       last_modified, created_at FROM tasks"
                "  WHERE status IN (0,4,5,6);";
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

    std::vector<TaskRecord> TaskStore::load_all() {
        // 全量任务（含暂停 / 完成 / 错误），供 dw_list_tasks 快照使用。列序与 fill_record 一致。
        std::vector<TaskRecord> out;
        const char *sql =
                "SELECT id, info_hash, protocol, name, save_path, filename, url, magnet_link,"
                "       torrent_file, trackers, file_indexes, priority,"
                "       status, progress, total_size, total_done, support_range, etag,"
                "       last_modified, created_at FROM tasks"
                "  ORDER BY created_at DESC, id DESC;";
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
        /// 按协议 + 指定列查询单条任务（列序与 fill_record 一致）。命中填充 out 返回 true。
        /// col 为库内固定列名（"url" / "info_hash"），值走绑定参数，无注入风险。
        bool load_one_by(sqlite3 *db, const dw_protocol_t protocol, const char *col,
                         const std::string &value, TaskRecord &out) {
            std::string sql =
                    "SELECT id, info_hash, protocol, name, save_path, filename, url, magnet_link,"
                    "       torrent_file, trackers, file_indexes, priority,"
                    "       status, progress, total_size, total_done, support_range, etag,"
                    "       last_modified, created_at FROM tasks WHERE protocol=? AND ";
            sql += col;
            sql += "=? LIMIT 1;";
            sqlite3_stmt *st = nullptr;
            if (sqlite3_prepare_v2(db, sql.c_str(), -1, &st, nullptr) != SQLITE_OK) return false;
            sqlite3_bind_int(st, 1, protocol);
            sqlite3_bind_text(st, 2, value.c_str(), -1, SQLITE_TRANSIENT);
            bool found = false;
            if (sqlite3_step(st) == SQLITE_ROW) {
                fill_record(st, out);
                found = true;
            }
            sqlite3_finalize(st);
            return found;
        }
    } // namespace

    bool TaskStore::load_by_url(const std::string &url, TaskRecord &out) {
        // HTTP 按 url 查（url 列有非唯一索引）。业务层据此判重。
        return load_one_by(db_, DW_PROTOCOL_HTTP, "url", url, out);
    }

    bool TaskStore::load_by_info_hash(const std::string &info_hash, TaskRecord &out) {
        // BT 按 info_hash 查（info_hash 列有非唯一索引）。业务层据此判重。
        return load_one_by(db_, DW_PROTOCOL_TORRENT, "info_hash", info_hash, out);
    }

    bool TaskStore::load_by_id(int64_t id, TaskRecord &out) {
        const char *sql =
                "SELECT id, info_hash, protocol, name, save_path, filename, url, magnet_link,"
                "       torrent_file, trackers, file_indexes, priority,"
                "       status, progress, total_size, total_done, support_range, etag,"
                "       last_modified, created_at"
                " FROM tasks WHERE id=?;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return false;
        sqlite3_bind_int64(st, 1, id);
        bool found = false;
        if (sqlite3_step(st) == SQLITE_ROW) {
            fill_record(st, out);
            found = true;
        }
        sqlite3_finalize(st);
        return found;
    }

    std::vector<std::string> TaskStore::load_names_by_save_path(const std::string &save_path,
                                                                 const int64_t exclude_id) {
        // 同 save_path 下其他任务的 name / filename 合并返回（非空值），供唯一名判重。
        std::vector<std::string> out;
        const char *sql =
                "SELECT name, filename FROM tasks WHERE save_path=? AND id!=?;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return out;
        sqlite3_bind_text(st, 1, save_path.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(st, 2, exclude_id);
        while (sqlite3_step(st) == SQLITE_ROW) {
            if (std::string name = col_text(st, 0); !name.empty()) out.push_back(std::move(name));
            if (std::string filename = col_text(st, 1); !filename.empty()) out.push_back(std::move(filename));
        }
        sqlite3_finalize(st);
        return out;
    }

    void TaskStore::insert(TaskRecord &r) {
        // 新增任务：纯 INSERT，回填自增主键。去重已由调用方（add）预先按 url / info_hash 判定。
        const char *sql =
                "INSERT INTO tasks (info_hash, protocol, name, save_path,"
                " filename, url, magnet_link, torrent_file, trackers, file_indexes,"
                " priority, status, progress, total_size, total_done,"
                " support_range, etag, last_modified, created_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
                " RETURNING id;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return;

        const std::string trackers = join_lines(r.trackers);
        const std::string indexes = join_ints(r.file_indexes);

        sqlite3_bind_text(st, 1, r.info_hash.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 2, r.protocol);
        sqlite3_bind_text(st, 3, r.name.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 4, r.save_path.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 5, r.filename.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 6, r.url.c_str(), -1, SQLITE_TRANSIENT);
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

        // INSERT ... RETURNING id：直接从结果行读回自增主键，不依赖连接级
        // last_insert_rowid，规避未来共享连接 / 嵌套写入场景下的取值歧义。
        if (sqlite3_step(st) == SQLITE_ROW) {
            r.id = sqlite3_column_int64(st, 0);
        }
        sqlite3_finalize(st);
    }

    void TaskStore::update(const TaskRecord &r) {
        // 更新既有任务：按 id 原地 UPDATE 全字段（保留 rowid 与提交次序）。要求 r.id 有效。
        const char *sql =
                "UPDATE tasks SET info_hash=?, protocol=?, name=?, save_path=?,"
                " filename=?, url=?, magnet_link=?, torrent_file=?, trackers=?, file_indexes=?,"
                " priority=?, status=?, progress=?, total_size=?, total_done=?,"
                " support_range=?, etag=?, last_modified=?, created_at=?"
                " WHERE id=?;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return;

        const std::string trackers = join_lines(r.trackers);
        const std::string indexes = join_ints(r.file_indexes);

        sqlite3_bind_text(st, 1, r.info_hash.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 2, r.protocol);
        sqlite3_bind_text(st, 3, r.name.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 4, r.save_path.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 5, r.filename.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 6, r.url.c_str(), -1, SQLITE_TRANSIENT);
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
        sqlite3_bind_int64(st, 20, r.id);

        sqlite3_step(st);
        sqlite3_finalize(st);
    }

    void TaskStore::remove(int64_t id) {
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, "DELETE FROM tasks WHERE id=?;", -1, &st,
                               nullptr) == SQLITE_OK) {
            sqlite3_bind_int64(st, 1, id);
            sqlite3_step(st);
            sqlite3_finalize(st);
        }
        st = nullptr;
        if (sqlite3_prepare_v2(db_, "DELETE FROM resume_data WHERE task_id=?;", -1,
                               &st, nullptr) == SQLITE_OK) {
            sqlite3_bind_int64(st, 1, id);
            sqlite3_step(st);
            sqlite3_finalize(st);
        }
        // 联动删除任务关联的文件信息（子表统一以自增 id 为键）
        delete_task_files(id);
        // 联动删除边下边播缓存记录
        st = nullptr;
        if (sqlite3_prepare_v2(db_, "DELETE FROM file_cache WHERE task_id=?;", -1,
                               &st, nullptr) == SQLITE_OK) {
            sqlite3_bind_int64(st, 1, id);
            sqlite3_step(st);
            sqlite3_finalize(st);
        }
        // 联动删除已下载区间快照
        st = nullptr;
        if (sqlite3_prepare_v2(db_, "DELETE FROM file_segments WHERE task_id=?;", -1,
                               &st, nullptr) == SQLITE_OK) {
            sqlite3_bind_int64(st, 1, id);
            sqlite3_step(st);
            sqlite3_finalize(st);
        }
    }

    void TaskStore::save_resume(int64_t id,
                                const uint8_t *data, size_t size) {
        const char *sql =
                "INSERT OR REPLACE INTO resume_data (task_id, data, saved_at)"
                " VALUES (?,?,?);";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return;
        sqlite3_bind_int64(st, 1, id);
        sqlite3_bind_blob(st, 2, data, static_cast<int>(size), SQLITE_TRANSIENT);
        sqlite3_bind_int64(st, 3, now_unix_ms());
        sqlite3_step(st);
        sqlite3_finalize(st);
    }

    std::vector<uint8_t> TaskStore::load_resume(int64_t id) {
        std::vector<uint8_t> out;
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, "SELECT data FROM resume_data WHERE task_id=?;",
                               -1, &st, nullptr) != SQLITE_OK) {
            return out;
        }
        sqlite3_bind_int64(st, 1, id);
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

    void TaskStore::clear_resume(int64_t id) {
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, "DELETE FROM resume_data WHERE task_id=?;",
                               -1, &st, nullptr) == SQLITE_OK) {
            sqlite3_bind_int64(st, 1, id);
            sqlite3_step(st);
            sqlite3_finalize(st);
        }
    }

    // ---- 任务文件信息 ----

    void TaskStore::save_task_files(int64_t id,
                                    const std::vector<dw_file_info_t> &files) {
        if (files.empty()) return;

        // 先清旧节点再批量写入（全量重建），事务包裹保证原子性。
        sqlite3_exec(db_, "BEGIN;", nullptr, nullptr, nullptr);
        {
            sqlite3_stmt *del = nullptr;
            if (sqlite3_prepare_v2(db_, "DELETE FROM task_files WHERE task_id=?;",
                                   -1, &del, nullptr) == SQLITE_OK) {
                sqlite3_bind_int64(del, 1, id);
                sqlite3_step(del);
                sqlite3_finalize(del);
            }
        }

        const char *sql =
                "INSERT OR REPLACE INTO task_files"
                " (task_id, node_id, parent_id, file_index, type, prefix,"
                "  name, ext, size, status, created_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?);";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) {
            sqlite3_exec(db_, "ROLLBACK;", nullptr, nullptr, nullptr);
            return;
        }

        for (const auto &f: files) {
            sqlite3_reset(st);
            sqlite3_bind_int64(st, 1, id);
            sqlite3_bind_int64(st, 2, f.node_id);
            if (f.parent_id < 0) {
                sqlite3_bind_null(st, 3);   // 根节点 parent_id 为 NULL
            } else {
                sqlite3_bind_int64(st, 3, f.parent_id);
            }
            sqlite3_bind_int(st, 4, f.index);
            sqlite3_bind_int(st, 5, f.type);
            sqlite3_bind_text(st, 6, f.prefix ? f.prefix : "", -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(st, 7, f.name ? f.name : "", -1, SQLITE_TRANSIENT);
            if (f.ext) {
                sqlite3_bind_text(st, 8, f.ext, -1, SQLITE_TRANSIENT);
            } else {
                sqlite3_bind_null(st, 8);   // 文件夹无后缀
            }
            sqlite3_bind_int64(st, 9, f.size);
            sqlite3_bind_int(st, 10, f.status);
            sqlite3_bind_int64(st, 11, f.created_at);
            sqlite3_step(st);
        }
        sqlite3_finalize(st);
        sqlite3_exec(db_, "COMMIT;", nullptr, nullptr, nullptr);
    }

    std::vector<dw_file_info_t> TaskStore::load_task_files(int64_t id) {
        std::vector<dw_file_info_t> out;
        // 按 created_at 升序返回（同目录内按建节点次序），调用方再按 parent_id 组树。
        const char *sql =
                "SELECT node_id, parent_id, file_index, type, prefix,"
                "       name, ext, size, status, created_at FROM task_files"
                " WHERE task_id=? ORDER BY created_at, node_id;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return out;

        sqlite3_bind_int64(st, 1, id);
        while (sqlite3_step(st) == SQLITE_ROW) {
            dw_file_info_t f{};
            f.node_id = sqlite3_column_int64(st, 0);
            f.parent_id = (sqlite3_column_type(st, 1) == SQLITE_NULL)
                              ? -1 : sqlite3_column_int64(st, 1);
            f.index = sqlite3_column_int(st, 2);
            f.type = sqlite3_column_int(st, 3);
            f.prefix = dup_col_text(st, 4);
            f.name = dup_col_text(st, 5);
            f.ext = dup_col_text(st, 6);
            f.size = sqlite3_column_int64(st, 7);
            f.status = sqlite3_column_int(st, 8);
            f.created_at = sqlite3_column_int64(st, 9);
            out.push_back(f);
        }
        sqlite3_finalize(st);
        return out;
    }

    void TaskStore::delete_task_files(int64_t id) {
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, "DELETE FROM task_files WHERE task_id=?;", -1,
                               &st, nullptr) == SQLITE_OK) {
            sqlite3_bind_int64(st, 1, id);
            sqlite3_step(st);
            sqlite3_finalize(st);
        }
    }

    void TaskStore::mark_task_files_completed(int64_t id) {
        // 任务级 0→2 传播：仅文件节点（type=1）置完成正常，不触碰已删除(1)态。
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_,
                               "UPDATE task_files SET status=2 WHERE task_id=? AND type=1 AND status<>1;",
                               -1, &st, nullptr) == SQLITE_OK) {
            sqlite3_bind_int64(st, 1, id);
            sqlite3_step(st);
            sqlite3_finalize(st);
        }
    }

    // ---- 边下边播缓存 ----

    void TaskStore::set_play_position(int64_t id, int32_t file_index, int64_t position_ms) {
        // upsert：仅更新 play_position_ms / updated_at，不触碰 downloaded_bytes（由引擎独立写）。
        const char *sql =
                "INSERT INTO file_cache (task_id, file_index, play_position_ms, updated_at)"
                " VALUES (?,?,?,?)"
                " ON CONFLICT(task_id, file_index) DO UPDATE SET"
                "   play_position_ms=excluded.play_position_ms,"
                "   updated_at=excluded.updated_at;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return;
        sqlite3_bind_int64(st, 1, id);
        sqlite3_bind_int(st, 2, file_index);
        sqlite3_bind_int64(st, 3, position_ms);
        sqlite3_bind_int64(st, 4, now_unix_ms());
        sqlite3_step(st);
        sqlite3_finalize(st);
    }

    int64_t TaskStore::get_play_position(int64_t id, int32_t file_index) {
        int64_t position_ms = 0;
        const char *sql =
                "SELECT play_position_ms FROM file_cache"
                " WHERE task_id=? AND file_index=?;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return position_ms;
        sqlite3_bind_int64(st, 1, id);
        sqlite3_bind_int(st, 2, file_index);
        if (sqlite3_step(st) == SQLITE_ROW &&
            sqlite3_column_type(st, 0) != SQLITE_NULL) {
            position_ms = sqlite3_column_int64(st, 0);
        }
        sqlite3_finalize(st);
        return position_ms;
    }

    // ---- 已下载区间快照（任务未加载进引擎时的播放兜底）----

    void TaskStore::save_segments(int64_t id, int32_t file_index,
                                 const std::vector<dw_byte_range_t> &segments) {
        // 事务内先删该 (task_id,file_index) 旧区间，再批量写入新快照；空 segments 即清空。
        sqlite3_exec(db_, "BEGIN;", nullptr, nullptr, nullptr);

        sqlite3_stmt *del = nullptr;
        if (sqlite3_prepare_v2(db_,
                               "DELETE FROM file_segments WHERE task_id=? AND file_index=?;",
                               -1, &del, nullptr) != SQLITE_OK) {
            sqlite3_exec(db_, "ROLLBACK;", nullptr, nullptr, nullptr);
            return;
        }
        sqlite3_bind_int64(del, 1, id);
        sqlite3_bind_int(del, 2, file_index);
        sqlite3_step(del);
        sqlite3_finalize(del);

        if (!segments.empty()) {
            const char *sql =
                    "INSERT INTO file_segments (task_id, file_index, seg_start, seg_end)"
                    " VALUES (?,?,?,?);";
            sqlite3_stmt *st = nullptr;
            if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) {
                sqlite3_exec(db_, "ROLLBACK;", nullptr, nullptr, nullptr);
                return;
            }
            for (const auto &seg: segments) {
                sqlite3_reset(st);
                sqlite3_bind_int64(st, 1, id);
                sqlite3_bind_int(st, 2, file_index);
                sqlite3_bind_int64(st, 3, seg.start);
                sqlite3_bind_int64(st, 4, seg.end);
                sqlite3_step(st);
            }
            sqlite3_finalize(st);
        }
        sqlite3_exec(db_, "COMMIT;", nullptr, nullptr, nullptr);
    }

    std::vector<dw_byte_range_t> TaskStore::load_segments(int64_t id, int32_t file_index) {
        std::vector<dw_byte_range_t> out;
        const char *sql =
                "SELECT seg_start, seg_end FROM file_segments"
                " WHERE task_id=? AND file_index=? ORDER BY seg_start;";
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) return out;
        sqlite3_bind_int64(st, 1, id);
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

    void TaskStore::clear_segments(int64_t id) {
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db_, "DELETE FROM file_segments WHERE task_id=?;",
                               -1, &st, nullptr) == SQLITE_OK) {
            sqlite3_bind_int64(st, 1, id);
            sqlite3_step(st);
            sqlite3_finalize(st);
        }
    }
} // namespace dw
