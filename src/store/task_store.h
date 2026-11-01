#pragma once

#include "core/task.h"
#include "core/task_record.h"
#include "utils/string_util.h"
#include <sqlite3.h>
#include <mutex>

namespace dw {
    /// 仅处理 tasks 表；借用统一连接，不管理连接生命周期。
    class TaskStore {
    public:
        explicit TaskStore(sqlite3 *const &db) : db_(db) {}
        TaskStore(const TaskStore &) = delete;
        TaskStore &operator=(const TaskStore &) = delete;

        bool init_schema() const {
            std::scoped_lock lock(mutex_);
            return db_ && sqlite3_exec(db_, R"SQL(CREATE TABLE IF NOT EXISTS tasks (protocol INTEGER NOT NULL, unique_key TEXT NOT NULL, id INTEGER NOT NULL, client_id TEXT NOT NULL, is_remote INTEGER NOT NULL, save_path TEXT NOT NULL, original_root_name TEXT NOT NULL, root_name TEXT NOT NULL, full_path TEXT NOT NULL, file_type INTEGER NOT NULL, ext TEXT NOT NULL, parsed INTEGER NOT NULL, url TEXT NOT NULL, info_hash TEXT NOT NULL, magnet_link TEXT NOT NULL, torrent_file TEXT NOT NULL, status INTEGER NOT NULL, total_size INTEGER NOT NULL, total_done INTEGER NOT NULL, priority INTEGER NOT NULL, reason INTEGER NOT NULL, message TEXT NOT NULL, created_at INTEGER NOT NULL, modified_at INTEGER NOT NULL, support_range INTEGER NOT NULL, etag TEXT NOT NULL, last_modified TEXT NOT NULL, force INTEGER NOT NULL, dirty INTEGER NOT NULL, is_delete INTEGER NOT NULL, download_rate REAL NOT NULL, upload_rate REAL NOT NULL, select_file_indexes TEXT NOT NULL, priority_file_indexes TEXT NOT NULL, PRIMARY KEY(protocol, unique_key)); CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(protocol, status); CREATE INDEX IF NOT EXISTS idx_tasks_info_hash ON tasks(protocol, info_hash);)SQL", nullptr, nullptr, nullptr) == SQLITE_OK;
        }

        bool insert_task(const Task &task, const std::string &unique_key) const {
            std::scoped_lock lock(mutex_);
            if (!db_ || unique_key.empty()) return false;
            sqlite3_stmt *statement = nullptr;
            if (sqlite3_prepare_v2(
                    db_,
                    "INSERT INTO tasks(protocol,unique_key,id,client_id,is_remote,save_path,original_root_name,root_name,full_path,file_type,ext,parsed,url,info_hash,magnet_link,torrent_file,status,total_size,total_done,priority,reason,message,created_at,modified_at,support_range,etag,last_modified,force,dirty,is_delete,download_rate,upload_rate,select_file_indexes,priority_file_indexes) VALUES(:protocol,:unique_key,:id,:client_id,:is_remote,:save_path,:original_root_name,:root_name,:full_path,:file_type,:ext,:parsed,:url,:info_hash,:magnet_link,:torrent_file,:status,:total_size,:total_done,:priority,:reason,:message,:created_at,:modified_at,:support_range,:etag,:last_modified,:force,:dirty,:is_delete,:download_rate,:upload_rate,:select_file_indexes,:priority_file_indexes)",
                    -1, &statement, nullptr) != SQLITE_OK)
                return false;
            sqlite3_bind_int(statement, sqlite3_bind_parameter_index(statement, ":protocol"),
                             static_cast<int>(task.task_protocol));
            sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":unique_key"), unique_key.c_str(), -1,
                              SQLITE_TRANSIENT);
            sqlite3_bind_int64(statement, sqlite3_bind_parameter_index(statement, ":id"), task.id);
            sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":client_id"), task.client_id.c_str(), -1,
                              SQLITE_TRANSIENT);
            sqlite3_bind_int64(statement, sqlite3_bind_parameter_index(statement, ":is_remote"), task.is_remote);
            sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":save_path"), task.save_path.c_str(), -1,
                              SQLITE_TRANSIENT);
            sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":original_root_name"),
                              task.original_root_name.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":root_name"), task.root_name.c_str(), -1,
                              SQLITE_TRANSIENT);
            sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":full_path"), task.full_path.c_str(), -1,
                              SQLITE_TRANSIENT);
            sqlite3_bind_int64(statement, sqlite3_bind_parameter_index(statement, ":file_type"), task.file_type);
            sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":ext"), task.ext.c_str(), -1,
                              SQLITE_TRANSIENT);
            sqlite3_bind_int64(statement, sqlite3_bind_parameter_index(statement, ":parsed"), task.parsed);
            sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":url"), task.url.c_str(), -1,
                              SQLITE_TRANSIENT);
            sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":info_hash"), task.info_hash.c_str(), -1,
                              SQLITE_TRANSIENT);
            sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":magnet_link"), task.magnet_link.c_str(),
                              -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":torrent_file"),
                              task.torrent_file.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int64(statement, sqlite3_bind_parameter_index(statement, ":status"), task.status);
            sqlite3_bind_int64(statement, sqlite3_bind_parameter_index(statement, ":total_size"), task.total_size);
            sqlite3_bind_int64(statement, sqlite3_bind_parameter_index(statement, ":total_done"), task.total_done);
            sqlite3_bind_int64(statement, sqlite3_bind_parameter_index(statement, ":priority"), task.priority);
            sqlite3_bind_int64(statement, sqlite3_bind_parameter_index(statement, ":reason"), task.reason);
            sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":message"), task.message.c_str(), -1,
                              SQLITE_TRANSIENT);
            sqlite3_bind_int64(statement, sqlite3_bind_parameter_index(statement, ":created_at"), task.created_at);
            sqlite3_bind_int64(statement, sqlite3_bind_parameter_index(statement, ":modified_at"), task.modified_at);
            sqlite3_bind_int64(statement, sqlite3_bind_parameter_index(statement, ":support_range"), task.support_range);
            sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":etag"), task.etag.c_str(), -1,
                              SQLITE_TRANSIENT);
            sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":last_modified"),
                              task.last_modified.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int64(statement, sqlite3_bind_parameter_index(statement, ":force"), task.force);
            sqlite3_bind_int64(statement, sqlite3_bind_parameter_index(statement, ":dirty"), task.dirty);
            sqlite3_bind_int64(statement, sqlite3_bind_parameter_index(statement, ":is_delete"), task.is_delete);
            sqlite3_bind_double(statement, sqlite3_bind_parameter_index(statement, ":download_rate"), task.download_rate);
            sqlite3_bind_double(statement, sqlite3_bind_parameter_index(statement, ":upload_rate"), task.upload_rate);
            const auto selected = utils::join_ints(task.select_file_indexes);
            const auto priorities = utils::join_ints(task.priority_file_indexes);
            sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":select_file_indexes"), selected.c_str(),
                              -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":priority_file_indexes"),
                              priorities.c_str(), -1, SQLITE_TRANSIENT);
            const bool success = sqlite3_step(statement) == SQLITE_DONE;
            sqlite3_finalize(statement);
            return success;
        }

        bool delete_task(dw_protocol_t protocol, const std::string &unique_key) const {
            std::scoped_lock lock(mutex_);
            if (!db_ || unique_key.empty()) return false;
            sqlite3_stmt *statement = nullptr;
            if (sqlite3_prepare_v2(db_, "DELETE FROM tasks WHERE protocol=:protocol AND unique_key=:unique_key",
                                   -1, &statement, nullptr) != SQLITE_OK)
                return false;
            sqlite3_bind_int(statement, sqlite3_bind_parameter_index(statement, ":protocol"), static_cast<int>(protocol));
            sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":unique_key"), unique_key.c_str(), -1,
                              SQLITE_TRANSIENT);
            const bool success = sqlite3_step(statement) == SQLITE_DONE;
            sqlite3_finalize(statement);
            return success;
        }

        /// 按 info_hash 查询 BT 任务；未找到或失败返回 false，out 清空。
        bool find_by_info_hash(const std::string &info_hash, Task &out) const {
            std::scoped_lock lock(mutex_);
            out = {};
            if (!db_ || info_hash.empty()) return false;
            sqlite3_stmt *statement = nullptr;
            if (sqlite3_prepare_v2(db_, "SELECT id,client_id,is_remote,save_path,original_root_name,root_name,full_path,file_type,ext,parsed,url,info_hash,magnet_link,torrent_file,protocol,status,total_size,total_done,priority,reason,message,created_at,modified_at,support_range,etag,last_modified,force,dirty,is_delete,download_rate,upload_rate,select_file_indexes,priority_file_indexes FROM tasks WHERE protocol=:protocol AND info_hash=:info_hash LIMIT 1", -1, &statement, nullptr) != SQLITE_OK) return false;
            sqlite3_bind_int(statement, sqlite3_bind_parameter_index(statement, ":protocol"), DW_PROTOCOL_TORRENT);
            sqlite3_bind_text(statement, sqlite3_bind_parameter_index(statement, ":info_hash"), info_hash.c_str(), -1, SQLITE_TRANSIENT);
            const bool found = sqlite3_step(statement) == SQLITE_ROW;
            if (found) {
                auto text = [&](int index) -> std::string {
                    const auto *value = sqlite3_column_text(statement, index);
                    return value ? reinterpret_cast<const char *>(value) : "";
                };
                out.id = sqlite3_column_int64(statement, 0);
                out.client_id = text(1);
                out.is_remote = sqlite3_column_int(statement, 2) != 0;
                out.save_path = text(3);
                out.original_root_name = text(4);
                out.root_name = text(5);
                out.full_path = text(6);
                out.file_type = sqlite3_column_int(statement, 7) != 0;
                out.ext = text(8);
                out.parsed = sqlite3_column_int(statement, 9) != 0;
                out.url = text(10);
                out.info_hash = text(11);
                out.magnet_link = text(12);
                out.torrent_file = text(13);
                out.task_protocol = static_cast<dw_protocol_t>(sqlite3_column_int(statement, 14));
                out.status = sqlite3_column_int(statement, 15);
                out.total_size = sqlite3_column_int64(statement, 16);
                out.total_done = sqlite3_column_int64(statement, 17);
                out.priority = sqlite3_column_int(statement, 18);
                out.reason = sqlite3_column_int(statement, 19);
                out.message = text(20);
                out.created_at = sqlite3_column_int64(statement, 21);
                out.modified_at = sqlite3_column_int64(statement, 22);
                out.support_range = sqlite3_column_int(statement, 23);
                out.etag = text(24);
                out.last_modified = text(25);
                out.force = sqlite3_column_int(statement, 26) != 0;
                out.dirty = sqlite3_column_int(statement, 27) != 0;
                out.is_delete = sqlite3_column_int(statement, 28) != 0;
                out.download_rate = sqlite3_column_double(statement, 29);
                out.upload_rate = sqlite3_column_double(statement, 30);
                out.select_file_indexes = utils::split_ints(text(31));
                out.priority_file_indexes = utils::split_ints(text(32));
            }
            sqlite3_finalize(statement);
            return found;
        }

    private:
        sqlite3 *const &db_;
        mutable std::mutex mutex_;
    };
    /**
     * SQLite 任务存储。借用统一连接，不负责打开和关闭。
     */
    class LegacyTaskStore {
    public:
        explicit LegacyTaskStore(sqlite3 *const &db) : db_(db) {}

        ~LegacyTaskStore() = default;

        LegacyTaskStore(const LegacyTaskStore &) = delete;

        LegacyTaskStore &operator=(const LegacyTaskStore &) = delete;

        /// 建表（IF NOT EXISTS）：复合主键 (client_id, protocol, natural_key)；
        /// 检测到旧 schema（task_id 列）则 DROP 全部表重建（项目未上线）。
        void init_schema() const;


        /// 删除任务及其 resume_data / file_progress_cache / file_records（统一按复合键）。
        void remove(const std::string &client_id, dw_protocol_t protocol, const std::string &natural_key) const;

        /// 写入 / 覆盖断点续传数据。
        void save_resume(const std::string &client_id, dw_protocol_t protocol,
                         const std::string &natural_key, const uint8_t *data, size_t size) const;

        /// 保存任务来源（save_path / magnet_link / torrent_file），不影响 data 列；用于 resume data 尚未生成时的兜底恢复。
        void save_resume_source(const std::string &client_id, dw_protocol_t protocol,
                                const std::string &natural_key,
                                const std::string &save_path,
                                const std::string &magnet_link, const std::string &torrent_file) const;

        /// 恢复信息结构体：包含 resume data 二进制、magnet_link、torrent_file
        struct ResumeInfo {
            std::vector<uint8_t> data; ///< libtorrent resume 二进制数据
            std::string magnet_link; ///< 磁力链接
            std::string torrent_file; ///< 种子文件路径
        };

        /// 一次性加载全部恢复信息（data + magnet + torrent）
        ResumeInfo load_resume_info(const std::string &client_id, dw_protocol_t protocol,
                                    const std::string &natural_key) const;

        // ---- 文件目录表（file_records）----

        /// 新增文件记录：自增 id 回填到 r.id。
        void insert_file_record(FileRecord &r) const;

        /// 按任务关联三要素（client_id + task_protocol + task_natural_key）加载单条文件记录；不存在返回 false。
        bool find_file_record(const std::string &client_id, dw_protocol_t task_protocol,
                              const std::string &task_natural_key, FileRecord &out) const;

        /// 按 full_path 检查文件记录是否已存在。
        bool file_record_exists_by_path(const std::string &full_path) const;

        /// 按任务关联三要素刷新 modified_at 为当前时间，用于排序置顶。
        void touch_file_record(const std::string &client_id, dw_protocol_t task_protocol,
                               const std::string &task_natural_key) const;

        /// 载入全部文件记录（按 modified_at DESC）。
        std::vector<FileRecord> load_file_records() const;

        /// 载入指定客户端的文件记录（按 modified_at DESC）。
        std::vector<FileRecord> load_file_records(const std::string &client_id) const;

        /// 载入指定客户端、指定协议集合、指定状态集合的文件记录（按 modified_at DESC）。
        /// 用于按协议维度精确过滤，避免加载不相关协议的任务。
        std::vector<FileRecord> load_file_records(const std::string &client_id,
                                                  const std::vector<dw_protocol_t> &protocols,
                                                  const std::vector<dw_task_status_t> &statuses) const;

        /// 按 full_path 删除单条文件记录。
        void delete_file_record_by_path(const std::string &full_path) const;

        /// 按 save_path + root_name 同步文件记录状态（validate_local_tasks 用）。
        void update_file_record_status_by_name(const std::string &save_path, const std::string &root_name,
                                               int32_t status) const;

        /// 按三要素更新文件记录的运行时可变字段（元数据 + 状态 + 进度），单次 SQL。
        void update_file_record(const FileRecord &rec) const;

        // ---- 播放进度（独立表 play_progress，以完整路径为键）----

        /// 写入 / 覆盖文件播放进度（毫秒）。完整路径由调用方（TaskManager）解析后直传。
        void set_play_position(const std::string &full_path, int64_t position_ms) const;

        /// 读取文件播放进度（毫秒）；无记录返回 0。
        int64_t get_play_position(const std::string &full_path) const;

        // ---- 文件下载进度缓存（file_progress_cache：三要素 + file_index 定位，完整路径供 App 关联）----
        // intervals 为 JSON 序列化区间集合：[[start1,end1],[start2,end2],...]；engine 侧按 1% 文件大小（兑底 4 piece）判定后才入库。

        /// 全量重写多个文件的进度区间（单事务）：先删该文件旧区间再插入，HTTP 周期快照用。
        void replace_file_progress(const std::string &client_id, dw_protocol_t protocol,
                                   const std::string &natural_key,
                                   const std::vector<std::tuple<std::string, int32_t, std::vector<dw_byte_range_t> > > &
                                   file_ranges) const;

        /// 保存完整区间集合（BT piece 事件驱动）：UPSERT 语义，直接覆盖该文件的 segments 字段，
        /// 返回该文件累计已下载字节（供调用方判定文件完成）。
        int64_t save_file_progress(const std::string &client_id, dw_protocol_t protocol,
                                   const std::string &natural_key, int32_t file_index,
                                   const std::string &full_path,
                                   int64_t size, int64_t downloaded_bytes,
                                   const std::string &segments_json) const;

        /// 按文件删除进度缓存（文件下载完成，区间失去意义）。
        void delete_file_progress_by_file(const std::string &client_id, dw_protocol_t protocol,
                                          const std::string &natural_key, int32_t file_index) const;

        /// 标记文件下载完成：file_done = file_size，intervals = [[0, file_size-1]]。
        void mark_file_complete(const std::string &client_id, dw_protocol_t protocol,
                                const std::string &natural_key, int32_t file_index) const;

        /// 聚合文件累计已下载字节；无记录返回 0。
        int64_t sum_file_progress_by_file(const std::string &client_id, dw_protocol_t protocol,
                                          const std::string &natural_key, int32_t file_index) const;

        /// 按任务三要素查询文件进度缓存（每个文件一条记录）；不存在返回空 vector。
        std::vector<FileProgressInfo> load_file_progress(const std::string &client_id,
                                                          dw_protocol_t protocol,
                                                          const std::string &natural_key) const;

        /// 按完整路径批量查询文件进度（file_done + intervals 原始 JSON）。
        /// @return full_path -> {file_done, intervals_json} 映射
        std::unordered_map<std::string, std::pair<int64_t, std::string>>
        load_file_progress_by_paths(const std::vector<std::string> &paths) const;

    private:
        sqlite3 *const &db_;
    };

}
