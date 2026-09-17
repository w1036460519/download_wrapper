/**
 * @file task_store.h
 * @brief 任务持久化存储层：封装 SQLite 连接与全部读写操作。
 *
 * 职责边界：
 *   - 负责 file_records（任务状态持久化权威）、resume_data、file_progress_cache、play_progress 的存取；
 *     不触碰内存注册表与调度逻辑；
 *   - 表结构自维护：建表（IF NOT EXISTS）+ 旧库补列（ALTER，列已存在时忽略错误）；
 *   - 不做并发保护，要求调用方自行串行化（TaskManager 在持有 mtx_ 时调用）。
 *
 * 唯一键约定：
 *   - 任务主键 = (client_id, protocol, natural_key) 复合键
 *   - 客户端标识 client_id 由 App 启动时注入（UUIDv4）
 *   - protocol：0=http(url) 1=bt(info_hash) 2=local
 *   - natural_key：HTTP=url / BT=info_hash，唯一标识
 *   - 全部读写操作按复合键定位，无单字段 task_id 概念
 */

#pragma once

#include "task_record.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

struct sqlite3;

namespace dw {

/**
 * SQLite 任务存储。生命周期与 TaskManager 绑定，析构时自动关闭连接。
 */
class TaskStore {
public:
    TaskStore() = default;
    ~TaskStore();

    TaskStore(const TaskStore&)            = delete;
    TaskStore& operator=(const TaskStore&) = delete;

    /// 打开数据库并设置 WAL / synchronous。失败返回 false。
    bool open(const std::string& path);
    /// 关闭数据库连接（幂等）。
    void close();
    /// 建表（IF NOT EXISTS）：复合主键 (client_id, protocol, natural_key)；
    /// 检测到旧 schema（task_id 列）则 DROP 全部表重建（项目未上线）。
    void init_schema() const;

    /// 清理指定 save_path 下的 file_records 中 type=0（本地文件条目）。
    void clear_local_tasks(const std::string& client_id, const std::string& save_path) const;
    /// 删除任务及其 resume_data / file_progress_cache / file_records（统一按复合键）。
    void remove(const std::string &client_id, dw_protocol_t protocol, const std::string &natural_key) const;
    /// 重置任务进度：清除 resume_data / file_progress_cache，file_records 状态回 QUEUED、进度归零。
    void reset_task_progress(const std::string &client_id, dw_protocol_t protocol, const std::string &natural_key) const;
    /// 写入 / 覆盖断点续传数据。
    void save_resume(const std::string &client_id, dw_protocol_t protocol,
                     const std::string &natural_key, const uint8_t* data, size_t size) const;
    /// 读取断点续传数据；不存在返回空 vector。
    std::vector<uint8_t> load_resume(const std::string &client_id, dw_protocol_t protocol,
                                     const std::string &natural_key) const;
    /// 仅清除某任务的断点续传数据。
    void clear_resume(const std::string &client_id, dw_protocol_t protocol, const std::string &natural_key) const;

    // ---- 文件目录表（file_records）----

    /// 新增文件记录：自增 id 回填到 r.id。
    void insert_file_record(FileRecord &r) const;
    /// 按任务关联三要素（client_id + task_protocol + task_natural_key）判存，
    /// 用于占位插入的幂等检查，避免重复新建。
    bool has_file_record(const std::string &client_id, dw_protocol_t task_protocol,
                         const std::string &task_natural_key) const;
    /// 按任务关联三要素刷新 modified_at 为当前时间，用于排序置顶。
    void touch_file_record(const std::string &client_id, dw_protocol_t task_protocol,
                           const std::string &task_natural_key) const;
    /// 载入指定客户端的全部文件记录（按 modified_at DESC）。
    std::vector<FileRecord> load_file_records(const std::string &client_id) const;
    /// 载入指定客户端某 save_path 下的文件记录（按 modified_at DESC）。
    std::vector<FileRecord> load_file_records_by_save_path(const std::string &client_id, const std::string &save_path) const;
    /// 按 save_path + root_name 删除单条文件记录（LOCAL 条目删除）。
    void delete_file_record_by_name(const std::string &client_id, const std::string &save_path,
                                    const std::string &root_name) const;
    /// 按 save_path + root_name 同步文件记录状态（validate_local_tasks 用）。
    void update_file_record_status_by_name(const std::string &save_path, const std::string &root_name,
                                           int32_t status) const;
    /// 同步文件记录的进度与错误信息冗余字段，免全字段 UPDATE。
    void sync_file_record_progress(dw_protocol_t task_protocol, const std::string &task_natural_key,
                                   int32_t status, int64_t total_size, int64_t total_done,
                                   int32_t reason, const std::string &message) const;
    /// 任务状态迁移即时写（权威列）：QUEUED/DOWNLOADING/PAUSED/COMPLETED/ERROR 等关键迁移点调用，
    /// 不受进度遥测节流影响；与 sync_file_record_progress 的差异为不带进度、可带错误文本。
    void update_file_record_status(const std::string &client_id, dw_protocol_t task_protocol,
                                   const std::string &task_natural_key,
                                   dw_task_status_t status, dw_reason_t reason, const std::string &message) const;
    /// 按任务关联三要素（client_id + task_protocol + task_natural_key）更新文件记录的
    /// save_path、original_root_name、root_name、full_path、file_type、ext
    /// （PARSED 后修正为解析时保存目录、判重后根名、磁盘根实体全路径、实际形态、文件后缀）。
    /// full_path = save_path/root_name（目录与单文件统一公式）；
    /// ext 不含点（如 "mp4"），目录场景传空串保持 NULL。
    void update_file_record_meta(const std::string &client_id,
                                 dw_protocol_t task_protocol, const std::string &task_natural_key,
                                 const std::string &task_save_path,
                                 const std::string &original_root_name, const std::string &root_name, const std::string &full_path,
                                 bool file_type, const std::string &ext) const;

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
                              const std::vector<std::tuple<std::string, int32_t, std::vector<dw_byte_range_t>>> &file_ranges) const;
    /// 保存完整区间集合（BT piece 事件驱动）：UPSERT 语义，直接覆盖该文件的 intervals 字段，
    /// 返回该文件累计已下载字节（供调用方判定文件完成）。
    int64_t save_file_progress(const std::string &client_id, dw_protocol_t protocol,
                               const std::string &natural_key, int32_t file_index,
                               const std::string &full_path,
                               const std::string &intervals_json) const;
    /// 按文件删除进度缓存（文件下载完成，区间失去意义）。
    void delete_file_progress_by_file(const std::string &client_id, dw_protocol_t protocol,
                                     const std::string &natural_key, int32_t file_index) const;
    /// 按任务删除全部进度缓存（任务删除级联）。
    void delete_file_progress_by_task(const std::string &client_id, dw_protocol_t protocol,
                                     const std::string &natural_key) const;
    /// 聚合文件累计已下载字节；无记录返回 0。
    int64_t sum_file_progress_by_file(const std::string &client_id, dw_protocol_t protocol,
                                      const std::string &natural_key, int32_t file_index) const;
    /// 按完整路径读取某文件的已下载区间（按 offset_start 升序）；不存在返回空 vector。
    std::vector<dw_byte_range_t> load_segments(const std::string &full_path, int32_t file_index) const;

private:
    sqlite3* db_ = nullptr;
};

} // namespace dw


