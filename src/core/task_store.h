/**
 * @file task_store.h
 * @brief 任务持久化存储层：封装 SQLite 连接与全部读写操作。
 *
 * 职责边界：
 *   - 仅负责 TaskRecord 落库 / 回读与 resume_data 存取，不触碰内存注册表与调度逻辑；
 *   - 表结构自维护：建表（IF NOT EXISTS）；
 *   - 不做并发保护，要求调用方自行串行化（TaskManager 在持有 mtx_ 时调用）。
 *
 * 唯一键约定：
 *   - 任务主键 = (client_id, key_type, natural_key) 复合键
 *   - 客户端标识 client_id 由 App 启动时注入（UUIDv4）
 *   - key_type：0=http(url) 1=bt(info_hash) 2=local(content_root)
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
    /// 建表（IF NOT EXISTS）：复合主键 (client_id, key_type, natural_key)；
    /// 检测到旧 schema（task_id 列）则 DROP 全部表重建（项目未上线）。
    void init_schema();

    /// 载入指定客户端的排队 / 活跃任务（DOWNLOADING/QUEUED/RESOLVING/PARSED），全字段填充。
    std::vector<TaskRecord> load_active(const std::string &client_id);
    /// 载入全部任务（含暂停 / 完成 / 错误），用于快照列表。
    std::vector<TaskRecord> load_all();
    /// 按 (client_id, key_type, natural_key) 查记录：key_type 决定 natural_key 语义
    /// （HTTP=url / BT=info_hash / LOCAL=content_root）。命中返回 true 并填充 out。
    bool load_by_natural_key(const std::string &client_id, dw_protocol_t protocol,
                             const std::string &natural_key, TaskRecord &out) const;
    /// 按 save_path 载入全部任务（用于本地文件浏览：排除下载任务占用的目录条目）。
    std::vector<TaskRecord> load_tasks_by_save_path(const std::string& save_path);
    /// 清理指定 save_path 下的非下载任务（source IN (1,2)）；用于全量清理本地/远程文件任务。
    void clear_local_tasks(const std::string& save_path);
    /// 新增任务：纯 INSERT，复合键 (client_id, key_type, natural_key) 须由 r 三字段预填。
    void insert(TaskRecord& r);
    /// 更新既有任务：按复合键原地 UPDATE 全字段。
    void update(const TaskRecord& r);
    /// 仅更新任务状态（轻量操作，避免全字段 UPDATE）。
    void update_status(const std::string &client_id, dw_protocol_t protocol,
                       const std::string &natural_key, int32_t status);
    /// 删除任务及其 resume_data / file_progress_cache（统一按复合键）。
    void remove(const std::string &client_id, dw_protocol_t protocol, const std::string &natural_key);
    /// 写入 / 覆盖断点续传数据。
    void save_resume(const std::string &client_id, dw_protocol_t protocol,
                     const std::string &natural_key, const uint8_t* data, size_t size);
    /// 读取断点续传数据；不存在返回空 vector。
    std::vector<uint8_t> load_resume(const std::string &client_id, dw_protocol_t protocol,
                                     const std::string &natural_key);
    /// 仅清除某任务的断点续传数据。
    void clear_resume(const std::string &client_id, dw_protocol_t protocol, const std::string &natural_key);

    // ---- 文件目录表（file_records）----

    /// 新增文件记录：自增 id 回填到 r.id。
    void insert_file_record(FileRecord &r);
    /// 按任务关联三要素（client_id + task_protocol + task_natural_key）判存，
    /// 用于占位插入的幂等检查，避免重复新建。
    bool has_file_record(const std::string &client_id, dw_protocol_t task_protocol,
                         const std::string &task_natural_key) const;
    /// 载入指定客户端的全部文件记录（按 modified_at DESC）。
    std::vector<FileRecord> load_file_records(const std::string &client_id);
    /// 同步文件记录的进度冗余字段（status/total_size/total_done），免全字段 UPDATE。
    void sync_file_record_progress(dw_protocol_t task_protocol, const std::string &task_natural_key,
                                   int32_t status, int64_t total_size, int64_t total_done);
    /// 按任务关联三要素（client_id + task_protocol + task_natural_key）更新文件记录的
    /// root_name、full_path、file_type 和 ext（PARSED 后修正为判重后根名、
    /// 磁盘根实体全路径、实际形态与文件后缀）。
    /// full_path = save_path/root_name（目录与单文件统一公式）；
    /// ext 不含点（如 "mp4"），目录场景传空串保持 NULL。
    void update_file_record_meta(const std::string &client_id,
                                 dw_protocol_t task_protocol, const std::string &task_natural_key,
                                 const std::string &root_name, const std::string &full_path,
                                 bool file_type, const std::string &ext);

    // ---- 播放进度（独立表 play_progress，以物理路径为键）----

    /// 写入 / 覆盖文件播放进度（毫秒）。物理路径由调用方（TaskManager）解析后直传。
    void set_play_position(const std::string &file_path, int64_t position_ms);
    /// 读取文件播放进度（毫秒）；无记录返回 0。
    int64_t get_play_position(const std::string &file_path);
    
    // ---- 文件下载进度缓存（file_progress_cache：三要素 + file_index 定位，物理路径供 App 关联）----
    // intervals 为 JSON 序列化区间集合：[[start1,end1],[start2,end2],...]；engine 侧按 1% 文件大小（兑底 4 piece）判定后才入库。

    /// 全量重写多个文件的进度区间（单事务）：先删该文件旧区间再插入，HTTP 周期快照用。
    void replace_file_progress(const std::string &client_id, dw_protocol_t protocol,
                              const std::string &natural_key,
                              const std::vector<std::tuple<std::string, int32_t, std::vector<dw_byte_range_t>>> &file_ranges);
    /// 保存完整区间集合（BT piece 事件驱动）：UPSERT 语义，直接覆盖该文件的 intervals 字段，
    /// 返回该文件累计已下载字节（供调用方判定文件完成）。
    int64_t save_file_progress(const std::string &client_id, dw_protocol_t protocol,
                               const std::string &natural_key, int32_t file_index,
                               const std::string &physical_path,
                               const std::string &intervals_json);
    /// 按文件删除进度缓存（文件下载完成，区间失去意义）。
    void delete_file_progress_by_file(const std::string &client_id, dw_protocol_t protocol,
                                     const std::string &natural_key, int32_t file_index);
    /// 按任务删除全部进度缓存（任务删除级联）。
    void delete_file_progress_by_task(const std::string &client_id, dw_protocol_t protocol,
                                     const std::string &natural_key);
    /// 聚合文件累计已下载字节；无记录返回 0。
    int64_t sum_file_progress_by_file(const std::string &client_id, dw_protocol_t protocol,
                                      const std::string &natural_key, int32_t file_index) const;
    /// 按物理路径读取某文件的已下载区间（按 offset_start 升序）；不存在返回空 vector。
    std::vector<dw_byte_range_t> load_segments(const std::string &physical_path, int32_t file_index) const;

private:
    sqlite3* db_ = nullptr;
};

} // namespace dw


