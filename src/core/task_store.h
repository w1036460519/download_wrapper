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
    /// 按 source 清理全部非下载任务（用于批量清理，不限 save_path）。
    void clear_tasks_by_source(int source);
    /// 新增任务：纯 INSERT，复合键 (client_id, key_type, natural_key) 须由 r 三字段预填。
    void insert(TaskRecord& r);
    /// 更新既有任务：按复合键原地 UPDATE 全字段。
    void update(const TaskRecord& r);
    /// 仅更新任务状态（轻量操作，避免全字段 UPDATE）。
    void update_status(const std::string &client_id, dw_protocol_t protocol,
                       const std::string &natural_key, int32_t status);
    /// 删除任务及其 resume_data / task_files / file_segments（统一按复合键）。
    void remove(const std::string &client_id, dw_protocol_t protocol, const std::string &natural_key);
    /// 写入 / 覆盖断点续传数据。
    void save_resume(const std::string &client_id, dw_protocol_t protocol,
                     const std::string &natural_key, const uint8_t* data, size_t size);
    /// 读取断点续传数据；不存在返回空 vector。
    std::vector<uint8_t> load_resume(const std::string &client_id, dw_protocol_t protocol,
                                     const std::string &natural_key);
    /// 仅清除某任务的断点续传数据。
    void clear_resume(const std::string &client_id, dw_protocol_t protocol, const std::string &natural_key);
    
    // ---- 任务文件信息 ----
    
    /// 全量重写任务节点树（先删后插，事务包裹）。
    void save_task_files(const std::string &client_id, dw_protocol_t protocol,
                         const std::string &natural_key,
                         const std::vector<dw_file_info_t>& files);
    /// 加载任务节点树（按 file_index 升序）；不存在返回空 vector。
    std::vector<dw_file_info_t> load_task_files(const std::string &client_id, dw_protocol_t protocol,
                                                const std::string &natural_key);
    /// 任务级 0→2 传播：将全部文件节点置为完成正常（不触碰已删除态）。
    void mark_task_files_completed(const std::string &client_id, dw_protocol_t protocol,
                                   const std::string &natural_key);
    /// 单文件完成标记：将指定 (key, file_index) 的文件节点置为完成（幂等，已删除态不触碰）。
    void mark_file_completed(const std::string &client_id, dw_protocol_t protocol,
                             const std::string &natural_key, int32_t file_index);
    /// 批量更新文件的已下载字节数（事务包裹）；由 snapshot_segments_locked 从 ranges 推导后调用。
    void update_downloaded_bytes(const std::string &client_id, dw_protocol_t protocol,
                                 const std::string &natural_key,
                                 const std::vector<std::pair<int32_t, int64_t>>& file_bytes);
    /// 单条文件节点懒创建 / 进度推送（upsert）：存在则更新 downloaded_bytes（必要时 size），
    /// 不存在则插入一行（name='' 探测占位、offset=0、status=0）。HTTP 探测定名时、BT
    /// 元数据全量上报之外的进度推送均走此路径，保证 task_files 随进度按需落地，不必
    /// 一次性写齐。元数据靠后续 save_task_files 全量写时补齐（name/ext/offset）。
    void upsert_task_file(const std::string &client_id, dw_protocol_t protocol,
                          const std::string &natural_key, int32_t file_index,
                          int64_t downloaded_bytes, int64_t total_size);
    
    // ---- 边下边播缓存（已合并到 task_files.play_position_ms）----
    
    /// 写入 / 覆盖文件播放进度（毫秒）；直接 UPDATE task_files。
    void set_play_position(const std::string &client_id, dw_protocol_t protocol,
                           const std::string &natural_key, int32_t file_index, int64_t position_ms);
    /// 读取文件播放进度（毫秒）；无记录返回 0。
    int64_t get_play_position(const std::string &client_id, dw_protocol_t protocol,
                              const std::string &natural_key, int32_t file_index);
    
    // ---- 已下载区间快照 ----
    
    /// 覆盖写入某文件的已下载连续区间快照（事务：先删该 file_index 旧区间再批量写；空即清空）。
    void save_segments(const std::string &client_id, dw_protocol_t protocol,
                       const std::string &natural_key, int32_t file_index,
                       const std::vector<dw_byte_range_t>& segments);
    /// 批量覆盖写入多文件的已下载区间快照（单事务）；替代逐文件 save_segments 调用。
    void save_segments_batch(const std::string &client_id, dw_protocol_t protocol,
                             const std::string &natural_key,
                             const std::vector<std::pair<int32_t, std::vector<dw_byte_range_t>>>& file_segments);
    /// 读取某文件的已下载区间快照（按 seg_start 升序）；不存在返回空 vector。
    std::vector<dw_byte_range_t> load_segments(const std::string &client_id, dw_protocol_t protocol,
                                               const std::string &natural_key, int32_t file_index);
    /// 清除某任务的全部区间快照（error 重下用）。
    void clear_segments(const std::string &client_id, dw_protocol_t protocol, const std::string &natural_key);

private:
    sqlite3* db_ = nullptr;
};

} // namespace dw


