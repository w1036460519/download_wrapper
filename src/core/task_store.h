/**
 * @file task_store.h
 * @brief 任务持久化存储层：封装 SQLite 连接与全部读写操作。
 *
 * 职责边界：
 *   - 仅负责 TaskRecord 落库 / 回读与 resume_data 存取，不触碰内存注册表与调度逻辑；
 *   - 表结构自维护：建表（IF NOT EXISTS）；
 *   - 不做并发保护，要求调用方自行串行化（TaskManager 在持有 mtx_ 时调用）。
 */

#ifndef DW_TASK_STORE_H
#define DW_TASK_STORE_H

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
    /// 建表（IF NOT EXISTS）：自增 id 主键；去重由业务层按 url / info_hash 处理，无唯一约束。
    void init_schema();

    /// 载入排队 / 活跃任务（DOWNLOADING/QUEUED，兼容旧库 5/6），全字段填充。
    std::vector<TaskRecord> load_active();
    /// 载入全部任务（含暂停 / 完成 / 错误），用于快照列表。
    std::vector<TaskRecord> load_all();
    /// 按 url 去重回读（HTTP）。命中返回 true 并填充 out。仅供 add 去重。
    bool load_by_url(const std::string& url, TaskRecord& out);
    /// 按 info_hash 去重回读（BT）。命中返回 true 并填充 out。仅供 add 去重。
    bool load_by_info_hash(const std::string& info_hash, TaskRecord& out);
    /// 按自增 id 载入全字段（含分片续传态）；供低频控制操作。命中返回 true。
    bool load_by_id(int64_t id, TaskRecord& out);
    /// 查询同 save_path 下（排除 exclude_id 自身）已占用的名称集合：
    /// 合并 name / filename 两列非空值，供唯一名判重（库即持久预留，跨重启有效）。
    std::vector<std::string> load_names_by_save_path(const std::string& save_path,
                                                     int64_t exclude_id);
    /// 新增任务：纯 INSERT，回填自增主键到 r.id。仅 add 未命中去重时调用。
    void insert(TaskRecord& r);
    /// 更新既有任务：按 id 原地 UPDATE 全字段（要求 r.id 有效）。add 之外的操作统一走此路径。
    void update(const TaskRecord& r);
    /// 删除任务及其 resume_data / task_files（三表统一按自增 id）。
    void remove(int64_t id);
    /// 写入 / 覆盖断点续传数据（key 为自增 id）。
    void save_resume(int64_t id, const uint8_t* data, size_t size);
    /// 读取断点续传数据（key 为自增 id）；不存在返回空 vector。
    std::vector<uint8_t> load_resume(int64_t id);
    /// 仅清除某任务的断点续传数据（key 为自增 id）；用于错误任务重下前丢弃残留存档。
    void clear_resume(int64_t id);

    // ---- 任务文件信息 ----

    /// 全量重写任务节点树（先删后插，事务包裹；key 为自增 id）。
    void save_task_files(int64_t id,
                         const std::vector<dw_file_info_t>& files);
    /// 探测任务是否已有文件节点记录（存在即已判重定名的凭证）。
    bool has_task_files(int64_t id);
    /// 加载任务节点树（按 created_at 升序；key 为自增 id）；不存在返回空 vector。
    std::vector<dw_file_info_t> load_task_files(int64_t id);
    /// 删除任务关联的全部节点记录（key 为自增 id）。
    void delete_task_files(int64_t id);
    /// 任务级 0→2 传播：将全部文件节点置为完成正常（不触碰已删除态）。
    void mark_task_files_completed(int64_t id);

    // ---- 边下边播缓存 ----

    /// 写入 / 覆盖文件播放进度（毫秒）；upsert，key 为 (task_id, file_index)。
    void set_play_position(int64_t id, int32_t file_index, int64_t position_ms);
    /// 读取文件播放进度（毫秒）；无记录返回 0。
    int64_t get_play_position(int64_t id, int32_t file_index);

    // ---- 已下载区间快照 ----

    /// 覆盖写入某文件的已下载连续区间快照（事务：先删该 file_index 旧区间再批量写；空即清空）。
    void save_segments(int64_t id, int32_t file_index,
                       const std::vector<dw_byte_range_t>& segments);
    /// 读取某文件的已下载区间快照（按 seg_start 升序）；不存在返回空 vector。
    std::vector<dw_byte_range_t> load_segments(int64_t id, int32_t file_index);
    /// 清除某任务的全部区间快照（error 重下用）。
    void clear_segments(int64_t id);

private:
    sqlite3* db_ = nullptr;
};

} // namespace dw

#endif /* DW_TASK_STORE_H */
