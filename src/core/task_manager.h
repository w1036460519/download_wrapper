/**
 * @file task_manager.h
 * @brief 库内任务中枢：SQLite 持久化 + 内存注册表 + 优先级就绪队列 + 事件驱动调度。
 *
 * 设计要点：
 *   - 注册表仅常驻排队 / 活跃任务（DOWNLOADING/QUEUED），
 *     暂停 / 完成 / 错误任务状态即时投影至 file_records 后从内存移除，按需经 add/resume/list 回读；
 *     引擎仅持有当前活跃任务的运行时句柄；
 *   - 注册表 key 为 union_id（"client_id|type|raw_key" 格式），client_id 过滤在 API 入口校验；
 *   - 控制接口统一接 (proto, natural_key)，client_id 从 session 取；
 *   - TaskKey 包装结构已剔除，主键三字段平铺于 FileRecord；
 *   - 事件驱动模型：引擎经 post_engine_event 投递事件（STATUS_UPDATE/PARSED/DOWNLOAD_FAILED/
 *     DOWNLOAD_COMPLETED），B 线程消费并更新 FileRecord 内存，A 线程按固定周期读取并转发上层；
 *   - 断点续传经 on_resume_data 汇入持久化（HTTP worker 线程自推 / BT save_resume_data_alert 事件驱动）；
 *   - 并发准入：活跃任务数 < max_concurrent_downloads 才准入下载，其余置 QUEUED；
 *   - 调度纯事件驱动：状态跃迁释放许可 / 新增 / 恢复 / 调整优先级时唤醒调度线程；
 *   - 引擎启动动作统一在调度线程执行，规避回调线程重入。
 */

#pragma once

#include "download_wrapper/download_wrapper.h"
#include "task_record.h"
#include "task_store.h"
#include "utils/memory_util.h"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <boost/asio.hpp>

namespace dw {
    class IDownloadEngine;
    struct EngineEvent;

    /**
     * 任务中枢单例（由 dw_downloader 持有）。
     */
    class TaskManager {
    public:
        TaskManager() = default;

        ~TaskManager();

        TaskManager(const TaskManager &) = delete;

        TaskManager &operator=(const TaskManager) = delete;

        /// 注入引擎（由 download_wrapper.cpp 在 init 时提供，经统一接口分发）。
        void set_engines(IDownloadEngine *http, IDownloadEngine *torrent);

        /// 打开 DB、建表、加载注册表、启动调度线程；恢复既有任务由调度线程按并发上限重新准入。
        int32_t start(const dw_config_t &cfg);

        /// 停止调度线程、最终刷库、关闭 DB。
        void stop();

        // ---- 控制操作（C ABI 转发到此） ----

        /// 添加任务。
        /// @param client_id 客户端标识（必填）。
        /// @param force 强制重新添加：清理内存与 DB 中的旧记录（含 resume_data），从零开始；
        ///              false 时若任务已存在则仅刷新 created_at 用于排序置顶。
        int32_t add(dw_protocol_t proto, const std::string &client_id, const dw_task_params_t *params,
                    dw_submit_result_t *out, bool force = false);

        int32_t pause(dw_protocol_t proto, const std::string &natural_key, dw_submit_result_t *out) const;

        int32_t resume(dw_protocol_t proto, const std::string &natural_key,
                       dw_submit_result_t *out);

        /// 删除任务：标记 DELETING + 调引擎 delete_task(delete_files)；
        /// 引擎发 DELETED 事件后 wrapper 回收资源 + 按标识删文件。
        int32_t remove(dw_protocol_t proto, const std::string &natural_key, int32_t delete_files,
                       dw_submit_result_t *out);

        int32_t set_priority(dw_protocol_t proto, const std::string &natural_key,
                             const int32_t *priority_file_indexes, int32_t priority_file_index_size);

        /// 读取断点续传数据（三要素定位）；不存在返回空 vector。
        std::vector<uint8_t> load_resume(const std::string &client_id, dw_protocol_t proto,
                                         const std::string &natural_key);

        /// 保存任务来源（save_path / magnet_link / torrent_file），用于 resume data 尚未生成时的兜底恢复。
        void save_resume_source(const std::string &client_id, dw_protocol_t proto,
                                const std::string &natural_key,
                                const std::string &save_path,
                                const std::string &magnet_link, const std::string &torrent_file);
        /// 一次性加载全部恢复信息（data + magnet + torrent）。
        TaskStore::ResumeInfo load_resume_info(const std::string &client_id, dw_protocol_t proto,
                                               const std::string &natural_key);

        /// 读取任务保存目录（三要素定位）；任务不存在返回空串。
        std::string load_save_path(const std::string &client_id, dw_protocol_t proto,
                                   const std::string &natural_key);

        /// 按 (proto, natural_key) 查询任务记录：内存优先，DB 命中时注册入内存。
        /// 供低频工具函数（磁力/文件列表）定位任务。命中返回 true。
        bool load_task_record(dw_protocol_t proto, const std::string &natural_key, FileRecord &out_record);

        // ---- 边下边播缓存（直落 task_store，与协议无关） ----

        /// 写入 / 覆盖文件播放进度（毫秒）。
        void set_play_position(dw_protocol_t proto, const std::string &natural_key, int32_t file_index,
                               int64_t position_ms);

        /// 读取文件播放进度（毫秒）；无记录返回 0。
        int64_t get_play_position(dw_protocol_t proto, const std::string &natural_key, int32_t file_index);

        /// 读取某文件已下载区间快照（任务未加载进引擎时的播放兜底）；无记录返回空 vector。
        std::vector<dw_byte_range_t> load_segments(dw_protocol_t proto, const std::string &natural_key,
                                                   int32_t file_index);

        // ---- 分段内存缓存（STATUS_UPDATE 事件时从引擎拉取并缓存） ----

        /// 读取缓存的已下载区间（代理热路径，无需查引擎或 DB）；未缓存返回空 vector。
        std::vector<dw_byte_range_t> get_cached_segments(dw_protocol_t proto, const std::string &natural_key,
                                                         int32_t file_index);

        /// 查询任务当前状态（mtx_ 保护）；任务不存在返回 -1。
        int32_t get_task_status(dw_protocol_t proto, const std::string &natural_key);

        // ---- 引擎事件消费（Boost.Asio 事件投递入口） ----
        /// 引擎 alert 经 Boost.Asio io_context::post 投递到此，B 线程消费。
        /// 事件经值语义拷贝后投递，线程安全。
        void on_engine_event(EngineEvent event);

        // ---- 快照查询 ----
        int32_t list(dw_task_snapshot_t **out_tasks, int32_t *out_count);

        /// 从数据库加载全部文件目录记录（UI 渲染主表）。
        std::vector<FileRecord> list_file_records();

        /// 同步文件记录到内存缓存（供外部经 store 操作后调用，持 mtx_）。
        /// 缓存未加载时直接返回；已加载时按三要素定位插入或刷新 modified_at。
        void sync_file_record_cache(const std::string &client_id, dw_protocol_t proto,
                                    const std::string &natural_key, const FileRecord *fr = nullptr);

        /// 查找文件记录：优先内存缓存，未命中则从 DB 加载到缓存。
        /// @return 缓存中的 FileRecord 指针，不存在返回 nullptr（假定已持 mtx_）。
        FileRecord *find_file_record(const std::string &client_id, dw_protocol_t proto,
                                     const std::string &natural_key);

        /// 获取文件记录的解析状态：已存在则返回其 parsed 状态；不存在返回 false。
        /// 用于引擎在重名检测前查询文件记录的解析状态。
        /// @return true=已解析（引擎可跳过重名检测），false=未解析或不存在。
        bool is_file_record_parsed(const std::string &client_id, dw_protocol_t proto,
                                   const std::string &natural_key);

        /// 设置流量闸门：allowed=false 时逐任务暂停所有活跃下载（BT/HTTP）并回落 QUEUED，
        /// 调度线程不再准入新任务；true 时唤醒调度按 QUEUED→准入路径自动重启。
        void set_network_allowed(bool allowed);

        /// 运行期调整全局最大并发下载数（<=0 取默认 3）。
        /// 调高后唤醒调度线程准入 QUEUED 任务；调低不中断已运行任务，
        /// 多余名额随任务自然结束逐步收敛。
        void set_max_concurrent(int32_t value);

        // ---- 任务文件实时查询（task_files 表已移除，磁盘为事实源） ----

        /// 解析任务内文件的物理路径与大小（dw_get_task_file_info 消费）。
        /// HTTP：save_path/content_root/name 推导；BT：handle 在线实时查询。
        /// @return false=无法解析（定名未落定 / handle 离线 / 序号越界）。
        bool resolve_file_path(dw_protocol_t proto, const std::string &natural_key,
                               int32_t file_index, std::string &out_path, int64_t &out_size);

        /// 任务文件列表：BT 引擎实时查询（选中文件，handle 离线返回空）；
        /// HTTP 从任务记录推导单文件条目。连续数组由 alloc_file_list 分配，调用方负责释放。
        utils::file_array load_files(dw_protocol_t proto, const std::string &natural_key);

        // ---- 本地文件浏览与管理 ----

        /// 增量扫描本地文件任务：扫描目录，仅添加新文件（不删除旧记录），返回新增任务快照。
        int32_t scan_local_tasks(const std::string &save_path,
                                 dw_task_snapshot_t **out_tasks,
                                 int32_t *out_count);

        /// 校验本地文件任务的存在性：物理文件不存在则标记为 INVALIDATED。
        int32_t validate_local_tasks(const std::string &save_path,
                                     int32_t *out_invalidated_count);

        /// 全量清理指定 save_path 下的本地文件条目（type=0）：DB + 物理文件。
        int32_t clear_local_tasks(const std::string &save_path);

        /// 删除单个本地文件条目（type=0）：仅 DB + 磁盘清理，不涉及 engine 层。
        /// 下载任务（type=1/2）拒绝，应走 dw_delete_task。
        int32_t delete_local_entry(const std::string &save_path, const std::string &root_name);

        // ---- 路径与展示辅助（静态，不依赖实例态） ----

        /// 根据 FileRecord 计算磁盘根路径。
        /// 统一为 save_path / root_name。
        static std::string disk_root_path(const FileRecord &rec);

        /// 根据 FileRecord 计算展示名：root_name（已含可能的去重后缀）。
        static std::string display_name(const FileRecord &rec);

        /// 当前本机 clientId（init 注入，跨所有方法使用）。
        const std::string &client_id() const { return client_id_; }

        // ---- 内部访问器（供同库模块经持锁快照访问持久化层） ----

        /// 返回内部互斥锁引用，供调用方持锁期间安全访问 store_。
        std::mutex &get_mutex() { return mtx_; }
        /// 返回持久化存储层引用（调用方须持 mtx_ 保证线程安全）。
        TaskStore &get_store() { return store_; }

    private:
        // A 线程采集（持 mtx_，单段）：遍历活跃任务，直接读 FileRecord 已被引擎推入
        // 的进度字段；判终态 → 置 schedule_needed_ → push 转发记录。
        // 落库 / 区间快照 / 注销均延后到 B 线程。
        void collect_progress_locked(std::vector<FileRecord> &fwd_records);

        // A 线程（轻量）：周期遍历内存 + 转发回调，不落库 / 不快照 / 不移除 / 不 sweep。
        // stop() 置 running_=false 后由 notify_all 唤醒等待点并退出循环。
        void scheduler_loop();

        // B 线程（重载）：较长节拍或被 schedule 唤醒，持锁完成持久化 / 区间快照 / 终态注销 / 准入，随后锁外 sweep。
        void maintenance_loop();

        // B 线程消费单个引擎事件（PARSED/DOWNLOAD_FAILED/DOWNLOAD_COMPLETED/STATUS_UPDATE/
        // RESUME_DATA/PAUSED/RESUMED/DELETED）。
        void consume_engine_event(EngineEvent event);

        // B 线程持锁持久化：为下载中 / 终态任务落区间快照（引擎 ctx 尚在），同步进度遥测与暂存续传，最后注销终态任务。
        void maintenance_persist_locked();

        // 统一进度转发：由记录（权威态 + 采集遥测）构造 dw_progress_t 发上层（不持 mtx_）；
        // remaining/eta 由 total_size/total_done/download_rate 现算。QUEUED/PAUSED 合成帧同走此路径，
        // 遥测字段已在状态迁移时归零。
        void emit_progress(const FileRecord &rec);

        // 准入队列中任务直到占满并发额度（在调度线程，准入操作均在释锁后执行）。
        void run_schedule(std::unique_lock<std::mutex> &lock);

        // 在引擎恢复任务（不持 mtx_）；引擎内部经三要素自取 resume_data，双行为：
        // handle/ctx 存在直接恢复，不存在则重建。
        bool call_resume_task(const FileRecord &task_record);

        // 复位运行态遥测（速率/探测/原因/消息）：任务离开活跃态转 PAUSED/QUEUED 时调用，避免合成帧残留旧速率。
        static void reset_live_telemetry(FileRecord &rec);

        // ---- 内部工具 ----
        int32_t active_count_locked() const; // 占用下载额度的任务数
        void flush_dirty_locked(); // 同步任务进度遥测到 file_records（节流写，假定已持 mtx_）

        // 确保 file_cache_ 已从 DB 全量加载（假定已持 mtx_；已加载则幂等跳过）。
        void ensure_file_cache_locked();

        // 按协议取引擎（统一接口分发点；HTTP/BT 之外无其他协议）
        IDownloadEngine *engine_of(dw_protocol_t proto) const;

        // 解析任务内文件的物理路径与大小（假定已持 mtx_）：HTTP 从任务记录推导
        // （wrapper 模型 save_path/content_root/name）；BT 经引擎 handle 实时查询。
        // 返回 false=任务不存在 / 定名未落定 / handle 离线 / 序号越界。
        bool resolve_file_path_locked(dw_protocol_t proto, const std::string &natural_key,
                                      int32_t file_index, std::string &out_path,
                                      int64_t &out_size);

        // 内存注册：union_id → FileRecord，无冗余索引。
        void register_task(FileRecord task_record);

        // 按 natural_key 查询任务：内存优先，未命中则从 file_records（状态持久化权威）
        // 重建并注册入内存。成功返回内存中任务的指针（可直接修改）；
        // 任务不存在返回 nullptr。持 mtx_ 调用。
        FileRecord *load_task_record_locked(const std::string &client_id, dw_protocol_t proto,
                                            const std::string &natural_key);

        // 注销：清 tasks_，union_id 定位。
        void unregister_task(const std::string &union_id);

        // HTTP 周期快照：单文件已下载连续区间全量重写落库（file_index=0）；
        // BT 进度由 FILE_PROGRESS 事件（piece 驱动）增量维护不经此路径。
        // 供任务未加载进引擎时的播放兜底。假定已持 mtx_，仅短暂访问引擎自有锁，无死锁。
        // 同时检查文件/任务级完成条件。
        void snapshot_segments_locked(FileRecord &task_record);

        std::mutex mtx_;
        std::condition_variable cv_;
        // 任务主表：union_id → FileRecord。仅常驻活跃/排队任务，暂停/完成/错误
        // 状态投影至 file_records 后由 unregister_task 清出。
        std::unordered_map<std::string, FileRecord> tasks_;
        // Boost.Asio 事件队列：引擎 alert 经此投递，B 线程 maintenance_loop 中 poll 消费。
        boost::asio::io_context event_ioc_;

        // 文件目录内存缓存：三要素平铺，key = union_id(client_id|protocol|natural_key)。
        // 懒加载：首次 list_file_records() 从 DB 全量载入，后续直接返回缓存。
        // 写穿：insert/update/delete 同步更新缓存；无法定位 key 的操作直接失效重载。
        std::unordered_map<std::string, FileRecord> file_cache_;
        bool file_cache_loaded_ = false;

        TaskStore store_; // 持久化存储层（持有 sqlite3 连接，析构自动关闭）
        std::thread worker_; // A 线程：轻量采集 + 回调（stop() 显式 join）
        std::thread maintenance_; // B 线程：持久化 + 区间快照 + 终态注销 + 准入 + sweep
        std::atomic<bool> running_{false}; // 生命周期标志：start 准入 / stop 幂等守卫 / run_schedule 准入闸门
        bool schedule_needed_ = false; // 调度线程需被唤醒
        bool net_allowed_ = true; // 流量闸门：false=关闭（不准入新任务）；默认开启，不持久化，由调用方重启后重新下发
        int32_t max_concurrent_ = 3;
        int32_t flush_interval_ms_ = 1000; // 数据采集/回调节拍
        int32_t maintenance_interval_ms_ = 2000; // 内存数据持久化节拍

        IDownloadEngine *http_ = nullptr;
        IDownloadEngine *torrent_ = nullptr;
        std::string client_id_; // App 启动时注入的 UUIDv4

        // 由 (client_id, protocol, raw_key) 构造 union_id，供 tasks_ 查找。
        static std::string union_id_of(const std::string &client_id, const dw_protocol_t proto, const std::string &raw_key) {
            return client_id + '|' + std::string(to_string(proto)) + '|' + raw_key;
        }
        std::string union_id_of(const dw_protocol_t proto, const std::string &raw_key) const {
            return client_id_ + '|' + std::string(to_string(proto)) + '|' + raw_key;
        }
    };
} // namespace dw
