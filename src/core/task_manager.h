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
#include "internal/downloader_internal.h"
#include "task_record.h"
#include "task_store.h"
#include "utils/memory_util.h"

#include <atomic>
#include <cstdint>
#include <map>
#include <mutex>
#include <optional>
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

        /// 校验并深拷贝配置到内部；参数非法时返回失败原因。
        dw_submit_result_t apply_config(const Config &cfg);

        /// 当前有效配置（已校验，引擎直接消费无需再判合法）。
        const Config &config() const { return config_; }

        /// 打开 DB、建表、加载注册表、启动调度线程；恢复既有任务由调度线程按并发上限重新准入。
        int32_t start();

        /// 停止调度线程、最终刷库、关闭 DB。
        void stop();

        /// 获取默认保存目录（由 dw_config_t.save_path 初始化）。
        const std::string &save_path() const { return config_.save_path; }

        // ---- 控制操作（C ABI 转发到此） ----

        /// 添加任务（三要素从 params 取，无需单独传递）。
        dw_submit_result_t add(TaskParams &params);

        dw_submit_result_t pause(const TaskParams &params) const;

        dw_submit_result_t resume(const TaskParams &params);

        /// 删除任务：标记 DELETING + 调引擎 delete_task(delete_files)；
        /// 引擎发 DELETED 事件后 wrapper 回收资源 + 按标识删文件。
        dw_submit_result_t remove(const TaskParams &params);

        /// 保存任务来源（save_path / magnet_link / torrent_file），用于 resume data 尚未生成时的兜底恢复。
        void save_resume_source(const std::string &client_id, dw_protocol_t proto,
                                const std::string &natural_key,
                                const std::string &save_path,
                                const std::string &magnet_link, const std::string &torrent_file);
        /// 一次性加载全部恢复信息（data + magnet + torrent）。
        TaskStore::ResumeInfo load_resume_info(const std::string &client_id, dw_protocol_t proto,
                                               const std::string &natural_key);

        /// 按 (client_id, proto, natural_key) 查询任务记录：内存优先，DB 命中时注册入内存。
        /// 返回内存中任务的指针（可直接修改）；未找到返回 nullptr。
        /// recursive_mutex 支持重入，外部调用方须持锁或自行加锁。
        FileRecord *load_task_record(const std::string &client_id, dw_protocol_t proto, const std::string &natural_key);

        // ---- 边下边播缓存（直落 task_store，与协议无关） ----

        /// 写入 / 覆盖文件播放进度（毫秒）。
        void set_play_position(dw_protocol_t proto, const std::string &natural_key, int32_t file_index,
                               int64_t position_ms);

        /// 读取文件播放进度（毫秒）；无记录返回 0。
        int64_t get_play_position(dw_protocol_t proto, const std::string &natural_key, int32_t file_index);

        /// 查询任务的文件进度缓存（按三要素查询）；不存在返回空 vector。
        std::vector<FileProgressInfo> get_file_progress(const std::string &client_id,
                                                         dw_protocol_t proto,
                                                         const std::string &natural_key);

        // ---- 引擎事件消费（Boost.Asio 事件投递入口） ----
        /// 引擎 alert 经 Boost.Asio io_context::post 投递到此，B 线程消费。
        /// 事件经值语义拷贝后投递，线程安全。
        void on_engine_event(EngineEvent event);

        // ---- 快照查询 ----
        /// 查询全部任务记录（供 App 启动恢复列表）。
        std::vector<FileRecord> list();

        /// 查找文件记录：优先内存（tasks_），未命中则从 DB 加载并注册入内存。
        /// @return 内存中的 FileRecord 指针，不存在返回 nullptr。
        FileRecord *find_file_record(const std::string &client_id, dw_protocol_t proto,
                                     const std::string &natural_key);

        /// 获取当前配置（供运行期更新）。
        const Config &get_config() const { return config_; }

        // ---- 任务文件实时查询（task_files 表已移除，磁盘为事实源） ----

        /// 解析任务内文件的物理路径与大小（dw_get_task_file_info 消费）。
        /// HTTP：save_path/content_root/name 推导；BT：handle 在线实时查询。
        /// @return false=无法解析（定名未落定 / handle 离线 / 序号越界）。
        bool resolve_file_path(dw_protocol_t proto, const std::string &natural_key,
                               int32_t file_index, std::string &out_path, int64_t &out_size);

        /// 根据目录路径查询下一级文件和目录信息（不递归）。
        /// @param dir_path 目录路径
        /// @return 文件和目录列表（FileInfo，目录的 size=0）
        std::vector<FileInfo> get_files(const std::string &dir_path) const;

        // ---- 本地文件浏览与管理 ----

        /// 扫描本地文件：校验全量记录存在性 + 增量发现新文件/目录。
        /// @param catalog_paths 扫描目录路径集合（用于判断 save_path 是否仍有效）
        /// @return affected_count = 本次新增记录数（失效/删除记录经回调通知，不计入）
        dw_submit_result_t scan_local_file(const std::vector<std::string> &catalog_paths);

        // ---- BT 解析工具（转发至 TorrentEngine 静态方法） ----

        /// 解析磁力链接获取 info_hash。
        dw_submit_result_t parse_magnet(const std::string &magnet_link);

        /// 解析 .torrent 文件获取 info_hash 和文件列表。
        dw_submit_result_t parse_torrent_file(const std::string &torrent_file_path);

        /// 当前本机 clientId（从 config_ 读取）。
        const std::string &client_id() const { return config_.client_id; }

        // ---- 内部访问器（供同库模块经持锁快照访问持久化层） ----

        /// 返回内部互斥锁引用，供调用方持锁期间安全访问 store_。
        std::recursive_mutex &get_mutex() { return mtx_; }
        /// 返回持久化存储层引用（调用方须持 mtx_ 保证线程安全）。
        TaskStore &get_store() { return store_; }

    private:
        /// 本机任务添加（入队等待调度）。
        dw_submit_result_t self_add(TaskParams &params);

        /// 远程任务添加（预留扩展）。
        dw_submit_result_t remote_add(TaskParams &params);

        /// 本机任务暂停。
        dw_submit_result_t self_pause(const TaskParams &params) const;

        /// 远程任务暂停。
        dw_submit_result_t remote_pause(const TaskParams &params) const;

        /// 本机任务恢复。
        dw_submit_result_t self_resume(const TaskParams &params);

        /// 远程任务恢复。
        dw_submit_result_t remote_resume(const TaskParams &params);

        /// 本机任务删除。
        dw_submit_result_t self_remove(const TaskParams &params);

        /// 远程任务删除。
        dw_submit_result_t remote_remove(const TaskParams &params);

        // A 线程（轻量）：周期任务调度准入。
        // stop() 置 running_=false 后由 notify_all 唤醒等待点并退出循环。
        void scheduler_loop();

        // B 线程（重载）：周期持久化 dirty 记录 + 推送 JSON + 引擎 sweep。
        void maintenance_loop();

        // C 线程：周期消费引擎事件（500ms 节拍 poll event_ioc_）。
        void event_consumer_loop();

        // 消费单个引擎事件（PARSED/DOWNLOAD_FAILED/DOWNLOAD_COMPLETED/STATUS_UPDATE/
        // RESUME_DATA/PAUSED/RESUMED/DELETED）。
        void consume_engine_event(const EngineEvent &event);

        // ---- 内部工具 ----
        int32_t active_count_locked() const; // 占用下载额度的任务数

        // 按协议取引擎（统一接口分发点；HTTP/BT 之外无其他协议）
        IDownloadEngine *engine_of(dw_protocol_t proto) const;

        // 解析任务内文件的物理路径与大小（假定已持 mtx_）：HTTP 从任务记录推导
        // （wrapper 模型 save_path/content_root/name）；BT 经引擎 handle 实时查询。
        // 返回 false=任务不存在 / 定名未落定 / handle 离线 / 序号越界。
        bool resolve_file_path_locked(dw_protocol_t proto, const std::string &natural_key,
                                      int32_t file_index, std::string &out_path,
                                      int64_t &out_size);

        // 填充 FileInfo 列表的下载进度（根据 full_path 匹配 file_progress_cache 表）。
        void fill_file_progress(std::vector<FileInfo> &files) const;

        // 内存注册：union_id → FileRecord，无冗余索引。
        void register_task(FileRecord task_record);

        // 注销：清 tasks_，union_id 定位。
        void unregister_task(const std::string &union_id);

        // 兜底删除任务数据目录：self_remove 与 DELETED 分支共用，含路径逃逸防护。
        static void remove_data_directory(const std::string &save_path,
                                          const std::string &root_name,
                                          const std::string &original_root_name);

        std::recursive_mutex mtx_;
        // 任务主表：union_id → FileRecord。常驻活跃/排队任务，后续引入淘汰策略。
        std::unordered_map<std::string, FileRecord> tasks_;
        // Boost.Asio 事件队列：引擎 alert 经此投递，event_consumer_loop 阻塞消费。
        boost::asio::io_context event_ioc_;
        std::optional<boost::asio::executor_work_guard<boost::asio::io_context::executor_type>> event_work_guard_;

        TaskStore store_; // 持久化存储层（持有 sqlite3 连接，析构自动关闭）
        std::thread worker_; // A 线程：任务调度准入（stop() 显式 join）
        std::thread maintenance_; // B 线程：持久化 + JSON 推送 + sweep
        std::thread event_consumer_; // C 线程：引擎事件阻塞消费（投递即响应）
        std::atomic<bool> running_{false}; // 生命周期标志：start 准入 / stop 幂等守卫 / scheduler_loop 准入闸门
        bool net_allowed_ = true; // 网络允许标志：由 apply_config 根据 network_type + allow_mobile_data 计算
        Config config_; // 权威配置快照（深拷贝，已校验）
        static constexpr int32_t flush_interval_ms_ = 1000; // 调度线程节拍（内部固定）
        static constexpr int32_t maintenance_interval_ms_ = 1000; // 持久化线程节拍（内部固定）

        IDownloadEngine *http_ = nullptr;
        IDownloadEngine *torrent_ = nullptr;

        // 由 (client_id, protocol, raw_key) 构造 union_id，供 tasks_ 查找。
        static std::string union_id_of(const std::string &client_id, const dw_protocol_t proto, const std::string &raw_key) {
            return client_id + '|' + std::string(to_string(proto)) + '|' + raw_key;
        }
        std::string union_id_of(const dw_protocol_t proto, const std::string &raw_key) const {
            return config_.client_id + '|' + std::string(to_string(proto)) + '|' + raw_key;
        }
    };
} // namespace dw
