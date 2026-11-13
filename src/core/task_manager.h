/**
 * @file task_manager.h
 * @brief 库内任务中枢：SQLite 持久化 + 内存注册表 + 优先级就绪队列 + 周期调度。
 *
 * 设计要点：
 *   - 内存任务按 Task::task_id 索引；
 *   - HTTP worker 和 BT 引擎各自保存断点续传数据；
 *   - 并发准入：活跃任务数 < max_concurrent_downloads 才准入下载，其余置 QUEUED；
 *   - 引擎启动动作统一在调度线程执行，规避回调线程重入。
 */

#pragma once

#include "download_wrapper/download_wrapper.h"
#include "internal/downloader_internal.h"
#include "store/task_store.h"
#include "store/task_resume_store.h"
#include "store/task_file_store.h"
#include "sqlite_database.h"

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace dw {
    class IDownloadEngine;

    /**
     * 任务中枢单例（由 dw_downloader 持有）。
     */
    class TaskManager {
    public:
        explicit TaskManager();

        ~TaskManager();

        TaskManager(const TaskManager &) = delete;

        TaskManager &operator=(const TaskManager) = delete;


        /// 校验并深拷贝配置到内部；参数非法时返回失败原因。
        dw_result<> config(const Config &cfg);

        /// 当前有效配置（已校验，引擎直接消费无需再判合法）。
        const Config &config() const { return config_; }

        /// 打开数据库、初始化引擎并启动维护线程。
        dw_result<> init();

        /// 停止调度线程、最终刷库、关闭 DB。
        // 退出时可保留数据库供引擎保存最终快照，析构时统一关闭。
        void stop();

        /// 获取默认保存目录（由 配置中的 save_path 初始化）。
        const std::string &save_path() const { return config_.save_path; }

        // ---- 控制操作（C ABI 转发到此） ----

        /// 添加任务（三要素从 params 取，无需单独传递）。
        dw_result<> add(const TaskParams &params) const;

        dw_result<> pause(const TaskParams &params) const;

        dw_result<> resume(const TaskParams &params) const;

        /// 向对应引擎提交删除请求。
        dw_result<> remove(const TaskParams &params) const;

        // ---- 快照查询 ----
        /// 查询所有未删除的任务及其文件（供 App 启动恢复列表）。
        std::vector<Task> list();

        /// 获取当前配置（供运行期更新）。
        const Config &get_config() const { return config_; }

        /// 根据目录路径查询下一级文件和目录信息（不递归）。
        /// @param dir_path 目录路径
        /// @return 文件和目录列表（TaskFile，目录的 total_size=0）
        std::vector<TaskFile> get_files(const std::string &dir_path) const;

        // ---- BT 解析工具（转发至 TorrentEngine 静态方法） ----

        /// 解析磁力链接获取 info_hash。
        static dw_result<std::string> parse_magnet(const std::string &magnet_link);

        /// 解析 .torrent 文件获取 info_hash 和文件列表。
        static dw_result<std::vector<TaskFile>> parse_torrent_file(const std::string &torrent_file_path, std::string *info_hash = nullptr);

        /// 当前本机 clientId（从 config_ 读取）。
        const std::string &client_id() const { return config_.client_id; }

        // ---- 内部访问器（供同库模块经持锁快照访问持久化层） ----

        /// 返回内部互斥锁引用，供调用方持锁期间安全访问 store_。
        std::recursive_mutex &get_mutex() { return mtx_; }
        /// 返回持久化存储层引用（调用方须持 mtx_ 保证线程安全）。
        TaskStore &get_task_store() { return task_store_; }

        /// 内存未命中时加载任务及文件。使用返回指针期间须持有 get_mutex()。
        Task *get_engine_task(const std::string &key);


        TaskResumeStore &get_task_resume_store() { return task_resume_store_; }

    private:
        /// 任务调度 调度准入任务
        void scheduler_loop();

        // B 线程（重载）：周期持久化 dirty 记录 + 推送 JSON + 引擎 sweep。
        void maintenance_loop();

        // ---- 内部工具 ----
        // 按协议取引擎（统一接口分发点；HTTP/BT 之外无其他协议）
        IDownloadEngine *engine_of(dw_protocol_t proto) const;


        // 删除任务数据目录，含路径逃逸防护。
        static void remove_data_directory(const std::string &save_path,
                                          const std::string &root_name,
                                          const std::string &original_root_name);

        mutable std::recursive_mutex mtx_;
        // 任务主表：task_id → Task。
        std::unordered_map<std::string, Task> tasks_;

        SqliteDatabase database_; // 统一连接生命周期，先于 store 构造、后于 store 析构。
        TaskFileStore task_file_store_{database_.connection()};
        TaskStore task_store_{database_.connection()};
        TaskResumeStore task_resume_store_{database_.connection()};
        std::thread worker_; // A 线程：任务调度准入（stop() 显式 join）
        std::thread maintenance_; // B 线程：持久化 + JSON 推送 + sweep
        std::atomic<bool> running_{false}; // 生命周期标志：start 准入 / stop 幂等守卫 / scheduler_loop 准入闸门
        bool net_allowed_ = true; // 网络允许标志：由 config 根据 network_type + allow_mobile_data 计算
        Config config_; // 权威配置快照（深拷贝，已校验）
        static constexpr int32_t flush_interval_ms_ = 1000; // 调度线程节拍（内部固定）
        static constexpr int32_t maintenance_interval_ms_ = 1000; // 持久化线程节拍（内部固定）

        std::unique_ptr<IDownloadEngine> http_;
        std::unique_ptr<IDownloadEngine> torrent_;
    };
} // namespace dw
