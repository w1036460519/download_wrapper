/**
 * @file engine_interface.h
 * @brief 下载引擎统一接口：TaskManager 经此抽象分发，屏蔽协议实现差异。
 *
 * 设计要点：
 *   - 纯虚方法为各引擎必须实现的任务生命周期能力；
 *   - post_updates 为带默认空实现的节拍钩子：Torrent 引擎覆写（触发
 *     post_torrent_updates 刷新 + 续传检查点），HTTP 引擎无需实现；
 *   - 协议专属能力（如 BT 的文件列表 / 优先级 / 磁力解析）留在具体引擎类，
 *     由 download_wrapper.cpp 经具体指针调用，不污染本接口。
 */

#pragma once

#include "download_wrapper/download_wrapper.h"
#include "internal/downloader_internal.h"

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace dw {
    class TaskManager; // 前向声明

    /**
     * 下载引擎抽象接口（HttpEngine / TorrentEngine 实现）。
     */
    class IDownloadEngine {
    public:
        bool initialized_ = false;

        void set_task_manager(TaskManager *task_manager) {
            task_manager_ = task_manager;
        }

        virtual ~IDownloadEngine() = default;

        /// 初始化引擎。@return 0=成功，-1=失败。
        virtual int32_t init(const std::vector<std::string> &task_ids = {}) = 0;

        /// 销毁引擎，释放所有资源。
        virtual void destroy() = 0;

        /// 应用运行期配置，支持动态配置的引擎覆写。
        virtual void config(std::optional<int32_t> /*downloads*/,
                            std::optional<int64_t> /*download_rate*/,
                            std::optional<int64_t> /*upload_rate*/,
                            std::optional<bool> /*network_allowed*/ = std::nullopt) {
        }

        /// 添加单个下载任务。
        virtual dw_result<> add_task(const TaskParams *params) = 0;

        /// 提交恢复请求，更新任务状态并等待调度。
        virtual dw_result<> resume_task(const TaskParams *params) = 0;

        /// 调度器准入后实际恢复下载。
        virtual dw_result<> do_resume(const TaskParams *params) {
            return resume_task(params);
        }

        /// 暂停单个下载任务。
        virtual dw_result<> pause_task(const TaskParams *params) = 0;

        /// 提交单个下载任务的删除请求。
        virtual dw_result<> delete_task(const TaskParams *params) = 0;

        /// 周期性维护：回收终态任务的运行时上下文。由上层调度循环定时调用。
        virtual void sweep() = 0;

        // ---- 可选钩子（默认空实现，Torrent 覆写） ----

        /// 节拍入口（A 线程调用）：BT 覆写（触发 post_torrent_updates 刷新 + 续传检查点），
        /// HTTP 引擎使用默认实现。
        virtual void post_updates() {
        }

        /// 文件列表实时查询（BT 覆写）：返回全量文件清单（pad 已过滤），含选中状态。
        virtual dw_result<> get_file_list(const std::string &/*client_id*/,
                                                 const std::string &/*natural_key*/) {
            return dw_result<>::failure(DW_REASON_ERROR, "不支持");
        }

    protected:
        TaskManager *task_manager_ = nullptr;
    };
} // namespace dw
