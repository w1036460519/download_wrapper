/**
 * @file router.h
 * @brief L2 路由层：按 client_id 分发到本机 TaskManager 或远程代理。
 *
 * 职责：
 *   - 维护本机 client_id（由 dw_init 时配置）
 *   - 按 client_id 路由请求：匹配本机 → TaskManager，不匹配 → RemoteProxy（未来）
 *   - 持有 TaskManager 实例，管理其生命周期
 */

#pragma once

#include <memory>
#include <string>

// 前向声明：dw_config_t 定义在 extern "C" 块中（全局命名空间）
struct dw_config;
typedef struct dw_config dw_config_t;

namespace dw {
    class TaskManager;
    struct dw_downloader;

    /**
     * 路由层单例，由 dw_downloader 持有。
     */
    class Router {
    public:
        Router() = default;
        ~Router() = default;

        Router(const Router&) = delete;
        Router& operator=(const Router&) = delete;

        /// 设置本机 client_id（由 dw_init 调用）。
        void set_local_client_id(const std::string& client_id);

        /// 获取本机 client_id。
        const std::string& local_client_id() const { return local_client_id_; }

        /// 判断是否为本地任务。
        bool is_local(const std::string& client_id) const {
            return client_id == local_client_id_;
        }

        /// 路由到 TaskManager（目前仅支持本机，远程留接口）。
        /// @return 本机返回 TaskManager*，远程返回 nullptr（未来返回 RemoteProxy）。
        TaskManager* route(const std::string& client_id) const;

        /// 初始化 TaskManager（由 dw_init 调用）。
        int32_t start(const dw_downloader* owner, const dw_config_t& cfg);

        /// 停止 TaskManager。
        void stop();

        /// 获取 TaskManager（仅本机使用，不做路由判断）。
        TaskManager* task_manager() { return task_manager_.get(); }

    private:
        std::string local_client_id_;
        std::unique_ptr<TaskManager> task_manager_;
        // std::unique_ptr<RemoteProxy> remote_proxy_; // 未来扩展
    };

} // namespace dw
