/**
 * @file router.cpp
 * @brief L2 路由层实现。
 */

#include "router.h"
#include "task_manager.h"
#include "internal/downloader_internal.h"
#include "http/http_engine.h"
#include "torrent/torrent_engine.h"

namespace dw {
    void Router::set_local_client_id(const std::string &client_id) {
        local_client_id_ = client_id;
    }

    TaskManager *Router::route(const std::string &client_id) const {
        if (is_local(client_id)) {
            return task_manager_.get();
        }
        // 远程任务：未来返回 RemoteProxy
        // if (!is_local(client_id) && remote_proxy_) {
        //     return remote_proxy_->task_manager();
        // }
        return nullptr;
    }

    int32_t Router::start(const dw_downloader *owner, const dw_config_t &cfg) {
        task_manager_ = std::make_unique<TaskManager>();

        // 注入引擎
        task_manager_->set_engines(owner->http_engine.get(), owner->torrent_engine.get());

        // 启动 TaskManager
        return task_manager_->start(cfg);
    }

    void Router::stop() {
        if (task_manager_) {
            task_manager_->stop();
            task_manager_.reset();
        }
    }
} // namespace dw
