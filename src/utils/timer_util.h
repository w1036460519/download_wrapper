/**
 * @file timer_util.h
 * @brief 基于 boost::asio::steady_timer 的一次性定时器。
 *
 * 业务侧只需构造实例并调用 start(duration, callback) 注册回调；
 * 到期时回调在内部后台线程执行。支持 cancel() 主动取消。
 * 全局共享一条后台线程，所有实例的定时器在同一 io_context 上调度。
 */
#pragma once

#include <chrono>
#include <functional>
#include <memory>

namespace dw::utils {

    class OneShotTimer : public std::enable_shared_from_this<OneShotTimer> {
    public:
        using callback_t = std::function<void()>;

        OneShotTimer();
        ~OneShotTimer();

        OneShotTimer(const OneShotTimer &) = delete;
        OneShotTimer &operator=(const OneShotTimer &) = delete;

        /// 启动定时器。到期后 callback 在后台线程执行一次。
        /// 重复调用 start 会先取消上一次。
        void start(std::chrono::milliseconds delay, callback_t callback);

        /// 取消定时器。若回调尚未执行则不会执行；若已在执行则等其结束。
        void cancel();

    private:
        class impl;
        std::unique_ptr<impl> impl_;
    };

} // namespace dw::utils
