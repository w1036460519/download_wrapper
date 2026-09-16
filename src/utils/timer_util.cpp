/**
 * @file timer_util.cpp
 * @brief OneShotTimer 实现：全局 io_context + 后台线程驱动 steady_timer。
 */
#include "utils/timer_util.h"

#include <atomic>
#include <mutex>
#include <thread>

#include <boost/asio.hpp>

namespace dw::utils {

    // ---- 全局 io_context 与后台线程（Meyer 单例，进程级生命周期）----
    namespace {
        struct timer_io_ctx {
            boost::asio::io_context ioc;
            boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work;
            std::jthread worker;

            timer_io_ctx()
                : work(boost::asio::make_work_guard(ioc))
                , worker([this] { ioc.run(); }) {}

            ~timer_io_ctx() {
                work.reset();
                worker.request_stop();
                if (worker.joinable()) worker.join();
            }
        };

        timer_io_ctx &get_ctx() {
            static timer_io_ctx ctx;
            return ctx;
        }
    } // namespace

    // ---- pimpl 实现 ----
    class OneShotTimer::impl {
    public:
        explicit impl(boost::asio::io_context &ioc)
            : timer_(ioc) {}

        boost::asio::steady_timer timer_;
        std::atomic<bool> cancelled_{false};
    };

    OneShotTimer::OneShotTimer()
        : impl_(std::make_unique<impl>(get_ctx().ioc)) {}

    OneShotTimer::~OneShotTimer() {
        cancel();
    }

    void OneShotTimer::start(std::chrono::milliseconds delay, callback_t callback) {
        // 取消上一次（若有）
        cancel();

        impl_->cancelled_.store(false);
        impl_->timer_.expires_after(delay);

        // 捕获 shared_ptr 保证 async_wait 期间 timer 对象存活
        auto self = shared_from_this();
        impl_->timer_.async_wait(
            [self, cb = std::move(callback)](const boost::system::error_code &ec) {
                if (ec || self->impl_->cancelled_.load()) return;
                if (cb) cb();
            });
    }

    void OneShotTimer::cancel() {
        impl_->cancelled_.store(true);
        impl_->timer_.cancel();

        // 屏障：若回调正在执行，等其结束后再返回
        if (impl_->cancelled_.load()) {
            std::mutex mtx;
            std::condition_variable cv;
            bool done = false;
            boost::asio::post(get_ctx().ioc, [&] {
                std::lock_guard lock(mtx);
                done = true;
                cv.notify_one();
            });
            std::unique_lock lock(mtx);
            cv.wait(lock, [&] { return done; });
        }
    }

} // namespace dw::utils
