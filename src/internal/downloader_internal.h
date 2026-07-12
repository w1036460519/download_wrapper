/**
 * @file downloader_internal.h
 * @brief download_wrapper 内部实现头文件，不对外暴露。
 */

#ifndef DOWNLOADER_INTERNAL_H
#define DOWNLOADER_INTERNAL_H

#include "download_wrapper/download_wrapper.h"

#include <atomic>
#include <memory>
#include <mutex>

namespace dw {

// 前向声明各协议引擎
class HttpEngine;
class TorrentEngine;
class TaskManager;

/**
 * 下载器全局单例内部实现。
 *
 * 持有 HTTP 和 BT 两个引擎实例，以及回调函数指针与。
 */
struct dw_downloader {
    std::mutex mutex;
    std::atomic<bool> initialized{false};

    std::unique_ptr<HttpEngine>     http_engine;
    std::unique_ptr<TorrentEngine>  torrent_engine;
    std::unique_ptr<TaskManager>    task_manager;

    dw_progress_cb progress_cb = nullptr;
    dw_log_cb      log_cb      = nullptr;
    dw_resume_data_cb resume_data_cb = nullptr;

    dw_config_t    config{};
};

/**
 * 获取全局单例；若未初始化返回 nullptr。
 */
dw_downloader* global_downloader();

/**
 * 内部日志输出。
 *
 * func / line 由 DW_LOG 宏自动捕获，直接调用时可为空/0。
 */
void log_message(dw_log_level_t level,
                 const char*    message,
                 const char*    trace_id = "",
                 const char*    func = "",
                 int32_t        line = 0);

/**
 * 内部进度推送，调用 progress_cb（若已注册）。
 */
void post_progress(const dw_progress_t* progress);

/**
 * 内部断点续传数据推送，调用 resume_data_cb（若已注册）。
 *
 * data / size 仅在调用期间有效，回调内如需持有须深拷贝。
 */
void post_resume_data(const char*    task_id,
                      dw_protocol_t  protocol,
                      const uint8_t* data,
                      size_t         size);

/**
 * 格式化日志输出（内部）。
 *
 * func / line 由 DW_LOGF 宏自动捕获；fmt 后接可变参。
 */
void emit_logf(dw_log_level_t level, const char* trace_id,
               const char* func, int32_t line,
               const char* fmt, ...);

} // namespace dw

/// 日志宏：自动捕获调用方函数名与行号。
#define DW_LOG(level, message, trace_id) \
    dw::log_message((level), (message), (trace_id), __FUNCTION__, __LINE__)

/// 格式化日志宏：自动捕获调用方函数名与行号。
#define DW_LOGF(level, trace_id, fmt, ...) \
    dw::emit_logf((level), (trace_id), __FUNCTION__, __LINE__, fmt, ##__VA_ARGS__)

#endif /* DOWNLOADER_INTERNAL_H */
