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
#include <string>
#include <vector>

namespace dw {

// 前向声明各协议引擎
class HttpEngine;
class TorrentEngine;
class TaskManager;

/**
 * 引擎进度快照（推模型载体）。
 *
 * 引擎线程组装后经 post_progress 写入 TaskRecord 内存字段；A 线程节拍读取转发上层。
 * 自带值语义存储（std::string / std::vector），可安全跨线程按值传递。
 * 仅承载进度数值与终态判定，调度权威态（QUEUED/PAUSED/DOWNLOADING 切换）由 TaskManager 独占。
 */
struct EngineProgress {
    bool             valid  = false;  // 引擎中是否存在该任务运行时上下文
    dw_protocol_t    protocol = DW_PROTOCOL_HTTP;
    dw_task_status_t status = DW_TASK_STATUS_QUEUED;  // 引擎视角状态；上层仅取终态（COMPLETED/ERROR）

    // 进度数值
    int64_t          total_size    = -1;
    int64_t          total_done    = -1;
    double           progress      = -1.0;
    double           download_rate = 0.0;

    // 展示 / 元数据
    std::string      name;
    int32_t          support_range = 0;
    std::string      etag;
    std::string      last_modified;

    // 终态原因
    dw_reason_t      reason  = DW_REASON_NONE;
    std::string      message;

    // BT 扩展字段
    double           upload_rate  = 0.0;
    bool             metadata_ready = false; // BT 元数据是否就绪（torrent_status::has_metadata）；HTTP 不使用
    int32_t          naming_ready   = 0;     // 定名迁移信号：0=无信号(不更新 TaskRecord) 2=迁移完成；仅 storage_moved_alert 设置
    bool             multi_file     = false; // BT 是否多文件（name 为根目录名）；定名占位形态判据，HTTP 恒单文件不使用
};

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
 * 内部断点续传数据推送：转交 TaskManager 深拷贝暂存，由维护线程落库。
 *
 * data / size 仅在调用期间有效。
 */
void post_resume_data(const char*    engine_key,
                      dw_protocol_t  protocol,
                      const uint8_t* data,
                      size_t         size);

/**
 * 内部任务节点树推送：引擎元数据就绪时构树后经此落库。
 *
 * files 为包含文件夹 + 文件的扁平节点数组（仅在调用期间有效，数据由库内深拷）。
 */
void post_task_files(const char*           engine_key,
                     dw_protocol_t         protocol,
                     const dw_file_info_t* files,
                     int32_t               count);

/**
 * 引擎进度推送：将最新进度写入 TaskRecord 内存字段。
 *
 * 引擎线程调用，持 TaskManager::mtx_ 极短时间（几个字段赋值）。
 * 任务不在内存 map 时静默丢弃（进度为瞬态无需落库）。A 线程节拍读取并转发上层。
 */
void post_progress(const char*          engine_key,
                   dw_protocol_t        protocol,
                   const EngineProgress& ep);

/**
 * 内部唯一名上调：引擎在元数据就绪 / 探测出名时请求定名。
 *
 * 内部转 TaskManager::resolve_and_record_name：持锁以磁盘为唯一真相源抢占唯一名
 *（候选未被占用即立即创建条目物化占位）→ 回写任务 name=filename=原名、
 * wrap_dir=包层目录名（未冲突为空）并立即落库（持久预留）。返回第一层条目名：
 * 与入参 name 不等即重名包层（返回值即 wrap_dir），引擎应将落盘目录追加一层该返回值
 * 目录（文件本名不变）；任务未知时仅抢名返回（不落库）。
 * 本通道占位形态恒为文件（HTTP 单文件模型），多文件协议不得经此路径定名。
 */
std::string request_unique_name(const char*   engine_key,
                                dw_protocol_t protocol,
                                const char*   dir,
                                const char*   name);

/**
 * 格式化日志输出（内部）。
 *
 * func / line 由 DW_LOGF 宏自动捕获；fmt 后接可变参。
 * DW_PRINTF_FMT 令 GCC/Clang 编译期校验格式串与实参类型匹配（MSVC 下为空）。
 */
#if defined(__GNUC__) || defined(__clang__)
#define DW_PRINTF_FMT(fmt_idx, arg_idx) __attribute__((format(printf, fmt_idx, arg_idx)))
#else
#define DW_PRINTF_FMT(fmt_idx, arg_idx)
#endif

void emit_logf(dw_log_level_t level, const char* trace_id,
               const char* func, int32_t line,
               const char* fmt, ...) DW_PRINTF_FMT(5, 6);

/// 从 task_id 计算 trace_id（hash 截取 8 位十六进制），无需跨层透传。
inline std::string make_trace(const char* task_id) {
    if (!task_id || !task_id[0]) return {};
    char buf[16];
    std::snprintf(buf, sizeof(buf), "%08zx",
                  std::hash<std::string_view>{}(task_id));
    return buf;
}

} // namespace dw

/// 日志宏：自动捕获调用方函数名与行号。
#define DW_LOG(level, message, trace_id) \
    dw::log_message((level), (message), (trace_id), __FUNCTION__, __LINE__)

/// 格式化日志宏：自动捕获调用方函数名与行号（需调用方预计算 trace_id）。
#define DW_LOGF(level, trace_id, fmt, ...) \
    dw::emit_logf((level), (trace_id), __FUNCTION__, __LINE__, fmt, ##__VA_ARGS__)

/// 任务级格式化日志：从 task_id 自动计算 trace_id，无需调用方透传。
#define DW_LOG_TASK(level, task_id, fmt, ...) do { \
    const std::string _dw_tr = dw::make_trace(task_id); \
    dw::emit_logf((level), _dw_tr.c_str(), __FUNCTION__, __LINE__, fmt, ##__VA_ARGS__); \
} while (0)

/// 系统级格式化日志：无关联任务，trace_id 为空。
#define DW_LOG_SYS(level, fmt, ...) \
    dw::emit_logf((level), "", __FUNCTION__, __LINE__, fmt, ##__VA_ARGS__)

#endif /* DOWNLOADER_INTERNAL_H */