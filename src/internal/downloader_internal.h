/**
 * @file downloader_internal.h
 * @brief download_wrapper 内部实现头文件，不对外暴露。
 */

#pragma once

#include "download_wrapper/download_wrapper.h"
#include "core/task_record.h"

#include <atomic>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>

#include <boost/asio.hpp>
#include <boost/json.hpp>

namespace dw {
    // 前向声明各协议引擎
    class HttpEngine;
    class TorrentEngine;
    class TaskManager;

    /**
     * 引擎事件类型（引擎 alert / 状态变化经此转递给 Wrapper B 线程消费）。
     */
    enum class EngineEventType {
        PARSED, // 解析完成（元数据+文件信息，含存储迁移完成）
        DOWNLOAD_FAILED, // 下载失败（通用失败事件）
        DOWNLOAD_COMPLETED, // 下载完成
        STATUS_UPDATE, // 状态+进度更新（替代原 post_progress）
        RESUME_DATA, // 断点续传数据就绪（BT resume）
        BT_PAUSED, // BT 引擎已实际暂停（libtorrent handle.pause 已生效），由 alert 线程确认后投递
        BT_RESUMED, // BT 引擎已实际恢复（libtorrent handle.resume 已生效），由 alert 线程确认后投递
        DELETED, // 任务已从引擎移除（remove_torrent 收敛 / handle 无效直接删除），wrapper 据此回收资源
    };

    /**
     * 引擎事件载体（自带值语义，可安全跨线程按值传递）。
     *
     * 字段按事件类型复用：PARSED 事件用 name/save_path/files，STATUS_UPDATE 用进度/速率/总量，
     * BT_PAUSED/BT_RESUMED 等简单事件仅设 type/engine_key/protocol。消费方按 event.type 分支解析。
     * files 中各节点的字符串字段由调用方释放（投递前已完成深拷贝）。
     */
    struct EngineEvent {
        EngineEventType type; // 事件类型（决定其余字段语义）
        std::string engine_key; // 引擎侧标识（BT=info_hash，HTTP=url）
        dw_protocol_t protocol = DW_PROTOCOL_TORRENT; // 来源协议

        // PARSED 事件字段
        std::string name; // 种子/文件名（HTTP 探测定名或 BT 元数据）
        std::string save_path; // 引擎当前 save_path
        std::vector<dw_file_info_t> files; // 节点树（深拷贝，仅 PARSED 使用）

        // DOWNLOAD_FAILED 事件字段
        dw_reason_t reason = DW_REASON_NONE; // 失败原因码
        std::string message; // 错误描述文本

        // STATUS_UPDATE 事件字段：进度+上报量（不携带 status，状态由事件类型或终态分支解析）
        int64_t total_size = -1; // 总字节数（bytes），-1=unknown
        int64_t total_done = -1; // 已完成字节数（bytes），-1=unknown
        double progress = -1.0; // 完成比例（0.0~1.0），-1.0=unknown
        double download_rate = 0.0; // 下载速率（B/s）
        double upload_rate = 0.0; // 上传速率（B/s，HTTP 恒为 0）
        int64_t total_upload = 0; // 累计上传字节（bytes，HTTP 恒为 0）
        int32_t support_range = 0; // 服务端 Range 支持：0=不支持（200，单分片全量），1=支持（206，可分片并发/续传）
        std::string etag; // HTTP ETag
        std::string last_modified; // HTTP Last-Modified

        // RESUME_DATA 事件字段
        std::vector<uint8_t> resume_data; // 序列化续传数据

        // DELETED 事件字段
        int32_t delete_files = 0; // 是否删除落盘文件（1=删，0=不删）
    };

    // ---- 枚举名称序列化（供 to_string 重载使用）----

    inline const char *to_string(EngineEventType t) {
        switch (t) {
            case EngineEventType::PARSED: return "PARSED";
            case EngineEventType::DOWNLOAD_FAILED: return "DOWNLOAD_FAILED";
            case EngineEventType::DOWNLOAD_COMPLETED: return "DOWNLOAD_COMPLETED";
            case EngineEventType::STATUS_UPDATE: return "STATUS_UPDATE";
            case EngineEventType::RESUME_DATA: return "RESUME_DATA";
            case EngineEventType::BT_PAUSED: return "BT_PAUSED";
            case EngineEventType::BT_RESUMED: return "BT_RESUMED";
            case EngineEventType::DELETED: return "DELETED";
            default: return "UNKNOWN";
        }
    }

    // to_string(dw_protocol_t) 已定义于 task_record.h

    inline const char *to_string(dw_task_status_t s) {
        switch (s) {
            case DW_TASK_STATUS_DOWNLOADING: return "DOWNLOADING";
            case DW_TASK_STATUS_PAUSED: return "PAUSED";
            case DW_TASK_STATUS_COMPLETED: return "COMPLETED";
            case DW_TASK_STATUS_ERROR: return "ERROR";
            case DW_TASK_STATUS_QUEUED: return "QUEUED";
            case DW_TASK_STATUS_RESOLVING: return "RESOLVING";
            case DW_TASK_STATUS_PARSED: return "PARSED";
            case DW_TASK_STATUS_INVALIDATED: return "INVALIDATED";
            case DW_TASK_STATUS_DELETING: return "DELETING";
            default: return "UNKNOWN";
        }
    }

    inline const char *to_string(dw_reason_t r) {
        switch (r) {
            case DW_REASON_NONE: return "NONE";
            case DW_REASON_INTERNAL: return "INTERNAL";
            case DW_REASON_NETWORK: return "NETWORK";
            case DW_REASON_INVALID_INPUT: return "INVALID_INPUT";
            case DW_REASON_AUTH: return "AUTH";
            case DW_REASON_ERROR: return "ERROR";
            default: return "UNKNOWN";
        }
    }

    inline const char *to_string_source(int32_t s) {
        switch (s) {
            case 0: return "LOCAL_TASK";
            case 1: return "LOCAL_FILE";
            case 2: return "REMOTE_FILE";
            default: return "UNKNOWN";
        }
    }

    // ---- EngineEvent 序列化（重载，类 std::to_string 约定）----

    inline std::string to_string(const EngineEvent &e) {
        boost::json::object obj;
        obj["type"] = to_string(e.type);
        obj["key"] = e.engine_key;
        obj["protocol"] = to_string(e.protocol);
        obj["name"] = e.name;
        obj["save_path"] = e.save_path;
        obj["files"] = e.files.size();
        obj["reason"] = to_string(e.reason);
        obj["message"] = e.message;
        obj["total_size"] = e.total_size;
        obj["total_done"] = e.total_done;
        obj["progress"] = e.progress;
        obj["dl_rate"] = e.download_rate;
        obj["ul_rate"] = e.upload_rate;
        obj["total_upload"] = e.total_upload;
        obj["support_range"] = e.support_range;
        obj["etag"] = e.etag;
        obj["last_modified"] = e.last_modified;
        obj["resume_size"] = e.resume_data.size();
        return boost::json::serialize(obj);
    }

    // ---- dw_task_params_t 序列化（调试日志用，不含 resume_data）----

    inline std::string to_string(const dw_task_params_t &p) {
        boost::json::object obj;
        obj["save_path"] = p.save_path ? p.save_path : "";
        obj["filename"] = p.filename ? p.filename : "";
        obj["url"] = p.url ? p.url : "";
        obj["trace_id"] = p.trace_id ? p.trace_id : "";
        obj["info_hash"] = p.info_hash ? p.info_hash : "";
        obj["magnet"] = p.magnet_link ? p.magnet_link : "";
        obj["torrent"] = p.torrent_file ? p.torrent_file : "";
        obj["trackers"] = p.tracker_count;
        obj["file_indexes"] = p.file_index_size;
        obj["url_seeds"] = p.url_seed_count;
        obj["priority"] = p.priority;
        obj["source"] = to_string_source(p.source);
        return boost::json::serialize(obj);
    }

    /**
     * 下载器全局单例内部实现。
     *
     * 持有 HTTP 和 BT 两个引擎实例，以及回调函数指针与。
     */
    struct dw_downloader {
        std::mutex mutex;
        std::atomic<bool> initialized{false};

        std::unique_ptr<HttpEngine> http_engine;
        std::unique_ptr<TorrentEngine> torrent_engine;
        std::unique_ptr<TaskManager> task_manager;

        dw_progress_cb progress_cb = nullptr;
        dw_log_cb log_cb = nullptr;

        dw_config_t config{};
    };

    /**
     * 获取全局单例；若未初始化返回 nullptr。
     */
    dw_downloader *global_downloader();

    /**
     * 内部日志输出。
     *
     * func / line 由 DW_LOG 宏自动捕获，直接调用时可为空/0。
     */
    void log_message(dw_log_level_t level,
                     const char *message,
                     const char *trace_id = "",
                     const char *func = "",
                     int32_t line = 0);

    /**
     * 内部断点续传数据推送：转交 TaskManager 深拷贝暂存，由维护线程落库。
     *
     * data / size 仅在调用期间有效。
     */
    void post_resume_data(const char *engine_key,
                          dw_protocol_t protocol,
                          const uint8_t *data,
                          size_t size);

    /**
     * 内部任务节点树推送：引擎元数据就绪时构树后经此落库。
     *
     * files 为包含文件夹 + 文件的扁平节点数组（仅在调用期间有效，数据由库内深拷）。
     */
    void post_task_files(const char *engine_key,
                         dw_protocol_t protocol,
                         const dw_file_info_t *files,
                         int32_t count);

    /**
     * 内部单文件节点懒创建 / 进度推送：按 (engine_key, file_index) 命中 task_id 后
     * upsert task_files 一行（不存在则插入下载中占位，存在则仅上提 downloaded_bytes / size）。
     * HTTP 探测定名后零碎进度 / BT 元数据外的 per-file 进度均走此路径。
     */
    void post_task_file_update(const char *engine_key,
                               dw_protocol_t protocol,
                               int32_t file_index,
                               int64_t downloaded_bytes,
                               int64_t total_size);

    /**
     * 内部唯一名上调：引擎在元数据就绪 / 探测出名时请求定名。
     *
     * 内部转 TaskManager::resolve_and_record_name：持锁以磁盘为唯一真相源抢占唯一 wrapper 名
     * （候选未被占用即立即创建 wrapper 目录物化占位）→ 回写任务 name=wrapper（可能含 (n) 后缀）、
     * filename=inner_name（HTTP/BT 单文件）、save_path=原 dir（不变）并立即落库（持久预留）。
     * 返回 wrapper 名（与入参 wrapper_name 不等即重名包层，调用方按目录名创建 wrapper）；
     * 任务未知时仅抢名返回（不落库）。
     * 本通道 wrapper 占位恒为目录，多文件 BT 不得经此路径定名（走 PARSED 事件）。
     */
    std::string request_unique_name(const char *engine_key,
                                    dw_protocol_t protocol,
                                    const char *dir,
                                    const char *wrapper_name,
                                    const char *inner_name);

    /* ================================================================== */
    /*                          引擎事件投递                              */
    /* ================================================================== */

    /**
     * 引擎事件投递：经 Boost.Asio io_context::post 投递到 B 线程消费。
     *
     * 引擎 alert 线程调用，线程安全。事件经值语义拷贝后投递，调用方无需保持数据存活。
     * files 中各节点的字符串字段由调用方释放（投递前已完成深拷贝）。
     */
    void post_engine_event(EngineEvent event);

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

    void emit_logf(dw_log_level_t level, const char *trace_id,
                   const char *func, int32_t line,
                   const char *fmt, ...) DW_PRINTF_FMT(5, 6);

    /// 从 task_id 计算 trace_id（取 size_t 自然宽度，不补零），无需跨层透传。
    inline std::string make_trace(const char *task_id) {
        if (!task_id || !task_id[0]) return {};
        char buf[17];
        std::snprintf(buf, sizeof(buf), "%zx",
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
