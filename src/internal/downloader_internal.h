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
#include <source_location>
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
        RESUME_DATA, // 断点续传数据就绪（BT resume / HTTP 定期存档）
        BT_PAUSED, // BT 引擎已实际暂停（libtorrent handle.pause 已生效），由 alert 线程确认后投递
        BT_RESUMED, // BT 引擎已实际恢复（libtorrent handle.resume 已生效），由 alert 线程确认后投递
        DELETED, // 任务已从引擎移除（remove_torrent 收敛 / handle 无效直接删除），wrapper 据此回收资源
        TASK_FILES, // 任务文件列表推送（HTTP 响应头就绪后推送单文件信息）
        FILE_PROGRESS, // 文件进度区间就绪（BT 连续 piece 达阈值后合并上报，HTTP 无此事件）
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

        // TASK_FILES 事件字段（HTTP 磁盘定名就绪）：name=判重后 wrapper 目录名
        //（磁盘根实体名），files[0].name=原始文件名（含后缀）。

        // FILE_PROGRESS 事件字段（BT：连续 piece 达 1% 阈值后合并的文件内区间）
        int32_t file_index = -1; // 目标文件索引（libtorrent 文件序号，-1=未设置）
        std::string file_path; // 文件物理路径（引擎侧拼好，handle 视角 save_path/相对路径）
        int64_t offset_start = 0; // 区间起点（文件内相对偏移，含）
        int64_t offset_end = 0; // 区间终点（文件内相对偏移，含）
        int64_t file_size = 0; // 文件总大小（bytes，供完成判定）
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
            case EngineEventType::TASK_FILES: return "TASK_FILES";
            case EngineEventType::FILE_PROGRESS: return "FILE_PROGRESS";
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

    // ---- dw_file_info_t 序列化（调试日志用）----

    inline std::string to_string(const dw_file_info_t &f) {
        boost::json::object obj;
        obj["index"] = f.index;
        obj["name"] = f.name ? f.name : "";
        obj["size"] = f.size;
        obj["ext"] = f.ext ? f.ext : "";
        obj["status"] = f.status;
        obj["offset"] = f.offset;
        obj["downloaded_bytes"] = f.downloaded_bytes;
        obj["physical_path"] = (f.physical_path ? f.physical_path : "");
        return boost::json::serialize(obj);
    }

    // ---- EngineEvent 序列化（重载，类 std::to_string 约定）----

    inline std::string to_string(const EngineEvent &e) {
        boost::json::object obj;
        obj["type"] = to_string(e.type);
        obj["key"] = e.engine_key;
        obj["protocol"] = to_string(e.protocol);
        obj["name"] = e.name;
        obj["save_path"] = e.save_path;
        // 文件列表：序列化每个文件信息
        boost::json::array files_arr;
        for (const auto &f : e.files) {
            files_arr.push_back(boost::json::parse(to_string(f)));
        }
        obj["files"] = std::move(files_arr);
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
        obj["url"] = p.url ? p.url : "";
        obj["trace_id"] = p.trace_id ? p.trace_id : "";
        obj["info_hash"] = p.info_hash ? p.info_hash : "";
        obj["magnet"] = p.magnet_link ? p.magnet_link : "";
        obj["torrent"] = p.torrent_file ? p.torrent_file : "";
        obj["trackers"] = p.tracker_count;
        obj["file_indexes"] = p.file_index_size;
        obj["priority_file_indexes"] = p.priority_file_index_size;
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
     * func / line 由 log_i / log_d / log_e 函数模板自动捕获，直接调用时可为空/0。
     */
    void log_message(dw_log_level_t level,
                     const char *message,
                     const char *trace_id = "",
                     const char *func = "",
                     int32_t line = 0);

    /**
     * 内部唯一名上调：引擎在元数据就绪 / 探测出名时请求定名。
     *
     * 内部转 TaskManager::resolve_and_record_name：持锁以磁盘为唯一真相源抢占唯一 wrapper 名
     * （候选未被占用即立即创建 wrapper 目录物化占位）→ 回写任务 name=wrapper（可能含 (n) 后缀）、
     * save_path=原 dir（不变）并立即落库（持久预留）。
     * 返回 wrapper 名（与入参 wrapper_name 不等即重名包层，调用方按目录名创建 wrapper）；
     * 任务未知时仅抢名返回（不落库）。
     * 本通道 wrapper 占位恒为目录，多文件 BT 不得经此路径定名（走 PARSED 事件）。
     */
    std::string request_unique_name(const char *engine_key,
                                    dw_protocol_t protocol,
                                    const char *dir,
                                    const char *wrapper_name,
                                    const char *inner_name);

    /**
     * 格式化日志输出（内部）。
     *
     * func / line 由 log_i / log_d / log_e 函数模板自动捕获；fmt 后接可变参。
     * 注：调用均经模板转发，fmt 为运行期参数，格式串不做编译期校验；
     * 非标量实参仍由变参函数通用诊断（-Wnon-pod-varargs）在实例化时拦截。
     */
    void emit_logf(dw_log_level_t level, const char *trace_id,
                   const char *func, int32_t line,
                   const char *fmt, ...);

    /**
     * 日志调用点上下文：由追踪 ID 实参隐式转换而来，构造默认参自动捕获调用位置。
     *
     * 作为 log_i / log_d / log_e 的首参，携带追踪 ID 与 C++20 source_location
     *（函数名 / 行号），替代早期宏方案的 __FUNCTION__ / __LINE__ 捕获。
     */
    struct log_site {
        const char *trace_id;      // 追踪 ID；NULL 或空串按无关联任务处理
        std::source_location loc;  // 调用点位置：构造时（即日志调用处）捕获

        log_site(const char *tid,
                 std::source_location l = std::source_location::current())
            : trace_id(tid), loc(l) {}
    };

    /**
     * 快捷日志公共实现（内部）：级别由参数指定，调用点位置取自 log_site。
     *
     * 纯文本调用（无可变参）内部经 "%s" 转发，文本不会被解释为格式串。
     */
    template <typename... Args>
    void log_at(dw_log_level_t level, log_site site, const char *fmt, Args... args) {
        if constexpr (sizeof...(Args) == 0) {
            emit_logf(level, site.trace_id, site.loc.function_name(),
                      static_cast<int32_t>(site.loc.line()), "%s", fmt);
        } else {
            emit_logf(level, site.trace_id, site.loc.function_name(),
                      static_cast<int32_t>(site.loc.line()), fmt, args...);
        }
    }

    /**
     * 快捷日志接口（库内唯一日志入口）：调用方仅需传入追踪 ID 与格式串。
     *
     * 函数模板（早期宏实现已移除）：级别固定为 INFO；调用方函数名与行号经
     * log_site 的 source_location 自动捕获。实参类型安全：非标量实参（如 std::string）
     * 实例化时报错；注意格式说明符失配（%s 配 int 等）不再编译期拦截。
     *
     * @param site 调用点上下文：由追踪 ID（任务原始标识）隐式转换；
     *             允许 NULL 或空串，此时按无关联任务处理。
     * @param fmt  printf 风格格式串，须为非 NULL 字符串；纯文本时可不含转换说明。
     * @param args 与格式串对应的可变参数，可省略（纯文本调用）。
     * @return 无。投递失败 / 未配置回调的兑底（输出 stderr）由 emit_logf 内部统一处理。
     */
    template <typename... Args>
    void log_i(log_site site, const char *fmt, Args... args) {
        log_at(DW_LOG_INFO, site, fmt, args...);
    }

    /// DEBUG 级别快捷日志：参数语义与错误处理同 log_i。
    template <typename... Args>
    void log_d(log_site site, const char *fmt, Args... args) {
        log_at(DW_LOG_DEBUG, site, fmt, args...);
    }

    /// ERROR 级别快捷日志：参数语义与错误处理同 log_i。
    template <typename... Args>
    void log_e(log_site site, const char *fmt, Args... args) {
        log_at(DW_LOG_ERROR, site, fmt, args...);
    }
} // namespace dw
