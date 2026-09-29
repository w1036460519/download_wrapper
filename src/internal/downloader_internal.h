/**
 * @file downloader_internal.h
 * @brief download_wrapper 内部实现头文件，不对外暴露。
 */

#pragma once

#include "download_wrapper/download_wrapper.h"
#include "core/task_record.h"
#include "internal/file_info.h"
#include "internal/engine_event.h"
#include "internal/json_util.h"
#include "internal/config.h"
#include "internal/task_identity.h"
#include "internal/task_params.h"
#include "internal/submit_result.h"

#include <atomic>
#include <format>
#include <map>
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

    // ---- 枚举名称序列化（供 to_string 重载使用）----

    inline const char *to_string(dw_task_status_t s) {
        switch (s) {
            case DW_TASK_STATUS_DOWNLOADING: return "DOWNLOADING";
            case DW_TASK_STATUS_PAUSED: return "PAUSED";
            case DW_TASK_STATUS_COMPLETED: return "COMPLETED";
            case DW_TASK_STATUS_ERROR: return "ERROR";
            case DW_TASK_STATUS_QUEUED: return "QUEUED";
            case DW_TASK_STATUS_RESOLVING: return "RESOLVING";
            case DW_TASK_STATUS_INVALIDATED: return "INVALIDATED";
            case DW_TASK_STATUS_DELETING: return "DELETING";
            case DW_TASK_STATUS_FAIL: return "FAIL";
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
            case DW_REASON_FAIL: return "FAIL";
            default: return "UNKNOWN";
        }
    }

    // ---- std::vector<int32_t> 序列化（调试日志用）----

    inline std::string to_string(const std::vector<int32_t> &v) {
        boost::json::array arr;
        for (const int32_t x: v) arr.push_back(x);
        return boost::json::serialize(arr);
    }

    // ---- dw_file_info_t 序列化（调试日志用）----

    inline std::string to_string(const dw_file_info_t &f) {
        boost::json::object obj;
        obj["index"] = f.index;
        obj["name"] = f.name ? f.name : "";
        obj["full_path"] = f.full_path ? f.full_path : "";
        obj["size"] = f.size;
        obj["ext"] = f.ext ? f.ext : "";
        obj["status"] = f.status;
        obj["offset"] = f.offset;
        obj["downloaded_bytes"] = f.downloaded_bytes;
        return boost::json::serialize(obj);
    }

    // ---- EngineEvent 序列化（重载，类 std::to_string 约定）----

    inline std::string to_string(const EngineEvent &e) {
        boost::json::object obj;
        obj["type"] = to_string(e.type);
        obj["key"] = e.engine_key;
        obj["protocol"] = to_string(e.protocol);
        obj["client_id"] = e.client_id;
        obj["name"] = e.name;
        obj["save_path"] = e.save_path;
        // 文件列表：序列化每个文件信息
        boost::json::array files_arr;
        for (const auto &f: e.files) {
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
        // FILE_PROGRESS 字段
        obj["file_index"] = e.file_index;
        obj["file_path"] = e.file_path;
        obj["full_path"] = e.full_path;
        obj["file_size"] = e.size;
        // 区间集合序列化
        boost::json::array segments_arr;
        for (const auto &[start, end]: e.segments) {
            boost::json::array interval;
            interval.push_back(start);
            interval.push_back(end);
            segments_arr.push_back(std::move(interval));
        }
        obj["segments"] = std::move(segments_arr);
        return boost::json::serialize(obj);
    }

    // ---- dw_task_params_t 序列化（调试日志用，不含 resume_data）----

    inline std::string to_string(const dw_task_params_t &p) {
        boost::json::object obj;
        obj["save_path"] = p.save_path ? p.save_path : "";
        obj["url"] = p.url ? p.url : "";
        obj["info_hash"] = p.info_hash ? p.info_hash : "";
        obj["magnet"] = p.magnet_link ? p.magnet_link : "";
        obj["torrent"] = p.torrent_file ? p.torrent_file : "";
        obj["file_indexes"] = p.file_index_size;
        obj["priority_file_indexes"] = p.priority_file_index_size;
        obj["url_seeds"] = p.url_seed_count;
        obj["priority"] = p.priority;
        // source 已从参数结构体移除（由库内按 protocol 推导），此处不再输出。
        return boost::json::serialize(obj);
    }

    // ---- dw_config_t 序列化（调试日志用）----

    inline std::string to_string(const dw_config_t &c) {
        boost::json::object obj;
        // HTTP 配置
        obj["connect_timeout"] = c.connect_timeout_seconds;
        obj["request_timeout"] = c.request_timeout_seconds;
        obj["low_speed_limit"] = c.low_speed_limit_bps;
        obj["low_speed_time"] = c.low_speed_time;
        obj["max_redirect"] = c.max_redirect;
        obj["proxy"] = c.proxy ? c.proxy : "";
        obj["user_agent"] = c.user_agent ? c.user_agent : "";
        obj["verify_ssl"] = c.verify_ssl;
        obj["ca_bundle"] = c.ca_bundle ? c.ca_bundle : "";
        obj["max_retries"] = c.max_retries;
        obj["default_parts"] = c.default_parts;
        obj["min_size_for_split"] = c.min_size_for_split;
        // BT 配置
        obj["listen_port"] = c.listen_port;
        obj["max_concurrent"] = c.max_concurrent_downloads;
        obj["dl_rate_limit"] = c.download_rate_limit;
        obj["ul_rate_limit"] = c.upload_rate_limit;
        obj["seed_ratio"] = c.seed_ratio_limit;
        // 通用配置
        obj["log_level"] = static_cast<int>(c.log_level);
        obj["work_dir"] = c.work_dir ? c.work_dir : "";
        obj["client_id"] = c.client_id ? c.client_id : "";
        return boost::json::serialize(obj);
    }

    /**
     * 下载器全局单例内部实现。
     *
     * 持有 HTTP 和 BT 两个引擎实例及 TaskManager 任务中枢。
     */
    struct dw_downloader {
        std::mutex mutex;
        std::atomic<bool> initialized{false};

        std::unique_ptr<HttpEngine> http_engine;
        std::unique_ptr<TorrentEngine> torrent_engine;
        std::unique_ptr<TaskManager> task_manager;

        std::atomic<dw_progress_cb> progress_cb{nullptr};
        std::atomic<dw_log_cb> log_cb{nullptr};

        Config config{};
    };

    /**
     * 获取全局单例；若未初始化返回 nullptr。
     */
    dw_downloader *global_downloader();

    /**
     * 安全调用进度回调（JSON 字符串格式）。
     *
     * 内部检查下载器状态与回调有效性，调用失败（未注册/已销毁）时静默忽略，
     * 不影响业务流程。
     */
    void emit_progress(const char *json);

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
     * 日志调用点上下文：由追踪 ID 实参隐式转换而来，构造默认参自动捕获调用位置。
     *
     * 作为 log_i / log_d / log_e 的首参，携带追踪 ID 与 C++20 source_location
     *（函数名 / 行号），替代早期宏方案的 __FUNCTION__ / __LINE__ 捕获。
     */
    struct log_site {
        const char *trace_id; // 追踪 ID；NULL 或空串按无关联任务处理
        std::source_location loc; // 调用点位置：构造时（即日志调用处）捕获

        log_site(const char *tid,
                 std::source_location l = std::source_location::current())
            : trace_id(tid), loc(l) {
        }
    };

    /**
     * 快捷日志公共实现（内部）：级别由参数指定，调用点位置取自 log_site。
     * 使用 std::format 进行类型安全的格式化。
     */
    template<typename... Args>
    void log_at(dw_log_level_t level, log_site site, const char *fmt, const Args &... args) {
        std::string msg;
        if constexpr (sizeof...(Args) == 0) {
            msg = fmt;
        } else {
            msg = std::vformat(fmt, std::make_format_args(args...));
        }
        log_message(level, msg.c_str(), site.trace_id,
                    site.loc.function_name(),
                    static_cast<int32_t>(site.loc.line()));
    }

    /**
     * 快捷日志接口（库内唯一日志入口）：调用方仅需传入追踪 ID 与格式串。
     *
     * 函数模板：级别固定为 INFO；调用方函数名与行号经
     * log_site 的 source_location 自动捕获。实参类型安全：std::format
     * 自动推导类型，无需 .c_str()，格式说明符与类型不匹配时编译期报错。
     *
     * @param site 调用点上下文：由追踪 ID（任务原始标识）隐式转换；
     *             允许 NULL 或空串，此时按无关联任务处理。
     * @param fmt std::format 风格格式串（{} 占位符），须为非 NULL 字符串。
     * @param args 与格式串对应的可变参数，可省略（纯文本调用）。
     */
    template<typename... Args>
    void log_i(log_site site, const char *fmt, const Args &... args) {
        log_at(DW_LOG_INFO, site, fmt, args...);
    }

    /// DEBUG 级别快捷日志：参数语义与错误处理同 log_i。
    template<typename... Args>
    void log_d(log_site site, const char *fmt, const Args &... args) {
        log_at(DW_LOG_DEBUG, site, fmt, args...);
    }

    /// ERROR 级别快捷日志：参数语义与错误处理同 log_i。
    template<typename... Args>
    void log_e(log_site site, const char *fmt, const Args &... args) {
        log_at(DW_LOG_ERROR, site, fmt, args...);
    }

    /* ================================================================== */
    /*                     响应 JSON 辅助函数                             */
    /* ================================================================== */

    /// 构建成功响应：{"code": 0, "data": {...}}
    inline std::string make_success_response(const boost::json::object &data = {}) {
        boost::json::object resp;
        resp["code"] = 0;
        if (!data.empty()) {
            resp["data"] = data;
        }
        return boost::json::serialize(resp);
    }

    /// 构建成功响应（带字符串 data）
    inline std::string make_success_response(const std::string &data_key, const std::string &data_value) {
        boost::json::object data;
        data[data_key] = data_value;
        return make_success_response(data);
    }

    /// 构建错误响应：{"code": -1, "message": "..."}
    inline std::string make_error_response(const std::string &message) {
        boost::json::object resp;
        resp["code"] = -1;
        resp["message"] = message;
        return boost::json::serialize(resp);
    }

    /// 构建错误响应（带错误码）
    inline std::string make_error_response(int code, const std::string &message) {
        boost::json::object resp;
        resp["code"] = code;
        resp["message"] = message;
        return boost::json::serialize(resp);
    }

    /// 从 JSON 字符串中提取指定字段
    inline std::string json_get_string(const boost::json::object &obj, const char *key) {
        if (auto *v = obj.if_contains(key); v && v->is_string()) {
            return v->as_string().c_str();
        }
        return {};
    }
} // namespace dw
