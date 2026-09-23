/**
 * @file downloader_internal.h
 * @brief download_wrapper 内部实现头文件，不对外暴露。
 */

#pragma once

#include "download_wrapper/download_wrapper.h"
#include "core/task_record.h"
#include "utils/memory_util.h"

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

    /**
     * 文件信息（C++ 内部版本）：std::string 字段，无手动内存管理。
     * 用于内部传递和 JSON 序列化；FFI 输出仍经 dw_file_info_t 转换。
     */
    struct FileInfo {
        int32_t index = 0;
        std::string name;
        std::string full_path;
        int64_t size = 0;
        std::string ext;
        int32_t status = 0;
        int64_t offset = 0;
        int64_t downloaded_bytes = 0;
        bool selected = true; // 是否选中下载（BT 由优先级决定）
    };

    /**
     * 引擎事件类型（引擎 alert / 状态变化经此转递给 Wrapper B 线程消费）。
     */
    enum class EngineEventType {
        PARSED, // 解析完成（元数据+文件信息，含存储迁移完成）
        DOWNLOAD_FAILED, // 下载失败（通用失败事件）
        STATUS_UPDATE, // 状态+进度更新（含冗余状态字段 status）
        RESUME_DATA, // 断点续传数据就绪（BT resume / HTTP 定期存档）
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
        std::string client_id;

        // PARSED 事件字段
        std::string name; // 种子/文件名（HTTP 探测定名或 BT 元数据）
        std::string save_path; // 引擎当前 save_path
        std::string original_name; // 重名/包装前的原始目录/文件名（未重名时与 content_root 相同）
        std::string original_root_name;
        std::string root_name;
        std::string content_root; // 磁盘根目录名（重名判定后的最终名称）
        bool is_dir = true; // 内容是否为目录（单文件无父路径 = false）
        std::string ext; // 文件后缀（仅单文件时有值，不含 '.'）
        std::vector<dw_file_info_t> files; // 节点树（深拷贝，仅 PARSED 使用）

        // DOWNLOAD_FAILED 事件字段
        dw_reason_t reason = DW_REASON_NONE; // 失败原因码
        std::string message; // 错误描述文本

        // STATUS_UPDATE 事件字段：进度+速率+状态冗余
        dw_task_status_t status = DW_TASK_STATUS_DOWNLOADING; // 引擎侧状态冗余（与专用事件 PAUSED/RESUMED 互补）
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
        std::string file_path; // 文件相对路径（引擎侧，handle 视角相对路径）
        std::string full_path; // 完整路径（save_path + file_path，供调用方直接使用）
        int64_t file_size = 0; // 文件总大小（bytes，供完成判定）
        std::map<int64_t, int64_t> intervals; // 有序区间集合（key=offset_start, value=offset_end），序列化后保存
    };

    // ---- 枚举名称序列化（供 to_string 重载使用）----

    inline const char *to_string(EngineEventType t) {
        switch (t) {
            case EngineEventType::PARSED: return "PARSED";
            case EngineEventType::DOWNLOAD_FAILED: return "DOWNLOAD_FAILED";
            case EngineEventType::STATUS_UPDATE: return "STATUS_UPDATE";
            case EngineEventType::RESUME_DATA: return "RESUME_DATA";
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
        obj["file_size"] = e.file_size;
        // 区间集合序列化
        boost::json::array intervals_arr;
        for (const auto &[start, end]: e.intervals) {
            boost::json::array interval;
            interval.push_back(start);
            interval.push_back(end);
            intervals_arr.push_back(std::move(interval));
        }
        obj["intervals"] = std::move(intervals_arr);
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
        obj["callback_interval"] = c.status_callback_interval_ms;
        obj["log_level"] = static_cast<int>(c.log_level);
        obj["work_dir"] = c.work_dir ? c.work_dir : "";
        obj["client_id"] = c.client_id ? c.client_id : "";
        return boost::json::serialize(obj);
    }

    /**
     * JSON 字段提取工具：类型匹配则填充，否则跳过。
     * 调用方负责必填字段的非空判断。
     */
    namespace json_util {
        inline void extract(const boost::json::object &obj, const char *key, std::string &out) {
            if (auto *v = obj.if_contains(key); v && v->is_string()) {
                out = v->as_string().c_str();
            }
        }

        inline void extract(const boost::json::object &obj, const char *key, int32_t &out) {
            if (auto *v = obj.if_contains(key); v && v->is_int64()) {
                out = static_cast<int32_t>(v->as_int64());
            }
        }

        inline void extract(const boost::json::object &obj, const char *key, int64_t &out) {
            if (auto *v = obj.if_contains(key); v && v->is_int64()) {
                out = v->as_int64();
            }
        }

        inline void extract(const boost::json::object &obj, const char *key, bool &out) {
            if (auto *v = obj.if_contains(key); v && v->is_bool()) {
                out = v->as_bool();
            }
        }

        inline void extract(const boost::json::object &obj, const char *key, std::vector<int32_t> &out) {
            if (auto *v = obj.if_contains(key); v && v->is_array()) {
                out.clear();
                for (const auto &elem: v->as_array()) {
                    if (elem.is_int64()) {
                        out.push_back(static_cast<int32_t>(elem.as_int64()));
                    }
                }
            }
        }

        inline void extract(const boost::json::object &obj, const char *key, std::vector<std::string> &out) {
            if (auto *v = obj.if_contains(key); v && v->is_array()) {
                out.clear();
                for (const auto &elem: v->as_array()) {
                    if (elem.is_string()) {
                        out.push_back(elem.as_string().c_str());
                    }
                }
            }
        }
    } // namespace json_util

    /* ================================================================== */
    /*                     Config 序列化层                                */
    /* ================================================================== */

    /**
     * 配置结构体（内部 C++ 版本）：用于 FFI 边界的 JSON 序列化/反序列化。
     */
    struct Config {
        // HTTP 配置
        int32_t connect_timeout_seconds = 30;
        int32_t request_timeout_seconds = 300;
        int64_t low_speed_limit_bps = 1024;
        int32_t low_speed_time = 60;
        int32_t max_redirect = 5;
        std::string proxy;
        std::string proxy_username;
        std::string proxy_password;
        std::string user_agent;
        bool verify_ssl = true;
        std::string ca_bundle;
        int32_t max_retries = 3;
        int32_t default_parts = 4;
        int64_t min_size_for_split = 1024 * 1024;

        // BT 配置
        int32_t listen_port = 6881;
        int32_t max_concurrent_downloads = 5;
        int64_t download_rate_limit = 0;
        int64_t upload_rate_limit = 0;
        double seed_ratio_limit = 1.0;

        // 通用配置
        int32_t status_callback_interval_ms = 1000;
        dw_log_level_t log_level = DW_LOG_INFO;
        std::string work_dir;
        std::string client_id;
        std::string save_path;
        std::vector<std::string> trackers;
    };

    // ---- Config JSON 序列化 ----

    inline boost::json::object to_json(const Config &c) {
        boost::json::object obj;
        obj["connect_timeout"] = c.connect_timeout_seconds;
        obj["request_timeout"] = c.request_timeout_seconds;
        obj["low_speed_limit"] = c.low_speed_limit_bps;
        obj["low_speed_time"] = c.low_speed_time;
        obj["max_redirect"] = c.max_redirect;
        obj["proxy"] = c.proxy;
        obj["proxy_username"] = c.proxy_username;
        obj["proxy_password"] = c.proxy_password;
        obj["user_agent"] = c.user_agent;
        obj["verify_ssl"] = c.verify_ssl;
        obj["ca_bundle"] = c.ca_bundle;
        obj["max_retries"] = c.max_retries;
        obj["default_parts"] = c.default_parts;
        obj["min_size_for_split"] = c.min_size_for_split;
        obj["listen_port"] = c.listen_port;
        obj["max_concurrent"] = c.max_concurrent_downloads;
        obj["dl_rate_limit"] = c.download_rate_limit;
        obj["ul_rate_limit"] = c.upload_rate_limit;
        obj["seed_ratio"] = c.seed_ratio_limit;
        obj["callback_interval"] = c.status_callback_interval_ms;
        obj["log_level"] = static_cast<int>(c.log_level);
        obj["work_dir"] = c.work_dir;
        obj["client_id"] = c.client_id;
        obj["save_path"] = c.save_path;
        boost::json::array trackers_arr;
        for (const auto &t: c.trackers) trackers_arr.emplace_back(t);
        obj["trackers"] = std::move(trackers_arr);
        return obj;
    }

    inline void from_json(const boost::json::object &obj, Config &c) {
        json_util::extract(obj, "connect_timeout", c.connect_timeout_seconds);
        json_util::extract(obj, "request_timeout", c.request_timeout_seconds);
        json_util::extract(obj, "low_speed_limit", c.low_speed_limit_bps);
        json_util::extract(obj, "low_speed_time", c.low_speed_time);
        json_util::extract(obj, "max_redirect", c.max_redirect);
        json_util::extract(obj, "proxy", c.proxy);
        json_util::extract(obj, "proxy_username", c.proxy_username);
        json_util::extract(obj, "proxy_password", c.proxy_password);
        json_util::extract(obj, "user_agent", c.user_agent);
        if (auto *v = obj.if_contains("verify_ssl"); v && v->is_bool()) {
            c.verify_ssl = v->as_bool();
        }
        json_util::extract(obj, "ca_bundle", c.ca_bundle);
        json_util::extract(obj, "max_retries", c.max_retries);
        json_util::extract(obj, "default_parts", c.default_parts);
        json_util::extract(obj, "min_size_for_split", c.min_size_for_split);
        json_util::extract(obj, "listen_port", c.listen_port);
        json_util::extract(obj, "max_concurrent", c.max_concurrent_downloads);
        json_util::extract(obj, "dl_rate_limit", c.download_rate_limit);
        json_util::extract(obj, "ul_rate_limit", c.upload_rate_limit);
        if (auto *v = obj.if_contains("seed_ratio"); v && v->is_double()) {
            c.seed_ratio_limit = v->as_double();
        }
        json_util::extract(obj, "callback_interval", c.status_callback_interval_ms);
        if (auto *v = obj.if_contains("log_level"); v && v->is_int64()) {
            c.log_level = static_cast<dw_log_level_t>(v->as_int64());
        }
        json_util::extract(obj, "work_dir", c.work_dir);
        json_util::extract(obj, "client_id", c.client_id);
        json_util::extract(obj, "save_path", c.save_path);
        json_util::extract(obj, "trackers", c.trackers);
    }

    inline int parse_config(const std::string &json, Config &out) {
        try {
            auto obj = boost::json::parse(json).as_object();
            from_json(obj, out);
            return 0;
        } catch (const std::exception &) {
            return -1;
        }
    }

    /**
     * 操作结果（内部版本）：全 std 类型，按值返回无内存管理负担。
     */
    struct dw_submit_result_t {
        dw_reason_t code = DW_REASON_NONE;
        std::string message;
        std::string info_hash;
        std::vector<FileInfo> files;

        dw_submit_result_t() = default;

        /// 构造成功结果
        static dw_submit_result_t success() { return {}; }

        /// 构造失败结果
        static dw_submit_result_t failure(dw_reason_t reason, std::string msg = {}) {
            dw_submit_result_t r;
            r.code = reason;
            r.message = std::move(msg);
            return r;
        }
    };

    class Router;

    /**
     * 下载器全局单例内部实现。
     *
     * 持有 HTTP 和 BT 两个引擎实例，通过 Router 路由到 TaskManager。
     */
    struct dw_downloader {
        std::mutex mutex;
        std::atomic<bool> initialized{false};

        std::unique_ptr<HttpEngine> http_engine;
        std::unique_ptr<TorrentEngine> torrent_engine;
        std::unique_ptr<Router> router; // L2 路由层

        std::atomic<dw_progress_cb> progress_cb{nullptr};
        std::atomic<dw_log_cb> log_cb{nullptr};

        Config config{};
    };

    /**
     * 获取全局单例；若未初始化返回 nullptr。
     */
    dw_downloader *global_downloader();

    /**
     * 安全调用进度回调。
     *
     * 内部检查下载器状态与回调有效性，调用失败（未注册/已销毁）时静默忽略，
     * 不影响业务流程。
     */
    void emit_progress(const dw_progress_t *progress);

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
    /*                     JSON FFI 序列化层                              */
    /* ================================================================== */

    /**
     * 任务三要素（基类）：client_id + protocol + natural_key。
     * 所有任务参数的公共标识字段。
     */
    struct TaskIdentity {
        std::string client_id;
        dw_protocol_t protocol = DW_PROTOCOL_HTTP;
        std::string natural_key;

        /// 复合键：client_id|protocol|natural_key
        std::string union_id() const {
            return client_id + "|" + std::to_string(static_cast<int>(protocol)) + "|" + natural_key;
        }
    };

    /**
     * 任务参数（继承三要素）：用于 FFI 边界的 JSON 序列化/反序列化。
     * 内部使用 C++ 特性（std::string, std::vector），无需跨边界内存管理。
     */
    struct TaskParams : TaskIdentity {
        std::string save_path;
        std::string url;
        std::string info_hash;
        std::string magnet_link;
        std::string torrent_file;
        std::vector<int32_t> file_indexes;
        std::vector<int32_t> priority_file_indexes;
        std::vector<std::string> url_seeds;
        std::vector<uint8_t> resume_data; // 断点续传数据
        int32_t priority = 0;
        bool delete_files = false; // 删除任务时是否同时删除本地文件
        bool force = false;        // 恢复时强制准入（调度器暂停最慢任务让行）
    };

    // ---- JSON 序列化（C++ 对象 → JSON）----

    inline boost::json::object to_json(const TaskIdentity &t) {
        boost::json::object obj;
        obj["client_id"] = t.client_id;
        obj["protocol"] = static_cast<int>(t.protocol);
        obj["natural_key"] = t.natural_key;
        return obj;
    }

    inline boost::json::object to_json(const TaskParams &p) {
        auto obj = to_json(static_cast<const TaskIdentity &>(p));
        if (!p.save_path.empty()) obj["save_path"] = p.save_path;
        if (!p.url.empty()) obj["url"] = p.url;
        if (!p.info_hash.empty()) obj["info_hash"] = p.info_hash;
        if (!p.magnet_link.empty()) obj["magnet_link"] = p.magnet_link;
        if (!p.torrent_file.empty()) obj["torrent_file"] = p.torrent_file;
        if (p.priority != 0) obj["priority"] = p.priority;
        if (p.delete_files) obj["delete_files"] = true;
        if (p.force) obj["force"] = true;

        if (!p.file_indexes.empty()) {
            boost::json::array fi;
            for (auto x: p.file_indexes) fi.push_back(x);
            obj["file_indexes"] = std::move(fi);
        }

        if (!p.priority_file_indexes.empty()) {
            boost::json::array pfi;
            for (auto x: p.priority_file_indexes) pfi.push_back(x);
            obj["priority_file_indexes"] = std::move(pfi);
        }

        if (!p.url_seeds.empty()) {
            boost::json::array seeds;
            for (const auto &s: p.url_seeds) seeds.emplace_back(s);
            obj["url_seeds"] = std::move(seeds);
        }

        return obj;
    }

    // ---- JSON 反序列化（JSON → C++ 对象，宽松模式）----

    inline void from_json(const boost::json::object &obj, TaskIdentity &t) {
        json_util::extract(obj, "client_id", t.client_id);
        if (auto *v = obj.if_contains("protocol"); v && v->is_int64()) {
            t.protocol = static_cast<dw_protocol_t>(v->as_int64());
        }
        json_util::extract(obj, "natural_key", t.natural_key);
    }

    inline void from_json(const boost::json::object &obj, TaskParams &p) {
        // 基类字段
        from_json(obj, static_cast<TaskIdentity &>(p));
        // 派生类字段（宽松提取）
        json_util::extract(obj, "save_path", p.save_path);
        json_util::extract(obj, "url", p.url);
        json_util::extract(obj, "info_hash", p.info_hash);
        json_util::extract(obj, "magnet_link", p.magnet_link);
        json_util::extract(obj, "torrent_file", p.torrent_file);
        json_util::extract(obj, "priority", p.priority);
        json_util::extract(obj, "delete_files", p.delete_files);
        json_util::extract(obj, "force", p.force);
        json_util::extract(obj, "file_indexes", p.file_indexes);
        json_util::extract(obj, "priority_file_indexes", p.priority_file_indexes);
        json_util::extract(obj, "url_seeds", p.url_seeds);
    }

    // ---- FFI 边界：字符串版本 ----

    /// JSON 字符串 → TaskParams，成功返回 0，解析失败返回 -1
    inline int parse_task_params(const std::string &json, TaskParams &out) {
        try {
            auto obj = boost::json::parse(json).as_object();
            from_json(obj, out);
            return 0;
        } catch (const std::exception &) {
            return -1;
        }
    }

    /// TaskParams → JSON 字符串
    inline std::string to_json_string(const TaskParams &p) {
        return boost::json::serialize(to_json(p));
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

    /// 解析 JSON 字符串并提取通用三要素
    inline int parse_identity(const std::string &json, TaskIdentity &out) {
        try {
            auto obj = boost::json::parse(json).as_object();
            from_json(obj, out);
            return 0;
        } catch (const std::exception &) {
            return -1;
        }
    }
} // namespace dw
