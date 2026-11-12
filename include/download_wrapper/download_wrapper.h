/**
 * @file download_wrapper.h
 * @brief 统一多协议下载封装库对外 C ABI。
 *
 * 将 libcurl（HTTP/HTTPS）与 libtorrent（BT/Torrent）封装在同一动态库内，
 * 对外暴露统一的 dw_* C ABI，供 Flutter / Dart FFI / 其他语言调用。
 *
 * 设计原则：
 *   - 所有对外接口均为纯 C ABI（extern "C"），数据结构仅含 C 类型；
 *   - 内部使用 C++20 实现，对外隐藏实现细节；
 *   - 字符串均为 UTF-8、以 '\0' 结尾；
 *   - 回调中所有指针仅在回调周期内有效，调用方如需保存须深拷贝；
 *   - 数值字段默认值统一为 -1 或 0 表示未知 / 无效。
 *
 * 结构体布局规则：
 *   - 通用字段在前，协议特有字段在后；
 *   - 含义相同的字段只保留一份，命名以 libcurl_wrapper 为基准；
 *   - 新增字段仅追加到末尾，保持 ABI 向后兼容。
 */

#pragma once

#ifdef __cplusplus
#include <cstddef>
#include <cstdint>
#else
#include <stddef.h>
#include <stdint.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

#if defined(_WIN32) || defined(__CYGWIN__)
#  ifdef DOWNLOAD_WRAPPER_BUILD
#    define DW_API __declspec(dllexport)
#  else
#    define DW_API __declspec(dllimport)
#  endif
#else
#  define DW_API __attribute__((visibility("default")))
#endif

/* ================================================================== */
/*                              枚举定义                              */
/* ================================================================== */

/**
 * 下载协议类型（同时决定 natural_key 语义与 SQLite protocol 列值）。
 *
 *   HTTP    → natural_key = url
 *   TORRENT → natural_key = info_hash
 *   LOCAL   → natural_key = content_root（save_path 下的实际根目录名）
 */
typedef enum {
    DW_PROTOCOL_HTTP = 0, /**< HTTP/HTTPS 链接下载。 */
    DW_PROTOCOL_TORRENT = 1, /**< BT/Torrent 种子或磁力链接下载。 */
    DW_PROTOCOL_LOCAL = 2, /**< 本地文件任务（无引擎对应）。 */
} dw_protocol_t;

/**
 * 任务唯一键约定：(protocol, natural_key) 二元组，无对应结构体，所有接口平铺传参。
 *
 * natural_key 的取值按协议区分：HTTP=url，TORRENT=info_hash，LOCAL=content_root。
 *   - 入参：由调用方持有，调用期间须保持有效。
 *   - JSON 响应由库分配，调用方通过 release_memory 释放。
 *
 * client_id 不属于唯一键语义：由 App 启动时经初始化 JSON 注入 session，作为
 * 独立入参随接口传递；多客户端 / 远端任务处理暂未实现。
 */

/**
 * 任务状态枚举。
 *
 * 前 4 值与 libcurl_wrapper 的 tw_task_status_t 完全对齐，
 * QUEUED / RESOLVING 为 download_app / torrent 扩展状态。
 */
typedef enum {
    DW_TASK_STATUS_DOWNLOADING = 0, /**< 下载中（元数据已就绪且已确认文件选择）。 */
    DW_TASK_STATUS_PAUSED = 1, /**< 已暂停（BT 未确认文件选择时也回落此态等待用户选择）。 */
    DW_TASK_STATUS_COMPLETED = 2, /**< 已完成（终态）。 */
    DW_TASK_STATUS_ERROR = 3, /**< 已失败（不可重试，终态）。 */
    DW_TASK_STATUS_QUEUED = 4, /**< 排队中（等待调度）。 */
    DW_TASK_STATUS_RESOLVING = 5, /**< 解析中（BT 等待元数据，占用下载名额；
                                         元数据就绪后经事件驱动迁 DOWNLOADING）。 */
    /* 6 = PARSED 已删除，RESOLVING 直接迁 DOWNLOADING */
    DW_TASK_STATUS_INVALIDATED = 7, /**< 已失效（物理文件不存在，仅可删除）。 */
    DW_TASK_STATUS_DELETING = 8, /**< 删除中（已调引擎删除，等待 DELETED 事件回收）。 */
    DW_TASK_STATUS_FAIL = 9, /**< 可重试失败（调度器自动重试）。 */
} dw_task_status_t;

/**
 * 同步返回码 / 终态原因。
 *
 * 用于表示操作失败原因。
 * 也作为操作结果码（add / resume / pause / delete 同步反馈）。
 */
typedef enum {
    DW_REASON_NONE = 0, /**< 成功 / 无错误。 */
    DW_REASON_INTERNAL = 1, /**< 系统 / 代码内部错误。 */
    DW_REASON_NETWORK = 2, /**< 网络问题。 */
    DW_REASON_INVALID_INPUT = 3, /**< 输入非法。 */
    DW_REASON_AUTH = 4, /**< 认证问题。 */
    DW_REASON_ERROR = 5, /**< 通用错误（不可重试）。 */
    DW_REASON_FAIL = 6, /**< 可重试失败（调度器自动重试）。 */
} dw_reason_t;

/** 日志级别（从低到高）。 */
typedef enum {
    DW_LOG_DEBUG = 0,
    DW_LOG_INFO = 1,
    DW_LOG_ERROR = 2,
} dw_log_level_t;

typedef enum {
    DW_SOURCE_LOCAL_FILE = 0, /**< 文件任务（本地文件条目，无引擎对应）。 */
    DW_SOURCE_TASK_FILE = 1, /**< HTTP 任务（用户主动添加的 HTTP 下载任务）。 */
    DW_SOURCE_REMOTE_FILE = 2, /**< BT 任务（用户主动添加的 BT 下载任务）。 */
} dw_source_t;

/* ================================================================== */
/*                          不透明句柄                                */
/* ================================================================== */

/** 下载器全局单例不透明句柄，内部由库自行管理。 */
typedef struct dw_downloader dw_downloader_t;

/* ================================================================== */
/*                          回调类型                                  */
/* ================================================================== */

/**
 * 周期 / 终态进度回调。
 *
 * 在 IO 线程或后台线程上同步调用，回调内严禁反转调用接口。
 * json 为任务状态 JSON 字符串，仅在回调持续期间有效，调用方如需持有须深拷贝。
 */
typedef void (*dw_progress_cb)(const char *json);

/**
 * 日志回调。
 *
 * message / trace_id / func 仅在本次回调返回前有效，调用方如需持有须深拷贝。
 * func 为调用方函数名（通过 __FUNCTION__ 宏捕获），line 为源码行号；
 * 无宏捕获时 func 为空字符串、line 为 0。
 * 未设置日志回调时日志输出到 stderr。
 */
typedef void (*dw_log_cb)(dw_log_level_t level,
                          const char *message,
                          const char *trace_id,
                          const char *func,
                          int32_t line,
                          int64_t timestamp_unix_ms);

/* ================================================================== */
/*                          结构体定义                                */
/* ================================================================== */

/* ================================================================== */
/*                            生命周期                                */
/* ================================================================== */

/** 构造 TaskManager。成功返回 0，失败返回 -1。 */
DW_API char *create(void);

/**
 * 初始化下载器全局单例。
 *
 * 同时初始化 HTTP 引擎（libcurl）和 BT 引擎（libtorrent）。
 * JSON 配置中 client_id 为必填：库内以此为 session 级 clientId 隔离多客户端任务。
 *
 * JSON schema:
 * {
 *   "client_id": "uuid",           // 必填
 *   "work_dir": "/path/to/dir",    // 必填
 *   "max_concurrent": 5,
 *   "listen_port": 6881,
 *   ...
 * }
 *
 * @param config_json  JSON 配置字符串，NULL 或空字符串时使用默认配置。
 * @return             成功返回 JSON 响应（调用者 release_memory 释放），失败返回 NULL。
 *                     响应格式：{"code": 0} 或 {"code": -1, "message": "..."}
 */
DW_API char *init(const char *config_json);

/**
 * 销毁下载器全局单例，释放所有资源。
 *
 * 依次销毁 BT 引擎和 HTTP 引擎，释放全局状态。
 * 可重复调用（已销毁时直接返回）。
 */
DW_API void destroy(void);

/**
 * 动态更新配置。
 *
 * 完整配置会被库内深拷保存，但仅下列字段会即时生效：
 *   - download_rate_limit / upload_rate_limit：下发至 BT session；HTTP 侧均摊到各分片连接
 *     （新建连接即时生效，已建立连接下一次重建时校正）；
 *   - seed_ratio_limit：做种分享率上限；
 *   - max_concurrent_downloads：调度并发上限（调高即时准入排队任务，调低不中断已运行任务）；
 *   - HTTP 超时 / 重试次数 / 默认分片数 / SSL 校验开关（对后续新建连接生效）。
 *
 * 代理、user_agent、ca_bundle、listen_port、work_dir、client_id、trackers 仅在 init 时生效。
 *
 * @param config_json  JSON 配置字符串，不可为 NULL。
 * @return             成功返回 JSON 响应（调用者 release_memory 释放），失败返回 NULL。
 *                     响应格式：{"code": 0} 或 {"code": -1, "message": "..."}
 */
DW_API char *set_config(const char *config_json);

/* ================================================================== */
/*                            回调注册                                */
/* ================================================================== */

/**
 * 设置统一进度回调。
 *
 * HTTP 和 BT 任务的状态变更、周期进度均通过此回调推送。
 * cb 为 NULL 表示清除回调。
 */
DW_API char *set_progress_callback(dw_progress_cb cb);

/**
 * 设置日志回调。
 *
 * 日志级别仅通过 配置 JSON 的 log_level 配置过滤。
 * cb 为 NULL 时日志输出到 stderr。
 */
DW_API char *set_log_callback(dw_log_cb cb);

/* ================================================================== */
/*                            任务接口                                */
/* ================================================================== */

/**
 * 添加单个下载任务。
 *
 * 文件名由库内自动确定：HTTP 从 URL 或响应头解析，BT 从种子元数据获取。
 * 结果经进度回调的 name / output_path / content_root 回报。
 *
 * JSON schema:
 * {
 *   "protocol": 1,                    // 0=HTTP, 1=TORRENT, 2=LOCAL
 *   "client_id": "uuid",              // 必填
 *   "natural_key": "...",             // 必填
 *   "force": false,                   // 可选，默认 false
 *   "save_path": "/downloads",        // 必填
 *   "url": "...",                     // HTTP 必填
 *   "info_hash": "...",               // BT 必填
 *   "magnet_link": "...",             // BT 可选
 *   "torrent_file": "...",            // BT 可选
 *   "file_indexes": [0, 1, 2],        // BT 可选，应用文件优先级的索引
 *   "file_priority": 4,               // BT 可选，范围 0–7，默认 4
 *   "url_seeds": [],                  // 可选
 *   "priority": 0                     // 可选
 * }
 *
 * @param params_json  JSON 参数字符串，不可为 NULL。
 * @return             成功返回 JSON 响应（调用者 release_memory 释放），失败返回 NULL。
 *                     响应格式：{"code": 0, "data": {...}} 或 {"code": -1, "message": "..."}
 */
DW_API char *add_task(const char *params_json);

/**
 * 暂停单个任务。
 *
 * JSON schema:
 * {
 *   "protocol": 1,
 *   "client_id": "uuid",
 *   "natural_key": "..."
 * }
 *
 * @param params_json  JSON 参数字符串，不可为 NULL。
 * @return             成功返回 JSON 响应（调用者 release_memory 释放），失败返回 NULL。
 */
DW_API char *pause_task(const char *params_json);

/**
 * 恢复（继续）单个任务。
 *
 * JSON schema 同 pause_task。BT 可选填 file_priorities，例如 {"0":4,"2":7}。
 * 键为文件索引，值为 0 到 7 的下载优先级。
 *
 * @param params_json  JSON 参数字符串，不可为 NULL。
 * @return             成功返回 JSON 响应（调用者 release_memory 释放），失败返回 NULL。
 */
DW_API char *resume_task(const char *params_json);

/**
 * 删除单个任务。
 *
 * JSON schema:
 * {
 *   "protocol": 1,
 *   "client_id": "uuid",
 *   "natural_key": "...",
 *   "delete_files": true
 * }
 *
 * @param params_json  JSON 参数字符串，不可为 NULL。
 * @return             成功返回 JSON 响应（调用者 release_memory 释放），失败返回 NULL。
 */
DW_API char *delete_task(const char *params_json);

/* ================================================================== */
/*                         BT 工具函数                                */
/* ================================================================== */

/**
 * 从磁力链接解析 info_hash。
 *
 * 仅做解析，不创建任务、不依赖 session。
 *
 * @param params_json  JSON 字符串：{"magnet_link": "..."}
 * @return             成功返回 JSON 响应（调用者 release_memory 释放）。
 *                     响应格式：{"code": 0, "data": {"info_hash": "..."}} 或 {"code": -1, "message": "..."}
 */
DW_API char *parse_magnet(const char *params_json);

/**
 * 从 .torrent 文件解析 info_hash 和文件列表。
 *
 * 仅做解析，不创建任务、不依赖 session。
 *
 * @param params_json  JSON 字符串：{"torrent_file": "..."}
 * @return             成功返回 JSON 响应（调用者 release_memory 释放）。
 *                     响应格式：{"code": 0, "data": {"info_hash": "...", "files": [...]}} 或 {"code": -1, "message": "..."}
 */
DW_API char *parse_torrent_file(const char *params_json);

/* ================================================================== */
/*                        任务快照与队列                              */
/* ================================================================== */

/**
 * 获取指定客户端的全部任务持久化快照。
 *
 * 用于 App 启动时一次性还原任务列表（含已完成 / 暂停 / 排队 / 下载中）。
 * 数据来自库内 SQLite，无需引擎运行即可返回。
 *
 * @param params_json  JSON 字符串：{"client_id": "..."}
 * @return             成功返回 JSON 响应（调用者 release_memory 释放），失败返回 NULL。
 *                     响应格式：{"code": 0, "data": {"tasks": [...]}} 或 {"code": -1, "message": "..."}
 */
DW_API char *list_tasks(const char *params_json);

/* ================================================================== */
/*                        任务文件查询                                */
/* ================================================================== */

/**
 * 本地目录扫描接口暂不可用，保留符号供后续实现。
 *
 * @param params_json  JSON 字符串：{"client_id": "...", "catalog_paths": ["/path1", "/path2"]}
 * @return             返回错误 JSON（调用者 release_memory 释放）。
 */
DW_API char *scan_local_file(const char *params_json);

/* ================================================================== */
/*                          资源释放                                  */
/* ================================================================== */

/**
 * 通用内存释放。
 *
 * 用于释放由本库返回的堆分配内存（如 parse_magnet 返回的字符串）。
 *
 * @param ptr  待释放的内存指针，NULL 时无操作。
 */
DW_API void release_memory(void *ptr);

#ifdef __cplusplus
} /* extern "C" */
#endif
