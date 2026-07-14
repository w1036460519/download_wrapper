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

#ifndef DOWNLOAD_WRAPPER_H
#define DOWNLOAD_WRAPPER_H

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

/** 下载协议类型。 */
typedef enum {
    DW_PROTOCOL_HTTP    = 0, /**< HTTP/HTTPS 链接下载。 */
    DW_PROTOCOL_TORRENT = 1, /**< BT/Torrent 种子或磁力链接下载。 */
} dw_protocol_t;

/**
 * 任务状态枚举。
 *
 * 前 4 值与 libcurl_wrapper 的 tw_task_status_t 完全对齐，
 * 后 3 值为 download_app / torrent 扩展状态。
 */
typedef enum {
    DW_TASK_STATUS_DOWNLOADING = 0, /**< 下载中。 */
    DW_TASK_STATUS_PAUSED      = 1, /**< 已暂停。 */
    DW_TASK_STATUS_COMPLETED   = 2, /**< 已完成（终态）。 */
    DW_TASK_STATUS_ERROR       = 3, /**< 已失败（终态）。 */
    DW_TASK_STATUS_QUEUED      = 4, /**< 排队中（等待调度）。 */
    DW_TASK_STATUS_PARSING     = 5, /**< 解析中（BT 元数据获取阶段）。 */
    DW_TASK_STATUS_PARSED      = 6, /**< 解析完成（元数据已接收）。 */
} dw_task_status_t;

/**
 * 同步返回码 / 终态原因。
 *
 * 既作为 dw_progress_t.reason（仅 task_status == ERROR 时有效），
 * 也作为 dw_submit_result_t.code（add / resume / pause / delete 同步反馈）。
 */
typedef enum {
    DW_REASON_NONE          = 0, /**< 成功 / 无错误。 */
    DW_REASON_INTERNAL      = 1, /**< 系统 / 代码内部错误。 */
    DW_REASON_NETWORK       = 2, /**< 网络问题。 */
    DW_REASON_INVALID_INPUT = 3, /**< 输入非法。 */
    DW_REASON_AUTH          = 4, /**< 认证问题。 */
} dw_reason_t;

/** 日志级别（从低到高）。 */
typedef enum {
    DW_LOG_DEBUG = 0,
    DW_LOG_INFO  = 1,
    DW_LOG_ERROR = 2,
} dw_log_level_t;

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
 * progress 内所有指针仅在回调持续期间有效。
 */
typedef void (*dw_progress_cb)(const struct dw_progress* progress);

/**
 * 日志回调。
 *
 * message / trace_id / func 仅在本次回调返回前有效，调用方如需持有须深拷贝。
 * func 为调用方函数名（通过 __FUNCTION__ 宏捕获），line 为源码行号；
 * 无宏捕获时 func 为空字符串、line 为 0。
 * 未设置日志回调时日志输出到 stderr。
 */
typedef void (*dw_log_cb)(dw_log_level_t  level,
                          const char*     message,
                          const char*     trace_id,
                          const char*     func,
                          int32_t         line,
                          int64_t         timestamp_unix_ms);

/**
 * 断点续传数据回调。
 *
 * 当引擎生成 / 更新任务的断点续传数据时触发（如暂停、周期保存等时机）。
 * data 指向 resume_data 字节缓冲、size 为长度，二者仅在本次回调周期内有效，
 * 调用方如需持久化必须立即深拷贝。
 * 目前仅 BT（libtorrent）任务会触发；HTTP 任务不产生 resume_data。
 */
typedef void (*dw_resume_data_cb)(const char*    task_id,
                                  dw_protocol_t  protocol,
                                  const uint8_t* data,
                                  size_t         size);

/* ================================================================== */
/*                          结构体定义                                */
/* ================================================================== */

/* ------------------------------------------------------------------ */
/*  dw_part_state_t — 分片状态（HTTP 分片下载，BT 不使用）            */
/* ------------------------------------------------------------------ */

/**
 * 单个分片状态。
 *
 * 运行状态复用 dw_task_status_t 与 dw_reason_t。
 * BT 任务无分片，dw_progress_t.part_count = 0、part_states = NULL。
 */
typedef struct dw_part_state {
    int32_t          index;    /**< 分片序号（0 起）。 */
    int64_t          start;    /**< 分片在目标文件中的起始字节（含）。 */
    int64_t          end;      /**< 分片结束字节（含）。 */
    int64_t          size;     /**< 分片总字节 = end - start + 1。 */
    int64_t          done;     /**< 本分片已下载字节。 */
    double           progress; /**< 本分片进度 0.0-1.0；size=0 时为 -1。 */
    double           download_rate; /**< 分片实时速率（B/s）。 */
    dw_task_status_t status;   /**< 分片运行状态。 */
    dw_reason_t      reason;   /**< 仅 status==ERROR 时有效。 */
} dw_part_state_t;

/* ------------------------------------------------------------------ */
/*  dw_file_info_t — BT 文件信息                                     */
/* ------------------------------------------------------------------ */

/**
 * 单个文件信息（BT 多文件种子）。
 *
 * name / size 字段由库分配，通过 dw_file_list_free 统一释放。
 * HTTP 任务不使用。
 */
typedef struct dw_file_info {
    int32_t  index; /**< 文件索引（用于设置优先级）。 */
    char*    name;  /**< 文件相对路径名。 */
    int64_t  size;  /**< 文件大小（字节）。 */
} dw_file_info_t;

/* ------------------------------------------------------------------ */
/*  dw_progress_t — 进度 / 状态推送 payload                          */
/* ------------------------------------------------------------------ */

/**
 * 进度 / 终态推送 payload。
 *
 * 同时充当进度回调的传出数据和 dw_add_task / dw_resume_task 的入参。
 * 布局规则：通用字段在前，HTTP 特有字段居中，BT 特有字段在后，
 * 分片数组在末尾。
 *
 * 所有 const char* 指针仅在回调 / 入参生命周期内有效。
 * 数值字段默认 -1 或 0 表示未知。
 */
typedef struct dw_progress {
    /* ===== 通用字段（所有协议共用） ===== */

    const char*      task_id;          /**< 任务标识：HTTP=URL，BT=info_hash。 */
    const char*      trace_id;         /**< 日志追踪 ID；BT 可为空字符串。 */
    dw_protocol_t    protocol;         /**< 协议类型。 */
    const char*      name;             /**< 任务显示名称。 */
    const char*      output_path;      /**< 保存目录绝对路径（不含文件名）。 */
    const char*      filename;         /**< 目标文件名（不含目录）。 */
    int64_t          total_size;       /**< 总大小（字节）；-1=未知。 */
    int64_t          total_done;       /**< 已完成字节；-1=未知。 */
    int64_t          remaining;        /**< 剩余字节；-1=未知。 */
    double           progress;         /**< 进度 0.0-1.0；-1=未知。 */
    double           download_rate;    /**< 下载速率（B/s）。 */
    double           eta;              /**< 预计剩余秒数；-1=未知。 */
    dw_task_status_t task_status;      /**< 任务当前状态。 */
    dw_reason_t      reason;           /**< 失败原因；仅 ERROR 时有效。 */
    const char*      message;          /**< 状态描述 / 错误文本。 */
    int64_t          saved_at_unix_ms; /**< 快照生成时间戳（Unix 毫秒）。 */

    /* ===== HTTP 特有字段 ===== */

    int32_t          support_range;    /**< 服务器是否支持 Range：0/1。 */
    const char*      etag;             /**< HTTP ETag。 */
    const char*      last_modified;    /**< HTTP Last-Modified。 */
    int32_t          probing;          /**< 探测状态：1=需探测，0=已探测。 */

    /* ===== BT 特有字段 ===== */

    double           upload_rate;      /**< 上传速率（B/s）。 */
    int64_t          total_upload;     /**< 已上传字节。 */
    int32_t          is_seeding;       /**< 是否做种：0/1。 */
    int32_t          state;            /**< libtorrent 原始 state_t 值。 */
    int32_t          peers_count;      /**< 当前 peer 连接数。 */

    /* ===== 分片（共用，BT 任务 part_count=0） ===== */

    const dw_part_state_t* part_states; /**< 分片状态数组。 */
    int32_t                part_count;  /**< 分片数量。 */
} dw_progress_t;

/* ------------------------------------------------------------------ */
/*  dw_task_params_t — 添加 / 恢复任务参数                            */
/* ------------------------------------------------------------------ */

/**
 * 添加 / 恢复任务时的参数结构体。
 *
 * 通用字段在前，协议特有字段在后。
 * 库仅读取与当前 protocol 相关的字段，其余忽略。
 * 所有字符串由调用方管理生命周期；库内深拷贝。
 */
typedef struct dw_task_params {
    /* ===== 通用字段 ===== */

    const char*    task_id;          /**< 任务标识：HTTP=URL，BT=info_hash。 */
    const char*    save_path;        /**< 保存目录（必填）。 */
    const char*    filename;         /**< 目标文件名（HTTP 必填）。 */
    int32_t        auto_start;       /**< 1=立即开始，0=暂停状态。 */
    const uint8_t* resume_data;      /**< 断点续传数据（可为 NULL）。 */
    size_t         resume_data_size; /**< resume_data 字节长度。 */

    /* ===== HTTP 特有 ===== */

    const char*    url;              /**< 下载 URL（与 task_id 可同值）。 */
    const char*    trace_id;         /**< 追踪 ID。 */

    /* ===== BT 特有 ===== */

    const char*    magnet_link;      /**< 磁力链接（优先级最高）。 */
    const char*    torrent_file;     /**< .torrent 文件路径。 */
    const char**   trackers;         /**< tracker URL 数组（可为 NULL）。 */
    int32_t        tracker_count;    /**< trackers 数组长度。 */
    const int32_t* file_indexes;     /**< 待下载文件索引数组（NULL=全部）。 */
    int32_t        file_index_size;  /**< file_indexes 数组长度。 */
    const char**   url_seeds;        /**< Web Seed URL 数组（BEP 19）。 */
    int32_t        url_seed_count;   /**< url_seeds 数组长度。 */

    /* ===== 队列（通用，追加保持 ABI 兼容） ===== */

    int32_t        priority;         /**< 队列优先级：越大越优先，默认 0；同级按提交顺序 FIFO。 */
} dw_task_params_t;

/* ------------------------------------------------------------------ */
/*  dw_submit_result_t — 同步返回结果                                 */
/* ------------------------------------------------------------------ */

/**
 * 同步返回结果（每个任务一条，与入参顺序对应）。
 *
 * task_id：HTTP 任务回传 URL，BT 任务回传 info_hash。
 *           添加 BT 磁力链接时，库内解析后生成 info_hash 写入此字段。
 *           该字段引用入参或库内分配的字符串，由 dw_submit_results_release 统一释放。
 * code：    同步返回码；DW_REASON_NONE 表示成功。
 * message： 错误描述；成功时为 NULL，由库分配并通过 dw_submit_results_release 释放。
 */
typedef struct dw_submit_result {
    const char* task_id;
    dw_reason_t code;
    char*       message;
} dw_submit_result_t;

/* ------------------------------------------------------------------ */
/*  dw_config_t — 下载器全局配置                                      */
/* ------------------------------------------------------------------ */

/**
 * 全局配置结构体。
 *
 * 调用方必须传入非空指针，零值 / NULL 字段由库内填充默认值。
 * 字符串字段由库内 strdup 深拷贝，调用方可安全回收入参。
 *
 * 布局：HTTP 配置 → BT 配置 → 通用配置。
 */
typedef struct dw_config {
    /* ===== HTTP 配置（libcurl） ===== */

    int32_t     connect_timeout_seconds;     /**< 连接超时（秒）。 */
    int32_t     request_timeout_seconds;     /**< 请求超时（秒）。 */
    int32_t     low_speed_limit_bps;         /**< 低速阈值（B/s）；0=关闭。 */
    int32_t     low_speed_time;              /**< 低速持续时间（秒）。 */
    int32_t     max_redirect;                /**< 最大重定向次数。 */
    const char* proxy;                       /**< 代理地址（NULL=禁用）。 */
    const char* proxy_username;              /**< 代理用户名。 */
    const char* proxy_password;              /**< 代理密码。 */
    const char* user_agent;                  /**< User-Agent。 */
    int32_t     verify_ssl;                  /**< SSL 校验：0=跳过，1=校验。 */
    const char* ca_bundle;                   /**< 自定义 CA 证书路径。 */
    int32_t     max_retries;                 /**< 单分片最大重试次数。 */
    int32_t     default_parts;               /**< 默认分片数。 */
    int64_t     min_size_for_split;          /**< 触发分片的最小文件（字节）。 */
    int32_t     max_concurrent_connections;  /**< 最大并发连接数。 */

    /* ===== BT 配置（libtorrent） ===== */

    int32_t     listen_port;                 /**< BT 监听端口；0=随机。 */
    int32_t     max_concurrent_downloads;    /**< 全局最大并发下载数（队列限流，HTTP+BT 共用）；<=0 时库内取默认值 3。 */
    int32_t     download_rate_limit;         /**< 下载限速（B/s）；0=不限。 */
    int32_t     upload_rate_limit;           /**< 上传限速（B/s）；0=不限。 */
    const char* session_state_path;          /**< session 状态保存目录。 */
    const char* dht_routers;                 /**< DHT 引导节点。 */

    /* ===== 通用配置 ===== */

    int32_t        status_callback_interval_ms; /**< 进度回调间隔（ms）。 */
    dw_log_level_t log_level;                   /**< 日志级别。 */
    const char*    work_dir;                    /**< 工作目录（临时文件等）。 */

    /* ===== 追加字段（置于末尾以保持既有字段偏移的 ABI 兼容） ===== */

    double seed_ratio_limit;   /**< BT 做种分享率上限：total_upload/total_done 达到该值后释放做种上下文；0=库内默认 3.0（即下载:上传=1:3），<0=永久做种。 */

    int32_t wifi_only;         /**< 仅 WiFi 下载开关：0=不限网络（默认），1=仅在 WiFi 下下载。 */
    int32_t is_wifi;           /**< 当前网络是否为 WiFi（由调用方实时上报）：0=否，1=是；仅当 wifi_only=1 时参与流量闸门判定。
                                    闸门关闭条件：wifi_only!=0 && is_wifi==0，此时暂停所有下载与做种流量。 */
} dw_config_t;

/* ------------------------------------------------------------------ */
/*  dw_task_snapshot_t — 任务列表快照（启动恢复用）                   */
/* ------------------------------------------------------------------ */

/**
 * 单个任务的持久化快照。
 *
 * 由 dw_list_tasks 返回，用于 App 启动时一次性还原任务列表；
 * 之后的实时变更仍通过 dw_progress_cb 增量推送。
 * 所有字符串由库分配，整个数组通过 dw_task_list_free 统一释放。
 */
typedef struct dw_task_snapshot {
    char*            task_id;      /**< 任务标识：HTTP=URL，BT=info_hash。 */
    dw_protocol_t    protocol;     /**< 协议类型。 */
    char*            name;         /**< 任务显示名称。 */
    char*            save_path;    /**< 保存目录。 */
    char*            filename;     /**< 目标文件名（可能为空串）。 */
    dw_task_status_t status;       /**< 持久化的任务状态。 */
    double           progress;     /**< 进度 0.0-1.0；-1=未知。 */
    int64_t          total_size;   /**< 总大小（字节）；-1=未知。 */
    int64_t          total_done;   /**< 已完成字节；-1=未知。 */
    int32_t          priority;     /**< 队列优先级。 */
    int64_t          created_at;   /**< 创建时间（Unix 毫秒）。 */
} dw_task_snapshot_t;

/* ================================================================== */
/*                            生命周期                                */
/* ================================================================== */

/**
 * 初始化下载器全局单例。
 *
 * 同时初始化 HTTP 引擎（libcurl）和 BT 引擎（libtorrent）。
 *
 * @param cfg  全局配置指针，NULL 时使用默认配置。
 * @return     0=成功，-1=失败。
 */
DW_API int32_t dw_init(const dw_config_t* cfg);

/**
 * 销毁下载器全局单例，释放所有资源。
 *
 * 依次销毁 BT 引擎和 HTTP 引擎，释放全局状态。
 * 可重复调用（已销毁时直接返回）。
 */
DW_API void dw_destroy(void);

/**
 * 动态更新配置。
 *
 * 仅更新可热更新的字段（如限速、代理等），不影响已运行任务的核心参数。
 *
 * @param cfg  新配置指针，不可为 NULL。
 * @return     0=成功，-1=失败。
 */
DW_API int32_t dw_set_config(const dw_config_t* cfg);

/* ================================================================== */
/*                            回调注册                                */
/* ================================================================== */

/**
 * 设置统一进度回调。
 *
 * HTTP 和 BT 任务的状态变更、周期进度均通过此回调推送。
 * cb 为 NULL 表示清除回调。
 */
DW_API void dw_set_progress_callback(dw_progress_cb cb);

/**
 * 设置日志回调。
 *
 * 日志级别仅通过 dw_config_t.log_level 配置过滤。
 * cb 为 NULL 时日志输出到 stderr。
 */
DW_API void dw_set_log_callback(dw_log_cb cb);

/**
 * 设置断点续传数据回调。
 *
 * 引擎生成 resume_data 时通过此回调输出，供上层持久化。
 * cb 为 NULL 表示清除回调。
 */
DW_API void dw_set_resume_data_callback(dw_resume_data_cb cb);

/* ================================================================== */
/*                            任务接口                                */
/* ================================================================== */

/**
 * 添加单个下载任务。
 *
 * @param protocol    协议类型。
 * @param params      任务参数指针，不可为 NULL。
 * @param out_result  同步返回结果指针，不可为 NULL。
 * @return            0=成功，-1=失败（参数非法或内部错误）。
 */
DW_API int32_t dw_add_task(dw_protocol_t           protocol,
                           const dw_task_params_t* params,
                           dw_submit_result_t*     out_result);

/**
 * 暂停单个任务。
 *
 * @param protocol    协议类型。
 * @param id          任务标识符（HTTP=URL，BT=info_hash），不可为 NULL。
 * @param out_result  同步返回结果指针，不可为 NULL。
 * @return            0=成功，-1=失败（参数非法或内部错误）。
 */
DW_API int32_t dw_pause_task(dw_protocol_t       protocol,
                             const char*         id,
                             dw_submit_result_t* out_result);

/**
 * 恢复（继续）单个任务。
 *
 * @param protocol    协议类型。
 * @param params      任务参数指针（恢复时可能需要 save_path / resume_data 等）。
 * @param out_result  同步返回结果指针，不可为 NULL。
 * @return            0=成功，-1=失败（参数非法或内部错误）。
 */
DW_API int32_t dw_resume_task(dw_protocol_t           protocol,
                              const dw_task_params_t* params,
                              dw_submit_result_t*     out_result);

/**
 * 删除单个任务。
 *
 * @param protocol    协议类型。
 * @param id          任务标识符，不可为 NULL。
 * @param out_result  同步返回结果指针，不可为 NULL。
 * @return            0=成功，-1=失败（参数非法或内部错误）。
 */
DW_API int32_t dw_delete_task(dw_protocol_t       protocol,
                              const char*         id,
                              dw_submit_result_t* out_result);

/* ================================================================== */
/*                         BT 工具函数                                */
/* ================================================================== */

/**
 * 从磁力链接解析 info_hash。
 *
 * 仅做解析，不创建任务、不依赖 session。
 *
 * @param magnet_link  磁力链接字符串，不可为 NULL。
 * @return             成功返回堆分配的 info_hash hex 字符串（调用者 dw_free 释放），
 *                     失败返回 NULL。
 */
DW_API char* dw_magnet_to_info_hash(const char* magnet_link);

/**
 * 从 .torrent 文件解析 info_hash。
 *
 * 仅做解析，不创建任务、不依赖 session。
 *
 * @param torrent_file_path  .torrent 文件路径，不可为 NULL。
 * @return                   成功返回堆分配的 info_hash hex 字符串（调用者 dw_free 释放），
 *                           失败返回 NULL。
 */
DW_API char* dw_torrent_file_to_info_hash(const char* torrent_file_path);

/**
 * 通过 info_hash 获取磁力链接。
 *
 * 任务必须已存在于 session 中。
 *
 * @param info_hash  任务 info_hash，不可为 NULL。
 * @return           成功返回堆分配的磁力链接（调用者 dw_free 释放），失败返回 NULL。
 */
DW_API char* dw_info_hash_to_magnet(const char* info_hash);

/**
 * 设置 BT 任务的文件下载优先级。
 *
 * @param info_hash   任务 info_hash，不可为 NULL。
 * @param file_index  文件索引。
 * @param priority    优先级：0=不下载，1=正常，7=最高。
 * @return            1=成功，0=失败。
 */
DW_API int dw_set_file_priority(const char* info_hash,
                                int32_t     file_index,
                                int32_t     priority);

/**
 * 本地解析 .torrent 文件（不创建任务、不依赖 session）。
 *
 * 用于添加任务前获取文件列表，供前端展示文件选择对话框。
 *
 * @param torrent_file_path  .torrent 文件路径，不可为 NULL。
 * @param out_info_hash      输出：堆分配的 info_hash hex 字符串（调用者 dw_free 释放）。
 * @param out_files          输出：堆分配的文件信息数组（调用者 dw_file_list_free 释放）。
 * @param out_count          输出：文件数量。
 * @return                   0=成功，-1=失败。
 */
DW_API int32_t dw_parse_torrent_file(const char*      torrent_file_path,
                                     char**           out_info_hash,
                                     dw_file_info_t** out_files,
                                     int32_t*         out_count);

/**
 * 获取已存在任务的文件列表（元数据就绪后可用）。
 *
 * 用于磁力链接任务 PARSING → PARSED 后获取文件列表。
 *
 * @param task_id     任务标识（BT=info_hash），不可为 NULL。
 * @param out_files   输出：堆分配的文件信息数组（调用者 dw_file_list_free 释放）。
 * @param out_count   输出：文件数量。
 * @return            0=成功，-1=失败（元数据未就绪或任务不存在）。
 */
DW_API int32_t dw_get_file_list(const char*      task_id,
                                dw_file_info_t** out_files,
                                int32_t*         out_count);

/* ================================================================== */
/*                        任务快照与队列                              */
/* ================================================================== */

/**
 * 获取全部任务的持久化快照。
 *
 * 用于 App 启动时一次性还原任务列表（含已完成 / 暂停 / 排队 / 下载中）。
 * 数据来自库内 SQLite，无需引擎运行即可返回。
 *
 * @param out_tasks  输出：堆分配的快照数组（调用者 dw_task_list_free 释放）。
 * @param out_count  输出：任务数量。
 * @return           0=成功，-1=失败。
 */
DW_API int32_t dw_list_tasks(dw_task_snapshot_t** out_tasks,
                             int32_t*             out_count);

/**
 * 设置任务队列优先级（越大越优先）。
 *
 * 立即持久化并触发一次队列调度；对下载中任务仅更新优先级、不中断。
 *
 * @param task_id   任务标识，不可为 NULL。
 * @param priority  新优先级值。
 * @return          0=成功，-1=失败（任务不存在）。
 */
DW_API int32_t dw_set_task_priority(const char* task_id, int32_t priority);

/* ================================================================== */
/*                          资源释放                                  */
/* ================================================================== */

/**
 * 释放单个 dw_submit_result_t 中 message 占用的内存。
 *
 * @param result  结果指针，NULL 时无操作。
 */
DW_API void dw_submit_result_release(dw_submit_result_t* result);

/**
 * 释放文件信息数组。
 *
 * @param files  文件信息数组。
 * @param count  数组长度。
 */
DW_API void dw_file_list_free(dw_file_info_t* files, int32_t count);

/**
 * 释放任务快照数组（含各字段字符串）。
 *
 * @param tasks  dw_list_tasks 返回的数组。
 * @param count  数组长度。
 */
DW_API void dw_task_list_free(dw_task_snapshot_t* tasks, int32_t count);

/**
 * 通用内存释放。
 *
 * 用于释放由本库返回的堆分配内存（如 dw_magnet_to_info_hash 等返回的字符串）。
 *
 * @param ptr  待释放的内存指针，NULL 时无操作。
 */
DW_API void dw_free(void* ptr);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* DOWNLOAD_WRAPPER_H */
