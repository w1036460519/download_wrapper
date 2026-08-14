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

/** 下载协议类型。 */
typedef enum {
    DW_PROTOCOL_HTTP    = 0, /**< HTTP/HTTPS 链接下载。 */
    DW_PROTOCOL_TORRENT = 1, /**< BT/Torrent 种子或磁力链接下载。 */
} dw_protocol_t;

/**
 * 任务键类型：决定 natural_key 字段的语义，并同步 SQLite 库内 key_type 列。
 *
 *   HTTP  → natural_key = url
 *   BT    → natural_key = info_hash
 *   LOCAL → natural_key = content_root（save_path 下的实际根目录名）
 */
typedef enum {
    DW_KEY_TYPE_HTTP  = 0,
    DW_KEY_TYPE_BT    = 1,
    DW_KEY_TYPE_LOCAL = 2,
} dw_key_type_t;

/**
 * 任务唯一键：(key_type, natural_key) 二元组。
 *
 *   - 输入时（函数入参）：natural_key 由调用方持有，调用期间须保持有效。
 *   - 输出时（dw_progress_t / dw_submit_result_t / dw_task_snapshot_t 内嵌 key）：natural_key 由库
 *     分配并随宿主结构体一同释放（dw_free / dw_task_list_free / dw_submit_result_release）。
 *
 * clientId 不在此结构中：clientId 由 App 启动时经 dw_config_t 注入 session，所有任务的 clientId
 * 均取自 session 配置；本机任务不需要每调用携带，多客户端 / 远端任务处理暂未实现。
 */
typedef struct dw_task_key {
    dw_key_type_t key_type;
    const char*   natural_key;
} dw_task_key_t;

/**
 * 任务状态枚举。
 *
 * 前 4 值与 libcurl_wrapper 的 tw_task_status_t 完全对齐，
 * QUEUED / RESOLVING 为 download_app / torrent 扩展状态。
 */
typedef enum {
    DW_TASK_STATUS_DOWNLOADING = 0, /**< 下载中（元数据已就绪且已确认文件选择）。 */
    DW_TASK_STATUS_PAUSED      = 1, /**< 已暂停（BT 未确认文件选择时也回落此态等待用户选择）。 */
    DW_TASK_STATUS_COMPLETED   = 2, /**< 已完成（终态）。 */
    DW_TASK_STATUS_ERROR       = 3, /**< 已失败（终态）。 */
    DW_TASK_STATUS_QUEUED      = 4, /**< 排队中（等待调度）。 */
    DW_TASK_STATUS_RESOLVING   = 5, /**< 解析中（BT 等待元数据，占用下载名额；
                                         元数据就绪后经事件驱动迁 PARSED）。 */
    DW_TASK_STATUS_PARSED      = 6, /**< 解析完成（元数据就绪 + 冲突检测通过，
                                         文件列表已落库；调度准入后迁 DOWNLOADING）。 */
    DW_TASK_STATUS_INVALIDATED = 7, /**< 已失效（物理文件不存在，仅可删除）。 */
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
    DW_REASON_ERROR         = 5, /**< 通用错误。 */
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

/* ================================================================== */
/*                          结构体定义                                */
/* ================================================================== */

/* ------------------------------------------------------------------ */
/*  dw_file_info_t — BT 文件信息                                     */
/* ------------------------------------------------------------------ */

/**
 * 单个文件信息（扁平文件列表）。
 *
 * name 为相对路径（含目录），ext 为后缀（不含点）。
 * name / ext 由库分配，统一经 dw_file_list_free 释放。
 * HTTP 任务不使用。
 */
typedef struct dw_file_info {
    int32_t  index;      /**< libtorrent 文件索引（用于设置优先级）。 */
    char*    name;       /**< 相对路径（含目录）。 */
    int64_t  size;       /**< 文件字节数。 */
    char*    ext;        /**< 后缀（不含点，如 mkv）；可为 NULL。 */
    int32_t  status;     /**< 文件状态：0=下载中，1=磁盘已删除，2=完成正常。 */
    int64_t  offset;     /**< 文件在 torrent 全局字节流中的起始偏移（HTTP 单文件为 0）。 */
    int64_t  downloaded_bytes; /**< 已下载字节数（BT 从 piece bitmap 推算）。 */
    int64_t  play_position_ms; /**< 播放进度（毫秒）。 */
} dw_file_info_t;

/* ------------------------------------------------------------------ */
/*  dw_byte_range_t — 文件内已下载字节区间                            */
/* ------------------------------------------------------------------ */

/**
 * 文件内已下载字节区间（已合并连续段）。
 *
 * end 采用"含"约定，与 HTTP 分片区间语义一致（不引入第二套区间语义）。
 * 由 dw_get_file_ranges 分配数组，dw_byte_range_free 统一释放。
 */
typedef struct dw_byte_range {
    int64_t start; /**< 文件内起始字节（含）。 */
    int64_t end;   /**< 文件内结束字节（含）。 */
} dw_byte_range_t;

/* ------------------------------------------------------------------ */
/*  dw_progress_t — 进度 / 状态推送 payload                          */
/* ------------------------------------------------------------------ */

/**
 * 进度 / 终态推送 payload。
 *
 * 同时充当进度回调的传出数据和 dw_add_task / dw_resume_task 的入参。
 * 布局规则：通用字段在前，HTTP 特有字段居中，BT 特有字段在后，
 * 交互主键 id 在末尾。
 *
 * 所有 const char* 指针仅在回调 / 入参生命周期内有效。
 * remaining / eta 由 wrapper 层由 total_size / total_done / download_rate 现算后回填。
 * 数值字段默认 -1 或 0 表示未知。
 */
typedef struct dw_progress {
    /* ===== 通用字段（所有协议共用） ===== */

    const char*      url;              /**< 下载 URL：HTTP 任务的识别键与展示；BT 为空串。 */
    const char*      info_hash;        /**< 种子 info_hash：BT 任务的识别键与展示；HTTP 为空串。 */
    dw_protocol_t    protocol;         /**< 协议类型。 */
    const char*      name;             /**< 任务显示名称。 */
    const char*      output_path;      /**< 用户指定的保存目录 save_path（恒不变）。实际落盘目录 = output_path / content_root。 */
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

    /* ===== 扩展字段（追加，保持既有字段偏移） ===== */

    int32_t          source;         /**< 来源：0=本地任务 1=本地文件 2=远程文件。 */
    dw_task_key_t    key;            /**< 任务唯一键：key_type + natural_key。natural_key 由库分配。 */

    /* ===== HTTP 特有字段 ===== */

    int32_t          support_range;    /**< 服务器是否支持 Range：0=不支持（200，单分片全量），1=支持（206，可分片并发/续传）。 */
    const char*      etag;             /**< HTTP ETag。 */
    const char*      last_modified;    /**< HTTP Last-Modified。 */

    /* ===== BT 特有字段 ===== */

    double           upload_rate;      /**< 上传速率（B/s）。 */

    /* ===== content_root（追加，保持既有字段偏移） ===== */

    const char*      content_root;     /**< save_path 下的实际根目录名。物理路径 = output_path / content_root。
                                                空串 = 尚未定名（PARSED 前）。 */
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

    const char*    save_path;        /**< 保存目录（必填）。 */
    const char*    filename;         /**< 已定名的目标文件名：库内派发引擎时回填的定名结果。
                                          dw_add_task 忽略此字段——文件名一律由库内判重定名
                                          产生（外部指定名不被接受），经进度回调回报。 */
    const uint8_t* resume_data;      /**< 断点续传数据（可为 NULL）。 */
    size_t         resume_data_size; /**< resume_data 字节长度。 */

    /* ===== HTTP 特有 ===== */

    const char*    url;              /**< 下载 URL：HTTP 任务识别键（必填）。 */
    const char*    trace_id;         /**< 追踪 ID。 */

    /* ===== BT 特有 ===== */

    const char*    info_hash;        /**< 种子 info_hash：BT 任务识别键（必填）。 */
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
    int32_t        source;           /**< 来源：0=本地任务 1=本地文件 2=远程文件；默认 0。 */
} dw_task_params_t;

/* ------------------------------------------------------------------ */
/*  dw_submit_result_t — 同步返回结果                                 */
/* ------------------------------------------------------------------ */

/**
 * 同步返回结果（每个任务一条，与入参顺序对应）。
 *
 * code：    同步返回码；DW_REASON_NONE 表示成功。
 * message： 错误描述；成功时为 NULL，由库分配并通过 dw_submit_results_release 释放。
 * id：      任务自增 id（上层交互主键）；add 成功回填新建 id，控制操作回显入参 id。
 */
typedef struct dw_submit_result {
    dw_reason_t   code;
    char*         message;
    dw_task_key_t key; /**< 任务唯一键：add 成功后回填，控制操作回显入参 key。natural_key 由库分配。 */
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

    /* ===== BT 配置（libtorrent） ===== */

    int32_t     listen_port;                 /**< BT 监听端口；0=随机。 */
    int32_t     max_concurrent_downloads;    /**< 全局最大并发下载数（队列限流，HTTP+BT 共用）；<=0 时库内取默认值 3。 */
    int32_t     download_rate_limit;         /**< 下载限速（B/s）；0=不限。 */
    int32_t     upload_rate_limit;           /**< 上传限速（B/s）；0=不限。 */

    /* ===== 通用配置 ===== */

    int32_t        status_callback_interval_ms; /**< 进度回调间隔（ms）。 */
    dw_log_level_t log_level;                   /**< 日志级别。 */
    const char*    work_dir;                    /**< 工作目录（临时文件等）。 */

    /* ===== 追加字段（置于末尾以保持既有字段偏移的 ABI 兼容） ===== */

    double seed_ratio_limit;   /**< BT 做种分享率上限：total_upload/total_done 达到该值后释放做种上下文；0=库内默认 3.0（即下载:上传=1:3），<0=永久做种。 */
    const char* client_id;     /**< 客户端唯一标识（UUIDv4，App 启动时从 SharedPreferences 读出后传入）。库内以此为 session 级别 clientId 隔离多客户端任务；本字段必填。 */
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
    dw_task_key_t   key;           /**< 任务唯一键。natural_key 由库分配。 */
    char*           url;          /**< 下载 URL：HTTP 任务识别键与展示；BT 为空串。 */
    char*           info_hash;    /**< 种子 info_hash：BT 任务识别键与展示；HTTP 为空串。 */
    dw_protocol_t   protocol;     /**< 协议类型。 */
    char*           name;         /**< 任务显示名称。 */
    char*           save_path;    /**< 保存目录。 */
    char*           filename;     /**< 目标文件名（可能为空串）。 */
    dw_task_status_t status;       /**< 持久化的任务状态。 */
    double          progress;     /**< 进度 0.0-1.0；-1=未知。 */
    int64_t         total_size;   /**< 总大小（字节）；-1=未知。 */
    int64_t         total_done;   /**< 已完成字节；-1=未知。 */
    int32_t         priority;     /**< 队列优先级。 */
    int64_t         created_at;   /**< 创建时间（Unix 毫秒）。 */
    int64_t         modified_at;  /**< 最近修改时间（Unix 毫秒）。 */
    int32_t         source;       /**< 来源：0=本地任务 1=本地文件 2=远程文件。 */
    char*           content_root; /**< save_path 下的实际根目录名。物理路径 = save_path / content_root。空串=尚未定名。 */
} dw_task_snapshot_t;

/* ================================================================== */
/*                            生命周期                                */
/* ================================================================== */

/**
 * 初始化下载器全局单例。
 *
 * 同时初始化 HTTP 引擎（libcurl）和 BT 引擎（libtorrent）。
 * cfg.client_id 为必填：库内以此为 session 级 clientId 隔离多客户端任务。
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

/**
 * 流量闸门：由调用方根据网络状态主动下发。
 *
 * allowed=false 时逐任务暂停所有活跃下载（BT 暂停句柄 / HTTP 停传输线程）并回落 QUEUED，
 *   调度线程不再准入新任务；session 存活以维持连接与心跳（不分享载荷）。
 * allowed=true 时唤醒调度按 QUEUED→准入路径重启（BT 经 add_task 幂等分支恢复）。
 * 幂等：状态未变时直接跳过。
 *
 * @param allowed  是否允许网络传输：true=允许，false=禁止。
 */
DW_API void dw_set_network_allowed(bool allowed);

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

/* ================================================================== */
/*                            任务接口                                */
/* ================================================================== */

/**
 * 添加单个下载任务。
 *
 * 文件名不接受外部指定（params.filename 被忽略）：库内以磁盘为唯一真相源判重定名，
 * 结果经进度回调的 filename / output_path 回报。
 *
 * @param protocol    协议类型。natural_key 从 params.url (HTTP) / params.info_hash (BT) 推导。
 * @param params      任务参数指针，不可为 NULL。
 * @param out_result  同步返回结果指针，不可为 NULL；成功后回填 key。
 * @return            0=成功，-1=失败（参数非法或内部错误）。
 */
DW_API int32_t dw_add_task(dw_protocol_t           protocol,
                           const dw_task_params_t* params,
                           dw_submit_result_t*     out_result);

/**
 * 暂停单个任务。
 *
 * @param key         任务唯一键。调用期间 natural_key 须保持有效。
 * @param out_result  同步返回结果指针，不可为 NULL。
 * @return            0=成功，-1=失败（参数非法或内部错误）。
 */
DW_API int32_t dw_pause_task(const dw_task_key_t*  key,
                             dw_submit_result_t*   out_result);

/**
 * 恢复（继续）单个任务。
 *
 * @param key         任务唯一键。调用期间 natural_key 须保持有效。
 * @param out_result  同步返回结果指针，不可为 NULL。
 * @return            0=成功，-1=失败（参数非法或内部错误）。
 */
DW_API int32_t dw_resume_task(const dw_task_key_t* key,
                              dw_submit_result_t*  out_result);

/**
 * 删除单个任务。
 *
 * @param key           任务唯一键。调用期间 natural_key 须保持有效。
 * @param delete_files  非 0=同步删除落盘文件（引擎确认资源释放后异步执行，
 *                      仅尝试一次，成败不影响任务删除本身）；0=仅删任务记录。
 * @param out_result    同步返回结果指针，不可为 NULL。
 * @return              0=成功，-1=失败（参数非法或内部错误）。
 */
DW_API int32_t dw_delete_task(const dw_task_key_t* key,
                              int32_t              delete_files,
                              dw_submit_result_t*  out_result);

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
 * 通过任务键获取磁力链接。
 *
 * 任务必须已存在于 session 中；库内按 key 的 natural_key 回读 info_hash 后调引擎。
 *
 * @param key   任务唯一键（key_type=DW_KEY_TYPE_BT，natural_key=info_hash）。
 * @return      成功返回堆分配的磁力链接（调用者 dw_free 释放），失败返回 NULL。
 */
DW_API char* dw_info_hash_to_magnet(const dw_task_key_t* key);

/**
 * 本地解析 .torrent 文件（不创建任务、不依赖 session）。
 *
 * 用于添加任务前获取种子名称、info_hash 与文件列表，供前端展示文件选择对话框。
 *
 * @param torrent_file_path  .torrent 文件路径，不可为 NULL。
 * @param out_name           输出：堆分配的种子名称（调用者 dw_free 释放）。
 * @param out_info_hash      输出：堆分配的 info_hash hex 字符串（调用者 dw_free 释放）。
 * @param out_files          输出：堆分配的文件信息数组（调用者 dw_file_list_free 释放）。
 * @param out_count          输出：文件数量。
 * @return                   0=成功，-1=失败。
 */
DW_API int32_t dw_parse_torrent_file(const char*      torrent_file_path,
                                     char**           out_name,
                                     char**           out_info_hash,
                                     dw_file_info_t** out_files,
                                     int32_t*         out_count);

/**
 * 获取已存在任务的文件列表（元数据就绪后可用）。
 *
 * 用于磁力链接任务元数据就绪后获取文件列表。
 *
 * @param key        任务唯一键。
 * @param out_files  输出：堆分配的文件信息数组（调用者 dw_file_list_free 释放）。
 * @param out_count  输出：文件数量。
 * @return           0=成功，-1=失败（元数据未就绪或任务不存在）。
 */
DW_API int32_t dw_get_file_list(const dw_task_key_t* key,
                                dw_file_info_t**     out_files,
                                int32_t*             out_count);

/* ================================================================== */
/*                        边下边播（区间 / 提优 / 进度）              */
/* ================================================================== */

/**
 * 查询单文件当前已下载字节区间（已合并连续段）。
 *
 * 优先读 wrapper 内存缓存（B 线程周期从引擎拉取更新），缓存未命中时：
 *   - 下载中任务（DOWNLOADING/RESOLVING）：返回空 + 状态码 1（代理应等待）；
 *   - 非下载中任务：回退 DB 快照（静态数据，不会增长）。
 *
 * @param key         任务唯一键。
 * @param file_index  文件索引（HTTP 恒 0）。
 * @param out_ranges  输出：堆分配的区间数组（调用者 dw_byte_range_free 释放）；无区间时为 NULL。
 * @param out_count   输出：区间数量。
 * @return            0=成功且有数据，1=成功但无数据（下载中，应等待重试），
 *                    2=成功但无数据（非下载中，不应等待），-1=失败。
 */
DW_API int32_t dw_get_file_ranges(const dw_task_key_t*  key,
                                  int32_t              file_index,
                                  dw_byte_range_t**    out_ranges,
                                  int32_t*             out_count);

/**
 * 释放 dw_get_file_ranges 返回的区间数组。
 *
 * @param ranges  区间数组，NULL 时无操作。
 * @param count   数组长度。
 */
DW_API void dw_byte_range_free(dw_byte_range_t* ranges, int32_t count);

/**
 * 查询任务指定文件的物理路径与总大小（边下边播代理用）。
 *
 * 按 save_path/filename 拼接物理路径（save_path 已含包层目录）；
 * BT 多文件另经 task_files 表按 file_index 查 name（name 已含完整相对路径）。
 *
 * @param key         任务唯一键。
 * @param file_index  文件索引（HTTP 恒 0）。
 * @param out_path    输出：堆分配路径字符串（调用者 dw_free 释放）；失败时为 NULL。
 * @param out_size    输出：文件总字节数；未知时为 -1。
 * @return            0=成功，-1=失败（任务不存在 / 参数非法）。
 */
DW_API int32_t dw_get_task_file_info(const dw_task_key_t* key,
                                     int32_t             file_index,
                                     char**              out_path,
                                     int64_t*            out_size);

/**
 * 声明当前正在播放的文件与播放字节偏移，驱动播放点附近 piece 提优。
 *
 * HTTP 为 no-op，返回 DW_REASON_NONE。
 * torrent：对偏移附近 piece 施加 set_piece_deadline（readahead 窗口）。
 * file_index<0 表示停止播放态提优（clear_piece_deadlines）。
 *
 * @param key         任务唯一键。
 * @param file_index  文件索引；<0 表示停止提优。
 * @param byte_offset 当前播放字节偏移（文件内）。
 * @param out_result  输出：同步结果；code=DW_REASON_NONE 表示成功。
 * @return            0=成功，-1=失败（任务不存在 / 参数非法）。
 */
DW_API int32_t dw_set_playing_file(const dw_task_key_t*  key,
                                   int32_t              file_index,
                                   int64_t              byte_offset,
                                   dw_submit_result_t*  out_result);

/**
 * 写入文件播放进度（毫秒），落 task_files 表。由 App 侧防抖调用。
 *
 * 与下载协议无关，wrapper 只存不解释播放语义。
 *
 * @param key         任务唯一键。
 * @param file_index  文件索引（HTTP 恒 0）。
 * @param position_ms 播放进度（毫秒）。
 * @param out_result  输出：同步结果；code=DW_REASON_NONE 表示成功。
 * @return            0=成功，-1=失败（参数非法 / 落库失败）。
 */
DW_API int32_t dw_set_play_position(const dw_task_key_t*  key,
                                    int32_t              file_index,
                                    int64_t              position_ms,
                                    dw_submit_result_t*  out_result);

/**
 * 读取已保存的文件播放进度（毫秒）。
 *
 * @param key             任务唯一键。
 * @param file_index      文件索引（HTTP 恒 0）。
 * @param out_position_ms 输出：播放进度毫秒；无记录时为 0，不可为 NULL。
 * @return                0=成功（含无记录），-1=失败（参数非法）。
 */
DW_API int32_t dw_get_play_position(const dw_task_key_t* key,
                                    int32_t              file_index,
                                    int64_t*             out_position_ms);

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
 * @param key      任务唯一键。
 * @param priority 新优先级值。
 * @return         0=成功，-1=失败（任务不存在）。
 */
DW_API int32_t dw_set_task_priority(const dw_task_key_t* key, int32_t priority);

/* ================================================================== */
/*                        任务文件持久化                              */
/* ================================================================== */

/**
 * 从数据库加载任务的文件信息（无需引擎运行）。
 *
 * 用于任务详情页展示文件列表；即使引擎句柄已释放仍可读取。
 *
 * @param key       任务唯一键。
 * @param out_files 输出：堆分配的文件信息数组（调用者 dw_file_list_free 释放）。
 * @param out_count 输出：文件数量。
 * @return          0=成功，-1=失败（任务不存在或无文件记录）。
 */
DW_API int32_t dw_load_task_files(const dw_task_key_t* key,
                                   dw_file_info_t**    out_files,
                                   int32_t*            out_count);

/* ================================================================== */
/*                        本地文件浏览与管理                          */
/* ================================================================== */

/**
 * 增量扫描本地文件任务。
 *
 * 扫描 save_path 目录，将非下载任务占用且尚未登记的条目注册为本地文件任务
 * （source=1，status=COMPLETED）。已有任务不做任何处理（增量添加，不删除旧记录）。
 * 仅返回本次新增的任务快照，返回的数组经 dw_task_list_free 释放。
 *
 * @param save_path    下载目录路径。
 * @param out_tasks    输出：堆分配的新增任务快照数组。
 * @param out_count    输出：新增任务数量。
 * @return             0=成功，-1=失败。
 */
DW_API int32_t dw_scan_local_tasks(const char*           save_path,
                                    dw_task_snapshot_t** out_tasks,
                                    int32_t*             out_count);

/**
 * 校验本地文件任务的存在性。
 *
 * 遍历 save_path 下所有 source=1 且未失效的任务，检查物理文件是否仍存在。
 * 不存在的任务状态迁移为 INVALIDATED。
 *
 * @param save_path              下载目录路径。
 * @param out_invalidated_count  输出：本次新标记为失效的任务数量（可为 NULL）。
 * @return                       0=成功，-1=失败。
 */
DW_API int32_t dw_validate_local_tasks(const char* save_path,
                                        int32_t*    out_invalidated_count);

/**
 * 全量清理指定 save_path 下的非下载任务（source IN (1,2)）。
 *
 * 删除物理文件 + DB 记录，不涉及 engine 层。
 *
 * @param save_path  下载目录路径。
 * @return           0=成功，-1=失败。
 */
DW_API int32_t dw_clear_local_tasks(const char* save_path);

/**
 * 删除本地文件任务（source=1）。
 *
 * 仅 DB + 磁盘清理，不涉及 engine 层。
 * 下载任务（source=0）拒绝，应走 dw_delete_task。
 *
 * @param key  任务唯一键（key_type=DW_KEY_TYPE_LOCAL）。
 * @return     0=成功，-1=失败（非本地任务或不存在）。
 */
DW_API int32_t dw_delete_local_entry(const dw_task_key_t* key);

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


