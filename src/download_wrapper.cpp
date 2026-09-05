/**
 * @file download_wrapper.cpp
 * @brief 统一多协议下载封装库的 C ABI 入口实现。
 */

#include "download_wrapper/download_wrapper.h"

#include "internal/downloader_internal.h"
#include "core/router.h"
#include "core/task_manager.h"
#include "http/http_engine.h"
#include "torrent/torrent_engine.h"
#include "utils/string_util.h"
#include "utils/time_util.h"

#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>

namespace dw {
    namespace {
        // 全局单例实例
        std::once_flag g_init_flag;
        std::unique_ptr<dw_downloader> g_downloader;

        void do_init_singleton() {
            g_downloader = std::make_unique<dw_downloader>();
        }

        /// C ABI 的 dw_task_key_t 提取 natural_key。key 为空或 natural_key 为 NULL 返回空串。
        std::string natural_key_of(const dw_task_key_t *key) {
            if (!key || !key->natural_key) return "";
            return key->natural_key;
        }

        /// 释放配置中所有深拷贝的字符串字段。
        void free_config_strings(dw_config_t &cfg) {
            std::free(const_cast<char *>(cfg.proxy));
            std::free(const_cast<char *>(cfg.proxy_username));
            std::free(const_cast<char *>(cfg.proxy_password));
            std::free(const_cast<char *>(cfg.user_agent));
            std::free(const_cast<char *>(cfg.ca_bundle));
            std::free(const_cast<char *>(cfg.work_dir));
            std::free(const_cast<char *>(cfg.client_id));
            cfg = {};
        }

        /// 深拷贝配置：先释放 dst 旧字符串，再从 src 分配独立副本。
        /// 可安全重复调用，不会泄漏历史内存。
        void deep_copy_config(dw_config_t &dst, const dw_config_t &src) {
            free_config_strings(dst); // 释放旧字符串
            dst = src; // 拷贝基础字段
            dst.proxy = utils::dup_cstr(src.proxy);
            dst.proxy_username = utils::dup_cstr(src.proxy_username);
            dst.proxy_password = utils::dup_cstr(src.proxy_password);
            dst.user_agent = utils::dup_cstr(src.user_agent);
            dst.ca_bundle = utils::dup_cstr(src.ca_bundle);
            dst.work_dir = utils::dup_cstr(src.work_dir);
            dst.client_id = utils::dup_cstr(src.client_id);
        }
    } // namespace

    /// 格式化日志辅助：snprintf 后调用 log_message。
    void emit_logf(dw_log_level_t level, const char *trace_id,
                   const char *func, int32_t line,
                   const char *fmt, ...) {
        char buf[512];
        va_list args;
        va_start(args, fmt);
        vsnprintf(buf, sizeof(buf), fmt, args);
        va_end(args);
        log_message(level, buf, trace_id, func, line);
    }

    dw_downloader *global_downloader() {
        return g_downloader.get();
    }

    void emit_progress(const dw_progress_t *progress) {
        if (!g_downloader || !progress) return;
        if (auto cb = g_downloader->progress_cb.load()) {
            cb(progress);
        }
    }

    void log_message(const dw_log_level_t level,
                     const char *message,
                     const char *trace_id,
                     const char *func,
                     const int32_t line) {
        // 统一日志级别过滤：低于全局配置级别的消息直接丢弃
        if (g_downloader && level < g_downloader->config.log_level) return;
        const char *tid = (trace_id && trace_id[0]) ? trace_id : "";
        const int64_t ts = utils::now_unix_ms();
        if (auto cb = g_downloader ? g_downloader->log_cb.load() : nullptr) {
            cb(level, message, tid, func ? func : "", line, ts);
        } else {
            /* 日志回调未就绪，fallback 到 stderr */
            auto lvl_str = "INFO";
            switch (level) {
                case DW_LOG_DEBUG: lvl_str = "DEBUG";
                    break;
                case DW_LOG_ERROR: lvl_str = "ERROR";
                    break;
                default: break;
            }
            const std::string time_str = utils::format_unix_ms(ts);
            std::fprintf(stderr, "[wrapper][%s][%s] %s:%d %s: %s\n",
                         lvl_str, time_str.c_str(), func ? func : "?", line, tid, message);
        }
    }
} // namespace dw

// C ABI 函数位于全局命名空间：引入 dw 命名空间的快捷日志模板（原宏方案无此需求）。
using dw::log_d;
using dw::log_e;
using dw::log_i;

/* ================================================================== */
/*                          C ABI 接口实现                            */
/* ================================================================== */

extern "C" {
/* ------------------------------------------------------------------ */
/*  生命周期                                                          */
/* ------------------------------------------------------------------ */

DW_API int32_t dw_init(const dw_config_t *cfg) {
    std::call_once(dw::g_init_flag, dw::do_init_singleton);
    if (!dw::g_downloader) {
        log_e("", "下载器创建失败");
        return -1;
    }

    std::lock_guard<std::mutex> lock(dw::g_downloader->mutex);

    if (!cfg || !cfg->client_id || !cfg->client_id[0]) {
        log_e("", "下载器初始化失败");
        return -1;
    }

    if (cfg) {
        dw::deep_copy_config(dw::g_downloader->config, *cfg);
    }

    if (dw::g_downloader->initialized.load()) {
        log_d("", "下载器已初始化");
        return 0;
    }

    // 创建引擎（先于 Router，因为 Router::start 需要注入引擎）
    dw::g_downloader->http_engine = std::make_unique<dw::HttpEngine>();
    dw::g_downloader->torrent_engine = std::make_unique<dw::TorrentEngine>();

    if (dw::g_downloader->http_engine->init(cfg, nullptr) != 0) {
        log_e("", "HTTP 引擎初始化失败");
        return -1;
    }
    if (dw::g_downloader->torrent_engine->init(cfg, nullptr) != 0) {
        dw::g_downloader->http_engine->destroy();
        log_e("", "BT 引擎初始化失败");
        return -1;
    }

    // 创建 Router 并启动 TaskManager
    dw::g_downloader->router = std::make_unique<dw::Router>();
    dw::g_downloader->router->set_local_client_id(dw::g_downloader->config.client_id);
    if (dw::g_downloader->router->start(dw::g_downloader.get(), dw::g_downloader->config) != 0) {
        log_e("", "下载器启动失败");
        dw::g_downloader->router.reset();
        return -1;
    }

    dw::g_downloader->initialized.store(true);

    log_i("", "下载器初始化完成");
    return 0;
}

DW_API void dw_destroy(void) {
    if (!dw::g_downloader) {
        log_d("", "下载器已销毁");
        return;
    }

    std::lock_guard<std::mutex> lock(dw::g_downloader->mutex);
    if (!dw::g_downloader->initialized.load()) {
        log_d("", "下载器尚未初始化");
        return;
    }

    // 先停止 Router（含 TaskManager）
    if (dw::g_downloader->router) {
        dw::g_downloader->router->stop();
        dw::g_downloader->router.reset();
    }

    if (dw::g_downloader->http_engine) {
        dw::g_downloader->http_engine->destroy();
        dw::g_downloader->http_engine.reset();
    }
    if (dw::g_downloader->torrent_engine) {
        dw::g_downloader->torrent_engine->destroy();
        dw::g_downloader->torrent_engine.reset();
    }

    // 释放释放配置
    dw::free_config_strings(dw::g_downloader->config);

    dw::g_downloader->initialized.store(false);
    log_i("", "下载器销毁完成");
}

DW_API int32_t dw_set_config(const dw_config_t *cfg) {
    auto *d = dw::global_downloader();
    if (!d) {
        log_e("", "配置失败: 下载器已销毁");
        return -1;
    }
    if (!cfg) {
        log_e("", "配置失败: 参数为空");
        return -1;
    }

    log_i("", "配置开始: %s", dw::to_string(*cfg).c_str());
    {
        std::lock_guard<std::mutex> lock(d->mutex);
        dw::deep_copy_config(d->config, *cfg);
    }
    log_i("", "配置完成");
    return 0;
}

DW_API void dw_set_network_allowed(const bool allowed) {
    auto *d = dw::global_downloader();
    if (!d) {
        log_e("", "网络切换失败: 下载器已销毁");
        return;
    }
    log_i("", "网络切换开始: allowed=%d", allowed);
    dw::TaskManager *tm = nullptr;
    {
        std::lock_guard<std::mutex> lock(d->mutex);
        if (d->router) {
            tm = d->router->task_manager();
        }
    }
    if (tm) {
        tm->set_network_allowed(allowed);
    }
    log_i("", "网络切换完成");
}

/* ------------------------------------------------------------------ */
/*  回调注册                                                          */
/* ------------------------------------------------------------------ */

DW_API void dw_set_progress_callback(const dw_progress_cb cb) {
    if (!dw::g_downloader) {
        log_e("", "进度回调注册失败: 下载器已销毁");
        return;
    }
    log_i("", "注册进度回调: enabled=%d", cb != nullptr);
    std::lock_guard<std::mutex> lock(dw::g_downloader->mutex);
    dw::g_downloader->progress_cb = cb;
}

DW_API void dw_set_log_callback(const dw_log_cb cb) {
    if (!dw::g_downloader) {
        log_e("", "日志回调注册失败: 下载器已销毁");
        return;
    }
    log_i("", "注册日志回调: enabled=%d", cb != nullptr);
    std::lock_guard<std::mutex> lock(dw::g_downloader->mutex);
    dw::g_downloader->log_cb = cb;
}

/* ------------------------------------------------------------------ */
/*  任务接口                                                          */
/* ------------------------------------------------------------------ */

DW_API int32_t dw_add_task(dw_protocol_t protocol,
                           const dw_task_params_t *params,
                           dw_submit_result_t *out_result,
                           const int32_t force) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !params || !out_result) {
        log_e("",
              "失败: 参数非法 d=%p init=%d params=%p out=%p",
              d, d ? d->initialized.load() : 0, params, out_result);
        if (out_result) {
            out_result->code = DW_REASON_ERROR;
            out_result->message = nullptr;
        }
        return -1;
    }

    if (protocol != DW_PROTOCOL_HTTP && protocol != DW_PROTOCOL_TORRENT) {
        out_result->code = DW_REASON_ERROR;
        out_result->message = nullptr;
        log_e("", "失败: 未知协议 protocol=%d", protocol);
        return -1;
    }

    // 路由到 TaskManager（目前仅支持本机）
    const std::string client_id = params->client_id ? params->client_id : "";
    dw::TaskManager *tm = nullptr;
    if (d->router) {
        tm = d->router->route(client_id);
    }
    if (!tm) {
        out_result->code = DW_REASON_ERROR;
        out_result->message = nullptr;
        log_e("", "失败: 路由失败 client_id=%s", client_id.c_str());
        return -1;
    }

    // 入队 + 调度由 TaskManager 统一接管，引擎启动由调度线程按并发额度触发。
    return tm->add(protocol, params, out_result, force != 0);
}

DW_API int32_t dw_pause_task(const char *client_id,
                             const dw_task_key_t *key,
                             dw_submit_result_t *out_result) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !client_id || !out_result) {
        log_e("",
              "失败: 参数非法 d=%p init=%d client_id=%p out=%p",
              d, d ? d->initialized.load() : 0, client_id, out_result);
        if (out_result) {
            out_result->code = DW_REASON_ERROR;
            out_result->message = nullptr;
        }
        return -1;
    }
    out_result->code = DW_REASON_NONE;
    out_result->message = nullptr;

    // 协议由 protocol 推导：HTTP/BT 走引擎，LOCAL 仅为内存态迁移。
    if (key && key->protocol == DW_PROTOCOL_LOCAL) {
        return 0;
    }

    auto *tm = d->router ? d->router->route(client_id) : nullptr;
    if (!tm) {
        out_result->code = DW_REASON_ERROR;
        return -1;
    }

    const dw_protocol_t proto = key ? key->protocol : DW_PROTOCOL_HTTP;
    const std::string nk = dw::natural_key_of(key);
    const int32_t rc = tm->pause(proto, nk, out_result);
    if (rc != 0) {
        out_result->code = DW_REASON_ERROR;
        return rc;
    }
    return 0;
}

DW_API int32_t dw_resume_task(const char *client_id,
                              const dw_task_key_t *key,
                              const char **trackers, int32_t tracker_count,
                              dw_submit_result_t *out_result) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !client_id || !out_result) {
        log_e("",
              "失败: 参数非法 d=%p init=%d client_id=%p out=%p",
              d, d ? d->initialized.load() : 0, client_id, out_result);
        if (out_result) {
            out_result->code = DW_REASON_ERROR;
            out_result->message = nullptr;
        }
        return -1;
    }
    out_result->code = DW_REASON_NONE;
    out_result->message = nullptr;

    auto *tm = d->router ? d->router->route(client_id) : nullptr;
    if (!tm) {
        out_result->code = DW_REASON_ERROR;
        return -1;
    }

    const dw_protocol_t proto = key ? key->protocol : DW_PROTOCOL_HTTP;
    const std::string nk = dw::natural_key_of(key);
    const int32_t rc = tm->resume(proto, nk, trackers, tracker_count, out_result);
    if (rc != 0) {
        out_result->code = DW_REASON_ERROR;
        return rc;
    }
    return 0;
}

DW_API int32_t dw_delete_task(const char *client_id,
                              const dw_task_key_t *key,
                              int32_t delete_files,
                              dw_submit_result_t *out_result) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !client_id || !out_result) {
        log_e("",
              "失败: 参数非法 d=%p init=%d client_id=%p out=%p",
              d, d ? d->initialized.load() : 0, client_id, out_result);
        if (out_result) {
            out_result->code = DW_REASON_ERROR;
            out_result->message = nullptr;
        }
        return -1;
    }
    out_result->code = DW_REASON_NONE;
    out_result->message = nullptr;

    // LOCAL 任务已迁移到 file_records，不走引擎 + 任务中枢。
    // 调用方应使用 dw_delete_local_entry(client_id, save_path, root_name)。
    if (key && key->protocol == DW_PROTOCOL_LOCAL) {
        log_e("", "LOCAL 任务请使用 dw_delete_local_entry");
        if (out_result) {
            out_result->code = DW_REASON_ERROR;
            out_result->message = nullptr;
        }
        return -1;
    }

    auto *tm = d->router ? d->router->route(client_id) : nullptr;
    if (!tm) {
        out_result->code = DW_REASON_ERROR;
        return -1;
    }

    const dw_protocol_t proto = key ? key->protocol : DW_PROTOCOL_HTTP;
    const std::string nk = dw::natural_key_of(key);
    const int32_t rc = tm->remove(proto, nk, delete_files, out_result);
    if (rc != 0) {
        out_result->code = DW_REASON_ERROR;
        return rc;
    }
    return 0;
}

/* ------------------------------------------------------------------ */
/*  BT 工具函数                                                       */
/* ------------------------------------------------------------------ */

DW_API char *dw_magnet_to_info_hash(const char *magnet_link) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !magnet_link) {
        log_e("",
              "失败: 参数非法 d=%p init=%d magnet_link=%p",
              d, d ? d->initialized.load() : 0, magnet_link);
        return nullptr;
    }
    return dw::TorrentEngine::magnet_to_info_hash(magnet_link);
}

DW_API char *dw_torrent_file_to_info_hash(const char *torrent_file_path) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !torrent_file_path) {
        log_e("",
              "失败: 参数非法 d=%p init=%d path=%p",
              d, d ? d->initialized.load() : 0, torrent_file_path);
        return nullptr;
    }
    return dw::TorrentEngine::torrent_file_to_info_hash(torrent_file_path);
}

DW_API char *dw_info_hash_to_magnet(const char *client_id, const dw_task_key_t *key) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !client_id || !key) {
        log_e("",
              "失败: 参数非法 d=%p init=%d client_id=%p key=%p",
              d, d ? d->initialized.load() : 0, client_id, key);
        return nullptr;
    }
    auto *tm = d->router ? d->router->route(client_id) : nullptr;
    if (!tm) return nullptr;

    // 按任务键回读 info_hash（BT 的 natural_key 即 info_hash）。
    dw::TaskRecord task_record;
    const dw_protocol_t proto = key->protocol;
    if (!tm->load_task_record(proto, dw::natural_key_of(key), task_record)) {
        log_e("", "失败: 任务不存在 protocol=%d natural_key=%s",
              key->protocol, key->natural_key ? key->natural_key : "");
        return nullptr;
    }
    return dw::TorrentEngine::info_hash_to_magnet(task_record.natural_key.c_str());
}

DW_API int32_t dw_parse_torrent_file(const char *torrent_file_path,
                                     char **out_name,
                                     char **out_info_hash,
                                     dw_file_info_t **out_files,
                                     int32_t *out_count) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !torrent_file_path ||
        !out_name || !out_info_hash || !out_files || !out_count) {
        log_e("",
              "失败: 参数非法 d=%p init=%d path=%p",
              d, d ? d->initialized.load() : 0, torrent_file_path);
        return -1;
    }
    return dw::TorrentEngine::parse_torrent_file(torrent_file_path,
                                                 out_name,
                                                 out_info_hash,
                                                 out_files,
                                                 out_count);
}

DW_API int32_t dw_get_file_list(const char *client_id,
                                const dw_task_key_t *key,
                                dw_file_info_t **out_files,
                                int32_t *out_count) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !client_id ||
        !key || !out_files || !out_count) {
        log_e("",
              "失败: 参数非法 d=%p init=%d client_id=%p key=%p",
              d, d ? d->initialized.load() : 0, client_id, key);
        return -1;
    }
    auto *tm = d->router ? d->router->route(client_id) : nullptr;
    if (!tm) return -1;

    *out_files = nullptr;
    *out_count = 0;

    // 文件清单实时查询：BT 经 handle 在线查询（选中文件），HTTP 从任务记录推导；
    // downloaded_bytes 由进度缓存聚合回填。
    const dw_protocol_t proto = key->protocol;
    const auto files = tm->load_files(proto, dw::natural_key_of(key));
    if (files.empty()) {
        return -1;
    }
    const int32_t n = static_cast<int32_t>(files.size());
    auto *arr = static_cast<dw_file_info_t *>(
        std::calloc(static_cast<size_t>(n), sizeof(dw_file_info_t)));
    if (!arr) return -1;
    for (int32_t i = 0; i < n; ++i) {
        arr[i] = files[static_cast<size_t>(i)];
    }
    *out_files = arr;
    *out_count = n;
    return 0;
}

/* ------------------------------------------------------------------ */
/*  边下边播（区间 / 提优 / 进度）                                    */
/* ------------------------------------------------------------------ */

DW_API int32_t dw_get_file_ranges(const char *client_id,
                                  const dw_task_key_t *key,
                                  int32_t file_index,
                                  dw_byte_range_t **out_ranges,
                                  int32_t *out_count) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !client_id ||
        !key || !out_ranges || !out_count) {
        log_e("",
              "失败: 参数非法 d=%p init=%d client_id=%p key=%p",
              d, d ? d->initialized.load() : 0, client_id, key);
        return -1;
    }
    auto *tm = d->router ? d->router->route(client_id) : nullptr;
    if (!tm) return -1;

    *out_ranges = nullptr;
    *out_count = 0;

    const dw_protocol_t proto = key->protocol;
    const std::string nk = dw::natural_key_of(key);

    // ---- 1. 优先读内存缓存（B 线程周期从引擎拉取更新） ----
    std::vector<dw_byte_range_t> vec = tm->get_cached_segments(proto, nk, file_index);

    if (!vec.empty()) {
        // 缓存命中，直接返回
        const int32_t n = static_cast<int32_t>(vec.size());
        auto *arr = static_cast<dw_byte_range_t *>(
            std::calloc(static_cast<size_t>(n), sizeof(dw_byte_range_t)));
        if (!arr) return -1;
        for (int32_t i = 0; i < n; ++i) arr[i] = vec[static_cast<size_t>(i)];
        *out_ranges = arr;
        *out_count = n;
        return 0;
    }

    // ---- 2. 缓存为空：按任务状态决定行为 ----
    const int32_t status = tm->get_task_status(proto, nk);
    if (status < 0) {
        // 任务不存在
        return 2;
    }

    const bool downloading = (status == DW_TASK_STATUS_DOWNLOADING ||
                              status == DW_TASK_STATUS_RESOLVING ||
                              status == DW_TASK_STATUS_PARSED);
    if (downloading) {
        // 下载中但缓存为空（引擎尚未产出数据 / 元数据未就绪）：
        // 不回退 DB（DB 可能是过时快照），返回 1 让代理等待
        return 1;
    }

    // ---- 3. 非下载中：回退 DB 快照（静态数据） ----
    vec = tm->load_segments(proto, nk, file_index);
    if (vec.empty()) {
        return 2; // 无数据且不会增长
    }
    const int32_t n = static_cast<int32_t>(vec.size());
    auto *arr = static_cast<dw_byte_range_t *>(
        std::calloc(static_cast<size_t>(n), sizeof(dw_byte_range_t)));
    if (!arr) return -1;
    for (int32_t i = 0; i < n; ++i) arr[i] = vec[static_cast<size_t>(i)];
    *out_ranges = arr;
    *out_count = n;
    return 0;
}

DW_API void dw_byte_range_free(dw_byte_range_t *ranges, int32_t count) {
    (void) count; // 无嵌套指针，直接释放主数组
    if (ranges) {
        std::free(ranges);
    }
}

DW_API int32_t dw_get_task_file_info(const char *client_id,
                                     const dw_task_key_t *key,
                                     int32_t file_index,
                                     char **out_path,
                                     int64_t *out_size) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !client_id ||
        !key || !out_path || !out_size) {
        log_e("",
              "失败: 参数非法 d=%p init=%d client_id=%p key=%p",
              d, d ? d->initialized.load() : 0, client_id, key);
        return -1;
    }
    auto *tm = d->router ? d->router->route(client_id) : nullptr;
    if (!tm) return -1;

    *out_path = nullptr;
    *out_size = -1;

    // 物理路径与大小统一经 TaskManager 解析：
    // HTTP 从任务记录推导（wrapper 模型），BT 经引擎 handle 实时查询。
    const dw_protocol_t proto = key->protocol;
    const std::string nk = dw::natural_key_of(key);
    std::string file_path;
    int64_t file_size = -1;
    if (!tm->resolve_file_path(proto, nk, file_index, file_path, file_size)) {
        log_e("", "失败: 无法解析文件路径 protocol=%d natural_key=%s fi=%d",
              key->protocol, key->natural_key ? key->natural_key : "", file_index);
        return -1;
    }

    // 堆拷贝路径字符串
    const size_t len = file_path.size();
    char *path_copy = static_cast<char *>(std::malloc(len + 1));
    if (!path_copy) return -1;
    std::memcpy(path_copy, file_path.c_str(), len + 1);

    *out_path = path_copy;
    *out_size = file_size;
    return 0;
}

DW_API int32_t dw_set_playing_file(const char *client_id,
                                   const dw_task_key_t *key,
                                   int32_t file_index,
                                   int64_t byte_offset,
                                   dw_submit_result_t *out_result) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !client_id || !out_result) {
        log_e("",
              "失败: 参数非法 d=%p init=%d client_id=%p out=%p",
              d, d ? d->initialized.load() : 0, client_id, out_result);
        if (out_result) {
            out_result->code = DW_REASON_ERROR;
            out_result->message = nullptr;
        }
        return -1;
    }
    auto *tm = d->router ? d->router->route(client_id) : nullptr;
    if (!tm) {
        out_result->code = DW_REASON_ERROR;
        return -1;
    }
    out_result->message = nullptr;

    // 仅写播放标识，由调度器统一处理任务准入与 piece deadline 设置。
    const dw_protocol_t play_proto = key ? key->protocol : DW_PROTOCOL_HTTP;
    if (!tm->set_playing(play_proto, dw::natural_key_of(key), file_index, byte_offset)) {
        out_result->code = DW_REASON_ERROR;
        return -1;
    }
    out_result->code = DW_REASON_NONE;
    return 0;
}

DW_API int32_t dw_set_play_position(const char *client_id,
                                    const dw_task_key_t *key,
                                    int32_t file_index,
                                    int64_t position_ms,
                                    dw_submit_result_t *out_result) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !client_id || !out_result) {
        log_e("",
              "失败: 参数非法 d=%p init=%d client_id=%p out=%p",
              d, d ? d->initialized.load() : 0, client_id, out_result);
        if (out_result) {
            out_result->code = DW_REASON_ERROR;
            out_result->message = nullptr;
        }
        return -1;
    }
    auto *tm = d->router ? d->router->route(client_id) : nullptr;
    if (!tm) {
        out_result->code = DW_REASON_ERROR;
        return -1;
    }
    // 播放进度直落 task_store，与协议无关。
    const dw_protocol_t pp_proto = key ? key->protocol : DW_PROTOCOL_HTTP;
    tm->set_play_position(pp_proto, dw::natural_key_of(key), file_index, position_ms);
    out_result->code = DW_REASON_NONE;
    out_result->message = nullptr;
    return 0;
}

DW_API int32_t dw_get_play_position(const char *client_id,
                                    const dw_task_key_t *key,
                                    int32_t file_index,
                                    int64_t *out_position_ms) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !client_id || !out_position_ms) {
        log_e("",
              "失败: 参数非法 d=%p init=%d client_id=%p out=%p",
              d, d ? d->initialized.load() : 0, client_id, out_position_ms);
        if (out_position_ms) *out_position_ms = 0;
        return -1;
    }
    auto *tm = d->router ? d->router->route(client_id) : nullptr;
    if (!tm) {
        if (out_position_ms) *out_position_ms = 0;
        return -1;
    }
    const dw_protocol_t gp_proto = key ? key->protocol : DW_PROTOCOL_HTTP;
    *out_position_ms = tm->get_play_position(gp_proto, dw::natural_key_of(key), file_index);
    return 0;
}

/* ------------------------------------------------------------------ */
/*  任务快照与队列                                              */
/* ------------------------------------------------------------------ */

DW_API int32_t dw_list_tasks(const char *client_id,
                             dw_task_snapshot_t **out_tasks,
                             int32_t *out_count) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !client_id ||
        !out_tasks || !out_count) {
        log_e("",
              "失败: 参数非法 d=%p init=%d client_id=%p out_tasks=%p out_count=%p",
              d, d ? d->initialized.load() : 0, client_id, out_tasks, out_count);
        if (out_tasks) *out_tasks = nullptr;
        if (out_count) *out_count = 0;
        return -1;
    }
    auto *tm = d->router ? d->router->route(client_id) : nullptr;
    if (!tm) {
        if (out_tasks) *out_tasks = nullptr;
        if (out_count) *out_count = 0;
        return -1;
    }
    return tm->list(out_tasks, out_count);
}

DW_API int32_t dw_set_task_priority(const char *client_id,
                                    const dw_task_key_t *key,
                                    const int32_t *priority_file_indexes,
                                    const int32_t priority_file_index_size) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !client_id || !key) {
        log_e("",
              "失败: 参数非法 d=%p init=%d client_id=%p key=%p",
              d, d ? d->initialized.load() : 0, client_id, key);
        return -1;
    }
    auto *tm = d->router ? d->router->route(client_id) : nullptr;
    if (!tm) return -1;

    const dw_protocol_t sp_proto = key ? key->protocol : DW_PROTOCOL_HTTP;
    return tm->set_priority(sp_proto, dw::natural_key_of(key),
                            priority_file_indexes, priority_file_index_size);
}

/* ------------------------------------------------------------------ */
/*  任务文件查询                                                      */
/* ------------------------------------------------------------------ */

DW_API int32_t dw_load_task_files(const char *client_id,
                                  const dw_task_key_t *key,
                                  dw_file_info_t **out_files,
                                  int32_t *out_count) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !client_id ||
        !key || !out_files || !out_count) {
        log_e("",
              "失败: 参数非法 d=%p init=%d client_id=%p key=%p out_files=%p out_count=%p",
              d, d ? d->initialized.load() : 0, client_id, key, out_files, out_count);
        if (out_files) *out_files = nullptr;
        if (out_count) *out_count = 0;
        return -1;
    }
    auto *tm = d->router ? d->router->route(client_id) : nullptr;
    if (!tm) {
        if (out_files) *out_files = nullptr;
        if (out_count) *out_count = 0;
        return -1;
    }
    // 实时查询文件清单（task_files 表已移除）：BT 经引擎 handle，HTTP 由任务记录推导。
    const dw_protocol_t lf_proto = key->protocol;
    auto file_vec = tm->load_files(lf_proto, dw::natural_key_of(key));
    if (file_vec.empty()) {
        *out_files = nullptr;
        *out_count = 0;
        return -1;
    }
    // 转为堆数组：直接移交 file_vec 各节点的字符串所有权（库内已堆分配），
    // 避免二次拷贝与释放遗漏；调用方经 dw_file_list_free 统一释放。
    const int32_t n = static_cast<int32_t>(file_vec.size());
    dw_file_info_t *arr = static_cast<dw_file_info_t *>(
        std::malloc(sizeof(dw_file_info_t) * n));
    if (!arr) {
        // 分配失败：释放已持有的堆字符串，避免泄露。
        for (auto &f: file_vec) {
            std::free(f.name);
            std::free(f.ext);
            std::free(f.physical_path);
        }
        *out_files = nullptr;
        *out_count = 0;
        return -1;
    }
    for (int32_t i = 0; i < n; ++i) {
        arr[i] = file_vec[i]; // 结构拷贝（含指针），所有权转移至 arr
    }
    *out_files = arr;
    *out_count = n;
    return 0;
}

/* ------------------------------------------------------------------ */
/*  本地文件浏览与管理                                                */
/* ------------------------------------------------------------------ */

DW_API int32_t dw_scan_local_tasks(const char *client_id,
                                   const char *save_path,
                                   dw_task_snapshot_t **out_tasks,
                                   int32_t *out_count) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !client_id ||
        !save_path || !out_tasks || !out_count) {
        log_e("",
              "失败: 参数非法 d=%p init=%d client_id=%p save_path=%p out_tasks=%p out_count=%p",
              d, d ? d->initialized.load() : 0, client_id, save_path, out_tasks, out_count);
        if (out_tasks) *out_tasks = nullptr;
        if (out_count) *out_count = 0;
        return -1;
    }
    auto *tm = d->router ? d->router->route(client_id) : nullptr;
    if (!tm) {
        if (out_tasks) *out_tasks = nullptr;
        if (out_count) *out_count = 0;
        return -1;
    }
    return tm->scan_local_tasks(save_path, out_tasks, out_count);
}

DW_API int32_t dw_validate_local_tasks(const char *client_id,
                                       const char *save_path,
                                       int32_t *out_invalidated_count) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !client_id || !save_path) {
        log_e("", "失败: 参数非法");
        if (out_invalidated_count) *out_invalidated_count = 0;
        return -1;
    }
    auto *tm = d->router ? d->router->route(client_id) : nullptr;
    if (!tm) {
        if (out_invalidated_count) *out_invalidated_count = 0;
        return -1;
    }
    return tm->validate_local_tasks(save_path, out_invalidated_count);
}

DW_API int32_t dw_clear_local_tasks(const char *client_id, const char *save_path) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !client_id || !save_path) {
        log_e("", "失败: 参数非法");
        return -1;
    }
    auto *tm = d->router ? d->router->route(client_id) : nullptr;
    if (!tm) return -1;
    return tm->clear_local_tasks(save_path);
}

DW_API int32_t dw_delete_local_entry(const char *client_id, const char *save_path, const char *root_name) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !client_id) {
        log_e("", "失败: 未初始化");
        return -1;
    }
    if (!save_path || !root_name) {
        log_e("", "失败: 参数非法");
        return -1;
    }
    auto *tm = d->router ? d->router->route(client_id) : nullptr;
    if (!tm) return -1;
    return tm->delete_local_entry(save_path, root_name);
}

/* ------------------------------------------------------------------ */
/*  资源释放                                                          */
/* ------------------------------------------------------------------ */

DW_API void dw_submit_result_release(dw_submit_result_t *result) {
    if (!result) {
        return;
    }
    if (result->message) {
        std::free(result->message);
        result->message = nullptr;
    }
}

DW_API void dw_file_list_free(dw_file_info_t *files, int32_t count) {
    if (!files || count <= 0) {
        return;
    }
    // 释放各节点由库分配的全部字符串字段。
    for (int32_t i = 0; i < count; ++i) {
        std::free(files[i].name);
        std::free(files[i].ext);
        std::free(files[i].physical_path);
        files[i].name = nullptr;
        files[i].ext = nullptr;
        files[i].physical_path = nullptr;
    }
    std::free(files);
}

DW_API void dw_task_list_free(dw_task_snapshot_t *tasks, int32_t count) {
    if (!tasks || count <= 0) {
        return;
    }
    for (int32_t i = 0; i < count; ++i) {
        if (tasks[i].key.natural_key) {
            std::free(const_cast<char *>(tasks[i].key.natural_key));
        }
        std::free(tasks[i].url);
        std::free(tasks[i].info_hash);
        std::free(tasks[i].name);
        std::free(tasks[i].save_path);
        std::free(tasks[i].content_root);
    }
    std::free(tasks);
}

DW_API int32_t dw_list_file_records(const char *client_id,
                                    dw_file_record_t **out_records,
                                    int32_t *out_count) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !client_id ||
        !out_records || !out_count) {
        log_e("",
              "失败: 参数非法 d=%p init=%d client_id=%p out_records=%p out_count=%p",
              d, d ? d->initialized.load() : 0, client_id, out_records, out_count);
        if (out_records) *out_records = nullptr;
        if (out_count) *out_count = 0;
        return -1;
    }
    auto *tm = d->router ? d->router->route(client_id) : nullptr;
    if (!tm) {
        if (out_records) *out_records = nullptr;
        if (out_count) *out_count = 0;
        return -1;
    }
    auto vec = tm->list_file_records();
    if (vec.empty()) {
        *out_records = nullptr;
        *out_count = 0;
        return 0;
    }
    const int32_t n = static_cast<int32_t>(vec.size());
    auto *arr = static_cast<dw_file_record_t *>(
        std::calloc(n, sizeof(dw_file_record_t)));
    if (!arr) {
        *out_records = nullptr;
        *out_count = 0;
        return -1;
    }
    for (int32_t i = 0; i < n; ++i) {
        const auto &r = vec[i];
        arr[i].id = r.id;
        arr[i].client_id = dw::utils::dup_cstr(r.client_id);
        arr[i].type = r.type;
        arr[i].is_remote = r.is_remote;
        arr[i].save_path = dw::utils::dup_cstr(r.save_path);
        arr[i].root_name = dw::utils::dup_cstr(r.root_name);
        arr[i].full_path = dw::utils::dup_cstr(r.full_path);
        arr[i].file_type = r.file_type;
        arr[i].task_protocol = r.task_protocol;
        arr[i].task_natural_key = dw::utils::dup_cstr(r.task_natural_key);
        // 字符串复制失败（内存不足）：回滚已分配的字段与数组，
        // 不向调用方返回含 NULL 字段的半成品快照。
        if (!arr[i].client_id || !arr[i].save_path ||
            !arr[i].root_name || !arr[i].full_path || !arr[i].task_natural_key) {
            log_e("", "失败: 文件记录字符串复制内存不足 i=%d n=%d", i, n);
            dw_file_record_list_free(arr, i + 1);
            *out_records = nullptr;
            *out_count = 0;
            return -1;
        }
        arr[i].status = r.status;
        arr[i].total_size = r.total_size;
        arr[i].total_done = r.total_done;
        arr[i].created_at = r.created_at;
        arr[i].modified_at = r.modified_at;
    }
    *out_records = arr;
    *out_count = n;
    return 0;
}

DW_API void dw_file_record_list_free(dw_file_record_t *records, int32_t count) {
    if (!records || count <= 0) return;
    for (int32_t i = 0; i < count; ++i) {
        std::free(records[i].client_id);
        std::free(records[i].save_path);
        std::free(records[i].root_name);
        std::free(records[i].full_path);
        std::free(records[i].task_natural_key);
    }
    std::free(records);
}

DW_API void dw_free(void *ptr) {
    if (ptr) {
        std::free(ptr);
    }
}
} /* extern "C" */
