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
#include "utils/memory_util.h"
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

        /// 释放配置中所有深拷贝的字符串字段。
        void free_config_strings(dw_config_t &cfg) {
            std::free(const_cast<char *>(cfg.proxy));
            std::free(const_cast<char *>(cfg.proxy_username));
            std::free(const_cast<char *>(cfg.proxy_password));
            std::free(const_cast<char *>(cfg.user_agent));
            std::free(const_cast<char *>(cfg.ca_bundle));
            std::free(const_cast<char *>(cfg.work_dir));
            std::free(const_cast<char *>(cfg.client_id));
            if (cfg.trackers) {
                for (int32_t i = 0; i < cfg.tracker_count; ++i) {
                    std::free(const_cast<char *>(cfg.trackers[i]));
                }
                std::free(cfg.trackers);
            }
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
            // trackers 数组深拷贝：dst = src 后指针指向 src 数组，置空后重建副本
            dst.trackers = nullptr;
            dst.tracker_count = 0;
            if (src.trackers && src.tracker_count > 0) {
                if (auto *arr = static_cast<const char **>(
                        std::malloc(sizeof(const char *) * src.tracker_count))) {
                    for (int32_t i = 0; i < src.tracker_count; ++i) {
                        arr[i] = utils::dup_cstr(src.trackers[i] ? src.trackers[i] : "");
                    }
                    dst.trackers = arr;
                    dst.tracker_count = src.tracker_count;
                }
            }
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
    if (dw::g_downloader->progress_cb.load() == cb) return;
    log_i("", "注册进度回调: enabled=%d", cb != nullptr);
    std::lock_guard<std::mutex> lock(dw::g_downloader->mutex);
    dw::g_downloader->progress_cb.store(cb);
}

DW_API void dw_set_log_callback(const dw_log_cb cb) {
    if (!dw::g_downloader) {
        log_e("", "日志回调注册失败: 下载器已销毁");
        return;
    }
    if (dw::g_downloader->log_cb.load() == cb) return;
    log_i("", "注册日志回调: enabled=%d", cb != nullptr);
    std::lock_guard<std::mutex> lock(dw::g_downloader->mutex);
    dw::g_downloader->log_cb.store(cb);
}

/* ------------------------------------------------------------------ */
/*  任务接口                                                          */
/* ------------------------------------------------------------------ */

DW_API int32_t dw_add_task(const dw_protocol_t protocol,
                           const char *client_id,
                           const dw_task_params_t *params,
                           dw_submit_result_t *out_result,
                           const int32_t force) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !client_id || !params || !out_result) {
        log_e("",
              "添加任务失败: 参数非法 d=%p init=%d client_id=%p params=%p out=%p",
              d, d && d->initialized.load(), client_id, params, out_result);
        if (out_result) {
            out_result->code = DW_REASON_ERROR;
            out_result->message = nullptr;
        }
        return -1;
    }

    if (protocol != DW_PROTOCOL_HTTP && protocol != DW_PROTOCOL_TORRENT) {
        out_result->code = DW_REASON_ERROR;
        out_result->message = nullptr;
        log_e("", "添加任务失败: 未知协议 protocol=%s", dw::to_string(protocol));
        return -1;
    }

    const std::string task_key = dw_task_params_key(params, protocol) ? dw_task_params_key(params, protocol) : "";
    log_i(task_key.c_str(), "添加任务开始: protocol=%s client_id=%s force=%d",
          dw::to_string(protocol), client_id, force);

    // 路由到 TaskManager（目前仅支持本机）
    dw::TaskManager *tm = nullptr;
    if (d->router) {
        tm = d->router->route(client_id);
    }
    if (!tm) {
        out_result->code = DW_REASON_ERROR;
        out_result->message = nullptr;
        log_e(task_key.c_str(), "添加任务失败: 路由失败 client_id=%s", client_id);
        return -1;
    }

    const int32_t rc = tm->add(protocol, client_id, params, out_result, force != 0);
    if (rc != 0 || out_result->code != DW_REASON_NONE) {
        log_e(task_key.c_str(), "添加任务失败: rc=%d code=%d", rc, out_result->code);
        return rc;
    }

    // FileRecord 维护：force 时刷新时间戳置顶，否则幂等插入占位。
    auto &store = tm->get_store();
    const std::string save_path = params->save_path ? params->save_path : "";
    {
        std::lock_guard<std::mutex> lock(tm->get_mutex());
        if (force && store.has_file_record(client_id, protocol, task_key)) {
            store.touch_file_record(client_id, protocol, task_key);
            tm->sync_file_record_cache(client_id, protocol, task_key);
        } else if (!store.has_file_record(client_id, protocol, task_key)) {
            dw::FileRecord fr;
            fr.client_id = client_id;
            // 任务类型按协议细化：HTTP=1，BT=2（本地文件条目=0）。
            fr.type = (protocol == DW_PROTOCOL_HTTP) ? DW_SOURCE_TASK_FILE : DW_SOURCE_REMOTE_FILE;
            fr.is_remote = false;
            fr.save_path = save_path;
            fr.original_root_name = task_key; // 占位，PARSED 时经 update_file_record_meta 修正
            fr.root_name = task_key; // 占位，PARSED 时经 update_file_record_meta 修正
            fr.file_type = true;     // 默认目录
            fr.task_protocol = protocol;
            fr.task_natural_key = task_key;
            fr.status = DW_TASK_STATUS_RESOLVING; // 解析中：与 TaskManager.add 状态机对齐，PARSED 后转 QUEUED
            fr.created_at = dw::utils::now_unix_ms();
            fr.modified_at = fr.created_at;
            store.insert_file_record(fr);
            tm->sync_file_record_cache(client_id, protocol, task_key, &fr);
        }
    }

    log_i(task_key.c_str(), "添加任务完成: rc=%d code=%d", rc, out_result->code);
    return rc;
}

DW_API int32_t dw_pause_task(const dw_protocol_t protocol,
                             const char *client_id,
                             const char *natural_key,
                             dw_submit_result_t *out_result) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !client_id || !out_result) {
        log_e("", "暂停任务失败: 参数非法 d=%p init=%d client_id=%p out=%p",
              d, d && d->initialized.load(), client_id, out_result);
        if (out_result) {
            out_result->code = DW_REASON_ERROR;
            out_result->message = nullptr;
        }
        return -1;
    }
    out_result->code = DW_REASON_NONE;
    out_result->message = nullptr;

    const std::string nk = natural_key ? natural_key : "";
    log_i(nk.c_str(), "暂停任务开始: protocol=%s client_id=%s", dw::to_string(protocol), client_id);

    auto *tm = d->router ? d->router->route(client_id) : nullptr;
    if (!tm) {
        out_result->code = DW_REASON_ERROR;
        log_e(nk.c_str(), "暂停任务失败: 路由失败");
        return -1;
    }

    if (const int32_t rc = tm->pause(protocol, nk, out_result); rc != 0) {
        out_result->code = DW_REASON_ERROR;
        log_e(nk.c_str(), "暂停任务失败: rc=%d", rc);
        return rc;
    }
    log_i(nk.c_str(), "暂停任务完成");
    return 0;
}

DW_API int32_t dw_resume_task(const dw_protocol_t protocol,
                              const char *client_id,
                              const char *natural_key,
                              dw_submit_result_t *out_result) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !client_id || !out_result) {
        log_e("", "恢复任务失败: 参数非法 d=%p init=%d client_id=%p out=%p",
              d, d && d->initialized.load(), client_id, out_result);
        if (out_result) {
            out_result->code = DW_REASON_ERROR;
            out_result->message = nullptr;
        }
        return -1;
    }
    out_result->code = DW_REASON_NONE;
    out_result->message = nullptr;

    const std::string nk = natural_key ? natural_key : "";
    log_i(nk.c_str(), "恢复任务开始: protocol=%s client_id=%s", dw::to_string(protocol), client_id);

    auto *tm = d->router ? d->router->route(client_id) : nullptr;
    if (!tm) {
        out_result->code = DW_REASON_ERROR;
        log_e(nk.c_str(), "恢复任务失败: 路由失败");
        return -1;
    }

    const int32_t rc = tm->resume(protocol, nk, out_result);
    if (rc != 0) {
        out_result->code = DW_REASON_ERROR;
        log_e(nk.c_str(), "恢复任务失败: rc=%d", rc);
        return rc;
    }
    log_i(nk.c_str(), "恢复任务完成");
    return 0;
}

DW_API int32_t dw_delete_task(const dw_protocol_t protocol,
                              const char *client_id,
                              const char *natural_key,
                              const int32_t delete_files,
                              dw_submit_result_t *out_result) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !client_id || !out_result) {
        log_e("", "删除任务失败: 参数非法 d=%p init=%d client_id=%p out=%p",
              d, d && d->initialized.load(), client_id, out_result);
        if (out_result) {
            out_result->code = DW_REASON_ERROR;
            out_result->message = nullptr;
        }
        return -1;
    }
    out_result->code = DW_REASON_NONE;
    out_result->message = nullptr;

    const std::string nk = natural_key ? natural_key : "";
    log_i(nk.c_str(), "删除任务开始: protocol=%s client_id=%s delete_files=%d", dw::to_string(protocol), client_id,
          delete_files);

    if (protocol == DW_PROTOCOL_LOCAL) {
        log_e(nk.c_str(), "删除任务失败: LOCAL 任务请使用 dw_delete_local_entry");
        if (out_result) {
            out_result->code = DW_REASON_ERROR;
            out_result->message = nullptr;
        }
        return -1;
    }

    auto *tm = d->router ? d->router->route(client_id) : nullptr;
    if (!tm) {
        out_result->code = DW_REASON_ERROR;
        log_e(nk.c_str(), "删除任务失败: 路由失败");
        return -1;
    }

    const int32_t rc = tm->remove(protocol, nk, delete_files, out_result);
    if (rc != 0) {
        out_result->code = DW_REASON_ERROR;
        log_e(nk.c_str(), "删除任务失败: rc=%d", rc);
        return rc;
    }
    log_i(nk.c_str(), "删除任务完成");
    return 0;
}

/* ------------------------------------------------------------------ */
/*  BT 工具函数                                                       */
/* ------------------------------------------------------------------ */

DW_API char *dw_magnet_to_info_hash(const char *magnet_link) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !magnet_link) {
        log_e("", "失败: 参数非法 d=%p init=%d magnet_link=%p",
              d, d && d->initialized.load(), magnet_link);
        return nullptr;
    }
    return dw::TorrentEngine::magnet_to_info_hash(magnet_link);
}

DW_API char *dw_torrent_file_to_info_hash(const char *torrent_file_path) {
    if (auto *d = dw::global_downloader();
        !d || !d->initialized.load() || !torrent_file_path) {
        log_e("", "失败: 参数非法 d=%p init=%d path=%p",
              d, d && d->initialized.load(), torrent_file_path);
        return nullptr;
    }
    return dw::TorrentEngine::torrent_file_to_info_hash(torrent_file_path);
}

DW_API char *dw_info_hash_to_magnet(const char *client_id, const char *natural_key) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !client_id || !natural_key) {
        log_e("", "失败: 参数非法 d=%p init=%d client_id=%p natural_key=%p",
              d, d && d->initialized.load(), client_id, natural_key);
        return nullptr;
    }
    auto *tm = d->router ? d->router->route(client_id) : nullptr;
    if (!tm) return nullptr;

    // 按任务键回读 info_hash（BT 的 natural_key 即 info_hash）。
    dw::FileRecord task_record;
    const dw_protocol_t proto = DW_PROTOCOL_TORRENT;
    if (!tm->load_task_record(proto, natural_key, task_record)) {
        log_e("", "失败: 任务不存在 protocol=%s natural_key=%s",
              dw::to_string(proto), natural_key);
        return nullptr;
    }
    return dw::TorrentEngine::info_hash_to_magnet(task_record.task_natural_key.c_str());
}

DW_API int32_t dw_get_file_list(const char *client_id,
                                const char *natural_key,
                                dw_file_info_t **out_files,
                                int32_t *out_count) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !client_id ||
        !natural_key || !out_files || !out_count) {
        log_e("",
              "失败: 参数非法 d=%p init=%d client_id=%p natural_key=%p",
              d, d && d->initialized.load(), client_id, natural_key);
        return -1;
    }
    auto *tm = d->router ? d->router->route(client_id) : nullptr;
    if (!tm) return -1;

    *out_files = nullptr;
    *out_count = 0;

    // 文件清单实时查询：BT 经 handle 在线查询（选中文件），HTTP 从任务记录推导；
    // downloaded_bytes 由进度缓存聚合回填。
    // 协议类型从任务记录推导（natural_key 唯一对应一个任务）。
    dw::FileRecord task_record;
    if (!tm->load_task_record(DW_PROTOCOL_HTTP, natural_key, task_record) &&
        !tm->load_task_record(DW_PROTOCOL_TORRENT, natural_key, task_record)) {
        return -1;
    }
    const dw_protocol_t proto = task_record.task_protocol;
    auto [files, count] = tm->load_files(proto, natural_key);
    if (!files || count <= 0) {
        return -1;
    }
    *out_files = files;
    *out_count = count;
    return 0;
}

/* ------------------------------------------------------------------ */
/*  边下边播（区间 / 提优 / 进度）                                    */
/* ------------------------------------------------------------------ */

DW_API int32_t dw_get_file_ranges(const char *client_id,
                                  const char *natural_key,
                                  int32_t file_index,
                                  dw_byte_range_t **out_ranges,
                                  int32_t *out_count) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !client_id ||
        !natural_key || !out_ranges || !out_count) {
        log_e("",
              "失败: 参数非法 d=%p init=%d client_id=%p natural_key=%p",
              d, d ? d->initialized.load() : 0, client_id, natural_key);
        return -1;
    }
    auto *tm = d->router ? d->router->route(client_id) : nullptr;
    if (!tm) return -1;

    *out_ranges = nullptr;
    *out_count = 0;

    const std::string nk = natural_key;

    // 协议类型从任务记录推导（natural_key 唯一对应一个任务）。
    dw::FileRecord task_record;
    if (!tm->load_task_record(DW_PROTOCOL_HTTP, nk, task_record) &&
        !tm->load_task_record(DW_PROTOCOL_TORRENT, nk, task_record)) {
        return 2;
    }
    const dw_protocol_t proto = task_record.task_protocol;

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
                                     const char *natural_key,
                                     int32_t file_index,
                                     char **out_path,
                                     int64_t *out_size) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !client_id ||
        !natural_key || !out_path || !out_size) {
        log_e("",
              "失败: 参数非法 d=%p init=%d client_id=%p natural_key=%p",
              d, d ? d->initialized.load() : 0, client_id, natural_key);
        return -1;
    }
    auto *tm = d->router ? d->router->route(client_id) : nullptr;
    if (!tm) return -1;

    *out_path = nullptr;
    *out_size = -1;

    // 物理路径与大小统一经 TaskManager 解析：
    // HTTP 从任务记录推导（wrapper 模型），BT 经引擎 handle 实时查询。
    // 协议类型从任务记录推导（natural_key 唯一对应一个任务）。
    dw::FileRecord task_record;
    if (!tm->load_task_record(DW_PROTOCOL_HTTP, natural_key, task_record) &&
        !tm->load_task_record(DW_PROTOCOL_TORRENT, natural_key, task_record)) {
        return -1;
    }
    const dw_protocol_t proto = task_record.task_protocol;
    const std::string nk = natural_key;
    std::string file_path;
    int64_t file_size = -1;
    if (!tm->resolve_file_path(proto, nk, file_index, file_path, file_size)) {
        log_e("", "失败: 无法解析文件路径 protocol=%d natural_key=%s fi=%d",
              proto, natural_key, file_index);
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

DW_API int32_t dw_set_play_position(const char *client_id,
                                    const char *natural_key,
                                    int32_t file_index,
                                    int64_t position_ms,
                                    dw_submit_result_t *out_result) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !client_id || !natural_key || !out_result) {
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
    // 协议类型从任务记录推导（natural_key 唯一对应一个任务）。
    dw::FileRecord task_record;
    if (!tm->load_task_record(DW_PROTOCOL_HTTP, natural_key, task_record) &&
        !tm->load_task_record(DW_PROTOCOL_TORRENT, natural_key, task_record)) {
        out_result->code = DW_REASON_ERROR;
        return -1;
    }
    const dw_protocol_t pp_proto = task_record.task_protocol;
    tm->set_play_position(pp_proto, natural_key, file_index, position_ms);
    out_result->code = DW_REASON_NONE;
    out_result->message = nullptr;
    return 0;
}

DW_API int32_t dw_get_play_position(const char *client_id,
                                    const char *natural_key,
                                    int32_t file_index,
                                    int64_t *out_position_ms) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !client_id || !natural_key || !out_position_ms) {
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
    // 协议类型从任务记录推导（natural_key 唯一对应一个任务）。
    dw::FileRecord task_record;
    if (!tm->load_task_record(DW_PROTOCOL_HTTP, natural_key, task_record) &&
        !tm->load_task_record(DW_PROTOCOL_TORRENT, natural_key, task_record)) {
        if (out_position_ms) *out_position_ms = 0;
        return -1;
    }
    const dw_protocol_t gp_proto = task_record.task_protocol;
    *out_position_ms = tm->get_play_position(gp_proto, natural_key, file_index);
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
                                    const char *natural_key,
                                    const int32_t *priority_file_indexes,
                                    const int32_t priority_file_index_size) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !client_id || !natural_key) {
        log_e("",
              "失败: 参数非法 d=%p init=%d client_id=%p natural_key=%p",
              d, d ? d->initialized.load() : 0, client_id, natural_key);
        return -1;
    }
    auto *tm = d->router ? d->router->route(client_id) : nullptr;
    if (!tm) return -1;

    // 协议类型从任务记录推导（natural_key 唯一对应一个任务）。
    dw::FileRecord task_record;
    if (!tm->load_task_record(DW_PROTOCOL_HTTP, natural_key, task_record) &&
        !tm->load_task_record(DW_PROTOCOL_TORRENT, natural_key, task_record)) {
        return -1;
    }
    const dw_protocol_t sp_proto = task_record.task_protocol;
    return tm->set_priority(sp_proto, natural_key,
                            priority_file_indexes, priority_file_index_size);
}

/* ------------------------------------------------------------------ */
/*  任务文件查询                                                      */
/* ------------------------------------------------------------------ */

DW_API int32_t dw_load_task_files(const char *client_id,
                                  const char *natural_key,
                                  dw_file_info_t **out_files,
                                  int32_t *out_count) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !client_id ||
        !natural_key || !out_files || !out_count) {
        log_e("",
              "失败: 参数非法 d=%p init=%d client_id=%p natural_key=%p out_files=%p out_count=%p",
              d, d ? d->initialized.load() : 0, client_id, natural_key, out_files, out_count);
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
    // 协议类型从任务记录推导（natural_key 唯一对应一个任务）。
    dw::FileRecord task_record;
    if (!tm->load_task_record(DW_PROTOCOL_HTTP, natural_key, task_record) &&
        !tm->load_task_record(DW_PROTOCOL_TORRENT, natural_key, task_record)) {
        if (out_files) *out_files = nullptr;
        if (out_count) *out_count = 0;
        return -1;
    }
    const dw_protocol_t lf_proto = task_record.task_protocol;
    auto [files, count] = tm->load_files(lf_proto, natural_key);
    if (!files || count <= 0) {
        *out_files = nullptr;
        *out_count = 0;
        return -1;
    }
    *out_files = files;
    *out_count = count;
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
        log_e("", "清理本地任务失败: 参数非法");
        return -1;
    }
    log_i("", "清理本地任务开始: client_id=%s save_path=%s", client_id, save_path);
    auto *tm = d->router ? d->router->route(client_id) : nullptr;
    if (!tm) {
        log_e("", "清理本地任务失败: 路由失败");
        return -1;
    }
    const int32_t rc = tm->clear_local_tasks(save_path);
    log_i("", "清理本地任务完成: rc=%d", rc);
    return rc;
}

DW_API int32_t dw_delete_local_entry(const char *client_id, const char *save_path, const char *root_name) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !client_id) {
        log_e("", "删除本地条目失败: 未初始化");
        return -1;
    }
    if (!save_path || !root_name) {
        log_e("", "删除本地条目失败: 参数非法");
        return -1;
    }
    log_i("", "删除本地条目开始: client_id=%s root_name=%s", client_id, root_name);
    auto *tm = d->router ? d->router->route(client_id) : nullptr;
    if (!tm) {
        log_e("", "删除本地条目失败: 路由失败");
        return -1;
    }
    const int32_t rc = tm->delete_local_entry(save_path, root_name);
    log_i("", "删除本地条目完成: rc=%d", rc);
    return rc;
}

/* ------------------------------------------------------------------ */
/*  资源释放                                                          */
/* ------------------------------------------------------------------ */

DW_API void dw_submit_result_release(dw_submit_result_t *result) {
    // 释放逻辑统一收敛至内存工具（message/files/info_hash）。
    dw::utils::free_submit_result_fields(*result);
}

DW_API void dw_file_list_free(dw_file_info_t *files, int32_t count) {
    // 释放逻辑统一收敛至内存工具（各节点字符串 + 数组本体）。
    dw::utils::free_file_list(files, count);
}

DW_API void dw_task_list_free(dw_task_snapshot_t *tasks, int32_t count) {
    if (!tasks || count <= 0) {
        return;
    }
    for (int32_t i = 0; i < count; ++i) {
        if (tasks[i].natural_key) {
            std::free(const_cast<char *>(tasks[i].natural_key));
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
        arr[i].original_root_name = dw::utils::dup_cstr(r.original_root_name);
        arr[i].root_name = dw::utils::dup_cstr(r.root_name);
        arr[i].full_path = dw::utils::dup_cstr(r.full_path);
        arr[i].file_type = r.file_type;
        arr[i].ext = dw::utils::dup_cstr(r.ext);
        arr[i].parsed = r.parsed;
        arr[i].task_protocol = r.task_protocol;
        arr[i].task_natural_key = dw::utils::dup_cstr(r.task_natural_key);
        // 字符串复制失败（内存不足）：回滚已分配的字段与数组，
        // 不向调用方返回含 NULL 字段的半成品快照。
        if (!arr[i].client_id || !arr[i].save_path ||
            !arr[i].original_root_name || !arr[i].root_name || !arr[i].full_path ||
            !arr[i].ext || !arr[i].task_natural_key || !arr[i].message) {
            log_e("", "失败: 文件记录字符串复制内存不足 i=%d n=%d", i, n);
            dw_file_record_list_free(arr, i + 1);
            *out_records = nullptr;
            *out_count = 0;
            return -1;
        }
        arr[i].status = r.status;
        arr[i].total_size = r.total_size;
        arr[i].total_done = r.total_done;
        arr[i].priority = r.priority;
        arr[i].reason = r.reason;
        arr[i].message = dw::utils::dup_cstr(r.message);
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
        std::free(records[i].original_root_name);
        std::free(records[i].root_name);
        std::free(records[i].full_path);
        std::free(records[i].ext);
        std::free(records[i].task_natural_key);
        std::free(records[i].message);
    }
    std::free(records);
}

DW_API int32_t dw_get_or_register_file_record(const char *client_id,
                                               dw_protocol_t protocol,
                                               const char *natural_key,
                                               const char *save_path,
                                               bool *out_parsed) {
    if (!client_id || !natural_key || !out_parsed) return -1;
    if (!dw::g_downloader) return -1;
    auto *tm = dw::g_downloader->router ? dw::g_downloader->router->task_manager() : nullptr;
    if (!tm) return -1;
    const std::string sp = save_path ? save_path : "";
    *out_parsed = tm->get_or_register_file_record(client_id, protocol, natural_key, sp);
    return 0;
}

DW_API void dw_free(void *ptr) {
    if (ptr) {
        std::free(ptr);
    }
}
} /* extern "C" */
