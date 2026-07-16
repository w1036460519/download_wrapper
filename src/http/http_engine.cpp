/**
 * @file http_engine.cpp
 * @brief HTTP 下载引擎实现：委托到 http_engine_impl 中的核心逻辑。
 */

#include "http/http_engine.h"
#include "http/http_engine_internal.h"
#include "internal/downloader_internal.h"

#include <cstdlib>
#include <cstring>
#include <filesystem>

namespace dw {

namespace he = http_engine;
using he::internal::task_create_new;
using he::internal::validate_add_input;
using he::internal::set_result;
using he::internal::mkdir_recursive;
using he::internal::start_task;
using he::internal::monitor_thread_func;

namespace {

char *dup_string(const char *target, const char *source, const char *def) {
    if (target == source) return const_cast<char *>(target);
    std::free(const_cast<char *>(target));
    return (source && *source) ? strdup(source) : (def ? strdup(def) : nullptr);
}

void apply_config(const dw_config_t *cfg) {
    auto &g = he::g_cfg;
    g.proxy = dup_string(g.proxy, cfg->proxy, nullptr);
    g.proxy_username = dup_string(g.proxy_username, cfg->proxy_username, nullptr);
    g.proxy_password = dup_string(g.proxy_password, cfg->proxy_password, nullptr);
    g.user_agent = dup_string(g.user_agent, cfg->user_agent, "download_wrapper/2.0");
    g.ca_bundle = dup_string(g.ca_bundle, cfg->ca_bundle, nullptr);
    g.connect_timeout_seconds = cfg->connect_timeout_seconds > 0 ? cfg->connect_timeout_seconds : 15;
    g.request_timeout_seconds = cfg->request_timeout_seconds;
    g.low_speed_limit_bps = cfg->low_speed_limit_bps >= 0 ? cfg->low_speed_limit_bps : 0;
    g.low_speed_time = cfg->low_speed_time > 0 ? cfg->low_speed_time : 0;
    g.max_redirect = cfg->max_redirect > 0 ? cfg->max_redirect : 5;
    g.verify_ssl = cfg->verify_ssl >= 0 ? cfg->verify_ssl : 1;
    g.max_retries = cfg->max_retries >= 0 ? cfg->max_retries : 3;
    g.default_parts = cfg->default_parts > 0 ? cfg->default_parts : 4;
    g.min_size_for_split = cfg->min_size_for_split > 0 ? cfg->min_size_for_split : 1 * 1024 * 1024;
    g.max_concurrent_connections = cfg->max_concurrent_connections;
    g.status_callback_interval_ms = cfg->status_callback_interval_ms > 0 ? cfg->status_callback_interval_ms : 1000;
    g.log_level = cfg->log_level >= DW_LOG_DEBUG && cfg->log_level <= DW_LOG_ERROR ? cfg->log_level : DW_LOG_INFO;
}

bool ensure_running() {
    if (he::g_running.load()) return true;
    he::g_exit_flag.store(false);
    if (curl_global_init(CURL_GLOBAL_DEFAULT) != CURLE_OK) return false;
    he::g_running.store(true);
    try {
        he::g_monitor_thread = std::thread(monitor_thread_func);
    } catch (...) {
        he::g_running.store(false);
        curl_global_cleanup();
        return false;
    }
    return true;
}

} // anonymous namespace

HttpEngine::HttpEngine() = default;

HttpEngine::~HttpEngine() {
    if (initialized_) {
        destroy();
    }
}

int32_t HttpEngine::init(const dw_config_t *cfg) {
    if (!cfg) return -1;

    /* 桥接回调：HTTP 引擎使用全局下载器的回调 */
    he::g_progress_cb = nullptr; /* 将由 ensure_running 时从 global_downloader 获取 */
    he::g_log_cb = nullptr;

    /* 设置 HTTP 配置（仅 HTTP 相关字段） */
    apply_config(cfg);

    /* 从全局下载器同步回调 */
    if (auto *dl = global_downloader()) {
        he::g_progress_cb = dl->progress_cb;
        he::g_log_cb = dl->log_cb;
    }

    if (!ensure_running()) {
        return -1;
    }

    initialized_ = true;
    HTTP_LOG(DW_LOG_INFO, "", "HTTP 引擎初始化完成");
    return 0;
}

void HttpEngine::destroy() {
    if (!initialized_) return;
    initialized_ = false;

    if (!he::g_running.load()) return;
    he::g_exit_flag.store(true);

    /* 取消所有任务 */
    {
        std::lock_guard<std::mutex> lk(he::g_map_mtx);
        for (auto &tCtx: he::g_tasks | std::views::values) {
            tCtx->cancel_req.store(1);
        }
    }

    /* 等待 monitor 线程退出 */
    if (he::g_monitor_thread.joinable()) he::g_monitor_thread.join();

    /* 等待所有任务线程退出 + 清理 */
    {
        std::lock_guard<std::mutex> lk(he::g_map_mtx);
        for (auto &[url, tCtx]: he::g_tasks) {
            if (tCtx->task_thread.joinable()) tCtx->task_thread.join();
        }
        he::g_tasks.clear();
    }

    curl_global_cleanup();
    std::free(const_cast<char *>(he::g_cfg.proxy));
    std::free(const_cast<char *>(he::g_cfg.proxy_username));
    std::free(const_cast<char *>(he::g_cfg.proxy_password));
    std::free(const_cast<char *>(he::g_cfg.user_agent));
    std::free(const_cast<char *>(he::g_cfg.ca_bundle));
    he::g_cfg = {};
    he::g_running.store(false);
    he::g_exit_flag.store(false);

    HTTP_LOG(DW_LOG_INFO, "", "HTTP 引擎已销毁");
}

int32_t HttpEngine::add_task(const dw_task_params_t *params,
                             dw_submit_result_t *    out_result) {
    if (!params || !out_result) return -1;

    /* 同步回调指针 */
    if (auto *dl = global_downloader()) {
        he::g_progress_cb = dl->progress_cb;
        he::g_log_cb = dl->log_cb;
    }

    const char *url = (params->url && params->url[0]) ? params->url : params->task_id;
    const char *trace_id = params->trace_id ? params->trace_id : "";
    const char *err = nullptr;

    if (!ensure_running()) {
        set_result(out_result, url, trace_id, DW_REASON_INTERNAL, nullptr,
                   "ensure_running failed");
        return -1;
    }

    if (!validate_add_input(url, params->save_path, &err)) {
        set_result(out_result, url, trace_id, DW_REASON_INVALID_INPUT, nullptr,
                   "validate_add_input failed: url=%s output=%s err=%s",
                   url ? url : "(null)", params->save_path ? params->save_path : "(null)", err);
        return -1;
    }

    /* URL 幂等检查 */
    {
        std::lock_guard<std::mutex> lk(he::g_map_mtx);
        if (he::g_tasks.contains(url)) {
            set_result(out_result, url, trace_id, DW_REASON_NONE, nullptr, nullptr);
            return 0;
        }
    }

    /* 创建目录 */
    if (auto dir_path = std::filesystem::path(params->save_path);
        !dir_path.empty() && !mkdir_recursive(dir_path.string())) {
        set_result(out_result, url, trace_id, DW_REASON_INTERNAL, "目录创建失败",
                   "mkdir_recursive failed: dir=%s", dir_path.string().c_str());
        return -1;
    }

    const bool has_filename = (params->filename && params->filename[0]);
    const bool need_probing = !has_filename || !params->resume_data;

    auto tCtx_guard = task_create_new(url, params->save_path, trace_id, params->filename);
    if (!tCtx_guard) {
        set_result(out_result, url, trace_id, DW_REASON_INTERNAL, nullptr,
                   "task_create_new failed: url=%s", url);
        return -1;
    }
    dl_task_ctx *tCtx = tCtx_guard.get();

    /* 不需要探测（有 resume_data 且有 filename）的情况 */
    if (!need_probing) {
        std::string full_path;
        if (params->filename && params->filename[0]) {
            full_path = (std::filesystem::path(params->save_path) / params->filename).string();
        }
        const int fd = dw_file_open_write(full_path.c_str());
        if (fd < 0) {
            set_result(out_result, url, trace_id, DW_REASON_INTERNAL, nullptr,
                       "open failed: path=%s errno=%d (%s)", full_path.c_str(), errno, std::strerror(errno));
            return -1;
        }
        tCtx->fd = fd;
        tCtx->full_file_path = full_path;
        tCtx->probing = 0;
    }

    /* 插入 map */
    bool inserted = false;
    try {
        std::lock_guard<std::mutex> lk(he::g_map_mtx);
        const auto [fst, snd] = he::g_tasks.emplace(tCtx->url, std::move(tCtx_guard));
        inserted = snd;
    } catch (...) { inserted = false; }
    if (!inserted) {
        set_result(out_result, url, trace_id, DW_REASON_INTERNAL, nullptr,
                   "g_tasks.emplace failed: url=%s", url);
        return -1;
    }

    start_task(tCtx);

    set_result(out_result, url, trace_id, DW_REASON_NONE,
               need_probing ? nullptr : "跳过探测，使用 resume_data",
               "add_task ok: url=%s probing=%d", tCtx->url.c_str(), tCtx->probing);
    HTTP_LOG(DW_LOG_INFO, tCtx->trace_id.c_str(),
        "add_task ok: url=%s output=%s probing=%d",
        tCtx->url.c_str(), tCtx->output_path.c_str(), tCtx->probing);
    return 0;
}

int32_t HttpEngine::pause_task(const char *         id,
                                dw_submit_result_t * out_result) {
    if (!id || !*id || !out_result) return -1;

    if (!ensure_running()) {
        set_result(out_result, id, nullptr, DW_REASON_INTERNAL, nullptr,
                   "ensure_running failed");
        return -1;
    }

    try {
        const char *url = id;
        dl_task_ctx *hit = nullptr;
        std::string trace_id, url_str;
        std::unique_ptr<dl_task_ctx> hit_owner;
        {
            std::lock_guard<std::mutex> lk(he::g_map_mtx);
            if (auto it = he::g_tasks.find(url); it != he::g_tasks.end()) {
                it->second->pause_req.store(1);
                hit_owner = std::move(it->second);
                hit = hit_owner.get();
                he::g_tasks.erase(it);
            }
        }
        if (hit) {
            trace_id = hit->trace_id;
            url_str = hit->url;
            if (hit->task_thread.joinable()) hit->task_thread.join();
            int64_t downloaded = 0;
            for (const auto &part: hit->parts) downloaded += part.done;
            hit->status = DW_TASK_STATUS_PAUSED;
            hit->reason = DW_REASON_NONE;
            hit->message = "已暂停";
            HTTP_LOG(DW_LOG_INFO, trace_id.c_str(), "pause_task ok: url=%s downloaded=%lld",
                url_str.c_str(), static_cast<long long>(downloaded));
        }
        set_result(out_result, url, hit ? trace_id.c_str() : "", DW_REASON_NONE, nullptr, nullptr);
        return 0;
    } catch (const std::exception &e) {
        HTTP_LOG(DW_LOG_ERROR, "", "pause_task exception: %s", e.what());
        return -1;
    } catch (...) {
        HTTP_LOG(DW_LOG_ERROR, "", "pause_task unknown exception");
        return -1;
    }
}

int32_t HttpEngine::resume_task(const dw_task_params_t *params,
                                 dw_submit_result_t *    out_result) {
    /* resume 复用 add_task 逻辑：带 resume_data 时跳过探测 */
    return add_task(params, out_result);
}

int32_t HttpEngine::delete_task(const char *         id,
                                 dw_submit_result_t * out_result) {
    if (!id || !*id || !out_result) return -1;

    if (!ensure_running()) {
        set_result(out_result, id, nullptr, DW_REASON_INTERNAL, nullptr,
                   "ensure_running failed");
        return -1;
    }

    try {
        const char *url = id;
        dl_task_ctx *hit = nullptr;
        std::string trace_id, url_str, output_path;
        std::unique_ptr<dl_task_ctx> hit_owner;
        {
            std::lock_guard<std::mutex> lk(he::g_map_mtx);
            if (auto it = he::g_tasks.find(url); it != he::g_tasks.end()) {
                it->second->cancel_req.store(1);
                hit_owner = std::move(it->second);
                hit = hit_owner.get();
                he::g_tasks.erase(it);
            }
        }
        if (hit) {
            trace_id = hit->trace_id;
            url_str = hit->url;
            output_path = hit->full_file_path;
            if (hit->task_thread.joinable()) hit->task_thread.join();
            if (!output_path.empty() && dw_file_unlink(output_path.c_str()) != 0 && errno != ENOENT)
                HTTP_LOG(DW_LOG_ERROR, trace_id.c_str(),
                "delete_task: unlink failed url=%s path=%s errno=%d", url_str.c_str(), output_path.c_str(), errno);
            HTTP_LOG(DW_LOG_INFO, trace_id.c_str(), "delete_task ok: url=%s", url_str.c_str());
        }
        set_result(out_result, url, hit ? trace_id.c_str() : "", DW_REASON_NONE, nullptr, nullptr);
        return 0;
    } catch (const std::exception &e) {
        HTTP_LOG(DW_LOG_ERROR, "", "delete_task exception: %s", e.what());
        return -1;
    } catch (...) {
        HTTP_LOG(DW_LOG_ERROR, "", "delete_task unknown exception");
        return -1;
    }
}

void HttpEngine::sweep() {
    if (!initialized_ || !he::g_running.load()) return;
    // HTTP 仅在下载中需要持有线程/curl handle；终态任务线程结束后回收上下文。
    // 暂停态由 pause_task 即时回收，下载中任务保留，故此处只处理 COMPLETED/ERROR。
    std::vector<std::string> to_reclaim;
    {
        std::lock_guard<std::mutex> lk(he::g_map_mtx);
        for (auto &[url, tCtx]: he::g_tasks) {
            if (!tCtx) continue;
            const dw_task_status_t st = tCtx->status;
            const bool terminal = (st == DW_TASK_STATUS_COMPLETED || st == DW_TASK_STATUS_ERROR);
            if (terminal && tCtx->thread_done.load() == 1) {
                to_reclaim.push_back(url);
            }
        }
    }
    for (const auto &url: to_reclaim) {
        std::unique_ptr<dl_task_ctx> owned;
        {
            std::lock_guard<std::mutex> lk(he::g_map_mtx);
            const auto it = he::g_tasks.find(url);
            if (it == he::g_tasks.end()) continue;
            owned = std::move(it->second); // 移出 map，脱离全局可见
            he::g_tasks.erase(it);
        }
        // 锁外 join 已结束的线程并析构 ctx（触发 curl/文件句柄释放）
        if (owned->task_thread.joinable()) owned->task_thread.join();
        HTTP_LOG(DW_LOG_INFO, owned->trace_id.c_str(),
                 "[CLEANUP] 终态回收 HTTP 上下文 url=%s", owned->url.c_str());
    }
}

} // namespace dw
