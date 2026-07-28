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
#include <algorithm>

namespace dw {

namespace he = http_engine;
using he::internal::task_create_new;
using he::internal::validate_add_input;
using he::internal::set_result;
using he::internal::mkdir_recursive;
using he::internal::start_task;
using he::internal::fill_progress;
using he::internal::emit_resume;

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
    // 拉模型：不再启动监控线程，进度与续传由 TaskManager 采集循环驱动。
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

    /* 设置 HTTP 配置（仅 HTTP 相关字段） */
    apply_config(cfg);

    if (!ensure_running()) {
        return -1;
    }

    initialized_ = true;
    DW_LOG_SYS(DW_LOG_INFO, "HTTP 引擎初始化完成");
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

    DW_LOG_SYS(DW_LOG_INFO, "HTTP 引擎已销毁");
}

int32_t HttpEngine::add_task(const dw_task_params_t *params,
                             dw_submit_result_t *    out_result) {
    if (!params || !out_result) return -1;
    DW_LOG_TASK(DW_LOG_DEBUG, params->url ? params->url : "",
                "[EVENT] HTTP add_task 进入");

    const char *url = (params->url && params->url[0]) ? params->url : nullptr;
    const char *err = nullptr;

    if (!ensure_running()) {
        set_result(out_result, url, DW_REASON_ERROR, nullptr,
                   "ensure_running failed");
        return -1;
    }

    if (!validate_add_input(url, params->save_path, &err)) {
        set_result(out_result, url, DW_REASON_ERROR, nullptr,
                   "validate_add_input failed: url=%s output=%s err=%s",
                   url ? url : "(null)", params->save_path ? params->save_path : "(null)", err);
        return -1;
    }

    /* URL 幂等检查 */
    {
        std::lock_guard<std::mutex> lk(he::g_map_mtx);
        if (he::g_tasks.contains(url)) {
            DW_LOG_TASK(DW_LOG_INFO, url, "[EVENT] HTTP add_task 任务已存在（幂等返回）");
            set_result(out_result, url, DW_REASON_NONE, nullptr, nullptr);
            return 0;
        }
    }

    /* 创建目录 */
    if (auto dir_path = std::filesystem::path(params->save_path);
        !dir_path.empty() && !mkdir_recursive(dir_path.string())) {
        set_result(out_result, url, DW_REASON_ERROR, "目录创建失败",
                   "mkdir_recursive failed: dir=%s", dir_path.string().c_str());
        return -1;
    }

    const bool has_filename = (params->filename && params->filename[0]);
    const bool need_probing = !has_filename || !params->resume_data;

    auto tCtx_guard = task_create_new(url, params->save_path, params->filename);
    if (!tCtx_guard) {
        set_result(out_result, url, DW_REASON_ERROR, nullptr,
                   "task_create_new failed: url=%s", url);
        return -1;
    }
    dl_task_ctx *tCtx = tCtx_guard.get();

    // 开始即定名：filename 非空即 TaskManager 已定唯一名，直接落盘 save_path/filename；
    // 为空（URL 解析不出名）则待 probing 阶段 Content-Disposition 出名后上调定名。
    tCtx->name_resolved = has_filename ? 1 : 0;

    /* 不需要探测（有 resume_data 且有 filename）的情况 */
    if (!need_probing) {
        // 消费续传数据：反序列化回灌分片续传态与元数据，复用磁盘已有字节（不 ftruncate）。
        he::internal::HttpResumeData rd =
                he::internal::deserialize_resume(params->resume_data, params->resume_data_size);
        if (rd.ok) {
            // 续传回落同一落盘文件：优先用 resume 持久化的物理路径（即最终路径）；
            // 旧格式无 path 时回退 save_path/filename。
            std::string full_path = rd.full_file_path;
            if (full_path.empty() && params->filename && params->filename[0]) {
                full_path = (std::filesystem::path(params->save_path) / params->filename).string();
            }
            const int fd = dw_file_open_write(full_path.c_str()); // O_CREAT|O_WRONLY，不截断，保留已下字节
            if (fd < 0) {
                set_result(out_result, url, DW_REASON_ERROR, nullptr,
                           "open failed: path=%s errno=%d (%s)", full_path.c_str(), errno, std::strerror(errno));
                return -1;
            }
            tCtx->fd = fd;
            tCtx->full_file_path = full_path;
            // output_path 指向文件所在目录（即最终目录）。
            tCtx->output_path = std::filesystem::path(full_path).parent_path().string();
            tCtx->filename = std::filesystem::path(full_path).filename().string();
            tCtx->total_size = rd.total_size;
            tCtx->support_range = rd.support_range;
            tCtx->etag = rd.etag;
            tCtx->last_modified = rd.last_modified;
            tCtx->parts = std::move(rd.parts);
            // part_ctx 与 parts 一一对应：重建成相同数量（task_create_new 仅建 1 个）。
            tCtx->part_ctx.clear();
            tCtx->part_ctx.resize(tCtx->parts.size());
            for (size_t i = 0; i < tCtx->parts.size(); ++i) {
                tCtx->part_ctx[i].task = tCtx;
                tCtx->part_ctx[i].index = static_cast<int32_t>(i);
            }
            tCtx->probing = 0;
        }
        // rd 无效（格式损坏）则保持 probing=1，回退为全量探测重下。
    }

    /* 插入 map */
    bool inserted = false;
    try {
        std::lock_guard<std::mutex> lk(he::g_map_mtx);
        const auto [fst, snd] = he::g_tasks.emplace(tCtx->url, std::move(tCtx_guard));
        inserted = snd;
    } catch (...) { inserted = false; }
    if (!inserted) {
        set_result(out_result, url, DW_REASON_ERROR, nullptr,
                   "g_tasks.emplace failed: url=%s", url);
        return -1;
    }

    start_task(tCtx);

    DW_LOG_TASK(DW_LOG_INFO, tCtx->url.c_str(),
        "[EVENT] HTTP add_task 成功: output=%s probing=%d",
        tCtx->output_path.c_str(), tCtx->probing);
    set_result(out_result, url, DW_REASON_NONE,
               need_probing ? nullptr : "跳过探测，使用 resume_data",
               "add_task ok: url=%s probing=%d", tCtx->url.c_str(), tCtx->probing);
    return 0;
}

int32_t HttpEngine::pause_task(const char *         id,
                                dw_submit_result_t * out_result) {
    if (!id || !*id || !out_result) return -1;
    DW_LOG_TASK(DW_LOG_DEBUG, id, "[EVENT] HTTP pause_task 进入");

    if (!ensure_running()) {
        set_result(out_result, id, DW_REASON_ERROR, nullptr,
                   "ensure_running failed");
        return -1;
    }

    try {
        const char *url = id;
        bool hit = false;
        {
            std::lock_guard<std::mutex> lk(he::g_map_mtx);
            if (const auto it = he::g_tasks.find(url); it != he::g_tasks.end()) {
                // 非销毁暂停：仅置暂停标志。worker 线程据此自行退出，退出前固化一次 resume 断点；
                // ctx 保留在 g_tasks，待线程结束（thread_done）后统一由 sweep 回收，
                // 使资源回收职责回归 B（对齐 A/B 生命周期模型），不再由 pause 同步 join/析构。
                it->second->pause_req.store(1);
                hit = true;
            }
        }
        if (hit) {
            DW_LOG_TASK(DW_LOG_INFO, url, "[EVENT] HTTP pause_task 成功（非销毁，待 sweep 回收 ctx）");
        }
        set_result(out_result, url, DW_REASON_NONE, nullptr, nullptr);
        return 0;
    } catch (const std::exception &e) {
        DW_LOG_SYS(DW_LOG_ERROR, "[ERROR] HTTP pause_task exception: %s", e.what());
        return -1;
    } catch (...) {
        DW_LOG_SYS(DW_LOG_ERROR, "[ERROR] HTTP pause_task unknown exception");
        return -1;
    }
}

int32_t HttpEngine::delete_task(const char *         id,
                                 dw_submit_result_t * out_result) {
    if (!id || !*id || !out_result) return -1;
    DW_LOG_TASK(DW_LOG_DEBUG, id, "[EVENT] HTTP delete_task 进入");

    if (!ensure_running()) {
        set_result(out_result, id, DW_REASON_ERROR, nullptr,
                   "ensure_running failed");
        return -1;
    }

    try {
        const char *url = id;
        dl_task_ctx *hit = nullptr;
        std::string url_str, output_path;
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
            url_str = hit->url;
            output_path = hit->full_file_path;
            if (hit->task_thread.joinable()) hit->task_thread.join();
            if (!output_path.empty() && dw_file_unlink(output_path.c_str()) != 0 && errno != ENOENT)
                DW_LOG_TASK(DW_LOG_ERROR, url_str.c_str(),
                    "[ERROR] HTTP delete_task unlink failed: path=%s errno=%d", output_path.c_str(), errno);
            DW_LOG_TASK(DW_LOG_INFO, url_str.c_str(), "[CLEANUP] HTTP delete_task 成功");
        }
        set_result(out_result, url, DW_REASON_NONE, nullptr, nullptr);
        return 0;
    } catch (const std::exception &e) {
        DW_LOG_SYS(DW_LOG_ERROR, "[ERROR] HTTP delete_task exception: %s", e.what());
        return -1;
    } catch (...) {
        DW_LOG_SYS(DW_LOG_ERROR, "[ERROR] HTTP delete_task unknown exception");
        return -1;
    }
}

bool HttpEngine::query_progress(const char *key, EngineProgress &out) {
    out = {};
    out.protocol = DW_PROTOCOL_HTTP;
    if (!key || !*key) return false;

    // 纯读快照：本函数不触碰 TaskManager mtx_，也不产生任何 resume。
    // HTTP 状态由 libcurl 回调持续写入 dl_task_ctx（推送模型），此处仅聚合读出；
    // resume 已改为 worker 线程按门槛经 post_resume_data 异步上报（见 http_engine_impl.cpp），
    // 不再随返回值携带 resume_blob。
    std::lock_guard<std::mutex> lk(he::g_map_mtx);
    const auto it = he::g_tasks.find(key);
    if (it == he::g_tasks.end() || !it->second) return false;
    dl_task_ctx *tCtx = it->second.get();
    dw_progress_t snap;
    fill_progress(tCtx, &snap);   // 复用推送路径的组装逻辑（内部持 speed_mtx 聚合分片）
    out.valid         = true;
    out.status        = snap.task_status;
    out.total_size    = snap.total_size;
    out.total_done    = snap.total_done;
    out.progress      = snap.progress;
    out.download_rate = snap.download_rate;
    out.name          = tCtx->filename;
    out.output_path   = tCtx->output_path;   // 落盘目录（= save_path，无临时目录）
    out.support_range = snap.support_range;
    out.etag          = tCtx->etag;
    out.last_modified = tCtx->last_modified;
    out.reason        = snap.reason;
    out.message       = tCtx->message;
    out.server_name   = tCtx->server_filename;   // 服务器原始建议名（完成拍改名比对用）
    // 分片实时状态不再导出（app 已下线分片展示）；tCtx->parts 仅供引擎内部多连接与区间快照使用。
    // 采集到终态即置位：sweep 仅在终态被采集至少一次后才回收 ctx，
    // 避免抢在采集前回收导致 TaskManager 无法观测终态、任务卡在下载中。
    if (out.status == DW_TASK_STATUS_COMPLETED || out.status == DW_TASK_STATUS_ERROR) {
        tCtx->terminal_reported.store(1);
    }
    return true;
}

std::vector<dw_byte_range_t> HttpEngine::get_file_ranges(const char *id, int32_t /*file_index*/) {
    // HTTP 单文件模型：file_index 忽略（签名与接口统一）。
    std::vector<dw_byte_range_t> ranges;
    if (!id || !*id) return ranges;
    // 收集各 part 已下载区间 [start, start+done-1]（仅 done>0），随后排序合并。
    {
        std::lock_guard<std::mutex> lk(he::g_map_mtx);
        auto it = he::g_tasks.find(id);
        if (it == he::g_tasks.end() || !it->second) return ranges;
        dl_task_ctx *tCtx = it->second.get();
        std::lock_guard<std::mutex> slk(tCtx->speed_mtx);
        for (const auto &part: tCtx->parts) {
            if (part.done > 0) {
                ranges.push_back(dw_byte_range_t{part.start, part.start + part.done - 1});
            }
        }
    }
    if (ranges.size() <= 1) return ranges;
    std::sort(ranges.begin(), ranges.end(),
              [](const dw_byte_range_t &a, const dw_byte_range_t &b) { return a.start < b.start; });
    std::vector<dw_byte_range_t> merged;
    merged.push_back(ranges.front());
    for (size_t i = 1; i < ranges.size(); ++i) {
        // 相接或重叠则合并（end 含约定）
        if (ranges[i].start <= merged.back().end + 1) {
            if (ranges[i].end > merged.back().end) merged.back().end = ranges[i].end;
        } else {
            merged.push_back(ranges[i]);
        }
    }
    return merged;
}

void HttpEngine::sweep() {
    if (!initialized_ || !he::g_running.load()) return;
    // HTTP 仅在下载中需要持有线程/curl handle；线程结束后统一在此回收上下文（含暂停态）。
    std::vector<std::string> to_reclaim;
    {
        std::lock_guard<std::mutex> lk(he::g_map_mtx);
        for (auto &[url, tCtx]: he::g_tasks) {
            if (!tCtx) continue;
            if (tCtx->thread_done.load() != 1) continue;   // 线程未结束不回收，规避 use-after-free
            const dw_task_status_t st = tCtx->status;
            if (st == DW_TASK_STATUS_COMPLETED || st == DW_TASK_STATUS_ERROR) {
                // 拉模型：终态需已被采集循环（query_progress）观测至少一次后才回收，
                // 否则会抢在 TaskManager 处理终态前销毁 ctx，使任务永久卡在下载中。
                if (tCtx->terminal_reported.load() == 1) to_reclaim.push_back(url);
            } else if (tCtx->pause_req.load() == 1) {
                // 暂停态：PAUSED 帧由 TaskManager 合成，无需 terminal_reported 守卫；
                // worker 已结束即可回收，内存记录随后由 B 在确认 ctx 回收后逐出。
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
        DW_LOG_TASK(DW_LOG_INFO, owned->url.c_str(),
                 "[CLEANUP] 终态回收 HTTP 上下文 url=%s", owned->url.c_str());
    }
}

} // namespace dw
