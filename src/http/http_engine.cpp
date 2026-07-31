/**
 * @file http_engine.cpp
 * @brief HTTP 下载引擎实现：委托到 http_engine_impl 中的核心逻辑。
 */

#include "http/http_engine.h"
#include "http/http_engine_internal.h"
#include "internal/downloader_internal.h"
#include "utils/time_util.h"

#include <algorithm>
#include <cstdlib>
#include <filesystem>

namespace dw {
    namespace he = http_engine;
    using he::internal::task_create_new;
    using he::internal::validate_add_input;
    using he::internal::set_result;
    using he::internal::mkdir_recursive;
    using he::internal::start_task;
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
            g.log_level = cfg->log_level >= DW_LOG_DEBUG && cfg->log_level <= DW_LOG_ERROR
                              ? cfg->log_level
                              : DW_LOG_INFO;
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
        if (!cfg) {
            DW_LOG_SYS(DW_LOG_ERROR, "[ERROR] HTTP init 失败: cfg 为空");
            return -1;
        }

        /* 设置 HTTP 配置（仅 HTTP 相关字段） */
        apply_config(cfg);

        if (!ensure_running()) {
            DW_LOG_SYS(DW_LOG_ERROR, "[ERROR] HTTP init 失败: curl_global_init 失败");
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
            for (const auto &tCtx: he::g_tasks | std::views::values) {
                tCtx->cancel_req.store(1);
            }
        }

        /* 等待所有任务线程退出 + 清理 */
        {
            std::lock_guard<std::mutex> lk(he::g_map_mtx);
            for (const auto &tCtx: he::g_tasks | std::views::values) {
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
                                 dw_submit_result_t *out_result) {
        if (!params || !out_result) {
            DW_LOG_SYS(DW_LOG_ERROR, "[ERROR] HTTP add_task 失败: 入参为空 params=%p out_result=%p",
                       static_cast<const void *>(params), static_cast<void *>(out_result));
            return -1;
        }
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

        /* URL 判重：线程仍在运行且非删除中视为继续（幂等返回）；删除中 / 线程已结束
           的残留 ctx 就地收割后按重新添加走，携 resume 即续传。文件删除归 TaskManager，
           此处只回收运行时资源（同 URL 重新添加时 TaskManager 会撤销旧的待删意图）。 */
        {
            std::unique_ptr<dl_task_ctx> stale;
            {
                std::lock_guard<std::mutex> lk(he::g_map_mtx);
                if (const auto it = he::g_tasks.find(url); it != he::g_tasks.end()) {
                    if (it->second->thread_done.load() != 1 && it->second->delete_req.load() != 1) {
                        DW_LOG_TASK(DW_LOG_INFO, url, "[EVENT] HTTP add_task 任务运行中（幂等继续）");
                        set_result(out_result, url, DW_REASON_NONE, nullptr, nullptr);
                        return 0;
                    }
                    stale = std::move(it->second);
                    he::g_tasks.erase(it);
                }
            }
            if (stale) {
                // 锁外 join 并析构（释放 curl/文件句柄），与 sweep 同模式：移出 map 即独占。
                // 删除中的 ctx：cancel_req 已置位，worker 短暂后自行退出，join 开销可控。
                if (stale->task_thread.joinable()) stale->task_thread.join();
                stale.reset();
                DW_LOG_TASK(DW_LOG_INFO, url, "[CLEANUP] 回收残留上下文后重新添加");
            }
        }

        /* 创建目录 */
        if (auto dir_path = std::filesystem::path(params->save_path);
            !dir_path.empty() && !mkdir_recursive(dir_path.string())) {
            set_result(out_result, url, DW_REASON_ERROR, "目录创建失败",
                       "mkdir_recursive failed: dir=%s", dir_path.string().c_str());
            return -1;
        }

        const bool has_resume = params->resume_data && params->resume_data_size > 0;

        auto tCtx_guard = task_create_new(url, params->save_path, params->filename);
        if (!tCtx_guard) {
            set_result(out_result, url, DW_REASON_ERROR, nullptr,
                       "task_create_new failed: url=%s", url);
            return -1;
        }
        dl_task_ctx *tCtx = tCtx_guard.get();

        /* 续传回灌：resume 解析成功且既有文件可打开才跳过探测；否则保持 probing=1，
           由探测即下载路径全量重下（沿用历史凭证名回落同一文件，不再判重） */
        if (has_resume) {
            // 恢复任务标志：不论存档是否有效均置位。回退全量探测时 finalize_probing
            // 仍上调判重，TaskManager 幂等守卫沿用既有唯一名（包层任务重建包层路径），
            // 不会分裂出第二个文件。
            tCtx->is_resume = 1;
            // 消费续传数据：反序列化回灌分片续传态与元数据，复用磁盘已有字节（不 ftruncate）。
            he::internal::HttpResumeData rd =
                    he::internal::deserialize_resume(params->resume_data, params->resume_data_size);
            // 存档损坏在此同步检出（与下方文件不可用同为回退全量重下，仅补可观测性）。
            if (!rd.ok) {
                DW_LOG_TASK(DW_LOG_INFO, url,
                            "[EVENT] resume 存档无效（格式损坏），回退全量重下（沿用原名）: size=%zu",
                            static_cast<size_t>(params->resume_data_size));
            }
            // 续传回落同一落盘文件：优先用 resume 持久化的物理路径（即最终路径）；
            // 旧格式无 path 时回退 save_path/filename。
            std::string full_path = rd.full_file_path;
            if (full_path.empty() && params->filename && params->filename[0]) {
                full_path = (std::filesystem::path(params->save_path) / params->filename).string();
            }
            // 局部验证打开既有文件（不创建，即开即关；写入由各分片句柄承担）：
            // 文件被外部删除/不可写时打开失败，丢弃续传进度回退全量重下，
            // 避免静默重建空稀疏文件在已下区间留洞。
            if (rd.ok && !full_path.empty()) {
                if (DwFile probe; !probe.open(full_path, false)) {
                    DW_LOG_TASK(DW_LOG_INFO, url,
                                "[EVENT] 续传文件不可用（errno=%d），回退全量重下: %s",
                                errno, full_path.c_str());
                } else {
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
            }
            // rd 无效（格式损坏）同样保持 probing=1，回退全量探测重下。
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
                   tCtx->probing ? nullptr : "跳过探测，使用 resume_data",
                   "add_task ok: url=%s probing=%d", tCtx->url.c_str(), tCtx->probing);
        return 0;
    }

    int32_t HttpEngine::pause_task(const char *id,
                                   dw_submit_result_t *out_result) {
        if (!id || !*id || !out_result) {
            DW_LOG_SYS(DW_LOG_ERROR, "[ERROR] HTTP pause_task 失败: 入参为空 id=%s out_result=%p",
                       (id && *id) ? id : "(null)", static_cast<void *>(out_result));
            return -1;
        }
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

    int32_t HttpEngine::delete_task(const char *id,
                                    dw_submit_result_t *out_result) {
        if (!id || !*id || !out_result) {
            DW_LOG_SYS(DW_LOG_ERROR, "[ERROR] HTTP delete_task 失败: 入参为空 id=%s out_result=%p",
                       (id && *id) ? id : "(null)", static_cast<void *>(out_result));
            return -1;
        }
        DW_LOG_TASK(DW_LOG_DEBUG, id, "[EVENT] HTTP delete_task 进入");

        if (!ensure_running()) {
            set_result(out_result, id, DW_REASON_ERROR, nullptr,
                       "ensure_running failed");
            return -1;
        }

        // 删除异步化：仅置取消 + 删除标志立即返回（与 pause 同模式），资源回收由
        // B 线程 sweep 执行（join/析构关句柄）；文件删除归 TaskManager，经
        // task_released 确认 ctx 回收（句柄已关闭）后按配置执行。
        try {
            const char *url = id;
            bool hit = false;
            {
                std::lock_guard<std::mutex> lk(he::g_map_mtx);
                if (const auto it = he::g_tasks.find(url); it != he::g_tasks.end()) {
                    it->second->cancel_req.store(1);
                    it->second->delete_req.store(1);
                    hit = true;
                }
            }
            set_result(out_result, url, DW_REASON_NONE, nullptr, nullptr);
            if (hit) {
                DW_LOG_TASK(DW_LOG_INFO, url, "[EVENT] HTTP delete_task 已标记（待 sweep 回收）");
                return 0;
            }
            // 未持有任务（暂停 ctx 已回收 / 从未运行）：无运行时资源。
            return 1;
        } catch (const std::exception &e) {
            DW_LOG_SYS(DW_LOG_ERROR, "[ERROR] HTTP delete_task exception: %s", e.what());
            return -1;
        } catch (...) {
            DW_LOG_SYS(DW_LOG_ERROR, "[ERROR] HTTP delete_task unknown exception");
            return -1;
        }
    }

    bool HttpEngine::task_released(const char *id) {
        if (!id || !*id) return true;
        // 引擎停止 / 未初始化：destroy 已 join 全部线程并析构 ctx，视为已释放。
        if (!initialized_ || !he::g_running.load()) return true;
        // ctx 仍在 map（含删除中待 sweep 回收）即持有线程 / 分片文件句柄，未释放；
        // sweep 移出并析构后（文件全关）方可安全删除落盘文件。
        std::lock_guard<std::mutex> lk(he::g_map_mtx);
        return !he::g_tasks.contains(id);
    }

    std::vector<dw_byte_range_t> HttpEngine::get_file_ranges(const char *id, int32_t /*file_index*/) {
        // HTTP 单文件模型：file_index 忽略（签名与接口统一）。
        std::vector<dw_byte_range_t> ranges;
        if (!id || !*id) return ranges;
        // 收集各 part 已下载区间 [start, start+done-1]（仅 done>0），随后排序合并。
        {
            std::lock_guard<std::mutex> lk(he::g_map_mtx);
            const auto it = he::g_tasks.find(id);
            if (it == he::g_tasks.end() || !it->second) return ranges;
            dl_task_ctx *tCtx = it->second.get();
            std::lock_guard<std::mutex> slk(tCtx->speed_mtx);
            for (const auto &part: tCtx->parts) {
                if (part.done > 0) {
                    ranges.push_back(dw_byte_range_t{.start = part.start, .end = part.start + part.done - 1});
                }
            }
        }
        if (ranges.size() <= 1) return ranges;
        std::ranges::sort(ranges,
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
        const int64_t now_ms = dw::utils::now_unix_ms();
        std::vector<std::string> to_reclaim;
        {
            std::lock_guard<std::mutex> lk(he::g_map_mtx);
            for (auto &[url, tCtx]: he::g_tasks) {
                if (!tCtx) continue;
                if (tCtx->thread_done.load() != 1) continue; // 线程未结束不回收，规避 use-after-free
                if (tCtx->delete_req.load() == 1) {
                    // 删除中：TaskManager 已移除记录不再采集，无需等待终态观测。
                    to_reclaim.push_back(url);
                    continue;
                }
                if (const dw_task_status_t st = tCtx->status;
                    st == DW_TASK_STATUS_COMPLETED || st == DW_TASK_STATUS_ERROR) {
                    // 推模型：终态已由 push_progress 推入 TaskManager 内存；延迟 4s 回收
                    // 确保 A 线程至少采集一拍终态后再销毁 ctx（2 个 maintenance 周期）。
                    const int64_t pushed_at = tCtx->terminal_pushed_at_ms.load();
                    if (pushed_at > 0 && (now_ms - pushed_at) >= 4000) {
                        to_reclaim.push_back(url);
                    }
                } else if (tCtx->pause_req.load() == 1) {
                    // 暂停态：PAUSED 帧由 TaskManager 合成，worker 已结束即可回收。
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
            // 锁外 join 已结束的线程并析构 ctx（触发 curl/文件句柄释放；
            // 删除中任务的文件由 TaskManager 在 task_released 确认后按配置删除）
            if (owned->task_thread.joinable()) owned->task_thread.join();
            const bool deleting = owned->delete_req.load() == 1;
            owned.reset(); // 显式析构关闭全部分片文件句柄（Windows 句柄打开期间禁止删除文件）
            DW_LOG_TASK(DW_LOG_INFO, url.c_str(),
                        deleting ? "[CLEANUP] 删除回收 HTTP 上下文 url=%s"
                                 : "[CLEANUP] 终态回收 HTTP 上下文 url=%s", url.c_str());
        }
    }
} // namespace dw
