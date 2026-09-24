/**
 * @file http_engine.cpp
 * @brief HTTP 下载引擎实现：委托到 http_engine_impl 中的核心逻辑。
 */

#include "http/http_engine.h"
#include "core/task_manager.h"
#include "http/http_engine_internal.h"
#include "internal/downloader_internal.h"
#include "utils/string_util.h"
#include "utils/time_util.h"

#include <algorithm>
#include <cstdlib>
#include <filesystem>

namespace dw {
    namespace he = http_engine;
    using he::internal::task_create_new;
    using he::internal::validate_add_input;
    using he::internal::mkdir_recursive;
    using he::internal::start_task;
    using he::internal::emit_resume;

    namespace {
        /// 从权威配置同步到 HTTP 引擎全局变量。
        void sync_config(const Config &cfg) {
            auto &g = he::g_cfg;
            g.connect_timeout_seconds = cfg.connect_timeout_seconds > 0 ? cfg.connect_timeout_seconds : 15;
            g.request_timeout_seconds = cfg.request_timeout_seconds;
            g.low_speed_limit_bps = cfg.low_speed_limit_bps >= 0 ? cfg.low_speed_limit_bps : 0;
            g.low_speed_time = cfg.low_speed_time > 0 ? cfg.low_speed_time : 0;
            g.max_redirect = cfg.max_redirect > 0 ? cfg.max_redirect : 5;
            g.verify_ssl = cfg.verify_ssl;
            g.max_retries = cfg.max_retries >= 0 ? cfg.max_retries : 3;
            g.default_parts = cfg.default_parts > 0 ? cfg.default_parts : 4;
            g.min_size_for_split = cfg.min_size_for_split > 0 ? cfg.min_size_for_split : 1 * 1024 * 1024;
            g.download_rate_limit = cfg.download_rate_limit > 0 ? cfg.download_rate_limit : 0;
            g.log_level = cfg.log_level >= DW_LOG_DEBUG && cfg.log_level <= DW_LOG_ERROR
                              ? cfg.log_level
                              : DW_LOG_INFO;
            g.proxy = cfg.proxy;
            g.proxy_username = cfg.proxy_username;
            g.proxy_password = cfg.proxy_password;
            g.user_agent = cfg.user_agent;
            g.ca_bundle = cfg.ca_bundle;
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

    HttpEngine::HttpEngine(TaskManager *task_manager) : IDownloadEngine(task_manager) {
        he::g_task_manager = task_manager;
    }

    HttpEngine::~HttpEngine() {
        if (initialized_) {
            destroy();
        }
    }

    int32_t HttpEngine::init() {
        if (!task_manager_) {
            log_e("", "HTTP init 失败: task_manager 为空");
            return -1;
        }
        sync_config(task_manager_->config());

        if (!ensure_running()) {
            log_e("", "HTTP init 失败: curl_global_init 失败");
            return -1;
        }

        initialized_ = true;
        log_i("", "HTTP 引擎初始化完成");
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
            for (const auto &[_, tCtx]: he::g_tasks) {
                tCtx->cancel_req.store(1);
            }
        }

        /* 等待所有任务线程退出 + 清理 */
        {
            std::lock_guard<std::mutex> lk(he::g_map_mtx);
            for (const auto &[_, tCtx]: he::g_tasks) {
                if (tCtx->task_thread.joinable()) tCtx->task_thread.join();
            }
            he::g_tasks.clear();
        }

        curl_global_cleanup();
        he::g_cfg = {};
        he::g_running.store(false);
        he::g_exit_flag.store(false);
        he::g_task_manager = nullptr;

        log_i("", "HTTP 引擎已销毁");
    }

    dw_submit_result_t HttpEngine::add_task(const TaskParams *params) {
        if (!params) {
            log_e("", "HTTP add_task 失败: 入参为空");
            return dw_submit_result_t::failure(DW_REASON_ERROR, "入参为空");
        }
        const char *url = params->url.c_str();
        log_d(url, "HTTP 添加任务: url={}", url);
        const char *err = nullptr;

        if (!ensure_running()) {
            return dw_submit_result_t::failure(DW_REASON_ERROR, "ensure_running failed");
        }

        // save_path 回退链：任务级 > 配置级 > 空（拒绝）
        const std::string save_path = [&]() -> std::string {
            if (!params->save_path.empty()) return params->save_path;
            if (auto *d = global_downloader(); d && !d->config.save_path.empty()) {
                return d->config.save_path;
            }
            return "";
        }();

        if (!validate_add_input(url, save_path.c_str(), &err)) {
            log_e(url, "validate_add_input failed: output={} err={}", save_path, err ? err : "");
            return dw_submit_result_t::failure(DW_REASON_ERROR, err ? err : "输入校验失败");
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
                        log_i(url, "HTTP add_task 任务运行中（幂等继续）");
                        return dw_submit_result_t::success();
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
                log_i(url, "回收残留上下文后重新添加");
            }
        }

        /* 创建目录 */
        if (auto dir_path = std::filesystem::path(save_path);
            !dir_path.empty() && !mkdir_recursive(dir_path.string())) {
            log_e(url, "mkdir_recursive failed: dir={}", dir_path.string());
            return dw_submit_result_t::failure(DW_REASON_ERROR, "目录创建失败");
        }

        const bool has_resume = !params->resume_data.empty();

        auto tCtx_guard = task_create_new(url, save_path.c_str());
        if (!tCtx_guard) {
            return dw_submit_result_t::failure(DW_REASON_ERROR, "task_create_new failed");
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
                    he::internal::deserialize_resume(params->resume_data.data(), params->resume_data.size());
            // 存档损坏在此同步检出（与下方文件不可用同为回退全量重下，仅补可观测性）。
            if (!rd.ok) {
                log_i(url, "resume 存档无效（格式损坏），回退全量重下（沿用原名）: size={}", params->resume_data.size());
            }
            // 续传回落同一落盘文件：使用 resume 持久化的物理路径。
            // 旧格式无 path 时 full_path 为空，走全量重下路径。
            std::string full_path = rd.full_file_path;
            // 局部验证打开既有文件（不创建，即开即关；写入由各分片句柄承担）：
            // 文件被外部删除/不可写时打开失败，丢弃续传进度回退全量重下，
            // 避免静默重建空稀疏文件在已下区间留洞。
            if (rd.ok && !full_path.empty()) {
                if (DwFile probe; !probe.open(full_path, false)) {
                    log_i(url, "续传文件不可用（errno={}），回退全量重下: {}", errno, full_path);
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
            return dw_submit_result_t::failure(DW_REASON_ERROR, "g_tasks.emplace failed");
        }

        start_task(tCtx);

        log_i(tCtx->url.c_str(), "HTTP add_task 成功: output={} probing={}", tCtx->output_path, tCtx->probing);
        return dw_submit_result_t::success();
    }

    void HttpEngine::resume_task(const std::string &natural_key,
                                 const std::string &client_id,
                                 const std::vector<int32_t> &priority_file_indexes) {
        (void)priority_file_indexes;
        if (natural_key.empty() || client_id.empty() || !he::g_task_manager) {
            return;
        }
        // HTTP 无句柄概念，恢复即重新添加（幂等：已存在则继续运行）。
        // save_path 从 file_record 获取，resume_data 从 resume_info 获取。
        auto *file_record = he::g_task_manager->load_task_record(client_id, DW_PROTOCOL_HTTP, natural_key);
        if (!file_record || file_record->save_path.empty()) {
            return;
        }
        const auto resume_info = he::g_task_manager->load_resume_info(client_id, DW_PROTOCOL_HTTP, natural_key);
        TaskParams p{};
        p.client_id = client_id;
        p.save_path = file_record->save_path;
        p.resume_data = resume_info.data;
        p.url = natural_key;
        add_task(&p);
    }

    dw_submit_result_t HttpEngine::pause_task(const std::string &id,
                                                const std::string &client_id) {
        (void)client_id; // HTTP 任务表按 url 全局索引，client_id 不使用（接口一致性保留）
        if (id.empty()) {
            return dw_submit_result_t::failure(DW_REASON_ERROR, "入参为空");
        }
        log_d(id.c_str(), "HTTP pause_task 进入");

        if (!ensure_running()) {
            return dw_submit_result_t::failure(DW_REASON_ERROR, "ensure_running failed");
        }

        try {
            {
                std::lock_guard<std::mutex> lk(he::g_map_mtx);
                if (const auto it = he::g_tasks.find(id); it != he::g_tasks.end()) {
                    it->second->pause_req.store(1);
                    log_i(id.c_str(), "HTTP pause_task 成功（非销毁，待 sweep 回收 ctx）");
                }
            }
            return dw_submit_result_t::success();
        } catch (const std::exception &e) {
            log_e("", "HTTP pause_task exception: {}", e.what());
            return dw_submit_result_t::failure(DW_REASON_ERROR, e.what());
        }
    }

    dw_submit_result_t HttpEngine::delete_task(const std::string &id,
                                                const std::string &client_id,
                                                const int32_t /*delete_files*/) {
        (void)client_id; // HTTP 任务表按 url 全局索引，client_id 不使用（接口一致性保留）
        if (id.empty()) {
            return dw_submit_result_t::failure(DW_REASON_ERROR, "入参为空");
        }
        log_d(id.c_str(), "HTTP delete_task");

        if (!ensure_running()) {
            return dw_submit_result_t::failure(DW_REASON_ERROR, "ensure_running failed");
        }

        // 置取消 + 删除标志，sweep 回收后发 DELETED 事件。
        try {
            bool hit = false;
            {
                std::lock_guard<std::mutex> lk(he::g_map_mtx);
                if (const auto it = he::g_tasks.find(id); it != he::g_tasks.end()) {
                    it->second->cancel_req.store(1);
                    it->second->delete_req.store(1);
                    hit = true;
                }
            }
            if (hit) {
                log_i(id.c_str(), "HTTP delete_task 已标记（待 sweep 回收发 DELETED）");
                return dw_submit_result_t::success();
            }
            // 未持有任务：直接发 DELETED 事件，wrapper 据此回收资源 + 删文件。
            log_i(id.c_str(), "HTTP delete_task 任务不在引擎，直接发 DELETED");
            if (he::g_task_manager) {
                EngineEvent ev;
                ev.type = EngineEventType::DELETED;
                ev.engine_key = id;
                ev.protocol = DW_PROTOCOL_HTTP;
                ev.delete_files = 1; // HTTP 引擎不直接删文件，由 wrapper 处理
                he::g_task_manager->on_engine_event(ev);
            }
            return dw_submit_result_t::success();
        } catch (const std::exception &e) {
            log_e("", "HTTP delete_task exception: {}", e.what());
            return dw_submit_result_t::failure(DW_REASON_ERROR, e.what());
        }
    }

    bool HttpEngine::task_released(const std::string &id) {
        if (id.empty()) return true;
        // 引擎停止 / 未初始化：destroy 已 join 全部线程并析构 ctx，视为已释放。
        if (!initialized_ || !he::g_running.load()) return true;
        // ctx 仍在 map（含删除中待 sweep 回收）即持有线程 / 分片文件句柄，未释放；
        // sweep 移出并析构后（文件全关）方可安全删除落盘文件。
        std::lock_guard<std::mutex> lk(he::g_map_mtx);
        return he::g_tasks.find(id) == he::g_tasks.end();
    }

    std::vector<dw_byte_range_t> HttpEngine::get_file_ranges(const std::string &id, int32_t /*file_index*/) {
        // HTTP 单文件模型：file_index 忽略（签名与接口统一）。
        std::vector<dw_byte_range_t> ranges;
        if (id.empty()) return ranges;
        // 收集各 part 已下载区间 [start, start+done-1]（仅 done>0），随后排序合并。
        {
            std::lock_guard<std::mutex> lk(he::g_map_mtx);
            const auto it = he::g_tasks.find(id);
            if (it == he::g_tasks.end() || !it->second) return ranges;
            dl_task_ctx *tCtx = it->second.get();
            std::lock_guard<std::mutex> slk(tCtx->speed_mtx);
            for (const auto &part: tCtx->parts) {
                if (part.done > 0) {
                    ranges.push_back({part.start, part.start + part.done - 1});
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
            bool deleting = false;
            {
                std::lock_guard<std::mutex> lk(he::g_map_mtx);
                const auto it = he::g_tasks.find(url);
                if (it == he::g_tasks.end()) continue;
                owned = std::move(it->second); // 移出 map，脱离全局可见
                he::g_tasks.erase(it);
                deleting = owned->delete_req.load() == 1;
            }
            // 锁外 join 已结束的线程并析构 ctx（触发 curl/文件句柄释放）
            if (owned->task_thread.joinable()) owned->task_thread.join();
            owned.reset(); // 显式析构关闭全部分片文件句柄
            log_i(url.c_str(),
                  deleting ? "删除回收 HTTP 上下文" : "终态回收 HTTP 上下文");
            // 删除中任务：回收完成后发 DELETED 事件，wrapper 据此回收资源 + 删文件。
            if (deleting) {
                if (he::g_task_manager) {
                    he::g_task_manager->on_engine_event(EngineEvent{
                        .type = EngineEventType::DELETED,
                        .engine_key = url,
                        .protocol = DW_PROTOCOL_HTTP,
                        .delete_files = 1 // HTTP 引擎不直接删文件，由 wrapper 处理
                    });
                }
            }
        }
    }
} // namespace dw
