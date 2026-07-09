/**
 * @file http_engine_impl.cpp
 * @brief HTTP 下载引擎核心实现：多线程分片下载。
 *
 * 架构：
 *   - 每分片 std::thread + curl_easy_perform（真正并行）
 *   - 每任务一个 task thread：探测 → 创建分片 → spawn 分片线程 → join → 终态
 *   - 一个全局 monitor thread：周期性推送进度
 *   - 回调中共享字段用 mutex / atomic 保护
 */

#include "http/http_engine_internal.h"

#include "internal/downloader_internal.h"

#include <filesystem>

/* ===================== 全局变量定义 ===================== */
namespace dw { namespace http_engine {

dw_config_t g_cfg{};
std::mutex g_map_mtx;
std::unordered_map<std::string, std::unique_ptr<dl_task_ctx>> g_tasks;
std::thread g_monitor_thread;
std::atomic<bool> g_exit_flag{false};
std::atomic<bool> g_running{false};
dw_progress_cb g_progress_cb = nullptr;
dw_log_cb g_log_cb = nullptr;

namespace internal {

    static constexpr int64_t DEFAULT_PART_SIZE = 1 * 1024 * 1024; /* 1 MiB */

    int64_t now_unix_ms() {
        using namespace std::chrono;
        return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    }

    static constexpr const char *log_level_name(int lvl) {
        switch (lvl) {
            case DW_LOG_DEBUG: return "DEBUG";
            case DW_LOG_ERROR: return "ERROR";
            default: return "INFO ";
        }
    }

    void dl_emit_log(const char *trace_id, const char *msg,
                     const char *file, const int line, const char *func, const int lvl) {
        const dw_log_cb cb = g_log_cb;
        const int64_t ts = now_unix_ms();

        if (cb) {
            /* 回调就绪：func/line 作为独立参数传递，不再拼入 message */
            cb(static_cast<dw_log_level_t>(lvl), msg,
               trace_id ? trace_id : "",
               func ? func : "", line, ts);
        } else {
            /* 回调未就绪，fallback 到 stderr：格式化全部信息 */
            char ts_buf[32];
            {
                const auto secs = static_cast<time_t>(ts / 1000);
                struct tm tm_buf{};
                localtime_r(&secs, &tm_buf);
                std::strftime(ts_buf, sizeof(ts_buf), "%Y-%m-%d %H:%M:%S", &tm_buf);
            }
            const std::string base_name = file ? std::filesystem::path(file).filename().string() : std::string{};
            const char *base = base_name.empty() ? "?" : base_name.c_str();
            std::fprintf(stderr, "[%s] [%s] %s:%d (%s): %s\n",
                         ts_buf, log_level_name(lvl),
                         base, line, func ? func : "?", msg);
        }
    }

    void dl_logf(const int lvl, const char *trace_id,
                 const char *file, const int line, const char *func,
                 const char *fmt, ...) {
        if (lvl < g_cfg.log_level) return;
        va_list ap1, ap2;
        va_start(ap1, fmt);
        va_copy(ap2, ap1);
        const int n = std::vsnprintf(nullptr, 0, fmt, ap1);
        va_end(ap1);
        if (n < 0) {
            va_end(ap2);
            return;
        }
        try {
            std::vector<char> buf(static_cast<size_t>(n) + 1);
            std::vsnprintf(buf.data(), buf.size(), fmt, ap2);
            va_end(ap2);
            dl_emit_log(trace_id, buf.data(), file, line, func, lvl);
        } catch (...) { va_end(ap2); }
    }

    /* ---------- 字符串工具 ---------- */

    bool equal_ignore_case(std::string_view a, std::string_view b) noexcept {
        return std::ranges::equal(a, b, [](char x, char y) noexcept {
            return std::tolower(static_cast<unsigned char>(x))
                   == std::tolower(static_cast<unsigned char>(y));
        });
    }

    std::string_view trim_view(std::string_view s) noexcept {
        constexpr std::string_view ws = " \t\r\n\v\f";
        const auto b = s.find_first_not_of(ws);
        if (b == std::string_view::npos) return {};
        return s.substr(b, s.find_last_not_of(ws) - b + 1);
    }

    template<typename T>
    bool sv_to_int(std::string_view sv, T &out) noexcept {
        sv = trim_view(sv);
        if (sv.empty()) return false;
        return std::from_chars(sv.data(), sv.data() + sv.size(), out).ec == std::errc{};
    }

    std::string parse_content_disposition_filename(const std::string_view value) {
        if (const auto pos_star = value.find("filename*="); pos_star != std::string_view::npos) {
            auto encoded = value.substr(pos_star + 10);
            encoded = trim_view(encoded);
            if (const auto q = encoded.find('\''); q != std::string_view::npos) {
                if (const auto q2 = encoded.find('\'', q + 1); q2 != std::string_view::npos)
                    encoded = encoded.substr(q2 + 1);
            }
            if (encoded.size() >= 2 && encoded.front() == '"' && encoded.back() == '"')
                encoded = encoded.substr(1, encoded.size() - 2);
            return std::string(encoded);
        }
        if (const auto pos = value.find("filename="); pos != std::string_view::npos) {
            auto filename = value.substr(pos + 9);
            filename = trim_view(filename);
            if (filename.size() >= 2 && filename.front() == '"' && filename.back() == '"')
                filename = filename.substr(1, filename.size() - 2);
            return std::string(filename);
        }
        return {};
    }

    std::string extract_filename_from_url(std::string_view url) {
        if (const auto hash = url.find('#'); hash != std::string_view::npos) {
            url = url.substr(0, hash);
        }
        if (const auto query = url.find('?'); query != std::string_view::npos) {
            url = url.substr(0, query);
        }
        if (const auto slash = url.find_last_of('/'); slash != std::string_view::npos) {
            if (const auto filename = url.substr(slash + 1); !filename.empty()) {
                return std::string(filename);
            }
        }
        return {};
    }

    std::string resolve_unique_path(const std::string &dir, const std::string &filename) {
        if (filename.empty()) return {};
        if (std::string full_path = (std::filesystem::path(dir) / filename).string(); !
            std::filesystem::exists(full_path)) {
            return full_path;
        }
        const std::filesystem::path p(filename);
        const std::string base = p.stem().string();
        const std::string ext = p.extension().string();
        for (int n = 1; n <= 9999; ++n) {
            const std::string candidate = (std::filesystem::path(dir) / (base + " (" + std::to_string(n) + ")" + ext)).
                    string();
            if (!std::filesystem::exists(candidate)) {
                return candidate;
            }
        }
        return base + "_" + std::to_string(now_unix_ms()) + ext;
    }

    bool mkdir_recursive(const std::string &path) {
        const std::filesystem::path fs_path(path);
        if (fs_path.empty()) {
            return false;
        }
        if (std::filesystem::exists(fs_path)) {
            return true;
        }
        std::error_code ec;
        return std::filesystem::create_directories(fs_path, ec);
    }

    /* =====================================================================
     *                     Part 2: 进度推送 + 回调
     * ===================================================================== */

    void fill_progress(dl_task_ctx *tCtx, dw_progress_t *task_progress) {
        std::memset(task_progress, 0, sizeof(*task_progress));
        task_progress->task_id = tCtx->url.c_str();
        task_progress->trace_id = tCtx->trace_id.c_str();
        task_progress->protocol = DW_PROTOCOL_HTTP;
        task_progress->name = tCtx->filename.c_str();
        task_progress->output_path = tCtx->output_path.c_str();
        task_progress->filename = tCtx->filename.c_str();
        task_progress->total_size = tCtx->total_size;

        int64_t total_downloaded = 0;
        double total_speed = 0.0;
        {
            std::lock_guard<std::mutex> lk(tCtx->speed_mtx);
            for (const auto &part: tCtx->parts) {
                total_downloaded += part.done;
                total_speed += part.download_rate;
            }
        }

        task_progress->total_done = total_downloaded;
        task_progress->remaining = (tCtx->total_size > 0) ? (tCtx->total_size - total_downloaded) : -1;
        task_progress->progress = (tCtx->total_size > 0)
                                     ? static_cast<double>(total_downloaded) / static_cast<double>(tCtx->total_size)
                                     : -1.0;
        task_progress->download_rate = total_speed;
        task_progress->eta = (total_speed > 0.0 && tCtx->total_size > 0 && task_progress->remaining >= 0)
                                 ? static_cast<double>(task_progress->remaining) / total_speed
                                 : -1.0;
        task_progress->task_status = tCtx->status;
        task_progress->support_range = tCtx->support_range;
        task_progress->etag = tCtx->etag.c_str();
        task_progress->last_modified = tCtx->last_modified.c_str();
        task_progress->probing = tCtx->probing;
        task_progress->saved_at_unix_ms = now_unix_ms();
        task_progress->reason = tCtx->reason;
        task_progress->message = tCtx->message.c_str();
        task_progress->part_states = tCtx->parts.empty()
                                         ? nullptr
                                         : const_cast<dw_part_state_t *>(tCtx->parts.data());
        task_progress->part_count = static_cast<int32_t>(tCtx->parts.size());
    }

    void push_progress(dl_task_ctx *tCtx) {
        if (!tCtx) return;
        dw_progress_t snap;
        bool should_push = false;
        try {
            std::lock_guard<std::mutex> lk(g_map_mtx);
            if (const auto it = g_tasks.find(tCtx->url); it != g_tasks.end() && it->second.get() == tCtx) {
                fill_progress(tCtx, &snap);
                should_push = true;
            }
        } catch (...) { return; }
        if (should_push) {
            try {
                if (const dw_progress_cb cb = g_progress_cb) {
                    cb(&snap);
                }
            } catch (...) {
            }
        }
    }

    int part_progress_cb(void *userdata, curl_off_t /*dl_total*/, const curl_off_t dl_now,
                         curl_off_t /*ul_total*/, curl_off_t /*ul_now*/) {
        auto *pCtx = static_cast<dl_part_ctx *>(userdata);
        if (!pCtx || !pCtx->task) return 0;
        auto *tCtx = pCtx->task;

        if (tCtx->cancel_req.load() || tCtx->pause_req.load()) return 1;

        const int64_t now_ms = now_unix_ms();

        if (pCtx->last_speed_sample_ms > 0) {
            if (const int64_t delta_ms = now_ms - pCtx->last_speed_sample_ms; delta_ms > 0) {
                const double part_instant = static_cast<double>(dl_now - pCtx->last_speed_sample_bytes)
                                            * 1000.0 / static_cast<double>(delta_ms);
                std::lock_guard<std::mutex> lk(tCtx->speed_mtx);
                auto &ps = tCtx->parts[pCtx->index].download_rate;
                ps = (ps > 0.0) ? (0.3 * part_instant + 0.7 * ps) : part_instant;
            }
        }
        pCtx->last_speed_sample_bytes = dl_now;
        pCtx->last_speed_sample_ms = now_ms;

        return 0;
    }

    size_t header_cb(const char *buffer, size_t size, size_t n_items, void *userdata) {
        auto *pCtx = static_cast<dl_part_ctx *>(userdata);
        auto *tCtx = pCtx->task;
        const size_t len = size * n_items;
        if (tCtx->status == DW_TASK_STATUS_ERROR || tCtx->cancel_req.load() || len == 0) return 0;

        try {
            auto mark_drift_error = [&]() -> size_t {
                std::lock_guard<std::mutex> lk(tCtx->speed_mtx);
                tCtx->parts[pCtx->index].status = DW_TASK_STATUS_ERROR;
                tCtx->parts[pCtx->index].reason = DW_REASON_INVALID_INPUT;
                tCtx->parts[pCtx->index].download_rate = 0.0;
                tCtx->status = DW_TASK_STATUS_ERROR;
                tCtx->reason = DW_REASON_INVALID_INPUT;
                tCtx->message = "文件已变更，需重新下载";
                return 0;
            };

            std::string_view raw(buffer, len);
            while (!raw.empty() && (raw.back() == '\r' || raw.back() == '\n')) {
                raw.remove_suffix(1);
            }
            if (raw.empty()) {
                return len;
            }

            HTTP_LOG(DW_LOG_DEBUG, tCtx->trace_id.c_str(), "[part %d] header: %.*s",
                pCtx->index, static_cast<int>(raw.size()), raw.data());

            if (raw.starts_with("HTTP/")) {
                if (const auto sp = raw.find(' '); sp != std::string_view::npos) {
                    if (long code = 0; sv_to_int(raw.substr(sp + 1), code)) {
                        pCtx->seen_http_code = code;
                        if (tCtx->support_range
                            && static_cast<int32_t>(tCtx->parts.size()) > 1
                            && code == 200) {
                            HTTP_LOG(DW_LOG_ERROR, tCtx->trace_id.c_str(),
                                "[part %d] drift: expected 206, got http_code=%ld (multi-part task)",
                                pCtx->index, code);
                            return mark_drift_error();
                        }
                    }
                }
                return len;
            }

            const auto colon = raw.find(':');
            if (colon == std::string_view::npos) {
                return len;
            }
            const auto name = trim_view(raw.substr(0, colon));
            const auto val = trim_view(raw.substr(colon + 1));

            if (equal_ignore_case(name, "Content-Range")) {
                if (const auto slash = val.find('/'); slash != std::string_view::npos) {
                    if (const auto total_sv = val.substr(slash + 1); !total_sv.empty() && total_sv.front() != '*') {
                        if (int64_t total = 0; sv_to_int(total_sv, total) && total > 0) {
                            pCtx->seen_total_size = total;
                            if (tCtx->total_size <= 0) {
                                tCtx->total_size = total;
                            }
                            if (tCtx->total_size > 0 && total != tCtx->total_size) {
                                HTTP_LOG(DW_LOG_ERROR, tCtx->trace_id.c_str(),
                                    "[part %d] drift: Content-Range total=%lld, expected=%lld",
                                    pCtx->index, static_cast<long long>(total),
                                    static_cast<long long>(tCtx->total_size));
                                return mark_drift_error();
                            }
                        }
                    }
                }
            } else if (equal_ignore_case(name, "Content-Length")) {
                if (int64_t cl = 0; sv_to_int(val, cl) && cl > 0 && pCtx->seen_total_size <= 0 && tCtx->probing) {
                    pCtx->seen_total_size = cl;
                }
            } else if (equal_ignore_case(name, "Content-Disposition")) {
                if (tCtx->filename.empty()) {
                    auto parsed = parse_content_disposition_filename(val);
                    if (!parsed.empty()) tCtx->filename = parsed;
                }
            } else if (equal_ignore_case(name, "ETag")) {
                pCtx->seen_etag.assign(val);
                if (tCtx->etag.empty()) tCtx->etag = pCtx->seen_etag;
                if (!tCtx->etag.empty() && pCtx->seen_etag != tCtx->etag) {
                    HTTP_LOG(DW_LOG_ERROR, tCtx->trace_id.c_str(),
                        "[part %d] drift: ETag changed, expected=\"%s\", got=\"%s\"",
                        pCtx->index, tCtx->etag.c_str(), pCtx->seen_etag.c_str());
                    return mark_drift_error();
                }
            } else if (equal_ignore_case(name, "Last-Modified")) {
                pCtx->seen_last_modified.assign(val);
                if (tCtx->last_modified.empty()) tCtx->last_modified = pCtx->seen_last_modified;
                if (!tCtx->last_modified.empty() && pCtx->seen_last_modified != tCtx->last_modified) {
                    HTTP_LOG(DW_LOG_ERROR, tCtx->trace_id.c_str(),
                        "[part %d] drift: Last-Modified changed, expected=\"%s\", got=\"%s\"",
                        pCtx->index, tCtx->last_modified.c_str(), pCtx->seen_last_modified.c_str());
                    return mark_drift_error();
                }
            }
            return len;
        } catch (...) {
            HTTP_LOG(DW_LOG_ERROR, tCtx->trace_id.c_str(), "[part %d] header_cb exception", pCtx->index);
            return 0;
        }
    }

    size_t write_cb(const char *ptr, size_t size, size_t n_member, void *userdata) {
        auto *pCtx = static_cast<dl_part_ctx *>(userdata);
        auto *tCtx = pCtx->task;
        const size_t len = size * n_member;
        if (tCtx->status == DW_TASK_STATUS_ERROR || tCtx->cancel_req.load() || tCtx->pause_req.load() || len == 0) {
            return 0;
        }

        if (tCtx->probing) {
            if (pCtx->seen_http_code == 206) {
                return len;
            }
            tCtx->probing = 0;
            tCtx->support_range = 0;
            finalize_probing(tCtx, pCtx);
        }

        auto &part = tCtx->parts[pCtx->index];
        const auto off = static_cast<off_t>(part.start + part.done);
        const ssize_t w = pwrite(tCtx->fd, ptr, len, off);
        if (w < 0 || static_cast<size_t>(w) != len) {
            HTTP_LOG(DW_LOG_ERROR, tCtx->trace_id.c_str(), "[part %d] pwrite failed: wanted=%zu got=%zd errno=%d",
                pCtx->index, len, w, errno);
            std::lock_guard<std::mutex> lk(tCtx->speed_mtx);
            part.status = DW_TASK_STATUS_ERROR;
            part.reason = DW_REASON_INTERNAL;
            return 0;
        }
        {
            std::lock_guard<std::mutex> lk(tCtx->speed_mtx);
            part.done += static_cast<int64_t>(w);
            part.progress = (part.size > 0)
                               ? static_cast<double>(part.done) * 100.0 / static_cast<double>(part.size)
                               : -1.0;
        }
        return len;
    }

    /* =====================================================================
     *                     Part 3: 构造 + 分类 + 聚合
     * ===================================================================== */

    CURL *build_easy_for_part(dl_task_ctx *tCtx, dl_part_ctx *pCtx) {
        CurlEasyGuard easy_guard(curl_easy_init());
        if (!easy_guard) return nullptr;
        auto &part = tCtx->parts[pCtx->index];

        pCtx->seen_total_size = 0;
        pCtx->seen_etag.clear();
        pCtx->seen_last_modified.clear();
        pCtx->seen_http_code = 0;

        CURL *curl = easy_guard.get();
        curl_easy_setopt(curl, CURLOPT_URL, tCtx->url.c_str());
        curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
        curl_easy_setopt(curl, CURLOPT_MAXREDIRS, static_cast<long>(g_cfg.max_redirect));
        curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, static_cast<long>(g_cfg.connect_timeout_seconds));
        if (g_cfg.request_timeout_seconds > 0)
            curl_easy_setopt(curl, CURLOPT_TIMEOUT, static_cast<long>(g_cfg.request_timeout_seconds));
        if (g_cfg.low_speed_limit_bps > 0) {
            curl_easy_setopt(curl, CURLOPT_LOW_SPEED_LIMIT, static_cast<long>(g_cfg.low_speed_limit_bps));
            curl_easy_setopt(curl, CURLOPT_LOW_SPEED_TIME, static_cast<long>(g_cfg.low_speed_time));
        }
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, g_cfg.verify_ssl ? 1L : 0L);
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, g_cfg.verify_ssl ? 2L : 0L);
        if (g_cfg.ca_bundle && *g_cfg.ca_bundle)
            curl_easy_setopt(curl, CURLOPT_CAINFO, g_cfg.ca_bundle);
        if (g_cfg.proxy && *g_cfg.proxy) {
            curl_easy_setopt(curl, CURLOPT_PROXY, g_cfg.proxy);
            if (g_cfg.proxy_username && *g_cfg.proxy_username)
                curl_easy_setopt(curl, CURLOPT_PROXYUSERNAME, g_cfg.proxy_username);
            if (g_cfg.proxy_password && *g_cfg.proxy_password)
                curl_easy_setopt(curl, CURLOPT_PROXYPASSWORD, g_cfg.proxy_password);
        }
        curl_easy_setopt(curl, CURLOPT_USERAGENT,
                         (g_cfg.user_agent && *g_cfg.user_agent) ? g_cfg.user_agent : "download_wrapper/2.0");
        curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);

        int64_t restart = part.start + part.done;
        int64_t rend = part.end;
        std::string range_hdr;
        if (tCtx->probing) {
        {
            char buf[64];
            std::snprintf(buf, sizeof(buf), "%lld-%lld", (long long)restart, (long long)restart);
            range_hdr = buf;
        }
        } else if (rend >= restart) {
        {
            char buf[64];
            std::snprintf(buf, sizeof(buf), "%lld-%lld", (long long)restart, (long long)rend);
            range_hdr = buf;
        }
        } else {
        {
            char buf[64];
            std::snprintf(buf, sizeof(buf), "%lld-", (long long)restart);
            range_hdr = buf;
        }
        }
        curl_easy_setopt(curl, CURLOPT_RANGE, range_hdr.c_str());

        CurlSlistGuard hdrs_guard;
        if (!tCtx->probing && (!tCtx->etag.empty() || !tCtx->last_modified.empty())) {
            const std::string ifr = "If-Range: " + (tCtx->etag.empty() ? tCtx->last_modified : tCtx->etag);
            hdrs_guard.append(ifr.c_str());
        }
        if (hdrs_guard)
            curl_easy_setopt(curl, CURLOPT_HTTPHEADER, hdrs_guard.get());

        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_cb);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, pCtx);
        curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, header_cb);
        curl_easy_setopt(curl, CURLOPT_HEADERDATA, pCtx);
        curl_easy_setopt(curl, CURLOPT_PRIVATE, pCtx);
        curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 0L);
        curl_easy_setopt(curl, CURLOPT_XFERINFOFUNCTION, part_progress_cb);
        curl_easy_setopt(curl, CURLOPT_XFERINFODATA, pCtx);

        pCtx->easy_hdrs = hdrs_guard.release();
        return easy_guard.release();
    }

    dw_reason_t classify_failure(CURLcode rc, long http_code, int *retryable) {
        *retryable = 0;
        if (rc == CURLE_OK) {
            if (http_code >= 500 && http_code <= 599) {
                *retryable = 1;
                return DW_REASON_NETWORK;
            }
            if (http_code == 408 || http_code == 429) {
                *retryable = 1;
                return DW_REASON_NETWORK;
            }
            if (http_code == 401 || http_code == 403) return DW_REASON_AUTH;
            if (http_code == 416 || http_code == 400 || http_code == 404 || http_code == 410)
                return DW_REASON_INVALID_INPUT;
            return DW_REASON_INTERNAL;
        }
        switch (rc) {
            case CURLE_COULDNT_RESOLVE_HOST:
            case CURLE_COULDNT_RESOLVE_PROXY:
            case CURLE_COULDNT_CONNECT:
            case CURLE_OPERATION_TIMEDOUT:
            case CURLE_RECV_ERROR:
            case CURLE_SEND_ERROR:
            case CURLE_PARTIAL_FILE:
            case CURLE_GOT_NOTHING:
            case CURLE_SSL_CONNECT_ERROR:
            case CURLE_PEER_FAILED_VERIFICATION:
                *retryable = 1;
                return DW_REASON_NETWORK;
            case CURLE_WRITE_ERROR: return DW_REASON_INTERNAL;
            default: return DW_REASON_INTERNAL;
        }
    }

    void aggregate_status(dl_task_ctx *tCtx) {
        if (tCtx->status == DW_TASK_STATUS_ERROR && tCtx->reason == DW_REASON_INVALID_INPUT) return;
        int has_downloading = 0, has_error = 0, all_completed = 1;
        dw_reason_t best = DW_REASON_NONE;
        int best_rank = -1;
        for (auto &part: tCtx->parts) {
            switch (part.status) {
                case DW_TASK_STATUS_DOWNLOADING: has_downloading = 1;
                    all_completed = 0;
                    break;
                case DW_TASK_STATUS_ERROR: {
                    has_error = 1;
                    all_completed = 0;
                    int rank;
                    switch (part.reason) {
                        case DW_REASON_AUTH: rank = 4; break;
                        case DW_REASON_INVALID_INPUT: rank = 3; break;
                        case DW_REASON_INTERNAL: rank = 2; break;
                        case DW_REASON_NETWORK: rank = 1; break;
                        default: rank = 0; break;
                    }
                    if (rank > best_rank) {
                        best_rank = rank;
                        best = part.reason;
                    }
                    break;
                }
                case DW_TASK_STATUS_COMPLETED: break;
                default: all_completed = 0; break;
            }
        }
        if (has_downloading) {
            tCtx->status = DW_TASK_STATUS_DOWNLOADING;
            tCtx->reason = DW_REASON_NONE;
        } else if (all_completed) {
            tCtx->status = DW_TASK_STATUS_COMPLETED;
            tCtx->reason = DW_REASON_NONE;
        } else if (has_error) {
            tCtx->status = DW_TASK_STATUS_ERROR;
            tCtx->reason = best;
        }
    }


    /* =====================================================================
     *                     Part 4: 探测 + 元数据
     * ===================================================================== */

    void finalize_probing(dl_task_ctx *tCtx, const dl_part_ctx *pCtx) {
        const long code = pCtx->seen_http_code;
        tCtx->probing = 0;

        if (tCtx->total_size <= 0 && pCtx->seen_total_size > 0) {
            tCtx->total_size = pCtx->seen_total_size;
        }

        if (tCtx->filename.empty()) {
            tCtx->filename = extract_filename_from_url(tCtx->url);
        }

        if (!tCtx->filename.empty() && tCtx->output_path.empty())
            tCtx->full_file_path = resolve_unique_path(".", tCtx->filename);
        else if (!tCtx->filename.empty() && !tCtx->output_path.empty())
            tCtx->full_file_path = resolve_unique_path(tCtx->output_path, tCtx->filename);

        if (tCtx->fd >= 0) {
            ::close(tCtx->fd);
            tCtx->fd = -1;
        }

        if (!tCtx->full_file_path.empty()) {
            if (const auto dir_path = std::filesystem::path(tCtx->full_file_path).parent_path();
                !dir_path.empty() && !mkdir_recursive(dir_path.string())) {
                HTTP_LOG(DW_LOG_ERROR, tCtx->trace_id.c_str(), "mkdir failed: %s", dir_path.string().c_str());
                tCtx->status = DW_TASK_STATUS_ERROR;
                tCtx->reason = DW_REASON_INTERNAL;
                tCtx->message = "目录创建失败";
                return;
            }
            tCtx->fd = ::open(tCtx->full_file_path.c_str(), O_CREAT | O_WRONLY, 0644);
            if (tCtx->fd < 0) {
                HTTP_LOG(DW_LOG_ERROR, tCtx->trace_id.c_str(), "open failed: %s errno=%d",
                    tCtx->full_file_path.c_str(), errno);
                tCtx->status = DW_TASK_STATUS_ERROR;
                tCtx->reason = DW_REASON_INTERNAL;
                tCtx->message = (errno == ENOSPC) ? "存储空间不足" : "存储异常，无法保存文件";
                return;
            }
        }

        if (tCtx->total_size > 0 && tCtx->fd >= 0 &&
            ftruncate(tCtx->fd, static_cast<off_t>(tCtx->total_size)) != 0) {
            HTTP_LOG(DW_LOG_ERROR, tCtx->trace_id.c_str(), "ftruncate failed: size=%lld errno=%d",
                (long long)tCtx->total_size, errno);
            tCtx->status = DW_TASK_STATUS_ERROR;
            tCtx->reason = DW_REASON_INTERNAL;
            tCtx->message = (errno == ENOSPC) ? "存储空间不足" : "存储异常，无法保存文件";
            return;
        }

        if (code == 206) {
            tCtx->support_range = 1;
            int32_t want_parts = 1;
            if (tCtx->total_size >= g_cfg.min_size_for_split && g_cfg.default_parts > 1)
                want_parts = g_cfg.default_parts;
            const int64_t chunk = (tCtx->total_size > 0) ? (tCtx->total_size / want_parts) : 0;

            tCtx->parts.reserve(static_cast<size_t>(want_parts));
            tCtx->part_ctx.reserve(static_cast<size_t>(want_parts));

            tCtx->parts[0].index = 0;
            tCtx->parts[0].start = 0;
            tCtx->parts[0].end = (want_parts > 1) ? (chunk - 1) : (tCtx->total_size > 0 ? tCtx->total_size - 1 : -1);
            tCtx->parts[0].size = (tCtx->parts[0].end >= 0) ? (tCtx->parts[0].end + 1) : 0;
            tCtx->parts[0].done = 0;
            tCtx->parts[0].progress = 0.0;
            tCtx->parts[0].status = DW_TASK_STATUS_DOWNLOADING;
            tCtx->parts[0].reason = DW_REASON_NONE;

            auto &pCtx0 = tCtx->part_ctx[0];
            pCtx0.seen_total_size = 0;
            pCtx0.seen_etag.clear();
            pCtx0.seen_last_modified.clear();
            pCtx0.seen_http_code = 0;
            pCtx0.retry_count = 0;

            for (int32_t i = 1; i < want_parts; ++i) {
                const int64_t start = chunk * i;
                const int64_t end = (i == want_parts - 1)
                                        ? (tCtx->total_size > 0 ? tCtx->total_size - 1 : -1)
                                        : (chunk * (i + 1) - 1);
                dw_part_state_t part{};
                part.index = i;
                part.start = start;
                part.end = end;
                part.size = (end >= start) ? (end - start + 1) : 0;
                part.status = DW_TASK_STATUS_DOWNLOADING;
                part.reason = DW_REASON_NONE;
                tCtx->parts.push_back(part);
                dl_part_ctx ctx{};
                ctx.task = tCtx;
                ctx.index = i;
                tCtx->part_ctx.push_back(std::move(ctx));
            }
        } else if (code == 200) {
            tCtx->support_range = 0;
            if (!tCtx->parts.empty()) {
                tCtx->parts[0].start = 0;
                tCtx->parts[0].end = (tCtx->total_size > 0) ? (tCtx->total_size - 1) : -1;
                tCtx->parts[0].size = (tCtx->total_size > 0) ? tCtx->total_size : 0;
                tCtx->parts[0].progress = (tCtx->parts[0].size > 0)
                                             ? static_cast<double>(tCtx->parts[0].done) * 100.0 / static_cast<double>(
                                                   tCtx->parts[0].size)
                                             : -1.0;
            }
        }
    }

    /* =====================================================================
     *          Part 5: 分片线程 + 任务线程 + monitor 线程
     * ===================================================================== */

    void part_thread_func(dl_task_ctx *tCtx, dl_part_ctx *pCtx) {
        try {
        while (!tCtx->cancel_req.load() && !tCtx->pause_req.load()) {
            if (tCtx->status == DW_TASK_STATUS_ERROR &&
                tCtx->reason == DW_REASON_INVALID_INPUT)
                break;

            CURL *curl = build_easy_for_part(tCtx, pCtx);
            if (!curl) {
                HTTP_LOG(DW_LOG_ERROR, tCtx->trace_id.c_str(), "[part %d] build_easy_for_part failed", pCtx->index);
                std::lock_guard<std::mutex> lk(tCtx->speed_mtx);
                tCtx->parts[pCtx->index].status = DW_TASK_STATUS_ERROR;
                tCtx->parts[pCtx->index].reason = DW_REASON_INTERNAL;
                tCtx->parts[pCtx->index].download_rate = 0.0;
                break;
            }
            CurlEasyGuard easy_guard(curl);
            pCtx->easy = curl;

            const CURLcode rc = curl_easy_perform(curl);
            long http_code = 0;
            curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

            easy_guard.reset();
            pCtx->easy = nullptr;
            if (pCtx->easy_hdrs) {
                curl_slist_free_all(pCtx->easy_hdrs);
                pCtx->easy_hdrs = nullptr;
            }

            if (tCtx->cancel_req.load() || tCtx->pause_req.load()) break;

            if (rc == CURLE_OK && http_code >= 200 && http_code < 300) {
                std::lock_guard<std::mutex> lk(tCtx->speed_mtx);
                auto &part = tCtx->parts[pCtx->index];
                if (part.done >= part.size && part.size > 0) {
                    part.status = DW_TASK_STATUS_COMPLETED;
                    part.reason = DW_REASON_NONE;
                    part.download_rate = 0.0;
                } else {
                    HTTP_LOG(DW_LOG_INFO, tCtx->trace_id.c_str(),
                        "incomplete: part=%d done=%lld/%lld",
                        pCtx->index, static_cast<long long>(part.done), static_cast<long long>(part.size));
                    if (pCtx->retry_count >= g_cfg.max_retries) {
                        part.status = DW_TASK_STATUS_ERROR;
                        part.reason = DW_REASON_NETWORK;
                        part.download_rate = 0.0;
                    } else {
                        pCtx->retry_count++;
                        HTTP_LOG(DW_LOG_INFO, tCtx->trace_id.c_str(),
                            "retry: part=%d attempt=%d/%d", pCtx->index, pCtx->retry_count, g_cfg.max_retries);
                        continue;
                    }
                }
                break;
            }

            int retryable = 0;
            const dw_reason_t reason = classify_failure(rc, http_code, &retryable);
            HTTP_LOG(DW_LOG_INFO, tCtx->trace_id.c_str(),
                "failed: part=%d rc=%d http=%ld reason=%d retryable=%d",
                pCtx->index, static_cast<int>(rc), http_code, static_cast<int>(reason), retryable);

            if (retryable && pCtx->retry_count < g_cfg.max_retries) {
                pCtx->retry_count++;
                HTTP_LOG(DW_LOG_INFO, tCtx->trace_id.c_str(),
                    "retry: part=%d attempt=%d/%d", pCtx->index, pCtx->retry_count, g_cfg.max_retries);
                continue;
            }

            {
                std::lock_guard<std::mutex> lk(tCtx->speed_mtx);
                auto &part = tCtx->parts[pCtx->index];
                part.status = DW_TASK_STATUS_ERROR;
                part.reason = reason;
                part.download_rate = 0.0;
                if (tCtx->message.empty()) {
                    switch (reason) {
                        case DW_REASON_AUTH: tCtx->message = "认证失败"; break;
                        case DW_REASON_INVALID_INPUT: tCtx->message = "资源不存在或已失效"; break;
                        case DW_REASON_NETWORK: tCtx->message = "网络连接异常"; break;
                        default: tCtx->message = "下载过程中出现异常"; break;
                    }
                }
            }
            break;
        }
        } catch (const std::exception &e) {
            HTTP_LOG(DW_LOG_ERROR, tCtx->trace_id.c_str(),
                "[part %d] part_thread_func exception: %s", pCtx->index, e.what());
            std::lock_guard<std::mutex> lk(tCtx->speed_mtx);
            tCtx->parts[pCtx->index].status = DW_TASK_STATUS_ERROR;
            tCtx->parts[pCtx->index].reason = DW_REASON_INTERNAL;
            tCtx->parts[pCtx->index].download_rate = 0.0;
        } catch (...) {
            HTTP_LOG(DW_LOG_ERROR, tCtx->trace_id.c_str(),
                "[part %d] part_thread_func unknown exception", pCtx->index);
            std::lock_guard<std::mutex> lk(tCtx->speed_mtx);
            tCtx->parts[pCtx->index].status = DW_TASK_STATUS_ERROR;
            tCtx->parts[pCtx->index].reason = DW_REASON_INTERNAL;
            tCtx->parts[pCtx->index].download_rate = 0.0;
        }
    }

    void task_thread_func(dl_task_ctx *tCtx) {
        try {
        {
            dl_part_ctx probe_ctx{};
            probe_ctx.task = tCtx;
            probe_ctx.index = 0;
            CurlEasyGuard probe_guard(build_easy_for_part(tCtx, &probe_ctx));
            if (probe_guard) {
                probe_ctx.easy = probe_guard.get();
                curl_easy_perform(probe_guard.get());
                long http_code = 0;
                curl_easy_getinfo(probe_guard.get(), CURLINFO_RESPONSE_CODE, &http_code);
                probe_guard.reset();
                probe_ctx.easy = nullptr;

                if (probe_ctx.seen_http_code == 200 || probe_ctx.seen_http_code == 206) {
                    if (probe_ctx.seen_http_code == 206) {
                        finalize_probing(tCtx, &probe_ctx);
                    }
                } else {
                    int retryable = 0;
                    dw_reason_t reason = classify_failure(CURLE_OK, probe_ctx.seen_http_code, &retryable);
                    tCtx->status = DW_TASK_STATUS_ERROR;
                    tCtx->reason = reason;
                    tCtx->message = "探测失败";
                }
            } else {
                tCtx->status = DW_TASK_STATUS_ERROR;
                tCtx->reason = DW_REASON_INTERNAL;
                tCtx->message = "创建请求失败";
            }
        }

        if (tCtx->status == DW_TASK_STATUS_ERROR) {
            push_progress(tCtx);
            return;
        }

        if (tCtx->cancel_req.load() || tCtx->pause_req.load()) return;

        {
            std::vector<std::thread> part_threads;
            part_threads.reserve(tCtx->part_ctx.size());
            for (size_t i = 0; i < tCtx->part_ctx.size(); ++i) {
                part_threads.emplace_back(part_thread_func, tCtx, &tCtx->part_ctx[i]);
            }
            for (auto &t: part_threads) t.join();
        }

        {
            std::lock_guard<std::mutex> lk(tCtx->speed_mtx);
            aggregate_status(tCtx);
        }
        push_progress(tCtx);
        } catch (const std::exception &e) {
            HTTP_LOG(DW_LOG_ERROR, tCtx->trace_id.c_str(), "task_thread_func exception: %s", e.what());
            std::lock_guard<std::mutex> lk(tCtx->speed_mtx);
            tCtx->status = DW_TASK_STATUS_ERROR;
            tCtx->reason = DW_REASON_INTERNAL;
            tCtx->message = "任务线程异常终止";
            push_progress(tCtx);
        } catch (...) {
            HTTP_LOG(DW_LOG_ERROR, tCtx->trace_id.c_str(), "task_thread_func unknown exception");
            std::lock_guard<std::mutex> lk(tCtx->speed_mtx);
            tCtx->status = DW_TASK_STATUS_ERROR;
            tCtx->reason = DW_REASON_INTERNAL;
            tCtx->message = "任务线程异常终止";
            push_progress(tCtx);
        }
    }

    void monitor_thread_func() {
        try {
        while (!g_exit_flag.load()) {
            std::vector<dl_task_ctx *> snap;
            {
                std::lock_guard<std::mutex> lk(g_map_mtx);
                snap.reserve(g_tasks.size());
                for (auto &val: g_tasks | std::views::values) {
                    snap.push_back(val.get());
                }
            }
            for (auto *tCtx: snap) {
                if (g_exit_flag.load()) break;
                push_progress(tCtx);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
        } catch (const std::exception &e) {
            HTTP_LOG(DW_LOG_ERROR, "", "monitor_thread_func exception: %s", e.what());
        } catch (...) {
            HTTP_LOG(DW_LOG_ERROR, "", "monitor_thread_func unknown exception");
        }
    }

    void start_task(dl_task_ctx *tCtx) {
        tCtx->task_thread = std::thread(task_thread_func, tCtx);
    }

    /* =====================================================================
     *          Part 6: 辅助函数
     * ===================================================================== */

    dl_task_ctx *task_create_new(const char *url, const char *output_path,
                                 const char *trace_id, const char *filename) {
        auto *tCtx = new(std::nothrow) dl_task_ctx();
        if (!tCtx) return nullptr;
        tCtx->url = url;
        tCtx->output_path = output_path;
        tCtx->trace_id = trace_id ? trace_id : "";
        tCtx->filename = filename ? filename : "";
        tCtx->total_size = -1;
        tCtx->status = DW_TASK_STATUS_DOWNLOADING;
        tCtx->reason = DW_REASON_NONE;
        tCtx->probing = 1;
        tCtx->fd = -1;
        tCtx->start_time_ms = now_unix_ms();
        tCtx->parts.resize(1);
        tCtx->part_ctx.resize(1);
        auto &part = tCtx->parts[0];
        part.index = 0;
        part.start = 0;
        part.end = -1;
        part.size = 0;
        part.done = 0;
        part.progress = 0.0;
        part.status = DW_TASK_STATUS_DOWNLOADING;
        part.reason = DW_REASON_NONE;
        tCtx->part_ctx[0].task = tCtx;
        tCtx->part_ctx[0].index = 0;
        return tCtx;
    }

    int validate_add_input(const char *url, const char *output_path, const char **err_out) {
        if (!url || !*url) {
            *err_out = "url is empty";
            return 0;
        }
        if (!output_path) {
            *err_out = "output_path is empty";
            return 0;
        }
        const std::string_view path(output_path);
        if (path.empty()) {
            *err_out = "output_path is empty";
            return 0;
        }
        return 1;
    }

    void set_result(dw_submit_result_t *r, const char *task_id, const char *trace_id,
                    dw_reason_t code, const char *msg, const char *fmt, ...) {
        r->task_id = task_id;
        r->code = code;
        if (msg) {
            const size_t n = std::strlen(msg);
            auto p = static_cast<char *>(std::malloc(n + 1));
            if (p) std::memcpy(p, msg, n + 1);
            r->message = p;
        } else { r->message = nullptr; }
        if (code != DW_REASON_NONE && fmt) {
            va_list args;
            va_start(args, fmt);
            char buf[512];
            std::vsnprintf(buf, sizeof(buf), fmt, args);
            va_end(args);
            dl_logf(DW_LOG_ERROR, trace_id ? trace_id : "",
                    __FILE__, __LINE__, __FUNCTION__, "%s", buf);
        }
    }

}}} /* namespace dw::http_engine::internal */
