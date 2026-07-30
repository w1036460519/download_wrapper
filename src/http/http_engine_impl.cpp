/**
 * @file http_engine_impl.cpp
 * @brief HTTP 下载引擎核心实现：多线程分片下载。
 *
 * 架构：
 *   - 每个任务一个 task thread：单线程 curl_multi 事件循环驱动本任务所有分片；
 *     探测即下载——首个连接（探测窗口 Range: 0-n）就是 part 0 的正式下载流，
 *     首笔 body 到达时定名建文件切分片，其余分片动态挂入同一 multi；
 *   - 进度由 TaskManager 采集循环经 query_progress 拉取（无独立监控线程），
 *     resume 由 worker 线程按下载推进门槛自推；
 *   - 回调中共享字段用 mutex / atomic 保护。
 */

#include "http/http_engine_internal.h"

#include "internal/downloader_internal.h"
#include "utils/string_util.h"
#include "utils/time_util.h"

// 续传 BLOB 复用 libtorrent bencode 编解码（与 BT resume 同族格式，项目已链接）。
#include <libtorrent/bdecode.hpp>
#include <libtorrent/bencode.hpp>
#include <libtorrent/entry.hpp>
#include <libtorrent/error_code.hpp>

#include <algorithm>
#include <filesystem>

/* ===================== 全局变量定义 ===================== */
namespace dw {
    namespace http_engine {
        dw_config_t g_cfg{};
        std::mutex g_map_mtx;
        std::unordered_map<std::string, std::unique_ptr<dl_task_ctx> > g_tasks;
        std::atomic<bool> g_exit_flag{false};
        std::atomic<bool> g_running{false};

        namespace internal {
            using dw::utils::format_unix_ms;
            using dw::utils::now_unix_ms;

            /// 探测窗口大小（字节）：探测请求发 "Range: 0-(n-1)" 有界区间，
            /// 收满即由服务器正常断连；剩余区间经续接请求补齐。
            constexpr int64_t kProbeWindowBytes = 1024 * 1024;

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

            std::string percent_decode(std::string_view in) {
                auto hex = [](const char c) -> int {
                    if (c >= '0' && c <= '9') return c - '0';
                    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
                    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
                    return -1;
                };
                std::string out;
                out.reserve(in.size());
                for (size_t i = 0; i < in.size(); ++i) {
                    if (in[i] == '%' && i + 2 < in.size()) {
                        const int hi = hex(in[i + 1]), lo = hex(in[i + 2]);
                        if (hi >= 0 && lo >= 0) {
                            out.push_back(static_cast<char>((hi << 4) | lo));
                            i += 2;
                            continue;
                        }
                    }
                    out.push_back(in[i] == '+' ? ' ' : in[i]);
                }
                return out;
            }

            /// 剥离路径分隔符，仅保留 basename（防 query 参数携路径逃逸落盘目录）。
            std::string sanitize_basename(std::string s) {
                if (const auto pos = s.find_last_of("/\\"); pos != std::string::npos) {
                    s = s.substr(pos + 1);
                }
                return s;
            }

            std::string extract_filename_from_url(std::string_view url) {
                if (const auto hash = url.find('#'); hash != std::string_view::npos) {
                    url = url.substr(0, hash);
                }
                std::string_view query;
                if (const auto q = url.find('?'); q != std::string_view::npos) {
                    query = url.substr(q + 1);
                    url = url.substr(0, q);
                }
                // 优先级：query 参数 filename > query 参数 name > 路径末段（键大小写不敏感，均 percent-decode）。
                std::string by_filename, by_name;
                size_t pos = 0;
                while (pos < query.size()) {
                    const size_t amp = query.find('&', pos);
                    const std::string_view kv = (amp == std::string_view::npos)
                                                    ? query.substr(pos)
                                                    : query.substr(pos, amp - pos);
                    pos = (amp == std::string_view::npos) ? query.size() : amp + 1;
                    const auto eq = kv.find('=');
                    if (eq == std::string_view::npos) continue;
                    const auto key = kv.substr(0, eq);
                    const auto val = kv.substr(eq + 1);
                    if (val.empty()) continue;
                    if (by_filename.empty() && equal_ignore_case(key, "filename")) {
                        by_filename = sanitize_basename(percent_decode(val));
                    } else if (by_name.empty() && equal_ignore_case(key, "name")) {
                        by_name = sanitize_basename(percent_decode(val));
                    }
                }
                if (!by_filename.empty()) return by_filename;
                if (!by_name.empty()) return by_name;
                if (const auto slash = url.find_last_of('/'); slash != std::string_view::npos) {
                    if (const auto filename = url.substr(slash + 1); !filename.empty()) {
                        return sanitize_basename(percent_decode(filename));
                    }
                }
                return {};
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
                *task_progress = {};
                task_progress->url = tCtx->url.c_str(); // HTTP 识别键与展示（info_hash 保持空）
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
                                              ? static_cast<double>(total_downloaded) / static_cast<double>(tCtx->
                                                    total_size)
                                              : -1.0;
                task_progress->download_rate = total_speed;
                // remaining / eta 由 wrapper 层现算（EngineProgress 不承载 eta），此处不再计算 eta。
                task_progress->task_status = tCtx->status;
                task_progress->support_range = tCtx->support_range;
                task_progress->etag = tCtx->etag.c_str();
                task_progress->last_modified = tCtx->last_modified.c_str();
                task_progress->saved_at_unix_ms = now_unix_ms();
                task_progress->reason = tCtx->reason;
                task_progress->message = tCtx->message.c_str();
            }

            /**
             * 分片进度回调（CURLOPT_XFERINFOFUNCTION）：采样分片瞬时速度并做 EMA 平滑，
             * 同时充当取消/暂停的快速响应点。
             * @param userdata 挂接时传入的 dl_part_ctx*（CURLOPT_XFERINFODATA）。
             * @param dl_total 本连接预期下载总量（未用，分片区间自行管理）。
             * @param dl_now   本连接已下载字节数（连接视角，重试重建后从 0 重计）。
             * @param ul_total/ul_now 上传方向计数（下载场景未用）。
             * @return 0 继续传输；非 0 令 curl 以 CURLE_ABORTED_BY_CALLBACK 终止本连接。
             */
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

            /**
             * 响应头回调（CURLOPT_HEADERFUNCTION）：curl 按行回调，逐行解析状态行与
             * Content-Range / Content-Length / ETag / Last-Modified / Content-Disposition，
             * 并执行续传漂移（drift）检查。
             * @param buffer  单行 header 原始数据（含行尾 CRLF，不保证以 NUL 结尾）。
             * @param size    单元素字节数，与 n_items 相乘得本行长度（curl 约定 size 恒为 1）。
             * @param n_items 元素个数。
             * @param userdata 挂接时传入的 dl_part_ctx*（CURLOPT_HEADERDATA）。
             * @return 返回本行长度表示消费成功；返回 0 令 curl 以 CURLE_WRITE_ERROR
             *         终止本连接（任务已定错 / 取消 / 漂移检测失败时）。
             */
            size_t header_cb(const char *buffer, size_t size, size_t n_items, void *userdata) {
                auto *pCtx = static_cast<dl_part_ctx *>(userdata);
                auto *tCtx = pCtx->task;
                const size_t len = size * n_items;
                if (tCtx->status == DW_TASK_STATUS_ERROR || tCtx->cancel_req.load() || len == 0) return 0;

                try {
                    auto mark_drift_error = [&]() -> size_t {
                        std::lock_guard<std::mutex> lk(tCtx->speed_mtx);
                        tCtx->parts[pCtx->index].status = DW_TASK_STATUS_ERROR;
                        tCtx->parts[pCtx->index].reason = DW_REASON_ERROR;
                        tCtx->parts[pCtx->index].download_rate = 0.0;
                        tCtx->status = DW_TASK_STATUS_ERROR;
                        tCtx->reason = DW_REASON_ERROR;
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

                    DW_LOG_TASK(DW_LOG_DEBUG, tCtx->url.c_str(), "[part %d] header: %.*s",
                                pCtx->index, static_cast<int>(raw.size()), raw.data());

                    if (raw.starts_with("HTTP/")) {
                        if (const auto sp = raw.find(' '); sp != std::string_view::npos) {
                            if (long code = 0; sv_to_int(raw.substr(sp + 1), code)) {
                                pCtx->seen_http_code = code;
                                // 期望 206 却收到 200（If-Range 失配/内容已变/服务器不再支持 Range）。
                                // 反向判定：200 全量流可正常落盘的唯一形态是"单分片且写偏移为 0"
                                //（探测降级、从头全量重收），此时照单全收；其余（多分片并行、
                                // done>0 续传）从 0 开始的全量流必然错位，标记漂移：经管理层重启时
                                // clear_resume+clear_segments 并重探测、ftruncate 至新 total_size，全量复位。
                                const bool full_stream_ok = tCtx->parts.size() <= 1
                                                            && tCtx->parts[pCtx->index].done == 0;
                                if (code == 200 && !full_stream_ok) {
                                    DW_LOG_TASK(DW_LOG_ERROR, tCtx->url.c_str(),
                                                "[part %d] drift: expected 206, got http_code=%ld (parts=%d done=%lld)",
                                                pCtx->index, code,
                                                static_cast<int>(tCtx->parts.size()),
                                                static_cast<long long>(tCtx->parts[pCtx->index].done));
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
                        // bytes 0-1/239784356
                        if (const auto slash = val.find('/'); slash != std::string_view::npos) {
                            if (const auto total_sv = val.substr(slash + 1);
                                !total_sv.empty() && total_sv.front() != '*') {
                                if (int64_t total = 0; sv_to_int(total_sv, total) && total > 0) {
                                    pCtx->seen_total_size = total;
                                    if (tCtx->total_size <= 0) {
                                        tCtx->total_size = total;
                                    }
                                    if (tCtx->total_size > 0 && total != tCtx->total_size) {
                                        DW_LOG_TASK(DW_LOG_ERROR, tCtx->url.c_str(),
                                                    "[part %d] drift: Content-Range total=%lld, expected=%lld",
                                                    pCtx->index, static_cast<long long>(total),
                                                    static_cast<long long>(tCtx->total_size));
                                        return mark_drift_error();
                                    }
                                }
                            }
                        }
                    } else if (equal_ignore_case(name, "Content-Length")) {
                        // 兜底仅限探测收到 200（全量流，CL 即总大小）；206 的 CL 是探测
                        // 窗口大小而非文件总长，总大小恒由上方 Content-Range 的 total 提供。
                        if (int64_t cl = 0; sv_to_int(val, cl) && cl > 0 && pCtx->seen_total_size <= 0 && tCtx->
                                            probing && pCtx->seen_http_code != 206) {
                            pCtx->seen_total_size = cl;
                        }
                    } else if (equal_ignore_case(name, "Content-Disposition")) {
                        // 服务器真实建议名优先于 URL 推断（header 先于首笔 body，finalize_probing
                        // 定名时已可用）；filename 已有值时跳过解析（add 显式指定名 /
                        // 恢复任务的历史凭证名，两者优先级均更高）。
                        if (tCtx->filename.empty()) {
                            if (auto parsed = parse_content_disposition_filename(val); !parsed.empty()) {
                                tCtx->filename = parsed;
                            }
                        }
                    } else if (equal_ignore_case(name, "ETag")) {
                        pCtx->seen_etag.assign(val);
                        if (tCtx->etag.empty()) tCtx->etag = pCtx->seen_etag;
                        if (!tCtx->etag.empty() && pCtx->seen_etag != tCtx->etag) {
                            DW_LOG_TASK(DW_LOG_ERROR, tCtx->url.c_str(),
                                        "[part %d] drift: ETag changed, expected=\"%s\", got=\"%s\"",
                                        pCtx->index, tCtx->etag.c_str(), pCtx->seen_etag.c_str());
                            return mark_drift_error();
                        }
                    } else if (equal_ignore_case(name, "Last-Modified")) {
                        pCtx->seen_last_modified.assign(val);
                        if (tCtx->last_modified.empty()) tCtx->last_modified = pCtx->seen_last_modified;
                        if (!tCtx->last_modified.empty() && pCtx->seen_last_modified != tCtx->last_modified) {
                            DW_LOG_TASK(DW_LOG_ERROR, tCtx->url.c_str(),
                                        "[part %d] drift: Last-Modified changed, expected=\"%s\", got=\"%s\"",
                                        pCtx->index, tCtx->last_modified.c_str(), pCtx->seen_last_modified.c_str());
                            return mark_drift_error();
                        }
                    }
                    return len;
                } catch (...) {
                    DW_LOG_TASK(DW_LOG_ERROR, tCtx->url.c_str(), "[part %d] header_cb exception", pCtx->index);
                    return 0;
                }
            }

            /**
             * 响应体回调（CURLOPT_WRITEFUNCTION）：探测连接首笔数据触发 finalize_probing
             * 定名建文件切分片，随后将本笔数据定点写入分片自身区间并推进 done。
             * @param ptr      本笔 body 数据起始地址。
             * @param size     单元素字节数，与 n_member 相乘得本笔长度（curl 约定 size 恒为 1）。
             * @param n_member 元素个数。
             * @param userdata 挂接时传入的 dl_part_ctx*（CURLOPT_WRITEDATA）。
             * @return 返回实际消费字节数；小于本笔长度（含 0）令 curl 以 CURLE_WRITE_ERROR
             *         终止本连接（任务已定错 / 取消 / 暂停 / 磁盘写失败 / 防御性截断）。
             */
            size_t write_cb(const char *ptr, const size_t size, const size_t n_member, void *userdata) {
                auto *pCtx = static_cast<dl_part_ctx *>(userdata);
                auto *tCtx = pCtx->task;
                const size_t len = size * n_member;
                if (tCtx->status == DW_TASK_STATUS_ERROR || tCtx->cancel_req.load() || tCtx->pause_req.load()
                    || len == 0) {
                    // 任务已定错 / 取消 / 暂停 / 空数据：短返回终止本连接（预期控制流）
                    DW_LOG_TASK(DW_LOG_DEBUG, tCtx->url.c_str(),
                                "[part %d] write_cb abort: status=%d cancel=%d pause=%d len=%zu",
                                pCtx->index, tCtx->status, tCtx->cancel_req.load(),
                                tCtx->pause_req.load(), len);
                    return 0;
                }

                if (tCtx->probing) {
                    // 探测即下载：首笔 body 到达时 header 已到齐（总大小 / 真实建议名已采集），
                    // 就地定名建文件切分片；本连接即 part 0 的正式下载流，本笔数据直接落盘，
                    // 其余分片由 run_parts_multi 主循环动态挂载。
                    finalize_probing(tCtx, pCtx);
                    if (tCtx->status == DW_TASK_STATUS_ERROR) {
                        // 失败细节（建目录/建文件/预分配）已由 finalize_probing 记录 ERROR
                        DW_LOG_TASK(DW_LOG_DEBUG, tCtx->url.c_str(),
                                    "[part %d] write_cb abort: finalize_probing failed", pCtx->index);
                        return 0;
                    }
                }

                auto &part = tCtx->parts[pCtx->index];
                // 数据块与分片边界检查
                size_t write_len = len;
                if (part.size > 0) {
                    const int64_t remain = part.size - part.done;
                    if (remain <= 0) {
                        // 分片已满，不再写入
                        DW_LOG_TASK(DW_LOG_DEBUG, tCtx->url.c_str(),
                                    "[part %d] write_cb abort: part full, extra=%zu done=%lld",
                                    pCtx->index, len, static_cast<long long>(part.done));
                        return 0;
                    }
                    if (static_cast<int64_t>(len) > remain) {
                        // 数据块超过分片边界，截断
                        write_len = static_cast<size_t>(remain);
                    }
                }
                if (!std::filesystem::exists(tCtx->full_file_path)) {
                    DW_LOG_TASK(DW_LOG_ERROR, tCtx->url.c_str(), "[part %d] file missing: path=%s",
                                pCtx->index, tCtx->full_file_path.c_str());
                    std::lock_guard<std::mutex> lk(tCtx->speed_mtx);
                    part.status = DW_TASK_STATUS_ERROR;
                    part.reason = DW_REASON_ERROR;
                    if (tCtx->message.empty()) tCtx->message = "文件不存在";
                    return 0;
                }
                if (!pCtx->file.is_open()) {
                    pCtx->file.open(tCtx->full_file_path, false);
                }
                const auto off = static_cast<long long>(part.start + part.done);
                if (const int werr = pCtx->file.pwrite_at(ptr, write_len, off); werr != 0) {
                    DW_LOG_TASK(DW_LOG_ERROR, tCtx->url.c_str(),
                                "[part %d] write failed: wanted=%zu errno=%d",
                                pCtx->index, write_len, werr);
                    std::lock_guard<std::mutex> lk(tCtx->speed_mtx);
                    part.status = DW_TASK_STATUS_ERROR;
                    part.reason = DW_REASON_ERROR;
                    if (tCtx->message.empty()) {
                        tCtx->message = werr == ENOSPC ? "存储空间不足" : "保存失败 [" + std::to_string(werr) + "]";
                    }
                    return 0;
                }
                {
                    std::lock_guard<std::mutex> lk(tCtx->speed_mtx);
                    part.done += static_cast<int64_t>(write_len);
                    part.progress = part.size > 0
                                        ? static_cast<double>(part.done) * 100.0 / static_cast<double>(part.size)
                                        : -1.0;
                }
                return write_len;
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
                // 探测请求发有界窗口 "0-(n-1)"：闭区间令服务器收满即正常断连。
                // 切分保证 part 0 区间 ≥ 窗口，窗口内字节全部落盘利用；part 0 剩余
                // 区间由 handle_part_result 的 probe_window 续接路径（不计重试）补齐。
                if (tCtx->probing) {
                    range_hdr = "0-" + std::to_string(kProbeWindowBytes - 1);
                    pCtx->probe_window = 1;
                } else {
                    pCtx->probe_window = 0;
                    if (rend >= restart) {
                        range_hdr = std::to_string(restart) + "-" + std::to_string(rend);
                    } else {
                        range_hdr = std::to_string(restart) + "-";
                    }
                }
                curl_easy_setopt(curl, CURLOPT_RANGE, range_hdr.c_str());

                // 4xx/5xx 直接以 CURLE_HTTP_RETURNED_ERROR 失败，错误页 body 不进 write_cb（防污染落盘文件）。
                curl_easy_setopt(curl, CURLOPT_FAILONERROR, 1L);

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
                // FAILONERROR 产生的 HTTP_RETURNED_ERROR 同样按状态码归因。
                if (rc == CURLE_OK || rc == CURLE_HTTP_RETURNED_ERROR) {
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
                        return DW_REASON_ERROR;
                    return DW_REASON_ERROR;
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
                    case CURLE_WRITE_ERROR: return DW_REASON_ERROR;
                    default: return DW_REASON_ERROR;
                }
            }

            void aggregate_status(dl_task_ctx *tCtx) {
                if (tCtx->status == DW_TASK_STATUS_ERROR && tCtx->reason == DW_REASON_ERROR) return;
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
                                case DW_REASON_AUTH: rank = 4;
                                    break;
                                case DW_REASON_ERROR: rank = 3;
                                    break;
                                case DW_REASON_NETWORK: rank = 1;
                                    break;
                                default: rank = 0;
                                    break;
                            }
                            if (rank > best_rank) {
                                best_rank = rank;
                                best = part.reason;
                            }
                            break;
                        }
                        case DW_TASK_STATUS_COMPLETED: break;
                        default: all_completed = 0;
                            break;
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

                // 文件名优先级 Content-Disposition -> URL 参数 filename/name -> 路径末段 -> 兜底名（download）
                {
                    if (tCtx->filename.empty()) {
                        tCtx->filename = extract_filename_from_url(tCtx->url);
                    }
                    if (tCtx->filename.empty()) {
                        tCtx->filename = "download";
                    }

                    // 定名单点：优先级 add 指定名 > Content-Disposition（header_cb 已填）>
                    // URL 参数 filename/name > 路径末段 > 兜底名；无论哪级来源均经
                    // request_unique_name 上调 TaskManager 判重（重名时返回包层目录名，
                    // 文件本名不变），随后经 post_task_files 推单文件节点落文件表
                    //（与 BT 统一通道，先删后存）。恢复任务回退全量重下（is_resume=1
                    // 但存档失效）同样上调：TaskManager 幂等守卫沿用既有唯一名，
                    // 包层任务据此重建包层路径，不会落回原目录与他任务相撞。
                    // 已知允许误差：恢复任务若 resume 存档尚未落库即终止（is_resume=0），
                    // 此处按首次重新定名，磁盘半成品会命中判重再包一层 name(1) 全量重下，
                    // 窗口极小（探测定名后至首个 resume 落库前异常终止），不做额外防护。
                    const std::string unique = dw::request_unique_name(
                        tCtx->url.c_str(), DW_PROTOCOL_HTTP,
                        tCtx->output_path.c_str(), tCtx->filename.c_str());
                    if (unique != tCtx->filename) {
                        // 重名包层：落盘目录追加一层唯一包层目录（save_path/unique/原名），
                        // 与 BT move_storage 迁 save_path 语义统一。
                        tCtx->output_path =
                                (std::filesystem::path(tCtx->output_path) / unique).string();
                    }

                    // HTTP 单文件模型：头到齐即可确定最终文件，推单条根层文件节点。
                    // 指针字段仅在调用期有效，库内深拷落库。
                    dw_file_info_t f{};
                    f.index = 0;
                    f.node_id = 1;
                    f.parent_id = -1;
                    f.type = 1;
                    f.prefix = const_cast<char *>("");
                    f.name = const_cast<char *>(tCtx->filename.c_str());
                    const std::string ext = dw::utils::file_extension(tCtx->filename);
                    f.ext = ext.empty() ? nullptr : const_cast<char *>(ext.c_str());
                    f.size = tCtx->total_size > 0 ? tCtx->total_size : 0;
                    f.status = 0;
                    f.created_at = now_unix_ms();
                    dw::post_task_files(tCtx->url.c_str(), DW_PROTOCOL_HTTP, &f, 1);
                }
                tCtx->full_file_path =
                        (std::filesystem::path(tCtx->output_path) / tCtx->filename).string();

                if (!tCtx->full_file_path.empty()) {
                    if (const auto dir_path = std::filesystem::path(tCtx->full_file_path).parent_path();
                        !dir_path.empty() && !mkdir_recursive(dir_path.string())) {
                        DW_LOG_TASK(DW_LOG_ERROR, tCtx->url.c_str(), "mkdir failed: %s", dir_path.string().c_str());
                        tCtx->status = DW_TASK_STATUS_ERROR;
                        tCtx->reason = DW_REASON_ERROR;
                        tCtx->message = "目录创建失败";
                        return;
                    }
                    // 局部即开即关：创建文件并验证可写（写入由各分片句柄承担），
                    // 随后按总大小预分配（resize_file 按路径操作，无需持有句柄）。
                    DwFile creator;
                    if (!creator.open(tCtx->full_file_path)) {
                        DW_LOG_TASK(DW_LOG_ERROR, tCtx->url.c_str(), "open failed: %s errno=%d",
                                    tCtx->full_file_path.c_str(), errno);
                        tCtx->status = DW_TASK_STATUS_ERROR;
                        tCtx->reason = DW_REASON_ERROR;
                        tCtx->message = (errno == ENOSPC) ? "存储空间不足" : "存储异常，无法保存文件";
                        return;
                    }
                    if (tCtx->total_size > 0 &&
                        !creator.truncate(tCtx->full_file_path, tCtx->total_size)) {
                        DW_LOG_TASK(DW_LOG_ERROR, tCtx->url.c_str(), "truncate failed: size=%lld errno=%d",
                                    static_cast<long long>(tCtx->total_size), errno);
                        tCtx->status = DW_TASK_STATUS_ERROR;
                        tCtx->reason = DW_REASON_ERROR;
                        tCtx->message = (errno == ENOSPC) ? "存储空间不足" : "存储异常，无法保存文件";
                        return;
                    }
                }

                if (code == 206) {
                    // 切分片持 speed_mtx：采集线程并发遍历 parts；容量已在 task_create_new 预留，
                    // push_back 不会 realloc，part 0 连接持有的指针/引用始终稳定。
                    std::lock_guard<std::mutex> lk(tCtx->speed_mtx);
                    tCtx->support_range = 1;
                    const int64_t total = tCtx->total_size;
                    int32_t want_parts = 1;
                    if (total >= g_cfg.min_size_for_split && g_cfg.default_parts > 1)
                        want_parts = g_cfg.default_parts;
                    // part 0 至少覆盖探测窗口：窗口内已收字节全部属于自身区间落盘利用，
                    // 探测连接恒以 rc==OK 正常收尾（区间写满完成或 probe_window 续接），
                    // 不再依赖 write_cb 截断丢弃在途字节。
                    int64_t part0_size = (want_parts > 1)
                                             ? std::max(total / want_parts, std::min(kProbeWindowBytes, total))
                                             : total;
                    if (want_parts > 1) {
                        const int64_t rest = total - part0_size;
                        if (rest <= 0) want_parts = 1; // 剩余为空：退化单分片
                        else if (rest < want_parts - 1) want_parts = 2; // 剩余不足均分：只留一个尾分片
                    }
                    if (want_parts == 1) part0_size = total;
                    const int64_t chunk = (want_parts > 1) ? ((total - part0_size) / (want_parts - 1)) : 0;

                    tCtx->parts[0].index = 0;
                    tCtx->parts[0].start = 0;
                    tCtx->parts[0].end = (total > 0) ? (part0_size - 1) : -1;
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

                    // 剩余区间 [part0_size, total-1] 均分给其余分片，尾分片吸收余数。
                    for (int32_t i = 1; i < want_parts; ++i) {
                        const int64_t start = part0_size + chunk * (i - 1);
                        const int64_t end = (i == want_parts - 1)
                                                ? (total - 1)
                                                : (start + chunk - 1);
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
                } else {
                    // 非 206（含不支持 Range 的 200）：单分片全量，探测连接一条流写到底。
                    std::lock_guard<std::mutex> lk(tCtx->speed_mtx);
                    tCtx->support_range = 0;
                    if (!tCtx->parts.empty()) {
                        tCtx->parts[0].start = 0;
                        tCtx->parts[0].end = (tCtx->total_size > 0) ? (tCtx->total_size - 1) : -1;
                        tCtx->parts[0].size = (tCtx->total_size > 0) ? tCtx->total_size : 0;
                        tCtx->parts[0].progress = (tCtx->parts[0].size > 0)
                                                      ? static_cast<double>(tCtx->parts[0].done) * 100.0 / static_cast<
                                                            double>(
                                                            tCtx->parts[0].size)
                                                      : -1.0;
                    }
                }
            }

            /* =====================================================================
             *          Part 4.5: 续传数据序列化 / 反序列化 / emit
             * ===================================================================== */

            std::string serialize_resume(dl_task_ctx *tCtx) {
                // bencode 字典（复用 libtorrent 编解码，无版本概念）：元数据键值 +
                // parts 列表（每项 [index, start, end, done]）。
                lt::entry root(lt::entry::dictionary_t);
                std::lock_guard<std::mutex> lk(tCtx->speed_mtx);
                root["total_size"] = tCtx->total_size;
                root["support_range"] = tCtx->support_range;
                root["etag"] = tCtx->etag;
                root["last_modified"] = tCtx->last_modified;
                // 持久化落盘物理完整路径（即最终路径），续传直接回落同一文件。
                root["path"] = tCtx->full_file_path;
                lt::entry::list_type parts;
                for (const auto &p: tCtx->parts) {
                    lt::entry::list_type item;
                    item.emplace_back(static_cast<int64_t>(p.index));
                    item.emplace_back(p.start);
                    item.emplace_back(p.end);
                    item.emplace_back(p.done);
                    parts.emplace_back(std::move(item));
                }
                root["parts"] = std::move(parts);
                std::string out;
                lt::bencode(std::back_inserter(out), root);
                return out;
            }

            HttpResumeData deserialize_resume(const uint8_t *data, size_t size) {
                HttpResumeData rd;
                if (!data || size == 0) return rd;
                // 解析失败（非 bencode / 非字典 / 无分片）均 ok=false，调用方回退全量重下。
                lt::error_code ec;
                const lt::bdecode_node root = lt::bdecode(
                    {reinterpret_cast<const char *>(data), static_cast<std::ptrdiff_t>(size)}, ec);
                if (ec || root.type() != lt::bdecode_node::dict_t) return rd;
                rd.total_size = root.dict_find_int_value("total_size", -1);
                rd.support_range = static_cast<int32_t>(root.dict_find_int_value("support_range", 0));
                rd.etag = std::string(root.dict_find_string_value("etag"));
                rd.last_modified = std::string(root.dict_find_string_value("last_modified"));
                rd.full_file_path = std::string(root.dict_find_string_value("path"));
                if (const lt::bdecode_node list = root.dict_find_list("parts"); list) {
                    for (int i = 0; i < list.list_size(); ++i) {
                        const lt::bdecode_node item = list.list_at(i);
                        if (item.type() != lt::bdecode_node::list_t || item.list_size() < 4) continue;
                        dw_part_state_t part{};
                        part.index = static_cast<int32_t>(item.list_int_value_at(0));
                        part.start = item.list_int_value_at(1);
                        part.end = item.list_int_value_at(2);
                        part.done = item.list_int_value_at(3);
                        part.size = (part.end >= part.start) ? (part.end - part.start + 1) : 0;
                        const bool done = (part.size > 0 && part.done >= part.size);
                        part.status = done ? DW_TASK_STATUS_COMPLETED : DW_TASK_STATUS_DOWNLOADING;
                        part.reason = DW_REASON_NONE;
                        part.progress = (part.size > 0)
                                            ? static_cast<double>(part.done) * 100.0 / static_cast<double>(part.size)
                                            : -1.0;
                        rd.parts.push_back(part);
                    }
                }
                rd.ok = !rd.parts.empty();
                return rd;
            }

            void emit_resume(dl_task_ctx *tCtx) {
                if (!tCtx) return;
                const std::string blob = serialize_resume(tCtx);
                if (blob.empty()) return;
                dw::post_resume_data(tCtx->url.c_str(), DW_PROTOCOL_HTTP,
                                     reinterpret_cast<const uint8_t *>(blob.data()), blob.size());
            }

            // worker 线程按门槛异步上报 resume：仅支持续传、且总已下载相比上次有推进时触发。
            // last_emit_done 由 worker 线程独占推进（去重），全程不持 TaskManager mtx_；
            // 计算 total_done 时短持 speed_mtx，emit_resume 在锁外调用（其内部再取 speed_mtx 序列化）。
            void maybe_emit_resume(dl_task_ctx *tCtx) {
                if (!tCtx || !tCtx->support_range) return;
                {
                    std::lock_guard<std::mutex> lk(tCtx->speed_mtx);
                    int64_t total_done = 0;
                    for (const auto &part: tCtx->parts) total_done += part.done;
                    if (total_done <= tCtx->last_emit_done) return;
                    tCtx->last_emit_done = total_done;
                }
                emit_resume(tCtx);
            }

            /* =====================================================================
             *          Part 5: 分片结果判定 + multi 事件循环 + 任务线程
             * ===================================================================== */

            // 处理单个分片一次请求完成后的结果判定。
            // 返回 true 表示需要重试（调用方应重新构建 easy 并重新入队），
            // 返回 false 表示该分片已进入终态（完成或错误）。
            bool handle_part_result(dl_task_ctx *tCtx, dl_part_ctx *pCtx, CURLcode rc, long http_code) {
                {
                    std::lock_guard<std::mutex> lk(tCtx->speed_mtx);
                    auto &part = tCtx->parts[pCtx->index];
                    // write_cb 内已定错（磁盘写失败 / 内容漂移）：直接终态，不重试。
                    if (part.status == DW_TASK_STATUS_ERROR) return false;
                    // 区间写满即完成（rc==OK 正常收尾为主路径；write_cb 截断仅剩防御场景：
                    // 服务器超发区间外字节）。
                    if (part.size > 0 && part.done >= part.size) {
                        part.status = DW_TASK_STATUS_COMPLETED;
                        part.reason = DW_REASON_NONE;
                        part.download_rate = 0.0;
                        return false;
                    }
                    // 探测窗口连接收满窗口正常断开：预期内的续接，重建 easy 续下剩余
                    // 区间，不消耗重试次数。限定 206（200 全量流正常结束即完成，不续接）
                    // 且已有进展（防 206 空 body 反复续接）；必须先于下方 chunked 完成
                    // 判定，否则 206 总长未知（Content-Range total=*）时窗口收满会被
                    // 误判为完成而截断文件。
                    if (pCtx->probe_window && rc == CURLE_OK && http_code == 206 && part.done > 0) {
                        pCtx->probe_window = 0;
                        DW_LOG_TASK(DW_LOG_INFO, tCtx->url.c_str(),
                                    "probe window done, continue: part=%d done=%lld/%lld",
                                    pCtx->index, static_cast<long long>(part.done),
                                    static_cast<long long>(part.size));
                        return true;
                    }
                    // 无总长（chunked 200）：连接正常结束即完成，以实收字节补全大小。
                    if (rc == CURLE_OK && http_code >= 200 && http_code < 300 && part.size <= 0) {
                        part.status = DW_TASK_STATUS_COMPLETED;
                        part.reason = DW_REASON_NONE;
                        part.download_rate = 0.0;
                        part.size = part.done;
                        part.progress = 100.0;
                        if (tCtx->total_size <= 0) tCtx->total_size = part.done;
                        return false;
                    }
                }
                if (rc == CURLE_OK && http_code >= 200 && http_code < 300) {
                    std::lock_guard<std::mutex> lk(tCtx->speed_mtx);
                    auto &part = tCtx->parts[pCtx->index];
                    // 2xx 但区间未收满（服务器提前断开）：按次数重试续传。
                    DW_LOG_TASK(DW_LOG_INFO, tCtx->url.c_str(),
                                "incomplete: part=%d done=%lld/%lld",
                                pCtx->index, static_cast<long long>(part.done), static_cast<long long>(part.size));
                    if (pCtx->retry_count >= g_cfg.max_retries) {
                        part.status = DW_TASK_STATUS_ERROR;
                        part.reason = DW_REASON_NETWORK;
                        part.download_rate = 0.0;
                        return false;
                    }
                    pCtx->retry_count++;
                    DW_LOG_TASK(DW_LOG_INFO, tCtx->url.c_str(),
                                "retry: part=%d attempt=%d/%d", pCtx->index, pCtx->retry_count, g_cfg.max_retries);
                    return true;
                }

                int retryable = 0;
                const dw_reason_t reason = classify_failure(rc, http_code, &retryable);
                DW_LOG_TASK(DW_LOG_INFO, tCtx->url.c_str(),
                            "failed: part=%d rc=%d http=%ld reason=%d retryable=%d",
                            pCtx->index, static_cast<int>(rc), http_code, static_cast<int>(reason), retryable);

                if (retryable && pCtx->retry_count < g_cfg.max_retries) {
                    pCtx->retry_count++;
                    DW_LOG_TASK(DW_LOG_INFO, tCtx->url.c_str(),
                                "retry: part=%d attempt=%d/%d", pCtx->index, pCtx->retry_count, g_cfg.max_retries);
                    return true;
                }

                std::lock_guard<std::mutex> lk(tCtx->speed_mtx);
                auto &part = tCtx->parts[pCtx->index];
                part.status = DW_TASK_STATUS_ERROR;
                part.reason = reason;
                part.download_rate = 0.0;
                if (tCtx->message.empty()) {
                    switch (reason) {
                        case DW_REASON_AUTH: tCtx->message = "认证失败";
                            break;
                        case DW_REASON_ERROR: tCtx->message = "资源不存在或已失效";
                            break;
                        case DW_REASON_NETWORK: tCtx->message = "网络连接异常";
                            break;
                        default: tCtx->message = "下载过程中出现异常";
                            break;
                    }
                }
                return false;
            }

            // 每任务一个 curl_multi：单线程事件循环驱动本任务所有分片。
            // 各分片持有独立 easy handle 与独立文件对象，回调在本线程串行执行；
            // 与其他任务的 multi 互不影响，磁盘写阻塞仅局限于当前任务内部。
            void run_parts_multi(dl_task_ctx *tCtx) {
                CURLM *multi = curl_multi_init();
                if (!multi) {
                    DW_LOG_TASK(DW_LOG_ERROR, tCtx->url.c_str(), "curl_multi_init failed");
                    std::lock_guard<std::mutex> lk(tCtx->speed_mtx);
                    tCtx->status = DW_TASK_STATUS_ERROR;
                    tCtx->reason = DW_REASON_ERROR;
                    if (tCtx->message.empty()) tCtx->message = "下载过程中出现异常";
                    return;
                }

                // 退出/异常时统一回收所有仍在的 easy：先从 multi 移除再释放。
                auto cleanup_all = [&]() {
                    for (auto &pc: tCtx->part_ctx) {
                        if (pc.easy) {
                            curl_multi_remove_handle(multi, pc.easy);
                            curl_easy_cleanup(pc.easy);
                            pc.easy = nullptr;
                        }
                        if (pc.easy_hdrs) {
                            curl_slist_free_all(pc.easy_hdrs);
                            pc.easy_hdrs = nullptr;
                        }
                    }
                    curl_multi_cleanup(multi);
                };

                try {
                    // 为分片构建 easy 并加入 multi；失败则直接标记该分片错误。
                    auto add_part = [&](dl_part_ctx *pCtx) -> bool {
                        CURL *curl = build_easy_for_part(tCtx, pCtx);
                        if (!curl) {
                            DW_LOG_TASK(DW_LOG_ERROR, tCtx->url.c_str(), "[part %d] build_easy_for_part failed",
                                        pCtx->index);
                            std::lock_guard<std::mutex> lk(tCtx->speed_mtx);
                            tCtx->parts[pCtx->index].status = DW_TASK_STATUS_ERROR;
                            tCtx->parts[pCtx->index].reason = DW_REASON_ERROR;
                            tCtx->parts[pCtx->index].download_rate = 0.0;
                            return false;
                        }
                        pCtx->easy = curl;
                        curl_multi_add_handle(multi, curl);
                        return true;
                    };

                    // 单个分片本轮结束后回收其 easy（drop 前 pCtx->easy 应等于完成的 handle）。
                    auto drop_easy = [&](dl_part_ctx *pCtx) {
                        if (pCtx->easy) {
                            curl_multi_remove_handle(multi, pCtx->easy);
                            curl_easy_cleanup(pCtx->easy);
                            pCtx->easy = nullptr;
                        }
                        if (pCtx->easy_hdrs) {
                            curl_slist_free_all(pCtx->easy_hdrs);
                            pCtx->easy_hdrs = nullptr;
                        }
                    };

                    int active = 0;
                    size_t spawned = 0;
                    // 统一挂载水位：初始批与探测切分后新增的分片都经此挂入 multi
                    //（finalize_probing 在 write_cb 回调栈内仅扩容 parts/part_ctx，挂载收归主循环）。
                    auto spawn_new_parts = [&]() {
                        while (spawned < tCtx->part_ctx.size()) {
                            dl_part_ctx *pc = &tCtx->part_ctx[spawned++];
                            // 续传回灌时已完成的分片无需重新请求（避免 200 重下/重写）。
                            if (tCtx->parts[pc->index].status == DW_TASK_STATUS_COMPLETED) continue;
                            if (add_part(pc)) ++active;
                        }
                    };
                    spawn_new_parts();

                    while (active > 0) {
                        if (tCtx->cancel_req.load() || tCtx->pause_req.load()) break;
                        if (tCtx->status == DW_TASK_STATUS_ERROR && tCtx->reason == DW_REASON_ERROR) break;

                        int still_running = 0;
                        CURLMcode mc = curl_multi_perform(multi, &still_running);
                        if (mc == CURLM_OK)
                            mc = curl_multi_poll(multi, nullptr, 0, 200, nullptr);
                        if (mc != CURLM_OK) {
                            DW_LOG_TASK(DW_LOG_ERROR, tCtx->url.c_str(), "curl_multi error: %d", static_cast<int>(mc));
                            break;
                        }

                        spawn_new_parts();

                        int msgs_left = 0;
                        CURLMsg *msg;
                        while ((msg = curl_multi_info_read(multi, &msgs_left)) != nullptr) {
                            if (msg->msg != CURLMSG_DONE) continue;
                            CURL *e = msg->easy_handle;
                            const CURLcode rc = msg->data.result;

                            dl_part_ctx *pCtx = nullptr;
                            curl_easy_getinfo(e, CURLINFO_PRIVATE, &pCtx);
                            long http_code = 0;
                            curl_easy_getinfo(e, CURLINFO_RESPONSE_CODE, &http_code);
                            if (!pCtx) {
                                curl_multi_remove_handle(multi, e);
                                curl_easy_cleanup(e);
                                continue;
                            }

                            drop_easy(pCtx);

                            if (tCtx->cancel_req.load() || tCtx->pause_req.load()) {
                                --active;
                                continue;
                            }

                            if (handle_part_result(tCtx, pCtx, rc, http_code)) {
                                // 需要重试：重新构建并入队；构建失败则计为终态
                                if (!add_part(pCtx)) --active;
                            } else {
                                --active;
                            }
                        }

                        // worker 线程节流上报 resume：一个 poll 周期至多一次，仅在总已下载推进时真正 emit。
                        maybe_emit_resume(tCtx);
                    }
                } catch (const std::exception &e) {
                    DW_LOG_TASK(DW_LOG_ERROR, tCtx->url.c_str(), "run_parts_multi exception: %s", e.what());
                    std::lock_guard<std::mutex> lk(tCtx->speed_mtx);
                    tCtx->status = DW_TASK_STATUS_ERROR;
                    tCtx->reason = DW_REASON_ERROR;
                    if (tCtx->message.empty()) tCtx->message = "下载过程中出现异常";
                } catch (...) {
                    DW_LOG_TASK(DW_LOG_ERROR, tCtx->url.c_str(), "run_parts_multi unknown exception");
                    std::lock_guard<std::mutex> lk(tCtx->speed_mtx);
                    tCtx->status = DW_TASK_STATUS_ERROR;
                    tCtx->reason = DW_REASON_ERROR;
                    if (tCtx->message.empty()) tCtx->message = "下载过程中出现异常";
                }

                cleanup_all();
            }


            void task_thread_func(dl_task_ctx *tCtx) {
                // 线程退出即置位（在所有终态推送之后），确保 sweep 只在推送完成后回收上下文，规避 use-after-free
                struct DoneGuard {
                    dl_task_ctx *t;

                    ~DoneGuard() {
                        t->thread_done.store(1);
                    }
                } done_guard{tCtx};
                try {
                    // 探测即下载：首下任务（probing=1）由 part 0 探测窗口请求在 multi 循环内完成
                    // 探测与下载；续传任务（probing=0）已由 add_task 回灌分片直接续传。
                    if (tCtx->cancel_req.load()) return;
                    if (tCtx->pause_req.load()) {
                        // 暂停早退（分片下载尚未开始）：worker 线程退出前固化一次续传断点。
                        maybe_emit_resume(tCtx);
                        return;
                    }

                    run_parts_multi(tCtx);

                    bool all_done = false;
                    {
                        std::lock_guard<std::mutex> lk(tCtx->speed_mtx);
                        aggregate_status(tCtx);
                        all_done = (tCtx->status == DW_TASK_STATUS_COMPLETED);
                    }
                    if (all_done) {
                        // 文件已在最终位置（开始即定名，无移出环节）：关闭全部分片写句柄
                        // （multi 已回收无并发写者），完成后文件立即可被外部使用，不等
                        // sweep 析构；随后异步 emit 一次 resume 固化最终断点。
                        for (auto &pc: tCtx->part_ctx) pc.file.close();
                        emit_resume(tCtx);
                    }
                    // 暂停退出（分片下载中被打断）：worker 结束前固化一次续传断点，ctx 随后由 sweep 回收。
                    else if (tCtx->pause_req.load() && !tCtx->cancel_req.load()) maybe_emit_resume(tCtx);
                } catch (const std::exception &e) {
                    DW_LOG_TASK(DW_LOG_ERROR, tCtx->url.c_str(), "task_thread_func exception: %s", e.what());
                    std::lock_guard<std::mutex> lk(tCtx->speed_mtx);
                    tCtx->status = DW_TASK_STATUS_ERROR;
                    tCtx->reason = DW_REASON_ERROR;
                    tCtx->message = "任务线程异常终止";
                } catch (...) {
                    DW_LOG_TASK(DW_LOG_ERROR, tCtx->url.c_str(), "task_thread_func unknown exception");
                    std::lock_guard<std::mutex> lk(tCtx->speed_mtx);
                    tCtx->status = DW_TASK_STATUS_ERROR;
                    tCtx->reason = DW_REASON_ERROR;
                    tCtx->message = "任务线程异常终止";
                }
                // 拉模型：终态不再主动推送状态，由 TaskManager 采集循环经 query_progress 感知；
                // resume 已在推进 / 终态处由 worker 线程经 post_resume_data 异步上报。
            }

            void start_task(dl_task_ctx *tCtx) {
                tCtx->task_thread = std::thread(task_thread_func, tCtx);
            }

            /* =====================================================================
             *          Part 6: 辅助函数
             * ===================================================================== */

            std::unique_ptr<dl_task_ctx> task_create_new(const char *url, const char *output_path,
                                                         const char *filename) {
                auto tCtx = std::make_unique<dl_task_ctx>();
                tCtx->url = url;
                tCtx->output_path = output_path;
                tCtx->filename = filename ? filename : "";
                tCtx->total_size = -1;
                tCtx->status = DW_TASK_STATUS_DOWNLOADING;
                tCtx->reason = DW_REASON_NONE;
                tCtx->probing = 1;
                tCtx->start_time_ms = now_unix_ms();
                // 预留最大分片容量：探测切分在 part 0 连接运行期间 push_back 扩容，
                // reserve 保证不发生 realloc，easy 回调持有的 part_ctx 指针与 parts 引用始终稳定。
                const auto cap = static_cast<size_t>(g_cfg.default_parts > 1 ? g_cfg.default_parts : 1);
                tCtx->parts.reserve(cap);
                tCtx->part_ctx.reserve(cap);
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
                tCtx->part_ctx[0].task = tCtx.get();
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

            void set_result(dw_submit_result_t *r, const char *task_id,
                            dw_reason_t code, const char *msg, const char *fmt, ...) {
                // task_id 仅用于日志 trace；dw_submit_result_t 不再回传字符串标识。
                r->code = code;
                const std::string trace_id = dw::make_trace(task_id);
                if (msg) {
                    const size_t n = std::strlen(msg);
                    auto p = static_cast<char *>(std::malloc(n + 1));
                    if (p) std::memcpy(p, msg, n + 1);
                    r->message = p;
                } else { r->message = nullptr; }
                if (code != DW_REASON_NONE) {
                    if (fmt) {
                        va_list args;
                        va_start(args, fmt);
                        char buf[512];
                        std::vsnprintf(buf, sizeof(buf), fmt, args);
                        va_end(args);
                        DW_LOGF(DW_LOG_ERROR, trace_id.c_str(), "%s", buf);
                    } else if (msg) {
                        DW_LOGF(DW_LOG_ERROR, trace_id.c_str(), "%s", msg);
                    }
                }
            }
        }
    }
} /* namespace dw::http_engine::internal */
