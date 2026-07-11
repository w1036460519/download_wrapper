/**
 * @file torrent_engine.cpp
 * @brief BT/Torrent 下载引擎实现（基于 libtorrent）。
 *
 * 架构（push 模型）：
 *   - 单例 lt::session 管理所有 BT 任务；
 *   - 一个 alert 轮询线程：周期 post_torrent_updates() 触发 state_update_alert，
 *     消化 alert 后仅提取下载必要信息，组装 dw_progress_t 通过 dw::post_progress 推送；
 *   - 断点续传数据通过 save_resume_data_alert 经 dw::post_resume_data 输出；
 *   - 只订阅 error / status 两类 alert，不处理 peer / piece / block 等细粒度事件。
 */

#include "torrent/torrent_engine.h"

#include "internal/downloader_internal.h"

#include <libtorrent/session.hpp>
#include <libtorrent/settings_pack.hpp>
#include <libtorrent/add_torrent_params.hpp>
#include <libtorrent/torrent_handle.hpp>
#include <libtorrent/torrent_status.hpp>
#include <libtorrent/torrent_info.hpp>
#include <libtorrent/file_storage.hpp>
#include <libtorrent/magnet_uri.hpp>
#include <libtorrent/error_code.hpp>
#include <libtorrent/alert_types.hpp>
#include <libtorrent/read_resume_data.hpp>
#include <libtorrent/write_resume_data.hpp>
#include <libtorrent/hex.hpp>

#include <atomic>
#include <chrono>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace lt = libtorrent;

namespace dw {

/* ===================================================================== */
/*                        文件内全局状态与辅助                            */
/* ===================================================================== */
namespace torrent_engine {

// 单例 session
std::unique_ptr<lt::session> g_session;
// 任务句柄表：key = info_hash hex 字符串
std::unordered_map<std::string, lt::torrent_handle> g_handles;
std::mutex g_mtx;
// alert 轮询线程
std::thread g_alert_thread;
std::atomic<bool> g_running{false};
// 进度回调间隔（ms）
int g_interval_ms = 1000;
// 日志级别过滤
dw_log_level_t g_log_level = DW_LOG_INFO;

// 当前 Unix 毫秒时间戳
int64_t now_unix_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

// 内部格式化日志（带级别过滤），统一走 dw::log_message
void tlogf(dw_log_level_t level, const char* fmt, ...) {
    if (level < g_log_level) return;
    char buf[512];
    va_list ap;
    va_start(ap, fmt);
    std::vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    log_message(level, buf, "", "torrent_engine", 0);
}

// 从 torrent_handle 提取 info_hash hex（优先 v2，回退 v1）；无效返回空串
std::string info_hash_hex(const lt::torrent_handle& h) {
    if (!h.is_valid()) return {};
    try {
        const lt::info_hash_t ih = h.info_hashes();
        return ih.has_v2() ? lt::aux::to_hex(ih.v2) : lt::aux::to_hex(ih.v1);
    } catch (...) {
        return {};
    }
}

// 将 torrent_status 映射为 dw_task_status_t（仅下载相关状态）
dw_task_status_t map_status(const lt::torrent_status& s) {
    if (s.errc) {
        return DW_TASK_STATUS_ERROR;
    }
    const bool paused = (s.flags & lt::torrent_flags::paused) != lt::torrent_flags_t{};
    const bool auto_managed = (s.flags & lt::torrent_flags::auto_managed) != lt::torrent_flags_t{};
    if (paused && !auto_managed) {
        return DW_TASK_STATUS_PAUSED;
    }
    if (s.is_seeding || s.is_finished) {
        return DW_TASK_STATUS_COMPLETED;
    }
    if (s.state == lt::torrent_status::downloading_metadata) {
        return DW_TASK_STATUS_PARSING; // 磁力链接获取元数据阶段
    }
    return DW_TASK_STATUS_DOWNLOADING;
}

// 组装 dw_progress_t 并推送。task_id 使用传入的 key（回调周期内有效）。
void push_status(const std::string& key, const lt::torrent_status& s) {
    dw_progress_t p;
    std::memset(&p, 0, sizeof(p));

    // errc 消息在推送期间需保持有效
    const std::string msg = s.errc ? s.errc.message() : std::string{};

    p.task_id      = key.c_str();
    p.trace_id     = "";
    p.protocol     = DW_PROTOCOL_TORRENT;
    p.name         = s.name.c_str();
    p.output_path  = "";
    p.filename     = "";
    p.total_size   = s.total_wanted;
    p.total_done   = s.total_done;
    p.remaining    = (s.total_wanted > s.total_done) ? (s.total_wanted - s.total_done) : 0;
    p.progress     = static_cast<double>(s.progress);
    p.download_rate = static_cast<double>(s.download_payload_rate);
    p.eta          = (s.download_payload_rate > 0 && p.remaining > 0)
                         ? static_cast<double>(p.remaining) / s.download_payload_rate
                         : -1.0;
    p.task_status  = map_status(s);
    p.reason       = s.errc ? DW_REASON_NETWORK : DW_REASON_NONE;
    p.message      = msg.c_str();
    p.saved_at_unix_ms = now_unix_ms();

    // HTTP 特有字段（BT 不使用）
    p.etag          = "";
    p.last_modified = "";

    // BT 特有字段
    p.upload_rate  = static_cast<double>(s.upload_payload_rate);
    p.total_upload = s.total_upload;
    p.is_seeding   = s.is_seeding ? 1 : 0;
    p.state        = static_cast<int32_t>(s.state);
    p.peers_count  = s.num_peers;

    // BT 无分片
    p.part_states = nullptr;
    p.part_count  = 0;

    post_progress(&p);
}

// 请求保存断点续传数据（异步，结果经 save_resume_data_alert 返回）
void request_save_resume(const lt::torrent_handle& h) {
    if (!h.is_valid()) return;
    try {
        h.save_resume_data(lt::torrent_handle::save_info_dict
                           | lt::torrent_handle::only_if_modified);
    } catch (const std::exception& e) {
        tlogf(DW_LOG_ERROR, "[ERROR] 请求保存 resume_data 失败: %s", e.what());
    }
}

// 处理单个 alert：仅关注下载生命周期与状态更新
void handle_alert(const lt::alert* a) {
    if (!a) return;

    // 批量状态更新（post_torrent_updates 触发）
    if (const auto* su = lt::alert_cast<lt::state_update_alert>(a)) {
        for (const lt::torrent_status& s : su->status) {
            const std::string key = info_hash_hex(s.handle);
            if (!key.empty()) {
                push_status(key, s);
            }
        }
        return;
    }
    // 下载完成
    if (const auto* tf = lt::alert_cast<lt::torrent_finished_alert>(a)) {
        const std::string key = info_hash_hex(tf->handle);
        if (!key.empty()) {
            try { push_status(key, tf->handle.status()); } catch (...) {}
        }
        request_save_resume(tf->handle); // 完成时保存一次
        return;
    }
    // 任务错误
    if (const auto* te = lt::alert_cast<lt::torrent_error_alert>(a)) {
        const std::string key = info_hash_hex(te->handle);
        tlogf(DW_LOG_ERROR, "[ERROR] 任务错误 info_hash=%s msg=%s",
              key.c_str(), te->error.message().c_str());
        if (!key.empty()) {
            try { push_status(key, te->handle.status()); } catch (...) {}
        }
        return;
    }
    // 元数据接收（磁力链接解析完成）：推送一次 PARSED 状态，通知上层可取文件列表
    if (const auto* mr = lt::alert_cast<lt::metadata_received_alert>(a)) {
        const std::string key = info_hash_hex(mr->handle);
        if (!key.empty()) {
            try {
                lt::torrent_status s = mr->handle.status();
                dw_progress_t p;
                std::memset(&p, 0, sizeof(p));
                p.task_id     = key.c_str();
                p.trace_id    = "";
                p.protocol    = DW_PROTOCOL_TORRENT;
                p.name        = s.name.c_str();
                p.output_path = "";
                p.filename    = "";
                p.total_size  = s.total_wanted;
                p.total_done  = s.total_done;
                p.remaining   = (s.total_wanted > s.total_done) ? (s.total_wanted - s.total_done) : 0;
                p.progress    = static_cast<double>(s.progress);
                p.task_status = DW_TASK_STATUS_PARSED;
                p.reason      = DW_REASON_NONE;
                p.message     = "";
                p.etag        = "";
                p.last_modified = "";
                p.saved_at_unix_ms = now_unix_ms();
                p.state       = static_cast<int32_t>(s.state);
                p.peers_count = s.num_peers;
                post_progress(&p);
            } catch (...) {}
        }
        return;
    }
    // 断点续传数据就绪：输出给上层持久化
    if (const auto* rd = lt::alert_cast<lt::save_resume_data_alert>(a)) {
        const std::string key = info_hash_hex(rd->handle);
        if (!key.empty()) {
            try {
                const std::vector<char> buf = lt::write_resume_data_buf(rd->params);
                if (!buf.empty()) {
                    post_resume_data(key.c_str(), DW_PROTOCOL_TORRENT,
                                     reinterpret_cast<const uint8_t*>(buf.data()),
                                     buf.size());
                }
            } catch (const std::exception& e) {
                tlogf(DW_LOG_ERROR, "[ERROR] 写入 resume_data 失败: %s", e.what());
            }
        }
        return;
    }
    // 其余 alert（含 save_resume_data_failed 等）忽略
}

// alert 轮询线程主循环
void alert_loop() {
    while (g_running.load()) {
        try {
            if (g_session) {
                g_session->post_torrent_updates(); // 触发 state_update_alert
                g_session->wait_for_alert(std::chrono::milliseconds(g_interval_ms));
                std::vector<lt::alert*> alerts;
                g_session->pop_alerts(&alerts);
                for (const lt::alert* a : alerts) {
                    handle_alert(a);
                }
            } else {
                std::this_thread::sleep_for(std::chrono::milliseconds(g_interval_ms));
            }
        } catch (const std::exception& e) {
            tlogf(DW_LOG_ERROR, "[ERROR] alert_loop 异常: %s", e.what());
        } catch (...) {
            tlogf(DW_LOG_ERROR, "[ERROR] alert_loop 未知异常");
        }
    }
}

// 从 info_hash 查找 handle
lt::torrent_handle find_handle(const std::string& info_hash) {
    std::lock_guard<std::mutex> lock(g_mtx);
    if (const auto it = g_handles.find(info_hash); it != g_handles.end()) {
        return it->second;
    }
    return {};
}

// 填充文件列表到 C 数组（供 parse / get_file_list 复用）
int32_t fill_file_list(const std::shared_ptr<const lt::torrent_info>& ti,
                       dw_file_info_t** out_files, int32_t* out_count) {
    const lt::file_storage& fs = ti->files();
    const int file_count = fs.num_files();
    *out_count = 0;
    *out_files = static_cast<dw_file_info_t*>(
        std::calloc(static_cast<size_t>(file_count > 0 ? file_count : 1), sizeof(dw_file_info_t)));
    if (!*out_files) {
        return -1;
    }
    for (int i = 0; i < file_count; ++i) {
        const lt::file_index_t idx{i};
        (*out_files)[i].index = i;
        const std::string path = fs.file_path(idx);
        (*out_files)[i].name = static_cast<char*>(std::malloc(path.size() + 1));
        if ((*out_files)[i].name) {
            std::memcpy((*out_files)[i].name, path.c_str(), path.size() + 1);
        }
        (*out_files)[i].size = fs.file_size(idx);
    }
    *out_count = file_count;
    return 0;
}

// 设置同步返回结果，message 由 malloc 分配（成功时为 nullptr）
void set_result(dw_submit_result_t* r, const char* task_id,
                dw_reason_t code, const char* msg) {
    r->task_id = task_id;
    r->code    = code;
    if (msg) {
        const size_t n = std::strlen(msg);
        auto* p = static_cast<char*>(std::malloc(n + 1));
        if (p) std::memcpy(p, msg, n + 1);
        r->message = p;
    } else {
        r->message = nullptr;
    }
}

} // namespace torrent_engine

using namespace torrent_engine;

/* ===================================================================== */
/*                        TorrentEngine 成员实现                          */
/* ===================================================================== */

TorrentEngine::TorrentEngine() = default;
TorrentEngine::~TorrentEngine() {
    if (initialized_) {
        destroy();
    }
}

int32_t TorrentEngine::init(const dw_config_t* cfg) {
    if (initialized_) {
        return 0;
    }
    try {
        lt::settings_pack pack;
        // 仅订阅错误与状态更新，屏蔽 peer/piece/block 等细粒度事件
        pack.set_int(lt::settings_pack::alert_mask,
                     lt::alert_category::error | lt::alert_category::status);

        int listen_port = 0;
        if (cfg) {
            g_log_level  = cfg->log_level;
            g_interval_ms = (cfg->status_callback_interval_ms > 0)
                                ? cfg->status_callback_interval_ms : 1000;
            listen_port  = cfg->listen_port;
            if (cfg->download_rate_limit > 0) {
                pack.set_int(lt::settings_pack::download_rate_limit, cfg->download_rate_limit);
            }
            if (cfg->upload_rate_limit > 0) {
                pack.set_int(lt::settings_pack::upload_rate_limit, cfg->upload_rate_limit);
            }
        }
        if (listen_port > 0) {
            pack.set_str(lt::settings_pack::listen_interfaces,
                         "0.0.0.0:" + std::to_string(listen_port));
        }

        g_session = std::make_unique<lt::session>(std::move(pack));
    } catch (const std::exception& e) {
        tlogf(DW_LOG_ERROR, "[ERROR] 创建 session 失败: %s", e.what());
        return -1;
    }
    if (!g_session || !g_session->is_valid()) {
        tlogf(DW_LOG_ERROR, "[ERROR] session 创建后无效");
        g_session.reset();
        return -1;
    }

    g_running.store(true);
    g_alert_thread = std::thread(alert_loop);

    initialized_ = true;
    tlogf(DW_LOG_INFO, "[OK] BT 引擎初始化完成 interval=%dms", g_interval_ms);
    return 0;
}

void TorrentEngine::destroy() {
    if (!initialized_) {
        return;
    }
    // 先停止 alert 线程，再销毁 session
    g_running.store(false);
    if (g_alert_thread.joinable()) {
        g_alert_thread.join();
    }
    {
        std::lock_guard<std::mutex> lock(g_mtx);
        g_handles.clear();
    }
    g_session.reset();

    initialized_ = false;
    tlogf(DW_LOG_INFO, "[CLEANUP] BT 引擎已销毁");
}

int32_t TorrentEngine::add_task(const dw_task_params_t* params,
                                 dw_submit_result_t*     out_result) {
    if (!params || !params->task_id || !params->task_id[0]) {
        set_result(out_result, params ? params->task_id : nullptr,
                   DW_REASON_INVALID_INPUT, "task_id 为空");
        return -1;
    }
    if (!params->save_path || !params->save_path[0]) {
        set_result(out_result, params->task_id, DW_REASON_INVALID_INPUT, "save_path 为空");
        return -1;
    }
    if (!g_session || !g_session->is_valid()) {
        set_result(out_result, params->task_id, DW_REASON_INTERNAL, "session 无效");
        return -1;
    }

    const std::string key(params->task_id);

    // 幂等：已存在直接返回成功
    if (find_handle(key).is_valid()) {
        set_result(out_result, params->task_id, DW_REASON_NONE, nullptr);
        return 0;
    }

    lt::add_torrent_params atp;
    atp.save_path = params->save_path;

    // 来源优先级：resume_data > magnet_link > torrent_file > info_hash(task_id)
    bool source_ok = false;
    if (params->resume_data && params->resume_data_size > 0) {
        try {
            const lt::span<const char> buf(
                reinterpret_cast<const char*>(params->resume_data),
                static_cast<std::ptrdiff_t>(params->resume_data_size));
            atp = lt::read_resume_data(buf);
            atp.save_path = params->save_path;
            source_ok = true;
        } catch (const std::exception& e) {
            tlogf(DW_LOG_ERROR, "[ERROR] 解析 resume_data 失败: %s", e.what());
        }
    }
    if (!source_ok && params->magnet_link && params->magnet_link[0]) {
        lt::error_code ec;
        lt::parse_magnet_uri(params->magnet_link, atp, ec);
        if (ec) {
            tlogf(DW_LOG_ERROR, "[ERROR] 解析磁力链接失败: %s", ec.message().c_str());
        } else {
            source_ok = true;
        }
    }
    if (!source_ok && params->torrent_file && params->torrent_file[0]) {
        lt::error_code ec;
        auto ti = std::make_shared<lt::torrent_info>(params->torrent_file, ec);
        if (ec) {
            tlogf(DW_LOG_ERROR, "[ERROR] 加载 .torrent 文件失败: %s", ec.message().c_str());
        } else {
            atp.ti = ti;
            source_ok = true;
        }
    }
    if (!source_ok) {
        // 用 task_id 作为 info_hash 构造磁力链接
        lt::error_code ec;
        const std::string magnet = "magnet:?xt=urn:btih:" + key;
        lt::parse_magnet_uri(magnet, atp, ec);
        if (!ec) {
            source_ok = true;
        }
    }
    if (!source_ok) {
        set_result(out_result, params->task_id, DW_REASON_INVALID_INPUT, "无有效任务来源");
        return -1;
    }

    // trackers
    if (params->trackers && params->tracker_count > 0) {
        for (int i = 0; i < params->tracker_count; ++i) {
            if (params->trackers[i]) {
                atp.trackers.emplace_back(params->trackers[i]);
            }
        }
    }
    // web seeds
    if (params->url_seeds && params->url_seed_count > 0) {
        for (int i = 0; i < params->url_seed_count; ++i) {
            if (params->url_seeds[i] && params->url_seeds[i][0]) {
                atp.url_seeds.emplace_back(params->url_seeds[i]);
            }
        }
    }
    // 文件选择（仅在已有 torrent_info 时可确定文件数）
    if (params->file_indexes && params->file_index_size > 0 && atp.ti) {
        const int file_count = atp.ti->num_files();
        if (file_count > 0) {
            atp.file_priorities.resize(file_count, lt::dont_download);
            for (int i = 0; i < params->file_index_size; ++i) {
                if (const int idx = params->file_indexes[i]; idx >= 0 && idx < file_count) {
                    atp.file_priorities[idx] = lt::default_priority;
                }
            }
        }
    }

    // flags
    lt::torrent_flags_t flags = lt::torrent_flags::update_subscribe
                              | lt::torrent_flags::need_save_resume;
    if (params->auto_start) {
        flags |= lt::torrent_flags::auto_managed;
    } else {
        flags |= lt::torrent_flags::paused;
    }
    atp.flags = flags;

    lt::torrent_handle handle;
    try {
        handle = g_session->add_torrent(std::move(atp));
    } catch (const std::exception& e) {
        set_result(out_result, params->task_id, DW_REASON_INTERNAL, e.what());
        return -1;
    }
    if (!handle.is_valid()) {
        set_result(out_result, params->task_id, DW_REASON_INTERNAL, "add_torrent 返回无效句柄");
        return -1;
    }

    {
        std::lock_guard<std::mutex> lock(g_mtx);
        g_handles[key] = handle;
    }

    tlogf(DW_LOG_INFO, "[EVENT] BT 任务已添加 info_hash=%s auto_start=%d",
          key.c_str(), params->auto_start);
    set_result(out_result, params->task_id, DW_REASON_NONE, nullptr);
    return 0;
}

int32_t TorrentEngine::pause_task(const char*         id,
                                   dw_submit_result_t* out_result) {
    if (!id || !id[0]) {
        set_result(out_result, id, DW_REASON_INVALID_INPUT, "id 为空");
        return -1;
    }
    lt::torrent_handle handle = find_handle(std::string(id));
    if (!handle.is_valid()) {
        set_result(out_result, id, DW_REASON_INVALID_INPUT, "任务不存在");
        return -1;
    }
    try {
        handle.unset_flags(lt::torrent_flags::auto_managed);
        handle.pause();
    } catch (const std::exception& e) {
        set_result(out_result, id, DW_REASON_INTERNAL, e.what());
        return -1;
    }
    request_save_resume(handle); // 暂停时保存断点续传数据
    set_result(out_result, id, DW_REASON_NONE, nullptr);
    return 0;
}

int32_t TorrentEngine::resume_task(const dw_task_params_t* params,
                                    dw_submit_result_t*     out_result) {
    if (!params || !params->task_id || !params->task_id[0]) {
        set_result(out_result, params ? params->task_id : nullptr,
                   DW_REASON_INVALID_INPUT, "task_id 为空");
        return -1;
    }
    lt::torrent_handle handle = find_handle(std::string(params->task_id));
    // 任务不在 session 中：若带 resume_data / 来源，走 add 重新加入
    if (!handle.is_valid()) {
        return add_task(params, out_result);
    }
    try {
        handle.set_flags(lt::torrent_flags::auto_managed);
        handle.resume();
    } catch (const std::exception& e) {
        set_result(out_result, params->task_id, DW_REASON_INTERNAL, e.what());
        return -1;
    }
    set_result(out_result, params->task_id, DW_REASON_NONE, nullptr);
    return 0;
}

int32_t TorrentEngine::delete_task(const char*         id,
                                    dw_submit_result_t* out_result) {
    if (!id || !id[0]) {
        set_result(out_result, id, DW_REASON_INVALID_INPUT, "id 为空");
        return -1;
    }
    const std::string key(id);
    lt::torrent_handle handle = find_handle(key);
    if (!handle.is_valid()) {
        set_result(out_result, id, DW_REASON_INVALID_INPUT, "任务不存在");
        return -1;
    }
    try {
        if (g_session) {
            g_session->remove_torrent(handle);
        }
    } catch (const std::exception& e) {
        set_result(out_result, id, DW_REASON_INTERNAL, e.what());
        return -1;
    }
    {
        std::lock_guard<std::mutex> lock(g_mtx);
        g_handles.erase(key);
    }
    set_result(out_result, id, DW_REASON_NONE, nullptr);
    return 0;
}

char* TorrentEngine::magnet_to_info_hash(const char* magnet_link) {
    if (!magnet_link || !magnet_link[0]) {
        return nullptr;
    }
    lt::error_code ec;
    lt::add_torrent_params atp;
    lt::parse_magnet_uri(magnet_link, atp, ec);
    if (ec) {
        tlogf(DW_LOG_ERROR, "[ERROR] 解析磁力链接失败: %s", ec.message().c_str());
        return nullptr;
    }
    std::string s;
    if (atp.info_hashes.has_v2()) {
        s = lt::aux::to_hex(atp.info_hashes.v2);
    } else if (atp.info_hashes.has_v1()) {
        s = lt::aux::to_hex(atp.info_hashes.v1);
    } else {
        return nullptr;
    }
    auto* result = static_cast<char*>(std::malloc(s.size() + 1));
    if (result) std::memcpy(result, s.c_str(), s.size() + 1);
    return result;
}

char* TorrentEngine::torrent_file_to_info_hash(const char* torrent_file_path) {
    if (!torrent_file_path || !torrent_file_path[0]) {
        return nullptr;
    }
    lt::error_code ec;
    const auto ti = std::make_shared<lt::torrent_info>(torrent_file_path, ec);
    if (ec) {
        tlogf(DW_LOG_ERROR, "[ERROR] 加载 .torrent 文件失败: %s", ec.message().c_str());
        return nullptr;
    }
    const lt::info_hash_t ih = ti->info_hashes();
    std::string s;
    if (ih.has_v2()) {
        s = lt::aux::to_hex(ih.v2);
    } else if (ih.has_v1()) {
        s = lt::aux::to_hex(ih.v1);
    } else {
        return nullptr;
    }
    auto* result = static_cast<char*>(std::malloc(s.size() + 1));
    if (result) std::memcpy(result, s.c_str(), s.size() + 1);
    return result;
}

char* TorrentEngine::info_hash_to_magnet(const char* info_hash) {
    if (!info_hash || !info_hash[0]) {
        return nullptr;
    }
    lt::torrent_handle handle = find_handle(std::string(info_hash));
    if (!handle.is_valid()) {
        return nullptr;
    }
    try {
        const std::string magnet = lt::make_magnet_uri(handle);
        if (magnet.empty()) return nullptr;
        auto* result = static_cast<char*>(std::malloc(magnet.size() + 1));
        if (result) std::memcpy(result, magnet.c_str(), magnet.size() + 1);
        return result;
    } catch (const std::exception& e) {
        tlogf(DW_LOG_ERROR, "[ERROR] 生成磁力链接失败: %s", e.what());
        return nullptr;
    }
}

int TorrentEngine::set_file_priority(const char* info_hash,
                                     int32_t     file_index,
                                     int32_t     priority) {
    if (!info_hash || !info_hash[0]) {
        return 0;
    }
    lt::torrent_handle handle = find_handle(std::string(info_hash));
    if (!handle.is_valid()) {
        return 0;
    }
    try {
        handle.file_priority(lt::file_index_t(file_index),
                             lt::download_priority_t(static_cast<uint8_t>(priority)));
    } catch (const std::exception& e) {
        tlogf(DW_LOG_ERROR, "[ERROR] 设置文件优先级失败: %s", e.what());
        return 0;
    }
    return 1;
}

int32_t TorrentEngine::parse_torrent_file(const char*      torrent_file_path,
                                           char**           out_info_hash,
                                           dw_file_info_t** out_files,
                                           int32_t*         out_count) {
    if (!torrent_file_path || !out_info_hash || !out_files || !out_count) {
        return -1;
    }
    *out_info_hash = nullptr;
    *out_files     = nullptr;
    *out_count     = 0;

    lt::error_code ec;
    auto ti = std::make_shared<lt::torrent_info>(torrent_file_path, ec);
    if (ec) {
        tlogf(DW_LOG_ERROR, "[ERROR] 解析 .torrent 文件失败: %s", ec.message().c_str());
        return -1;
    }

    // info_hash
    const lt::info_hash_t& hashes = ti->info_hashes();
    std::string info_hash_str;
    if (hashes.has_v2()) {
        info_hash_str = lt::aux::to_hex(hashes.v2);
    } else if (hashes.has_v1()) {
        info_hash_str = lt::aux::to_hex(hashes.v1);
    } else {
        tlogf(DW_LOG_ERROR, "[ERROR] .torrent 文件无有效 info_hash");
        return -1;
    }
    *out_info_hash = static_cast<char*>(std::malloc(info_hash_str.size() + 1));
    if (!*out_info_hash) {
        return -1;
    }
    std::memcpy(*out_info_hash, info_hash_str.c_str(), info_hash_str.size() + 1);

    // 文件列表
    if (fill_file_list(ti, out_files, out_count) != 0) {
        std::free(*out_info_hash);
        *out_info_hash = nullptr;
        return -1;
    }
    return 0;
}

int32_t TorrentEngine::get_file_list(const char*      task_id,
                                      dw_file_info_t** out_files,
                                      int32_t*         out_count) {
    if (!task_id || !out_files || !out_count) {
        return -1;
    }
    *out_files = nullptr;
    *out_count = 0;

    lt::torrent_handle handle = find_handle(std::string(task_id));
    if (!handle.is_valid()) {
        return -1;
    }
    std::shared_ptr<const lt::torrent_info> ti;
    try {
        ti = handle.torrent_file();
    } catch (...) {
        return -1;
    }
    if (!ti) {
        return -1; // 元数据未就绪
    }
    return fill_file_list(ti, out_files, out_count);
}

} // namespace dw
