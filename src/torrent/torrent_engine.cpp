/**
 * @file torrent_engine.cpp
 * @brief BT/Torrent 下载引擎实现（基于 libtorrent）。
 *
 * 架构（事件驱动）：
 *   - 单例 lt::session 管理所有 BT 任务；
 *   - A 线程每拍经 post_updates 触发 post_torrent_updates + 携变更门槛的续传检查点；
 *     alert 线程消费 state_update_alert 后经 post_engine_event 投递 STATUS_UPDATE 事件，
 *     B 线程消费后写入 TaskRecord 内存，A 线程下一拍直接从 TaskRecord 字段采集进度；
 *   - alert 轮询线程另处理生命周期事件：完成时投递 DOWNLOAD_COMPLETED 并请求保存续传、
 *     阻断性错误投递 DOWNLOAD_FAILED、save_resume_data_alert 经 dw::post_resume_data 输出续传数据；
 *   - 只订阅 error / 完成 / 续传相关 alert，不处理 peer / piece / block 等细粒度事件。
 */

#include "torrent/torrent_engine.h"

#include "internal/downloader_internal.h"
#include "utils/string_util.h"
#include "utils/time_util.h"

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
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace lt = libtorrent;

namespace dw {
    using utils::now_unix_ms;

    /* ===================================================================== */
    /*                        文件内全局状态与辅助                            */
    /* ===================================================================== */
    namespace {
        // 单例 session
        std::unique_ptr<lt::session> g_session;
        // alert 轮询线程
        std::thread g_alert_thread;
        std::atomic<bool> g_running{false};
        // 进度回调间隔（ms）
        int g_interval_ms = 1000;
        // BT 做种分享率上限：total_upload/total_done 达到该值后释放做种上下文。
        // 默认 3.0（下载:上传=1:3）；init 从 cfg->seed_ratio_limit 读取（0=默认，<0=永久做种）。
        double g_seed_ratio_limit = 3.0;

        // 当前 Unix 毫秒时间戳（统一由 dw::utils::now_unix_ms 提供）

        // 从 torrent_handle 提取 info_hash hex（优先 v2，回退 v1）；无效返回空串
        std::string info_hash_hex(const lt::torrent_handle &h) {
            if (!h.is_valid()) return {};
            try {
                const lt::info_hash_t ih = h.info_hashes();
                return ih.has_v2() ? lt::aux::to_hex(ih.v2) : lt::aux::to_hex(ih.v1);
            } catch (...) {
                return {};
            }
        }

        // 将 torrent_status 映射为 dw_task_status_t（仅下载相关状态）
        dw_task_status_t map_status(const lt::torrent_status &s) {
            if (s.errc) {
                return DW_TASK_STATUS_ERROR;
            }
            const bool paused = (s.flags & lt::torrent_flags::paused) != lt::torrent_flags_t{};
            const bool auto_managed = (s.flags & lt::torrent_flags::auto_managed) != lt::torrent_flags_t{};
            if (paused && !auto_managed) {
                return DW_TASK_STATUS_PAUSED; // 用户主动暂停，优先级最高
            }
            // 磁力元数据未就绪（ut_metadata 拉取中），或元数据已就绪但尚未应用文件选择
            //（default_dont_download 使全部文件优先级为 0、total_wanted==0）：
            // 引擎视角均为解析态 RESOLVING；权威状态机（RESOLVING→DOWNLOADING/PAUSED
            // 迁移与名额管理）由 TaskManager 调度独占，本值仅供采集侧参考。
            // total_wanted==0 的判断必须早于 is_finished，否则会被 libtorrent 误判为完成。
            if (!s.has_metadata || s.total_wanted == 0) {
                return DW_TASK_STATUS_RESOLVING;
            }
            if (s.is_seeding || s.is_finished) {
                return DW_TASK_STATUS_COMPLETED;
            }
            return DW_TASK_STATUS_DOWNLOADING;
        }

        // 由 torrent_status 组装 STATUS_UPDATE 事件并经 post_engine_event 投递。
        // alert 线程消费 state_update_alert 时批量调用。
        EngineEvent make_status_update_event(const lt::torrent_status &s, const std::string &key) {
            EngineEvent ev;
            ev.type = EngineEventType::STATUS_UPDATE;
            ev.engine_key = key;
            ev.protocol = DW_PROTOCOL_TORRENT;
            ev.valid = true;
            ev.status = map_status(s);
            ev.total_size = s.total_wanted;
            ev.total_done = s.total_done;
            ev.progress = static_cast<double>(s.progress);
            ev.download_rate = static_cast<double>(s.download_payload_rate);
            ev.upload_rate = static_cast<double>(s.upload_payload_rate);
            ev.name = s.name;
            ev.reason = s.errc ? DW_REASON_NETWORK : DW_REASON_NONE;
            ev.message = s.errc ? s.errc.message() : std::string{};
            // 占位形态判据：多文件 torrent 的 name 是根目录名（占位须建目录），单文件
            // name 即文件名（占位须建空文件）。post_torrent_updates 默认
            // status_flags_t::all() 已填充 torrent_file，无需额外同步查询。
            if (s.has_metadata) {
                if (const auto ti = s.torrent_file.lock()) {
                    ev.multi_file = ti->num_files() > 1;
                }
            }
            return ev;
        }

        // 请求保存断点续传数据（异步，结果经 save_resume_data_alert 返回）。
        // force=true 时无条件保存一次（用于完成移出后固化最终 save_path）；否则携
        // 变更门槛标志：下载推进 / 配置变更（含改名）/ 暂停态变化 / 元数据变化才产生
        // alert，去重下沉 libtorrent，支撑每拍无节流调用；刻意排除 if_counters_changed
        //（活跃计时器每秒都在变，纳入会退化为每拍必存）。
        void request_save_resume(const lt::torrent_handle &h, bool force = false) {
            if (!h.is_valid()) return;
            // 元数据未获取完成前不保存 resume：磁力此时仅有 infohash，
            // 落库无实际续传价值，跳过以免无意义写入 / 覆盖有效续传。
            try {
                const lt::torrent_status st = h.status();
                if (!st.has_metadata) return;
            } catch (...) {
                return;
            }
            try {
                lt::resume_data_flags_t flags = lt::torrent_handle::save_info_dict;
                if (!force) {
                    flags |= lt::torrent_handle::if_download_progress
                            | lt::torrent_handle::if_config_changed
                            | lt::torrent_handle::if_state_changed
                            | lt::torrent_handle::if_metadata_changed;
                }
                h.save_resume_data(flags);
            } catch (const std::exception &e) {
                const std::string rsk = info_hash_hex(h);
                DW_LOG_TASK(DW_LOG_ERROR, rsk.c_str(), "[ERROR] 请求保存 resume_data 失败: %s", e.what());
            }
        }

        // 前向声明：扁平文件列表构建（供 PARSED 事件使用）。
        std::vector<dw_file_info_t> build_flat_file_list(
            const std::shared_ptr<const lt::torrent_info> &ti);

        // 从 torrent handle 构建 PARSED 事件（元数据就绪时调用）。
        // 返回空事件表示元数据未就绪或构建失败。
        EngineEvent build_parsed_event(const lt::torrent_handle &h) {
            EngineEvent ev;
            ev.type = EngineEventType::PARSED;
            ev.protocol = DW_PROTOCOL_TORRENT;
            if (!h.is_valid()) return ev;
            ev.engine_key = info_hash_hex(h);
            if (ev.engine_key.empty()) return ev;
            try {
                const lt::torrent_status st = h.status();
                std::shared_ptr<const lt::torrent_info> ti;
                ti = h.torrent_file();
                if (!ti) return ev; // 元数据未就绪
                ev.name = ti->name();
                ev.save_path = st.save_path;
                ev.multi_file = (ti->num_files() > 1) ||
                                (ti->num_files() == 1 && ti->files().file_path(lt::file_index_t{0}).find('/') !=
                                 std::string::npos);
                ev.files = build_flat_file_list(ti);
            } catch (...) {
                ev.engine_key.clear(); // 异常时返回空事件
            }
            return ev;
        }

        // 处理单个 alert：推模型下关注生命周期、阻断性错误与续传事件，进度由
        // state_update_alert 推入 TaskManager 内存。元数据就绪经 PARSED 事件投递，
        // 冲突检测与定名决策归 TaskManager 事件消费。
        void handle_alert(const lt::alert *a) {
            if (!a) return;

            // 进度推送：批量 torrent_status 经 STATUS_UPDATE 事件投递到 B 线程。
            if (const auto *su = lt::alert_cast<lt::state_update_alert>(a)) {
                for (const lt::torrent_status &s: su->status) {
                    const std::string key = info_hash_hex(s.handle);
                    if (key.empty()) continue;
                    EngineEvent ev = make_status_update_event(s, key);
                    post_engine_event(std::move(ev));
                }
                return;
            }

            // 任务添加成功（.torrent / 续传恢复）：已有元数据则投递 PARSED 事件；
            // 磁力此刻无元数据，事件构建自然跳过，待 metadata_received 到达后投递。
            if (const auto *at = lt::alert_cast<lt::add_torrent_alert>(a)) {
                if (!at->error) {
                    EngineEvent ev = build_parsed_event(at->handle);
                    if (!ev.engine_key.empty()) {
                        post_engine_event(std::move(ev));
                    }
                }
                return;
            }
            // 磁力元数据就绪：投递 PARSED 事件（含名称、路径、文件列表）。
            if (const auto *mr = lt::alert_cast<lt::metadata_received_alert>(a)) {
                EngineEvent ev = build_parsed_event(mr->handle);
                if (!ev.engine_key.empty()) {
                    post_engine_event(std::move(ev));
                }
                return;
            }
            // 下载完成：投递 DOWNLOAD_COMPLETED 事件，并请求保存一次续传数据。
            if (const auto *tf = lt::alert_cast<lt::torrent_finished_alert>(a)) {
                const std::string key = info_hash_hex(tf->handle);
                if (!key.empty()) {
                    EngineEvent ev;
                    ev.type = EngineEventType::DOWNLOAD_COMPLETED;
                    ev.engine_key = key;
                    ev.protocol = DW_PROTOCOL_TORRENT;
                    post_engine_event(std::move(ev));
                }
                request_save_resume(tf->handle);
                return;
            }
            // 任务错误（阻断性）：投递 DOWNLOAD_FAILED 事件。
            if (const auto *te = lt::alert_cast<lt::torrent_error_alert>(a)) {
                const std::string key = info_hash_hex(te->handle);
                DW_LOG_TASK(DW_LOG_ERROR, key.c_str(), "[ERROR] 任务错误 info_hash=%s msg=%s",
                            key.c_str(), te->error.message().c_str());
                if (!key.empty()) {
                    EngineEvent ev;
                    ev.type = EngineEventType::DOWNLOAD_FAILED;
                    ev.engine_key = key;
                    ev.protocol = DW_PROTOCOL_TORRENT;
                    ev.reason = DW_REASON_NETWORK;
                    ev.message = te->error.message();
                    post_engine_event(std::move(ev));
                }
                return;
            }
            // 存储读写失败（阻断性）：投递 DOWNLOAD_FAILED 事件。
            if (const auto *fe = lt::alert_cast<lt::file_error_alert>(a)) {
                const std::string key = info_hash_hex(fe->handle);
                DW_LOG_TASK(DW_LOG_ERROR, key.c_str(), "[ERROR] 存储错误 file=%s msg=%s",
                            fe->filename(), fe->error.message().c_str());
                if (!key.empty()) {
                    EngineEvent ev;
                    ev.type = EngineEventType::DOWNLOAD_FAILED;
                    ev.engine_key = key;
                    ev.protocol = DW_PROTOCOL_TORRENT;
                    ev.reason = DW_REASON_ERROR;
                    ev.message = (fe->error.value() == ENOSPC)
                                     ? "存储空间不足"
                                     : "存储异常，无法保存文件";
                    post_engine_event(std::move(ev));
                }
                return;
            }
            // 断点续传数据就绪：输出给上层持久化
            if (const auto *rd = lt::alert_cast<lt::save_resume_data_alert>(a)) {
                const std::string key = info_hash_hex(rd->handle);
                if (!key.empty()) {
                    try {
                        const std::vector<char> buf = lt::write_resume_data_buf(rd->params);
                        if (!buf.empty()) {
                            post_resume_data(key.c_str(), DW_PROTOCOL_TORRENT,
                                             reinterpret_cast<const uint8_t *>(buf.data()),
                                             buf.size());
                        }
                    } catch (const std::exception &e) {
                        DW_LOG_TASK(DW_LOG_ERROR, key.c_str(), "[ERROR] 写入 resume_data 失败: %s", e.what());
                    }
                }
                return;
            }
            // 存储路径迁移完成：投递 PARSED 事件（携带最新 save_path）。
            if (const auto *sm = lt::alert_cast<lt::storage_moved_alert>(a)) {
                const std::string key = info_hash_hex(sm->handle);
                if (key.empty()) return;
                DW_LOG_TASK(DW_LOG_INFO, key.c_str(), "[OK] 存储迁移完成: -> '%s'",
                            sm->storage_path());
                EngineEvent ev;
                ev.type = EngineEventType::PARSED;
                ev.engine_key = key;
                ev.protocol = DW_PROTOCOL_TORRENT;
                ev.save_path = sm->storage_path();
                post_engine_event(std::move(ev));
                return;
            }
            // 迁移失败：投递 DOWNLOAD_FAILED 事件。
            if (const auto *smf = lt::alert_cast<lt::storage_moved_failed_alert>(a)) {
                const std::string key = info_hash_hex(smf->handle);
                if (key.empty()) return;
                DW_LOG_TASK(DW_LOG_ERROR, key.c_str(), "[ERROR] move_storage 失败: %s",
                            smf->error.message().c_str());
                EngineEvent ev;
                ev.type = EngineEventType::DOWNLOAD_FAILED;
                ev.engine_key = key;
                ev.protocol = DW_PROTOCOL_TORRENT;
                ev.reason = DW_REASON_ERROR;
                ev.message = (smf->error.value() == ENOSPC)
                                 ? "存储空间不足"
                                 : "存储异常";
                post_engine_event(std::move(ev));
                return;
            }
            // 其余 alert（含 save_resume_data_failed 等）忽略
        }

        // alert 轮询线程主循环
        void alert_loop() {
            while (g_running.load()) {
                try {
                    if (g_session) {
                        // post_torrent_updates 由 A 线程按节拍触发；本线程仅等待并处理
                        // 会话 alert：state_update（推入进度）、生命周期与续传 alert。
                        g_session->wait_for_alert(std::chrono::milliseconds(g_interval_ms));
                        std::vector<lt::alert *> alerts;
                        g_session->pop_alerts(&alerts);
                        for (const lt::alert *a: alerts) {
                            handle_alert(a);
                        }
                    } else {
                        std::this_thread::sleep_for(std::chrono::milliseconds(g_interval_ms));
                    }
                } catch (const std::exception &e) {
                    DW_LOG_SYS(DW_LOG_ERROR, "[ERROR] alert_loop 异常: %s", e.what());
                } catch (...) {
                    DW_LOG_SYS(DW_LOG_ERROR, "[ERROR] alert_loop 未知异常");
                }
            }
        }

        // 从 info_hash hex 字符串查找 handle（直接查询 session，无需本地缓存）。
        lt::torrent_handle find_handle(const std::string &info_hash) {
            if (!g_session || info_hash.empty()) return {};
            lt::sha1_hash h;
            if (lt::aux::from_hex(info_hash, h.data())) {
                return g_session->find_torrent(h);
            }
            const auto handles = g_session->get_torrents();
            for (const auto &th: handles) {
                if (info_hash_hex(th) == info_hash) return th;
            }
            return {};
        }

        // 填充文件列表到 C 数组（供 parse / get_file_list 复用）
        int32_t fill_file_list(const std::shared_ptr<const lt::torrent_info> &ti,
                               dw_file_info_t **out_files, int32_t *out_count) {
            const lt::file_storage &fs = ti->files();
            const int file_count = fs.num_files();
            *out_count = 0;
            *out_files = static_cast<dw_file_info_t *>(
                std::calloc(static_cast<size_t>(file_count > 0 ? file_count : 1), sizeof(dw_file_info_t)));
            if (!*out_files) {
                return -1;
            }
            for (int i = 0; i < file_count; ++i) {
                const lt::file_index_t idx{i};
                (*out_files)[i].index = i;
                const std::string path = fs.file_path(idx);
                (*out_files)[i].name = static_cast<char *>(std::malloc(path.size() + 1));
                if ((*out_files)[i].name) {
                    std::memcpy((*out_files)[i].name, path.c_str(), path.size() + 1);
                }
                (*out_files)[i].size = fs.file_size(idx);
                (*out_files)[i].offset = fs.file_offset(idx);
                const std::string ext = dw::utils::file_extension(path);
                (*out_files)[i].ext = ext.empty() ? nullptr : static_cast<char *>(std::malloc(ext.size() + 1));
                if ((*out_files)[i].ext) {
                    std::memcpy(const_cast<char*>((*out_files)[i].ext), ext.c_str(), ext.size() + 1);
                }
            }
            *out_count = file_count;
            return 0;
        }

        // 扁平文件列表：每个文件一行，name 为相对路径（含目录），不建文件夹节点。
        // 返回的各节点字符串字段由 std::malloc 分配，调用方负责释放。
        std::vector<dw_file_info_t> build_flat_file_list(
            const std::shared_ptr<const lt::torrent_info> &ti) {
            std::vector<dw_file_info_t> files;
            const lt::file_storage &fs = ti->files();
            const int file_count = fs.num_files();
            if (file_count <= 0) return files;
            files.reserve(file_count);

            auto dup = [](const std::string &s) -> char * {
                auto *p = static_cast<char *>(std::malloc(s.size() + 1));
                if (p) std::memcpy(p, s.c_str(), s.size() + 1);
                return p;
            };

            for (int i = 0; i < file_count; ++i) {
                const lt::file_index_t idx{i};
                const std::string path = fs.file_path(idx);
                dw_file_info_t f{};
                f.index = i;
                f.size = fs.file_size(idx);
                f.offset = fs.file_offset(idx);
                f.name = dup(path);
                const std::string ext = dw::utils::file_extension(path);
                f.ext = ext.empty() ? nullptr : dup(ext);
                f.status = 0; // 下载中
                f.downloaded_bytes = 0; // PARSED 时刻无 piece 下载，初始为 0
                f.play_position_ms = 0;
                files.push_back(f);
            }
            return files;
        }

        // 设置同步返回结果，message 由 malloc 分配（成功时为 nullptr）。
        // 非 NONE 时自动输出日志，便于追踪错误链路。
        void set_result(dw_submit_result_t *r, const char *task_id,
                        dw_reason_t code, const char *msg) {
            // task_id 仅用于日志 trace；dw_submit_result_t 不再回传字符串标识。
            r->code = code;
            if (msg) {
                const size_t n = std::strlen(msg);
                auto *p = static_cast<char *>(std::malloc(n + 1));
                if (p) std::memcpy(p, msg, n + 1);
                r->message = p;
            } else {
                r->message = nullptr;
            }
            if (code != DW_REASON_NONE) {
                DW_LOG_TASK(DW_LOG_ERROR, task_id ? task_id : "",
                            "[ERROR] set_result code=%d msg=%s", code, msg ? msg : "");
            }
        }
    } // namespace（匿名）

    /* ===================================================================== */
    /*                        TorrentEngine 成员实现                          */
    /* ===================================================================== */

    TorrentEngine::TorrentEngine() = default;

    TorrentEngine::~TorrentEngine() {
        if (initialized_) {
            destroy();
        }
    }

    int32_t TorrentEngine::init(const dw_config_t *cfg) {
        if (initialized_) {
            return 0;
        }
        try {
            lt::settings_pack pack;
            // 仅订阅错误、状态更新与存储（元数据就绪改名的 rename 事件），屏蔽 peer/piece/block 等细粒度事件
            pack.set_int(lt::settings_pack::alert_mask,
                         lt::alert_category::error | lt::alert_category::status
                         | lt::alert_category::storage);

            int listen_port = 0;
            if (cfg) {
                g_interval_ms = (cfg->status_callback_interval_ms > 1000)
                                    ? cfg->status_callback_interval_ms
                                    : 1000;
                listen_port = cfg->listen_port;
                if (cfg->download_rate_limit > 0) {
                    pack.set_int(lt::settings_pack::download_rate_limit, cfg->download_rate_limit);
                }
                if (cfg->upload_rate_limit > 0) {
                    pack.set_int(lt::settings_pack::upload_rate_limit, cfg->upload_rate_limit);
                }
                // 做种分享率上限：0 保持库内默认 3.0，非 0（含负数=永久做种）以配置为准。
                if (cfg->seed_ratio_limit != 0.0) {
                    g_seed_ratio_limit = cfg->seed_ratio_limit;
                }
            }
            if (listen_port > 0) {
                pack.set_str(lt::settings_pack::listen_interfaces,
                             "0.0.0.0:" + std::to_string(listen_port));
            }

            g_session = std::make_unique<lt::session>(std::move(pack));
        } catch (const std::exception &e) {
            DW_LOG_SYS(DW_LOG_ERROR, "[ERROR] 创建 session 失败: %s", e.what());
            return -1;
        }
        if (!g_session || !g_session->is_valid()) {
            DW_LOG_SYS(DW_LOG_ERROR, "[ERROR] session 创建后无效");
            g_session.reset();
            return -1;
        }

        g_running.store(true);
        g_alert_thread = std::thread(alert_loop);

        initialized_ = true;
        DW_LOG_SYS(DW_LOG_INFO, "[OK] BT 引擎初始化完成 interval=%dms", g_interval_ms);
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
        g_session.reset();

        initialized_ = false;
        DW_LOG_SYS(DW_LOG_INFO, "[CLEANUP] BT 引擎已销毁");
    }

    int32_t TorrentEngine::add_task(const dw_task_params_t *params,
                                    dw_submit_result_t *out_result) {
        DW_LOG_TASK(DW_LOG_DEBUG, params ? params->info_hash : "", "[EVENT] add_task 进入");
        if (!params || !params->info_hash || !params->info_hash[0]) {
            set_result(out_result, params ? params->info_hash : nullptr,
                       DW_REASON_ERROR, "info_hash 为空");
            return -1;
        }
        if (!params->save_path || !params->save_path[0]) {
            set_result(out_result, params->info_hash, DW_REASON_ERROR, "save_path 为空");
            return -1;
        }
        if (!g_session || !g_session->is_valid()) {
            set_result(out_result, params->info_hash, DW_REASON_ERROR, "session 无效");
            return -1;
        }

        const std::string key(params->info_hash);

        // 幂等：任务已存在。若处于暂停态（如网络闸门挂起），恢复其运行以支持调度
        // 重新准入；不触碰 default_dont_download，保留既有文件选择（待选态仍待选）。
        if (lt::torrent_handle exist = find_handle(key); exist.is_valid()) {
            try {
                exist.set_flags(lt::torrent_flags::auto_managed);
                exist.resume();
            } catch (const std::exception &e) {
                DW_LOG_TASK(DW_LOG_ERROR, key.c_str(), "[ERROR] 幂等 resume 失败: %s", e.what());
            }
            DW_LOG_TASK(DW_LOG_INFO, key.c_str(), "[EVENT] add_task 任务已存在，恢复运行（幂等）");
            set_result(out_result, params->info_hash, DW_REASON_NONE, nullptr);
            return 0;
        }

        lt::add_torrent_params atp;
        // 来源优先级：resume_data > magnet_link > torrent_file > info_hash
        bool source_ok = false;
        if (params->resume_data && params->resume_data_size > 0) {
            try {
                const lt::span<const char> buf(
                    reinterpret_cast<const char *>(params->resume_data),
                    static_cast<std::ptrdiff_t>(params->resume_data_size));
                atp = lt::read_resume_data(buf);
                source_ok = true;
            } catch (const std::exception &e) {
                DW_LOG_TASK(DW_LOG_ERROR, key.c_str(), "[ERROR] 解析 resume_data 失败: %s", e.what());
            }
        }
        // resume_data 自带 save_path（含定名包层迁移后的目录），续传天然复用；
        // 新任务直接落最终目录：根名判重由 TaskManager 调度在 RESOLVING 校验拍
        // 完成（重名时引擎经 move_storage 把 save_path 迁至包层目录）。
        const bool from_resume = source_ok;
        if (!from_resume) {
            atp.save_path = params->save_path;
        }
        if (!source_ok && params->magnet_link && params->magnet_link[0]) {
            lt::error_code ec;
            lt::parse_magnet_uri(params->magnet_link, atp, ec);
            if (ec) {
                DW_LOG_TASK(DW_LOG_ERROR, key.c_str(), "[ERROR] 解析磁力链接失败: %s", ec.message().c_str());
            } else {
                source_ok = true;
            }
        }
        if (!source_ok && params->torrent_file && params->torrent_file[0]) {
            lt::error_code ec;
            auto ti = std::make_shared<lt::torrent_info>(params->torrent_file, ec);
            if (ec) {
                DW_LOG_TASK(DW_LOG_ERROR, key.c_str(), "[ERROR] 加载 .torrent 文件失败: %s", ec.message().c_str());
            } else {
                atp.ti = ti;
                source_ok = true;
            }
        }
        if (!source_ok) {
            // 用 key（info_hash 十六进制串）构造磁力链接
            lt::error_code ec;
            const std::string magnet = "magnet:?xt=urn:btih:" + key;
            lt::parse_magnet_uri(magnet, atp, ec);
            if (!ec) {
                source_ok = true;
            }
        }
        if (!source_ok) {
            set_result(out_result, params->info_hash, DW_REASON_ERROR, "无有效任务来源");
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

        // default_dont_download + auto_managed：接 swarm 拿元数据、零 payload 下载，
        // 真正开下由 apply_file_selection 显式定型优先级并解除 default_dont_download。
        atp.flags = lt::torrent_flags::update_subscribe
                    | lt::torrent_flags::need_save_resume
                    | lt::torrent_flags::default_dont_download
                    | lt::torrent_flags::auto_managed;

        lt::torrent_handle handle;
        try {
            handle = g_session->add_torrent(std::move(atp));
        } catch (const std::exception &e) {
            set_result(out_result, params->info_hash, DW_REASON_ERROR, e.what());
            return -1;
        }
        if (!handle.is_valid()) {
            set_result(out_result, params->info_hash, DW_REASON_ERROR, "add_torrent 返回无效句柄");
            return -1;
        }

        DW_LOG_TASK(DW_LOG_INFO, key.c_str(), "[EVENT] add_task 成功");
        set_result(out_result, params->info_hash, DW_REASON_NONE, nullptr);
        return 0;
    }

    int32_t TorrentEngine::pause_task(const char *task_id,
                                      dw_submit_result_t *out_result) {
        DW_LOG_TASK(DW_LOG_DEBUG, task_id ? task_id : "", "[EVENT] pause_task 进入");
        if (!task_id || !task_id[0]) {
            set_result(out_result, task_id, DW_REASON_ERROR, "task_id 为空");
            return -1;
        }
        const lt::torrent_handle handle = find_handle(std::string(task_id));
        if (handle.is_valid()) {
            try {
                handle.unset_flags(lt::torrent_flags::auto_managed);
                handle.pause();
            } catch (const std::exception &e) {
                set_result(out_result, task_id, DW_REASON_ERROR, e.what());
                return -1;
            }
            request_save_resume(handle);
            DW_LOG_TASK(DW_LOG_INFO, task_id, "[EVENT] pause_task 成功");
        }
        set_result(out_result, task_id, DW_REASON_NONE, nullptr);
        return 0;
    }

    int32_t TorrentEngine::delete_task(const char *task_id,
                                       dw_submit_result_t *out_result) {
        DW_LOG_TASK(DW_LOG_DEBUG, task_id ? task_id : "", "[EVENT] delete_task 进入");
        if (!task_id || !task_id[0]) {
            set_result(out_result, task_id, DW_REASON_ERROR, "task_id 为空");
            return -1;
        }
        const std::string key(task_id);
        const lt::torrent_handle handle = find_handle(key);
        if (handle.is_valid()) {
            // 统一删除模型（与 HTTP 对齐）：此处仅移出 session 释放运行时资源，
            // 存储句柄由 disk-io 线程异步关闭；落盘文件删除由 TaskManager 经
            // task_released 确认移除完成后按配置执行。
            try {
                if (g_session) {
                    g_session->remove_torrent(handle);
                }
            } catch (const std::exception &e) {
                set_result(out_result, task_id, DW_REASON_ERROR, e.what());
                return -1;
            }
            set_result(out_result, task_id, DW_REASON_NONE, nullptr);
            DW_LOG_TASK(DW_LOG_INFO, key.c_str(), "[CLEANUP] delete_task 已移出会话");
            return 0;
        }
        // 任务不在会话（未准入 / 分享率回收后）：无运行时资源。
        DW_LOG_TASK(DW_LOG_INFO, key.c_str(), "[EVENT] delete_task 任务不在会话");
        set_result(out_result, task_id, DW_REASON_NONE, nullptr);
        return 1;
    }

    bool TorrentEngine::task_released(const char *task_id) {
        if (!task_id || !task_id[0]) return true;
        // session 已销毁 / 未初始化：全部存储句柄已关闭，视为已释放。
        if (!initialized_ || !g_session) return true;
        // remove_torrent 后 handle 在 libtorrent 内部任务（disk-io 等）释放引用
        // 前仍有效；find_handle 失效即代表移除收敛、文件句柄已关闭。
        return !find_handle(std::string(task_id)).is_valid();
    }

    void TorrentEngine::sweep() {
        if (!initialized_ || !g_session) return;

        if (g_seed_ratio_limit < 0.0) return;
        const auto handles = g_session->get_torrents();
        for (const auto &handle: handles) {
            if (!handle.is_valid()) continue;
            lt::torrent_status s;
            try { s = handle.status(); } catch (...) { continue; }
            if (!s.is_seeding || s.total_done <= 0) continue; // 仅做种中任务；规避除零
            const double ratio = static_cast<double>(s.total_upload)
                                 / static_cast<double>(s.total_done);
            if (ratio >= g_seed_ratio_limit) {
                const std::string key = info_hash_hex(handle);
                try {
                    g_session->remove_torrent(handle);
                    DW_LOG_TASK(DW_LOG_INFO, key.c_str(), "[CLEANUP] 分享率达标释放做种上下文 key=%s", key.c_str());
                } catch (const std::exception &e) {
                    DW_LOG_TASK(DW_LOG_ERROR, key.c_str(), "[ERROR] sweep remove_torrent 失败 key=%s msg=%s",
                                key.c_str(), e.what());
                }
            }
        }
    }

    int32_t TorrentEngine::move_storage(const char *task_id, const char *new_save_path) {
        if (!task_id || !task_id[0] || !new_save_path || !new_save_path[0]) return -1;
        const std::string key(task_id);
        const lt::torrent_handle h = find_handle(key);
        if (!h.is_valid()) return -1;

        // 已一致则空操作（去尾分隔符后比较，容忍书写差异），供调度幂等重入校验。
        const auto trim_sep = [](std::string s) {
            while (s.size() > 1 && (s.back() == '/' || s.back() == '\\')) s.pop_back();
            return s;
        };
        try {
            const std::string cur =
                    h.status(lt::torrent_handle::query_save_path).save_path;
            if (trim_sep(cur) == trim_sep(new_save_path)) return 0;
        } catch (...) { return -1; }

        // 零字节落盘时段（default_dont_download）move_storage 仅创建目标目录并改
        // 内部 save_path，storage_moved_alert 异步收敛后投递 PARSED 事件通知调度侧放行。
        try {
            h.move_storage(std::string(new_save_path));
        } catch (const std::exception &e) {
            DW_LOG_TASK(DW_LOG_ERROR, key.c_str(), "[ERROR] 包层迁移发起失败: %s", e.what());
            return -1;
        }
        DW_LOG_TASK(DW_LOG_INFO, key.c_str(), "[EVENT] 包层迁移发起: -> '%s'", new_save_path);
        return 1;
    }

    void TorrentEngine::post_updates() {
        // 节拍入口（A 线程调用）：
        //   1) post_torrent_updates：触发引擎收集变更任务状态，结果经 state_update_alert
        //      异步回 alert 线程，由 handle_alert 投递 STATUS_UPDATE 事件；
        //   2) 续传检查点：对有元数据任务请求 save_resume_data（携变更门槛，无变化
        //      不产生 alert），结果经 save_resume_data_alert → post_resume_data 输出。
        if (!g_session) return;
        try { g_session->post_torrent_updates(); } catch (...) {
        }
        std::vector<lt::torrent_handle> handles;
        try { handles = g_session->get_torrents(); } catch (...) { return; }
        for (const auto &h: handles) {
            request_save_resume(h);
        }
    }

    char *TorrentEngine::magnet_to_info_hash(const char *magnet_link) {
        if (!magnet_link || !magnet_link[0]) {
            return nullptr;
        }
        lt::error_code ec;
        lt::add_torrent_params atp;
        lt::parse_magnet_uri(magnet_link, atp, ec);
        if (ec) {
            DW_LOG_SYS(DW_LOG_ERROR, "[ERROR] 解析磁力链接失败: %s", ec.message().c_str());
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
        auto *result = static_cast<char *>(std::malloc(s.size() + 1));
        if (result) std::memcpy(result, s.c_str(), s.size() + 1);
        return result;
    }

    char *TorrentEngine::torrent_file_to_info_hash(const char *torrent_file_path) {
        if (!torrent_file_path || !torrent_file_path[0]) {
            return nullptr;
        }
        lt::error_code ec;
        const auto ti = std::make_shared<lt::torrent_info>(torrent_file_path, ec);
        if (ec) {
            DW_LOG_SYS(DW_LOG_ERROR, "[ERROR] 加载 .torrent 文件失败: %s", ec.message().c_str());
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
        auto *result = static_cast<char *>(std::malloc(s.size() + 1));
        if (result) std::memcpy(result, s.c_str(), s.size() + 1);
        return result;
    }

    char *TorrentEngine::info_hash_to_magnet(const char *task_id) {
        DW_LOG_TASK(DW_LOG_DEBUG, task_id ? task_id : "", "[EVENT] info_hash_to_magnet 进入");
        if (!task_id || !task_id[0]) {
            return nullptr;
        }
        lt::torrent_handle handle = find_handle(std::string(task_id));
        if (!handle.is_valid()) {
            DW_LOG_TASK(DW_LOG_ERROR, task_id, "[ERROR] info_hash_to_magnet 任务不存在");
            return nullptr;
        }
        try {
            const std::string magnet = lt::make_magnet_uri(handle);
            if (magnet.empty()) return nullptr;
            auto *result = static_cast<char *>(std::malloc(magnet.size() + 1));
            if (result) std::memcpy(result, magnet.c_str(), magnet.size() + 1);
            DW_LOG_TASK(DW_LOG_INFO, task_id, "[EVENT] info_hash_to_magnet 成功");
            return result;
        } catch (const std::exception &e) {
            DW_LOG_TASK(DW_LOG_ERROR, task_id, "[ERROR] 生成磁力链接失败: %s", e.what());
            return nullptr;
        }
    }

    int TorrentEngine::set_file_priority(const char *task_id,
                                         int32_t file_index,
                                         int32_t priority) {
        DW_LOG_TASK(DW_LOG_DEBUG, task_id ? task_id : "",
                    "[EVENT] set_file_priority 进入 file_index=%d priority=%d", file_index, priority);
        if (!task_id || !task_id[0]) {
            return 0;
        }
        lt::torrent_handle handle = find_handle(std::string(task_id));
        if (!handle.is_valid()) {
            DW_LOG_TASK(DW_LOG_ERROR, task_id, "[ERROR] set_file_priority 任务不存在");
            return 0;
        }
        try {
            handle.file_priority(lt::file_index_t(file_index),
                                 lt::download_priority_t(static_cast<uint8_t>(priority)));
            handle.set_flags(lt::torrent_flags::auto_managed);
            handle.resume();
        } catch (const std::exception &e) {
            DW_LOG_TASK(DW_LOG_ERROR, task_id, "[ERROR] 设置文件优先级失败: %s", e.what());
            return 0;
        }
        DW_LOG_TASK(DW_LOG_INFO, task_id,
                    "[EVENT] set_file_priority 成功 file_index=%d priority=%d", file_index, priority);
        return 1;
    }

    int32_t TorrentEngine::parse_torrent_file(const char *torrent_file_path,
                                              char **out_name,
                                              char **out_info_hash,
                                              dw_file_info_t **out_files,
                                              int32_t *out_count) {
        if (!torrent_file_path || !out_name || !out_info_hash || !out_files || !out_count) {
            return -1;
        }
        *out_name = nullptr;
        *out_info_hash = nullptr;
        *out_files = nullptr;
        *out_count = 0;

        lt::error_code ec;
        auto ti = std::make_shared<lt::torrent_info>(torrent_file_path, ec);
        if (ec) {
            DW_LOG_SYS(DW_LOG_ERROR, "[ERROR] 解析 .torrent 文件失败: %s", ec.message().c_str());
            return -1;
        }

        // 种子名称
        const std::string name = ti->name();
        *out_name = static_cast<char *>(std::malloc(name.size() + 1));
        if (!*out_name) {
            return -1;
        }
        std::memcpy(*out_name, name.c_str(), name.size() + 1);

        // info_hash
        const lt::info_hash_t &hashes = ti->info_hashes();
        std::string info_hash_str;
        if (hashes.has_v2()) {
            info_hash_str = lt::aux::to_hex(hashes.v2);
        } else if (hashes.has_v1()) {
            info_hash_str = lt::aux::to_hex(hashes.v1);
        } else {
            DW_LOG_SYS(DW_LOG_ERROR, "[ERROR] .torrent 文件无有效 info_hash");
            std::free(*out_name);
            *out_name = nullptr;
            return -1;
        }
        *out_info_hash = static_cast<char *>(std::malloc(info_hash_str.size() + 1));
        if (!*out_info_hash) {
            std::free(*out_name);
            *out_name = nullptr;
            return -1;
        }
        std::memcpy(*out_info_hash, info_hash_str.c_str(), info_hash_str.size() + 1);

        // 文件列表
        if (fill_file_list(ti, out_files, out_count) != 0) {
            std::free(*out_name);
            *out_name = nullptr;
            std::free(*out_info_hash);
            *out_info_hash = nullptr;
            return -1;
        }
        return 0;
    }

    // Piece→文件进度映射：从 piece bitmap 计算每个文件的已下载字节数。
    // 处理跨 piece（首尾 piece 部分重叠文件边界）和跨文件（一个 piece 跨越两个文件）。
    // 返回 vector 长度等于 file_count，每项为对应文件的已下载字节数。
    static std::vector<int64_t> calc_per_file_downloaded(
        const lt::file_storage &fs,
        const lt::typed_bitfield<lt::piece_index_t> &pieces) {

        const int file_count = fs.num_files();
        const int64_t piece_len = fs.piece_length();
        const int64_t total_size = fs.total_size();
        std::vector<int64_t> result(file_count, 0);
        if (file_count <= 0 || piece_len <= 0 || total_size <= 0) {
            return result;
        }

        for (int i = 0; i < file_count; ++i) {
            const lt::file_index_t idx{i};
            const int64_t f_off = fs.file_offset(idx);
            const int64_t f_size = fs.file_size(idx);
            if (f_size <= 0) continue;

            const int64_t f_end = f_off + f_size - 1; // 文件末字节（含）
            const int first_piece = static_cast<int>(f_off / piece_len);
            const int last_piece = static_cast<int>(f_end / piece_len);

            int64_t downloaded = 0;
            for (int p = first_piece; p <= last_piece; ++p) {
                const lt::piece_index_t pi{p};
                if (pi >= pieces.end_index() || !pieces.get_bit(pi)) {
                    continue; // 该 piece 未下载
                }
                // piece 在全局字节流的窗口
                const int64_t piece_start = static_cast<int64_t>(p) * piece_len;
                int64_t piece_end = piece_start + piece_len - 1;
                // 最后一个 piece 可能不足 piece_len
                if (piece_end >= total_size) {
                    piece_end = total_size - 1;
                }
                // 裁剪到文件窗口（处理跨文件边界）
                const int64_t seg_start = (piece_start > f_off) ? piece_start : f_off;
                const int64_t seg_end = (piece_end < f_end) ? piece_end : f_end;
                if (seg_end >= seg_start) {
                    downloaded += seg_end - seg_start + 1;
                }
            }
            result[i] = downloaded;
        }
        return result;
    }

    int32_t TorrentEngine::get_file_list(const char *task_id,
                                         dw_file_info_t **out_files,
                                         int32_t *out_count) {
        DW_LOG_TASK(DW_LOG_DEBUG, task_id ? task_id : "", "[EVENT] get_file_list 进入");
        if (!task_id || !out_files || !out_count) {
            return -1;
        }
        *out_files = nullptr;
        *out_count = 0;

        lt::torrent_handle handle = find_handle(std::string(task_id));
        if (!handle.is_valid()) {
            DW_LOG_TASK(DW_LOG_ERROR, task_id, "[ERROR] get_file_list 任务不存在");
            return -1;
        }
        std::shared_ptr<const lt::torrent_info> ti;
        try {
            ti = handle.torrent_file();
        } catch (...) {
            DW_LOG_TASK(DW_LOG_ERROR, task_id, "[ERROR] get_file_list 获取 torrent_info 失败");
            return -1;
        }
        if (!ti) {
            DW_LOG_TASK(DW_LOG_DEBUG, task_id, "[EVENT] get_file_list 元数据未就绪");
            return -1;
        }
        const int32_t ret = fill_file_list(ti, out_files, out_count);
        if (ret != 0) {
            return ret;
        }

        // 运行时查询：从 piece bitmap 计算每个文件的已下载字节数
        try {
            const lt::torrent_status st = handle.status(lt::torrent_handle::query_pieces);
            const lt::file_storage &fs = ti->files();
            const auto downloaded = calc_per_file_downloaded(fs, st.pieces);
            for (int32_t i = 0; i < *out_count && i < static_cast<int32_t>(downloaded.size()); ++i) {
                (*out_files)[i].downloaded_bytes = downloaded[i];
            }
        } catch (const std::exception &e) {
            DW_LOG_TASK(DW_LOG_ERROR, task_id, "[ERROR] get_file_list 计算进度失败: %s", e.what());
            // 进度计算失败不影响文件列表返回，downloaded_bytes 保持 0
        }

        DW_LOG_TASK(DW_LOG_INFO, task_id, "[EVENT] get_file_list 成功 count=%d", *out_count);
        return 0;
    }

    std::vector<dw_byte_range_t> TorrentEngine::get_file_ranges(const char *task_id,
                                                                int32_t file_index) {
        std::vector<dw_byte_range_t> ranges;
        if (!task_id || !task_id[0] || file_index < 0) {
            return ranges;
        }
        lt::torrent_handle handle = find_handle(std::string(task_id));
        if (!handle.is_valid()) {
            return ranges;
        }
        std::shared_ptr<const lt::torrent_info> ti;
        try {
            ti = handle.torrent_file();
        } catch (...) {
            return ranges;
        }
        if (!ti) {
            // 元数据未就绪，无区间可报
            return ranges;
        }
        try {
            const lt::file_storage &fs = ti->files();
            if (file_index >= fs.num_files()) {
                return ranges;
            }
            const lt::file_index_t fidx{file_index};
            const int64_t f_off = fs.file_offset(fidx);
            const int64_t f_size = fs.file_size(fidx);
            const int32_t piece_len = fs.piece_length();
            if (f_size <= 0 || piece_len <= 0) {
                return ranges;
            }
            const int64_t f_end = f_off + f_size - 1; // 文件末字节（含，torrent 全局偏移）
            const int first_piece = static_cast<int>(f_off / piece_len);
            const int last_piece = static_cast<int>(f_end / piece_len);

            // have 位图：仅整块 have 的 piece 计入（bit 置位即该 piece 已校验完成）。
            const lt::torrent_status st = handle.status(lt::torrent_handle::query_pieces);
            const lt::typed_bitfield<lt::piece_index_t> &pieces = st.pieces;

            for (int p = first_piece; p <= last_piece; ++p) {
                const lt::piece_index_t pi{p};
                if (pi >= pieces.end_index() || !pieces.get_bit(pi)) {
                    continue;
                }
                // piece 在 torrent 全局的字节窗口，裁剪到文件窗口后转文件内相对偏移
                const int64_t piece_start = static_cast<int64_t>(p) * piece_len;
                const int64_t piece_end = piece_start + piece_len - 1;
                const int64_t seg_start = (piece_start > f_off) ? piece_start : f_off;
                const int64_t seg_end = (piece_end < f_end) ? piece_end : f_end;
                if (seg_end < seg_start) continue;
                const int64_t rel_start = seg_start - f_off;
                const int64_t rel_end = seg_end - f_off;
                // 合并连续段：与上一段首尾相接则延展
                if (!ranges.empty() && ranges.back().end + 1 == rel_start) {
                    ranges.back().end = rel_end;
                } else {
                    ranges.push_back(dw_byte_range_t{rel_start, rel_end});
                }
            }
        } catch (const std::exception &e) {
            DW_LOG_TASK(DW_LOG_ERROR, task_id, "[ERROR] get_file_ranges 异常: %s", e.what());
            ranges.clear();
        }
        return ranges;
    }

    int TorrentEngine::set_playing_file(const char *task_id,
                                        int32_t file_index,
                                        int64_t byte_offset) {
        if (!task_id || !task_id[0]) {
            return 0;
        }
        lt::torrent_handle handle = find_handle(std::string(task_id));
        if (!handle.is_valid()) {
            DW_LOG_TASK(DW_LOG_ERROR, task_id, "[ERROR] set_playing_file 任务不存在");
            return 0;
        }
        // file_index<0：停止播放态提优
        if (file_index < 0) {
            try {
                handle.clear_piece_deadlines();
            } catch (const std::exception &e) {
                DW_LOG_TASK(DW_LOG_ERROR, task_id, "[ERROR] clear_piece_deadlines 失败: %s", e.what());
                return 0;
            }
            DW_LOG_TASK(DW_LOG_DEBUG, task_id, "[EVENT] set_playing_file 停止提优");
            return 1;
        }
        try {
            std::shared_ptr<const lt::torrent_info> ti = handle.torrent_file();
            if (!ti) {
                return 0; // 元数据未就绪
            }
            const lt::file_storage &fs = ti->files();
            if (file_index >= fs.num_files()) {
                return 0;
            }
            const lt::file_index_t fidx{file_index};
            const int64_t f_off = fs.file_offset(fidx);
            const int64_t f_size = fs.file_size(fidx);
            const int32_t piece_len = fs.piece_length();
            if (f_size <= 0 || piece_len <= 0) {
                return 0;
            }
            const int64_t off = (byte_offset > 0) ? byte_offset : 0;
            const int start_piece = static_cast<int>((f_off + off) / piece_len);
            const int last_piece = static_cast<int>((f_off + f_size - 1) / piece_len);
            // readahead 窗口 N 片，按递增 deadline 提优（越靠前越紧急）。
            constexpr int kReadaheadPieces = 8;
            constexpr int kDeadlineStepMs = 1000;
            for (int i = 0; i < kReadaheadPieces; ++i) {
                const int p = start_piece + i;
                if (p > last_piece) break;
                handle.set_piece_deadline(lt::piece_index_t{p}, i * kDeadlineStepMs);
            }
            handle.set_flags(lt::torrent_flags::auto_managed);
            handle.resume();
        } catch (const std::exception &e) {
            DW_LOG_TASK(DW_LOG_ERROR, task_id, "[ERROR] set_playing_file 失败: %s", e.what());
            return 0;
        }
        DW_LOG_TASK(DW_LOG_DEBUG, task_id,
                    "[EVENT] set_playing_file 成功 file_index=%d offset=%lld",
                    file_index, (long long)byte_offset);
        return 1;
    }

    int TorrentEngine::set_file_priorities(const char *task_id,
                                           const int32_t *file_indexes,
                                           const int32_t *priorities,
                                           int32_t count) {
        if (!task_id || !task_id[0] || !file_indexes || !priorities || count <= 0) {
            return 0;
        }
        lt::torrent_handle handle = find_handle(std::string(task_id));
        if (!handle.is_valid()) {
            DW_LOG_TASK(DW_LOG_ERROR, task_id, "[ERROR] set_file_priorities 任务不存在");
            return 0;
        }
        try {
            // 逐文件设置后统一 resume，避免每项重复 auto_managed/resume。
            for (int32_t i = 0; i < count; ++i) {
                handle.file_priority(lt::file_index_t(file_indexes[i]),
                                     lt::download_priority_t(static_cast<uint8_t>(priorities[i])));
            }
            handle.set_flags(lt::torrent_flags::auto_managed);
            handle.resume();
        } catch (const std::exception &e) {
            DW_LOG_TASK(DW_LOG_ERROR, task_id, "[ERROR] set_file_priorities 失败: %s", e.what());
            return 0;
        }
        DW_LOG_TASK(DW_LOG_INFO, task_id, "[EVENT] set_file_priorities 成功 count=%d", count);
        return 1;
    }

    int32_t TorrentEngine::apply_file_selection(const char *task_id,
                                                const int32_t *file_indexes,
                                                int32_t count) {
        if (!task_id || !task_id[0]) return -1;
        lt::torrent_handle handle = find_handle(std::string(task_id));
        if (!handle.is_valid()) {
            DW_LOG_TASK(DW_LOG_ERROR, task_id, "[ERROR] apply_file_selection 任务不存在");
            return -1;
        }
        std::shared_ptr<const lt::torrent_info> ti;
        try { ti = handle.torrent_file(); } catch (...) { ti = nullptr; }
        if (!ti) {
            // 调度出口以 metadata_ready && naming_ready 合取为前提，正常不应到此。
            DW_LOG_TASK(DW_LOG_ERROR, task_id, "[ERROR] apply_file_selection 元数据未就绪");
            return -1;
        }
        try {
            const int n = ti->files().num_files();
            // 全量显式定型：磁力（default_dont_download 物化全 0）与 .torrent（默认全 4）
            // 添加路径的初始优先级不同，统一覆写为确认意图，消除路径差异。
            // count<=0 = 全部默认优先级；count>0 = 选中默认、其余不下载。
            std::vector<lt::download_priority_t> prio(
                static_cast<size_t>(n > 0 ? n : 0),
                count > 0 ? lt::dont_download : lt::default_priority);
            if (count > 0 && file_indexes) {
                for (int32_t i = 0; i < count; ++i) {
                    if (file_indexes[i] >= 0 && file_indexes[i] < n) {
                        prio[static_cast<size_t>(file_indexes[i])] = lt::default_priority;
                    }
                }
            }
            handle.prioritize_files(prio);
            // 优先级已显式定型，解除待选保护并恢复运行（此刻解除标志不再影响优先级）。
            handle.unset_flags(lt::torrent_flags::default_dont_download);
            handle.set_flags(lt::torrent_flags::auto_managed);
            handle.resume();
        } catch (const std::exception &e) {
            DW_LOG_TASK(DW_LOG_ERROR, task_id, "[ERROR] apply_file_selection 失败: %s", e.what());
            return -1;
        }
        DW_LOG_TASK(DW_LOG_INFO, task_id,
                    "[EVENT] apply_file_selection 成功 count=%d (<=0 即全部)", count);
        return 0;
    }
} // namespace dw
