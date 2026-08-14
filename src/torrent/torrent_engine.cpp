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
#include <cerrno>
#include <chrono>
#include <filesystem>
#include <string>
#include <thread>
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

        // 从 info_hash_t 提取 hex（优先 v2，回退 v1）
        std::string info_hash_hex(const lt::info_hash_t &ih) {
            return ih.has_v2() ? lt::aux::to_hex(ih.v2) : lt::aux::to_hex(ih.v1);
        }

        // 从 torrent_handle 提取 info_hash hex；无效或异常返回空串
        std::string info_hash_hex(const lt::torrent_handle &h) {
            if (!h.is_valid()) return {};
            try {
                return info_hash_hex(h.info_hashes());
            } catch (...) {
                return {};
            }
        }

        // 处理 state_update_alert 事件
        EngineEvent make_status_update_event(const lt::torrent_status &s, const std::string &key) {
            if (s.errc) {
                DW_LOG_TASK(DW_LOG_ERROR, key.c_str(), "任务发生错误. code: %d, msg: %s",
                            s.errc.value(), s.errc.message().c_str());
                return EngineEvent{
                    .type = EngineEventType::DOWNLOAD_FAILED,
                    .engine_key = key,
                    .protocol = DW_PROTOCOL_TORRENT
                };
            }
            EngineEvent ev;
            ev.type = EngineEventType::STATUS_UPDATE;
            ev.engine_key = key;
            ev.protocol = DW_PROTOCOL_TORRENT;
            ev.total_size = s.total_wanted;
            ev.total_done = s.total_done;
            ev.progress = static_cast<double>(s.progress);
            ev.download_rate = static_cast<double>(s.download_payload_rate);
            ev.upload_rate = static_cast<double>(s.upload_payload_rate);
            ev.total_upload = s.all_time_upload;
            ev.name = s.name;
            return ev;
        }

        // 请求保存恢复数据
        void request_save_resume(const lt::torrent_handle &h, const bool force = false) {
            if (!h.is_valid()) return;
            try {
                if (const lt::torrent_status st = h.status(); !st.has_metadata) return;
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
                DW_LOG_TASK(DW_LOG_ERROR, rsk.c_str(), "请求恢复数据失败: %s", e.what());
            }
        }

        // 前向声明：扁平文件列表构建（供 PARSED 事件与 fill_file_list 复用）。
        std::vector<dw_file_info_t> build_flat_file_list(
            const std::shared_ptr<const lt::torrent_info> &ti);

        // 构建解析完成事件
        EngineEvent build_parsed_event(const lt::torrent_handle &h) {
            EngineEvent ev;
            ev.type = EngineEventType::PARSED;
            ev.protocol = DW_PROTOCOL_TORRENT;
            if (!h.is_valid()) return ev;
            ev.engine_key = info_hash_hex(h);
            if (ev.engine_key.empty()) return ev;
            try {
                const lt::torrent_status st = h.status();
                const std::shared_ptr<const lt::torrent_info> ti = h.torrent_file();
                if (!ti) {
                    ev.engine_key.clear();
                    return ev;
                }
                ev.name = ti->name();
                ev.save_path = st.save_path;
                ev.files = build_flat_file_list(ti);
            } catch (...) {
                ev.engine_key.clear(); // 异常时返回空事件
            }
            return ev;
        }

        // 处理 alert
        void handle_alert(const lt::alert *a) {
            if (!a) return;

            // 任务状态变化
            if (const auto *su = lt::alert_cast<lt::state_update_alert>(a)) {
                for (const lt::torrent_status &s: su->status) {
                    const std::string key = info_hash_hex(s.handle);
                    if (key.empty()) continue;
                    EngineEvent ev = make_status_update_event(s, key);
                    post_engine_event(std::move(ev));
                }
            }
            // 添加任务
            else if (const auto *at = lt::alert_cast<lt::add_torrent_alert>(a)) {
                const std::string key = info_hash_hex(at->params.info_hashes);
                if (at->error) {
                    // 添加任务失败
                    DW_LOG_TASK(DW_LOG_ERROR, key.c_str(), "添加失败: %s", at->error.message().c_str());
                    if (key.empty()) return;
                    post_engine_event(EngineEvent{
                        .type = EngineEventType::DOWNLOAD_FAILED,
                        .engine_key = key,
                        .protocol = DW_PROTOCOL_TORRENT
                    });
                } else if (at->handle.is_valid()) {
                    // 添加任务成功
                    DW_LOG_TASK(DW_LOG_INFO, key.c_str(), "添加成功");
                    if (const lt::torrent_status st = at->handle.status(); st.has_metadata) {
                        if (EngineEvent ev = build_parsed_event(at->handle); !ev.engine_key.empty()) {
                            post_engine_event(std::move(ev));
                        }
                    }
                }
            }
            // 磁力元数据就绪
            else if (const auto *mr = lt::alert_cast<lt::metadata_received_alert>(a)) {
                const std::string key = info_hash_hex(mr->handle);
                DW_LOG_TASK(DW_LOG_INFO, key.c_str(), "元数据就绪");
                if (EngineEvent ev = build_parsed_event(mr->handle); !ev.engine_key.empty()) {
                    post_engine_event(std::move(ev));
                }
            }
            // 下载完成
            else if (const auto *tf = lt::alert_cast<lt::torrent_finished_alert>(a)) {
                const std::string key = info_hash_hex(tf->handle);
                DW_LOG_TASK(DW_LOG_INFO, key.c_str(), "下载完成");
                if (key.empty()) return;
                post_engine_event(EngineEvent{
                    .type = EngineEventType::DOWNLOAD_COMPLETED,
                    .engine_key = key,
                    .protocol = DW_PROTOCOL_TORRENT
                });
            }
            // 任务错误
            else if (const auto *te = lt::alert_cast<lt::torrent_error_alert>(a)) {
                const std::string key = info_hash_hex(te->handle);
                DW_LOG_TASK(DW_LOG_ERROR, key.c_str(), "下载错误: %s", te->error.message().c_str());
                if (key.empty()) return;
                post_engine_event(EngineEvent{
                    .type = EngineEventType::DOWNLOAD_FAILED,
                    .engine_key = key,
                    .protocol = DW_PROTOCOL_TORRENT
                });
            }
            // 文件错误
            else if (const auto *fe = lt::alert_cast<lt::file_error_alert>(a)) {
                const std::string key = info_hash_hex(fe->handle);
                DW_LOG_TASK(DW_LOG_ERROR, key.c_str(), "文件错误 file: %s, msg: %s, errno: %d",
                            fe->filename(), fe->error.message().c_str(), fe->error.value());
                if (key.empty()) return;
                post_engine_event(EngineEvent{
                    .type = EngineEventType::DOWNLOAD_FAILED,
                    .engine_key = key,
                    .protocol = DW_PROTOCOL_TORRENT
                });
            }
            // 断点续传数据就绪
            else if (const auto *rd = lt::alert_cast<lt::save_resume_data_alert>(a)) {
                const std::string key = info_hash_hex(rd->handle);
                if (key.empty()) return;
                try {
                    const std::vector<char> buf = lt::write_resume_data_buf(rd->params);
                    if (!buf.empty()) {
                        EngineEvent ev;
                        ev.type = EngineEventType::RESUME_DATA;
                        ev.engine_key = key;
                        ev.protocol = DW_PROTOCOL_TORRENT;
                        ev.resume_data.assign(reinterpret_cast<const uint8_t *>(buf.data()),
                                              reinterpret_cast<const uint8_t *>(buf.data()) + buf.size());
                        post_engine_event(std::move(ev));
                    }
                } catch (const std::exception &e) {
                    DW_LOG_TASK(DW_LOG_ERROR, key.c_str(), "断点续传数据处理失败: %s", e.what());
                }
            }
            // 存储路径迁移完成
            else if (const auto *sm = lt::alert_cast<lt::storage_moved_alert>(a)) {
                const std::string key = info_hash_hex(sm->handle);
                if (key.empty()) return;
                DW_LOG_TASK(DW_LOG_INFO, key.c_str(), "存储路径迁移完成 路径: %s", sm->storage_path());
                if (EngineEvent ev = build_parsed_event(sm->handle); !ev.engine_key.empty()) {
                    post_engine_event(std::move(ev));
                }
            }
            // 存储路径迁失败
            else if (const auto *smf = lt::alert_cast<lt::storage_moved_failed_alert>(a)) {
                const std::string key = info_hash_hex(smf->handle);
                if (key.empty()) return;
                DW_LOG_TASK(DW_LOG_ERROR, key.c_str(), "存储路径迁移失败 errno: %d, msg: %s",
                            smf->error.value(), smf->error.message().c_str());
                post_engine_event(EngineEvent{
                    .type = EngineEventType::DOWNLOAD_FAILED,
                    .engine_key = key,
                    .protocol = DW_PROTOCOL_TORRENT
                });
            }
            // 暂停
            else if (lt::alert_cast<lt::torrent_paused_alert>(a)) {
                const std::string key = info_hash_hex(lt::alert_cast<lt::torrent_paused_alert>(a)->handle);
                DW_LOG_TASK(DW_LOG_INFO, key.c_str(), "暂停");
                if (key.empty()) return;
                post_engine_event(EngineEvent{
                    .type = EngineEventType::BT_PAUSED,
                    .engine_key = key,
                    .protocol = DW_PROTOCOL_TORRENT
                });
            }
            // 恢复
            else if (lt::alert_cast<lt::torrent_resumed_alert>(a)) {
                const std::string key = info_hash_hex(lt::alert_cast<lt::torrent_resumed_alert>(a)->handle);
                DW_LOG_TASK(DW_LOG_INFO, key.c_str(), "恢复");
                if (key.empty()) return;
                post_engine_event(EngineEvent{
                    .type = EngineEventType::BT_RESUMED,
                    .engine_key = key,
                    .protocol = DW_PROTOCOL_TORRENT
                });
            }
        }

        // 采集 alert
        void alert_loop() {
            while (g_running.load()) {
                try {
                    if (g_session) {
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
                    DW_LOG_SYS(DW_LOG_ERROR, "采集事件异常: %s", e.what());
                } catch (...) {
                    DW_LOG_SYS(DW_LOG_ERROR, "采集事件未知异常");
                }
            }
        }

        // 获取 handle
        lt::torrent_handle find_handle(const std::string &info_hash) {
            if (!g_session || info_hash.empty()) return {};
            lt::sha1_hash h;
            if (lt::aux::from_hex(info_hash, h.data())) {
                return g_session->find_torrent(h);
            }
            for (const auto handles = g_session->get_torrents(); const auto &th: handles) {
                if (info_hash_hex(th) == info_hash) return th;
            }
            return {};
        }

        // 填充文件列表到 C 数组
        int32_t fill_file_list(const std::shared_ptr<const lt::torrent_info> &ti,
                               dw_file_info_t **out_files, int32_t *out_count) {
            const auto files = build_flat_file_list(ti);
            const auto count = static_cast<int32_t>(files.size());
            *out_count = 0;
            *out_files = static_cast<dw_file_info_t *>(
                std::calloc(static_cast<size_t>(count > 0 ? count : 1), sizeof(dw_file_info_t)));
            if (!*out_files) return -1;
            for (int32_t i = 0; i < count; ++i) {
                (*out_files)[i] = files[static_cast<size_t>(i)];
            }
            *out_count = count;
            return 0;
        }

        // 扁平文件列表
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
                // 过滤 pad 文件
                if (fs.pad_file_at(idx)) continue;
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

        // 操作结果
        void set_result(dw_submit_result_t *r, const char *task_id,
                        const dw_reason_t code, const char *msg) {
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
                DW_LOG_TASK(DW_LOG_ERROR, task_id, "操作结果 code=%d, msg=%s", code, msg ? msg : "");
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
            DW_LOG_SYS(DW_LOG_ERROR, "初始化BT引擎失败: %s", e.what());
            return -1;
        }
        if (!g_session || !g_session->is_valid()) {
            DW_LOG_SYS(DW_LOG_ERROR, "初始化BT引擎失败");
            g_session.reset();
            return -1;
        }

        g_running.store(true);
        g_alert_thread = std::thread(alert_loop);

        initialized_ = true;
        DW_LOG_SYS(DW_LOG_INFO, "初始化BT引擎完成 interval: %dms", g_interval_ms);
        return 0;
    }

    void TorrentEngine::destroy() {
        if (!initialized_) {
            return;
        }
        // 先停止 alert 采集线程，再销毁 session
        g_running.store(false);
        if (g_alert_thread.joinable()) {
            g_alert_thread.join();
        }
        g_session.reset();

        initialized_ = false;
        DW_LOG_SYS(DW_LOG_INFO, "销毁BT引擎完成");
    }

    int32_t TorrentEngine::add_task(const dw_task_params_t *params,
                                    dw_submit_result_t *out_result) {
        DW_LOG_TASK(DW_LOG_DEBUG, params ? params->info_hash : nullptr, "add_task 进入");
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
                DW_LOG_TASK(DW_LOG_ERROR, key.c_str(), "幂等 resume 失败: %s", e.what());
            }
            DW_LOG_TASK(DW_LOG_INFO, key.c_str(), "add_task 任务已存在，恢复运行（幂等）");
            set_result(out_result, params->info_hash, DW_REASON_NONE, nullptr);
            return 0;
        }

        lt::add_torrent_params atp;
        atp.save_path = params->save_path; // 新任务默认值；resume_data 整体覆盖 atp 时自然替换
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
                DW_LOG_TASK(DW_LOG_ERROR, key.c_str(), "解析 resume_data 失败: %s", e.what());
            }
        }
        // resume_data 自带 save_path（含定名包层迁移后的目录），续传天然复用；
        // 新任务直接落最终目录：根名判重由 TaskManager 调度在 RESOLVING 校验拍
        // 完成（重名时引擎经 move_storage 把 save_path 迁至包层目录）。
        if (!source_ok && params->magnet_link && params->magnet_link[0]) {
            lt::error_code ec;
            lt::parse_magnet_uri(params->magnet_link, atp, ec);
            if (ec) {
                DW_LOG_TASK(DW_LOG_ERROR, key.c_str(), "解析磁力链接失败: %s", ec.message().c_str());
            } else {
                source_ok = true;
            }
        }
        if (!source_ok && params->torrent_file && params->torrent_file[0]) {
            lt::error_code ec;
            auto ti = std::make_shared<lt::torrent_info>(params->torrent_file, ec);
            if (ec) {
                DW_LOG_TASK(DW_LOG_ERROR, key.c_str(), "加载 .torrent 文件失败: %s", ec.message().c_str());
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

        DW_LOG_TASK(DW_LOG_INFO, key.c_str(), "add_task 成功");
        set_result(out_result, params->info_hash, DW_REASON_NONE, nullptr);
        return 0;
    }

    int32_t TorrentEngine::pause_task(const char *task_id,
                                      dw_submit_result_t *out_result) {
        DW_LOG_TASK(DW_LOG_DEBUG, task_id, "pause_task 进入");
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
            DW_LOG_TASK(DW_LOG_INFO, task_id, "pause_task 成功");
        }
        set_result(out_result, task_id, DW_REASON_NONE, nullptr);
        return 0;
    }

    int32_t TorrentEngine::delete_task(const char *task_id,
                                       dw_submit_result_t *out_result) {
        DW_LOG_TASK(DW_LOG_DEBUG, task_id, "delete_task 进入");
        if (!task_id || !task_id[0]) {
            set_result(out_result, task_id, DW_REASON_ERROR, "task_id 为空");
            return -1;
        }
        const std::string key(task_id);
        if (const lt::torrent_handle handle = find_handle(key); handle.is_valid()) {
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
            DW_LOG_TASK(DW_LOG_INFO, key.c_str(), "delete_task 已移出会话");
            return 0;
        }
        // 任务不在会话（未准入 / 分享率回收后）：无运行时资源。
        DW_LOG_TASK(DW_LOG_INFO, key.c_str(), "delete_task 任务不在会话");
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
        for (const auto handles = g_session->get_torrents(); const auto &handle: handles) {
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
                    DW_LOG_TASK(DW_LOG_INFO, key.c_str(), "分享率达标释放做种上下文 key=%s", key.c_str());
                } catch (const std::exception &e) {
                    DW_LOG_TASK(DW_LOG_ERROR, key.c_str(), "sweep remove_torrent 失败 key=%s msg=%s",
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
            DW_LOG_TASK(DW_LOG_ERROR, key.c_str(), "包层迁移发起失败: %s", e.what());
            return -1;
        }
        DW_LOG_TASK(DW_LOG_INFO, key.c_str(), "包层迁移发起: -> '%s'", new_save_path);
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
            DW_LOG_SYS(DW_LOG_ERROR, "解析磁力链接失败: %s", ec.message().c_str());
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
            DW_LOG_SYS(DW_LOG_ERROR, "加载 .torrent 文件失败: %s", ec.message().c_str());
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
        DW_LOG_TASK(DW_LOG_DEBUG, task_id, "info_hash_to_magnet 进入");
        if (!task_id || !task_id[0]) {
            return nullptr;
        }
        const lt::torrent_handle handle = find_handle(std::string(task_id));
        if (!handle.is_valid()) {
            DW_LOG_TASK(DW_LOG_ERROR, task_id, "info_hash_to_magnet 任务不存在");
            return nullptr;
        }
        try {
            const std::string magnet = lt::make_magnet_uri(handle);
            if (magnet.empty()) return nullptr;
            auto *result = static_cast<char *>(std::malloc(magnet.size() + 1));
            if (result) std::memcpy(result, magnet.c_str(), magnet.size() + 1);
            DW_LOG_TASK(DW_LOG_INFO, task_id, "info_hash_to_magnet 成功");
            return result;
        } catch (const std::exception &e) {
            DW_LOG_TASK(DW_LOG_ERROR, task_id, "生成磁力链接失败: %s", e.what());
            return nullptr;
        }
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
        const auto ti = std::make_shared<lt::torrent_info>(torrent_file_path, ec);
        if (ec) {
            DW_LOG_SYS(DW_LOG_ERROR, "解析 .torrent 文件失败: %s", ec.message().c_str());
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
            DW_LOG_SYS(DW_LOG_ERROR, ".torrent 文件无有效 info_hash");
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

    // Piece→文件窗口裁剪共享逻辑：遍历与文件 [f_off, f_end] 重叠的已下载 piece，
    // 对每个 piece 裁剪到文件窗口后回调 (seg_start, seg_end)（全局绝对偏移）。
    // 供 get_file_ranges 复用。
    template<typename Callback>
    static void for_each_file_piece_segment(
        const lt::file_storage &fs,
        const lt::typed_bitfield<lt::piece_index_t> &pieces,
        const lt::file_index_t idx,
        Callback &&cb) {
        const int64_t piece_len = fs.piece_length();
        const int64_t f_off = fs.file_offset(idx);
        const int64_t f_size = fs.file_size(idx);
        if (f_size <= 0 || piece_len <= 0) return;
        const int64_t f_end = f_off + f_size - 1;
        const int first_piece = static_cast<int>(f_off / piece_len);
        const int last_piece = static_cast<int>(f_end / piece_len);
        for (int p = first_piece; p <= last_piece; ++p) {
            if (const lt::piece_index_t pi{p}; pi >= pieces.end_index() || !pieces.get_bit(pi)) continue;
            const int64_t piece_start = static_cast<int64_t>(p) * piece_len;
            const int64_t piece_end = piece_start + piece_len - 1;
            const int64_t seg_start = (piece_start > f_off) ? piece_start : f_off;
            const int64_t seg_end = (piece_end < f_end) ? piece_end : f_end;
            if (seg_end >= seg_start) cb(seg_start, seg_end);
        }
    }

    std::vector<dw_byte_range_t> TorrentEngine::get_file_ranges(const char *task_id,
                                                                int32_t file_index) {
        std::vector<dw_byte_range_t> ranges;
        if (!task_id || !task_id[0] || file_index < 0) {
            return ranges;
        }
        const lt::torrent_handle handle = find_handle(std::string(task_id));
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

            // have 位图：仅整块 have 的 piece 计入（bit 置位即该 piece 已校验完成）。
            const lt::torrent_status st = handle.status(lt::torrent_handle::query_pieces);
            const lt::typed_bitfield<lt::piece_index_t> &pieces = st.pieces;

            for_each_file_piece_segment(fs, pieces, fidx,
                                        [&ranges, f_off](int64_t seg_start, int64_t seg_end) {
                                            const int64_t rel_start = seg_start - f_off;
                                            const int64_t rel_end = seg_end - f_off;
                                            // 合并连续段：与上一段首尾相接则延展
                                            if (!ranges.empty() && ranges.back().end + 1 == rel_start) {
                                                ranges.back().end = rel_end;
                                            } else {
                                                ranges.push_back(dw_byte_range_t{rel_start, rel_end});
                                            }
                                        });
        } catch (const std::exception &e) {
            DW_LOG_TASK(DW_LOG_ERROR, task_id, "get_file_ranges 异常: %s", e.what());
            ranges.clear();
        }
        return ranges;
    }

    int32_t TorrentEngine::set_playing_file(const char *task_id,
                                            int32_t file_index,
                                            int64_t byte_offset) {
        if (!task_id || !task_id[0] || file_index < 0) {
            return -1;
        }
        const lt::torrent_handle handle = find_handle(std::string(task_id));
        if (!handle.is_valid()) {
            DW_LOG_TASK(DW_LOG_ERROR, task_id, "set_playing_file 任务不存在");
            return -1;
        }
        try {
            std::shared_ptr<const lt::torrent_info> ti = handle.torrent_file();
            if (!ti) {
                return -1; // 元数据未就绪
            }
            const lt::file_storage &fs = ti->files();
            if (file_index >= fs.num_files()) {
                return -1;
            }
            const lt::file_index_t fidx{file_index};
            const int64_t f_off = fs.file_offset(fidx);
            const int64_t f_size = fs.file_size(fidx);
            const int32_t piece_len = fs.piece_length();
            if (f_size <= 0 || piece_len <= 0) {
                return -1;
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
        } catch (const std::exception &e) {
            DW_LOG_TASK(DW_LOG_ERROR, task_id, "set_playing_file 失败: %s", e.what());
            return -1;
        }
        DW_LOG_TASK(DW_LOG_DEBUG, task_id,
                    "set_playing_file 成功 file_index=%d offset=%lld",
                    file_index, (long long)byte_offset);
        return 0;
    }

    int32_t TorrentEngine::apply_file_selection(const char *task_id,
                                                const int32_t *file_indexes,
                                                int32_t count) {
        if (!task_id || !task_id[0]) return -1;
        lt::torrent_handle handle = find_handle(std::string(task_id));
        if (!handle.is_valid()) {
            DW_LOG_TASK(DW_LOG_ERROR, task_id, "apply_file_selection 任务不存在");
            return -1;
        }
        std::shared_ptr<const lt::torrent_info> ti;
        try { ti = handle.torrent_file(); } catch (...) { ti = nullptr; }
        if (!ti) {
            // 调度出口以 metadata_ready && naming_ready 合取为前提，正常不应到此。
            DW_LOG_TASK(DW_LOG_ERROR, task_id, "apply_file_selection 元数据未就绪");
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
            DW_LOG_TASK(DW_LOG_ERROR, task_id, "apply_file_selection 失败: %s", e.what());
            return -1;
        }
        DW_LOG_TASK(DW_LOG_INFO, task_id,
                    "apply_file_selection 成功 count=%d (<=0 即全部)", count);
        return 0;
    }
} // namespace dw
