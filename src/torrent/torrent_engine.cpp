/**
 * @file torrent_engine.cpp
 * @brief BT/Torrent 下载引擎实现（基于 libtorrent）。
 *
 * 架构（快照拉模型）：
 *   - 单例 lt::session 管理所有 BT 任务；
 *   - A 线程每拍经 post_updates 触发 post_torrent_updates + 携变更门槛的续传检查点；
 *     alert 线程消费 state_update_alert 写入进度快照，query_progress 纯读快照；
 *   - alert 轮询线程另处理生命周期事件：完成时请求保存续传、阻断性错误
 *     （torrent_error / file_error）直写快照置终态、save_resume_data_alert 经
 *     dw::post_resume_data 输出续传数据；
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
#include <libtorrent/storage_defs.hpp>
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
#include <filesystem>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace lt = libtorrent;

namespace dw {
    using utils::now_unix_ms;

    /* ===================================================================== */
    /*                        文件内全局状态与辅助                            */
    /* ===================================================================== */
    namespace torrent_engine {
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

        // ===== 引擎进度快照（推送模型） =====
        // 由 alert 线程消费 state_update_alert 持续更新，query_progress 纯读。
        // key=info_hash hex。仅持 g_status_mtx，绝不触碰 TaskManager mtx_、不落库。
        std::mutex g_status_mtx;
        std::unordered_map<std::string, EngineProgress> g_status_snapshot;

        // ===== 定名包层迁移进行时集合 =====
        // 判重决策归 TaskManager（调度在 RESOLVING 校验拍定名后经 move_storage 发起），
        // 引擎仅提供 save_path 迁移能力并跟踪进行中的迁移：
        // g_pending_moves：迁移中任务 key 集合，storage_moved_alert /
        //   storage_moved_failed_alert 收敛时移除；naming_ready 门禁据此计算。
        std::mutex g_move_mtx;
        std::unordered_set<std::string> g_pending_moves;

        // 遗忘某任务的迁移状态（删除 / sweep 回收时调用）。
        void move_state_cleanup(const std::string &key) {
            std::lock_guard<std::mutex> lk(g_move_mtx);
            g_pending_moves.erase(key);
        }

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

        // 由 torrent_status 组装 EngineProgress：供 alert 线程消费 state_update_alert 写入
        // 引擎内进度快照；query_progress 纯读该快照，不再同步调用 handle.status()。
        EngineProgress make_progress(const lt::torrent_status &s) {
            EngineProgress ep;
            ep.valid         = true;
            ep.protocol      = DW_PROTOCOL_TORRENT;
            ep.status        = map_status(s);
            ep.total_size    = s.total_wanted;
            ep.total_done    = s.total_done;
            ep.progress      = static_cast<double>(s.progress);
            ep.download_rate = static_cast<double>(s.download_payload_rate);
            ep.name          = s.name;
            ep.output_path   = s.save_path;   // 物理目录（即最终 save_path，无临时目录）
            ep.reason        = s.errc ? DW_REASON_NETWORK : DW_REASON_NONE;
            ep.message       = s.errc ? s.errc.message() : std::string{};
            ep.upload_rate   = static_cast<double>(s.upload_payload_rate);
            ep.metadata_ready = s.has_metadata;
            // naming_ready 不在此填：迁移完成不触发 state_update（快照可能陈旧），
            // 由 query_progress 出口按 g_pending_moves 现算。
            return ep;
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
                if (!h.status().has_metadata) return;
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

        // 前向声明：节点树落库辅助定义在下方（alert 事件点调用）。
        void post_file_tree(const lt::torrent_handle &h);

        // 处理单个 alert：快照模型下关注生命周期、阻断性错误与续传事件，进度由
        // state_update 快照承载。文件树由引擎在事件点主动推送落表（添加成功 /
        // 元数据就绪 / 改名收敛），判重定名决策仍归 TaskManager 调度。
        void handle_alert(const lt::alert *a) {
            if (!a) return;

            // 进度快照更新：A 线程触发 post_torrent_updates 后，批量 torrent_status 经此写入
            // 引擎内快照（仅持 g_status_mtx，不碰 TaskManager mtx_、不落库），query_progress 纯读。
            if (const auto *su = lt::alert_cast<lt::state_update_alert>(a)) {
                std::lock_guard<std::mutex> lk(g_status_mtx);
                for (const lt::torrent_status &s: su->status) {
                    const std::string key = info_hash_hex(s.handle);
                    if (key.empty()) continue;
                    EngineProgress ep = make_progress(s);
                    // 错误归因防降级：阻断性 alert 直写的归因（如存储错误）比
                    // make_progress 恒 NETWORK 更准确，errc 持续期间保留首个归因。
                    if (ep.status == DW_TASK_STATUS_ERROR) {
                        const auto it = g_status_snapshot.find(key);
                        if (it != g_status_snapshot.end() &&
                            it->second.status == DW_TASK_STATUS_ERROR) {
                            ep.reason = it->second.reason;
                            ep.message = it->second.message;
                        }
                    }
                    g_status_snapshot[key] = std::move(ep);
                }
                return;
            }

            // 任务添加成功（.torrent / 磁力 / 续传恢复统一入口）：已有元数据则立即
            // 推送文件树落表；磁力此刻无元数据，post_file_tree 内部自然跳过，
            // 待 metadata_received 到达后推送。
            if (const auto *at = lt::alert_cast<lt::add_torrent_alert>(a)) {
                if (!at->error) post_file_tree(at->handle);
                return;
            }
            // 磁力元数据就绪：推送文件树落表（先删后存，存在即覆盖）。
            if (const auto *mr = lt::alert_cast<lt::metadata_received_alert>(a)) {
                post_file_tree(mr->handle);
                return;
            }
            // 下载完成：请求保存一次续传数据（文件已在最终位置，做种原地继续，无移出环节）。
            if (const auto *tf = lt::alert_cast<lt::torrent_finished_alert>(a)) {
                request_save_resume(tf->handle);
                return;
            }
            // 任务错误（阻断性：转入 error state 时自动推送）：直写快照置终态，
            // 采集拍立即感知，不必等下一轮 post_torrent_updates 快照刷新。
            // 已是终态时不覆盖归因（file_error 可能先到，其存储归因更准确）。
            if (const auto *te = lt::alert_cast<lt::torrent_error_alert>(a)) {
                const std::string key = info_hash_hex(te->handle);
                DW_LOG_TASK(DW_LOG_ERROR, key.c_str(), "[ERROR] 任务错误 info_hash=%s msg=%s",
                            key.c_str(), te->error.message().c_str());
                if (!key.empty()) {
                    std::lock_guard<std::mutex> lk(g_status_mtx);
                    auto &ep = g_status_snapshot[key];
                    if (ep.status != DW_TASK_STATUS_ERROR) {
                        ep.valid = true;
                        ep.protocol = DW_PROTOCOL_TORRENT;
                        ep.status = DW_TASK_STATUS_ERROR;
                        ep.reason = DW_REASON_NETWORK;
                        ep.message = te->error.message();
                    }
                }
                return;
            }
            // 存储读写失败（阻断性：libtorrent 自动暂停任务并转入 error state）：
            // 以存储归因直写快照，随后到达的 torrent_error / state_update 不再降级覆盖。
            if (const auto *fe = lt::alert_cast<lt::file_error_alert>(a)) {
                const std::string key = info_hash_hex(fe->handle);
                DW_LOG_TASK(DW_LOG_ERROR, key.c_str(), "[ERROR] 存储错误 file=%s msg=%s",
                            fe->filename(), fe->error.message().c_str());
                if (!key.empty()) {
                    std::lock_guard<std::mutex> lk(g_status_mtx);
                    auto &ep = g_status_snapshot[key];
                    ep.valid = true;
                    ep.protocol = DW_PROTOCOL_TORRENT;
                    ep.status = DW_TASK_STATUS_ERROR;
                    ep.reason = DW_REASON_ERROR;
                    ep.message = (fe->error.value() == ENOSPC)
                                     ? "存储空间不足"
                                     : "存储异常，无法保存文件";
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
            // 定名包层迁移完成：handle 内部 save_path 已指向包层目录，补存一次
            // resume（write_resume_data 携带新 save_path，跨重启保持）。包层不改
            // 文件内部相对路径，文件树无需重推。
            if (const auto *sm = lt::alert_cast<lt::storage_moved_alert>(a)) {
                const std::string key = info_hash_hex(sm->handle);
                if (key.empty()) return;
                move_state_cleanup(key);
                request_save_resume(sm->handle, true);
                DW_LOG_TASK(DW_LOG_INFO, key.c_str(), "[OK] 包层迁移完成: -> '%s'",
                            sm->storage_path());
                return;
            }
            // 迁移失败：零字节落盘时段仅创建目标目录可涉磁盘（mkdir 失败等极端
            // 情形），记录后移除 pending 放行（naming_ready 解除，调度按现路径继续）。
            if (const auto *smf = lt::alert_cast<lt::storage_moved_failed_alert>(a)) {
                const std::string key = info_hash_hex(smf->handle);
                if (key.empty()) return;
                DW_LOG_TASK(DW_LOG_ERROR, key.c_str(), "[ERROR] move_storage 失败: %s",
                            smf->error.message().c_str());
                move_state_cleanup(key);
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
                        // 会话 alert：state_update（写进度快照）、生命周期与续传 alert。
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
            for (const auto &h: handles) {
                if (info_hash_hex(h) == info_hash) return h;
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
                (*out_files)[i].type = 1;        // 扁平列表均为文件
                (*out_files)[i].parent_id = -1;  // 扁平列表无树层级
                const std::string path = fs.file_path(idx);
                (*out_files)[i].name = static_cast<char *>(std::malloc(path.size() + 1));
                if ((*out_files)[i].name) {
                    std::memcpy((*out_files)[i].name, path.c_str(), path.size() + 1);
                }
                (*out_files)[i].size = fs.file_size(idx);
            }
            *out_count = file_count;
            return 0;
        }

        // 依据 torrent_info 的文件相对路径构建显式节点树（文件夹 + 文件）。
        // 每个文件路径按 '/'、'\\' 拆分：末段为文件，前置各段为文件夹（去重复用）；
        // prefix 为父路径累积（含尾部分隔符，根层空串）；文件夹 size 聚合后代文件字节。
        // 调用时机为 alert 事件点（添加成功 / 元数据就绪 / 改名收敛），file_storage
        // 反映当下命名；改名收敛后重推即覆盖旧名记录（落表为先删后存）。
        // 返回的各节点字符串字段由 std::malloc 分配，调用方负责释放。
        std::vector<dw_file_info_t> build_node_tree(
                const std::shared_ptr<const lt::torrent_info> &ti) {
            std::vector<dw_file_info_t> nodes;
            const lt::file_storage &fs = ti->files();
            const int file_count = fs.num_files();
            if (file_count <= 0) return nodes;
            const int64_t now = now_unix_ms();

            auto dup = [](const std::string &s) -> char * {
                auto *p = static_cast<char *>(std::malloc(s.size() + 1));
                if (p) std::memcpy(p, s.c_str(), s.size() + 1);
                return p;
            };

            // 累积路径（如 "root/sub"）-> 该文件夹节点在 nodes 中的下标，用于去重。
            std::unordered_map<std::string, size_t> dir_index;
            int64_t next_node_id = 1;

            for (int i = 0; i < file_count; ++i) {
                const lt::file_index_t idx{i};
                const std::string path = fs.file_path(idx);
                // 拆分为路径段
                std::vector<std::string> comps;
                size_t start = 0;
                while (start <= path.size()) {
                    const size_t sep = path.find_first_of("/\\", start);
                    if (sep == std::string::npos) {
                        if (start < path.size()) comps.push_back(path.substr(start));
                        break;
                    }
                    if (sep > start) comps.push_back(path.substr(start, sep - start));
                    start = sep + 1;
                }
                if (comps.empty()) continue;

                // 逐级创建 / 复用文件夹节点，prefix 随层级累积。
                int64_t parent_id = -1;
                std::string cum;      // 当前累积路径（无尾分隔符）
                std::string prefix;   // 当前层 prefix（父路径累积，含尾分隔符）
                for (size_t d = 0; d + 1 < comps.size(); ++d) {
                    cum += comps[d];
                    auto it = dir_index.find(cum);
                    if (it == dir_index.end()) {
                        dw_file_info_t dir{};
                        dir.index = -1;
                        dir.node_id = next_node_id++;
                        dir.parent_id = parent_id;
                        dir.type = 0;
                        dir.size = 0;
                        dir.status = 0;
                        dir.created_at = now;
                        dir.name = dup(comps[d]);
                        dir.prefix = dup(prefix);
                        dir.ext = nullptr;
                        nodes.push_back(dir);
                        dir_index[cum] = nodes.size() - 1;
                        parent_id = dir.node_id;
                    } else {
                        parent_id = nodes[it->second].node_id;
                    }
                    cum += "/";
                    prefix += comps[d] + "/";
                }

                // 文件节点
                const std::string &fname = comps.back();
                dw_file_info_t file{};
                file.index = i;
                file.node_id = next_node_id++;
                file.parent_id = parent_id;
                file.type = 1;
                file.size = fs.file_size(idx);
                file.status = 0;
                file.created_at = now;
                file.name = dup(fname);
                file.prefix = dup(prefix);
                const std::string ext = dw::utils::file_extension(fname);
                file.ext = ext.empty() ? nullptr : dup(ext);
                nodes.push_back(file);

                // 聚合文件大小到全部祖先文件夹（沿 parent_id 上溯）。
                int64_t cur = parent_id;
                while (cur >= 0) {
                    bool matched = false;
                    for (auto &nd: nodes) {
                        if (nd.node_id == cur && nd.type == 0) {
                            nd.size += file.size;
                            cur = nd.parent_id;
                            matched = true;
                            break;
                        }
                    }
                    if (!matched) break;
                }
            }
            return nodes;
        }

        // 事件点推送：构建节点树并经内部通道落库（无元数据时自然跳过）。
        // 释放构建期分配的字符串。
        void post_file_tree(const lt::torrent_handle &h) {
            if (!h.is_valid()) return;
            const std::string key = info_hash_hex(h);
            if (key.empty()) return;
            std::shared_ptr<const lt::torrent_info> ti;
            try { ti = h.torrent_file(); } catch (...) { return; }
            if (!ti) return;
            std::vector<dw_file_info_t> nodes = build_node_tree(ti);
            if (nodes.empty()) return;
            post_task_files(key.c_str(), DW_PROTOCOL_TORRENT,
                            nodes.data(), static_cast<int32_t>(nodes.size()));
            for (auto &nd: nodes) {
                std::free(nd.name);
                std::free(nd.prefix);
                std::free(nd.ext);
            }
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

        // flags：两来源（磁力 / .torrent）统一 metadata-only 模式——
        // default_dont_download 使全部文件优先级为 0（接 swarm 拿元数据、零 payload 下载），
        // auto_managed 保持运行以拉取元数据；真正开下由 apply_file_selection 显式定型
        // 优先级并解除 default_dont_download（RESOLVING 校验通过后由调度触发）。
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
            move_state_cleanup(key);   // 遗忘进行中的包层迁移状态
            { std::lock_guard<std::mutex> lk(g_status_mtx); g_status_snapshot.erase(key); }
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
                    move_state_cleanup(key);   // 遗忘进行中的包层迁移状态
                    { std::lock_guard<std::mutex> lk(g_status_mtx); g_status_snapshot.erase(key); }
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
        {
            // 上一轮迁移尚未收敛（storage_moved_alert 未回）：视作进行中，不叠加。
            std::lock_guard<std::mutex> lk(g_move_mtx);
            if (g_pending_moves.count(key) > 0) return 1;
        }
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
        // 内部 save_path，无数据搬移；storage_moved_alert 收敛后移除 pending 并补存
        // resume（新 save_path 跨重启保持）。
        {
            std::lock_guard<std::mutex> lk(g_move_mtx);
            g_pending_moves.insert(key);
        }
        try {
            h.move_storage(std::string(new_save_path));
        } catch (const std::exception &e) {
            DW_LOG_TASK(DW_LOG_ERROR, key.c_str(), "[ERROR] 包层迁移发起失败: %s", e.what());
            std::lock_guard<std::mutex> lk(g_move_mtx);
            g_pending_moves.erase(key);
            return -1;
        }
        DW_LOG_TASK(DW_LOG_INFO, key.c_str(), "[EVENT] 包层迁移发起: -> '%s'", new_save_path);
        return 1;
    }

    bool TorrentEngine::query_progress(const char *task_id, EngineProgress &out) {
        out = {};
        out.protocol = DW_PROTOCOL_TORRENT;
        if (!task_id || !task_id[0]) return false;
        // 纯读引擎内进度快照（由 alert 线程消费 state_update_alert 持续更新）：不再同步
        // 调用 handle.status()，消除 A 线程持 mtx_ 时逐任务同步跨库调用的开销。
        // 首个 state_update_alert 到达前快照缺失，返回 false 由采集循环自然跳过。
        std::lock_guard<std::mutex> lk(g_status_mtx);
        const auto it = g_status_snapshot.find(task_id);
        if (it == g_status_snapshot.end() || !it->second.valid) return false;
        out = it->second;
        // naming_ready 现算：move_storage 收敛不产生 state_update（快照该位可能陈旧），
        // g_pending_moves 不含 key 即无进行中迁移（含从未发起迁移的任务）。
        // 锁序 g_status_mtx → g_move_mtx 单向，无反向获取路径，不构成死锁。
        {
            std::lock_guard<std::mutex> rk(g_move_mtx);
            out.naming_ready = g_pending_moves.count(task_id) == 0;
        }
        return true;
    }

    void TorrentEngine::post_updates() {
        // 节拍统一推送入口：
        //   1) post_torrent_updates：结果经 state_update_alert 异步回 alert 线程写入快照；
        //   2) 续传检查点：对有元数据任务请求 save_resume_data（携变更门槛，无变化
        //      不产生 alert），结果经 save_resume_data_alert → post_resume_data 输出。
        // 去重下沉 libtorrent，上层无需再做低频节流。
        if (!g_session) return;
        try { g_session->post_torrent_updates(); } catch (...) {}
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
                                              char **out_info_hash,
                                              dw_file_info_t **out_files,
                                              int32_t *out_count) {
        if (!torrent_file_path || !out_info_hash || !out_files || !out_count) {
            return -1;
        }
        *out_info_hash = nullptr;
        *out_files = nullptr;
        *out_count = 0;

        lt::error_code ec;
        auto ti = std::make_shared<lt::torrent_info>(torrent_file_path, ec);
        if (ec) {
            DW_LOG_SYS(DW_LOG_ERROR, "[ERROR] 解析 .torrent 文件失败: %s", ec.message().c_str());
            return -1;
        }

        // info_hash
        const lt::info_hash_t &hashes = ti->info_hashes();
        std::string info_hash_str;
        if (hashes.has_v2()) {
            info_hash_str = lt::aux::to_hex(hashes.v2);
        } else if (hashes.has_v1()) {
            info_hash_str = lt::aux::to_hex(hashes.v1);
        } else {
            DW_LOG_SYS(DW_LOG_ERROR, "[ERROR] .torrent 文件无有效 info_hash");
            return -1;
        }
        *out_info_hash = static_cast<char *>(std::malloc(info_hash_str.size() + 1));
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
        if (ret == 0) {
            DW_LOG_TASK(DW_LOG_INFO, task_id, "[EVENT] get_file_list 成功 count=%d", *out_count);
        }
        return ret;
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
            const int64_t f_off  = fs.file_offset(fidx);
            const int64_t f_size = fs.file_size(fidx);
            const int32_t piece_len = fs.piece_length();
            if (f_size <= 0 || piece_len <= 0) {
                return ranges;
            }
            const int64_t f_end = f_off + f_size - 1; // 文件末字节（含，torrent 全局偏移）
            const int first_piece = static_cast<int>(f_off / piece_len);
            const int last_piece  = static_cast<int>(f_end / piece_len);

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
                const int64_t piece_end   = piece_start + piece_len - 1;
                const int64_t seg_start = (piece_start > f_off) ? piece_start : f_off;
                const int64_t seg_end   = (piece_end   < f_end)  ? piece_end   : f_end;
                if (seg_end < seg_start) continue;
                const int64_t rel_start = seg_start - f_off;
                const int64_t rel_end   = seg_end   - f_off;
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
            const int64_t f_off  = fs.file_offset(fidx);
            const int64_t f_size = fs.file_size(fidx);
            const int32_t piece_len = fs.piece_length();
            if (f_size <= 0 || piece_len <= 0) {
                return 0;
            }
            const int64_t off = (byte_offset > 0) ? byte_offset : 0;
            const int start_piece = static_cast<int>((f_off + off) / piece_len);
            const int last_piece  = static_cast<int>((f_off + f_size - 1) / piece_len);
            // readahead 窗口 N 片，按递增 deadline 提优（越靠前越紧急）。
            constexpr int kReadaheadPieces = 8;
            constexpr int kDeadlineStepMs  = 1000;
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
