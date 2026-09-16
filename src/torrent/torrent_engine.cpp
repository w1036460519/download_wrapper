/**
 * @file torrent_engine.cpp
 * @brief BT/Torrent 下载引擎实现（基于 libtorrent）。
 *
 * 架构（事件驱动）：
 *   - 单例 lt::session 管理所有 BT 任务；
 *   - A 线程每拍经 post_updates 触发 post_torrent_updates + 携变更门槛的续传检查点；
 *     alert 线程消费 state_update_alert 后经 post_engine_event 投递 STATUS_UPDATE 事件，
 *     B 线程消费后写入 TaskRuntime 内存，A 线程下一拍直接从 TaskRuntime 字段采集进度；
 *   - alert 轮询线程另处理生命周期事件：完成时投递 DOWNLOAD_COMPLETED 并请求保存续传、
 *     阻断性错误投递 DOWNLOAD_FAILED、save_resume_data_alert 经 dw::post_resume_data 输出续传数据；
 *   - 只订阅 error / 完成 / 续传相关 alert，不处理 peer / piece / block 等细粒度事件。
 */

#include "torrent/torrent_engine.h"

#include "core/task_manager.h"
#include "internal/downloader_internal.h"
#include "utils/memory_util.h"
#include "utils/string_util.h"
#include "utils/time_util.h"
#include "utils/timer_util.h"
#include "utils/unique_name.h"

#include <libtorrent/session.hpp>
#include <libtorrent/settings_pack.hpp>
#include <libtorrent/add_torrent_params.hpp>
#include <libtorrent/torrent_handle.hpp>
#include <libtorrent/torrent_status.hpp>
#include <libtorrent/torrent_info.hpp>
#include <libtorrent/file_storage.hpp>
#include <libtorrent/magnet_uri.hpp>
#include <libtorrent/load_torrent.hpp>
#include <libtorrent/error_code.hpp>
#include <libtorrent/alert_types.hpp>

#include <fstream>
#include <unordered_map>
#include <libtorrent/read_resume_data.hpp>
#include <libtorrent/write_resume_data.hpp>
#include <libtorrent/hex.hpp>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
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
        // alert 轮询线程（jthread：停止请求 + 自动 join）
        std::jthread g_alert_thread;
        // 恢复数据定时请求线程（每 5 秒）
        std::jthread g_resume_thread;
        // 进度回调间隔（ms）
        int g_interval_ms = 1000;
        // BT 做种分享率上限：total_upload/total_done 达到该值后释放做种上下文。
        // 默认 3.0（下载:上传=1:3）；init 从 cfg->seed_ratio_limit 读取（0=默认，<0=永久做种）。
        double g_seed_ratio_limit = 3.0;
        // 默认 tracker 列表：init 从 cfg->trackers 拷贝，添加/恢复任务时统一注入。
        std::vector<std::string> g_default_trackers;
        // 事件投递目标
        class TaskManager *g_task_manager = nullptr;
        // 重名处理：存储待迁移任务的原始名称（key → base_name），move_storage 完成后消费
        std::unordered_map<std::string, std::string> g_pending_original_names;

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

        lt::torrent_handle find_handle(const std::string &info_hash,
                                       const std::string &client_id = {},
                                       const dw_protocol_t protocol = DW_PROTOCOL_TORRENT) {
            if (!g_session || info_hash.empty()) return {};
            lt::sha1_hash h;
            if (lt::aux::from_hex(info_hash, h.data())) {
                if (auto th = g_session->find_torrent(h); th.is_valid()) return th;
            }
            for (const auto handles = g_session->get_torrents();
                 const auto &th: handles) {
                if (info_hash_hex(th) == info_hash) return th;
            }
            if (client_id.empty() || !g_task_manager) return {};
            const auto resume = g_task_manager->load_resume(client_id, protocol, info_hash);
            if (resume.empty()) return {};
            try {
                const lt::span<const char> buf(
                    reinterpret_cast<const char *>(resume.data()),
                    static_cast<std::ptrdiff_t>(resume.size()));
                lt::add_torrent_params atp = lt::read_resume_data(buf);
                for (const auto &t: g_default_trackers) {
                    atp.trackers.emplace_back(t);
                }
                atp.flags = lt::torrent_flags::update_subscribe
                            | lt::torrent_flags::need_save_resume
                            | lt::torrent_flags::default_dont_download;
                if (auto handle = g_session->add_torrent(std::move(atp)); handle.is_valid()) {
                    log_i(info_hash.c_str(), "handle 构建成功");
                    return handle;
                }
            } catch (const std::exception &e) {
                log_e(info_hash.c_str(), "handle 构建失败: %s", e.what());
            }
            return {};
        }

        // 处理 piece 完成事件（piece_finished_alert）
        // 基于 bitfield 位运算收集文件级区间
        void handle_piece_finished(const lt::torrent_handle &h, const lt::piece_index_t piece) {
            if (!g_task_manager || !h.is_valid()) return;
            std::shared_ptr<const lt::torrent_info> ti;
            try {
                ti = h.torrent_file();
            } catch (...) {
                return;
            }
            if (!ti) return;
            const std::string key = info_hash_hex(h);
            if (key.empty()) return;
            const lt::file_storage &fs = ti->layout();
            if (const lt::piece_index_t last = ti->last_piece();
                piece < lt::piece_index_t{0} || piece > last)
                return;

            const int64_t piece_len = ti->piece_length();

            const lt::torrent_status status = h.status(lt::torrent_handle::query_pieces);
            const lt::bitfield &pieces = status.pieces;

            std::string save_path;
            try {
                save_path = h.status(lt::torrent_handle::query_save_path).save_path;
            } catch (...) {
                log_e(key.c_str(), "获取 save_path 异常");
                return;
            }
            if (save_path.empty()) {
                log_e(key.c_str(), "save_path 为空");
                return;
            }

            const int file_count = fs.num_files();
            for (int i = 0; i < file_count; ++i) {
                const lt::file_index_t idx{i};
                if (fs.pad_file_at(idx)) continue;
                const int64_t f_off = fs.file_offset(idx);
                const int64_t f_size = fs.file_size(idx);
                if (f_size <= 0) continue;

                const int64_t f_end = f_off + f_size - 1;

                // 文件的 piece 范围
                const int f_piece_start = static_cast<int>(f_off / piece_len);
                const int f_piece_end = static_cast<int>((f_off + f_size - 1) / piece_len);

                // 判断 piece 是否在当前文件中
                if (const int piece_idx = static_cast<int>(piece);
                    piece_idx < f_piece_start || piece_idx > f_piece_end)
                    continue;

                EngineEvent ev;
                ev.type = EngineEventType::FILE_PROGRESS;
                ev.engine_key = key;
                ev.protocol = DW_PROTOCOL_TORRENT;
                ev.client_id = g_task_manager->client_id();
                ev.file_index = i;
                ev.file_size = f_size;
                ev.file_path = fs.file_path(idx);
                ev.full_path = (std::filesystem::path(save_path) / fs.file_path(idx)).string();

                // 阈值：1% 文件大小，兑底 4 个 piece 字节
                const int64_t threshold = std::max(f_size / 100, 4 * piece_len);

                // 从文件的开始 piece_index 遍历，计算连续区间
                int j = f_piece_start;
                while (j <= f_piece_end) {
                    // 跳过未完成的 piece
                    if (!pieces.get_bit(j)) {
                        ++j;
                        continue;
                    }
                    // 找到连续完成的起点
                    const int start = j;
                    while (j <= f_piece_end && pieces.get_bit(j)) {
                        ++j;
                    }
                    const int end = j - 1;

                    // 计算文件内的 byte 范围（闭区间）
                    const int64_t range_start = std::max(static_cast<int64_t>(start) * piece_len, f_off) - f_off;
                    const int64_t range_end = std::min(static_cast<int64_t>(end + 1) * piece_len - 1, f_end) - f_off;
                    if (range_end < range_start) continue;

                    if (const int64_t bytes = range_end - range_start + 1;
                        bytes < threshold)
                        continue; // 未达阈值，跳过

                    // 放入区间集合（std::map 自动按 offset_start 有序）
                    ev.intervals[range_start] = range_end;
                }

                // 发送事件（区间集合已完整，调用方直接序列化保存）
                if (!ev.intervals.empty()) {
                    g_task_manager->on_engine_event(std::move(ev));
                }
            }
        }

        // 处理 state_update_alert 事件
        EngineEvent make_status_update_event(const lt::torrent_status &s, const std::string &key) {
            if (s.errc) {
                log_e(key.c_str(), "任务发生错误. code: %d, msg: %s",
                      s.errc.value(), s.errc.message().c_str());
                return EngineEvent{
                    .type = EngineEventType::DOWNLOAD_FAILED,
                    .engine_key = key,
                    .protocol = DW_PROTOCOL_TORRENT,
                    .client_id = g_task_manager->client_id()
                };
            }
            EngineEvent ev;
            ev.client_id = g_task_manager->client_id();
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

        /**
         *  获取文件列表
         * @param h lt::torrent_handle
         * @param is_select 三态过滤（true=仅已选中，false=仅未选中，NULL=全部）
         * @return utils::file_array
         */
        utils::file_array build_flat_file_list(
            const lt::torrent_handle &h, const bool *is_select = nullptr) {
            if (!h.is_valid()) return {nullptr, 0};
            try {
                const std::shared_ptr<const lt::torrent_info> ti = h.torrent_file();
                if (!ti) return {nullptr, 0};
                const std::string save_path = h.status().save_path;
                const std::vector<lt::download_priority_t> priorities = h.get_file_priorities();
                const lt::file_storage &fs = ti->layout();
                const int file_count = fs.num_files();
                if (file_count <= 0) return {nullptr, 0};

                // 第一遍：计数
                int32_t count = 0;
                for (int i = 0; i < file_count; ++i) {
                    const lt::file_index_t idx{i};
                    if (fs.pad_file_at(idx)) continue;
                    const bool selected = i >= static_cast<int>(priorities.size())
                                          || priorities[static_cast<size_t>(i)] != lt::download_priority_t{0};
                    if (is_select && *is_select != selected) continue;
                    ++count;
                }
                if (count <= 0) return {nullptr, 0};

                // 分配连续数组
                dw_file_info_t *arr = utils::alloc_file_list(count);
                if (!arr) return {nullptr, 0};

                // RAII：异常时释放已分配的数组
                struct arr_guard {
                    dw_file_info_t *p = nullptr;
                    int32_t filled = 0;

                    ~arr_guard() {
                        if (!p) return;
                        for (int32_t k = 0; k < filled; ++k) {
                            std::free(p[k].name);
                            std::free(p[k].full_path);
                            std::free(p[k].ext);
                            std::free(p[k].physical_path);
                        }
                        std::free(p);
                    }
                } guard{.p = arr, .filled = 0};

                // 第二遍：填充
                int32_t j = 0;
                for (int i = 0; i < file_count; ++i) {
                    const lt::file_index_t idx{i};
                    if (fs.pad_file_at(idx)) continue;
                    const bool selected = i >= static_cast<int>(priorities.size())
                                          || priorities[static_cast<size_t>(i)] != lt::download_priority_t{0};
                    if (is_select && *is_select != selected) continue;
                    const std::string path = fs.file_path(idx);
                    arr[j].index = i;
                    arr[j].size = fs.file_size(idx);
                    arr[j].offset = fs.file_offset(idx);
                    arr[j].name = utils::dup_cstr(path);
                    arr[j].full_path = utils::dup_cstr(
                        save_path.empty() ? path : (std::filesystem::path(save_path) / path).string());
                    const std::string ext = utils::file_extension(path);
                    arr[j].ext = ext.empty() ? nullptr : utils::dup_cstr(ext);
                    ++j;
                    guard.filled = j;
                }
                guard.p = nullptr; // 成功，取消保护
                return {arr, count};
            } catch (const std::exception &e) {
                log_e("", "文件列表构建异常: %s", e.what());
                return {nullptr, 0};
            }
        }

        // 处理解析完成事件（含重名检测与路径整理）
        // 使用 orig_files() 获取原始文件结构检测磁盘重名；
        // 重名时调用 move_storage 整体迁移，完成后由 storage_moved_alert 发送 PARSED 事件。
        void handle_parsed(const lt::torrent_handle &h) {
            if (!h.is_valid()) return;
            const std::string key = info_hash_hex(h);
            if (key.empty()) return;

            EngineEvent ev;
            ev.type = EngineEventType::PARSED;
            ev.protocol = DW_PROTOCOL_TORRENT;
            ev.client_id = g_task_manager->client_id();
            ev.engine_key = key;

            // 重名判定已完成：从 file_record.parsed 判断
            if (g_task_manager) {
                const lt::torrent_status st = h.status();
                ev.save_path = st.save_path;
                bool parsed = g_task_manager->get_or_register_file_record(
                    g_task_manager->client_id(), DW_PROTOCOL_TORRENT, key, ev.save_path);
                if (parsed) {
                    log_i(key.c_str(), "已解析，跳过重名检测");
                    g_task_manager->on_engine_event(std::move(ev));
                    return;
                }
            }

            auto send_error = [&](const std::string &msg) {
                log_e(key.c_str(), "%s", msg.c_str());
                if (g_task_manager) {
                    ev.type = EngineEventType::DOWNLOAD_FAILED;
                    ev.message = msg;
                    g_task_manager->on_engine_event(ev);
                }
            };

            const lt::torrent_status st = h.status();
            const std::shared_ptr<const lt::torrent_info> ti = h.torrent_file();
            if (!ti) {
                send_error("解析失败");
                return;
            }

            const lt::file_storage &fs = ti->layout();

            ev.name = ti->name();
            ev.save_path = st.save_path;

            // 收集根条目（过滤 pad 文件）
            std::unordered_map<std::string, bool> root_entries;
            for (lt::file_index_t i(0); i < fs.end_file(); ++i) {
                if (fs.pad_file_at(i)) continue;
                std::filesystem::path fp(fs.file_path(i));
                std::string root = fp.begin() != fp.end() ? fp.begin()->string() : "";
                if (root.empty() || root == "." || root == ".." || root == "/") continue;
                bool is_dir = fp.has_parent_path();
                root_entries.try_emplace(root, is_dir);
                log_i(key.c_str(), "原始文件 -> %s", fp.string().c_str());
            }

            // 统计非 pad 文件数
            int non_pad_count = 0;
            for (lt::file_index_t i(0); i < fs.end_file(); ++i) {
                if (!fs.pad_file_at(i)) ++non_pad_count;
            }

            // 确定 base_name / is_dir / ext
            std::string base_name;
            bool is_dir = true;
            std::string ext;
            // 单文件 torrent：根条目可能是前置目录，提取实际文件名
            if (non_pad_count == 1) {
                for (lt::file_index_t i(0); i < fs.end_file(); ++i) {
                    if (fs.pad_file_at(i)) continue;
                    std::filesystem::path fp(fs.file_path(i));
                    base_name = fp.filename().string();
                    is_dir = false;
                    ext = fp.extension().string();
                    if (!ext.empty() && ext[0] == '.') ext = ext.substr(1);
                    break;
                }
            } else if (root_entries.size() == 1) {
                base_name = root_entries.begin()->first;
                is_dir = root_entries.begin()->second;
                if (!is_dir) {
                    ext = std::filesystem::path(base_name).extension().string();
                    if (!ext.empty() && ext[0] == '.') ext = ext.substr(1);
                }
            } else if (root_entries.size() > 1) {
                base_name = ev.name;
                if (base_name.empty()) {
                    base_name = std::filesystem::path(root_entries.begin()->first).stem().string();
                }
                is_dir = true;
            }
            ev.is_dir = is_dir;
            ev.ext = ext;
            log_i(key.c_str(), "根文件 -> %s(%s)", base_name.c_str(), is_dir ? "目录" : "文件");
            if (base_name.empty()) {
                send_error("无法确定文件名");
                return;
            }

            // 检测磁盘重名
            const bool needs_rename = std::filesystem::exists(
                std::filesystem::path(ev.save_path) / base_name);

            // 无重名：创建占位，直接发送 PARSED（含 content_root / is_dir / ext）
            if (!needs_rename) {
                ev.original_name = base_name; // 无重名时 original_name = content_root
                ev.content_root = base_name;
                std::filesystem::create_directories(ev.save_path);
                if (is_dir) {
                    std::filesystem::create_directories(
                        std::filesystem::path(ev.save_path) / base_name);
                } else {
                    std::ofstream ofs(
                        std::filesystem::path(ev.save_path) / base_name, std::ios::app);
                }
                log_i(key.c_str(), "解析完成");
                if (g_task_manager) g_task_manager->on_engine_event(std::move(ev));
                return;
            }

            // 磁盘重名：计算唯一目标名，调用 move_storage 整体迁移
            std::string target_name;
            if (!is_dir) {
                const std::string stem = std::filesystem::path(base_name).stem().string();
                const std::string new_stem = utils::acquire_wrapper_name(
                    ev.save_path, stem, nullptr);
                target_name = ext.empty() ? new_stem : new_stem + "." + ext;
            } else {
                target_name = utils::acquire_wrapper_name(
                    ev.save_path, base_name, nullptr);
            }
            const std::string new_save_path =
                    (std::filesystem::path(ev.save_path) / target_name).string();
            log_i(key.c_str(), "重名 '%s' -> '%s'，调用 move_storage",
                  base_name.c_str(), target_name.c_str());
            // 存储原始名称，供 storage_moved_alert 处理时消费
            g_pending_original_names[key] = base_name;
            try {
                h.move_storage(new_save_path);
            } catch (const std::exception &e) {
                log_e(key.c_str(), "move_storage 异常: %s", e.what());
                send_error("move_storage 调用失败");
            }
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
                    if (g_task_manager) g_task_manager->on_engine_event(std::move(ev));
                }
            }
            // 添加任务
            else if (const auto *at = lt::alert_cast<lt::add_torrent_alert>(a)) {
                const std::string key = info_hash_hex(at->params.info_hashes);
                if (at->error) {
                    // 添加任务失败
                    log_e(key.c_str(), "添加失败: %s", at->error.message().c_str());
                    if (key.empty()) return;
                    if (g_task_manager) {
                        g_task_manager->on_engine_event(EngineEvent{
                            .type = EngineEventType::DOWNLOAD_FAILED,
                            .engine_key = key,
                            .protocol = DW_PROTOCOL_TORRENT,
                            .client_id = g_task_manager->client_id()
                        });
                    }
                } else if (at->handle.is_valid()) {
                    // 添加任务成功
                    log_i(key.c_str(), "添加成功");
                    if (const lt::torrent_status st = at->handle.status(); st.has_metadata) {
                        handle_parsed(at->handle);
                    }
                }
            }
            // 磁力元数据就绪
            else if (const auto *mr = lt::alert_cast<lt::metadata_received_alert>(a)) {
                const std::string key = info_hash_hex(mr->handle);
                log_i(key.c_str(), "元数据就绪");
                handle_parsed(mr->handle);
            }
            // piece 完成：维护文件进度区间（连续达 1% 阈值后合并上报）
            else if (const auto *pf = lt::alert_cast<lt::piece_finished_alert>(a)) {
                handle_piece_finished(pf->handle, pf->piece_index);
            }
            // 下载完成
            else if (const auto *tf = lt::alert_cast<lt::torrent_finished_alert>(a)) {
                const std::string key = info_hash_hex(tf->handle);
                log_i(key.c_str(), "下载完成");
                if (key.empty()) return;
                if (g_task_manager) {
                    g_task_manager->on_engine_event(EngineEvent{
                        .type = EngineEventType::DOWNLOAD_COMPLETED,
                        .engine_key = key,
                        .protocol = DW_PROTOCOL_TORRENT,
                        .client_id = g_task_manager->client_id()
                    });
                }
            }
            // 任务错误
            else if (const auto *te = lt::alert_cast<lt::torrent_error_alert>(a)) {
                const std::string key = info_hash_hex(te->handle);
                log_e(key.c_str(), "下载错误: %s", te->error.message().c_str());
                if (key.empty()) return;
                if (g_task_manager) {
                    g_task_manager->on_engine_event(EngineEvent{
                        .type = EngineEventType::DOWNLOAD_FAILED,
                        .engine_key = key,
                        .protocol = DW_PROTOCOL_TORRENT,
                        .client_id = g_task_manager->client_id()
                    });
                }
            }
            // 文件错误
            else if (const auto *fe = lt::alert_cast<lt::file_error_alert>(a)) {
                const std::string key = info_hash_hex(fe->handle);
                log_e(key.c_str(), "文件错误 file: %s, msg: %s, errno: %d",
                      fe->filename(), fe->error.message().c_str(), fe->error.value());
                if (key.empty()) return;
                if (g_task_manager) {
                    g_task_manager->on_engine_event(EngineEvent{
                        .type = EngineEventType::DOWNLOAD_FAILED,
                        .engine_key = key,
                        .protocol = DW_PROTOCOL_TORRENT,
                        .client_id = g_task_manager->client_id()
                    });
                }
            }
            // 种子冲突：两个磁力链接解析到同一 torrent，双方进入 error 状态
            else if (const auto *tc = lt::alert_cast<lt::torrent_conflict_alert>(a)) {
                const std::string key = info_hash_hex(tc->handle);
                log_e(key.c_str(), "种子冲突: 两个磁力链接解析到同一 torrent");
                if (key.empty()) return;
                if (g_task_manager) {
                    g_task_manager->on_engine_event(EngineEvent{
                        .type = EngineEventType::DOWNLOAD_FAILED,
                        .engine_key = key,
                        .protocol = DW_PROTOCOL_TORRENT,
                        .client_id = g_task_manager->client_id()
                    });
                }
            }
            // 元数据解析失败：info-hash 校验不通过，libtorrent 自动重试，重试耗尽后 torrent 进入 error 状态
            else if (const auto *mf = lt::alert_cast<lt::metadata_failed_alert>(a)) {
                const std::string key = info_hash_hex(mf->handle);
                log_e(key.c_str(), "元数据解析失败: %s", mf->error.message().c_str());
                if (key.empty()) return;
                if (g_task_manager) {
                    g_task_manager->on_engine_event(EngineEvent{
                        .type = EngineEventType::DOWNLOAD_FAILED,
                        .engine_key = key,
                        .protocol = DW_PROTOCOL_TORRENT,
                        .client_id = g_task_manager->client_id()
                    });
                }
            }
            // 存储移动失败：move_storage() 调用失败，torrent 进入 error 状态
            else if (const auto *sm = lt::alert_cast<lt::storage_moved_failed_alert>(a)) {
                const std::string key = info_hash_hex(sm->handle);
                log_e(key.c_str(), "存储移动失败: %s, errno: %d",
                      sm->error.message().c_str(), sm->error.value());
                if (key.empty()) return;
                if (g_task_manager) {
                    g_task_manager->on_engine_event(EngineEvent{
                        .type = EngineEventType::DOWNLOAD_FAILED,
                        .engine_key = key,
                        .protocol = DW_PROTOCOL_TORRENT,
                        .client_id = g_task_manager->client_id()
                    });
                }
            }
            // 断点续传数据就绪
            else if (const auto *rd = lt::alert_cast<lt::save_resume_data_alert>(a)) {
                const std::string key = info_hash_hex(rd->handle);
                if (key.empty()) return;
                try {
                    const std::vector<char> buf = lt::write_resume_data_buf(rd->params);
                    if (!buf.empty() && g_task_manager) {
                        g_task_manager->get_store().save_resume(
                            g_task_manager->client_id(), DW_PROTOCOL_TORRENT, key,
                            reinterpret_cast<const uint8_t *>(buf.data()), buf.size());
                    }
                } catch (const std::exception &e) {
                    log_e(key.c_str(), "断点续传数据保存失败: %s", e.what());
                }
            }
            // 存储迁移完成：从新 save_path 提取 content_root，发送 PARSED 事件
            else if (const auto *sm = lt::alert_cast<lt::storage_moved_alert>(a)) {
                const std::string key = info_hash_hex(sm->handle);
                if (key.empty()) return;

                const std::string new_path = sm->storage_path();
                const std::string content_root =
                        std::filesystem::path(new_path).filename().string();
                // 从 pending map 取出原始名称（重名前的名字）
                std::string original_name;
                auto it = g_pending_original_names.find(key);
                if (it != g_pending_original_names.end()) {
                    original_name = it->second;
                    g_pending_original_names.erase(it);
                } else {
                    original_name = content_root; // 兜底：无重名时相同
                }
                log_i(key.c_str(), "存储迁移完成 -> '%s'，content_root='%s'，original_name='%s'",
                      new_path.c_str(), content_root.c_str(), original_name.c_str());

                if (g_task_manager) {
                    EngineEvent ev;
                    ev.type = EngineEventType::PARSED;
                    ev.protocol = DW_PROTOCOL_TORRENT;
                    ev.client_id = g_task_manager->client_id();
                    ev.engine_key = key;
                    ev.original_name = original_name;
                    ev.content_root = content_root;
                    ev.save_path = new_path;
                    ev.is_dir = true;
                    ev.ext = "";
                    g_task_manager->on_engine_event(std::move(ev));
                }
            }
            // 存储迁移失败
            else if (const auto *smf = lt::alert_cast<lt::storage_moved_failed_alert>(a)) {
                const std::string key = info_hash_hex(smf->handle);
                if (key.empty()) return;
                log_e(key.c_str(), "存储迁移失败: %s (op=%d)",
                      smf->error.message().c_str(), static_cast<int>(smf->op));
                if (g_task_manager) {
                    g_task_manager->on_engine_event(EngineEvent{
                        .type = EngineEventType::DOWNLOAD_FAILED,
                        .engine_key = key,
                        .protocol = DW_PROTOCOL_TORRENT,
                        .client_id = g_task_manager->client_id(),
                        .message = smf->error.message().c_str()
                    });
                }
            }
            // 暂停
            else if (lt::alert_cast<lt::torrent_paused_alert>(a)) {
                const std::string key = info_hash_hex(lt::alert_cast<lt::torrent_paused_alert>(a)->handle);
                log_i(key.c_str(), "暂停");
                if (key.empty()) return;
                if (g_task_manager) {
                    g_task_manager->on_engine_event(EngineEvent{
                        .type = EngineEventType::PAUSED,
                        .engine_key = key,
                        .protocol = DW_PROTOCOL_TORRENT,
                        .client_id = g_task_manager->client_id()
                    });
                }
            }
            // 恢复
            else if (lt::alert_cast<lt::torrent_resumed_alert>(a)) {
                const std::string key = info_hash_hex(lt::alert_cast<lt::torrent_resumed_alert>(a)->handle);
                log_i(key.c_str(), "恢复");
                if (key.empty()) return;
                if (g_task_manager) {
                    g_task_manager->on_engine_event(EngineEvent{
                        .type = EngineEventType::RESUMED,
                        .engine_key = key,
                        .protocol = DW_PROTOCOL_TORRENT,
                        .client_id = g_task_manager->client_id()
                    });
                }
            }
            // 任务已从 session 移除（不删文件场景：remove_torrent 不带 delete_files）
            else if (const auto *tr = lt::alert_cast<lt::torrent_removed_alert>(a)) {
                const std::string key = info_hash_hex(tr->info_hashes);
                if (key.empty()) return;
                log_i(key.c_str(), "任务已从 session 移除");
                if (g_task_manager) {
                    g_task_manager->on_engine_event(EngineEvent{
                        .type = EngineEventType::DELETED,
                        .engine_key = key,
                        .protocol = DW_PROTOCOL_TORRENT,
                        .client_id = g_task_manager->client_id(),
                        .delete_files = 0 // 不删文件，仅回收内存与 DB
                    });
                }
            }
            // 任务文件删除完成（删文件场景：remove_torrent 带 delete_files）
            // libtorrent 已删除下载文件，wrapper 负责清理包层目录（content_root）
            else if (const auto *td = lt::alert_cast<lt::torrent_deleted_alert>(a)) {
                const std::string key = info_hash_hex(td->info_hashes);
                if (key.empty()) return;
                log_i(key.c_str(), "任务文件删除完成");
                if (g_task_manager) {
                    g_task_manager->on_engine_event(EngineEvent{
                        .type = EngineEventType::DELETED,
                        .engine_key = key,
                        .protocol = DW_PROTOCOL_TORRENT,
                        .client_id = g_task_manager->client_id(),
                        .delete_files = 1 // 引擎已删文件，wrapper 清理包层目录
                    });
                }
            }
            // 任务文件删除失败
            else if (const auto *tdf = lt::alert_cast<lt::torrent_delete_failed_alert>(a)) {
                const std::string key = info_hash_hex(tdf->info_hashes);
                if (key.empty()) return;
                log_e(key.c_str(), "任务文件删除失败: %s",
                      tdf->error.message().c_str());
                if (g_task_manager) {
                    g_task_manager->on_engine_event(EngineEvent{
                        .type = EngineEventType::DOWNLOAD_FAILED,
                        .engine_key = key,
                        .protocol = DW_PROTOCOL_TORRENT
                    });
                }
            }
        }

        // 采集 alert
        void alert_loop(std::stop_token st) {
            while (!st.stop_requested()) {
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
                    log_e("bt", "采集事件异常: %s", e.what());
                } catch (...) {
                    log_e("bt", "采集事件未知异常");
                }
            }
        }

        // 每 5 秒对发起元数据请求 save_resume_data，
        void resume_loop(const std::stop_token &st) {
            while (!st.stop_requested()) {
                if (g_session) {
                    try {
                        int total = 0, downloading = 0, finished = 0, seeding = 0, paused = 0;
                        int downloading_meta = 0, checking = 0, checking_resume = 0;
                        for (const auto handles = g_session->get_torrents();
                             const auto &h: handles) {
                            try {
                                if (!h.is_valid()) continue;
                                const lt::torrent_status torrent_status = h.status();
                                if (torrent_status.has_metadata
                                    && torrent_status.state == lt::torrent_status::downloading) {
                                    // 请求保存恢复数据
                                    h.save_resume_data(lt::torrent_handle::save_info_dict
                                                       | lt::torrent_handle::if_download_progress
                                                       | lt::torrent_handle::if_config_changed
                                                       | lt::torrent_handle::if_state_changed
                                                       | lt::torrent_handle::if_metadata_changed);
                                }

                                // 数量统计
                                ++total;
                                if (torrent_status.flags & lt::torrent_flags::paused) ++paused;
                                else
                                    switch (torrent_status.state) {
                                        case lt::torrent_status::downloading: ++downloading;
                                            break;
                                        case lt::torrent_status::finished: ++finished;
                                            break;
                                        case lt::torrent_status::seeding: ++seeding;
                                            break;
                                        case lt::torrent_status::downloading_metadata: ++downloading_meta;
                                            break;
                                        case lt::torrent_status::checking_files: ++checking;
                                            break;
                                        case lt::torrent_status::checking_resume_data: ++checking_resume;
                                            break;
                                        default: break;
                                    }
                            } catch (...) {
                            }
                        }
                        log_i("bt", "任务状态统计: 总数=%d 下载中=%d 元数据=%d 校验=%d 校验resume=%d 已完成=%d 做种=%d 暂停=%d",
                              total, downloading, downloading_meta, checking, checking_resume, finished, seeding,
                              paused);
                    } catch (const std::exception &e) {
                        log_e("bt", "恢复数据定时请求异常: %s", e.what());
                    }
                }
                std::this_thread::sleep_for(std::chrono::seconds(5));
            }
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
                log_e(task_id, "操作结果 code=%d, msg=%s", code, msg ? msg : "");
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

    int32_t TorrentEngine::init(const dw_config_t *cfg, TaskManager *task_manager) {
        if (initialized_) {
            return 0;
        }
        try {
            lt::settings_pack pack;
            // 订阅错误、状态、存储与 piece 完成（进度区间维护）；屏蔽 peer/block 等细粒度事件
            pack.set_int(lt::settings_pack::alert_mask,
                         lt::alert_category::error | lt::alert_category::status
                         | lt::alert_category::storage | lt::alert_category::piece_progress);

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
                // 默认 trackers：深拷贝到引擎内，避免依赖调用方指针生命周期。
                g_default_trackers.clear();
                if (cfg->trackers && cfg->tracker_count > 0) {
                    for (int32_t i = 0; i < cfg->tracker_count; ++i) {
                        if (cfg->trackers[i] && cfg->trackers[i][0]) {
                            g_default_trackers.emplace_back(cfg->trackers[i]);
                        }
                    }
                }
            }
            if (listen_port > 0) {
                pack.set_str(lt::settings_pack::listen_interfaces,
                             "0.0.0.0:" + std::to_string(listen_port));
            }

            g_session = std::make_unique<lt::session>(std::move(pack));
        } catch (const std::exception &e) {
            log_e("bt", "初始化引擎失败: %s", e.what());
            return -1;
        }
        if (!g_session || !g_session->is_valid()) {
            log_e("bt", "初始化引擎失败");
            g_session.reset();
            return -1;
        }

        g_task_manager = task_manager;
        g_alert_thread = std::jthread(alert_loop);
        g_resume_thread = std::jthread(resume_loop);

        initialized_ = true;
        log_i("bt", "初始化引擎完成 interval: %dms", g_interval_ms);
        return 0;
    }

    void TorrentEngine::destroy() {
        if (!initialized_) {
            return;
        }
        // 先停止工作线程，再销毁 session。
        if (g_resume_thread.joinable()) {
            g_resume_thread.request_stop();
            g_resume_thread.join();
        }
        if (g_alert_thread.joinable()) {
            g_alert_thread.request_stop();
            g_alert_thread.join();
        }
        g_session.reset();

        g_task_manager = nullptr;
        g_default_trackers.clear();
        initialized_ = false;
        log_i("bt", "销毁引擎完成");
    }

    void TorrentEngine::apply_file_priorities(const std::string &task_id,
                                              const std::string &client_id,
                                              const std::vector<int32_t> &priority_file_indexes) {
        log_i(task_id.c_str(), "设置文件优先级: priority_indexes=%s count=%zu",
              to_string(priority_file_indexes).c_str(), priority_file_indexes.size());
        const lt::torrent_handle handle = find_handle(task_id, client_id);
        if (!handle.is_valid()) {
            log_e(task_id.c_str(), "handle 无效");
            return;
        }
        const std::shared_ptr<const lt::torrent_info> ti = handle.torrent_file();
        if (!ti) return;
        const int n = ti->layout().num_files();
        // 读取当前优先级，将已有 top_priority 重置为 default（适配取消优先）
        std::vector<lt::download_priority_t> prio = handle.get_file_priorities();
        if (static_cast<int>(prio.size()) < n) {
            prio.resize(static_cast<size_t>(n), lt::dont_download);
        }
        for (auto &p: prio) {
            if (p == lt::top_priority) p = lt::default_priority;
        }
        // priority_file_indexes: 优先下载，设为最高优先级
        for (const int32_t idx: priority_file_indexes) {
            if (idx >= 0 && idx < n) {
                prio[static_cast<size_t>(idx)] = lt::top_priority;
            }
        }
        try {
            handle.prioritize_files(prio);
        } catch (const std::exception &e) {
            log_e(task_id.c_str(), "设置文件优先级失败: %s", e.what());
        }
        log_i(task_id.c_str(), "设置文件优先级完成");
    }

    void TorrentEngine::post_fail(const std::string &key, const std::string &message) {
        if (!g_task_manager) return;
        EngineEvent engine_event;
        engine_event.type = EngineEventType::DOWNLOAD_FAILED;
        engine_event.engine_key = key;
        engine_event.protocol = DW_PROTOCOL_TORRENT;
        engine_event.client_id = g_task_manager->client_id();
        engine_event.message = message;
        g_task_manager->on_engine_event(engine_event);
    }

    void TorrentEngine::post_error(const std::string &key, const std::string &message) {
        if (!g_task_manager) return;
        EngineEvent engine_event;
        engine_event.type = EngineEventType::DOWNLOAD_FAILED;
        engine_event.engine_key = key;
        engine_event.protocol = DW_PROTOCOL_TORRENT;
        engine_event.client_id = g_task_manager->client_id();
        engine_event.reason = DW_REASON_ERROR;
        engine_event.message = message;
        g_task_manager->on_engine_event(engine_event);
    }

    int32_t TorrentEngine::add_task(const dw_task_params_t *params,
                                    dw_submit_result_t *out_result) {
        log_i("", "添加任务: %s", params ? to_string(*params).c_str() : "");
        if (!params->save_path || !params->save_path[0]) {
            log_e("", "save_path 无效");
            set_result(out_result, "", DW_REASON_ERROR, "保存路径无效");
            return -1;
        }
        if (!g_session || !g_session->is_valid()) {
            log_e("", "session 无效");
            set_result(out_result, "", DW_REASON_ERROR, "下载引擎未启动");
            return -1;
        }

        lt::add_torrent_params atp;
        atp.save_path = params->save_path;
        bool source_ok = false;

        // magnet_link > torrent_file > info_hash。
        if (params->magnet_link && params->magnet_link[0]) {
            lt::error_code ec;
            lt::parse_magnet_uri(params->magnet_link, atp, ec);
            if (!ec) {
                source_ok = true;
            }
        }
        if (!source_ok && params->torrent_file && params->torrent_file[0]) {
            lt::error_code ec;
            const lt::add_torrent_params loaded = lt::load_torrent_file(
                params->torrent_file, ec, lt::load_torrent_limits{});
            if (!ec) {
                atp.ti = loaded.ti;
                source_ok = true;
            }
        }
        if (!source_ok && params->magnet_link && params->magnet_link[0]) {
            lt::sha1_hash h;
            if (lt::aux::from_hex(std::string(params->magnet_link), h.data())) {
                atp.info_hashes = lt::info_hash_t(h);
                source_ok = true;
            }
        }
        if (!source_ok) {
            log_e("", "无效下载任务");
            set_result(out_result, "", DW_REASON_ERROR, "无效下载任务");
            return -1;
        }

        for (const auto &t: g_default_trackers) {
            atp.trackers.emplace_back(t);
        }
        // web seeds
        if (params->url_seeds && params->url_seed_count > 0) {
            for (int i = 0; i < params->url_seed_count; ++i) {
                if (params->url_seeds[i] && params->url_seeds[i][0]) {
                    atp.url_seeds.emplace_back(params->url_seeds[i]);
                }
            }
        }

        atp.flags = lt::torrent_flags::update_subscribe
                    | lt::torrent_flags::need_save_resume
                    | lt::torrent_flags::default_dont_download;

        lt::torrent_handle handle;
        try {
            handle = g_session->add_torrent(std::move(atp));
        } catch (const std::exception &e) {
            log_e("", "任务添加失败: %s", e.what());
            set_result(out_result, "", DW_REASON_ERROR, "任务添加失败");
            return -1;
        }
        if (!handle.is_valid()) {
            set_result(out_result, "", DW_REASON_ERROR, "任务添加失败");
            return -1;
        }

        const std::string info_hash = info_hash_hex(handle);
        log_i(info_hash.c_str(), "添加任务完成");

        set_result(out_result, info_hash.c_str(), DW_REASON_NONE, nullptr);
        out_result->info_hash = utils::dup_cstr(info_hash);

        if (auto [files, count] = build_flat_file_list(handle); files && count > 0) {
            out_result->files = files;
            out_result->file_count = count;
        }
        return 0;
    }

    void TorrentEngine::resume_task(const std::string &info_hash,
                                    const std::string &client_id,
                                    const std::vector<int32_t> &priority_file_indexes) {
        log_i(info_hash.c_str(), "恢复任务");
        if (info_hash.empty()) {
            log_e("", "info_hash 为空");
            return;
        }
        if (!g_session || !g_session->is_valid()) {
            log_e(info_hash.c_str(), "session 无效");
            post_error(info_hash, "下载引擎未启动");
            return;
        }

        const lt::torrent_handle handle = find_handle(info_hash, client_id);
        if (!handle.is_valid()) {
            log_e(info_hash.c_str(), "handle 获取失败");
            post_error(info_hash, "任务不存在");
            return;
        }

        if (const lt::torrent_status ts = handle.status(); ts.errc) {
            handle.clear_error();
        }

        // 设置文件优先级
        apply_file_priorities(info_hash, client_id, priority_file_indexes);
        try {
            if (const lt::torrent_status st = handle.status(); st.flags & lt::torrent_flags::paused) {
                handle.set_flags(lt::torrent_flags::auto_managed);
                handle.resume();
            }
        } catch (const std::exception &e) {
            log_e(info_hash.c_str(), "handle 恢复失败: %s", e.what());
            post_error(info_hash, "下载失败");
        }
    }

    int32_t TorrentEngine::pause_task(const std::string &info_hash,
                                      const std::string &client_id,
                                      dw_submit_result_t *out_result) {
        log_i(info_hash.c_str(), "暂停任务");
        if (info_hash.empty()) {
            log_e("", "info_hash 为空");
            set_result(out_result, info_hash.c_str(), DW_REASON_ERROR, "任务不存在");
            return -1;
        }
        const lt::torrent_handle handle = find_handle(info_hash, client_id);
        if (!handle.is_valid()) {
            log_e(info_hash.c_str(), "handle 获取失败");
            set_result(out_result, info_hash.c_str(), DW_REASON_ERROR, "任务不存在");
            return -1;
        }
        try {
            if (const lt::torrent_status st = handle.status(); !(st.flags & lt::torrent_flags::paused)) {
                handle.unset_flags(lt::torrent_flags::auto_managed);
                handle.pause();
            }
        } catch (const std::exception &e) {
            log_e(info_hash.c_str(), "handle 暂停失败: %s", e.what());
            set_result(out_result, info_hash.c_str(), DW_REASON_ERROR, "暂停失败");
            return -1;
        }

        set_result(out_result, info_hash.c_str(), DW_REASON_NONE, nullptr);
        return 0;
    }

    int32_t TorrentEngine::delete_task(const std::string &info_hash,
                                       const std::string &client_id,
                                       const int32_t delete_files,
                                       dw_submit_result_t *out_result) {
        log_i(info_hash.c_str(), "删除任务 delete_files=%d", delete_files);
        if (info_hash.empty()) {
            log_e("", "info_hash 为空");
            set_result(out_result, info_hash.c_str(), DW_REASON_ERROR, "任务不存在");
            return -1;
        }
        if (const lt::torrent_handle handle = find_handle(info_hash, client_id); handle.is_valid()) {
            try {
                if (g_session) {
                    const auto opt = delete_files
                                         ? lt::session::delete_files
                                         : lt::session::delete_partfile;
                    g_session->remove_torrent(handle, opt);
                }
            } catch (const std::exception &e) {
                log_e(info_hash.c_str(), "handle 删除失败: %s", e.what());
                set_result(out_result, info_hash.c_str(), DW_REASON_ERROR, "删除失败");
                return -1;
            }
        }
        set_result(out_result, info_hash.c_str(), DW_REASON_NONE, nullptr);
        return 0;
    }

    bool TorrentEngine::task_released(const std::string &task_id) {
        if (task_id.empty()) return true;
        // session 已销毁 / 未初始化：全部存储句柄已关闭，视为已释放。
        if (!initialized_ || !g_session) return true;
        // remove_torrent 后 handle 在 libtorrent 内部任务（disk-io 等）释放引用
        // 前仍有效；find_handle 失效即代表移除收敛、文件句柄已关闭。
        return !find_handle(task_id).is_valid();
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
                    log_i(key.c_str(), "分享率达标，释放完成");
                } catch (const std::exception &e) {
                    log_e(key.c_str(), "分享率达标，释放失败 msg=%s", e.what());
                }
            }
        }
    }

    bool TorrentEngine::get_file_path(const std::string &task_id, int32_t file_index,
                                      std::string &out_path, int64_t &out_size) {
        if (task_id.empty() || file_index < 0) return false;
        const lt::torrent_handle h = find_handle(task_id);
        if (!h.is_valid()) return false;
        try {
            const std::shared_ptr<const lt::torrent_info> ti = h.torrent_file();
            if (!ti || file_index >= ti->layout().num_files()) return false;
            const lt::file_index_t idx{file_index};
            // 物理路径 = handle 当前 save_path / 文件相对路径（move_storage 后自动跟随）。
            const std::string save_path =
                    h.status(lt::torrent_handle::query_save_path).save_path;
            if (save_path.empty()) {
                log_e(task_id.c_str(), "save_path 为空");
                return false;
            }
            out_path = (std::filesystem::path(save_path) / ti->layout().file_path(idx)).string();
            out_size = ti->layout().file_size(idx);
            return true;
        } catch (const std::exception &e) {
            log_e(task_id.c_str(), "文件路径查询异常: %s", e.what());
            return false;
        }
    }

    utils::file_array TorrentEngine::get_file_list(const std::string &task_id) {
        if (task_id.empty()) return {nullptr, 0};
        const lt::torrent_handle h = find_handle(task_id);
        if (!h.is_valid()) return {nullptr, 0};
        // 优先级口径与 PARSED 上报一致：仅选中文件（dont_download 不含）。
        const bool selected = true;
        return build_flat_file_list(h, &selected);
    }

    void TorrentEngine::post_updates() {
        // 节拍入口（A 线程调用）：仅触发状态更新，续传由独立定时线程负责。
        if (!g_session) return;
        try { g_session->post_torrent_updates(); } catch (...) {
        }
    }

    char *TorrentEngine::magnet_to_info_hash(const std::string &magnet_link) {
        if (magnet_link.empty()) {
            return nullptr;
        }
        lt::error_code ec;
        lt::add_torrent_params atp;
        lt::parse_magnet_uri(magnet_link, atp, ec);
        if (ec) {
            log_e("bt", "解析磁力链接失败: %s", ec.message().c_str());
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
        return utils::dup_cstr(s);
    }

    char *TorrentEngine::torrent_file_to_info_hash(const std::string &torrent_file_path) {
        if (torrent_file_path.empty()) {
            return nullptr;
        }
        lt::error_code ec;
        lt::add_torrent_params loaded = lt::load_torrent_file(
            torrent_file_path, ec, lt::load_torrent_limits{});
        if (ec) {
            log_e("bt", "加载 .torrent 文件失败: %s", ec.message().c_str());
            return nullptr;
        }
        const std::shared_ptr<const lt::torrent_info> ti = loaded.ti;
        const lt::info_hash_t ih = ti->info_hashes();
        std::string s;
        if (ih.has_v2()) {
            s = lt::aux::to_hex(ih.v2);
        } else if (ih.has_v1()) {
            s = lt::aux::to_hex(ih.v1);
        } else {
            return nullptr;
        }
        return utils::dup_cstr(s);
    }

    char *TorrentEngine::info_hash_to_magnet(const std::string &task_id) {
        log_i(task_id.c_str(), "info_hash_to_magnet 进入");
        if (task_id.empty()) {
            return nullptr;
        }
        const lt::torrent_handle handle = find_handle(task_id);
        if (!handle.is_valid()) {
            log_e(task_id.c_str(), "info_hash_to_magnet 任务不存在");
            return nullptr;
        }
        try {
            // ABI=100（deprecated-functions=OFF）下 handle 重载不可用，手工构造 atp 生成。
            lt::add_torrent_params atp;
            atp.info_hashes = handle.info_hashes();
            atp.name = handle.status(lt::torrent_handle::query_name).name;
            for (const auto &te: handle.trackers()) atp.trackers.push_back(te.url);
            const std::string magnet = lt::make_magnet_uri(atp);
            if (magnet.empty()) return nullptr;
            log_i(task_id.c_str(), "info_hash_to_magnet 成功");
            return utils::dup_cstr(magnet);
        } catch (const std::exception &e) {
            log_e(task_id.c_str(), "生成磁力链接失败: %s", e.what());
            return nullptr;
        }
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

    std::vector<dw_byte_range_t> TorrentEngine::get_file_ranges(const std::string &task_id,
                                                                int32_t file_index) {
        std::vector<dw_byte_range_t> ranges;
        if (task_id.empty() || file_index < 0) {
            return ranges;
        }
        const lt::torrent_handle handle = find_handle(task_id);
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
            const lt::file_storage &fs = ti->layout();
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
            log_e(task_id.c_str(), "get_file_ranges 异常: %s", e.what());
            ranges.clear();
        }
        return ranges;
    }

    int32_t TorrentEngine::apply_file_selection(const std::string &task_id,
                                                const std::vector<int32_t> &file_indexes) {
        if (task_id.empty()) return -1;
        lt::torrent_handle handle = find_handle(task_id);
        if (!handle.is_valid()) {
            log_e(task_id.c_str(), "apply_file_selection 任务不存在");
            return -1;
        }
        std::shared_ptr<const lt::torrent_info> ti;
        try { ti = handle.torrent_file(); } catch (...) { ti = nullptr; }
        if (!ti) {
            // 调度出口以 metadata_ready && naming_ready 合取为前提，正常不应到此。
            log_e(task_id.c_str(), "apply_file_selection 元数据未就绪");
            return -1;
        }
        try {
            const int n = ti->layout().num_files();
            // 全量显式定型：磁力（default_dont_download 物化全 0）与 .torrent（默认全 4）
            // 添加路径的初始优先级不同，统一覆写为确认意图，消除路径差异。
            // 空 = 全部默认优先级；非空 = 选中默认、其余不下载。
            std::vector<lt::download_priority_t> prio(
                static_cast<size_t>(n > 0 ? n : 0),
                file_indexes.empty() ? lt::default_priority : lt::dont_download);
            for (const int32_t idx: file_indexes) {
                if (idx >= 0 && idx < n) {
                    prio[static_cast<size_t>(idx)] = lt::default_priority;
                }
            }
            handle.prioritize_files(prio);
            // 优先级已显式定型，解除待选保护并恢复运行（此刻解除标志不再影响优先级）。
            handle.unset_flags(lt::torrent_flags::default_dont_download);
            handle.set_flags(lt::torrent_flags::auto_managed);
            handle.resume();
        } catch (const std::exception &e) {
            log_e(task_id.c_str(), "apply_file_selection 失败: %s", e.what());
            return -1;
        }
        log_i(task_id.c_str(),
              "apply_file_selection 成功 count=%zu (空即全部)", file_indexes.size());
        return 0;
    }
} // namespace dw
