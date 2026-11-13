/**
 * @file torrent_engine.cpp
 * @brief BT/Torrent 下载引擎实现（基于 libtorrent）。
 *
 * 单例 session 管理 BT 任务；alert 线程回填 Task/TaskFile 并标记 dirty。
 * status 线程每秒触发状态采集及配置同步，resume 线程每 5 秒请求续传数据。
 * piece 完成时按文件进度阈值更新已下载区间。
 */

#include "torrent/torrent_engine.h"

#include "core/task_manager.h"
#include "core/task.h"
#include "store/task_resume_store.h"
#include "store/task_store.h"
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
#include <libtorrent/load_torrent.hpp>
#include <libtorrent/error_code.hpp>
#include <libtorrent/alert_types.hpp>

#include <algorithm>
#include <atomic>
#include <memory>
#include <limits>
#include <libtorrent/read_resume_data.hpp>
#include <libtorrent/write_resume_data.hpp>
#include <libtorrent/hex.hpp>

#include <chrono>
#include <cmath>
#include <filesystem>
#include <mutex>
#include <string>
#include <exception>
#include <ranges>
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
        // alert 轮询线程
        std::thread g_alert_thread;
        // 恢复数据定时请求线程（每 5 秒）
        std::thread g_resume_thread;
        // 状态采集定时线程（每 1 秒触发 post_torrent_updates）
        std::thread g_status_thread;
        // 线程运行标志：destroy() 置 false，各循环在下一次检查时退出。
        std::atomic<bool> g_running{false};
        // 默认 tracker 列表：init 从 cfg->trackers 拷贝，添加/恢复任务时统一注入。
        // 任务管理器
        class TaskManager *g_task_manager = nullptr;

        // 从 info_hash_t 提取 hex（优先 v2，回退 v1）
        std::string info_hash_hex(const lt::info_hash_t &ih) {
            if (ih.has_v2())
                return lt::aux::to_hex(ih.v2);
            if (ih.has_v1())
                return lt::aux::to_hex(ih.v1);
            return {};
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

        lt::torrent_handle find_handle(const std::string &info_hash, const bool upload_only = false) {
            if (info_hash.empty()) {
                return {};
            }
            if (!g_session || !g_task_manager) {
                log_e(info_hash.c_str(), "引擎未启动");
                return {};
            }
            lt::sha1_hash h1;
            if (info_hash.size() == 40 && lt::aux::from_hex(info_hash, h1.data())) {
                if (auto th = g_session->find_torrent(h1); th.is_valid()) {
                    return th;
                }
            }
            lt::sha256_hash h2;
            if (info_hash.size() == 64 && lt::aux::from_hex(info_hash, h2.data())) {
                if (auto th = g_session->find_torrent(h2); th.is_valid()) {
                    return th;
                }
            }
            for (const auto handles = g_session->get_torrents();
                 const auto &th: handles) {
                if (info_hash_hex(th) == info_hash) {
                    return th;
                }
            }

            Task task;
            if (!g_task_manager->get_task_store().find_by_task_id(info_hash, task)) {
                return {};
            }

            // 只调整引擎必需的标志，保留续传中的其他设置。
            // 按调用方需要启用仅上传模式。
            const auto add_torrent = [upload_only](lt::add_torrent_params &atp,
                                                   const std::string &trace_id) -> lt::torrent_handle {
                atp.flags |= lt::torrent_flags::update_subscribe
                        | lt::torrent_flags::need_save_resume;
                if (upload_only) atp.flags |= lt::torrent_flags::upload_mode;

                atp.flags &= ~lt::torrent_flags::auto_managed;
                try {
                    if (auto handle = g_session->add_torrent(std::move(atp)); handle.is_valid()) {
                        return handle;
                    }
                    log_e(trace_id.c_str(), "构建处理器失败 无法构建任务");
                } catch (const std::exception &e) {
                    log_e(trace_id.c_str(), "构建处理器失败 massage={}", e.what());
                }
                return {};
            };
            // 根据恢复数据恢复 handler
            if (TaskResume task_resume;
                g_task_manager->get_task_resume_store().find(info_hash, task_resume) && !task_resume.resume_data.
                empty()) {
                try {
                    const lt::span buf(
                        reinterpret_cast<const char *>(task_resume.resume_data.data()),
                        static_cast<std::ptrdiff_t>(task_resume.resume_data.size()));
                    lt::add_torrent_params atp = lt::read_resume_data(buf);
                    if (auto handle = add_torrent(atp, info_hash); handle.is_valid()) {
                        return handle;
                    }
                } catch (const std::exception &e) {
                    log_e(info_hash.c_str(), "恢复数据解析失败 massage={}", e.what());
                }
            }

            // 根据任务的原始数据恢复 handler
            if (!task.magnet_link.empty()) {
                lt::add_torrent_params atp;
                lt::error_code ec;
                lt::parse_magnet_uri(task.magnet_link, atp, ec);
                atp.save_path = task.save_path;
                if (ec) {
                    log_e(info_hash.c_str(), "无法通过链接[{}]恢复 massage={}", task.magnet_link, ec.what());
                } else {
                    if (auto handle = add_torrent(atp, info_hash); handle.is_valid()) {
                        return handle;
                    }
                }
            }
            if (!task.torrent_file.empty()) {
                lt::error_code ec;
                lt::add_torrent_params atp =
                        lt::load_torrent_file(task.torrent_file, ec, lt::load_torrent_limits{});
                if (ec) {
                    log_e(info_hash.c_str(), "无法通过文件[{}]恢复 massage={}", task.torrent_file, ec.what());
                } else {
                    atp.save_path = task.save_path;
                    if (auto handle = add_torrent(atp, info_hash); handle.is_valid()) {
                        return handle;
                    }
                }
            }

            if (task.torrent_file.empty() && task.magnet_link == info_hash) {
                lt::add_torrent_params atp;
                atp.info_hashes = info_hash.size() == 40 ? lt::info_hash_t(h1) : lt::info_hash_t(h2);
                atp.save_path = task.save_path;
                if (auto handle = add_torrent(atp, info_hash); handle.is_valid()) return handle;
            }

            log_e(info_hash.c_str(), "无法获取处理器");
            return {};
        }

        // 按 piece 位图重建受影响文件的完整下载区间。
        void handle_piece_finished(const lt::torrent_handle &h, const lt::piece_index_t piece) {
            const std::string info_hash = info_hash_hex(h);
            if (info_hash.empty()) return;
            std::scoped_lock task_lock(g_task_manager->get_mutex());
            Task *task = TorrentEngine::get_task(info_hash);
            if (!task || task->is_delete) return;
            const auto ti = h.torrent_file();
            if (!ti || piece < lt::piece_index_t{0} || piece > ti->last_piece()) return;
            const auto &fs = ti->layout();
            const int64_t piece_length = ti->piece_length();
            if (piece_length <= 0) return;

            // 收集 piece 涉及的文件
            std::vector<TaskFile *> affected;
            for (const auto &slice: ti->map_block(piece, 0, ti->piece_size(piece))) {
                if (fs.pad_file_at(slice.file_index)) continue;
                const auto index = static_cast<int32_t>(slice.file_index);
                const auto it = std::ranges::find_if(task->files,
                                                     [index](const TaskFile &file) {
                                                         return file.file_index == index;
                                                     });
                if (it != task->files.end() && it->total_size > 0)
                    affected.push_back(&*it);
            }
            if (affected.empty()) return;

            // 受影响文件共用一次位图查询。
            const auto status = h.status(lt::torrent_handle::query_pieces);
            const auto &pieces = status.pieces;
            for (TaskFile *file: affected) {
                const lt::file_index_t index{file->file_index};
                const int64_t offset = fs.file_offset(index);
                const int64_t size = fs.file_size(index);
                if (size <= 0) continue;
                const int64_t end = offset + size - 1;
                const int first = static_cast<int>(offset / piece_length);
                const int last = static_cast<int>(end / piece_length);
                std::vector<FileSegment> segments;
                int64_t done = 0;
                for (int p = first; p <= last; ++p) {
                    const lt::piece_index_t pi{p};
                    const bool downloaded = pi < pieces.end_index() && pieces.get_bit(pi);
                    const auto segment_status = downloaded ? SegmentStatus::Downloaded : SegmentStatus::NotDownloaded;
                    const int64_t start = std::max<int64_t>(p * piece_length, offset) - offset;
                    const int64_t finish = std::min<int64_t>((p + 1LL) * piece_length - 1, end) - offset;
                    if (downloaded) done += finish - start + 1;
                    if (!segments.empty() && segments.back().status == segment_status &&
                        segments.back().end + 1 == start)
                        segments.back().end = finish;
                    else
                        segments.push_back({.start = start, .end = finish, .status = segment_status});
                }
                // 进度和区间来自同一份位图快照，保证两者一致。
                file->segments = std::move(segments);
                file->total_done = done;
                file->progress = static_cast<double>(done) / static_cast<double>(size);
                file->dirty = true;
                task->dirty = true;
            }
        }

        // 处理错误
        void handle_task_error(const std::string &info_hash, const lt::error_code &error) {
            if (info_hash.empty() || !error) return;
            log_e(info_hash.c_str(), "任务错误 category={}, code={}, msg={}",
                  error.category().name(), error.value(), error.what());
            std::scoped_lock task_lock(g_task_manager->get_mutex());
            Task *task = TorrentEngine::get_task(info_hash);
            if (!task || task->is_delete) return;
            task->message = "下载失败";
            task->dirty = true;

            namespace errc = boost::system::errc;
            // 同时配置系统错误的展示信息及重试策略，未知错误默认不自动重试。
            struct SystemError {
                errc::errc_t code;
                const char *message;
                bool retryable;
            };
            static const SystemError system_errors[] = {
                {errc::no_space_on_device, "磁盘空间不足", false},
                {errc::permission_denied, "没有文件访问权限", false},
                {errc::read_only_file_system, "保存位置为只读", false},
                {errc::no_such_file_or_directory, "文件或目录不存在", false},
                {errc::io_error, "文件读写失败", false},
                {errc::not_enough_memory, "内存不足", false},
                {errc::timed_out, "网络错误", true},
                {errc::connection_refused, "网络错误", true},
                {errc::connection_reset, "网络错误", true},
                {errc::connection_aborted, "网络错误", true},
                {errc::network_down, "网络错误", true},
                {errc::network_unreachable, "网络错误", true},
                {errc::host_unreachable, "网络错误", true},
                {errc::not_connected, "网络错误", false},
                {errc::broken_pipe, "网络错误", false},
                {errc::resource_unavailable_try_again, "下载失败", true},
                {errc::interrupted, "下载失败", true},
            };
            for (const auto &[code, message, retryable]: system_errors) {
                if (error == errc::make_error_condition(code)) {
                    task->message = message;
                    task->status = retryable ? DW_TASK_STATUS_FAIL : DW_TASK_STATUS_ERROR;
                    return;
                }
            }

            if (error.category() == lt::libtorrent_category()) {
                namespace errors = lt::errors;
                switch (error.value()) {
                    case errors::no_memory: task->message = "内存不足";
                        break;
                    case errors::duplicate_torrent: task->message = "任务已存在";
                        break;
                    case errors::file_collision: task->message = "任务文件路径冲突";
                        break;
                    case errors::file_too_short: task->message = "文件读写失败";
                        break;
                    case errors::failed_hash_check:
                        task->status = DW_TASK_STATUS_FAIL;
                        break;
                    case errors::timed_out:
                        task->message = "网络错误";
                        task->status = DW_TASK_STATUS_FAIL;
                        break;
                    case errors::timed_out_no_interest:
                    case errors::timed_out_inactivity:
                    case errors::timed_out_no_handshake:
                    case errors::timed_out_no_request:
                        task->message = "网络错误";
                        break;
                    case errors::torrent_is_no_dict:
                    case errors::torrent_info_no_dict:
                    case errors::torrent_file_parse_failed:
                    case errors::invalid_bencoding:
                    case errors::invalid_swarm_metadata:
                    case errors::torrent_missing_info:
                    case errors::torrent_missing_piece_length:
                    case errors::torrent_missing_name:
                    case errors::torrent_missing_pieces:
                    case errors::torrent_invalid_name:
                    case errors::torrent_invalid_length:
                    case errors::torrent_invalid_hashes:
                    case errors::no_files_in_torrent:
                    case errors::too_many_pieces_in_torrent:
                    case errors::mismatching_info_hash:
                        task->message = "种子文件无效";
                        break;
                    case errors::missing_info_hash_in_uri:
                    case errors::unsupported_url_protocol:
                    case errors::url_parse_error:
                        task->message = "下载链接无效";
                        break;
                    default: break;
                }
            }
        }

        void handle_files(const lt::torrent_handle &h) {
            const std::string info_hash = info_hash_hex(h);
            if (info_hash.empty()) return;
            if (!h.is_valid() || !g_task_manager) {
                log_e("bt", "引擎未启动");
                return;
            }
            std::scoped_lock task_lock(g_task_manager->get_mutex());
            Task *task = TorrentEngine::get_task(info_hash);
            if (!task || task->is_delete) {
                log_e(info_hash.c_str(), "任务不存在或已删除，跳过任务解析");
                return;
            }

            if (const auto ti = h.torrent_file(); ti) {
                const auto &files = ti->layout();
                const auto save_path = h.status(lt::torrent_handle::query_save_path).save_path;
                const auto priorities = h.get_file_priorities();
                std::vector<std::int64_t> progress;
                h.file_progress(progress);
                task->files.reserve(files.num_files());
                for (int i = 0; i < files.num_files(); ++i) {
                    const lt::file_index_t index{i};
                    if (files.pad_file_at(index)) continue;
                    auto entry = std::ranges::lower_bound(task->files, i, {}, &TaskFile::file_index);
                    if (entry == task->files.end() || entry->file_index != i) {
                        entry = task->files.emplace(entry);
                        entry->file_index = i;
                    }
                    auto &file = *entry;
                    file.task_id = info_hash;
                    file.path = files.file_path(index);
                    if (!save_path.empty()) {
                        file.full_path = (std::filesystem::path(save_path) / file.path).string();
                    }
                    file.name = std::filesystem::path(file.path).filename().string();
                    file.extension = utils::file_extension(file.path);
                    file.total_size = files.file_size(index);
                    if (i < static_cast<int>(progress.size())) {
                        const auto total_done = std::clamp<int64_t>(progress[i], 0, file.total_size);
                        if (total_done < file.total_done) file.segments.clear();
                        file.total_done = total_done;
                    }
                    file.progress = file.total_size > 0
                                        ? static_cast<double>(file.total_done) / static_cast<double>(file.total_size)
                                        : 1.0;
                    if (i < static_cast<int>(priorities.size()))
                        file.prioritize = static_cast<std::uint8_t>(priorities[i]);
                    file.dirty = true;
                }
                task->dirty = true;
            }
        }

        // 处理 alert
        void handle_alert(const lt::alert *a) {
            if (!a) return;
            if (!g_task_manager) {
                log_e("bt", "引擎未启动");
                return;
            }
            std::scoped_lock task_lock(g_task_manager->get_mutex());

            // 任务状态改变
            if (const auto *su = lt::alert_cast<lt::state_update_alert>(a)) {
                for (const lt::torrent_status &s: su->status) {
                    if (!s.handle.is_valid()) continue;
                    const std::string info_hash = info_hash_hex(s.handle);
                    if (Task *task = TorrentEngine::get_task(info_hash); task && !task->is_delete) {
                        if (s.errc) {
                            log_e(info_hash.c_str(), "任务错误. category={}, code={}, msg={}", s.errc.category().name(),
                                  s.errc.value(), s.errc.what());
                            handle_task_error(info_hash, s.errc);
                        } else {
                            task->total_size = s.total_wanted;
                            task->total_done = s.total_wanted_done;
                            task->download_rate = static_cast<double>(s.download_payload_rate);
                            task->upload_rate = static_cast<double>(s.upload_payload_rate);
                            if (s.is_finished) {
                                task->status = DW_TASK_STATUS_COMPLETED;
                            } else if (s.flags & lt::torrent_flags::paused) {
                                task->status = (s.flags & lt::torrent_flags::auto_managed)
                                                   ? DW_TASK_STATUS_QUEUED
                                                   : DW_TASK_STATUS_PAUSED;
                            } else if (g_session->is_paused()) {
                                task->status = DW_TASK_STATUS_PAUSED;
                            } else {
                                switch (s.state) {
                                    case lt::torrent_status::downloading_metadata:
                                    case lt::torrent_status::checking_files:
                                    case lt::torrent_status::checking_resume_data:
                                        task->status = DW_TASK_STATUS_RESOLVING;
                                        break;
                                    case lt::torrent_status::downloading:
                                        if (!(s.flags & lt::torrent_flags::upload_mode)) {
                                            task->status = DW_TASK_STATUS_DOWNLOADING;
                                        }
                                        break;
                                    default:
                                        break;
                                }
                            }
                            task->dirty = true;
                        }
                    }
                }
            }
            // 任务完成
            else if (const auto *tf = lt::alert_cast<lt::torrent_finished_alert>(a)) {
                const std::string info_hash = info_hash_hex(tf->handle);
                log_e(info_hash.c_str(), "下载完成");
                if (Task *task = TorrentEngine::get_task(info_hash); task && !task->is_delete) {
                    task->status = DW_TASK_STATUS_COMPLETED;
                    task->dirty = true;
                }
            }
            // 添加任务
            else if (const auto *at = lt::alert_cast<lt::add_torrent_alert>(a)) {
                const std::string info_hash = info_hash_hex(at->params.info_hashes);
                if (at->error) {
                    log_e(info_hash.c_str(), "添加任务失败. category={}, code={}, msg={}", at->error.category().name(),
                          at->error.value(), at->error.what());
                    handle_task_error(info_hash, at->error);
                } else {
                    handle_files(at->handle);
                    if (Task *task = TorrentEngine::get_task(info_hash); task && !task->is_delete) {
                        if (task->magnet_link.empty()) {
                            task->magnet_link = lt::make_magnet_uri(at->params);
                            task->dirty = true;
                        }
                    }
                    log_i(info_hash.c_str(), "添加任务成功");
                }
            }
            // 磁力元数据就绪
            else if (const auto *mr = lt::alert_cast<lt::metadata_received_alert>(a)) {
                const std::string info_hash = info_hash_hex(mr->handle);
                handle_files(mr->handle);
                log_i(info_hash.c_str(), "元数据接收完成");
            }
            // piece 完成：更新受影响文件的下载区间。
            else if (const auto *pf = lt::alert_cast<lt::piece_finished_alert>(a)) {
                handle_piece_finished(pf->handle, pf->piece_index);
            }
            // 文件下载完成
            else if (const auto *fc = lt::alert_cast<lt::file_completed_alert>(a)) {
                const std::string info_hash = info_hash_hex(fc->handle);
                const auto file_index = static_cast<int32_t>(fc->index);
                log_i(info_hash.c_str(), "文件下载完成 file_index={}", file_index);
                if (Task *task = TorrentEngine::get_task(info_hash); task && !task->is_delete) {
                    const auto it = std::ranges::find_if(task->files,
                                                         [file_index](const TaskFile &file) {
                                                             return file.file_index == file_index;
                                                         });
                    if (it == task->files.end()) return;
                    it->total_done = it->total_size;
                    it->progress = 1.0;
                    it->segments.clear();
                    if (it->total_size > 0) {
                        it->segments.push_back({0, it->total_size - 1, SegmentStatus::Downloaded});
                    }
                    it->dirty = true;
                    task->dirty = true;
                }
            }
            // 任务错误
            else if (const auto *te = lt::alert_cast<lt::torrent_error_alert>(a)) {
                const std::string info_hash = info_hash_hex(te->handle);
                log_e(info_hash.c_str(), "任务错误 category={}, code={}, msg={}",
                      te->error.category().name(), te->error.value(), te->error.what());
                handle_task_error(info_hash, te->error);
            }
            // 文件错误
            else if (const auto *fe = lt::alert_cast<lt::file_error_alert>(a)) {
                const std::string info_hash = info_hash_hex(fe->handle);
                log_e(info_hash.c_str(), "文件错误 file={}, category={}, code={}, msg={}",
                      fe->filename(), fe->error.category().name(), fe->error.value(), fe->error.what());
                handle_task_error(info_hash, fe->error);
            }
            // 种子冲突
            else if (const auto *tc = lt::alert_cast<lt::torrent_conflict_alert>(a)) {
                const std::string info_hash = info_hash_hex(tc->handle);
                log_e(info_hash.c_str(), "种子冲突 msg={}", tc->what());
                if (Task *task = TorrentEngine::get_task(info_hash); task && !task->is_delete) {
                    task->message = "无效任务";
                    task->dirty = true;
                }
            }
            // 元数据校验失败由 libtorrent 自动重新获取，只记录日志。
            else if (const auto *mf = lt::alert_cast<lt::metadata_failed_alert>(a)) {
                const std::string info_hash = info_hash_hex(mf->handle);
                log_e(info_hash.c_str(), "元数据解析失败 category={}, code={}, msg={}",
                      mf->error.category().name(), mf->error.value(), mf->error.what());
            }
            // 改变保存路径失败：move_storage() 调用失败
            else if (const auto *smf = lt::alert_cast<lt::storage_moved_failed_alert>(a)) {
                const std::string info_hash = info_hash_hex(smf->handle);
                log_e(info_hash.c_str(), "改变保存路径失败 category={}, code={}, msg={}",
                      smf->error.category().name(), smf->error.value(), smf->error.what());
            }
            // 断点续传数据就绪
            else if (const auto *rd = lt::alert_cast<lt::save_resume_data_alert>(a)) {
                const std::string info_hash = info_hash_hex(rd->handle);
                if (rd->handle.is_valid() && rd->handle.status().has_metadata) {
                    try {
                        if (const std::vector<char> buf = lt::write_resume_data_buf(rd->params); !buf.empty()) {
                            if (Task *task = TorrentEngine::get_task(info_hash); task && !task->is_delete) {
                                task->resume_data = {buf.begin(), buf.end()};
                                task->resume_dirty = true;
                            }
                        }
                    } catch (const std::exception &e) {
                        log_e(info_hash.c_str(), "恢复数据处理失败 msg={}", e.what());
                    }
                }
            } else if (const auto *failed = lt::alert_cast<lt::save_resume_data_failed_alert>(a)) {
                if (failed->error != lt::errors::resume_data_not_modified) {
                    const auto info_hash = info_hash_hex(failed->handle);
                    log_e(info_hash.c_str(), "生成续传数据失败: {}", failed->error.what());
                }
            }
            // 存储迁移完成
            else if (const auto *sm = lt::alert_cast<lt::storage_moved_alert>(a)) {
                const std::string info_hash = info_hash_hex(sm->handle);
                log_i(info_hash.c_str(), "文件迁移完成");
            }
            // 任务已从 session 移除
            else if (const auto *tr = lt::alert_cast<lt::torrent_removed_alert>(a)) {
                const std::string info_hash = info_hash_hex(tr->info_hashes);
                log_i(info_hash.c_str(), "任务处理器移除完成");
            }
            // 任务文件删除完成
            else if (const auto *td = lt::alert_cast<lt::torrent_deleted_alert>(a)) {
                const std::string info_hash = info_hash_hex(td->info_hashes);
                log_i(info_hash.c_str(), "任务文件删除完成");
            } else if (const auto *tdf = lt::alert_cast<lt::torrent_delete_failed_alert>(a)) {
                const auto info_hash = info_hash_hex(tdf->info_hashes);
                log_e(info_hash.c_str(), "任务文件删除失败: {}", tdf->error.what());
            } else if (const auto *fp = lt::alert_cast<lt::file_prio_alert>(a)) {
                const auto info_hash = info_hash_hex(fp->handle);
                log_i(info_hash.c_str(), "任务优先级配置完成");
            }
        }

        // 采集 alert
        void alert_loop() {
            while (g_running.load()) {
                try {
                    if (g_session->wait_for_alert(std::chrono::milliseconds(1000))) {
                        std::vector<lt::alert *> alerts;
                        g_session->pop_alerts(&alerts);
                        for (const lt::alert *a: alerts) {
                            try {
                                handle_alert(a);
                            } catch (const std::exception &e) {
                                log_e("bt", "处理事件异常 type={}, msg={}", a ? a->type() : -1, e.what());
                            } catch (...) {
                                log_e("bt", "处理事件未知异常 type={}", a ? a->type() : -1);
                            }
                        }
                    }
                } catch (const std::exception &e) {
                    log_e("bt", "采集事件异常: {}", e.what());
                } catch (...) {
                    log_e("bt", "采集事件未知异常");
                }
            }
        }

        // 每 1 秒触发状态采集。
        void status_loop() {
            while (g_running.load()) {
                if (g_session) {
                    try {
                        g_session->post_torrent_updates();
                    } catch (const std::exception &e) {
                        log_e("bt", "状态采集失败: {}", e.what());
                    } catch (...) {
                        log_e("bt", "状态采集发生未知异常");
                    }
                }
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
        }

        // 每 5 秒触发恢复数据采集 （save_resume_data）
        void resume_loop() {
            while (g_running.load()) {
                if (g_session) {
                    try {
                        for (const auto handles = g_session->get_torrents();
                             const auto &h: handles) {
                            try {
                                if (!h.is_valid()) continue;
                                if (const lt::torrent_status torrent_status = h.status();
                                    torrent_status.has_metadata) {
                                    // 请求保存恢复数据
                                    h.save_resume_data(lt::torrent_handle::save_info_dict
                                                       | lt::torrent_handle::if_download_progress
                                                       | lt::torrent_handle::if_config_changed
                                                       | lt::torrent_handle::if_state_changed
                                                       | lt::torrent_handle::if_metadata_changed);
                                }
                            } catch (...) {
                            }
                        }
                    } catch (const std::exception &e) {
                        log_e("bt", "恢复数据定时请求异常: {}", e.what());
                    }
                }
                std::this_thread::sleep_for(std::chrono::seconds(5));
            }
        }
    } // namespace（匿名）

    /* ===================================================================== */
    /*                        TorrentEngine 成员实现                          */
    /* ===================================================================== */

    std::recursive_mutex &TorrentEngine::tasks_mutex() {
        return g_task_manager->get_mutex();
    }

    Task *TorrentEngine::get_task(const std::string &info_hash) {
        if (!g_task_manager) {
            log_e("", "bt 未初始化");
            return nullptr;
        }
        Task *task = g_task_manager->get_engine_task(info_hash);
        return task && task->task_protocol == DW_PROTOCOL_TORRENT ? task : nullptr;
    }

    TorrentEngine::~TorrentEngine() {
        if (initialized_) {
            destroy();
        }
    }

    int32_t TorrentEngine::init(const std::vector<std::string> &task_ids) {
        if (initialized_) {
            return 0;
        }
        if (!task_manager_) {
            log_e("", "BT 引擎初始化失败: TaskManager 未设置");
            return -1;
        }
        try {
            lt::settings_pack pack;
            // 订阅错误、状态、存储与 piece 完成（进度区间维护）；屏蔽 peer/block 等细粒度事件
            pack.set_int(lt::settings_pack::alert_mask,
                         lt::alert_category::error
                         | lt::alert_category::status
                         | lt::alert_category::storage
                         | lt::alert_category::piece_progress);
            pack.set_bool(lt::settings_pack::dont_count_slow_torrents, false);
            g_session = std::make_unique<lt::session>(std::move(pack));
        } catch (const std::exception &e) {
            log_e("bt", "初始化引擎失败: {}", e.what());
            return -1;
        }
        if (!g_session || !g_session->is_valid()) {
            log_e("bt", "初始化引擎失败");
            g_session.reset();
            return -1;
        }

        g_task_manager = task_manager_;
        initialized_ = true;
        const auto &cfg = task_manager_->config();
        config(cfg.max_concurrent_downloads, cfg.download_rate_limit, cfg.upload_rate_limit,
               cfg.network_type != 0 && (cfg.network_type != 2 || cfg.allow_mobile_data));

        for (const auto &task_id: task_ids) {
            Task *task = get_task(task_id);
            if (!task || task->is_delete
                || task->task_protocol != DW_PROTOCOL_TORRENT
                || task->status == DW_TASK_STATUS_COMPLETED) {
                continue;
            }
            try {
                if (const auto handle = find_handle(task_id); !handle.is_valid()) {
                    task->status = DW_TASK_STATUS_PAUSED;
                    task->dirty = true;
                    log_e(task_id.c_str(), "bt 初始化 加载任务处理器失败");
                }
            } catch (const std::exception &e) {
                log_e(task_id.c_str(), "bt 初始化 加载任务失败: {}", e.what());
            }
        }

        g_running.store(true);
        g_alert_thread = std::thread(alert_loop);
        g_status_thread = std::thread(status_loop);
        g_resume_thread = std::thread(resume_loop);

        log_i("bt", "初始化引擎完成");
        return 0;
    }

    void TorrentEngine::config(const std::optional<int32_t> downloads,
                               const std::optional<int64_t> download_rate,
                               const std::optional<int64_t> upload_rate,
                               const std::optional<bool> network_allowed) {
        std::scoped_lock lock(tasks_mutex());
        if (!g_session || !g_session->is_valid()) {
            log_e("", "bt 引擎未启动");
            return;
        }

        if (downloads || download_rate || upload_rate) {
            const auto rate_limit = [](const int64_t value) {
                return static_cast<int>(std::clamp<int64_t>(value, 0, std::numeric_limits<int>::max()));
            };
            lt::settings_pack settings;
            if (downloads) settings.set_int(lt::settings_pack::active_downloads, std::max(0, *downloads));
            if (download_rate) settings.set_int(lt::settings_pack::download_rate_limit, rate_limit(*download_rate));
            if (upload_rate) settings.set_int(lt::settings_pack::upload_rate_limit, rate_limit(*upload_rate));
            g_session->apply_settings(std::move(settings));
        }
        if (network_allowed) {
            if (*network_allowed) {
                g_session->resume();
            } else {
                g_session->pause();
            }
        }
    }

    void TorrentEngine::destroy() {
        if (!initialized_) {
            return;
        }
        // 先停止工作线程，再销毁 session。
        g_running.store(false);
        if (g_status_thread.joinable()) {
            g_status_thread.join();
        }
        if (g_resume_thread.joinable()) {
            g_resume_thread.join();
        }
        if (g_alert_thread.joinable()) {
            g_alert_thread.join();
        }
        g_session.reset();

        g_task_manager = nullptr;
        initialized_ = false;
        log_i("bt", "销毁引擎完成");
    }

    dw_result<> TorrentEngine::add_task(const TaskParams *params) {
        if (!g_session || !g_session->is_valid()) {
            log_e("", "bt 添加任务 引擎未启动");
            return dw_result<>::failure();
        }
        std::scoped_lock task_lock(g_task_manager->get_mutex());

        // bt 参数检查
        lt::add_torrent_params atp;
        bool source_ok = false;

        // magnet_link > torrent_file > info_hash。
        if (!params->magnet_link.empty()) {
            lt::error_code ec;
            lt::parse_magnet_uri(params->magnet_link, atp, ec);
            if (!ec) {
                source_ok = true;
            } else {
                log_e("", "bt 添加任务 解析链接失败: code={}, message={}", ec.value(), ec.what());
            }
        }
        if (!source_ok && !params->torrent_file.empty()) {
            lt::error_code ec;
            const lt::add_torrent_params loaded = lt::load_torrent_file(
                params->torrent_file, ec, lt::load_torrent_limits{});
            if (!ec) {
                atp.ti = loaded.ti;
                source_ok = true;
            } else {
                log_e("", "bt 添加任务 加载文件失败: code={}, message={}", ec.value(), ec.what());
            }
        }
        // magnet_link 作为 info_hash
        if (!source_ok && !params->magnet_link.empty()) {
            if (params->magnet_link.size() == 40) {
                lt::sha1_hash h;
                if (lt::aux::from_hex(params->magnet_link, h.data())) {
                    atp.info_hashes = lt::info_hash_t(h);
                    source_ok = true;
                }
            } else if (params->magnet_link.size() == 64) {
                lt::sha256_hash h;
                if (lt::aux::from_hex(params->magnet_link, h.data())) {
                    atp.info_hashes = lt::info_hash_t(h);
                    source_ok = true;
                }
            }
        }
        if (!source_ok) {
            log_e("", "bt 添加任务 非法参数");
            return dw_result<>::failure();
        }

        const auto info_hash = info_hash_hex(atp.ti ? atp.ti->info_hashes() : atp.info_hashes);
        if (info_hash.empty()) {
            log_e("", "bt 添加任务 无法解析出info_hash");
            return dw_result<>::failure();
        }
        Task *existing_task = get_task(info_hash);
        Task new_task;
        Task &task = existing_task ? *existing_task : new_task;

        task.name = atp.ti ? atp.ti->name() : atp.name;
        task.client_id = params->client_id;
        task.is_remote = false;
        task.task_protocol = DW_PROTOCOL_TORRENT;
        task.task_id = info_hash;
        task.info_hash = info_hash;
        task.magnet_link = params->magnet_link;
        task.torrent_file = params->torrent_file;

        task.is_delete = false;
        task.is_read = false;
        task.modified_at = now_unix_ms();
        if (!existing_task) {
            task.status = DW_TASK_STATUS_RESOLVING;
            task.created_at = task.modified_at;

            // 重名处理
            const std::filesystem::path base = g_task_manager->config().save_path;
            task.save_path = base.string();

            const auto filename = std::filesystem::path(task.name).filename();
            if (std::error_code ec;
                !filename.empty()
                && filename != "."
                && filename != ".."
                && std::filesystem::exists(base / filename, ec)) {
                const auto stem = filename.stem().string();
                for (int32_t i = 1; i < 10000; ++i) {
                    const auto candidate = base / (stem + "(" + std::to_string(i) + ")");
                    const bool occupied = std::filesystem::exists(candidate, ec);
                    if (ec) break;
                    if (!occupied) {
                        task.save_path = candidate.string();
                        break;
                    }
                }
            }
        }
        task.dirty = true;

        if (!g_task_manager->get_task_store().save(task)) {
            log_e(info_hash.c_str(), "bt 添加任务 任务持久化失败");
            return dw_result<>::failure();
        }

        Task *registered_task = get_task(info_hash);
        if (!registered_task) return dw_result<>::failure();
        registered_task->dirty = true;

        // 添加任务
        try {
            const auto handle = find_handle(info_hash);
            if (!handle.is_valid()) {
                log_e(info_hash.c_str(), "bt 添加任务  构建处理器失败");
                registered_task->status = DW_TASK_STATUS_ERROR;
                registered_task->modified_at = now_unix_ms();
                registered_task->dirty = true;
                return dw_result<>::failure();
            }
            // 设置tracker
            if (!g_task_manager->config().trackers.empty()) {
                for (const auto &url: g_task_manager->config().trackers) {
                    lt::announce_entry tracker(url);
                    tracker.source = lt::announce_entry::source_client;
                    handle.add_tracker(tracker);
                }
            }

            // 设置优先级
            if (!params->file_indexes.empty()
                && (params->file_priority == 0 || params->file_priority == 4 || params->file_priority == 7)) {
                if (const auto torrent = handle.torrent_file()) {
                    const auto &files = torrent->layout();
                    auto priorities = handle.get_file_priorities();
                    priorities.resize(files.num_files(), lt::default_priority);
                    const auto requested = lt::download_priority_t{static_cast<uint8_t>(params->file_priority)};
                    for (const auto index: params->file_indexes) {
                        if (index >= files.num_files() || files.pad_file_at(lt::file_index_t{index})) {
                            continue;
                        }
                        priorities[index] = requested;
                    }
                    handle.prioritize_files(priorities);
                }
            }
            handle.set_flags(lt::torrent_flags::auto_managed,
                             lt::torrent_flags::auto_managed | lt::torrent_flags::upload_mode);
            g_session->post_torrent_updates();
        } catch (const std::exception &error) {
            registered_task->status = DW_TASK_STATUS_ERROR;
            registered_task->message = error.what();
            registered_task->modified_at = now_unix_ms();
            registered_task->dirty = true;
            log_e(info_hash.c_str(), "bt 添加任务失败 massage={}", error.what());
            return dw_result<>::failure();
        }
        return dw_result<>::success();
    }

    dw_result<> TorrentEngine::resume_task(const TaskParams *params) {
        const auto &info_hash = params->task_key();
        if (!g_session || !g_session->is_valid()) {
            log_e(info_hash.c_str(), "bt 恢复任务 引擎未启动");
            return dw_result<>::failure();
        }

        std::scoped_lock task_lock(tasks_mutex());

        Task *task = get_task(info_hash);
        if (!task || task->is_delete) return dw_result<>::failure();
        try {
            const auto handle = find_handle(info_hash);
            if (!handle.is_valid()) {
                log_e(info_hash.c_str(), "bt 恢复任务 任务处理器不存在");
                return dw_result<>::failure();
            }

            // 设置tracker
            if (!g_task_manager->config().trackers.empty()) {
                for (const auto &url: g_task_manager->config().trackers) {
                    lt::announce_entry tracker(url);
                    tracker.source = lt::announce_entry::source_client;
                    handle.add_tracker(tracker);
                }
            }
            // 设置优先级
            if (!params->file_indexes.empty()
                && (params->file_priority == 0 || params->file_priority == 4 || params->file_priority == 7)) {
                if (const auto torrent = handle.torrent_file()) {
                    const auto &files = torrent->layout();
                    auto priorities = handle.get_file_priorities();
                    priorities.resize(files.num_files(), lt::default_priority);
                    const auto requested = lt::download_priority_t{static_cast<uint8_t>(params->file_priority)};
                    for (const auto index: params->file_indexes) {
                        if (index >= files.num_files() || files.pad_file_at(lt::file_index_t{index})) {
                            continue;
                        }
                        priorities[index] = requested;
                    }
                    handle.prioritize_files(priorities);
                }
            }
            handle.set_flags(lt::torrent_flags::auto_managed,
                             lt::torrent_flags::auto_managed | lt::torrent_flags::upload_mode);
            handle.resume();
            g_session->post_torrent_updates();
            task->message.clear();
            task->dirty = true;
            return dw_result<>::success();
        } catch (const std::exception &e) {
            log_e(info_hash.c_str(), "bt 恢复任务失败 massage={}", e.what());
            return dw_result<>::failure();
        }
    }

    dw_result<> TorrentEngine::pause_task(const TaskParams *params) {
        const auto &info_hash = params->task_key();
        if (!g_session || !g_session->is_valid()) {
            log_e(info_hash.c_str(), "bt 暂停任务 引擎未启动");
            return dw_result<>::failure();
        }
        std::scoped_lock task_lock(tasks_mutex());
        log_i(info_hash.c_str(), "bt 暂停任务");

        Task *task = get_task(info_hash);
        if (!task || task->is_delete) return dw_result<>::failure();
        try {
            const auto handle = find_handle(info_hash);
            if (!handle.is_valid()) {
                log_e(info_hash.c_str(), "bt 暂停任务 任务处理器不存在");
                return dw_result<>::failure();
            }
            handle.unset_flags(lt::torrent_flags::auto_managed);
            handle.pause();
            g_session->post_torrent_updates();
            return dw_result<>::success();
        } catch (const std::exception &e) {
            log_e(info_hash.c_str(), "bt 暂停任务失败 massage={}", e.what());
            return dw_result<>::failure();
        }
    }

    dw_result<> TorrentEngine::delete_task(const TaskParams *params) {
        const auto &info_hash = params->task_key();
        if (!g_session || !g_session->is_valid()) {
            log_e(info_hash.c_str(), "bt 删除任务 引擎未启动");
            return dw_result<>::failure();
        }
        const int32_t delete_files = params->delete_files ? 1 : 0;
        log_i(info_hash.c_str(), "bt 删除任务 delete_files={}", delete_files);
        std::scoped_lock task_lock(tasks_mutex());

        try {
            if (Task *task = get_task(info_hash); task && !task->is_delete) {
                if (const auto handle = find_handle(info_hash, true); handle.is_valid()) {
                    g_session->remove_torrent(handle, delete_files ? lt::session::delete_files : lt::remove_flags_t{});
                }
                task->is_delete = true;
                task->dirty = true;
            }
        } catch (const std::exception &e) {
            log_e(info_hash.c_str(), "bt 删除任务失败 massage={}", e.what());
            return dw_result<>::failure();
        }
        log_i(info_hash.c_str(), "bt 删除任务完成");
        return dw_result<>::success();
    }

    void TorrentEngine::sweep() {
        if (!initialized_ || !g_session || !g_task_manager) return;

        std::scoped_lock lock(tasks_mutex());
        const double configured_limit = g_task_manager->config().seed_ratio_limit;
        const double ratio_limit = configured_limit == 0.0 ? 3.0 : configured_limit;
        for (const auto handles = g_session->get_torrents(); const auto &handle: handles) {
            if (!handle.is_valid()) continue;
            const std::string key = info_hash_hex(handle);
            try {
                if (const Task *task = get_task(key); !task || task->is_delete) {
                    g_session->remove_torrent(handle);
                    log_i(key.c_str(), "任务清理 任务已标记删除，移除处理器");
                    continue;
                }
                if (ratio_limit < 0.0) continue;
                const auto status = handle.status();
                if (!status.is_seeding || status.total_done <= 0) continue;
                const double ratio = static_cast<double>(status.all_time_upload) / static_cast<double>(status.
                                         total_done);
                if (ratio >= ratio_limit) {
                    g_session->remove_torrent(handle);
                    log_i(key.c_str(), "任务清理 分享率达标，移除处理器 ratio={} limit={}", ratio, ratio_limit);
                }
            } catch (const std::exception &e) {
                log_e(key.c_str(), "任务清理 失败[{}]", e.what());
            }
        }
    }

    dw_result<std::string> TorrentEngine::parse_magnet(const std::string &magnet_link) {
        if (magnet_link.empty()) {
            log_e("bt", "链接[{}]无效", magnet_link);
            return dw_result<std::string>::failure();
        }
        lt::error_code ec;
        lt::add_torrent_params atp;
        lt::parse_magnet_uri(magnet_link, atp, ec);
        if (ec) {
            log_e("bt", "解析链接失败[{}]", ec.what());
            return dw_result<std::string>::failure();
        }
        std::string s;
        if (atp.info_hashes.has_v2()) {
            s = lt::aux::to_hex(atp.info_hashes.v2);
        } else if (atp.info_hashes.has_v1()) {
            s = lt::aux::to_hex(atp.info_hashes.v1);
        } else {
            log_e("bt", "解析链接失败[{}]", magnet_link);
            return dw_result<std::string>::failure();
        }
        auto result = dw_result<std::string>::success();
        result.data = std::move(s);
        return result;
    }

    dw_result<std::vector<TaskFile>> TorrentEngine::parse_torrent_file(const std::string &torrent_file_path, std::string *info_hash) {
        if (torrent_file_path.empty()) {
            log_e("bt", ".torrent 文件[{}]无效", torrent_file_path);
            return dw_result<std::vector<TaskFile>>::failure();
        }
        lt::error_code ec;
        const lt::add_torrent_params loaded = lt::load_torrent_file(torrent_file_path, ec, lt::load_torrent_limits{});
        if (ec) {
            log_e("bt", "加载 .torrent 文件失败[{}]", ec.what());
            return dw_result<std::vector<TaskFile>>::failure();
        }
        const std::shared_ptr<const lt::torrent_info> ti = loaded.ti;
        if (!ti) {
            log_e("bt", "解析 .torrent 文件失败[{}]", torrent_file_path);
            return dw_result<std::vector<TaskFile>>::failure();
        }
        const lt::info_hash_t ih = ti->info_hashes();
        std::string s;
        if (ih.has_v2()) {
            s = lt::aux::to_hex(ih.v2);
        } else if (ih.has_v1()) {
            s = lt::aux::to_hex(ih.v1);
        } else {
            log_e("", "解析 .torrent 文件失败[{}]", torrent_file_path);
            return dw_result<std::vector<TaskFile>>::failure();
        }
        auto result = dw_result<std::vector<TaskFile>>::success();
        try {
            const auto &files = ti->layout();
            result.data.reserve(files.num_files());
            for (int i = 0; i < files.num_files(); ++i) {
                const lt::file_index_t index{i};
                if (files.pad_file_at(index)) continue;
                TaskFile file;
                file.file_index = i;
                file.task_id = s;
                file.path = files.file_path(index);
                file.name = std::filesystem::path(file.path).filename().string();
                file.total_size = files.file_size(index);
                file.progress = file.total_size == 0 ? 1.0 : 0.0;
                file.extension = utils::file_extension(file.path);
                result.data.push_back(std::move(file));
            }
        } catch (const std::exception &e) {
            log_e("bt", "解析 .torrent 文件任务列表失败[{}]", e.what());
            return dw_result<std::vector<TaskFile>>::failure();
        }
        if (info_hash) *info_hash = std::move(s);
        return result;
    }
} // namespace dw
