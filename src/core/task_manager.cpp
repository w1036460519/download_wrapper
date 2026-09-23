/**
 * @file task_manager.cpp
 * @brief 库内任务中枢实现：SQLite 持久化 + 优先级就绪队列 + 事件驱动准入调度。
 *
 * 并发模型（双线程职责分离）：
 *   - mtx_ 保护注册表 tasks_ 与 DB（sqlite3 串行化模式，读写均在持锁期间）；
 *   - A 线程（scheduler_loop，快节拍）：仅 query 纯读 + 改内存 + 转发回调，持锁期间只做内存操作；
 *   - B 线程（maintenance_loop，慢节拍）：唯一的落库 / 区间快照 / 终态注销 / 准入与引擎 sweep 执行方；
 *   - 引擎启动 / 合成回调一律在释放 mtx_ 后执行，规避回调线程重入。
 *
 * 任务主键模型：
 *   - 唯一键 = (client_id, protocol, natural_key) 三元组（client_id 来自 dw_config_t，启动时注入）；
 *   - tasks_ 以 natural_key 为 key（仅本机任务，client_id 冗余），API 入口校验隔离非本机请求；
 *   - 引擎回调经 natural_key 直接 O(1) 查表。
 */

#include "task_manager.h"

#include "internal/downloader_internal.h"
#include "internal/engine_interface.h"
#include "torrent/torrent_engine.h"
#include "utils/memory_util.h"
#include "utils/time_util.h"
#include "utils/string_util.h"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <optional>
#include <ranges>
#include <unordered_set>

#include <boost/asio.hpp>
#include <utility>

namespace dw {
    using utils::now_unix_ms;

    namespace {
        /// 占用下载额度的状态集合：仅 DOWNLOADING。
        /// RESOLVING/PARSED/QUEUED 均为等待态，不占名额；暂停/错误/完成自动释放额度。
        bool status_occupies_slot(dw_task_status_t s) {
            return s == DW_TASK_STATUS_DOWNLOADING;
        }

        /// 已下载区间是否完全覆盖文件 [0, file_size-1]（区间须已合并且按 start 升序）。
        /// file_size <= 0 时无法判定，返回 false（HTTP chunked 无总长场景）。
        bool is_file_complete(const std::vector<dw_byte_range_t> &segs, int64_t file_size) {
            if (segs.empty() || file_size <= 0) return false;
            int64_t cursor = 0;
            for (const auto &s: segs) {
                if (s.start > cursor) return false; // 有间隙
                cursor = std::max(cursor, s.end + 1);
                if (cursor >= file_size) return true;
            }
            return false;
        }
    } // namespace

    TaskManager::~TaskManager() {
        stop();
    }

    void TaskManager::set_engines(IDownloadEngine *http, IDownloadEngine *torrent) {
        http_ = http;
        torrent_ = torrent;
    }

    /* ================================================================== */
    /*                          生命周期                                  */
    /* ================================================================== */

    int32_t TaskManager::start(const Config &cfg) {
        max_concurrent_ = cfg.max_concurrent_downloads > 0 ? cfg.max_concurrent_downloads : 3;
        flush_interval_ms_ = cfg.status_callback_interval_ms > 0 ? cfg.status_callback_interval_ms : 1000;
        maintenance_interval_ms_ = flush_interval_ms_ * 2;

        // 注入 clientId：App 启动时从 SharedPreferences 读取并经 Config 传入。
        // 此后本 session 创建/加载的所有任务均归此 clientId 隔离。
        client_id_ = cfg.client_id;
        if (client_id_.empty()) {
            log_e("", "下载器无客户端标识，启动失败");
            return -1;
        }

        // 默认保存目录：任务未指定 save_path 时回退到此值。
        save_path_ = cfg.save_path;

        const std::string dir = !cfg.work_dir.empty() ? cfg.work_dir : ".";
        const std::string story_path = dir + "/leopard_tasks.db";

        std::scoped_lock lock(mtx_);
        if (!store_.open(story_path)) {
            log_e("", "下载器打开数据库失败: {}", story_path);
            return -1;
        }
        store_.init_schema();
        {
            // 预先加载进行中的任务
            const std::vector<dw_protocol_t> active_protocols = {
                DW_PROTOCOL_HTTP,
                DW_PROTOCOL_TORRENT
            };
            const std::vector<dw_task_status_t> active_statuses = {
                DW_TASK_STATUS_RESOLVING,
                DW_TASK_STATUS_PARSED,
                DW_TASK_STATUS_QUEUED,
                DW_TASK_STATUS_DOWNLOADING
            };
            for (auto &fr: store_.load_file_records(client_id_, active_protocols, active_statuses)) {
                register_task(std::move(fr));
            }
        }

        running_.store(true);
        schedule_needed_ = true;
        worker_ = std::thread([this] { scheduler_loop(); });
        maintenance_ = std::thread([this] { maintenance_loop(); });

        log_i("", "下载器启动 clientId={} tasks={} max_concurrent={}", client_id_, tasks_.size(), max_concurrent_);
        return 0;
    }

    void TaskManager::stop() {
        if (!running_.exchange(false)) {
            return;
        }
        // running_=false 驱动循环条件退出，sleep_for 自然醒来后检查退出。
        if (worker_.joinable()) {
            worker_.join();
        }
        if (maintenance_.joinable()) {
            maintenance_.join();
        }

        std::scoped_lock lock(mtx_);
        flush_dirty_locked();
        store_.close();
        log_i("", "下载器已停止");
    }

    /* ================================================================== */
    /*                          控制操作                                  */
    /* ================================================================== */

    dw_submit_result_t TaskManager::add(TaskParams &params) {
        log_i("", "添加任务[{}]", to_json_string(params));
        if (save_path_.empty()) {
            return dw_submit_result_t::failure(DW_REASON_ERROR, "未配置下载目录");
        }
        const dw_protocol_t proto = params.protocol;
        const std::string &client_id = params.client_id;
        std::string natural_key;
        if (params.save_path.empty()) {
            params.save_path = save_path_;
        }

        dw_submit_result_t engine_result;
        if (proto == DW_PROTOCOL_TORRENT) {
            engine_result = torrent_->add_task(&params);
            if (engine_result.code == DW_REASON_NONE) {
                natural_key = engine_result.info_hash;
            }
        } else if (proto == DW_PROTOCOL_HTTP) {
            engine_result = http_->add_task(&params);
            if (engine_result.code == DW_REASON_NONE) {
                natural_key = params.url;
            }
        }

        if (natural_key.empty()) {
            if (engine_result.code == DW_REASON_NONE) {
                return dw_submit_result_t::failure(DW_REASON_ERROR, "任务添加失败");
            }
            return engine_result;
        }

        {
            std::scoped_lock lock(mtx_);

            if (FileRecord *rec = load_task_record(client_id, proto, natural_key)) {
                rec->modified_at = now_unix_ms();
                rec->synth_notified = false;
                store_.touch_file_record(client_id, proto, natural_key);
            } else {
                FileRecord task_record;
                task_record.client_id = client_id;
                task_record.task_protocol = proto;
                task_record.task_natural_key = natural_key;
                task_record.save_path = params.save_path;
                task_record.type = (proto == DW_PROTOCOL_TORRENT) ? DW_SOURCE_REMOTE_FILE : DW_SOURCE_TASK_FILE;
                task_record.is_remote = true;
                task_record.status = DW_TASK_STATUS_RESOLVING;
                task_record.created_at = now_unix_ms();
                task_record.modified_at = task_record.created_at;
                store_.insert_file_record(task_record);
            }
            schedule_needed_ = true;
        }

        return engine_result;
    }

    dw_submit_result_t TaskManager::pause(const TaskParams &params) const {
        log_i(params.natural_key.c_str(), "暂停任务[{}]", to_json_string(params));
        if (IDownloadEngine *eng = engine_of(params.protocol)) {
            return eng->pause_task(params.natural_key, client_id_);
        }
        return dw_submit_result_t::failure(DW_REASON_ERROR, "下载引擎不可用");
    }


    dw_submit_result_t TaskManager::resume(const TaskParams &params) {
        log_i(params.natural_key.c_str(), "恢复任务[{}]", to_json_string(params));
        {
            std::scoped_lock lock(mtx_);
            FileRecord *rec = load_task_record(client_id_, params.protocol, params.natural_key);
            if (!rec) {
                return dw_submit_result_t::failure(DW_REASON_ERROR, "任务不存在");
            }
            if (!params.priority_file_indexes.empty()) {
                for (int32_t idx: params.priority_file_indexes) {
                    if (std::ranges::find(rec->priority_file_indexes, idx) == rec->priority_file_indexes.end()) {
                        rec->priority_file_indexes.push_back(idx);
                    }
                }
            }
            rec->force = params.force;
            rec->status = DW_TASK_STATUS_QUEUED;
            rec->synth_notified = false;
            store_.update_file_record_status(rec->client_id, rec->task_protocol, rec->task_natural_key,
                                             DW_TASK_STATUS_QUEUED, DW_REASON_NONE, "");
        }
        return dw_submit_result_t::success();
    }

    dw_submit_result_t TaskManager::remove(const TaskParams &params) const {
        const int32_t delete_files = params.delete_files ? 1 : 0;
        log_i(params.natural_key.c_str(), "删除任务[{}]", to_json_string(params));
        if (IDownloadEngine *eng = engine_of(params.protocol)) {
            return eng->delete_task(params.natural_key, client_id_, delete_files);
        }
        return dw_submit_result_t::failure(DW_REASON_ERROR, "引擎不可用");
    }


    void TaskManager::save_resume_source(const std::string &client_id, const dw_protocol_t proto,
                                         const std::string &natural_key,
                                         const std::string &save_path,
                                         const std::string &magnet_link, const std::string &torrent_file) {
        std::scoped_lock lock(mtx_);
        store_.save_resume_source(client_id, proto, natural_key, save_path, magnet_link, torrent_file);
    }

    TaskStore::ResumeInfo TaskManager::load_resume_info(const std::string &client_id, const dw_protocol_t proto,
                                                        const std::string &natural_key) {
        std::scoped_lock lock(mtx_);
        return store_.load_resume_info(client_id, proto, natural_key);
    }


    FileRecord *TaskManager::load_task_record(const std::string &client_id, const dw_protocol_t proto,
                                              const std::string &natural_key) {
        std::scoped_lock lock(mtx_);
        const std::string uid = union_id_of(client_id, proto, natural_key);
        // 内存查询
        if (const auto it = tasks_.find(uid); it != tasks_.end()) {
            return &it->second;
        }
        // DB 查询
        FileRecord record;
        if (!store_.find_file_record(client_id, proto, natural_key, record)) {
            return nullptr;
        }
        register_task(std::move(record));
        if (const auto it = tasks_.find(uid); it != tasks_.end()) {
            return &it->second;
        }
        return nullptr;
    }

    void TaskManager::set_play_position(dw_protocol_t proto, const std::string &natural_key, const int32_t file_index,
                                        const int64_t position_ms) {
        std::scoped_lock lock(mtx_);
        // play_progress 以物理路径为键：路径解析失败（定名未落定 / handle 离线）静默丢弃。
        std::string path;
        int64_t size = 0;
        if (resolve_file_path_locked(proto, natural_key, file_index, path, size)) {
            store_.set_play_position(path, position_ms);
        }
    }

    int64_t TaskManager::get_play_position(dw_protocol_t proto, const std::string &natural_key,
                                           const int32_t file_index) {
        std::scoped_lock lock(mtx_);
        std::string path;
        int64_t size = 0;
        if (!resolve_file_path_locked(proto, natural_key, file_index, path, size)) return 0;
        return store_.get_play_position(path);
    }

    bool TaskManager::resolve_file_path(dw_protocol_t proto, const std::string &natural_key,
                                        int32_t file_index, std::string &out_path, int64_t &out_size) {
        std::scoped_lock lock(mtx_);
        return resolve_file_path_locked(proto, natural_key, file_index, out_path, out_size);
    }

    bool TaskManager::resolve_file_path_locked(dw_protocol_t proto, const std::string &natural_key,
                                               int32_t file_index, std::string &out_path,
                                               int64_t &out_size) {
        // 内存优先，DB 兆底（状态持久化权威）。
        const FileRecord *rec_ptr = nullptr;
        FileRecord db_rec;
        const auto it = tasks_.find(union_id_of(client_id_, proto, natural_key));
        if (it != tasks_.end()) {
            rec_ptr = &it->second;
        } else if (store_.find_file_record(client_id_, proto, natural_key, db_rec)) {
            rec_ptr = &db_rec;
        }
        if (!rec_ptr) return false;

        if (proto == DW_PROTOCOL_HTTP) {
            // wrapper 模型：物理路径 = save_path / root_name（wrapper 目录） / original_root_name（原始文件名）。
            if (rec_ptr->root_name.empty() || rec_ptr->original_root_name.empty()) return false; // 定名未落定
            out_path = (std::filesystem::path(rec_ptr->save_path) /
                        rec_ptr->root_name / rec_ptr->original_root_name).string();
            out_size = rec_ptr->total_size;
            return true;
        }
        // BT：handle 在线实时查询（引擎调用不回调 TaskManager，持锁安全）。
        if (!torrent_) return false;
        return torrent_->get_file_path(rec_ptr->task_natural_key, file_index, out_path, out_size);
    }

    std::vector<dw_byte_range_t> TaskManager::load_segments(dw_protocol_t proto, const std::string &natural_key,
                                                            const int32_t file_index) {
        std::scoped_lock lock(mtx_);
        // 进度缓存按物理路径存键：先解析路径，再查区间。
        std::string path;
        int64_t size = 0;
        if (!resolve_file_path_locked(proto, natural_key, file_index, path, size)) return {};
        return store_.load_segments(path, file_index);
    }

    /* ================================================================== */
    /*                          引擎事件消费                              */
    /* ================================================================== */

    void TaskManager::on_engine_event(EngineEvent event) {
        boost::asio::post(event_ioc_, [this, ev = std::move(event)]() mutable {
            consume_engine_event(std::move(ev));
        });
    }

    void TaskManager::consume_engine_event(EngineEvent event) {
        log_d("", "接受到事件:{}", to_string(event));
        const std::string &key = event.engine_key;
        if (key.empty()) {
            log_e("", "无效事件，丢弃:{}", to_string(event));
            return;
        }

        // 前置检查：部分事件可在无锁情况下提前退出。
        if (event.type == EngineEventType::TASK_FILES && event.files.empty()) {
            return;
        }

        std::scoped_lock lock(mtx_);
        FileRecord *rec = load_task_record(client_id_, event.protocol, key);
        if (!rec) {
            log_e(key.c_str(), "事件 {} 任务不存在", to_string(event));
            return;
        }

        switch (event.type) {
            case EngineEventType::PARSED: {
                // 解析完成（重名检测已在 torrent_engine 中完成，content_root 由引擎传入）。
                // 本 handler 仅负责：更新文件记录元数据、状态迁移 RESOLVING → QUEUED（即时写）。

                if (!rec->parsed) {
                    // 首次 PARSED：使用引擎传入的 is_dir/ext 更新文件记录。
                    // root_name 为空 = 无需包装，物理路径 = save_path
                    const bool is_dir = event.is_dir;
                    const std::string &ext = event.ext;

                    rec->root_name = event.content_root;
                    rec->save_path = event.save_path; // move_storage 后路径可能已变更
                    rec->parsed = true;

                    // 更新文件列表元数据（含解析时保存目录，move_storage 可能变更）
                    // content_root 非空时：full_path = save_path / content_root
                    // content_root 为空时：full_path = save_path（文件直接落在 save_path 下）
                    const std::string full_path = event.content_root.empty()
                                                      ? event.save_path
                                                      : (std::filesystem::path(event.save_path) / event.content_root).
                                                      string();
                    store_.update_file_record_meta(rec->client_id, rec->task_protocol, rec->task_natural_key,
                                                   event.save_path,
                                                   event.original_name, event.content_root, full_path,
                                                   is_dir, ext);

                    log_i(key.c_str(), "解析完成 original_name='{}' content_root='{}' is_dir={}", event.original_name,
                          event.content_root, is_dir);
                } else {
                    // 快路：重名判定已完成（如 storage_moved_alert 触发的二次 PARSED），
                    // 元数据已落库，仅做状态迁移。
                    log_i(key.c_str(), "解析完成(快路) content_root='{}'", rec->root_name);
                }

                // RESOLVING → QUEUED 状态迁移（即时写，等待调度器准入）
                if (rec->status == DW_TASK_STATUS_RESOLVING) {
                    rec->status = DW_TASK_STATUS_QUEUED;
                    rec->synth_notified = false;
                    store_.update_file_record_status(rec->client_id, rec->task_protocol, rec->task_natural_key,
                                                     DW_TASK_STATUS_QUEUED, DW_REASON_NONE, "");
                    schedule_needed_ = true;
                    log_i(key.c_str(), "转 QUEUED 等待调度器准入");
                }
                break;
            }
            case EngineEventType::DOWNLOAD_FAILED: {
                // 按 reason 区分：FAIL=可重试（调度器自动重试），其余=不可重试终态
                const bool retryable = (event.reason == DW_REASON_FAIL);
                rec->status = retryable ? DW_TASK_STATUS_FAIL : DW_TASK_STATUS_ERROR;
                rec->reason = event.reason;
                rec->message = event.message;
                // 终态即时写（保留失败展示，用户手动重试或调度器自动重试）
                store_.update_file_record_status(rec->client_id, rec->task_protocol, rec->task_natural_key,
                                                 static_cast<dw_task_status_t>(rec->status), event.reason,
                                                 event.message);
                log_e(key.c_str(), "下载失败 retryable={} msg={}", retryable, event.message);
                schedule_needed_ = true;
                break;
            }
            case EngineEventType::STATUS_UPDATE: {
                // 进度遥测：仅更新内存，由 flush_dirty_locked 周期节流同步至 file_records。
                rec->total_size = event.total_size;
                rec->total_done = event.total_done;
                rec->download_rate = event.download_rate;
                rec->upload_rate = event.upload_rate;
                rec->support_range = event.support_range;
                rec->reason = event.reason;
                rec->message = event.message;
                if (!event.name.empty()) {
                    rec->original_root_name = event.name;
                }
                if (!event.etag.empty()) rec->etag = event.etag;
                if (!event.last_modified.empty()) rec->last_modified = event.last_modified;

                // 状态迁移（引擎侧冗余状态驱动）
                if (event.status != rec->status) {
                    switch (event.status) {
                        case DW_TASK_STATUS_COMPLETED:
                            rec->status = DW_TASK_STATUS_COMPLETED;
                            rec->reason = DW_REASON_NONE;
                            rec->message.clear();
                            store_.update_file_record_status(rec->client_id, rec->task_protocol, rec->task_natural_key,
                                                             DW_TASK_STATUS_COMPLETED, DW_REASON_NONE, "");
                            store_.delete_file_progress_by_task(rec->client_id, rec->task_protocol, rec->task_natural_key);
                            log_i(key.c_str(), "下载完成");
                            schedule_needed_ = true;
                            break;
                        case DW_TASK_STATUS_PAUSED:
                            rec->status = DW_TASK_STATUS_PAUSED;
                            rec->synth_notified = false;
                            reset_live_telemetry(*rec);
                            store_.update_file_record_status(rec->client_id, rec->task_protocol, rec->task_natural_key,
                                                             DW_TASK_STATUS_PAUSED, static_cast<dw_reason_t>(rec->reason),
                                                             rec->message);
                            log_i(key.c_str(), "暂停生效");
                            break;
                        case DW_TASK_STATUS_DOWNLOADING:
                            // 引擎恢复生效：仅暂停态回 QUEUED 等待调度准入
                            if (rec->status == DW_TASK_STATUS_PAUSED) {
                                rec->status = DW_TASK_STATUS_QUEUED;
                                rec->synth_notified = false;
                                store_.update_file_record_status(rec->client_id, rec->task_protocol, rec->task_natural_key,
                                                                 DW_TASK_STATUS_QUEUED, DW_REASON_NONE, "");
                                schedule_needed_ = true;
                            }
                            log_i(key.c_str(), "恢复生效 status={}", static_cast<int>(rec->status));
                            break;
                        default:
                            break;
                    }
                }
                break;
            }
            case EngineEventType::DELETED: {
                const std::string save_path = rec->save_path;
                const std::string content_root = rec->root_name;
                // 删除数据库相关数据
                store_.remove(rec->client_id, rec->task_protocol, rec->task_natural_key);
                log_i(key.c_str(), "任务已删除回收完成");
                // 清理包装目录：BT engine 已删除内部文件，wrapper 只需删除 save_path/content_root
                if (event.delete_files) {
                    std::error_code ec;
                    std::filesystem::remove_all(
                        std::filesystem::path(save_path) / content_root, ec);
                }
                schedule_needed_ = true;
                break;
            }
            case EngineEventType::TASK_FILES: {
                // HTTP 磁盘定名就绪（wrapper 模型）：落定 content_root（判重后 wrapper 目录名）、
                // name（原始文件名），同步修正 file_records。BT 不发此事件。
                // 幂等守卫：content_root 已落定则跳过（全量重下定名沿用既有 wrapper，同值）。
                if (event.protocol != DW_PROTOCOL_HTTP || event.files.empty() || event.name.empty()) break;
                if (!rec->root_name.empty()) break;
                const std::string raw_name = event.files[0].name ? event.files[0].name : "";
                rec->root_name = event.name; // 磁盘根实体 = wrapper 目录
                // name 与 STATUS_UPDATE 同款占位守卫：仅初始 natural_key 占位态写入。
                if (!raw_name.empty() && rec->original_root_name == rec->task_natural_key) {
                    rec->original_root_name = raw_name;
                }
                const std::string full_path =
                        (std::filesystem::path(rec->save_path) / event.name).string();
                // 元数据落库：root_name=wrapper 目录名，original_root_name=内层文件名（供重启恢复展示名/物理路径）
                store_.update_file_record_meta(rec->client_id, rec->task_protocol, rec->task_natural_key,
                                               rec->save_path,
                                               event.name, event.name, full_path, true, std::string());
                log_i(key.c_str(), "HTTP 定名 wrapper='{}' file='{}'", event.name, raw_name);
                break;
            }
            case EngineEventType::FILE_PROGRESS: {
                // 文件进度区间就绪（BT 连续 piece 达阈值）：引擎侧已合并完整区间集合，
                // 调用方序列化后直接保存；文件完成时区间缓存失去意义即删。
                if (event.full_path.empty()) break; // 引擎未能解析路径：丢弃该区间
                // 序列化区间集合为 JSON：[[start1,end1],[start2,end2],...]
                boost::json::array intervals_arr;
                for (const auto &[start, end]: event.intervals) {
                    boost::json::array interval;
                    interval.push_back(start);
                    interval.push_back(end);
                    intervals_arr.push_back(std::move(interval));
                }
                const std::string intervals_json = boost::json::serialize(intervals_arr);
                const int64_t downloaded = store_.save_file_progress(
                    rec->client_id, rec->task_protocol, rec->task_natural_key, event.file_index,
                    event.full_path, intervals_json);
                if (event.file_size > 0 && downloaded >= event.file_size) {
                    store_.delete_file_progress_by_file(rec->client_id, rec->task_protocol,
                                                        rec->task_natural_key, event.file_index);
                    log_d(key.c_str(), "文件进度缓存清空（完成） file_index={}", event.file_index);
                }
                break;
            }
            default:
                log_d(key.c_str(), "未处理的事件类型 type={}", static_cast<int>(event.type));
                break;
        }
    }

    /* ================================================================== */
    /*                          快照查询                                  */
    /* ================================================================== */

    int32_t TaskManager::list(dw_task_snapshot_t **out_tasks, int32_t *out_count) {
        if (!out_tasks || !out_count) return -1;

        std::scoped_lock lock(mtx_);

        // 先同步活跃任务进度遥测，使快照反映最新内存态（状态迁移已即时写，进度由节拍同步）。
        flush_dirty_locked();

        // 全量任务来自 DB（状态持久化权威，含未常驻内存的暂停/完成/错误任务），
        // 仅投影任务关联记录（本地文件/目录条目不参与任务快照）。
        const auto all_records = store_.load_file_records(client_id_);
        std::vector<const FileRecord *> all;
        all.reserve(all_records.size());
        for (const auto &fr: all_records) {
            if (fr.has_task()) all.push_back(&fr);
        }
        const auto n = static_cast<int32_t>(all.size());
        if (n == 0) {
            *out_tasks = nullptr;
            *out_count = 0;
            return 0;
        }

        auto *arr = static_cast<dw_task_snapshot_t *>(
            std::calloc(n, sizeof(dw_task_snapshot_t)));
        if (!arr) {
            *out_tasks = nullptr;
            *out_count = 0;
            return -1;
        }

        // 逐条从 FileRecord 投影为 C ABI 快照（字符串堆分配，调用方经 dw_task_list_free 释放）。
        for (int32_t i = 0; i < n; ++i) {
            const FileRecord &fr = *all[i];
            dw_task_snapshot_t s{};
            s.protocol = fr.task_protocol;
            // 契约要求纯 natural_key（HTTP=url / BT=info_hash / LOCAL=content_root）：
            // client_id 由调用方自身持有，不得混入，否则 App 回传的 key 无法命中记录。
            s.natural_key = utils::dup_cstr(fr.task_natural_key);
            // FFI 输出保持 url/info_hash 分离：按协议从 natural_key 填充
            s.url = utils::dup_cstr(fr.task_protocol == DW_PROTOCOL_HTTP ? fr.task_natural_key : std::string());
            s.info_hash = utils::dup_cstr(
                fr.task_protocol == DW_PROTOCOL_TORRENT ? fr.task_natural_key : std::string());
            s.name = utils::dup_cstr(fr.original_root_name.empty() ? fr.root_name : fr.original_root_name);
            s.save_path = utils::dup_cstr(fr.save_path);
            s.status = static_cast<dw_task_status_t>(fr.status);
            s.progress = (fr.total_size > 0) ? static_cast<double>(fr.total_done) / fr.total_size : -1.0;
            s.total_size = fr.total_size;
            s.total_done = fr.total_done;
            s.priority = fr.priority;
            s.created_at = fr.created_at;
            s.modified_at = fr.modified_at;
            s.source = fr.type;
            s.content_root = utils::dup_cstr(fr.root_name);
            arr[i] = s;
        }
        *out_tasks = arr;
        *out_count = n;
        return 0;
    }

    std::vector<FileRecord> TaskManager::list_file_records() {
        std::scoped_lock lock(mtx_);
        auto out = store_.load_file_records(client_id_);
        // 按 modified_at DESC 排序（与 DB 查询一致）
        std::sort(out.begin(), out.end(), [](const FileRecord &a, const FileRecord &b) {
            return a.modified_at > b.modified_at;
        });
        return out;
    }

    FileRecord *TaskManager::find_file_record(const std::string &client_id, dw_protocol_t proto,
                                              const std::string &natural_key) {
        return load_task_record(client_id, proto, natural_key);
    }

    bool TaskManager::is_file_record_parsed(const std::string &client_id, dw_protocol_t proto,
                                            const std::string &natural_key) {
        std::scoped_lock lock(mtx_);
        FileRecord *fr = find_file_record(client_id, proto, natural_key);
        return fr && fr->parsed;
    }

    /* ================================================================== */
    /*                          调度线程                                  */
    /* ================================================================== */

    void TaskManager::scheduler_loop() {
        // 周期采集进度并转发
        while (running_.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(flush_interval_ms_));
            if (!running_.load()) break;

            std::vector<FileRecord> fwd_records;
            {
                std::scoped_lock lock(mtx_);
                collect_progress_locked(fwd_records);
            }

            // 状态采集已收敛至各引擎内部定时线程（Torrent: status_loop 2s），此处不再驱动。

            // 锁外转发（周期节奏）：只读锁内已拷出的本地副本（已含遥测），避免与上层回调重入交叉。
            for (const auto &rec: fwd_records) {
                emit_progress(rec);
            }
        }
    }

    void TaskManager::collect_progress_locked(std::vector<FileRecord> &fwd_records) {
        for (auto &[_, task_record]: tasks_) {
            // 引擎无 ctx 的合成态（QUEUED/PAUSED）：直接从记录投影合成一帧，一次性去重；
            // 回调唯一出口收归 A 线程，pause()/run_schedule（B 线程）仅置态，不再直接发射。
            if ((task_record.status == DW_TASK_STATUS_QUEUED ||
                 task_record.status == DW_TASK_STATUS_PAUSED) && !task_record.synth_notified) {
                task_record.synth_notified = true;
                fwd_records.push_back(task_record);
                continue;
            }
            if (task_record.status != DW_TASK_STATUS_DOWNLOADING &&
                task_record.status != DW_TASK_STATUS_RESOLVING)
                continue;

            // 推模型：进度字段已由引擎线程经 on_progress 实时写入 FileRecord，
            // 此处仅判终态并收集转发帧，不再调 query_progress。
            // RESOLVING（解析中）一并转发：上层据此感知解析阶段；元数据就绪经
            // PARSED 事件迁 QUEUED，准入入下载由 run_schedule 单点完成。

            fwd_records.push_back(task_record);
        }
    }

    void TaskManager::maintenance_loop() {
        // 周期调度 + 持久化
        while (running_.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(maintenance_interval_ms_));
            if (!running_.load()) break;

            {
                std::scoped_lock lock(mtx_);
                maintenance_persist_locked();

                if (schedule_needed_) {
                    schedule_needed_ = false;
                    run_schedule();
                }
            }

            // B 线程消费引擎事件（Boost.Asio io_context 投递的 PARSED/STORAGE_MOVED 等）。
            // poll 非阻塞，处理所有待消费事件后立即返回。
            event_ioc_.poll();

            if (http_) http_->sweep();
            if (torrent_) torrent_->sweep();
        }
    }

    void TaskManager::maintenance_persist_locked() {
        std::vector<std::string> to_remove;
        for (auto &[union_id, task_record]: tasks_) {
            const bool paused = (task_record.status == DW_TASK_STATUS_PAUSED);
            // 判终态
            const bool terminal = (task_record.status == DW_TASK_STATUS_COMPLETED ||
                                   task_record.status == DW_TASK_STATUS_ERROR);
            if (task_record.status == DW_TASK_STATUS_DOWNLOADING || terminal || paused) {
                snapshot_segments_locked(task_record);
            }
            // snapshot 可能经数据驱动置 status=COMPLETED，重判终态
            const bool now_terminal = (task_record.status == DW_TASK_STATUS_COMPLETED ||
                                       task_record.status == DW_TASK_STATUS_ERROR);
            if (now_terminal) {
                to_remove.push_back(union_id);
            } else if (paused) {
                if (task_record.task_protocol == DW_PROTOCOL_HTTP) {
                    // HTTP 暂停态延迟逐出：待引擎 ctx 被 sweep 回收（task_released 确认）后
                    // 再移出内存，规避先逐出导致异步 resume 丢失。
                    if (!http_ || http_->task_released(task_record.task_natural_key)) {
                        to_remove.push_back(union_id);
                    }
                } else {
                    // BT：handle 常驻 session，无 ctx 回收信号；暂停态已即时写权威列，直接逐出。
                    to_remove.push_back(union_id);
                }
            }
        }

        flush_dirty_locked();

        // 内存中移除任务
        for (const auto &nk: to_remove) {
            unregister_task(nk);
        }
    }

    void TaskManager::emit_progress(const FileRecord &rec) {
        dw_progress_t p{};
        // FFI 输出保持 url/info_hash 分离：按协议从 task_natural_key 填充
        p.url = (rec.task_protocol == DW_PROTOCOL_HTTP) ? rec.task_natural_key.c_str() : "";
        p.info_hash = (rec.task_protocol == DW_PROTOCOL_TORRENT) ? rec.task_natural_key.c_str() : "";
        p.protocol = rec.task_protocol;
        p.name = rec.root_name.c_str();
        // output_path 为权威 save_path（已含包层目录，冲突时直接追加）。
        p.output_path = rec.save_path.c_str();
        p.total_size = rec.total_size;
        p.total_done = rec.total_done;
        p.remaining = (rec.total_size > 0 && rec.total_size >= rec.total_done)
                          ? (rec.total_size - rec.total_done)
                          : -1;
        p.progress = (rec.total_size > 0) ? static_cast<double>(rec.total_done) / rec.total_size : -1.0;
        p.download_rate = rec.download_rate;
        p.eta = (rec.download_rate > 0.0 && p.remaining > 0)
                    ? static_cast<double>(p.remaining) / rec.download_rate
                    : -1.0;
        p.task_status = static_cast<dw_task_status_t>(rec.status); // 权威态由 TaskManager 独占
        p.reason = static_cast<dw_reason_t>(rec.reason);
        p.message = rec.message.c_str();
        p.saved_at_unix_ms = now_unix_ms();
        p.support_range = rec.support_range;
        p.etag = rec.etag.c_str();
        p.last_modified = rec.last_modified.c_str();
        p.upload_rate = rec.upload_rate;
        p.source = rec.type;
        // task_natural_key 在回调周期内有效（borrowed 指针，与 progress 其他字符串字段一致）；
        // 调用方如需保留须深拷贝。
        p.protocol = (rec.task_protocol == DW_PROTOCOL_HTTP) ? DW_PROTOCOL_HTTP : DW_PROTOCOL_TORRENT;
        p.natural_key = rec.task_natural_key.c_str();
        p.content_root = rec.root_name.c_str();
        dw::emit_progress(&p);
    }

    void TaskManager::reset_live_telemetry(FileRecord &rec) {
        rec.download_rate = 0.0;
        rec.upload_rate = 0.0;
        rec.reason = DW_REASON_NONE;
        rec.message.clear();
    }

    void TaskManager::run_schedule() {
        // 周期性检查额度（仅 DOWNLOADING 占用），按 priority + created_at 顺序选取
        // QUEUED 任务调 resume_task 准入：
        //   QUEUED：元数据已落定 / 重试任务，handle 存在走快路径直接恢复下载。
        //   BT 任务添加时已同步创建 handle（RESOLVING），PARSED 事件后转 QUEUED 等待调度。
        std::scoped_lock lock(mtx_);
        while (running_.load() && net_allowed_) {
            int32_t active = active_count_locked();

            // Force 让行：名额已满但有 force 任务时，暂停最慢的活跃任务腾出槽位。
            if (active >= max_concurrent_) {
                FileRecord *force_task = nullptr;
                for (auto &[_, tr]: tasks_) {
                    if (tr.status == DW_TASK_STATUS_QUEUED && tr.force) {
                        force_task = &tr;
                        break;
                    }
                }
                if (!force_task) break; // 无 force 任务，正常退出

                // 找下载最慢的活跃任务
                FileRecord *slowest = nullptr;
                for (auto &[_, tr]: tasks_) {
                    if (tr.status != DW_TASK_STATUS_DOWNLOADING) continue;
                    if (!slowest || tr.download_rate < slowest->download_rate) {
                        slowest = &tr;
                    }
                }
                if (!slowest) break; // 无活跃任务可暂停（不应发生）

                log_i(slowest->task_natural_key.c_str(), "force 让行: 暂停最慢任务 rate={}", slowest->download_rate);
                slowest->status = DW_TASK_STATUS_QUEUED;
                slowest->synth_notified = false;
                store_.update_file_record_status(slowest->client_id, slowest->task_protocol,
                                                 slowest->task_natural_key,
                                                 DW_TASK_STATUS_QUEUED, DW_REASON_NONE, "");

                // 引擎暂停（recursive_mutex 支持同线程重入）
                if (IDownloadEngine *eng = engine_of(slowest->task_protocol)) {
                    eng->pause_task(slowest->task_natural_key, client_id_);
                }
                schedule_needed_ = true; // 暂停后释放了槽位，触发下一轮调度
                continue;
            }

            FileRecord *best = nullptr;
            for (auto &[_, task_record]: tasks_) {
                if (task_record.status != DW_TASK_STATUS_QUEUED)
                    continue;
                if (!best ||
                    task_record.priority > best->priority ||
                    (task_record.priority == best->priority && task_record.created_at < best->created_at)) {
                    best = &task_record;
                }
            }
            if (!best) {
                break;
            }

            const bool ok = call_resume_task(*best);
            auto it = tasks_.find(best->union_id());
            if (it == tasks_.end()) continue; // 期间被删除，跳过
            FileRecord &rec = it->second;
            if (!ok) {
                // 准入失败：迁 FAIL 释放名额（可重试，调度器自动重新准入）。
                rec.status = DW_TASK_STATUS_FAIL;
                rec.reason = DW_REASON_FAIL;
                rec.message = "调度恢复失败";
                store_.update_file_record_status(rec.client_id, rec.task_protocol, rec.task_natural_key,
                                                 DW_TASK_STATUS_FAIL, DW_REASON_FAIL, "调度恢复失败");
                schedule_needed_ = true;
            } else {
                // QUEUED 准入成功：直接进入下载（即时写权威列）。
                rec.status = DW_TASK_STATUS_DOWNLOADING;
                rec.force = false; // force 标记已消费
                rec.synth_notified = false;
                store_.update_file_record_status(rec.client_id, rec.task_protocol, rec.task_natural_key,
                                                 DW_TASK_STATUS_DOWNLOADING, DW_REASON_NONE, "");
            }
        }
    }

    void TaskManager::set_network_allowed(bool allowed) {
        // 需停传输的活跃任务：(协议, 引擎键)。引擎键锁外喂引擎。
        struct PendingPause {
            dw_protocol_t protocol;
            std::string key; // HTTP=url，BT=info_hash
            std::string union_id; // tasks_ 键（union_id 格式）
        };
        std::vector<PendingPause> to_pause;
        {
            std::scoped_lock lock(mtx_);
            if (net_allowed_ == allowed) return; // 状态未变，幂等跳过
            net_allowed_ = allowed;
            if (!allowed) {
                // 闸门关闭：收集所有活跃任务（HTTP + BT），锁外逐任务暂停后回落 QUEUED。
                // 不再整会话 pause，仅停各任务的载荷传输；session 存活维持连接/心跳。
                for (const auto &[_, task_record]: tasks_) {
                    if (!status_occupies_slot(static_cast<dw_task_status_t>(task_record.status))) continue;
                    to_pause.push_back({
                        task_record.task_protocol,
                        task_record.task_natural_key,
                        task_record.union_id()
                    });
                }
            } else {
                // 闸门开启：唤醒调度按 QUEUED→准入路径重启（BT 经 add_task 幂等分支 resume）。
                schedule_needed_ = true;
            }
        }
        if (allowed) {
            return;
        }

        // 锁外逐任务停传输（pause_task 内部可能 join / 加引擎锁，不可持 mtx_ 调用以规避死锁）。
        for (const auto &pp: to_pause) {
            if (IDownloadEngine *eng = engine_of(pp.protocol)) {
                eng->pause_task(pp.key, client_id_);
            }
        }

        // 引擎已停，回落 QUEUED 待闸门开启后重启；遥测归零，置 schedule_needed_ 由调度线程单点发射 QUEUED 合成帧。
        {
            std::scoped_lock lock(mtx_);
            for (const auto &pp: to_pause) {
                auto it = tasks_.find(pp.union_id);
                if (it == tasks_.end()) continue;
                FileRecord &task_record = it->second;
                task_record.status = DW_TASK_STATUS_QUEUED;
                task_record.synth_notified = false; // 待采集拍统一发射合成帧
                reset_live_telemetry(task_record);
                // 回落队列即时写（迁移点）：闸门开启后经准入路径重启。
                store_.update_file_record_status(task_record.client_id, task_record.task_protocol,
                                                 task_record.task_natural_key,
                                                 DW_TASK_STATUS_QUEUED, DW_REASON_NONE, "");
            }
            schedule_needed_ = true;
        }
    }


    void TaskManager::set_max_concurrent(int32_t value) {
        const int32_t next = value > 0 ? value : 3;
        {
            std::scoped_lock lock(mtx_);
            if (max_concurrent_ == next) return; // 未变，幂等跳过
            max_concurrent_ = next;
            schedule_needed_ = true; // 调高时由调度线程准入 QUEUED 任务
        }
        log_i("", "[EVENT] 并发上限调整: max_concurrent={}", next);
    }


    // ---- 任务文件实时查询 ----

    dw_submit_result_t TaskManager::load_files(dw_protocol_t proto, const std::string &natural_key) {
        // task_files 表已移除（磁盘为事实源）：BT 经 handle 实时查询，HTTP 从任务记录推导。
        std::scoped_lock lock(mtx_);
        if (proto == DW_PROTOCOL_TORRENT) {
            if (torrent_) {
                return torrent_->get_file_list(client_id_, natural_key);
            }
            return dw_submit_result_t::failure(DW_REASON_ERROR, "BT 引擎不可用");
        }
        // HTTP 单文件：root_name（wrapper 目录）+ original_root_name（原始文件名）落定后可以推导。
        const FileRecord *rec_ptr = nullptr;
        FileRecord db_rec;
        const auto it = tasks_.find(union_id_of(client_id_, proto, natural_key));
        if (it != tasks_.end()) {
            rec_ptr = &it->second;
        } else if (store_.find_file_record(client_id_, proto, natural_key, db_rec)) {
            rec_ptr = &db_rec;
        }
        if (!rec_ptr || rec_ptr->root_name.empty() || rec_ptr->original_root_name.empty()) {
            return dw_submit_result_t::failure(DW_REASON_ERROR, "定名未落定");
        }
        auto result = dw_submit_result_t::success();
        FileInfo fi;
        fi.index = 0;
        fi.name = rec_ptr->original_root_name;
        fi.full_path = (std::filesystem::path(rec_ptr->save_path) / rec_ptr->root_name / rec_ptr->original_root_name).string();
        fi.ext = utils::file_extension(rec_ptr->original_root_name);
        fi.size = rec_ptr->total_size;
        fi.offset = 0; // HTTP 单文件模型，全局偏移恒为 0
        fi.status = rec_ptr->status == DW_TASK_STATUS_COMPLETED ? 2 : 0;
        fi.selected = true;
        // downloaded_bytes 回填
        fi.downloaded_bytes = store_.sum_file_progress_by_file(client_id_, proto, natural_key, 0);
        result.files.push_back(std::move(fi));
        return result;
    }

    // ---- 路径与展示辅助 ----

    std::string TaskManager::disk_root_path(const FileRecord &rec) {
        // 最终落盘目录 = save_path / root_name；root_name 即 wrapper 名（去后缀，冲突时含 (n)）。
        return (std::filesystem::path(rec.save_path) / rec.root_name).string();
    }

    std::string TaskManager::display_name(const FileRecord &rec) {
        // 展示名 = root_name（已含可能的后缀以去重名）。
        return rec.root_name;
    }

    // ---- 本地文件浏览与管理 ----

    int32_t TaskManager::scan_local_tasks(const std::string &save_path,
                                          dw_task_snapshot_t **out_tasks,
                                          int32_t *out_count) {
        if (!out_tasks || !out_count || save_path.empty()) return -1;

        std::scoped_lock lock(mtx_);

        // 1. 构建被占用的根条目名集合（已登记的本地文件视为已占用）
        std::unordered_set<std::string> occupied;
        const auto existing_files = store_.load_file_records_by_save_path(client_id_, save_path);
        for (const auto &f: existing_files) {
            occupied.insert(f.root_name);
        }

        // 2. 扫描目录，将非占用条目增量注册为本地文件条目（type=1）
        std::vector<FileRecord> new_records;
        std::error_code ec;
        for (auto it = std::filesystem::directory_iterator(save_path, ec);
             it != std::filesystem::directory_iterator(); ++it) {
            const auto entry_name = it->path().filename().string();
            if (!entry_name.empty() && entry_name[0] == '.') continue;
            if (occupied.count(entry_name)) continue;

            FileRecord fr;
            fr.client_id = client_id_;
            fr.type = DW_SOURCE_LOCAL_FILE; // 本地扫描发现
            fr.save_path = save_path;
            fr.root_name = entry_name;
            // 本地文件条目的复合主键：(client_id, LOCAL, root_name)。
            // task_natural_key 必须赋值，否则所有条目共用同一 union_id 而相互覆盖。
            fr.task_natural_key = entry_name;
            fr.full_path = (std::filesystem::path(save_path) / entry_name).string();
            fr.status = DW_TASK_STATUS_COMPLETED;
            fr.created_at = now_unix_ms();
            if (it->is_directory(ec)) {
                fr.file_type = true; // 目录
                fr.total_size = 0;
            } else {
                fr.file_type = false; // 文件
                fr.total_size = static_cast<int64_t>(it->file_size(ec));
                fr.total_done = fr.total_size;
                // 提取后缀
                const auto ext_pos = entry_name.rfind('.');
                if (ext_pos != std::string::npos && ext_pos + 1 < entry_name.size()) {
                    fr.ext = entry_name.substr(ext_pos + 1);
                }
            }
            store_.insert_file_record(fr);
            new_records.push_back(std::move(fr));
        }
        if (ec) return -1;

        // 3. 仅返回新增条目的快照
        const auto n = static_cast<int32_t>(new_records.size());
        if (n == 0) {
            *out_tasks = nullptr;
            *out_count = 0;
            return 0;
        }
        auto *arr = static_cast<dw_task_snapshot_t *>(
            std::calloc(n, sizeof(dw_task_snapshot_t)));
        if (!arr) {
            *out_tasks = nullptr;
            *out_count = 0;
            return -1;
        }
        for (int32_t i = 0; i < n; ++i) {
            const FileRecord &f = new_records[i];
            dw_task_snapshot_t s{};
            s.protocol = DW_PROTOCOL_LOCAL;
            s.natural_key = utils::dup_cstr(f.root_name);
            // 本地文件条目既无 url 也无 info_hash，与 list() 保持空串而非空指针。
            s.url = utils::dup_cstr(std::string());
            s.info_hash = utils::dup_cstr(std::string());
            s.name = utils::dup_cstr(f.root_name);
            s.save_path = utils::dup_cstr(f.save_path);
            s.status = static_cast<dw_task_status_t>(f.status);
            s.progress = 1.0;
            s.total_size = f.total_size;
            s.total_done = f.total_done;
            s.created_at = f.created_at;
            s.modified_at = f.modified_at;
            s.source = f.type; // 与入库值一致（DW_SOURCE_LOCAL_FILE）
            s.content_root = utils::dup_cstr(f.root_name);
            arr[i] = s;
        }
        *out_tasks = arr;
        *out_count = n;
        return 0;
    }

    int32_t TaskManager::validate_local_tasks(const std::string &save_path,
                                              int32_t *out_invalidated_count) {
        if (save_path.empty()) return -1;

        std::scoped_lock lock(mtx_);

        // 加载该 save_path 下的文件记录，筛选 type=1 且物理文件不存在的条目
        const auto files = store_.load_file_records_by_save_path(client_id_, save_path);
        int32_t invalidated = 0;
        std::error_code ec;
        for (const auto &f: files) {
            if (f.type != DW_SOURCE_LOCAL_FILE) continue; // 仅校验本地文件条目
            // 构建物理路径：save_path / root_name
            std::filesystem::path physical(std::filesystem::path(f.save_path) / f.root_name);
            if (!std::filesystem::exists(physical, ec)) {
                // 标记为已失效：更新 status 为 INVALIDATED
                store_.update_file_record_status_by_name(f.save_path, f.root_name,
                                                         DW_TASK_STATUS_INVALIDATED);
                ++invalidated;
            }
        }

        if (out_invalidated_count) *out_invalidated_count = invalidated;
        return 0;
    }

    int32_t TaskManager::clear_local_tasks(const std::string &save_path) {
        if (save_path.empty()) return -1;

        std::scoped_lock lock(mtx_);

        // 加载该 save_path 下的文件记录，仅清理本地文件条目（type=DW_SOURCE_LOCAL_FILE）
        const auto files = store_.load_file_records_by_save_path(client_id_, save_path);
        std::error_code ec;
        for (const auto &f: files) {
            if (f.type != DW_SOURCE_LOCAL_FILE) continue;
            // 删除物理文件/目录：save_path / root_name
            const std::filesystem::path full_path = std::filesystem::path(f.save_path) / f.root_name;
            std::filesystem::remove_all(full_path, ec);
        }
        // 批量删除 file_records 中的本地文件条目
        store_.clear_local_tasks(client_id_, save_path);
        return 0;
    }

    int32_t TaskManager::delete_local_entry(const std::string &save_path, const std::string &root_name) {
        std::scoped_lock lock(mtx_);

        if (save_path.empty() || root_name.empty()) return -1;

        // 类型门禁：仅允许删除本地文件条目。HTTP / BT 任务必须走 dw_delete_task，
        // 否则会越过 engine 层删文件，留下引擎内仍在运行的孤儿任务。
        const auto records = store_.load_file_records_by_save_path(client_id_, save_path);
        const FileRecord *target = nullptr;
        for (const auto &cr: records) {
            if (cr.root_name == root_name) {
                target = &cr;
                break;
            }
        }
        if (!target || target->type != DW_SOURCE_LOCAL_FILE) return -1;

        // 删除物理文件/目录：save_path / root_name
        const std::filesystem::path full_path = std::filesystem::path(save_path) / root_name;
        std::error_code ec;
        std::filesystem::remove_all(full_path, ec);

        // 删除 file_records 记录
        store_.delete_file_record_by_name(client_id_, save_path, root_name);
        return ec ? -1 : 0;
    }

    dw_submit_result_t TaskManager::parse_magnet(const std::string &magnet_link) {
        return TorrentEngine::parse_magnet(magnet_link);
    }

    dw_submit_result_t TaskManager::parse_torrent_file(const std::string &torrent_file_path) {
        return TorrentEngine::parse_torrent_file(torrent_file_path);
    }

    bool TaskManager::call_resume_task(const FileRecord &task_record) {
        IDownloadEngine *eng = engine_of(task_record.task_protocol);
        if (!eng) return false;

        // 调度准入：平铺参数直传；引擎内部经三要素自取 resume_data。
        eng->resume_task(task_record.task_natural_key, client_id_,
                         task_record.priority_file_indexes);
        return true;
    }

    IDownloadEngine *TaskManager::engine_of(const dw_protocol_t proto) const {
        return proto == DW_PROTOCOL_HTTP ? http_ : torrent_;
    }

    int32_t TaskManager::active_count_locked() const {
        int32_t n = 0;
        for (const auto &[_, task_record]: tasks_) {
            if (status_occupies_slot(static_cast<dw_task_status_t>(task_record.status))) ++n;
        }
        return n;
    }

    void TaskManager::flush_dirty_locked() {
        for (auto &[_, task_record]: tasks_) {
            // 同步 file_records 进度与错误字段（状态迁移已即时写，此处仅节流同步遥测）。
            if (task_record.total_size >= 0 || task_record.total_done > 0) {
                store_.sync_file_record_progress(task_record.task_protocol, task_record.task_natural_key,
                                                 task_record.status, task_record.total_size,
                                                 task_record.total_done, task_record.reason,
                                                 task_record.message);
            }
        }
    }

    void TaskManager::register_task(FileRecord task_record) {
        // tasks_ 以 union_id 为 key，无冗余索引。
        tasks_[task_record.union_id()] = std::move(task_record);
    }

    void TaskManager::unregister_task(const std::string &union_id) {
        tasks_.erase(union_id);
    }

    void TaskManager::snapshot_segments_locked(FileRecord &task_record) {
        // HTTP 用周期快照维护进度缓存；BT 已改由 FILE_PROGRESS 事件（piece 驱动）增量维护，
        // 此处不再轮询引擎（避免全量重写覆盖事件增量，也省去逐文件 get_file_ranges 调用）。
        if (task_record.task_protocol != DW_PROTOCOL_HTTP) return;
        const std::string &key = task_record.task_natural_key;
        if (key.empty() || !http_) return;

        // 收集单文件最新分段，更新 DB 快照（HTTP 恒 file_index=0）。
        struct file_result {
            int32_t file_index;
            std::vector<dw_byte_range_t> ranges;
        };
        std::vector<file_result> results;
        {
            auto ranges = http_->get_file_ranges(key, 0);
            if (!ranges.empty()) {
                results.push_back({0, std::move(ranges)});
            }
        }

        // 进度区间缓存（全量重写，HTTP 周期快照；BT 已改由 FILE_PROGRESS 事件增量维护）。
        // 物理路径经 resolve 推导（wrapper 模型 save_path/content_root/name）。
        {
            std::vector<std::tuple<std::string, int32_t, std::vector<dw_byte_range_t> > > batch;
            batch.reserve(results.size());
            for (const auto &fr: results) {
                std::string path;
                int64_t size = 0;
                if (!resolve_file_path_locked(task_record.task_protocol, task_record.task_natural_key,
                                              fr.file_index, path, size))
                    continue;
                batch.emplace_back(std::move(path), fr.file_index, fr.ranges);
            }
            store_.replace_file_progress(task_record.client_id, task_record.task_protocol,
                                         task_record.task_natural_key, batch);
        }

        // ---- 数据驱动完成检测（仅 DOWNLOADING 态） ----
        if (task_record.status != DW_TASK_STATUS_DOWNLOADING) return;

        // HTTP 单文件：总大小取任务记录 total_size（chunked 无总长为 -1，跳过完成判定）。
        const int64_t file_size = task_record.total_size;
        if (file_size <= 0) return;

        bool all_complete = true;
        bool any_checkable = false;
        for (const auto &fr: results) {
            any_checkable = true;
            if (is_file_complete(fr.ranges, file_size)) {
                // 文件完成：区间缓存失去意义即删（完成态由"磁盘存在+无缓存"推断）。
                store_.delete_file_progress_by_file(task_record.client_id, task_record.task_protocol,
                                                    task_record.task_natural_key, fr.file_index);
            } else {
                all_complete = false;
            }
        }

        // 所有可判定文件均完成 → 数据驱动任务完成（直接迁权威态）
        if (any_checkable && all_complete) {
            task_record.status = DW_TASK_STATUS_COMPLETED;
            task_record.reason = DW_REASON_NONE;
            task_record.message.clear();
            store_.update_file_record_status(task_record.client_id, task_record.task_protocol,
                                             task_record.task_natural_key,
                                             DW_TASK_STATUS_COMPLETED, DW_REASON_NONE, "");
            schedule_needed_ = true;
            log_i(task_record.task_natural_key.c_str(), "数据驱动任务完成 union_id={}", task_record.union_id());
        }
    }

    std::vector<dw_byte_range_t> TaskManager::get_cached_segments(dw_protocol_t proto, const std::string &natural_key,
                                                                  int32_t file_index) {
        // 内存缓存已移除：直接读 DB 最新快照（经物理路径查询）。
        std::scoped_lock lock(mtx_);
        std::string path;
        int64_t size = 0;
        if (!resolve_file_path_locked(proto, natural_key, file_index, path, size)) return {};
        return store_.load_segments(path, file_index);
    }

    int32_t TaskManager::get_task_status(dw_protocol_t proto, const std::string &natural_key) {
        std::scoped_lock lock(mtx_);
        const auto it = tasks_.find(union_id_of(proto, natural_key));
        if (it == tasks_.end()) return -1;
        return static_cast<int32_t>(it->second.status);
    }
} // namespace dw
