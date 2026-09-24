/**
 * @file task_manager.cpp
 * @brief 库内任务中枢实现：SQLite 持久化 + 优先级就绪队列 + 事件驱动准入调度。
 *
 * 并发模型（双线程职责分离）：
 *   - mtx_ 保护注册表 tasks_ 与 DB（sqlite3 串行化模式，读写均在持锁期间）；
 *   - A 线程（scheduler_loop，快节拍）：任务调度准入，检查额度并准入 QUEUED 任务；
 *   - B 线程（maintenance_loop，慢节拍）：持久化落库 + 进度推送 + 事件消费 + 引擎 sweep；
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
        /// 占用下载额度的状态集合：DOWNLOADING + RESOLVING。
        /// RESOLVING 正在下载元数据（BT 磁力链），占用网络带宽；
        /// QUEUED 为纯等待态，不占名额；暂停/错误/完成自动释放额度。
        bool status_occupies_slot(dw_task_status_t s) {
            return s == DW_TASK_STATUS_DOWNLOADING || s == DW_TASK_STATUS_RESOLVING;
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

    void TaskManager::apply_config(const Config &cfg) {
        config_ = cfg;
        if (config_.max_concurrent_downloads <= 0) config_.max_concurrent_downloads = 3;
        net_allowed_ = !(config_.network_type == 2 && !config_.allow_mobile_data);
        log_i("manager", "应用配置: {}", to_json_string(config_));
    }

    int32_t TaskManager::start() {
        if (config_.client_id.empty()) {
            log_e("manager", "未知客户端标识");
            return -1;
        }

        const std::string dir = !config_.work_dir.empty() ? config_.work_dir : ".";
        const std::string story_path = dir + "/leopard_tasks.db";

        std::scoped_lock lock(mtx_);
        if (!store_.open(story_path)) {
            log_e("manager", "下载器数据加载失败: {}", story_path);
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
                DW_TASK_STATUS_QUEUED,
                DW_TASK_STATUS_DOWNLOADING
            };
            for (auto &fr: store_.load_file_records(config_.client_id, active_protocols, active_statuses)) {
                register_task(std::move(fr));
            }
        }

        running_.store(true);
        // 创建 work_guard 保持 event_ioc_ 运行，直到 stop() 时 reset
        event_work_guard_.emplace(boost::asio::make_work_guard(event_ioc_));
        worker_ = std::thread([this] { scheduler_loop(); });
        maintenance_ = std::thread([this] { maintenance_loop(); });
        event_consumer_ = std::thread([this] { event_consumer_loop(); });

        log_i("manager", "下载器[{}]启动成功", config_.client_id);
        return 0;
    }

    void TaskManager::stop() {
        if (!running_.exchange(false)) {
            return;
        }
        if (worker_.joinable()) {
            worker_.join();
        }
        if (maintenance_.joinable()) {
            maintenance_.join();
        }

        event_work_guard_.reset();
        event_ioc_.stop();
        if (event_consumer_.joinable()) {
            event_consumer_.join();
        }

        std::scoped_lock lock(mtx_);
        flush_dirty_locked();
        store_.close();
        log_i("manager", "下载器已停止");
    }

    /* ================================================================== */
    /*                          控制操作                                  */
    /* ================================================================== */

    dw_submit_result_t TaskManager::add(TaskParams &params) {
        log_i("manager", "添加任务[{}]", to_json_string(params));
        if (params.natural_key.empty()) {
            return dw_submit_result_t::failure(DW_REASON_ERROR, "natural_key 为空");
        }
        if (config_.client_id == params.client_id) {
            return self_add(params);
        } else {
            return remote_add(params);
        }
    }

    dw_submit_result_t TaskManager::remote_add(TaskParams & /*params*/) {
        return dw_submit_result_t::failure(DW_REASON_ERROR, "远程任务暂不支持");
    }

    dw_submit_result_t TaskManager::self_add(TaskParams &params) {
        const dw_protocol_t proto = params.protocol;
        const std::string &client_id = params.client_id;
        const std::string &natural_key = params.natural_key;
        if (params.save_path.empty()) {
            params.save_path = config_.save_path;
        }

        {
            std::scoped_lock lock(mtx_);

            if (FileRecord *rec = load_task_record(client_id, proto, natural_key)) {
                // 任务已存在：更新修改时间，重置状态为 QUEUED
                rec->modified_at = now_unix_ms();
                rec->status = DW_TASK_STATUS_QUEUED;
                rec->dirty = true;
                store_.touch_file_record(client_id, proto, natural_key);
            } else {
                FileRecord task_record;
                task_record.client_id = client_id;
                task_record.task_protocol = proto;
                task_record.task_natural_key = natural_key;
                task_record.save_path = params.save_path;
                task_record.type = (proto == DW_PROTOCOL_TORRENT) ? DW_SOURCE_REMOTE_FILE : DW_SOURCE_TASK_FILE;
                task_record.is_remote = true;
                task_record.status = DW_TASK_STATUS_QUEUED;
                task_record.created_at = now_unix_ms();
                task_record.modified_at = task_record.created_at;
                store_.insert_file_record(task_record);
            }
        }

        return dw_submit_result_t::success();
    }

    dw_submit_result_t TaskManager::pause(const TaskParams &params) const {
        log_i(params.natural_key.c_str(), "暂停任务[{}]", to_json_string(params));
        if (config_.client_id == params.client_id) {
            return self_pause(params);
        } else {
            return remote_pause(params);
        }
    }

    dw_submit_result_t TaskManager::self_pause(const TaskParams &params) const {
        if (IDownloadEngine *eng = engine_of(params.protocol)) {
            return eng->pause_task(params.natural_key, params.client_id);
        }
        return dw_submit_result_t::failure(DW_REASON_ERROR, "下载引擎不可用");
    }

    dw_submit_result_t TaskManager::remote_pause(const TaskParams & /*params*/) const {
        return dw_submit_result_t::failure(DW_REASON_ERROR, "远程任务暂不支持");
    }


    dw_submit_result_t TaskManager::resume(const TaskParams &params) {
        log_i(params.natural_key.c_str(), "恢复任务[{}]", to_json_string(params));
        if (config_.client_id == params.client_id) {
            return self_resume(params);
        } else {
            return remote_resume(params);
        }
    }

    dw_submit_result_t TaskManager::self_resume(const TaskParams &params) {
        {
            std::scoped_lock lock(mtx_);
            FileRecord *rec = load_task_record(params.client_id, params.protocol, params.natural_key);
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
            rec->dirty = true;
            store_.update_file_record_status(rec->client_id, rec->task_protocol, rec->task_natural_key,
                                             DW_TASK_STATUS_QUEUED, DW_REASON_NONE, "");
        }
        return dw_submit_result_t::success();
    }

    dw_submit_result_t TaskManager::remote_resume(const TaskParams & /*params*/) {
        return dw_submit_result_t::failure(DW_REASON_ERROR, "远程任务暂不支持");
    }

    dw_submit_result_t TaskManager::remove(const TaskParams &params) const {
        log_i(params.natural_key.c_str(), "删除任务[{}]", to_json_string(params));
        if (config_.client_id == params.client_id) {
            return self_remove(params);
        } else {
            return remote_remove(params);
        }
    }

    dw_submit_result_t TaskManager::self_remove(const TaskParams &params) const {
        const int32_t delete_files = params.delete_files ? 1 : 0;
        if (IDownloadEngine *eng = engine_of(params.protocol)) {
            return eng->delete_task(params.natural_key, params.client_id, delete_files);
        }
        return dw_submit_result_t::failure(DW_REASON_ERROR, "引擎不可用");
    }

    dw_submit_result_t TaskManager::remote_remove(const TaskParams & /*params*/) const {
        return dw_submit_result_t::failure(DW_REASON_ERROR, "远程任务暂不支持");
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
        const auto it = tasks_.find(union_id_of(config_.client_id, proto, natural_key));
        if (it != tasks_.end()) {
            rec_ptr = &it->second;
        } else if (store_.find_file_record(config_.client_id, proto, natural_key, db_rec)) {
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
        boost::asio::post(event_ioc_, [this, ev = std::move(event)]() {
            consume_engine_event(ev);
        });
    }

    void TaskManager::consume_engine_event(const EngineEvent &event) {
        log_d("manager", "接受事件[{}]", to_string(event));
        const std::string &key = event.engine_key;
        if (key.empty()) {
            log_e("manager", "无效事件[{}]", to_string(event));
            return;
        }

        std::scoped_lock lock(mtx_);
        FileRecord *rec = load_task_record(config_.client_id, event.protocol, key);
        if (!rec) {
            log_e(key.c_str(), "任务不存在");
            return;
        }

        switch (event.type) {
            case EngineEventType::PARSED: {
                if (!rec->parsed) {
                    rec->root_name = event.root_name;
                    rec->original_root_name = event.original_name;
                    rec->save_path = event.save_path;
                    rec->parsed = true;
                    rec->ext = event.ext;
                    rec->file_type = event.is_dir;
                    rec->full_path = event.root_name.empty()
                                         ? event.save_path
                                         : (std::filesystem::path(event.save_path) / event.root_name).string();
                    rec->dirty = true;

                    log_i(key.c_str(), "解析完成[{}]", to_json_string(rec));
                } else {
                    // 已解析
                    log_i(key.c_str(), "已解析完成[{}]", to_json_string(rec));
                }
                if (IDownloadEngine *eng = engine_of(rec->task_protocol)) {
                    eng->resume_task(rec->task_natural_key, rec->client_id, rec->priority_file_indexes);
                }
                break;
            }
            case EngineEventType::DOWNLOAD_FAILED: {
                const bool retryable = (event.reason == DW_REASON_FAIL);
                rec->status = retryable ? DW_TASK_STATUS_FAIL : DW_TASK_STATUS_ERROR;
                rec->reason = event.reason;
                rec->message = event.message;
                rec->dirty = true;
                break;
            }
            case EngineEventType::STATUS_UPDATE: {
                rec->total_size = event.total_size;
                rec->total_done = event.total_done;
                rec->download_rate = event.download_rate;
                rec->upload_rate = event.upload_rate;
                rec->support_range = event.support_range;
                rec->status = event.status;
                rec->reason = event.reason;
                rec->message = event.message;
                if (!event.etag.empty()) rec->etag = event.etag;
                if (!event.last_modified.empty()) rec->last_modified = event.last_modified;
                rec->dirty = true;
                break;
            }
            case EngineEventType::DELETED: {
                const std::string save_path = rec->save_path;
                const std::string root_name = rec->root_name;
                const std::string original_root_name = rec->original_root_name;
                store_.remove(rec->client_id, rec->task_protocol, rec->task_natural_key);
                log_i(key.c_str(), "任务删除成功");
                if (event.delete_files) {
                    std::error_code ec;
                    if (!root_name.empty()) {
                        std::filesystem::remove_all(std::filesystem::path(save_path) / root_name, ec);
                    }
                    if (!original_root_name.empty()) {
                        std::filesystem::remove_all(std::filesystem::path(save_path) / original_root_name, ec);
                    }
                    log_i(key.c_str(), "文件删除成功");
                }
                rec->is_delete = true;
                rec->dirty = true;
                break;
            }
            case EngineEventType::FILE_PROGRESS: {
                // 序列化区间集合为 JSON：[[start1,end1],[start2,end2],...]
                boost::json::array segments_arr;
                for (const auto &[start, end]: event.segments) {
                    boost::json::array interval;
                    interval.push_back(start);
                    interval.push_back(end);
                    segments_arr.push_back(std::move(interval));
                }
                const std::string segments_json = boost::json::serialize(segments_arr);
                store_.save_file_progress(rec->client_id, rec->task_protocol, rec->task_natural_key, event.file_index,
                                          event.full_path, event.size, event.downloaded_bytes, segments_json);
                break;
            }
            case EngineEventType::FILE_COMPLETED: {
                store_.mark_file_complete(rec->client_id, rec->task_protocol,
                                          rec->task_natural_key, event.file_index);
                break;
            }
            case EngineEventType::RESUME_DATA: {
                if (!event.resume_data.empty()) {
                    store_.save_resume(rec->client_id, rec->task_protocol, rec->task_natural_key,
                                       event.resume_data.data(), event.resume_data.size());
                }
                break;
            }
            default:
                log_d(key.c_str(), "未处理的事件类型 type={}", to_string(event.type));
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
        const auto all_records = store_.load_file_records(config_.client_id);
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
        auto out = store_.load_file_records(config_.client_id);
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

    /* ================================================================== */
    /*                          调度线程                                  */
    /* ================================================================== */

    void TaskManager::scheduler_loop() {
        while (running_.load()) {
            std::vector<std::pair<dw_protocol_t, std::string> > to_pause;
            {
                std::scoped_lock lock(mtx_);
                // 清理已删除数据
                std::vector<std::string> to_remove;
                for (auto &[union_id, task_record]: tasks_) {
                    if (task_record.is_delete) {
                        to_remove.push_back(union_id);
                    }
                }
                for (const auto &uid: to_remove) {
                    unregister_task(uid);
                }
                if (net_allowed_) {
                    // 调度优先级 force > 优先级 > 时间
                    while (running_.load()) {
                        // 名额已满：force 任务让行（暂停最慢的活跃任务）
                        if (active_count_locked() >= config_.max_concurrent_downloads) {
                            const FileRecord *force_task = nullptr;
                            for (auto &tr: tasks_ | std::views::values) {
                                if (tr.status == DW_TASK_STATUS_QUEUED && tr.force) {
                                    force_task = &tr;
                                    break;
                                }
                            }
                            if (!force_task) break;

                            const FileRecord *slowest = nullptr;
                            for (auto &tr: tasks_ | std::views::values) {
                                if (tr.status != DW_TASK_STATUS_DOWNLOADING) continue;
                                if (!slowest || tr.download_rate < slowest->download_rate) {
                                    slowest = &tr;
                                }
                            }
                            if (!slowest) break;

                            log_i(slowest->task_natural_key.c_str(), "暂停最慢任务[{}]", to_json_string(slowest));
                            if (IDownloadEngine *eng = engine_of(slowest->task_protocol)) {
                                eng->pause_task(slowest->task_natural_key, config_.client_id);
                            }
                            continue;
                        }

                        // 选取最佳 QUEUED 任务
                        FileRecord *best = nullptr;
                        for (auto &task_record: tasks_ | std::views::values) {
                            if (task_record.status != DW_TASK_STATUS_QUEUED) continue;
                            if (!best ||
                                task_record.force > best->force ||
                                (task_record.force == best->force && task_record.priority > best->priority) ||
                                (task_record.force == best->force && task_record.priority == best->priority &&
                                 task_record.created_at < best->created_at)) {
                                best = &task_record;
                            }
                        }
                        if (!best) break;

                        if (IDownloadEngine *eng = engine_of(best->task_protocol)) {
                            eng->resume_task(best->task_natural_key, config_.client_id, best->priority_file_indexes);
                            best->force = false;
                        }
                    }
                } else {
                    // 禁用流量
                    for (auto &task_record: tasks_ | std::views::values) {
                        if (status_occupies_slot(static_cast<dw_task_status_t>(task_record.status))) {
                            to_pause.emplace_back(task_record.task_protocol, task_record.task_natural_key);
                        }
                    }
                }
            }
            // 锁外暂停任务
            for (const auto &[proto, key]: to_pause) {
                if (IDownloadEngine *eng = engine_of(proto)) {
                    eng->pause_task(key, config_.client_id);
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
    }

    void TaskManager::maintenance_loop() {
        while (running_.load()) {
            std::vector<FileRecord> dirty_records;
            {
                std::scoped_lock lock(mtx_);
                for (auto &task_record: tasks_ | std::views::values) {
                    if (task_record.dirty) {
                        dirty_records.push_back(task_record);
                        // 保存 DB
                        store_.update_file_record(task_record);
                        task_record.dirty = false;
                    }
                }
            }

            for (const auto &rec: dirty_records) {
                // 回调
                const std::string json = boost::json::serialize(to_json(rec));
                emit_progress(json.c_str());
            }

            // 引擎清理
            if (http_) http_->sweep();
            if (torrent_) torrent_->sweep();

            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
    }

    void TaskManager::event_consumer_loop() {
        event_ioc_.run();
    }

    void TaskManager::reset_live_telemetry(FileRecord &rec) {
        rec.download_rate = 0.0;
        rec.upload_rate = 0.0;
        rec.reason = DW_REASON_NONE;
        rec.message.clear();
    }


    // ---- 任务文件实时查询 ----

    dw_submit_result_t TaskManager::load_files(dw_protocol_t proto, const std::string &natural_key) {
        // task_files 表已移除（磁盘为事实源）：BT 经 handle 实时查询，HTTP 从任务记录推导。
        std::scoped_lock lock(mtx_);
        if (proto == DW_PROTOCOL_TORRENT) {
            if (torrent_) {
                return torrent_->get_file_list(config_.client_id, natural_key);
            }
            return dw_submit_result_t::failure(DW_REASON_ERROR, "BT 引擎不可用");
        }
        // HTTP 单文件：root_name（wrapper 目录）+ original_root_name（原始文件名）落定后可以推导。
        const FileRecord *rec_ptr = nullptr;
        FileRecord db_rec;
        const auto it = tasks_.find(union_id_of(config_.client_id, proto, natural_key));
        if (it != tasks_.end()) {
            rec_ptr = &it->second;
        } else if (store_.find_file_record(config_.client_id, proto, natural_key, db_rec)) {
            rec_ptr = &db_rec;
        }
        if (!rec_ptr || rec_ptr->root_name.empty() || rec_ptr->original_root_name.empty()) {
            return dw_submit_result_t::failure(DW_REASON_ERROR, "定名未落定");
        }
        auto result = dw_submit_result_t::success();
        FileInfo fi;
        fi.index = 0;
        fi.name = rec_ptr->original_root_name;
        fi.full_path = (std::filesystem::path(rec_ptr->save_path) / rec_ptr->root_name / rec_ptr->original_root_name).
                string();
        fi.ext = utils::file_extension(rec_ptr->original_root_name);
        fi.size = rec_ptr->total_size;
        fi.offset = 0; // HTTP 单文件模型，全局偏移恒为 0
        fi.status = rec_ptr->status == DW_TASK_STATUS_COMPLETED ? 2 : 0;
        fi.selected = true;
        // downloaded_bytes 回填
        fi.downloaded_bytes = store_.sum_file_progress_by_file(config_.client_id, proto, natural_key, 0);
        result.files.push_back(std::move(fi));
        return result;
    }

    std::vector<FileInfo> TaskManager::get_files(const std::string &dir_path) {
        std::vector<FileInfo> result;
        try {
            if (!std::filesystem::exists(dir_path) || !std::filesystem::is_directory(dir_path)) {
                return result;
            }
            for (const auto &entry: std::filesystem::directory_iterator(dir_path)) {
                FileInfo fi;
                fi.index = 0;
                fi.full_path = entry.path().string();
                fi.name = entry.path().filename().string();
                if (entry.is_directory()) {
                    fi.is_dir = true;
                    fi.size = 0;
                } else if (entry.is_regular_file()) {
                    fi.size = static_cast<int64_t>(entry.file_size());
                    fi.ext = utils::file_extension(fi.name);
                } else {
                    continue; // 跳过其他类型
                }
                result.push_back(std::move(fi));
            }
        } catch (...) {
            // 目录访问失败，返回空列表
        }
        fill_file_progress(result);
        return result;
    }

    dw_submit_result_t TaskManager::get_task_files(const std::string &client_id, dw_protocol_t proto,
                                                   const std::string &natural_key) {
        auto result = dw_submit_result_t::success();
        std::scoped_lock lock(mtx_);
        if (proto == DW_PROTOCOL_TORRENT && torrent_) {
            auto r = torrent_->get_file_list(client_id, natural_key);
            result.files = std::move(r.files);
        } else {
            // HTTP 单文件：从任务记录推导
            const FileRecord *rec_ptr = nullptr;
            FileRecord db_rec;
            const auto it = tasks_.find(union_id_of(client_id, proto, natural_key));
            if (it != tasks_.end()) {
                rec_ptr = &it->second;
            } else if (store_.find_file_record(client_id, proto, natural_key, db_rec)) {
                rec_ptr = &db_rec;
            }
            if (rec_ptr && !rec_ptr->root_name.empty()) {
                FileInfo fi;
                fi.index = 0;
                fi.name = rec_ptr->original_root_name;
                fi.full_path = (std::filesystem::path(rec_ptr->save_path) / rec_ptr->root_name / rec_ptr->original_root_name).
                        string();
                fi.ext = utils::file_extension(rec_ptr->original_root_name);
                fi.size = rec_ptr->total_size;
                fi.status = rec_ptr->status == DW_TASK_STATUS_COMPLETED ? 2 : 0;
                result.files.push_back(std::move(fi));
            }
        }
        // 填充下载进度与区间
        fill_file_progress(result.files);
        return result;
    }

    void TaskManager::fill_file_progress(std::vector<FileInfo> &files) const {
        if (files.empty()) return;
        // 收集所有 full_path，批量查询进度
        std::vector<std::string> paths;
        for (const auto &fi: files) {
            if (!fi.full_path.empty()) {
                paths.push_back(fi.full_path);
            }
        }
        const auto progress_map = store_.load_file_progress_by_paths(paths);
        for (auto &fi: files) {
            auto it = progress_map.find(fi.full_path);
            if (it == progress_map.end()) continue;
            fi.downloaded_bytes = it->second.first;
            fi.segments = it->second.second;
        }
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
        const auto existing_files = store_.load_file_records_by_save_path(config_.client_id, save_path);
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
            fr.client_id = config_.client_id;
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
        const auto files = store_.load_file_records_by_save_path(config_.client_id, save_path);
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
        const auto files = store_.load_file_records_by_save_path(config_.client_id, save_path);
        std::error_code ec;
        for (const auto &f: files) {
            if (f.type != DW_SOURCE_LOCAL_FILE) continue;
            // 删除物理文件/目录：save_path / root_name
            const std::filesystem::path full_path = std::filesystem::path(f.save_path) / f.root_name;
            std::filesystem::remove_all(full_path, ec);
        }
        // 批量删除 file_records 中的本地文件条目
        store_.clear_local_tasks(config_.client_id, save_path);
        return 0;
    }

    int32_t TaskManager::delete_local_entry(const std::string &save_path, const std::string &root_name) {
        std::scoped_lock lock(mtx_);

        if (save_path.empty() || root_name.empty()) return -1;

        // 类型门禁：仅允许删除本地文件条目。HTTP / BT 任务必须走 dw_delete_task，
        // 否则会越过 engine 层删文件，留下引擎内仍在运行的孤儿任务。
        const auto records = store_.load_file_records_by_save_path(config_.client_id, save_path);
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
        store_.delete_file_record_by_name(config_.client_id, save_path, root_name);
        return ec ? -1 : 0;
    }

    dw_submit_result_t TaskManager::parse_magnet(const std::string &magnet_link) {
        return TorrentEngine::parse_magnet(magnet_link);
    }

    dw_submit_result_t TaskManager::parse_torrent_file(const std::string &torrent_file_path) {
        return TorrentEngine::parse_torrent_file(torrent_file_path);
    }

    IDownloadEngine *TaskManager::engine_of(const dw_protocol_t proto) const {
        return proto == DW_PROTOCOL_HTTP ? http_ : torrent_;
    }

    int32_t TaskManager::active_count_locked() const {
        int32_t n = 0;
        for (const auto &task_record: tasks_ | std::views::values) {
            if (status_occupies_slot(static_cast<dw_task_status_t>(task_record.status))) ++n;
        }
        return n;
    }

    void TaskManager::flush_dirty_locked() {
        for (auto &[_, task_record]: tasks_) {
            if (!task_record.dirty) continue;
            // 数据已变更：全量刷盘（元数据 + 状态 + 进度）
            store_.update_file_record_meta(task_record.client_id, task_record.task_protocol,
                                           task_record.task_natural_key,
                                           task_record.save_path, task_record.original_root_name,
                                           task_record.root_name, task_record.full_path,
                                           task_record.file_type, task_record.ext);
            store_.sync_file_record_progress(task_record.task_protocol, task_record.task_natural_key,
                                             task_record.status, task_record.total_size,
                                             task_record.total_done, task_record.reason,
                                             task_record.message);
            task_record.dirty = false;
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
