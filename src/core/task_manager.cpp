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

    dw_submit_result_t TaskManager::apply_config(const Config &cfg) {
        if (cfg.client_id.empty()) {
            return dw_submit_result_t::failure(DW_REASON_INVALID_INPUT, "client_id 为空");
        }
        config_ = cfg;
        if (config_.max_concurrent_downloads <= 0) config_.max_concurrent_downloads = 3;
        net_allowed_ = !(config_.network_type == 2 && !config_.allow_mobile_data);
        log_i("manager", "应用配置: {}", to_json_string(config_));
        return dw_submit_result_t::success();
    }

    int32_t TaskManager::start() {
        const std::string dir = !config_.work_dir.empty() ? config_.work_dir : ".";
        const std::string story_path = dir + "/leopard_tasks.db";

        std::scoped_lock lock(mtx_);
        if (!store_.open(story_path)) {
            log_e("manager", "下载器数据加载失败: {}", story_path);
            return -1;
        }
        store_.init_schema();
        {
            // 加载进行中的任务（调度器需要感知的活跃任务）
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
                // 文件选择持久化：BT 任务以 default_dont_download 添加（初始全部不下载），
                // 调度派发依赖此字段定型下载范围；空列表语义为全部下载。
                task_record.priority_file_indexes = params.file_indexes;
                store_.insert_file_record(task_record);
                // 新任务必须同步注册进内存调度表：scheduler_loop 只遍历 tasks_，
                // 漏注册会导致任务对调度器不可见（永远 QUEUED），仅重启后经 start() 加载才恢复。
                register_task(std::move(task_record));
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
            store_.update_file_record(*rec);
        }
        return dw_submit_result_t::success();
    }

    dw_submit_result_t TaskManager::remote_resume(const TaskParams & /*params*/) {
        return dw_submit_result_t::failure(DW_REASON_ERROR, "远程任务暂不支持");
    }

    dw_submit_result_t TaskManager::remove(const TaskParams &params) {
        log_i(params.natural_key.c_str(), "删除任务[{}]", to_json_string(params));
        if (config_.client_id == params.client_id) {
            return self_remove(params);
        } else {
            return remote_remove(params);
        }
    }

    dw_submit_result_t TaskManager::self_remove(const TaskParams &params) {
        if (params.protocol == DW_PROTOCOL_LOCAL) {
            FileRecord *tr = load_task_record(params.client_id, DW_PROTOCOL_LOCAL, params.natural_key);
            if (!tr) return dw_submit_result_t::failure(DW_REASON_ERROR, "任务不存在");
            tr->is_delete = true;
            tr->dirty = true;
            return dw_submit_result_t::success();
        }
        const int32_t delete_files = params.delete_files ? 1 : 0;
        if (IDownloadEngine *eng = engine_of(params.protocol)) {
            return eng->delete_task(params.natural_key, params.client_id, delete_files);
        }
        return dw_submit_result_t::failure(DW_REASON_ERROR, "引擎不可用");
    }

    dw_submit_result_t TaskManager::remote_remove(const TaskParams & /*params*/) {
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

    std::vector<FileProgressInfo> TaskManager::get_file_progress(const std::string &client_id,
                                                                 const dw_protocol_t proto,
                                                                 const std::string &natural_key) {
        std::scoped_lock lock(mtx_);
        return store_.load_file_progress(client_id, proto, natural_key);
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
                    const std::string &effective_name = event.root_name.empty() ? event.original_name : event.root_name;
                    rec->full_path = effective_name.empty()
                                         ? event.save_path
                                         : (std::filesystem::path(event.save_path) / effective_name).string();
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

    std::vector<FileRecord> TaskManager::list() {
        std::scoped_lock lock(mtx_);
        return store_.load_file_records();
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
                std::vector<std::string> to_remove;
                for (auto &[union_id, task_record]: tasks_) {
                    if (task_record.is_delete && !task_record.dirty) {
                        // 任务被删除或者文件不存在了
                        unregister_task(union_id);
                    }
                }
                if (net_allowed_) {
                    // 调度优先级 force > 优先级 > 时间
                    while (running_.load()) {
                        // 名额已满：force 任务让行（暂停最慢的活跃任务）
                        if (active_count_locked() >= config_.max_concurrent_downloads) {
                            const FileRecord *force_task = nullptr;
                            for (auto &tr: tasks_ | std::views::values) {
                                if (tr.status == DW_TASK_STATUS_QUEUED && tr.force && !tr.is_delete) {
                                    force_task = &tr;
                                    break;
                                }
                            }
                            if (!force_task) break;

                            const FileRecord *slowest = nullptr;
                            for (auto &tr: tasks_ | std::views::values) {
                                if (tr.status != DW_TASK_STATUS_DOWNLOADING || tr.is_delete) continue;
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
                            if (task_record.status != DW_TASK_STATUS_QUEUED || task_record.is_delete) continue;
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
                            // 派发即占用调度名额：resume_task 的状态回写经引擎事件异步完成，
                            // 事件消费线程与本线程争用 mtx_。若此处不先翻离 QUEUED，
                            // 内层循环会在持锁状态下对同一任务每秒重复派发上万次，
                            // 同时饿死事件线程（状态永远无法翻转）与 dw_list_tasks（死锁）。
                            // 后续准确状态仍由 STATUS_UPDATE 事件覆盖。
                            best->status = DW_TASK_STATUS_DOWNLOADING;
                            best->dirty = true;
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
                // 收集脏记录快照（锁外回调用）
                for (auto &task_record: tasks_ | std::views::values) {
                    if (task_record.dirty) {
                        dirty_records.push_back(task_record);
                        store_.update_file_record(task_record);
                    }
                }
            }

            for (const auto &rec: dirty_records) {
                // 回调
                const std::string json = boost::json::serialize(to_json(rec));
                emit_progress(json.c_str());
            }

            // 回调完成后清除 dirty 标志
            if (!dirty_records.empty()) {
                std::scoped_lock lock(mtx_);
                for (const auto &rec: dirty_records) {
                    if (auto it = tasks_.find(rec.union_id()); it != tasks_.end()) {
                        it->second.dirty = false;
                    }
                }
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


    std::vector<FileInfo> TaskManager::get_files(const std::string &dir_path) const {
        std::vector<FileInfo> result;
        try {
            if (!std::filesystem::exists(dir_path) || !std::filesystem::is_directory(dir_path)) {
                return result;
            }
            for (const auto &entry: std::filesystem::directory_iterator(dir_path)) {
                const auto name = entry.path().filename().string();
                // 跳过隐藏文件/目录
                if (!name.empty() && name[0] == '.') continue;

                FileInfo fi;
                fi.index = 0;
                fi.full_path = entry.path().string();
                fi.name = name;
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

    // ---- 本地文件浏览与管理 ----

    dw_submit_result_t TaskManager::scan_local_file(const std::vector<std::string> &catalog_paths) {
        if (catalog_paths.empty()) {
            return dw_submit_result_t::failure(DW_REASON_INVALID_INPUT, "目录路径集合为空");
        }

        // 排除不存在路径
        std::error_code ec;
        std::vector<std::string> valid_input_paths = catalog_paths;
        std::erase_if(valid_input_paths, [&ec](const std::string &path) {
            return !std::filesystem::is_directory(path, ec) || ec;
        });

        // 排除重合路径
        std::ranges::sort(valid_input_paths);
        std::vector<std::string> deduped_paths;
        for (const auto &path: valid_input_paths) {
            if (deduped_paths.empty() ||
                (path != deduped_paths.back() && !path.starts_with(deduped_paths.back() + "/"))) {
                deduped_paths.push_back(path);
            }
        }

        // 构建 save_path 集合用于快速查找（为空时所有本地文件将被标记删除）
        std::unordered_set<std::string> valid_paths(deduped_paths.begin(), deduped_paths.end());

        std::scoped_lock lock(mtx_);

        // 从 DB 获取全量记录，逐条校验并标记
        const auto all_records = store_.load_file_records();
        std::unordered_set<std::string> existing_paths;
        for (const auto &f: all_records) {
            if (!f.full_path.empty()) {
                existing_paths.insert(f.full_path);
            }
            // 加载到内存后进行标记
            FileRecord *tr = load_task_record(f.client_id, f.task_protocol, f.task_natural_key);
            if (!tr || tr->is_delete || tr->full_path.empty()) continue;

            if (!std::filesystem::exists(tr->full_path, ec)) {
                // 文件不存在
                if (tr->type == DW_SOURCE_LOCAL_FILE) {
                    // 标记待清理
                    tr->is_delete = true;
                    tr->dirty = true;
                } else {
                    // 修改为失败 可重试
                    tr->status = DW_TASK_STATUS_ERROR;
                    tr->reason = DW_REASON_FAIL;
                    tr->message = "文件不存在";
                    tr->dirty = true;
                }
            } else if (!valid_paths.contains(tr->save_path) && tr->type == DW_SOURCE_LOCAL_FILE) {
                // 文件存在但 save_path 不在传入集合中：标记待清理
                tr->is_delete = true;
                tr->dirty = true;
            }
        }

        // 新增本地文件
        int32_t added = 0;
        for (const auto &catalog_path: deduped_paths) {
            for (auto it = std::filesystem::directory_iterator(catalog_path, ec);
                 it != std::filesystem::directory_iterator(); ++it) {
                const auto entry_name = it->path().filename().string();
                // 跳过隐藏文件/目录
                if (!entry_name.empty() && entry_name[0] == '.') continue;

                const std::string full_path = (std::filesystem::path(catalog_path) / entry_name).string();
                // 已存在
                if (existing_paths.contains(full_path)) continue;

                FileRecord fr;
                fr.client_id = config_.client_id;
                fr.type = DW_SOURCE_LOCAL_FILE;
                fr.save_path = catalog_path;
                fr.original_root_name = entry_name;
                fr.task_natural_key = entry_name;
                fr.full_path = full_path;
                fr.created_at = now_unix_ms();
                fr.dirty = true;
                if (it->is_directory(ec)) {
                    fr.file_type = true;
                    fr.total_size = 0;
                } else {
                    fr.file_type = false;
                    fr.total_size = static_cast<int64_t>(it->file_size(ec));
                    fr.total_done = fr.total_size;
                    if (const auto ext_pos = entry_name.rfind('.');
                        ext_pos != std::string::npos && ext_pos + 1 < entry_name.size()) {
                        fr.ext = entry_name.substr(ext_pos + 1);
                    }
                }
                store_.insert_file_record(fr);
                register_task(std::move(fr));
                ++added;
            }
        }
        auto result = dw_submit_result_t::success();
        result.affected_count = added;
        return result;
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
            if (task_record.is_delete) continue;
            if (status_occupies_slot(static_cast<dw_task_status_t>(task_record.status))) ++n;
        }
        return n;
    }

    void TaskManager::register_task(FileRecord task_record) {
        // tasks_ 以 union_id 为 key，无冗余索引。
        tasks_[task_record.union_id()] = std::move(task_record);
    }

    void TaskManager::unregister_task(const std::string &union_id) {
        const auto it = tasks_.find(union_id);
        if (it == tasks_.end()) return;

        const FileRecord &rec = it->second;
        // 删除 DB 记录
        if (!rec.full_path.empty()) {
            store_.delete_file_record_by_path(rec.full_path);
        }
        // 删除进度缓存（下载任务）
        if (rec.task_protocol != DW_PROTOCOL_LOCAL && !rec.task_natural_key.empty()) {
            store_.delete_file_progress_by_file(config_.client_id, rec.task_protocol,
                                                rec.task_natural_key, 0);
        }
        tasks_.erase(it);
    }
} // namespace dw
