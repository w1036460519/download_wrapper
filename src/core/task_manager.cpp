/**
 * @file task_manager.cpp
 * @brief 库内任务中枢实现：SQLite 持久化 + 优先级就绪队列 + 周期准入调度。
 *
 * 并发模型（双线程职责分离）：
 *   - mtx_ 保护注册表 tasks_ 与 DB（sqlite3 串行化模式，读写均在持锁期间）；
 *   - A 线程（scheduler_loop，快节拍）：任务调度准入，检查额度并准入 QUEUED 任务；
 *   - B 线程（maintenance_loop，慢节拍）：持久化落库 + 进度推送 + 引擎 sweep；
 *   - 调度线程持锁派发恢复，进度回调在释放 mtx_ 后执行。
 *
 * 任务主键模型：
 *   - tasks_ 以 Task::task_id 为键，HTTP 使用 URL，BT 使用 info_hash。
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
#include <ranges>
#include <unordered_set>
#include <utility>

namespace dw {
    using utils::now_unix_ms;

    namespace {
        bool is_download_task(const Task &task) {
            return task.task_protocol == DW_PROTOCOL_HTTP || task.task_protocol == DW_PROTOCOL_TORRENT;
        }

        /// 占用下载额度的状态集合：DOWNLOADING + RESOLVING。
        /// RESOLVING 正在下载元数据（BT 磁力链），占用网络带宽；
        /// QUEUED 为纯等待态，不占名额；暂停/错误/完成自动释放额度。
        bool status_occupies_slot(int32_t s) {
            return s == DW_TASK_STATUS_DOWNLOADING || s == DW_TASK_STATUS_RESOLVING;
        }
    } // namespace

    TaskManager::~TaskManager() {
        stop();
        database_.close();
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
            log_e("manager", "客户端标识为空");
            return dw_submit_result_t::failure();
        }
        std::scoped_lock lock(mtx_);
        config_ = cfg;
        if (config_.max_concurrent_downloads < 0) config_.max_concurrent_downloads = 3;
        net_allowed_ = !(config_.network_type == 2 && !config_.allow_mobile_data);
        log_i("manager", "应用配置[{}]", to_json_string(config_));
        return dw_submit_result_t::success();
    }

    int32_t TaskManager::start() {
        const std::string dir = !config_.work_dir.empty() ? config_.work_dir : ".";
        const std::string story_path = dir + "/leopard_tasks.db";

        std::scoped_lock lock(mtx_);
        if (!database_.open(story_path)) {
            log_e("manager", "数据仓初始化失败[{}]", story_path);
            return -1;
        }
        if (!store_.init_schema() || !task_store_.init_schema() || !task_file_store_.init_schema() ||
            !task_file_store_.init_task_triggers() || !bt_resume_store_.init_schema()) {
            log_e("manager", "数据任务初始化失败");
            database_.close();
            return -1;
        }
        // 上次运行的活跃任务还没有运行时句柄，重新排队等待准入。
        const std::vector<dw_task_status_t> active_statuses = {
            DW_TASK_STATUS_RESOLVING,
            DW_TASK_STATUS_QUEUED,
            DW_TASK_STATUS_DOWNLOADING
        };
        for (auto &task: task_store_.find_by_status(active_statuses)) {
            if (!is_download_task(task)) continue;
            if (task.status != DW_TASK_STATUS_QUEUED) {
                task.status = DW_TASK_STATUS_QUEUED;
                task.modified_at = now_unix_ms();
                task.dirty = true;
            }
            const auto key = task.task_id;
            task.files = task_file_store_.get(key);
            tasks_.insert_or_assign(key, std::move(task));
        }

        running_.store(true);
        worker_ = std::thread([this] { scheduler_loop(); });
        maintenance_ = std::thread([this] { maintenance_loop(); });

        log_i("manager", "下载器[{}]启动成功", config_.client_id);
        return 0;
    }

    void TaskManager::stop(const bool close_database) {
        if (!running_.exchange(false)) {
            return;
        }
        if (worker_.joinable()) {
            worker_.join();
        }
        if (maintenance_.joinable()) {
            maintenance_.join();
        }

        std::scoped_lock lock(mtx_);
        if (close_database) database_.close();
        log_i("manager", "下载器已停止");
    }

    /* ================================================================== */
    /*                          控制操作                                  */
    /* ================================================================== */

    dw_submit_result_t TaskManager::add(TaskParams &params) const {
        log_i("manager", "添加任务[{}]", to_json_string(params));
        const dw_protocol_t protocol = params.protocol;
        if (protocol == DW_PROTOCOL_HTTP && params.url.empty()) {
            log_e("manager", "添加任务失败 链接不能为空");
            return dw_submit_result_t::failure();
        }
        if (protocol == DW_PROTOCOL_TORRENT && (params.magnet_link.empty() || params.torrent_file.empty())) {
            log_e("manager", "添加任务失败 链接/文件不能为空");
            return dw_submit_result_t::failure();
        }
        if (params.client_id.empty()) {
            // 使用配置中的路径
            params.save_path = config_.save_path;
            IDownloadEngine *eng = engine_of(protocol);
            return eng->add_task(&params);;
        }
        log_e("manager", "添加任务 暂不支持");
        return dw_submit_result_t::failure();
    }

    dw_submit_result_t TaskManager::pause(const TaskParams &params) const {
        log_i("manager", "暂停任务[{}]", to_json_string(params));
        if (params.client_id.empty()) {
            IDownloadEngine *eng = engine_of(params.protocol);
            return eng->pause_task(&params);
        }
        log_e("manager", "暂停任务 暂不支持");
        return dw_submit_result_t::failure();
    }


    dw_submit_result_t TaskManager::resume(const TaskParams &params) const {
        log_i("manager", "恢复任务[{}]", to_json_string(params));
        if (params.client_id.empty()) {
            IDownloadEngine *engine = engine_of(params.protocol);
            return engine->resume_task(&params);
        }
        log_e("manager", "恢复任务 暂不支持");
        return dw_submit_result_t::failure();
    }

    dw_submit_result_t TaskManager::remove(const TaskParams &params) const {
        log_i("manager", "删除任务[{}]", to_json_string(params));
        if (params.client_id.empty()) {
            IDownloadEngine *engine = engine_of(params.protocol);
            return engine->delete_task(&params);
        }
        log_e("manager", "恢复任务 暂不支持");
        return dw_submit_result_t::failure();
    }

    Task *TaskManager::get_engine_task(const std::string &key) {
        std::scoped_lock lock(mtx_);
        if (const auto it = tasks_.find(key); it != tasks_.end()) return &it->second;

        Task task;
        if (!task_store_.find_by_task_id(key, task)) {
            return nullptr;
        }
        task.files = task_file_store_.get(key);
        return &tasks_.try_emplace(key, std::move(task)).first->second;
    }

    /* ================================================================== */
    /*                          快照查询                                  */
    /* ================================================================== */

    std::vector<Task> TaskManager::list() {
        std::scoped_lock lock(mtx_);
        auto tasks = task_store_.find_all();
        for (auto &task : tasks) {
            task.files = task_file_store_.get(task.task_id);
        }
        return tasks;
    }

    /* ================================================================== */
    /*                          调度线程                                  */
    /* ================================================================== */

    void TaskManager::scheduler_loop() {
        const auto task_params = [this](const Task &task) {
            TaskParams params;
            params.protocol = task.task_protocol;
            params.client_id = task.client_id.empty() ? config_.client_id : task.client_id;
            params.natural_key = task.task_id;
            return params;
        };
        while (running_.load()) {
            std::vector<TaskParams> to_pause;
            {
                std::scoped_lock lock(mtx_);
                std::erase_if(tasks_, [](const auto &entry) {
                    const auto &task = entry.second;
                    return task.is_delete && !task.dirty &&
                           (task.task_protocol != DW_PROTOCOL_TORRENT ||
                            (task.need_delete_session && task.need_delete_file));
                });

                if (net_allowed_) {
                    while (running_.load() && config_.max_concurrent_downloads > 0) {
                        // 强制任务优先，再按任务优先级和创建时间排序。
                        Task *next = nullptr;
                        for (auto &task: tasks_ | std::views::values) {
                            if (task.is_delete || task.status != DW_TASK_STATUS_QUEUED ||
                                !is_download_task(task) || !engine_of(task.task_protocol)) continue;
                            if (!next ||
                                task.force > next->force ||
                                (task.force == next->force && task.priority > next->priority) ||
                                (task.force == next->force && task.priority == next->priority &&
                                 task.created_at < next->created_at)) {
                                next = &task;
                            }
                        }
                        if (!next) break;

                        if (active_count_locked() >= config_.max_concurrent_downloads) {
                            if (!next->force) break;
                            Task *slowest = nullptr;
                            for (auto &task: tasks_ | std::views::values) {
                                if (task.is_delete || task.status != DW_TASK_STATUS_DOWNLOADING ||
                                    !is_download_task(task) || !engine_of(task.task_protocol)) continue;
                                if (!slowest || task.download_rate < slowest->download_rate) {
                                    slowest = &task;
                                }
                            }
                            if (!slowest) break;

                            log_i(slowest->task_id.c_str(), "暂停最慢任务");
                            const auto params = task_params(*slowest);
                            if (engine_of(params.protocol)->pause_task(&params).code != DW_REASON_NONE) {
                                break;
                            }
                            slowest->status = DW_TASK_STATUS_PAUSED;
                            slowest->download_rate = 0.0;
                            slowest->upload_rate = 0.0;
                            slowest->modified_at = now_unix_ms();
                            slowest->dirty = true;
                            continue;
                        }

                        auto params = task_params(*next);
                        params.file_priorities = next->priority_file_indexes;
                        log_i(next->task_id.c_str(), "调度派发[proto={}]", static_cast<int>(params.protocol));
                        const auto result = engine_of(params.protocol)->do_resume(&params);
                        if (result.code == DW_REASON_FAIL) break;

                        next->force = false;
                        next->status = result.code == DW_REASON_NONE
                                           ? DW_TASK_STATUS_DOWNLOADING
                                           : DW_TASK_STATUS_ERROR;
                        next->reason = result.code;
                        next->message = result.message;
                        next->modified_at = now_unix_ms();
                        next->dirty = true;
                    }
                } else {
                    for (const auto &task: tasks_ | std::views::values) {
                        if (!task.is_delete && is_download_task(task) && status_occupies_slot(task.status)) {
                            to_pause.push_back(task_params(task));
                        }
                    }
                }
            }

            for (const auto &params: to_pause) {
                if (IDownloadEngine *engine = engine_of(params.protocol)) {
                    engine->pause_task(&params);
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(flush_interval_ms_));
        }
    }

    void TaskManager::maintenance_loop() {
        while (running_.load()) {
            std::vector<Task> dirty_records;
            {
                std::scoped_lock lock(mtx_);
                // 持锁保存并收集快照，回调期间发生的新变更留给下一轮。
                for (auto &task_record: tasks_ | std::views::values) {
                    if (task_record.dirty) {
                        if (task_record.is_delete) {
                            if (task_record.task_protocol == DW_PROTOCOL_LOCAL) {
                                if (!task_record.full_path.empty())
                                    store_.delete_file_record_by_path(
                                        task_record.full_path);
                            } else {
                                store_.remove(task_record.client_id, task_record.task_protocol,
                                              task_record.task_id);
                            }
                        } else {
                            store_.update_file_record(task_record);
                            if (task_record.task_protocol != DW_PROTOCOL_LOCAL) {
                                task_store_.update_read(task_record.task_id, task_record.is_read);
                            }
                        }
                        dirty_records.push_back(task_record);
                        task_record.dirty = false;
                    }
                }
            }

            for (const auto &rec: dirty_records) {
                // 回调
                auto payload = to_json(rec);
                payload["task_natural_key"] = rec.task_id;
                const auto source = rec.task_protocol == DW_PROTOCOL_TORRENT
                                        ? DW_SOURCE_REMOTE_FILE
                                        : (rec.task_protocol == DW_PROTOCOL_HTTP
                                               ? DW_SOURCE_TASK_FILE
                                               : DW_SOURCE_LOCAL_FILE);
                payload["type"] = static_cast<int>(source);
                const std::string json = boost::json::serialize(payload);
                emit_progress(json.c_str());
            }

            // 引擎清理
            if (http_) http_->sweep();
            if (torrent_) torrent_->sweep();

            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
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
            if (task_record.is_delete || !is_download_task(task_record)) continue;
            if (status_occupies_slot(task_record.status)) ++n;
        }
        return n;
    }

    void TaskManager::remove_data_directory(const std::string &save_path,
                                            const std::string &root_name,
                                            const std::string &original_root_name) {
        std::error_code ec;
        const std::filesystem::path base = std::filesystem::path(save_path).lexically_normal();
        for (const std::string &name: {root_name, original_root_name}) {
            if (name.empty()) continue;
            const std::filesystem::path target = (base / name).lexically_normal();
            if (const std::filesystem::path rel = target.lexically_relative(base);
                rel.empty() || std::filesystem::path("..") == *rel.begin())
                continue;
            std::filesystem::remove_all(target, ec);
        }
    }
} // namespace dw
