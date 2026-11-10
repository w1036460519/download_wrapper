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
#include <cstddef>
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
            !bt_resume_store_.init_schema()) {
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
        if (protocol == DW_PROTOCOL_TORRENT && (params.magnet_link.empty() && params.torrent_file.empty())) {
            log_e("manager", "添加任务失败 链接/文件不能为空");
            return dw_submit_result_t::failure();
        }
        if (params.client_id.empty()) {
            // 使用配置中的路径
            params.save_path = config_.save_path;
            IDownloadEngine *eng = engine_of(protocol);
            return eng ? eng->add_task(&params) : dw_submit_result_t::failure();
        }
        log_e("manager", "添加任务 暂不支持");
        return dw_submit_result_t::failure();
    }

    dw_submit_result_t TaskManager::pause(const TaskParams &params) const {
        log_i("manager", "暂停任务[{}]", to_json_string(params));
        if (params.client_id.empty()) {
            IDownloadEngine *eng = engine_of(params.protocol);
            return eng ? eng->pause_task(&params) : dw_submit_result_t::failure();
        }
        log_e("manager", "暂停任务 暂不支持");
        return dw_submit_result_t::failure();
    }


    dw_submit_result_t TaskManager::resume(const TaskParams &params) const {
        log_i("manager", "恢复任务[{}]", to_json_string(params));
        if (params.client_id.empty()) {
            IDownloadEngine *engine = engine_of(params.protocol);
            return engine ? engine->resume_task(&params) : dw_submit_result_t::failure();
        }
        log_e("manager", "恢复任务 暂不支持");
        return dw_submit_result_t::failure();
    }

    dw_submit_result_t TaskManager::remove(const TaskParams &params) const {
        log_i("manager", "删除任务[{}]", to_json_string(params));
        if (params.client_id.empty()) {
            IDownloadEngine *engine = engine_of(params.protocol);
            return engine ? engine->delete_task(&params) : dw_submit_result_t::failure();
        }
        log_e("manager", "恢复任务 暂不支持");
        return dw_submit_result_t::failure();
    }

    Task *TaskManager::get_engine_task(const std::string &key, bool include_deleted) {
        std::scoped_lock lock(mtx_);
        if (const auto it = tasks_.find(key); it != tasks_.end())
            return !include_deleted && it->second.is_delete ? nullptr : &it->second;

        Task task;
        if (!task_store_.find_by_task_id(key, task, include_deleted)) {
            return nullptr;
        }
        task.files = task_file_store_.get(key);
        return &tasks_.try_emplace(key, std::move(task)).first->second;
    }

    bool TaskManager::replace_engine_task(Task task, bool cache_task) {
        std::scoped_lock lock(mtx_);
        const auto key = task.task_id;
        if (const auto it = tasks_.find(key); it != tasks_.end() && it->second.is_delete)
            return false; // 等待维护线程保存并取走删除通知。
        if (!task_store_.replace_task(task)) return false;
        if (cache_task) {
            task.dirty = true;
            tasks_.insert_or_assign(key, std::move(task));
        }
        return true;
    }

    /* ================================================================== */
    /*                          快照查询                                  */
    /* ================================================================== */

    std::vector<Task> TaskManager::list() {
        std::scoped_lock lock(mtx_);
        auto tasks = task_store_.find_all();
        std::erase_if(tasks, [this](const Task &task) {
            const auto it = tasks_.find(task.task_id);
            return it != tasks_.end() && it->second.is_delete;
        });
        for (auto &task: tasks) {
            if (const auto it = tasks_.find(task.task_id); it != tasks_.end()) {
                const auto &live = it->second;
                task.select_file_indexes = live.select_file_indexes;
                task.priority_file_indexes = live.priority_file_indexes;
                task.support_range = live.support_range;
                task.etag = live.etag;
                task.last_modified = live.last_modified;
                task.force = live.force;
                task.download_rate = live.download_rate;
                task.upload_rate = live.upload_rate;
            }
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
        const auto higher_priority = [](const Task &a, const Task &b) {
            if (a.force != b.force) return a.force;
            if (a.priority != b.priority) return a.priority > b.priority;
            if (a.created_at != b.created_at) return a.created_at < b.created_at;
            return a.task_id < b.task_id;
        };
        while (running_.load()) {
            {
                std::scoped_lock lock(mtx_);
                Task *priority_task = nullptr;
                Task *slowest_task = nullptr;
                std::vector<Task *> downloading;

                for (auto &task: tasks_ | std::views::values) {
                    if (task.is_delete || !is_download_task(task) || !engine_of(task.task_protocol)) continue;

                    // 检索“队列中”任务，获取高优先级的任务
                    if (task.status == DW_TASK_STATUS_QUEUED && (
                            !priority_task || higher_priority(task, *priority_task))) {
                        priority_task = &task;
                    }
                    // 检索“下载中”任务，获取低下载速率的任务
                    if (task.status == DW_TASK_STATUS_DOWNLOADING) {
                        downloading.push_back(&task);
                        if (!slowest_task || task.download_rate < slowest_task->download_rate) {
                            slowest_task = &task;
                        }
                    }
                }

                if (!net_allowed_) {
                    // 禁用流量时，暂停所有正在下载的任务。
                    for (const auto *task: downloading) {
                        const auto params = task_params(*task);
                        engine_of(params.protocol)->pause_task(&params);
                    }
                } else if (priority_task && config_.max_concurrent_downloads > 0 && running_.load()) {
                    const auto limit = static_cast<std::size_t>(config_.max_concurrent_downloads);
                    if (downloading.size() >= limit && priority_task->force && slowest_task) {
                        log_i(slowest_task->task_id.c_str(), "调度器 暂停最慢任务[{}]", slowest_task->name);
                        const auto params = task_params(*slowest_task);
                        engine_of(params.protocol)->pause_task(&params);
                    }

                    if (downloading.size() < limit) {
                        auto params = task_params(*priority_task);
                        params.file_priorities = priority_task->priority_file_indexes;
                        log_i(priority_task->task_id.c_str(), "调度器 派发任务[{}]", priority_task->name);
                        engine_of(params.protocol)->do_resume(&params);
                    }
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(flush_interval_ms_));
        }
    }

    void TaskManager::maintenance_loop() {
        for (;;) {
            const bool stopping = !running_.load();
            std::vector<Task> dirty_records;
            {
                std::scoped_lock lock(mtx_);
                for (auto it = tasks_.begin(); it != tasks_.end();) {
                    const auto current = it++;
                    auto &task = current->second;
                    // 清理删除任务 内存+数据库
                    if (task.is_delete) {
                        if (!task_store_.delete_task(task.task_id)) {
                            log_e(task.task_id.c_str(), "任务[{}]软删除失败", task.name);
                        } else if (!task_file_store_.delete_task(task.task_id)) {
                            log_e(task.task_id.c_str(), "任务[{}]文件软删除失败", task.name);
                        }
                        tasks_.erase(current);
                        continue;
                    }

                    bool saved_changes = false;
                    for (auto &file: task.files) {
                        if (!file.dirty) continue;
                        if (task_file_store_.save(file)) {
                            file.dirty = false;
                            saved_changes = true;
                        } else {
                            log_e(task.task_id.c_str(), "任务[{}]文件保存失败", task.name);
                        }
                    }

                    if (task.dirty) {
                        if (task_store_.save(task)) {
                            task.dirty = false;
                            saved_changes = true;
                        } else {
                            log_e(task.task_id.c_str(), "任务[{}]保存失败", task.name);
                        }
                    }
                    if (saved_changes) dirty_records.push_back(task);
                }
            }

            for (const auto &task: dirty_records) {
                // 回调
                auto payload = to_json(task);
                payload["task_natural_key"] = task.task_id;
                const auto source = task.task_protocol == DW_PROTOCOL_TORRENT
                                        ? DW_SOURCE_REMOTE_FILE
                                        : (task.task_protocol == DW_PROTOCOL_HTTP
                                               ? DW_SOURCE_TASK_FILE
                                               : DW_SOURCE_LOCAL_FILE);
                payload["type"] = static_cast<int>(source);
                payload["files"] = boost::json::value_from(task.files);
                const std::string json = boost::json::serialize(payload);
                emit_progress(json.c_str());
            }

            if (stopping) break;

            // 引擎清理
            if (http_) http_->sweep();
            if (torrent_) torrent_->sweep();

            std::this_thread::sleep_for(std::chrono::milliseconds(maintenance_interval_ms_));
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
        const auto progress_map = task_file_store_.load_progress_by_paths(paths);
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
