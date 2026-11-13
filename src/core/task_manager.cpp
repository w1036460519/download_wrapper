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
#include "http/http_engine.h"
#include "utils/time_util.h"
#include "utils/string_util.h"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <exception>
#include <filesystem>
#include <fstream>
#include <ranges>
#include <stdexcept>
#include <unordered_set>
#include <utility>

namespace dw {
    using utils::now_unix_ms;

    TaskManager::TaskManager() {
        http_ = std::make_unique<HttpEngine>();
        torrent_ = std::make_unique<TorrentEngine>();
    }

    TaskManager::~TaskManager() {
        stop();
    }

    /* ================================================================== */
    /*                          生命周期                                  */
    /* ================================================================== */

    dw_result<> TaskManager::config(const Config &cfg) {
        if (cfg.client_id.empty()) {
            log_e("", "manager 客户端标识为空");
            return dw_result<>::failure();
        }
        std::scoped_lock lock(mtx_);
        const auto previous_downloads = config_.max_concurrent_downloads;
        const auto previous_download_rate = config_.download_rate_limit;
        const auto previous_upload_rate = config_.upload_rate_limit;
        const bool previous_net_allowed = net_allowed_;
        config_ = cfg;
        if (config_.max_concurrent_downloads < 0) config_.max_concurrent_downloads = 3;
        net_allowed_ = config_.network_type != 0 &&
                       (config_.network_type != 2 || config_.allow_mobile_data);
        if (torrent_ && running_.load()) {
            const auto changed = [](auto previous, auto current) {
                return previous == current ? std::nullopt : std::optional{current};
            };
            torrent_->config(changed(previous_downloads, config_.max_concurrent_downloads),
                             changed(previous_download_rate, config_.download_rate_limit),
                             changed(previous_upload_rate, config_.upload_rate_limit),
                             changed(previous_net_allowed, net_allowed_));
        }
        log_i("", "manager 应用配置[{}]", to_json_string(config_));
        return dw_result<>::success();
    }

    dw_result<> TaskManager::init() {
        const std::string dir = !config_.work_dir.empty() ? config_.work_dir : ".";
        const std::string story_path = dir + "/leopard_tasks.db";

        std::unique_lock lock(mtx_);
        if (running_.load()) {
            log_i("", "manager 已初始化");
            return dw_result<>::success();
        }
        if (!database_.open(story_path)) {
            log_e("", "manager 数据存储初始化失败[{}]", story_path);
            return dw_result<>::failure();
        }
        if (!task_store_.init_schema() || !task_file_store_.init_schema() ||
            !task_resume_store_.init_schema()) {
            log_e("", "manager 数据任务初始化失败");
            database_.close();
            return dw_result<>::failure();
        }
        const std::vector<Task> all_tasks = task_store_.find_all();
        std::vector<std::string> torrent_task_ids;
        for (auto &task: all_tasks) {
            if (task.status == DW_TASK_STATUS_COMPLETED) continue;
            if (task.task_protocol == DW_PROTOCOL_TORRENT) {
                torrent_task_ids.push_back(task.task_id);
            }
        }
        try {
            http_->set_task_manager(this);
            if (http_->init() != 0) {
                log_e("", "manager HTTP 引擎初始化失败");
                lock.unlock();
                stop();
                return dw_result<>::failure();
            }

            torrent_->set_task_manager(this);
            if (torrent_->init(torrent_task_ids) != 0) {
                log_e("", "manager BT 引擎初始化失败");
                lock.unlock();
                stop();
                return dw_result<>::failure();
            }

            running_.store(true);
            maintenance_ = std::thread([this] { maintenance_loop(); });
        } catch (const std::exception &e) {
            log_e("", "manager 下载器初始化失败: {}", e.what());
            lock.unlock();
            stop();
            return dw_result<>::failure();
        }

        log_i(config_.client_id.c_str(), "manager 下载器启动成功");
        return dw_result<>::success();
    }

    void TaskManager::stop() {
        const bool was_running = running_.exchange(false);
        if (worker_.joinable()) worker_.join();
        if (maintenance_.joinable()) maintenance_.join();

        if (http_) http_->destroy();
        if (torrent_) torrent_->destroy();

        std::scoped_lock lock(mtx_);
        tasks_.clear();
        database_.close();
        if (was_running) log_i("", "manager 下载器已停止");
    }

    /* ================================================================== */
    /*                          控制操作                                  */
    /* ================================================================== */

    dw_result<> TaskManager::add(const TaskParams &params) const {
        log_i("", "manager 添加任务[{}]", to_json_string(params));
        const dw_protocol_t protocol = params.protocol;
        if (protocol == DW_PROTOCOL_HTTP && params.url.empty()) {
            log_e("", "manager 添加任务失败 链接不能为空");
            return dw_result<>::failure();
        }
        if (protocol == DW_PROTOCOL_TORRENT && params.magnet_link.empty() && params.torrent_file.empty()) {
            log_e("", "manager 添加任务失败 链接/文件不能为空");
            return dw_result<>::failure();
        }
        if (params.client_id.empty() || params.client_id == config_.client_id) {
            IDownloadEngine *eng = engine_of(protocol);
            return eng ? eng->add_task(&params) : dw_result<>::failure();
        }
        log_e("", "manager 添加任务 暂不支持");
        return dw_result<>::failure();
    }

    dw_result<> TaskManager::pause(const TaskParams &params) const {
        log_i(params.task_key().c_str(), "manager 暂停任务[{}]", to_json_string(params));
        if (params.client_id.empty() || params.client_id == config_.client_id) {
            IDownloadEngine *eng = engine_of(params.protocol);
            return eng ? eng->pause_task(&params) : dw_result<>::failure();
        }
        log_e(params.task_key().c_str(), "manager 暂停任务 暂不支持");
        return dw_result<>::failure();
    }


    dw_result<> TaskManager::resume(const TaskParams &params) const {
        log_i(params.task_key().c_str(), "manager 恢复任务[{}]", to_json_string(params));
        if (params.client_id.empty() || params.client_id == config_.client_id) {
            IDownloadEngine *engine = engine_of(params.protocol);
            return engine ? engine->resume_task(&params) : dw_result<>::failure();
        }
        log_e(params.task_key().c_str(), "manager 恢复任务 暂不支持");
        return dw_result<>::failure();
    }

    dw_result<> TaskManager::remove(const TaskParams &params) const {
        log_i(params.task_key().c_str(), "manager 删除任务[{}]", to_json_string(params));
        if (params.client_id.empty() || params.client_id == config_.client_id) {
            IDownloadEngine *engine = engine_of(params.protocol);
            return engine ? engine->delete_task(&params) : dw_result<>::failure();
        }
        log_e(params.task_key().c_str(), "manager 删除任务 暂不支持");
        return dw_result<>::failure();
    }

    Task *TaskManager::get_engine_task(const std::string &key) {
        std::scoped_lock lock(mtx_);
        if (const auto it = tasks_.find(key); it != tasks_.end())
            return &it->second;

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
        for (auto &task: tasks) {
            task.files = task_file_store_.get(task.task_id);
        }
        return tasks;
    }

    /* ================================================================== */
    /*                          调度线程                                  */
    /* ================================================================== */

    void TaskManager::scheduler_loop() {
        while (running_.load()) {
            {
                // 调度两个引擎的下载速度和同时下载数
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(flush_interval_ms_));
        }
    }

    void TaskManager::maintenance_loop() {
        while (running_.load()) {
            std::vector<Task> dirty_records;
            {
                std::scoped_lock lock(mtx_);
                for (auto it = tasks_.begin(); it != tasks_.end();) {
                    const auto current = it++;
                    auto &task = current->second;
                    if (task.is_delete) {
                        if (task.dirty) {
                            if (!task_store_.save(task) || !task_file_store_.delete_task(task.task_id)) {
                                log_e(task.task_id.c_str(), "manager 任务[{}]软删除保存失败", task.name);
                                continue;
                            }
                            task.dirty = false;
                            dirty_records.push_back(std::move(task));
                        }
                        tasks_.erase(current);
                        continue;
                    }
                    for (auto &file: task.files) {
                        if (!file.dirty) continue;
                        if (task_file_store_.save(file)) {
                            file.dirty = false;
                        } else {
                            log_e(task.task_id.c_str(), "manager 任务[{}]文件保存失败", task.name);
                        }
                    }

                    if (task.dirty) {
                        if (task_store_.save(task)) {
                            task.dirty = false;
                        } else {
                            log_e(task.task_id.c_str(), "manager 任务[{}]保存失败", task.name);
                        }
                        dirty_records.push_back(task);
                    }

                    if (task.resume_dirty) {
                        if (TaskResume resume{.task_id = task.task_id, .resume_data = std::move(task.resume_data)};
                            task_resume_store_.save(resume)) {
                            task.resume_dirty = false;
                        } else {
                            task.resume_data = std::move(resume.resume_data);
                            log_e(task.task_id.c_str(), "manager 任务[{}]续传数据保存失败", task.name);
                        }
                    }
                }
            }

            for (const auto &task: dirty_records) {
                emit_progress(to_string(task).c_str());
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(maintenance_interval_ms_));
        }
    }

    std::vector<TaskFile> TaskManager::get_files(const std::string &dir_path) const {
        std::vector<TaskFile> result;
        try {
            if (!std::filesystem::exists(dir_path) || !std::filesystem::is_directory(dir_path)) {
                return result;
            }
            for (const auto &entry: std::filesystem::directory_iterator(dir_path)) {
                const auto name = entry.path().filename().string();
                // 跳过隐藏文件/目录
                if (!name.empty() && name[0] == '.') continue;

                TaskFile fi;
                fi.path = name;
                fi.full_path = entry.path().string();
                fi.name = name;
                if (entry.is_directory()) {
                    fi.is_dir = true;
                } else if (entry.is_regular_file()) {
                    fi.total_size = static_cast<int64_t>(entry.file_size());
                    fi.extension = utils::file_extension(fi.name);
                    {
                        std::scoped_lock lock(mtx_);
                        if (TaskFile stored_file; task_file_store_.find_by_path(fi.full_path, stored_file)) {
                            fi.total_done = std::max<int64_t>(0, stored_file.total_done);
                            fi.total_size = std::max(fi.total_size, fi.total_done);
                            fi.progress = stored_file.progress;
                            fi.segments = std::move(stored_file.segments);
                        }
                    }
                } else {
                    continue;
                }
                result.push_back(std::move(fi));
            }
        } catch (const std::exception &e) {
            log_e("", "manager 扫描目录[{}]失败: {}", dir_path, e.what());
        } catch (...) {
            log_e("", "manager 扫描目录[{}]失败: 未知异常", dir_path);
        }
        return result;
    }

    // ---- 本地文件浏览与管理 ----


    dw_result<std::string> TaskManager::parse_magnet(const std::string &magnet_link) {
        return TorrentEngine::parse_magnet(magnet_link);
    }

    dw_result<std::vector<TaskFile>> TaskManager::parse_torrent_file(const std::string &torrent_file_path, std::string *info_hash) {
        return TorrentEngine::parse_torrent_file(torrent_file_path, info_hash);
    }

    IDownloadEngine *TaskManager::engine_of(const dw_protocol_t proto) const {
        auto *engine = proto == DW_PROTOCOL_HTTP ? http_.get() : torrent_.get();
        return engine && engine->initialized_ ? engine : nullptr;
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
