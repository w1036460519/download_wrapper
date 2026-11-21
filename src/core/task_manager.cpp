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
#include <cmath>
#include <cstddef>
#include <exception>
#include <filesystem>
#include <fstream>
#include <ranges>
#include <stdexcept>
#include <utility>

#if defined(_WIN32)
#include <windows.h>
#else
#include <sys/stat.h>
#if defined(__linux__) && __has_include(<linux/stat.h>)
#include <fcntl.h>
#include <linux/stat.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif
#endif

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
        const bool trackers_changed = config_.trackers != cfg.trackers;
        const bool proxy_changed = config_.bt_proxy_type != cfg.bt_proxy_type ||
                                   config_.bt_proxy_host != cfg.bt_proxy_host ||
                                   config_.bt_proxy_port != cfg.bt_proxy_port ||
                                   config_.proxy_username != cfg.proxy_username ||
                                   config_.proxy_password != cfg.proxy_password;
        config_ = cfg;
        for (auto &task: tasks_ | std::views::values) {
            if (task.save_path.empty() && !task.storage_root_id.empty() &&
                config_.storage_roots.contains(task.storage_root_id)) {
                if (Task stored; task_store_.find_by_task_id(task.task_id, stored)) {
                    task.full_path = std::move(stored.full_path);
                }
                resolve_download_location(task);
            }
        }
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
            if (proxy_changed) torrent_->update_proxy(config_);
            if (trackers_changed) torrent_->update_trackers(config_.trackers);
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

    dw_result<> TaskManager::set_file_priority(const TaskParams &params) const {
        std::scoped_lock lock(mtx_);
        if (params.protocol != DW_PROTOCOL_TORRENT || params.client_id != config_.client_id) {
            log_e(params.task_key().c_str(), "manager 下载优先级 暂不支持");
            return dw_result<>::failure();
        }
        if (!engine_of(DW_PROTOCOL_TORRENT)) return dw_result<>::failure();
        return torrent_->set_file_priority(params);
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

    void TaskManager::assign_download_location(Task &task) const {
        std::scoped_lock lock(mtx_);
        task.storage_root_id.clear();
        task.relative_save_path.clear();
        if (const auto root = config_.storage_roots.find(config_.default_download_root);
            root != config_.storage_roots.end()) {
            const auto relative = std::filesystem::path(task.save_path).lexically_normal()
                    .lexically_relative(std::filesystem::path(root->second).lexically_normal());
            if (!relative.empty() && !relative.is_absolute() && *relative.begin() != "..") {
                task.storage_root_id = root->first;
                if (relative != ".") task.relative_save_path = relative.generic_string();
            }
        }
    }

    void TaskManager::resolve_task_file_paths(Task &task) {
        namespace fs = std::filesystem;
        fs::path common_root;
        bool has_file = false;
        bool single_root = true;
        for (auto &file: task.files) {
            file.full_path.clear();
            const auto relative = fs::path(file.path).lexically_normal();
            if (file.is_delete || task.save_path.empty() || relative.empty() || relative == "." ||
                relative.has_root_path() || *relative.begin() == "..")
                continue;
            file.full_path = (fs::path(task.save_path) / relative).string();
            if (!has_file) {
                common_root = *relative.begin();
                has_file = true;
            } else if (*relative.begin() != common_root) {
                single_root = false;
            }
        }
        if (task.save_path.empty()) task.full_path.clear();
        else if (has_file) {
            task.full_path = single_root ? (fs::path(task.save_path) / common_root).string() : task.save_path;
        }
    }

    std::string TaskManager::resolve_save_path(const std::string &root_id, const std::string &subdirectory) const {
        namespace fs = std::filesystem;
        const auto root = config_.storage_roots.find(root_id);
        const auto relative = fs::path(subdirectory).lexically_normal();
        if (root_id.empty() || root == config_.storage_roots.end() || !fs::path(root->second).is_absolute() ||
            relative.has_root_path() || (!relative.empty() && *relative.begin() == "..")) {
            return {};
        }
        return relative.empty() ? root->second : (fs::path(root->second) / relative).string();
    }

    void TaskManager::resolve_download_location(Task &task) const {
        namespace fs = std::filesystem;
        const fs::path previous = task.save_path;
        task.save_path = resolve_save_path(task.storage_root_id, task.relative_save_path);
        if (previous != task.save_path) {
            const auto relative_full_path = task.full_path.empty() || previous.empty()
                                                ? fs::path(task.full_path)
                                                : fs::path(task.full_path).lexically_relative(previous);
            task.full_path = task.save_path.empty() || relative_full_path.empty() || relative_full_path.has_root_path()
                             || *relative_full_path.begin() == ".."
                                 ? ""
                                 : (fs::path(task.save_path) / relative_full_path).string();
        }
        resolve_task_file_paths(task);
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
        resolve_download_location(task);
        return &tasks_.try_emplace(key, std::move(task)).first->second;
    }

    /* ================================================================== */
    /*                          快照查询                                  */
    /* ================================================================== */

    bool TaskManager::get_task(const std::string &task_id, Task &out) {
        std::scoped_lock lock(mtx_);
        if (const auto it = tasks_.find(task_id); it != tasks_.end()) {
            out = it->second;
            return true;
        }
        if (!task_store_.find_by_task_id(task_id, out)) return false;
        out.files = task_file_store_.get(task_id);
        resolve_download_location(out);
        return true;
    }

    std::vector<Task> TaskManager::list() {
        std::scoped_lock lock(mtx_);
        auto tasks = task_store_.find_all();
        for (auto &task: tasks) resolve_download_location(task);
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
        const auto append_rate = [](boost::json::array &history, double rate) {
            constexpr std::size_t sample_capacity = 60;
            if (!std::isfinite(rate) || rate < 0) return;
            if (history.size() >= sample_capacity) history.erase(history.begin());
            history.emplace_back(rate);
        };
        while (running_.load()) {
            std::vector<std::string> dirty_records;
            {
                std::scoped_lock lock(mtx_);
                for (auto it = tasks_.begin(); it != tasks_.end();) {
                    const auto current = it++;
                    auto &task = current->second;
                    if (!task.is_delete && std::ranges::any_of(task.files, &TaskFile::dirty)) {
                        task.dirty = true;
                    }
                    if (task.dirty) {
                        task.modified_at = std::max(now_unix_ms(), task.modified_at + 1);
                    }
                    if (task.is_delete) {
                        if (task.dirty) {
                            auto *db = database_.connection();
                            if (sqlite3_exec(db, "BEGIN IMMEDIATE", nullptr, nullptr, nullptr) != SQLITE_OK) {
                                log_e(task.task_id.c_str(), "manager 任务[{}]开启删除事务失败", task.name);
                                continue;
                            }
                            if (!task_store_.save(task) || !task_file_store_.delete_task(task.task_id) ||
                                sqlite3_exec(db, "COMMIT", nullptr, nullptr, nullptr) != SQLITE_OK) {
                                sqlite3_exec(db, "ROLLBACK", nullptr, nullptr, nullptr);
                                log_e(task.task_id.c_str(), "manager 任务[{}]删除保存失败", task.name);
                                continue;
                            }
                            task.dirty = false;
                            dirty_records.push_back(boost::json::serialize(to_summary_json(task)));
                        }
                        tasks_.erase(current);
                        continue;
                    }
                    for (auto &file: task.files) {
                        if (!file.dirty) continue;
                        file.storage_root_id = task.storage_root_id;
                        file.relative_save_path = task.relative_save_path;
                        if (task_file_store_.save(file)) {
                            file.dirty = false;
                        } else {
                            log_e(task.task_id.c_str(), "manager 任务[{}]文件保存失败", task.name);
                        }
                    }

                    if (task.dirty) {
                        // 下载中的有效速率在 wrapper 采样，详情页是否打开不影响历史。
                        if (task.status == DW_TASK_STATUS_DOWNLOADING) {
                            append_rate(task.download_rate_history, task.download_rate);
                            append_rate(task.upload_rate_history, task.upload_rate);
                        }
                        if (task_store_.save(task)) {
                            task.dirty = false;
                        } else {
                            log_e(task.task_id.c_str(), "manager 任务[{}]保存失败", task.name);
                        }
                        dirty_records.push_back(boost::json::serialize(to_summary_json(task)));
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

            for (const auto &record: dirty_records) {
                emit_progress(record.c_str());
            }

            if (torrent_) torrent_->sweep();

            std::this_thread::sleep_for(std::chrono::milliseconds(maintenance_interval_ms_));
        }
    }

    namespace {
        void read_file_times(const std::filesystem::path &path, TaskFile &file) {
#if defined(_WIN32)
            WIN32_FILE_ATTRIBUTE_DATA attributes{};
            if (!GetFileAttributesExW(path.c_str(), GetFileExInfoStandard, &attributes)) return;
            const auto to_unix_ms = [](const FILETIME &time) -> std::optional<int64_t> {
                const auto ticks = (static_cast<uint64_t>(time.dwHighDateTime) << 32) | time.dwLowDateTime;
                if (ticks == 0) return std::nullopt;
                return static_cast<int64_t>(ticks / 10000) - 11644473600000LL;
            };
            file.modified_at_ms = to_unix_ms(attributes.ftLastWriteTime);
            file.created_at_ms = to_unix_ms(attributes.ftCreationTime);
#else
            const auto to_unix_ms = [](int64_t seconds, int64_t nanoseconds) {
                return seconds * 1000 + nanoseconds / 1000000;
            };
#if defined(SYS_statx) && defined(STATX_BTIME)
            struct statx extended_info{};
            // 旧内核和部分文件系统只提供修改时间，创建时间保持缺省。
            if (::syscall(SYS_statx, AT_FDCWD, path.c_str(), 0, STATX_MTIME | STATX_BTIME, &extended_info) == 0) {
                if (extended_info.stx_mask & STATX_BTIME) {
                    file.created_at_ms = to_unix_ms(extended_info.stx_btime.tv_sec, extended_info.stx_btime.tv_nsec);
                }
                if (extended_info.stx_mask & STATX_MTIME) {
                    file.modified_at_ms = to_unix_ms(extended_info.stx_mtime.tv_sec, extended_info.stx_mtime.tv_nsec);
                    return;
                }
            }
#endif
            struct stat info{};
            if (::stat(path.c_str(), &info) != 0) return;
#if defined(__APPLE__)
            file.modified_at_ms = to_unix_ms(info.st_mtimespec.tv_sec, info.st_mtimespec.tv_nsec);
            if (info.st_birthtimespec.tv_sec != 0 || info.st_birthtimespec.tv_nsec != 0) {
                file.created_at_ms = to_unix_ms(info.st_birthtimespec.tv_sec, info.st_birthtimespec.tv_nsec);
            }
#else
            file.modified_at_ms = to_unix_ms(info.st_mtim.tv_sec, info.st_mtim.tv_nsec);
#endif
#endif
        }
    } // namespace

    std::vector<TaskFile> TaskManager::get_files(const std::string &dir_path) const {
        namespace fs = std::filesystem;
        std::scoped_lock lock(mtx_);
        std::vector<TaskFile> result;
        try {
            if (!fs::is_directory(dir_path)) return result;
            const auto directory = fs::absolute(dir_path).lexically_normal();
            constexpr int page_size = 32;

            for (const auto &entry: fs::directory_iterator(directory)) {
                const auto name = entry.path().filename().string();
                TaskFile file;
                file.path = name;
                file.full_path = entry.path().string();
                file.name = name;
                if (entry.is_directory()) {
                    file.is_dir = true;
                } else if (entry.is_regular_file()) {
                    file.total_size = static_cast<int64_t>(entry.file_size());
                    file.extension = utils::file_extension(name);
                    for (int64_t offset = 0; file.task_id.empty(); offset += page_size) {
                        auto candidates = task_file_store_.find_candidates(name, page_size, offset);
                        for (auto &candidate: candidates) {
                            const auto save_path = resolve_save_path(candidate.storage_root_id,
                                                                     candidate.relative_save_path);
                            const auto relative = fs::path(candidate.path).lexically_normal();
                            if (save_path.empty() || relative.empty() || relative == "." ||
                                relative.has_root_path() || *relative.begin() == ".." ||
                                (fs::path(save_path) / relative).lexically_normal() != entry.path())
                                continue;

                            file.task_id = std::move(candidate.task_id);
                            file.file_index = candidate.file_index;
                            file.prioritize = candidate.prioritize;
                            file.is_read = candidate.is_read;
                            file.total_done = std::max<int64_t>(0, candidate.total_done);
                            file.total_size = std::max(file.total_size, file.total_done);
                            file.progress = candidate.progress;
                            file.segments = std::move(candidate.segments);
                            break;
                        }
                        if (candidates.size() < page_size) break;
                    }
                } else {
                    continue;
                }
                read_file_times(entry.path(), file);
                result.push_back(std::move(file));
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

    dw_result<std::vector<TaskFile> > TaskManager::parse_torrent_file(const std::string &torrent_file_path,
                                                                      std::string *info_hash) {
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
