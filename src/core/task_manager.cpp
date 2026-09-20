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
#include "utils/memory_util.h"
#include "utils/time_util.h"
#include "utils/unique_name.h"
#include "utils/string_util.h"

#include <chrono>
#include <cstdlib>
#include <cstring>
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

    int32_t TaskManager::start(const dw_config_t &cfg) {
        max_concurrent_ = cfg.max_concurrent_downloads > 0 ? cfg.max_concurrent_downloads : 3;
        flush_interval_ms_ = cfg.status_callback_interval_ms > 0 ? cfg.status_callback_interval_ms : 1000;
        maintenance_interval_ms_ = flush_interval_ms_ * 2;

        // 注入 clientId：App 启动时从 SharedPreferences 读取并经 dw_config_t 传入。
        // 此后本 session 创建/加载的所有任务均归此 clientId 隔离。
        client_id_ = cfg.client_id ? cfg.client_id : "";
        if (client_id_.empty()) {
            log_e("", "下载器客户端标识缺失，启动失败");
            return -1;
        }

        const std::string dir = cfg.work_dir && cfg.work_dir[0] ? cfg.work_dir : ".";
        const std::string story_path = dir + "/leopard_tasks.db";

        std::lock_guard<std::mutex> lock(mtx_);
        if (!store_.open(story_path)) {
            log_e("", "下载器打开数据库失败: %s", story_path.c_str());
            return -1;
        }
        store_.init_schema();

        // 从 file_records 重建内存注册表（任务状态持久化权威投影）：
        // RESOLVING/QUEUED/DOWNLOADING/ERROR/FAIL → QUEUED（回队列重新准入，权威列同步归一化）；
        // PAUSED 保持；COMPLETED/INVALIDATED 与本地文件条目不进调度。
        {
            for (auto &fr: store_.load_file_records(client_id_)) {
                if (!fr.has_task()) continue; // 本地文件/目录条目不参与调度
                switch (static_cast<dw_task_status_t>(fr.status)) {
                    case DW_TASK_STATUS_RESOLVING:
                    case DW_TASK_STATUS_QUEUED:
                    case DW_TASK_STATUS_DOWNLOADING:
                    case DW_TASK_STATUS_ERROR:
                    case DW_TASK_STATUS_FAIL:
                        // 活跃态归一化：回队列等待重新准入，权威列同步刷新
                        fr.status = DW_TASK_STATUS_QUEUED;
                        store_.update_file_record_status(
                            fr.client_id, fr.task_protocol, fr.task_natural_key,
                            DW_TASK_STATUS_QUEUED, DW_REASON_NONE, "");
                        register_task(std::move(fr));
                        break;
                    case DW_TASK_STATUS_PAUSED:
                        register_task(std::move(fr));
                        break;
                    default: // COMPLETED/INVALIDATED 不进调度
                        break;
                }
            }
        }

        running_.store(true);
        schedule_needed_ = true;
        worker_ = std::thread([this] { scheduler_loop(); });
        maintenance_ = std::thread([this] { maintenance_loop(); });

        log_i("", "下载器启动 clientId=%s tasks=%zu concurrent=%d",
              client_id_.c_str(), tasks_.size(), max_concurrent_);
        return 0;
    }

    void TaskManager::stop() {
        if (!running_.exchange(false)) {
            return;
        }
        // running_=false 驱动循环条件退出；notify_all 立即唤醒两循环的 cv 等待点。
        cv_.notify_all();
        if (worker_.joinable()) {
            worker_.join();
        }
        if (maintenance_.joinable()) {
            maintenance_.join();
        }

        std::lock_guard<std::mutex> lock(mtx_);
        flush_dirty_locked();
        store_.close();
        log_i("", "下载器已停止");
    }

    /* ================================================================== */
    /*                          控制操作                                  */
    /* ================================================================== */

    int32_t TaskManager::add(const dw_protocol_t proto, const std::string &client_id,
                             const dw_task_params_t *params,
                             dw_submit_result_t *out, const bool force) {
        if (!params || !out) {
            if (out) {
                out->code = DW_REASON_ERROR;
                out->message = nullptr;
            }
            return -1;
        }
        // add 接口不接收 resume_data：由调度器调 resume_task 时从库中加载。
        if (params->resume_data != nullptr && params->resume_data_size > 0) {
            const_cast<dw_task_params_t *>(params)->resume_data = nullptr;
            const_cast<dw_task_params_t *>(params)->resume_data_size = 0;
        }
        const char *raw_key = dw_task_params_key(params, proto);
        log_i("", "添加任务[%s] force=%d %s",
              to_string(proto), force, to_string(*params).c_str());
        if (!raw_key || !raw_key[0]) {
            out->code = DW_REASON_ERROR;
            out->message = nullptr;
            return -1;
        }
        if (client_id.empty()) {
            out->code = DW_REASON_ERROR;
            out->message = nullptr;
            return -1;
        }

        // add 任务以 RESOLVING（解析中）入列；BT 全新任务同步入引擎创建 handle
        //（元数据就绪即同步返回文件列表），元数据就绪（PARSED 事件）后转 QUEUED，
        // 再经调度准入进入 DOWNLOADING。
        // resume_task 双行为：handle 不存在时用 magnet/torrent/info_hash/resume_data 创建；
        // handle 已存在时直接恢复下载。同 key 重复添加 / file_records 已存在任务快路仅刷新排序时间。

        bool fresh_registered = false;
        {
            std::unique_lock<std::mutex> lock(mtx_);
            const std::string key = raw_key;
            const std::string uid = union_id_of(client_id, proto, key);

            // 强制添加：清理进度数据（内存 + DB）。
            if (force) {
                if (const auto it = tasks_.find(uid); it != tasks_.end()) {
                    tasks_.erase(it);
                }
                store_.reset_task_progress(client_id, proto, key);
                // 同步缓存：进度已归零，状态回 RESOLVING
                if (file_cache_loaded_) {
                    auto cit = file_cache_.find(union_id_of(client_id, proto, key));
                    if (cit != file_cache_.end()) {
                        cit->second.status = DW_TASK_STATUS_RESOLVING;
                        cit->second.total_size = -1;
                        cit->second.total_done = 0;
                        cit->second.reason = 0;
                        cit->second.message.clear();
                        cit->second.modified_at = now_unix_ms();
                    }
                }
            }

            if (const auto it = tasks_.find(uid); it != tasks_.end()) {
                // 已常驻内存：仅刷新 created_at 用于排序置顶，状态保持不变。
                it->second.created_at = now_unix_ms();
                it->second.synth_notified = false;
                store_.touch_file_record(client_id, proto, key);
                sync_file_record_cache(client_id, proto, key);
            } else {
                ensure_file_cache_locked();
                const auto cit = file_cache_.find(union_id_of(client_id, proto, key));
                if (cit != file_cache_.end()) {
                    // 历史任务：从 file_records（状态持久化权威）重建，仅刷新 created_at
                    // 用于排序置顶，其余状态保持不变。
                    FileRecord task_record = cit->second;
                    task_record.created_at = now_unix_ms();
                    task_record.synth_notified = false;
                    store_.touch_file_record(client_id, proto, key);
                    sync_file_record_cache(client_id, proto, key);
                    register_task(std::move(task_record));
                    schedule_needed_ = true;
                } else {
                    // 全新任务：构造复合主键三元组（client_id 来自 params）。
                    // 占位 file_records 由 dw_add_task 在本调用后插入（status=RESOLVING），
                    // 此处仅入内存注册表（tasks 表已移除）。
                    FileRecord task_record;
                    task_record.client_id = client_id;
                    task_record.task_protocol = proto;
                    task_record.task_natural_key = key;
                    task_record.save_path = params->save_path ? params->save_path : "";
                    // magnet_link / torrent_file 不入 FileRecord：由 dw_add_task 写入 resume_data 表，
                    // 用于 resume data 尚未生成时的兜底恢复。
                    // trackers 不持久化：直接由引擎处理，不存入 FileRecord。
                    if (params->file_indexes && params->file_index_size > 0) {
                        task_record.file_indexes.assign(params->file_indexes,
                                                        params->file_indexes + params->file_index_size);
                    }
                    task_record.priority = params->priority;
                    // 来源由协议完全决定，不由调用方传入：避免上层遗漏赋值时
                    // 以 DW_SOURCE_LOCAL_FILE 错误入库，导致下载任务被当作本地文件条目。
                    task_record.type = (proto == DW_PROTOCOL_TORRENT)
                                           ? DW_SOURCE_REMOTE_FILE
                                           : DW_SOURCE_TASK_FILE;
                    task_record.is_remote = true; // 下载任务均为远程来源
                    task_record.created_at = now_unix_ms();
                    // 占位后续事件回调修正
                    task_record.root_name = key;
                    task_record.status = DW_TASK_STATUS_RESOLVING; // 解析中：等待调度准入入引擎与元数据就绪
                    task_record.synth_notified = false;
                    register_task(std::move(task_record));
                    schedule_needed_ = true;
                    fresh_registered = true;
                }
            }
        }

        // 全新 BT 任务：同步入引擎创建 handle（文件列表由引擎按 handle 元数据就绪
        // 情况填充出参，wrapper 仅透传）。引擎返回后再 notify：避免调度器在 handle
        // 就绪前准入触发 resume_task 重建竞态。HTTP 不同步入引擎（其 add_task 会
        // 直接启动传输，绕过调度闸门），仍由调度准入 resume_task 驱动。
        if (fresh_registered && proto == DW_PROTOCOL_TORRENT) {
            if (IDownloadEngine *eng = engine_of(proto)) {
                if (eng->add_task(params, out) != 0) {
                    // 引擎添加失败：回收内存注册（出参错误信息已由引擎填充）。
                    const std::lock_guard<std::mutex> lock(mtx_);
                    tasks_.erase(union_id_of(client_id, proto, raw_key));
                    return -1;
                }
                cv_.notify_all();
                return 0;
            }
        }

        cv_.notify_all();

        out->code = DW_REASON_NONE;
        out->message = nullptr;
        // 幂等快路 / 历史重建路径：BT 成功同样回填 info_hash，口径与引擎路径一致。
        if (proto == DW_PROTOCOL_TORRENT) {
            out->info_hash = utils::dup_cstr(raw_key);
        }
        return 0;
    }

    int32_t TaskManager::pause(const dw_protocol_t proto, const std::string &natural_key,
                               dw_submit_result_t *out) const {
        if (!out) return -1;
        log_i(natural_key.c_str(), "暂停任务[%s]", to_string(proto));
        if (IDownloadEngine *eng = engine_of(proto)) {
            return eng->pause_task(natural_key, client_id_, out);
        }
        out->code = DW_REASON_NONE;
        out->message = nullptr;
        return 0;
    }

    int32_t TaskManager::resume(const dw_protocol_t proto, const std::string &natural_key,
                                dw_submit_result_t *out) {
        if (!out) return -1;
        log_i(natural_key.c_str(), "恢复任务[%s]", to_string(proto));

        // 提取优先级索引（持锁保证一致性；拷贝出锁外生命周期）。
        std::vector<int32_t> priority_file_indexes;
        {
            std::lock_guard<std::mutex> lock(mtx_);
            // 从内存或 DB 加载任务（DB 命中时注册入内存）。
            const FileRecord *rec = load_task_record_locked(client_id_, proto, natural_key);
            if (!rec) {
                out->code = DW_REASON_ERROR;
                out->message = utils::dup_cstr("任务不存在");
                return -1;
            }
            if (rec->task_protocol == DW_PROTOCOL_TORRENT) {
                priority_file_indexes = rec->priority_file_indexes;
            }
        }

        // 调引擎恢复（锁外调用，避免死锁）；引擎内部经三要素自取 resume_data。
        // 状态变更由事件驱动（BT_RESUMED / 后续 HTTP_RESUMED）。
        if (IDownloadEngine *eng = engine_of(proto)) {
            eng->resume_task(natural_key, client_id_, priority_file_indexes);
            return 0;
        }
        out->code = DW_REASON_ERROR;
        out->message = utils::dup_cstr("引擎不可用");
        return -1;
    }

    int32_t TaskManager::remove(const dw_protocol_t proto, const std::string &natural_key,
                                const int32_t delete_files, dw_submit_result_t *out) {
        if (!out) return -1;
        log_i(natural_key.c_str(), "删除任务[%s] delete_files=%d",
              to_string(proto), delete_files);
        if (IDownloadEngine *eng = engine_of(proto)) {
            return eng->delete_task(natural_key, client_id_, delete_files, out);
        }
        out->code = DW_REASON_ERROR;
        out->message = utils::dup_cstr("引擎不可用");
        return -1;
    }

    int32_t TaskManager::set_priority(const dw_protocol_t proto, const std::string &natural_key,
                                      const int32_t *priority_file_indexes,
                                      const int32_t priority_file_index_size) {
        // C ABI 边界：裸指针组装为 vector 后统一走内部接口（空 vector 等价取消优先）。
        std::vector<int32_t> prios;
        if (priority_file_indexes && priority_file_index_size > 0) {
            prios.assign(priority_file_indexes, priority_file_indexes + priority_file_index_size);
        }
        {
            std::lock_guard<std::mutex> lock(mtx_);
            FileRecord *rec = load_task_record_locked(client_id_, proto, natural_key);
            if (!rec) {
                return -1;
            }
            rec->priority_file_indexes = prios;
            // 置 QUEUED，调度器调 resume_task 时携带 priority_file_indexes 生效。
            rec->status = DW_TASK_STATUS_QUEUED;
            rec->synth_notified = false;
            store_.update_file_record_status(rec->client_id, rec->task_protocol, rec->task_natural_key,
                                             DW_TASK_STATUS_QUEUED, DW_REASON_NONE, "");
        }
        // 文件进度行由 apply_file_priorities 经 build_flat_file_list 统一入库。
        cv_.notify_all();
        return 0;
    }

    std::vector<uint8_t> TaskManager::load_resume(const std::string &client_id, const dw_protocol_t proto,
                                                  const std::string &natural_key) {
        std::lock_guard<std::mutex> lock(mtx_);
        return store_.load_resume(client_id, proto, natural_key);
    }

    void TaskManager::save_resume_source(const std::string &client_id, const dw_protocol_t proto,
                                         const std::string &natural_key,
                                         const std::string &save_path,
                                         const std::string &magnet_link, const std::string &torrent_file) {
        std::lock_guard<std::mutex> lock(mtx_);
        store_.save_resume_source(client_id, proto, natural_key, save_path, magnet_link, torrent_file);
    }

    TaskStore::ResumeInfo TaskManager::load_resume_info(const std::string &client_id, const dw_protocol_t proto,
                                                        const std::string &natural_key) {
        std::lock_guard<std::mutex> lock(mtx_);
        return store_.load_resume_info(client_id, proto, natural_key);
    }

    std::string TaskManager::load_save_path(const std::string &client_id, const dw_protocol_t proto,
                                            const std::string &natural_key) {
        std::lock_guard<std::mutex> lock(mtx_);
        if (const FileRecord *rec = load_task_record_locked(client_id, proto, natural_key)) {
            return rec->save_path;
        }
        return {};
    }

    bool TaskManager::load_task_record(dw_protocol_t proto, const std::string &natural_key, FileRecord &out_record) {
        std::lock_guard<std::mutex> lock(mtx_);
        FileRecord *rec = load_task_record_locked(client_id_, proto, natural_key);
        if (rec) {
            out_record = *rec;
            return true;
        }
        return false;
    }

    void TaskManager::set_play_position(dw_protocol_t proto, const std::string &natural_key, const int32_t file_index,
                                        const int64_t position_ms) {
        std::lock_guard<std::mutex> lock(mtx_);
        // play_progress 以物理路径为键：路径解析失败（定名未落定 / handle 离线）静默丢弃。
        std::string path;
        int64_t size = 0;
        if (resolve_file_path_locked(proto, natural_key, file_index, path, size)) {
            store_.set_play_position(path, position_ms);
        }
    }

    int64_t TaskManager::get_play_position(dw_protocol_t proto, const std::string &natural_key,
                                           const int32_t file_index) {
        std::lock_guard<std::mutex> lock(mtx_);
        std::string path;
        int64_t size = 0;
        if (!resolve_file_path_locked(proto, natural_key, file_index, path, size)) return 0;
        return store_.get_play_position(path);
    }

    bool TaskManager::resolve_file_path(dw_protocol_t proto, const std::string &natural_key,
                                        int32_t file_index, std::string &out_path, int64_t &out_size) {
        std::lock_guard<std::mutex> lock(mtx_);
        return resolve_file_path_locked(proto, natural_key, file_index, out_path, out_size);
    }

    bool TaskManager::resolve_file_path_locked(dw_protocol_t proto, const std::string &natural_key,
                                               int32_t file_index, std::string &out_path,
                                               int64_t &out_size) {
        // 内存优先，file_records 兆底（状态持久化权威）。
        const FileRecord *rec_ptr = nullptr;
        FileRecord db_rec;
        const auto it = tasks_.find(union_id_of(proto, natural_key));
        if (it != tasks_.end()) {
            rec_ptr = &it->second;
        } else {
            ensure_file_cache_locked();
            const auto cit = file_cache_.find(union_id_of(client_id_, proto, natural_key));
            if (cit != file_cache_.end()) {
                db_rec = cit->second;
                rec_ptr = &db_rec;
            }
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
        std::lock_guard<std::mutex> lock(mtx_);
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
        // 经 Boost.Asio io_context::post 投递到 B 线程消费，线程安全。
        // 事件经值语义拷贝后投递，调用方无需保持数据存活。
        boost::asio::post(event_ioc_, [this, ev = std::move(event)]() mutable {
            consume_engine_event(std::move(ev));
        });
        // 唤醒 B 线程及时处理事件（不等维护周期超时）。
        cv_.notify_all();
    }

    void TaskManager::consume_engine_event(EngineEvent event) {
        // B 线程消费单个引擎事件。事件消费在锁外进行磁盘检测，锁内更新状态。
        log_d("", "consume event:%s", to_string(event).c_str());
        const std::string &key = event.engine_key;
        if (key.empty()) {
            log_e("", "engine_key 为空，丢弃事件 %s", to_string(event).c_str());
            return;
        }

        // 前置检查：部分事件可在无锁情况下提前退出。
        if (event.type == EngineEventType::TASK_FILES && event.files.empty()) {
            return;
        }

        // 加锁并加载任务记录（内存优先，DB 兆底）。
        std::lock_guard<std::mutex> lock(mtx_);
        FileRecord *rec = load_task_record_locked(client_id_, event.protocol, key);
        if (!rec) {
            log_e(key.c_str(), "事件 %s 任务不存在", to_string(event).c_str());
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
                    // 同步 file_cache
                    if (file_cache_loaded_) {
                        const std::string ck = union_id_of(rec->client_id, rec->task_protocol, rec->task_natural_key);
                        auto cit = file_cache_.find(ck);
                        if (cit != file_cache_.end()) {
                            cit->second.save_path = event.save_path;
                            cit->second.original_root_name = event.original_name;
                            cit->second.root_name = event.content_root;
                            cit->second.full_path = full_path;
                            cit->second.file_type = is_dir;
                            cit->second.ext = ext;
                            cit->second.parsed = true;
                            cit->second.modified_at = now_unix_ms();
                        }
                    }

                    log_i(key.c_str(), "解析完成 original_name='%s' content_root='%s' is_dir=%d",
                          event.original_name.c_str(), event.content_root.c_str(), is_dir);
                } else {
                    // 快路：重名判定已完成（如 storage_moved_alert 触发的二次 PARSED），
                    // 元数据已落库，仅做状态迁移。
                    log_i(key.c_str(), "解析完成(快路) content_root='%s'", rec->root_name.c_str());
                }

                // RESOLVING → QUEUED 状态迁移（即时写，等待调度器准入）
                if (rec->status == DW_TASK_STATUS_RESOLVING) {
                    rec->status = DW_TASK_STATUS_QUEUED;
                    rec->synth_notified = false;
                    store_.update_file_record_status(rec->client_id, rec->task_protocol, rec->task_natural_key,
                                                     DW_TASK_STATUS_QUEUED, DW_REASON_NONE, "");
                    schedule_needed_ = true;
                    log_i(key.c_str(), "转 QUEUED 等待调度器准入");
                    cv_.notify_all();
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
                                                 static_cast<dw_task_status_t>(rec->status), event.reason, event.message);
                log_e(key.c_str(), "下载失败 retryable=%d msg=%s", retryable, event.message.c_str());
                schedule_needed_ = true;
                break;
            }
            case EngineEventType::DOWNLOAD_COMPLETED: {
                rec->status = DW_TASK_STATUS_COMPLETED;
                rec->reason = DW_REASON_NONE;
                rec->message.clear();
                // 终态即时写
                store_.update_file_record_status(rec->client_id, rec->task_protocol, rec->task_natural_key,
                                                 DW_TASK_STATUS_COMPLETED, DW_REASON_NONE, "");
                // 清除文件进度缓存
                store_.delete_file_progress_by_task(rec->client_id, rec->task_protocol, rec->task_natural_key);
                log_i(key.c_str(), "下载完成");
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
                break;
            }
            case EngineEventType::PAUSED: {
                rec->status = DW_TASK_STATUS_PAUSED;
                rec->synth_notified = false;
                reset_live_telemetry(*rec);
                // 暂停态即时写（迁移点）
                store_.update_file_record_status(rec->client_id, rec->task_protocol, rec->task_natural_key,
                                                 DW_TASK_STATUS_PAUSED, static_cast<dw_reason_t>(rec->reason),
                                                 rec->message);
                log_i(key.c_str(), "暂停生效");
                break;
            }
            case EngineEventType::RESUMED: {
                // 引擎恢复生效：仅暂停态回 QUEUED 等待调度准入；
                // DOWNLOADING 保持（调度准入 resume 后补发的 RESUMED 不得回退权威态），
                // RESOLVING 保持（BT handle 已建等元数据，提前 resume 语义无效）。
                if (rec->status == DW_TASK_STATUS_PAUSED) {
                    rec->status = DW_TASK_STATUS_QUEUED;
                    rec->synth_notified = false;
                    store_.update_file_record_status(rec->client_id, rec->task_protocol, rec->task_natural_key,
                                                     DW_TASK_STATUS_QUEUED, DW_REASON_NONE, "");
                    schedule_needed_ = true;
                    cv_.notify_all();
                }
                log_i(key.c_str(), "恢复生效 status=%d", static_cast<int>(rec->status));
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
                // 同步 file_cache
                if (file_cache_loaded_) {
                    const std::string ck = union_id_of(rec->client_id, rec->task_protocol, rec->task_natural_key);
                    auto cit = file_cache_.find(ck);
                    if (cit != file_cache_.end()) {
                        cit->second.original_root_name = event.name;
                        cit->second.root_name = event.name;
                        cit->second.full_path = full_path;
                        cit->second.original_root_name = raw_name;
                        cit->second.modified_at = now_unix_ms();
                    }
                }
                log_i(key.c_str(), "HTTP 定名 wrapper='%s' file='%s'",
                      event.name.c_str(), raw_name.c_str());
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
                    log_d(key.c_str(), "文件进度缓存清空（完成） file_index=%d",
                          event.file_index);
                }
                break;
            }
            default:
                log_d(key.c_str(), "未处理的事件类型 type=%d",
                      static_cast<int>(event.type));
                break;
        }
    }

    /* ================================================================== */
    /*                          快照查询                                  */
    /* ================================================================== */

    int32_t TaskManager::list(dw_task_snapshot_t **out_tasks, int32_t *out_count) {
        if (!out_tasks || !out_count) return -1;

        std::lock_guard<std::mutex> lock(mtx_);

        // 先同步活跃任务进度遥测，使快照反映最新内存态（状态迁移已即时写，进度由节拍同步）。
        flush_dirty_locked();

        // 全量任务来自 file_records（状态持久化权威，含未常驻内存的暂停/完成/错误任务），
        // 仅投影任务关联记录（本地文件/目录条目不参与任务快照）。
        ensure_file_cache_locked();
        std::vector<const FileRecord *> all;
        all.reserve(file_cache_.size());
        for (const auto &[_, fr]: file_cache_) {
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
        std::lock_guard<std::mutex> lock(mtx_);
        ensure_file_cache_locked();
        std::vector<FileRecord> out;
        out.reserve(file_cache_.size());
        for (auto &[_, r]: file_cache_) out.push_back(r);
        // 按 modified_at DESC 排序（与 DB 查询一致）
        std::sort(out.begin(), out.end(), [](const FileRecord &a, const FileRecord &b) {
            return a.modified_at > b.modified_at;
        });
        return out;
    }

    void TaskManager::sync_file_record_cache(const std::string &client_id, dw_protocol_t proto,
                                             const std::string &natural_key, const FileRecord *fr) {
        // 假定调用方已持 mtx_
        if (!file_cache_loaded_) return;
        const std::string ck = union_id_of(client_id, proto, natural_key);
        if (fr) {
            file_cache_[ck] = *fr;
        } else {
            // touch：仅刷新 modified_at
            auto it = file_cache_.find(ck);
            if (it != file_cache_.end()) {
                it->second.modified_at = now_unix_ms();
            }
        }
    }

    FileRecord *TaskManager::find_file_record(const std::string &client_id, dw_protocol_t proto,
                                              const std::string &natural_key) {
        const std::string ck = union_id_of(client_id, proto, natural_key);
        const auto it = file_cache_.find(ck);
        if (it != file_cache_.end()) {
            return &it->second;
        }
        if (FileRecord fr; store_.find_file_record(client_id, proto, natural_key, fr)) {
            file_cache_[ck] = std::move(fr);
            return &file_cache_[ck];
        }
        return nullptr;
    }

    bool TaskManager::is_file_record_parsed(const std::string &client_id, dw_protocol_t proto,
                                            const std::string &natural_key) {
        std::lock_guard<std::mutex> lock(mtx_);
        FileRecord *fr = find_file_record(client_id, proto, natural_key);
        return fr && fr->parsed;
    }

    /* ================================================================== */
    /*                          调度线程                                  */
    /* ================================================================== */

    void TaskManager::scheduler_loop() {
        // 采集后同步到内存和调用回调
        while (running_.load()) {
            std::vector<FileRecord> fwd_records;
            bool wake_schedule = false;

            {
                std::unique_lock<std::mutex> lock(mtx_);
                // 超时至下一拍；stop() 置 running_=false 后由 notify_all 立即唤醒。
                cv_.wait_for(lock, std::chrono::milliseconds(flush_interval_ms_),
                             [this] { return !running_.load(); });
                if (!running_.load()) break;

                // 采集数据
                collect_progress_locked(fwd_records);
                // 采集拍产生终态（COMPLETED/ERROR）即置 schedule_needed_，记录到本地，
                // 锁外 notify 立即唤醒 B 线程调度，不等维护周期超时。
                wake_schedule = schedule_needed_;
            } // ← 作用域退出，自动释放 mtx_

            // 触发引擎续传检查点（锁外，绝不持 mtx_）：post_updates 让 Torrent 引擎
            // 携变更门槛请求续传（无变化不产生 alert，去重下沉 libtorrent）。
            // 进度推送已由引擎经 STATUS_UPDATE 事件实时完成，此处仅驱动续传检查点。
            for (IDownloadEngine *eng: {http_, torrent_}) {
                if (eng) eng->post_updates();
            }

            // wake_schedule（采集拍产生调度需求）需要唤醒 B 线程调度。
            if (wake_schedule) cv_.notify_all();

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
        while (running_.load()) {
            {
                std::unique_lock<std::mutex> lock(mtx_);
                // 停止请求 / 调度请求任一满足即唤醒；停止与否出锁后统一判。
                cv_.wait_for(lock, std::chrono::milliseconds(maintenance_interval_ms_),
                             [this] { return !running_.load() || schedule_needed_; });
                if (!running_.load()) break;

                maintenance_persist_locked();

                if (schedule_needed_) {
                    schedule_needed_ = false;
                    run_schedule(lock);
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

    void TaskManager::run_schedule(std::unique_lock<std::mutex> &lock) {
        // ---- 常规准入调度 ----
        // 周期性检查额度（仅 DOWNLOADING 占用），按 priority + created_at 顺序选取
        // QUEUED 任务调 resume_task 准入：
        //   QUEUED：元数据已落定 / 重试任务，handle 存在走快路径直接恢复下载。
        //   BT 任务添加时已同步创建 handle（RESOLVING），PARSED 事件后转 QUEUED 等待调度。
        while (running_.load() && net_allowed_) {
            if (const int32_t active = active_count_locked(); active >= max_concurrent_) {
                break;
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

            // 拷贝任务记录供锁外使用（priority_file_indexes 数据随拷贝持有）
            FileRecord copy = *best;
            lock.unlock();

            const bool ok = call_resume_task(copy);

            lock.lock();
            auto it = tasks_.find(copy.union_id());
            if (it == tasks_.end()) continue; // 锁外期间被删除，跳过
            FileRecord &rec = it->second;
            if (rec.status != copy.status) continue; // 期间已被事件/用户操作迁移，保持权威态
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
            std::lock_guard<std::mutex> lock(mtx_);
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
            cv_.notify_all();
            return;
        }

        // 锁外逐任务停传输（pause_task 内部可能 join / 加引擎锁，不可持 mtx_ 调用以规避死锁）。
        for (const auto &pp: to_pause) {
            dw_submit_result_t res{};
            if (IDownloadEngine *eng = engine_of(pp.protocol)) {
                eng->pause_task(pp.key, client_id_, &res);
            }
            dw_submit_result_release(&res);
        }

        // 引擎已停，回落 QUEUED 待闸门开启后重启；遥测归零，置 schedule_needed_ 由调度线程单点发射 QUEUED 合成帧。
        {
            std::lock_guard<std::mutex> lock(mtx_);
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
        cv_.notify_all();
    }


    void TaskManager::set_max_concurrent(int32_t value) {
        const int32_t next = value > 0 ? value : 3;
        {
            std::lock_guard<std::mutex> lock(mtx_);
            if (max_concurrent_ == next) return; // 未变，幂等跳过
            max_concurrent_ = next;
            schedule_needed_ = true; // 调高时由调度线程准入 QUEUED 任务
        }
        cv_.notify_all();
        log_i("", "[EVENT] 并发上限调整: max_concurrent=%d", next);
    }


    // ---- 任务文件实时查询 ----

    utils::file_array TaskManager::load_files(dw_protocol_t proto, const std::string &natural_key) {
        // task_files 表已移除（磁盘为事实源）：BT 经 handle 实时查询，HTTP 从任务记录推导。
        std::lock_guard<std::mutex> lock(mtx_);
        utils::file_array result{nullptr, 0};
        if (proto == DW_PROTOCOL_TORRENT) {
            if (torrent_) {
                result = torrent_->get_file_list(natural_key); // 选中文件，pad 已过滤
            }
        } else {
            // HTTP 单文件：root_name（wrapper 目录）+ original_root_name（原始文件名）落定后可以推导。
            const FileRecord *rec_ptr = nullptr;
            FileRecord db_rec;
            const auto it = tasks_.find(union_id_of(proto, natural_key));
            if (it != tasks_.end()) {
                rec_ptr = &it->second;
            } else {
                ensure_file_cache_locked();
                const auto cit = file_cache_.find(union_id_of(client_id_, proto, natural_key));
                if (cit != file_cache_.end()) {
                    db_rec = cit->second;
                    rec_ptr = &db_rec;
                }
            }
            if (rec_ptr && !rec_ptr->root_name.empty() && !rec_ptr->original_root_name.empty()) {
                dw_file_info_t *arr = utils::alloc_file_list(1);
                if (arr) {
                    arr[0].index = 0;
                    arr[0].name = utils::dup_cstr(rec_ptr->original_root_name);
                    arr[0].full_path = utils::dup_cstr((std::filesystem::path(rec_ptr->save_path) /
                                                        rec_ptr->root_name / rec_ptr->original_root_name).string());
                    const std::string ext = utils::file_extension(rec_ptr->original_root_name);
                    arr[0].ext = ext.empty() ? nullptr : utils::dup_cstr(ext);
                    arr[0].size = rec_ptr->total_size;
                    arr[0].offset = 0; // HTTP 单文件模型，全局偏移恒为 0
                    arr[0].status = rec_ptr->status == DW_TASK_STATUS_COMPLETED ? 2 : 0;
                    result = {arr, 1};
                }
            }
        }
        // downloaded_bytes 聚合回填：进度缓存区间和；未命中（未开始/已完成清缓存）保持 0。
        if (result.first && result.second > 0) {
            for (int32_t i = 0; i < result.second; ++i) {
                result.first[i].downloaded_bytes = store_.sum_file_progress_by_file(
                    client_id_, proto, natural_key, result.first[i].index);
            }
        }
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

        std::lock_guard<std::mutex> lock(mtx_);

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
            // 同步 file_cache
            if (file_cache_loaded_) {
                file_cache_[union_id_of(fr.client_id, fr.task_protocol, fr.task_natural_key)] = fr;
            }
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

        std::lock_guard<std::mutex> lock(mtx_);

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
                // 同步 file_cache：按 save_path + root_name 定位
                if (file_cache_loaded_) {
                    for (auto &[_, cr]: file_cache_) {
                        if (cr.save_path == f.save_path && cr.root_name == f.root_name) {
                            cr.status = DW_TASK_STATUS_INVALIDATED;
                            cr.modified_at = now_unix_ms();
                            break;
                        }
                    }
                }
                ++invalidated;
            }
        }

        if (out_invalidated_count) *out_invalidated_count = invalidated;
        return 0;
    }

    int32_t TaskManager::clear_local_tasks(const std::string &save_path) {
        if (save_path.empty()) return -1;

        std::lock_guard<std::mutex> lock(mtx_);

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
        // 同步 file_cache：移除该 save_path 下的本地文件条目缓存（与 DB 删除条件一致）
        if (file_cache_loaded_) {
            for (auto it = file_cache_.begin(); it != file_cache_.end();) {
                if (it->second.save_path == save_path && it->second.type == DW_SOURCE_LOCAL_FILE) {
                    it = file_cache_.erase(it);
                } else {
                    ++it;
                }
            }
        }
        return 0;
    }

    int32_t TaskManager::delete_local_entry(const std::string &save_path, const std::string &root_name) {
        std::lock_guard<std::mutex> lock(mtx_);

        if (save_path.empty() || root_name.empty()) return -1;

        // 类型门禁：仅允许删除本地文件条目。HTTP / BT 任务必须走 dw_delete_task，
        // 否则会越过 engine 层删文件，留下引擎内仍在运行的孤儿任务。
        ensure_file_cache_locked();
        const FileRecord *target = nullptr;
        for (const auto &[_, cr]: file_cache_) {
            if (cr.save_path == save_path && cr.root_name == root_name) {
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
        // 同步 file_cache：按 save_path + root_name 定位并移除
        for (auto it = file_cache_.begin(); it != file_cache_.end(); ++it) {
            if (it->second.save_path == save_path && it->second.root_name == root_name) {
                file_cache_.erase(it);
                break;
            }
        }
        return ec ? -1 : 0;
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
                // 同步 file_cache
                if (file_cache_loaded_) {
                    const std::string ck = union_id_of(task_record.client_id, task_record.task_protocol,
                                                       task_record.task_natural_key);
                    auto cit = file_cache_.find(ck);
                    if (cit != file_cache_.end()) {
                        cit->second.status = task_record.status;
                        cit->second.total_size = task_record.total_size;
                        cit->second.total_done = task_record.total_done;
                        cit->second.reason = task_record.reason;
                        cit->second.message = task_record.message;
                        cit->second.modified_at = now_unix_ms();
                    }
                }
            }
        }
    }

    void TaskManager::ensure_file_cache_locked() {
        // 懒加载：首次访问从 DB 全量载入（含本地文件条目），后续幂等跳过。
        if (file_cache_loaded_) return;
        file_cache_.clear();
        for (auto &fr: store_.load_file_records(client_id_)) {
            file_cache_[union_id_of(fr.client_id, fr.task_protocol, fr.task_natural_key)] = std::move(fr);
        }
        file_cache_loaded_ = true;
    }

    void TaskManager::register_task(FileRecord task_record) {
        // tasks_ 以 union_id 为 key，无冗余索引。
        tasks_[task_record.union_id()] = std::move(task_record);
    }

    FileRecord *TaskManager::load_task_record_locked(const std::string &client_id, const dw_protocol_t proto,
                                                     const std::string &natural_key) {
        const std::string uid = union_id_of(client_id, proto, natural_key);
        // 内存命中：直接返回指针。
        if (const auto it = tasks_.find(uid); it != tasks_.end()) {
            return &it->second;
        }
        // 从 file_records（状态持久化权威）重建。
        ensure_file_cache_locked();
        const auto cit = file_cache_.find(uid);
        if (cit == file_cache_.end()) {
            return nullptr;
        }
        FileRecord record = cit->second;
        register_task(std::move(record));
        // 返回内存中的指针。
        if (const auto it = tasks_.find(uid); it != tasks_.end()) {
            return &it->second;
        }
        return nullptr;
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
            log_i(task_record.task_natural_key.c_str(), "数据驱动任务完成 union_id=%s",
                  task_record.union_id().c_str());
        }
    }

    std::vector<dw_byte_range_t> TaskManager::get_cached_segments(dw_protocol_t proto, const std::string &natural_key,
                                                                  int32_t file_index) {
        // 内存缓存已移除：直接读 DB 最新快照（经物理路径查询）。
        std::lock_guard<std::mutex> lock(mtx_);
        std::string path;
        int64_t size = 0;
        if (!resolve_file_path_locked(proto, natural_key, file_index, path, size)) return {};
        return store_.load_segments(path, file_index);
    }

    int32_t TaskManager::get_task_status(dw_protocol_t proto, const std::string &natural_key) {
        std::lock_guard<std::mutex> lock(mtx_);
        const auto it = tasks_.find(union_id_of(proto, natural_key));
        if (it == tasks_.end()) return -1;
        return static_cast<int32_t>(it->second.status);
    }
} // namespace dw
