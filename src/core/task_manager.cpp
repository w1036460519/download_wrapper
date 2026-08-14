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
 *   - 唯一键 = (client_id, key_type, natural_key) 三元组（client_id 来自 dw_config_t，启动时注入）；
 *   - tasks_ 单 map<TaskKey, TaskRecord> 索引，引擎回调按 (client_id, key_type, engine_key) 构造 TaskKey 后 O(1) 查表；
 *   - 不再维护 url_index_ / info_hash_index_（冗余索引已合并到复合键哈希）。
 */

#include "task_manager.h"

#include "internal/downloader_internal.h"
#include "internal/engine_interface.h"
#include "utils/time_util.h"
#include "utils/unique_name.h"
#include "utils/string_util.h"

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <optional>
#include <ranges>
#include <unordered_set>

#include <boost/asio.hpp>

namespace dw {
    using utils::now_unix_ms;

    namespace {
        /// 内部 ID 生成辅助（仅 wrapper 内部场景使用，例：本地文件扫描占位）。
        /// 格式：毫秒时间戳 hex + 原子计数器，保证同进程内唯一。
        std::string generate_internal_task_id() {
            static std::atomic<uint64_t> counter{0};
            const auto ms = static_cast<uint64_t>(now_unix_ms());
            char buf[32];
            std::snprintf(buf, sizeof(buf), "%016llx%08llx",
                          static_cast<unsigned long long>(ms),
                          static_cast<unsigned long long>(counter.fetch_add(1)));
            return std::string(buf);
        }

        /// 由协议枚举推导 TaskKeyType。
        TaskKeyType protocol_to_key_type(const dw_protocol_t proto) {
            return proto == DW_PROTOCOL_HTTP ? TaskKeyType::HTTP : TaskKeyType::BT;
        }

        /// 由 TaskRecord 推 engine_key：HTTP 取 url，BT 取 info_hash。
        const std::string &engine_key(const TaskRecord &task_record) {
            return task_record.protocol == DW_PROTOCOL_HTTP ? task_record.url : task_record.info_hash;
        }

        /// 占用下载额度的状态集合：活跃态 {DOWNLOADING, RESOLVING, PARSED}。
        /// RESOLVING（BT 等待元数据）和 PARSED（元数据就绪待准入）均占用名额防止超发；
        /// 其余状态（QUEUED / PAUSED / COMPLETED / ERROR）均不占额度。
        bool status_occupies_slot(dw_task_status_t s) {
            return s == DW_TASK_STATUS_DOWNLOADING ||
                   s == DW_TASK_STATUS_RESOLVING ||
                   s == DW_TASK_STATUS_PARSED;
        }

        /// 已下载区间是否完全覆盖文件 [0, file_size-1]（区间须已合并且按 start 升序）。
        /// file_size <= 0 时无法判定，返回 false（HTTP chunked 无总长场景）。
        bool is_file_complete(const std::vector<dw_byte_range_t> &segs, int64_t file_size) {
            if (segs.empty() || file_size <= 0) return false;
            int64_t cursor = 0;
            for (const auto &s : segs) {
                if (s.start > cursor) return false; // 有间隙
                cursor = std::max(cursor, s.end + 1);
                if (cursor >= file_size) return true;
            }
            return false;
        }

        /// std::string 拷贝为堆分配 C 字符串（供快照数组使用，调用方 free）。
        char *dup_cstr(const std::string &s) {
            char *p = static_cast<char *>(std::malloc(s.size() + 1));
            if (p) std::memcpy(p, s.c_str(), s.size() + 1);
            return p;
        }

        /// 为 dw_task_key_t.natural_key 堆分配 C 字符串副本（out->key 回填时使用）。
        /// 由 dw_submit_result_release 统一释放，避免直接拷贝 std::string 指针导致悬空。
        char *dup_natural_key(const std::string &s) {
            return dup_cstr(s);
        }

        /// TaskKey 是否表示"空键"（未命中查表结果）。
        bool task_key_is_empty(const TaskKey &k) {
            return k.client_id.empty() && k.natural_key.empty();
        }
    } // namespace

    TaskManager::~TaskManager() {
        stop();
    }

    void TaskManager::set_engines(IDownloadEngine *http, IDownloadEngine *torrent) {
        http_ = http;
        torrent_ = torrent;
    }

    void TaskManager::set_progress_cb(dw_progress_cb cb) {
        std::lock_guard<std::mutex> lock(mtx_);
        progress_cb_ = cb;
    }

    /* ================================================================== */
    /*                          生命周期                                  */
    /* ================================================================== */

    int32_t TaskManager::start(const dw_config_t &cfg) {
        max_concurrent_ = cfg.max_concurrent_downloads > 0
                              ? cfg.max_concurrent_downloads
                              : 3;
        flush_interval_ms_ = cfg.status_callback_interval_ms > 0
                                 ? cfg.status_callback_interval_ms
                                 : 1000;
        maintenance_interval_ms_ = flush_interval_ms_ * 2;

        // 注入 clientId：App 启动时从 SharedPreferences 读取并经 dw_config_t 传入。
        // 此后本 session 创建/加载的所有任务均归此 clientId 隔离。
        client_id_ = cfg.client_id ? cfg.client_id : "";
        if (client_id_.empty()) {
            DW_LOG(DW_LOG_ERROR, "TaskManager clientId 缺失，启动失败", "");
            return -1;
        }

        const std::string dir = cfg.work_dir && cfg.work_dir[0] ? cfg.work_dir : ".";
        const std::string story_path = dir + "/leopard_tasks.db";

        std::lock_guard<std::mutex> lock(mtx_);
        if (!store_.open(story_path)) {
            DW_LOGF(DW_LOG_ERROR, "", "TaskManager 打开数据库失败: %s",
                    story_path.c_str());
            return -1;
        }
        store_.init_schema();
        // 全量 load：DB 不过滤；应用层在登记到 tasks_ 时按 client_id_ 过滤，
        // 其他客户端的历史任务仍落库但不入内存注册表（remote 处理暂不考虑）。
        for (auto &task_record: store_.load_active()) {
            if (task_record.task_key.client_id != client_id_) continue;
            register_task(std::move(task_record));
        }

        // 首次加载时将下载中/解析中的任务设置为队列中等待调度
        for (auto &task_record: tasks_ | std::views::values) {
            if (task_record.status == DW_TASK_STATUS_DOWNLOADING ||
                task_record.status == DW_TASK_STATUS_RESOLVING) {
                task_record.status = DW_TASK_STATUS_QUEUED;
                task_record.synth_notified = false;
                store_.update(task_record);
            }
        }

        running_.store(true);
        schedule_needed_ = true;
        worker_ = std::thread(&TaskManager::scheduler_loop, this);
        maintenance_ = std::thread(&TaskManager::maintenance_loop, this);

        DW_LOGF(DW_LOG_INFO, "", "TaskManager 启动 clientId=%s tasks=%zu concurrent=%d",
                client_id_.c_str(), tasks_.size(), max_concurrent_);
        return 0;
    }

    void TaskManager::stop() {
        if (!running_.exchange(false)) {
            return;
        }
        cv_.notify_all();
        if (worker_.joinable()) {
            worker_.join();
        }
        if (maintenance_.joinable()) {
            maintenance_.join();
        }

        // 停机兜底：强制处理残余待删项（跳过释放确认，尽力删除一次）。
        process_pending_deletes(true);

        std::lock_guard<std::mutex> lock(mtx_);
        flush_dirty_locked();
        store_.close();
        DW_LOG(DW_LOG_INFO, "TaskManager 已停止", "");
    }

    /* ================================================================== */
    /*                          控制操作                                  */
    /* ================================================================== */

    int32_t TaskManager::add(const dw_protocol_t proto, const dw_task_params_t *params,
                             dw_submit_result_t *out) {
        if (!params || !out) {
            if (out) {
                out->code = DW_REASON_ERROR;
                out->message = nullptr;
            }
            return -1;
        }
        // add_task 不接受 resume_data：仅 resume 路径负责。预入是为避免 libtorrent
        // 误用历史存档造成文件错位（与 TaskManager 状态机冲突）。
        if (params->resume_data != nullptr && params->resume_data_size > 0) {
            DW_LOGF(DW_LOG_INFO, "",
                    "add_task 忽略 resume_data（仅 resume_task 接受） key=%s",
                    (proto == DW_PROTOCOL_HTTP ? params->url : params->info_hash));
        }
        const char *raw_key = proto == DW_PROTOCOL_HTTP ? params->url : params->info_hash;
        if (!raw_key || !raw_key[0]) {
            out->code = DW_REASON_ERROR;
            out->message = nullptr;
            return -1;
        }

        // add_task 同步语义（仅全新任务路径）：调用返回时任务已达 QUEUED 状态。
        // 状态机：RESOLVING (add_torrent) → PARSED (元数据就绪) → QUEUED (判重/定名完成)。
        // 同 key 重复添加 / 库中已存在任务快路仅改状态为 QUEUED，不重复入引擎
        // （与 resume_task 的 handle 检查逻辑重叠，但接口语义保留兼容）。
        constexpr auto kAddTimeout = std::chrono::minutes(10);

        TaskKey out_key;
        bool need_engine_start = false;
        TaskRecord to_start; // 仅 need_engine_start=true 时填充
        {
            std::unique_lock<std::mutex> lock(mtx_);
            const std::string key = raw_key;

            // 同 key 重新添加：撤销尚未执行的待删文件意图，防旧删除项在新任务
            // 释放后误删其落盘产物。
            if (pending_deletes_.erase(key) > 0) {
                DW_LOGF(DW_LOG_INFO, "",
                        "重新添加撤销待删文件意图 key=%s", key.c_str());
            }

            if (const TaskKey existing = task_key_of_engine_key(proto, key);
                !task_key_is_empty(existing)) {
                // 活跃任务：直接引用内存中的现有对象。引擎侧 handle 仍存在，
                // 重复 add 仅重置状态为 QUEUED（与原设计一致）。
                TaskRecord &task_record = tasks_[existing];
                if (!status_occupies_slot(task_record.status) &&
                    task_record.status != DW_TASK_STATUS_COMPLETED) {
                    reset_error_task_for_restart(task_record);
                    task_record.status = DW_TASK_STATUS_QUEUED;
                    task_record.synth_notified = false;
                    task_record.created_at = now_unix_ms();
                    store_.update(task_record);
                }
                out_key = task_record.task_key;
                schedule_needed_ = true;
            } else if (TaskRecord task_record;
                       proto == DW_PROTOCOL_HTTP
                           ? store_.load_by_url(client_id_, key, task_record)
                           : store_.load_by_info_hash(client_id_, key, task_record)) {
                // 从库中取历史任务重新入队：引擎侧 handle 可能丢失（分享率回收/进程重启），
                // 本路径与 resume_task 行为重叠（resume_data 为空）——走 start_engine_task
                // 重建并同步等 QUEUED。
                out_key = task_record.task_key;
                if (task_record.status != DW_TASK_STATUS_COMPLETED) {
                    reset_error_task_for_restart(task_record);
                    task_record.status = DW_TASK_STATUS_RESOLVING;
                    task_record.synth_notified = false;
                    task_record.created_at = now_unix_ms();
                    store_.update(task_record);
                    register_task(task_record); // 先占用位置锁外重建
                    to_start = task_record;
                    need_engine_start = true;
                    schedule_needed_ = true;
                }
            } else {
                // 全新任务：构造 TaskKey 三元组（client_id_ 来自 session 配置）
                task_record.task_key.client_id = client_id_;
                task_record.task_key.key_type = protocol_to_key_type(proto);
                task_record.task_key.natural_key = key;
                task_record.protocol = proto;
                if (proto == DW_PROTOCOL_TORRENT) task_record.info_hash = key;
                else task_record.url = key;
                task_record.save_path = params->save_path ? params->save_path : "";
                // filename 为定名凭证（非空即已判重定名并已物化磁盘占位），恒由定名链
                // 写入：HTTP 经引擎探测上调、BT 经 RESOLVING 校验拍。add 一律不预填，
                // 外部指定名不被接受——预填会产生无占位的凭证，令该名对判重不可见。
                task_record.magnet_link = params->magnet_link ? params->magnet_link : "";
                task_record.torrent_file = params->torrent_file ? params->torrent_file : "";
                if (params->trackers && params->tracker_count > 0) {
                    for (int32_t i = 0; i < params->tracker_count; ++i) {
                        if (params->trackers[i]) task_record.trackers.emplace_back(params->trackers[i]);
                    }
                }
                if (params->file_indexes && params->file_index_size > 0) {
                    task_record.file_indexes.assign(params->file_indexes,
                                                    params->file_indexes + params->file_index_size);
                }
                task_record.priority = params->priority;
                task_record.source = params->source;
                task_record.created_at = now_unix_ms();
                // 初始显示名占位为识别键（HTTP url / BT info_hash / magnet link），
                // 待引擎回报真实名后经事件同步。
                task_record.name = key;
                // 全新任务：状态 RESOLVING（与原 QUEUED 不同：用于在 PARSED handler
                // 中走状态守卫 RESOLVING → PARSED → QUEUED。resume_data 走 start_engine_task
                // 不接收，由 resume_task 负责）。
                task_record.status = DW_TASK_STATUS_RESOLVING;
                task_record.synth_notified = false;
                store_.insert(task_record);
                out_key = task_record.task_key;
                to_start = task_record;
                register_task(std::move(task_record));
                need_engine_start = true;
                schedule_needed_ = true;
            }
        }
        // 唤醒调度器（现有/从库任务亦需 run_schedule 拉起）。
        cv_.notify_all();

        // 全新/从库重建任务：同步调 start_engine_task 入引擎（锁外，引擎回调不占 mtx_），
        // 随后持锁同步等 PARSED→QUEUED 收敛（resume_data 一律不携带）。
        if (need_engine_start) {
            const bool ok = start_engine_task(to_start, {});
            if (!ok) {
                std::lock_guard<std::mutex> lock(mtx_);
                if (auto it = tasks_.find(out_key); it != tasks_.end()) {
                    it->second.status = DW_TASK_STATUS_ERROR;
                    it->second.reason = DW_REASON_ERROR;
                    it->second.message = "引擎添加失败";
                    store_.update(it->second);
                }
                out->code = DW_REASON_ERROR;
                out->message = nullptr;
                return -1;
            }
            // 同步等 status 收敛：BT 走 PARSED→QUEUED，HTTP 走 RESOLVING→DOWNLOADING
            // （HTTP 引擎不推 PARSED 事件，状态机直接跳过 QUEUED）。
            std::unique_lock<std::mutex> lock(mtx_);
            const std::initializer_list<dw_task_status_t> targets = {
                DW_TASK_STATUS_QUEUED, DW_TASK_STATUS_DOWNLOADING};
            const bool reached = await_status_locked(lock, out_key, targets, kAddTimeout);
            if (!reached) {
                if (auto it = tasks_.find(out_key); it != tasks_.end()) {
                    if (it->second.status != DW_TASK_STATUS_ERROR) {
                        it->second.status = DW_TASK_STATUS_ERROR;
                        it->second.reason = DW_REASON_ERROR;
                        it->second.message = "等待 QUEUED 超时";
                        store_.update(it->second);
                    }
                }
                out->code = DW_REASON_ERROR;
                out->message = nullptr;
                return -1;
            }
        }

        out->code = DW_REASON_NONE;
        out->message = nullptr;
        out->key.key_type = static_cast<dw_key_type_t>(out_key.key_type);
        out->key.natural_key = dup_natural_key(out_key.natural_key);
        return 0;
    }

    int32_t TaskManager::pause(const dw_protocol_t proto, const TaskKey &key,
                               dw_submit_result_t *out) {
        if (!out) return -1;

        std::string engine_k;
        {
            std::lock_guard<std::mutex> lock(mtx_);
            // 仅活跃/排队任务（常驻内存）可暂停；暂停/终态任务视为无效操作。
            const auto it = tasks_.find(key);
            if (it == tasks_.end()) {
                out->code = DW_REASON_ERROR;
                out->message = nullptr;
                return -1;
            }
            engine_k = engine_key(it->second);
            // 暂停仅改内存态与标志：两引擎 pause 均非销毁（HTTP worker 自退 + ctx 待 sweep，
            // BT handle 常驻 session）。落库交 B 的 flush_dirty_locked，区间快照 / 内存逐出
            // 交 B 的 maintenance_persist_locked，PAUSED 帧回调交 A 的 collect_progress_locked 合成。
            it->second.status = DW_TASK_STATUS_PAUSED;
            it->second.synth_notified = false; // A 线程合成一次 PAUSED 帧后置位
            it->second.dirty = true; // 待 B flush 落库
            reset_live_telemetry(it->second); // 暂停帧不残留旧速率
        }

        dw_submit_result_t r{};
        if (IDownloadEngine *eng = engine_of(proto)) eng->pause_task(engine_k.c_str(), &r);
        dw_submit_result_release(&r);

        {
            std::lock_guard<std::mutex> lock(mtx_);
            schedule_needed_ = true;
        }
        cv_.notify_all();

        out->code = DW_REASON_NONE;
        out->message = nullptr;
        out->key.key_type = static_cast<dw_key_type_t>(key.key_type);
        out->key.natural_key = dup_natural_key(key.natural_key);
        return 0;
    }

    int32_t TaskManager::resume(dw_protocol_t /*proto*/, const TaskKey &key,
                                dw_submit_result_t *out) {
        if (!out) return -1;

        // resume_task 同步语义（仅"需重建 handle"路径）：返回时任务已达 QUEUED 状态。
        // 状态机：RESOLVING (start_engine_task) → PARSED (元数据/文件就绪) → QUEUED (判重/定名完成)。
        // 三个分支：
        //   A) 内存中 + handle 有效：仅改状态为 QUEUED，快路返回（不入引擎，run_schedule 后续择机准出）；
        //   B) 内存中 + handle 无效：状态 RESOLVING，调 start_engine_task 携 resume_data 重建；
        //   C) 从库加载：必然不在内存中（handle 缺失），按 TaskKey 回读后走重建。
        constexpr auto kResumeTimeout = std::chrono::minutes(10);

        TaskKey out_key = key;
        bool need_engine_start = false;
        TaskRecord to_start;                       // 仅 need_engine_start=true 时填充
        std::vector<uint8_t> resume_data;          // 仅 need_engine_start=true 时填充

        {
            std::lock_guard<std::mutex> lock(mtx_);
            if (auto it = tasks_.find(key); it != tasks_.end()) {
                // 分支 A/B：内存中已有记录。检查引擎 handle 有效性。
                TaskRecord &rec = it->second;
                const std::string ek = engine_key(rec);
                if (IDownloadEngine *eng = engine_of(rec.protocol);
                    eng && !eng->task_released(ek.c_str())) {
                    // A) handle 有效：仅改状态为 QUEUED。
                    rec.status = DW_TASK_STATUS_QUEUED;
                    rec.synth_notified = false;
                    store_.update(rec);
                    schedule_needed_ = true;
                } else {
                    // B) handle 无效：走重建路径。状态 RESOLVING（与 add_task 全新路径同形），
                    // 锁外调 start_engine_task 携 resume_data 重建。
                    rec.status = DW_TASK_STATUS_RESOLVING;
                    rec.synth_notified = false;
                    rec.created_at = now_unix_ms();
                    store_.update(rec);
                    to_start = rec;
                    resume_data = store_.load_resume(rec.task_key);
                    need_engine_start = true;
                }
            } else {
                // 分支 C：从库加载。resume_task 必不命中（未在内存中），按 TaskKey 回读全字段。
                TaskRecord task_record;
                if (!store_.load_by_task_key(key, task_record)) {
                    out->code = DW_REASON_ERROR;
                    out->message = nullptr;
                    return -1;
                }
                // 已完成任务不再 resume。
                if (task_record.status == DW_TASK_STATUS_COMPLETED) {
                    out->code = DW_REASON_ERROR;
                    out->message = nullptr;
                    return -1;
                }
                reset_error_task_for_restart(task_record);
                task_record.status = DW_TASK_STATUS_RESOLVING;
                task_record.synth_notified = false;
                task_record.created_at = now_unix_ms();
                store_.update(task_record);
                register_task(task_record);
                to_start = task_record;
                resume_data = store_.load_resume(task_record.task_key);
                need_engine_start = true;
                schedule_needed_ = true;
            }
        }
        cv_.notify_all();

        // 重建路径：锁外调 start_engine_task 携 resume_data 入引擎，随后持锁同步等 PARSED→QUEUED 收敛。
        if (need_engine_start) {
            const bool ok = start_engine_task(to_start, resume_data);
            if (!ok) {
                std::lock_guard<std::mutex> lock(mtx_);
                if (auto it = tasks_.find(out_key); it != tasks_.end()) {
                    it->second.status = DW_TASK_STATUS_ERROR;
                    it->second.reason = DW_REASON_ERROR;
                    it->second.message = "引擎恢复失败";
                    store_.update(it->second);
                }
                out->code = DW_REASON_ERROR;
                out->message = nullptr;
                return -1;
            }
            std::unique_lock<std::mutex> lock(mtx_);
            const std::initializer_list<dw_task_status_t> targets = {
                DW_TASK_STATUS_QUEUED, DW_TASK_STATUS_DOWNLOADING};
            const bool reached = await_status_locked(lock, out_key, targets, kResumeTimeout);
            if (!reached) {
                if (auto it = tasks_.find(out_key); it != tasks_.end()) {
                    if (it->second.status != DW_TASK_STATUS_ERROR) {
                        it->second.status = DW_TASK_STATUS_ERROR;
                        it->second.reason = DW_REASON_ERROR;
                        it->second.message = "等待 QUEUED 超时";
                        store_.update(it->second);
                    }
                }
                out->code = DW_REASON_ERROR;
                out->message = nullptr;
                return -1;
            }
        }

        out->code = DW_REASON_NONE;
        out->message = nullptr;
        out->key.key_type = static_cast<dw_key_type_t>(key.key_type);
        out->key.natural_key = dup_natural_key(key.natural_key);
        return 0;
    }

    int32_t TaskManager::remove(const dw_protocol_t proto, const TaskKey &key,
                                const int32_t delete_files, dw_submit_result_t *out) {
        if (!out) return -1;

        std::string engine_k;
        std::string save_path, filename; // 登记待删文件项所需的落盘路径要素
        {
            std::lock_guard<std::mutex> lock(mtx_);
            if (const auto it = tasks_.find(key);
                it != tasks_.end()) {
                engine_k = engine_key(it->second);
                save_path = it->second.save_path;
                filename = it->second.filename;
                unregister_task(key);
            } else {
                TaskRecord task_record;
                if (!store_.load_by_task_key(key, task_record)) {
                    out->code = DW_REASON_ERROR;
                    out->message = nullptr;
                    return -1;
                }
                engine_k = engine_key(task_record);
                save_path = task_record.save_path;
                filename = task_record.filename;
            }
            store_.remove(key);
            if (!filename.empty()) {
                // filename 为定名凭证：为空即从未定名占位，无需处理。
                // save_path 已含包层目录（冲突时直接追加），直接拼接 filename。
                // delete_files=1 删整棵产物；=0 仅回收仍为空的占位条目（放弃该名字的持有），
                // 已有真实数据则原样保留。两者均登记待删项，由 B 线程在引擎确认资源
                // 释放（task_released）后执行，避免删到引擎仍持有的路径。
                pending_deletes_[engine_k] = PendingFileDelete{
                        proto, (std::filesystem::path(save_path) / filename).string(),
                        delete_files == 0};
            }
        }

        // 引擎仅释放运行时资源（0=已接管释放，1=未持有即已释放）；文件删除统一由
        // B 线程按上方登记项处理，此处无需按返回值分支。
        dw_submit_result_t r{};
        if (IDownloadEngine *eng = engine_of(proto)) eng->delete_task(engine_k.c_str(), &r);
        dw_submit_result_release(&r);

        {
            std::lock_guard<std::mutex> lock(mtx_);
            schedule_needed_ = true;
        }
        cv_.notify_all();

        out->code = DW_REASON_NONE;
        out->message = nullptr;
        out->key.key_type = static_cast<dw_key_type_t>(key.key_type);
        out->key.natural_key = dup_natural_key(key.natural_key);
        return 0;
    }

    int32_t TaskManager::set_priority(const TaskKey &key, const int32_t priority) {
        {
            std::lock_guard<std::mutex> lock(mtx_);
            if (const auto it = tasks_.find(key);
                it != tasks_.end()) {
                it->second.priority = priority;
                store_.update(it->second);
            } else {
                TaskRecord task_record;
                if (!store_.load_by_task_key(key, task_record)) return -1;
                task_record.priority = priority;
                store_.update(task_record);
            }
            schedule_needed_ = true;
        }
        cv_.notify_all();
        return 0;
    }

    bool TaskManager::engine_key_of(const TaskKey &key, std::string &out_key) {
        std::lock_guard<std::mutex> lock(mtx_);
        if (const auto it = tasks_.find(key); it != tasks_.end()) {
            out_key = engine_key(it->second);
            return true;
        }
        TaskRecord task_record;
        if (!store_.load_by_task_key(key, task_record)) return false;
        out_key = engine_key(task_record);
        return true;
    }

    bool TaskManager::engine_ref_of(const TaskKey &key, std::string &out_key, dw_protocol_t &out_proto) {
        std::lock_guard<std::mutex> lock(mtx_);
        if (const auto it = tasks_.find(key); it != tasks_.end()) {
            out_key = engine_key(it->second);
            out_proto = it->second.protocol;
            return true;
        }
        TaskRecord task_record;
        if (!store_.load_by_task_key(key, task_record)) return false;
        out_key = engine_key(task_record);
        out_proto = task_record.protocol;
        return true;
    }

    void TaskManager::set_play_position(const TaskKey &key, const int32_t file_index, const int64_t position_ms) {
        std::lock_guard<std::mutex> lock(mtx_);
        store_.set_play_position(key, file_index, position_ms);
    }

    int64_t TaskManager::get_play_position(const TaskKey &key, const int32_t file_index) {
        std::lock_guard<std::mutex> lock(mtx_);
        return store_.get_play_position(key, file_index);
    }

    std::vector<dw_byte_range_t> TaskManager::load_segments(const TaskKey &key, const int32_t file_index) {
        std::lock_guard<std::mutex> lock(mtx_);
        return store_.load_segments(key, file_index);
    }

    /* ================================================================== */
    /*                          回调拦截                                  */
    /* ================================================================== */

    TaskKey TaskManager::on_resume_data(const char *engine_key, const dw_protocol_t proto,
                                        const uint8_t *data, const size_t size) {
        if (!engine_key || !data || size == 0) return {};
        std::lock_guard<std::mutex> lock(mtx_);
        const TaskKey key = task_key_of_engine_key(proto, engine_key);
        if (!task_key_is_empty(key)) {
            if (const auto it = tasks_.find(key); it != tasks_.end()) {
                // 常规路径：任务在内存中，暂存 pending_resume，由 B 线程 flush_dirty_locked 落库。
                it->second.pending_resume.assign(reinterpret_cast<const char *>(data), size);
                return key;
            }
        }
        // 兜底路径：任务已从内存逐出（如完成移出后 resume 晚到）。不重新加载入内存，
        // 查库确认记录仍存在则直接落库；库中也不存在（已删除）则丢弃不处理。
        TaskRecord task_record;
        const bool found = (proto == DW_PROTOCOL_HTTP)
                               ? store_.load_by_url(client_id_, engine_key, task_record)
                               : store_.load_by_info_hash(client_id_, engine_key, task_record);
        if (!found) return {};
        store_.save_resume(task_record.task_key, data, size);
        return task_record.task_key;
    }

    void TaskManager::on_task_files(const char *engine_key, const dw_protocol_t proto,
                                    const dw_file_info_t *files, const int32_t count) {
        if (!engine_key || !files || count <= 0) return;
        std::lock_guard<std::mutex> lock(mtx_);
        TaskKey key = task_key_of_engine_key(proto, engine_key);
        if (task_key_is_empty(key)) {
            TaskRecord task_record;
            const bool found = (proto == DW_PROTOCOL_HTTP)
                                   ? store_.load_by_url(client_id_, engine_key, task_record)
                                   : store_.load_by_info_hash(client_id_, engine_key, task_record);
            if (found) key = task_record.task_key;
        }
        if (task_key_is_empty(key)) return; // 非已知任务，丢弃
        // 浅拷贝为 vector（仅拷贝指针，字符串所有权仍属调用方），库内自行深拷落库。
        const std::vector<dw_file_info_t> file_vec(files, files + count);
        store_.save_task_files(key, file_vec);
    }

    void TaskManager::on_task_file_update(const char *engine_key, const dw_protocol_t proto,
                                          int32_t file_index, int64_t downloaded_bytes, int64_t total_size) {
        if (!engine_key || file_index < 0) return;
        std::lock_guard<std::mutex> lock(mtx_);
        TaskKey key = task_key_of_engine_key(proto, engine_key);
        if (task_key_is_empty(key)) {
            TaskRecord task_record;
            const bool found = (proto == DW_PROTOCOL_HTTP)
                                   ? store_.load_by_url(client_id_, engine_key, task_record)
                                   : store_.load_by_info_hash(client_id_, engine_key, task_record);
            if (found) key = task_record.task_key;
        }
        if (task_key_is_empty(key)) return; // 非已知任务，丢弃
        store_.upsert_task_file(key, file_index, downloaded_bytes, total_size);
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
        DW_LOGF(DW_LOG_DEBUG, "", "consume event:%s", to_string(event).c_str());
        const std::string &key = event.engine_key;
        if (key.empty()) {
            DW_LOGF(DW_LOG_ERROR, "", "engine_key 为空，丢弃事件 %s",
                    to_string(event).c_str());
            return;
        }

        switch (event.type) {
        case EngineEventType::PARSED: {
            // 解析完成（元数据+文件信息就绪，或存储迁移后再次就绪）。
            // content_root 模型：content_root = save_path 下的实际根目录名，
            //   物理路径 = save_path/content_root。
            //
            // 单锁覆盖整个 case：最多 10 个并发任务，竞争极低，
            //   反复释放/重获锁反而增加 find 次数和代码复杂度。
            //   std::mutex 不可重入，持锁期间所有内部调用均不再 lock mtx_。
            //
            // 状态守卫：仅 RESOLVING → PARSED，防止覆盖并发状态变更。

            std::lock_guard<std::mutex> lock(mtx_);

            // 查找任务。
            const TaskKey tk = task_key_of_engine_key(event.protocol, key);
            if (task_key_is_empty(tk)) {
                DW_LOGF(DW_LOG_ERROR, "", "PARSED 但 task_key 为空 key=%s", key.c_str());
                return;
            }
            const auto it = tasks_.find(tk);
            if (it == tasks_.end()) {
                DW_LOGF(DW_LOG_ERROR, "", "PARSED 但任务不在内存 key=%s", key.c_str());
                return;
            }
            TaskRecord &rec = it->second;

            const std::string torrent_name = event.name;
            if (!torrent_name.empty()) {
                rec.bt_metadata_ready = true;
            }

            // ---- 快路：content_root 已设定（storage_moved_alert 触发的重复 PARSED）----
            if (!rec.content_root.empty()) {
                if (!event.files.empty()) {
                    store_.save_task_files(rec.task_key, event.files);
                }
                if (rec.status == DW_TASK_STATUS_RESOLVING) {
                    rec.status = DW_TASK_STATUS_PARSED;
                    rec.dirty = true;
                    DW_LOGF(DW_LOG_INFO, "", "解析完成（content_root 已设定，快路转 PARSED）key=%s",
                            key.c_str());
                    schedule_needed_ = true;
                }
                // 跌穿到底部统一迁移 PARSED → QUEUED（与首次解析路径同步）。
            }

            // ---- 首次解析 ----
            if (torrent_name.empty()) {
                // 种子名为空（畸形 torrent）：无法定名，迁 ERROR 释放名额。
                rec.status = DW_TASK_STATUS_ERROR;
                rec.reason = DW_REASON_ERROR;
                rec.message = "种子名称为空，无法定名";
                store_.update(rec);
                schedule_needed_ = true;
                DW_LOGF(DW_LOG_ERROR, "", "解析事件种子名为空 key=%s", key.c_str());
                return;
            }

            // 文件列表父路径分析：收集顶层父目录名。
            std::unordered_set<std::string> parent_paths;
            for (const auto &f : event.files) {
                std::filesystem::path fp(f.name);
                if (fp.has_parent_path()) {
                    const auto root_name = *fp.begin();
                    if (!root_name.empty() && root_name != "." && root_name != "..") {
                        parent_paths.insert(root_name.string());
                    }
                }
            }
            // 1 个父路径 → base_name = parent_path；0/多个 → base_name = torrent_name。
            const std::string base_name =
                (parent_paths.size() == 1) ? *parent_paths.begin() : torrent_name;
            // 多文件判据：存在父路径（即顶层存在目录节点或文件路径含 '/'），与 event.multi_file 同语义。
            const bool multi_file = !parent_paths.empty();

            // 磁盘冲突检测。
            const bool conflict = std::filesystem::exists(
                std::filesystem::path(event.save_path) / base_name);

            if (conflict) {
                // 冲突：抢占唯一 content_root 名。
                std::string unique =
                    utils::acquire_wrapper_name(event.save_path, base_name, nullptr);
                rec.content_root = unique;
                rec.bt_multi_file = multi_file;
                rec.filename = multi_file ? std::string() : torrent_name;
                store_.update(rec);
                DW_LOGF(DW_LOG_INFO, "", "content_root 重名 key=%s '%s' -> '%s'",
                        key.c_str(), base_name.c_str(), unique.c_str());

                // content_root != base_name 时需 move_storage 迁入。
                if (unique != base_name && torrent_) {
                    const std::string effective =
                        (std::filesystem::path(event.save_path) / unique).string();
                    if (torrent_->move_storage(key.c_str(), effective.c_str()) == 0) {
                        // 迁移已发起：不改状态，等 storage_moved_alert 再次触发 PARSED 走快路。
                        DW_LOGF(DW_LOG_INFO, "", "move_storage 已发起 key=%s -> '%s'",
                                key.c_str(), effective.c_str());
                    } else {
                        rec.status = DW_TASK_STATUS_ERROR;
                        rec.reason = DW_REASON_ERROR;
                        rec.message = "存储迁移发起失败";
                        store_.update(rec);
                        schedule_needed_ = true;
                        DW_LOGF(DW_LOG_ERROR, "", "move_storage 发起失败 key=%s",
                                key.c_str());
                    }
                }
            } else {
                // 无冲突：content_root = base_name。
                rec.content_root = base_name;
                rec.bt_multi_file = multi_file;
                rec.filename = multi_file ? std::string() : torrent_name;
                store_.update(rec);
                DW_LOGF(DW_LOG_INFO, "", "解析完成 key=%s content_root='%s'",
                        key.c_str(), base_name.c_str());
                // 非单父路径时创建目录占位。
                if (parent_paths.size() != 1) {
                    (void)utils::acquire_wrapper_name(event.save_path, base_name, nullptr);
                }
                if (!event.files.empty()) {
                    store_.save_task_files(rec.task_key, event.files);
                }
                // 状态守卫：仅 RESOLVING → PARSED。
                if (rec.status == DW_TASK_STATUS_RESOLVING) {
                    rec.status = DW_TASK_STATUS_PARSED;
                    rec.dirty = true;
                    schedule_needed_ = true;
                }
            }
            // ---- PARSED → QUEUED 辅助迁移 ----
            // 判重/定名完成后由 PARSED 拍同步转 QUEUED（add_task/resume_task 同步路径需此拍）。
            // run_schedule 后续择机调用 apply_file_selection 准入 → DOWNLOADING。
            // 状态守卫仅允许 PARSED 拍转，避免覆盖并发状态变更。
            if (rec.status == DW_TASK_STATUS_PARSED) {
                rec.status = DW_TASK_STATUS_QUEUED;
                rec.dirty = true;
                rec.synth_notified = false;
                DW_LOGF(DW_LOG_INFO, "", "判重完成转 QUEUED 等待调度器准入 key=%s",
                        key.c_str());
                // 唤醒 add_task / resume_task 同步等待者。schedule_needed_ 留由 run_schedule
                // 择机准入（与原设计一致），本次 notify 仅负责同步路径。
                cv_.notify_all();
            }
            break;
        }
        case EngineEventType::DOWNLOAD_FAILED: {
            // 下载失败（通用）：转 ERROR。
            std::lock_guard<std::mutex> lock(mtx_);
            const TaskKey tk = task_key_of_engine_key(event.protocol, key);
            if (task_key_is_empty(tk)) {
                DW_LOGF(DW_LOG_ERROR, "", "DOWNLOAD_FAILED 但 task_key 为空 key=%s", key.c_str());
                return;
            }
            const auto it = tasks_.find(tk);
            if (it == tasks_.end()) {
                DW_LOGF(DW_LOG_ERROR, "", "DOWNLOAD_FAILED 但任务不在内存 key=%s", key.c_str());
                return;
            }
            it->second.status = DW_TASK_STATUS_ERROR;
            it->second.reason = event.reason;
            it->second.message = event.message;
            it->second.dirty = true;
            DW_LOGF(DW_LOG_ERROR, "", "下载失败 key=%s msg=%s",
                    key.c_str(), event.message.c_str());
            schedule_needed_ = true;
            break;
        }
        case EngineEventType::DOWNLOAD_COMPLETED: {
            // 下载完成：迁 COMPLETED 状态。
            std::lock_guard<std::mutex> lock(mtx_);
            const TaskKey tk = task_key_of_engine_key(event.protocol, key);
            if (task_key_is_empty(tk)) {
                DW_LOGF(DW_LOG_ERROR, "", "DOWNLOAD_COMPLETED 但 task_key 为空 key=%s", key.c_str());
                return;
            }
            const auto it = tasks_.find(tk);
            if (it == tasks_.end()) {
                DW_LOGF(DW_LOG_ERROR, "", "DOWNLOAD_COMPLETED 但任务不在内存 key=%s", key.c_str());
                return;
            }
            it->second.status = DW_TASK_STATUS_COMPLETED;
            it->second.dirty = true;
            DW_LOGF(DW_LOG_INFO, "", "下载完成 key=%s", key.c_str());
            schedule_needed_ = true;
            break;
        }
        case EngineEventType::STATUS_UPDATE: {
            // 状态+进度更新：写入 TaskRecord 内存字段。状态不再由事件携带（map_status 已移除），
            // 终态判断由 progress>=1.0（BT 完成）或 DOWNLOAD_FAILED 事件提供；PAUSED/RESUMED
            // 由独立 BT_PAUSED/BT_RESUMED 事件负责迁移。
            std::lock_guard<std::mutex> lock(mtx_);
            const TaskKey tk = task_key_of_engine_key(event.protocol, key);
            if (task_key_is_empty(tk)) {
                DW_LOGF(DW_LOG_DEBUG, "", "STATUS_UPDATE 任务不在内存 key=%s", key.c_str());
                return;
            }
            const auto it = tasks_.find(tk);
            if (it == tasks_.end()) {
                DW_LOGF(DW_LOG_DEBUG, "", "STATUS_UPDATE find 失败 key=%s", key.c_str());
                return;
            }
            TaskRecord &rec = it->second;

            // 进度数值
            rec.progress = event.progress;
            rec.total_size = event.total_size;
            rec.total_done = event.total_done;
            rec.download_rate = event.download_rate;
            rec.upload_rate = event.upload_rate;
            rec.total_upload = event.total_upload;
            rec.support_range = event.support_range;
            rec.reason = event.reason;
            rec.message = event.message;

            // 元数据字段：仅在 name 仍处于初始占位态（等于 engine key）时写入。
            // PARSED / request_unique_name 回调锁定 wrapper 名后，后续帧不再冲刷，
            // 防止去重后缀 (n) 被 libtorrent 原始种子名覆盖。
            // wrapper 名 = name（去后缀）；内部文件名 = filename（含后缀）。
            //   HTTP / BT 单文件：filename = 原始名（含后缀）
            //   BT 多文件：filename 清空（torrent_root 本身就是 wrapper）
            const std::string &ekey = (event.protocol == DW_PROTOCOL_HTTP) ? rec.url : rec.info_hash;
            if (!event.name.empty() && rec.name == ekey) {
                rec.name = utils::strip_extension(event.name);
                if (event.protocol == DW_PROTOCOL_HTTP) {
                    rec.filename = event.name;
                } else {
                    // BT 多文件判定：files 中任一节点的相对路径含 '/' 即为多文件（torrent 含目录结构）。
                    // 无 flags 字段可借，直接以 name 路径分隔符为准。
                    bool multi_file = false;
                    for (const auto &f : event.files) {
                        if (f.name && std::strchr(f.name, '/') != nullptr) {
                            multi_file = true;
                            break;
                        }
                    }
                    rec.bt_multi_file = multi_file;
                    rec.filename = multi_file ? std::string() : event.name;
                }
            }
            if (!event.etag.empty()) rec.etag = event.etag;
            if (!event.last_modified.empty()) rec.last_modified = event.last_modified;

            rec.dirty = true;
            break;
        }
        case EngineEventType::RESUME_DATA: {
            // 断点续传数据就绪：暂存内存，由 B 线程 flush_dirty_locked 落库。
            if (event.resume_data.empty()) {
                DW_LOGF(DW_LOG_DEBUG, "", "RESUME_DATA 为空 key=%s", key.c_str());
                return;
            }
            std::lock_guard<std::mutex> lock(mtx_);
            const TaskKey tk = task_key_of_engine_key(event.protocol, key);
            if (!task_key_is_empty(tk)) {
                if (const auto it = tasks_.find(tk); it != tasks_.end()) {
                    // 常规路径：任务在内存中，暂存 pending_resume
                    it->second.pending_resume.assign(
                        reinterpret_cast<const char *>(event.resume_data.data()),
                        event.resume_data.size());
                    return;
                }
            }
            // 兜底路径：任务已从内存逐出（如完成移出后 resume 晚到），直接落库
            TaskRecord task_record;
            const bool found = (event.protocol == DW_PROTOCOL_HTTP)
                                   ? store_.load_by_url(client_id_, key, task_record)
                                   : store_.load_by_info_hash(client_id_, key, task_record);
            if (found) {
                store_.save_resume(task_record.task_key, event.resume_data.data(),
                                   event.resume_data.size());
            }
            break;
        }
        case EngineEventType::BT_PAUSED: {
            // BT 暂停生效：状态机迁 PAUSED。状态守卫仅允许活跃态迁入，避免重复帧覆盖。
            std::lock_guard<std::mutex> lock(mtx_);
            const TaskKey tk = task_key_of_engine_key(event.protocol, key);
            if (task_key_is_empty(tk)) return;
            if (const auto it = tasks_.find(tk); it != tasks_.end()) {
                TaskRecord &rec = it->second;
                if (rec.status == DW_TASK_STATUS_DOWNLOADING ||
                    rec.status == DW_TASK_STATUS_QUEUED ||
                    rec.status == DW_TASK_STATUS_RESOLVING ||
                    rec.status == DW_TASK_STATUS_PARSED) {
                    rec.status = DW_TASK_STATUS_PAUSED;
                    rec.synth_notified = false;
                    rec.dirty = true;
                    reset_live_telemetry(rec); // 暂停帧不残留旧速率
                    DW_LOGF(DW_LOG_INFO, "", "BT 暂停生效 key=%s", key.c_str());
                }
            }
            break;
        }
        case EngineEventType::BT_RESUMED: {
            // BT 恢复生效：状态机迁 QUEUED 等待 run_schedule 准入，任务进入正常调度路径。
            // PAUSED 迁 QUEUED 为常规路径；如任务已被外部制以 COMPLETED/ERROR 则不覆盖。
            std::lock_guard<std::mutex> lock(mtx_);
            const TaskKey tk = task_key_of_engine_key(event.protocol, key);
            if (task_key_is_empty(tk)) return;
            if (const auto it = tasks_.find(tk); it != tasks_.end()) {
                TaskRecord &rec = it->second;
                if (rec.status == DW_TASK_STATUS_PAUSED) {
                    rec.status = DW_TASK_STATUS_QUEUED;
                    rec.synth_notified = false;
                    rec.dirty = true;
                    schedule_needed_ = true;
                    DW_LOGF(DW_LOG_INFO, "", "BT 恢复生效 key=%s", key.c_str());
                    cv_.notify_all();
                }
            }
            break;
        }
        }
    }

    /* ================================================================== */
    /*                          唯一名定名                                */
    /* ================================================================== */

    std::string TaskManager::resolve_and_record_name(const char *engine_key, const dw_protocol_t proto,
                                                     const std::string &dir, const std::string &wrapper_name,
                                                     const std::string &inner_name, const bool multi_file) {
        if (!engine_key || !engine_key[0] || wrapper_name.empty()) return wrapper_name;
        std::lock_guard<std::mutex> lock(mtx_);
        if (const TaskKey key = task_key_of_engine_key(proto, engine_key);
            !task_key_is_empty(key)) {
            if (const auto it = tasks_.find(key); it != tasks_.end()) {
                return resolve_and_record_name_locked(it->second, dir, wrapper_name, inner_name, multi_file);
            }
        }
        // 任务未常驻内存（罕见：上调早于登记 / 已被逐出）：回落库定位记录后仍走定名落库。
        TaskRecord task_record;
        const bool found = (proto == DW_PROTOCOL_HTTP)
                               ? store_.load_by_url(client_id_, engine_key, task_record)
                               : store_.load_by_info_hash(client_id_, engine_key, task_record);
        if (found) {
            return resolve_and_record_name_locked(task_record, dir, wrapper_name, inner_name, multi_file);
        }
        // 未知任务：仅抢名返回，不落库（wrapper 目录占位已在抢名时物化）。
        return utils::acquire_wrapper_name(dir, wrapper_name);
    }

    std::string TaskManager::resolve_and_record_name_locked(TaskRecord &rec,
                                                            const std::string &dir,
                                                            const std::string &wrapper_name,
                                                            const std::string &inner_name,
                                                            const bool multi_file) {
        // 幂等重入：同一 wrapper 已定名（持久预留跨重启有效），直接沿用既有 wrapper 名，
        // 避免自身 wrapper 目录/半成品文件被当作冲突源导致序号漂移或二次包层。
        // 同时幂等补齐占位——重启恢复后引擎会再次上调定名，此间占位若被外部清理，
        // 补回可维持名字持有的连续性，防其他任务判重时看不见该名。
        if (!rec.name.empty() && rec.name == wrapper_name) {
            // wrapper 目录已存在则不重复建。
            return rec.name;
        }
        // 判重真相源为磁盘：抢 wrapper 名即物化目录占位（未冲突建原名目录，冲突建 name(n) 目录），
        // 不再查库取占用名集合。
        std::string place_err;
        const std::string unique = utils::acquire_wrapper_name(dir, wrapper_name, &place_err);
        if (!place_err.empty()) {
            DW_LOGF(DW_LOG_ERROR, "", "wrapper 占位创建失败: %s/%s (%s)",
                    dir.c_str(), unique.c_str(), place_err.c_str());
        }
        // 新方案：name = wrapper（可能含 (n) 后缀）；save_path 保持不变（恒为 dir）；
        // filename 仅 HTTP/BT 单文件记录（wrapper 内的主文件名，含后缀）；BT 多文件为空。
        rec.name = unique;
        rec.save_path = dir;
        rec.filename = multi_file ? std::string() : inner_name;
        store_.update(rec); // 落库即完成持久预留（跨重启有效）。
        // 文件表由引擎在事件点经 post_task_files 自主推送，此处不再代写。
        if (unique != wrapper_name) {
            DW_LOGF(DW_LOG_INFO, "", "wrapper 去重定名: '%s' -> '%s/' (dir=%s key=%s)",
                    wrapper_name.c_str(), unique.c_str(), dir.c_str(),
                    rec.task_key.natural_key.c_str());
        }
        return unique;
    }

    /* ================================================================== */
    /*                          快照查询                                  */
    /* ================================================================== */

    int32_t TaskManager::list(dw_task_snapshot_t **out_tasks, int32_t *out_count) {
        if (!out_tasks || !out_count) return -1;

        std::lock_guard<std::mutex> lock(mtx_);

        // 先刷写活跃任务脏进度，使快照反映最新内存态（暂停/完成/错误已在状态迁移时落库）。
        flush_dirty_locked();

        // 全量任务来自库（含未常驻内存的暂停/完成/错误任务）。DB 不过滤，调用方按 key 自行识别。
        const std::vector<TaskRecord> all = store_.load_all();
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

        // 逐条从 TaskRecord 投影为 C ABI 快照（字符串堆分配，调用方经 dw_task_list_free 释放）。
        for (int32_t i = 0; i < n; ++i) {
            const TaskRecord &task_record = all[i];
            dw_task_snapshot_t s{};
            s.key.key_type = static_cast<dw_key_type_t>(task_record.task_key.key_type);
            s.key.natural_key = dup_cstr(task_record.task_key.natural_key);
            s.url = dup_cstr(task_record.url);
            s.info_hash = dup_cstr(task_record.info_hash);
            s.protocol = task_record.protocol;
            s.name = dup_cstr(task_record.name);
            s.save_path = dup_cstr(task_record.save_path);
            s.filename = dup_cstr(task_record.filename);
            s.status = task_record.status;
            s.progress = task_record.progress;
            s.total_size = task_record.total_size;
            s.total_done = task_record.total_done;
            s.priority = task_record.priority;
            s.created_at = task_record.created_at;
            s.modified_at = task_record.modified_at;
            s.source = task_record.source;
            s.content_root = dup_cstr(task_record.content_root);
            arr[i] = s;
        }
        *out_tasks = arr;
        *out_count = n;
        return 0;
    }

    /* ================================================================== */
    /*                          调度线程                                  */
    /* ================================================================== */

    void TaskManager::scheduler_loop() {
        // 采集后同步到内存和调用回调
        while (running_.load()) {
            std::vector<TaskRecord> fwd_records;
            std::vector<ResolveAction> resolve_actions;
            dw_progress_cb cb = nullptr;
            bool wake_schedule = false;

            {
                std::unique_lock<std::mutex> lock(mtx_);
                cv_.wait_for(lock, std::chrono::milliseconds(flush_interval_ms_),
                             [this] { return !running_.load(); });
                if (!running_.load()) break;

                // 采集数据
                collect_progress_locked(fwd_records, resolve_actions);
                cb = progress_cb_;
                // 采集拍产生终态（COMPLETED/ERROR）即置 schedule_needed_，记录到本地，
                // 锁外与 slot_released 合并 notify 立即唤醒 B 线程调度，不等维护周期超时。
                wake_schedule = schedule_needed_;
            } // ← 作用域退出，自动释放 mtx_

            // 触发引擎续传检查点（锁外，绝不持 mtx_）：post_updates 让 Torrent 引擎
            // 携变更门槛请求续传（无变化不产生 alert，去重下沉 libtorrent）。
            // 进度推送已由引擎经 STATUS_UPDATE 事件实时完成，此处仅驱动续传检查点。
            for (IDownloadEngine *eng: {http_, torrent_}) {
                if (eng) eng->post_updates();
            }

            // RESOLVING/PARSED 校验执行（锁外引擎调用，回锁校验后定态；期间可能已被暂停/删除，
            // 仅对应状态才迁移）：
            //   BT PARSED：事件驱动已完成冲突检测与文件落库，此处仅 apply_file_selection
            //       定型开下 → DOWNLOADING；
            //   HTTP RESOLVING：定名已在引擎 finalize_probing 完成，此处入引擎（携续传存档）→
            //       成功迁 DOWNLOADING，失败迁 ERROR 释放名额。
            bool slot_released = false;
            bool status_changed = false; // 跟踪锁内 status 转换（含 DOWNLOADING），用于唤醒同步等待者
            for (const auto &act: resolve_actions) {
                if (act.rec.protocol == DW_PROTOCOL_TORRENT) {
                    if (!torrent_) {
                        // 引擎不可用：迁 ERROR 释放名额。
                        std::lock_guard<std::mutex> lock(mtx_);
                        if (auto it2 = tasks_.find(act.rec.task_key); it2 != tasks_.end()) {
                            it2->second.status = DW_TASK_STATUS_ERROR;
                            it2->second.reason = DW_REASON_ERROR;
                            it2->second.message = "下载引擎不可用";
                            store_.update(it2->second);
                            schedule_needed_ = true;
                            slot_released = true;
                            status_changed = true;
                        }
                        continue;
                    }
                    const std::string &key = act.rec.info_hash;
                    // BT PARSED：冲突检测已由事件完成，直接 apply_file_selection 开下载。
                    const bool ok = (torrent_->apply_file_selection(
                            key.c_str(),
                            act.rec.file_indexes.empty() ? nullptr : act.rec.file_indexes.data(),
                            static_cast<int32_t>(act.rec.file_indexes.size())) == 0);
                    std::lock_guard<std::mutex> lock(mtx_);
                    if (const auto it = tasks_.find(act.rec.task_key);
                        it != tasks_.end() && it->second.status == DW_TASK_STATUS_PARSED) {
                        if (ok) {
                            it->second.status = DW_TASK_STATUS_DOWNLOADING;
                            it->second.dirty = true;
                            status_changed = true;
                            DW_LOGF(DW_LOG_INFO, "", "任务解析完成转下载 key=%s",
                                    act.rec.task_key.natural_key.c_str());
                        } else {
                            it->second.status = DW_TASK_STATUS_ERROR;
                            it->second.reason = DW_REASON_ERROR;
                            it->second.message = "文件选择应用失败";
                            store_.update(it->second);
                            schedule_needed_ = true;
                            slot_released = true;
                            status_changed = true;
                            DW_LOGF(DW_LOG_ERROR, "", "apply_file_selection 失败 key=%s",
                                    act.rec.task_key.natural_key.c_str());
                        }
                    }
                } else {
                    const bool ok = start_engine_task(act.rec, act.resume);
                    std::lock_guard<std::mutex> lock(mtx_);
                    if (const auto it = tasks_.find(act.rec.task_key);
                        it != tasks_.end() && it->second.status == DW_TASK_STATUS_RESOLVING) {
                        if (ok) {
                            it->second.status = DW_TASK_STATUS_DOWNLOADING;
                            it->second.dirty = true;
                            status_changed = true;
                            DW_LOGF(DW_LOG_INFO, "", "任务校验通过转下载 key=%s",
                                    act.rec.task_key.natural_key.c_str());
                        } else {
                            it->second.status = DW_TASK_STATUS_ERROR;
                            store_.update(it->second);
                            schedule_needed_ = true; // 名额释放，唤醒调度准入后续任务
                            slot_released = true;
                            status_changed = true;
                            DW_LOGF(DW_LOG_ERROR, "", "任务引擎启动失败 key=%s",
                                    act.rec.task_key.natural_key.c_str());
                        }
                    }
                }
            }

            // slot_released（ERROR 释放名额）/ wake_schedule（采集拍产生调度需求）需要唤醒。
            // status_changed（锁内 status 转换，含 DOWNLOADING）也 notify：HTTP 任务不经
            // QUEUED，add_task / resume_task 同步等待者需在转 DOWNLOADING 时被唤醒。
            // notify_all 是廉价唤醒，等待者自身的 status 检查会过滤误唤醒。
            if (slot_released || wake_schedule || status_changed) cv_.notify_all();

            // 锁外转发（周期节奏）：只读锁内已拷出的本地副本（已含遥测），避免与上层回调重入交叉。
            for (const auto &rec: fwd_records) {
                emit_progress(cb, rec);
            }
        }
    }

    void TaskManager::collect_progress_locked(std::vector<TaskRecord> &fwd_records,
                                              std::vector<ResolveAction> &resolve_actions) {
        for (auto &task_record: tasks_ | std::views::values) {
            // 引擎无 ctx 的合成态（QUEUED/PAUSED）：直接从记录投影合成一帧，一次性去重；
            // 回调唯一出口收归 A 线程，pause()/run_schedule（B 线程）仅置态，不再直接发射。
            if ((task_record.status == DW_TASK_STATUS_QUEUED ||
                 task_record.status == DW_TASK_STATUS_PAUSED) && !task_record.synth_notified) {
                task_record.synth_notified = true;
                fwd_records.push_back(task_record);
                continue;
            }
            if (task_record.status != DW_TASK_STATUS_DOWNLOADING &&
                task_record.status != DW_TASK_STATUS_RESOLVING &&
                task_record.status != DW_TASK_STATUS_PARSED)
                continue;

            // HTTP RESOLVING 校验拍：直接收集启动动作交 scheduler_loop 锁外入引擎。
            // 定名下放引擎 finalize_probing 单点（首个响应到达时优先级链取名，
            // 经 request_unique_name 上调判重定名并落库+写文件表凭证）。
            if (task_record.status == DW_TASK_STATUS_RESOLVING &&
                task_record.protocol == DW_PROTOCOL_HTTP) {
                resolve_actions.push_back({
                    task_record, store_.load_resume(task_record.task_key)
                });
                continue;
            }

            // 推模型：进度字段已由引擎线程经 on_progress 实时写入 TaskRecord，
            // 此处仅判终态与收集校验动作，不再调 query_progress。

            // 终态消费：引擎推入 COMPLETED/ERROR 后 pending_engine_status 非 QUEUED，
            // 消费一次迁权威态并释放名额。
            if (task_record.pending_engine_status == DW_TASK_STATUS_COMPLETED ||
                task_record.pending_engine_status == DW_TASK_STATUS_ERROR) {
                task_record.status = task_record.pending_engine_status;
                task_record.pending_engine_status = DW_TASK_STATUS_QUEUED; // 消费后复位
                schedule_needed_ = true;
            }

            fwd_records.push_back(task_record);

            // BT PARSED 校验拍：元数据就绪且冲突检测已通过（事件驱动），
            // 收集动作交 scheduler_loop 锁外 apply_file_selection 后迁 DOWNLOADING。
            // 置于终态判断之后：本拍已迁终态的任务不再收集。
            if (task_record.status == DW_TASK_STATUS_PARSED &&
                task_record.protocol == DW_PROTOCOL_TORRENT) {
                resolve_actions.push_back({
                    task_record, {}
                });
            }
        }
    }

    void TaskManager::maintenance_loop() {
        while (running_.load()) {
            std::vector<PlayingAction> playing_actions;
            {
                std::unique_lock<std::mutex> lock(mtx_);
                cv_.wait_for(lock, std::chrono::milliseconds(maintenance_interval_ms_),
                             [this] { return !running_.load() || schedule_needed_; });
                if (!running_.load()) break;

                maintenance_persist_locked();

                if (schedule_needed_) {
                    schedule_needed_ = false;
                    run_schedule(lock, playing_actions);
                }
            }
            // B 线程锁外执行播放提优 piece deadline（引擎调用不占 mtx_）
            for (const auto &pa: playing_actions) {
                if (IDownloadEngine *eng = engine_of(pa.protocol)) {
                    eng->set_playing_file(pa.key.c_str(), pa.file_index, pa.byte_offset);
                }
            }

            // B 线程消费引擎事件（Boost.Asio io_context 投递的 PARSED/STORAGE_MOVED 等）。
            // poll 非阻塞，处理所有待消费事件后立即返回。
            event_ioc_.poll();

            if (http_) http_->sweep();
            if (torrent_) torrent_->sweep();
            process_pending_deletes();
        }
    }

    void TaskManager::process_pending_deletes(const bool force) {
        // 锁内快照待删项，释放确认与文件删除均在锁外执行（引擎调用 / 磁盘 IO 不占 mtx_）。
        std::vector<std::pair<std::string, PendingFileDelete>> snapshot;
        {
            std::lock_guard<std::mutex> lock(mtx_);
            if (pending_deletes_.empty()) return;
            snapshot.assign(pending_deletes_.begin(), pending_deletes_.end());
        }

        for (auto &[key, item]: snapshot) {
            if (!force) {
                IDownloadEngine *eng = engine_of(item.proto);
                if (eng && !eng->task_released(key.c_str())) continue; // 资源未释放，下拍再查
            }
            {
                // 回锁出队；erase==0 说明该项已被同 key 重新添加撤销，跳过删除。
                std::lock_guard<std::mutex> lock(mtx_);
                if (pending_deletes_.erase(key) == 0) continue;
            }
            if (item.placeholder_only) {
                // 仅回收空占位：release_placeholder 只删空目录 / 零字节文件，
                // 已有真实数据一律不动（delete_files=0 语义：保留文件，只放弃名字）。
                const std::filesystem::path p(item.path);
                if (utils::release_placeholder(p.parent_path().string(),
                                              p.filename().string())) {
                    DW_LOGF(DW_LOG_INFO, "", "回收空占位完成 path=%s",
                            item.path.c_str());
                }
                continue;
            }
            // 仅尝试一次，成败均不重试（POSIX 下占用文件 unlink 立即成功，
            // inode 随最后一个 fd 关闭回收；目标不存在时 remove_all 幂等无错）。
            std::error_code ec;
            std::filesystem::remove_all(item.path, ec);
            if (ec) {
                DW_LOGF(DW_LOG_ERROR, "", "删除落盘产物失败 path=%s err=%s",
                        item.path.c_str(), ec.message().c_str());
            } else {
                DW_LOGF(DW_LOG_INFO, "", "删除落盘产物完成 path=%s",
                        item.path.c_str());
            }
        }
    }

    void TaskManager::maintenance_persist_locked() {
        std::vector<TaskKey> to_remove;
        for (auto &[task_key, task_record]: tasks_) {
            const bool paused = (task_record.status == DW_TASK_STATUS_PAUSED);
            // 初判终态（含引擎驱动的 pending_engine_status 信号）
            const bool terminal = (task_record.status == DW_TASK_STATUS_COMPLETED ||
                                   task_record.status == DW_TASK_STATUS_ERROR ||
                                   task_record.pending_engine_status == DW_TASK_STATUS_COMPLETED ||
                                   task_record.pending_engine_status == DW_TASK_STATUS_ERROR);
            if (task_record.status == DW_TASK_STATUS_DOWNLOADING || terminal || paused) {
                snapshot_segments_locked(task_record);
            }
            // snapshot 可能经数据驱动置 pending_engine_status=COMPLETED，重判终态
            const bool now_terminal = (task_record.status == DW_TASK_STATUS_COMPLETED ||
                                       task_record.status == DW_TASK_STATUS_ERROR ||
                                       task_record.pending_engine_status == DW_TASK_STATUS_COMPLETED ||
                                       task_record.pending_engine_status == DW_TASK_STATUS_ERROR);
            if (now_terminal) {
                // 任务级 0→2 传播：完成态任务把其文件节点统一置为完成正常。
                if (task_record.status == DW_TASK_STATUS_COMPLETED ||
                    task_record.pending_engine_status == DW_TASK_STATUS_COMPLETED) {
                    store_.mark_task_files_completed(task_key);
                }
                to_remove.push_back(task_key);
            } else if (paused) {
                if (task_record.protocol == DW_PROTOCOL_HTTP) {
                    // HTTP 暂停态延迟逐出：待引擎 ctx 被 sweep 回收（task_released 确认）后
                    // 再移出内存。此时 worker 已结束并经 post_resume_data 汇入 pending_resume，
                    // 由下方 flush_dirty_locked 落库，规避先逐出导致异步 resume 被 on_resume_data 丢弃。
                    if (!http_ || http_->task_released(engine_key(task_record).c_str())) {
                        to_remove.push_back(task_key);
                    }
                } else {
                    // BT：handle 常驻 session，无 ctx 回收信号；待记录已落库（!dirty）且
                    // pending_resume 已被 flush 清空后再逐出，尽力保住暂停时续传（晚到的检查点可能丢一次）。
                    if (!task_record.dirty && task_record.pending_resume.empty()) {
                        to_remove.push_back(task_key);
                    }
                }
            }
        }

        flush_dirty_locked();

        // 内存中移除任务
        for (const auto &task_key: to_remove) {
            unregister_task(task_key);
        }
    }

    void TaskManager::emit_progress(const dw_progress_cb cb, const TaskRecord &rec) {
        if (!cb) return;
        dw_progress_t p{};
        p.url = rec.url.c_str(); // HTTP 展示 / 识别；BT 为空串
        p.info_hash = rec.info_hash.c_str(); // BT 展示 / 识别；HTTP 为空串
        p.protocol = rec.protocol;
        p.name = rec.name.c_str();
        // output_path 为权威 save_path（已含包层目录，冲突时直接追加）。
        p.output_path = rec.save_path.c_str();
        p.filename = rec.filename.c_str();
        p.total_size = rec.total_size;
        p.total_done = rec.total_done;
        p.remaining = (rec.total_size > 0 && rec.total_size >= rec.total_done)
                          ? (rec.total_size - rec.total_done)
                          : -1;
        p.progress = rec.progress;
        p.download_rate = rec.download_rate;
        p.eta = (rec.download_rate > 0.0 && p.remaining > 0)
                    ? static_cast<double>(p.remaining) / rec.download_rate
                    : -1.0;
        p.task_status = rec.status; // 权威态由 TaskManager 独占
        p.reason = rec.reason;
        p.message = rec.message.c_str();
        p.saved_at_unix_ms = now_unix_ms();
        p.support_range = rec.support_range;
        p.etag = rec.etag.c_str();
        p.last_modified = rec.last_modified.c_str();
        p.upload_rate = rec.upload_rate;
        p.source = rec.source;
        // key.natural_key 在回调周期内有效（borrowed 指针，与 progress 其他字符串字段一致）；
        // 调用方如需保留须深拷贝。
        p.key.key_type = (rec.protocol == DW_PROTOCOL_HTTP) ? DW_KEY_TYPE_HTTP : DW_KEY_TYPE_BT;
        p.key.natural_key = (rec.protocol == DW_PROTOCOL_HTTP) ? rec.url.c_str() : rec.info_hash.c_str();
        p.content_root = rec.content_root.c_str();
        cb(&p);
    }

    void TaskManager::reset_live_telemetry(TaskRecord &rec) {
        rec.download_rate = 0.0;
        rec.upload_rate = 0.0;
        rec.reason = DW_REASON_NONE;
        rec.message.clear();
    }

    bool TaskManager::pause_slowest_downloading_locked(std::string &out_key,
                                                       dw_protocol_t &out_proto) {
        TaskRecord *slowest = nullptr;
        for (auto &[task_key, task_record]: tasks_) {
            if (task_record.status != DW_TASK_STATUS_DOWNLOADING) continue;
            if (!slowest || task_record.download_rate < slowest->download_rate) {
                slowest = &task_record;
            }
        }
        if (!slowest) return false;
        out_key = engine_key(*slowest);
        out_proto = slowest->protocol;
        DW_LOGF(DW_LOG_INFO, "", "播放提优暂停最慢任务 key=%s rate=%.1f",
                slowest->task_key.natural_key.c_str(), slowest->download_rate);
        slowest->status = DW_TASK_STATUS_PAUSED;
        slowest->synth_notified = false;
        slowest->dirty = true;
        reset_live_telemetry(*slowest);
        return true;
    }

    bool TaskManager::set_playing(const TaskKey &key, const int32_t file_index,
                                  const int64_t byte_offset) {
        std::lock_guard<std::mutex> lock(mtx_);
        TaskRecord *rec = nullptr;
        TaskRecord loaded;

        if (const auto it = tasks_.find(key); it != tasks_.end()) {
            rec = &it->second;
        } else if (store_.load_by_task_key(key, loaded)) {
            rec = &loaded;
        } else {
            return false; // 任务不存在
        }

        // HTTP 不支持 piece deadline
        if (rec->protocol != DW_PROTOCOL_TORRENT) return false;

        // 已完成任务拒绝播放提优（piece 已全部下载，deadline 无效）
        if (rec->status == DW_TASK_STATUS_COMPLETED) return false;

        // 写入播放信号
        rec->playing_file_index = file_index;
        rec->playing_byte_offset = byte_offset;

        if (rec->status != DW_TASK_STATUS_DOWNLOADING) {
            // 非活跃态：转 QUEUED 等待调度器准入
            reset_error_task_for_restart(*rec);
            rec->status = DW_TASK_STATUS_QUEUED;
            rec->synth_notified = false;
            store_.update(*rec);
            // 从 DB 加载的任务需登记到内存
            if (rec == &loaded) register_task(std::move(loaded));
        }

        schedule_needed_ = true;
        cv_.notify_all();
        return true;
    }

    void TaskManager::run_schedule(std::unique_lock<std::mutex> &lock,
                                   std::vector<PlayingAction> &playing_actions) {
        // ---- 播放提优处理（优先于常规准入） ----
        // 扫描 playing_file_index >= 0 的任务：
        //   DOWNLOADING 态：收集 PlayingAction 交调用方锁外设 piece deadline；
        //   非 DOWNLOADING 态：若名额已满则暂停最慢任务腾出额度，将播放任务转 RESOLVING 准入。
        for (auto &[task_key, task_record]: tasks_) {
            if (task_record.playing_file_index < 0) continue;
            if (task_record.protocol != DW_PROTOCOL_TORRENT) {
                // HTTP 不支持 piece deadline，清除信号
                task_record.playing_file_index = -1;
                task_record.playing_byte_offset = 0;
                continue;
            }
            if (task_record.status == DW_TASK_STATUS_DOWNLOADING) {
                // 已在下载中：收集动作，锁外设 piece deadline
                playing_actions.push_back({
                    task_key, task_record.protocol,
                    engine_key(task_record),
                    task_record.playing_file_index,
                    task_record.playing_byte_offset
                });
            } else if (status_occupies_slot(task_record.status) ||
                       task_record.status == DW_TASK_STATUS_QUEUED ||
                       task_record.status == DW_TASK_STATUS_PAUSED) {
                // 需要准入：名额已满时暂停最慢的 DOWNLOADING 任务腾出额度
                if (const int32_t active = active_count_locked(); active >= max_concurrent_) {
                    std::string pause_key;
                    dw_protocol_t pause_proto{};
                    if (pause_slowest_downloading_locked(pause_key, pause_proto)) {
                        // 锁外暂停引擎（pause_task 可能阻塞，不可持 mtx_ 调用）
                        lock.unlock();
                        dw_submit_result_t r{};
                        if (IDownloadEngine *eng = engine_of(pause_proto)) {
                            eng->pause_task(pause_key.c_str(), &r);
                        }
                        dw_submit_result_release(&r);
                        lock.lock();
                    }
                }
                // 转 RESOLVING 准入（BT 入引擎等元数据 / 幂等恢复已有 handle）
                task_record.status = DW_TASK_STATUS_RESOLVING;
                task_record.dirty = false;
                task_record.synth_notified = false;
                store_.update(task_record);
                TaskRecord copy = task_record;

                lock.unlock();
                const bool ok = start_engine_task(copy, store_.load_resume(copy.task_key));
                lock.lock();

                if (!ok) {
                    if (auto it2 = tasks_.find(copy.task_key); it2 != tasks_.end()) {
                        it2->second.status = DW_TASK_STATUS_ERROR;
                        it2->second.reason = DW_REASON_ERROR;
                        it2->second.message = "播放提优准入失败";
                        store_.update(it2->second);
                        schedule_needed_ = true;
                    }
                }
            }
            // 清除播放信号（已消费）
            task_record.playing_file_index = -1;
            task_record.playing_byte_offset = 0;
        }

        // ---- 常规准入调度 ----
        // add_task / resume_task 同步路径在返回前已将任务推进到 QUEUED 状态
        // （PARSED 事件处理后同步拍转），且入引擎动作（start_engine_task）已在 add/resume
        // 中完成。本节不再调 start_engine_task，避免重复入引擎：仅将 QUEUED 拍转
        // 为 PARSED（与事件驱动路径同形），再由 A 线程 collect_progress_locked 收集为
        // resolve_actions，scheduler_loop 锁外 apply_file_selection (BT) 或保持
        // HTTP 已在引擎中运行的状态（无需再调）→ DOWNLOADING。
        while (running_.load() && net_allowed_) {
            if (const int32_t active = active_count_locked(); active >= max_concurrent_) {
                break;
            }

            TaskRecord *best = nullptr;
            for (auto &task_record: tasks_ | std::views::values) {
                if (task_record.status != DW_TASK_STATUS_QUEUED) continue;
                if (!best ||
                    task_record.priority > best->priority ||
                    (task_record.priority == best->priority && task_record.created_at < best->created_at)) {
                    best = &task_record;
                }
            }
            if (!best) {
                break;
            }

            // QUEUED → PARSED：与事件路径同形（add/resume 已入引擎且 PARSED 事件已收敛），
            // A 线程 collect_progress_locked 会收集 PARSED → resolve_actions 并交
            // scheduler_loop 锁外 apply_file_selection (BT) / 保持 HTTP 运行 → DOWNLOADING。
            best->status = DW_TASK_STATUS_PARSED;
            best->dirty = false;
            store_.update(*best);
            schedule_needed_ = true; // A 线程下一拍需介入
        }
    }

    void TaskManager::set_network_allowed(bool allowed) {
        // 需停传输的活跃任务：(协议, 引擎键)。引擎键锁外喂引擎。
        struct PendingPause {
            dw_protocol_t protocol;
            std::string key; // HTTP=url，BT=info_hash
            TaskKey task_key;
        };
        std::vector<PendingPause> to_pause;
        {
            std::lock_guard<std::mutex> lock(mtx_);
            if (net_allowed_ == allowed) return; // 状态未变，幂等跳过
            net_allowed_ = allowed;
            if (!allowed) {
                // 闸门关闭：收集所有活跃任务（HTTP + BT），锁外逐任务暂停后回落 QUEUED。
                // 不再整会话 pause，仅停各任务的载荷传输；session 存活维持连接/心跳。
                for (const auto &task_record: tasks_ | std::views::values) {
                    if (!status_occupies_slot(task_record.status)) continue;
                    to_pause.push_back({
                        task_record.protocol,
                        task_record.protocol == DW_PROTOCOL_HTTP ? task_record.url : task_record.info_hash,
                        task_record.task_key
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
                eng->pause_task(pp.key.c_str(), &res);
            }
            dw_submit_result_release(&res);
        }

        // 引擎已停，回落 QUEUED 待闸门开启后重启；遥测归零，置 schedule_needed_ 由调度线程单点发射 QUEUED 合成帧。
        {
            std::lock_guard<std::mutex> lock(mtx_);
            for (const auto &pp: to_pause) {
                auto it = tasks_.find(pp.task_key);
                if (it == tasks_.end()) continue;
                TaskRecord &task_record = it->second;
                task_record.status = DW_TASK_STATUS_QUEUED;
                task_record.synth_notified = false; // 待 run_schedule 尾部统一发射
                reset_live_telemetry(task_record);
                store_.update(task_record);
            }
            schedule_needed_ = true;
        }
        cv_.notify_all();
    }


    // ---- 任务文件持久化 ----

    std::vector<dw_file_info_t> TaskManager::load_files(const TaskKey &key) {
        std::lock_guard<std::mutex> lock(mtx_);
        std::vector<dw_file_info_t> files = store_.load_task_files(key);
        if (files.empty()) return files;
        // 惰性删除检测：仅对完成态(2)文件 stat 物理路径，缺失则标记为已删除(1)。
        // 物理路径 = save_path / name（save_path 已含包层目录，name 已含完整相对路径）。
        TaskRecord task_record;
        if (!store_.load_by_task_key(key, task_record) || task_record.save_path.empty()) return files;
        std::filesystem::path base(task_record.save_path);
        for (auto &f: files) {
            if (f.status != 2) continue;
            std::filesystem::path p(base);
            if (f.name) p /= f.name;
            std::error_code ec;
            if (!std::filesystem::exists(p, ec)) f.status = 1;
        }
        return files;
    }

    // ---- 路径与展示辅助 ----

    std::string TaskManager::disk_root_path(const TaskRecord &rec) {
        // 最终落盘目录 = save_path / name；name 即 wrapper 名（去后缀，冲突时含 (n)）。
        return (std::filesystem::path(rec.save_path) / rec.name).string();
    }

    std::string TaskManager::display_name(const TaskRecord &rec) {
        // 展示名 = name（已含可能的后缀以去重名）。
        return rec.name;
    }

    // ---- 本地文件浏览与管理 ----

    int32_t TaskManager::scan_local_tasks(const std::string &save_path,
                                           dw_task_snapshot_t **out_tasks,
                                           int32_t *out_count) {
        if (!out_tasks || !out_count || save_path.empty()) return -1;

        std::lock_guard<std::mutex> lock(mtx_);

        // 1. 查询该 save_path 下既有任务，构建被占用的根条目名集合
        //    （含下载任务 + 已登记的本地文件任务，均视为已占用）
        const auto existing = store_.load_tasks_by_save_path(save_path);
        std::unordered_set<std::string> occupied;
        for (const auto &t : existing) {
            if (!t.filename.empty()) {
                occupied.insert(t.filename);
            } else {
                occupied.insert(t.name);
            }
        }

        // 2. 扫描目录，将非占用条目增量注册为本地文件任务（source=1）
        std::vector<TaskRecord> new_records;
        std::error_code ec;
        for (auto it = std::filesystem::directory_iterator(save_path, ec);
             it != std::filesystem::directory_iterator(); ++it) {
            const auto entry_name = it->path().filename().string();
            if (!entry_name.empty() && entry_name[0] == '.') continue;
            if (occupied.count(entry_name)) continue;

            TaskRecord rec;
            // LOCAL 任务主键：content_root = entry_name
            rec.task_key.client_id = client_id_;
            rec.task_key.key_type = TaskKeyType::LOCAL;
            rec.task_key.natural_key = entry_name;
            rec.name = entry_name;
            rec.filename = entry_name;
            rec.save_path = save_path;
            rec.content_root = entry_name; // 本地文件：content_root = 条目名
            rec.source = 1; // 本地文件
            rec.status = DW_TASK_STATUS_COMPLETED;
            rec.progress = 1.0;
            rec.created_at = now_unix_ms();
            if (it->is_directory(ec)) {
                rec.total_size = 0; // 目录大小不计算
            } else {
                rec.total_size = static_cast<int64_t>(it->file_size(ec));
                rec.total_done = rec.total_size;
            }
            store_.insert(rec);
            new_records.push_back(std::move(rec));
        }
        if (ec) return -1;

        // 3. 仅返回新增任务的快照
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
            const TaskRecord &r = new_records[i];
            dw_task_snapshot_t s{};
            s.key.key_type = static_cast<dw_key_type_t>(r.task_key.key_type);
            s.key.natural_key = dup_cstr(r.task_key.natural_key);
            s.url = dup_cstr(r.url);
            s.info_hash = dup_cstr(r.info_hash);
            s.protocol = r.protocol;
            s.name = dup_cstr(r.name);
            s.save_path = dup_cstr(r.save_path);
            s.filename = dup_cstr(r.filename);
            s.status = r.status;
            s.progress = r.progress;
            s.total_size = r.total_size;
            s.total_done = r.total_done;
            s.priority = r.priority;
            s.created_at = r.created_at;
            s.modified_at = r.modified_at;
            s.source = r.source;
            s.content_root = dup_cstr(r.content_root);
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

        // 加载该 save_path 下所有任务，筛选 source=1 且未失效的
        const auto all = store_.load_tasks_by_save_path(save_path);
        int32_t invalidated = 0;
        std::error_code ec;
        for (const auto &t : all) {
            if (t.source != 1) continue;
            if (t.status == DW_TASK_STATUS_INVALIDATED) continue;

            // 构建物理路径：save_path / content_root
            std::filesystem::path physical(std::filesystem::path(t.save_path) / t.content_root);
            if (!std::filesystem::exists(physical, ec)) {
                store_.update_status(t.task_key, DW_TASK_STATUS_INVALIDATED);
                ++invalidated;
            }
        }

        if (out_invalidated_count) *out_invalidated_count = invalidated;
        return 0;
    }

    int32_t TaskManager::clear_local_tasks(const std::string &save_path) {
        if (save_path.empty()) return -1;

        std::lock_guard<std::mutex> lock(mtx_);

        // 1. 加载该 save_path 下的全部任务，筛选 source IN (1,2)
        const auto tasks = store_.load_tasks_by_save_path(save_path);
        std::error_code ec;
        for (const auto &t : tasks) {
            if (t.source == 1 || t.source == 2) {
                // 删除物理文件/目录：save_path / content_root
                std::filesystem::path full_path = std::filesystem::path(t.save_path) / t.content_root;
                std::filesystem::remove_all(full_path, ec);
                // 删除 DB 记录
                store_.remove(t.task_key);
            }
        }
        return 0;
    }

    int32_t TaskManager::delete_local_entry(const TaskKey &key) {
        std::lock_guard<std::mutex> lock(mtx_);

        TaskRecord rec;
        if (!store_.load_by_task_key(key, rec)) return -1;

        // 仅允许删除本地文件任务（source=1）
        if (rec.source != 1) return -1;

        // 删除物理文件/目录：save_path / content_root
        std::filesystem::path full_path = std::filesystem::path(rec.save_path) / rec.content_root;
        std::error_code ec;
        std::filesystem::remove_all(full_path, ec);

        // 删除 DB 记录
        store_.remove(key);
        return ec ? -1 : 0;
    }

    bool TaskManager::start_engine_task(const TaskRecord &task_record,
                                        const std::vector<uint8_t> &resume) {
        dw_task_params_t p{};
        const std::string &ekey = engine_key(task_record); // 引擎标识（HTTP=url，BT=info_hash）；下方 c_str 依赖其存活
        p.save_path = task_record.save_path.c_str();
        p.filename = task_record.filename.empty() ? nullptr : task_record.filename.c_str();
        p.trace_id = ekey.c_str();
        p.priority = task_record.priority;

        std::vector<const char *> tk;
        if (task_record.protocol == DW_PROTOCOL_HTTP) {
            p.url = task_record.url.c_str();
        } else {
            p.info_hash = task_record.info_hash.c_str();
            if (!task_record.magnet_link.empty()) p.magnet_link = task_record.magnet_link.c_str();
            if (!task_record.torrent_file.empty()) p.torrent_file = task_record.torrent_file.c_str();
            if (!task_record.trackers.empty()) {
                for (auto &t: task_record.trackers) tk.push_back(t.c_str());
                p.trackers = tk.data();
                p.tracker_count = static_cast<int32_t>(tk.size());
            }
            if (!task_record.file_indexes.empty()) {
                p.file_indexes = task_record.file_indexes.data();
                p.file_index_size = static_cast<int32_t>(task_record.file_indexes.size());
            }
        }

        // 续传存档（两协议均适用）：HTTP 为分片续传 BLOB，BT 为 libtorrent 存档。
        if (!resume.empty()) {
            p.resume_data = resume.data();
            p.resume_data_size = resume.size();
        }

        dw_submit_result_t out{};
        IDownloadEngine *eng = engine_of(task_record.protocol);
        const int32_t rc = eng ? eng->add_task(&p, &out) : -1;
        dw_submit_result_release(&out);
        return rc == 0;
    }

    // 同步等待指定任务达到任一 target 状态。调用方须持 unique_lock 传入：
    // cv_.wait_for 期间释放锁以便 B 线程消费事件；返回时锁仍由调用方持有。
    // 用于 add_task / resume_task 路径：BT 阻 QUEUED，HTTP 阻 DOWNLOADING
    // （HTTP 引擎不推 PARSED 事件，状态机直接 RESOLVING→DOWNLOADING）。
    bool TaskManager::await_status_locked(std::unique_lock<std::mutex> &lock,
                                           const TaskKey &key,
                                           const std::initializer_list<dw_task_status_t> &targets,
                                           std::chrono::milliseconds timeout) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (running_.load()) {
            const auto it = tasks_.find(key);
            if (it == tasks_.end()) return false; // 任务被删除
            for (const auto t : targets) {
                if (it->second.status == t) return true;
            }
            // 中途进入 ERROR 也视为失败（add 路径下状态机会将入引擎失败转 ERROR）
            if (it->second.status == DW_TASK_STATUS_ERROR) return false;
            if (timeout.count() > 0) {
                const auto now = std::chrono::steady_clock::now();
                if (now >= deadline) return false;
                cv_.wait_for(lock, deadline - now);
            } else {
                cv_.wait(lock);
            }
        }
        return false;
    }

    IDownloadEngine *TaskManager::engine_of(const dw_protocol_t proto) const {
        return proto == DW_PROTOCOL_HTTP ? http_ : torrent_;
    }

    int32_t TaskManager::active_count_locked() const {
        int32_t n = 0;
        for (const auto &task_record: tasks_ | std::views::values) {
            if (status_occupies_slot(task_record.status)) ++n;
        }
        return n;
    }

    void TaskManager::flush_dirty_locked() {
        for (auto &task_record: tasks_ | std::views::values) {
            // 保存续传数据
            if (!task_record.pending_resume.empty()) {
                store_.save_resume(task_record.task_key,
                                   reinterpret_cast<const uint8_t *>(task_record.pending_resume.data()),
                                   task_record.pending_resume.size());
                task_record.pending_resume.clear();
            }
            // 保存任务数据
            if (task_record.dirty) {
                store_.update(task_record);
                task_record.dirty = false;
            }
        }
    }

    void TaskManager::register_task(TaskRecord task_record) {
        // tasks_ 单 map，key=TaskKey；无冗余索引维护。engine 事件匹配走 task_key_of_engine_key
        // 临时构造 TaskKey 后 O(1) 查表。
        tasks_[task_record.task_key] = std::move(task_record);
    }

    void TaskManager::unregister_task(const TaskKey &key) {
        tasks_.erase(key);
        segment_cache_.erase(key); // 同步清理分段缓存
    }

    TaskKey TaskManager::task_key_of_engine_key(const dw_protocol_t proto, const std::string &key) const {
        // 引擎回调给出的 key 是 (protocol, engine_key) 二元组，本机 clientId 来自 session 配置。
        TaskKey k;
        k.client_id = client_id_;
        k.key_type = protocol_to_key_type(proto);
        k.natural_key = key;
        const auto it = tasks_.find(k);
        return it != tasks_.end() ? it->first : TaskKey{};
    }

    void TaskManager::reset_error_task_for_restart(TaskRecord &task_record) {
        if (task_record.status == DW_TASK_STATUS_ERROR) {
            store_.clear_resume(task_record.task_key);
            store_.clear_segments(task_record.task_key);
            segment_cache_.erase(task_record.task_key); // 同步清理内存缓存
            if (task_record.protocol == DW_PROTOCOL_HTTP) {
                task_record.total_done = -1;
                task_record.progress = -1.0;
            }
        }
    }

    void TaskManager::snapshot_segments_locked(TaskRecord &task_record) {
        const std::string key = engine_key(task_record);
        if (key.empty()) return;

        // 收集每个已选文件的最新分段，同时更新内存缓存与 DB 快照。
        // HTTP 单文件 file_index=0；BT 遍历已选 file_indexes。
        struct file_result { int32_t file_index; std::vector<dw_byte_range_t> ranges; };
        std::vector<file_result> results;

        if (task_record.protocol == DW_PROTOCOL_HTTP) {
            if (!http_) return;
            auto ranges = http_->get_file_ranges(key.c_str(), 0);
            if (!ranges.empty()) {
                results.push_back({0, std::move(ranges)});
            }
        } else {
            if (!torrent_) return;
            for (const int32_t idx : task_record.file_indexes) {
                auto ranges = torrent_->get_file_ranges(key.c_str(), idx);
                if (!ranges.empty()) {
                    results.push_back({idx, std::move(ranges)});
                }
            }
        }

        // 写入内存缓存
        for (const auto &fr : results) {
            segment_cache_[task_record.task_key][fr.file_index] = fr.ranges;
        }

        // 从 ranges 推导 downloaded_bytes（避免重复遍历 piece bitmap）
        std::vector<std::pair<int32_t, int64_t>> file_bytes;
        file_bytes.reserve(results.size());
        for (const auto &fr : results) {
            int64_t bytes = 0;
            for (const auto &r : fr.ranges) {
                bytes += r.end - r.start + 1;
            }
            file_bytes.emplace_back(fr.file_index, bytes);
        }

        // 批量写入 DB：区间快照 + downloaded_bytes（各一个事务）
        {
            std::vector<std::pair<int32_t, std::vector<dw_byte_range_t>>> batch;
            batch.reserve(results.size());
            for (const auto &fr : results) {
                batch.emplace_back(fr.file_index, fr.ranges);
            }
            store_.save_segments_batch(task_record.task_key, batch);
        }
        store_.update_downloaded_bytes(task_record.task_key, file_bytes);

        // ---- 数据驱动完成检测（仅 DOWNLOADING 态） ----
        if (task_record.status != DW_TASK_STATUS_DOWNLOADING) return;

        // 从 task_files 表一次性加载文件大小（B 线程持 mtx_，DB 访问安全）。
        // file_size <= 0 的文件跳过完成判定（HTTP chunked 无总长场景）。
        const auto all_files = store_.load_task_files(task_record.task_key);

        bool all_complete = true;
        bool any_checkable = false; // 是否有可判定的文件（file_size > 0）
        for (const auto &fr : results) {
            // 从已加载的文件列表查当前文件大小
            int64_t file_size = -1;
            for (const auto &f : all_files) {
                if (f.index == fr.file_index) {
                    file_size = f.size;
                    break;
                }
            }
            if (file_size <= 0) {
                all_complete = false;
                continue; // 不可判定（HTTP chunked 无总长），不阻塞整体判定
            }
            any_checkable = true;
            if (is_file_complete(fr.ranges, file_size)) {
                store_.mark_file_completed(task_record.task_key, fr.file_index);
            } else {
                all_complete = false;
            }
        }

        // 所有可判定文件均完成 → 数据驱动任务完成（设 pending_engine_status 交 A 线程消费）
        if (any_checkable && all_complete) {
            task_record.pending_engine_status = DW_TASK_STATUS_COMPLETED;
            DW_LOGF(DW_LOG_INFO, "", "数据驱动任务完成 key=%s",
                    task_record.task_key.natural_key.c_str());
        }
    }

    std::vector<dw_byte_range_t> TaskManager::get_cached_segments(const TaskKey &key, int32_t file_index) {
        std::lock_guard<std::mutex> lock(mtx_);
        const auto it = segment_cache_.find(key);
        if (it == segment_cache_.end()) return {};
        const auto fit = it->second.find(file_index);
        if (fit == it->second.end()) return {};
        return fit->second;
    }

    int32_t TaskManager::get_task_status(const TaskKey &key) {
        std::lock_guard<std::mutex> lock(mtx_);
        const auto it = tasks_.find(key);
        if (it == tasks_.end()) return -1;
        return static_cast<int32_t>(it->second.status);
    }
} // namespace dw
