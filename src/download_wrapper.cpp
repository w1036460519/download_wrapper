/**
 * @file download_wrapper.cpp
 * @brief 统一多协议下载封装库的 C ABI 入口实现。
 */

#include "download_wrapper/download_wrapper.h"

#include "internal/downloader_internal.h"
#include "core/task_manager.h"
#include "http/http_engine.h"
#include "torrent/torrent_engine.h"
#include "utils/time_util.h"

#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>

namespace dw {

namespace {

// 全局单例实例
std::once_flag g_init_flag;
std::unique_ptr<dw_downloader> g_downloader;

void do_init_singleton() {
    g_downloader = std::make_unique<dw_downloader>();
}

} // namespace

/// 格式化日志辅助：snprintf 后调用 log_message。
void emit_logf(dw_log_level_t level, const char* trace_id,
               const char* func, int32_t line,
               const char* fmt, ...) {
    char buf[512];
    va_list args;
    va_start(args, fmt);
    vsnprintf(buf, sizeof(buf), fmt, args);
    va_end(args);
    log_message(level, buf, trace_id, func, line);
}

dw_downloader* global_downloader() {
    return g_downloader.get();
}

void log_message(dw_log_level_t level,
                 const char*    message,
                 const char*    trace_id,
                 const char*    func,
                 int32_t        line) {
    // 统一日志级别过滤：低于全局配置级别的消息直接丢弃
    if (g_downloader && level < g_downloader->config.log_level) return;
    const char* tid = (trace_id && trace_id[0]) ? trace_id : "";
    const int64_t ts = utils::now_unix_ms();
    if (g_downloader && g_downloader->log_cb) {
        g_downloader->log_cb(level, message, tid, func ? func : "", line, ts);
    } else {
        /* 日志回调未就绪，fallback 到 stderr */
        const char* lvl_str = "INFO";
        switch (level) {
            case DW_LOG_DEBUG: lvl_str = "DEBUG"; break;
            case DW_LOG_ERROR: lvl_str = "ERROR"; break;
            default: break;
        }
        const std::string time_str = utils::format_unix_ms(ts);
        std::fprintf(stderr, "[download_wrapper][%s][%s] %s:%d %s: %s\n",
                     lvl_str, time_str.c_str(),
                     func ? func : "?", line, tid, message);
    }
}

void post_resume_data(const char*    engine_key,
                      dw_protocol_t  protocol,
                      const uint8_t* data,
                      size_t         size) {
    if (!g_downloader || !engine_key) {
        return;
    }
    // resume_data 由库内 SQLite 持久化；同时保留对外回调以兼容已注册的调用方。
    int64_t id = 0;
    if (g_downloader->task_manager) {
        // 内部用引擎键（HTTP=url，BT=info_hash）定位；返回命中记录的自增 id 供对外回调。
        id = g_downloader->task_manager->on_resume_data(engine_key, protocol, data, size);
    }
    if (g_downloader->resume_data_cb && id != 0) {
        g_downloader->resume_data_cb(id, protocol, data, size);
    }
}

void post_task_files(const char*           engine_key,
                     dw_protocol_t         protocol,
                     const dw_file_info_t* files,
                     int32_t               count) {
    if (!g_downloader || !engine_key || !files || count <= 0) {
        return;
    }
    // 节点树由引擎构好、经此汇入库内 SQLite 持久化（仅内部通道，不对外回调）。
    if (g_downloader->task_manager) {
        g_downloader->task_manager->on_task_files(engine_key, protocol, files, count);
    }
}

std::string request_unique_name(const char*   engine_key,
                                dw_protocol_t protocol,
                                const char*   dir,
                                const char*   name) {
    if (!engine_key || !dir || !name) return name ? name : "";
    // 定名上调：TaskManager 持锁取占用名集 → 解唯一名 → 回写任务并落库（持久预留）。
    // alert 线程上调锁 mtx_ 与现有 on_task_files 同模式，无死锁。
    if (g_downloader && g_downloader->task_manager) {
        return g_downloader->task_manager->resolve_and_record_name(engine_key, protocol, dir, name);
    }
    return name;
}

} // namespace dw

/* ================================================================== */
/*                          C ABI 接口实现                            */
/* ================================================================== */

extern "C" {

/* ------------------------------------------------------------------ */
/*  生命周期                                                          */
/* ------------------------------------------------------------------ */

DW_API int32_t dw_init(const dw_config_t* cfg) {
    std::call_once(dw::g_init_flag, dw::do_init_singleton);
    if (!dw::g_downloader) {
        DW_LOG(DW_LOG_ERROR, "失败: 全局单例创建失败", "");
        return -1;
    }

    std::lock_guard<std::mutex> lock(dw::g_downloader->mutex);
    if (dw::g_downloader->initialized.load()) {
        DW_LOG(DW_LOG_DEBUG, "跳过: 已初始化", "");
        return 0;
    }

    // 保存配置
    if (cfg) {
        dw::g_downloader->config = *cfg;
    }

    dw::g_downloader->http_engine    = std::make_unique<dw::HttpEngine>();
    dw::g_downloader->torrent_engine = std::make_unique<dw::TorrentEngine>();

    if (dw::g_downloader->http_engine->init(cfg) != 0) {
        DW_LOG(DW_LOG_ERROR, "失败: HTTP 引擎初始化失败", "");
        return -1;
    }
    if (dw::g_downloader->torrent_engine->init(cfg) != 0) {
        dw::g_downloader->http_engine->destroy();
        DW_LOG(DW_LOG_ERROR, "失败: BT 引擎初始化失败", "");
        return -1;
    }

    dw::g_downloader->initialized.store(true);

    // 启动任务中枢：打开 SQLite、加载注册表、启动事件驱动调度线程。
    dw::g_downloader->task_manager = std::make_unique<dw::TaskManager>();
    dw::g_downloader->task_manager->set_engines(
        dw::g_downloader->http_engine.get(),
        dw::g_downloader->torrent_engine.get());
    dw::g_downloader->task_manager->set_progress_cb(dw::g_downloader->progress_cb);
    if (dw::g_downloader->task_manager->start(dw::g_downloader->config) != 0) {
        DW_LOG(DW_LOG_ERROR, "失败: TaskManager 启动失败", "");
        dw::g_downloader->task_manager.reset();
        return -1;
    }

    DW_LOG(DW_LOG_INFO, "初始化完成", "");
    return 0;
}

DW_API void dw_destroy(void) {
    if (!dw::g_downloader) {
        DW_LOG(DW_LOG_DEBUG, "跳过: 全局单例不存在", "");
        return;
    }

    std::lock_guard<std::mutex> lock(dw::g_downloader->mutex);
    if (!dw::g_downloader->initialized.load()) {
        DW_LOG(DW_LOG_DEBUG, "跳过: 尚未初始化", "");
        return;
    }

    // 先停止调度线程并最终刷库，再销毁引擎。
    if (dw::g_downloader->task_manager) {
        dw::g_downloader->task_manager->stop();
        dw::g_downloader->task_manager.reset();
    }

    if (dw::g_downloader->http_engine) {
        dw::g_downloader->http_engine->destroy();
        dw::g_downloader->http_engine.reset();
    }
    if (dw::g_downloader->torrent_engine) {
        dw::g_downloader->torrent_engine->destroy();
        dw::g_downloader->torrent_engine.reset();
    }

    dw::g_downloader->initialized.store(false);
    DW_LOG(DW_LOG_INFO, "已销毁", "");
}

DW_API int32_t dw_set_config(const dw_config_t* cfg) {
    auto* d = dw::global_downloader();
    if (!d || !cfg) {
        DW_LOGF(DW_LOG_ERROR, "",
            "失败: 参数非法 d=%p cfg=%p", d, cfg);
        return -1;
    }

    {
        std::lock_guard<std::mutex> lock(d->mutex);
        d->config = *cfg;
    }
    return 0;
}

DW_API void dw_set_network_allowed(bool allowed) {
    auto* d = dw::global_downloader();
    if (!d) return;
    dw::TaskManager* tm = nullptr;
    {
        std::lock_guard<std::mutex> lock(d->mutex);
        tm = d->task_manager.get();
    }
    if (tm) {
        tm->set_network_allowed(allowed);
    }
}

/* ------------------------------------------------------------------ */
/*  回调注册                                                          */
/* ------------------------------------------------------------------ */

DW_API void dw_set_progress_callback(dw_progress_cb cb) {
    if (!dw::g_downloader) {
        DW_LOG(DW_LOG_DEBUG, "跳过: 全局单例不存在", "");
        return;
    }
    std::lock_guard<std::mutex> lock(dw::g_downloader->mutex);
    dw::g_downloader->progress_cb = cb;
    if (dw::g_downloader->task_manager) {
        dw::g_downloader->task_manager->set_progress_cb(cb);
    }
}

DW_API void dw_set_log_callback(dw_log_cb cb) {
    if (!dw::g_downloader) {
        DW_LOG(DW_LOG_DEBUG, "跳过: 全局单例不存在", "");
        return;
    }
    std::lock_guard<std::mutex> lock(dw::g_downloader->mutex);
    dw::g_downloader->log_cb = cb;
}

DW_API void dw_set_resume_data_callback(dw_resume_data_cb cb) {
    if (!dw::g_downloader) {
        DW_LOG(DW_LOG_DEBUG, "跳过: 全局单例不存在", "");
        return;
    }
    std::lock_guard<std::mutex> lock(dw::g_downloader->mutex);
    dw::g_downloader->resume_data_cb = cb;
}

/* ------------------------------------------------------------------ */
/*  任务接口                                                          */
/* ------------------------------------------------------------------ */

DW_API int32_t dw_add_task(dw_protocol_t           protocol,
                           const dw_task_params_t* params,
                           dw_submit_result_t*     out_result) {
    auto* d = dw::global_downloader();
    const char* trace_id = (params && params->trace_id) ? params->trace_id : "";
    if (!d || !d->initialized.load() || !params || !out_result) {
        DW_LOGF(DW_LOG_ERROR, trace_id,
            "失败: 参数非法 d=%p init=%d params=%p out=%p",
            d, d ? d->initialized.load() : 0, params, out_result);
        if (out_result) {
            out_result->code    = DW_REASON_ERROR;
            out_result->message = nullptr;
        }
        return -1;
    }

    if (protocol != DW_PROTOCOL_HTTP && protocol != DW_PROTOCOL_TORRENT) {
        out_result->code    = DW_REASON_ERROR;
        out_result->message = nullptr;
        DW_LOGF(DW_LOG_ERROR, trace_id, "失败: 未知协议 protocol=%d", protocol);
        return -1;
    }
    // 入队 + 调度由 TaskManager 统一接管，引擎启动由调度线程按并发额度触发。
    return d->task_manager->add(protocol, params, out_result);
}

DW_API int32_t dw_pause_task(dw_protocol_t       protocol,
                             int64_t             id,
                             dw_submit_result_t* out_result) {
    auto* d = dw::global_downloader();
    if (!d || !d->initialized.load() || !out_result) {
        DW_LOGF(DW_LOG_ERROR, "",
            "失败: 参数非法 d=%p init=%d id=%lld out=%p",
            d, d ? d->initialized.load() : 0, (long long)id, out_result);
        if (out_result) {
            out_result->code    = DW_REASON_ERROR;
            out_result->message = nullptr;
        }
        return -1;
    }

    if (protocol != DW_PROTOCOL_HTTP && protocol != DW_PROTOCOL_TORRENT) {
        out_result->code    = DW_REASON_ERROR;
        out_result->message = nullptr;
        DW_LOGF(DW_LOG_ERROR, "", "失败: 未知协议 protocol=%d", protocol);
        return -1;
    }
    return d->task_manager->pause(protocol, id, out_result);
}

DW_API int32_t dw_resume_task(dw_protocol_t       protocol,
                              int64_t             id,
                              dw_submit_result_t* out_result) {
    auto* d = dw::global_downloader();
    if (!d || !d->initialized.load() || !out_result) {
        DW_LOGF(DW_LOG_ERROR, "",
            "失败: 参数非法 d=%p init=%d id=%lld out=%p",
            d, d ? d->initialized.load() : 0, (long long)id, out_result);
        if (out_result) {
            out_result->code    = DW_REASON_ERROR;
            out_result->message = nullptr;
        }
        return -1;
    }

    if (protocol != DW_PROTOCOL_HTTP && protocol != DW_PROTOCOL_TORRENT) {
        out_result->code    = DW_REASON_ERROR;
        out_result->message = nullptr;
        DW_LOGF(DW_LOG_ERROR, "", "失败: 未知协议 protocol=%d", protocol);
        return -1;
    }
    return d->task_manager->resume(protocol, id, out_result);
}

DW_API int32_t dw_delete_task(dw_protocol_t       protocol,
                              int64_t             id,
                              dw_submit_result_t* out_result) {
    auto* d = dw::global_downloader();
    if (!d || !d->initialized.load() || !out_result) {
        DW_LOGF(DW_LOG_ERROR, "",
            "失败: 参数非法 d=%p init=%d id=%lld out=%p",
            d, d ? d->initialized.load() : 0, (long long)id, out_result);
        if (out_result) {
            out_result->code    = DW_REASON_ERROR;
            out_result->message = nullptr;
        }
        return -1;
    }

    if (protocol != DW_PROTOCOL_HTTP && protocol != DW_PROTOCOL_TORRENT) {
        out_result->code    = DW_REASON_ERROR;
        out_result->message = nullptr;
        DW_LOGF(DW_LOG_ERROR, "", "失败: 未知协议 protocol=%d", protocol);
        return -1;
    }
    return d->task_manager->remove(protocol, id, out_result);
}

/* ------------------------------------------------------------------ */
/*  BT 工具函数                                                       */
/* ------------------------------------------------------------------ */

DW_API char* dw_magnet_to_info_hash(const char* magnet_link) {
    auto* d = dw::global_downloader();
    if (!d || !d->initialized.load() || !magnet_link) {
        DW_LOGF(DW_LOG_ERROR, "",
            "失败: 参数非法 d=%p init=%d magnet_link=%p",
            d, d ? d->initialized.load() : 0, magnet_link);
        return nullptr;
    }
    return d->torrent_engine->magnet_to_info_hash(magnet_link);
}

DW_API char* dw_torrent_file_to_info_hash(const char* torrent_file_path) {
    auto* d = dw::global_downloader();
    if (!d || !d->initialized.load() || !torrent_file_path) {
        DW_LOGF(DW_LOG_ERROR, "",
            "失败: 参数非法 d=%p init=%d path=%p",
            d, d ? d->initialized.load() : 0, torrent_file_path);
        return nullptr;
    }
    return d->torrent_engine->torrent_file_to_info_hash(torrent_file_path);
}

DW_API char* dw_info_hash_to_magnet(int64_t id) {
    auto* d = dw::global_downloader();
    if (!d || !d->initialized.load() || !d->task_manager) {
        DW_LOGF(DW_LOG_ERROR, "",
            "失败: 参数非法 d=%p init=%d id=%lld",
            d, d ? d->initialized.load() : 0, (long long)id);
        return nullptr;
    }
    // 低频操作：按 id 回读 info_hash（BT 的引擎键即 info_hash）后调引擎。
    std::string info_hash;
    if (!d->task_manager->engine_key_of(id, info_hash)) {
        DW_LOGF(DW_LOG_ERROR, "", "失败: 任务不存在 id=%lld", (long long)id);
        return nullptr;
    }
    return d->torrent_engine->info_hash_to_magnet(info_hash.c_str());
}

DW_API int dw_set_file_priority(int64_t id,
                                int32_t file_index,
                                int32_t priority) {
    auto* d = dw::global_downloader();
    if (!d || !d->initialized.load() || !d->task_manager) {
        DW_LOGF(DW_LOG_ERROR, "",
            "失败: 参数非法 d=%p init=%d id=%lld",
            d, d ? d->initialized.load() : 0, (long long)id);
        return 0;
    }
    std::string info_hash;
    if (!d->task_manager->engine_key_of(id, info_hash)) {
        DW_LOGF(DW_LOG_ERROR, "", "失败: 任务不存在 id=%lld", (long long)id);
        return 0;
    }
    return d->torrent_engine->set_file_priority(info_hash.c_str(), file_index, priority);
}

DW_API int32_t dw_parse_torrent_file(const char*      torrent_file_path,
                                     char**           out_info_hash,
                                     dw_file_info_t** out_files,
                                     int32_t*         out_count) {
    auto* d = dw::global_downloader();
    if (!d || !d->initialized.load() || !torrent_file_path ||
        !out_info_hash || !out_files || !out_count) {
        DW_LOGF(DW_LOG_ERROR, "",
            "失败: 参数非法 d=%p init=%d path=%p",
            d, d ? d->initialized.load() : 0, torrent_file_path);
        return -1;
    }
    return d->torrent_engine->parse_torrent_file(torrent_file_path,
                                                  out_info_hash,
                                                  out_files,
                                                  out_count);
}

DW_API int32_t dw_get_file_list(int64_t          id,
                                dw_file_info_t** out_files,
                                int32_t*         out_count) {
    auto* d = dw::global_downloader();
    if (!d || !d->initialized.load() || !d->task_manager ||
        !out_files || !out_count) {
        DW_LOGF(DW_LOG_ERROR, "",
            "失败: 参数非法 d=%p init=%d id=%lld",
            d, d ? d->initialized.load() : 0, (long long)id);
        return -1;
    }
    // 低频操作：按 id 回读 info_hash 后调引擎。
    std::string info_hash;
    if (!d->task_manager->engine_key_of(id, info_hash)) {
        DW_LOGF(DW_LOG_ERROR, "", "失败: 任务不存在 id=%lld", (long long)id);
        return -1;
    }
    return d->torrent_engine->get_file_list(info_hash.c_str(), out_files, out_count);
}

/* ------------------------------------------------------------------ */
/*  边下边播（区间 / 提优 / 进度）                                    */
/* ------------------------------------------------------------------ */

DW_API int32_t dw_get_file_ranges(int64_t           id,
                                  int32_t           file_index,
                                  dw_byte_range_t** out_ranges,
                                  int32_t*          out_count) {
    auto* d = dw::global_downloader();
    if (!d || !d->initialized.load() || !d->task_manager ||
        !out_ranges || !out_count) {
        DW_LOGF(DW_LOG_ERROR, "",
            "失败: 参数非法 d=%p init=%d id=%lld",
            d, d ? d->initialized.load() : 0, (long long)id);
        return -1;
    }
    *out_ranges = nullptr;
    *out_count  = 0;

    std::string key;
    dw_protocol_t proto = DW_PROTOCOL_HTTP;
    if (!d->task_manager->engine_ref_of(id, key, proto)) {
        DW_LOGF(DW_LOG_ERROR, "", "失败: 任务不存在 id=%lld", (long long)id);
        return -1;
    }

    // 接口统一双参签名：HTTP 单文件模型忽略 file_index。
    std::vector<dw_byte_range_t> vec =
        (proto == DW_PROTOCOL_TORRENT)
            ? d->torrent_engine->get_file_ranges(key.c_str(), file_index)
            : d->http_engine->get_file_ranges(key.c_str(), file_index);
    if (vec.empty()) {
        // 引擎实时无区间：任务可能未加载进引擎（暂停 / 跨重启空窗）。
        // 回退读 file_segments 表的低频快照，支撑任务未加载时的播放兜底。
        vec = d->task_manager->load_segments(id, file_index);
    }
    if (vec.empty()) {
        // 元数据未就绪 / 无已下区间且无快照：返回空结果（成功）。
        return 0;
    }
    const int32_t n = static_cast<int32_t>(vec.size());
    auto* arr = static_cast<dw_byte_range_t*>(
        std::calloc(static_cast<size_t>(n), sizeof(dw_byte_range_t)));
    if (!arr) {
        return -1;
    }
    for (int32_t i = 0; i < n; ++i) {
        arr[i] = vec[static_cast<size_t>(i)];
    }
    *out_ranges = arr;
    *out_count  = n;
    return 0;
}

DW_API void dw_byte_range_free(dw_byte_range_t* ranges, int32_t count) {
    (void)count; // 无嵌套指针，直接释放主数组
    if (ranges) {
        std::free(ranges);
    }
}

DW_API int32_t dw_set_file_priorities(int64_t             id,
                                      const int32_t*      file_indexes,
                                      const int32_t*      priorities,
                                      int32_t             count,
                                      dw_submit_result_t* out_result) {
    auto* d = dw::global_downloader();
    if (!d || !d->initialized.load() || !d->task_manager ||
        !file_indexes || !priorities || count <= 0 || !out_result) {
        DW_LOGF(DW_LOG_ERROR, "",
            "失败: 参数非法 d=%p init=%d id=%lld count=%d",
            d, d ? d->initialized.load() : 0, (long long)id, count);
        if (out_result) { out_result->code = DW_REASON_ERROR; out_result->message = nullptr; }
        return -1;
    }
    out_result->message = nullptr;
    out_result->id      = id;

    std::string key;
    dw_protocol_t proto = DW_PROTOCOL_HTTP;
    if (!d->task_manager->engine_ref_of(id, key, proto)) {
        DW_LOGF(DW_LOG_ERROR, "", "失败: 任务不存在 id=%lld", (long long)id);
        out_result->code = DW_REASON_ERROR;
        return -1;
    }
    if (proto != DW_PROTOCOL_TORRENT) {
        // HTTP 单文件：no-op。
        out_result->code = DW_REASON_NONE;
        return 0;
    }
    const int ok = d->torrent_engine->set_file_priorities(
        key.c_str(), file_indexes, priorities, count);
    out_result->code = ok ? DW_REASON_NONE : DW_REASON_ERROR;
    return ok ? 0 : -1;
}

DW_API int32_t dw_confirm_file_selection(int64_t             id,
                                         const int32_t*      file_indexes,
                                         int32_t             count,
                                         dw_submit_result_t* out_result) {
    auto* d = dw::global_downloader();
    if (!d || !d->initialized.load() || !d->task_manager || !out_result) {
        DW_LOGF(DW_LOG_ERROR, "",
            "失败: 参数非法 d=%p init=%d id=%lld",
            d, d ? d->initialized.load() : 0, (long long)id);
        if (out_result) { out_result->code = DW_REASON_ERROR; out_result->message = nullptr; }
        return -1;
    }
    // 确认意图落库 + 重新入队均由 TaskManager 处理（file_indexes 为 NULL/空 = 全部）。
    return d->task_manager->confirm_file_selection(id, file_indexes, count, out_result);
}

DW_API int32_t dw_set_playing_file(int64_t             id,
                                   int32_t             file_index,
                                   int64_t             byte_offset,
                                   dw_submit_result_t* out_result) {
    auto* d = dw::global_downloader();
    if (!d || !d->initialized.load() || !d->task_manager || !out_result) {
        DW_LOGF(DW_LOG_ERROR, "",
            "失败: 参数非法 d=%p init=%d id=%lld",
            d, d ? d->initialized.load() : 0, (long long)id);
        if (out_result) { out_result->code = DW_REASON_ERROR; out_result->message = nullptr; }
        return -1;
    }
    out_result->message = nullptr;
    out_result->id      = id;

    std::string key;
    dw_protocol_t proto = DW_PROTOCOL_HTTP;
    if (!d->task_manager->engine_ref_of(id, key, proto)) {
        DW_LOGF(DW_LOG_ERROR, "", "失败: 任务不存在 id=%lld", (long long)id);
        out_result->code = DW_REASON_ERROR;
        return -1;
    }
    if (proto != DW_PROTOCOL_TORRENT) {
        // HTTP：no-op（顺序下载，无 piece 提优概念）。
        out_result->code = DW_REASON_NONE;
        return 0;
    }
    const int ok = d->torrent_engine->set_playing_file(key.c_str(), file_index, byte_offset);
    out_result->code = ok ? DW_REASON_NONE : DW_REASON_ERROR;
    return ok ? 0 : -1;
}

DW_API int32_t dw_set_play_position(int64_t             id,
                                    int32_t             file_index,
                                    int64_t             position_ms,
                                    dw_submit_result_t* out_result) {
    auto* d = dw::global_downloader();
    if (!d || !d->initialized.load() || !d->task_manager || !out_result) {
        DW_LOGF(DW_LOG_ERROR, "",
            "失败: 参数非法 d=%p init=%d id=%lld",
            d, d ? d->initialized.load() : 0, (long long)id);
        if (out_result) { out_result->code = DW_REASON_ERROR; out_result->message = nullptr; }
        return -1;
    }
    // 播放进度直落 task_store，与协议无关。
    d->task_manager->set_play_position(id, file_index, position_ms);
    out_result->code    = DW_REASON_NONE;
    out_result->message = nullptr;
    out_result->id      = id;
    return 0;
}

DW_API int32_t dw_get_play_position(int64_t  id,
                                    int32_t  file_index,
                                    int64_t* out_position_ms) {
    auto* d = dw::global_downloader();
    if (!d || !d->initialized.load() || !d->task_manager || !out_position_ms) {
        DW_LOGF(DW_LOG_ERROR, "",
            "失败: 参数非法 d=%p init=%d id=%lld",
            d, d ? d->initialized.load() : 0, (long long)id);
        if (out_position_ms) *out_position_ms = 0;
        return -1;
    }
    *out_position_ms = d->task_manager->get_play_position(id, file_index);
    return 0;
}

/* ------------------------------------------------------------------ */
/*  任务快照与队列                                              */
/* ------------------------------------------------------------------ */

DW_API int32_t dw_list_tasks(dw_task_snapshot_t** out_tasks,
                             int32_t*             out_count) {
    auto* d = dw::global_downloader();
    if (!d || !d->initialized.load() || !d->task_manager ||
        !out_tasks || !out_count) {
        DW_LOGF(DW_LOG_ERROR, "",
            "失败: 参数非法 d=%p init=%d out_tasks=%p out_count=%p",
            d, d ? d->initialized.load() : 0, out_tasks, out_count);
        if (out_tasks) *out_tasks = nullptr;
        if (out_count) *out_count = 0;
        return -1;
    }
    return d->task_manager->list(out_tasks, out_count);
}

DW_API int32_t dw_set_task_priority(int64_t id, int32_t priority) {
    auto* d = dw::global_downloader();
    if (!d || !d->initialized.load() || !d->task_manager) {
        DW_LOGF(DW_LOG_ERROR, "",
            "失败: 参数非法 d=%p init=%d id=%lld",
            d, d ? d->initialized.load() : 0, (long long)id);
        return -1;
    }
    return d->task_manager->set_priority(id, priority);
}

/* ------------------------------------------------------------------ */
/*  任务文件持久化                                                    */
/* ------------------------------------------------------------------ */

DW_API int32_t dw_save_task_files(int64_t id,
                                   const dw_file_info_t* files,
                                   int32_t count) {
    auto* d = dw::global_downloader();
    if (!d || !d->initialized.load() || !d->task_manager ||
        !files || count <= 0) {
        DW_LOGF(DW_LOG_ERROR, "",
            "失败: 参数非法 d=%p init=%d id=%lld files=%p count=%d",
            d, d ? d->initialized.load() : 0, (long long)id, files, count);
        return -1;
    }
    // task_files 表按自增 id 直接关联，无需回读引擎键；转换为 vector 后批量写入。
    std::vector<dw_file_info_t> file_vec(files, files + count);
    d->task_manager->save_files(id, file_vec);
    return 0;
}

DW_API int32_t dw_load_task_files(int64_t id,
                                   dw_file_info_t** out_files,
                                   int32_t* out_count) {
    auto* d = dw::global_downloader();
    if (!d || !d->initialized.load() || !d->task_manager ||
        !out_files || !out_count) {
        DW_LOGF(DW_LOG_ERROR, "",
            "失败: 参数非法 d=%p init=%d id=%lld out_files=%p out_count=%p",
            d, d ? d->initialized.load() : 0, (long long)id, out_files, out_count);
        if (out_files) *out_files = nullptr;
        if (out_count) *out_count = 0;
        return -1;
    }
    // task_files 表按自增 id 直接关联，无需回读引擎键；直接从数据库加载。
    auto file_vec = d->task_manager->load_files(id);
    if (file_vec.empty()) {
        *out_files = nullptr;
        *out_count = 0;
        return -1;
    }
    // 转为堆数组：直接移交 file_vec 各节点的字符串所有权（库内已堆分配），
    // 避免二次拷贝与释放遗漏；调用方经 dw_file_list_free 统一释放。
    const int32_t n = static_cast<int32_t>(file_vec.size());
    dw_file_info_t* arr = static_cast<dw_file_info_t*>(
        std::malloc(sizeof(dw_file_info_t) * n));
    if (!arr) {
        // 分配失败：释放已持有的堆字符串，避免泄露。
        for (auto& f : file_vec) {
            std::free(f.name); std::free(f.prefix);
            std::free(f.temp_dir); std::free(f.ext);
        }
        *out_files = nullptr;
        *out_count = 0;
        return -1;
    }
    for (int32_t i = 0; i < n; ++i) {
        arr[i] = file_vec[i];   // 结构拷贝（含指针），所有权转移至 arr
    }
    *out_files = arr;
    *out_count = n;
    return 0;
}

/* ------------------------------------------------------------------ */
/*  资源释放                                                          */
/* ------------------------------------------------------------------ */

DW_API void dw_submit_result_release(dw_submit_result_t* result) {
    if (!result) {
        return;
    }
    if (result->message) {
        std::free(result->message);
        result->message = nullptr;
    }
}

DW_API void dw_file_list_free(dw_file_info_t* files, int32_t count) {
    if (!files || count <= 0) {
        return;
    }
    // 释放各节点由库分配的全部字符串字段。
    for (int32_t i = 0; i < count; ++i) {
        std::free(files[i].name);
        std::free(files[i].prefix);
        std::free(files[i].temp_dir);
        std::free(files[i].ext);
        files[i].name = nullptr;
        files[i].prefix = nullptr;
        files[i].temp_dir = nullptr;
        files[i].ext = nullptr;
    }
    std::free(files);
}

DW_API void dw_task_list_free(dw_task_snapshot_t* tasks, int32_t count) {
    if (!tasks || count <= 0) {
        return;
    }
    for (int32_t i = 0; i < count; ++i) {
        std::free(tasks[i].url);
        std::free(tasks[i].info_hash);
        std::free(tasks[i].name);
        std::free(tasks[i].save_path);
        std::free(tasks[i].filename);
    }
    std::free(tasks);
}

DW_API void dw_free(void* ptr) {
    if (ptr) {
        std::free(ptr);
    }
}

} /* extern "C" */
