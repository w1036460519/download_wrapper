/**
 * @file download_wrapper.cpp
 * @brief 统一多协议下载封装库的 C ABI 入口实现。
 */

#include "download_wrapper/download_wrapper.h"

#include "internal/downloader_internal.h"
#include "http/http_engine.h"
#include "torrent/torrent_engine.h"

#include <chrono>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>

namespace dw {

namespace {

// 全局单例实例
std::once_flag g_init_flag;
dw_downloader* g_downloader = nullptr;

void do_init_singleton() {
    g_downloader = new dw_downloader();
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
    return g_downloader;
}

void log_message(dw_log_level_t level,
                 const char*    message,
                 const char*    trace_id,
                 const char*    func,
                 int32_t        line) {
    const char* tid = (trace_id && trace_id[0]) ? trace_id : "";
    const int64_t ts = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
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
        std::fprintf(stderr, "[download_wrapper][%s][%lld] %s:%d %s: %s\n",
                     lvl_str, static_cast<long long>(ts),
                     func ? func : "?", line, tid, message);
    }
}

void post_progress(const dw_progress_t* progress) {
    if (!g_downloader || !g_downloader->progress_cb || !progress) {
        return;
    }
    g_downloader->progress_cb(progress);
}

void post_resume_data(const char*    task_id,
                      dw_protocol_t  protocol,
                      const uint8_t* data,
                      size_t         size) {
    if (!g_downloader || !g_downloader->resume_data_cb || !task_id) {
        return;
    }
    g_downloader->resume_data_cb(task_id, protocol, data, size);
}

} // namespace dw

/// 格式化日志宏：自动捕获调用方函数名与行号。
#define DW_LOGF(level, trace_id, fmt, ...) \
    dw::emit_logf((level), (trace_id), __FUNCTION__, __LINE__, fmt, ##__VA_ARGS__)

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

    std::lock_guard<std::mutex> lock(d->mutex);
    d->config = *cfg;
    return 0;
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
            out_result->task_id = nullptr;
            out_result->code    = DW_REASON_INVALID_INPUT;
            out_result->message = nullptr;
        }
        return -1;
    }

    switch (protocol) {
        case DW_PROTOCOL_HTTP:
            return d->http_engine->add_task(params, out_result);
        case DW_PROTOCOL_TORRENT:
            return d->torrent_engine->add_task(params, out_result);
        default:
            out_result->task_id = nullptr;
            out_result->code    = DW_REASON_INVALID_INPUT;
            out_result->message = nullptr;
            DW_LOGF(DW_LOG_ERROR, trace_id,
                "失败: 未知协议 protocol=%d", protocol);
            return -1;
    }
}

DW_API int32_t dw_pause_task(dw_protocol_t       protocol,
                             const char*         id,
                             dw_submit_result_t* out_result) {
    auto* d = dw::global_downloader();
    if (!d || !d->initialized.load() || !id || !out_result) {
        DW_LOGF(DW_LOG_ERROR, "",
            "失败: 参数非法 d=%p init=%d id=%p out=%p",
            d, d ? d->initialized.load() : 0, id, out_result);
        if (out_result) {
            out_result->task_id = nullptr;
            out_result->code    = DW_REASON_INVALID_INPUT;
            out_result->message = nullptr;
        }
        return -1;
    }

    switch (protocol) {
        case DW_PROTOCOL_HTTP:
            return d->http_engine->pause_task(id, out_result);
        case DW_PROTOCOL_TORRENT:
            return d->torrent_engine->pause_task(id, out_result);
        default:
            out_result->task_id = nullptr;
            out_result->code    = DW_REASON_INVALID_INPUT;
            out_result->message = nullptr;
            DW_LOGF(DW_LOG_ERROR, "",
                "失败: 未知协议 protocol=%d", protocol);
            return -1;
    }
}

DW_API int32_t dw_resume_task(dw_protocol_t           protocol,
                              const dw_task_params_t* params,
                              dw_submit_result_t*     out_result) {
    auto* d = dw::global_downloader();
    const char* trace_id = (params && params->trace_id) ? params->trace_id : "";
    if (!d || !d->initialized.load() || !params || !out_result) {
        DW_LOGF(DW_LOG_ERROR, trace_id,
            "失败: 参数非法 d=%p init=%d params=%p out=%p",
            d, d ? d->initialized.load() : 0, params, out_result);
        if (out_result) {
            out_result->task_id = nullptr;
            out_result->code    = DW_REASON_INVALID_INPUT;
            out_result->message = nullptr;
        }
        return -1;
    }

    switch (protocol) {
        case DW_PROTOCOL_HTTP:
            return d->http_engine->resume_task(params, out_result);
        case DW_PROTOCOL_TORRENT:
            return d->torrent_engine->resume_task(params, out_result);
        default:
            out_result->task_id = nullptr;
            out_result->code    = DW_REASON_INVALID_INPUT;
            out_result->message = nullptr;
            DW_LOGF(DW_LOG_ERROR, trace_id,
                "失败: 未知协议 protocol=%d", protocol);
            return -1;
    }
}

DW_API int32_t dw_delete_task(dw_protocol_t       protocol,
                              const char*         id,
                              dw_submit_result_t* out_result) {
    auto* d = dw::global_downloader();
    if (!d || !d->initialized.load() || !id || !out_result) {
        DW_LOGF(DW_LOG_ERROR, "",
            "失败: 参数非法 d=%p init=%d id=%p out=%p",
            d, d ? d->initialized.load() : 0, id, out_result);
        if (out_result) {
            out_result->task_id = nullptr;
            out_result->code    = DW_REASON_INVALID_INPUT;
            out_result->message = nullptr;
        }
        return -1;
    }

    switch (protocol) {
        case DW_PROTOCOL_HTTP:
            return d->http_engine->delete_task(id, out_result);
        case DW_PROTOCOL_TORRENT:
            return d->torrent_engine->delete_task(id, out_result);
        default:
            out_result->task_id = nullptr;
            out_result->code    = DW_REASON_INVALID_INPUT;
            out_result->message = nullptr;
            DW_LOGF(DW_LOG_ERROR, "",
                "失败: 未知协议 protocol=%d", protocol);
            return -1;
    }
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

DW_API char* dw_info_hash_to_magnet(const char* info_hash) {
    auto* d = dw::global_downloader();
    if (!d || !d->initialized.load() || !info_hash) {
        DW_LOGF(DW_LOG_ERROR, "",
            "失败: 参数非法 d=%p init=%d info_hash=%p",
            d, d ? d->initialized.load() : 0, info_hash);
        return nullptr;
    }
    return d->torrent_engine->info_hash_to_magnet(info_hash);
}

DW_API int dw_set_file_priority(const char* info_hash,
                                int32_t     file_index,
                                int32_t     priority) {
    auto* d = dw::global_downloader();
    if (!d || !d->initialized.load() || !info_hash) {
        DW_LOGF(DW_LOG_ERROR, "",
            "失败: 参数非法 d=%p init=%d info_hash=%p",
            d, d ? d->initialized.load() : 0, info_hash);
        return 0;
    }
    return d->torrent_engine->set_file_priority(info_hash, file_index, priority);
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

DW_API int32_t dw_get_file_list(const char*      task_id,
                                dw_file_info_t** out_files,
                                int32_t*         out_count) {
    auto* d = dw::global_downloader();
    if (!d || !d->initialized.load() || !task_id ||
        !out_files || !out_count) {
        DW_LOGF(DW_LOG_ERROR, "",
            "失败: 参数非法 d=%p init=%d task_id=%p",
            d, d ? d->initialized.load() : 0, task_id);
        return -1;
    }
    return d->torrent_engine->get_file_list(task_id, out_files, out_count);
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
    for (int32_t i = 0; i < count; ++i) {
        if (files[i].name) {
            std::free(files[i].name);
            files[i].name = nullptr;
        }
    }
    std::free(files);
}

DW_API void dw_free(void* ptr) {
    if (ptr) {
        std::free(ptr);
    }
}

} /* extern "C" */
