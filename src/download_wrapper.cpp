/**
 * @file download_wrapper.cpp
 * @brief 统一多协议下载封装库的 C ABI 入口实现。
 */

#include "download_wrapper/download_wrapper.h"

#include "internal/downloader_internal.h"
#include "core/task_manager.h"
#include "http/http_engine.h"
#include "torrent/torrent_engine.h"
#include "utils/memory_util.h"
#include "utils/string_util.h"
#include "utils/time_util.h"

#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
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

    dw_downloader *global_downloader() {
        return g_downloader.get();
    }

    void emit_progress(const char *json) {
        if (!g_downloader || !json) return;
        if (const auto cb = g_downloader->progress_cb.load()) {
            // 堆分配字符串，所有权交给回调消费方（Dart 侧 dw_free 释放）。
            // NativeCallable.listener 异步投递，回调立即返回，
            // 不能用局部变量（返回即析构），必须 strdup 保证生命周期。
            char *heap_json = strdup(json);
            if (!heap_json) return;
            cb(heap_json);
        }
    }

    void log_message(const dw_log_level_t level,
                     const char *message,
                     const char *trace_id,
                     const char *func,
                     const int32_t line) {
        // 统一日志级别过滤：低于全局配置级别的消息直接丢弃
        if (g_downloader && level < g_downloader->config.log_level) return;
        const char *tid = (trace_id && trace_id[0]) ? trace_id : "";
        const int64_t ts = utils::now_unix_ms();
        if (auto cb = g_downloader ? g_downloader->log_cb.load() : nullptr) {
            cb(level, message, tid, func ? func : "", line, ts);
        } else {
            /* 日志回调未就绪，fallback 到 stderr */
            auto lvl_str = "INFO";
            switch (level) {
                case DW_LOG_DEBUG: lvl_str = "DEBUG";
                    break;
                case DW_LOG_ERROR: lvl_str = "ERROR";
                    break;
                default: break;
            }
            const std::string time_str = utils::format_unix_ms(ts);
            // 调用点输出（方法名/行号）临时关闭，恢复时还原下方注释代码。
            // std::fprintf(stderr, "[wrapper][%s][%s] %s:%d %s: %s\n",
            //              lvl_str, time_str.c_str(), func ? func : "?", line, tid, message);
            std::fprintf(stderr, "[wrapper][%s][%s] %s: %s\n",
                         lvl_str, time_str.c_str(), tid, message);
        }
    }
} // namespace dw

// C ABI 函数位于全局命名空间：引入 dw 命名空间的快捷日志模板（原宏方案无此需求）。
using dw::log_d;
using dw::log_e;
using dw::log_i;

/* ================================================================== */
/*                          C ABI 接口实现                            */
/* ================================================================== */

extern "C" {
/* ------------------------------------------------------------------ */
/*  生命周期                                                          */
/* ------------------------------------------------------------------ */

DW_API char *dw_init(const char *config_json) {
    std::call_once(dw::g_init_flag, dw::do_init_singleton);
    if (!dw::g_downloader) {
        return dw::result_to_json(dw::dw_submit_result_t::failure(DW_REASON_ERROR, "下载器创建失败"));
    }

    std::lock_guard<std::mutex> lock(dw::g_downloader->mutex);

    // 解析 JSON 配置（NULL 或空字符串使用默认配置）
    if (config_json && config_json[0]) {
        if (dw::parse_config(config_json, dw::g_downloader->config) != 0) {
            return dw::result_to_json(dw::dw_submit_result_t::failure(DW_REASON_INVALID_INPUT, "非法输入"));
        }
    }

    if (dw::g_downloader->initialized.load()) {
        return dw::result_to_json(dw::dw_submit_result_t::success());
    }

    // 创建 TaskManager，先于引擎（引擎初始化时从 TaskManager config 读取）
    dw::g_downloader->task_manager = std::make_unique<dw::TaskManager>();
    auto *tm = dw::g_downloader->task_manager.get();

    // 校验并应用配置
    if (auto cfg_result = tm->apply_config(dw::g_downloader->config); cfg_result.code != DW_REASON_NONE) {
        return dw::result_to_json(std::move(cfg_result));
    }

    // 创建引擎（构造时注入 TaskManager）
    dw::g_downloader->http_engine = std::make_unique<dw::HttpEngine>(tm);
    dw::g_downloader->torrent_engine = std::make_unique<dw::TorrentEngine>(tm);

    if (dw::g_downloader->http_engine->init() != 0) {
        return dw::result_to_json(dw::dw_submit_result_t::failure(DW_REASON_ERROR, "HTTP 引擎初始化失败"));
    }
    if (dw::g_downloader->torrent_engine->init() != 0) {
        dw::g_downloader->http_engine->destroy();
        return dw::result_to_json(dw::dw_submit_result_t::failure(DW_REASON_ERROR, "BT 引擎初始化失败"));
    }

    // 注入引擎并启动 TaskManager
    tm->set_engines(dw::g_downloader->http_engine.get(), dw::g_downloader->torrent_engine.get());
    if (dw::g_downloader->task_manager->start() != 0) {
        dw::g_downloader->task_manager.reset();
        return dw::result_to_json(dw::dw_submit_result_t::failure(DW_REASON_ERROR, "下载器启动失败"));
    }

    dw::g_downloader->initialized.store(true);
    return dw::result_to_json(dw::dw_submit_result_t::success());
}

DW_API void dw_destroy(void) {
    if (!dw::g_downloader) {
        log_d("", "下载器已销毁");
        return;
    }

    std::lock_guard<std::mutex> lock(dw::g_downloader->mutex);
    if (!dw::g_downloader->initialized.load()) {
        log_d("", "下载器尚未初始化");
        return;
    }

    // 先停止任务中枢线程：调度/维护循环持有引擎裸指针（set_engines 注入），
    // 若先销毁引擎，stop 前仍存活的 maintenance_loop 对已析构对象的
    // sweep()/resume_task() 虚调用会段错误。
    if (dw::g_downloader->task_manager) {
        dw::g_downloader->task_manager->stop(false);
    }

    // 再停止引擎：其线程 join 前可能回调 TaskManager，此时对象仍存活，
    // 数据库保留至引擎最终快照保存完成；事件循环已停止
    //（post 到 stopped io_context 空转安全），回调均有防护。
    if (dw::g_downloader->http_engine) {
        dw::g_downloader->http_engine->destroy();
        dw::g_downloader->http_engine.reset();
    }
    if (dw::g_downloader->torrent_engine) {
        dw::g_downloader->torrent_engine->destroy();
        dw::g_downloader->torrent_engine.reset();
    }

    // 最后销毁任务中枢对象（线程均已 join）
    if (dw::g_downloader->task_manager) {
        dw::g_downloader->task_manager.reset();
    }

    // 释放配置
    dw::g_downloader->config = {};

    dw::g_downloader->initialized.store(false);
    log_i("", "下载器销毁完成");
}

DW_API char *dw_set_config(const char *config_json) {
    auto *d = dw::global_downloader();
    if (!d) {
        log_e("", "配置失败: 下载器已销毁");
        return dw::utils::dup_cstr(dw::make_error_response("下载器已销毁"));
    }
    if (!config_json) {
        log_e("", "配置失败: 参数为空");
        return dw::utils::dup_cstr(dw::make_error_response("参数为空"));
    }

    // 解析 JSON 配置
    dw::Config cfg;
    if (dw::parse_config(config_json, cfg) != 0) {
        log_e("", "配置失败: JSON 解析失败");
        return dw::utils::dup_cstr(dw::make_error_response("非法输入"));
    }

    log_i("", "配置开始: {}", config_json);

    // 更新权威配置并下发到 TaskManager（引擎运行时从 TaskManager config 拉取）
    dw::TaskManager *tm = nullptr;
    {
        std::lock_guard<std::mutex> lock(d->mutex);
        d->config = cfg;
        if (d->task_manager) tm = d->task_manager.get();
    }
    if (tm) tm->apply_config(cfg);

    log_i("", "配置完成");
    return dw::utils::dup_cstr(dw::make_success_response());
}

/* ------------------------------------------------------------------ */
/*  回调注册                                                          */
/* ------------------------------------------------------------------ */

DW_API void dw_set_progress_callback(const dw_progress_cb cb) {
    if (!dw::g_downloader) {
        log_e("", "进度回调注册失败: 下载器已销毁");
        return;
    }
    if (dw::g_downloader->progress_cb.load() == cb) return;
    log_i("", "注册进度回调: enabled={}", cb != nullptr);
    std::lock_guard<std::mutex> lock(dw::g_downloader->mutex);
    dw::g_downloader->progress_cb.store(cb);
}

DW_API void dw_set_log_callback(const dw_log_cb cb) {
    if (!dw::g_downloader) {
        log_e("", "日志回调注册失败: 下载器已销毁");
        return;
    }
    if (dw::g_downloader->log_cb.load() == cb) return;
    log_i("", "注册日志回调: enabled={}", cb != nullptr);
    std::lock_guard<std::mutex> lock(dw::g_downloader->mutex);
    dw::g_downloader->log_cb.store(cb);
}

/* ------------------------------------------------------------------ */
/*  任务接口                                                          */
/* ------------------------------------------------------------------ */

DW_API char *dw_add_task(const char *params_json) {
    log_i("engine", "添加任务 params={}", params_json ? params_json : "null");

    // 通用参数校验与解析
    dw::TaskParams params;
    if (char *err = dw::validate_and_parse_task_params(params_json, params)) {
        return err;
    }

    auto *d = dw::global_downloader();
    dw::TaskManager *tm = d->task_manager.get();
    if (!tm) {
        log_e(params.natural_key.c_str(), "添加任务失败: 管理器未初始化");
        return dw::utils::dup_cstr(dw::make_error_response("管理器未初始化"));
    }

    auto result = tm->add(params);
    log_i(params.natural_key.c_str(), "添加任务完成: {}", boost::json::serialize(to_json(result)));
    return dw::result_to_json(std::move(result));
}

DW_API char *dw_pause_task(const char *params_json) {
    log_i("engine", "暂停任务 params={}", params_json ? params_json : "null");

    dw::TaskParams params;
    if (char *err = dw::validate_and_parse_task_params(params_json, params)) {
        return err;
    }

    const auto *d = dw::global_downloader();
    auto *tm = d->task_manager.get();
    if (!tm) {
        log_e(params.natural_key.c_str(), "暂停任务失败: 管理器未初始化");
        return dw::utils::dup_cstr(dw::make_error_response("管理器未初始化"));
    }

    auto result = tm->pause(params);
    log_i(params.natural_key.c_str(), "暂停任务完成: {}", boost::json::serialize(to_json(result)));
    return dw::result_to_json(std::move(result));
}

DW_API char *dw_resume_task(const char *params_json) {
    log_i("engine", "恢复任务 params={}", params_json ? params_json : "null");

    dw::TaskParams params;
    if (char *err = dw::validate_and_parse_task_params(params_json, params)) {
        return err;
    }

    const auto *d = dw::global_downloader();
    auto *tm = d->task_manager.get();
    if (!tm) {
        log_e(params.natural_key.c_str(), "恢复任务失败: 管理器未初始化");
        return dw::utils::dup_cstr(dw::make_error_response("管理器未初始化"));
    }

    auto result = tm->resume(params);
    log_i(params.natural_key.c_str(), "恢复任务完成: {}", boost::json::serialize(to_json(result)));
    return dw::result_to_json(std::move(result));
}

DW_API char *dw_delete_task(const char *params_json) {
    log_i("engine", "删除任务 params={}", params_json ? params_json : "null");

    dw::TaskParams params;
    if (char *err = dw::validate_and_parse_task_params(params_json, params)) {
        return err;
    }
    const auto *d = dw::global_downloader();
    auto *tm = d->task_manager.get();
    if (!tm) {
        log_e(params.natural_key.c_str(), "删除任务失败: 管理器未初始化");
        return dw::utils::dup_cstr(dw::make_error_response("管理器未初始化"));
    }

    auto result = tm->remove(params);
    log_i(params.natural_key.c_str(), "删除任务完成: {}",boost::json::serialize(to_json(result)));
    return dw::result_to_json(std::move(result));
}

/* ------------------------------------------------------------------ */
/*  BT 工具函数                                                       */
/* ------------------------------------------------------------------ */

DW_API char *dw_parse_magnet(const char *params_json) {
    if (!params_json) {
        return dw::utils::dup_cstr(dw::make_error_response("参数为空"));
    }
    try {
        auto obj = boost::json::parse(params_json).as_object();
        std::string magnet = dw::json_get_string(obj, "magnet_link");
        if (magnet.empty()) {
            return dw::utils::dup_cstr(dw::make_error_response("magnet_link 为空"));
        }
        auto result = dw::TorrentEngine::parse_magnet(magnet);
        if (result.code != DW_REASON_NONE) {
            return dw::utils::dup_cstr(dw::make_error_response(result.message));
        }
        boost::json::object data;
        data["info_hash"] = result.info_hash;
        return dw::utils::dup_cstr(dw::make_success_response(data));
    } catch (const std::exception &) {
        return dw::utils::dup_cstr(dw::make_error_response("JSON 解析失败"));
    }
}

DW_API char *dw_parse_torrent_file(const char *params_json) {
    if (!params_json) {
        return dw::utils::dup_cstr(dw::make_error_response("参数为空"));
    }
    try {
        auto obj = boost::json::parse(params_json).as_object();
        std::string path = dw::json_get_string(obj, "torrent_file");
        if (path.empty()) {
            return dw::utils::dup_cstr(dw::make_error_response("torrent_file 为空"));
        }
        auto result = dw::TorrentEngine::parse_torrent_file(path);
        if (result.code != DW_REASON_NONE) {
            return dw::utils::dup_cstr(dw::make_error_response(result.message));
        }
        boost::json::object data;
        data["info_hash"] = result.info_hash;
        // 文件列表
        if (!result.files.empty()) {
            boost::json::array files_arr;
            for (const auto &fi: result.files) {
                files_arr.push_back(dw::to_json(fi));
            }
            data["files"] = std::move(files_arr);
        }
        return dw::utils::dup_cstr(dw::make_success_response(data));
    } catch (const std::exception &) {
        return dw::utils::dup_cstr(dw::make_error_response("JSON 解析失败"));
    }
}

/* ------------------------------------------------------------------ */
/*  任务快照与队列                                              */
/* ------------------------------------------------------------------ */

DW_API char *dw_list_tasks(const char *params_json) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !params_json) {
        return dw::utils::dup_cstr(dw::make_error_response("参数非法"));
    }

    std::string client_id;
    try {
        auto obj = boost::json::parse(params_json).as_object();
        client_id = dw::json_get_string(obj, "client_id");
    } catch (const std::exception &) {
        return dw::utils::dup_cstr(dw::make_error_response("JSON 解析失败"));
    }
    if (client_id.empty()) {
        return dw::utils::dup_cstr(dw::make_error_response("client_id 为空"));
    }

    auto *tm = d->task_manager.get();
    if (!tm) {
        return dw::utils::dup_cstr(dw::make_error_response("路由失败"));
    }

    const auto tasks = tm->list();
    boost::json::array arr;
    for (const auto &task : tasks) {
        auto item = dw::to_json(task);
        item["task_natural_key"] = task.task_id;
        const auto source = task.task_protocol == DW_PROTOCOL_TORRENT ? DW_SOURCE_REMOTE_FILE
            : (task.task_protocol == DW_PROTOCOL_HTTP ? DW_SOURCE_TASK_FILE : DW_SOURCE_LOCAL_FILE);
        item["type"] = static_cast<int>(source);
        item["files"] = boost::json::value_from(task.files);
        arr.push_back(std::move(item));
    }
    boost::json::object data;
    data["tasks"] = std::move(arr);
    return dw::utils::dup_cstr(dw::make_success_response(data));
}

/* ------------------------------------------------------------------ */
/*  本地文件浏览与管理                                                */
/* ------------------------------------------------------------------ */

DW_API char *dw_scan_local_file(const char *params_json) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !params_json) {
        return dw::utils::dup_cstr(dw::make_error_response("参数非法"));
    }

    std::string client_id;
    std::vector<std::string> catalog_paths;
    try {
        auto obj = boost::json::parse(params_json).as_object();
        client_id = dw::json_get_string(obj, "client_id");
        const auto &paths_arr = obj.at("catalog_paths").as_array();
        for (const auto &v: paths_arr) {
            catalog_paths.push_back(v.as_string().c_str());
        }
    } catch (const std::exception &) {
        return dw::utils::dup_cstr(dw::make_error_response("JSON 解析失败"));
    }
    if (client_id.empty() || catalog_paths.empty()) {
        return dw::utils::dup_cstr(dw::make_error_response("参数不完整"));
    }

    auto *tm = d->task_manager.get();
    if (!tm) {
        return dw::utils::dup_cstr(dw::make_error_response("路由失败"));
    }

    auto result = tm->scan_local_file(catalog_paths);
    if (result.code != DW_REASON_NONE) {
        return dw::utils::dup_cstr(dw::make_error_response(result.message));
    }

    boost::json::object data;
    data["affected_count"] = result.affected_count;
    return dw::utils::dup_cstr(dw::make_success_response(data));
}

/* ------------------------------------------------------------------ */
/*  资源释放                                                          */
/* ------------------------------------------------------------------ */

DW_API void dw_file_list_free(dw_file_info_t *files, int32_t count) {
    // 释放逻辑统一收敛至内存工具（各节点字符串 + 数组本体）。
    dw::utils::free_file_list(files, count);
}

DW_API void dw_task_list_free(dw_task_snapshot_t *tasks, int32_t count) {
    if (!tasks || count <= 0) {
        return;
    }
    for (int32_t i = 0; i < count; ++i) {
        if (tasks[i].natural_key) {
            std::free(const_cast<char *>(tasks[i].natural_key));
        }
        std::free(tasks[i].url);
        std::free(tasks[i].info_hash);
        std::free(tasks[i].name);
        std::free(tasks[i].save_path);
        std::free(tasks[i].content_root);
    }
    std::free(tasks);
}

DW_API void dw_free(void *ptr) {
    if (ptr) {
        std::free(ptr);
    }
}
} /* extern "C" */
