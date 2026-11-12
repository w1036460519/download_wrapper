/**
 * @file download_wrapper.cpp
 * @brief 统一多协议下载封装库的 C ABI 入口实现。
 */

#include "download_wrapper/download_wrapper.h"

#include "internal/downloader_internal.h"
#include "core/task_manager.h"
#include "utils/string_util.h"
#include "utils/time_util.h"

#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>

namespace dw {
    namespace {
        // 全局单例实例
        std::once_flag g_init_flag;
        std::atomic<dw_log_level_t> g_log_level{DW_LOG_INFO};
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
            // 堆分配字符串，所有权交给回调消费方（Dart 侧 release_memory 释放）。
            const char *heap_json = strdup(json);
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
        if (level < g_log_level.load()) return;
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

DW_API char *create(void) {
    try {
        log_i("", "wrapper 下载器构建开始...");
        std::call_once(dw::g_init_flag, dw::do_init_singleton);
        std::lock_guard<std::mutex> lock(dw::g_downloader->mutex);
        if (dw::g_downloader->destroying) return dw::result_to_json(dw::dw_submit_result_t::failure());
        if (!dw::g_downloader->task_manager) {
            dw::g_downloader->task_manager = std::make_unique<dw::TaskManager>();
        }
        log_i("", "wrapper 下载器构建完成");
        return dw::result_to_json(dw::dw_submit_result_t::success());
    } catch (const std::exception &e) {
        log_e("", "wrapper 创建下载器失败: {}", e.what());
        return dw::result_to_json(dw::dw_submit_result_t::failure());
    } catch (...) {
        log_e("", "wrapper 创建下载器失败: 未知异常");
        return dw::result_to_json(dw::dw_submit_result_t::failure());
    }
}

DW_API char *init(const char *config_json) {
    log_i("", "wrapper 下载器初始化开始 config={}", config_json ? config_json : "(null)");
    if (!dw::g_downloader || !config_json || !config_json[0]) {
        return dw::result_to_json(dw::dw_submit_result_t::failure());
    }
    std::lock_guard<std::mutex> lock(dw::g_downloader->mutex);
    dw::Config config;
    if (dw::parse_config(config_json, config) != 0) {
        log_e("", "wrapper 配置无效");
        return dw::result_to_json(dw::dw_submit_result_t::failure());
    }

    if (dw::g_downloader->initialized.load()) {
        log_i("", "wrapper 下载器已初始化完成");
        return dw::result_to_json(dw::dw_submit_result_t::success());
    }

    auto *tm = dw::g_downloader->task_manager.get();
    if (!tm) {
        log_e("", "wrapper 管理器无效");
        return dw::result_to_json(dw::dw_submit_result_t::failure());
    }
    if (auto cfg_result = tm->config(config); cfg_result.code != DW_REASON_NONE) {
        log_e("", "wrapper 管理器配置失败");
        return dw::result_to_json(std::move(cfg_result));
    }

    dw::g_log_level.store(config.log_level);

    if (auto init_result = tm->init(); init_result.code != DW_REASON_NONE) {
        log_e("", "wrapper 管理器初始化失败");
        return dw::result_to_json(std::move(init_result));
    }

    dw::g_downloader->initialized.store(true);
    return dw::result_to_json(dw::dw_submit_result_t::success());
}

DW_API void destroy(void) {
    if (!dw::g_downloader) {
        log_d("", "wrapper 下载器已销毁");
        return;
    }

    auto &downloader = *dw::g_downloader;
    std::unique_ptr<dw::TaskManager> manager;
    {
        std::lock_guard<std::mutex> lock(downloader.mutex);
        if (downloader.destroying) return;
        downloader.destroying = true;
        downloader.initialized.store(false);
        downloader.progress_cb.store(nullptr);
        downloader.log_cb.store(nullptr);
        manager = std::move(downloader.task_manager);
    }
    manager.reset();
    {
        std::lock_guard<std::mutex> lock(downloader.mutex);
        dw::g_log_level.store(DW_LOG_INFO);
        downloader.destroying = false;
    }
    log_i("", "下载器销毁完成");
}

DW_API char *set_config(const char *config_json) {
    log_i("", "wrapper 配置开始 config={}", config_json ? config_json : "(null)");
    std::lock_guard<std::mutex> lock(dw::global_downloader()->mutex);
    if (!dw::global_downloader()->initialized.load()) {
        log_e("", "wrapper 下载器未初始化");
        return dw::result_to_json(dw::dw_submit_result_t::failure());
    }

    dw::Config cfg;
    if (dw::parse_config(config_json, cfg) != 0) {
        log_e("", "wrapper 配置失败 参数无效");
        return dw::result_to_json(dw::dw_submit_result_t::failure());
    }
    auto *tm = dw::global_downloader()->task_manager.get();
    auto result = tm->config(cfg);
    if (result.code != DW_REASON_NONE) {
        return dw::result_to_json(dw::dw_submit_result_t::failure());
    }
    dw::g_log_level.store(cfg.log_level);
    log_i("", "wrapper 配置完成");
    return dw::result_to_json(std::move(result));
}

/* ------------------------------------------------------------------ */
/*  回调注册                                                          */
/* ------------------------------------------------------------------ */

DW_API char *set_progress_callback(const dw_progress_cb cb) {
    auto *downloader = dw::global_downloader();
    if (!downloader) return dw::result_to_json(dw::dw_submit_result_t::failure());
    std::lock_guard<std::mutex> lock(downloader->mutex);
    if (!downloader->task_manager || downloader->destroying) {
        return dw::result_to_json(dw::dw_submit_result_t::failure());
    }
    downloader->progress_cb.store(cb);
    return dw::result_to_json(dw::dw_submit_result_t::success());
}

DW_API char *set_log_callback(const dw_log_cb cb) {
    auto *downloader = dw::global_downloader();
    if (!downloader) return dw::result_to_json(dw::dw_submit_result_t::failure());
    std::lock_guard<std::mutex> lock(downloader->mutex);
    if (!downloader->task_manager || downloader->destroying) {
        return dw::result_to_json(dw::dw_submit_result_t::failure());
    }
    downloader->log_cb.store(cb);
    return dw::result_to_json(dw::dw_submit_result_t::success());
}

/* ------------------------------------------------------------------ */
/*  任务接口                                                          */
/* ------------------------------------------------------------------ */

DW_API char *add_task(const char *params_json) {
    log_i("", "wrapper 添加任务 params={}", params_json ? params_json : "null");
    if (!dw::global_downloader()->initialized.load()) {
        log_e("", "wrapper 下载器未初始化");
        return dw::result_to_json(dw::dw_submit_result_t::failure());
    }
    // 通用参数校验与解析
    dw::TaskParams params;
    if (char *err = dw::validate_and_parse_task_params(params_json, params)) {
        log_e("", "wrapper 添加任务 参数无效");
        return err;
    }

    auto *d = dw::global_downloader();
    std::lock_guard<std::mutex> lock(d->mutex);
    if (!d->initialized.load()) {
        return dw::utils::dup_cstr(dw::make_error_response("下载器未初始化"));
    }
    const dw::TaskManager *tm = d->task_manager.get();
    auto result = tm->add(params);
    log_i("", "wrapper 添加任务完成: {}", boost::json::serialize(to_json(result)));
    return dw::result_to_json(std::move(result));
}

DW_API char *pause_task(const char *params_json) {
    log_i("", "wrapper 暂停任务 params={}", params_json ? params_json : "null");
    if (!dw::global_downloader()->initialized.load()) {
        log_e("", "wrapper 下载器未初始化");
        return dw::result_to_json(dw::dw_submit_result_t::failure());
    }
    dw::TaskParams params;
    if (char *err = dw::validate_and_parse_task_params(params_json, params)) {
        log_e("", "wrapper 暂停任务 参数无效");
        return err;
    }

    std::lock_guard<std::mutex> lock(dw::global_downloader()->mutex);
    const auto *tm = dw::global_downloader()->task_manager.get();
    auto result = tm->pause(params);
    log_i("", "暂停任务完成: {}", boost::json::serialize(to_json(result)));
    return dw::result_to_json(std::move(result));
}

DW_API char *resume_task(const char *params_json) {
    log_i("", "wrapper 恢复任务 params={}", params_json ? params_json : "null");
    if (!dw::global_downloader()->initialized.load()) {
        log_e("", "wrapper 下载器未初始化");
        return dw::result_to_json(dw::dw_submit_result_t::failure());
    }
    dw::TaskParams params;
    if (char *err = dw::validate_and_parse_task_params(params_json, params)) {
        log_e("", "wrapper 恢复任务 参数无效");
        return err;
    }

    std::lock_guard<std::mutex> lock(dw::global_downloader()->mutex);
    const auto *tm = dw::global_downloader()->task_manager.get();
    auto result = tm->resume(params);
    log_i("", "wrapper 恢复任务完成: {}", boost::json::serialize(to_json(result)));
    return dw::result_to_json(std::move(result));
}

DW_API char *delete_task(const char *params_json) {
    log_i("", "wrapper 删除任务 params={}", params_json ? params_json : "null");
    if (!dw::global_downloader()->initialized.load()) {
        log_e("", "wrapper 下载器未初始化");
        return dw::result_to_json(dw::dw_submit_result_t::failure());
    }

    dw::TaskParams params;
    if (char *err = dw::validate_and_parse_task_params(params_json, params)) {
        log_e("", "wrapper 删除任务 参数无效");
        return err;
    }
    std::lock_guard<std::mutex> lock(dw::global_downloader()->mutex);
    const auto *tm = dw::global_downloader()->task_manager.get();
    auto result = tm->remove(params);
    log_i("", "删除任务完成: {}", boost::json::serialize(to_json(result)));
    return dw::result_to_json(std::move(result));
}

/* ------------------------------------------------------------------ */
/*  BT 工具函数                                                       */
/* ------------------------------------------------------------------ */

DW_API char *parse_magnet(const char *params_json) {
    if (!params_json) {
        return dw::utils::dup_cstr(dw::make_error_response("参数为空"));
    }
    try {
        auto obj = boost::json::parse(params_json).as_object();
        std::string magnet = dw::json_get_string(obj, "magnet_link");
        if (magnet.empty()) {
            return dw::utils::dup_cstr(dw::make_error_response("magnet_link 为空"));
        }
        auto result = dw::TaskManager::parse_magnet(magnet);
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

DW_API char *parse_torrent_file(const char *params_json) {
    if (!params_json) {
        return dw::utils::dup_cstr(dw::make_error_response("参数为空"));
    }
    try {
        auto obj = boost::json::parse(params_json).as_object();
        std::string path = dw::json_get_string(obj, "torrent_file");
        if (path.empty()) {
            return dw::utils::dup_cstr(dw::make_error_response("torrent_file 为空"));
        }
        auto result = dw::TaskManager::parse_torrent_file(path);
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

DW_API char *list_tasks(const char *params_json) {
    auto *d = dw::global_downloader();
    if (!d || !params_json) {
        return dw::utils::dup_cstr(dw::make_error_response("参数非法"));
    }
    log_i("", "wrapper 任务列表 params={}", params_json);

    std::lock_guard<std::mutex> lock(d->mutex);
    auto *tm = d->task_manager.get();
    if (!d->initialized.load() || !tm) {
        return dw::utils::dup_cstr(dw::make_error_response("下载器未初始化"));
    }

    const auto tasks = tm->list();
    return dw::utils::dup_cstr(dw::make_success_response(boost::json::value_from(tasks).as_array()));
}

/* ------------------------------------------------------------------ */
/*  本地文件浏览与管理                                                */
/* ------------------------------------------------------------------ */

DW_API char *scan_local_file(const char *params_json) {
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

    return dw::utils::dup_cstr(dw::make_error_response("本地文件扫描暂不可用"));
}

/* ------------------------------------------------------------------ */
/*  资源释放                                                          */
/* ------------------------------------------------------------------ */

DW_API void release_memory(void *ptr) {
    if (ptr) {
        std::free(ptr);
    }
}
} /* extern "C" */
