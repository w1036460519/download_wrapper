/**
 * @file download_wrapper.cpp
 * @brief 统一多协议下载封装库的 C ABI 入口实现。
 */

#include "download_wrapper/download_wrapper.h"

#include "internal/downloader_internal.h"
#include "core/router.h"
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

        /// 堆分配 JSON 字符串并返回 char*（调用方 dw_free 释放）。
        char *dup_json_string(const std::string &s) {
            char *p = static_cast<char *>(std::malloc(s.size() + 1));
            if (p) {
                std::memcpy(p, s.c_str(), s.size() + 1);
            }
            return p;
        }

        /// 内部 dw_submit_result_t → JSON char*（含 info_hash + files 透传）。
        /// 移动语义接管 files 所有权，返回后 result 的 files 已转移。
        char *result_to_json(dw_submit_result_t &&result) {
            boost::json::object resp;
            resp["code"] = static_cast<int>(result.code);
            if (result.code == DW_REASON_NONE) {
                boost::json::object data;
                if (!result.info_hash.empty()) {
                    data["info_hash"] = result.info_hash;
                }
                if (!result.files.empty()) {
                    boost::json::array files_arr;
                    for (const auto &fi: result.files) {
                        boost::json::object f;
                        f["index"] = fi.index;
                        f["name"] = fi.name;
                        f["full_path"] = fi.full_path;
                        f["size"] = fi.size;
                        f["ext"] = fi.ext;
                        f["status"] = fi.status;
                        f["offset"] = fi.offset;
                        f["downloaded_bytes"] = fi.downloaded_bytes;
                        files_arr.push_back(std::move(f));
                    }
                    data["files"] = std::move(files_arr);
                }
                if (!data.empty()) resp["data"] = std::move(data);
            } else {
                resp["message"] = result.message.empty() ? "操作失败" : result.message;
            }
            return dup_json_string(boost::json::serialize(resp));
        }
    } // namespace

    dw_downloader *global_downloader() {
        return g_downloader.get();
    }

    void emit_progress(const dw_progress_t *progress) {
        if (!g_downloader || !progress) return;
        if (auto cb = g_downloader->progress_cb.load()) {
            cb(progress);
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
            std::fprintf(stderr, "[wrapper][%s][%s] %s:%d %s: %s\n",
                         lvl_str, time_str.c_str(), func ? func : "?", line, tid, message);
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
        log_e("", "下载器创建失败");
        return dw::dup_json_string(dw::make_error_response("下载器创建失败"));
    }

    std::lock_guard<std::mutex> lock(dw::g_downloader->mutex);

    // 解析 JSON 配置（NULL 或空字符串使用默认配置）
    dw::Config cfg;
    if (config_json && config_json[0]) {
        if (dw::parse_config(config_json, cfg) != 0) {
            log_e("", "下载器初始化失败: JSON 解析失败");
            return dw::dup_json_string(dw::make_error_response("JSON 解析失败"));
        }
    }
    if (cfg.client_id.empty()) {
        log_e("", "下载器初始化失败: client_id 为空");
        return dw::dup_json_string(dw::make_error_response("client_id 为空"));
    }

    // 存储到全局配置
    dw::g_downloader->config = cfg;

    if (dw::g_downloader->initialized.load()) {
        log_d("", "下载器已初始化");
        return dw::dup_json_string(dw::make_success_response());
    }

    // 创建引擎（先于 Router，因为 Router::start 需要注入引擎）
    dw::g_downloader->http_engine = std::make_unique<dw::HttpEngine>();
    dw::g_downloader->torrent_engine = std::make_unique<dw::TorrentEngine>();

    const dw::Config *cfg_ptr = &dw::g_downloader->config;
    if (dw::g_downloader->http_engine->init(cfg_ptr, nullptr) != 0) {
        log_e("", "HTTP 引擎初始化失败");
        return dw::dup_json_string(dw::make_error_response("HTTP 引擎初始化失败"));
    }
    if (dw::g_downloader->torrent_engine->init(cfg_ptr, nullptr) != 0) {
        dw::g_downloader->http_engine->destroy();
        log_e("", "BT 引擎初始化失败");
        return dw::dup_json_string(dw::make_error_response("BT 引擎初始化失败"));
    }

    // 创建 Router 并启动 TaskManager
    dw::g_downloader->router = std::make_unique<dw::Router>();
    dw::g_downloader->router->set_local_client_id(dw::g_downloader->config.client_id);
    if (dw::g_downloader->router->start(dw::g_downloader.get(), dw::g_downloader->config) != 0) {
        log_e("", "下载器启动失败");
        dw::g_downloader->router.reset();
        return dw::dup_json_string(dw::make_error_response("下载器启动失败"));
    }

    dw::g_downloader->initialized.store(true);

    log_i("", "下载器初始化完成");
    return dw::dup_json_string(dw::make_success_response());
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

    // 先停止 Router（含 TaskManager）
    if (dw::g_downloader->router) {
        dw::g_downloader->router->stop();
        dw::g_downloader->router.reset();
    }

    if (dw::g_downloader->http_engine) {
        dw::g_downloader->http_engine->destroy();
        dw::g_downloader->http_engine.reset();
    }
    if (dw::g_downloader->torrent_engine) {
        dw::g_downloader->torrent_engine->destroy();
        dw::g_downloader->torrent_engine.reset();
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
        return dw::dup_json_string(dw::make_error_response("下载器已销毁"));
    }
    if (!config_json) {
        log_e("", "配置失败: 参数为空");
        return dw::dup_json_string(dw::make_error_response("参数为空"));
    }

    // 解析 JSON 配置
    dw::Config cfg;
    if (dw::parse_config(config_json, cfg) != 0) {
        log_e("", "配置失败: JSON 解析失败");
        return dw::dup_json_string(dw::make_error_response("JSON 解析失败"));
    }

    log_i("", "配置开始: {}", config_json);

    // 先在锁内更新配置副本并取出下游目标，再锁外下发：
    // 引擎 update_config / TaskManager 唤醒内部各自加锁，不可持 d->mutex 调用以规避锁序风险。
    dw::IDownloadEngine *http = nullptr;
    dw::IDownloadEngine *torrent = nullptr;
    dw::TaskManager *tm = nullptr;
    {
        std::lock_guard<std::mutex> lock(d->mutex);
        d->config = cfg;
        http = d->http_engine.get();
        torrent = d->torrent_engine.get();
        if (d->router) tm = d->router->task_manager();
    }

    // 热更新运行期可生效的项：引擎限速 / 做种分享率、调度并发上限。
    const dw::Config *cfg_ptr = &d->config;
    if (http) http->update_config(cfg_ptr);
    if (torrent) torrent->update_config(cfg_ptr);
    if (tm) tm->set_max_concurrent(cfg.max_concurrent_downloads);

    log_i("", "配置完成");
    return dw::dup_json_string(dw::make_success_response());
}

DW_API char *dw_set_network_allowed(const char *params_json) {
    auto *d = dw::global_downloader();
    if (!d) {
        log_e("", "网络切换失败: 下载器已销毁");
        return dw::dup_json_string(dw::make_error_response("下载器已销毁"));
    }
    if (!params_json) {
        log_e("", "网络切换失败: 参数为空");
        return dw::dup_json_string(dw::make_error_response("参数为空"));
    }

    // 解析 JSON：{"allowed": true/false}
    bool allowed = false;
    try {
        auto obj = boost::json::parse(params_json).as_object();
        if (auto *v = obj.if_contains("allowed"); v && v->is_bool()) {
            allowed = v->as_bool();
        }
    } catch (const std::exception &) {
        return dw::dup_json_string(dw::make_error_response("JSON 解析失败"));
    }

    log_i("", "网络切换开始: allowed={}", allowed);
    dw::TaskManager *tm = nullptr;
    {
        std::lock_guard<std::mutex> lock(d->mutex);
        if (d->router) {
            tm = d->router->task_manager();
        }
    }
    if (tm) {
        tm->set_network_allowed(allowed);
    }
    log_i("", "网络切换完成");
    return dw::dup_json_string(dw::make_success_response());
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
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !params_json) {
        log_e("", "添加任务失败: 参数非法 d={} init={} params_json={}", d != nullptr, d && d->initialized.load(), params_json);
        return dw::dup_json_string(dw::make_error_response("参数非法"));
    }

    // 解析 JSON 参数
    dw::TaskParams params;
    if (dw::parse_task_params(params_json, params) != 0) {
        log_e("", "添加任务失败: JSON 解析失败");
        return dw::dup_json_string(dw::make_error_response("JSON 解析失败"));
    }

    const dw_protocol_t protocol = params.protocol;
    const std::string &client_id = params.client_id;

    if (protocol != DW_PROTOCOL_HTTP && protocol != DW_PROTOCOL_TORRENT) {
        log_e("", "添加任务失败: 未知协议 protocol={}", static_cast<int>(protocol));
        return dw::dup_json_string(dw::make_error_response("未知协议"));
    }
    if (client_id.empty()) {
        log_e("", "添加任务失败: client_id 为空");
        return dw::dup_json_string(dw::make_error_response("client_id 为空"));
    }

    // 确定 natural_key
    const std::string task_key = (protocol == DW_PROTOCOL_HTTP) ? params.url : params.info_hash;
    if (task_key.empty()) {
        log_e(client_id.c_str(), "添加任务失败: natural_key 为空");
        return dw::dup_json_string(dw::make_error_response("natural_key 为空"));
    }

    log_i(task_key.c_str(), "添加任务开始：protocol={} client_id={}", dw::to_string(protocol), client_id);

    // 路由到 TaskManager
    dw::TaskManager *tm = nullptr;
    if (d->router) {
        tm = d->router->route(client_id);
    }
    if (!tm) {
        log_e(task_key.c_str(), "添加任务失败：路由失败 client_id={}", client_id);
        return dw::dup_json_string(dw::make_error_response("路由失败"));
    }

    // save_path 回退链：任务级 > 配置级（TaskManager 持有）> 空（拒绝）
    const std::string save_path = !params.save_path.empty()
        ? params.save_path : tm->save_path();
    if (save_path.empty()) {
        log_e(task_key.c_str(), "添加任务失败：save_path 未指定");
        return dw::dup_json_string(dw::make_error_response("save_path 未指定"));
    }
    // 更新参数的 save_path
    params.save_path = save_path;

    auto out_result = tm->add(params);
    if (out_result.code != DW_REASON_NONE) {
        log_e(task_key.c_str(), "添加任务失败: code={}", static_cast<int>(out_result.code));
        return dw::dup_json_string(
            dw::make_error_response(out_result.message.empty() ? "添加失败" : out_result.message));
    }

    log_i(task_key.c_str(), "添加任务完成");

    // BT 任务保存来源（magnet/torrent 用于 resume data 未生成时的兜底恢复）
    if (protocol == DW_PROTOCOL_TORRENT) {
        if (!params.magnet_link.empty() || !params.torrent_file.empty()) {
            tm->save_resume_source(client_id.c_str(), protocol, task_key.c_str(),
                                   save_path, params.magnet_link, params.torrent_file);
        }
    }

    return dw::result_to_json(std::move(out_result));
}

DW_API char *dw_pause_task(const char *params_json) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !params_json) {
        log_e("", "暂停任务失败: 参数非法");
        return dw::dup_json_string(dw::make_error_response("参数非法"));
    }

    dw::TaskParams params;
    if (dw::parse_task_params(params_json, params) != 0) {
        return dw::dup_json_string(dw::make_error_response("JSON 解析失败"));
    }
    if (params.client_id.empty() || params.natural_key.empty()) {
        return dw::dup_json_string(dw::make_error_response("参数不完整"));
    }

    const std::string &nk = params.natural_key;
    log_i(nk.c_str(), "暂停任务开始: protocol={} client_id={}", dw::to_string(params.protocol), params.client_id);

    auto *tm = d->router ? d->router->route(params.client_id.c_str()) : nullptr;
    if (!tm) {
        log_e(nk.c_str(), "暂停任务失败: 路由失败");
        return dw::dup_json_string(dw::make_error_response("路由失败"));
    }

    auto result = tm->pause(params);
    if (result.code != DW_REASON_NONE) {
        log_e(nk.c_str(), "暂停任务失败: {}", result.message);
        return dw::dup_json_string(dw::make_error_response(result.message));
    }
    log_i(nk.c_str(), "暂停任务完成");
    return dw::result_to_json(std::move(result));
}

DW_API char *dw_resume_task(const char *params_json) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !params_json) {
        log_e("", "恢复任务失败: 参数非法");
        return dw::dup_json_string(dw::make_error_response("参数非法"));
    }

    dw::TaskParams params;
    if (dw::parse_task_params(params_json, params) != 0) {
        return dw::dup_json_string(dw::make_error_response("JSON 解析失败"));
    }
    if (params.client_id.empty() || params.natural_key.empty()) {
        return dw::dup_json_string(dw::make_error_response("参数不完整"));
    }

    const std::string &nk = params.natural_key;
    log_i(nk.c_str(), "恢复任务开始: protocol={} client_id={}", dw::to_string(params.protocol), params.client_id);

    auto *tm = d->router ? d->router->route(params.client_id.c_str()) : nullptr;
    if (!tm) {
        log_e(nk.c_str(), "恢复任务失败: 路由失败");
        return dw::dup_json_string(dw::make_error_response("路由失败"));
    }

    auto result = tm->resume(params);
    if (result.code != DW_REASON_NONE) {
        log_e(nk.c_str(), "恢复任务失败: {}", result.message);
        return dw::dup_json_string(dw::make_error_response(result.message));
    }
    log_i(nk.c_str(), "恢复任务完成");
    return dw::result_to_json(std::move(result));
}

DW_API char *dw_delete_task(const char *params_json) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !params_json) {
        log_e("", "删除任务失败: 参数非法");
        return dw::dup_json_string(dw::make_error_response("参数非法"));
    }

    dw::TaskParams params;
    if (dw::parse_task_params(params_json, params) != 0) {
        return dw::dup_json_string(dw::make_error_response("JSON 解析失败"));
    }
    if (params.client_id.empty() || params.natural_key.empty()) {
        return dw::dup_json_string(dw::make_error_response("参数不完整"));
    }

    const std::string &nk = params.natural_key;
    log_i(nk.c_str(), "删除任务开始: protocol={} client_id={} delete_files={}", dw::to_string(params.protocol), params.client_id, params.delete_files);

    if (params.protocol == DW_PROTOCOL_LOCAL) {
        log_e(nk.c_str(), "删除任务失败: LOCAL 任务请使用 dw_delete_local_entry");
        return dw::dup_json_string(dw::make_error_response("LOCAL 任务请使用 dw_delete_local_entry"));
    }

    auto *tm = d->router ? d->router->route(params.client_id.c_str()) : nullptr;
    if (!tm) {
        log_e(nk.c_str(), "删除任务失败: 路由失败");
        return dw::dup_json_string(dw::make_error_response("路由失败"));
    }

    auto result = tm->remove(params);
    if (result.code != DW_REASON_NONE) {
        log_e(nk.c_str(), "删除任务失败: {}", result.message);
        return dw::dup_json_string(dw::make_error_response(result.message));
    }
    log_i(nk.c_str(), "删除任务完成");
    return dw::result_to_json(std::move(result));
}

/* ------------------------------------------------------------------ */
/*  BT 工具函数                                                       */
/* ------------------------------------------------------------------ */

DW_API char *dw_parse_magnet(const char *params_json) {
    if (!params_json) {
        return dw::dup_json_string(dw::make_error_response("参数为空"));
    }
    try {
        auto obj = boost::json::parse(params_json).as_object();
        std::string magnet = dw::json_get_string(obj, "magnet_link");
        if (magnet.empty()) {
            return dw::dup_json_string(dw::make_error_response("magnet_link 为空"));
        }
        auto result = dw::TorrentEngine::parse_magnet(magnet);
        if (result.code != DW_REASON_NONE) {
            return dw::dup_json_string(dw::make_error_response(result.message));
        }
        boost::json::object data;
        data["info_hash"] = result.info_hash;
        return dw::dup_json_string(dw::make_success_response(data));
    } catch (const std::exception &) {
        return dw::dup_json_string(dw::make_error_response("JSON 解析失败"));
    }
}

DW_API char *dw_parse_torrent_file(const char *params_json) {
    if (!params_json) {
        return dw::dup_json_string(dw::make_error_response("参数为空"));
    }
    try {
        auto obj = boost::json::parse(params_json).as_object();
        std::string path = dw::json_get_string(obj, "torrent_file");
        if (path.empty()) {
            return dw::dup_json_string(dw::make_error_response("torrent_file 为空"));
        }
        auto result = dw::TorrentEngine::parse_torrent_file(path);
        if (result.code != DW_REASON_NONE) {
            return dw::dup_json_string(dw::make_error_response(result.message));
        }
        boost::json::object data;
        data["info_hash"] = result.info_hash;
        // 文件列表
        if (!result.files.empty()) {
            boost::json::array files_arr;
            for (const auto &fi: result.files) {
                boost::json::object file_obj;
                file_obj["index"] = fi.index;
                file_obj["name"] = fi.name;
                file_obj["size"] = fi.size;
                file_obj["offset"] = fi.offset;
                file_obj["full_path"] = fi.full_path;
                file_obj["ext"] = fi.ext;
                files_arr.push_back(std::move(file_obj));
            }
            data["files"] = std::move(files_arr);
        }
        return dw::dup_json_string(dw::make_success_response(data));
    } catch (const std::exception &) {
        return dw::dup_json_string(dw::make_error_response("JSON 解析失败"));
    }
}

DW_API char *dw_info_hash_to_magnet(const char *params_json) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !params_json) {
        return dw::dup_json_string(dw::make_error_response("参数非法"));
    }

    dw::TaskIdentity identity;
    if (dw::parse_identity(params_json, identity) != 0) {
        return dw::dup_json_string(dw::make_error_response("JSON 解析失败"));
    }
    if (identity.client_id.empty() || identity.natural_key.empty()) {
        return dw::dup_json_string(dw::make_error_response("参数不完整"));
    }

    auto *tm = d->router ? d->router->route(identity.client_id.c_str()) : nullptr;
    if (!tm) {
        return dw::dup_json_string(dw::make_error_response("路由失败"));
    }

    std::scoped_lock task_lock(tm->get_mutex());
    auto *task_record = tm->load_task_record(identity.client_id.c_str(), DW_PROTOCOL_TORRENT, identity.natural_key.c_str());
    if (!task_record) {
        return dw::dup_json_string(dw::make_error_response("任务不存在"));
    }
    char *magnet = dw::TorrentEngine::info_hash_to_magnet(task_record->task_natural_key.c_str());
    if (!magnet) {
        return dw::dup_json_string(dw::make_error_response("转换失败"));
    }
    boost::json::object data;
    data["magnet_link"] = magnet;
    std::free(magnet);
    return dw::dup_json_string(dw::make_success_response(data));
}

DW_API char *dw_get_file_list(const char *params_json) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !params_json) {
        return dw::dup_json_string(dw::make_error_response("参数非法"));
    }

    dw::TaskIdentity identity;
    if (dw::parse_identity(params_json, identity) != 0) {
        return dw::dup_json_string(dw::make_error_response("JSON 解析失败"));
    }
    if (identity.client_id.empty() || identity.natural_key.empty()) {
        return dw::dup_json_string(dw::make_error_response("参数不完整"));
    }

    auto *tm = d->router ? d->router->route(identity.client_id.c_str()) : nullptr;
    if (!tm) {
        return dw::dup_json_string(dw::make_error_response("路由失败"));
    }

    const std::string &nk = identity.natural_key;
    std::scoped_lock task_lock(tm->get_mutex());
    auto *task_record = tm->load_task_record(identity.client_id.c_str(), DW_PROTOCOL_HTTP, nk.c_str());
    if (!task_record) {
        task_record = tm->load_task_record(identity.client_id.c_str(), DW_PROTOCOL_TORRENT, nk.c_str());
    }
    if (!task_record) {
        return dw::dup_json_string(dw::make_error_response("任务不存在"));
    }
    const dw_protocol_t proto = task_record->task_protocol;
    auto result = tm->load_files(proto, nk.c_str());
    if (result.code != DW_REASON_NONE || result.files.empty()) {
        return dw::dup_json_string(dw::make_error_response("无文件记录"));
    }

    // 构建 JSON 文件列表
    boost::json::array files_arr;
    for (const auto &fi: result.files) {
        boost::json::object f;
        f["index"] = fi.index;
        f["name"] = fi.name;
        f["full_path"] = fi.full_path;
        f["size"] = fi.size;
        f["ext"] = fi.ext;
        f["status"] = fi.status;
        f["offset"] = fi.offset;
        f["downloaded_bytes"] = fi.downloaded_bytes;
        f["selected"] = fi.selected;
        files_arr.push_back(std::move(f));
    }

    boost::json::object data;
    data["files"] = std::move(files_arr);
    return dw::dup_json_string(dw::make_success_response(data));
}

/* ------------------------------------------------------------------ */
/*  边下边播（区间 / 提优 / 进度）                                    */
/* ------------------------------------------------------------------ */

DW_API char *dw_get_file_ranges(const char *params_json) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !params_json) {
        return dw::dup_json_string(dw::make_error_response("参数非法"));
    }

    dw::TaskIdentity identity;
    int32_t file_index = 0;
    try {
        auto obj = boost::json::parse(params_json).as_object();
        dw::from_json(obj, identity);
        dw::json_util::extract(obj, "file_index", file_index);
    } catch (const std::exception &) {
        return dw::dup_json_string(dw::make_error_response("JSON 解析失败"));
    }
    if (identity.client_id.empty() || identity.natural_key.empty()) {
        return dw::dup_json_string(dw::make_error_response("参数不完整"));
    }

    auto *tm = d->router ? d->router->route(identity.client_id.c_str()) : nullptr;
    if (!tm) {
        return dw::dup_json_string(dw::make_error_response("路由失败"));
    }

    const std::string &nk = identity.natural_key;
    std::scoped_lock task_lock(tm->get_mutex());
    auto *task_record = tm->load_task_record(identity.client_id.c_str(), DW_PROTOCOL_HTTP, nk.c_str());
    if (!task_record) {
        task_record = tm->load_task_record(identity.client_id.c_str(), DW_PROTOCOL_TORRENT, nk.c_str());
    }
    if (!task_record) {
        return dw::dup_json_string(dw::make_error_response("任务不存在"));
    }
    const dw_protocol_t proto = task_record->task_protocol;

    // 1. 优先读内存缓存
    std::vector<dw_byte_range_t> vec = tm->get_cached_segments(proto, nk.c_str(), file_index);
    if (!vec.empty()) {
        boost::json::array ranges_arr;
        for (const auto &r : vec) {
            boost::json::array pair;
            pair.push_back(r.start);
            pair.push_back(r.end);
            ranges_arr.push_back(std::move(pair));
        }
        boost::json::object data;
        data["ranges"] = std::move(ranges_arr);
        return dw::dup_json_string(dw::make_success_response(data));
    }

    // 2. 缓存为空：按任务状态决定行为
    const int32_t status = tm->get_task_status(proto, nk.c_str());
    if (status < 0) {
        return dw::dup_json_string(dw::make_error_response("任务不存在"));
    }
    const bool downloading = (status == DW_TASK_STATUS_DOWNLOADING ||
                              status == DW_TASK_STATUS_RESOLVING ||
                              status == DW_TASK_STATUS_PARSED);
    if (downloading) {
        // 下载中但缓存为空，返回空区间
        boost::json::object data;
        data["ranges"] = boost::json::array{};
        return dw::dup_json_string(dw::make_success_response(data));
    }

    // 3. 非下载中：回退 DB 快照
    vec = tm->load_segments(proto, nk.c_str(), file_index);
    boost::json::array ranges_arr;
    for (const auto &r : vec) {
        boost::json::array pair;
        pair.push_back(r.start);
        pair.push_back(r.end);
        ranges_arr.push_back(std::move(pair));
    }
    boost::json::object data;
    data["ranges"] = std::move(ranges_arr);
    return dw::dup_json_string(dw::make_success_response(data));
}

DW_API char *dw_get_task_file_info(const char *params_json) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !params_json) {
        return dw::dup_json_string(dw::make_error_response("参数非法"));
    }

    dw::TaskIdentity identity;
    int32_t file_index = 0;
    try {
        auto obj = boost::json::parse(params_json).as_object();
        dw::from_json(obj, identity);
        dw::json_util::extract(obj, "file_index", file_index);
    } catch (const std::exception &) {
        return dw::dup_json_string(dw::make_error_response("JSON 解析失败"));
    }
    if (identity.client_id.empty() || identity.natural_key.empty()) {
        return dw::dup_json_string(dw::make_error_response("参数不完整"));
    }

    auto *tm = d->router ? d->router->route(identity.client_id.c_str()) : nullptr;
    if (!tm) {
        return dw::dup_json_string(dw::make_error_response("路由失败"));
    }

    const std::string &nk = identity.natural_key;
    std::scoped_lock task_lock(tm->get_mutex());
    auto *task_record = tm->load_task_record(identity.client_id.c_str(), DW_PROTOCOL_HTTP, nk.c_str());
    if (!task_record) {
        task_record = tm->load_task_record(identity.client_id.c_str(), DW_PROTOCOL_TORRENT, nk.c_str());
    }
    if (!task_record) {
        return dw::dup_json_string(dw::make_error_response("任务不存在"));
    }
    const dw_protocol_t proto = task_record->task_protocol;
    std::string file_path;
    int64_t file_size = -1;
    if (!tm->resolve_file_path(proto, nk.c_str(), file_index, file_path, file_size)) {
        return dw::dup_json_string(dw::make_error_response("无法解析文件路径"));
    }

    boost::json::object data;
    data["path"] = file_path;
    data["size"] = file_size;
    return dw::dup_json_string(dw::make_success_response(data));
}

DW_API char *dw_set_play_position(const char *params_json) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !params_json) {
        return dw::dup_json_string(dw::make_error_response("参数非法"));
    }

    dw::TaskIdentity identity;
    int32_t file_index = 0;
    int64_t position_ms = 0;
    try {
        auto obj = boost::json::parse(params_json).as_object();
        dw::from_json(obj, identity);
        dw::json_util::extract(obj, "file_index", file_index);
        dw::json_util::extract(obj, "position_ms", position_ms);
    } catch (const std::exception &) {
        return dw::dup_json_string(dw::make_error_response("JSON 解析失败"));
    }
    if (identity.client_id.empty() || identity.natural_key.empty()) {
        return dw::dup_json_string(dw::make_error_response("参数不完整"));
    }

    auto *tm = d->router ? d->router->route(identity.client_id.c_str()) : nullptr;
    if (!tm) {
        return dw::dup_json_string(dw::make_error_response("路由失败"));
    }

    const std::string &nk = identity.natural_key;
    std::scoped_lock task_lock(tm->get_mutex());
    auto *task_record = tm->load_task_record(identity.client_id.c_str(), DW_PROTOCOL_HTTP, nk.c_str());
    if (!task_record) {
        task_record = tm->load_task_record(identity.client_id.c_str(), DW_PROTOCOL_TORRENT, nk.c_str());
    }
    if (!task_record) {
        return dw::dup_json_string(dw::make_error_response("任务不存在"));
    }
    tm->set_play_position(task_record->task_protocol, nk.c_str(), file_index, position_ms);
    return dw::dup_json_string(dw::make_success_response());
}

DW_API char *dw_get_play_position(const char *params_json) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !params_json) {
        return dw::dup_json_string(dw::make_error_response("参数非法"));
    }

    dw::TaskIdentity identity;
    int32_t file_index = 0;
    try {
        auto obj = boost::json::parse(params_json).as_object();
        dw::from_json(obj, identity);
        dw::json_util::extract(obj, "file_index", file_index);
    } catch (const std::exception &) {
        return dw::dup_json_string(dw::make_error_response("JSON 解析失败"));
    }
    if (identity.client_id.empty() || identity.natural_key.empty()) {
        return dw::dup_json_string(dw::make_error_response("参数不完整"));
    }

    auto *tm = d->router ? d->router->route(identity.client_id.c_str()) : nullptr;
    if (!tm) {
        boost::json::object data;
        data["position_ms"] = 0;
        return dw::dup_json_string(dw::make_success_response(data));
    }

    const std::string &nk = identity.natural_key;
    std::scoped_lock task_lock(tm->get_mutex());
    auto *task_record = tm->load_task_record(identity.client_id.c_str(), DW_PROTOCOL_HTTP, nk.c_str());
    if (!task_record) {
        task_record = tm->load_task_record(identity.client_id.c_str(), DW_PROTOCOL_TORRENT, nk.c_str());
    }
    if (!task_record) {
        boost::json::object data;
        data["position_ms"] = 0;
        return dw::dup_json_string(dw::make_success_response(data));
    }
    const int64_t pos = tm->get_play_position(task_record->task_protocol, nk.c_str(), file_index);
    boost::json::object data;
    data["position_ms"] = pos;
    return dw::dup_json_string(dw::make_success_response(data));
}

/* ------------------------------------------------------------------ */
/*  任务快照与队列                                              */
/* ------------------------------------------------------------------ */

DW_API char *dw_list_tasks(const char *params_json) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !params_json) {
        return dw::dup_json_string(dw::make_error_response("参数非法"));
    }

    std::string client_id;
    try {
        auto obj = boost::json::parse(params_json).as_object();
        client_id = dw::json_get_string(obj, "client_id");
    } catch (const std::exception &) {
        return dw::dup_json_string(dw::make_error_response("JSON 解析失败"));
    }
    if (client_id.empty()) {
        return dw::dup_json_string(dw::make_error_response("client_id 为空"));
    }

    auto *tm = d->router ? d->router->route(client_id.c_str()) : nullptr;
    if (!tm) {
        return dw::dup_json_string(dw::make_error_response("路由失败"));
    }

    dw_task_snapshot_t *tasks = nullptr;
    int32_t count = 0;
    if (tm->list(&tasks, &count) != 0 || count <= 0) {
        boost::json::object data;
        data["tasks"] = boost::json::array{};
        return dw::dup_json_string(dw::make_success_response(data));
    }

    // 构建 JSON 任务列表
    boost::json::array tasks_arr;
    for (int32_t i = 0; i < count; ++i) {
        boost::json::object t;
        t["natural_key"] = tasks[i].natural_key ? tasks[i].natural_key : "";
        t["url"] = tasks[i].url ? tasks[i].url : "";
        t["info_hash"] = tasks[i].info_hash ? tasks[i].info_hash : "";
        t["name"] = tasks[i].name ? tasks[i].name : "";
        t["save_path"] = tasks[i].save_path ? tasks[i].save_path : "";
        t["content_root"] = tasks[i].content_root ? tasks[i].content_root : "";
        t["status"] = tasks[i].status;
        t["total_size"] = tasks[i].total_size;
        t["total_done"] = tasks[i].total_done;
        t["priority"] = tasks[i].priority;
        t["created_at"] = tasks[i].created_at;
        t["modified_at"] = tasks[i].modified_at;
        tasks_arr.push_back(std::move(t));
    }
    dw_task_list_free(tasks, count);

    boost::json::object data;
    data["tasks"] = std::move(tasks_arr);
    return dw::dup_json_string(dw::make_success_response(data));
}

DW_API char *dw_set_task_priority(const char *params_json) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !params_json) {
        return dw::dup_json_string(dw::make_error_response("参数非法"));
    }

    dw::TaskParams params;
    if (dw::parse_task_params(params_json, params) != 0) {
        return dw::dup_json_string(dw::make_error_response("JSON 解析失败"));
    }
    if (params.client_id.empty() || params.natural_key.empty()) {
        return dw::dup_json_string(dw::make_error_response("参数不完整"));
    }

    auto *tm = d->router ? d->router->route(params.client_id.c_str()) : nullptr;
    if (!tm) {
        return dw::dup_json_string(dw::make_error_response("路由失败"));
    }

    // 补充 protocol（调用方可能未传递）
    if (params.protocol == DW_PROTOCOL_LOCAL) {
        std::scoped_lock task_lock(tm->get_mutex());
        auto *task_record = tm->load_task_record(params.client_id.c_str(), DW_PROTOCOL_HTTP, params.natural_key.c_str());
        if (!task_record) {
            task_record = tm->load_task_record(params.client_id.c_str(), DW_PROTOCOL_TORRENT, params.natural_key.c_str());
        }
        if (task_record) {
            params.protocol = task_record->task_protocol;
        } else {
            return dw::dup_json_string(dw::make_error_response("任务不存在"));
        }
    }

    auto result = tm->resume(params);
    if (result.code != DW_REASON_NONE) {
        return dw::dup_json_string(dw::make_error_response(result.message));
    }
    return dw::dup_json_string(dw::make_success_response());
}

/* ------------------------------------------------------------------ */
/*  任务文件查询                                                      */
/* ------------------------------------------------------------------ */

DW_API char *dw_load_task_files(const char *params_json) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !params_json) {
        return dw::dup_json_string(dw::make_error_response("参数非法"));
    }

    dw::TaskIdentity identity;
    if (dw::parse_identity(params_json, identity) != 0) {
        return dw::dup_json_string(dw::make_error_response("JSON 解析失败"));
    }
    if (identity.client_id.empty() || identity.natural_key.empty()) {
        return dw::dup_json_string(dw::make_error_response("参数不完整"));
    }

    auto *tm = d->router ? d->router->route(identity.client_id.c_str()) : nullptr;
    if (!tm) {
        return dw::dup_json_string(dw::make_error_response("路由失败"));
    }

    const std::string &nk = identity.natural_key;
    std::scoped_lock task_lock(tm->get_mutex());
    auto *task_record = tm->load_task_record(identity.client_id.c_str(), DW_PROTOCOL_HTTP, nk.c_str());
    if (!task_record) {
        task_record = tm->load_task_record(identity.client_id.c_str(), DW_PROTOCOL_TORRENT, nk.c_str());
    }
    if (!task_record) {
        return dw::dup_json_string(dw::make_error_response("任务不存在"));
    }
    auto result = tm->load_files(task_record->task_protocol, nk.c_str());
    if (result.code != DW_REASON_NONE || result.files.empty()) {
        return dw::dup_json_string(dw::make_error_response("无文件记录"));
    }

    boost::json::array files_arr;
    for (const auto &fi: result.files) {
        boost::json::object f;
        f["index"] = fi.index;
        f["name"] = fi.name;
        f["full_path"] = fi.full_path;
        f["size"] = fi.size;
        f["ext"] = fi.ext;
        f["status"] = fi.status;
        f["offset"] = fi.offset;
        f["downloaded_bytes"] = fi.downloaded_bytes;
        f["selected"] = fi.selected;
        files_arr.push_back(std::move(f));
    }

    boost::json::object data;
    data["files"] = std::move(files_arr);
    return dw::dup_json_string(dw::make_success_response(data));
}

/* ------------------------------------------------------------------ */
/*  本地文件浏览与管理                                                */
/* ------------------------------------------------------------------ */

DW_API char *dw_scan_local_tasks(const char *params_json) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !params_json) {
        return dw::dup_json_string(dw::make_error_response("参数非法"));
    }

    std::string client_id, save_path;
    try {
        auto obj = boost::json::parse(params_json).as_object();
        client_id = dw::json_get_string(obj, "client_id");
        save_path = dw::json_get_string(obj, "save_path");
    } catch (const std::exception &) {
        return dw::dup_json_string(dw::make_error_response("JSON 解析失败"));
    }
    if (client_id.empty() || save_path.empty()) {
        return dw::dup_json_string(dw::make_error_response("参数不完整"));
    }

    auto *tm = d->router ? d->router->route(client_id.c_str()) : nullptr;
    if (!tm) {
        return dw::dup_json_string(dw::make_error_response("路由失败"));
    }

    dw_task_snapshot_t *tasks = nullptr;
    int32_t count = 0;
    tm->scan_local_tasks(save_path.c_str(), &tasks, &count);

    boost::json::array tasks_arr;
    if (tasks && count > 0) {
        for (int32_t i = 0; i < count; ++i) {
            boost::json::object t;
            t["natural_key"] = tasks[i].natural_key ? tasks[i].natural_key : "";
            t["name"] = tasks[i].name ? tasks[i].name : "";
            t["save_path"] = tasks[i].save_path ? tasks[i].save_path : "";
            t["status"] = tasks[i].status;
            t["total_size"] = tasks[i].total_size;
            tasks_arr.push_back(std::move(t));
        }
        dw_task_list_free(tasks, count);
    }

    boost::json::object data;
    data["tasks"] = std::move(tasks_arr);
    return dw::dup_json_string(dw::make_success_response(data));
}

DW_API char *dw_validate_local_tasks(const char *params_json) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !params_json) {
        return dw::dup_json_string(dw::make_error_response("参数非法"));
    }

    std::string client_id, save_path;
    try {
        auto obj = boost::json::parse(params_json).as_object();
        client_id = dw::json_get_string(obj, "client_id");
        save_path = dw::json_get_string(obj, "save_path");
    } catch (const std::exception &) {
        return dw::dup_json_string(dw::make_error_response("JSON 解析失败"));
    }
    if (client_id.empty() || save_path.empty()) {
        return dw::dup_json_string(dw::make_error_response("参数不完整"));
    }

    auto *tm = d->router ? d->router->route(client_id.c_str()) : nullptr;
    if (!tm) {
        return dw::dup_json_string(dw::make_error_response("路由失败"));
    }

    int32_t invalidated_count = 0;
    tm->validate_local_tasks(save_path.c_str(), &invalidated_count);

    boost::json::object data;
    data["invalidated_count"] = invalidated_count;
    return dw::dup_json_string(dw::make_success_response(data));
}

DW_API char *dw_clear_local_tasks(const char *params_json) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !params_json) {
        return dw::dup_json_string(dw::make_error_response("参数非法"));
    }

    std::string client_id, save_path;
    try {
        auto obj = boost::json::parse(params_json).as_object();
        client_id = dw::json_get_string(obj, "client_id");
        save_path = dw::json_get_string(obj, "save_path");
    } catch (const std::exception &) {
        return dw::dup_json_string(dw::make_error_response("JSON 解析失败"));
    }
    if (client_id.empty() || save_path.empty()) {
        return dw::dup_json_string(dw::make_error_response("参数不完整"));
    }

    log_i("", "清理本地任务开始: client_id={} save_path={}", client_id, save_path);
    auto *tm = d->router ? d->router->route(client_id.c_str()) : nullptr;
    if (!tm) {
        return dw::dup_json_string(dw::make_error_response("路由失败"));
    }
    const int32_t rc = tm->clear_local_tasks(save_path.c_str());
    log_i("", "清理本地任务完成: rc={}", rc);
    return dw::dup_json_string(dw::make_success_response());
}

DW_API char *dw_delete_local_entry(const char *params_json) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !params_json) {
        return dw::dup_json_string(dw::make_error_response("参数非法"));
    }

    std::string client_id, save_path, root_name;
    try {
        auto obj = boost::json::parse(params_json).as_object();
        client_id = dw::json_get_string(obj, "client_id");
        save_path = dw::json_get_string(obj, "save_path");
        root_name = dw::json_get_string(obj, "root_name");
    } catch (const std::exception &) {
        return dw::dup_json_string(dw::make_error_response("JSON 解析失败"));
    }
    if (client_id.empty() || save_path.empty() || root_name.empty()) {
        return dw::dup_json_string(dw::make_error_response("参数不完整"));
    }

    log_i("", "删除本地条目开始: client_id={} root_name={}", client_id, root_name);
    auto *tm = d->router ? d->router->route(client_id.c_str()) : nullptr;
    if (!tm) {
        return dw::dup_json_string(dw::make_error_response("路由失败"));
    }
    const int32_t rc = tm->delete_local_entry(save_path.c_str(), root_name.c_str());
    log_i("", "删除本地条目完成: rc={}", rc);
    if (rc != 0) {
        return dw::dup_json_string(dw::make_error_response("删除失败"));
    }
    return dw::dup_json_string(dw::make_success_response());
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

DW_API char *dw_list_file_records(const char *params_json) {
    auto *d = dw::global_downloader();
    if (!d || !d->initialized.load() || !params_json) {
        return dw::dup_json_string(dw::make_error_response("参数非法"));
    }

    std::string client_id;
    try {
        auto obj = boost::json::parse(params_json).as_object();
        client_id = dw::json_get_string(obj, "client_id");
    } catch (const std::exception &) {
        return dw::dup_json_string(dw::make_error_response("JSON 解析失败"));
    }
    if (client_id.empty()) {
        return dw::dup_json_string(dw::make_error_response("client_id 为空"));
    }

    auto *tm = d->router ? d->router->route(client_id.c_str()) : nullptr;
    if (!tm) {
        return dw::dup_json_string(dw::make_error_response("路由失败"));
    }

    auto vec = tm->list_file_records();
    boost::json::array records_arr;
    for (const auto &r : vec) {
        boost::json::object rec;
        rec["id"] = r.id;
        rec["client_id"] = r.client_id;
        rec["type"] = r.type;
        rec["is_remote"] = r.is_remote;
        rec["save_path"] = r.save_path;
        rec["original_root_name"] = r.original_root_name;
        rec["root_name"] = r.root_name;
        rec["full_path"] = r.full_path;
        rec["file_type"] = r.file_type;
        rec["ext"] = r.ext;
        rec["parsed"] = r.parsed;
        rec["task_protocol"] = static_cast<int>(r.task_protocol);
        rec["task_natural_key"] = r.task_natural_key;
        rec["status"] = r.status;
        rec["total_size"] = r.total_size;
        rec["total_done"] = r.total_done;
        rec["priority"] = r.priority;
        rec["reason"] = r.reason;
        rec["message"] = r.message;
        rec["created_at"] = r.created_at;
        rec["modified_at"] = r.modified_at;
        records_arr.push_back(std::move(rec));
    }

    boost::json::object data;
    data["records"] = std::move(records_arr);
    return dw::dup_json_string(dw::make_success_response(data));
}

DW_API void dw_file_record_list_free(dw_file_record_t *records, int32_t count) {
    if (!records || count <= 0) return;
    for (int32_t i = 0; i < count; ++i) {
        std::free(records[i].client_id);
        std::free(records[i].save_path);
        std::free(records[i].original_root_name);
        std::free(records[i].root_name);
        std::free(records[i].full_path);
        std::free(records[i].ext);
        std::free(records[i].task_natural_key);
        std::free(records[i].message);
    }
    std::free(records);
}

DW_API char *dw_is_file_record_parsed(const char *params_json) {
    if (!params_json) {
        return dw::dup_json_string(dw::make_error_response("参数非法"));
    }
    if (!dw::g_downloader) {
        return dw::dup_json_string(dw::make_error_response("下载器未初始化"));
    }

    dw::TaskIdentity identity;
    if (dw::parse_identity(params_json, identity) != 0) {
        return dw::dup_json_string(dw::make_error_response("JSON 解析失败"));
    }
    if (identity.client_id.empty() || identity.natural_key.empty()) {
        return dw::dup_json_string(dw::make_error_response("参数不完整"));
    }

    auto *tm = dw::g_downloader->router ? dw::g_downloader->router->task_manager() : nullptr;
    if (!tm) {
        return dw::dup_json_string(dw::make_error_response("TaskManager 不可用"));
    }
    const bool parsed = tm->is_file_record_parsed(
        identity.client_id.c_str(), identity.protocol, identity.natural_key.c_str());

    boost::json::object data;
    data["parsed"] = parsed;
    return dw::dup_json_string(dw::make_success_response(data));
}

DW_API void dw_free(void *ptr) {
    if (ptr) {
        std::free(ptr);
    }
}
} /* extern "C" */
