/**
 * @file http_engine.h
 * @brief HTTP 下载引擎内部实现头文件。
 */

#ifndef DW_HTTP_ENGINE_H
#define DW_HTTP_ENGINE_H

#include "download_wrapper/download_wrapper.h"
#include "internal/engine_interface.h"

#include <cstdint>
#include <string>
#include <vector>

namespace dw {

struct EngineProgress;  // 定义见 internal/downloader_internal.h

/**
 * HTTP 下载引擎（IDownloadEngine 实现）。
 *
 * 内部基于 libcurl 实现，负责管理 HTTP/HTTPS 下载任务的生命周期、
 * 进度回调、分片下载等。对外通过 dw_* C ABI 接口间接调用。
 */
class HttpEngine final : public IDownloadEngine {
public:
    HttpEngine();
    ~HttpEngine() override;

    HttpEngine(const HttpEngine&)            = delete;
    HttpEngine& operator=(const HttpEngine&) = delete;

    /**
     * 初始化引擎。
     * @return 0=成功，-1=失败。
     */
    int32_t init(const dw_config_t* cfg) override;

    /**
     * 销毁引擎，释放所有资源。
     */
    void destroy() override;

    /**
     * 添加单个 HTTP 下载任务。
     */
    int32_t add_task(const dw_task_params_t* params,
                     dw_submit_result_t*     out_result) override;

    /**
     * 暂停单个 HTTP 下载任务。
     */
    int32_t pause_task(const char*         id,
                       dw_submit_result_t* out_result) override;

    /**
     * 删除单个 HTTP 下载任务：仅置取消 + 删除标志立即返回，资源回收由 sweep
     * 执行，不涉及落盘文件（文件删除归 TaskManager）。
     * 返回 0=已接管释放；1=未持有该任务（无运行时资源）；-1=错误。
     */
    int32_t delete_task(const char*         id,
                        dw_submit_result_t* out_result) override;

    /**
     * 查询任务运行时资源是否已释放：ctx 已不在任务表（sweep 已析构，线程 join、
     * 分片文件句柄全关）即视为已释放；引擎未初始化 / 未持有该任务同样视为已释放。
     */
    bool task_released(const char* id) override;

    /**
     * 查询单个 HTTP 任务的进度快照（拉模型）。
     * 同步从运行时上下文拼装 out；任务存在于引擎时置 out.valid=true 并返回 true。
     * @param key HTTP 任务键（即 url）。
     */
    bool query_progress(const char* key, EngineProgress& out) override;

    /**
     * 查询单文件已下载字节区间（边下边播；HTTP 单文件模型，忽略 file_index）。
     * 由现有 parts 的 [start, start+done-1] 排序合并连续段；
     * 任务不存在于运行时上下文时返回空 vector。
     */
    std::vector<dw_byte_range_t> get_file_ranges(const char* id,
                                                 int32_t     file_index) override;

    /**
     * 周期性维护策略：回收线程已退出（终态 / 暂停 / 删除中）任务的上下文
     *（join 线程 + 释放 curl/文件句柄），不涉及落盘文件。
     * 由上层调度循环定时调用；下载中任务保留。
     */
    void sweep() override;

private:
    bool initialized_ = false;
};

} // namespace dw

#endif /* DW_HTTP_ENGINE_H */