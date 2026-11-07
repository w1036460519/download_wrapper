/**
 * @file http_engine.h
 * @brief HTTP 下载引擎内部实现头文件。
 */

#pragma once

#include "download_wrapper/download_wrapper.h"
#include "internal/engine_interface.h"

#include <cstdint>
#include <vector>

namespace dw {

/**
 * HTTP 下载引擎（IDownloadEngine 实现）。
 *
 * 内部基于 libcurl 实现，负责管理 HTTP/HTTPS 下载任务的生命周期、
 * 进度回调、分片下载等。对外通过 dw_* C ABI 接口间接调用。
 */
class HttpEngine final : public IDownloadEngine {
public:
    explicit HttpEngine(TaskManager *task_manager);
    ~HttpEngine() override;

    HttpEngine(const HttpEngine&)            = delete;
    HttpEngine& operator=(const HttpEngine&) = delete;

    /**
     * 初始化引擎。
     * @return 0=成功，-1=失败。
     */
    int32_t init() override;

    /**
     * 销毁引擎，释放所有资源。
     */
    void destroy() override;

    /**
     * 添加单个 HTTP 下载任务。
     */
    dw_submit_result_t add_task(const TaskParams *params) override;

    dw_submit_result_t resume_task(const TaskParams *params) override;

    /// 调度准入后，读取保存路径和续传数据并启动下载。
    dw_submit_result_t do_resume(const TaskParams *params) override;

    /**
     * 暂停单个 HTTP 下载任务。
     */
    dw_submit_result_t pause_task(const TaskParams *params) override;

    /**
     * 设置取消和删除标志，待下载线程退出后由 sweep 回收上下文。
     */
    dw_submit_result_t delete_task(const TaskParams *params) override;

    /**
     * 周期性维护策略：回收线程已退出（终态 / 暂停 / 删除中）任务的上下文
     *（join 线程 + 释放 curl/文件句柄），不涉及落盘文件。
     * 由上层调度循环定时调用；下载中任务保留。
     */
    void sweep() override;

private:
    dw_submit_result_t start_download(const TaskParams *params, bool replace_record);
    bool initialized_ = false;
};

} // namespace dw

