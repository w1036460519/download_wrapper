/**
 * @file http_engine.h
 * @brief HTTP 下载引擎内部实现头文件。
 */

#ifndef DW_HTTP_ENGINE_H
#define DW_HTTP_ENGINE_H

#include "download_wrapper/download_wrapper.h"

#include <cstdint>
#include <string>

namespace dw {

/**
 * HTTP 下载引擎。
 *
 * 内部基于 libcurl 实现，负责管理 HTTP/HTTPS 下载任务的生命周期、
 * 进度回调、分片下载等。对外通过 dw_* C ABI 接口间接调用。
 */
class HttpEngine {
public:
    HttpEngine();
    ~HttpEngine();

    HttpEngine(const HttpEngine&)            = delete;
    HttpEngine& operator=(const HttpEngine&) = delete;

    /**
     * 初始化引擎。
     * @return 0=成功，-1=失败。
     */
    int32_t init(const dw_config_t* cfg);

    /**
     * 销毁引擎，释放所有资源。
     */
    void destroy();

    /**
     * 添加单个 HTTP 下载任务。
     */
    int32_t add_task(const dw_task_params_t* params,
                     dw_submit_result_t*     out_result);

    /**
     * 暂停单个 HTTP 下载任务。
     */
    int32_t pause_task(const char*         id,
                       dw_submit_result_t* out_result);

    /**
     * 恢复单个 HTTP 下载任务。
     */
    int32_t resume_task(const dw_task_params_t* params,
                        dw_submit_result_t*     out_result);

    /**
     * 删除单个 HTTP 下载任务。
     */
    int32_t delete_task(const char*         id,
                        dw_submit_result_t* out_result);

private:
    bool initialized_ = false;
};

} // namespace dw

#endif /* DW_HTTP_ENGINE_H */
