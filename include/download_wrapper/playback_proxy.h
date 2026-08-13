#pragma once

#include "download_wrapper/download_wrapper.h"

#ifdef __cplusplus
extern "C" {
#endif

/// 启动本地 HTTP 代理服务器（边下边播）
///
/// 在 127.0.0.1 随机端口启动 Boost.Beast HTTP 服务，
/// 根据本地已下载分段向播放器提供数据流。
///
/// \return 端口号（>0 成功，<=0 失败）
DW_API int dw_proxy_start(void);

/// 停止本地 HTTP 代理服务器
DW_API void dw_proxy_stop(void);

/// 生成代理 URL
///
/// URL 形如 http://127.0.0.1:<port>/file?type=<http|bt>&key=<urlencoded natural_key>&file=<index>
/// 调用方须保证 key 在返回的 URL 使用期间有效（线程局部静态存储）。
///
/// \param key        任务唯一键：key_type + natural_key。
/// \param file_index 文件索引（HTTP 恒 0）。
/// \return URL 字符串（线程局部静态存储，下次调用覆盖）。
DW_API const char* dw_proxy_get_url(const dw_task_key_t* key, int file_index);

/// 查询代理是否运行中
///
/// \return 1 运行中，0 未运行
DW_API int dw_proxy_is_running(void);

#ifdef __cplusplus
}
#endif
