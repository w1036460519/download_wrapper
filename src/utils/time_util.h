/**
 * @file time_util.h
 * @brief 通用时间工具：Unix 毫秒时间戳获取与本地时间格式化。
 *
 * 库内多个模块（任务中枢、HTTP/BT 引擎、日志）均需获取时间戳与格式化时间，
 * 统一收敛到此处，避免各文件重复实现。
 */
#ifndef DW_UTILS_TIME_UTIL_H
#define DW_UTILS_TIME_UTIL_H

#include <cstdint>
#include <string>

namespace dw { namespace utils {

/**
 * 获取当前 Unix 时间戳（毫秒）。
 * @return 自 epoch 起经过的毫秒数。
 */
int64_t now_unix_ms();

/**
 * 将 Unix 毫秒时间戳格式化为本地时间字符串。
 * @param ms Unix 毫秒时间戳。
 * @return 形如 "YYYY-MM-DD HH:MM:SS" 的本地时间字符串。
 */
std::string format_unix_ms(int64_t ms);

}} // namespace dw::utils

#endif /* DW_UTILS_TIME_UTIL_H */
