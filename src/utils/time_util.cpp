/**
 * @file time_util.cpp
 * @brief 通用时间工具实现。
 */
#include "utils/time_util.h"

#include <chrono>
#include <format>

namespace dw::utils {
    int64_t now_unix_ms() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                .count();
    }

    std::string format_unix_ms(const int64_t ms) {
        // C++20 chrono 格式化：按本地时区输出，毫秒经整型零填充单独拼接。
        // 要求部署目标 ≥ macOS 13.3 / iOS 16.3（libc++ 格式化符号门槛）。
        const std::chrono::sys_time<std::chrono::milliseconds> tp{std::chrono::milliseconds(ms)};
        return std::format("{:%Y-%m-%d %H:%M:%S}.{:03}",
                           std::chrono::floor<std::chrono::seconds>(tp), ms % 1000);
    }
} // namespace dw::utils
