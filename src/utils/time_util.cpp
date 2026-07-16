/**
 * @file time_util.cpp
 * @brief 通用时间工具实现。
 */
#include "utils/time_util.h"

#include <chrono>
#include <ctime>

namespace dw::utils {
    int64_t now_unix_ms() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                .count();
    }

    std::string format_unix_ms(int64_t ms) {
        const std::time_t sec = static_cast<std::time_t>(ms / 1000);
        std::tm tm_buf{};
        // localtime_r 为 POSIX 专有；MSVC 用参数顺序一致的 localtime_s
#if defined(_WIN32)
        localtime_s(&tm_buf, &sec);
#else
        localtime_r(&sec, &tm_buf);
#endif
        char buf[24];
        std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tm_buf);
        return std::string(buf);
    }
} // namespace dw::utils
