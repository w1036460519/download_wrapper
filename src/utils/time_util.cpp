/**
 * @file time_util.cpp
 * @brief 通用时间工具实现。
 */
#include "utils/time_util.h"

#include <boost/date_time/posix_time/posix_time.hpp>
#include <chrono>
#include <iomanip>
#include <sstream>

namespace dw::utils {
    int64_t now_unix_ms() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                .count();
    }

    std::string format_unix_ms(const int64_t ms) {
        using namespace boost::posix_time;
        const ptime pt(from_time_t(0) + milliseconds(ms));
        std::ostringstream oss;
        auto *facet = new time_facet("%Y-%m-%d %H:%M:%S");
        oss.imbue(std::locale(oss.getloc(), facet));
        oss << pt << '.' << std::setfill('0') << std::setw(3) << (ms % 1000);
        return oss.str();
    }
} // namespace dw::utils
