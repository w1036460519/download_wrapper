/**
 * @file string_util.h
 * @brief 通用字符串工具：分隔符 join/split（用于 trackers、file_indexes 等序列化）。
 *
 * 库内多处需要把字符串数组 / 整型数组与单一分隔符文本互转，
 * 统一收敛到此处，避免各文件重复实现。
 */
#ifndef DW_UTILS_STRING_UTIL_H
#define DW_UTILS_STRING_UTIL_H

#include <cstdint>
#include <string>
#include <vector>

namespace dw::utils {
    /**
 * 以 '\n' 连接字符串数组。
 * @param v 待连接的字符串数组。
 * @return 以换行连接后的文本；空数组返回空串。
 */
    std::string join_lines(const std::vector<std::string> &v);

    /**
 * 按 '\n' 拆分文本，忽略空行。
 * @param s 待拆分文本。
 * @return 非空行组成的数组。
 */
    std::vector<std::string> split_lines(const std::string &s);

    /**
 * 以 ',' 连接整型数组。
 * @param v 待连接的整型数组。
 * @return 以逗号连接后的文本；空数组返回空串。
 */
    std::string join_ints(const std::vector<int32_t> &v);

    /**
 * 按 ',' 拆分整型数组，忽略空段。
 * @param s 待拆分文本。
 * @return 解析出的整型数组。
 */
    std::vector<int32_t> split_ints(const std::string &s) noexcept;
}

#endif /* DW_UTILS_STRING_UTIL_H */
