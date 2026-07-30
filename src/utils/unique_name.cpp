/**
 * @file unique_name.cpp
 * @brief 唯一文件/目录名解析实现。
 */

#include "utils/unique_name.h"

#include <chrono>
#include <filesystem>
#include <system_error>

namespace dw::utils {

namespace {

/// 名称是否已被占用：磁盘存在 或 命中占用名集合。
bool name_taken(const std::filesystem::path& dir,
                const std::string& name,
                const std::unordered_set<std::string>& taken_names) {
    if (taken_names.count(name) > 0) return true;
    std::error_code ec;
    return std::filesystem::exists(dir / name, ec);
}

} // namespace

std::string resolve_unique_name(const std::string& dir,
                                const std::string& name,
                                const std::unordered_set<std::string>& taken_names) {
    const std::filesystem::path base_dir(dir);
    if (name.empty() || !name_taken(base_dir, name, taken_names)) {
        return name;
    }

    // 整名尾部加序号（唯一名用作包层目录名，不拆 stem/ext，带点名也不会插错位置）。
    for (int n = 1; n <= 9999; ++n) {
        const std::string candidate = name + "(" + std::to_string(n) + ")";
        if (!name_taken(base_dir, candidate, taken_names)) {
            return candidate;
        }
    }

    // 兜底：毫秒时间戳后缀，几乎不可能再冲突。
    const auto ts = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    return name + "_" + std::to_string(ts);
}

} // namespace dw::utils
