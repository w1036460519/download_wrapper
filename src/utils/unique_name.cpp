/**
 * @file unique_name.cpp
 * @brief 唯一文件/目录名抢占实现。
 */

#include "utils/unique_name.h"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <system_error>

namespace dw::utils {

namespace {

/// 条目是否已存在（文件或目录均视为占用）。
bool entry_exists(const std::filesystem::path &p) {
    std::error_code ec;
    return std::filesystem::exists(p, ec);
}

/// 按形态创建占位条目：is_dir 建目录，否则建零字节文件（引擎后续沿用同一文件写入）。
/// @return true=创建成功（本次持有该名）；false=失败，ec 携原因。
bool create_entry(const std::filesystem::path &p, const bool is_dir, std::error_code &ec) {
    ec.clear();
    if (is_dir) {
        std::filesystem::create_directories(p, ec);
        return !ec;
    }
    std::filesystem::create_directories(p.parent_path(), ec);
    if (ec) return false;
    const std::ofstream ofs(p, std::ios::binary | std::ios::app);
    if (!ofs) {
        ec = std::make_error_code(std::errc::io_error);
        return false;
    }
    return true;
}

} // namespace

std::string acquire_unique_name(const std::string &dir,
                                const std::string &name,
                                const bool multi_file,
                                std::string *out_error) {
    if (out_error) out_error->clear();
    if (name.empty()) return name;
    const std::filesystem::path base_dir(dir);

    // 原名未被占用：原地占位（形态随内容结构），创建成功即持有该名。
    // 占位失败不阻断定名，退化为无占位行为，真实落盘时由引擎错误路径归因。
    if (const auto target = base_dir / name; !entry_exists(target)) {
        std::error_code ec;
        if (!create_entry(target, multi_file, ec) && out_error) *out_error = ec.message();
        return name;
    }

    // 原名冲突：逐序号抢占包层目录（包层形态恒为目录，与内容结构无关）。
    for (int n = 1; n <= 9999; ++n) {
        const std::string candidate = name + "(" + std::to_string(n) + ")";
        const auto target = base_dir / candidate;
        if (entry_exists(target)) continue;
        std::error_code ec;
        if (!create_entry(target, true, ec) && out_error) *out_error = ec.message();
        return candidate;
    }

    // 兜底：毫秒时间戳后缀，几乎不可能再冲突。
    const auto ts = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    const std::string fallback = name + "_" + std::to_string(ts);
    std::error_code ec;
    if (!create_entry(base_dir / fallback, true, ec) && out_error) *out_error = ec.message();
    return fallback;
}

void ensure_placeholder(const std::string &dir,
                        const std::string &name,
                        const bool multi_file) {
    if (name.empty()) return;
    const std::filesystem::path target = std::filesystem::path(dir) / name;
    if (entry_exists(target)) return;
    std::error_code ec;
    create_entry(target, multi_file, ec);
}

bool release_placeholder(const std::string &dir, const std::string &name) {
    if (name.empty()) return false;
    const std::filesystem::path target = std::filesystem::path(dir) / name;
    std::error_code ec;
    // symlink_status 不跟随符号链接：避免顺链删到目标位置的用户数据。
    const auto st = std::filesystem::symlink_status(target, ec);
    if (ec) return false;
    if (std::filesystem::is_directory(st)) {
        // remove 对非空目录失败，天然只回收空占位目录，不触碰已落盘数据。
        return std::filesystem::remove(target, ec) && !ec;
    }
    if (std::filesystem::is_regular_file(st)) {
        const auto size = std::filesystem::file_size(target, ec);
        if (ec || size != 0) return false;
        return std::filesystem::remove(target, ec) && !ec;
    }
    return false; // 符号链接等非常规条目一律不动
}

} // namespace dw::utils
