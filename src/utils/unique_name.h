/**
 * @file unique_name.h
 * @brief 唯一文件/目录名解析工具：磁盘存在性 ∪ 占用名集合双重判重。
 */

#ifndef DW_UTILS_UNIQUE_NAME_H
#define DW_UTILS_UNIQUE_NAME_H

#include <string>
#include <unordered_set>

namespace dw::utils {

/**
 * 在目录 dir 下为 name 解析一个不冲突的名称（返回 basename）。
 *
 * 判重条件（任一命中即视为冲突）：
 *   1. dir/name 在磁盘上已存在（文件或目录）；
 *   2. name 存在于 taken_names（通常来自 tasks 表同 save_path 的 name/filename 占用集）。
 *
 * 冲突时按 n=1..9999 依次尝试整名尾部加序号：name(n)（如 movie.mkv(1)）。
 * 唯一名用作包层目录名（save_path/name(n)/原名…），不拆分扩展名，
 * 因此对带点目录名（如 Ubuntu.22.04-LTS）也不会插错序号位置。
 * 全部占用时兜底返回 name_{毫秒时间戳}。
 *
 * @param dir         目标目录（用于磁盘存在性判断）。
 * @param name        期望名称（basename，不含路径分隔符）。
 * @param taken_names 已被占用的名称集合（不含路径）。
 * @return 不冲突的 basename；name 本身不冲突时原样返回。
 */
std::string resolve_unique_name(const std::string& dir,
                                const std::string& name,
                                const std::unordered_set<std::string>& taken_names);

} // namespace dw::utils

#endif /* DW_UTILS_UNIQUE_NAME_H */
