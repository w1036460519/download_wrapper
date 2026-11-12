/**
 * @file unique_name.h
 * @brief 唯一文件/目录名抢占工具：以磁盘为唯一判重真相源，定名即物化占位。
 */

#pragma once

#include <string>

namespace dw::utils {

/**
 * 在目录 dir 下抢占一个不冲突的第一层条目名（返回 basename）。
 *
 * 判重与占位一体：检测到候选名未被占用即立即在磁盘创建该条目（"定名即持有"），
 * 使磁盘成为跨协议、跨进程的唯一判重真相源，不再依赖数据库占用名集合。
 * 本函数不自带锁，检测与创建的整段串行由调用方保证（TaskManager 定名入口持 mtx_，
 * BT 调度线程与 HTTP 引擎线程上调两路均在该锁内）。
 *
 * 占位形态：
 *   - 原名未冲突：由 multi_file 决定——多文件 BT 的 name 是根目录名，须建目录；
 *     单文件 BT 与 HTTP 的 name 是文件名，须建空文件。形态错配会导致引擎后续
 *     落盘失败（同名文件挡住建目录，或同名目录挡住写文件）。
 *   - 原名冲突：依次尝试带序号的包层目录，默认以原名为前缀。
 *
 * 占位创建失败（磁盘满 / 权限等）不阻断定名：经 out_error 回报原因并返回候选名 ，
 * 退化为无占位行为，真实落盘时由引擎自身的错误路径归因。
 *
 * @param dir        目标目录（不存在时自动递归创建）。
 * @param name       期望名称（basename，不含路径分隔符）。
 * @param multi_file 原名对应内容是否为多文件（决定原名占位形态：true=目录）。
 * @param out_error  非 NULL 时写入占位失败原因；成功置空串。
 * @param collision_base 可选的冲突目录名前缀，例如去掉扩展名后的名称。
 * @return 抢占到的 basename；原名未冲突时原样返回。
 */
std::string acquire_unique_name(const std::string& dir,
                                const std::string& name,
                                bool multi_file,
                                std::string* out_error = nullptr,
                                const std::string& collision_base = {});

/**
 * 在目录 dir 下抢占一个 wrapper 文件夹名（恒建目录占位）。
 *
 * 新方案下，每个任务统一在 save_path 下创建一个 wrapper 目录，HTTP/BT 内部
 * 文件落在 wrapper 内部，不再依赖文件名形态判重。
 *
 * 行为：
 *   - 原名未被占用：在 dir/name 建目录（直接是 wrapper 本身）
 *   - 原名被占用：依次尝试 name(1)/name(2)/...，建目录
 *
 * @param dir        目标目录（不存在时自动递归创建）。
 * @param name       期望的 wrapper 名（basename，不含路径分隔符）。
 * @param out_error  非 NULL 时写入占位失败原因；成功置空串。
 * @return 抢占到的 basename；原名未冲突时原样返回。
 */
std::string acquire_wrapper_name(const std::string& dir,
                                 const std::string& name,
                                 std::string* out_error = nullptr);

} // namespace dw::utils
