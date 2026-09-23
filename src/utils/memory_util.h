/**
 * @file memory_util.h
 * @brief dw_file_info_t 内存工具：文件信息数组的申请/释放统一入口。
 *
 * 库内所有 dw_file_info_t 相关堆内存操作（数组分配、字符串字段释放）
 * 统一收敛至此；后续新增使用场景直接复用本工具，不得散落 std::malloc / std::free。
 */
#pragma once

#include "download_wrapper/download_wrapper.h"

#include <cstdint>
#include <utility>

namespace dw::utils {
    /// 连续文件数组结果：alloc_file_list 分配的数组指针 + 节点数量。
    using file_array = std::pair<dw_file_info_t*, int32_t>;

    /**
    * 分配数组 dw_file_info_t。与 free_file_list 配对使用。
    * @param count 节点数；<=0 返回 nullptr。
    * @return calloc 分配的数组指针；失败返回 nullptr。
    */
    dw_file_info_t *alloc_file_list(int32_t count);

    /**
    * 释放数组 dw_file_info_t（含 name/full_path/ext 字符串字段）。与 alloc_file_list 配对使用。
    * @param files 数组指针（alloc_file_list 或等价 malloc 分配）；nullptr 或 count<=0 时无操作。
    * @param count 节点数。
    */
    void free_file_list(dw_file_info_t *files, int32_t count);
}
