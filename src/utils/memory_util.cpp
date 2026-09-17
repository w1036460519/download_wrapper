/**
 * @file memory_util.cpp
 * @brief dw_file_info_t / dw_submit_result_t 内存工具实现。
 */

#include "utils/memory_util.h"

#include "download_wrapper/download_wrapper.h"

#include <cstdlib>

namespace dw::utils {
    dw_file_info_t *alloc_file_list(const int32_t count) {
        if (count <= 0) {
            return nullptr;
        }
        return static_cast<dw_file_info_t *>(
            std::calloc(static_cast<size_t>(count), sizeof(dw_file_info_t)));
    }

    void free_file_list(dw_file_info_t *files, const int32_t count) {
        if (!files || count <= 0) {
            return;
        }
        for (int32_t i = 0; i < count; ++i) {
            std::free(files[i].name);
            std::free(files[i].full_path);
            std::free(files[i].ext);
        }
        std::free(files);
    }

    void free_submit_result_fields(dw_submit_result_t &result) {
        std::free(result.message);
        result.message = nullptr;
        if (result.files) {
            free_file_list(result.files, result.file_count);
            result.files = nullptr;
            result.file_count = 0;
        }
        std::free(result.info_hash);
        result.info_hash = nullptr;
    }
}
