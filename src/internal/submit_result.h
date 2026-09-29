/**
 * @file submit_result.h
 * @brief 操作结果（内部版本）+ JSON 序列化 + ABI 包装。
 */

#pragma once

#include "download_wrapper/download_wrapper.h"
#include "internal/file_info.h"
#include "utils/string_util.h"

#include <cstdint>
#include <string>
#include <vector>

#include <boost/json.hpp>

namespace dw {
    /**
     * 操作结果（内部版本）：全 std 类型，按值返回无内存管理负担。
     */
    struct dw_submit_result_t {
        dw_reason_t code = DW_REASON_NONE;
        std::string message;
        std::string info_hash;
        std::vector<FileInfo> files;
        int32_t affected_count = 0; // 本次操作影响的记录数量（新增/失效/删除等）

        dw_submit_result_t() = default;

        /// 构造成功结果
        static dw_submit_result_t success() { return {}; }

        /// 构造失败结果
        static dw_submit_result_t failure(dw_reason_t reason, std::string msg = {}) {
            dw_submit_result_t r;
            r.code = reason;
            r.message = std::move(msg);
            return r;
        }
    };

    // ---- C++ 序列化（内部消费） ----

    inline boost::json::object to_json(const dw_submit_result_t &result) {
        boost::json::object resp;
        resp["code"] = static_cast<int>(result.code);
        if (result.code == DW_REASON_NONE) {
            boost::json::object data;
            if (!result.info_hash.empty()) {
                data["info_hash"] = result.info_hash;
            }
            if (!result.files.empty()) {
                boost::json::array files_arr;
                for (const auto &fi: result.files) {
                    boost::json::object f;
                    f["index"] = fi.index;
                    f["name"] = fi.name;
                    f["full_path"] = fi.full_path;
                    f["size"] = fi.size;
                    f["ext"] = fi.ext;
                    f["status"] = fi.status;
                    f["offset"] = fi.offset;
                    f["downloaded_bytes"] = fi.downloaded_bytes;
                    files_arr.push_back(std::move(f));
                }
                data["files"] = std::move(files_arr);
            }
            if (!data.empty()) resp["data"] = std::move(data);
        } else {
            resp["message"] = result.message.empty() ? "操作失败" : result.message;
        }
        return resp;
    }

    // ---- ABI 包装（FFI 层消费） ----

    /// dw_submit_result_t → JSON char*（移动语义接管 files 所有权）
    inline char *result_to_json(dw_submit_result_t &&result) {
        auto resp = to_json(result);
        return utils::dup_cstr(boost::json::serialize(resp));
    }
} // namespace dw
