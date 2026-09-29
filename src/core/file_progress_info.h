/**
 * @file file_progress_info.h
 * @brief 文件进度缓存信息：file_progress_cache 表行投影。
 */

#pragma once

#include "download_wrapper/download_wrapper.h"

#include <cstdint>
#include <string>
#include <vector>

#include <boost/json.hpp>

namespace dw {
    /**
     * 文件进度缓存信息。
     *
     * 按任务三要素查询时返回，每个文件一条记录。
     */
    struct FileProgressInfo {
        int32_t file_index = -1; /**< 文件索引。 */
        std::string full_path; /**< 磁盘全路径。 */
        int64_t size = 0; /**< 文件总字节。 */
        int64_t downloaded_bytes = 0; /**< 已下载字节。 */
        std::vector<dw_byte_range_t> segments; /**< 已下载区间（已合并）。 */
    };

    /// FileProgressInfo → JSON 对象
    inline boost::json::object to_json(const FileProgressInfo &r) {
        boost::json::object obj;
        obj["file_index"] = r.file_index;
        obj["full_path"] = r.full_path;
        obj["size"] = r.size;
        obj["downloaded_bytes"] = r.downloaded_bytes;
        boost::json::array segs;
        for (const auto &s: r.segments) {
            boost::json::array pair;
            pair.push_back(s.start);
            pair.push_back(s.end);
            segs.push_back(std::move(pair));
        }
        obj["segments"] = std::move(segs);
        return obj;
    }
} // namespace dw
