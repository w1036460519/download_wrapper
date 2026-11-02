#pragma once

#include "download_wrapper/download_wrapper.h"
#include <boost/json.hpp>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace dw {
    // 仅内存保存，供 App 查询；不写入数据库。
    struct TaskFile {
        int32_t file_index = -1;
        std::string name;
        std::string path;
        int64_t total_size = 0;
        int64_t total_done = 0;
        // 文件内字节区间 [start, end]，首尾均包含；有序且不重叠。
        std::vector<dw_byte_range_t> segments;
        std::string extension; // 不带点，例如 mp4
        int32_t prioritize = 4; // download_priority_t 的值：0、1、4、7
        std::string info_hash;
    };

    inline boost::json::object to_json(const TaskFile &file) {
        boost::json::array segments;
        for (const auto &segment : file.segments) {
            segments.push_back(boost::json::array{segment.start, segment.end});
        }
        return {
            {"file_index", file.file_index}, {"name", file.name},
            {"path", file.path}, {"total_size", file.total_size},
            {"total_done", file.total_done}, {"segments", std::move(segments)},
            {"extension", file.extension}, {"prioritize", file.prioritize},
            {"info_hash", file.info_hash}
        };
    }

    inline void tag_invoke(boost::json::value_from_tag,
                           boost::json::value &value, const TaskFile &file) {
        value = to_json(file);
    }
} // namespace dw
