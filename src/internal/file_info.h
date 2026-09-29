/**
 * @file file_info.h
 * @brief 文件信息（C++ 内部版本）。
 */

#pragma once

#include "download_wrapper/download_wrapper.h"

#include <cstdint>
#include <string>

#include <boost/json.hpp>

namespace dw {
    /**
     * 文件信息（C++ 内部版本）：std::string 字段，无手动内存管理。
     * 用于内部传递和 JSON 序列化；FFI 输出仍经 dw_file_info_t 转换。
     */
    struct FileInfo {
        int32_t index = 0;
        std::string name;
        std::string full_path;
        int64_t size = 0;
        std::string ext;
        int32_t status = 0; // 文件状态：0=下载中，1=磁盘已删除，2=完成正常
        int64_t offset = 0;
        int64_t downloaded_bytes = 0;
        bool selected = true; // 是否选中下载（BT 由优先级决定）
        bool is_dir = false; // 是否为目录
        std::string segments; // 已下载区间 JSON（原始格式，供 App 序列化透传）
    };

    /// FileInfo → JSON 对象（供 App 序列化输出）。
    inline boost::json::object to_json(const FileInfo &fi) {
        boost::json::object obj;
        obj["index"] = fi.index;
        obj["name"] = fi.name;
        obj["full_path"] = fi.full_path;
        obj["size"] = fi.size;
        obj["ext"] = fi.ext;
        obj["status"] = fi.status;
        obj["offset"] = fi.offset;
        obj["downloaded_bytes"] = fi.downloaded_bytes;
        obj["selected"] = fi.selected;
        obj["is_dir"] = fi.is_dir;
        // segments 已是 JSON 字符串，解析后嵌入；解析失败则保留空数组
        if (!fi.segments.empty()) {
            try {
                obj["segments"] = boost::json::parse(fi.segments);
            } catch (...) {
                obj["segments"] = boost::json::array{};
            }
        } else {
            obj["segments"] = boost::json::array{};
        }
        return obj;
    }
} // namespace dw
