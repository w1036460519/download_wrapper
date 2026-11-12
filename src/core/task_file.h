#pragma once

#include <boost/json.hpp>
#include <string>
#include <utility>
#include <vector>

namespace dw {
    enum class SegmentStatus : int32_t {
        NotDownloaded = 0,
        Downloaded = 1,
        Failed = 2,
    };

    struct FileSegment {
        int64_t start = 0;
        int64_t end = 0; // 含结束字节。
        SegmentStatus status = SegmentStatus::NotDownloaded;
    };

    inline void tag_invoke(boost::json::value_from_tag, boost::json::value &value, const FileSegment &segment) {
        value = boost::json::object{
            {"start", segment.start},
            {"end", segment.end},
            {"status", static_cast<int32_t>(segment.status)},
        };
    }

    // 任务文件信息
    struct TaskFile {
        // 任务内的文件索引。
        int32_t file_index = -1;
        // 文件名
        std::string name;
        // 任务内的相对路径。
        std::string path;
        // 文件完整路径，由 save_path/ path 按平台路径分隔符拼接。
        std::string full_path;
        // 文件总大小，单位：字节（B）
        int64_t total_size = 0;
        // 已下载字节数，范围 [0, total_size]
        int64_t total_done = 0;
        // 文件下载进度，范围 [0.0, 1.0]
        double progress = 0.0;
        // 已下载文件区间
        std::vector<FileSegment> segments;
        // 文件扩展名
        std::string extension;
        // 下载优先级：0=不下载，1=低，4=默认，7=高；沿用 libtorrent download_priority_t
        int32_t prioritize = 4;
        // 所属任务标识：BT 为 info_hash，HTTP 为 URL。
        std::string task_id;
        bool is_delete = false;
        bool is_read = false;

        // ---- 非持久化运行时字段 ----
        bool is_dir = false; // 本地目录扫描结果。
        // 内存中的数据发生变化
        bool dirty = false;
    };

    inline boost::json::object to_json(const TaskFile &file) {
        return {
            {"file_index", file.file_index},
            {"name", file.name},
            {"path", file.path},
            {"full_path", file.full_path},
            {"total_size", file.total_size},
            {"total_done", file.total_done},
            {"progress", file.progress},
            {"segments", boost::json::value_from(file.segments)},
            {"extension", file.extension},
            {"prioritize", file.prioritize},
            {"task_id", file.task_id},
            {"is_delete", file.is_delete},
            {"is_read", file.is_read},
            {"is_dir", file.is_dir}
        };
    }

    inline void tag_invoke(boost::json::value_from_tag,
                           boost::json::value &value, const TaskFile &file) {
        value = to_json(file);
    }
} // namespace dw
