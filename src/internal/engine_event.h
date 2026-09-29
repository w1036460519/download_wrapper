/**
 * @file engine_event.h
 * @brief 引擎事件类型与载体。
 */

#pragma once

#include "download_wrapper/download_wrapper.h"

#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace dw {
    /**
     * 引擎事件类型（引擎 alert / 状态变化经此转递给 Wrapper B 线程消费）。
     */
    enum class EngineEventType {
        PARSED, // 解析完成（元数据+文件信息，含存储迁移完成）
        DOWNLOAD_FAILED, // 下载失败（通用失败事件）
        STATUS_UPDATE, // 状态+进度更新（含冗余状态字段 status）
        RESUME_DATA, // 断点续传数据就绪（BT resume / HTTP 定期存档）
        DELETED, // 任务已从引擎移除（remove_torrent 收敛 / handle 无效直接删除）
        FILE_PROGRESS, // 文件进度区间就绪（BT 连续 piece 达阈值后合并上报）
        FILE_COMPLETED, // 单个文件下载完成（BT file_completed_alert）
    };

    /// EngineEventType 可读名。
    inline const char *to_string(EngineEventType t) {
        switch (t) {
            case EngineEventType::PARSED: return "PARSED";
            case EngineEventType::DOWNLOAD_FAILED: return "DOWNLOAD_FAILED";
            case EngineEventType::STATUS_UPDATE: return "STATUS_UPDATE";
            case EngineEventType::RESUME_DATA: return "RESUME_DATA";
            case EngineEventType::DELETED: return "DELETED";
            case EngineEventType::FILE_PROGRESS: return "FILE_PROGRESS";
            case EngineEventType::FILE_COMPLETED: return "FILE_COMPLETED";
            default: return "UNKNOWN";
        }
    }

    /**
     * 引擎事件载体（自带值语义，可安全跨线程按值传递）。
     *
     * 字段按事件类型复用：PARSED 事件用 name/save_path/files，STATUS_UPDATE 用进度/速率/总量，
     * 简单事件仅设 type/engine_key/protocol。消费方按 event.type 分支解析。
     */
    struct EngineEvent {
        EngineEventType type; // 事件类型（决定其余字段语义）
        std::string engine_key; // 引擎侧标识（BT=info_hash，HTTP=url）
        dw_protocol_t protocol = DW_PROTOCOL_TORRENT; // 来源协议
        std::string client_id;

        // PARSED 事件字段
        std::string name; // 种子/文件名
        std::string save_path; // 引擎当前 save_path
        std::string original_name; // 重名/包装前的原始目录/文件名
        std::string original_root_name;
        std::string root_name; // 磁盘根目录名（重名判定后的最终名称）
        bool is_dir = true; // 内容是否为目录
        std::string ext; // 文件后缀（仅单文件时有值，不含 '.'）
        std::vector<dw_file_info_t> files; // 节点树（深拷贝，仅 PARSED 使用）

        // DOWNLOAD_FAILED 事件字段
        dw_reason_t reason = DW_REASON_NONE; // 失败原因码
        std::string message; // 错误描述文本

        // STATUS_UPDATE 事件字段
        dw_task_status_t status = DW_TASK_STATUS_DOWNLOADING; // 引擎侧状态冗余
        int64_t total_size = -1; // 总字节数，-1=unknown
        int64_t total_done = -1; // 已完成字节数，-1=unknown
        double progress = -1.0; // 完成比例（0.0~1.0），-1.0=unknown
        double download_rate = 0.0; // 下载速率（B/s）
        double upload_rate = 0.0; // 上传速率（B/s，HTTP 恒为 0）
        int64_t total_upload = 0; // 累计上传字节
        int32_t support_range = 0; // 服务端 Range 支持：0=不支持，1=支持
        std::string etag; // HTTP ETag
        std::string last_modified; // HTTP Last-Modified

        // RESUME_DATA 事件字段
        std::vector<uint8_t> resume_data; // 序列化续传数据

        // DELETED 事件字段
        int32_t delete_files = 0; // 是否删除落盘文件（1=删，0=不删）

        // FILE_PROGRESS 事件字段
        int32_t file_index = -1; // 目标文件索引
        std::string file_path; // 文件相对路径（引擎侧）
        std::string full_path; // 完整路径（save_path + file_path）
        int64_t size = 0; // 文件总大小
        int64_t downloaded_bytes = 0; // 文件已下载字节数
        std::map<int64_t, int64_t> segments; // 有序区间集合（key=offset_start, value=offset_end）
    };
} // namespace dw
