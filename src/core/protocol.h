/**
 * @file protocol.h
 * @brief 协议枚举辅助函数。
 */

#pragma once

#include "download_wrapper/download_wrapper.h"

namespace dw {
    /// dw_protocol_t 可读名（供 open_id / 日志使用）。
    inline const char *to_string(dw_protocol_t p) {
        switch (p) {
            case DW_PROTOCOL_HTTP: return "HTTP";
            case DW_PROTOCOL_TORRENT: return "BT";
            case DW_PROTOCOL_LOCAL: return "LOCAL";
            default: return "UNKNOWN";
        }
    }
} // namespace dw
