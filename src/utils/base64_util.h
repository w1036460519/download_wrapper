/**
 * @file base64_util.h
 * @brief base64 / base64url 编解码工具。
 *
 * 用于 P2P 信令码压缩（SDP + ICE 候选打包为紧凑可粘贴文本）。
 * base64url 采用 URL 安全字符集（- _ 替代 + /），无填充（省略 =）。
 */

#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace dw::utils {

    /// 标准 base64 编码（含 '+' '/' '=' 填充）
    std::string base64_encode(const uint8_t *data, size_t len);
    std::string base64_encode(std::string_view sv);

    /// 标准 base64 解码；非法字符返回空 vector
    std::vector<uint8_t> base64_decode(std::string_view encoded);

    /// URL 安全 base64 编码（'-' '_' 替换 '+' '/'，省略 '=' 填充）
    std::string base64url_encode(const uint8_t *data, size_t len);
    std::string base64url_encode(std::string_view sv);

    /// URL 安全 base64 解码；非法字符返回空 vector
    std::vector<uint8_t> base64url_decode(std::string_view encoded);

} // namespace dw::utils
