/**
 * @file base64_util.cpp
 * @brief base64 / base64url 编解码实现。
 *
 * 自包含实现，不依赖外部库；解码时对非法字符返回空结果而非抛异常，
 * 便于上层统一处理（信令解码失败统一归因到 DW_P2P_ERR_DECODE）。
 */

#include "base64_util.h"

#include <array>

namespace dw::utils {

    namespace {
        constexpr char kAlphabet[] =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        constexpr char kUrlAlphabet[] =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

        /// 反向查表：字符 → 6bit 值；非法字符为 0xFF
        constexpr std::array<uint8_t, 256> build_decode_table(const char *alphabet) {
            std::array<uint8_t, 256> table{};
            table.fill(0xFF);
            for (int i = 0; i < 64; ++i) {
                table[static_cast<unsigned char>(alphabet[i])] = static_cast<uint8_t>(i);
            }
            return table;
        }

        constexpr auto kStdTable = build_decode_table(kAlphabet);
        constexpr auto kUrlTable = build_decode_table(kUrlAlphabet);

        std::string encode_impl(const uint8_t *data, size_t len, const char *alphabet, bool pad) {
            std::string out;
            out.reserve(((len + 2) / 3) * 4);
            size_t i = 0;
            while (i + 3 <= len) {
                const uint32_t n = (uint32_t(data[i]) << 16)
                                 | (uint32_t(data[i + 1]) << 8)
                                 |  uint32_t(data[i + 2]);
                out.push_back(alphabet[(n >> 18) & 0x3F]);
                out.push_back(alphabet[(n >> 12) & 0x3F]);
                out.push_back(alphabet[(n >>  6) & 0x3F]);
                out.push_back(alphabet[ n        & 0x3F]);
                i += 3;
            }
            if (len - i == 1) {
                const uint32_t n = uint32_t(data[i]) << 16;
                out.push_back(alphabet[(n >> 18) & 0x3F]);
                out.push_back(alphabet[(n >> 12) & 0x3F]);
                if (pad) { out.push_back('='); out.push_back('='); }
            } else if (len - i == 2) {
                const uint32_t n = (uint32_t(data[i]) << 16) | (uint32_t(data[i + 1]) << 8);
                out.push_back(alphabet[(n >> 18) & 0x3F]);
                out.push_back(alphabet[(n >> 12) & 0x3F]);
                out.push_back(alphabet[(n >>  6) & 0x3F]);
                if (pad) out.push_back('=');
            }
            return out;
        }

        std::vector<uint8_t> decode_impl(std::string_view sv, const std::array<uint8_t, 256> &table) {
            std::vector<uint8_t> out;
            out.reserve((sv.size() / 4) * 3);
            uint32_t accum = 0;
            int      bits  = 0;
            for (char c: sv) {
                if (c == '=' || c == '\n' || c == '\r' || c == ' ') continue;
                const uint8_t v = table[static_cast<unsigned char>(c)];
                if (v == 0xFF) return {}; // 非法字符
                accum = (accum << 6) | v;
                bits += 6;
                if (bits >= 8) {
                    bits -= 8;
                    out.push_back(static_cast<uint8_t>((accum >> bits) & 0xFF));
                }
            }
            return out;
        }
    } // anonymous namespace

    std::string base64_encode(const uint8_t *data, size_t len) {
        return encode_impl(data, len, kAlphabet, true);
    }
    std::string base64_encode(std::string_view sv) {
        return base64_encode(reinterpret_cast<const uint8_t *>(sv.data()), sv.size());
    }

    std::vector<uint8_t> base64_decode(std::string_view encoded) {
        return decode_impl(encoded, kStdTable);
    }

    std::string base64url_encode(const uint8_t *data, size_t len) {
        return encode_impl(data, len, kUrlAlphabet, false);
    }
    std::string base64url_encode(std::string_view sv) {
        return base64url_encode(reinterpret_cast<const uint8_t *>(sv.data()), sv.size());
    }

    std::vector<uint8_t> base64url_decode(std::string_view encoded) {
        return decode_impl(encoded, kUrlTable);
    }

} // namespace dw::utils
