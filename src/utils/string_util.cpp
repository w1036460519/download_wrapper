/**
 * @file string_util.cpp
 * @brief 通用字符串工具实现。
 */
#include "utils/string_util.h"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <sstream>

namespace dw::utils {

    std::string join_lines(const std::vector<std::string>& v) {
        std::string out;
        for (size_t i = 0; i < v.size(); ++i) {
            if (i) out.push_back('\n');
            out += v[i];
        }
        return out;
    }

    std::vector<std::string> split_lines(const std::string& s) {
        std::vector<std::string> out;
        std::string line;
        std::istringstream iss(s);
        while (std::getline(iss, line)) {
            if (!line.empty()) out.push_back(line);
        }
        return out;
    }

    std::string join_ints(const std::vector<int32_t>& v) {
        std::string out;
        for (size_t i = 0; i < v.size(); ++i) {
            if (i) out.push_back(',');
            out += std::to_string(v[i]);
        }
        return out;
    }

    std::vector<int32_t> split_ints(const std::string& s) noexcept {
        std::vector<int32_t> out;
        const char* start = s.data();
        const char* end = start + s.size();
        const char* cur = start;

        while (cur <= end) {
            if (*cur == ',' || cur == end) {
                if (start != cur) {
                    int32_t val{};
                    if (const auto [ptr, ec] = std::from_chars(start, cur, val); ec == std::errc{}) {
                        out.push_back(val);
                    }
                }
                start = cur + 1;
            }
            ++cur;
        }
        return out;
    }

    std::string file_extension(const std::string& name) {
        // path::extension() 含首点（".gz"）；"."/".."/dotfile（".hidden"）/无点时返回空。
        const std::string ext = std::filesystem::path(name).extension().string();
        return ext.size() > 1 ? ext.substr(1) : std::string();
    }

    std::string strip_extension(const std::string &name) {
        // 查找最后一个点：保留 dotfile（首字符为 . 且无其他点）；只剥真正的扩展名。
        if (name.empty()) return name;
        const auto p = std::filesystem::path(name);
        const std::string stem = p.stem().string();
        return stem.empty() ? name : stem;
    }

    bool iequals(std::string_view a, std::string_view b) noexcept {
        // unsigned char 入参避开 std::tolower 对负值 char 的未定义行为。
        return std::ranges::equal(a, b, [](unsigned char ca, unsigned char cb) {
            return std::tolower(ca) == std::tolower(cb);
        });
    }

    char *dup_cstr(const std::string &s) {
        auto *p = static_cast<char *>(std::malloc(s.size() + 1));
        if (p) std::memcpy(p, s.c_str(), s.size() + 1);
        return p;
    }

    char *dup_cstr(const char *s) {
        if (!s || !*s) return nullptr;
        const size_t len = std::strlen(s);
        auto *p = static_cast<char *>(std::malloc(len + 1));
        if (p) std::memcpy(p, s, len + 1);
        return p;
    }

}
