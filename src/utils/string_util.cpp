/**
 * @file string_util.cpp
 * @brief 通用字符串工具实现。
 */
#include "utils/string_util.h"

#include <cstdlib>
#include <cstring>
#include <filesystem>

namespace dw::utils {

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
