/**
 * @file string_util.cpp
 * @brief 通用字符串工具实现。
 */
#include "utils/string_util.h"

#include <cstdlib>
#include <sstream>

namespace dw { namespace utils {

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

std::vector<int32_t> split_ints(const std::string& s) {
    std::vector<int32_t> out;
    std::string tok;
    std::istringstream iss(s);
    while (std::getline(iss, tok, ',')) {
        if (!tok.empty()) out.push_back(std::atoi(tok.c_str()));
    }
    return out;
}

}} // namespace dw::utils
