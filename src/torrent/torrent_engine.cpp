/**
 * @file torrent_engine.cpp
 * @brief BT/Torrent 下载引擎实现（占位）。
 */

#include "torrent/torrent_engine.h"

#include "internal/downloader_internal.h"

#include <libtorrent/torrent_info.hpp>
#include <libtorrent/file_storage.hpp>
#include <libtorrent/error_code.hpp>

#include <cstdlib>
#include <cstring>
#include <sstream>
#include <string>

namespace dw {

TorrentEngine::TorrentEngine() = default;
TorrentEngine::~TorrentEngine() {
    if (initialized_) {
        destroy();
    }
}

int32_t TorrentEngine::init(const dw_config_t* /*cfg*/) {
    // TODO: 初始化 libtorrent session、DHT、监听端口等
    initialized_ = true;
    DW_LOG(DW_LOG_INFO, "BT 引擎初始化完成", "");
    return 0;
}

void TorrentEngine::destroy() {
    // TODO: 释放 libtorrent session 资源
    initialized_ = false;
    DW_LOG(DW_LOG_INFO, "BT 引擎已销毁", "");
}

int32_t TorrentEngine::add_task(const dw_task_params_t* params,
                                 dw_submit_result_t*     out_result) {
    // TODO: 实现 BT 任务添加（种子 / 磁力链接）
    out_result->task_id = params->task_id;
    out_result->code    = DW_REASON_NONE;
    out_result->message = nullptr;
    return 0;
}

int32_t TorrentEngine::pause_task(const char*         id,
                                   dw_submit_result_t* out_result) {
    // TODO: 实现 BT 任务暂停
    out_result->task_id = id;
    out_result->code    = DW_REASON_NONE;
    out_result->message = nullptr;
    return 0;
}

int32_t TorrentEngine::resume_task(const dw_task_params_t* params,
                                    dw_submit_result_t*     out_result) {
    // TODO: 实现 BT 任务恢复
    out_result->task_id = params->task_id;
    out_result->code    = DW_REASON_NONE;
    out_result->message = nullptr;
    return 0;
}

int32_t TorrentEngine::delete_task(const char*         id,
                                    dw_submit_result_t* out_result) {
    // TODO: 实现 BT 任务删除
    out_result->task_id = id;
    out_result->code    = DW_REASON_NONE;
    out_result->message = nullptr;
    return 0;
}

char* TorrentEngine::magnet_to_info_hash(const char* /*magnet_link*/) {
    // TODO: 实现磁力链接解析
    return nullptr;
}

char* TorrentEngine::torrent_file_to_info_hash(const char* /*torrent_file_path*/) {
    // TODO: 实现 .torrent 文件解析
    return nullptr;
}

char* TorrentEngine::info_hash_to_magnet(const char* /*info_hash*/) {
    // TODO: 实现 info_hash 转磁力链接
    return nullptr;
}

int TorrentEngine::set_file_priority(const char* /*info_hash*/,
                                     int32_t     /*file_index*/,
                                     int32_t     /*priority*/) {
    // TODO: 实现文件优先级设置
    return 0;
}

int32_t TorrentEngine::parse_torrent_file(const char*      torrent_file_path,
                                           char**           out_info_hash,
                                           dw_file_info_t** out_files,
                                           int32_t*         out_count) {
    if (!torrent_file_path || !out_info_hash || !out_files || !out_count) {
        return -1;
    }

    *out_info_hash = nullptr;
    *out_files      = nullptr;
    *out_count      = 0;

    lt::error_code ec;
    auto ti = std::make_shared<lt::torrent_info>(torrent_file_path, ec);
    if (ec) {
        DW_LOG(DW_LOG_ERROR, "解析 .torrent 文件失败", "");
        return -1;
    }

    // 提取 info_hash hex 字符串
    std::string info_hash_str;
    const auto& hashes = ti->info_hashes();
    if (hashes.has_v2()) {
        std::stringstream ss;
        ss << hashes.v2;
        info_hash_str = ss.str();
    } else if (hashes.has_v1()) {
        std::stringstream ss;
        ss << hashes.v1;
        info_hash_str = ss.str();
    } else {
        DW_LOG(DW_LOG_ERROR, ".torrent 文件无有效 info_hash", "");
        return -1;
    }

    *out_info_hash = static_cast<char*>(std::malloc(info_hash_str.size() + 1));
    if (!*out_info_hash) {
        return -1;
    }
    std::strcpy(*out_info_hash, info_hash_str.c_str());

    // 提取文件列表
    const lt::file_storage& fs = ti->files();
    const int file_count = fs.num_files();

    *out_files = static_cast<dw_file_info_t*>(
        std::calloc(static_cast<size_t>(file_count), sizeof(dw_file_info_t)));
    if (!*out_files) {
        std::free(*out_info_hash);
        *out_info_hash = nullptr;
        return -1;
    }

    for (int i = 0; i < file_count; ++i) {
        const lt::file_index_t idx{i};
        (*out_files)[i].index = i;

        const std::string path = fs.file_path(idx);
        (*out_files)[i].name = static_cast<char*>(std::malloc(path.size() + 1));
        if ((*out_files)[i].name) {
            std::strcpy((*out_files)[i].name, path.c_str());
        }

        (*out_files)[i].size = fs.file_size(idx);
    }

    *out_count = file_count;
    return 0;
}

int32_t TorrentEngine::get_file_list(const char*      /*task_id*/,
                                      dw_file_info_t** out_files,
                                      int32_t*         out_count) {
    // TODO: 当 session 管理实现后，从 session 中查找任务并返回文件列表
    *out_files = nullptr;
    *out_count = 0;
    return -1;
}

} // namespace dw
