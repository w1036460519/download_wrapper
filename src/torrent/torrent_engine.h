/**
 * @file torrent_engine.h
 * @brief BT/Torrent 下载引擎内部实现头文件。
 */

#ifndef DW_TORRENT_ENGINE_H
#define DW_TORRENT_ENGINE_H

#include "download_wrapper/download_wrapper.h"

#include <cstdint>
#include <string>

namespace dw {

/**
 * BT/Torrent 下载引擎。
 *
 * 内部基于 libtorrent 实现，负责管理种子、磁力链接、做种、peer 等。
 * 对外通过 dw_* C ABI 接口间接调用。
 */
class TorrentEngine {
public:
    TorrentEngine();
    ~TorrentEngine();

    TorrentEngine(const TorrentEngine&)            = delete;
    TorrentEngine& operator=(const TorrentEngine&) = delete;

    /**
     * 初始化引擎。
     * @return 0=成功，-1=失败。
     */
    int32_t init(const dw_config_t* cfg);

    /**
     * 销毁引擎，释放所有资源。
     */
    void destroy();

    /**
     * 添加单个 BT 下载任务。
     */
    int32_t add_task(const dw_task_params_t* params,
                     dw_submit_result_t*     out_result);

    /**
     * 暂停单个 BT 下载任务。
     */
    int32_t pause_task(const char*         task_id,
                       dw_submit_result_t* out_result);

    /**
     * 恢复单个 BT 下载任务。
     */
    int32_t resume_task(const dw_task_params_t* params,
                        dw_submit_result_t*     out_result);

    /**
     * 删除单个 BT 下载任务。
     */
    int32_t delete_task(const char*         task_id,
                        dw_submit_result_t* out_result);

    /**
     * 解析磁力链接获取 info_hash。
     */
    char* magnet_to_info_hash(const char* magnet_link);

    /**
     * 解析 .torrent 文件获取 info_hash。
     */
    char* torrent_file_to_info_hash(const char* torrent_file_path);

    /**
     * info_hash 转磁力链接。
     */
    char* info_hash_to_magnet(const char* task_id);

    /**
     * 设置文件下载优先级。
     */
    int set_file_priority(const char* task_id,
                          int32_t     file_index,
                          int32_t     priority);

    /**
     * 本地解析 .torrent 文件，返回 info_hash 和文件列表。
     * 不依赖 session，不创建任务。
     */
    int32_t parse_torrent_file(const char*      torrent_file_path,
                               char**           out_info_hash,
                               dw_file_info_t** out_files,
                               int32_t*         out_count);

    /**
     * 获取 session 中已存在任务的文件列表。
     * 任务元数据就绪后可用（磁力链接 PARSED 后）。
     */
    int32_t get_file_list(const char*      task_id,
                          dw_file_info_t** out_files,
                          int32_t*         out_count);

    /**
     * 周期性维护策略：回收已达做种分享率阈值的任务（remove_torrent 释放上下文）。
     * 由上层调度循环定时调用；无匹配任务时为空操作。
     */
    void sweep();

    /**
     * 流量闸门：整体挂起 / 恢复。
     * paused=true 时遍历全部 handle 逐个暂停；
     * paused=false 时遍历全部 handle 逐个恢复。由上层流量闸门驱动，不改变单个任务状态。
     */
    void set_network_paused(bool paused);

private:
    bool initialized_ = false;
};

} // namespace dw

#endif /* DW_TORRENT_ENGINE_H */