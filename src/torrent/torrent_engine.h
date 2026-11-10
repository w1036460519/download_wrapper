// BT 下载引擎的内部接口。

#pragma once

#include "download_wrapper/download_wrapper.h"
#include "internal/downloader_internal.h"
#include "internal/engine_interface.h"

#include <cstdint>
#include <vector>
#include <mutex>
#include "core/task.h"

namespace dw {

/// 基于 libtorrent 的 BT 下载引擎。
class TorrentEngine final : public IDownloadEngine {
public:
    /// 关联任务管理器及其存储。
    /// @param task_manager 任务管理器，须在引擎生命周期内有效。
    explicit TorrentEngine(TaskManager *task_manager);

    /// 释放已初始化的引擎资源。
    ~TorrentEngine() override;

    TorrentEngine(const TorrentEngine&)            = delete;
    TorrentEngine& operator=(const TorrentEngine&) = delete;

    /// 持有 tasks_mutex() 时查询内存任务并使用返回指针。
    /// @param info_hash BT 任务哈希。
    /// @return 匹配的 BT 任务，未找到时返回 nullptr。
    static Task *get_task(const std::string &info_hash, bool include_deleted = false);

    /// 返回已关联任务管理器的递归互斥锁。
    static std::recursive_mutex &tasks_mutex();

    /// 创建 session 并启动后台线程。
    /// @return 成功返回 0，失败返回 -1。
    int32_t init() override;

    /// 停止后台线程并释放 session。
    void destroy() override;

    /// 注册并持久化 BT 任务，创建任务句柄。
    /// @param params 下载来源、保存路径和文件选择等参数。
    dw_submit_result_t add_task(const TaskParams *params) override;

    /// 更新内存任务并入队，等待调度器恢复下载。
    /// @param params 任务哈希、强制调度标记和文件优先级映射。
    /// @return 入队结果。
    dw_submit_result_t resume_task(const TaskParams *params) override;

    /// 请求暂停任务并刷新状态。
    /// @param params BT 任务哈希。
    dw_submit_result_t pause_task(const TaskParams *params) override;

    /// 请求从 session 移除任务并软删除数据库记录。
    /// @param params 任务哈希及是否删除本地文件。
    dw_submit_result_t delete_task(const TaskParams *params) override;

    /// 提取磁力链接中的十六进制哈希，优先返回 v2。
    /// @param magnet_link 待解析的磁力链接。
    static dw_submit_result_t parse_magnet(const std::string &magnet_link);

    /// 解析种子哈希和文件列表，跳过填充文件。
    /// @param torrent_file_path .torrent 文件路径。
    static dw_submit_result_t parse_torrent_file(const std::string &torrent_file_path);

    /// 移除已标记删除或做种分享率达标的任务句柄。
    void sweep() override;

private:
    /// 调度准入后获取或重建句柄，提交恢复操作。
    /// @param params 任务哈希和要修改的文件优先级。
    dw_submit_result_t do_resume(const TaskParams *params) override;

    bool initialized_ = false;
};

} // namespace dw

