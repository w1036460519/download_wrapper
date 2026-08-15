/**
 * @file engine_interface.h
 * @brief 下载引擎统一接口：TaskManager 经此抽象分发，屏蔽协议实现差异。
 *
 * 设计要点：
 *   - 纯虚方法为各引擎必须实现的任务生命周期能力；
 *   - 进度推送由引擎经 task_manager_->on_engine_event 投递事件（事件驱动），
 *     B 线程消费后写入 TaskRecord 内存，A 线程下一拍直接从 TaskRecord 字段采集；
 *   - post_updates 为带默认空实现的节拍钩子：Torrent 引擎覆写（触发
 *     post_torrent_updates 刷新 + 续传检查点），HTTP 引擎无需实现；
 *   - 协议专属能力（如 BT 的文件列表 / 优先级 / 磁力解析）留在具体引擎类，
 *     由 download_wrapper.cpp 经具体指针调用，不污染本接口。
 */

#pragma once

#include "download_wrapper/download_wrapper.h"

#include <cstdint>
#include <vector>

namespace dw {

class TaskManager; // 前向声明

/**
 * 下载引擎抽象接口（HttpEngine / TorrentEngine 实现）。
 */
class IDownloadEngine {
public:
    virtual ~IDownloadEngine() = default;

    /// 初始化引擎。task_manager 用于事件投递。@return 0=成功，-1=失败。
    virtual int32_t init(const dw_config_t* cfg, TaskManager* task_manager) = 0;

    /// 销毁引擎，释放所有资源。
    virtual void destroy() = 0;

    /// 添加单个下载任务。
    virtual int32_t add_task(const dw_task_params_t* params,
                             dw_submit_result_t*     out_result) = 0;

    /// 恢复单个下载任务（调度器准入时调用）。
    /// 双行为：handle 存在则直接恢复下载；handle 不存在则用 resume_data/参数重建。
    /// 重建失败时任务进入 ERROR 状态。
    virtual int32_t resume_task(const dw_task_params_t* params,
                                dw_submit_result_t*     out_result) = 0;

    /// 暂停单个下载任务（id 为引擎键：HTTP=url，BT=info_hash）。
    virtual int32_t pause_task(const char*         id,
                               dw_submit_result_t* out_result) = 0;

    /// 删除单个下载任务（事件驱动模型）：
    ///   - handle 有效 → remove_torrent(delete_files)，后续由 torrent_removed_alert /
    ///     torrent_deleted_alert 触发 DELETED 事件；
    ///   - handle 无效 → 按 delete_files 标识决定是否删文件，直接发 DELETED 事件。
    /// wrapper 收到 DELETED 事件后回收资源、清理数据。
    /// @return 0=引擎已接管；-1=错误。
    virtual int32_t delete_task(const char*         id,
                                int32_t             delete_files,
                                dw_submit_result_t* out_result) = 0;

    /// 查询任务运行时资源是否已全部释放（线程已 join、文件/存储句柄已关闭）。
    /// 引擎未持有该任务（从未添加 / 已回收）同样视为已释放。
    /// TaskManager 删除流程据此判断何时可安全删除落盘文件（Windows 句柄
    /// 打开期间禁止删除）。
    virtual bool task_released(const char* id) = 0;

    /// 查询文件已下载字节区间（边下边播）。HTTP 单文件模型忽略 file_index。
    virtual std::vector<dw_byte_range_t> get_file_ranges(const char* id,
                                                         int32_t     file_index) = 0;

    /// 周期性维护：回收终态任务的运行时上下文。由上层调度循环定时调用。
    virtual void sweep() = 0;

    // ---- 可选钩子（默认空实现，Torrent 覆写） ----

    /// 节拍入口（A 线程调用）：BT 覆写（触发 post_torrent_updates 刷新 + 续传检查点），
    /// HTTP 引擎由 worker 自推进度，无需实现。
    virtual void post_updates() {}

    /// 应用文件选择意图（RESOLVING 就绪出口调用）：显式定型全量文件优先级并解除待选保护。
    /// file_indexes 为 NULL / count<=0 表示下载全部。HTTP 单文件无选择语义，默认成功。
    /// @return 0=成功，-1=失败（任务不存在 / 元数据未就绪）。
    virtual int32_t apply_file_selection(const char*    /*id*/,
                                         const int32_t* /*file_indexes*/,
                                         int32_t        /*count*/) { return 0; }

    /// 播放提优能力（BT 覆写）：为指定文件设置 piece deadline 以加速播放。
    /// @return 0=成功，-1=失败（任务不存在 / 元数据未就绪）。
    virtual int32_t set_playing_file(const char* /*id*/, int32_t /*file_index*/,
                                     int64_t /*byte_offset*/) { return -1; }

    /// 包层迁移能力（BT 覆写）：把 handle 的 save_path 迁至 new_save_path，
    /// 异步收敛（storage_moved_alert 到达后推 naming_ready=2 通知调度放行）；
    /// 包层不改文件内部相对路径，文件树无需重推。
    /// 判重决策归 TaskManager（重名时目标为 save_path/唯一包层目录）。
    /// @return 0=已一致空操作，1=已发起异步迁移，-1=失败（任务不存在等）。
    virtual int32_t move_storage(const char* /*id*/, const char* /*new_save_path*/) { return -1; }
};

} // namespace dw


