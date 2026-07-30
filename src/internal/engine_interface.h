/**
 * @file engine_interface.h
 * @brief 下载引擎统一接口：TaskManager 经此抽象分发，屏蔽协议实现差异。
 *
 * 设计要点：
 *   - 纯虚方法为各引擎必须实现的任务生命周期与查询能力；
 *   - post_updates 为带默认空实现的节拍钩子：Torrent 引擎覆写（进度快照刷新 +
 *     续传检查点一并触发，去重下沉 libtorrent），HTTP 引擎无需实现；
 *   - 协议专属能力（如 BT 的文件列表 / 优先级 / 磁力解析）留在具体引擎类，
 *     由 download_wrapper.cpp 经具体指针调用，不污染本接口。
 */

#ifndef DW_ENGINE_INTERFACE_H
#define DW_ENGINE_INTERFACE_H

#include "download_wrapper/download_wrapper.h"

#include <cstdint>
#include <string>
#include <vector>

namespace dw {

struct EngineProgress;  // 定义见 internal/downloader_internal.h

/**
 * 下载引擎抽象接口（HttpEngine / TorrentEngine 实现）。
 */
class IDownloadEngine {
public:
    virtual ~IDownloadEngine() = default;

    /// 初始化引擎。@return 0=成功，-1=失败。
    virtual int32_t init(const dw_config_t* cfg) = 0;

    /// 销毁引擎，释放所有资源。
    virtual void destroy() = 0;

    /// 添加单个下载任务。
    virtual int32_t add_task(const dw_task_params_t* params,
                             dw_submit_result_t*     out_result) = 0;

    /// 暂停单个下载任务（id 为引擎键：HTTP=url，BT=info_hash）。
    virtual int32_t pause_task(const char*         id,
                               dw_submit_result_t* out_result) = 0;

    /// 删除单个下载任务（异步语义，两协议统一模型）：仅释放/移除运行时资源
    /// 并置标记，不涉及落盘文件；文件删除由 TaskManager 在 task_released
    /// 确认资源释放后按配置执行。
    /// @return 0=引擎已接管释放；1=引擎未持有该任务（无运行时资源）；-1=错误。
    virtual int32_t delete_task(const char*         id,
                                dw_submit_result_t* out_result) = 0;

    /// 查询任务运行时资源是否已全部释放（线程已 join、文件/存储句柄已关闭）。
    /// 引擎未持有该任务（从未添加 / 已回收）同样视为已释放。
    /// TaskManager 删除流程据此判断何时可安全删除落盘文件（Windows 句柄
    /// 打开期间禁止删除）。
    virtual bool task_released(const char* id) = 0;

    /// 查询单个任务的进度快照（拉模型）。任务存在于引擎时置 out.valid=true 并返回 true。
    virtual bool query_progress(const char* key, EngineProgress& out) = 0;

    /// 查询文件已下载字节区间（边下边播）。HTTP 单文件模型忽略 file_index。
    virtual std::vector<dw_byte_range_t> get_file_ranges(const char* id,
                                                         int32_t     file_index) = 0;

    /// 周期性维护：回收终态任务的运行时上下文。由上层调度循环定时调用。
    virtual void sweep() = 0;

    // ---- 可选钩子（默认空实现，Torrent 覆写） ----

    /// 触发引擎推送一轮更新（BT：post_torrent_updates 刷新进度快照 + 携变更门槛
    /// 请求续传检查点，无变化不产生 resume alert；HTTP 由 worker 自推，无需实现）。
    virtual void post_updates() {}

    /// 应用文件选择意图（RESOLVING 就绪出口调用）：显式定型全量文件优先级并解除待选保护。
    /// file_indexes 为 NULL / count<=0 表示下载全部。HTTP 单文件无选择语义，默认成功。
    /// @return 0=成功，-1=失败（任务不存在 / 元数据未就绪）。
    virtual int32_t apply_file_selection(const char*    /*id*/,
                                         const int32_t* /*file_indexes*/,
                                         int32_t        /*count*/) { return 0; }

    /// 包层迁移能力（BT 覆写）：把 handle 的 save_path 迁至 new_save_path，
    /// 异步收敛（进行中时 EngineProgress.naming_ready 为 false，收敛后恢复）；
    /// 包层不改文件内部相对路径，文件树无需重推。
    /// 判重决策归 TaskManager（重名时目标为 save_path/唯一包层目录）。
    /// @return 0=已一致空操作，1=已发起异步迁移，-1=失败（任务不存在等）。
    virtual int32_t move_storage(const char* /*id*/, const char* /*new_save_path*/) { return -1; }
};

} // namespace dw

#endif /* DW_ENGINE_INTERFACE_H */
