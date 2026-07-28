/**
 * @file engine_interface.h
 * @brief 下载引擎统一接口：TaskManager 经此抽象分发，屏蔽协议实现差异。
 *
 * 设计要点：
 *   - 纯虚方法为各引擎必须实现的任务生命周期与查询能力；
 *   - post_updates / request_resume_checkpoint 为带默认空实现的钩子：
 *     Torrent 引擎覆写（alert 驱动的进度推送 / 续传检查点），HTTP 引擎无需实现；
 *   - 协议专属能力（如 BT 的文件列表 / 优先级 / 磁力解析）留在具体引擎类，
 *     由 download_wrapper.cpp 经具体指针调用，不污染本接口。
 */

#ifndef DW_ENGINE_INTERFACE_H
#define DW_ENGINE_INTERFACE_H

#include "download_wrapper/download_wrapper.h"

#include <cstdint>
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

    /// 删除单个下载任务。
    virtual int32_t delete_task(const char*         id,
                                dw_submit_result_t* out_result) = 0;

    /// 查询单个任务的进度快照（拉模型）。任务存在于引擎时置 out.valid=true 并返回 true。
    virtual bool query_progress(const char* key, EngineProgress& out) = 0;

    /// 查询文件已下载字节区间（边下边播）。HTTP 单文件模型忽略 file_index。
    virtual std::vector<dw_byte_range_t> get_file_ranges(const char* id,
                                                         int32_t     file_index) = 0;

    /// 周期性维护：回收终态任务的运行时上下文。由上层调度循环定时调用。
    virtual void sweep() = 0;

    // ---- 可选钩子（默认空实现，Torrent 覆写） ----

    /// 触发引擎推送一轮进度快照（BT 经 post_torrent_updates；HTTP 由 worker 自推，无需实现）。
    virtual void post_updates() {}

    /// 低频触发续传检查点（BT 经 save_resume_data；HTTP 随进度自触发，无需实现）。
    virtual void request_resume_checkpoint() {}

    /// 应用文件选择意图（RESOLVING 就绪出口调用）：显式定型全量文件优先级并解除待选保护。
    /// file_indexes 为 NULL / count<=0 表示下载全部。HTTP 单文件无选择语义，默认成功。
    /// @return 0=成功，-1=失败（任务不存在 / 元数据未就绪）。
    virtual int32_t apply_file_selection(const char*    /*id*/,
                                         const int32_t* /*file_indexes*/,
                                         int32_t        /*count*/) { return 0; }

    /// 定名收敛（RESOLVING 校验拍调用，BT 覆写）：以当前根名经 request_unique_name
    /// 判重，冲突则发起改名，收敛后构树落文件表。HTTP 定名走独立路径，默认视为完成。
    /// @return 0=定名完成（文件表已落库），1=改名进行中（下拍再查），-1=错误/未就绪。
    virtual int32_t finalize_naming(const char* /*id*/) { return 0; }
};

} // namespace dw

#endif /* DW_ENGINE_INTERFACE_H */
