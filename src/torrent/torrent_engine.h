/**
 * @file torrent_engine.h
 * @brief BT/Torrent 下载引擎内部实现头文件。
 */

#pragma once

#include "download_wrapper/download_wrapper.h"
#include "internal/engine_interface.h"

#include <cstdint>
#include <vector>

namespace dw {

/**
 * BT/Torrent 下载引擎（IDownloadEngine 实现）。
 *
 * 内部基于 libtorrent 实现，负责管理种子、磁力链接、做种、peer 等。
 * 对外通过 dw_* C ABI 接口间接调用；协议专属能力（磁力解析 / 文件列表 /
 * 优先级等）保留在本具体类，由 download_wrapper.cpp 经具体指针调用。
 */
class TorrentEngine final : public IDownloadEngine {
public:
    TorrentEngine();
    ~TorrentEngine() override;

    TorrentEngine(const TorrentEngine&)            = delete;
    TorrentEngine& operator=(const TorrentEngine&) = delete;

    /**
     * 初始化引擎。
     * @return 0=成功，-1=失败。
     */
    int32_t init(const dw_config_t* cfg, TaskManager* task_manager) override;

    /**
     * 销毁引擎，释放所有资源。
     */
    void destroy() override;

    /**
     * 添加单个 BT 下载任务（不含 resume_data，仅创建 handle）。
     */
    int32_t add_task(const dw_task_params_t* params,
                     dw_submit_result_t*     out_result) override;

    /**
     * 恢复单个 BT 下载任务（调度器准入时调用）。
     * 双行为：handle 存在则直接恢复下载；handle 不存在则用 resume_data/参数重建。
     */
    int32_t resume_task(const dw_task_params_t* params,
                        dw_submit_result_t*     out_result) override;

    /**
     * 暂停单个 BT 下载任务。
     */
    int32_t pause_task(const char*         task_id,
                       dw_submit_result_t* out_result) override;

    /**
     * 删除单个 BT 下载任务（事件驱动模型）：
     *   - handle 有效 → remove_torrent(delete_files)，后续由 torrent_removed_alert /
     *     torrent_deleted_alert 触发 DELETED 事件；
     *   - handle 无效 → 按 delete_files 标识决定是否删文件，直接发 DELETED 事件。
     * @return 0=引擎已接管；-1=错误。
     */
    int32_t delete_task(const char*         task_id,
                        int32_t             delete_files,
                        dw_submit_result_t* out_result) override;

    /**
     * 查询任务运行时资源是否已释放：session 完成移除（find_handle 失效）即视为
     * 存储句柄已关闭；引擎未初始化 / 未持有该任务同样视为已释放。
     */
    bool task_released(const char* task_id) override;

    /**
     * 解析磁力链接获取 info_hash。
     * 纯解析、不依赖引擎实例状态，故为 static。
     */
    static char* magnet_to_info_hash(const char* magnet_link);

    /**
     * 解析 .torrent 文件获取 info_hash。
     * 纯解析、不依赖引擎实例状态，故为 static。
     */
    static char* torrent_file_to_info_hash(const char* torrent_file_path);

    /**
     * info_hash 转磁力链接。
     * 仅操作文件级 session 全局、不依赖引擎实例状态，故为 static（下同）。
     */
    static char* info_hash_to_magnet(const char* task_id);

    /**
     * 本地解析 .torrent 文件，返回种子名称、info_hash 和文件列表。
     * 不依赖 session，不创建任务。
     */
    static int32_t parse_torrent_file(const char*      torrent_file_path,
                                      char**           out_name,
                                      char**           out_info_hash,
                                      dw_file_info_t** out_files,
                                      int32_t*         out_count);

    /**
     * 节拍入口（A 线程调用，session 线程安全，无需持 TaskManager 锁）：
     *   1) post_torrent_updates：触发引擎收集变更任务状态，结果经 state_update_alert
     *      异步回 alert 线程，由 handle_alert 投递 STATUS_UPDATE 事件；
     *   2) 续传检查点：对有元数据任务携变更门槛请求 save_resume_data（无变化
     *      不产生 alert），结果经 save_resume_data_alert → post_resume_data 输出。
     */
    void post_updates() override;

    /**
     * 查询单文件已下载字节区间（边下边播）。
     * 由 have 位图裁剪到文件窗口后合并连续段；仅整块 have 的 piece 计入。
     * 元数据未就绪或任务不存在时返回空 vector。
     */
    std::vector<dw_byte_range_t> get_file_ranges(const char* task_id,
                                                 int32_t     file_index) override;

    /**
     * 播放提优：为指定文件设置 piece deadline（readahead 窗口）。
     * 仅 DOWNLOADING 态任务可调用（handle 有效）。
     * @return 0=成功，-1=失败。
     */
    int32_t set_playing_file(const char* task_id,
                             int32_t     file_index,
                             int64_t     byte_offset) override;

    /**
     * 应用文件选择意图（RESOLVING 就绪出口，TaskManager 经统一接口调用）。
     * 元数据就绪后显式定型全量文件优先级（count<=0 全部默认优先级；count>0 选中默认、
     * 其余置 0），随后解除 default_dont_download 并恢复运行。
     * @return 0=成功，-1=失败（任务不存在 / 元数据未就绪）。
     */
    int32_t apply_file_selection(const char*    task_id,
                                 const int32_t* file_indexes,
                                 int32_t        count) override;

    /**
     * 包层迁移能力（TaskManager 重名定名决策后锁外调用）。
     * 把 handle 的 save_path 迁至 new_save_path（零字节落盘时段仅建目录改路径，
     * 无数据搬移）；storage_moved_alert 收敛后推 naming_ready=2 通知调度放行。
     * @return 0=save_path 已一致（空操作），1=已发起异步迁移，
     *         -1=失败（任务不存在等）。
     */
    int32_t move_storage(const char* task_id, const char* new_save_path) override;

    /**
     * 周期性维护策略：回收已达做种分享率阈值的任务（remove_torrent 释放上下文）。
     * 由上层调度循环定时调用；无匹配任务时为空操作。
     */
    void sweep() override;

private:
    bool initialized_ = false;
};

} // namespace dw

