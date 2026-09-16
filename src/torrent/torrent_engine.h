/**
 * @file torrent_engine.h
 * @brief BT/Torrent 下载引擎内部实现头文件。
 */

#pragma once

#include "download_wrapper/download_wrapper.h"
#include "internal/engine_interface.h"
#include "utils/memory_util.h"

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
     * 双行为：handle 存在则直接恢复下载；handle 不存在则经三要素自取 resume_data 重建。
     */
    void resume_task(const std::string &info_hash,
                     const std::string &client_id,
                     const std::vector<int32_t> &priority_file_indexes) override;

    /**
     * 暂停单个 BT 下载任务。
     */
    int32_t pause_task(const std::string &info_hash,
                       const std::string &client_id,
                       dw_submit_result_t* out_result) override;

    /**
     * 删除单个 BT 下载任务（事件驱动模型）：
     *   - handle 有效 → remove_torrent(delete_files)，后续由 torrent_removed_alert /
     *     torrent_deleted_alert 触发 DELETED 事件；
     *   - handle 无效 → 按 delete_files 标识决定是否删文件，直接发 DELETED 事件。
     * @return 0=引擎已接管；-1=错误。
     */
    int32_t delete_task(const std::string &info_hash,
                        const std::string &client_id,
                        int32_t             delete_files,
                        dw_submit_result_t* out_result) override;

    /**
     * 查询任务运行时资源是否已释放：session 完成移除（find_handle 失效）即视为
     * 存储句柄已关闭；引擎未初始化 / 未持有该任务同样视为已释放。
     */
    bool task_released(const std::string &task_id) override;

    /**
     * 解析磁力链接获取 info_hash。
     * 纯解析、不依赖引擎实例状态，故为 static。
     */
    static char* magnet_to_info_hash(const std::string &magnet_link);

    /**
     * 解析 .torrent 文件获取 info_hash。
     * 纯解析、不依赖引擎实例状态，故为 static。
     */
    static char* torrent_file_to_info_hash(const std::string &torrent_file_path);

    /**
     * info_hash 转磁力链接。
     * 仅操作文件级 session 全局、不依赖引擎实例状态，故为 static（下同）。
     */
    static char* info_hash_to_magnet(const std::string &task_id);

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
    std::vector<dw_byte_range_t> get_file_ranges(const std::string &task_id,
                                                 int32_t     file_index) override;

    /**
     * 应用文件选择意图（RESOLVING 就绪出口，TaskManager 经统一接口调用）。
     * 元数据就绪后显式定型全量文件优先级（count<=0 全部默认优先级；count>0 选中默认、
     * 其余置 0），随后解除 default_dont_download 并恢复运行。
     * @return 0=成功，-1=失败（任务不存在 / 元数据未就绪）。
     */
    int32_t apply_file_selection(const std::string &task_id,
                                 const std::vector<int32_t> &file_indexes) override;

    /**
     * 文件路径实时查询：handle 在线时按文件序号解析物理路径与大小。
     * @return false=handle 离线（任务不在 session / 元数据未就绪 / 序号越界）。
     */
    bool get_file_path(const std::string &task_id, int32_t file_index,
                       std::string& out_path, int64_t& out_size) override;

    /**
     * 文件列表实时查询：选中文件的扁平清单（pad 文件过滤，优先级口径与
     * PARSED 上报一致）。连续数组由 alloc_file_list 分配，调用方负责释放。
     */
    utils::file_array get_file_list(const std::string &task_id) override;

    /**
     * 周期性维护策略：回收已达做种分享率阈值的任务（remove_torrent 释放上下文）。
     * 由上层调度循环定时调用；无匹配任务时为空操作。
     */
    void sweep() override;

private:
    bool initialized_ = false;

    /// 设置文件优先级并预建进度行：按 task_id 从 session 获取 handle（不在 session
    /// 时可经 client_id 对应的恢复数据重建），将 priority_file_indexes 应用到
    /// libtorrent，同时预建 file_progress_cache 行。
    static void apply_file_priorities(const std::string &task_id,
                                      const std::string &client_id,
                                      const std::vector<int32_t> &priority_file_indexes);

    // ---- 事件投递工具方法（固定 protocol=BT、client_id 取自 TaskManager）----

    /// 发送可重试失败事件（对应 DW_TASK_STATUS_FAIL）。
    static void post_fail(const std::string &key, const std::string &message);

    /// 发送不可重试错误事件（对应 DW_TASK_STATUS_ERROR）。
    static void post_error(const std::string &key, const std::string &message);
};

} // namespace dw

