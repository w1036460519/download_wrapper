/**
 * @file torrent_engine.h
 * @brief BT/Torrent 下载引擎内部实现头文件。
 */

#ifndef DW_TORRENT_ENGINE_H
#define DW_TORRENT_ENGINE_H

#include "download_wrapper/download_wrapper.h"
#include "internal/engine_interface.h"

#include <cstdint>
#include <string>
#include <vector>

namespace dw {

struct EngineProgress;  // 定义见 internal/downloader_internal.h

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
    int32_t init(const dw_config_t* cfg) override;

    /**
     * 销毁引擎，释放所有资源。
     */
    void destroy() override;

    /**
     * 添加单个 BT 下载任务。
     */
    int32_t add_task(const dw_task_params_t* params,
                     dw_submit_result_t*     out_result) override;

    /**
     * 暂停单个 BT 下载任务。
     */
    int32_t pause_task(const char*         task_id,
                       dw_submit_result_t* out_result) override;

    /**
     * 删除单个 BT 下载任务。
     */
    int32_t delete_task(const char*         task_id,
                        dw_submit_result_t* out_result) override;

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
     * 任务元数据就绪后可用。
     */
    int32_t get_file_list(const char*      task_id,
                          dw_file_info_t** out_files,
                          int32_t*         out_count);

    /**
     * 查询单个 BT 任务的进度快照（推送模型，纯读）。
     * 读引擎内快照（由 alert 线程消费 state_update_alert 持续更新）；命中且有效时置
     * out.valid=true 并返回 true，首个状态更新到达前返回 false。
     * @param task_id BT 任务键（即 info_hash 十六进制串）。
     */
    bool query_progress(const char* task_id, EngineProgress& out) override;

    /**
     * 触发一次全量状态更新（post_torrent_updates），结果经 state_update_alert 异步写入快照。
     * 由 A 线程（采集节拍）调用；session 线程安全，无需持 TaskManager 锁。
     */
    void post_updates() override;

    /**
     * 触发一次续传检查点：对会话内有元数据的任务请求 save_resume_data。
     * 由 A 线程低频节流调用；结果经 save_resume_data_alert → post_resume_data 输出。
     */
    void request_resume_checkpoint() override;

    /**
     * 查询单文件已下载字节区间（边下边播）。
     * 由 have 位图裁剪到文件窗口后合并连续段；仅整块 have 的 piece 计入。
     * 元数据未就绪或任务不存在时返回空 vector。
     */
    std::vector<dw_byte_range_t> get_file_ranges(const char* task_id,
                                                 int32_t     file_index) override;

    /**
     * 声明当前播放的文件与字节偏移，对播放点附近 piece 施加 set_piece_deadline 提优。
     * file_index<0 表示停止提优（clear_piece_deadlines）。
     * @return 1=成功，0=失败。
     */
    int set_playing_file(const char* task_id,
                         int32_t     file_index,
                         int64_t     byte_offset);

    /**
     * 批量设置任务内多个文件的下载优先级（任务内文件级优先）。
     * @return 1=成功，0=失败。
     */
    int set_file_priorities(const char*    task_id,
                            const int32_t* file_indexes,
                            const int32_t* priorities,
                            int32_t        count);

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
     * 定名收敛（RESOLVING 校验拍，TaskManager 锁外调用）。
     * 以 file_storage 当前根名经 request_unique_name 上调判重：冲突则逐文件发起
     * rename_file 改首段并登记计数（返回进行中）；无需改名则构建节点树落文件表。
     * @return 0=定名完成（文件表已落库），1=改名进行中（下拍再查），-1=错误/元数据未就绪。
     */
    int32_t finalize_naming(const char* task_id) override;

    /**
     * 周期性维护策略：回收已达做种分享率阈值的任务（remove_torrent 释放上下文）。
     * 由上层调度循环定时调用；无匹配任务时为空操作。
     */
    void sweep() override;

private:
    bool initialized_ = false;
};

} // namespace dw

#endif /* DW_TORRENT_ENGINE_H */