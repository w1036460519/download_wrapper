/**
 * @file transport_adapter.h
 * @brief P2P 传输适配器抽象：屏蔽底层打洞库差异。
 *
 * 设计要点：
 *   - 纯虚接口：所有具体打洞库（libwebrtc / 后续可替换实现）均实现本接口；
 *   - 业务层（p2p_bridge / 上层 C API）仅依赖本抽象，不直接引用具体库头文件；
 *   - 替换打洞库时只需新增一个 TransportAdapter 实现并切换工厂，业务层零改动；
 *   - 回调经 std::function 注入，避免接口层引入具体库的异步模型（如 asio io_context）。
 *
 * 信令码格式约定：
 *   由具体适配器自行定义编解码（如 base64(JSON)），业务层仅以 std::string 透传。
 *   不同适配器的信令码互不兼容，跨实现连接需双方使用同一适配器。
 */

#pragma once

#include "download_wrapper/download_wrapper.h"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace dw::p2p {

/// 连接信息（与 dw_p2p_info_t 字段一一对应，业务层在 C API 桥内做类型转换）
struct ConnectionInfo {
    dw_p2p_state_t state{DW_P2P_STATE_NEW};
    dw_p2p_role_t  role{DW_P2P_ROLE_INITIATOR};
    int64_t        bytes_sent{0};
    int64_t        bytes_recv{0};
    std::string    remote_peer;
    std::string    local_addr;
    std::string    remote_addr;
};

/// 状态回调：新状态 + 失败时的错误码（成功跃迁为 DW_P2P_OK）
using StateCallback = std::function<void(dw_p2p_state_t state, int32_t error)>;

/// 数据回调：接收到的二进制数据（指针仅在回调周期内有效）
using DataCallback = std::function<void(const uint8_t *data, size_t len)>;

/**
 * 传输适配器抽象接口。
 *
 * 每个实例对应一条独立的 P2P 连接；多连接并发互不干扰。
 * 生命周期由调用方经 std::unique_ptr 管理。
 */
class TransportAdapter {
public:
    virtual ~TransportAdapter() = default;

    /// 注册状态变更回调（覆盖前一个）
    virtual void set_state_callback(StateCallback cb) = 0;

    /// 注册数据接收回调（覆盖前一个）
    virtual void set_data_callback(DataCallback cb) = 0;

    /**
     * 生成 offer 信令码（发起方）。
     *
     * 启动本地 ICE 采集，完成后返回编码后的信令文本。
     * 异步实现应在返回前阻塞等待 ICE 采集完成；
     * 同步实现可直接返回。
     *
     * @return 信令码文本；失败抛 std::runtime_error。
     */
    virtual std::string generate_offer() = 0;

    /**
     * 接收 offer 并生成 answer 信令码（应答方）。
     *
     * @param offer_code  对端 offer 信令码。
     * @return answer 信令码；解码失败或底层异常抛 std::runtime_error。
     */
    virtual std::string accept_offer(const std::string &offer_code) = 0;

    /**
     * 接收 answer 并完成连接（发起方）。
     *
     * 调用后状态回调会异步通知 CONNECTED 或 FAILED。
     *
     * @param answer_code  对端 answer 信令码。
     */
    virtual void accept_answer(const std::string &answer_code) = 0;

    /**
     * 发送二进制数据。仅在 CONNECTED 状态可用。
     *
     * @return 0=已入队；非 0=失败（具体值由适配器定义，C API 桥统一映射到 dw_p2p_error_t）。
     */
    virtual int32_t send(const void *data, size_t len) = 0;

    /// 查询当前连接信息
    virtual ConnectionInfo get_info() = 0;

    /**
     * 主动关闭连接。
     *
     * 句柄仍可复用（可重新走信令流程）；彻底释放请析构实例。
     */
    virtual void close() = 0;
};

/**
 * 传输适配器工厂：根据配置创建具体实现。
 *
 * 当前默认实现为 libwebrtc（webrtc-sdk/libwebrtc）；后续替换 / 新增打洞库时，
 * 只需在此处切换返回类型，业务层零改动。
 */
class TransportFactory {
public:
    /// 创建默认实现（当前 = libwebrtc）
    static std::unique_ptr<TransportAdapter> create_default();

    /// 查询当前默认实现名称（用于日志 / 诊断）
    static const char *default_implementation_name();
};

} // namespace dw::p2p
