/**
 * @file webrtc_adapter.h
 * @brief 基于 webrtc-sdk/libwebrtc 的传输适配器实现。
 *
 * 内部持有 PeerConnectionFactory / PeerConnection / DataChannel；
 * 信令码格式为 base64url(JSON{type, sdp, candidates[]})。
 * ICE 采集采用同步等待：gathering 完成后一次性打包输出，
 * 避免调用方处理流式候选。
 *
 * libwebrtc 头文件仅在实现单元包含，上层只依赖 transport_adapter.h。
 * 所有 libwebrtc 类型经 pimpl 隐藏，本头文件无需 libwebrtc 即可编译。
 */

#pragma once

#include "transport_adapter.h"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace dw::p2p {

class WebrtcAdapter final : public TransportAdapter {
public:
    WebrtcAdapter();
    ~WebrtcAdapter() override;

    WebrtcAdapter(const WebrtcAdapter &)            = delete;
    WebrtcAdapter &operator=(const WebrtcAdapter &) = delete;

    void set_state_callback(StateCallback cb) override;
    void set_data_callback(DataCallback cb) override;

    std::string generate_offer() override;
    std::string accept_offer(const std::string &offer_code) override;
    void        accept_answer(const std::string &answer_code) override;

    int32_t        send(const void *data, size_t len) override;
    ConnectionInfo get_info() override;
    void           close() override;

private:
    /// 信令载荷（offer / answer 共用）
    struct SignalPayload {
        std::string              type; // "offer" / "answer"
        std::string              sdp;
        std::vector<std::string> candidates; // 每条: "candidate_str|mid|mline_index"
    };

    /// pimpl：持有所有 libwebrtc 类型实例，避免头文件暴露
    struct Impl;
    std::unique_ptr<Impl> impl_;

    /// 创建 PeerConnection 并绑定观察者
    void create_peer();

    /// 信令编解码（复用 base64url + JSON 格式）
    static std::string encode_signal(const SignalPayload &payload);
    static SignalPayload decode_signal(const std::string &code,
                                       const std::string &expected_type);

    /// 状态跃迁通知（统一入口）
    void emit_state(dw_p2p_state_t state, int32_t error = DW_P2P_OK);

    /// 重置内部状态，为新一轮信令做准备
    void reset_internal();

    // === 异步协调原语（供 Impl 内的 Observer 写入） ===

    /// SDP 创建结果（CreateOffer / CreateAnswer 回调写入）
    std::promise<std::pair<std::string, std::string>> sdp_promise_;

    /// SetLocalDescription / SetRemoteDescription 完成信号
    std::promise<void> sdp_set_promise_;

    /// ICE 采集完成信号
    std::mutex              ice_mtx_;
    std::condition_variable ice_cv_;
    bool                    ice_done_{false};

    /// 收集到的 ICE 候选（gathering 期间累积）
    std::vector<std::string> candidates_;

    // === 回调与状态 ===

    StateCallback state_cb_;
    DataCallback  data_cb_;
    std::mutex    cb_mtx_;

    std::atomic<dw_p2p_state_t> state_{DW_P2P_STATE_NEW};
    std::atomic<dw_p2p_role_t>  role_{DW_P2P_ROLE_INITIATOR};
    std::atomic<int64_t>        bytes_sent_{0};
    std::atomic<int64_t>        bytes_recv_{0};

    std::string remote_peer_;
    std::string local_addr_;
    std::string remote_addr_;
    std::mutex  info_mtx_;

    // Observer 需访问私有成员
    friend class PeerConnObserver;
    friend class DCObserver;
};

} // namespace dw::p2p
