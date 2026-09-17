/**
 * @file webrtc_adapter.cpp
 * @brief webrtc-sdk/libwebrtc 传输适配器实现。
 *
 * 信令码格式：base64url( JSON{ type:"offer"|"answer", sdp:string,
 *                              candidates:[ "cand|mid|mlineidx", ... ] } )。
 * ICE 采集采用同步等待：gathering 完成后一次性打包输出，
 * 避免调用方处理流式候选。
 *
 * libwebrtc 头文件仅在本编译单元包含，上层只依赖 transport_adapter.h。
 */

#include "webrtc_adapter.h"

// ── libwebrtc 头文件（仅此编译单元引用） ──
#include <libwebrtc.h>
#include <rtc_data_channel.h>
#include <rtc_ice_candidate.h>
#include <rtc_mediaconstraints.h>
#include <rtc_peerconnection.h>
#include <rtc_peerconnection_factory.h>
#include <rtc_session_description.h>

#include "../utils/base64_util.h"
#include "internal/downloader_internal.h"

#include <boost/json.hpp>

#include <chrono>
#include <condition_variable>
#include <future>
#include <mutex>
#include <stdexcept>
#include <string>
#include <vector>

namespace dw::p2p {

using namespace std::chrono_literals;
using libwebrtc::scoped_refptr;

/* ================================================================== */
/*                    全局初始化（进程级单例）                          */
/* ================================================================== */

static std::once_flag                               g_init_flag;
static scoped_refptr<libwebrtc::RTCPeerConnectionFactory> g_factory;

/// 首次调用时初始化 libwebrtc 并创建 PeerConnectionFactory
static void ensure_global_init() {
    std::call_once(g_init_flag, [] {
        if (!libwebrtc::LibWebRTC::Initialize()) {
            throw std::runtime_error("libwebrtc 初始化失败");
        }
        g_factory = libwebrtc::LibWebRTC::CreateRTCPeerConnectionFactory();
        if (!g_factory) {
            throw std::runtime_error("PeerConnectionFactory 创建失败");
        }
        // 进程退出时清理 libwebrtc 线程与 SSL
        std::atexit([] {
            g_factory = nullptr;
            libwebrtc::LibWebRTC::Terminate();
        });
        dw::log_i("p2p", "libwebrtc 全局初始化完成");
    });
}

/* ================================================================== */
/*                    Observer 实现（仅本编译单元可见）                  */
/* ================================================================== */

/// PeerConnection 事件观察者
class PeerConnObserver final : public libwebrtc::RTCPeerConnectionObserver {
public:
    explicit PeerConnObserver(WebrtcAdapter *adapter) : adapter_(adapter) {}

    void OnSignalingState(libwebrtc::RTCSignalingState) override {}

    void OnPeerConnectionState(libwebrtc::RTCPeerConnectionState s) override;

    void OnIceGatheringState(libwebrtc::RTCIceGatheringState s) override;

    void OnIceConnectionState(libwebrtc::RTCIceConnectionState) override {}

    void OnIceCandidate(scoped_refptr<libwebrtc::RTCIceCandidate> c) override;

    void OnAddStream(scoped_refptr<libwebrtc::RTCMediaStream>) override {}
    void OnRemoveStream(scoped_refptr<libwebrtc::RTCMediaStream>) override {}

    void OnDataChannel(scoped_refptr<libwebrtc::RTCDataChannel> dc) override;

    void OnRenegotiationNeeded() override {}
    void OnTrack(
        scoped_refptr<libwebrtc::RTCRtpTransceiver>) override {}
    void OnAddTrack(
        libwebrtc::vector<scoped_refptr<libwebrtc::RTCMediaStream>>,
        scoped_refptr<libwebrtc::RTCRtpReceiver>) override {}
    void OnRemoveTrack(scoped_refptr<libwebrtc::RTCRtpReceiver>) override {}

private:
    WebrtcAdapter *adapter_;
};

/// DataChannel 事件观察者
class DCObserver final : public libwebrtc::RTCDataChannelObserver {
public:
    explicit DCObserver(WebrtcAdapter *adapter) : adapter_(adapter) {}

    void OnStateChange(libwebrtc::RTCDataChannelState state) override;
    void OnMessage(const char *buffer, int length, bool binary) override;

private:
    WebrtcAdapter *adapter_;
};

/* ================================================================== */
/*                    Impl 定义（pimpl 实体）                          */
/* ================================================================== */

struct WebrtcAdapter::Impl {
    scoped_refptr<libwebrtc::RTCPeerConnectionFactory> factory;
    scoped_refptr<libwebrtc::RTCPeerConnection>        pc;
    scoped_refptr<libwebrtc::RTCDataChannel>           dc;
    std::unique_ptr<PeerConnObserver>                  pc_observer;
    std::unique_ptr<DCObserver>                        dc_observer;
};

/* ================================================================== */
/*                    构造 / 析构                                      */
/* ================================================================== */

WebrtcAdapter::WebrtcAdapter() : impl_(std::make_unique<Impl>()) {
    ensure_global_init();
    impl_->factory = g_factory;
}

WebrtcAdapter::~WebrtcAdapter() {
    try { close(); } catch (...) {}
}

void WebrtcAdapter::set_state_callback(StateCallback cb) {
    std::lock_guard<std::mutex> lk(cb_mtx_);
    state_cb_ = std::move(cb);
}

void WebrtcAdapter::set_data_callback(DataCallback cb) {
    std::lock_guard<std::mutex> lk(cb_mtx_);
    data_cb_ = std::move(cb);
}

/* ================================================================== */
/*                    信令流程                                         */
/* ================================================================== */

std::string WebrtcAdapter::generate_offer() {
    reset_internal();
    role_.store(DW_P2P_ROLE_INITIATOR);
    create_peer();

    // 重置异步协调原语
    sdp_promise_    = decltype(sdp_promise_)();
    sdp_set_promise_ = decltype(sdp_set_promise_)();
    {
        std::lock_guard<std::mutex> lk(ice_mtx_);
        ice_done_   = false;
        candidates_.clear();
    }

    // 创建 Offer（异步回调 → promise）
    auto constraints = libwebrtc::RTCMediaConstraints::Create();
    constraints->AddMandatoryConstraint("OfferToReceiveAudio", "false");
    constraints->AddMandatoryConstraint("OfferToReceiveVideo", "false");

    impl_->pc->CreateOffer(
        [this](const libwebrtc::string sdp, const libwebrtc::string type) {
            sdp_promise_.set_value({sdp.std_string(), type.std_string()});
        },
        [](const char *err) {
            dw::log_e("p2p", "CreateOffer 失败: %s", err);
        },
        constraints);

    // 等待 SDP 创建完成
    auto sdp_future = sdp_promise_.get_future();
    if (sdp_future.wait_for(10s) != std::future_status::ready) {
        emit_state(DW_P2P_STATE_FAILED, DW_P2P_ERR_TRANSPORT);
        throw std::runtime_error("CreateOffer 超时");
    }
    auto [local_sdp, sdp_type] = sdp_future.get();

    // 设置本地描述
    sdp_set_promise_ = decltype(sdp_set_promise_)();
    impl_->pc->SetLocalDescription(
        local_sdp.c_str(), sdp_type.c_str(),
        [this]() { sdp_set_promise_.set_value(); },
        [this](const char *err) {
            sdp_set_promise_.set_exception(
                std::make_exception_ptr(std::runtime_error(err)));
        });
    sdp_set_promise_.get_future().get(); // 等待完成或异常

    // 等待 ICE 采集完成
    {
        std::unique_lock<std::mutex> lk(ice_mtx_);
        if (!ice_cv_.wait_for(lk, 15s, [this] { return ice_done_; })) {
            emit_state(DW_P2P_STATE_FAILED, DW_P2P_ERR_TRANSPORT);
            throw std::runtime_error("ICE gathering 超时");
        }
    }

    // 打包信令码
    SignalPayload payload;
    payload.type       = "offer";
    payload.sdp        = std::move(local_sdp);
    payload.candidates = candidates_;
    return encode_signal(payload);
}

std::string WebrtcAdapter::accept_offer(const std::string &offer_code) {
    reset_internal();
    role_.store(DW_P2P_ROLE_RESPONDER);

    auto payload = decode_signal(offer_code, "offer");
    create_peer();

    // 重置异步协调原语
    sdp_promise_     = decltype(sdp_promise_)();
    sdp_set_promise_ = decltype(sdp_set_promise_)();
    {
        std::lock_guard<std::mutex> lk(ice_mtx_);
        ice_done_   = false;
        candidates_.clear();
    }

    // 设置远端描述（offer）
    impl_->pc->SetRemoteDescription(
        payload.sdp.c_str(), "offer",
        [this]() { sdp_set_promise_.set_value(); },
        [this](const char *err) {
            sdp_set_promise_.set_exception(
                std::make_exception_ptr(std::runtime_error(err)));
        });
    sdp_set_promise_.get_future().get();

    // 添加远端 ICE 候选
    for (const auto &entry : payload.candidates) {
        // 格式: "candidate_str|mid|mline_index"
        auto p1 = entry.find('|');
        if (p1 == std::string::npos) continue;
        auto p2 = entry.find('|', p1 + 1);
        if (p2 == std::string::npos) continue;
        std::string cand_str = entry.substr(0, p1);
        std::string mid      = entry.substr(p1 + 1, p2 - p1 - 1);
        int         mline    = std::stoi(entry.substr(p2 + 1));
        impl_->pc->AddCandidate(mid.c_str(), mline, cand_str.c_str());
    }

    // 创建 Answer
    auto constraints = libwebrtc::RTCMediaConstraints::Create();
    constraints->AddMandatoryConstraint("OfferToReceiveAudio", "false");
    constraints->AddMandatoryConstraint("OfferToReceiveVideo", "false");

    sdp_promise_ = decltype(sdp_promise_)();
    impl_->pc->CreateAnswer(
        [this](const libwebrtc::string sdp, const libwebrtc::string type) {
            sdp_promise_.set_value({sdp.std_string(), type.std_string()});
        },
        [](const char *err) {
            dw::log_e("p2p", "CreateAnswer 失败: %s", err);
        },
        constraints);

    auto sdp_future = sdp_promise_.get_future();
    if (sdp_future.wait_for(10s) != std::future_status::ready) {
        emit_state(DW_P2P_STATE_FAILED, DW_P2P_ERR_TRANSPORT);
        throw std::runtime_error("CreateAnswer 超时");
    }
    auto [local_sdp, sdp_type] = sdp_future.get();

    // 设置本地描述（answer）
    sdp_set_promise_ = decltype(sdp_set_promise_)();
    impl_->pc->SetLocalDescription(
        local_sdp.c_str(), sdp_type.c_str(),
        [this]() { sdp_set_promise_.set_value(); },
        [this](const char *err) {
            sdp_set_promise_.set_exception(
                std::make_exception_ptr(std::runtime_error(err)));
        });
    sdp_set_promise_.get_future().get();

    // 等待 ICE 采集完成
    {
        std::unique_lock<std::mutex> lk(ice_mtx_);
        if (!ice_cv_.wait_for(lk, 15s, [this] { return ice_done_; })) {
            emit_state(DW_P2P_STATE_FAILED, DW_P2P_ERR_TRANSPORT);
            throw std::runtime_error("ICE gathering 超时");
        }
    }

    SignalPayload answer;
    answer.type       = "answer";
    answer.sdp        = std::move(local_sdp);
    answer.candidates = candidates_;
    return encode_signal(answer);
}

void WebrtcAdapter::accept_answer(const std::string &answer_code) {
    auto payload = decode_signal(answer_code, "answer");
    if (!impl_->pc) {
        throw std::runtime_error("无 PeerConnection（未调用 generate_offer）");
    }

    sdp_set_promise_ = decltype(sdp_set_promise_)();
    impl_->pc->SetRemoteDescription(
        payload.sdp.c_str(), "answer",
        [this]() { sdp_set_promise_.set_value(); },
        [this](const char *err) {
            sdp_set_promise_.set_exception(
                std::make_exception_ptr(std::runtime_error(err)));
        });
    sdp_set_promise_.get_future().get();

    // 添加远端 ICE 候选
    for (const auto &entry : payload.candidates) {
        auto p1 = entry.find('|');
        if (p1 == std::string::npos) continue;
        auto p2 = entry.find('|', p1 + 1);
        if (p2 == std::string::npos) continue;
        std::string cand_str = entry.substr(0, p1);
        std::string mid      = entry.substr(p1 + 1, p2 - p1 - 1);
        int         mline    = std::stoi(entry.substr(p2 + 1));
        impl_->pc->AddCandidate(mid.c_str(), mline, cand_str.c_str());
    }

    emit_state(DW_P2P_STATE_CONNECTING);
}

/* ================================================================== */
/*                    数据发送 / 信息查询 / 关闭                        */
/* ================================================================== */

int32_t WebrtcAdapter::send(const void *data, size_t len) {
    if (state_.load() != DW_P2P_STATE_CONNECTED || !impl_->dc) {
        return DW_P2P_ERR_NOT_READY;
    }
    try {
        impl_->dc->Send(static_cast<const uint8_t *>(data),
                        static_cast<uint32_t>(len), true);
        bytes_sent_.fetch_add(static_cast<int64_t>(len));
        return DW_P2P_OK;
    } catch (const std::exception &e) {
        dw::log_e("p2p", "发送失败: %s", e.what());
        return DW_P2P_ERR_TRANSPORT;
    }
}

ConnectionInfo WebrtcAdapter::get_info() {
    ConnectionInfo info;
    info.state      = state_.load();
    info.role       = role_.load();
    info.bytes_sent = bytes_sent_.load();
    info.bytes_recv = bytes_recv_.load();
    std::lock_guard<std::mutex> lk(info_mtx_);
    info.remote_peer = remote_peer_;
    info.local_addr  = local_addr_;
    info.remote_addr = remote_addr_;
    return info;
}

void WebrtcAdapter::close() {
    if (state_.load() == DW_P2P_STATE_CLOSED) return;
    if (impl_->dc) {
        try { impl_->dc->Close(); } catch (...) {}
        impl_->dc = nullptr;
    }
    if (impl_->pc) {
        try { impl_->pc->Close(); } catch (...) {}
        impl_->pc = nullptr;
    }
    impl_->pc_observer.reset();
    impl_->dc_observer.reset();
    emit_state(DW_P2P_STATE_CLOSED);
}

/* ================================================================== */
/*                    内部实现                                         */
/* ================================================================== */

void WebrtcAdapter::create_peer() {
    libwebrtc::RTCConfiguration config;
    config.ice_servers[0].uri = libwebrtc::string("stun:stun.l.google.com:19302");
    config.ice_servers[1].uri = libwebrtc::string("stun:stun1.l.google.com:19302");

    auto constraints = libwebrtc::RTCMediaConstraints::Create();
    impl_->pc = impl_->factory->Create(config, constraints);
    if (!impl_->pc) {
        throw std::runtime_error("PeerConnection 创建失败");
    }

    // 绑定 PeerConnection 观察者
    impl_->pc_observer = std::make_unique<PeerConnObserver>(this);
    impl_->pc->RegisterRTCPeerConnectionObserver(impl_->pc_observer.get());
}

void WebrtcAdapter::emit_state(dw_p2p_state_t new_state, int32_t error) {
    state_.store(new_state);
    std::lock_guard<std::mutex> lk(cb_mtx_);
    if (state_cb_) state_cb_(new_state, error);
}

void WebrtcAdapter::reset_internal() {
    if (impl_->dc) {
        try { impl_->dc->Close(); } catch (...) {}
        impl_->dc = nullptr;
    }
    if (impl_->pc) {
        try { impl_->pc->Close(); } catch (...) {}
        impl_->pc = nullptr;
    }
    impl_->pc_observer.reset();
    impl_->dc_observer.reset();
    bytes_sent_.store(0);
    bytes_recv_.store(0);
    {
        std::lock_guard<std::mutex> lk(info_mtx_);
        remote_peer_.clear();
        local_addr_.clear();
        remote_addr_.clear();
    }
    state_.store(DW_P2P_STATE_NEW);
}

/* ================================================================== */
/*                    信令编解码                                        */
/* ================================================================== */

std::string WebrtcAdapter::encode_signal(const SignalPayload &payload) {
    boost::json::object obj;
    obj["type"] = payload.type;
    obj["sdp"]  = payload.sdp;
    boost::json::array arr;
    for (const auto &c : payload.candidates) arr.emplace_back(c);
    obj["candidates"] = std::move(arr);
    const std::string json = boost::json::serialize(obj);
    return utils::base64url_encode(
        reinterpret_cast<const uint8_t *>(json.data()), json.size());
}

WebrtcAdapter::SignalPayload WebrtcAdapter::decode_signal(
    const std::string &code, const std::string &expected_type) {
    const auto bin = utils::base64url_decode(code);
    if (bin.empty()) throw std::runtime_error("信令码解码失败");

    const std::string json_str(bin.begin(), bin.end());
    boost::json::value val;
    try {
        val = boost::json::parse(json_str);
    } catch (const std::exception &e) {
        throw std::runtime_error(std::string("信令 JSON 解析失败: ") + e.what());
    }
    if (!val.is_object()) throw std::runtime_error("信令格式非法：非对象");
    const auto &obj = val.as_object();

    auto type_it = obj.find("type");
    auto sdp_it  = obj.find("sdp");
    auto cands_it = obj.find("candidates");
    if (type_it == obj.end() || sdp_it == obj.end() || cands_it == obj.end()) {
        throw std::runtime_error("信令缺少必要字段");
    }
    if (!type_it->value().is_string() || !sdp_it->value().is_string()
        || !cands_it->value().is_array()) {
        throw std::runtime_error("信令字段类型错误");
    }
    const std::string type_str(type_it->value().as_string().c_str());
    if (type_str != expected_type) {
        throw std::runtime_error("信令类型不匹配：期望 " + expected_type);
    }

    SignalPayload payload;
    payload.type = type_str;
    payload.sdp  = std::string(sdp_it->value().as_string().c_str());
    for (const auto &c : cands_it->value().as_array()) {
        if (c.is_string()) payload.candidates.emplace_back(c.as_string().c_str());
    }
    return payload;
}

/* ================================================================== */
/*                    Observer 方法实现                                 */
/* ================================================================== */

void PeerConnObserver::OnPeerConnectionState(
    libwebrtc::RTCPeerConnectionState s) {
    switch (s) {
        case libwebrtc::RTCPeerConnectionStateNew:
        case libwebrtc::RTCPeerConnectionStateConnecting:
            adapter_->emit_state(DW_P2P_STATE_CONNECTING);
            break;
        case libwebrtc::RTCPeerConnectionStateConnected:
            adapter_->emit_state(DW_P2P_STATE_CONNECTED);
            break;
        case libwebrtc::RTCPeerConnectionStateFailed:
            adapter_->emit_state(DW_P2P_STATE_FAILED, DW_P2P_ERR_TRANSPORT);
            break;
        case libwebrtc::RTCPeerConnectionStateDisconnected:
        case libwebrtc::RTCPeerConnectionStateClosed:
            adapter_->emit_state(DW_P2P_STATE_CLOSED);
            break;
    }
}

void PeerConnObserver::OnIceGatheringState(
    libwebrtc::RTCIceGatheringState s) {
    if (s == libwebrtc::RTCIceGatheringStateComplete) {
        std::lock_guard<std::mutex> lk(adapter_->ice_mtx_);
        adapter_->ice_done_ = true;
        adapter_->ice_cv_.notify_all();
    }
}

void PeerConnObserver::OnIceCandidate(
    scoped_refptr<libwebrtc::RTCIceCandidate> c) {
    if (!c) return;
    // 序列化: "candidate_str|mid|mline_index"
    std::string entry = c->candidate().std_string() + "|"
                      + c->sdp_mid().std_string() + "|"
                      + std::to_string(c->sdp_mline_index());
    std::lock_guard<std::mutex> lk(adapter_->ice_mtx_);
    adapter_->candidates_.emplace_back(std::move(entry));
}

void PeerConnObserver::OnDataChannel(
    scoped_refptr<libwebrtc::RTCDataChannel> dc) {
    if (!dc) return;
    // 应答方：被动接收对端创建的 DataChannel
    auto *self = adapter_;
    self->impl_->dc = dc;
    self->impl_->dc_observer = std::make_unique<DCObserver>(self);
    dc->RegisterObserver(self->impl_->dc_observer.get());
}

void DCObserver::OnStateChange(libwebrtc::RTCDataChannelState state) {
    if (state == libwebrtc::RTCDataChannelOpen) {
        adapter_->emit_state(DW_P2P_STATE_CONNECTED);
    } else if (state == libwebrtc::RTCDataChannelClosed) {
        adapter_->emit_state(DW_P2P_STATE_CLOSED);
    }
}

void DCObserver::OnMessage(const char *buffer, int length, bool /*binary*/) {
    if (!buffer || length <= 0) return;
    adapter_->bytes_recv_.fetch_add(static_cast<int64_t>(length));
    std::lock_guard<std::mutex> lk(adapter_->cb_mtx_);
    if (adapter_->data_cb_) {
        adapter_->data_cb_(reinterpret_cast<const uint8_t *>(buffer),
                           static_cast<size_t>(length));
    }
}

/* ================================================================== */
/*                    工厂                                             */
/* ================================================================== */

std::unique_ptr<TransportAdapter> TransportFactory::create_default() {
    return std::make_unique<WebrtcAdapter>();
}

const char *TransportFactory::default_implementation_name() {
    return "libwebrtc";
}

} // namespace dw::p2p
