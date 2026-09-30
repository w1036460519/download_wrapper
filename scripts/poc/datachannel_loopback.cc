// Phase 0 PoC：stock WebRTC「纯数据通道」回环验证。
//
// 目的：证明不调用 EnableMedia 的 data-only PeerConnectionFactory 可以：
//   1. 创建两个 PeerConnection；
//   2. 进程内完成 offer/answer + ICE 协商（本机 host candidate，无需 STUN）；
//   3. 建立 DataChannel 并成功收发一条消息。
//
// 本文件由 gn 目标 //poc:datachannel_loopback 编译，随 stock WebRTC 源码树一同
// 构建，从而复用 WebRTC 自身的编译/链接参数（libc++、abseil、SCTP 等），规避
// 外部手工拼装链接命令的坑。WebRTC 头只在本实现单元出现，与最终 adapter 的
// pimpl 隔离原则一致。

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "api/create_modular_peer_connection_factory.h"
#include "api/data_channel_interface.h"
#include "api/jsep.h"
#include "api/peer_connection_interface.h"
#include "api/rtc_error.h"
#include "api/scoped_refptr.h"
#include "rtc_base/copy_on_write_buffer.h"
#include "rtc_base/ref_counted_object.h"
#include "rtc_base/thread.h"

namespace {

using webrtc::DataBuffer;
using webrtc::DataChannelInterface;
using webrtc::IceCandidate;
using webrtc::PeerConnectionInterface;
using webrtc::RTCError;
using webrtc::RTCOfferAnswerOptions;
using webrtc::SdpType;
using webrtc::SessionDescriptionInterface;

const char kLabel[] = "poc-data";
const char kPayload[] = "p2p-loopback-ok";

class Loopback;

// CreateOffer / CreateAnswer 回调：把生成的 SDP 交回编排器（所有权转移给回调）。
class DescObserver : public webrtc::CreateSessionDescriptionObserver {
 public:
  DescObserver(Loopback* lb, bool is_offer) : lb_(lb), is_offer_(is_offer) {}
  void OnSuccess(SessionDescriptionInterface* desc) override;
  void OnFailure(RTCError error) override;

 private:
  Loopback* lb_;
  bool is_offer_;
};

// SetLocalDescription / SetRemoteDescription 回调。
class SetDescObserver : public webrtc::SetSessionDescriptionObserver {
 public:
  enum Stage { kLocalOffer, kRemoteOffer, kLocalAnswer, kRemoteAnswer };
  SetDescObserver(Loopback* lb, Stage stage) : lb_(lb), stage_(stage) {}
  void OnSuccess() override;
  void OnFailure(RTCError error) override;

 private:
  Loopback* lb_;
  Stage stage_;
};

// PeerConnection 观察者：ICE 采集完成 / 连接状态 / 收到对端 DataChannel。
class PcObserver : public webrtc::PeerConnectionObserver {
 public:
  PcObserver(Loopback* lb, bool is_initiator)
      : lb_(lb), is_initiator_(is_initiator) {}
  void OnSignalingChange(PeerConnectionInterface::SignalingState) override {}
  void OnIceGatheringChange(
      PeerConnectionInterface::IceGatheringState state) override;
  void OnConnectionChange(
      PeerConnectionInterface::PeerConnectionState state) override;
  // 采用非 trickle（vanilla ICE）：候选随完整 SDP 一并交换，无需单独转发。
  void OnIceCandidate(const IceCandidate*) override {}
  void OnDataChannel(rtc::scoped_refptr<DataChannelInterface> dc) override;

 private:
  Loopback* lb_;
  bool is_initiator_;
};

// DataChannel 观察者：状态变化（open）与收消息。
class DcObserver : public webrtc::DataChannelObserver {
 public:
  DcObserver(Loopback* lb, bool is_initiator)
      : lb_(lb), is_initiator_(is_initiator) {}
  void SetChannel(DataChannelInterface* ch) { ch_ = ch; }
  void OnStateChange() override;
  void OnMessage(const DataBuffer& buffer) override;

 private:
  Loopback* lb_;
  bool is_initiator_;
  DataChannelInterface* ch_ = nullptr;
};

// 编排器：持有线程/工厂/两条 PeerConnection/DataChannel，串联整个协商流程。
// 所有 WebRTC 回调都在 signaling 线程触发，回调内直接驱动下一步（线程安全）；
// 主线程只负责发起与等待最终结果。
class Loopback {
 public:
  bool Run(int timeout_ms);

  // 供观察者回调（均在 signaling 线程）：
  void OnDescCreated(bool is_offer, SessionDescriptionInterface* desc);
  void OnCreateDescFailed(bool is_offer, RTCError& error);
  void OnSetSuccess(SetDescObserver::Stage stage);
  void OnSetFailed(SetDescObserver::Stage stage, RTCError& error);
  void OnGatheringComplete(bool is_initiator);
  void OnConnectionState(bool is_initiator,
                         PeerConnectionInterface::PeerConnectionState state);
  void OnDataChannelReceived(rtc::scoped_refptr<DataChannelInterface> dc);
  void OnChannelOpen(bool is_initiator);
  void OnMessageReceived(const std::string& msg);

  void Fail(const std::string& reason);

 private:
  void CreateAnswer();
  void CheckHandOffer();
  void CheckHandAnswer();
  void HandOfferToResponder();
  void HandAnswerToInitiator();
  void TrySend();
  void Pass();

  // 线程最先声明 → 最后析构（PeerConnection/工厂须先于线程释放）。
  std::unique_ptr<rtc::Thread> net_;
  std::unique_ptr<rtc::Thread> worker_;
  std::unique_ptr<rtc::Thread> sig_;

  rtc::scoped_refptr<webrtc::PeerConnectionFactoryInterface> factory_;
  rtc::scoped_refptr<PeerConnectionInterface> pc_a_;  // 发起方
  rtc::scoped_refptr<PeerConnectionInterface> pc_b_;  // 应答方
  std::unique_ptr<PcObserver> obs_a_;
  std::unique_ptr<PcObserver> obs_b_;
  rtc::scoped_refptr<DataChannelInterface> dc_a_;
  rtc::scoped_refptr<DataChannelInterface> dc_b_;
  std::unique_ptr<DcObserver> dcobs_a_;
  std::unique_ptr<DcObserver> dcobs_b_;

  // 保活异步 SDP 观察者，防止回调前被释放。
  std::vector<rtc::scoped_refptr<webrtc::RefCountInterface>> keepalive_;

  // 协商进度标志（仅 signaling 线程访问）。
  bool local_offer_set_ = false;
  bool gather_a_complete_ = false;
  bool offer_handed_ = false;
  bool local_answer_set_ = false;
  bool gather_b_complete_ = false;
  bool answer_handed_ = false;
  bool sent_ = false;

  std::atomic<int> result_{0};  // 0=进行中 1=通过 2=失败
  std::string reason_;
};

// ── 观察者实现 ──

void DescObserver::OnSuccess(SessionDescriptionInterface* desc) {
  lb_->OnDescCreated(is_offer_, desc);
}
void DescObserver::OnFailure(RTCError error) {
  lb_->OnCreateDescFailed(is_offer_, error);
}

void SetDescObserver::OnSuccess() { lb_->OnSetSuccess(stage_); }
void SetDescObserver::OnFailure(RTCError error) {
  lb_->OnSetFailed(stage_, error);
}

void PcObserver::OnIceGatheringChange(
    PeerConnectionInterface::IceGatheringState state) {
  if (state == PeerConnectionInterface::kIceGatheringComplete) {
    lb_->OnGatheringComplete(is_initiator_);
  }
}
void PcObserver::OnConnectionChange(
    PeerConnectionInterface::PeerConnectionState state) {
  lb_->OnConnectionState(is_initiator_, state);
}
void PcObserver::OnDataChannel(rtc::scoped_refptr<DataChannelInterface> dc) {
  lb_->OnDataChannelReceived(std::move(dc));
}

void DcObserver::OnStateChange() {
  if (ch_ && ch_->state() == DataChannelInterface::kOpen) {
    lb_->OnChannelOpen(is_initiator_);
  }
}
void DcObserver::OnMessage(const DataBuffer& buffer) {
  std::string msg(buffer.data.data<char>(), buffer.data.size());
  lb_->OnMessageReceived(msg);
}

// ── 编排器实现 ──

void Loopback::OnDescCreated(bool is_offer, SessionDescriptionInterface* desc) {
  if (is_offer) {
    std::printf("[EVENT] 发起方 CreateOffer 成功\n");
    auto obs = rtc::make_ref_counted<SetDescObserver>(
        this, SetDescObserver::kLocalOffer);
    keepalive_.push_back(obs);
    // SetLocalDescription 接管 desc 所有权。
    pc_a_->SetLocalDescription(obs.get(), desc);
  } else {
    std::printf("[EVENT] 应答方 CreateAnswer 成功\n");
    auto obs = rtc::make_ref_counted<SetDescObserver>(
        this, SetDescObserver::kLocalAnswer);
    keepalive_.push_back(obs);
    pc_b_->SetLocalDescription(obs.get(), desc);
  }
}

void Loopback::OnCreateDescFailed(bool is_offer, RTCError& error) {
  Fail(std::string(is_offer ? "CreateOffer" : "CreateAnswer") + " 失败: " +
       std::string(error.message()));
}

void Loopback::OnSetSuccess(SetDescObserver::Stage stage) {
  switch (stage) {
    case SetDescObserver::kLocalOffer:
      std::printf("[EVENT] 发起方 SetLocalDescription(offer) 完成\n");
      local_offer_set_ = true;
      CheckHandOffer();
      break;
    case SetDescObserver::kRemoteOffer:
      std::printf("[EVENT] 应答方 SetRemoteDescription(offer) 完成\n");
      CreateAnswer();
      break;
    case SetDescObserver::kLocalAnswer:
      std::printf("[EVENT] 应答方 SetLocalDescription(answer) 完成\n");
      local_answer_set_ = true;
      CheckHandAnswer();
      break;
    case SetDescObserver::kRemoteAnswer:
      std::printf("[EVENT] 发起方 SetRemoteDescription(answer) 完成，等待 ICE 连接\n");
      break;
  }
}

void Loopback::OnSetFailed(SetDescObserver::Stage stage, RTCError& error) {
  const char* names[] = {"SetLocal(offer)", "SetRemote(offer)",
                         "SetLocal(answer)", "SetRemote(answer)"};
  Fail(std::string(names[stage]) + " 失败: " + std::string(error.message()));
}

void Loopback::OnGatheringComplete(bool is_initiator) {
  std::printf("[EVENT] ICE 采集完成 (%s)\n", is_initiator ? "发起方" : "应答方");
  if (is_initiator) {
    gather_a_complete_ = true;
    CheckHandOffer();
  } else {
    gather_b_complete_ = true;
    CheckHandAnswer();
  }
}

void Loopback::CheckHandOffer() {
  if (local_offer_set_ && gather_a_complete_ && !offer_handed_) {
    offer_handed_ = true;
    HandOfferToResponder();
  }
}

void Loopback::CheckHandAnswer() {
  if (local_answer_set_ && gather_b_complete_ && !answer_handed_) {
    answer_handed_ = true;
    HandAnswerToInitiator();
  }
}

void Loopback::HandOfferToResponder() {
  const SessionDescriptionInterface* ld = pc_a_->local_description();
  if (!ld) {
    Fail("发起方 local_description 为空");
    return;
  }
  std::string sdp = ld->ToString();  // 含全部 host candidate
  std::printf("[EVENT] 向应答方投递完整 offer (%zu 字节)\n", sdp.size());
  auto remote = webrtc::CreateSessionDescription(SdpType::kOffer, sdp);
  auto obs =
      rtc::make_ref_counted<SetDescObserver>(this, SetDescObserver::kRemoteOffer);
  keepalive_.push_back(obs);
  pc_b_->SetRemoteDescription(obs.get(), remote.release());
}

void Loopback::HandAnswerToInitiator() {
  const SessionDescriptionInterface* ld = pc_b_->local_description();
  if (!ld) {
    Fail("应答方 local_description 为空");
    return;
  }
  std::string sdp = ld->ToString();
  std::printf("[EVENT] 向发起方投递完整 answer (%zu 字节)\n", sdp.size());
  auto remote = webrtc::CreateSessionDescription(SdpType::kAnswer, sdp);
  auto obs = rtc::make_ref_counted<SetDescObserver>(
      this, SetDescObserver::kRemoteAnswer);
  keepalive_.push_back(obs);
  pc_a_->SetRemoteDescription(obs.get(), remote.release());
}

void Loopback::CreateAnswer() {
  auto obs = rtc::make_ref_counted<DescObserver>(this, /*is_offer=*/false);
  keepalive_.push_back(obs);
  pc_b_->CreateAnswer(obs.get(), RTCOfferAnswerOptions());
}

void Loopback::OnConnectionState(
    bool is_initiator, PeerConnectionInterface::PeerConnectionState state) {
  if (state == PeerConnectionInterface::PeerConnectionState::kConnected) {
    std::printf("[EVENT] PeerConnection 已连接 (%s)\n",
                is_initiator ? "发起方" : "应答方");
  } else if (state == PeerConnectionInterface::PeerConnectionState::kFailed) {
    Fail(std::string(is_initiator ? "发起方" : "应答方") + " PeerConnection 连接失败");
  }
}

void Loopback::OnDataChannelReceived(
    rtc::scoped_refptr<DataChannelInterface> dc) {
  std::printf("[EVENT] 应答方收到 DataChannel: %s\n", dc->label().c_str());
  dc_b_ = std::move(dc);
  dcobs_b_ = std::make_unique<DcObserver>(this, /*is_initiator=*/false);
  dcobs_b_->SetChannel(dc_b_.get());
  dc_b_->RegisterObserver(dcobs_b_.get());
}

void Loopback::OnChannelOpen(bool is_initiator) {
  std::printf("[EVENT] DataChannel 打开 (%s)\n", is_initiator ? "发起方" : "应答方");
  if (is_initiator) {
    TrySend();
  }
}

void Loopback::TrySend() {
  if (sent_ || !dc_a_ || dc_a_->state() != DataChannelInterface::kOpen) {
    return;
  }
  sent_ = true;
  std::printf("[EVENT] 发起方发送消息: %s\n", kPayload);
  dc_a_->Send(DataBuffer(std::string(kPayload)));
}

void Loopback::OnMessageReceived(const std::string& msg) {
  std::printf("[EVENT] 应答方收到消息: %s (%zu 字节)\n", msg.c_str(), msg.size());
  if (msg == kPayload) {
    Pass();
  } else {
    Fail("消息内容不匹配，期望 '" + std::string(kPayload) + "' 实得 '" + msg + "'");
  }
}

void Loopback::Pass() {
  if (result_.load() == 0) {
    std::printf("[OK] 回环 DataChannel 收发成功\n");
    result_.store(1, std::memory_order_release);
  }
}

void Loopback::Fail(const std::string& reason) {
  if (result_.load() == 0) {
    std::printf("[ERROR] %s\n", reason.c_str());
    reason_ = reason;
    result_.store(2, std::memory_order_release);
  }
}

bool Loopback::Run(int timeout_ms) {
  // 1. 线程：data-only 工厂需要 network/worker/signaling 三条线程。
  net_ = rtc::Thread::CreateWithSocketServer();
  worker_ = rtc::Thread::CreateWithSocketServer();
  sig_ = rtc::Thread::CreateWithSocketServer();
  if (!net_->Start() || !worker_->Start() || !sig_->Start()) {
    Fail("rtc::Thread 启动失败");
    return false;
  }

  // 2. 工厂依赖：只给线程，media_factory 保持 nullptr。
  webrtc::PeerConnectionFactoryDependencies deps;
  deps.network_thread = net_.get();
  deps.worker_thread = worker_.get();
  deps.signaling_thread = sig_.get();
  // 关键：不调用 webrtc::EnableMedia(deps) → 纯数据通道构建。
  factory_ = webrtc::CreateModularPeerConnectionFactory(std::move(deps));
  if (!factory_) {
    Fail("CreateModularPeerConnectionFactory 返回空");
    return false;
  }
  std::printf("[OK] data-only PeerConnectionFactory 创建成功\n");

  // 3. 两条 PeerConnection：不配置任何 ICE server，纯 host candidate 本机回环。
  PeerConnectionInterface::RTCConfiguration config;
  obs_a_ = std::make_unique<PcObserver>(this, /*is_initiator=*/true);
  obs_b_ = std::make_unique<PcObserver>(this, /*is_initiator=*/false);

  auto res_a = factory_->CreatePeerConnectionOrError(
      config, webrtc::PeerConnectionDependencies(obs_a_.get()));
  if (!res_a.ok()) {
    Fail(std::string("创建发起方 PeerConnection 失败: ") +
         res_a.error().message());
    return false;
  }
  pc_a_ = res_a.MoveValue();

  auto res_b = factory_->CreatePeerConnectionOrError(
      config, webrtc::PeerConnectionDependencies(obs_b_.get()));
  if (!res_b.ok()) {
    Fail(std::string("创建应答方 PeerConnection 失败: ") +
         res_b.error().message());
    return false;
  }
  pc_b_ = res_b.MoveValue();
  std::printf("[OK] 两条 PeerConnection 创建成功\n");

  // 4. 发起方先建 DataChannel，使 offer 携带 m=application。
  webrtc::DataChannelInit init;
  init.ordered = true;
  auto dc_res = pc_a_->CreateDataChannelOrError(kLabel, &init);
  if (!dc_res.ok()) {
    Fail(std::string("创建 DataChannel 失败: ") + dc_res.error().message());
    return false;
  }
  dc_a_ = dc_res.MoveValue();
  dcobs_a_ = std::make_unique<DcObserver>(this, /*is_initiator=*/true);
  dcobs_a_->SetChannel(dc_a_.get());
  dc_a_->RegisterObserver(dcobs_a_.get());
  std::printf("[OK] 发起方 DataChannel 创建成功\n");

  // 5. 触发协商：CreateOffer（后续步骤全部由回调在 signaling 线程串联驱动）。
  auto offer_obs = rtc::make_ref_counted<DescObserver>(this, /*is_offer=*/true);
  keepalive_.push_back(offer_obs);
  pc_a_->CreateOffer(offer_obs.get(), RTCOfferAnswerOptions());

  // 6. 主线程等待最终结果（带超时）。
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
  while (result_.load(std::memory_order_acquire) == 0 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  if (result_.load() == 0) {
    Fail("超时：规定时间内未完成回环收发");
  }
  return result_.load() == 1;
}

}  // namespace

int main() {
  std::printf("== stock WebRTC data-only 回环验证 (Phase 0 PoC) ==\n");
  Loopback lb;
  const bool ok = lb.Run(/*timeout_ms=*/30000);
  std::printf("\n[RESULT] %s\n", ok ? "PASS" : "FAIL");
  // PoC 直接退出，跳过 rtc::Thread 析构期的线程 join 竞争（生产 adapter 会正规
  // 管理生命周期）。此处只求把 data-only 构建是否可用的信号稳定带回 CI。
  std::_Exit(ok ? 0 : 1);
}
