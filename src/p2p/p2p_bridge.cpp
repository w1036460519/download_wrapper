/**
 * @file p2p_bridge.cpp
 * @brief P2P C ABI 桥接：dw_p2p_* 接口到 TransportAdapter 的映射。
 *
 * 句柄映射：dw_p2p_handle 实际为 dw::p2p::AdapterEntry*，内部持有
 * std::unique_ptr<TransportAdapter>。注册表经全局 mutex 保护，
 * 回调经 std::function 透传，避免业务层感知具体打洞库。
 */

#include "download_wrapper/download_wrapper.h"
#include "p2p/transport_adapter.h"

#include <mutex>
#include <unordered_map>
#include <memory>
#include <cstring>
#include <string>

#include "internal/downloader_internal.h"

// 句柄实体结构体：与 C ABI 头文件中 typedef 的不透明类型同名（全局命名空间），
// 因此 AdapterEntry* 可直接作为 dw_p2p_handle 透传给调用方。
struct dw_p2p_handle_s {
    std::unique_ptr<dw::p2p::TransportAdapter> adapter;
    dw_p2p_state_cb state_cb{nullptr};
    void           *state_ud{nullptr};
    dw_p2p_data_cb  data_cb{nullptr};
    void           *data_ud{nullptr};
    std::mutex      cb_mtx;

    dw_p2p_handle_s() : adapter(dw::p2p::TransportFactory::create_default()) {
        adapter->set_state_callback([this](dw_p2p_state_t s, int32_t e) {
            std::lock_guard<std::mutex> lk(cb_mtx);
            if (state_cb) state_cb(state_ud, s, e);
        });
        adapter->set_data_callback([this](const uint8_t *data, size_t len) {
            std::lock_guard<std::mutex> lk(cb_mtx);
            if (data_cb) data_cb(data_ud, data, len);
        });
    }
};

namespace dw::p2p {

    using AdapterEntry = ::dw_p2p_handle_s;

    /// 句柄 → 实体注册表
    std::mutex                                                  g_registry_mtx;
    std::unordered_map<dw_p2p_handle, std::unique_ptr<AdapterEntry>> g_registry;

    /// 查找实体；未找到返回 nullptr
    AdapterEntry *find_entry(dw_p2p_handle h) {
        if (!h) return nullptr;
        std::lock_guard<std::mutex> lk(g_registry_mtx);
        auto it = g_registry.find(h);
        return it == g_registry.end() ? nullptr : it->second.get();
    }

    /// 将字符串写入输出缓冲区；返回所需大小（不含 NUL）
    /// 若 out_size 不足返回 DW_P2P_ERR_BUFFER 并写入所需大小到 out_required
    int32_t write_string(const std::string &src, char *out, size_t out_size, size_t *out_required) {
        const size_t need = src.size() + 1;
        if (out_required) *out_required = need;
        if (need > out_size) return DW_P2P_ERR_BUFFER;
        std::memcpy(out, src.data(), need);
        return DW_P2P_OK;
    }

} // namespace dw::p2p

using namespace dw::p2p;

/* ================================================================== */
/*                          C API 实现                                */
/* ================================================================== */

DW_API int32_t dw_p2p_create(dw_p2p_handle *out_handle) {
    if (!out_handle) {
        dw::log_e("p2p", "创建失败: 参数为空");
        return -1;
    }
    try {
        auto entry = std::make_unique<AdapterEntry>();
        auto *raw = entry.get(); // AdapterEntry* 即 dw_p2p_handle
        {
            std::lock_guard<std::mutex> lk(g_registry_mtx);
            g_registry.emplace(raw, std::move(entry));
        }
        *out_handle = raw;
        dw::log_i("p2p", "创建成功: impl=%s", TransportFactory::default_implementation_name());
        return 0;
    } catch (const std::exception &e) {
        dw::log_e("p2p", "创建失败: %s", e.what());
        return -1;
    }
}
DW_API void dw_p2p_destroy(dw_p2p_handle handle) {
    if (!handle) return;
    std::lock_guard<std::mutex> lk(g_registry_mtx);
    auto it = g_registry.find(handle);
    if (it == g_registry.end()) return;
    it->second->adapter.reset();
    g_registry.erase(it);
    dw::log_i("p2p", "销毁完成");
}

DW_API void dw_p2p_set_state_callback(dw_p2p_handle handle,
                                      dw_p2p_state_cb cb,
                                      void *user_data) {
    auto *e = find_entry(handle);
    if (!e) return;
    std::lock_guard<std::mutex> lk(e->cb_mtx);
    e->state_cb = cb;
    e->state_ud = user_data;
}

DW_API void dw_p2p_set_data_callback(dw_p2p_handle handle,
                                     dw_p2p_data_cb cb,
                                     void *user_data) {
    auto *e = find_entry(handle);
    if (!e) return;
    std::lock_guard<std::mutex> lk(e->cb_mtx);
    e->data_cb = cb;
    e->data_ud = user_data;
}

DW_API int32_t dw_p2p_generate_offer(dw_p2p_handle handle,
                                     char *out_code, size_t code_size,
                                     size_t *out_required) {
    auto *e = find_entry(handle);
    if (!e) return DW_P2P_ERR_NULL;
    try {
        const std::string code = e->adapter->generate_offer();
        return write_string(code, out_code, code_size, out_required);
    } catch (const std::exception &err) {
        dw::log_e("p2p", "生成 offer 失败: %s", err.what());
        return DW_P2P_ERR_TRANSPORT;
    }
}

DW_API int32_t dw_p2p_accept_offer(dw_p2p_handle handle,
                                   const char *offer_code,
                                   char *out_code, size_t code_size,
                                   size_t *out_required) {
    auto *e = find_entry(handle);
    if (!e || !offer_code) return DW_P2P_ERR_NULL;
    try {
        const std::string answer = e->adapter->accept_offer(offer_code);
        return write_string(answer, out_code, code_size, out_required);
    } catch (const std::exception &err) {
        dw::log_e("p2p", "接受 offer 失败: %s", err.what());
        return DW_P2P_ERR_DECODE;
    }
}

DW_API int32_t dw_p2p_accept_answer(dw_p2p_handle handle,
                                    const char *answer_code) {
    auto *e = find_entry(handle);
    if (!e || !answer_code) return DW_P2P_ERR_NULL;
    try {
        e->adapter->accept_answer(answer_code);
        return DW_P2P_OK;
    } catch (const std::exception &err) {
        dw::log_e("p2p", "接受 answer 失败: %s", err.what());
        return DW_P2P_ERR_DECODE;
    }
}

DW_API int32_t dw_p2p_send(dw_p2p_handle handle,
                           const void *data, size_t len) {
    auto *e = find_entry(handle);
    if (!e) return DW_P2P_ERR_NULL;
    if (!data || len == 0) return DW_P2P_ERR_NULL;
    return e->adapter->send(data, len);
}

DW_API int32_t dw_p2p_get_info(dw_p2p_handle handle, dw_p2p_info_t *out) {
    auto *e = find_entry(handle);
    if (!e || !out) return -1;
    const ConnectionInfo info = e->adapter->get_info();
    out->state = info.state;
    out->role = info.role;
    out->bytes_sent = info.bytes_sent;
    out->bytes_recv = info.bytes_recv;
    // 字符串字段：库内深拷贝，调用方经 dw_p2p_info_release 释放
    auto dup = [](const std::string &s) -> char * {
        if (s.empty()) return nullptr;
        char *p = static_cast<char *>(std::malloc(s.size() + 1));
        if (p) {
            std::memcpy(p, s.data(), s.size());
            p[s.size()] = '\0';
        }
        return p;
    };
    out->remote_peer = dup(info.remote_peer);
    out->local_addr = dup(info.local_addr);
    out->remote_addr = dup(info.remote_addr);
    return 0;
}

DW_API void dw_p2p_info_release(dw_p2p_info_t *info) {
    if (!info) return;
    std::free(const_cast<char *>(info->remote_peer));
    std::free(const_cast<char *>(info->local_addr));
    std::free(const_cast<char *>(info->remote_addr));
    info->remote_peer = nullptr;
    info->local_addr = nullptr;
    info->remote_addr = nullptr;
}

DW_API int32_t dw_p2p_close(dw_p2p_handle handle) {
    auto *e = find_entry(handle);
    if (!e) return -1;
    try {
        e->adapter->close();
        return 0;
    } catch (const std::exception &err) {
        dw::log_e("p2p", "关闭失败: %s", err.what());
        return -1;
    }
}
