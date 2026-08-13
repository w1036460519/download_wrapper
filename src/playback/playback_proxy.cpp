/**
 * @file playback_proxy.cpp
 * @brief 边下边播本地 HTTP 代理：Boost.Beast 实现。
 *
 * 在 127.0.0.1 随机端口启动 HTTP 服务，接收播放器 Range 请求后：
 *   1. 经 dw_get_file_ranges 查询已下载分段（优先读 wrapper 内存缓存）；
 *   2. 经 dw_get_task_file_info 获取物理路径与总大小；
 *   3. 已下载部分直接读文件返回；未下载部分阻塞等待新数据到达后继续传输；
 *   4. 客户端断开（connection_reset / eof）时立即终止传输与等待。
 *
 * dw_get_file_ranges 返回值驱动代理行为：
 *   0 = 有数据 → 正常传输；
 *   1 = 下载中暂无 → 等待重试（数据会增长）；
 *   2 = 非下载中无数据 → 停止（数据不会增长）。
 *
 * 响应模型：首次发送 206 响应头（含 Content-Range / Content-Length），
 * 后续仅写 body 数据（async_write raw bytes），避免重复发送头部。
 */

#include "download_wrapper/playback_proxy.h"

#include <boost/asio.hpp>
#include <boost/beast.hpp>
#include <boost/url.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace dw {
namespace playback {

namespace beast = boost::beast;
namespace http  = beast::http;
namespace net   = boost::asio;
using tcp = boost::asio::ip::tcp;

/// 单次读取并发送的最大块大小（64KB）
static constexpr std::size_t kChunkSize = 65536;

/// 等待新数据到达的定时间隔（毫秒）
static constexpr int kWaitIntervalMs = 500;

/// 全局状态
static std::atomic<int>          g_port{0};
static std::unique_ptr<net::io_context>    g_ioc;
static std::unique_ptr<tcp::acceptor>      g_acceptor;
static std::unique_ptr<std::thread>        g_thread;
static std::atomic<bool>         g_running{false};

// ========================================================================
//  工具函数
// ========================================================================

/// 解析 HTTP Range 头。支持 "bytes=start-end" 与 "bytes=start-"（开放区间）。
/// 解析成功返回 true。
bool parse_range_header(const std::string& value,
                        int64_t& out_start, int64_t& out_end) {
    // 期望格式：bytes=<start>-<end> 或 bytes=<start>-
    const std::string prefix = "bytes=";
    if (value.substr(0, prefix.size()) != prefix) return false;
    auto range_spec = value.substr(prefix.size());
    auto dash = range_spec.find('-');
    if (dash == std::string::npos) return false;

    try {
        out_start = std::stoll(range_spec.substr(0, dash));
    } catch (...) {
        return false;
    }

    auto end_spec = range_spec.substr(dash + 1);
    if (end_spec.empty()) {
        out_end = -1; // 开放区间，由 total_size 补齐
    } else {
        try {
            out_end = std::stoll(end_spec);
        } catch (...) {
            return false;
        }
    }
    return true;
}

/// 从文件读取数据。返回空 vector 表示读取失败或无数据。
std::vector<uint8_t> read_file_data(const std::string& path,
                                    int64_t offset, int64_t size) {
    if (size <= 0) return {};
    std::ifstream file(path, std::ios::binary);
    if (!file.is_open()) return {};

    file.seekg(offset);
    std::vector<uint8_t> buffer(static_cast<std::size_t>(size));
    file.read(reinterpret_cast<char*>(buffer.data()), size);
    const auto got = file.gcount();
    if (got <= 0) return {};
    buffer.resize(static_cast<std::size_t>(got));
    return buffer;
}

// ========================================================================
//  HTTP 会话
// ========================================================================

class session : public std::enable_shared_from_this<session> {
public:
    explicit session(tcp::socket socket)
        : stream_(std::move(socket)) {}

    void run() { do_read(); }

private:
    beast::tcp_stream stream_;
    beast::flat_buffer buffer_;
    http::request<http::string_body> req_;

    // ---- 请求级状态（handle_request 初始化，后续阶段只读） ----
    // key_type_ + natural_key_ 拆分存储：与 C ABI 一致，URL 参数原样透传。
    // type 取值 "http" / "bt"；natural_key 须 URL 编码。
    std::string natural_key_;
    int     key_type_     = DW_KEY_TYPE_HTTP;
    int     file_index_   = 0;
    int64_t range_start_  = 0;
    int64_t range_end_    = 0;   // 含（闭合区间）；-1 在 handle_request 中已补齐
    int64_t total_size_   = -1;
    std::string file_path_;
    bool header_sent_     = false;

    net::steady_timer timer_{stream_.get_executor()};

    // ================================================================
    //  阶段 1：读取请求
    // ================================================================
    void do_read() {
        auto self = shared_from_this();
        http::async_read(
            stream_, buffer_, req_,
            [self](beast::error_code ec, std::size_t) {
                if (ec) return;
                self->handle_request();
            });
    }

    // ================================================================
    //  阶段 2：解析请求、构造响应头
    // ================================================================
    void handle_request() {
        // ---- 路径校验与参数解析（Boost.URL）----
        boost::system::result<boost::urls::url_view> parsed =
            boost::urls::parse_relative_ref(req_.target());
        if (!parsed || parsed->encoded_path() != "/file") {
            send_error(http::status::not_found, "Not Found");
            return;
        }
        auto params = parsed->params();
        auto type_it = params.find("type");
        auto key_it  = params.find("key");
        auto file_it = params.find("file");
        if (type_it == params.end() || key_it == params.end() || file_it == params.end()) {
            send_error(http::status::bad_request, "Missing type/key/file parameter");
            return;
        }
        try {
            const std::string type_str((*type_it).value);
            if (type_str == "http") key_type_ = DW_KEY_TYPE_HTTP;
            else if (type_str == "bt") key_type_ = DW_KEY_TYPE_BT;
            else {
                send_error(http::status::bad_request, "Invalid type parameter");
                return;
            }
            natural_key_ = std::string((*key_it).value);
            file_index_  = std::stoi(std::string((*file_it).value));
        } catch (...) {
            send_error(http::status::bad_request, "Invalid type/key/file parameter");
            return;
        }

        // ---- 解析 Range 头 ----
        auto range_header = req_[http::field::range];
        if (range_header.empty()) {
            send_error(http::status::bad_request, "Missing Range header");
            return;
        }
        if (!parse_range_header(std::string(range_header),
                                range_start_, range_end_)) {
            send_error(http::status::bad_request, "Invalid Range header");
            return;
        }

        // ---- 获取文件路径与总大小 ----
        const dw_task_key_t task_key{
            static_cast<dw_key_type_t>(key_type_), natural_key_.c_str()};
        char*    raw_path = nullptr;
        int64_t  file_size = -1;
        if (dw_get_task_file_info(&task_key,
                                  static_cast<int32_t>(file_index_),
                                  &raw_path, &file_size) != 0 || !raw_path) {
            send_error(http::status::not_found, "Task or file not found");
            return;
        }
        file_path_  = raw_path;
        total_size_ = file_size;
        dw_free(raw_path);

        // ---- 开放区间补齐 ----
        if (range_end_ < 0) {
            if (total_size_ > 0) {
                range_end_ = total_size_ - 1;
            } else {
                // 总大小未知：尝试从文件系统获取
                std::ifstream f(file_path_, std::ios::ate | std::ios::binary);
                if (f.is_open() && f.tellg() > 0) {
                    range_end_ = static_cast<int64_t>(f.tellg()) - 1;
                } else {
                    send_error(http::status::internal_server_error,
                               "Cannot determine file size for open-ended range");
                    return;
                }
            }
        }

        // ---- 边界检查 ----
        if (range_start_ > range_end_) {
            send_range_error();
            return;
        }

        // ---- 开始流式传输 ----
        do_stream(range_start_);
    }

    // ================================================================
    //  阶段 3：核心流式传输
    //
    //  每次调用查询已下载分段（优先读 wrapper 内存缓存），按 current_pos 查找命中：
    //    情况 1（current_pos 未命中）→ 等待新数据
    //    情况 2（start 命中，end 未命中）→ 发送已有部分 → 继续等待
    //    情况 3（start 命中，end 命中）→ 发送完整数据 → 结束
    //
    //  dw_get_file_ranges 返回值语义：
    //    0 = 有数据，-1 = 错误，1 = 下载中暂无（应等待），2 = 非下载中无数据（应停止）
    // ================================================================
    void do_stream(int64_t current_pos) {
        auto self = shared_from_this();

        // ---- 查询已下载分段（优先缓存，按状态区分） ----
        const dw_task_key_t task_key{
            static_cast<dw_key_type_t>(key_type_), natural_key_.c_str()};
        dw_byte_range_t* ranges     = nullptr;
        int32_t          range_count = 0;
        const int32_t rc = dw_get_file_ranges(&task_key,
                                              static_cast<int32_t>(file_index_),
                                              &ranges, &range_count);

        if (rc == -1) {
            // 查询失败（任务不存在等）
            if (!header_sent_) {
                send_error(http::status::internal_server_error, "Range query failed");
            }
            return;
        }

        if (rc == 2) {
            // 非下载中且无数据：数据不会增长，停止等待
            if (!header_sent_) {
                send_error(http::status::not_found, "No data available");
            } else {
                // header 已发送但后续无数据：关闭写端
                beast::error_code ec;
                stream_.socket().shutdown(tcp::socket::shutdown_send, ec);
            }
            return;
        }

        // rc == 0（有数据）或 rc == 1（下载中暂无，等待）

        // 查找 current_pos 所在的已下载范围
        bool found = false;
        int64_t seg_end = 0;
        for (int32_t i = 0; i < range_count; ++i) {
            if (current_pos >= ranges[i].start &&
                current_pos <= ranges[i].end) {
                found  = true;
                seg_end = ranges[i].end;
                break;
            }
        }
        dw_byte_range_free(ranges, range_count);

        // ========== 情况 1：current_pos 未命中任何分段 → 等待 ==========
        if (!found) {
            wait_for_data(current_pos);
            return;
        }

        // ========== 情况 2 & 3：current_pos 命中 ==========
        const int64_t send_start = current_pos;
        int64_t send_end = std::min(seg_end, range_end_);
        // 限制单次读取大小，避免一次读入过大数据块
        const int64_t max_read = static_cast<int64_t>(kChunkSize);
        if (send_end - send_start + 1 > max_read) {
            send_end = send_start + max_read - 1;
        }
        const int64_t read_size = send_end - send_start + 1;

        auto data = read_file_data(file_path_, send_start, read_size);
        if (data.empty()) {
            // 分段信息显示有数据但文件读取失败，可能是分段信息过时
            if (!header_sent_) {
                send_error(http::status::internal_server_error, "File read failed");
            }
            return;
        }

        // ---- 首次发送：构造并发送 206 响应头，回调中再发 body ----
        if (!header_sent_) {
            header_sent_ = true;
            // 暂存首批 body 数据，header 发送成功后回调中取出
            pending_next_pos_ = send_end + 1;
            pending_data_     = std::move(data);
            do_write_header();
            return;
        }

        // ---- 非首次：header 已发送，直接发 body ----
        do_write_body(send_end + 1, std::move(data));
    }

    // ================================================================
    //  阶段 3a：发送 206 响应头（异步，成功后回调中发 body）
    // ================================================================
    void do_write_header() {
        auto self = shared_from_this();

        http::response<http::empty_body> res;
        res.version(11);
        res.result(http::status::partial_content);
        res.set(http::field::accept_ranges, "bytes");
        res.set(http::field::content_type, "application/octet-stream");

        // Content-Range: bytes start-end/total
        const std::string total_str =
            (total_size_ > 0) ? std::to_string(total_size_) : "*";
        res.set(http::field::content_range,
                "bytes " + std::to_string(range_start_) + "-" +
                std::to_string(range_end_) + "/" + total_str);

        // Content-Length = 请求区间总字节数
        res.set(http::field::content_length,
                std::to_string(range_end_ - range_start_ + 1));

        // 拷贝到成员以延长生命周期（async 期间需要存活）
        header_res_ = std::move(res);
        header_sr_.emplace(header_res_);

        http::async_write_header(
            stream_, *header_sr_,
            [self](beast::error_code ec, std::size_t) {
                if (ec) return; // 客户端断开，不再继续
                // header 发送成功，取出暂存的首批 body 数据并发送
                self->do_write_body(self->pending_next_pos_,
                                    std::move(self->pending_data_));
            });
    }

    // ================================================================
    //  阶段 3b：发送 body 数据（raw bytes，非完整 HTTP 响应）
    //
    //  data 移入成员 write_data_ 以延长生命周期（async 期间需存活），
    //  buffers_ 指向 write_data_ 的内存，两者同步有效。
    // ================================================================
    void do_write_body(int64_t next_pos, std::vector<uint8_t> data) {
        auto self = shared_from_this();

        // 数据移入成员，确保 async 期间内存有效
        write_data_ = std::move(data);

        buffers_.clear();
        buffers_.emplace_back(write_data_.data(), write_data_.size());

        net::async_write(
            stream_, buffers_,
            [self, next_pos]
            (beast::error_code ec, std::size_t) {
                // ---- 客户端取消检测 ----
                if (ec) {
                    // connection_reset / eof / operation_aborted / broken_pipe
                    // 均为客户端断开或取消，静默退出
                    return;
                }

                // ---- 传输完成？ ----
                if (next_pos > self->range_end_) {
                    // 全部请求区间已发送，关闭写端
                    beast::error_code shutdown_ec;
                    self->stream_.socket().shutdown(
                        tcp::socket::shutdown_send, shutdown_ec);
                    return;
                }

                // ---- 继续传输（情况 2：end 未命中，从 next_pos 继续） ----
                self->do_stream(next_pos);
            });
    }

    // ================================================================
    //  阶段 4：等待新数据到达
    //
    //  定时器每 500ms 触发一次，重新查询分段信息。
    //  客户端断开时 socket 关闭，async_wait 回调中检测到后退出。
    // ================================================================
    void wait_for_data(int64_t current_pos) {
        auto self = shared_from_this();

        timer_.expires_after(std::chrono::milliseconds(kWaitIntervalMs));
        timer_.async_wait(
            [self, current_pos](beast::error_code ec) {
                if (ec) return; // 定时器取消（session 析构等）

                // 检查客户端是否还在
                if (!self->stream_.socket().is_open()) return;

                // 重新查询分段，看是否有新数据到达
                self->do_stream(current_pos);
            });
    }

    // ================================================================
    //  错误响应
    // ================================================================
    void send_error(http::status status, const std::string& message) {
        http::response<http::string_body> res;
        res.version(11);
        res.result(status);
        res.set(http::field::content_type, "text/plain");
        res.body() = message;
        res.prepare_payload();

        beast::error_code ec;
        http::write(stream_, res, ec);
    }

    /// 416 Range Not Satisfiable
    void send_range_error() {
        http::response<http::empty_body> res;
        res.version(11);
        res.result(http::status::range_not_satisfiable);
        if (total_size_ > 0) {
            res.set(http::field::content_range,
                    "bytes */" + std::to_string(total_size_));
        }
        res.prepare_payload();

        beast::error_code ec;
        http::write(stream_, res, ec);
    }

    // ---- 成员 ----
    http::response<http::empty_body> header_res_;  // 响应头（async 期间需存活）
    std::optional<http::response_serializer<http::empty_body>> header_sr_; // 序列化器
    int64_t pending_next_pos_ = 0;                 // 首批 body 的下一传输位置
    std::vector<uint8_t> pending_data_;            // 首批 body 数据（header 发送期间暂存）
    std::vector<uint8_t> write_data_;              // 当前 body 写入数据（async 期间需存活）
    std::vector<net::const_buffer> buffers_;       // body 写入的 buffer 列表
};

// ========================================================================
//  接受连接
// ========================================================================

void do_accept() {
    g_acceptor->async_accept(
        [](beast::error_code ec, tcp::socket socket) {
            if (!ec) {
                std::make_shared<session>(std::move(socket))->run();
            }
            if (g_running) {
                do_accept();
            }
        });
}

} // namespace playback
} // namespace dw

// ========================================================================
//  C API 实现
// ========================================================================

// C API 位于 dw::playback 命名空间外部，需重新声明别名
namespace beast = boost::beast;
namespace net   = boost::asio;
using tcp = boost::asio::ip::tcp;

int dw_proxy_start(void) {
    if (dw::playback::g_running) {
        return dw::playback::g_port;
    }

    try {
        dw::playback::g_ioc = std::make_unique<net::io_context>();
        dw::playback::g_acceptor = std::make_unique<tcp::acceptor>(
            *dw::playback::g_ioc,
            tcp::endpoint(tcp::v4(), 0)); // 随机端口

        dw::playback::g_port =
            dw::playback::g_acceptor->local_endpoint().port();
        dw::playback::g_running = true;

        dw::playback::do_accept();

        dw::playback::g_thread = std::make_unique<std::thread>([]() {
            dw::playback::g_ioc->run();
        });

        return dw::playback::g_port;
    } catch (const std::exception&) {
        dw::playback::g_running = false;
        dw::playback::g_port = 0;
        return -1;
    }
}

void dw_proxy_stop(void) {
    dw::playback::g_running = false;

    if (dw::playback::g_acceptor) {
        beast::error_code ec;
        dw::playback::g_acceptor->close(ec);
    }

    if (dw::playback::g_ioc) {
        dw::playback::g_ioc->stop();
    }

    if (dw::playback::g_thread && dw::playback::g_thread->joinable()) {
        dw::playback::g_thread->join();
    }

    // 释放资源
    dw::playback::g_acceptor.reset();
    dw::playback::g_ioc.reset();
    dw::playback::g_thread.reset();
    dw::playback::g_port = 0;
}

const char* dw_proxy_get_url(const dw_task_key_t* key, int file_index) {
    if (!key || !key->natural_key) {
        return "";
    }
    static thread_local std::string url;
    const char *type_str =
        key->key_type == DW_KEY_TYPE_BT ? "bt" : "http";
    url = "http://127.0.0.1:" + std::to_string(dw::playback::g_port) +
          "/file?type=" + type_str +
          "&key=" + key->natural_key +
          "&file=" + std::to_string(file_index);
    return url.c_str();
}

int dw_proxy_is_running(void) {
    return dw::playback::g_running ? 1 : 0;
}
