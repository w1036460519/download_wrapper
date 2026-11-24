/**
 * @file playback_proxy.cpp
 * @brief 边下边播本地 HTTP 代理：Boost.Beast 实现。
 *
 * 在 127.0.0.1 随机端口启动 HTTP 服务，接收播放器 Range 请求后：
 *   1. 从 TaskManager 获取文件路径、总大小和已下载区间；
 *   2. 已下载部分直接读文件返回；缺片时提高对应 BT 分片的优先级；
 *   3. 客户端断开或代理停止时结束等待。
 *
 * 响应模型：首次发送 206 响应头（含 Content-Range / Content-Length），
 * 后续仅写 body 数据（async_write raw bytes），避免重复发送头部。
 */

#include "download_wrapper/playback_proxy.h"
#include "torrent/torrent_engine.h"

#include "core/task_manager.h"
#include "internal/downloader_internal.h"
#include "utils/string_util.h"

#include <boost/asio.hpp>
#include <boost/beast.hpp>
#include <boost/url.hpp>
#include <openssl/rand.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace dw {
namespace playback {

namespace beast = boost::beast;
namespace http  = beast::http;
namespace net   = boost::asio;
using tcp = boost::asio::ip::tcp;

static std::optional<TaskFile> find_playback_file(const std::string &client_id,
                                                  const std::string &task_id,
                                                  int32_t file_index,
                                                  const std::string &path = {},
                                                  int64_t offset = -1, int64_t end = -1) {
    auto *downloader = global_downloader();
    if (!downloader) return std::nullopt;

    std::scoped_lock wrapper_lock(downloader->mutex);
    auto *manager = downloader->task_manager.get();
    if (!downloader->initialized.load() || !manager) return std::nullopt;

    std::scoped_lock task_lock(manager->get_mutex());
    const auto *task = task_id.empty() ? nullptr : manager->get_engine_task(task_id);
    if (!task_id.empty() && (!task || task->is_delete || (!client_id.empty() && task->client_id != client_id))) return std::nullopt;

    if (!path.empty() && !task) {
        TaskFile stored;
        if (!manager->get_task_file_store().find_by_full_path(path, stored)) return std::nullopt;
        task = manager->get_engine_task(stored.task_id);
        file_index = stored.file_index;
    }
    if (!task || task->is_delete) return std::nullopt;

    const auto file = std::ranges::find(task->files, file_index, &TaskFile::file_index);
    if (file == task->files.end() || file->is_delete || file->full_path.empty() ||
        (!path.empty() && file->full_path != path)) return std::nullopt;
    if (offset >= 0 && task->task_protocol == DW_PROTOCOL_TORRENT && file->total_done < file->total_size) {
        try {
            TorrentEngine::prioritize_playback(*task, *file, offset, end);
        } catch (const std::exception &error) {
            log_e(task->task_id.c_str(), "播放分片提权失败: {}", error.what());
        }
    }
    return *file;
}

/// 单次读取并发送的最大块大小（64KB）
static constexpr std::size_t kChunkSize = 65536;

/// 等待新数据到达的定时间隔（毫秒）
static constexpr int kWaitIntervalMs = 500;

/// 全局状态
static std::atomic<int>          g_port{0};
static std::unique_ptr<net::io_context>    g_ioc;
static std::unique_ptr<tcp::acceptor>      g_acceptor;
static std::unique_ptr<std::thread>        g_thread; // 播放代理后台线程：stop 时显式 join
static std::atomic<bool>         g_running{false};
static std::mutex                g_lifecycle_mutex;
static std::mutex                g_paths_mutex;
static std::unordered_map<std::string, std::string> g_paths;

// ========================================================================
//  工具函数
// ========================================================================

/// 解析 HTTP Range 头。支持 "bytes=start-end" 与 "bytes=start-"（开放区间）。
/// 解析成功返回 true。
bool parse_range_header(const std::string& value,
                        int64_t& out_start, int64_t& out_end) {
    // 期望格式：bytes=<start>-<end> 或 bytes=<start>-
    constexpr std::string_view prefix = "bytes=";
    if (value.find(prefix) != 0) return false;
    std::string_view range_spec(value.data() + prefix.size(), value.size() - prefix.size());
    auto dash = range_spec.find('-');
    if (dash == std::string_view::npos) return false;

    // 解析 start
    auto start_sv = range_spec.substr(0, dash);
    if (start_sv.empty()) return false;
    const auto [start_ptr, start_error] =
        std::from_chars(start_sv.data(), start_sv.data() + start_sv.size(), out_start);
    if (start_error != std::errc{} || start_ptr != start_sv.data() + start_sv.size() || out_start < 0)
        return false;

    // 解析 end（开放区间由 total_size 补齐）
    auto end_sv = range_spec.substr(dash + 1);
    if (end_sv.empty()) {
        out_end = -1;
    } else {
        const auto [end_ptr, end_error] =
            std::from_chars(end_sv.data(), end_sv.data() + end_sv.size(), out_end);
        if (end_error != std::errc{} || end_ptr != end_sv.data() + end_sv.size() || out_end < 0)
            return false;
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

    // ---- 请求级 state（handle_request 初始化，后续阶段只读） ----
    std::string client_id_;
    std::string natural_key_;
    int     file_index_   = 0;
    int64_t range_start_  = 0;
    int64_t range_end_    = 0;   // 含（闭合区间）；-1 在 handle_request 中已补齐
    int64_t total_size_   = -1;
    std::string file_path_;
    bool header_sent_     = false;
    bool partial_ = false;
    bool task_file_ = false;
    bool watching_disconnect_ = false;
    std::string content_type_ = "application/octet-stream";

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
        if (req_.method() != http::verb::get && req_.method() != http::verb::head) {
            send_error(http::status::method_not_allowed, "Method not allowed");
            return;
        }

        const auto params = parsed->params();
        std::optional<TaskFile> file;
        if (const auto token = params.find("token"); token != params.end()) {
            {
                std::scoped_lock lock(g_paths_mutex);
                if (const auto entry = g_paths.find(std::string((*token).value)); entry != g_paths.end()) {
                    file_path_ = entry->second;
                }
            }
            if (!file_path_.empty()) {
                file = find_playback_file({}, {}, 0, file_path_);
                if (file) {
                    natural_key_ = file->task_id;
                    file_index_ = file->file_index;
                } else {
                    std::error_code error;
                    if (std::filesystem::is_regular_file(file_path_, error)) {
                        const auto size = std::filesystem::file_size(file_path_, error);
                        if (!error && size <= static_cast<uintmax_t>(INT64_MAX)) total_size_ = size;
                    }
                }
            }
        } else {
            const auto client = params.find("client_id");
            const auto key = params.find("key");
            const auto index = params.find("file");
            const auto type = params.find("type");
            if (client == params.end() || key == params.end() || index == params.end() ||
                type == params.end() || ((*type).value != "bt" && (*type).value != "http")) {
                send_error(http::status::bad_request, "Invalid client_id/type/key/file parameter");
                return;
            }
            const std::string value((*index).value);
            const auto [ptr, error] = std::from_chars(value.data(), value.data() + value.size(), file_index_);
            if (error != std::errc{} || ptr != value.data() + value.size()) {
                send_error(http::status::bad_request, "Invalid file index");
                return;
            }
            client_id_ = (*client).value;
            natural_key_ = (*key).value;
            if (client_id_.empty() || natural_key_.empty()) {
                send_error(http::status::bad_request, "Missing client or task ID");
                return;
            }
            file = find_playback_file(client_id_, natural_key_, file_index_);
        }
        if (file) {
            task_file_ = true;
            file_path_ = file->full_path;
            total_size_ = file->total_size;
        }
        if (total_size_ <= 0) {
            send_error(http::status::not_found, "File not found");
            return;
        }
        const auto extension = utils::file_extension(file_path_);
        if (boost::beast::iequals(extension, "mp4") || boost::beast::iequals(extension, "m4v")) content_type_ = "video/mp4";
        else if (boost::beast::iequals(extension, "mov")) content_type_ = "video/quicktime";
        else if (boost::beast::iequals(extension, "webm")) content_type_ = "video/webm";
        else if (boost::beast::iequals(extension, "mkv")) content_type_ = "video/x-matroska";
        else if (boost::beast::iequals(extension, "avi")) content_type_ = "video/x-msvideo";

        const auto range = req_[http::field::range];
        partial_ = !range.empty();
        range_end_ = total_size_ - 1;
        if (partial_) {
            if (range.starts_with("bytes=-")) {
                const auto suffix = range.substr(7);
                int64_t length = 0;
                const auto [ptr, ec] = std::from_chars(suffix.data(), suffix.data() + suffix.size(), length);
                if (ec != std::errc{} || ptr != suffix.data() + suffix.size() || length <= 0) {
                    send_range_error();
                    return;
                }
                range_start_ = total_size_ - std::min(length, total_size_);
            } else if (!parse_range_header(std::string(range), range_start_, range_end_)) {
                send_range_error();
                return;
            }
            if (range_end_ < 0 || range_end_ >= total_size_) range_end_ = total_size_ - 1;
        }
        if (range_start_ < 0 || range_start_ > range_end_) {
            send_range_error();
            return;
        }
        log_i(natural_key_.c_str(), "播放 Range file={} bytes={}-{}", file_index_, range_start_, range_end_);
        if (req_.method() == http::verb::head) {
            do_write_header();
            return;
        }

        if (file && file->total_done < total_size_) {
            find_playback_file(client_id_, natural_key_, file_index_, file_path_, range_start_, range_end_);
        }
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
    // ================================================================
    void do_stream(int64_t current_pos) {
        const auto chunk_end = current_pos + std::min<int64_t>(range_end_ - current_pos, kChunkSize - 1);
        int64_t seg_end = chunk_end;
        if (task_file_) {
            const auto file = find_playback_file(client_id_, natural_key_, file_index_, file_path_);
            if (!file) {
                if (!header_sent_) send_error(http::status::not_found, "Task file not found");
                finish();
                return;
            }
            if (file->total_done < total_size_) {
                const auto segment = std::ranges::find_if(file->segments, [current_pos](const auto &item) {
                    return item.status == SegmentStatus::Downloaded &&
                           current_pos >= item.start && current_pos <= item.end;
                });
                if (segment == file->segments.end()) {
                    wait_for_data(current_pos);
                    return;
                }
                seg_end = std::min(chunk_end, segment->end);
            }
        }

        auto data = read_file_data(file_path_, current_pos, seg_end - current_pos + 1);
        if (data.empty()) {
            // 分段信息显示有数据但文件读取失败，可能是分段信息过时
            if (!header_sent_) {
                send_error(http::status::internal_server_error, "File read failed");
            }
            finish();
            return;
        }

        const int64_t next_pos = current_pos + static_cast<int64_t>(data.size());
        // ---- 首次发送：构造并发送 206 响应头，回调中再发 body ----
        if (!header_sent_) {
            header_sent_ = true;
            // 暂存首批 body 数据，header 发送成功后回调中取出
            pending_next_pos_ = next_pos;
            pending_data_     = std::move(data);
            do_write_header();
            return;
        }

        // ---- 非首次：header 已发送，直接发 body ----
        do_write_body(next_pos, std::move(data));
    }

    // ================================================================
    //  阶段 3a：发送 206 响应头（异步，成功后回调中发 body）
    // ================================================================
    void do_write_header() {
        auto self = shared_from_this();

        http::response<http::empty_body> res;
        res.version(11);
        res.result(partial_ ? http::status::partial_content : http::status::ok);
        res.set(http::field::accept_ranges, "bytes");
        res.set(http::field::content_type, content_type_);
        res.keep_alive(false);

        // Content-Range: bytes start-end/total
        const std::string total_str =
            (total_size_ > 0) ? std::to_string(total_size_) : "*";
        if (partial_) res.set(http::field::content_range,
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
                if (ec || self->req_.method() == http::verb::head) {
                    self->finish();
                    return;
                }
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
                if (ec || next_pos > self->range_end_) {
                    self->finish();
                    return;
                }

                // ---- 继续传输（情况 2：end 未命中，从 next_pos 继续） ----
                self->do_stream(next_pos);
            });
    }

    // ================================================================
    //  阶段 4：等待新数据到达
    //
    // 定时检查下载区间，同时独立监听连接关闭。每个连接只处理一个请求。
    void wait_for_data(int64_t current_pos) {
        auto self = shared_from_this();
        if (!watching_disconnect_) {
            watching_disconnect_ = true;
            stream_.socket().async_wait(tcp::socket::wait_read, [self](beast::error_code error) {
                if (error != net::error::operation_aborted) self->finish();
            });
        }
        timer_.expires_after(std::chrono::milliseconds(kWaitIntervalMs));
        timer_.async_wait([self, current_pos](beast::error_code error) {
            if (!error && self->stream_.socket().is_open()) self->do_stream(current_pos);
        });
    }

    void finish() {
        timer_.cancel();
        beast::error_code error;
        stream_.socket().close(error);
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
    std::scoped_lock lock(dw::playback::g_lifecycle_mutex);
    if (dw::playback::g_running) {
        return dw::playback::g_port;
    }

    try {
        dw::playback::g_ioc = std::make_unique<net::io_context>();
        dw::playback::g_acceptor = std::make_unique<tcp::acceptor>(
            *dw::playback::g_ioc,
            tcp::endpoint(net::ip::address_v4::loopback(), 0)); // 随机端口

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
    std::scoped_lock lock(dw::playback::g_lifecycle_mutex);
    dw::playback::g_running = false;

    if (dw::playback::g_ioc) {
        dw::playback::g_ioc->stop();
    }

    if (dw::playback::g_thread && dw::playback::g_thread->joinable()) {
        dw::playback::g_thread->join();
    }

    {
        std::scoped_lock paths_lock(dw::playback::g_paths_mutex);
        dw::playback::g_paths.clear();
    }

    // 释放资源
    dw::playback::g_acceptor.reset();
    dw::playback::g_ioc.reset();
    dw::playback::g_thread.reset();
    dw::playback::g_port = 0;
}

const char* dw_proxy_get_url(const char* client_id, dw_protocol_t protocol, const char* natural_key, int file_index) {
    if (!client_id || !natural_key) {
        return "";
    }
    static thread_local std::string url;
    const char *type_str =
        protocol == DW_PROTOCOL_TORRENT ? "bt" : "http";
    url = "http://127.0.0.1:" + std::to_string(dw::playback::g_port) +
          "/file?client_id=" + client_id +
          "&type=" + type_str +
          "&key=" + natural_key +
          "&file=" + std::to_string(file_index);
    return url.c_str();
}

const char* dw_proxy_get_path_url(const char* full_path) {
    if (!full_path || !dw::playback::g_running) return "";
    try {
        const auto path = std::filesystem::path(full_path).lexically_normal();
        if (!path.is_absolute()) return "";
        static thread_local std::string url;
        std::array<unsigned char, 16> bytes{};
        if (RAND_bytes(bytes.data(), static_cast<int>(bytes.size())) != 1) return "";
        constexpr std::string_view hex = "0123456789abcdef";
        std::string token;
        token.reserve(bytes.size() * 2);
        for (const auto byte : bytes) {
            token += hex[byte >> 4];
            token += hex[byte & 15];
        }
        std::scoped_lock lock(dw::playback::g_paths_mutex);
        if (!dw::playback::g_running) return "";
        dw::playback::g_paths.emplace(token, path.generic_string());
        url = "http://127.0.0.1:" + std::to_string(dw::playback::g_port) + "/file?token=" + token;
        return url.c_str();
    } catch (const std::exception&) {
        return "";
    }
}

int dw_proxy_is_running(void) {
    return dw::playback::g_running ? 1 : 0;
}
