/**
 * @file http_engine_internal.h
 * @brief HTTP 下载引擎内部头文件：多线程分片下载实现。
 *
 * 架构：
 *   - 每个任务一个 task thread（探测 + 等待分片线程）
 *   - 每个分片一个 part thread（curl_easy_perform，真正并行）
 *   - 一个 monitor thread（周期性进度推送）
 */
#pragma once

#include "download_wrapper/download_wrapper.h"

#include <curl/curl.h>

#include <atomic>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <cstdarg>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <memory>
#include <mutex>
#include <new>
#include <ranges>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

/* ===================== 跨平台文件 I/O 兼容层 ===================== */
// POSIX 提供 open/close/pwrite/ftruncate/unlink；MSVC 无这些接口，改用 io.h 系列等价物。
#if defined(_WIN32)
    #include <fcntl.h>
    #include <io.h>
    #include <share.h>
    #include <sys/stat.h>
using dw_ssize_t = long long;
#else
    #include <fcntl.h>
    #include <unistd.h>
using dw_ssize_t = ssize_t;
#endif

/// 以写入方式打开文件（不存在则创建）。返回文件描述符，失败返回 -1。
/// Windows 强制二进制模式，避免 CRLF 文本转换损坏下载数据；允许多句柄共享打开。
inline int dw_file_open_write(const char *path) {
#if defined(_WIN32)
    int fd = -1;
    _sopen_s(&fd, path, _O_CREAT | _O_WRONLY | _O_BINARY, _SH_DENYNO, _S_IREAD | _S_IWRITE);
    return fd;
#else
    return ::open(path, O_CREAT | O_WRONLY, 0644);
#endif
}

/// 关闭文件描述符。成功返回 0。
inline int dw_file_close(int fd) {
#if defined(_WIN32)
    return ::_close(fd);
#else
    return ::close(fd);
#endif
}

/// 将文件截断/预分配到指定大小。成功返回 0。
inline int dw_file_truncate(int fd, long long size) {
#if defined(_WIN32)
    return ::_chsize_s(fd, size);
#else
    return ::ftruncate(fd, static_cast<off_t>(size));
#endif
}

/// 定点写入：向 fd 的 off 偏移写 len 字节，返回实际写入字节数，失败返回 -1。
/// 调用方须保证同一 fd 不被多线程并发共享（本项目每个分片持有独立 fd）。
inline dw_ssize_t dw_file_pwrite(int fd, const void *buf, size_t len, long long off) {
#if defined(_WIN32)
    if (::_lseeki64(fd, off, SEEK_SET) < 0) return -1;
    return ::_write(fd, buf, static_cast<unsigned int>(len));
#else
    return ::pwrite(fd, buf, len, static_cast<off_t>(off));
#endif
}

/// 删除文件。成功返回 0。
inline int dw_file_unlink(const char *path) {
#if defined(_WIN32)
    return ::_unlink(path);
#else
    return ::unlink(path);
#endif
}

/* ===================== RAII 辅助封装 ===================== */

/// CURL easy handle 的 RAII 包装。
struct CurlEasyGuard {
    CURL *curl = nullptr;

    CurlEasyGuard() = default;
    explicit CurlEasyGuard(CURL *c) noexcept : curl(c) {}
    ~CurlEasyGuard() { reset(); }

    CurlEasyGuard(const CurlEasyGuard &) = delete;
    CurlEasyGuard &operator=(const CurlEasyGuard &) = delete;

    CurlEasyGuard(CurlEasyGuard &&o) noexcept : curl(o.curl) { o.curl = nullptr; }
    CurlEasyGuard &operator=(CurlEasyGuard &&o) noexcept {
        if (this != &o) { reset(); curl = o.curl; o.curl = nullptr; }
        return *this;
    }

    CURL *get() const noexcept { return curl; }
    explicit operator bool() const noexcept { return curl != nullptr; }

    CURL *release() noexcept { CURL *c = curl; curl = nullptr; return c; }

    void reset() noexcept {
        if (curl) { curl_easy_cleanup(curl); curl = nullptr; }
    }
};

/// curl_slist 链表的 RAII 包装。
struct CurlSlistGuard {
    curl_slist *list = nullptr;

    CurlSlistGuard() = default;
    explicit CurlSlistGuard(curl_slist *l) noexcept : list(l) {}
    ~CurlSlistGuard() { reset(); }

    CurlSlistGuard(const CurlSlistGuard &) = delete;
    CurlSlistGuard &operator=(const CurlSlistGuard &) = delete;

    CurlSlistGuard(CurlSlistGuard &&o) noexcept : list(o.list) { o.list = nullptr; }
    CurlSlistGuard &operator=(CurlSlistGuard &&o) noexcept {
        if (this != &o) { reset(); list = o.list; o.list = nullptr; }
        return *this;
    }

    curl_slist *get() const noexcept { return list; }
    explicit operator bool() const noexcept { return list != nullptr; }

    bool append(const char *str) noexcept {
        if (curl_slist *n = curl_slist_append(list, str)) { list = n; return true; }
        return false;
    }

    curl_slist *release() noexcept { curl_slist *l = list; list = nullptr; return l; }

    void reset() noexcept {
        if (list) { curl_slist_free_all(list); list = nullptr; }
    }
};

/* ===================== 前向声明 ===================== */
struct dl_task_ctx;

/* ===================== 分片运行时上下文 ===================== */
struct dl_part_ctx {
    dl_task_ctx *task = nullptr;
    int32_t index = 0;
    CURL *easy = nullptr;
    curl_slist *easy_hdrs = nullptr;
    int retry_count = 0;
    int64_t last_speed_sample_bytes = 0;
    int64_t last_speed_sample_ms = 0;
    int64_t seen_total_size = 0;
    std::string seen_etag;
    std::string seen_last_modified;
    long seen_http_code = 0;

    int fd = -1;   // 本分片独立文件句柄（multiple-handle：各分片自持偏移，规避跨线程共享）

    ~dl_part_ctx() {
        if (fd >= 0) { dw_file_close(fd); fd = -1; }
        if (easy_hdrs) { curl_slist_free_all(easy_hdrs); easy_hdrs = nullptr; }
        if (easy) { curl_easy_cleanup(easy); easy = nullptr; }
    }
};

/* ===================== 任务运行时上下文 ===================== */
struct dl_task_ctx {
    std::string url;
    std::string trace_id;
    std::string output_path;
    std::string filename;
    std::string full_file_path;
    int64_t total_size = -1;
    int64_t start_time_ms = 0;

    dw_task_status_t status = DW_TASK_STATUS_DOWNLOADING;

    int32_t support_range = 0;
    std::string etag;
    std::string last_modified;

    dw_reason_t reason = DW_REASON_NONE;

    std::string message;

    std::vector<dw_part_state_t> parts;
    std::vector<dl_part_ctx> part_ctx;

    int fd = -1;
    std::atomic<int> pause_req{0};
    std::atomic<int> cancel_req{0};
    std::atomic<int> thread_done{0};   // 任务线程是否已结束（含终态推送完成）；sweep 据此安全回收
    int probing = 1;
    int cleanup_done = 0;
    std::mutex speed_mtx;
    std::thread task_thread;

    ~dl_task_ctx() {
        if (fd >= 0) { dw_file_close(fd); fd = -1; }
    }
};

/* ===================== 全局变量（dw::http_engine namespace） ===================== */
namespace dw { namespace http_engine {

extern dw_config_t g_cfg;
extern std::mutex g_map_mtx;
extern std::unordered_map<std::string, std::unique_ptr<dl_task_ctx>> g_tasks;
extern std::thread g_monitor_thread;
extern std::atomic<bool> g_exit_flag;
extern std::atomic<bool> g_running;
extern dw_progress_cb g_progress_cb;
extern dw_log_cb g_log_cb;

}} /* namespace dw::http_engine */

/* ===================== 内部函数声明 ===================== */
namespace dw { namespace http_engine { namespace internal {

/** 输出日志：优先走回调，无回调时 fallback 到 stderr。 */
void dl_emit_log(const char *trace_id, const char *msg,
                 const char *file, int line, const char *func, int lvl);

/** 格式化日志：lvl < cfg.log_level 时自动过滤。 */
void dl_logf(int lvl, const char *trace_id,
             const char *file, int line, const char *func,
             const char *fmt, ...);

/** 将任务上下文填充到 dw_progress_t（不持锁，调用方需按需加锁）。 */
void fill_progress(dl_task_ctx *tCtx, dw_progress_t *task_progress);

/** 构造 dw_progress_t 并推送到用户回调（线程安全）。 */
void push_progress(dl_task_ctx *tCtx);

/** libcurl header 回调 */
size_t header_cb(const char *buffer, size_t size, size_t n_items, void *userdata);

/** libcurl write 回调 */
size_t write_cb(const char *ptr, size_t size, size_t n_member, void *userdata);

/** libcurl 进度回调 */
int part_progress_cb(void *userdata, curl_off_t dl_total, curl_off_t dl_now,
                curl_off_t ul_total, curl_off_t ul_now);

/** 为单个分片构建 CURL easy handle */
CURL *build_easy_for_part(dl_task_ctx *tCtx, dl_part_ctx *pCtx);

/** 根据 CURLcode + HTTP status 分类失败原因 */
dw_reason_t classify_failure(CURLcode rc, long http_code, int *retryable);

/** 聚合所有分片状态，确定任务终态 */
void aggregate_status(dl_task_ctx *tCtx);

/** 从 URL 路径末段提取文件名 */
std::string extract_filename_from_url(std::string_view url);

/** 在 dir 下拼接 filename，若已存在则追加 (n) 后缀去重 */
std::string resolve_unique_path(const std::string &dir, const std::string &filename);

/** 递归创建目录 */
bool mkdir_recursive(const std::string &path);

/** 忽略大小写比较 */
bool equal_ignore_case(std::string_view a, std::string_view b) noexcept;

/** 去除首尾空白字符 */
std::string_view trim_view(std::string_view s) noexcept;

/** 将字符串视图解析为整数 */
template<typename T> bool sv_to_int(std::string_view sv, T &out) noexcept;

/** 解析 Content-Disposition 中的 filename / filename* 值 */
std::string parse_content_disposition_filename(std::string_view value);

/** 探测完成后冻结元数据字段 */
void finalize_probing(dl_task_ctx *tCtx, const dl_part_ctx *pCtx);

/** 单个分片一次请求完成后的结果判定，返回 true 表示需重试 */
bool handle_part_result(dl_task_ctx *tCtx, dl_part_ctx *pCtx, CURLcode rc, long http_code);

/** 每任务一个 curl_multi 事件循环，驱动本任务所有分片下载 */
void run_parts_multi(dl_task_ctx *tCtx);

/** 任务工作线程 */
void task_thread_func(dl_task_ctx *tCtx);

/** 全局 monitor 线程 */
void monitor_thread_func();

/** 启动任务 */
void start_task(dl_task_ctx *tCtx);

/** 创建新任务上下文 */
std::unique_ptr<dl_task_ctx> task_create_new(const char *url, const char *output_path,
                                             const char *trace_id, const char *filename);

/** 校验添加输入参数 */
int validate_add_input(const char *url, const char *output_path, const char **err_out);

/** 设置提交结果 */
void set_result(dw_submit_result_t *r, const char *task_id, const char *trace_id,
                dw_reason_t code, const char *msg, const char *fmt, ...);

}}} /* namespace dw::http_engine::internal */

#define HTTP_LOG(lvl, trace, fmt, ...) \
    dw::http_engine::internal::dl_logf((lvl), (trace), __FILE__, __LINE__, __FUNCTION__, fmt, ##__VA_ARGS__)
