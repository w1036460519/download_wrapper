/**
 * @file http_engine_internal.h
 * @brief HTTP 下载引擎内部头文件：多线程分片下载实现。
 *
 * 架构：
 *   - 每个任务一个 task thread：单线程 curl_multi 事件循环驱动本任务所有分片；
 *     探测即下载——首个连接（探测窗口 Range: 0-n）就是 part 0 的正式下载流，
 *     首笔 body 到达时定名建文件切分片，其余分片动态挂入同一 multi。
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

/* ===================== 文件定点写封装（std::fstream 全平台统一） ===================== */
// std::fstream 消除 POSIX/MSVC 平台分支；seekp+write 是两步操作，由内部互斥锁
// 合成原子复合操作（当前 write_cb 在任务 worker 线程内串行执行，锁为并发预防冗余）。
// 锁经 unique_ptr 持有以保持类型可移动（dl_part_ctx 存放于 vector）。
#include <fstream>

class DwFile {
public:
    /// 以二进制写入方式打开（保留既有内容，不截断）。
    /// @param create true 时不存在则新建；false 时仅打开既有文件（文件被外部
    ///               删除时失败，供写入路径传递错误，不静默重建空洞文件）。
    /// @return 是否打开成功。
    bool open(const std::string &path, const bool create = true) {
        std::lock_guard<std::mutex> lk(*mtx_);
        if (f_.is_open()) f_.close();
        f_.clear();
        // in|out 等价 "r+b"：要求文件存在且不截断既有字节。
        f_.open(path, std::ios::binary | std::ios::in | std::ios::out);
        if (!f_.is_open() && create) {
            // 不存在则新建（新文件本为空，无截断顾虑）。
            f_.clear();
            f_.open(path, std::ios::binary | std::ios::out);
        }
        return f_.is_open();
    }

    bool is_open() const { return f_.is_open(); }

    /// 关闭文件（幂等）。析构自动关闭，此接口供流程中的主动关闭点使用。
    void close() {
        std::lock_guard<std::mutex> lk(*mtx_);
        if (f_.is_open()) f_.close();
    }

    /// 定点写：向 off 偏移写入 len 字节并 flush 到内核页缓存。
    /// flush 是 resume 保守不变量（done ≤ 磁盘实际字节）的前提：不冲刷则数据
    /// 滞留用户态流缓冲，进程异常终止即丢失。
    /// @return 0 成功；失败返回失败点捕获的 errno（如 ENOSPC 磁盘满），
    ///         未打开返回 EBADF，底层未设置 errno 时兜底 EIO。
    int pwrite_at(const void *buf, const size_t len, const long long off) {
        std::lock_guard<std::mutex> lk(*mtx_);
        if (!f_.is_open()) return EBADF;
        errno = 0;
        f_.clear();
        f_.seekp(static_cast<std::streamoff>(off));
        f_.write(static_cast<const char *>(buf), static_cast<std::streamsize>(len));
        f_.flush();
        if (f_.good()) return 0;
        return errno != 0 ? errno : EIO;
    }

    /// 将文件截断/预分配到指定大小（std::filesystem 按路径操作，需已 open 同一
    /// 路径；先冲刷流缓冲再调整，避免缓冲滞后覆盖新尺寸）。成功返回 true。
    bool truncate(const std::string &path, const int64_t size) {
        std::lock_guard<std::mutex> lk(*mtx_);
        if (f_.is_open()) f_.flush();
        std::error_code ec;
        std::filesystem::resize_file(path, static_cast<std::uintmax_t>(size), ec);
        return !ec;
    }

private:
    std::fstream f_;
    std::unique_ptr<std::mutex> mtx_ = std::make_unique<std::mutex>();
};

/* ===================== 分片状态（HTTP 分片下载专用） ===================== */

/**
 * 单个分片状态。
 *
 * 运行状态复用 dw_task_status_t 与 dw_reason_t。
 * 仅 HTTP 引擎内部（分片续传态）使用，不随进度回调导出；BT 任务无分片。
 */
typedef struct dw_part_state {
    int32_t index; /**< 分片序号（0 起）。 */
    int64_t start; /**< 分片在目标文件中的起始字节（含）。 */
    int64_t end; /**< 分片结束字节（含）。 */
    int64_t size; /**< 分片总字节 = end - start + 1。 */
    int64_t done; /**< 本分片已下载字节。 */
    double progress; /**< 本分片进度 0.0-1.0；size=0 时为 -1。 */
    double download_rate; /**< 分片实时速率（B/s）。 */
    dw_task_status_t status; /**< 分片运行状态。 */
    dw_reason_t reason; /**< 仅 status==ERROR 时有效。 */
} dw_part_state_t;

/* ===================== RAII 辅助封装 ===================== */

/// CURL easy handle 的 RAII 包装。
struct CurlEasyGuard {
    CURL *curl = nullptr;

    CurlEasyGuard() = default;

    explicit CurlEasyGuard(CURL *c) noexcept : curl(c) {
    }

    ~CurlEasyGuard() { reset(); }

    CurlEasyGuard(const CurlEasyGuard &) = delete;

    CurlEasyGuard &operator=(const CurlEasyGuard &) = delete;

    CurlEasyGuard(CurlEasyGuard &&o) noexcept : curl(o.curl) { o.curl = nullptr; }

    CurlEasyGuard &operator=(CurlEasyGuard &&o) noexcept {
        if (this != &o) {
            reset();
            curl = o.curl;
            o.curl = nullptr;
        }
        return *this;
    }

    CURL *get() const noexcept { return curl; }
    explicit operator bool() const noexcept { return curl != nullptr; }

    CURL *release() noexcept {
        CURL *c = curl;
        curl = nullptr;
        return c;
    }

    void reset() noexcept {
        if (curl) {
            curl_easy_cleanup(curl);
            curl = nullptr;
        }
    }
};

/// curl_slist 链表的 RAII 包装。
struct CurlSlistGuard {
    curl_slist *list = nullptr;

    CurlSlistGuard() = default;

    explicit CurlSlistGuard(curl_slist *l) noexcept : list(l) {
    }

    ~CurlSlistGuard() { reset(); }

    CurlSlistGuard(const CurlSlistGuard &) = delete;

    CurlSlistGuard &operator=(const CurlSlistGuard &) = delete;

    CurlSlistGuard(CurlSlistGuard &&o) noexcept : list(o.list) { o.list = nullptr; }

    CurlSlistGuard &operator=(CurlSlistGuard &&o) noexcept {
        if (this != &o) {
            reset();
            list = o.list;
            o.list = nullptr;
        }
        return *this;
    }

    curl_slist *get() const noexcept { return list; }
    explicit operator bool() const noexcept { return list != nullptr; }

    bool append(const char *str) noexcept {
        if (curl_slist *n = curl_slist_append(list, str)) {
            list = n;
            return true;
        }
        return false;
    }

    curl_slist *release() noexcept {
        curl_slist *l = list;
        list = nullptr;
        return l;
    }

    void reset() noexcept {
        if (list) {
            curl_slist_free_all(list);
            list = nullptr;
        }
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
    int probe_window = 0; // 本连接为探测窗口请求（Range: 0-n）：收满窗口正常断开后续接剩余区间，不计入重试
    int64_t last_speed_sample_bytes = 0;
    int64_t last_speed_sample_ms = 0;
    int64_t seen_total_size = 0;
    std::string seen_etag;
    std::string seen_last_modified;
    long seen_http_code = 0;

    DwFile file; // 本分片独立文件对象（multiple-handle：各分片自持写偏移与内部锁）

    // 自定义析构抑制隐式移动，且 DwFile（含 fstream）不可拷贝：显式补移动语义供
    // vector 扩容/push_back 使用；移动后置空源指针，防止 easy/easy_hdrs 双重释放。
    dl_part_ctx() = default;

    dl_part_ctx(dl_part_ctx &&o) noexcept
        : task(o.task), index(o.index), easy(o.easy), easy_hdrs(o.easy_hdrs),
          retry_count(o.retry_count), probe_window(o.probe_window),
          last_speed_sample_bytes(o.last_speed_sample_bytes),
          last_speed_sample_ms(o.last_speed_sample_ms),
          seen_total_size(o.seen_total_size), seen_etag(std::move(o.seen_etag)),
          seen_last_modified(std::move(o.seen_last_modified)),
          seen_http_code(o.seen_http_code), file(std::move(o.file)) {
        o.easy = nullptr;
        o.easy_hdrs = nullptr;
    }

    dl_part_ctx &operator=(dl_part_ctx &&) = delete;

    dl_part_ctx(const dl_part_ctx &) = delete;

    dl_part_ctx &operator=(const dl_part_ctx &) = delete;

    // file 成员析构自动关闭（fstream RAII）；moved-from 对象的内部锁已被移走，
    // 不得在此调用加锁接口。
    ~dl_part_ctx() {
        if (easy_hdrs) {
            curl_slist_free_all(easy_hdrs);
            easy_hdrs = nullptr;
        }
        if (easy) {
            curl_easy_cleanup(easy);
            easy = nullptr;
        }
    }
};

/* ===================== 任务运行时上下文 ===================== */
struct dl_task_ctx {
    std::string url;
    std::string output_path; // 落盘目录（= 上层传入的 save_path，开始即定名，无临时目录）
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

    std::atomic<int> pause_req{0};
    std::atomic<int> cancel_req{0};
    std::atomic<int> delete_req{0}; // 删除请求：与 cancel_req 一并置位，sweep 回收 ctx（关句柄）后据此删除落盘文件
    std::atomic<int> thread_done{0}; // 任务线程是否已结束；sweep 据此安全回收
    std::atomic<int> terminal_reported{0}; // 终态已被 query_progress 采集至少一次；sweep 仅在置位后回收，避免抢在采集前回收导致任务卡在下载中
    int probing = 1;
    int is_resume = 0; // 恢复任务标志：携续传存档启动即置 1（不论存档是否有效）；finalize_probing 据此沿用历史凭证名（不再判重），首次下载走优先级链判重定名
    int64_t last_emit_done = -1; // 上次 emit resume_data 时的总已下载字节（续传上报去重基准：仅总量推进时才 emit）。
    std::mutex speed_mtx;
    std::thread task_thread;
};

/* ===================== 全局变量（dw::http_engine namespace） ===================== */
namespace dw {
    namespace http_engine {
        extern dw_config_t g_cfg;
        extern std::mutex g_map_mtx;
        extern std::unordered_map<std::string, std::unique_ptr<dl_task_ctx> > g_tasks;
        extern std::atomic<bool> g_exit_flag;
        extern std::atomic<bool> g_running;
    }
} /* namespace dw::http_engine */

/* ===================== 内部函数声明 ===================== */
namespace dw {
    namespace http_engine {
        namespace internal {
            /** 将任务上下文填充到 dw_progress_t（不持锁，调用方需按需加锁）。 */
            void fill_progress(dl_task_ctx *tCtx, dw_progress_t *task_progress);

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

            /** 从 URL 提取文件名：query 参数 filename > query 参数 name > 路径末段（均 percent-decode） */
            std::string extract_filename_from_url(std::string_view url);

            /** 递归创建目录 */
            bool mkdir_recursive(const std::string &path);

            /** 忽略大小写比较 */
            bool equal_ignore_case(std::string_view a, std::string_view b) noexcept;

            /** 去除首尾空白字符 */
            std::string_view trim_view(std::string_view s) noexcept;

            /** 将字符串视图解析为整数 */
            template<typename T>
            bool sv_to_int(std::string_view sv, T &out) noexcept;

            /** 解析 Content-Disposition 中的 filename / filename* 值 */
            std::string parse_content_disposition_filename(std::string_view value);

            /** 探测完成后冻结元数据字段 */
            void finalize_probing(dl_task_ctx *tCtx, const dl_part_ctx *pCtx);

            /** HTTP 续传数据（引擎自持序列化，落 resume_data 表；反序列化回灌 dl_task_ctx）。 */
            struct HttpResumeData {
                bool ok = false;
                int64_t total_size = -1;
                int32_t support_range = 0;
                std::string etag;
                std::string last_modified;
                std::string full_file_path; // 落盘物理完整路径（即最终路径），续传据此复用同一文件
                std::vector<dw_part_state_t> parts;
            };

            /** 将任务续传态序列化为 bencode BLOB（元数据键值 + parts 列表，复用 libtorrent 编解码）。 */
            std::string serialize_resume(dl_task_ctx *tCtx);

            /** 反序列化续传 BLOB；解析失败时 ok=false，调用方回退全量重下。 */
            HttpResumeData deserialize_resume(const uint8_t *data, size_t size);

            /** 序列化当前续传态并经全局下载器回灌 resume_data 表（以 url 为 key）。 */
            void emit_resume(dl_task_ctx *tCtx);

            /** 单个分片一次请求完成后的结果判定，返回 true 表示需重试 */
            bool handle_part_result(dl_task_ctx *tCtx, dl_part_ctx *pCtx, CURLcode rc, long http_code);

            /** 每任务一个 curl_multi 事件循环，驱动本任务所有分片下载 */
            void run_parts_multi(dl_task_ctx *tCtx);

            /** 任务工作线程 */
            void task_thread_func(dl_task_ctx *tCtx);

            /** 启动任务 */
            void start_task(dl_task_ctx *tCtx);

            /** 创建新任务上下文 */
            std::unique_ptr<dl_task_ctx> task_create_new(const char *url, const char *output_path,
                                                         const char *filename);

            /** 校验添加输入参数 */
            int validate_add_input(const char *url, const char *output_path, const char **err_out);

            /** 设置提交结果 */
            void set_result(dw_submit_result_t *r, const char *task_id,
                            dw_reason_t code, const char *msg, const char *fmt, ...);
        }
    }
} /* namespace dw::http_engine::internal */
