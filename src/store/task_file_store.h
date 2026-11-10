#pragma once

#include "core/task_file.h"
#include <sqlite3.h>
#include <mutex>
#include <span>
#include <unordered_map>

namespace dw {
    // 任务文件存储，借用统一 SQLite 连接。
    class TaskFileStore {
    public:
        explicit TaskFileStore(sqlite3 *const &db) : db_(db) {}
        bool init_schema() const;
        // 按 task_id + file_index 插入或更新任务文件字段。
        bool save(const TaskFile &file) const;
        // 逐条保存，单条失败时继续处理其余文件。任一条失败则返回 false。
        bool save(const std::vector<TaskFile> &files) const;
        TaskFileStore(const TaskFileStore &) = delete;
        TaskFileStore &operator=(const TaskFileStore &) = delete;

        std::vector<TaskFile> get(const std::string &task_id) const;
        // 按完整路径查询已下载字节数和下载区间。
        std::unordered_map<std::string, std::pair<int64_t, std::string>>
        load_progress_by_paths(const std::vector<std::string> &paths) const;
        // 查询指定索引的文件，排除软删除记录。
        bool find(const std::string &task_id, int32_t file_index, TaskFile &file) const;
        // 按任务标识软删除全部关联文件。没有待删除文件也视为成功。
        bool delete_task(const std::string &task_id) const;

    private:
        bool save_files(std::span<const TaskFile> files) const;
        sqlite3 *const &db_;
        mutable std::mutex mutex_;
    };
} // namespace dw
