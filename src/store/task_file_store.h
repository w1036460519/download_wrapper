#pragma once

#include "core/task_file.h"
#include <sqlite3.h>
#include <mutex>
#include <span>

namespace dw {
    // 任务文件存储，借用统一 SQLite 连接。
    class TaskFileStore {
    public:
        explicit TaskFileStore(sqlite3 *const &db);
        bool init_schema() const;
        // 按 task_id + file_index 插入或更新任务文件字段。
        bool save(const TaskFile &file) const;
        TaskFileStore(const TaskFileStore &) = delete;
        TaskFileStore &operator=(const TaskFileStore &) = delete;

        std::vector<TaskFile> get(const std::string &task_id) const;
        // 按完整路径查询下载进度最高的一条未删除文件记录。
        bool find_by_path(const std::string &full_path, TaskFile &file) const;
        // 按任务标识软删除全部关联文件。没有待删除文件也视为成功。
        bool delete_task(const std::string &task_id) const;

    private:
        bool save_files(std::span<const TaskFile> files) const;
        sqlite3 *const &db_;
        mutable std::mutex mutex_;
    };
} // namespace dw
