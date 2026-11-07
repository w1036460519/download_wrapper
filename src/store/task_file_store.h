#pragma once

#include "core/task_file.h"
#include <sqlite3.h>
#include <mutex>

namespace dw {
    // 任务文件存储，借用统一 SQLite 连接。
    class TaskFileStore {
    public:
        explicit TaskFileStore(sqlite3 *const &db) : db_(db) {}
        bool init_schema() const;
        // 任务表就绪后，关联任务与文件的软删除状态。
        bool init_task_triggers() const;
        // 按 task_id + file_index 插入或更新 task_file 中的完整文件 JSON。
        bool save(const std::vector<TaskFile> &files) const;
        TaskFileStore(const TaskFileStore &) = delete;
        TaskFileStore &operator=(const TaskFileStore &) = delete;

        // 新增或整体覆盖。无效数据返回 false。
        bool upsert(TaskFile file);
        std::vector<TaskFile> get(const std::string &task_id) const;
        // 查询指定索引的文件，排除软删除记录。
        bool find(const std::string &task_id, int32_t file_index, TaskFile &file) const;
        // 仅更新已有记录；参数中的键覆盖 file 中的键。
        bool update(const std::string &task_id, int32_t file_index, TaskFile file);
        // 软删除该任务所有文件记录，返回本次标记数量。
        std::size_t erase(const std::string &task_id);

    private:
        sqlite3 *const &db_;
        static bool valid(const TaskFile &file);
        mutable std::mutex mutex_;
    };
} // namespace dw
