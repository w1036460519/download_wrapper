#pragma once

#include "core/task.h"
#include <sqlite3.h>
#include <mutex>

namespace dw {
    /// 处理 tasks 表；借用统一连接，不管理连接生命周期。
    class TaskStore {
    public:
        explicit TaskStore(sqlite3 *const &db);

        TaskStore(const TaskStore &) = delete;

        TaskStore &operator=(const TaskStore &) = delete;

        bool init_schema() const;

        bool save(const Task &task) const;

        bool find_by_task_id(const std::string &task_id, Task &out) const;

        /// 查询所有未删除的任务。
        std::vector<Task> find_all() const;

    private:
        bool write_task(const Task &task) const;

        sqlite3 *const &db_;
        mutable std::mutex mutex_;
    };

} // namespace dw
