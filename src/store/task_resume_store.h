#pragma once

#include "core/task_resume.h"
#include <sqlite3.h>
#include <mutex>

namespace dw {
    class TaskResumeStore {
    public:
        explicit TaskResumeStore(sqlite3 *const &db);

        TaskResumeStore(const TaskResumeStore &) = delete;

        TaskResumeStore &operator=(const TaskResumeStore &) = delete;

        bool init_schema() const;

        bool save(const TaskResume &record) const;

        bool find(const std::string &task_id, TaskResume &out) const;

    private:
        sqlite3 *const &db_;
        mutable std::mutex mutex_;
    };
}
