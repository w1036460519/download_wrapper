#pragma once

#include "core/task_bt_resume.h"
#include <sqlite3.h>
#include <mutex>

namespace dw {
    class TaskBtResumeStore {
    public:
        explicit TaskBtResumeStore(sqlite3 *const &db);

        TaskBtResumeStore(const TaskBtResumeStore &) = delete;

        TaskBtResumeStore &operator=(const TaskBtResumeStore &) = delete;

        bool init_schema() const;

        bool save(const TaskBtResume &record) const;

        /// 按 info_hash 删除；记录不存在也视为成功。
        bool remove(const std::string &info_hash) const;

        bool find(const std::string &info_hash, TaskBtResume &out) const;

    private:
        sqlite3 *const &db_;
        mutable std::mutex mutex_;
    };
}
