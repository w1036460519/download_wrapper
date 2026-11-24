#pragma once

#include "core/task_file.h"

#include <sqlite3.h>

namespace dw {
    /// 由 TaskManager 的互斥锁保护，与任务和文件共用数据库连接。
    class PlaybackProgressStore {
    public:
        explicit PlaybackProgressStore(sqlite3 *const &db) : db_(db) {}

        bool init_schema() const;
        bool save(const std::string &full_path, int64_t position_ms, int64_t duration_ms) const;
        void attach(std::vector<TaskFile> &files) const;

    private:
        sqlite3 *const &db_;
    };
}
