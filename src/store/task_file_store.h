#pragma once

#include "core/task_file.h"
#include <map>
#include <mutex>
#include <unordered_map>

namespace dw {
    // 线程安全的内存缓存；查询返回副本，按 file_index 升序排列。
    class TaskFileStore {
    public:
        TaskFileStore() = default;
        TaskFileStore(const TaskFileStore &) = delete;
        TaskFileStore &operator=(const TaskFileStore &) = delete;

        // 新增或整体覆盖。无效数据返回 false。
        bool upsert(TaskFile file);
        std::vector<TaskFile> get(const std::string &info_hash) const;
        // 仅更新已有记录；参数中的键覆盖 file 中的键。
        bool update(const std::string &info_hash, int32_t file_index, TaskFile file);
        // 删除该任务所有文件，返回删除数量。
        std::size_t erase(const std::string &info_hash);

    private:
        static bool valid(const TaskFile &file);
        mutable std::mutex mutex_;
        std::unordered_map<std::string, std::map<int32_t, TaskFile>> files_;
    };
} // namespace dw
