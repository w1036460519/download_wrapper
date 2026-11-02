#include "task_file_store.h"

namespace dw {
    bool TaskFileStore::valid(const TaskFile &file) {
        if (file.info_hash.empty() || file.file_index < 0 || file.total_size < 0
            || file.total_done < 0 || file.total_done > file.total_size)
            return false;
        if (file.prioritize != 0 && file.prioritize != 1
            && file.prioritize != 4 && file.prioritize != 7)
            return false;
        int64_t previous_end = -1;
        for (const auto &segment : file.segments) {
            if (segment.start < 0 || segment.start <= previous_end
                || segment.end < segment.start || segment.end >= file.total_size)
                return false;
            previous_end = segment.end;
        }
        return true;
    }

    bool TaskFileStore::upsert(TaskFile file) {
        if (!valid(file)) return false;
        const auto hash = file.info_hash;
        const auto index = file.file_index;
        std::scoped_lock lock(mutex_);
        files_[hash].insert_or_assign(index, std::move(file));
        return true;
    }

    std::vector<TaskFile> TaskFileStore::get(const std::string &info_hash) const {
        std::scoped_lock lock(mutex_);
        const auto task = files_.find(info_hash);
        if (task == files_.end()) return {};
        std::vector<TaskFile> result;
        result.reserve(task->second.size());
        for (const auto &[index, file] : task->second) result.push_back(file);
        return result;
    }

    bool TaskFileStore::update(const std::string &info_hash,
                               int32_t file_index, TaskFile file) {
        file.info_hash = info_hash;
        file.file_index = file_index;
        if (!valid(file)) return false;
        std::scoped_lock lock(mutex_);
        const auto task = files_.find(info_hash);
        if (task == files_.end()) return false;
        const auto existing = task->second.find(file_index);
        if (existing == task->second.end()) return false;
        existing->second = std::move(file);
        return true;
    }

    std::size_t TaskFileStore::erase(const std::string &info_hash) {
        std::scoped_lock lock(mutex_);
        const auto task = files_.find(info_hash);
        if (task == files_.end()) return 0;
        const auto count = task->second.size();
        files_.erase(task);
        return count;
    }
} // namespace dw
