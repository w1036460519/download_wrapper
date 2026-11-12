#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace dw {
    struct TaskResume {
        std::string task_id;
        std::vector<uint8_t> resume_data;
    };
}
