#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace dw {
    struct TaskBtResume {
        std::string info_hash;
        std::vector<uint8_t> resume_data;
    };
}
