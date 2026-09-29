/**
 * @file task_record.h
 * @brief 任务数据结构聚合头文件。
 *
 * 独立成头，供 task_manager.h 与 task_store.h 各自 include，避免二者相互包含形成循环依赖。
 */

#pragma once

#include "file_record.h"
#include "file_progress_info.h"
