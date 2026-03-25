#pragma once

#include <string>
#include <vector>
#include <chrono>
#include <cstdint>

namespace kworker {

enum class TaskState : uint8_t {
    PENDING = 0,
    SCHEDULED = 1,
    RUNNING = 2,
    COMPLETED = 3,
    FAILED = 4,
    RETRYING = 5,
    DEAD = 6  // moved to dead-letter queue
};

enum class TaskPriority : uint8_t {
    LOW = 1,
    NORMAL = 5,
    HIGH = 8,
    CRITICAL = 10
};

struct RetryPolicy {
    uint32_t max_attempts = 3;
    double backoff_base = 2.0;     // exponential backoff base (seconds)
    double backoff_max = 300.0;    // max backoff cap (5 minutes)
    
    // Calculate delay for a given attempt number
    // attempt 1 → 2s, attempt 2 → 4s, attempt 3 → 8s, ...
    double delay_for_attempt(uint32_t attempt) const {
        double delay = backoff_base;
        for (uint32_t i = 1; i < attempt; ++i) {
            delay *= 2.0;
        }
        return std::min(delay, backoff_max);
    }
};

struct Task {
    std::string id;
    std::string queue;
    std::string handler_name;        // registered Python function name
    std::string payload_json;        // serialized args/kwargs

    uint8_t priority = static_cast<uint8_t>(TaskPriority::NORMAL);
    TaskState state = TaskState::PENDING;

    // Timing
    int64_t created_at_ms = 0;       // unix timestamp ms
    int64_t deadline_ms = 0;         // 0 = no deadline
    int64_t scheduled_at_ms = 0;     // when to execute (for delayed tasks)
    int64_t started_at_ms = 0;
    int64_t completed_at_ms = 0;

    // Retry
    uint32_t attempt = 0;
    RetryPolicy retry_policy;

    // Dependencies — task IDs that must complete before this runs
    std::vector<std::string> depends_on;

    // Compute the scheduling score
    // Lower score = higher priority (min-heap behavior)
    // Score formula: -(priority * 1000) + deadline_urgency + age_factor
    double compute_score(int64_t now_ms) const {
        double score = 0.0;

        // Base priority: higher priority → lower (more urgent) score
        score -= static_cast<double>(priority) * 1000.0;

        // Deadline urgency: if deadline is set and approaching, decrease score
        if (deadline_ms > 0) {
            double time_until_deadline_sec = static_cast<double>(deadline_ms - now_ms) / 1000.0;
            if (time_until_deadline_sec <= 0) {
                // Past deadline — maximum urgency
                score -= 50000.0;
            } else if (time_until_deadline_sec < 60.0) {
                // Within 1 minute — very urgent
                score -= 10000.0 / time_until_deadline_sec;
            }
        }

        // Age factor: older tasks get slight priority boost to prevent starvation
        if (created_at_ms > 0) {
            double age_sec = static_cast<double>(now_ms - created_at_ms) / 1000.0;
            score -= age_sec * 0.1;  // 0.1 points per second of age
        }

        return score;
    }
};

// Comparator for the priority queue (min-heap by score)
struct TaskScoreComparator {
    int64_t now_ms;

    explicit TaskScoreComparator(int64_t now) : now_ms(now) {}

    bool operator()(const Task& a, const Task& b) const {
        // We want LOWER score to come first (more urgent)
        // std::priority_queue is a max-heap, so we invert
        return a.compute_score(now_ms) > b.compute_score(now_ms);
    }
};

}  // namespace kworker
