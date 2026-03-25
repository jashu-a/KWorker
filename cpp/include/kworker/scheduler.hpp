#pragma once

#include "kworker/task.hpp"
#include "kworker/dag.hpp"

#include <vector>
#include <string>
#include <unordered_map>
#include <mutex>
#include <optional>

namespace kworker {

/**
 * Scheduler — The core scheduling engine.
 *
 * Maintains an in-memory set of candidate tasks and scores them
 * using the priority + deadline + age formula. Integrates with
 * DAGResolver to skip tasks whose dependencies aren't met.
 *
 * Thread-safety: All public methods are protected by a mutex,
 * safe to call from multiple Python threads.
 *
 * Design note: This scheduler runs *within* each worker process.
 * Workers compete for tasks in Redis using atomic ZPOPMIN, but
 * use this scheduler to evaluate which queues to pull from and
 * to manage local task state. The C++ engine is NOT the single
 * source of truth — Redis is. This is a local optimization layer.
 */
class Scheduler {
public:
    explicit Scheduler(size_t max_capacity = 10000);

    // Add a task to the scheduler's local pool
    void add_task(const Task& task);

    // Bulk add (e.g., when syncing from Redis)
    void add_tasks(const std::vector<Task>& tasks);

    // Get the next task to execute, considering:
    // - priority score
    // - deadline urgency
    // - dependency readiness
    // - scheduled_at_ms (don't return tasks scheduled for the future)
    // Returns std::nullopt if no task is ready
    std::optional<Task> next_task();

    // Peek at the top N tasks by score without removing them
    std::vector<Task> peek(size_t n = 10) const;

    // Mark a task as completed in DAG
    void complete_task(const std::string& task_id);

    // Mark a task as failed in DAG
    // Returns task IDs that became unrunnable due to this failure
    std::vector<std::string> fail_task(const std::string& task_id);

    // Remove a specific task
    bool remove_task(const std::string& task_id);

    // Calculate retry delay for a task
    static double calculate_retry_delay(const Task& task);

    // Score a batch of tasks and return sorted by priority
    // Useful for the Python side to display queue state
    std::vector<std::pair<std::string, double>> score_tasks() const;

    // Stats
    size_t pending_count() const;
    size_t ready_count() const;

    void clear();

private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, Task> tasks_;
    DAGResolver dag_;
    size_t max_capacity_;

    int64_t now_ms() const;
};

}  // namespace kworker
