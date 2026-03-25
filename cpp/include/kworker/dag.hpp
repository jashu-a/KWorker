#pragma once

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <queue>
#include <stdexcept>

namespace kworker {

/**
 * DAGResolver — Manages task dependency graphs.
 *
 * When task C depends on tasks A and B, C cannot be scheduled
 * until both A and B reach COMPLETED state. The DAG also detects
 * circular dependencies at insertion time.
 *
 * Thread-safety: NOT thread-safe. Callers must synchronize externally.
 * In practice, each worker holds its own DAGResolver instance and
 * rebuilds it from Redis state on startup.
 */
class DAGResolver {
public:
    // Register a task with its dependencies
    // Throws std::invalid_argument if adding this edge creates a cycle
    void add_task(const std::string& task_id,
                  const std::vector<std::string>& depends_on);

    // Mark a task as completed — removes it from blocking others
    void mark_completed(const std::string& task_id);

    // Mark a task as failed — propagates failure to all dependents
    // Returns list of task IDs that are now unrunnable
    std::vector<std::string> mark_failed(const std::string& task_id);

    // Check if a task's dependencies are all satisfied
    bool is_ready(const std::string& task_id) const;

    // Get all tasks that are currently ready to run
    std::vector<std::string> get_ready_tasks() const;

    // Get pending (unsatisfied) dependencies for a task
    std::vector<std::string> get_pending_deps(const std::string& task_id) const;

    // Check if adding this task would create a cycle
    bool would_create_cycle(const std::string& task_id,
                            const std::vector<std::string>& depends_on) const;

    // Remove a task and all its edges (cleanup)
    void remove_task(const std::string& task_id);

    // Number of tracked tasks
    size_t size() const { return dependencies_.size(); }

    void clear() {
        dependencies_.clear();
        dependents_.clear();
        completed_.clear();
    }

private:
    // task_id → set of task_ids it depends on (that are NOT yet completed)
    std::unordered_map<std::string, std::unordered_set<std::string>> dependencies_;

    // task_id → set of task_ids that depend on IT (reverse edges)
    std::unordered_map<std::string, std::unordered_set<std::string>> dependents_;

    // Set of completed task IDs
    std::unordered_set<std::string> completed_;

    // DFS-based cycle detection
    bool has_path(const std::string& from, const std::string& to,
                  std::unordered_set<std::string>& visited) const;
};

}  // namespace kworker
