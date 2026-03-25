#include "kworker/dag.hpp"

namespace kworker {

void DAGResolver::add_task(const std::string& task_id,
                           const std::vector<std::string>& depends_on) {
    // Check for cycles before adding
    if (!depends_on.empty() && would_create_cycle(task_id, depends_on)) {
        throw std::invalid_argument(
            "Adding task '" + task_id + "' would create a dependency cycle");
    }

    // Initialize the dependency set for this task
    auto& deps = dependencies_[task_id];

    for (const auto& dep_id : depends_on) {
        // Only add as a pending dependency if not already completed
        if (completed_.find(dep_id) == completed_.end()) {
            deps.insert(dep_id);
            dependents_[dep_id].insert(task_id);
        }
    }
}

void DAGResolver::mark_completed(const std::string& task_id) {
    completed_.insert(task_id);

    // Remove this task from everyone's pending dependencies
    auto it = dependents_.find(task_id);
    if (it != dependents_.end()) {
        for (const auto& dependent_id : it->second) {
            auto dep_it = dependencies_.find(dependent_id);
            if (dep_it != dependencies_.end()) {
                dep_it->second.erase(task_id);
            }
        }
    }
}

std::vector<std::string> DAGResolver::mark_failed(const std::string& task_id) {
    std::vector<std::string> unrunnable;

    // BFS to find all transitive dependents
    std::queue<std::string> to_process;
    to_process.push(task_id);
    std::unordered_set<std::string> visited;
    visited.insert(task_id);

    while (!to_process.empty()) {
        auto current = to_process.front();
        to_process.pop();

        auto it = dependents_.find(current);
        if (it != dependents_.end()) {
            for (const auto& dep : it->second) {
                if (visited.find(dep) == visited.end()) {
                    visited.insert(dep);
                    unrunnable.push_back(dep);
                    to_process.push(dep);
                }
            }
        }
    }

    return unrunnable;
}

bool DAGResolver::is_ready(const std::string& task_id) const {
    auto it = dependencies_.find(task_id);
    if (it == dependencies_.end()) {
        // No dependency record means no dependencies — it's ready
        return true;
    }
    return it->second.empty();
}

std::vector<std::string> DAGResolver::get_ready_tasks() const {
    std::vector<std::string> ready;
    for (const auto& [task_id, deps] : dependencies_) {
        if (deps.empty() && completed_.find(task_id) == completed_.end()) {
            ready.push_back(task_id);
        }
    }
    return ready;
}

std::vector<std::string> DAGResolver::get_pending_deps(
        const std::string& task_id) const {
    auto it = dependencies_.find(task_id);
    if (it == dependencies_.end()) {
        return {};
    }
    return std::vector<std::string>(it->second.begin(), it->second.end());
}

bool DAGResolver::would_create_cycle(
        const std::string& task_id,
        const std::vector<std::string>& depends_on) const {
    // If any dependency can reach task_id through existing edges,
    // adding task_id → dep would create a cycle
    for (const auto& dep_id : depends_on) {
        if (dep_id == task_id) {
            return true;  // self-loop
        }
        std::unordered_set<std::string> visited;
        // Check if task_id can reach dep_id (which would mean
        // dep_id → ... → task_id → dep_id = cycle)
        if (has_path(task_id, dep_id, visited)) {
            // Actually we need to check reverse: can dep_id reach task_id
            // through existing dependents edges
        }
    }

    // More thorough check: simulate adding the edges and run DFS
    for (const auto& dep_id : depends_on) {
        if (dep_id == task_id) return true;

        // Can we reach task_id starting from dep_id's dependents?
        // i.e., does task_id already depend (transitively) on dep_id?
        // If task_id already has a path TO dep_id, then adding
        // task_id depends_on dep_id creates a cycle
        std::unordered_set<std::string> visited;
        if (has_path(dep_id, task_id, visited)) {
            // dep_id can already reach task_id, so task_id → dep_id = cycle
            // Wait, we need the reverse logic:
            // If we add "task_id depends on dep_id", that's edge dep_id → task_id
            // A cycle exists if task_id can already reach dep_id
            // through existing edges (task_id → ... → dep_id)
        }
    }

    // Correct approach: check if task_id has any transitive path
    // to any of depends_on through existing dependency edges
    for (const auto& dep_id : depends_on) {
        if (dep_id == task_id) return true;

        // In dependency graph, edge "A depends on B" means B → A
        // We're adding "task_id depends on dep_id" = dep_id → task_id
        // Cycle if task_id → ... → dep_id already exists
        // i.e., dep_id is a transitive dependent of task_id
        std::unordered_set<std::string> visited;
        // Check: can we go from task_id to dep_id following dependents?
        std::queue<std::string> bfs;
        bfs.push(task_id);
        visited.insert(task_id);

        while (!bfs.empty()) {
            auto current = bfs.front();
            bfs.pop();

            auto it = dependents_.find(current);
            if (it != dependents_.end()) {
                for (const auto& next : it->second) {
                    if (next == dep_id) return true;
                    if (visited.find(next) == visited.end()) {
                        visited.insert(next);
                        bfs.push(next);
                    }
                }
            }
        }
    }

    return false;
}

void DAGResolver::remove_task(const std::string& task_id) {
    // Remove from dependencies
    auto dep_it = dependencies_.find(task_id);
    if (dep_it != dependencies_.end()) {
        // Remove self from dependents of each dependency
        for (const auto& dep : dep_it->second) {
            auto rev_it = dependents_.find(dep);
            if (rev_it != dependents_.end()) {
                rev_it->second.erase(task_id);
            }
        }
        dependencies_.erase(dep_it);
    }

    // Remove from dependents
    auto rev_it = dependents_.find(task_id);
    if (rev_it != dependents_.end()) {
        // Remove self from dependencies of each dependent
        for (const auto& dependent : rev_it->second) {
            auto fwd_it = dependencies_.find(dependent);
            if (fwd_it != dependencies_.end()) {
                fwd_it->second.erase(task_id);
            }
        }
        dependents_.erase(rev_it);
    }

    completed_.erase(task_id);
}

bool DAGResolver::has_path(const std::string& from, const std::string& to,
                           std::unordered_set<std::string>& visited) const {
    if (from == to) return true;
    visited.insert(from);

    auto it = dependents_.find(from);
    if (it != dependents_.end()) {
        for (const auto& next : it->second) {
            if (visited.find(next) == visited.end()) {
                if (has_path(next, to, visited)) return true;
            }
        }
    }
    return false;
}

}  // namespace kworker
