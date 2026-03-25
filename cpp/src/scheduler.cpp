#include "kworker/scheduler.hpp"

#include <algorithm>
#include <chrono>

namespace kworker {

Scheduler::Scheduler(size_t max_capacity) : max_capacity_(max_capacity) {}

void Scheduler::add_task(const Task& task) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (tasks_.size() >= max_capacity_) {
        // Evict the lowest-priority (highest score) task
        // In practice this rarely happens — workers pull from Redis
        // in small batches
        double worst_score = -1e18;
        std::string worst_id;
        int64_t now = now_ms();

        for (const auto& [id, t] : tasks_) {
            double s = t.compute_score(now);
            if (s > worst_score) {
                worst_score = s;
                worst_id = id;
            }
        }

        if (!worst_id.empty()) {
            dag_.remove_task(worst_id);
            tasks_.erase(worst_id);
        }
    }

    tasks_[task.id] = task;

    if (!task.depends_on.empty()) {
        dag_.add_task(task.id, task.depends_on);
    }
}

void Scheduler::add_tasks(const std::vector<Task>& tasks) {
    for (const auto& task : tasks) {
        add_task(task);
    }
}

std::optional<Task> Scheduler::next_task() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (tasks_.empty()) {
        return std::nullopt;
    }

    int64_t now = now_ms();
    double best_score = 1e18;
    std::string best_id;

    for (const auto& [id, task] : tasks_) {
        // Skip tasks scheduled for the future
        if (task.scheduled_at_ms > 0 && task.scheduled_at_ms > now) {
            continue;
        }

        // Skip tasks with unmet dependencies
        if (!task.depends_on.empty() && !dag_.is_ready(id)) {
            continue;
        }

        // Skip tasks not in PENDING or RETRYING state
        if (task.state != TaskState::PENDING &&
            task.state != TaskState::RETRYING) {
            continue;
        }

        double score = task.compute_score(now);
        if (score < best_score) {
            best_score = score;
            best_id = id;
        }
    }

    if (best_id.empty()) {
        return std::nullopt;
    }

    Task result = tasks_[best_id];
    result.state = TaskState::SCHEDULED;
    result.started_at_ms = now;
    tasks_.erase(best_id);
    return result;
}

std::vector<Task> Scheduler::peek(size_t n) const {
    std::lock_guard<std::mutex> lock(mutex_);

    int64_t now = now_ms();

    // Score all tasks and sort
    std::vector<std::pair<double, const Task*>> scored;
    scored.reserve(tasks_.size());

    for (const auto& [id, task] : tasks_) {
        scored.emplace_back(task.compute_score(now), &task);
    }

    // Sort by score ascending (lower = more urgent)
    std::sort(scored.begin(), scored.end(),
              [](const auto& a, const auto& b) {
                  return a.first < b.first;
              });

    std::vector<Task> result;
    size_t count = std::min(n, scored.size());
    result.reserve(count);

    for (size_t i = 0; i < count; ++i) {
        result.push_back(*scored[i].second);
    }

    return result;
}

void Scheduler::complete_task(const std::string& task_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    dag_.mark_completed(task_id);
    tasks_.erase(task_id);
}

std::vector<std::string> Scheduler::fail_task(const std::string& task_id) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = tasks_.find(task_id);
    if (it != tasks_.end()) {
        tasks_.erase(it);
    }

    auto unrunnable = dag_.mark_failed(task_id);

    // Mark all unrunnable tasks as DEAD
    for (const auto& uid : unrunnable) {
        auto uit = tasks_.find(uid);
        if (uit != tasks_.end()) {
            uit->second.state = TaskState::DEAD;
        }
    }

    return unrunnable;
}

bool Scheduler::remove_task(const std::string& task_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    dag_.remove_task(task_id);
    return tasks_.erase(task_id) > 0;
}

double Scheduler::calculate_retry_delay(const Task& task) {
    return task.retry_policy.delay_for_attempt(task.attempt);
}

std::vector<std::pair<std::string, double>> Scheduler::score_tasks() const {
    std::lock_guard<std::mutex> lock(mutex_);

    int64_t now = now_ms();
    std::vector<std::pair<std::string, double>> scores;
    scores.reserve(tasks_.size());

    for (const auto& [id, task] : tasks_) {
        scores.emplace_back(id, task.compute_score(now));
    }

    std::sort(scores.begin(), scores.end(),
              [](const auto& a, const auto& b) {
                  return a.second < b.second;
              });

    return scores;
}

size_t Scheduler::pending_count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return tasks_.size();
}

size_t Scheduler::ready_count() const {
    std::lock_guard<std::mutex> lock(mutex_);

    int64_t now = now_ms();
    size_t count = 0;

    for (const auto& [id, task] : tasks_) {
        if (task.state != TaskState::PENDING &&
            task.state != TaskState::RETRYING) continue;
        if (task.scheduled_at_ms > 0 && task.scheduled_at_ms > now) continue;
        if (!task.depends_on.empty() && !dag_.is_ready(id)) continue;
        ++count;
    }

    return count;
}

void Scheduler::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    tasks_.clear();
    dag_.clear();
}

int64_t Scheduler::now_ms() const {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
}

}  // namespace kworker
