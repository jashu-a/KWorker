#include "kworker/scheduler.hpp"
#include "kworker/dag.hpp"

#include <cassert>
#include <iostream>
#include <chrono>
#include <thread>

using namespace kworker;

int64_t now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
}

Task make_task(const std::string& id, uint8_t priority = 5,
               int64_t deadline_ms = 0,
               const std::vector<std::string>& deps = {}) {
    Task t;
    t.id = id;
    t.queue = "default";
    t.handler_name = "test_handler";
    t.payload_json = "{}";
    t.priority = priority;
    t.state = TaskState::PENDING;
    t.created_at_ms = now_ms();
    t.deadline_ms = deadline_ms;
    t.depends_on = deps;
    return t;
}

// ── DAG Tests ──

void test_dag_basic_dependency() {
    DAGResolver dag;
    dag.add_task("B", {"A"});

    assert(!dag.is_ready("B") && "B should not be ready — depends on A");
    
    dag.mark_completed("A");
    assert(dag.is_ready("B") && "B should be ready after A completes");

    std::cout << "  PASS: test_dag_basic_dependency\n";
}

void test_dag_diamond_dependency() {
    // A → B, A → C, B+C → D
    DAGResolver dag;
    dag.add_task("B", {"A"});
    dag.add_task("C", {"A"});
    dag.add_task("D", {"B", "C"});

    assert(!dag.is_ready("D"));

    dag.mark_completed("A");
    assert(dag.is_ready("B"));
    assert(dag.is_ready("C"));
    assert(!dag.is_ready("D") && "D needs both B and C");

    dag.mark_completed("B");
    assert(!dag.is_ready("D") && "D still needs C");

    dag.mark_completed("C");
    assert(dag.is_ready("D") && "D should be ready now");

    std::cout << "  PASS: test_dag_diamond_dependency\n";
}

void test_dag_cycle_detection() {
    DAGResolver dag;
    dag.add_task("B", {"A"});
    dag.add_task("C", {"B"});

    // Adding A depends on C would create A → B → C → A
    bool caught = false;
    try {
        dag.add_task("A", {"C"});
    } catch (const std::invalid_argument&) {
        caught = true;
    }
    assert(caught && "Should detect cycle");

    std::cout << "  PASS: test_dag_cycle_detection\n";
}

void test_dag_failure_propagation() {
    DAGResolver dag;
    dag.add_task("B", {"A"});
    dag.add_task("C", {"B"});
    dag.add_task("D", {"B"});

    auto unrunnable = dag.mark_failed("A");
    // B, C, D should all be unrunnable
    assert(unrunnable.size() >= 1 && "At least B should be unrunnable");

    std::cout << "  PASS: test_dag_failure_propagation\n";
}

// ── Scheduler Tests ──

void test_scheduler_priority_ordering() {
    Scheduler sched;

    sched.add_task(make_task("low", 1));       // low priority
    sched.add_task(make_task("normal", 5));    // normal
    sched.add_task(make_task("critical", 10)); // critical

    auto next = sched.next_task();
    assert(next.has_value());
    assert(next->id == "critical" && "Critical priority task should come first");

    next = sched.next_task();
    assert(next.has_value());
    assert(next->id == "normal" && "Normal should come second");

    next = sched.next_task();
    assert(next.has_value());
    assert(next->id == "low" && "Low should come last");

    std::cout << "  PASS: test_scheduler_priority_ordering\n";
}

void test_scheduler_deadline_urgency() {
    Scheduler sched;

    int64_t now = now_ms();

    // Low priority but imminent deadline
    auto urgent = make_task("urgent", 1);
    urgent.deadline_ms = now + 100;  // 100ms from now

    // High priority but no deadline
    auto relaxed = make_task("relaxed", 8);

    sched.add_task(urgent);
    sched.add_task(relaxed);

    // Wait a moment for deadline to become very close
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    auto next = sched.next_task();
    assert(next.has_value());
    // The urgent task with imminent deadline should beat the relaxed one
    // (deadline urgency bonus outweighs base priority)
    assert(next->id == "urgent" && "Deadline urgency should override base priority");

    std::cout << "  PASS: test_scheduler_deadline_urgency\n";
}

void test_scheduler_respects_dependencies() {
    Scheduler sched;

    auto a = make_task("A", 5);
    auto b = make_task("B", 10, 0, {"A"});  // B depends on A, higher priority

    sched.add_task(a);
    sched.add_task(b);

    // B has higher priority but depends on A
    auto next = sched.next_task();
    assert(next.has_value());
    assert(next->id == "A" && "A should come first despite B having higher priority");

    // Mark A complete so B can run
    sched.complete_task("A");

    next = sched.next_task();
    assert(next.has_value());
    assert(next->id == "B" && "B should be available after A completes");

    std::cout << "  PASS: test_scheduler_respects_dependencies\n";
}

void test_scheduler_delayed_tasks() {
    Scheduler sched;

    int64_t now = now_ms();

    auto delayed = make_task("delayed", 10);
    delayed.scheduled_at_ms = now + 5000;  // 5 seconds from now

    auto ready = make_task("ready", 1);  // lower priority but available now

    sched.add_task(delayed);
    sched.add_task(ready);

    auto next = sched.next_task();
    assert(next.has_value());
    assert(next->id == "ready" && "Delayed task should not be returned yet");

    std::cout << "  PASS: test_scheduler_delayed_tasks\n";
}

void test_scheduler_retry_delay() {
    Task t = make_task("retry_test");
    t.retry_policy.max_attempts = 5;
    t.retry_policy.backoff_base = 2.0;
    t.retry_policy.backoff_max = 60.0;

    t.attempt = 1;
    assert(Scheduler::calculate_retry_delay(t) == 2.0);

    t.attempt = 2;
    assert(Scheduler::calculate_retry_delay(t) == 4.0);

    t.attempt = 3;
    assert(Scheduler::calculate_retry_delay(t) == 8.0);

    // Should cap at backoff_max
    t.attempt = 10;
    assert(Scheduler::calculate_retry_delay(t) == 60.0);

    std::cout << "  PASS: test_scheduler_retry_delay\n";
}

void test_scheduler_empty() {
    Scheduler sched;
    auto next = sched.next_task();
    assert(!next.has_value() && "Empty scheduler should return nullopt");

    std::cout << "  PASS: test_scheduler_empty\n";
}

// ── Benchmark ──

void bench_scheduler_throughput() {
    Scheduler sched;
    int64_t now = now_ms();

    const int N = 10000;

    // Insert N tasks
    for (int i = 0; i < N; ++i) {
        auto t = make_task("task_" + std::to_string(i),
                           static_cast<uint8_t>(1 + (i % 10)));
        sched.add_task(t);
    }

    // Measure dequeue throughput
    auto start = std::chrono::high_resolution_clock::now();

    int dequeued = 0;
    while (auto t = sched.next_task()) {
        ++dequeued;
    }

    auto end = std::chrono::high_resolution_clock::now();
    double elapsed_ms = std::chrono::duration<double, std::milli>(end - start).count();
    double tasks_per_sec = (dequeued / elapsed_ms) * 1000.0;

    std::cout << "  BENCH: Dequeued " << dequeued << " tasks in "
              << elapsed_ms << "ms (" << static_cast<int>(tasks_per_sec)
              << " tasks/sec)\n";
}

int main() {
    std::cout << "=== DAG Tests ===\n";
    test_dag_basic_dependency();
    test_dag_diamond_dependency();
    test_dag_cycle_detection();
    test_dag_failure_propagation();

    std::cout << "\n=== Scheduler Tests ===\n";
    test_scheduler_priority_ordering();
    test_scheduler_deadline_urgency();
    test_scheduler_respects_dependencies();
    test_scheduler_delayed_tasks();
    test_scheduler_retry_delay();
    test_scheduler_empty();

    std::cout << "\n=== Benchmarks ===\n";
    bench_scheduler_throughput();

    std::cout << "\nAll tests passed!\n";
    return 0;
}
