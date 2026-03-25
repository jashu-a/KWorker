"""Benchmark: C++ scheduler vs pure Python implementation.

Run:
    python benchmarks/bench_scheduler.py

Requires the C++ engine to be built first:
    mkdir build && cd build && cmake .. && make
"""

import time
import sys
import heapq
from dataclasses import dataclass, field


# ── Pure Python Scheduler (for comparison) ──

@dataclass(order=True)
class PyTask:
    score: float
    id: str = field(compare=False)
    priority: int = field(compare=False, default=5)
    created_at_ms: int = field(compare=False, default=0)
    deadline_ms: int = field(compare=False, default=0)


class PythonScheduler:
    """Naive Python priority queue scheduler for benchmarking."""

    def __init__(self):
        self.heap: list[PyTask] = []

    def add_task(self, task_id: str, priority: int, created_at_ms: int,
                 deadline_ms: int = 0):
        now_ms = int(time.time() * 1000)
        score = self._compute_score(priority, created_at_ms, deadline_ms, now_ms)
        heapq.heappush(self.heap, PyTask(
            score=score, id=task_id, priority=priority,
            created_at_ms=created_at_ms, deadline_ms=deadline_ms,
        ))

    def next_task(self) -> PyTask | None:
        if not self.heap:
            return None
        return heapq.heappop(self.heap)

    def _compute_score(self, priority: int, created_at_ms: int,
                       deadline_ms: int, now_ms: int) -> float:
        score = 0.0
        score -= priority * 1000.0

        if deadline_ms > 0:
            time_until = (deadline_ms - now_ms) / 1000.0
            if time_until <= 0:
                score -= 50000.0
            elif time_until < 60.0:
                score -= 10000.0 / time_until

        if created_at_ms > 0:
            age_sec = (now_ms - created_at_ms) / 1000.0
            score -= age_sec * 0.1

        return score

    def pending_count(self) -> int:
        return len(self.heap)


def bench_python(n: int) -> dict:
    """Benchmark the pure Python scheduler."""
    sched = PythonScheduler()
    now = int(time.time() * 1000)

    # Insert
    start = time.perf_counter()
    for i in range(n):
        sched.add_task(
            task_id=f"task_{i}",
            priority=1 + (i % 10),
            created_at_ms=now - (i * 10),
        )
    insert_time = time.perf_counter() - start

    # Dequeue all
    start = time.perf_counter()
    dequeued = 0
    while sched.next_task() is not None:
        dequeued += 1
    dequeue_time = time.perf_counter() - start

    return {
        "insert_time_ms": insert_time * 1000,
        "dequeue_time_ms": dequeue_time * 1000,
        "dequeued": dequeued,
        "insert_rate": n / insert_time,
        "dequeue_rate": dequeued / dequeue_time,
    }


def bench_cpp(n: int) -> dict | None:
    """Benchmark the C++ scheduler via pybind11."""
    try:
        from _kworker_core import Scheduler, Task, TaskState
    except ImportError:
        return None

    sched = Scheduler(max_capacity=n + 100)
    now = int(time.time() * 1000)

    # Insert
    start = time.perf_counter()
    for i in range(n):
        t = Task()
        t.id = f"task_{i}"
        t.queue = "default"
        t.handler_name = "bench"
        t.payload_json = "{}"
        t.priority = 1 + (i % 10)
        t.state = TaskState.PENDING
        t.created_at_ms = now - (i * 10)
        sched.add_task(t)
    insert_time = time.perf_counter() - start

    # Dequeue all
    start = time.perf_counter()
    dequeued = 0
    while sched.next_task() is not None:
        dequeued += 1
    dequeue_time = time.perf_counter() - start

    return {
        "insert_time_ms": insert_time * 1000,
        "dequeue_time_ms": dequeue_time * 1000,
        "dequeued": dequeued,
        "insert_rate": n / insert_time,
        "dequeue_rate": dequeued / dequeue_time,
    }


def main():
    sizes = [1000, 5000, 10000, 50000]

    print("=" * 72)
    print("KWorker Scheduler Benchmark: C++ Engine vs Pure Python")
    print("=" * 72)

    for n in sizes:
        print(f"\n{'─' * 72}")
        print(f"  N = {n:,} tasks")
        print(f"{'─' * 72}")

        py_result = bench_python(n)
        cpp_result = bench_cpp(n)

        print(f"\n  {'Metric':<30} {'Python':>12} {'C++':>12} {'Speedup':>10}")
        print(f"  {'─' * 64}")

        print(f"  {'Insert (tasks/sec)':<30} {py_result['insert_rate']:>12,.0f}", end="")
        if cpp_result:
            speedup = cpp_result['insert_rate'] / py_result['insert_rate']
            print(f" {cpp_result['insert_rate']:>12,.0f} {speedup:>9.1f}x")
        else:
            print(f" {'N/A':>12} {'—':>10}")

        print(f"  {'Dequeue (tasks/sec)':<30} {py_result['dequeue_rate']:>12,.0f}", end="")
        if cpp_result:
            speedup = cpp_result['dequeue_rate'] / py_result['dequeue_rate']
            print(f" {cpp_result['dequeue_rate']:>12,.0f} {speedup:>9.1f}x")
        else:
            print(f" {'N/A':>12} {'—':>10}")

        print(f"  {'Dequeue time (ms)':<30} {py_result['dequeue_time_ms']:>12.2f}", end="")
        if cpp_result:
            print(f" {cpp_result['dequeue_time_ms']:>12.2f}")
        else:
            print(f" {'N/A':>12}")

    if not bench_cpp(1):
        print(f"\n⚠  C++ engine not available. Build it first:")
        print(f"   mkdir build && cd build && cmake .. && make")

    print()


if __name__ == "__main__":
    main()
