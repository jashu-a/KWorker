"""Prometheus metrics for KWorker.

Exposes:
    kworker_tasks_completed_total      — Counter by queue
    kworker_tasks_failed_total         — Counter by queue
    kworker_tasks_retried_total        — Counter by queue
    kworker_tasks_dead_total           — Counter by queue
    kworker_task_duration_seconds      — Histogram by queue
    kworker_queue_depth                — Gauge by queue
"""

from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry, generate_latest


# Use a custom registry so we don't conflict with other Prometheus users
REGISTRY = CollectorRegistry()

TASKS_COMPLETED = Counter(
    "kworker_tasks_completed_total",
    "Total tasks completed successfully",
    ["queue"],
    registry=REGISTRY,
)

TASKS_FAILED = Counter(
    "kworker_tasks_failed_total",
    "Total tasks that failed",
    ["queue"],
    registry=REGISTRY,
)

TASKS_RETRIED = Counter(
    "kworker_tasks_retried_total",
    "Total tasks retried",
    ["queue"],
    registry=REGISTRY,
)

TASKS_DEAD = Counter(
    "kworker_tasks_dead_total",
    "Total tasks moved to dead-letter queue",
    ["queue"],
    registry=REGISTRY,
)

TASK_DURATION = Histogram(
    "kworker_task_duration_seconds",
    "Task execution duration in seconds",
    ["queue"],
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0],
    registry=REGISTRY,
)

QUEUE_DEPTH = Gauge(
    "kworker_queue_depth",
    "Current number of pending tasks in queue",
    ["queue"],
    registry=REGISTRY,
)


class MetricsCollector:
    """Wrapper around Prometheus metrics for a single worker."""

    def __init__(self, worker_id: str):
        self.worker_id = worker_id

    def record_task_completed(self, queue: str, duration: float) -> None:
        TASKS_COMPLETED.labels(queue=queue).inc()
        TASK_DURATION.labels(queue=queue).observe(duration)

    def record_task_failed(self, queue: str) -> None:
        TASKS_FAILED.labels(queue=queue).inc()

    def record_task_retried(self, queue: str) -> None:
        TASKS_RETRIED.labels(queue=queue).inc()

    def record_task_dead(self, queue: str) -> None:
        TASKS_DEAD.labels(queue=queue).inc()

    def set_queue_depth(self, queue: str, depth: int) -> None:
        QUEUE_DEPTH.labels(queue=queue).set(depth)

    @staticmethod
    def generate_metrics() -> bytes:
        """Generate Prometheus exposition format output."""
        return generate_latest(REGISTRY)
