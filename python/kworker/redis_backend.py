"""Redis backend for task storage, queuing, and state management.

Key schema:
    kworker:queue:{queue_name}     — Sorted set (score = scheduling priority)
    kworker:task:{task_id}         — Hash (task metadata + state)
    kworker:result:{task_id}       — String (JSON result or error)
    kworker:dlq:{queue_name}       — List (dead-letter queue)
    kworker:metrics:queue_depth    — String (current total queue depth)
"""

import json
import time

import redis

from kworker.task import TaskInstance
from typing import Any

# Redis key prefixes
QUEUE_KEY = "kworker:queue:{queue}"
TASK_KEY = "kworker:task:{task_id}"
RESULT_KEY = "kworker:result:{task_id}"
DLQ_KEY = "kworker:dlq:{queue}"
ACTIVE_KEY = "kworker:active:{worker_id}"


class RedisBackend:
    """Redis operations for the task processing system."""

    def __init__(self, redis_url: str = "redis://localhost:6379", db: int = 0):
        self.client = redis.Redis.from_url(redis_url, db=db, decode_responses=True)

    def enqueue(self, task: TaskInstance) -> str:
        """Push a task to its queue.

        Uses a sorted set scored by negative priority (higher priority = lower
        score = popped first by ZPOPMIN).
        """
        pipe = self.client.pipeline()

        queue_key = QUEUE_KEY.format(queue=task.queue)
        task_key = TASK_KEY.format(task_id=task.id)

        # Score: lower = more urgent
        # Negate priority so that priority=10 → score=-10 (popped first)
        score = -task.priority

        # If task is delayed, factor in the scheduled time
        if task.created_at > 0:
            # Add a small time component to break ties (FIFO within same priority)
            score += task.created_at / 1e12

        # Store task data
        pipe.hset(task_key, mapping={
            "id": task.id,
            "name": task.name,
            "queue": task.queue,
            "priority": str(task.priority),
            "args": json.dumps(task.args),
            "kwargs": json.dumps(task.kwargs),
            "state": "pending",
            "attempt": str(task.attempt),
            "max_retries": str(task.max_retries),
            "created_at": str(task.created_at),
            "deadline": str(task.deadline) if task.deadline else "",
            "depends_on": json.dumps(task.depends_on),
        })

        # TTL: auto-cleanup after 24h for completed tasks
        pipe.expire(task_key, 86400)

        # Add to queue
        pipe.zadd(queue_key, {task.id: score})

        pipe.execute()
        return task.id

    def dequeue(self, queue: str, worker_id: str) -> TaskInstance | None:
        """Atomically pop the highest-priority task from a queue.

        Uses ZPOPMIN for atomic dequeue — no race conditions between workers.
        """
        queue_key = QUEUE_KEY.format(queue=queue)

        # ZPOPMIN returns the member with the lowest score
        result = self.client.zpopmin(queue_key, count=1)
        if not result:
            return None

        task_id, score = result[0]
        task_key = TASK_KEY.format(task_id=task_id)

        # Fetch full task data
        task_data = self.client.hgetall(task_key)
        if not task_data:
            return None

        # Mark as running
        pipe = self.client.pipeline()
        pipe.hset(task_key, mapping={
            "state": "running",
            "started_at": str(time.time()),
            "worker_id": worker_id,
        })
        # Track active task for this worker
        pipe.set(ACTIVE_KEY.format(worker_id=worker_id), task_id, ex=3600)
        pipe.execute()

        return TaskInstance(
            id=task_data["id"],
            name=task_data["name"],
            queue=task_data["queue"],
            priority=int(task_data["priority"]),
            args=json.loads(task_data["args"]),
            kwargs=json.loads(task_data["kwargs"]),
            state="running",
            attempt=int(task_data["attempt"]),
            max_retries=int(task_data["max_retries"]),
            created_at=float(task_data["created_at"]),
            deadline=float(task_data["deadline"]) if task_data.get("deadline") else None,
            depends_on=json.loads(task_data.get("depends_on", "[]")),
        )

    def complete(self, task_id: str, result: Any = None) -> None:
        """Mark a task as completed and store its result."""
        pipe = self.client.pipeline()
        task_key = TASK_KEY.format(task_id=task_id)
        result_key = RESULT_KEY.format(task_id=task_id)

        pipe.hset(task_key, mapping={
            "state": "completed",
            "completed_at": str(time.time()),
        })
        if result is not None:
            pipe.set(result_key, json.dumps(result), ex=86400)
        pipe.execute()

    def fail(self, task_id: str, error: str) -> None:
        """Mark a task as failed."""
        task_key = TASK_KEY.format(task_id=task_id)
        self.client.hset(task_key, mapping={
            "state": "failed",
            "error": error,
            "failed_at": str(time.time()),
        })

    def requeue(self, task: TaskInstance, delay_seconds: float) -> None:
        """Re-enqueue a task for retry after a delay."""
        task.state = "pending"
        task.attempt += 1
        # Set scheduled time in the future
        task.created_at = time.time() + delay_seconds
        self.enqueue(task)

    def move_to_dlq(self, task: TaskInstance, error: str) -> None:
        """Move a permanently failed task to the dead-letter queue."""
        dlq_key = DLQ_KEY.format(queue=task.queue)
        task_key = TASK_KEY.format(task_id=task.id)

        pipe = self.client.pipeline()
        pipe.hset(task_key, mapping={
            "state": "dead",
            "error": error,
            "dead_at": str(time.time()),
        })
        pipe.lpush(dlq_key, json.dumps({
            "task_id": task.id,
            "name": task.name,
            "error": error,
            "attempts": task.attempt,
            "died_at": time.time(),
        }))
        pipe.execute()

    def get_task_state(self, task_id: str) -> dict | None:
        """Get current task state from Redis."""
        task_key = TASK_KEY.format(task_id=task_id)
        return self.client.hgetall(task_key) or None

    def get_result(self, task_id: str) -> Any | None:
        """Get the result of a completed task."""
        result_key = RESULT_KEY.format(task_id=task_id)
        raw = self.client.get(result_key)
        return json.loads(raw) if raw else None

    def queue_depth(self, queue: str) -> int:
        """Get the number of pending tasks in a queue."""
        queue_key = QUEUE_KEY.format(queue=queue)
        return self.client.zcard(queue_key)

    def total_queue_depth(self, queues: list[str]) -> int:
        """Get total pending tasks across all specified queues."""
        total = 0
        for q in queues:
            total += self.queue_depth(q)
        return total

    def get_dlq(self, queue: str, limit: int = 100) -> list[dict]:
        """Get tasks from the dead-letter queue."""
        dlq_key = DLQ_KEY.format(queue=queue)
        items = self.client.lrange(dlq_key, 0, limit - 1)
        return [json.loads(item) for item in items]

    def flush_dlq(self, queue: str) -> int:
        """Clear the dead-letter queue. Returns number of items removed."""
        dlq_key = DLQ_KEY.format(queue=queue)
        count = self.client.llen(dlq_key)
        self.client.delete(dlq_key)
        return count

    def ping(self) -> bool:
        """Check Redis connectivity."""
        try:
            return self.client.ping()
        except redis.ConnectionError:
            return False
