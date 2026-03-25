"""Integration tests for Redis backend and worker.

These tests require a running Redis instance.
Set KWORKER_REDIS_URL env var or defaults to redis://localhost:6379.
"""

import os

import pytest

from kworker.redis_backend import RedisBackend
from kworker.task import create_task_instance, task


REDIS_URL = os.getenv("KWORKER_REDIS_URL", "redis://localhost:6379")


@pytest.fixture
def backend():
    """Create a Redis backend using a test database (db=15)."""
    b = RedisBackend(redis_url=REDIS_URL, db=15)
    if not b.ping():
        pytest.skip("Redis not available")
    # Flush test database
    b.client.flushdb()
    yield b
    b.client.flushdb()


@task(name="test.add", queue="test-queue", priority=5)
def add_numbers(a: int, b: int) -> int:
    return a + b


@task(name="test.failing", queue="test-queue", priority=5)
def failing_task():
    raise ValueError("intentional failure")


class TestRedisBackend:
    def test_enqueue_dequeue(self, backend: RedisBackend):
        instance = create_task_instance(
            definition=add_numbers,
            args=[3, 4],
        )

        backend.enqueue(instance)

        # Queue should have 1 item
        assert backend.queue_depth("test-queue") == 1

        # Dequeue
        dequeued = backend.dequeue("test-queue", "worker-test-1")
        assert dequeued is not None
        assert dequeued.name == "test.add"
        assert dequeued.args == [3, 4]

        # Queue should be empty
        assert backend.queue_depth("test-queue") == 0

    def test_priority_ordering(self, backend: RedisBackend):
        low = create_task_instance(add_numbers, args=[1, 1])
        low.priority = 1

        high = create_task_instance(add_numbers, args=[2, 2])
        high.priority = 10

        # Enqueue low first, then high
        backend.enqueue(low)
        backend.enqueue(high)

        # High priority should come out first (lower score)
        first = backend.dequeue("test-queue", "worker-test-1")
        assert first is not None
        assert first.priority == 10
        assert first.args == [2, 2]

    def test_complete_and_result(self, backend: RedisBackend):
        instance = create_task_instance(add_numbers, args=[5, 6])
        backend.enqueue(instance)

        dequeued = backend.dequeue("test-queue", "worker-test-1")
        backend.complete(dequeued.id, result=11)

        state = backend.get_task_state(dequeued.id)
        assert state["state"] == "completed"

        result = backend.get_result(dequeued.id)
        assert result == 11

    def test_fail_and_requeue(self, backend: RedisBackend):
        instance = create_task_instance(add_numbers, args=[1, 2])
        backend.enqueue(instance)

        dequeued = backend.dequeue("test-queue", "worker-test-1")
        backend.fail(dequeued.id, "test error")

        state = backend.get_task_state(dequeued.id)
        assert state["state"] == "failed"

        # Requeue with delay
        backend.requeue(dequeued, delay_seconds=0.1)
        assert backend.queue_depth("test-queue") == 1

    def test_dead_letter_queue(self, backend: RedisBackend):
        instance = create_task_instance(add_numbers, args=[1, 2])
        backend.enqueue(instance)

        dequeued = backend.dequeue("test-queue", "worker-test-1")
        backend.move_to_dlq(dequeued, "permanent failure")

        dlq = backend.get_dlq("test-queue")
        assert len(dlq) == 1
        assert dlq[0]["error"] == "permanent failure"

        # Flush DLQ
        count = backend.flush_dlq("test-queue")
        assert count == 1
        assert len(backend.get_dlq("test-queue")) == 0

    def test_concurrent_dequeue(self, backend: RedisBackend):
        """Two workers competing for one task — only one should get it."""
        instance = create_task_instance(add_numbers, args=[1, 1])
        backend.enqueue(instance)

        first = backend.dequeue("test-queue", "worker-1")
        second = backend.dequeue("test-queue", "worker-2")

        assert first is not None
        assert second is None  # Already taken by worker-1
