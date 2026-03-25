"""Tests for retry policy and task model."""

import json
import time
import pytest

from kworker.retry import RetryPolicy
from kworker.task import task, TaskDefinition, TaskInstance, create_task_instance, get_handler


class TestRetryPolicy:
    def test_exponential_backoff(self):
        policy = RetryPolicy(max_attempts=5, backoff_base=2.0, backoff_max=300.0)

        assert policy.delay_for_attempt(1) == 2.0
        assert policy.delay_for_attempt(2) == 4.0
        assert policy.delay_for_attempt(3) == 8.0
        assert policy.delay_for_attempt(4) == 16.0
        assert policy.delay_for_attempt(5) == 32.0

    def test_backoff_cap(self):
        policy = RetryPolicy(max_attempts=10, backoff_base=2.0, backoff_max=60.0)

        # 2^9 = 512 which exceeds max of 60
        assert policy.delay_for_attempt(10) == 60.0

    def test_should_retry(self):
        policy = RetryPolicy(max_attempts=3)

        assert policy.should_retry(0) is True
        assert policy.should_retry(1) is True
        assert policy.should_retry(2) is True
        assert policy.should_retry(3) is False
        assert policy.should_retry(4) is False

    def test_single_attempt(self):
        policy = RetryPolicy(max_attempts=1)
        assert policy.should_retry(0) is True
        assert policy.should_retry(1) is False


class TestTaskDecorator:
    def test_basic_decorator(self):
        @task
        def simple_task(x: int) -> int:
            return x * 2

        assert isinstance(simple_task, TaskDefinition)
        assert simple_task.queue == "default"
        assert simple_task.priority == 5
        assert simple_task(5) == 10  # direct call still works

    def test_decorator_with_options(self):
        @task(queue="high", priority=8, max_retries=5)
        def important_task(data: str) -> str:
            return data.upper()

        assert important_task.queue == "high"
        assert important_task.priority == 8
        assert important_task.max_retries == 5
        assert important_task("hello") == "HELLO"

    def test_handler_registry(self):
        @task(name="test.registered_task")
        def registered_task():
            return "done"

        handler = get_handler("test.registered_task")
        assert handler is registered_task

    def test_handler_not_found(self):
        with pytest.raises(KeyError):
            get_handler("nonexistent.task")


class TestTaskInstance:
    def test_json_roundtrip(self):
        instance = TaskInstance(
            id="test-123",
            name="my_task",
            queue="default",
            priority=5,
            args=[1, "hello"],
            kwargs={"key": "value"},
            created_at=time.time(),
            depends_on=["dep-1", "dep-2"],
        )

        json_str = instance.to_json()
        restored = TaskInstance.from_json(json_str)

        assert restored.id == instance.id
        assert restored.name == instance.name
        assert restored.args == instance.args
        assert restored.kwargs == instance.kwargs
        assert restored.depends_on == instance.depends_on

    def test_create_task_instance(self):
        @task(queue="batch", priority=3, max_retries=2)
        def batch_job(items: list):
            return len(items)

        instance = create_task_instance(
            definition=batch_job,
            args=[[1, 2, 3]],
            depends_on=["prev-task"],
        )

        assert instance.name == batch_job.name
        assert instance.queue == "batch"
        assert instance.priority == 3
        assert instance.max_retries == 2
        assert instance.args == [[1, 2, 3]]
        assert instance.depends_on == ["prev-task"]
        assert instance.id  # UUID generated
        assert instance.created_at > 0

    def test_priority_override(self):
        @task(priority=5)
        def normal_task():
            pass

        instance = create_task_instance(
            definition=normal_task,
            priority=10,  # override
        )
        assert instance.priority == 10
