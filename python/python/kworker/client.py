"""Client SDK for submitting and monitoring tasks."""

import time
from typing import Any

from kworker.redis_backend import RedisBackend
from kworker.task import TaskDefinition, create_task_instance


class JobHandle:
    """Handle to a submitted job for status tracking."""

    def __init__(self, task_id: str, backend: RedisBackend):
        self.id = task_id
        self._backend = backend

    @property
    def status(self) -> str:
        state = self._backend.get_task_state(self.id)
        return state.get("state", "unknown") if state else "unknown"

    @property
    def result(self) -> Any:
        return self._backend.get_result(self.id)

    def wait(self, timeout: float = 30.0, poll_interval: float = 0.5) -> Any:
        """Block until the task completes or timeout.

        Returns the task result, or raises TimeoutError.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            state = self._backend.get_task_state(self.id)
            if not state:
                raise RuntimeError(f"Task {self.id} not found")

            status = state.get("state", "")
            if status == "completed":
                return self.result
            elif status in ("failed", "dead"):
                raise RuntimeError(
                    f"Task {self.id} {status}: {state.get('error', 'unknown')}"
                )

            time.sleep(poll_interval)

        raise TimeoutError(f"Task {self.id} did not complete within {timeout}s")

    def __repr__(self) -> str:
        return f"JobHandle(id={self.id!r}, status={self.status!r})"


class KWorkerClient:
    """Client for submitting tasks to the KWorker system."""

    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.backend = RedisBackend(redis_url=redis_url)

    def submit(
        self,
        task_def: TaskDefinition,
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        depends_on: list[str | JobHandle] | None = None,
        deadline: float | None = None,
        priority: int | None = None,
    ) -> JobHandle:
        """Submit a task for async execution.

        Args:
            task_def: The @task decorated function to execute
            args: Positional arguments for the task
            kwargs: Keyword arguments for the task
            depends_on: List of task IDs or JobHandles this task depends on
            deadline: Unix timestamp by which this task should complete
            priority: Override the task's default priority (1-10)

        Returns:
            JobHandle for tracking the task
        """
        # Resolve JobHandle dependencies to IDs
        dep_ids = []
        if depends_on:
            for dep in depends_on:
                if isinstance(dep, JobHandle):
                    dep_ids.append(dep.id)
                else:
                    dep_ids.append(str(dep))

        instance = create_task_instance(
            definition=task_def,
            args=args,
            kwargs=kwargs,
            depends_on=dep_ids,
            deadline=deadline,
            priority=priority,
        )

        self.backend.enqueue(instance)
        return JobHandle(instance.id, self.backend)

    def get_job(self, task_id: str) -> JobHandle:
        """Get a handle to an existing job by ID."""
        return JobHandle(task_id, self.backend)

    def queue_depth(self, queue: str = "default") -> int:
        """Get the number of pending tasks in a queue."""
        return self.backend.queue_depth(queue)

    def get_dlq(self, queue: str = "default", limit: int = 100) -> list[dict]:
        """Get dead-letter queue contents."""
        return self.backend.get_dlq(queue, limit)

    def ping(self) -> bool:
        """Check if the backend is reachable."""
        return self.backend.ping()
