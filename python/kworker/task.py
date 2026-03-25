"""Task definition and decorator for registering handlers."""

import functools
import json
import uuid
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from kworker.retry import RetryPolicy

# Global registry of task handlers
_task_registry: dict[str, "TaskDefinition"] = {}


@dataclass
class TaskDefinition:
    """A registered task handler with its configuration."""
    name: str
    handler: Callable
    queue: str = "default"
    priority: int = 5
    max_retries: int = 3
    timeout_seconds: Optional[int] = None
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)

    def __call__(self, *args, **kwargs):
        """Direct invocation (for local testing / non-queued execution)."""
        return self.handler(*args, **kwargs)


@dataclass
class TaskInstance:
    """A concrete instance of a task submitted to the queue."""
    id: str
    name: str
    queue: str
    priority: int
    args: list[Any]
    kwargs: dict[str, Any]
    state: str = "pending"
    attempt: int = 0
    max_retries: int = 3
    created_at: float = 0.0
    deadline: Optional[float] = None
    depends_on: list[str] = field(default_factory=list)
    result: Any = None
    error: Optional[str] = None

    def to_json(self) -> str:
        return json.dumps({
            "id": self.id,
            "name": self.name,
            "queue": self.queue,
            "priority": self.priority,
            "args": self.args,
            "kwargs": self.kwargs,
            "state": self.state,
            "attempt": self.attempt,
            "max_retries": self.max_retries,
            "created_at": self.created_at,
            "deadline": self.deadline,
            "depends_on": self.depends_on,
        })

    @classmethod
    def from_json(cls, data: str) -> "TaskInstance":
        d = json.loads(data)
        return cls(**d)

    def to_cpp_task(self):
        """Convert to C++ Task struct for scheduling engine."""
        try:
            from _kworker_core import Task as CppTask, TaskState
        except ImportError:
            raise RuntimeError(
                "C++ engine not built. Run: mkdir build && cd build "
                "&& cmake .. && make"
            )

        t = CppTask()
        t.id = self.id
        t.queue = self.queue
        t.handler_name = self.name
        t.payload_json = json.dumps({"args": self.args, "kwargs": self.kwargs})
        t.priority = self.priority
        t.state = TaskState.PENDING
        t.created_at_ms = int(self.created_at * 1000)
        t.deadline_ms = int(self.deadline * 1000) if self.deadline else 0
        t.attempt = self.attempt
        t.depends_on = self.depends_on
        return t


def task(
    _func: Optional[Callable] = None,
    *,
    name: Optional[str] = None,
    queue: str = "default",
    priority: int = 5,
    max_retries: int = 3,
    timeout_seconds: Optional[int] = None,
) -> Callable:
    """Decorator to register a function as a KWorker task.

    Usage:
        @task(queue="high", priority=8, max_retries=5)
        def process_report(report_id: str):
            ...

        # Or without arguments:
        @task
        def simple_job(x: int):
            ...
    """
    def decorator(func: Callable) -> TaskDefinition:
        task_name = name or f"{func.__module__}.{func.__qualname__}"
        td = TaskDefinition(
            name=task_name,
            handler=func,
            queue=queue,
            priority=priority,
            max_retries=max_retries,
            timeout_seconds=timeout_seconds,
        )
        _task_registry[task_name] = td
        functools.update_wrapper(td, func)
        return td

    if _func is not None:
        return decorator(_func)
    return decorator


def get_handler(name: str) -> TaskDefinition:
    """Look up a registered task handler by name."""
    if name not in _task_registry:
        raise KeyError(
            f"No task handler registered with name '{name}'. "
            f"Available: {list(_task_registry.keys())}"
        )
    return _task_registry[name]


def create_task_instance(
    definition: TaskDefinition,
    args: list[Any] | None = None,
    kwargs: dict[str, Any] | None = None,
    depends_on: list[str] | None = None,
    deadline: float | None = None,
    priority: int | None = None,
) -> TaskInstance:
    """Create a TaskInstance from a TaskDefinition."""
    return TaskInstance(
        id=str(uuid.uuid4()),
        name=definition.name,
        queue=definition.queue,
        priority=priority or definition.priority,
        args=args or [],
        kwargs=kwargs or {},
        max_retries=definition.max_retries,
        created_at=time.time(),
        deadline=deadline,
        depends_on=depends_on or [],
    )
