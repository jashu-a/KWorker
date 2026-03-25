"""KWorker — Kubernetes-native distributed task processor."""

from kworker.task import task, TaskDefinition
from kworker.client import KWorkerClient
from kworker.retry import RetryPolicy

__version__ = "0.1.0"

__all__ = [
    "task",
    "TaskDefinition",
    "KWorkerClient",
    "RetryPolicy",
]
