"""FastAPI admin and monitoring API for KWorker."""

import os

from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel

from kworker.metrics import MetricsCollector
from kworker.redis_backend import RedisBackend

app = FastAPI(
    title="KWorker Admin API",
    description="Monitoring and management for KWorker task processor",
    version="0.1.0",
)

REDIS_URL = os.getenv("KWORKER_REDIS_URL", "redis://localhost:6379")
backend = RedisBackend(redis_url=REDIS_URL)


# ── Request/Response Models ──

class TaskSubmission(BaseModel):
    name: str
    args: list = []
    kwargs: dict = {}
    queue: str = "default"
    priority: int = 5
    depends_on: list[str] = []


class TaskStatus(BaseModel):
    id: str
    name: str | None = None
    state: str
    queue: str | None = None
    attempt: str | None = None
    error: str | None = None


class QueueInfo(BaseModel):
    queue: str
    depth: int
    dlq_size: int


class HealthResponse(BaseModel):
    status: str
    redis_connected: bool


# ── Routes ──

@app.get("/health", response_model=HealthResponse)
def health_check():
    """Health check endpoint for Kubernetes liveness/readiness probes."""
    redis_ok = backend.ping()
    return HealthResponse(
        status="healthy" if redis_ok else "degraded",
        redis_connected=redis_ok,
    )


@app.get("/metrics")
def prometheus_metrics():
    """Prometheus metrics endpoint.

    Scraped by Prometheus via ServiceMonitor in K8s.
    """
    return Response(
        content=MetricsCollector.generate_metrics(),
        media_type="text/plain; version=0.0.4; charset=utf-8",
    )


@app.get("/tasks/{task_id}", response_model=TaskStatus)
def get_task_status(task_id: str):
    """Get the current status of a task."""
    state = backend.get_task_state(task_id)
    if not state:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
    return TaskStatus(
        id=task_id,
        name=state.get("name"),
        state=state.get("state", "unknown"),
        queue=state.get("queue"),
        attempt=state.get("attempt"),
        error=state.get("error"),
    )


@app.get("/tasks/{task_id}/result")
def get_task_result(task_id: str):
    """Get the result of a completed task."""
    result = backend.get_result(task_id)
    if result is None:
        raise HTTPException(
            status_code=404,
            detail=f"No result found for task {task_id}",
        )
    return {"task_id": task_id, "result": result}


@app.get("/queues/{queue}", response_model=QueueInfo)
def get_queue_info(queue: str):
    """Get queue depth and dead-letter queue size."""
    return QueueInfo(
        queue=queue,
        depth=backend.queue_depth(queue),
        dlq_size=len(backend.get_dlq(queue)),
    )


@app.get("/queues/{queue}/dlq")
def get_dead_letter_queue(queue: str, limit: int = 100):
    """Get dead-letter queue contents."""
    items = backend.get_dlq(queue, limit=limit)
    return {"queue": queue, "count": len(items), "items": items}


@app.delete("/queues/{queue}/dlq")
def flush_dead_letter_queue(queue: str):
    """Clear the dead-letter queue."""
    count = backend.flush_dlq(queue)
    return {"queue": queue, "flushed": count}
