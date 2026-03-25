"""Worker process — pulls tasks from Redis and executes them.

Each worker:
1. Polls Redis for available tasks (ZPOPMIN — atomic, no double-processing)
2. Passes candidate tasks through the C++ scheduler for priority evaluation
3. Executes the task handler
4. Handles success (mark complete) or failure (retry or dead-letter)
"""

import logging
import signal
import time
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from kworker.redis_backend import RedisBackend
from kworker.task import TaskInstance, get_handler
from kworker.retry import RetryPolicy
from kworker.metrics import MetricsCollector

logger = logging.getLogger("kworker.worker")


class Worker:
    """Task execution worker."""

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        queues: list[str] | None = None,
        concurrency: int = 4,
        poll_interval: float = 1.0,
        worker_id: str | None = None,
    ):
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.queues = queues or ["default"]
        self.concurrency = concurrency
        self.poll_interval = poll_interval

        self.backend = RedisBackend(redis_url=redis_url)
        self.metrics = MetricsCollector(worker_id=self.worker_id)
        self.retry_policy = RetryPolicy()

        self._running = False
        self._executor: Optional[ThreadPoolExecutor] = None

        # Try to load C++ scheduler
        try:
            from _kworker_core import Scheduler
            self._cpp_scheduler = Scheduler()
            logger.info("C++ scheduling engine loaded")
        except ImportError:
            self._cpp_scheduler = None
            logger.warning(
                "C++ engine not available — using Python-only scheduling. "
                "Build the C++ module for better performance."
            )

    def start(self) -> None:
        """Start the worker loop."""
        self._running = True
        self._executor = ThreadPoolExecutor(max_workers=self.concurrency)

        # Handle graceful shutdown
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

        logger.info(
            f"Worker {self.worker_id} started | "
            f"queues={self.queues} concurrency={self.concurrency}"
        )

        self._poll_loop()

    def stop(self) -> None:
        """Stop the worker gracefully."""
        logger.info(f"Worker {self.worker_id} shutting down...")
        self._running = False
        if self._executor:
            self._executor.shutdown(wait=True, cancel_futures=False)

    def _handle_signal(self, signum, frame):
        logger.info(f"Received signal {signum}, initiating graceful shutdown")
        self.stop()

    def _poll_loop(self) -> None:
        """Main polling loop — round-robins across configured queues."""
        queue_index = 0

        while self._running:
            found_work = False

            # Try each queue in round-robin
            for _ in range(len(self.queues)):
                queue = self.queues[queue_index]
                queue_index = (queue_index + 1) % len(self.queues)

                task = self.backend.dequeue(queue, self.worker_id)
                if task:
                    found_work = True
                    self._executor.submit(self._execute_task, task)

            if not found_work:
                # No work available — sleep before polling again
                time.sleep(self.poll_interval)

            # Update queue depth metrics
            for q in self.queues:
                depth = self.backend.queue_depth(q)
                self.metrics.set_queue_depth(q, depth)

    def _execute_task(self, task: TaskInstance) -> None:
        """Execute a single task with error handling and retry logic."""
        start_time = time.time()
        logger.info(
            f"Executing task {task.id} | "
            f"name={task.name} attempt={task.attempt + 1}/{task.max_retries}"
        )

        try:
            # Look up the handler
            handler = get_handler(task.name)

            # Execute with timeout if configured
            result = handler(*task.args, **task.kwargs)

            # Success
            elapsed = time.time() - start_time
            self.backend.complete(task.id, result)
            self.metrics.record_task_completed(task.queue, elapsed)

            logger.info(
                f"Task {task.id} completed in {elapsed:.3f}s"
            )

        except Exception as e:
            elapsed = time.time() - start_time
            error_msg = f"{type(e).__name__}: {str(e)}"
            tb = traceback.format_exc()

            logger.error(f"Task {task.id} failed: {error_msg}\n{tb}")
            self.metrics.record_task_failed(task.queue)

            self._handle_failure(task, error_msg)

    def _handle_failure(self, task: TaskInstance, error: str) -> None:
        """Handle a failed task — retry or move to dead-letter queue."""
        task.attempt += 1

        if self.retry_policy.should_retry(task.attempt):
            delay = self.retry_policy.delay_for_attempt(task.attempt)
            logger.info(
                f"Retrying task {task.id} | "
                f"attempt {task.attempt}/{task.max_retries} | "
                f"delay={delay:.1f}s"
            )
            self.backend.requeue(task, delay_seconds=delay)
            self.metrics.record_task_retried(task.queue)

            # If C++ scheduler is available, update it
            if self._cpp_scheduler:
                cpp_task = task.to_cpp_task()
                self._cpp_scheduler.add_task(cpp_task)
        else:
            logger.warning(
                f"Task {task.id} exhausted retries — moving to DLQ"
            )
            self.backend.move_to_dlq(task, error)
            self.metrics.record_task_dead(task.queue)

            # If C++ scheduler is available, propagate failure
            if self._cpp_scheduler:
                unrunnable = self._cpp_scheduler.fail_task(task.id)
                if unrunnable:
                    logger.warning(
                        f"Tasks now unrunnable due to {task.id} failure: "
                        f"{unrunnable}"
                    )


def run_worker(
    redis_url: str = "redis://localhost:6379",
    queues: str = "default",
    concurrency: int = 4,
    poll_interval: float = 1.0,
) -> None:
    """Entry point for the worker CLI."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )

    queue_list = [q.strip() for q in queues.split(",")]
    worker = Worker(
        redis_url=redis_url,
        queues=queue_list,
        concurrency=concurrency,
        poll_interval=poll_interval,
    )
    worker.start()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="KWorker task processor")
    parser.add_argument("--redis-url", default="redis://localhost:6379")
    parser.add_argument("--queues", default="default",
                        help="Comma-separated queue names")
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--poll-interval", type=float, default=1.0)

    args = parser.parse_args()
    run_worker(
        redis_url=args.redis_url,
        queues=args.queues,
        concurrency=args.concurrency,
        poll_interval=args.poll_interval,
    )
