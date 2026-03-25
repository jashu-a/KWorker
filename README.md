# KWorker — Kubernetes-Native Distributed Task Processor

A high-performance distributed task processing system designed for Kubernetes, featuring a C++ scheduling engine for hot-path task prioritization and a Python worker framework for task execution.

## Why KWorker?

Existing task processors (Celery, Sidekiq, Bull) were designed before Kubernetes. They bolt on autoscaling as an afterthought. KWorker is **K8s-native from the ground up** — worker scaling is driven by queue depth via Kubernetes HPA custom metrics, and the scheduling engine is written in C++ for throughput-critical workloads.

### Key Differentiators

- **C++ Scheduling Core**: Priority queue with deadline-aware scheduling and task dependency (DAG) resolution. Handles 100K+ scheduling decisions/sec vs ~5K in pure Python.
- **Kubernetes-Native Autoscaling**: Workers scale via HPA based on real-time queue depth metrics exposed through Prometheus.
- **Retry & Dead Letter Handling**: Configurable retry with exponential backoff, max attempts, and dead-letter queue for failed tasks.
- **Task Dependencies**: Define DAGs — task C only runs after A and B complete successfully.
- **Observability Built-in**: Prometheus metrics for queue depth, task latency, worker utilization, and failure rates.

## Architecture

```
                    ┌──────────────────────────────────┐
                    │          FastAPI Admin            │
                    │     (Submit / Monitor / Cancel)   │
                    └──────────┬───────────────────────┘
                               │
                               ▼
┌──────────┐    ┌─────────────────────────────┐    ┌──────────────────┐
│  Client   │───▶│           Redis              │◀───│   Prometheus     │
│  (SDK)    │    │  • Task Queue (sorted set)   │    │   (metrics       │
└──────────┘    │  • State Store (hash)        │    │    scraping)     │
                 │  • Result Backend            │    └──────────────────┘
                 └──────────┬──────────────────┘
                            │
                ┌───────────┼───────────────┐
                ▼           ▼               ▼
         ┌──────────┐┌──────────┐   ┌──────────┐
         │ Worker 1 ││ Worker 2 │...│ Worker N │  ← HPA scales these
         │┌────────┐││┌────────┐│   │┌────────┐│
         ││ C++ Eng│││| C++ Eng││   ││ C++ Eng││
         │└────────┘││└────────┘│   │└────────┘│
         └──────────┘└──────────┘   └──────────┘
```

### How It Works

1. **Task Submission**: Client submits a task via FastAPI or Python SDK. Task is serialized and pushed to Redis (sorted set, scored by priority + deadline).

2. **Scheduling (C++ Engine)**: Each worker runs the C++ scheduler locally. The scheduler evaluates candidate tasks using a priority scoring function:
   ```
   score = (base_priority * 1000) + deadline_urgency_factor - dependency_penalty
   ```
   Tasks with unmet dependencies are skipped. This scoring + evaluation runs in C++ for throughput.

3. **Execution**: The Python worker deserializes the task, executes the registered handler, and updates state in Redis (RUNNING → COMPLETED / FAILED).

4. **Retry Logic**: On failure, the worker checks retry policy (max_attempts, backoff_base, backoff_max). If retries remain, the task is re-enqueued with an updated score reflecting the backoff delay. Otherwise, it moves to the dead-letter queue.

5. **Autoscaling**: A metrics exporter reads Redis queue depth and exposes it as a Prometheus metric. Kubernetes HPA watches this metric and scales worker pods accordingly.

## Project Structure

```
kworker/
├── cpp/                    # C++ scheduling engine
│   ├── include/
│   │   └── kworker/
│   │       ├── scheduler.hpp       # Priority scheduler
│   │       ├── task.hpp            # Task data structures
│   │       └── dag.hpp             # Dependency graph
│   ├── src/
│   │   ├── scheduler.cpp
│   │   ├── dag.cpp
│   │   └── bindings.cpp           # pybind11 bindings
│   ├── tests/
│   │   └── test_scheduler.cpp
│   └── CMakeLists.txt
├── python/
│   ├── kworker/
│   │   ├── __init__.py
│   │   ├── worker.py              # Worker process
│   │   ├── client.py              # Task submission SDK
│   │   ├── task.py                # Task models
│   │   ├── retry.py               # Retry policy
│   │   ├── redis_backend.py       # Redis operations
│   │   └── metrics.py             # Prometheus metrics
│   ├── api/
│   │   ├── __init__.py
│   │   └── server.py              # FastAPI admin
│   └── tests/
│       ├── test_worker.py
│       └── test_retry.py
├── k8s/
│   └── helm/
│       └── kworker/
│           ├── Chart.yaml
│           ├── values.yaml
│           └── templates/
│               ├── worker-deployment.yaml
│               ├── api-deployment.yaml
│               ├── redis-deployment.yaml
│               ├── hpa.yaml
│               └── servicemonitor.yaml
├── docker/
│   ├── Dockerfile.worker
│   └── Dockerfile.api
├── benchmarks/
│   └── bench_scheduler.py
├── .github/
│   └── workflows/
│       └── ci.yml
├── pyproject.toml
├── CMakeLists.txt
└── README.md
```

## Quick Start

### Local Development

```bash
# Build C++ engine
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
cd ..

# Install Python package (includes C++ extension)
pip install -e .

# Start Redis
docker run -d -p 6379:6379 redis:7-alpine

# Run a worker
kworker-worker --concurrency 4 --queues default,high

# Start the API
uvicorn kworker.api.server:app --reload
```

### Define and Submit Tasks

```python
from kworker import task, KWorkerClient

@task(queue="default", max_retries=3, priority=5)
def process_report(report_id: str):
    # your logic here
    return {"status": "processed", "id": report_id}

# Submit
client = KWorkerClient(redis_url="redis://localhost:6379")
job = client.submit(process_report, args=["report-123"])
print(f"Job ID: {job.id}, Status: {job.status}")

# Submit with dependencies
job_a = client.submit(extract_data, args=["file.csv"])
job_b = client.submit(validate_data, args=["file.csv"])
job_c = client.submit(generate_report, args=["file.csv"], depends_on=[job_a, job_b])
```

### Deploy to Kubernetes

```bash
helm install kworker k8s/helm/kworker \
  --set redis.host=redis.default.svc.cluster.local \
  --set worker.replicas.min=2 \
  --set worker.replicas.max=20 \
  --set worker.autoscaling.targetQueueDepth=100
```

## Benchmarks

Scheduler throughput (task scoring + dequeue) on Apple M2:

| Implementation | Throughput (tasks/sec) | p99 Latency |
|---------------|----------------------|-------------|
| C++ Engine    | ~120,000             | 0.008ms     |
| Pure Python   | ~4,500               | 0.22ms      |

See `benchmarks/` for reproducible benchmark scripts.

## Tech Stack

- **C++17** — Scheduling engine (priority queue, DAG resolution)
- **pybind11** — C++ → Python bridge
- **Python 3.11+** — Workers, API, SDK
- **FastAPI** — Admin and monitoring REST API
- **Redis** — Task broker, state store, result backend
- **Prometheus** — Metrics collection
- **Kubernetes** — Worker orchestration and autoscaling (HPA)
- **Helm** — K8s packaging
- **Docker** — Multi-stage container builds
- **GitHub Actions** — CI/CD (C++ build, Python tests, Docker image)

## License

MIT
