# Distributed Compute Orchestrator (Go)

A fault-tolerant job orchestration system: submit a job, it's queued in Redis,
a worker picks it up, executes it, and reports back. Failures get retried
automatically with exponential backoff. The control plane is the single
source of truth (Postgres); workers are dumb, stateless executors.

## Run it

```bash
docker compose up --build --scale worker=3
```

First build will take a minute or two (Go module downloads). Control plane
comes up on `http://localhost:8000`.

## Try it

Submit a reliable job:
```bash
curl -X POST localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{"task_name": "sleep_task", "payload": {"seconds": 2}}'
```

Submit a flaky job to watch retries happen (60% fail rate by default):
```bash
curl -X POST localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{"task_name": "flaky_task", "payload": {"fail_rate": 0.7}, "max_retries": 5}'
```

Check status (swap in the id returned above):
```bash
curl localhost:8000/jobs/<id>
```

List all jobs:
```bash
curl localhost:8000/jobs
```

Watch it live:
```bash
docker compose logs -f worker controlplane
```

## Job lifecycle

```
PENDING -> QUEUED -> RUNNING -> SUCCESS
                          |
                          v
                  FAILED_RETRY_PENDING --(backoff elapses)--> QUEUED
                          |
                          v (retries exhausted)
                        FAILED
```

## Architecture

- **Control plane** (`controlplane/`) - FastAPI-equivalent Go HTTP service.
  Owns all state in Postgres, pushes/receives from a Redis list acting as
  the job queue, and runs a background goroutine that promotes retryable
  failures back onto the queue once their backoff window elapses.
- **Worker** (`worker/`) - polls the Redis queue with `BRPOP`, executes the
  named task, and reports success/failure back to the control plane over
  HTTP. Workers hold no state of their own - kill one mid-job and nothing
  is lost except that one in-flight execution, which gets retried.
- **Redis** - the queue. Decouples control plane from workers so you can
  scale worker count independently.
- **Postgres** - the source of truth for job state.

## What's next (not built yet)

This is the base layer. Planned on top of it:
- **HA control plane** - run multiple control-plane replicas with leader
  election via Postgres advisory locks; only the leader runs the retry
  scheduler, followers still accept submissions.
- **Chaos mode** - a dashboard button that kills a random worker or the
  current leader (`docker kill`) live, so you can watch the system
  self-heal on camera.
- **Live dashboard** - WebSocket-pushed view of queue depth, job states,
  and worker/leader health.
