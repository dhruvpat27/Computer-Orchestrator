# Distributed Compute Orchestrator (Go)

A fault-tolerant job orchestration system: submit a job, it's queued in Redis,
a worker picks it up, executes it, and reports back. Failures get retried
automatically with exponential backoff. Three control-plane replicas run
behind Postgres-advisory-lock leader election, so the "brain" survives a
container getting killed mid-run.

## Run it

```bash
docker compose up --build --scale worker=3
```

First build will take a minute or two (Go module downloads). Three control
planes come up on `http://localhost:8001`, `:8002`, `:8003`.

## Try the basic flow

Submit a reliable job (any replica works):
```bash
curl -X POST localhost:8001/jobs \
  -H "Content-Type: application/json" \
  -d '{"task_name": "sleep_task", "payload": {"seconds": 2}}'
```

Submit a flaky job to watch retries happen:
```bash
curl -X POST localhost:8001/jobs \
  -H "Content-Type: application/json" \
  -d '{"task_name": "flaky_task", "payload": {"fail_rate": 0.7}, "max_retries": 5}'
```

Check status (swap in the id returned above), against any replica:
```bash
curl localhost:8001/jobs/<id>
curl localhost:8002/jobs/<id>
curl localhost:8003/jobs/<id>
```

## See who's leader

```bash
curl localhost:8001/leader
curl localhost:8002/leader
curl localhost:8003/leader
```

Exactly one should say `"is_leader": true`.

## The HA demo: kill the leader

1. Check which replica is currently leader with the `/leader` calls above.
2. Kill its container - say it's `controlplane-2`:
   ```bash
   docker compose kill controlplane-2
   ```
3. Within a few seconds, check `/leader` on the two survivors. One of them
   will have flipped to `true` - Postgres released the advisory lock the
   instant that container's connection died, and the next replica's
   election loop grabbed it.
4. Submit a flaky job during/after the kill and watch it still retry and
   resolve normally - the worker's client-side failover means it never
   depended on the dead replica anyway.
5. Bring it back if you want to see it rejoin as a follower:
   ```bash
   docker compose up -d controlplane-2
   ```

Watch it happen live across all three:
```bash
docker compose logs -f controlplane-1 controlplane-2 controlplane-3
```
Look for `ACQUIRED LEADERSHIP` / `LOST LEADERSHIP` lines.

## Job lifecycle

```
PENDING -> QUEUED -> RUNNING -> SUCCESS
                          |
                          v
                  FAILED_RETRY_PENDING --(backoff elapses, leader only)--> QUEUED
                          |
                          v (retries exhausted)
                        FAILED
```

## Architecture

- **Control plane** (`controlplane/`) - Go HTTP service, 3 replicas. Owns
  all state in Postgres. Any replica can accept submissions and report
  results (safe to duplicate), but only the elected **leader** runs the
  retry-scheduler goroutine - running that on multiple replicas at once
  would double-requeue the same job.
- **Leader election** (`controlplane/leader.go`) - each replica holds a
  dedicated Postgres connection and repeatedly attempts
  `pg_try_advisory_lock`. Whoever holds the connection holds the lock;
  the connection dying (crash, kill, network drop) releases the lock
  automatically, no custom failure detection needed.
- **Worker** (`worker/`) - polls Redis with `BRPOP`, executes the named
  task, reports back to whichever control-plane replica answers first
  (`CONTROL_PLANE_URLS`, tried in order). Holds no state of its own.
- **Redis** - the queue, decouples control plane from workers.
- **Postgres** - source of truth for job state, and the leader-election
  mechanism.

## What's next (not built yet)

- **Chaos mode** - a dashboard button that triggers `docker kill` on a
  random worker or the current leader live, so the self-healing is a
  one-click demo instead of a manual `docker compose kill`.
- **Live dashboard** - WebSocket-pushed view of queue depth, job states,
  and which replica is currently leader.
- **Load balancer / single entrypoint** - right now clients pick a replica
  themselves (`:8001`/`:8002`/`:8003`); a thin reverse proxy would give
  one stable address that survives any single replica dying, same as
  workers already get via client-side failover.

