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

## Workflows (DAG execution)

Submit a whole pipeline of tasks with dependencies in one call - tasks that
depend on others sit BLOCKED until every dependency succeeds, and get
automatically SKIPPED (not left hanging forever) if a dependency
permanently fails.

```bash
curl -X POST localhost:8001/workflows \
  -H "Content-Type: application/json" \
  -d '{
    "name": "training-pipeline",
    "tasks": [
      {"id": "fetch", "task_name": "sleep_task", "payload": {"seconds": 2}},
      {"id": "train", "task_name": "flaky_task", "payload": {"fail_rate": 0.4}, "depends_on": ["fetch"], "max_retries": 5},
      {"id": "evaluate", "task_name": "sleep_task", "payload": {"seconds": 1}, "depends_on": ["train"]}
    ]
  }'
```

Check the whole pipeline's status (swap in the workflow_id from the response above):
```bash
curl localhost:8001/workflows/<workflow_id>
```

Submitting a graph with a cycle (A depends on B, B depends on A) is
rejected at submission time with a 400 - the server runs a DFS cycle check
before inserting anything, so you can never get a workflow that's
structurally impossible to complete.

The easiest way to see this live: open the dashboard and click **Submit
3-Stage Pipeline** in the bottom-right panel, then watch the Recent Jobs
table - `train` sits `BLOCKED` until `fetch` hits `SUCCESS`, and if
`train` exhausts its retries and lands on `FAILED`, `evaluate` flips
straight to `SKIPPED` instead of waiting around.

## The dashboard

Open **http://localhost:8001** (or :8002 / :8003 - any replica serves it)
in a browser. You get:
- A topology strip showing all 3 replicas and which one currently holds
  the leader lock, live
- A worker grid pulled straight from the Docker API, each with its own
  kill button
- Queue depth and job status counts, updating every 2s
- A recent-jobs table
- A form to submit test jobs without touching curl
- Two chaos buttons: **Kill Random Worker** and **Kill Leader**

Click "Kill Leader" and watch the topology strip's glowing node jump to
a different replica within a couple seconds - that's the whole point of
Part 2, made visible instead of something you have to `curl /leader` to see.

## See who's leader from the command line

```bash
curl localhost:8001/leader
curl localhost:8002/leader
curl localhost:8003/leader
```

Exactly one should say `"is_leader": true`.

## The HA demo, manual version

If you'd rather trigger this from the terminal instead of the dashboard
buttons:
1. Check which replica is currently leader with the `/leader` calls above.
2. Kill its container - say it's `controlplane-2`:
   ```bash
   docker compose kill controlplane-2
   ```
3. Within a few seconds, check `/leader` on the two survivors. One of them
   will have flipped to `true`.
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

Workflow tasks with dependencies additionally start in:

  BLOCKED --(all deps SUCCESS, leader only)--> QUEUED
     |
     v (any dep FAILED or SKIPPED)
  SKIPPED
```

## What's next (not built yet)

- **Live dashboard over WebSockets** - right now it's 2-second polling,
  which is simple and fine but not truly real-time.
- **Load balancer / single entrypoint** - clients currently pick a replica
  themselves (`:8001`/`:8002`/`:8003`); a thin reverse proxy would give
  one stable address that survives any single replica dying, same as
  workers already get via client-side failover.
- **DAG visualization** - render the dependency graph itself in the
  dashboard, not just a flat table of jobs tagged with a workflow_id.

