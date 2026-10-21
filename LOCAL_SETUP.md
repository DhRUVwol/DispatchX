# Running this local checkout

This checkout includes the reference task queue plus an existing PostgreSQL
history extension. Use both Compose files (the commands below do this for you).
Docker Desktop must be running. No host Python installation is needed.

```bash
cd /Users/dhruvsharma/Documents/DispatchX/distributed-task-queue
make up
```

Open:

- Dashboard: http://localhost:8000/dashboard
- Interactive API docs: http://localhost:8000/docs
- Metrics: http://localhost:8000/metrics

The dashboard supports submission, job search/filtering, result/history inspection,
retry/cancel actions, and recurring schedules. You can also open `/docs`, expand
`POST /tasks`, click **Try it out**, and execute this body:

```json
{"task_type": "fibonacci", "args": [20], "priority": "high"}
```

Copy the returned `task_id` into `GET /tasks/{task_id}` to see its result.
`GET /history/tasks/{task_id}` shows its PostgreSQL event history.

## Day-to-day commands

```bash
make status  # Show services
make logs    # Follow API and worker logs; Ctrl-C exits log viewing
make stop    # Stop services without deleting containers or stored data
make up      # Rebuild and restart, including after editing Python files
make test    # Existing automated suite, using reserved Redis database 15
make smoke   # Submit demo jobs to the live stack and check their outcomes
```

`make test` refuses to run if database 15 is nonempty, because the existing
test fixtures clear that database. Keep database 15 reserved for tests and run
only one test suite at a time. Live jobs use database 0.

`make smoke` exercises all six handlers, high/medium/low submissions, scheduling,
retry success, exhausted retries, timeouts, API validation, monitoring, and
PostgreSQL history. It leaves ten demo tasks, including two intentionally
dead-lettered tasks, so these will appear in dashboard totals. Its input files,
resized image, and JSON report are saved under `data/smoke-<run-id>/`.
Priority ordering and FIFO behavior are checked by the automated suite.

## Shared files and services

Host files inside `data/` appear at `/data/` inside the API and all workers.
Use those container paths when submitting `image_resize` or file-based
`word_count` tasks. Output files also appear in the host's `data/` directory.

The stack runs the API, three workers, Redis, PostgreSQL, and a history relay. A short-lived
`migrate` service exits successfully after applying database migrations; this
is expected. PostgreSQL history has a named volume. Redis uses append-only persistence
(`appendfsync everysec`) in `data/redis/`. Keep this directory when recreating
containers. A power failure can still lose approximately the last second of
writes; persistence is not a backup. History events enter a Redis stream in the
same transaction as the active job transition. The history relay retries SQL
delivery and deduplicates replays. See [FEATURES.md](FEATURES.md) for semantics.

## Adding features

- Register task handlers in `app/task_registry.py`.
- Add API routes in `app/main.py` and request/response fields in `app/models.py`.
- Queue state is managed by `app/store.py`; execution by `app/worker.py`.
- PostgreSQL history lives in `app/history.py` and `app/history_store.py`.

After changes, run `make up`, then `make test` and `make smoke`.
