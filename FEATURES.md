# Job management and reliability roadmap

Implemented locally: management dashboard, retry/cancel, idempotency,
crash-recovery tests, durable history delivery, Redis persistence, and interval
schedules. Open http://localhost:8000/dashboard or the API reference at `/docs`.

## Management APIs

| Request | Behavior |
| --- | --- |
| `GET /tasks?status=completed&task_type=fibonacci&search=abc&offset=0&limit=25` | Newest-first job listing; all filters optional; limit 1–100 |
| `GET /task-types` | Registered task names |
| `POST /tasks/{id}/cancel` | Cancel pending/scheduled work atomically; running/terminal jobs return 409 |
| `POST /tasks/{id}/retry` | Requeue dead-lettered/failed work with a fresh retry budget; other states return 409 |
| `POST /tasks` with `Idempotency-Key` header | Reusing the same key and equivalent payload returns the original job; changed payload returns 409 |
| `GET /schedules` | List recurring schedules |
| `POST /schedules` | Create an interval schedule |
| `PATCH /schedules/{id}` with `{"enabled": false}` | Pause; use `true` to resume |

A missing job/schedule returns 404. Manual retries keep the same job ID and
lifetime attempt count; existing historical events remain unchanged. Cancellation
removes runnable/scheduled entries and contributes to the `cancelled` metric.

Idempotency keys are scoped to the queue, are case-sensitive, and are retained
without expiration. They prevent duplicate *submissions*, not repeated side
effects during worker recovery. Omitted defaults and explicit equivalent defaults
match. Both first submission and replay currently return HTTP 201.

The dashboard safely renders task values as text. It supports submission,
filters, pagination, result/history details, controls, and schedule pause/resume.
History is eventually consistent; reopen the details dialog to refresh it.

## Recurring jobs

Example body for `POST /schedules`:

```json
{
  "name": "Hourly calculation",
  "interval_seconds": 3600,
  "task": {"task_type": "fibonacci", "args": [20], "priority": "low"}
}
```

Intervals range from 10 seconds to one year. The first run is one interval after
creation; `task.run_at` must be omitted. Workers check schedules between jobs.
Each occurrence creates a new task. Creation and the next-run advance are one
Redis transaction, preventing duplicate occurrences when workers race or die.
Missed intervals coalesce into one catch-up run. Pause does not cancel jobs
already created. Resume schedules the next run one interval from resumption.
This release implements fixed intervals, not cron expressions or workflows.

## Reliability

- Claims have execution tokens and leases; expired attempts can be recovered by
  another worker. Late results from a previous attempt cannot overwrite the new
  attempt. Delivery is at least once: handlers with side effects need their own
  deduplication.
- Active submission, claim, promotion, completion/failure/retry, recovery and
  management transitions write immutable event snapshots to a Redis stream
  atomically with task state. A separate `history-relay` service writes these
  events to PostgreSQL and removes them from the stream only after SQL commit.
- A unique SQL event ID makes relay replay safe, including a crash between SQL
  commit and Redis acknowledgment. During a database outage jobs continue running,
  events accumulate, and the relay retries. `/metrics` exposes
  `history_pending_events`. Events missed before this upgrade are not backfilled.
- Redis persists to `data/redis/` using AOF with one-second syncing. PostgreSQL
  keeps its existing named volume. Existing Redis records were preserved during
  the upgrade. Neither persistence mechanism substitutes for backups.

## Verification

Run `make test` for the original suite plus management, concurrent idempotency,
recurring-worker races, process-kill recovery, stale-result fencing, retry
exhaustion, and outage/replay tests. Tests use reserved Redis database 15 and
in-memory SQLite for history failure/replay behavior. `make smoke` verifies the
running API, workers, task handlers and PostgreSQL history.

## Deliberate limits

- No user accounts or authentication in this phase; this remains a local demo.
- Job listing scans Redis hashes before filtering/sorting. It supports existing
  records immediately; indexed queries are the next step for large job volumes.
- Schedules are evaluated between worker jobs, so a saturated worker pool can
  delay scheduled dispatch. Recurring jobs may overlap when an earlier occurrence
  runs longer than its interval.
- History backlog and retained jobs/idempotency keys have no automatic retention
  policy yet; monitor storage for long-lived deployments.
