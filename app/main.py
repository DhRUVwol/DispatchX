"""FastAPI application exposing task submission and status endpoints."""

from __future__ import annotations

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse

from app.models import TaskResponse, TaskSubmission
from app.history_store import history_task_store as task_store
from app.history import get_job_history
from app.task_registry import available_task_types, get_task

app = FastAPI(
    title="Distributed Task Queue",
    description=(
        "A simplified Celery-like task queue using FastAPI, Redis, and "
        "multiple worker processes. Submit tasks via POST and poll status via GET."
    ),
    version="0.2.0",
)


def get_task_store():
    """Small indirection that makes the API easier to override in tests."""
    return task_store


@app.post(
    "/tasks",
    response_model=TaskResponse,
    status_code=201,
    summary="Submit a new task",
)
def submit_task(submission: TaskSubmission) -> JSONResponse:
    """
    Create a task record and queue it for a worker.

    Request example:
        {
          "task_type": "fibonacci",
          "args": [10],
          "kwargs": {}
        }
    """
    if get_task(submission.task_type) is None:
        available = ", ".join(available_task_types())
        raise HTTPException(
            status_code=400,
            detail=(
                f"Unknown task_type '{submission.task_type}'. "
                f"Available task types: {available}"
            ),
        )

    task_data = get_task_store().create_task(
        task_type=submission.task_type,
        args=submission.args,
        kwargs=submission.kwargs,
        timeout_seconds=submission.timeout_seconds,
        priority=submission.priority,
        run_at=submission.run_at,
    )
    response = TaskResponse(**task_data)
    return JSONResponse(content=response.model_dump(mode="json"), status_code=201)


@app.get(
    "/tasks/{task_id}",
    response_model=TaskResponse,
    summary="Get task status",
    responses={404: {"description": "Task not found"}},
)
def get_task_status(task_id: str) -> TaskResponse:
    """Return the latest stored state for a task."""
    task_data = get_task_store().get_task(task_id)
    if task_data is None:
        raise HTTPException(
            status_code=404,
            detail=f"Task '{task_id}' not found. It may not exist or may have expired.",
        )
    return TaskResponse(**task_data)


@app.get("/history/tasks/{task_id}", summary="Get PostgreSQL task history")
def get_persisted_task_history(task_id: str) -> dict:
    events = get_job_history(task_id)
    if not events:
        raise HTTPException(status_code=404, detail="No persistent history for this task")
    return {"task_id": task_id, "events": events}


@app.get("/metrics", summary="Get queue metrics")
def get_metrics() -> dict:
    """Return a lightweight operational snapshot for the queue system."""
    return get_task_store().get_metrics()


@app.get("/dashboard", response_class=HTMLResponse, summary="View dashboard")
def get_dashboard() -> HTMLResponse:
    """Serve a minimal local dashboard backed by the JSON metrics endpoint."""
    html = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Distributed Task Queue Dashboard</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, sans-serif; margin: 2rem; background: #f4f6f8; color: #102030; }
    h1 { margin-bottom: 0.5rem; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 1rem; margin-top: 1rem; }
    .card { background: white; border-radius: 12px; padding: 1rem; box-shadow: 0 4px 14px rgba(0,0,0,0.08); }
    pre { background: #0f172a; color: #e2e8f0; padding: 1rem; border-radius: 12px; overflow: auto; }
    .muted { color: #52606d; }
  </style>
</head>
<body>
  <h1>Distributed Task Queue Dashboard</h1>
  <p class="muted">Refreshes every 2 seconds using <code>/metrics</code>.</p>
  <div id="cards" class="grid"></div>
  <h2>Raw Metrics</h2>
  <pre id="raw">Loading...</pre>
  <script>
    async function refresh() {
      const response = await fetch('/metrics');
      const data = await response.json();
      const cards = [
        ['Pending', data.status_counts.pending],
        ['Scheduled', data.status_counts.scheduled],
        ['Running', data.status_counts.running],
        ['Completed', data.status_counts.completed],
        ['Dead-Lettered', data.status_counts.dead_lettered],
        ['Active Workers', data.workers.active_count]
      ];
      document.getElementById('cards').innerHTML = cards
        .map(([label, value]) => `<div class="card"><strong>${label}</strong><div style="font-size:2rem;margin-top:0.5rem;">${value}</div></div>`)
        .join('');
      document.getElementById('raw').textContent = JSON.stringify(data, null, 2);
    }
    refresh();
    setInterval(refresh, 2000);
  </script>
</body>
</html>
"""
    return HTMLResponse(content=html)
