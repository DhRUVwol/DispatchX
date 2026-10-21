"""FastAPI application exposing task submission and status endpoints."""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, HTTPException, Header, Query
from fastapi.requests import Request
from sqlalchemy.exc import SQLAlchemyError
from app.recurring import RecurringStore
from fastapi.responses import HTMLResponse, JSONResponse

from app.models import TaskResponse, TaskSubmission, TaskStatus, RecurringSubmission, RecurringUpdate
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
def submit_task(submission: TaskSubmission, idempotency_key: str | None = Header(default=None, min_length=1, max_length=200)) -> JSONResponse:
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
        idempotency_key=idempotency_key,
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
    """Serve the local job management console."""
    return HTMLResponse(Path(__file__).with_name("dashboard.html").read_text())


@app.exception_handler(ValueError)
async def conflict_handler(request: Request, exc: ValueError):
    return JSONResponse(status_code=409, content={"detail": str(exc)})


@app.exception_handler(SQLAlchemyError)
async def history_unavailable(request: Request, exc: SQLAlchemyError):
    return JSONResponse(status_code=503, content={"detail": "History database unavailable; queued events will be retried."})


@app.get("/tasks")
def list_tasks(status: TaskStatus | None = None, task_type: str | None = None,
               search: str = Query(default="", max_length=100),
               offset: int = Query(default=0, ge=0), limit: int = Query(default=25, ge=1, le=100)):
    return get_task_store().list_tasks(status=status.value if status else None,
        task_type=task_type, search=search, offset=offset, limit=limit)


@app.get("/task-types")
def task_types():
    return {"items": available_task_types()}


def control(task_id, action):
    try:
        return get_task_store().control_task(task_id, action)
    except KeyError:
        raise HTTPException(404, "Task not found")


@app.post("/tasks/{task_id}/cancel", response_model=TaskResponse)
def cancel_task(task_id: str):
    return control(task_id, "cancel")


@app.post("/tasks/{task_id}/retry", response_model=TaskResponse)
def retry_task(task_id: str):
    return control(task_id, "retry")


@app.get("/schedules")
def list_schedules():
    return {"items": RecurringStore(get_task_store()).list()}


@app.post("/schedules", status_code=201)
def create_schedule(submission: RecurringSubmission):
    if get_task(submission.task.task_type) is None:
        raise HTTPException(400, "Unknown task type")
    return RecurringStore(get_task_store()).create(submission.name, submission.interval_seconds,
                                                 submission.task.model_dump(mode="json"))


@app.patch("/schedules/{schedule_id}")
def update_schedule(schedule_id: str, update: RecurringUpdate):
    try:
        return RecurringStore(get_task_store()).update(schedule_id, update.enabled)
    except KeyError:
        raise HTTPException(404, "Schedule not found")
