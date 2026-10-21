"""
Data models shared by the API, store, and worker processes.

The system moves the same task record through several layers:

    client -> FastAPI -> Redis queue/hash -> worker -> Redis hash -> client

Pydantic models give us one contract for that data. FastAPI uses them for
request validation and response serialization, while the rest of the codebase
uses the same field names to avoid drift between layers.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import AliasChoices, BaseModel, Field, field_validator


class TaskStatus(str, Enum):
    """
    Lifecycle states a task can occupy.

    Notes:
    - `failed` is retained in the public contract for completeness and backward
      compatibility, although the current worker implementation dead-letters
      terminal failures instead of leaving them in `failed`.
    - `scheduled` means the task exists in Redis but is not yet eligible to run.
    """

    PENDING = "pending"
    SCHEDULED = "scheduled"
    RUNNING = "running"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    FAILED = "failed"
    DEAD_LETTERED = "dead_lettered"


class TaskPriority(str, Enum):
    """Supported queue priorities."""

    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class TaskSubmission(BaseModel):
    """
    Payload accepted by POST /tasks.

    `task_type` is the registry key workers use to locate a handler. We still
    accept the legacy field name `task_name` as an input alias so older demo
    payloads do not break, but the public API now documents `task_type`.
    """

    task_type: str = Field(
        ...,
        description="Registered task type the worker should execute.",
        examples=["dummy", "fibonacci", "image_resize"],
        validation_alias=AliasChoices("task_type", "task_name"),
    )
    args: List[Any] = Field(
        default_factory=list,
        description="Positional arguments passed to the task handler.",
        examples=[[10]],
    )
    kwargs: Dict[str, Any] = Field(
        default_factory=dict,
        description="Keyword arguments passed to the task handler.",
        examples=[{"text": "hello world"}],
    )
    priority: TaskPriority = Field(
        default=TaskPriority.MEDIUM,
        description="Queue priority used when choosing which runnable task executes next.",
        examples=["high", "medium", "low"],
    )
    run_at: Optional[datetime] = Field(
        default=None,
        description="Optional future timestamp; tasks do not become runnable before this time.",
    )
    timeout_seconds: Optional[int] = Field(
        default=None,
        ge=1,
        description=(
            "Optional per-task execution timeout. If omitted, the worker uses "
            "the configured default timeout."
        ),
        examples=[10],
    )

    @field_validator("run_at")
    @classmethod
    def validate_run_at_timezone(cls, value: Optional[datetime]) -> Optional[datetime]:
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("run_at must include a timezone offset, for example '+00:00'.")
        return value


class TaskResponse(BaseModel):
    """
    Task state returned by both POST /tasks and GET /tasks/{task_id}.

    Retry semantics:
    - retry_count counts how many retry attempts have already been scheduled.
    - max_retries is how many retries are allowed after the initial failure.
    - If max_retries=3, a task may run up to 4 total attempts.
    """

    task_id: str = Field(..., description="Unique UUID assigned to the task.")
    task_type: str = Field(..., description="Registry key of the task handler.")
    status: TaskStatus = Field(..., description="Current lifecycle state.")
    priority: TaskPriority = Field(
        default=TaskPriority.MEDIUM,
        description="Priority queue used for runnable execution order.",
    )
    run_at: Optional[datetime] = Field(
        default=None,
        description="Earliest time the task may be promoted into a runnable queue.",
    )
    result: Any = Field(
        default=None,
        description="Successful task result or permanent failure message.",
    )
    attempt_count: int = Field(
        default=0,
        description="How many execution attempts have started for this task.",
    )
    retry_count: int = Field(
        default=0,
        description="Number of retries already scheduled after failures.",
    )
    max_retries: int = Field(
        default=3,
        description="Maximum retries allowed after the initial failure.",
    )
    last_error: Any = Field(
        default=None,
        description="Most recent error captured from a failed attempt.",
    )
    failure_type: Optional[str] = Field(
        default=None,
        description="High-level failure category, such as exception or timeout.",
    )
    failure_reason: Optional[str] = Field(
        default=None,
        description="Short explanation of why the latest failure happened.",
    )
    first_failure_at: Optional[datetime] = Field(
        default=None,
        description="Timestamp of the first failed attempt.",
    )
    last_failure_at: Optional[datetime] = Field(
        default=None,
        description="Timestamp of the most recent failed attempt.",
    )
    dead_lettered_at: Optional[datetime] = Field(
        default=None,
        description="Timestamp when the task was pushed to the dead-letter queue.",
    )
    timeout_seconds: int = Field(
        default=30,
        description="Execution timeout applied to each task attempt in seconds.",
    )
    worker_id: Optional[str] = Field(
        default=None,
        description="Worker that last processed or is currently processing the task.",
    )
    created_at: datetime = Field(
        ...,
        description="Timestamp when the task was first submitted.",
    )
    updated_at: datetime = Field(
        ...,
        description="Timestamp when the task last changed state.",
    )


class RecurringSubmission(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    interval_seconds: int = Field(ge=10, le=31536000)
    task: TaskSubmission


class RecurringUpdate(BaseModel):
    enabled: bool
