"""Audited wrapper around the original Redis TaskStore.

The original store remains responsible for job dispatch and current state.
After each mutation, we attempt to append an audit event to PostgreSQL.
"""
from __future__ import annotations

import logging
from typing import Any

from app.config import settings
from app.history import append_event
from app.store import TaskStore

logger = logging.getLogger("dispatchx.audit")


class AuditedTaskStore(TaskStore):
    def _audit(self, event_type: str, task: dict | None) -> None:
        if task is None:
            return
        try:
            append_event(event_type, task)
        except Exception:  # noqa: BLE001 - best-effort audit must not disrupt job execution
            # Demonstration-only, best-effort audit. Redis work must not fail
            # just because the history DB is temporarily unavailable.
            logger.exception("Could not persist %s for task %s", event_type, task.get("task_id"))

    def create_task(self, *args: Any, **kwargs: Any) -> dict:
        task = super().create_task(*args, **kwargs)
        self._audit("submitted", task)
        return task

    def mark_running(self, task_id: str, worker_id: str) -> dict | None:
        task = super().mark_running(task_id, worker_id)
        self._audit("attempt_started", task)
        return task

    def mark_completed(self, task_id: str, result: Any, worker_id: str) -> dict | None:
        task = super().mark_completed(task_id, result, worker_id)
        self._audit("completed", task)
        return task

    def record_failure(self, task_id: str, **kwargs: Any) -> dict | None:
        task = super().record_failure(task_id, **kwargs)
        self._audit("attempt_failed", task)
        return task

    def requeue_task(self, task_id: str, **kwargs: Any) -> dict | None:
        task = super().requeue_task(task_id, **kwargs)
        self._audit("requeued", task)
        return task

    def schedule_retry(self, task_id: str, **kwargs: Any) -> dict | None:
        task = super().schedule_retry(task_id, **kwargs)
        self._audit("retry_scheduled", task)
        return task

    def dead_letter_task(self, task_id: str, **kwargs: Any) -> dict | None:
        # super().dead_letter_task() calls self.record_failure() internally;
        # this intentionally records both failure and dead-letter events.
        task = super().dead_letter_task(task_id, **kwargs)
        self._audit("dead_lettered", task)
        return task

    def patch_task_metadata(self, task_id: str, **kwargs: Any) -> dict | None:
        task = super().patch_task_metadata(task_id, **kwargs)
        self._audit("metadata_updated", task)
        return task


history_task_store = AuditedTaskStore(
    redis_url=settings.redis_url,
    queue_name=settings.queue_name,
    dead_letter_queue_name=settings.dead_letter_queue_name,
    scheduled_queue_name=settings.scheduled_queue_name,
)
