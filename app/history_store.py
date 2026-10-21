"""Compatibility name for the store with atomic Redis history outbox writes.

PostgreSQL delivery runs independently in app.history_relay so a history
outage cannot block task dispatch or execution.
"""
from app.config import settings
from app.store import TaskStore


class AuditedTaskStore(TaskStore):
    pass


history_task_store = AuditedTaskStore(
    redis_url=settings.redis_url,
    queue_name=settings.queue_name,
    dead_letter_queue_name=settings.dead_letter_queue_name,
    scheduled_queue_name=settings.scheduled_queue_name,
)
