"""PostgreSQL append-only history for DispatchX (M1).

Redis remains the live queue/status store. The durable Redis outbox is
replayed into PostgreSQL by history_relay; delivery is eventually consistent.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

from fastapi.encoders import jsonable_encoder
from sqlalchemy import DateTime, Index, Integer, JSON, String, create_engine, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg://dispatchx:dispatchx_dev_password@postgres:5432/dispatchx",
)
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(engine, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


class JobEvent(Base):
    __tablename__ = "job_events"
    __table_args__ = (Index("ix_job_events_job_time", "job_id", "occurred_at"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_id: Mapped[str | None] = mapped_column(String(160), nullable=True, unique=True)
    job_id: Mapped[str] = mapped_column(String(36), nullable=False)
    event_type: Mapped[str] = mapped_column(String(40), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    worker_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False)
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    snapshot: Mapped[dict[str, Any]] = mapped_column(
        JSON().with_variant(JSONB(), "postgresql"), nullable=False
    )


def append_event(event_type: str, task: dict[str, Any], event_id: str | None = None) -> None:
    """Commit one event; replaying an outbox event is idempotent."""
    snapshot = jsonable_encoder(task)
    when = task.get("updated_at") or datetime.now(timezone.utc)
    if isinstance(when, str):
        when = datetime.fromisoformat(when)
    values = dict(event_id=event_id, job_id=str(task["task_id"]), event_type=event_type,
                  status=str(snapshot["status"]), worker_id=task.get("worker_id"),
                  attempt_count=int(task.get("attempt_count", 0)),
                  retry_count=int(task.get("retry_count", 0)), occurred_at=when, snapshot=snapshot)
    with SessionLocal.begin() as session:
        if event_id:
            if session.bind.dialect.name == "postgresql":
                from sqlalchemy.dialects.postgresql import insert
            else:
                from sqlalchemy.dialects.sqlite import insert
            session.execute(insert(JobEvent).values(**values).on_conflict_do_nothing(index_elements=["event_id"]))
        else:
            session.add(JobEvent(**values))


def get_job_history(task_id: str) -> list[dict[str, Any]]:
    with SessionLocal() as session:
        events = session.scalars(
            select(JobEvent)
            .where(JobEvent.job_id == task_id)
            .order_by(JobEvent.occurred_at, JobEvent.id)
        ).all()
        return [
            {
                "id": e.id,
                "event_type": e.event_type,
                "status": e.status,
                "worker_id": e.worker_id,
                "attempt_count": e.attempt_count,
                "retry_count": e.retry_count,
                "occurred_at": e.occurred_at.isoformat(),
                "snapshot": e.snapshot,
            }
            for e in events
        ]
