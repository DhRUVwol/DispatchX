"""Unit test for event persistence, independent of Docker/PostgreSQL."""
from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import app.history as history


def test_append_and_read_history(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    history.Base.metadata.create_all(engine)
    monkeypatch.setattr(history, "SessionLocal", sessionmaker(engine))
    now = datetime.now(timezone.utc)
    task = {
        "task_id": "abcd1234",
        "task_type": "fibonacci",
        "status": "completed",
        "worker_id": "worker-1",
        "attempt_count": 1,
        "retry_count": 0,
        "updated_at": now,
        "result": {"n": 10, "value": 55},
    }
    history.append_event("completed", task)
    events = history.get_job_history("abcd1234")
    assert len(events) == 1
    assert events[0]["event_type"] == "completed"
    assert events[0]["snapshot"]["result"]["value"] == 55
