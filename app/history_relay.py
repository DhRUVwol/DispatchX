"""Retry durable history delivery; acknowledge only after PostgreSQL commits."""
import json
import logging
import time

from app.history import append_event
from app.history_store import history_task_store

logger = logging.getLogger(__name__)


def deliver_history(store, limit=100):
    delivered = 0
    for event_id, event in store.redis.xrange(store.outbox_key, count=limit):
        snapshot = store._deserialize_task(json.loads(event["snapshot"]))
        # Unique SQL key makes replay safe if the relay dies after commit.
        append_event(event["event_type"], snapshot, event_id=f"{store.queue_name}:{event_id}")
        store.redis.xdel(store.outbox_key, event_id)
        delivered += 1
    return delivered


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    while True:
        try:
            if not deliver_history(history_task_store):
                time.sleep(0.5)
        except Exception:
            logger.exception("History delivery failed; events retained for retry")
            time.sleep(5)
