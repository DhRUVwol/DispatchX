"""Interval schedules shared by workers, with atomic occurrence creation.

Missed intervals coalesce into one run. Pausing prevents future occurrences;
previously submitted jobs remain independently controllable.
"""
import json
import uuid
from datetime import datetime, timezone

import redis


class RecurringStore:
    def __init__(self, store):
        self.store = store
        self.redis = store.redis
        self.key = store.queue_name + ":recurring"

    def create(self, name, interval_seconds, task):
        if task.get("run_at"):
            raise ValueError("Recurring tasks use their interval; omit task.run_at")
        now = datetime.now(timezone.utc).timestamp()
        schedule = {"id": str(uuid.uuid4()), "name": name, "interval_seconds": interval_seconds,
                    "task": task, "enabled": True, "next_run": now + interval_seconds,
                    "created_at": datetime.fromtimestamp(now, timezone.utc).isoformat(),
                    "last_task_id": None}
        self.redis.hset(self.key, schedule["id"], json.dumps(schedule))
        return schedule

    def list(self):
        return sorted((json.loads(v) for v in self.redis.hvals(self.key)), key=lambda s: s["created_at"], reverse=True)

    def update(self, schedule_id, enabled):
        while True:
            with self.redis.pipeline() as pipe:
                try:
                    pipe.watch(self.key)
                    raw = pipe.hget(self.key, schedule_id)
                    if not raw:
                        raise KeyError(schedule_id)
                    schedule = json.loads(raw)
                    if enabled and not schedule["enabled"]:
                        schedule["next_run"] = datetime.now(timezone.utc).timestamp() + schedule["interval_seconds"]
                    schedule["enabled"] = enabled
                    pipe.multi()
                    pipe.hset(self.key, schedule_id, json.dumps(schedule))
                    pipe.execute()
                    return schedule
                except redis.WatchError:
                    continue

    def tick(self):
        # A WATCH transaction on the schedules hash coordinates ALL workers and
        # pause requests. Occurrence creation and next_run advance commit together.
        count = 0
        for candidate in self.list():
            with self.redis.pipeline() as pipe:
                try:
                    pipe.watch(self.key)
                    raw = pipe.hget(self.key, candidate["id"])
                    if not raw:
                        continue
                    schedule = json.loads(raw)
                    seconds, micros = pipe.time()
                    now = seconds + micros / 1_000_000
                    if not schedule["enabled"] or schedule["next_run"] > now:
                        continue
                    task = self.store.create_task(**schedule["task"], _pipeline=pipe)
                    schedule["next_run"] += (int((now - schedule["next_run"]) // schedule["interval_seconds"]) + 1) * schedule["interval_seconds"]
                    schedule["last_task_id"] = task["task_id"]
                    pipe.hset(self.key, schedule["id"], json.dumps(schedule))
                    pipe.execute()
                    count += 1
                except redis.WatchError:
                    continue
        return count
