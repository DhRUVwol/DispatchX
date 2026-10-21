"""Worker process that dequeues task IDs from Redis and executes handlers."""

from __future__ import annotations

import argparse
import json
import logging
import multiprocessing
import os
import queue
import signal
import sys
import uuid
from dataclasses import dataclass
from datetime import timedelta, timezone, datetime
from typing import Any, cast

import redis

from app.config import settings
from app.store import TaskStore
from app.history_store import AuditedTaskStore
from app.task_registry import available_task_types, get_task

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-5s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("worker")


@dataclass
class ExecutionOutcome:
    """Normalized outcome returned from the worker's execution wrapper."""

    succeeded: bool
    result: Any = None
    error_message: str | None = None
    failure_type: str | None = None
    failure_reason: str | None = None


def _load_task_payload(task_data: dict[str, str]) -> tuple[list[Any], dict[str, Any]]:
    """Deserialize JSON args/kwargs stored in the task hash."""
    try:
        task_args = json.loads(task_data.get("args", "[]"))
    except (json.JSONDecodeError, TypeError):
        task_args = []

    try:
        task_kwargs = json.loads(task_data.get("kwargs", "{}"))
    except (json.JSONDecodeError, TypeError):
        task_kwargs = {}

    return task_args, task_kwargs


def _run_task_in_subprocess(
    task_type: str,
    task_args: list[Any],
    task_kwargs: dict[str, Any],
    result_queue: multiprocessing.Queue,
) -> None:
    """Execute one handler inside a child process and push the outcome to a queue."""
    def safe_put(payload: dict[str, Any]) -> None:
        try:
            result_queue.put(payload)
        except Exception as exc:  # noqa: BLE001
            result_queue.put(
                {
                    "succeeded": False,
                    "failure_type": "serialization_error",
                    "failure_reason": "Worker could not serialize the task result or error payload.",
                    "error_message": f"{type(exc).__name__}: {exc}",
                }
            )

    task_handler = get_task(task_type)
    if task_handler is None:
        safe_put(
            {
                "succeeded": False,
                "failure_type": "unknown_task",
                "failure_reason": "No registered handler exists for this task type.",
                "error_message": (
                    f"Unknown task type: '{task_type}'. "
                    f"Available tasks: {available_task_types()}"
                ),
            }
        )
        return

    try:
        result = task_handler(*task_args, **task_kwargs)
        safe_put({"succeeded": True, "result": result})
    except Exception as exc:  # noqa: BLE001
        safe_put(
            {
                "succeeded": False,
                "failure_type": "exception",
                "failure_reason": f"Task handler raised {type(exc).__name__}.",
                "error_message": f"{type(exc).__name__}: {exc}",
            }
        )


def execute_task_with_timeout(
    task_type: str,
    task_args: list[Any],
    task_kwargs: dict[str, Any],
    timeout_seconds: int,
) -> ExecutionOutcome:
    """
    Run a task in a child process so hung handlers can be terminated safely.

    Threads cannot reliably stop arbitrary blocking Python code. A short-lived
    child process is heavier than inline execution, but it keeps the current
    architecture and gives the worker a robust way to enforce per-task timeouts.
    """
    ctx = multiprocessing.get_context("spawn")
    result_queue: multiprocessing.Queue = ctx.Queue()
    process = ctx.Process(
        target=_run_task_in_subprocess,
        args=(task_type, task_args, task_kwargs, result_queue),
    )
    process.start()
    process.join(timeout_seconds)

    if process.is_alive():
        process.terminate()
        process.join()
        result_queue.close()
        result_queue.join_thread()
        return ExecutionOutcome(
            succeeded=False,
            error_message=f"Task exceeded timeout of {timeout_seconds} seconds.",
            failure_type="timeout",
            failure_reason="Task exceeded its configured execution timeout.",
        )

    try:
        payload = result_queue.get_nowait()
        result_queue.close()
        result_queue.join_thread()
        return ExecutionOutcome(**payload)
    except queue.Empty:
        pass

    exitcode = process.exitcode
    result_queue.close()
    result_queue.join_thread()

    return ExecutionOutcome(
        succeeded=False,
        error_message=(
            "Task process exited without returning a result. "
            f"Child exit code: {exitcode}."
        ),
        failure_type="worker_error",
        failure_reason="Worker subprocess exited unexpectedly before reporting an outcome.",
    )


def process_task(
    task_id: str,
    redis_client: Any,
    store: TaskStore,
    worker_id: str,
) -> dict[str, Any] | None:
    """Execute one queued task and persist the result."""
    store.touch_worker_heartbeat(worker_id)
    task_key = f"task:{task_id}"
    task_data = cast(dict[str, str], redis_client.hgetall(task_key))
    if not task_data:
        logger.warning("[%s] Task %s not found in Redis - skipping", worker_id, task_id)
        return None

    task_type = task_data.get("task_type") or task_data.get("task_name", "unknown")
    retry_count = int(task_data.get("retry_count", "0"))
    max_retries = int(task_data.get("max_retries", str(settings.max_retries)))
    timeout_seconds = int(task_data.get("timeout_seconds", str(settings.default_task_timeout)))

    logger.info(
        "[%s] Picked up task %.8s: task_type='%s' attempt=%d retry=%d/%d timeout=%ss",
        worker_id,
        task_id,
        task_type,
        int(task_data.get("attempt_count", "0")) + 1,
        retry_count,
        max_retries,
        timeout_seconds,
    )

    running_task = store.mark_running(task_id, worker_id)
    if running_task is None:
        return None

    task_args, task_kwargs = _load_task_payload(task_data)
    task_kwargs["_retry_count"] = retry_count

    outcome = execute_task_with_timeout(
        task_type=task_type,
        task_args=task_args,
        task_kwargs=task_kwargs,
        timeout_seconds=timeout_seconds,
    )

    if outcome.succeeded:
        logger.info("[%s] Task %.8s COMPLETED - result=%s", worker_id, task_id, outcome.result)
        return store.mark_completed(task_id, outcome.result, worker_id)

    assert outcome.error_message is not None
    assert outcome.failure_type is not None
    assert outcome.failure_reason is not None

    next_retry_count = retry_count + 1

    if outcome.failure_type == "unknown_task":
        logger.error(
            "[%s] Task %.8s DEAD-LETTERED immediately - %s",
            worker_id,
            task_id,
            outcome.error_message,
        )
        return store.dead_letter_task(
            task_id,
            worker_id=worker_id,
            error_message=outcome.error_message,
            failure_type=outcome.failure_type,
            failure_reason=outcome.failure_reason,
        )

    if next_retry_count <= max_retries:
        backoff_seconds = 2 ** retry_count
        retry_at = datetime.now(timezone.utc) + timedelta(seconds=backoff_seconds)
        logger.warning(
            "[%s] Task %.8s FAILED - scheduling retry %d/%d at %s: %s",
            worker_id,
            task_id,
            next_retry_count,
            max_retries,
            retry_at.isoformat(),
            outcome.error_message,
        )
        store.record_failure(
            task_id,
            worker_id=worker_id,
            error_message=outcome.error_message,
            failure_type=outcome.failure_type,
            failure_reason=outcome.failure_reason,
        )
        store.schedule_retry(task_id, retry_count=next_retry_count, run_at=retry_at)
        logger.info(
            "[%s] Scheduled retry for task %.8s (retry %d/%d)",
            worker_id,
            task_id,
            next_retry_count,
            max_retries,
        )
        return store.get_task(task_id)

    logger.error(
        "[%s] Task %.8s DEAD-LETTERED after %d retries - %s",
        worker_id,
        task_id,
        retry_count,
        outcome.error_message,
    )
    return store.dead_letter_task(
        task_id,
        worker_id=worker_id,
        error_message=outcome.error_message,
        failure_type=outcome.failure_type,
        failure_reason=outcome.failure_reason,
    )


def worker_iteration(
    redis_client: Any,
    store: TaskStore,
    worker_id: str,
    queue_names: list[str],
    timeout: int = 0,
) -> bool:
    """Process at most one task from the queue and report whether work happened."""
    store.touch_worker_heartbeat(worker_id)
    promoted = store.promote_due_scheduled_tasks()
    if promoted:
        logger.info("[%s] Promoted %d scheduled task(s) into runnable queues", worker_id, promoted)

    result = redis_client.brpop(queue_names, timeout=timeout)
    if result is None:
        return False

    _, task_id = result
    process_task(task_id, redis_client, store, worker_id)
    return True


def run_worker(worker_id: str) -> None:
    """Run the infinite worker loop used by production and Docker containers."""
    store = AuditedTaskStore(
        redis_url=settings.redis_url,
        queue_name=settings.queue_name,
        dead_letter_queue_name=settings.dead_letter_queue_name,
        scheduled_queue_name=settings.scheduled_queue_name,
    )
    redis_client: Any = redis.from_url(settings.redis_url, decode_responses=True)

    logger.info("[%s] Starting - connected to %s", worker_id, settings.redis_url)
    logger.info("[%s] Listening on queues %s (BRPOP)...", worker_id, store.runnable_queue_names())

    while True:
        worker_iteration(
            redis_client,
            store,
            worker_id,
            store.runnable_queue_names(),
            timeout=settings.worker_poll_timeout,
        )


def _shutdown_handler(signum: int, frame: Any) -> None:
    sig_name = signal.Signals(signum).name
    logger.info("Received %s - shutting down gracefully...", sig_name)
    sys.exit(0)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Distributed Task Queue worker")
    parser.add_argument(
        "--id",
        dest="worker_id",
        type=str,
        default=None,
        help="Unique identifier that appears in worker logs and task metadata.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    signal.signal(signal.SIGINT, _shutdown_handler)
    signal.signal(signal.SIGTERM, _shutdown_handler)

    args = _parse_args()
    worker_id = args.worker_id or os.getenv("WORKER_ID") or f"worker-{uuid.uuid4().hex[:8]}"
    run_worker(worker_id)
