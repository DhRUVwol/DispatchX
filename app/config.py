"""Environment-driven configuration for the task queue project."""

from __future__ import annotations

import os


class Settings:
    """
    Centralized configuration.

    The project can be run locally, in Docker Compose, or under pytest. Keeping
    every Redis setting here lets each process derive the same connection info
    from environment variables instead of hard-coding hostnames.
    """

    def __init__(self) -> None:
        self.redis_host = os.getenv("REDIS_HOST", "localhost")
        self.redis_port = int(os.getenv("REDIS_PORT", "6379"))
        self.redis_password = os.getenv("REDIS_PASSWORD", "")
        self.redis_db = int(os.getenv("REDIS_DB", "0"))
        self.queue_name = os.getenv("QUEUE_NAME", "task_queue")
        self.dead_letter_queue_name = os.getenv("DEAD_LETTER_QUEUE_NAME", "task_queue_dead_letter")
        self.scheduled_queue_name = os.getenv("SCHEDULED_QUEUE_NAME", f"{self.queue_name}:scheduled")

        # MAX_RETRIES means "how many retries after the initial failure".
        # Example: MAX_RETRIES=3 allows attempt #1 plus 3 retries, for 4 total
        # executions before the task becomes permanently failed.
        self.max_retries = int(os.getenv("MAX_RETRIES", "3"))
        self.default_task_timeout = int(os.getenv("DEFAULT_TASK_TIMEOUT", "30"))
        self.worker_poll_timeout = int(os.getenv("WORKER_POLL_TIMEOUT", "1"))
        self.worker_heartbeat_ttl = int(os.getenv("WORKER_HEARTBEAT_TTL", "30"))

        self.worker_recovery_grace = max(1, int(os.getenv("WORKER_RECOVERY_GRACE", "10")))

        # Optional one-shot URL override that is convenient in tests.
        self.redis_url_override = os.getenv("REDIS_URL")

    @property
    def redis_url(self) -> str:
        """Return a redis:// URL assembled from parts or REDIS_URL."""
        if self.redis_url_override:
            return self.redis_url_override

        if self.redis_password:
            return (
                f"redis://:{self.redis_password}@{self.redis_host}:"
                f"{self.redis_port}/{self.redis_db}"
            )
        return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"


settings = Settings()
