"""Create append-only job events table.

Revision ID: 0001_job_events
Revises:
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "0001_job_events"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "job_events",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("job_id", sa.String(length=36), nullable=False),
        sa.Column("event_type", sa.String(length=40), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("worker_id", sa.String(length=128), nullable=True),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("retry_count", sa.Integer(), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("snapshot", JSONB(), nullable=False),
    )
    op.create_index("ix_job_events_job_time", "job_events", ["job_id", "occurred_at"])


def downgrade() -> None:
    op.drop_index("ix_job_events_job_time", table_name="job_events")
    op.drop_table("job_events")
