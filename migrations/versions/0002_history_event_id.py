"""Deduplicate retried history delivery."""
from alembic import op
import sqlalchemy as sa

revision = "0002_history_event_id"
down_revision = "0001_job_events"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("job_events", sa.Column("event_id", sa.String(160), nullable=True))
    op.create_unique_constraint("uq_job_events_event_id", "job_events", ["event_id"])


def downgrade():
    op.drop_constraint("uq_job_events_event_id", "job_events", type_="unique")
    op.drop_column("job_events", "event_id")
