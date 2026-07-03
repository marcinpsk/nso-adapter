# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""one active job per device (partial unique index)

enqueue_job's dedup was a check-then-insert (SELECT active, then INSERT) with no
DB-level guard, so a scheduler tick + SSE + API could each pass the check and
insert a second queued job for the same device (redundant syncs under worker
concurrency > 1). Add a partial UNIQUE index on jobs(device_id) restricted to the
active states so at most one queued/running job per device can exist. Excludes
removal jobs (enqueue_removal intentionally queues one per scope on the same
device) and provision jobs (device_id NULL → NULLs distinct, dedup by context).

Revision ID: 9d2f4e6a1c3b
Revises: 7c2e5a9d4b18
Create Date: 2026-07-02 12:55:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "9d2f4e6a1c3b"
down_revision: str | Sequence[str] | None = "7c2e5a9d4b18"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_index(
        "uq_job_active_per_device",
        "jobs",
        ["device_id"],
        unique=True,
        postgresql_where=sa.text("status IN ('queued', 'running') AND job_type <> 'removal'"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("uq_job_active_per_device", table_name="jobs")
