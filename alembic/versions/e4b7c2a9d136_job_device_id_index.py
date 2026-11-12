# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Index jobs by device and id.

Revision ID: e4b7c2a9d136
Revises: d8f2a6c1e904
"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "e4b7c2a9d136"
down_revision: str | Sequence[str] | None = "d8f2a6c1e904"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_index("ix_job_device_id", "jobs", ["device_id", "id"], unique=False)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ix_job_device_id", table_name="jobs")
