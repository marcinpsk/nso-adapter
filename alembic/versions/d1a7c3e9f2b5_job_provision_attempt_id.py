# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Key provision jobs by the plugin's provision attempt id.

Revision ID: d1a7c3e9f2b5
Revises: c6e8a1f42d90
Create Date: 2026-09-28
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "d1a7c3e9f2b5"
down_revision: str | Sequence[str] | None = "c6e8a1f42d90"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "jobs"


def upgrade() -> None:
    op.add_column(_TABLE, sa.Column("provision_attempt_id", postgresql.UUID(as_uuid=True), nullable=True))
    op.create_unique_constraint("uq_job_provision_attempt_id", _TABLE, ["provision_attempt_id"])
    op.create_check_constraint(
        "ck_job_attempt_id_only_on_provision",
        _TABLE,
        "provision_attempt_id IS NULL OR job_type = 'provision'",
    )


def downgrade() -> None:
    op.drop_constraint("ck_job_attempt_id_only_on_provision", _TABLE, type_="check")
    op.drop_constraint("uq_job_provision_attempt_id", _TABLE, type_="unique")
    op.drop_column(_TABLE, "provision_attempt_id")
