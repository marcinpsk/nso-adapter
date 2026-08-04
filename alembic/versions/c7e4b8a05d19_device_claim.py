# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The exclusive per-device execution claim.

One row per device, with ``device_id`` as the primary key: that is what makes
``INSERT … ON CONFLICT (device_id) DO NOTHING`` the mutual exclusion, decided by
PostgreSQL across connections and processes.

A transaction advisory lock cannot serve here — both the apply and removal runners
commit several times mid-run, and a transaction-scoped lock is released at the first
COMMIT. A session-level advisory lock survives commits but binds the claim to one
pooled connection, is invisible to any observer not holding it, and evaporates on a
pool reconnect without the holder noticing.

``claim_token`` is unique because it is per-ACQUISITION: writes validate it under a
row lock, so a reused token would let a revoked holder's write validate against its
successor's claim.

``job_id`` is nullable and carries ON DELETE SET NULL — losing the job must not
silently free the device.

Revision ID: c7e4b8a05d19
Revises: d5f2a9b16e83
Create Date: 2026-07-30
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "c7e4b8a05d19"
down_revision: str | Sequence[str] | None = "d5f2a9b16e83"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "device_claim",
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("claim_token", sa.String(length=64), nullable=False),
        sa.Column("purpose", sa.String(length=16), nullable=False),
        sa.Column("job_id", sa.Integer(), nullable=True),
        sa.Column("acquired_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint(
            "purpose IN ('job', 'intent_put', 'teardown', 'sweep', 'failover')",
            name="ck_device_claim_purpose",
        ),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("device_id"),
        sa.UniqueConstraint("claim_token", name="uq_device_claim_token"),
    )


def downgrade() -> None:
    op.drop_table("device_claim")
