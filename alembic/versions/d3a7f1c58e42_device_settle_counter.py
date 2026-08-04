# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The per-device settle counter and ``jobs.settle_seq`` (Appendix S §3.3).

Terminal jobs have to be orderable per device in COMMIT order, and nothing already on the
row can express that: insert order is unusable (admission inserts under a SAVEPOINT),
``created_at`` is transaction time, and a bare sequence hands values out in allocation
order while the transactions commit in whatever order they finish. Only a counter row whose
lock is held to COMMIT converts one into the other.

The counter row is created WITH its device and cascaded away with it, and this migration
backfills one for every device that already exists. It is never created from a terminal
transaction: the first insert's FK check takes ``FOR KEY SHARE`` on ``devices``, offboard
holds ``devices FOR UPDATE`` and then reaches for ``jobs``, and a lazy insert would close
that cycle into a deadlock.

Revision ID: d3a7f1c58e42
Revises: b2d9f4a71c63
Create Date: 2026-08-05
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "d3a7f1c58e42"
down_revision: str | Sequence[str] | None = "b2d9f4a71c63"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "device_settle_counter",
        sa.Column("device_id", sa.BigInteger(), nullable=False),
        sa.Column("last_seq", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("device_id"),
    )
    # Every device that predates the counter gets one here, so no terminal write ever has to
    # create it. The sweep repairs anything a future insert site forgets.
    op.execute("INSERT INTO device_settle_counter (device_id, last_seq) SELECT id, 0 FROM devices")

    op.add_column("jobs", sa.Column("settle_seq", sa.BigInteger(), nullable=True))
    op.create_index(
        "uq_job_settle_seq_per_device",
        "jobs",
        ["device_id", "settle_seq"],
        unique=True,
        postgresql_where=sa.text("settle_seq IS NOT NULL"),
    )
    op.create_index(
        "ix_job_device_settle_seq",
        "jobs",
        ["device_id", "settle_seq"],
        unique=False,
        postgresql_where=sa.text("settle_seq IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_job_device_settle_seq", table_name="jobs")
    op.drop_index("uq_job_settle_seq_per_device", table_name="jobs")
    op.drop_column("jobs", "settle_seq")
    op.drop_table("device_settle_counter")
