# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Per-device local logging levels (NX-P4a adapter half).

Additive: two singleton tables mirroring/owning the logging ``local-levels`` container
(console/monitor/module severity at the OC grain) — the SnmpSystemInfo shape.

* ``device_logging_levels`` — read mirror; one row per device, absent when the device
  sets no local level at all.
* ``logging_levels_intent`` — write-path intent; only severities present are owned.

Revision ID: a7e3c1f9d5b2
Revises: c9d4e2f1a3b5
Create Date: 2026-07-21
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "a7e3c1f9d5b2"
down_revision: str | Sequence[str] | None = "c9d4e2f1a3b5"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "device_logging_levels",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("console_severity", sa.String(length=16), nullable=True),
        sa.Column("monitor_severity", sa.String(length=16), nullable=True),
        sa.Column("module_severity", sa.String(length=16), nullable=True),
        sa.Column("last_refreshed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("refresh_source", sa.String(length=64), nullable=False),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_device_logging_levels_device_id"), "device_logging_levels", ["device_id"], unique=True)
    op.create_table(
        "logging_levels_intent",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("console_severity", sa.String(length=16), nullable=True),
        sa.Column("monitor_severity", sa.String(length=16), nullable=True),
        sa.Column("module_severity", sa.String(length=16), nullable=True),
        sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_apply_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_apply_error", sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_logging_levels_intent_device_id"), "logging_levels_intent", ["device_id"], unique=True)


def downgrade() -> None:
    op.drop_index(op.f("ix_logging_levels_intent_device_id"), table_name="logging_levels_intent")
    op.drop_table("logging_levels_intent")
    op.drop_index(op.f("ix_device_logging_levels_device_id"), table_name="device_logging_levels")
    op.drop_table("device_logging_levels")
