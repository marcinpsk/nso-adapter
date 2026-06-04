# SPDX-License-Identifier: Apache-2.0
"""add device_logging_host (logging/syslog read mirror)

network-state-export now exports remote syslog servers (logging hosts) per device
(IOS/Junos/Nokia). Mirror them on a read table the GET endpoint serves to the
plugin. Non-secret fields only: address/port/severity/facility/transport/vrf/source.

Ships as a proper incremental migration (never a baseline edit) so the container
entrypoint's ``alembic upgrade head`` applies it on start.

Revision ID: a7b8c9d0e1f2
Revises: f6a7b8c9d0e1
Create Date: 2026-06-04
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "a7b8c9d0e1f2"
down_revision: str | Sequence[str] | None = "f6a7b8c9d0e1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "device_logging_host",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("address", sa.String(length=256), nullable=False),
        sa.Column("port", sa.Integer(), nullable=True),
        sa.Column("severity", sa.String(length=32), nullable=True),
        sa.Column("facility", sa.String(length=32), nullable=True),
        sa.Column("transport", sa.String(length=16), nullable=True),
        sa.Column("vrf", sa.String(length=128), nullable=True),
        sa.Column("source", sa.String(length=256), nullable=True),
        sa.Column("last_refreshed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("refresh_source", sa.String(length=64), nullable=False),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("device_id", "address", name="uq_logginghost_device_address"),
    )
    op.create_index(op.f("ix_device_logging_host_device_id"), "device_logging_host", ["device_id"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_device_logging_host_device_id"), table_name="device_logging_host")
    op.drop_table("device_logging_host")
