# SPDX-License-Identifier: Apache-2.0
"""m14_isis_tables

Revision ID: a8b9c0d1e2f3
Revises: f7a8b9c0d1e2
Create Date: 2026-06-29 09:00:00.000000

Adds IS-IS interface enablement tables (M14 A2):
  - device_isis_process   (IS-IS processes read mirror from network-state-export)
  - device_isis_interface (IS-IS-enabled interface read mirror from network-state-export)
  - isis_interface_intent (write-path intent accepted by NetBox operator)
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "a8b9c0d1e2f3"
down_revision: str | None = "f7a8b9c0d1e2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "device_isis_process",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("process_tag", sa.String(128), nullable=False, server_default=""),
        sa.Column("net", sa.String(64), nullable=True),
        sa.Column("is_type", sa.String(32), nullable=True),
        sa.Column("last_refreshed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("refresh_source", sa.String(32), nullable=False, server_default="never"),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("device_id", "process_tag", name="uq_deviceisisprocess_identity"),
    )
    op.create_index("ix_device_isis_process_device_id", "device_isis_process", ["device_id"])

    op.create_table(
        "device_isis_interface",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("interface_name", sa.String(128), nullable=False),
        sa.Column("af", sa.String(8), nullable=False),
        sa.Column("process_tag", sa.String(128), nullable=False, server_default=""),
        sa.Column("circuit_type", sa.String(32), nullable=True),
        sa.Column("network_type", sa.String(32), nullable=True),
        sa.Column("metric", sa.Integer(), nullable=True),
        sa.Column("passive", sa.Boolean(), nullable=False, server_default="0"),
        sa.Column("last_refreshed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("refresh_source", sa.String(32), nullable=False, server_default="never"),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "device_id", "interface_name", "af", name="uq_deviceisisinterface_identity"
        ),
    )
    op.create_index("ix_device_isis_interface_device_id", "device_isis_interface", ["device_id"])

    op.create_table(
        "isis_interface_intent",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("interface_name", sa.String(128), nullable=False),
        sa.Column("af", sa.String(8), nullable=False),
        sa.Column("process_tag", sa.String(128), nullable=False, server_default=""),
        sa.Column("circuit_type", sa.String(32), nullable=True),
        sa.Column("network_type", sa.String(32), nullable=True),
        sa.Column("metric", sa.Integer(), nullable=True),
        sa.Column("passive", sa.Boolean(), nullable=False, server_default="0"),
        sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_apply_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_apply_error", sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "device_id", "interface_name", "af", name="uq_isisinterfaceintent_identity"
        ),
    )
    op.create_index("ix_isis_interface_intent_device_id", "isis_interface_intent", ["device_id"])


def downgrade() -> None:
    op.drop_index("ix_isis_interface_intent_device_id", table_name="isis_interface_intent")
    op.drop_table("isis_interface_intent")
    op.drop_index("ix_device_isis_interface_device_id", table_name="device_isis_interface")
    op.drop_table("device_isis_interface")
    op.drop_index("ix_device_isis_process_device_id", table_name="device_isis_process")
    op.drop_table("device_isis_process")
