"""add logging_host_intent (remote-syslog write path)

Revision ID: f3a4b5c6d7e8
Revises: e2f3a4b5c6d7
Create Date: 2026-06-09

Write-path intent mirror for remote syslog servers the NetBox operator accepted.
The single device Apply commits these via the logging-reconciler NSO service.
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "f3a4b5c6d7e8"
down_revision = "e2f3a4b5c6d7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "logging_host_intent",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("address", sa.String(length=256), nullable=False),
        sa.Column("port", sa.Integer(), nullable=True),
        sa.Column("severity", sa.String(length=32), nullable=False, server_default=""),
        sa.Column("facility", sa.String(length=32), nullable=False, server_default=""),
        sa.Column("transport", sa.String(length=16), nullable=False, server_default=""),
        sa.Column("vrf", sa.String(length=128), nullable=False, server_default=""),
        sa.Column("source", sa.String(length=256), nullable=False, server_default=""),
        sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_apply_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_apply_error", sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("device_id", "address", name="uq_logginghostintent_identity"),
    )
    op.create_index(op.f("ix_logging_host_intent_device_id"), "logging_host_intent", ["device_id"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_logging_host_intent_device_id"), table_name="logging_host_intent")
    op.drop_table("logging_host_intent")
