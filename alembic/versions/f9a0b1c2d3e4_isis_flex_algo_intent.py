"""add isis_flex_algo_intent (IS-IS Flex-Algorithm write path)

Revision ID: f9a0b1c2d3e4
Revises: e8f9a0b1c2d3
Create Date: 2026-06-11

Write-path intent mirror for IS-IS Flex-Algorithm definitions the operator
accepted. The single device Apply commits these via the isis-reconciler service
(flex-algo list under process-config).
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "f9a0b1c2d3e4"
down_revision = "e8f9a0b1c2d3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "isis_flex_algo_intent",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("process_tag", sa.String(length=128), nullable=False),
        sa.Column("algo_id", sa.Integer(), nullable=False),
        sa.Column("metric_type", sa.String(length=40), nullable=True),
        sa.Column("priority", sa.Integer(), nullable=True),
        sa.Column("admin_group_exclude", sa.String(length=200), nullable=True),
        sa.Column("admin_group_include_any", sa.String(length=200), nullable=True),
        sa.Column("admin_group_include_all", sa.String(length=200), nullable=True),
        sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_apply_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_apply_error", sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "device_id", "process_tag", "algo_id", name="uq_isisflexalgointent_identity"
        ),
    )
    op.create_index(
        op.f("ix_isis_flex_algo_intent_device_id"),
        "isis_flex_algo_intent",
        ["device_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_isis_flex_algo_intent_device_id"), table_name="isis_flex_algo_intent"
    )
    op.drop_table("isis_flex_algo_intent")
