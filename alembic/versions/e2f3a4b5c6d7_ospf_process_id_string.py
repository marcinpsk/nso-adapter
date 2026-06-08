"""widen OSPF process_id from integer to string (IOS-XR named processes)

Revision ID: e2f3a4b5c6d7
Revises: c2d3e4f5a6b7
Create Date: 2026-06-08

IOS-XR (and Junos) allow named OSPF processes (e.g. "test", "DEVICES"), which do
not fit an integer process-id. Widen process_id to a string on every OSPF table.
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "e2f3a4b5c6d7"
down_revision = "c2d3e4f5a6b7"
branch_labels = None
depends_on = None

_TABLES = (
    "device_ospf_instance",
    "device_ospf_interface",
    "ospf_instance_intent",
    "ospf_interface_intent",
)


def upgrade() -> None:
    for table in _TABLES:
        op.alter_column(
            table,
            "process_id",
            existing_type=sa.Integer(),
            type_=sa.String(length=64),
            postgresql_using="process_id::varchar",
        )


def downgrade() -> None:
    for table in _TABLES:
        op.alter_column(
            table,
            "process_id",
            existing_type=sa.String(length=64),
            type_=sa.Integer(),
            postgresql_using="process_id::integer",
        )
