# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""snmp v3 user intent protocols/group + host intent port

A v3 user without auth/priv protocols is unusable on-device (the snmp-reconciler
skips the auth/priv legs entirely), and nothing in the stack carried them. Adds
group_name/auth_protocol/priv_protocol to snmp_v3_user_intent and the optional
UDP port to snmp_host_intent, matching the snmp-reconciler YANG leaves.

Revision ID: a1d7f3c9e582
Revises: b3e8c1a52f47
Create Date: 2026-07-06 19:20:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a1d7f3c9e582"
down_revision: str | Sequence[str] | None = "b3e8c1a52f47"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column("snmp_v3_user_intent", sa.Column("group_name", sa.String(length=128), nullable=True))
    op.add_column("snmp_v3_user_intent", sa.Column("auth_protocol", sa.String(length=16), nullable=True))
    op.add_column("snmp_v3_user_intent", sa.Column("priv_protocol", sa.String(length=16), nullable=True))
    op.add_column("snmp_host_intent", sa.Column("port", sa.Integer(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("snmp_host_intent", "port")
    op.drop_column("snmp_v3_user_intent", "priv_protocol")
    op.drop_column("snmp_v3_user_intent", "auth_protocol")
    op.drop_column("snmp_v3_user_intent", "group_name")
