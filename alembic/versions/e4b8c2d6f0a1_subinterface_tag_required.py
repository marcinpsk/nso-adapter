# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Require explicit dot1q tags in subinterface mirror and intent.

Revision ID: e4b8c2d6f0a1
Revises: d1a7c3e9f2b5
Create Date: 2026-09-29
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "e4b8c2d6f0a1"
down_revision: str | Sequence[str] | None = "d1a7c3e9f2b5"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLES = ("device_subinterface", "subinterface_intent")


def upgrade() -> None:
    connection = op.get_bind()
    for table in _TABLES:
        connection.execute(sa.text(f"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE"))
        invalid = connection.execute(
            sa.text(f"SELECT device_id, interface_name FROM {table} WHERE dot1q_vlan IS NULL ORDER BY id LIMIT 1")
        ).first()
        if invalid is not None:
            raise RuntimeError(
                f"Cannot require {table}.dot1q_vlan: NULL tag at "
                f"device_id={invalid.device_id}, interface_name={invalid.interface_name}. "
                "Resolve this row explicitly, then rerun the migration."
            )
        op.alter_column(table, "dot1q_vlan", existing_type=sa.Integer(), nullable=False)


def downgrade() -> None:
    for table in reversed(_TABLES):
        op.alter_column(table, "dot1q_vlan", existing_type=sa.Integer(), nullable=True)
