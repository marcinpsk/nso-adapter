# SPDX-License-Identifier: Apache-2.0
"""add device_isis_interface.hello_auth_type / hello_auth_present

network-state-export now exports per-interface IS-IS hello (IIH) authentication
(Nokia native + Junos apply-group). Mirror it secret-safe: the normalised type
(md5/text) and a present flag — the key itself is never stored.

Revision ID: d0e1f2a3b4c5
Revises: c9d0e1f2a3b4
Create Date: 2026-06-05
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "d0e1f2a3b4c5"
down_revision: str | Sequence[str] | None = "c9d0e1f2a3b4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("device_isis_interface", sa.Column("hello_auth_type", sa.String(length=32), nullable=True))
    op.add_column("device_isis_interface", sa.Column("hello_auth_present", sa.Boolean(), nullable=True))


def downgrade() -> None:
    op.drop_column("device_isis_interface", "hello_auth_present")
    op.drop_column("device_isis_interface", "hello_auth_type")
