# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""static route next-hop-vrf

Read-path fix (#94): a NED may emit an IP next-hop together with an inter-VRF
(leaked) next-hop VRF — the arcos leak-route form. The read mirror previously
dropped it (misrepresenting the route), and the intent table could not carry it
(apply.py already emits ``next-hop-vrf``, so an intent row without the column
would strip the leak on a replace-apply). Nullable string on both tables.

Revision ID: b7d9e2f4a6c8
Revises: a1c3e5f7b9d2
Create Date: 2026-07-10 14:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "b7d9e2f4a6c8"
down_revision: str | None = "a1c3e5f7b9d2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("device_static_route", sa.Column("next_hop_vrf", sa.String(length=128), nullable=True))
    op.add_column("static_route_intent", sa.Column("next_hop_vrf", sa.String(length=128), nullable=True))


def downgrade() -> None:
    op.drop_column("static_route_intent", "next_hop_vrf")
    op.drop_column("device_static_route", "next_hop_vrf")
