# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""bgp router-id columns (#99 write path)

Global BGP router-id (dotted-quad) on both the read mirror
(``device_bgp_router.router_id``) and the write intent
(``bgp_router_intent.router_id``) — mirrors the network-state-export
``router-id`` leaf and the netbox-routing ``BGPRouter.router_id`` field.

Revision ID: e2f8b4a1c7d6
Revises: d1f4a8c3e6b9
Create Date: 2026-07-12 12:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "e2f8b4a1c7d6"
down_revision: str | None = "d1f4a8c3e6b9"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("device_bgp_router", sa.Column("router_id", sa.String(length=64), nullable=True))
    op.add_column("bgp_router_intent", sa.Column("router_id", sa.String(length=64), nullable=True))


def downgrade() -> None:
    op.drop_column("bgp_router_intent", "router_id")
    op.drop_column("device_bgp_router", "router_id")
