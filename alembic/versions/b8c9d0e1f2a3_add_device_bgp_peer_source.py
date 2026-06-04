# SPDX-License-Identifier: Apache-2.0
"""add device_bgp_peer.source (BGP session source: update-source iface / local-address)

network-state-export now exports each BGP neighbor's session source — IOS
``update-source <interface>`` or Junos/Nokia ``local-address`` (an IP). Mirror it
on the read row so the GET endpoint hands it to the plugin, which links it to
BGPPeer.source (an ipam.IPAddress).

Revision ID: b8c9d0e1f2a3
Revises: a7b8c9d0e1f2
Create Date: 2026-06-04
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "b8c9d0e1f2a3"
down_revision: str | Sequence[str] | None = "a7b8c9d0e1f2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("device_bgp_peer", sa.Column("source", sa.String(length=256), nullable=True))


def downgrade() -> None:
    op.drop_column("device_bgp_peer", "source")
