# SPDX-License-Identifier: Apache-2.0
"""add bgp_peer_intent.source (BGP session source: update-source iface / local-address)

The write path now pushes each BGP neighbor's session source — IOS/IOS-XR
``update-source <interface>`` or Junos/Nokia ``local-address`` (an IP). Mirror
the read-row ``device_bgp_peer.source`` column on the intent row so the plugin's
PUT survives the rebuild and ``apply_bgp_config`` can send it to the
bgp-reconciler (which gained the matching ``peer/source`` YANG leaf).

Revision ID: c1d2e3f4a5b6
Revises: debced55c971
Create Date: 2026-06-24
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "c1d2e3f4a5b6"
down_revision: str | Sequence[str] | None = "debced55c971"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("bgp_peer_intent", sa.Column("source", sa.String(length=256), nullable=True))


def downgrade() -> None:
    op.drop_column("bgp_peer_intent", "source")
