# SPDX-License-Identifier: Apache-2.0
"""add interfaces logical-modeling columns (M27R)

network-state-export now exports Nokia logical interfaces (Base router
interfaces; later IES/VPRN) as first-class entries with parent/kind/tag/vrf so
the plugin can model them as parented dcim.Interface rows instead of folding
their IPs onto the bound port. These columns are NULL for physical ports and for
Cisco/Junos (which keep the flat interface=physical model).

Revision ID: a1b2c3d4e5f6
Revises: f2a3b4c5d6e7
Create Date: 2026-06-05
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "a1b2c3d4e5f6"
down_revision: str | Sequence[str] | None = "f2a3b4c5d6e7"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("interfaces", sa.Column("parent_binding", sa.String(length=256), nullable=True))
    op.add_column("interfaces", sa.Column("kind", sa.String(length=16), nullable=True))
    op.add_column("interfaces", sa.Column("encap_tag", sa.String(length=64), nullable=True))
    op.add_column("interfaces", sa.Column("vrf", sa.String(length=256), nullable=True))
    op.add_column("interfaces", sa.Column("service", sa.String(length=256), nullable=True))


def downgrade() -> None:
    op.drop_column("interfaces", "service")
    op.drop_column("interfaces", "vrf")
    op.drop_column("interfaces", "encap_tag")
    op.drop_column("interfaces", "kind")
    op.drop_column("interfaces", "parent_binding")
