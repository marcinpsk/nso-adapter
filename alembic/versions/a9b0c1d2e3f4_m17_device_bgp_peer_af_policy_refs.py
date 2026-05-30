# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""M17 read-path fix — add policy ref columns to device_bgp_peer_af.

The M17 write-path migration (f8a9b0c1d2e3) added routemap_in/out and
prefixlist_in/out to bgp_peer_af_intent.  This migration adds the same four
columns to the read-mirror table so that api/bgp.py can surface them without
raising AttributeError.

Revision ID: a9b0c1d2e3f4
Revises: f8a9b0c1d2e3
Create Date: 2026-05-30
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "a9b0c1d2e3f4"
down_revision = "f8a9b0c1d2e3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("device_bgp_peer_af", sa.Column("routemap_in", sa.String(255), nullable=True))
    op.add_column("device_bgp_peer_af", sa.Column("routemap_out", sa.String(255), nullable=True))
    op.add_column("device_bgp_peer_af", sa.Column("prefixlist_in", sa.String(255), nullable=True))
    op.add_column("device_bgp_peer_af", sa.Column("prefixlist_out", sa.String(255), nullable=True))


def downgrade() -> None:
    op.drop_column("device_bgp_peer_af", "prefixlist_out")
    op.drop_column("device_bgp_peer_af", "prefixlist_in")
    op.drop_column("device_bgp_peer_af", "routemap_out")
    op.drop_column("device_bgp_peer_af", "routemap_in")
