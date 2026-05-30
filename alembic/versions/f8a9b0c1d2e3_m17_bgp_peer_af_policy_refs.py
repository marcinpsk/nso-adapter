# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""M17 B4 — add policy ref columns to bgp_peer_af_intent.

Revision ID: f8a9b0c1d2e3
Revises: e7f8a9b0c1d2
Create Date: 2026-07-10
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "f8a9b0c1d2e3"
down_revision = "e7f8a9b0c1d2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("bgp_peer_af_intent", sa.Column("routemap_in", sa.String(255), nullable=True))
    op.add_column("bgp_peer_af_intent", sa.Column("routemap_out", sa.String(255), nullable=True))
    op.add_column("bgp_peer_af_intent", sa.Column("prefixlist_in", sa.String(255), nullable=True))
    op.add_column("bgp_peer_af_intent", sa.Column("prefixlist_out", sa.String(255), nullable=True))


def downgrade() -> None:
    op.drop_column("bgp_peer_af_intent", "prefixlist_out")
    op.drop_column("bgp_peer_af_intent", "prefixlist_in")
    op.drop_column("bgp_peer_af_intent", "routemap_out")
    op.drop_column("bgp_peer_af_intent", "routemap_in")
