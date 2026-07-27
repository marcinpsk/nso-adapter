# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""isis interface prefix-sid mirror

Per-loopback SR prefix-SID(s) read from network-state-export's per-interface
``prefix-sid`` list -> netbox_routing ISISPrefixSID (keyed per interface +
algorithm). Stored as a JSON list on device_isis_interface, mirroring ``levels``.

Revision ID: d4b8f1a9c2e6
Revises: c7a2e5b91d04
Create Date: 2026-07-08 18:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "d4b8f1a9c2e6"
down_revision: str | None = "c7a2e5b91d04"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("device_isis_interface", sa.Column("prefix_sids", sa.JSON(), nullable=True))


def downgrade() -> None:
    op.drop_column("device_isis_interface", "prefix_sids")
