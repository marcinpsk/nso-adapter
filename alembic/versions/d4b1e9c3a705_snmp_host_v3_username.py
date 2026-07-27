# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""snmp_host: v3 security user name (CR-P16).

A v3 trap host's user name is NOT a secret — it is the same identity the v3-user mirror already
holds — and both NSO host writers KEY the receiver on it (IOS: the community-string leaf; IOS-XR:
the third key component). Without it a brownfield v3 host could be imported and displayed but never
pushed back: there was nothing to put in the field, so the plugin refused the push outright.

Nullable, and populated only for v3 hosts. The corresponding NED field on a v1/v2c host holds the
COMMUNITY STRING, which is a secret; network-state-export gates the export on version so it can
never arrive here.

Revision ID: d4b1e9c3a705
Revises: e2f8b4a1c7d6
Create Date: 2026-07-13
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "d4b1e9c3a705"
down_revision: str | Sequence[str] | None = "e2f8b4a1c7d6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("snmp_host", sa.Column("username", sa.String(length=128), nullable=True))


def downgrade() -> None:
    op.drop_column("snmp_host", "username")
