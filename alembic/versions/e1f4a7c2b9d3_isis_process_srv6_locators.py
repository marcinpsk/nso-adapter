# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""isis process srv6-locator mirror

SRv6 locators advertised by an IS-IS instance, read from network-state-export's
per-process ``srv6-locator`` list -> netbox_routing ISISSRv6Locator (keyed per
instance + name). Stored as a JSON list on device_isis_process, mirroring
``flex_algos``.

Revision ID: e1f4a7c2b9d3
Revises: d4b8f1a9c2e6
Create Date: 2026-07-08 21:30:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "e1f4a7c2b9d3"
down_revision: str | None = "d4b8f1a9c2e6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("device_isis_process", sa.Column("srv6_locators", sa.JSON(), nullable=True))


def downgrade() -> None:
    op.drop_column("device_isis_process", "srv6_locators")
