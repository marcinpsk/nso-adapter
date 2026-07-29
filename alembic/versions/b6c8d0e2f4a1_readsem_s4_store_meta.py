# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""READSEM S4: the store incarnation singleton (``store_meta``).

One row, ``id == 1``: the ``(incarnation, born)`` pair minted when this database was
first initialised. Rides every S4 ``read_state`` block as the plugin's adoption/reset
signal (attempt ids restart after a rebuild and numeric device ids can be reissued, so
neither can carry reset semantics). The migration also INSERTS the row, so the store is
born here and ``ensure_store_meta()`` takes its idempotent read path at startup.

Revision ID: b6c8d0e2f4a1
Revises: a7e3c1f9d5b2
Create Date: 2026-07-21
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "b6c8d0e2f4a1"
down_revision: str | Sequence[str] | None = "a7e3c1f9d5b2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    table = op.create_table(
        "store_meta",
        sa.Column("id", sa.Integer(), autoincrement=False, nullable=False),
        sa.Column("incarnation", sa.String(length=36), nullable=False),
        sa.Column("born", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("id = 1", name="ck_store_meta_singleton"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.execute(table.insert().values(id=1, incarnation=str(uuid.uuid4()), born=sa.func.now()))


def downgrade() -> None:
    op.drop_table("store_meta")
