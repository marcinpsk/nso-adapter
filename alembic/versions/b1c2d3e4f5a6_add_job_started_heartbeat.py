# SPDX-License-Identifier: Apache-2.0
"""add jobs.started_at + jobs.heartbeat_at (Layer B durable worker)

The durable worker stamps ``started_at`` when it claims a job and refreshes
``heartbeat_at`` while it runs (see ``core/worker.py``). These ship as a proper
incremental migration — never as an edit to the already-applied baseline — so the
container entrypoint's ``alembic upgrade head`` applies them automatically on
start (the dev DB that was hand-patched before this existed has been stamped past
this revision, so it is not re-run there).

Revision ID: b1c2d3e4f5a6
Revises: d9f7d5098d8c
Create Date: 2026-06-02
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "b1c2d3e4f5a6"
down_revision: str | Sequence[str] | None = "d9f7d5098d8c"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("jobs", sa.Column("started_at", sa.DateTime(), nullable=True))
    op.add_column("jobs", sa.Column("heartbeat_at", sa.DateTime(), nullable=True))


def downgrade() -> None:
    op.drop_column("jobs", "heartbeat_at")
    op.drop_column("jobs", "started_at")
