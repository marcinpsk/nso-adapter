# SPDX-License-Identifier: Apache-2.0
"""drop interface_attr_state.intent_value (intent split-brain fix)

``interface_attr_state.intent_value`` was a second, never-written copy of deployed
intent. The importer read it to decide Phase 1 vs Phase 2, so Phase 2 was effectively
dead. Deployed intent now has a single source of truth — the ``interface_intent``
table — which the importer reads instead. Drop the vestigial column.

Ships as a proper incremental migration (never an edit to the baseline) so the
container entrypoint's ``alembic upgrade head`` applies it on start.

Revision ID: c3d4e5f6a7b8
Revises: b1c2d3e4f5a6
Create Date: 2026-06-02
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "c3d4e5f6a7b8"
down_revision: str | Sequence[str] | None = "b1c2d3e4f5a6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_column("interface_attr_state", "intent_value")


def downgrade() -> None:
    op.add_column("interface_attr_state", sa.Column("intent_value", sa.Text(), nullable=True))
