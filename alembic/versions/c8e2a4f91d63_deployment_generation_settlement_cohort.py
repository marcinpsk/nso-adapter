# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Give marking-split deployment generations an explicit settlement cohort.

Only the generations created by one static-route marking split share a cohort. Ordinary
generations keep NULL, so a historical generation with the same device, stream and revision
cannot block a later deployment from certifying that revision.

Revision ID: c8e2a4f91d63
Revises: b7d5e2f18c40
Create Date: 2026-08-11
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op
from nso_adapter.store.ddl import generation_immutability_ddl

revision: str = "c8e2a4f91d63"
down_revision: str | None = "b7d5e2f18c40"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_SEQUENCE = sa.Sequence("deployment_generation_settlement_cohort_seq")
_PREVIOUS_IMMUTABLE_COLUMNS = (
    "device_id",
    "seq",
    "mode",
    "document",
    "digest",
    "allowed_removal_keys",
    "source_push_seq",
    "stream_revisions",
    "removal_context",
    "created_at",
)


def upgrade() -> None:
    op.execute(sa.schema.CreateSequence(_SEQUENCE))
    op.add_column("deployment_generation", sa.Column("settlement_cohort", sa.BigInteger(), nullable=True))
    for statement in generation_immutability_ddl():
        op.execute(statement)


def downgrade() -> None:
    for statement in generation_immutability_ddl(_PREVIOUS_IMMUTABLE_COLUMNS):
        op.execute(statement)
    op.drop_column("deployment_generation", "settlement_cohort")
    op.execute(sa.schema.DropSequence(_SEQUENCE))
