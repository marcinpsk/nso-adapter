# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Freeze terminal carrier evidence.

Revision ID: d8f2a6c1e904
Revises: c39b7d2e4a61
"""

from collections.abc import Sequence

from alembic import op
from nso_adapter.store.ddl import (
    generation_carrier_immutability_ddl,
    generation_carrier_immutability_drop_ddl,
    job_terminal_evidence_immutability_ddl,
    job_terminal_evidence_immutability_drop_ddl,
)

# revision identifiers, used by Alembic.
revision: str = "d8f2a6c1e904"
down_revision: str | Sequence[str] | None = "c39b7d2e4a61"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    for statement in job_terminal_evidence_immutability_ddl():
        op.execute(statement)
    for statement in generation_carrier_immutability_ddl():
        op.execute(statement)


def downgrade() -> None:
    """Downgrade schema."""
    for statement in job_terminal_evidence_immutability_drop_ddl():
        op.execute(statement)
    for statement in generation_carrier_immutability_drop_ddl():
        op.execute(statement)
