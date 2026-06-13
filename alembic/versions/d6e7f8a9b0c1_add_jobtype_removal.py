"""add JobType.removal (async removal-propagation jobs)

Revision ID: d6e7f8a9b0c1
Revises: c5d6e7f8a9b0
Create Date: 2026-06-13

Removal propagation (PUT-replace the reconciler service so FASTMAP reverts dropped
intent) used to run synchronously inside the intent PUT — a device commit that
could exceed the plugin's 30s client timeout. It now runs as a background
``removal`` job (see nso_adapter/core/removal.py), which needs a new ``jobtype``
enum label. PostgreSQL 12+ allows ``ALTER TYPE ... ADD VALUE`` inside the
migration transaction as long as the new value is not used in the same
transaction (it is not).
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "d6e7f8a9b0c1"
down_revision = "c5d6e7f8a9b0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TYPE jobtype ADD VALUE IF NOT EXISTS 'removal'")


def downgrade() -> None:
    # PostgreSQL cannot drop an enum value; leaving the unused label is harmless.
    pass
