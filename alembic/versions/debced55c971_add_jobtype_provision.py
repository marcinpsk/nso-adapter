"""add JobType.provision (async device-onboarding jobs)

Revision ID: debced55c971
Revises: 05953e22e3ba
Create Date: 2026-06-19

Device provisioning into NSO (create node → fetch-host-keys → unlock → sync-from)
used to run synchronously inside ``POST /api/v1/devices/provision`` — a flow that can
exceed the plugin client's 30s read timeout when the primary mgmt IP is unreachable and
the adapter has to probe-then-bootstrap over OOB before a (long) sync-from. It now runs
as a background ``provision`` job (see core/jobs.py::_run_provision), which needs a new
``jobtype`` enum label. PostgreSQL 12+ allows ``ALTER TYPE ... ADD VALUE`` inside the
migration transaction as long as the new value is not used in the same transaction.
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "debced55c971"
down_revision = "05953e22e3ba"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TYPE jobtype ADD VALUE IF NOT EXISTS 'provision'")


def downgrade() -> None:
    # PostgreSQL cannot drop an enum value; leaving the unused label is harmless.
    pass
