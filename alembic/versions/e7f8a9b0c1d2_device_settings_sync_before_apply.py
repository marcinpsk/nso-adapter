"""device_settings.sync_before_apply (sync-from before apply, per-device toggle)

Revision ID: e7f8a9b0c1d2
Revises: d6e7f8a9b0c1
Create Date: 2026-06-13

The apply worker now sync-froms the device before pushing intent, to clear the
NSO/device out-of-sync state a timed-out or partial prior commit leaves behind
(which otherwise makes the next apply fail with "device out of sync"). This is a
per-device toggle (default on); disable it for NEDs that already sync on connect.
server_default true so existing device_settings rows keep the safe default.
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "e7f8a9b0c1d2"
down_revision = "d6e7f8a9b0c1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "device_settings",
        sa.Column("sync_before_apply", sa.Boolean(), nullable=False, server_default=sa.text("true")),
    )


def downgrade() -> None:
    op.drop_column("device_settings", "sync_before_apply")
