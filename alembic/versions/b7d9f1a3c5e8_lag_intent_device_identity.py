# SPDX-License-Identifier: Apache-2.0
"""Enforce each device's numeric LAG identity."""

from alembic import op

revision = "b7d9f1a3c5e8"
down_revision = "a5c7e9b1d3f6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_unique_constraint(
        "uq_lag_bundle_intent_device_lag",
        "lag_bundle_intent",
        ["device_id", "lag_id"],
        deferrable=True,
        initially="DEFERRED",
    )


def downgrade() -> None:
    op.drop_constraint("uq_lag_bundle_intent_device_lag", "lag_bundle_intent", type_="unique")
