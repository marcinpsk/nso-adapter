# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""device identity uniqueness

Enforce the two device identities in the database. onboard_device only ever checked them with
a select-then-insert, so two concurrent onboards could both find nothing and both insert; the
resulting duplicates are permanent, because the scope reconcile keys ownership by
netbox_device_id and therefore keeps every row it finds.

netbox_device_id is unique only where non-null — a device provisioned into NSO with no NetBox
link is a legitimate leftover and several may coexist.

Revision ID: b3d7f1a9c204
Revises: a1c4e7f9b2d5
Create Date: 2026-07-28 20:32:36.111662

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b3d7f1a9c204"
down_revision: str | Sequence[str] | None = "a1c4e7f9b2d5"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add the NSO-identity unique constraint and the partial NetBox-id unique index."""
    op.create_unique_constraint("uq_device_nso_identity", "devices", ["nso_instance", "nso_device_name"])
    op.create_index(
        "uq_device_netbox_device_id",
        "devices",
        ["netbox_device_id"],
        unique=True,
        postgresql_where="netbox_device_id IS NOT NULL",
    )


def downgrade() -> None:
    """Drop both identity constraints."""
    op.drop_index("uq_device_netbox_device_id", table_name="devices")
    op.drop_constraint("uq_device_nso_identity", "devices", type_="unique")
