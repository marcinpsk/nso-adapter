# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""widen OSPF identity unique constraints

The full-replace OSPF refresh insert aborted with an IntegrityError when:
  * one interface was enabled under two OSPF processes — uq_deviceospfinterface_identity
    was (device_id, interface_name), missing process_id; and
  * two OSPF instances shared a process-id across VRFs — uq_deviceospfinstance_identity
    was (device_id, process_id), missing vrf.
Widen both unique constraints to their true identity.

Revision ID: 7c2e5a9d4b18
Revises: 3f9c1e7b2a04
Create Date: 2026-07-02 10:15:00.000000

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "7c2e5a9d4b18"
down_revision: str | Sequence[str] | None = "3f9c1e7b2a04"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.drop_constraint("uq_deviceospfinstance_identity", "device_ospf_instance", type_="unique")
    op.create_unique_constraint(
        "uq_deviceospfinstance_identity",
        "device_ospf_instance",
        ["device_id", "process_id", "vrf"],
    )
    op.drop_constraint("uq_deviceospfinterface_identity", "device_ospf_interface", type_="unique")
    op.create_unique_constraint(
        "uq_deviceospfinterface_identity",
        "device_ospf_interface",
        ["device_id", "interface_name", "process_id"],
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint("uq_deviceospfinterface_identity", "device_ospf_interface", type_="unique")
    op.create_unique_constraint(
        "uq_deviceospfinterface_identity",
        "device_ospf_interface",
        ["device_id", "interface_name"],
    )
    op.drop_constraint("uq_deviceospfinstance_identity", "device_ospf_instance", type_="unique")
    op.create_unique_constraint(
        "uq_deviceospfinstance_identity",
        "device_ospf_instance",
        ["device_id", "process_id"],
    )
