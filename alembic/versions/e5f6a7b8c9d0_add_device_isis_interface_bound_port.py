# SPDX-License-Identifier: Apache-2.0
"""add device_isis_interface.bound_port (Nokia IS-IS port correlation)

Nokia SR OS IS-IS interfaces are logical router-interfaces (e.g. ``LAG99:10``)
that bind to a physical/LAG port (e.g. ``lag-99:10``). network-state-export now
emits that binding as ``bound-port`` on each Nokia IS-IS interface. Mirror it on
the read row so the GET endpoint can hand it to the plugin, which correlates the
logical IS-IS interface to its NetBox ``dcim.Interface`` (named by port-id).
Null for Cisco/Junos and Nokia loopbacks.

Ships as a proper incremental migration (never an edit to the baseline) so the
container entrypoint's ``alembic upgrade head`` applies it on start.

Revision ID: e5f6a7b8c9d0
Revises: c3d4e5f6a7b8
Create Date: 2026-06-02
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "e5f6a7b8c9d0"
down_revision: str | Sequence[str] | None = "c3d4e5f6a7b8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("device_isis_interface", sa.Column("bound_port", sa.String(length=256), nullable=True))


def downgrade() -> None:
    op.drop_column("device_isis_interface", "bound_port")
