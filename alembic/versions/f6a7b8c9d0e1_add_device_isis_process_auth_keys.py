# SPDX-License-Identifier: Apache-2.0
"""add device_isis_process.{area,domain}_auth_key (IS-IS auth-key read path)

network-state-export now exports the IS-IS area/domain authentication *key*
(the ``area-password``/``domain-password`` value — a routing-protocol secret,
not a device-access credential) in addition to the auth type/presence flags.
Mirror those on the read row so the GET endpoint can hand them to the plugin,
which fills netbox-routing ISISInstance.{area,domain}_auth_key. Null when NSO
does not report a key (or only the presence is known).

Ships as a proper incremental migration (never an edit to the baseline) so the
container entrypoint's ``alembic upgrade head`` applies it on start.

Revision ID: f6a7b8c9d0e1
Revises: e5f6a7b8c9d0
Create Date: 2026-06-03
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "f6a7b8c9d0e1"
down_revision: str | Sequence[str] | None = "e5f6a7b8c9d0"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("device_isis_process", sa.Column("area_auth_key", sa.String(length=128), nullable=True))
    op.add_column("device_isis_process", sa.Column("domain_auth_key", sa.String(length=128), nullable=True))


def downgrade() -> None:
    op.drop_column("device_isis_process", "domain_auth_key")
    op.drop_column("device_isis_process", "area_auth_key")
