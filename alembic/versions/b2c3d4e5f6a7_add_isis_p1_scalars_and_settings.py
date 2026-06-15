# SPDX-License-Identifier: Apache-2.0
"""add IS-IS P1 cross-vendor scalars + settings EAV mirror (M33 P1)

network-state-export now exports the cross-vendor IS-IS instance/interface
scalars (SPF/LSP timers, lifetime/refresh/mtu, overload-on-startup, TE,
segment-routing, distance, maximum-paths, reference-bandwidth; per-interface
csnp/retransmit/lsp/mesh-group) plus a (key,value) ``setting`` list for the
divergent long-tail. Mirror them onto the read tables so the plugin can
reconcile them into netbox_routing columns + ISISSetting rows. All NULL for
devices that don't configure them.

Revision ID: b2c3d4e5f6a7
Revises: a1b2c3d4e5f6
Create Date: 2026-06-05
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "b2c3d4e5f6a7"
down_revision: str | Sequence[str] | None = "a1b2c3d4e5f6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


_PROC_INT_COLS = (
    "spf_initial_wait",
    "spf_max_wait",
    "lsp_initial_wait",
    "lsp_max_wait",
    "lsp_lifetime",
    "lsp_refresh_interval",
    "lsp_mtu",
    "overload_timeout",
    "sr_node_msd",
    "distance",
    "maximum_paths",
)
_PROC_BOOL_COLS = ("overload_on_startup", "te_enabled", "sr_enabled")
_IFACE_INT_COLS = ("csnp_interval", "retransmit_interval", "lsp_interval")


def upgrade() -> None:
    for col in _PROC_INT_COLS:
        op.add_column("device_isis_process", sa.Column(col, sa.Integer(), nullable=True))
    for col in _PROC_BOOL_COLS:
        op.add_column("device_isis_process", sa.Column(col, sa.Boolean(), nullable=True))
    op.add_column("device_isis_process", sa.Column("reference_bandwidth", sa.BigInteger(), nullable=True))
    op.add_column("device_isis_process", sa.Column("settings", sa.JSON(), nullable=True))

    for col in _IFACE_INT_COLS:
        op.add_column("device_isis_interface", sa.Column(col, sa.Integer(), nullable=True))
    op.add_column("device_isis_interface", sa.Column("mesh_group", sa.String(length=32), nullable=True))
    op.add_column("device_isis_interface", sa.Column("settings", sa.JSON(), nullable=True))


def downgrade() -> None:
    op.drop_column("device_isis_interface", "settings")
    op.drop_column("device_isis_interface", "mesh_group")
    for col in reversed(_IFACE_INT_COLS):
        op.drop_column("device_isis_interface", col)

    op.drop_column("device_isis_process", "settings")
    op.drop_column("device_isis_process", "reference_bandwidth")
    for col in reversed(_PROC_BOOL_COLS):
        op.drop_column("device_isis_process", col)
    for col in reversed(_PROC_INT_COLS):
        op.drop_column("device_isis_process", col)
