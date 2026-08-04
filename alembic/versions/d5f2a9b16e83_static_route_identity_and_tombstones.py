# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Static-route identity: route_id, deployed_key, tombstones, deferrable identity.

`route_id` carries the NetBox `routing.StaticRoute` pk so an identity edit updates the
intent row in place instead of appearing as an unrelated delete plus insert. It is
nullable and deliberately NOT backfilled — the adapter cannot learn NetBox pks, and the
surviving NULLs are the rollout fence that keeps today's detach-on-shrink behavior in
force until the plugin and the fleet resync fill them.

`deployed_key` is the triple last proven committed into the service, as a 3-element
JSONB array. JSONB, not json: PostgreSQL's `json` has no equality operator, so R2's
`IS NOT DISTINCT FROM` CAS cannot be expressed against it. It IS backfilled from
`last_apply_at`, sanctioned by the design because it only ever adds deletion authority
for a triple the store itself recorded as applied.

`static_route_tombstone` is the only surviving carrier of a deletion once the intent row
is gone. `device_id` cascades (an offboarded device has no service to reconcile against);
`job_id` is SET NULL so deleting a job returns the tombstone to the sweeper rather than
destroying the carrier.

`uq_staticrouteintent_identity` becomes DEFERRABLE INITIALLY DEFERRED: in-place identity
updates make transient collisions reachable inside one transaction (a same-payload swap,
a delete-then-reclaim), both of which reach a legal final state. No statement ordering
fixes the swap case.

Revision ID: d5f2a9b16e83
Revises: f4c1b7d92a05
Create Date: 2026-07-29
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "d5f2a9b16e83"
down_revision: str | Sequence[str] | None = "f4c1b7d92a05"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_IDENTITY_COLUMNS = ("device_id", "vrf", "prefix", "next_hop")


def upgrade() -> None:
    op.add_column("static_route_intent", sa.Column("route_id", sa.Integer(), nullable=True))
    op.add_column(
        "static_route_intent",
        sa.Column("deployed_key", postgresql.JSONB(none_as_null=True, astext_type=sa.Text()), nullable=True),
    )
    op.create_index(
        "uq_sr_intent_device_route_id",
        "static_route_intent",
        ["device_id", "route_id"],
        unique=True,
        postgresql_where=sa.text("route_id IS NOT NULL"),
    )

    op.create_table(
        "static_route_tombstone",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("route_id", sa.Integer(), nullable=False),
        sa.Column("vrf", sa.String(length=128), nullable=False),
        sa.Column("prefix", sa.String(length=64), nullable=False),
        sa.Column("next_hop", sa.String(length=64), nullable=False),
        sa.Column("deployed_key", postgresql.JSONB(none_as_null=True, astext_type=sa.Text()), nullable=True),
        sa.Column("marking", sa.String(length=16), nullable=False),
        sa.Column("job_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint("marking IN ('delete_origin', 'detach')", name="ck_srt_marking"),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_static_route_tombstone_device_id", "static_route_tombstone", ["device_id"])
    op.create_index("ix_static_route_tombstone_job_id", "static_route_tombstone", ["job_id"])
    op.create_index(
        "ix_srt_unclaimed",
        "static_route_tombstone",
        ["device_id", "id"],
        postgresql_where=sa.text("job_id IS NULL"),
    )

    # ALTER CONSTRAINT cannot add deferrability — drop and recreate.
    op.drop_constraint("uq_staticrouteintent_identity", "static_route_intent", type_="unique")
    op.create_unique_constraint(
        "uq_staticrouteintent_identity",
        "static_route_intent",
        list(_IDENTITY_COLUMNS),
        deferrable=True,
        initially="DEFERRED",
    )

    # Not reversible and needs no reversal: the downgrade drops the column.
    op.execute(
        "UPDATE static_route_intent "
        "SET deployed_key = jsonb_build_array(vrf, prefix, next_hop) "
        "WHERE last_apply_at IS NOT NULL"
    )


def downgrade() -> None:
    op.drop_constraint("uq_staticrouteintent_identity", "static_route_intent", type_="unique")
    op.create_unique_constraint("uq_staticrouteintent_identity", "static_route_intent", list(_IDENTITY_COLUMNS))

    op.drop_index("ix_srt_unclaimed", table_name="static_route_tombstone")
    op.drop_index("ix_static_route_tombstone_job_id", table_name="static_route_tombstone")
    op.drop_index("ix_static_route_tombstone_device_id", table_name="static_route_tombstone")
    op.drop_table("static_route_tombstone")

    op.drop_index("uq_sr_intent_device_route_id", table_name="static_route_intent")
    op.drop_column("static_route_intent", "deployed_key")
    op.drop_column("static_route_intent", "route_id")
