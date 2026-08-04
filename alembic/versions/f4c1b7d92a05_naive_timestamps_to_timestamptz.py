# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Normalize the last 25 naive timestamp columns to timestamptz.

72 of the store's 97 datetime columns were already timestamptz while these 25 were naive,
and single code paths write both kinds (core/apply.py computes one `now` for six
`last_apply_at` columns spanning the two). One storage kind makes `iso_z` the only
serializer and every wire timestamp `"<iso>Z"`.

All 25 hold semantically-UTC instants, so the conversion states that explicitly: without
`USING <col> AT TIME ZONE 'UTC'` PostgreSQL reads the stored wall clock in the *session*
TimeZone and silently shifts every historical row by the migrating session's offset.

Revision ID: f4c1b7d92a05
Revises: b3d7f1a9c204
Create Date: 2026-07-29
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "f4c1b7d92a05"
down_revision: str | Sequence[str] | None = "b3d7f1a9c204"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_COLUMNS: list[tuple[str, str]] = [
    ("device_failover", "last_probe_at"),
    ("device_failover", "last_switch_at"),
    ("device_failover", "next_oob_probe_at"),
    ("device_failover", "next_primary_probe_at"),
    ("device_failover", "oob_health_checked_at"),
    ("device_failover", "updated_at"),
    ("device_route_policy_as_path", "last_refreshed_at"),
    ("device_route_policy_community_list", "last_refreshed_at"),
    ("device_route_policy_prefix_list", "last_refreshed_at"),
    ("device_route_policy_route_map", "last_refreshed_at"),
    ("device_settings", "updated_at"),
    ("devices", "created_at"),
    ("devices", "last_sync_at"),
    ("devices", "updated_at"),
    ("failover_config", "updated_at"),
    ("interface_attr_state", "last_checked_at"),
    ("interface_intent", "accepted_at"),
    ("interface_intent", "last_apply_at"),
    ("jobs", "created_at"),
    ("jobs", "heartbeat_at"),
    ("jobs", "started_at"),
    ("jobs", "updated_at"),
    ("managed_scope", "updated_at"),
    ("route_policy_object_intent", "accepted_at"),
    ("route_policy_object_intent", "last_apply_at"),
]


def upgrade() -> None:
    for table, col in _COLUMNS:
        op.alter_column(
            table,
            col,
            type_=sa.DateTime(timezone=True),
            postgresql_using=f"{col} AT TIME ZONE 'UTC'",
        )


def downgrade() -> None:
    for table, col in _COLUMNS:
        op.alter_column(
            table,
            col,
            type_=sa.DateTime(),
            postgresql_using=f"{col} AT TIME ZONE 'UTC'",
        )
