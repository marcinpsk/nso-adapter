# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Stamp every authorized fragment with the execution context and proof it must execute with.

Revision ID: c7b4e1d90f28
Revises: b3d9f2a6c410
Create Date: 2026-09-06

A one-shot DATA migration, no schema change (#1663). Fragments written before the
execution-context contract carry tables only, and a worker now refuses such a document at
hydration. The migration attaches exactly what is derivable offline — the context for every
section, the interface proof, and the static-route apply plan — by calling the same pure
builders the runtime freeze calls, so a stamped fragment and a freshly frozen one agree.

It re-reads no intent rows and re-snapshots nothing, so it promotes no store-only state. It
stamps the CURRENT device NED, which is a reauthorization under the current context with no
operator action; accepted because this workspace is dev-only and the ratified cutover
reseeds every fragment anyway. Run with workers stopped, as the cutover order requires.
"""

from __future__ import annotations

from collections.abc import Sequence
from types import SimpleNamespace

import sqlalchemy as sa

from alembic import op

revision: str = "c7b4e1d90f28"
down_revision: str | None = "b3d9f2a6c410"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_INTERFACE_STREAMS = ("interface_config", "ip")


#: ``authorized_document`` is a JSON column, so the bind is typed rather than a text literal:
#: PostgreSQL has no assignment cast from text to json.
_STAMP = sa.text("UPDATE device_projection_stream SET authorized_document = :document WHERE id = :id").bindparams(
    sa.bindparam("document", type_=sa.JSON)
)


def _interface_facts(connection, device_id: int):
    interfaces = [
        SimpleNamespace(**dict(row))
        for row in connection.execute(
            sa.text(
                "SELECT id, name, kind, parent_binding, encap_tag, vrf, service "
                "FROM interfaces WHERE device_id = :device_id ORDER BY id"
            ),
            {"device_id": device_id},
        ).mappings()
    ]
    decisions = {
        (row["interface_id"], row["attribute"]): row["sync_state"]
        for row in connection.execute(
            sa.text(
                "SELECT s.interface_id, s.attribute, s.sync_state FROM interface_attr_state s "
                "JOIN interfaces i ON i.id = s.interface_id WHERE i.device_id = :device_id"
            ),
            {"device_id": device_id},
        ).mappings()
    }
    return interfaces, decisions


def upgrade() -> None:
    from nso_adapter.core.community_dialect import community_dialect_for
    from nso_adapter.core.projection import (
        EXECUTION_KEY,
        INTERFACE_ATTRIBUTE_ELIGIBLE_STATES,
        build_interface_proof,
        fragment_tables,
    )
    from nso_adapter.core.static_route_plan import freeze_static_route_proof
    from nso_adapter.store.models import SyncState

    eligible = {state.value for state in INTERFACE_ATTRIBUTE_ELIGIBLE_STATES}
    connection = op.get_bind()
    rows = (
        connection.execute(
            sa.text(
                "SELECT s.id, s.device_id, s.stream, s.authorized_document, d.ned_id "
                "FROM device_projection_stream s JOIN devices d ON d.id = s.device_id "
                "WHERE s.authorized_document IS NOT NULL ORDER BY s.id"
            )
        )
        .mappings()
        .all()
    )
    facts: dict[int, tuple] = {}
    for row in rows:
        tables = fragment_tables(row["authorized_document"])
        execution: dict = {"context": {"ned_id": row["ned_id"], "dialect": community_dialect_for(row["ned_id"]).name}}
        if row["stream"] in _INTERFACE_STREAMS:
            if row["device_id"] not in facts:
                facts[row["device_id"]] = _interface_facts(connection, row["device_id"])
            interfaces, states = facts[row["device_id"]]
            decisions = {key: SyncState(value).value in eligible for key, value in states.items()}
            execution["proof"] = build_interface_proof(row["stream"], tables, interfaces, decisions)
        elif row["stream"] == "static_route":
            execution["proof"] = freeze_static_route_proof(tables, device_id=row["device_id"])
        connection.execute(_STAMP, {"id": row["id"], "document": {**tables, EXECUTION_KEY: execution}})


def downgrade() -> None:
    from nso_adapter.core.projection import fragment_tables

    connection = op.get_bind()
    rows = (
        connection.execute(
            sa.text(
                "SELECT id, authorized_document FROM device_projection_stream "
                "WHERE authorized_document IS NOT NULL ORDER BY id"
            )
        )
        .mappings()
        .all()
    )
    for row in rows:
        connection.execute(_STAMP, {"id": row["id"], "document": fragment_tables(row["authorized_document"])})
