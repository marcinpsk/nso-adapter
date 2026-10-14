# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Private bookkeeping snapshots and live content fences for execution evidence."""

from functools import cache
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.core.projection import (
    hydrate_interface_execution,
    hydrate_section,
    intent_state,
    section_registry,
)
from nso_adapter.nso.origins import LogicalRow, OriginIndex, content_digest
from nso_adapter.store.models import (
    DbInterface,
    DeploymentGeneration,
    DeviceProjectionStream,
    InterfaceAttrState,
    InterfaceIntent,
    SyncState,
)

_SPECS = {spec.model: spec for section in section_registry().values() for spec in section.tables}


def _models() -> dict[str, Any]:
    return {spec.model.__tablename__: spec.model for section in section_registry().values() for spec in section.tables}


@cache
def _parent_column(spec) -> str:
    foreign_keys = [
        column
        for column in spec.model.__table__.columns
        if any(fk.column.table is spec.parent.__table__ for fk in column.foreign_keys)
    ]
    if len(foreign_keys) != 1:
        raise ValueError("Bookkeeping content fence requires one parent join")
    return foreign_keys[0].key


def _frozen_children(frozen: dict) -> dict:
    children: dict[tuple, list] = {}
    for row in frozen.values():
        spec = _SPECS[type(row)]
        if spec.parent is None:
            continue
        column = _parent_column(spec)
        children.setdefault((row.__tablename__, column, getattr(row, column)), []).append(row)
    for rows in children.values():
        rows.sort(key=lambda row: row.id)
    return children


def _tree(row, frozen_children: dict) -> dict:
    children = []
    for spec in _SPECS.values():
        if spec.parent is not type(row):
            continue
        column = _parent_column(spec)
        rows = frozen_children.get((spec.model.__tablename__, column, row.id), [])
        children.append(
            {
                "table": spec.model.__tablename__,
                "parent_column": column,
                "rows": [_tree(child, frozen_children) for child in rows],
            }
        )
    return {
        "table": row.__tablename__,
        "id": row.id,
        "content_identity": content_digest(intent_state(row)),
        "children": children,
    }


async def _lock_tree(db: AsyncSession, tree: dict):
    context = tree.get("interface_context")
    if context is not None:
        interface = await db.scalar(
            select(DbInterface)
            .where(DbInterface.id == context["id"])
            .with_for_update()
            .execution_options(populate_existing=True, autoflush=False)
        )
        if (
            interface is None
            or content_digest({column: getattr(interface, column) for column in context["columns"]})
            != context["content_identity"]
        ):
            return None
    model = _models()[tree["table"]]
    row = await db.scalar(
        select(model)
        .where(model.id == tree["id"])
        .with_for_update()
        .execution_options(populate_existing=True, autoflush=False)
    )
    if row is None or content_digest(intent_state(row)) != tree["content_identity"]:
        return None
    for group in tree["children"]:
        child_model = _models()[group["table"]]
        children = list(
            await db.scalars(
                select(child_model)
                .where(getattr(child_model, group["parent_column"]) == row.id)
                .order_by(child_model.id)
                .with_for_update()
                .execution_options(populate_existing=True, autoflush=False)
            )
        )
        if [child.id for child in children] != [child["id"] for child in group["rows"]]:
            return None
        for child_tree in group["rows"]:
            if await _lock_tree(db, child_tree) is None:
                return None
    return row


async def _authorized(db: AsyncSession, device_id: int, stream: str, revision: int) -> bool:
    current = await db.scalar(
        select(DeviceProjectionStream.authorized_revision)
        .where(DeviceProjectionStream.device_id == device_id, DeviceProjectionStream.stream == stream)
        .with_for_update()
        .execution_options(autoflush=False)
    )
    return current == revision


async def _lock_attribute(db: AsyncSession, row) -> InterfaceAttrState | None:
    return await db.scalar(
        select(InterfaceAttrState)
        .where(InterfaceAttrState.interface_id == row.interface_id, InterfaceAttrState.attribute == row.attribute)
        .with_for_update()
        .execution_options(populate_existing=True, autoflush=False)
    )


def _attribute_observation(state: InterfaceAttrState) -> dict:
    return {
        "sync_state": state.sync_state.value,
        "last_checked_at": state.last_checked_at.isoformat() if state.last_checked_at else None,
        "nso_value_identity": content_digest(state.nso_value),
        "netbox_value_identity": content_digest(state.netbox_value),
    }


def _bookkeeping(row) -> dict:
    return {
        key: value.isoformat() if key == "last_apply_at" and value is not None else value
        for key in ("last_apply_at", "last_apply_error")
        if hasattr(row, key)
        for value in (getattr(row, key),)
    }


def _snapshot_indexes(
    origins: OriginIndex, logicals: tuple[LogicalRow, ...]
) -> tuple[set[tuple], dict[tuple, list[dict]]]:
    sources = {source for binding in origins.bindings.values() for source in binding.sources}
    carrier_joins: dict[tuple[str, str], set[tuple[str, int]]] = {}
    for source in sources:
        if source.live_stamp_join:
            carrier_joins.setdefault((source.source_table, source.content_identity), set()).add(source.live_stamp_join)
    targets = {
        (*source.live_stamp_join, logical.stream, logical.frozen_revision)
        for logical in logicals
        for source in logical.source_refs
        if source.live_stamp_join
    }
    for carrier in {logical.carrier for logical in logicals}:
        targets.update(
            (*join, carrier.stream, carrier.frozen_revision)
            for join in carrier_joins.get((carrier.source_table, carrier.content_identity), ())
        )
    contributors: dict[tuple, list[dict]] = {}
    for logical in logicals:
        joins = {source.live_stamp_join for source in logical.source_refs if source.live_stamp_join}
        joins.update(carrier_joins.get((logical.carrier.source_table, logical.carrier.content_identity), ()))
        record = {
            "stream": logical.stream,
            "row": logical.public_record()["row"],
            "settlement_cohort": logical.settlement_cohort,
        }
        for join in joins:
            contributors.setdefault((*join, logical.stream, logical.frozen_revision), []).append(record)
    return targets, contributors


async def snapshot_bookkeeping(
    db: AsyncSession, generation: DeploymentGeneration, origins: OriginIndex, logicals: tuple[LogicalRow, ...]
) -> list[dict]:
    """Read actual prior state under locks and change only eligible attribute state."""
    frozen = {
        (row.__tablename__, row.id): row
        for section in generation.document
        if section in section_registry()
        for rows in hydrate_section(generation.document, section).values()
        for row in rows
    }
    frozen_children = _frozen_children(frozen)
    targets, contributors = _snapshot_indexes(origins, logicals)
    result = []
    proof = None
    for table, identity, stream, revision in sorted(targets):
        if revision is None:
            raise ValueError("Bookkeeping target requires a frozen stream revision")
        original = frozen.get((table, identity))
        if original is None:
            raise ValueError("Bookkeeping target is absent from the frozen generation")
        if not await _authorized(db, generation.device_id, stream, revision):
            continue
        tree = _tree(original, frozen_children)
        if isinstance(original, InterfaceIntent):
            if proof is None:
                proof = hydrate_interface_execution(generation.document)
            if (original.interface_id, original.attribute) not in proof.eligible_attributes:
                continue
            interface = proof.interfaces[original.interface_id]
            columns = sorted(key for key in vars(interface) if key != "_sa_instance_state")
            tree["interface_context"] = {
                "id": interface.id,
                "columns": columns,
                "content_identity": content_digest({column: getattr(interface, column) for column in columns}),
            }
        row = await _lock_tree(db, tree)
        if row is None:
            continue
        attribute = None
        if isinstance(row, InterfaceIntent):
            state = await _lock_attribute(db, row)
            if state is not None:
                prior_state = state.sync_state.value
                state.sync_state = SyncState.deploying
                attribute = {"id": state.id, "prior_sync_state": prior_state, "expected": _attribute_observation(state)}
        prior = _bookkeeping(row)
        if prior or attribute is not None:
            result.append(
                {
                    "stream": stream,
                    "frozen_revision": revision,
                    "fence": tree,
                    "prior": prior,
                    "expected": prior,
                    "attribute": attribute,
                    "contributors": contributors.get((table, identity, stream, revision), []),
                }
            )
    return result


async def restore_bookkeeping(db: AsyncSession, device_id: int, snapshots: list) -> set[tuple[str, int]]:
    """Restore only fields preparation changed, preserving all other bookkeeping."""
    restored = set()
    for snapshot in snapshots:
        stream = snapshot["stream"]
        if not await _authorized(db, device_id, stream, snapshot["frozen_revision"]):
            continue
        row = await _lock_tree(db, snapshot["fence"])
        if row is None or _bookkeeping(row) != snapshot["expected"]:
            continue
        attribute = snapshot["attribute"]
        if attribute is not None:
            state = await _lock_attribute(db, row)
            if state is None or state.id != attribute["id"] or _attribute_observation(state) != attribute["expected"]:
                continue
            state.sync_state = SyncState(attribute["prior_sync_state"])
        restored.add((row.__tablename__, row.id))
    await db.flush()
    return restored


async def stamp_refused_bookkeeping(db: AsyncSession, device_id: int, snapshots: list, causes: dict) -> None:
    """Stamp named carriers under the same content, authorization, and observation fences."""
    for snapshot in snapshots:
        records = [
            cause
            for contributor in snapshot["contributors"]
            for cause in causes.get(
                (
                    contributor["stream"],
                    contributor["row"]["subject"],
                    contributor["row"]["key"],
                    contributor["settlement_cohort"],
                ),
                [],
            )
        ]
        if not records or not await _authorized(db, device_id, snapshot["stream"], snapshot["frozen_revision"]):
            continue
        row = await _lock_tree(db, snapshot["fence"])
        if row is None or _bookkeeping(row) != snapshot["expected"]:
            continue
        attribute = snapshot["attribute"]
        state = None
        if attribute is not None:
            state = await _lock_attribute(db, row)
            if state is None or state.id != attribute["id"] or _attribute_observation(state) != attribute["expected"]:
                continue
        if hasattr(row, "last_apply_error"):
            row.last_apply_error = {
                "code": "refused",
                "refusals": list({content_digest(cause): cause for cause in records}.values()),
            }
        if state is not None:
            state.sync_state = SyncState.apply_failed
    await db.flush()
