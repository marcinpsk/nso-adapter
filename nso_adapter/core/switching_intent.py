# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Durable LAG and switchport desired-state snapshots (#1612).

These two families have no receipt lane: the POST PREPARES a snapshot and the manual Apply
authorizes it. A preparation replaces the live rows, bumps ``desired_revision`` and writes
the stream's prepared SLOT — the tables, the revision that selects them, and the deletion
provenance resolved against the AUTHORIZED roots. A store-only replacement writes the rows
and the revision and leaves the slot alone, so an Apply selecting the prepared revision
still promotes exactly what was prepared.

The slot holds TABLES ONLY. The fragment is frozen at authorization, inside the Apply that
promotes it, so the encoding context is the one read then and not the one read here.
"""

from __future__ import annotations

from collections.abc import Sequence
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy import update as sa_update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from nso_adapter.core.generation import lock_projection, note_write
from nso_adapter.core.projection import rows_by_intent_identity, snapshot_stream, stream_tables
from nso_adapter.store.models import (
    DeviceProjectionStream,
    LagBundleIntent,
    LagMemberIntent,
    SwitchportIntent,
    SwitchportTaggedVlanIntent,
)

#: The stream each writer prepares. Pinned against the route registry by
#: ``tests/core/test_switching_intent.py``, so a rename cannot split the two vocabularies.
LAG_STREAM = "lag"
SWITCHPORT_STREAM = "switchport"

#: The three marking groups one preparation resolves. ``delete_origin`` is what the operator
#: named, ``detach`` is every other authorized root the snapshot omits, and ``owned_content``
#: is a child dropped under a root that stays present and stays owned.
DELETION_GROUPS = ("delete_origin", "detach", "owned_content")


class SwitchingRequestRefused(ValueError):
    """A preparation the adapter refuses before it writes anything."""


@dataclass(frozen=True, slots=True)
class LagMemberSnapshot:
    interface_name: str
    mode: str | None = None
    port_priority: int | None = None


@dataclass(frozen=True, slots=True)
class LagBundleSnapshot:
    name: str
    lag_id: int | None = None
    min_links: int | None = None
    system_priority: int | None = None
    system_id: str | None = None
    timer: str | None = None
    admin_key: int | None = None
    members: tuple[LagMemberSnapshot, ...] = ()


@dataclass(frozen=True, slots=True)
class SwitchportSnapshot:
    interface_name: str
    mode: str | None = None
    untagged_vlan: int | None = None
    tagged_vlans: tuple[int, ...] = ()


@dataclass(frozen=True, slots=True)
class PreparedSnapshot:
    """What one accepted preparation recorded, and how an Apply selects it.

    ``selection_revision`` is the whole selection identity: the revision this request
    prepared, and ``None`` for a store-only replacement, which prepares nothing.
    """

    status: str
    stream: str
    count: int
    removed: int
    desired_revision: int
    selection_revision: int | None


_LAG_SCALARS = ("lag_id", "min_links", "system_priority", "system_id", "timer", "admin_key")


def _lag_changed(row: LagBundleIntent, bundle: LagBundleSnapshot) -> bool:
    if any(getattr(row, field) != getattr(bundle, field) for field in _LAG_SCALARS):
        return True
    current = {(member.interface_name, member.mode, member.port_priority) for member in row.members}
    desired = {(member.interface_name, member.mode, member.port_priority) for member in bundle.members}
    return current != desired


def _require_unique(values: Sequence[str | int], label: str) -> None:
    if len(values) != len(set(values)):
        raise ValueError(f"duplicate {label}")


def _require_uint(value: object, maximum: int, label: str, bits: int) -> None:
    if value is not None and (type(value) is not int or not 0 <= value <= maximum):
        raise ValueError(f"{label} must be a uint{bits}")


def _validate_lag_snapshot(bundles: Sequence[LagBundleSnapshot]) -> None:
    _require_unique([bundle.name for bundle in bundles], "LAG bundle name")
    for bundle in bundles:
        _require_uint(bundle.lag_id, 4294967295, "lag_id", 32)
        _require_uint(bundle.min_links, 65535, "min_links", 16)
        _require_uint(bundle.system_priority, 65535, "system_priority", 16)
        _require_uint(bundle.admin_key, 65535, "admin_key", 16)
        _require_unique([member.interface_name for member in bundle.members], "LAG member interface_name")
        for member in bundle.members:
            _require_uint(member.port_priority, 65535, "port_priority", 16)


def _validate_switchport_snapshot(interfaces: Sequence[SwitchportSnapshot]) -> None:
    _require_unique([interface.interface_name for interface in interfaces], "switchport interface_name")
    for interface in interfaces:
        _require_uint(interface.untagged_vlan, 65535, "untagged_vlan", 16)
        _require_unique(list(interface.tagged_vlans), "tagged VLAN")
        for vlan_id in interface.tagged_vlans:
            _require_uint(vlan_id, 65535, "tagged VLAN", 16)


def _refuse_unsupported_request_modes() -> None:
    """Refuse the two request modes these routes do not implement."""
    from nso_adapter.core.request_flags import BACKFILL_ONLY, DELETE_ORIGIN

    if DELETE_ORIGIN.get():
        raise SwitchingRequestRefused("delete_origin is not valid here: name the roots to retract in deleted_roots")
    if BACKFILL_ONLY.get():
        raise SwitchingRequestRefused("backfill_only is not valid on a switching snapshot")


def _root_names(fragment: dict, table: str) -> set[str]:
    """Return the root identities of *table* in *fragment*, one name each."""
    return {identity[0] for identity in rows_by_intent_identity(fragment or {}, table)}


def _resolve_deletions(
    authorized: dict,
    desired: dict,
    marked: set[str],
    root_table: str,
    child_table: str,
) -> dict[str, dict[str, list[dict]]]:
    """Split the authorized rows this snapshot drops into the three marking groups.

    The rows come from the AUTHORIZED fragment, never from the replaceable live tables, so
    the provenance is frozen with the snapshot it belongs to. A child's root is the first
    element of its parent-prefixed identity, so a root carries its whole subtree.
    """
    groups: dict[str, dict[str, list[dict]]] = {group: {} for group in DELETION_GROUPS}
    authorized_roots = rows_by_intent_identity(authorized, root_table)
    authorized_children = rows_by_intent_identity(authorized, child_table)
    retained = _root_names(desired, root_table)
    desired_children = rows_by_intent_identity(desired, child_table)
    for identity, row in authorized_roots.items():
        root = identity[0]
        if root in retained:
            continue
        group = "delete_origin" if root in marked else "detach"
        groups[group].setdefault(root_table, []).append(deepcopy(row))
        for child_identity, child in authorized_children.items():
            if child_identity[0] == root:
                groups[group].setdefault(child_table, []).append(deepcopy(child))
    for child_identity, child in authorized_children.items():
        if child_identity[0] in retained and child_identity not in desired_children:
            groups["owned_content"].setdefault(child_table, []).append(deepcopy(child))
    return groups


async def _prepare_snapshot(
    db: AsyncSession,
    device_id: int,
    stream: str,
    *,
    deleted_roots: Sequence[str],
    desired_roots: set[str],
    replace,
) -> PreparedSnapshot:
    """Own the whole order of one preparation: refuse, lock, validate, write, record.

    A refusal happens before any mutation, so the store and every revision are untouched.
    ``note_write`` runs exactly ONCE, with no push sequence, for a normal, an empty, an
    identical and a store-only replacement alike: ``desired_revision`` is what the store
    HOLDS. Only a normal replacement writes the slot; a store-only one preserves it, so an
    Apply selecting the prepared revision still promotes exactly what was prepared.
    """
    from nso_adapter.core.request_flags import STORE_ONLY

    _refuse_unsupported_request_modes()
    marked = list(deleted_roots)
    duplicates = sorted({root for root in marked if marked.count(root) > 1})
    if duplicates:
        raise SwitchingRequestRefused(f"deleted_roots repeats a root: {duplicates}")
    store_only = STORE_ONLY.get()
    if store_only and marked:
        raise SwitchingRequestRefused("a store-only replacement authorizes no deletion, so deleted_roots must be empty")
    kept = sorted(set(marked) & desired_roots)
    if kept:
        raise SwitchingRequestRefused(f"a deleted root is still present in this snapshot: {kept}")

    await lock_projection(db, device_id)
    root_table, child_table = stream_tables(stream)
    row = await db.scalar(
        select(DeviceProjectionStream).where(
            DeviceProjectionStream.device_id == device_id,
            DeviceProjectionStream.stream == stream,
        )
    )
    authorized = (row.authorized_document if row is not None else None) or {}
    unauthorized = sorted(set(marked) - _root_names(authorized, root_table))
    if unauthorized:
        raise SwitchingRequestRefused(f"a deleted root is not authorized on this device: {unauthorized}")

    revision = await note_write(db, device_id, stream, push_seq=None)
    count, removed = await replace()
    if store_only:
        return PreparedSnapshot("stored", stream, count, removed, revision, None)

    tables = await snapshot_stream(db, device_id, stream)
    await db.execute(
        sa_update(DeviceProjectionStream)
        .where(
            DeviceProjectionStream.device_id == device_id,
            DeviceProjectionStream.stream == stream,
        )
        .values(
            prepared_revision=revision,
            prepared_tables=tables,
            prepared_deletions=_resolve_deletions(authorized, tables, set(marked), root_table, child_table),
        )
        .execution_options(synchronize_session=False)
    )
    return PreparedSnapshot("prepared", stream, count, removed, revision, revision)


async def _replace_lag_rows(db: AsyncSession, device_id: int, bundles: Sequence[LagBundleSnapshot]) -> tuple[int, int]:
    existing = (
        (
            await db.execute(
                select(LagBundleIntent)
                .where(LagBundleIntent.device_id == device_id)
                .options(selectinload(LagBundleIntent.members))
            )
        )
        .scalars()
        .all()
    )
    by_name = {row.name: row for row in existing}
    desired_names = {bundle.name for bundle in bundles}
    removed = [row for name, row in by_name.items() if name not in desired_names]
    for row in removed:
        await db.delete(row)

    accepted_at = datetime.now(UTC)
    for bundle in bundles:
        lag_row = by_name.get(bundle.name)
        changed = lag_row is None or _lag_changed(lag_row, bundle)
        if lag_row is None:
            lag_row = LagBundleIntent(device_id=device_id, name=bundle.name, accepted_at=accepted_at, members=[])
            db.add(lag_row)
            await db.flush()

        for field in _LAG_SCALARS:
            setattr(lag_row, field, getattr(bundle, field))
        if changed:
            lag_row.accepted_at = accepted_at
            lag_row.last_apply_at = None
            lag_row.last_apply_error = None

        current_members = {member.interface_name: member for member in lag_row.members}
        desired_member_names = {member.interface_name for member in bundle.members}
        for name, existing_member in current_members.items():
            if name not in desired_member_names:
                lag_row.members.remove(existing_member)
        for member in bundle.members:
            desired_member = current_members.get(member.interface_name)
            if desired_member is None:
                desired_member = LagMemberIntent(interface_name=member.interface_name)
                lag_row.members.append(desired_member)
            desired_member.mode = member.mode
            desired_member.port_priority = member.port_priority

    await db.flush()
    return len(bundles), len(removed)


async def replace_lag_snapshot(
    db: AsyncSession,
    device_id: int,
    bundles: Sequence[LagBundleSnapshot],
    *,
    deleted_roots: Sequence[str],
) -> PreparedSnapshot:
    """Prepare one device's complete LAG snapshot. Caller commits."""
    _validate_lag_snapshot(bundles)
    return await _prepare_snapshot(
        db,
        device_id,
        LAG_STREAM,
        deleted_roots=deleted_roots,
        desired_roots={bundle.name for bundle in bundles},
        replace=lambda: _replace_lag_rows(db, device_id, bundles),
    )


async def _replace_switchport_rows(
    db: AsyncSession, device_id: int, interfaces: Sequence[SwitchportSnapshot]
) -> tuple[int, int]:
    existing = (
        (
            await db.execute(
                select(SwitchportIntent)
                .where(SwitchportIntent.device_id == device_id)
                .options(selectinload(SwitchportIntent.tagged_vlans))
            )
        )
        .scalars()
        .all()
    )
    by_name = {row.interface_name: row for row in existing}
    desired_names = {interface.interface_name for interface in interfaces}
    removed = [row for name, row in by_name.items() if name not in desired_names]
    for row in removed:
        await db.delete(row)

    accepted_at = datetime.now(UTC)
    for interface in interfaces:
        switchport_row = by_name.get(interface.interface_name)
        desired_tags = set(interface.tagged_vlans)
        changed = (
            switchport_row is None
            or switchport_row.mode != interface.mode
            or switchport_row.untagged_vlan != interface.untagged_vlan
        )
        if switchport_row is not None and {tag.vlan_id for tag in switchport_row.tagged_vlans} != desired_tags:
            changed = True
        if switchport_row is None:
            switchport_row = SwitchportIntent(
                device_id=device_id,
                interface_name=interface.interface_name,
                accepted_at=accepted_at,
                tagged_vlans=[],
            )
            db.add(switchport_row)
            await db.flush()

        switchport_row.mode = interface.mode
        switchport_row.untagged_vlan = interface.untagged_vlan
        if changed:
            switchport_row.accepted_at = accepted_at
            switchport_row.last_apply_at = None
            switchport_row.last_apply_error = None

        current_tags = {tag.vlan_id: tag for tag in switchport_row.tagged_vlans}
        for vlan_id, tag_row in current_tags.items():
            if vlan_id not in desired_tags:
                switchport_row.tagged_vlans.remove(tag_row)
        for vlan_id in interface.tagged_vlans:
            if vlan_id not in current_tags:
                switchport_row.tagged_vlans.append(SwitchportTaggedVlanIntent(vlan_id=vlan_id))

    await db.flush()
    return len(interfaces), len(removed)


async def replace_switchport_snapshot(
    db: AsyncSession,
    device_id: int,
    interfaces: Sequence[SwitchportSnapshot],
    *,
    deleted_roots: Sequence[str],
) -> PreparedSnapshot:
    """Prepare one device's complete switchport snapshot. Caller commits."""
    _validate_switchport_snapshot(interfaces)
    return await _prepare_snapshot(
        db,
        device_id,
        SWITCHPORT_STREAM,
        deleted_roots=deleted_roots,
        desired_roots={interface.interface_name for interface in interfaces},
        replace=lambda: _replace_switchport_rows(db, device_id, interfaces),
    )


#: LAG bundle store field -> its YANG leaf. Emitted only when the value is set.
_LAG_WIRE_FIELDS: tuple[tuple[str, str], ...] = (
    ("lag_id", "lag-id"),
    ("min_links", "min-links"),
    ("system_priority", "system-priority"),
    ("system_id", "system-id"),
    ("timer", "timer"),
    ("admin_key", "admin-key"),
)


def encode_lag_section(rows: dict[type, list], context: dict) -> dict:
    """Encode hydrated LAG rows into the section's wire container body.

    Pure: the rows plus the frozen encoding context are the whole input. This section
    declares no proof metadata and reads no live state, so *context* carries only the NED
    identity C9's registry hands every encoder; nothing in the LAG wire form depends on it.
    """
    bundles = rows.get(LagBundleIntent) or []
    if not bundles:
        return {}
    encoded_bundles: list[dict[str, object]] = []
    for bundle in sorted(bundles, key=lambda item: item.name):
        body: dict[str, object] = {"name": bundle.name}
        for field, leaf in _LAG_WIRE_FIELDS:
            if (value := getattr(bundle, field)) is not None:
                body[leaf] = value
        members: list[dict[str, object]] = []
        for member in sorted(bundle.members, key=lambda item: item.interface_name):
            encoded_member: dict[str, object] = {"interface-name": member.interface_name}
            if member.mode is not None:
                encoded_member["mode"] = member.mode
            if member.port_priority is not None:
                encoded_member["port-priority"] = member.port_priority
            members.append(encoded_member)
        if members:
            body["member"] = members
        encoded_bundles.append(body)
    return {"bundle": encoded_bundles}


def encode_switchport_section(rows: dict[type, list], context: dict) -> dict:
    """Encode hydrated switchport rows into the section's wire container body.

    Pure, for the same reason as :func:`encode_lag_section`.
    """
    interfaces = rows.get(SwitchportIntent) or []
    if not interfaces:
        return {}
    encoded: list[dict[str, object]] = []
    for interface in sorted(interfaces, key=lambda item: item.interface_name):
        body: dict[str, object] = {"interface-name": interface.interface_name}
        if interface.mode is not None:
            body["mode"] = interface.mode
        if interface.untagged_vlan is not None:
            body["untagged-vlan"] = interface.untagged_vlan
        if tagged_vlans := sorted(tag.vlan_id for tag in interface.tagged_vlans):
            body["tagged-vlan"] = tagged_vlans
        encoded.append(body)
    return {"interface": encoded}


__all__ = [
    "DELETION_GROUPS",
    "LAG_STREAM",
    "SWITCHPORT_STREAM",
    "LagBundleSnapshot",
    "LagMemberSnapshot",
    "PreparedSnapshot",
    "SwitchportSnapshot",
    "SwitchingRequestRefused",
    "encode_lag_section",
    "encode_switchport_section",
    "replace_lag_snapshot",
    "replace_switchport_snapshot",
]
