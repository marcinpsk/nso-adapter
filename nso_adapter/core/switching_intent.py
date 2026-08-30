# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Durable LAG and switchport desired-state snapshots."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from nso_adapter.core.generation import lock_device_document
from nso_adapter.store.models import (
    LagBundleIntent,
    LagMemberIntent,
    SwitchportIntent,
    SwitchportTaggedVlanIntent,
)


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
class ReplacementSummary:
    count: int
    removed: int


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


async def replace_lag_snapshot(
    db: AsyncSession,
    device_id: int,
    bundles: Sequence[LagBundleSnapshot],
) -> ReplacementSummary:
    """Atomically reconcile one device's complete LAG snapshot. Caller commits."""
    _validate_lag_snapshot(bundles)
    await lock_device_document(db, device_id)
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
                await db.delete(existing_member)
        for member in bundle.members:
            desired_member = current_members.get(member.interface_name)
            if desired_member is None:
                desired_member = LagMemberIntent(lag_bundle_id=lag_row.id, interface_name=member.interface_name)
                db.add(desired_member)
            desired_member.mode = member.mode
            desired_member.port_priority = member.port_priority

    await db.flush()
    return ReplacementSummary(count=len(bundles), removed=len(removed))


async def replace_switchport_snapshot(
    db: AsyncSession,
    device_id: int,
    interfaces: Sequence[SwitchportSnapshot],
) -> ReplacementSummary:
    """Atomically reconcile one device's complete switchport snapshot. Caller commits."""
    _validate_switchport_snapshot(interfaces)
    await lock_device_document(db, device_id)
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
                await db.delete(tag_row)
        for vlan_id in interface.tagged_vlans:
            if vlan_id not in current_tags:
                db.add(SwitchportTaggedVlanIntent(switchport_id=switchport_row.id, vlan_id=vlan_id))

    await db.flush()
    return ReplacementSummary(count=len(interfaces), removed=len(removed))


async def render_switching_sections(db: AsyncSession, device_id: int) -> dict[str, dict]:
    """Render canonical YANG sections while the caller holds the device-document lock."""
    lag_rows = (
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
    switchport_rows = (
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
    sections: dict[str, dict] = {}
    if lag_rows:
        rendered_bundles: list[dict[str, object]] = []
        for lag_row in sorted(lag_rows, key=lambda item: item.name):
            rendered_bundle: dict[str, object] = {"name": lag_row.name}
            for field, wire_name in (
                ("lag_id", "lag-id"),
                ("min_links", "min-links"),
                ("system_priority", "system-priority"),
                ("system_id", "system-id"),
                ("timer", "timer"),
                ("admin_key", "admin-key"),
            ):
                if (value := getattr(lag_row, field)) is not None:
                    rendered_bundle[wire_name] = value
            rendered_members: list[dict[str, object]] = []
            for row_member in sorted(lag_row.members, key=lambda item: item.interface_name):
                rendered_member: dict[str, object] = {"interface-name": row_member.interface_name}
                if row_member.mode is not None:
                    rendered_member["mode"] = row_member.mode
                if row_member.port_priority is not None:
                    rendered_member["port-priority"] = row_member.port_priority
                rendered_members.append(rendered_member)
            if rendered_members:
                rendered_bundle["member"] = rendered_members
            rendered_bundles.append(rendered_bundle)
        sections["lag"] = {"bundle": rendered_bundles}
    if switchport_rows:
        rendered_interfaces: list[dict[str, object]] = []
        for switchport_row in sorted(switchport_rows, key=lambda item: item.interface_name):
            rendered_interface: dict[str, object] = {"interface-name": switchport_row.interface_name}
            if switchport_row.mode is not None:
                rendered_interface["mode"] = switchport_row.mode
            if switchport_row.untagged_vlan is not None:
                rendered_interface["untagged-vlan"] = switchport_row.untagged_vlan
            if tagged_vlans := sorted(tag.vlan_id for tag in switchport_row.tagged_vlans):
                rendered_interface["tagged-vlan"] = tagged_vlans
            rendered_interfaces.append(rendered_interface)
        sections["switchport"] = {"interface": rendered_interfaces}
    return sections


__all__ = [
    "LagBundleSnapshot",
    "LagMemberSnapshot",
    "ReplacementSummary",
    "SwitchportSnapshot",
    "render_switching_sections",
    "replace_lag_snapshot",
    "replace_switchport_snapshot",
]
