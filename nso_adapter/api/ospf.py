# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""GET /api/v1/devices/{id}/ospf and PUT /api/v1/devices/{id}/ospf-intent."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime

import structlog
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, get_read_db, verify_token
from nso_adapter.api.errors import RESP_401, RESP_404_DEVICE, RESP_409_PUSH_SEQ, RESP_422_VALIDATION, api_error
from nso_adapter.api.intent_push import begin_delivery, get_intent_delivery
from nso_adapter.api.read_state import FamilyReadState, read_state_payload
from nso_adapter.api.timestamps import iso_z, latest_refreshed
from nso_adapter.core.removal import is_cleared
from nso_adapter.store import outcome_store
from nso_adapter.store.models import (
    Device,
    DeviceOspfInstance,
    DeviceOspfInterface,
    DeviceSettings,
    OspfInstanceIntent,
    OspfInterfaceIntent,
    RedistributionIntent,
)

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/v1/devices", tags=["ospf"])


# ── Read-mirror response models (GET /ospf) ───────────────────────────────────
# `areas` is an opaque JSON list, always present. router_id/enabled and the interface
# optionals are emitted only when set (response_model_exclude_unset).


class OspfInstanceOut(BaseModel):
    process_id: str
    vrf: str
    areas: list = []
    router_id: str | None = None
    enabled: bool | None = None


class OspfInterfaceOut(BaseModel):
    interface_name: str
    passive: bool
    auth_present: bool
    process_id: str | None = None
    area_id: str | None = None
    priority: int | None = None
    cost: int | None = None
    network_type: str | None = None
    auth_type: str | None = None


class OspfConfigOut(BaseModel):
    device_id: int
    last_refreshed_at: str | None = None  # "<iso>Z", None when never refreshed
    refresh_source: str  # legacy freshness (S5 retires it); read_state is the S4 truth
    read_state: FamilyReadState
    instances: list[OspfInstanceOut]
    interfaces: list[OspfInterfaceOut]


@router.get(
    "/{device_id}/ospf",
    dependencies=[Depends(verify_token)],
    response_model=OspfConfigOut,
    response_model_exclude_unset=True,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_422_VALIDATION},
)
async def get_ospf(device_id: int, db: AsyncSession = Depends(get_read_db)):
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    # Pointer first, rows second, one snapshot (S4 D2 — benign direction).
    read_state = read_state_payload(
        await outcome_store.get_current_outcome(db, device_id, "ospf"), source_epoch=device.source_epoch
    )

    inst_result = await db.execute(
        select(DeviceOspfInstance)
        .where(DeviceOspfInstance.device_id == device_id)
        .order_by(DeviceOspfInstance.process_id)
    )
    inst_rows = inst_result.scalars().all()

    iface_result = await db.execute(
        select(DeviceOspfInterface)
        .where(DeviceOspfInterface.device_id == device_id)
        .order_by(DeviceOspfInterface.interface_name)
    )
    iface_rows = iface_result.scalars().all()

    all_rows = list(inst_rows) + list(iface_rows)
    if not all_rows:
        return {
            "device_id": device_id,
            "last_refreshed_at": None,
            "refresh_source": "never",
            "read_state": read_state,
            "instances": [],
            "interfaces": [],
        }

    latest = latest_refreshed(all_rows)

    instances = []
    for instance_row in inst_rows:
        instance_entry: dict = {
            "process_id": instance_row.process_id,
            "vrf": instance_row.vrf or "",
            "areas": instance_row.areas or [],
        }
        if instance_row.router_id is not None:
            instance_entry["router_id"] = instance_row.router_id
        if instance_row.enabled is not None:
            instance_entry["enabled"] = instance_row.enabled
        instances.append(instance_entry)

    interfaces = []
    for interface_row in iface_rows:
        interface_entry = {
            "interface_name": interface_row.interface_name,
            "passive": interface_row.passive,
            "auth_present": interface_row.auth_present,
        }
        if interface_row.process_id is not None:
            interface_entry["process_id"] = interface_row.process_id
        if interface_row.area_id is not None:
            interface_entry["area_id"] = interface_row.area_id
        if interface_row.priority is not None:
            interface_entry["priority"] = interface_row.priority
        if interface_row.cost is not None:
            interface_entry["cost"] = interface_row.cost
        if interface_row.network_type is not None:
            interface_entry["network_type"] = interface_row.network_type
        if interface_row.auth_type is not None:
            interface_entry["auth_type"] = interface_row.auth_type
        interfaces.append(interface_entry)

    return {
        "device_id": device_id,
        "last_refreshed_at": iso_z(latest.last_refreshed_at),
        "refresh_source": latest.refresh_source,
        "read_state": read_state,
        "instances": instances,
        "interfaces": interfaces,
    }


# ── Intent models ────────────────────────────────────────────────────────────


class RedistributionEntry(BaseModel):
    source_protocol: str
    source_ref: str = ""
    route_map: str | None = None
    metric: int | None = None
    metric_type: str | None = None


class OspfInstanceEntry(BaseModel):
    # process_id is a STRING (matches the OspfInstanceIntent.process_id String column and
    # the plugin's CharField; IOS-XR/Junos allow named processes). Declaring it int made
    # asyncpg reject the coerced value on insert — only surfaced once OSPF intent was first
    # pushed (greenfield Nokia OSPF).
    process_id: str
    router_id: str | None = None
    vrf: str = ""
    enabled: bool | None = None
    areas: list[dict] = []
    redistribution: list[RedistributionEntry] = []


class OspfInterfaceEntry(BaseModel):
    interface_name: str
    process_id: str | None = None
    area_id: str | None = None
    passive: bool = False
    priority: int | None = None
    cost: int | None = None
    network_type: str | None = None
    auth_type: str | None = None
    auth_key: str | None = None


class OspfIntentUpdate(BaseModel):
    instances: list[OspfInstanceEntry] = []
    interfaces: list[OspfInterfaceEntry] = []


async def _sync_keyed_intent(
    db: AsyncSession,
    model,
    device_id: int,
    *,
    key_attr: str,
    entries: list,
    now: datetime,
    apply_fields: Callable,
    make_row: Callable,
    state_fields: tuple[str, ...] = (),
) -> tuple[list[str], bool]:
    """Full-replace one keyed OSPF intent collection. Returns ``(removed_keys, cleared)``.

    Rows whose key is absent from *entries* are deleted; the rest are upserted
    (``make_row`` builds a new row from key+accepted_at, ``apply_fields`` writes
    the mutable fields on both new and existing rows).

    ``cleared`` is True when a RETAINED row's previously-set *state_fields* scalar went
    back to ``None`` (a cost blanked). The row stays owned and accepted, so nothing is
    un-owned — but a merge-PATCH apply never drops the leaf, so the caller must enqueue a
    PUT-replace removal that actually reaches the device. Kept apart from the removed keys
    because those are an UN-OWN and must NOT touch the device absent ?delete_origin (#106).
    """
    rows = (await db.execute(select(model).where(model.device_id == device_id))).scalars().all()
    existing = {getattr(r, key_attr): r for r in rows}
    incoming = {getattr(e, key_attr) for e in entries}
    removed = [k for k in existing if k not in incoming]
    for k in removed:
        await db.delete(existing[k])
    cleared = False
    for entry in entries:
        row = existing.get(getattr(entry, key_attr))
        before = {f: getattr(row, f) for f in state_fields} if row is not None else None
        if row is None:
            row = make_row(entry, now)
            db.add(row)
        apply_fields(row, entry)
        if before is not None and any(is_cleared(before[f], getattr(row, f)) for f in state_fields):
            cleared = True
    return removed, cleared


def _apply_ospf_instance_fields(row: OspfInstanceIntent, e: OspfInstanceEntry) -> None:
    row.router_id = e.router_id
    row.vrf = e.vrf
    row.areas = e.areas
    row.enabled = e.enabled


def _apply_ospf_interface_fields(row: OspfInterfaceIntent, e: OspfInterfaceEntry) -> None:
    row.process_id = e.process_id
    row.area_id = e.area_id
    row.passive = e.passive
    row.priority = e.priority
    row.cost = e.cost
    row.network_type = e.network_type
    row.auth_type = e.auth_type
    row.auth_key = e.auth_key


def _iter_ospf_redistribution(instances: list[OspfInstanceEntry]):
    """Yield ``(dest_ref, entry)`` for every per-instance redistribution entry (dest_ref = process_id)."""
    for inst in instances:
        dest_ref = str(inst.process_id)
        for entry in inst.redistribution:
            yield dest_ref, entry


async def _sync_ospf_redistribution(
    db: AsyncSession, device_id: int, instances: list[OspfInstanceEntry], now: datetime
) -> tuple[list[tuple], bool]:
    """Full-replace OSPF (dest_protocol=ospf) redistribution intent rows.

    Returns ``(removed_keys, cleared)`` — see :func:`_sync_ospf_intent` for why a dropped
    row and a cleared scalar must be told apart.
    """
    existing = (
        (
            await db.execute(
                select(RedistributionIntent).where(
                    RedistributionIntent.device_id == device_id,
                    RedistributionIntent.dest_protocol == "ospf",
                )
            )
        )
        .scalars()
        .all()
    )
    existing_map = {(r.dest_ref, r.source_protocol, r.source_ref): r for r in existing}
    incoming_keys = {
        (dest_ref, e.source_protocol, e.source_ref) for dest_ref, e in _iter_ospf_redistribution(instances)
    }

    removed = [k for k in existing_map if k not in incoming_keys]
    for key in removed:
        await db.delete(existing_map[key])

    cleared = False
    for dest_ref, entry in _iter_ospf_redistribution(instances):
        key = (dest_ref, entry.source_protocol, entry.source_ref)
        row = existing_map.get(key)
        if row is None:
            row = RedistributionIntent(
                device_id=device_id,
                dest_protocol="ospf",
                dest_ref=dest_ref,
                source_protocol=entry.source_protocol,
                source_ref=entry.source_ref,
                accepted_at=now,
            )
            db.add(row)
        else:
            for old, new in (
                (row.route_map, entry.route_map),
                (row.metric, entry.metric),
                (row.metric_type, entry.metric_type),
            ):
                if old is not None and new is None:
                    cleared = True
        row.route_map = entry.route_map
        row.metric = entry.metric
        row.metric_type = entry.metric_type
    return removed, cleared


async def _maybe_enqueue_apply(db: AsyncSession, device_id: int, count: int, *, stream: str) -> None:
    """Enqueue an apply job when the payload is non-empty and the device has auto_apply on."""
    if count <= 0:
        return
    settings = (
        await db.execute(select(DeviceSettings).where(DeviceSettings.device_id == device_id))
    ).scalar_one_or_none()
    if settings and settings.auto_apply:
        from nso_adapter.core.apply import enqueue_apply

        await enqueue_apply(db, device_id, force=True, stream=stream)


class OspfIntentResult(BaseModel):
    device_id: int
    instance_count: int
    interface_count: int


@router.put(
    "/{device_id}/ospf-intent",
    dependencies=[Depends(verify_token)],
    response_model=OspfIntentResult,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_409_PUSH_SEQ, **RESP_422_VALIDATION},
)
async def put_ospf_intent(
    device_id: int,
    payload: OspfIntentUpdate,
    db: AsyncSession = Depends(get_db),
    delivery=Depends(get_intent_delivery),
):
    """Replace the adapter's OSPF intent mirror for this device atomically.

    Full-replace semantics per device for instances, interfaces and (instance-scoped)
    redistribution. If ``auto_apply`` is enabled and the new payload is non-empty, an
    apply job is enqueued so the accepted config reaches the device. If any of the three
    dropped a row, a `removal` job is queued so the ospf-reconciler PUT-replace reverts it
    on-device (a merge-PATCH apply would not). Both jobs run in the background so this PUT
    never blocks on the device commit.
    """
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    # Every accepted write records its projection revision, store-only and
    # auto-apply-off included, and takes the device's projection lock before anything is
    # read (#1522 §G2). Only a promotion authorizes a deployment.
    from nso_adapter.core.receipt import record_response

    if (replay := await begin_delivery(db, device_id, delivery)) is not None:
        return replay

    now = datetime.now(UTC)

    removed_inst, inst_cleared = await _sync_keyed_intent(
        db,
        OspfInstanceIntent,
        device_id,
        key_attr="process_id",
        entries=payload.instances,
        now=now,
        apply_fields=_apply_ospf_instance_fields,
        make_row=lambda e, ts: OspfInstanceIntent(device_id=device_id, process_id=e.process_id, accepted_at=ts),
        # `vrf` is emitted only when truthy (`if row.vrf` in the writer), so a merge-PATCH
        # cannot drop it either — a cleared vrf needs the same PUT-replace retract.
        state_fields=("router_id", "areas", "enabled", "vrf"),
    )
    removed_iface, iface_cleared = await _sync_keyed_intent(
        db,
        OspfInterfaceIntent,
        device_id,
        key_attr="interface_name",
        entries=payload.interfaces,
        now=now,
        apply_fields=_apply_ospf_interface_fields,
        make_row=lambda e, ts: OspfInterfaceIntent(
            device_id=device_id, interface_name=e.interface_name, accepted_at=ts
        ),
        state_fields=("priority", "cost", "network_type", "auth_type", "auth_key"),
    )
    removed_redist, redist_cleared = await _sync_ospf_redistribution(db, device_id, payload.instances, now)

    # Same two-cause split as IS-IS (see api/isis.py): a DROPPED row is an un-own and must
    # not strip config off the device absent ?delete_origin (#106 → detach), while a
    # CLEARED scalar on a retained, still-owned row is an explicit operator retraction that
    # MUST reach the device (#83). OSPF only ever tracked dropped rows, so clearing a cost
    # queued nothing at all — and the merge-PATCH apply never drops a leaf, so the device
    # kept the old value forever and the operator could not clear it.
    deleted = bool(removed_inst or removed_iface or removed_redist)
    cleared = inst_cleared or iface_cleared or redist_cleared
    if deleted or cleared:
        from nso_adapter.core.removal import enqueue_removal, query_flag_marking

        # Thread the just-removed keys so the collateral guard can tell this intended
        # retraction from an orphaned service row (redistribute rows are nested,
        # non-guarded content — only the keyed lists matter here, hence `shrank`).
        marks = query_flag_marking(deletes=deleted)
        await enqueue_removal(
            db,
            device_id,
            "ospf",
            marking=marks.marking,
            defer_retract=marks.defer_retract,
            promotes=(delivery.stream,),
            removed={"interface-config": removed_iface, "process-config": removed_inst},
            retract=cleared,
            shrank=deleted,
        )

    await _maybe_enqueue_apply(db, device_id, len(payload.instances) + len(payload.interfaces), stream=delivery.stream)

    result = {
        "device_id": device_id,
        "instance_count": len(payload.instances),
        "interface_count": len(payload.interfaces),
    }
    await record_response(db, device_id, delivery, result)
    await db.commit()
    return result
