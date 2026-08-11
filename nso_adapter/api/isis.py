# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""GET /api/v1/devices/{id}/isis-interfaces and PUT /api/v1/devices/{id}/isis-interface-intent."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import structlog
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, get_read_db, verify_token
from nso_adapter.api.errors import RESP_401, RESP_404_DEVICE, RESP_409_PUSH_SEQ, RESP_422_VALIDATION, api_error
from nso_adapter.api.intent_push import begin_delivery, get_intent_delivery
from nso_adapter.api.read_state import FamilyReadState, read_state_payload
from nso_adapter.api.timestamps import UtcInstant, iso_z, latest_refreshed
from nso_adapter.core.removal import is_cleared
from nso_adapter.store import outcome_store
from nso_adapter.store.models import (
    Device,
    DeviceIsisInterface,
    DeviceIsisProcess,
    IsisFlexAlgoIntent,
    IsisInterfaceIntent,
    IsisLevelIntent,
    IsisProcessIntent,
    RedistributionIntent,
)

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/v1/devices", tags=["isis"])


def _snake(d: dict) -> dict:
    """Normalize a dict's hyphenated keys (as stored from NSO) to snake_case.

    So the plugin can map them straight onto netbox_routing model fields.
    """
    return {k.replace("-", "_"): v for k, v in d.items()}


# Process attributes surfaced only when not None (kept in their on-wire order).
_PROCESS_OPTIONAL_FIELDS = (
    "net",
    "is_type",
    "metric_style",
    "overload_bit",
    "area_auth_type",
    "area_auth_present",
    "area_auth_key",
    "domain_auth_type",
    "domain_auth_present",
    "domain_auth_key",
    "spf_initial_wait",
    "spf_max_wait",
    "lsp_initial_wait",
    "lsp_max_wait",
    "lsp_lifetime",
    "lsp_refresh_interval",
    "lsp_mtu",
    "overload_on_startup",
    "overload_timeout",
    "te_enabled",
    "suppress_attached_bit",
    "ignore_attached_bit",
    "fast_reroute",
    "microloop_avoidance",
    "distance",
    "maximum_paths",
    "reference_bandwidth",
    "segment_routing_reported",
    "segment_routing_configured",
)
_INTERFACE_OPTIONAL_FIELDS = (
    "circuit_type",
    "network_type",
    "metric",
    "bound_port",
    "hello_auth_type",
    "hello_auth_present",
    "bfd_enabled",
    "frr_enabled",
    "frr_protection",
    "csnp_interval",
    "retransmit_interval",
    "lsp_interval",
    "mesh_group",
)


def _present_fields(row, names: tuple[str, ...]) -> dict:
    """Include each named column only when its value is not None."""
    return {n: getattr(row, n) for n in names if getattr(row, n) is not None}


def _serialize_isis_process(row: DeviceIsisProcess) -> dict:
    entry: dict = {"process_tag": row.process_tag, **_present_fields(row, _PROCESS_OPTIONAL_FIELDS)}
    if row.settings:
        entry["settings"] = row.settings
    if row.levels:
        entry["levels"] = [_snake(lvl) for lvl in row.levels]
    if row.segment_routing is not None:
        entry["segment_routing"] = _snake(row.segment_routing)
    if row.flex_algos:
        entry["flex_algos"] = [_snake(fa) for fa in row.flex_algos]
    if row.srv6_locators:
        entry["srv6_locators"] = [_snake(loc) for loc in row.srv6_locators]
    return entry


def _serialize_isis_interface(row: DeviceIsisInterface) -> dict:
    entry: dict = {
        "interface_name": row.interface_name,
        "af": row.af,
        "process_tag": row.process_tag,
        **_present_fields(row, _INTERFACE_OPTIONAL_FIELDS),
    }
    if row.settings:
        entry["settings"] = row.settings
    if row.levels:
        entry["levels"] = [_snake(lvl) for lvl in row.levels]
    if row.prefix_sids:
        entry["prefix_sids"] = [_snake(ps) for ps in row.prefix_sids]
    entry["passive"] = row.passive
    return entry


# ── Read-mirror response models (GET /isis-interfaces) ────────────────────────
# The nested bags (settings/levels/segment_routing/flex_algos/srv6_locators/
# prefix_sids) are opaque JSON pass-throughs — typed dict/list so their contents
# are preserved verbatim (the reader _snake()s the container keys; the plugin
# reads fixed sub-keys the adapter does not itself constrain). Optional scalars
# and containers are emitted only when set (response_model_exclude_unset).


class IsisProcessOut(BaseModel):
    process_tag: str
    net: str | None = None
    is_type: str | None = None
    metric_style: str | None = None
    overload_bit: bool | None = None
    area_auth_type: str | None = None
    area_auth_present: bool | None = None
    area_auth_key: str | None = None
    domain_auth_type: str | None = None
    domain_auth_present: bool | None = None
    domain_auth_key: str | None = None
    spf_initial_wait: int | None = None
    spf_max_wait: int | None = None
    lsp_initial_wait: int | None = None
    lsp_max_wait: int | None = None
    lsp_lifetime: int | None = None
    lsp_refresh_interval: int | None = None
    lsp_mtu: int | None = None
    overload_on_startup: bool | None = None
    overload_timeout: int | None = None
    te_enabled: bool | None = None
    suppress_attached_bit: bool | None = None
    ignore_attached_bit: bool | None = None
    fast_reroute: str | None = None
    microloop_avoidance: bool | None = None
    distance: int | None = None
    maximum_paths: int | None = None
    reference_bandwidth: int | None = None
    segment_routing_reported: bool | None = None
    segment_routing_configured: bool | None = None
    settings: dict | None = None
    levels: list | None = None
    segment_routing: dict | None = None
    flex_algos: list | None = None
    srv6_locators: list | None = None


class IsisInterfaceOut(BaseModel):
    interface_name: str
    af: str
    process_tag: str
    circuit_type: str | None = None
    network_type: str | None = None
    metric: int | None = None
    bound_port: str | None = None
    hello_auth_type: str | None = None
    hello_auth_present: bool | None = None
    bfd_enabled: bool | None = None
    frr_enabled: bool | None = None
    frr_protection: str | None = None
    csnp_interval: int | None = None
    retransmit_interval: int | None = None
    lsp_interval: int | None = None
    mesh_group: str | None = None
    settings: dict | None = None
    levels: list | None = None
    prefix_sids: list | None = None
    passive: bool


class IsisInterfacesOut(BaseModel):
    device_id: int
    last_refreshed_at: str | None = None  # reader formats "<iso>Z"; None when never refreshed
    refresh_source: str  # legacy freshness (S5 retires it); read_state is the S4 truth
    read_state: FamilyReadState
    processes: list[IsisProcessOut]
    interfaces: list[IsisInterfaceOut]


@router.get(
    "/{device_id}/isis-interfaces",
    dependencies=[Depends(verify_token)],
    response_model=IsisInterfacesOut,
    response_model_exclude_unset=True,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_422_VALIDATION},
)
async def get_isis_interfaces(device_id: int, db: AsyncSession = Depends(get_read_db)):
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    # Pointer first, rows second, one snapshot (S4 D2 — benign direction).
    read_state = read_state_payload(
        await outcome_store.get_current_outcome(db, device_id, "isis"), source_epoch=device.source_epoch
    )

    proc_rows = (
        (
            await db.execute(
                select(DeviceIsisProcess)
                .where(DeviceIsisProcess.device_id == device_id)
                .order_by(DeviceIsisProcess.process_tag)
            )
        )
        .scalars()
        .all()
    )
    iface_rows = (
        (
            await db.execute(
                select(DeviceIsisInterface)
                .where(DeviceIsisInterface.device_id == device_id)
                .order_by(DeviceIsisInterface.interface_name, DeviceIsisInterface.af)
            )
        )
        .scalars()
        .all()
    )

    all_rows = list(proc_rows) + list(iface_rows)
    if not all_rows:
        return {
            "device_id": device_id,
            "last_refreshed_at": None,
            "refresh_source": "never",
            "read_state": read_state,
            "processes": [],
            "interfaces": [],
        }

    latest = latest_refreshed(all_rows)
    last_ts = latest.last_refreshed_at
    return {
        "device_id": device_id,
        "last_refreshed_at": iso_z(last_ts),
        "refresh_source": latest.refresh_source,
        "read_state": read_state,
        "processes": [_serialize_isis_process(r) for r in proc_rows],
        "interfaces": [_serialize_isis_interface(r) for r in iface_rows],
    }


# ---------------------------------------------------------------------------
# PUT /{device_id}/isis-interface-intent
# ---------------------------------------------------------------------------


class RedistributionEntry(BaseModel):
    source_protocol: str
    source_ref: str = ""
    route_map: str | None = None
    metric: int | None = None
    metric_type: str | None = None


class IsisInterfaceEntry(BaseModel):
    interface_name: str
    af: str
    process_tag: str = ""
    circuit_type: str | None = None
    network_type: str | None = None
    metric: int | None = None
    passive: bool = False
    bfd_enabled: bool | None = None
    frr_enabled: bool | None = None
    frr_protection: str | None = None
    accepted_at: UtcInstant | None = None


class IsisLevelEntry(BaseModel):
    level: int
    wide_metrics_only: bool | None = None
    labeled_preference: int | None = None
    disabled: bool | None = None


class IsisProcessEntry(BaseModel):
    process_tag: str = ""
    net: str | None = None
    is_type: str | None = None
    metric_style: str | None = None
    overload_bit: bool | None = None
    area_auth_type: str | None = None
    area_auth_key: str | None = None
    domain_auth_type: str | None = None
    domain_auth_key: str | None = None
    fast_reroute: str | None = None
    microloop_avoidance: bool | None = None
    accepted_at: UtcInstant | None = None
    redistribution: list[RedistributionEntry] = []
    levels: list[IsisLevelEntry] = []


class IsisInterfaceIntentUpdate(BaseModel):
    interfaces: list[IsisInterfaceEntry]
    processes: list[IsisProcessEntry] = []


async def _sync_keyed_intent(
    db, model, device_id: int, *, key_of, entries, now, apply_fields, make_row, state_fields=()
) -> tuple[int, bool, bool]:
    """Full-replace a keyed IS-IS intent collection. Returns ``(count, deleted, cleared)``.

    Rows whose key (``key_of``, possibly composite) is absent from *entries* are
    deleted; the rest are upserted — ``make_row`` builds a new identity row,
    ``apply_fields`` writes the mutable fields (incl. accepted_at) on new + existing.

    Both outcomes drop something the device may still carry, and both need the caller to
    enqueue an ``isis`` removal (a merge-PATCH apply never drops a leaf) — but they are
    DIFFERENT operations and the removal must treat them differently:

    ``deleted``  a whole row went away. Absent ?delete_origin this is an UN-OWN — NetBox
                 stops governing the row — and must not strip its config off the device
                 (#106: detach, no-networking).
    ``cleared``  a retained row's previously-set *state_fields* scalar went back to
                 ``None`` (metric blanked). The row is still owned and accepted, so
                 nothing is un-owned: this is an explicit operator retraction and MUST
                 reach the device (#83), even though no NetBox object was deleted and the
                 push therefore cannot carry ?delete_origin.

    ``state_fields`` empty ⇒ ``cleared`` is always False.
    """
    rows = (await db.execute(select(model).where(model.device_id == device_id))).scalars().all()
    existing = {key_of(r): r for r in rows}
    incoming = {key_of(e) for e in entries}
    deleted = False
    for key, row in existing.items():
        if key not in incoming:
            await db.delete(row)
            deleted = True
    await db.flush()

    count = 0
    cleared = False
    for entry in entries:
        accepted = entry.accepted_at if entry.accepted_at else now
        row = existing.get(key_of(entry))
        before = {f: getattr(row, f) for f in state_fields} if row is not None else None
        if row is None:
            row = make_row(entry)
            db.add(row)
        apply_fields(row, entry, accepted)
        if before is not None and any(is_cleared(before[f], getattr(row, f)) for f in state_fields):
            cleared = True
        count += 1
    return count, deleted, cleared


def _apply_isis_interface_fields(row: IsisInterfaceIntent, e: IsisInterfaceEntry, accepted: datetime) -> None:
    row.process_tag = e.process_tag
    row.circuit_type = e.circuit_type
    row.network_type = e.network_type
    row.metric = e.metric
    row.passive = e.passive
    row.bfd_enabled = e.bfd_enabled
    row.frr_enabled = e.frr_enabled
    row.frr_protection = e.frr_protection
    row.accepted_at = accepted


def _apply_isis_level_fields(row, entry, accepted) -> None:
    row.wide_metrics_only = entry.wide_metrics_only
    row.labeled_preference = entry.labeled_preference
    row.disabled = entry.disabled
    row.accepted_at = accepted


def _apply_isis_process_fields(row: IsisProcessIntent, e: IsisProcessEntry, accepted: datetime) -> None:
    row.net = e.net
    row.is_type = e.is_type
    row.metric_style = e.metric_style
    row.overload_bit = e.overload_bit
    row.area_auth_type = e.area_auth_type
    row.area_auth_key = e.area_auth_key
    row.domain_auth_type = e.domain_auth_type
    row.domain_auth_key = e.domain_auth_key
    row.fast_reroute = e.fast_reroute
    row.microloop_avoidance = e.microloop_avoidance
    row.accepted_at = accepted


def _iter_isis_redistribution(processes: list[IsisProcessEntry]):
    """Yield ``(dest_ref, entry)`` for every per-process redistribution entry (dest_ref = process_tag)."""
    for proc_entry in processes:
        dest_ref = proc_entry.process_tag
        for entry in proc_entry.redistribution:
            yield dest_ref, entry


async def _sync_isis_redistribution(
    db, device_id: int, processes: list[IsisProcessEntry], now: datetime
) -> tuple[bool, bool]:
    """Full-replace IS-IS (dest_protocol=isis) redistribution intent rows for this device.

    Returns ``(deleted, cleared)`` — a redistribution row dropped, and a retained row's
    ``route_map``/``metric``/``metric_type`` cleared to ``None``. A merge-PATCH apply can
    drop neither, so either makes the caller enqueue an ``isis`` removal; they are kept
    apart because only the second may reach the device without ?delete_origin (see
    :func:`_sync_keyed_intent`). Redistribute rows are nested, non-guarded content, so a
    dropped one never shows up in the removal's ``removed`` keys — the caller must thread
    ``deleted`` through explicitly.
    """
    existing = (
        (
            await db.execute(
                select(RedistributionIntent).where(
                    RedistributionIntent.device_id == device_id,
                    RedistributionIntent.dest_protocol == "isis",
                )
            )
        )
        .scalars()
        .all()
    )
    existing_map = {(r.dest_ref, r.source_protocol, r.source_ref): r for r in existing}
    incoming_keys = {
        (dest_ref, e.source_protocol, e.source_ref) for dest_ref, e in _iter_isis_redistribution(processes)
    }

    deleted = False
    for key in list(existing_map):
        if key not in incoming_keys:
            await db.delete(existing_map[key])
            deleted = True

    cleared = False
    for dest_ref, entry in _iter_isis_redistribution(processes):
        key = (dest_ref, entry.source_protocol, entry.source_ref)
        row = existing_map.get(key)
        if row is None:
            row = RedistributionIntent(
                device_id=device_id,
                dest_protocol="isis",
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
    return deleted, cleared


async def _maybe_enqueue_isis_apply(db, device_id: int, iface_count: int, proc_count: int, *, stream: str) -> None:
    """Enqueue an apply job when auto_apply is on and the payload changed something."""
    from nso_adapter.store.models import DeviceSettings

    settings = (
        await db.execute(select(DeviceSettings).where(DeviceSettings.device_id == device_id))
    ).scalar_one_or_none()
    if settings and settings.auto_apply and (iface_count > 0 or proc_count > 0):
        from nso_adapter.core.apply import enqueue_apply

        await enqueue_apply(db, device_id, force=True, stream=stream)


class IsisInterfaceIntentResult(BaseModel):
    device_id: int
    interface_count: int
    process_count: int


@router.put(
    "/{device_id}/isis-interface-intent",
    dependencies=[Depends(verify_token)],
    response_model=IsisInterfaceIntentResult,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_409_PUSH_SEQ, **RESP_422_VALIDATION},
)
async def put_isis_interface_intent(
    device_id: int,
    body: IsisInterfaceIntentUpdate,
    db: AsyncSession = Depends(get_db),
    delivery=Depends(get_intent_delivery),
):
    """Replace the adapter's IS-IS interface and process intent mirror for this device atomically.

    Full-replace semantics: rows not present in the request body are deleted.
    ``accepted_at`` defaults to now if not supplied.  If ``auto_apply`` is
    enabled on the device, an apply job is enqueued after the upsert.
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

    # Capture the pre-sync keys so the removal job knows what THIS put deleted —
    # the collateral guard's discriminator between an intended retraction and an
    # orphaned service row (the ra1 lo0 incident).
    pre_iface_keys = {
        (r.interface_name, r.af)
        for r in (
            await db.execute(select(IsisInterfaceIntent).where(IsisInterfaceIntent.device_id == device_id))
        ).scalars()
    }
    pre_proc_tags = {
        r.process_tag
        for r in (await db.execute(select(IsisProcessIntent).where(IsisProcessIntent.device_id == device_id))).scalars()
    }

    iface_count, iface_deleted, iface_cleared = await _sync_keyed_intent(
        db,
        IsisInterfaceIntent,
        device_id,
        key_of=lambda x: (x.interface_name, x.af),
        entries=body.interfaces,
        now=now,
        apply_fields=_apply_isis_interface_fields,
        make_row=lambda e: IsisInterfaceIntent(device_id=device_id, interface_name=e.interface_name, af=e.af),
        state_fields=("circuit_type", "network_type", "metric", "bfd_enabled", "frr_enabled", "frr_protection"),
    )
    proc_count, proc_deleted, proc_cleared = await _sync_keyed_intent(
        db,
        IsisProcessIntent,
        device_id,
        key_of=lambda x: x.process_tag,
        entries=body.processes,
        now=now,
        apply_fields=_apply_isis_process_fields,
        make_row=lambda e: IsisProcessIntent(device_id=device_id, process_tag=e.process_tag),
        state_fields=(
            "net",  # `if row.net is not None` in the writer → a merge-PATCH cannot drop it
            "is_type",
            "metric_style",
            "overload_bit",
            "area_auth_type",
            "area_auth_key",
            "domain_auth_type",
            "domain_auth_key",
            "fast_reroute",
            "microloop_avoidance",
        ),
    )
    # Flatten the per-process level entries into (process_tag, level)-keyed rows;
    # accepted_at rides the parent process entry (a level is accepted with its process).
    level_entries = [
        SimpleNamespace(
            process_tag=p.process_tag,
            level=lv.level,
            wide_metrics_only=lv.wide_metrics_only,
            labeled_preference=lv.labeled_preference,
            disabled=lv.disabled,
            accepted_at=p.accepted_at,
        )
        for p in body.processes
        for lv in p.levels
    ]
    _, level_deleted, level_cleared = await _sync_keyed_intent(
        db,
        IsisLevelIntent,
        device_id,
        key_of=lambda x: (x.process_tag, x.level),
        entries=level_entries,
        now=now,
        apply_fields=_apply_isis_level_fields,
        make_row=lambda e: IsisLevelIntent(device_id=device_id, process_tag=e.process_tag, level=e.level),
        state_fields=("wide_metrics_only", "labeled_preference", "disabled"),
    )
    redist_deleted, redist_cleared = await _sync_isis_redistribution(db, device_id, body.processes, now)

    # A merge-PATCH apply never drops a cleared/deleted leaf, so retracting owned IS-IS
    # intent needs a PUT-replace. Queue the async ``isis`` removal job — it re-asserts the
    # full remaining accepted snapshot so FASTMAP reverts what was dropped.
    #
    # The two causes are NOT the same operation and the job must tell them apart:
    #   deleted — a row went away. Absent ?delete_origin that is an UN-OWN and must not
    #             strip the row's config off the device (#106 → detach).
    #   cleared — a retained, still-owned row's scalar was blanked (metric back to none).
    #             Nothing is un-owned, so this MUST reach the device (#83) even though no
    #             NetBox object was deleted and the push cannot carry ?delete_origin.
    # Conflating them is what let detach-by-default silently kill the cleared-scalar
    # retract: the device kept the old value forever while the operator saw it as removed.
    deleted = iface_deleted or proc_deleted or level_deleted or redist_deleted
    cleared = iface_cleared or proc_cleared or level_cleared or redist_cleared
    if deleted or cleared:
        from nso_adapter.core.removal import enqueue_removal, query_flag_marking

        removed_ifaces = sorted(pre_iface_keys - {(e.interface_name, e.af) for e in body.interfaces})
        removed_procs = sorted(pre_proc_tags - {p.process_tag for p in body.processes})
        marks = query_flag_marking(deletes=deleted)
        await enqueue_removal(
            db,
            device_id,
            "isis",
            marking=marks.marking,
            defer_retract=marks.defer_retract,
            promotes=(delivery.stream,),
            removed={"interface-config": removed_ifaces, "process-config": removed_procs},
            retract=cleared,
            shrank=deleted,
        )

    await _maybe_enqueue_isis_apply(db, device_id, iface_count, proc_count, stream=delivery.stream)

    result = {"device_id": device_id, "interface_count": iface_count, "process_count": proc_count}
    await record_response(db, device_id, delivery, result)
    await db.commit()
    return result


class IsisFlexAlgoEntry(BaseModel):
    process_tag: str = ""
    algo_id: int
    metric_type: str | None = None
    priority: int | None = None
    admin_group_exclude: str | None = None
    admin_group_include_any: str | None = None
    admin_group_include_all: str | None = None
    accepted_at: UtcInstant | None = None


class IsisFlexAlgoIntentUpdate(BaseModel):
    flex_algos: list[IsisFlexAlgoEntry]


class IsisFlexAlgoIntentResult(BaseModel):
    device_id: int
    flex_algo_count: int
    removal_queued: bool


@router.put(
    "/{device_id}/isis-flex-algo-intent",
    dependencies=[Depends(verify_token)],
    response_model=IsisFlexAlgoIntentResult,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_409_PUSH_SEQ, **RESP_422_VALIDATION},
)
async def put_isis_flex_algo_intent(
    device_id: int,
    body: IsisFlexAlgoIntentUpdate,
    db: AsyncSession = Depends(get_db),
    delivery=Depends(get_intent_delivery),
):
    """Replace the adapter's IS-IS Flex-Algorithm intent mirror for this device atomically.

    Full-replace semantics: rows not present in the request body are deleted.
    ``accepted_at`` defaults to now if not supplied.  If ``auto_apply`` is enabled
    on the device, an apply job is enqueued after the upsert.
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

    existing_result = await db.execute(select(IsisFlexAlgoIntent).where(IsisFlexAlgoIntent.device_id == device_id))
    existing_rows: dict[tuple, IsisFlexAlgoIntent] = {
        (r.process_tag, r.algo_id): r for r in existing_result.scalars().all()
    }
    new_keys: set[tuple] = {(item.process_tag, item.algo_id) for item in body.flex_algos}

    removed_keys: list[tuple] = []
    for key, row in existing_rows.items():
        if key not in new_keys:
            removed_keys.append(key)
            await db.delete(row)
    await db.flush()

    _FLEX_STATE_FIELDS = (
        "metric_type",
        "priority",
        "admin_group_exclude",
        "admin_group_include_any",
        "admin_group_include_all",
    )

    count = 0
    cleared = False
    for item in body.flex_algos:
        key = (item.process_tag, item.algo_id)
        accepted = item.accepted_at if item.accepted_at else now
        flex_row = existing_rows.get(key)
        # Every flex-algo scalar is emitted only when set (`if row.priority is not None`),
        # so a merge-PATCH apply can never drop one that goes back to unset — a cleared
        # scalar needs the same PUT-replace retract as a dropped flex-algo.
        before = {f: getattr(flex_row, f) for f in _FLEX_STATE_FIELDS} if flex_row is not None else None
        if flex_row is None:
            flex_row = IsisFlexAlgoIntent(
                device_id=device_id,
                process_tag=item.process_tag,
                algo_id=item.algo_id,
                accepted_at=accepted,
            )
            db.add(flex_row)
        flex_row.metric_type = item.metric_type
        flex_row.priority = item.priority
        flex_row.admin_group_exclude = item.admin_group_exclude
        flex_row.admin_group_include_any = item.admin_group_include_any
        flex_row.admin_group_include_all = item.admin_group_include_all
        if before is not None and any(is_cleared(before[f], getattr(flex_row, f)) for f in _FLEX_STATE_FIELDS):
            cleared = True
        flex_row.accepted_at = accepted
        count += 1

    await db.flush()

    # A merge-PATCH apply never drops an omitted flex-algo (and a node-level DELETE can't
    # address an empty-string process-tag key), so retracting one needs a PUT-replace of
    # the whole service. Queue the async ``isis`` removal job — :func:`_replace_isis`
    # re-asserts the full remaining accepted snapshot (flex-algos included) so FASTMAP
    # reverts what was dropped.
    #
    # This MUST go through enqueue_removal rather than replacing the service inline: that
    # is the single choke point where STORE_ONLY (#103 — the plugin's re-sync promises it
    # "does not touch the device"), the un-own detach (#106) and the collateral orphan
    # guard (#90) are enforced. An inline write here bypassed all three and could retract
    # live IS-IS config on a re-sync push. No ``removed=`` is passed: a flex-algo lives
    # INSIDE process-config, so the shrink removes no key at the guard's grain and needs
    # no orphan allowance.
    removal_queued = False
    if removed_keys or cleared:
        from nso_adapter.core.removal import enqueue_removal, query_flag_marking

        marks = query_flag_marking(deletes=bool(removed_keys))
        job = await enqueue_removal(
            db,
            device_id,
            "isis",
            marking=marks.marking,
            defer_retract=marks.defer_retract,
            promotes=(delivery.stream,),
            retract=cleared,
            shrank=bool(removed_keys),
        )
        removal_queued = job is not None

    await _maybe_enqueue_isis_apply(db, device_id, count, 0, stream=delivery.stream)

    result = {"device_id": device_id, "flex_algo_count": count, "removal_queued": removal_queued}
    await record_response(db, device_id, delivery, result)
    await db.commit()
    return result
