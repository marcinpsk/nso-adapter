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

from nso_adapter.api.deps import get_db, verify_token
from nso_adapter.api.errors import api_error
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
    "distance",
    "maximum_paths",
    "reference_bandwidth",
)
_INTERFACE_OPTIONAL_FIELDS = (
    "circuit_type",
    "network_type",
    "metric",
    "bound_port",
    "hello_auth_type",
    "hello_auth_present",
    "bfd_enabled",
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
    if row.segment_routing:
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


@router.get("/{device_id}/isis-interfaces", dependencies=[Depends(verify_token)])
async def get_isis_interfaces(device_id: int, db: AsyncSession = Depends(get_db)):
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

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
            "processes": [],
            "interfaces": [],
        }

    latest = max(all_rows, key=lambda r: r.last_refreshed_at or "")
    last_ts = latest.last_refreshed_at
    return {
        "device_id": device_id,
        "last_refreshed_at": last_ts.isoformat() + "Z" if last_ts else None,
        "refresh_source": latest.refresh_source,
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
    accepted_at: datetime | None = None


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
    accepted_at: datetime | None = None
    redistribution: list[RedistributionEntry] = []
    levels: list[IsisLevelEntry] = []


class IsisInterfaceIntentUpdate(BaseModel):
    interfaces: list[IsisInterfaceEntry]
    processes: list[IsisProcessEntry] = []


async def _sync_keyed_intent(
    db, model, device_id: int, *, key_of, entries, now, apply_fields, make_row, state_fields=()
) -> tuple[int, bool]:
    """Full-replace a keyed IS-IS intent collection. Returns ``(count, retracted)``.

    Rows whose key (``key_of``, possibly composite) is absent from *entries* are
    deleted; the rest are upserted — ``make_row`` builds a new identity row,
    ``apply_fields`` writes the mutable fields (incl. accepted_at) on new + existing.

    ``retracted`` is True when this sync drops something the device may still carry —
    a whole row deleted, OR a retained row whose previously-set *state_fields* scalar
    is cleared to ``None`` (metric back to blank). A merge-PATCH apply never drops such
    a leaf, so the caller must enqueue an ``isis`` removal (PUT-replace) to revert it.
    ``state_fields`` empty ⇒ only deletions count as retractions.
    """
    rows = (await db.execute(select(model).where(model.device_id == device_id))).scalars().all()
    existing = {key_of(r): r for r in rows}
    incoming = {key_of(e) for e in entries}
    retracted = False
    for key, row in existing.items():
        if key not in incoming:
            await db.delete(row)
            retracted = True
    await db.flush()

    count = 0
    for entry in entries:
        accepted = entry.accepted_at.replace(tzinfo=None) if entry.accepted_at else now
        row = existing.get(key_of(entry))
        before = {f: getattr(row, f) for f in state_fields} if row is not None else None
        if row is None:
            row = make_row(entry)
            db.add(row)
        apply_fields(row, entry, accepted)
        if before is not None and any(before[f] is not None and getattr(row, f) is None for f in state_fields):
            retracted = True
        count += 1
    return count, retracted


def _apply_isis_interface_fields(row: IsisInterfaceIntent, e: IsisInterfaceEntry, accepted: datetime) -> None:
    row.process_tag = e.process_tag
    row.circuit_type = e.circuit_type
    row.network_type = e.network_type
    row.metric = e.metric
    row.passive = e.passive
    row.bfd_enabled = e.bfd_enabled
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
    row.accepted_at = accepted


def _iter_isis_redistribution(processes: list[IsisProcessEntry]):
    """Yield ``(dest_ref, entry)`` for every per-process redistribution entry (dest_ref = process_tag)."""
    for proc_entry in processes:
        dest_ref = proc_entry.process_tag
        for entry in proc_entry.redistribution:
            yield dest_ref, entry


async def _sync_isis_redistribution(db, device_id: int, processes: list[IsisProcessEntry], now: datetime) -> bool:
    """Full-replace IS-IS (dest_protocol=isis) redistribution intent rows for this device.

    Returns True if this sync retracts something (a redistribution row deleted, or a
    retained row's ``route_map``/``metric``/``metric_type`` cleared to ``None``) — a
    merge-PATCH apply cannot drop it, so the caller enqueues an ``isis`` removal.
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

    retracted = False
    for key in list(existing_map):
        if key not in incoming_keys:
            await db.delete(existing_map[key])
            retracted = True

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
                    retracted = True
        row.route_map = entry.route_map
        row.metric = entry.metric
        row.metric_type = entry.metric_type
    return retracted


async def _maybe_enqueue_isis_apply(db, device_id: int, iface_count: int, proc_count: int) -> None:
    """Enqueue an apply job when auto_apply is on and the payload changed something."""
    from nso_adapter.store.models import DeviceSettings

    settings = (
        await db.execute(select(DeviceSettings).where(DeviceSettings.device_id == device_id))
    ).scalar_one_or_none()
    if settings and settings.auto_apply and (iface_count > 0 or proc_count > 0):
        from nso_adapter.core.apply import enqueue_apply

        await enqueue_apply(db, device_id, force=True)


@router.put("/{device_id}/isis-interface-intent", dependencies=[Depends(verify_token)])
async def put_isis_interface_intent(
    device_id: int, body: IsisInterfaceIntentUpdate, db: AsyncSession = Depends(get_db)
):
    """Replace the adapter's IS-IS interface and process intent mirror for this device atomically.

    Full-replace semantics: rows not present in the request body are deleted.
    ``accepted_at`` defaults to now if not supplied.  If ``auto_apply`` is
    enabled on the device, an apply job is enqueued after the upsert.
    """
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    now = datetime.now(UTC).replace(tzinfo=None)

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

    iface_count, iface_retracted = await _sync_keyed_intent(
        db,
        IsisInterfaceIntent,
        device_id,
        key_of=lambda x: (x.interface_name, x.af),
        entries=body.interfaces,
        now=now,
        apply_fields=_apply_isis_interface_fields,
        make_row=lambda e: IsisInterfaceIntent(device_id=device_id, interface_name=e.interface_name, af=e.af),
        state_fields=("circuit_type", "network_type", "metric", "bfd_enabled"),
    )
    proc_count, proc_retracted = await _sync_keyed_intent(
        db,
        IsisProcessIntent,
        device_id,
        key_of=lambda x: x.process_tag,
        entries=body.processes,
        now=now,
        apply_fields=_apply_isis_process_fields,
        make_row=lambda e: IsisProcessIntent(device_id=device_id, process_tag=e.process_tag),
        state_fields=(
            "is_type",
            "metric_style",
            "overload_bit",
            "area_auth_type",
            "area_auth_key",
            "domain_auth_type",
            "domain_auth_key",
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
    _, level_retracted = await _sync_keyed_intent(
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
    redist_retracted = await _sync_isis_redistribution(db, device_id, body.processes, now)
    await _maybe_enqueue_isis_apply(db, device_id, iface_count, proc_count)

    # A merge-PATCH apply never drops a cleared/deleted leaf, so retracting owned IS-IS
    # intent (metric back to blank, an interface un-accepted) needs a PUT-replace. Queue
    # the async ``isis`` removal job — it re-asserts the full remaining accepted snapshot
    # so FASTMAP reverts what was dropped, while un-owned brownfield stays (reconcile).
    if iface_retracted or proc_retracted or level_retracted or redist_retracted:
        from nso_adapter.core.removal import enqueue_removal

        removed_ifaces = sorted(pre_iface_keys - {(e.interface_name, e.af) for e in body.interfaces})
        removed_procs = sorted(pre_proc_tags - {p.process_tag for p in body.processes})
        await enqueue_removal(db, device_id, "isis", removed_interfaces=removed_ifaces, removed_processes=removed_procs)

    await db.commit()
    return {"device_id": device_id, "interface_count": iface_count, "process_count": proc_count}


class IsisFlexAlgoEntry(BaseModel):
    process_tag: str = ""
    algo_id: int
    metric_type: str | None = None
    priority: int | None = None
    admin_group_exclude: str | None = None
    admin_group_include_any: str | None = None
    admin_group_include_all: str | None = None
    accepted_at: datetime | None = None


class IsisFlexAlgoIntentUpdate(BaseModel):
    flex_algos: list[IsisFlexAlgoEntry]


@router.put("/{device_id}/isis-flex-algo-intent", dependencies=[Depends(verify_token)])
async def put_isis_flex_algo_intent(device_id: int, body: IsisFlexAlgoIntentUpdate, db: AsyncSession = Depends(get_db)):
    """Replace the adapter's IS-IS Flex-Algorithm intent mirror for this device atomically.

    Full-replace semantics: rows not present in the request body are deleted.
    ``accepted_at`` defaults to now if not supplied.  If ``auto_apply`` is enabled
    on the device, an apply job is enqueued after the upsert.
    """
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    now = datetime.now(UTC).replace(tzinfo=None)

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

    count = 0
    for item in body.flex_algos:
        key = (item.process_tag, item.algo_id)
        accepted = item.accepted_at.replace(tzinfo=None) if item.accepted_at else now
        row = existing_rows.get(key)
        if row is None:
            row = IsisFlexAlgoIntent(
                device_id=device_id,
                process_tag=item.process_tag,
                algo_id=item.algo_id,
                accepted_at=accepted,
            )
            db.add(row)
        row.metric_type = item.metric_type
        row.priority = item.priority
        row.admin_group_exclude = item.admin_group_exclude
        row.admin_group_include_any = item.admin_group_include_any
        row.admin_group_include_all = item.admin_group_include_all
        row.accepted_at = accepted
        count += 1

    await db.flush()

    from nso_adapter.store.models import DeviceSettings

    settings_result = await db.execute(select(DeviceSettings).where(DeviceSettings.device_id == device_id))
    settings = settings_result.scalar_one_or_none()
    if settings and settings.auto_apply and count > 0:
        from nso_adapter.core.apply import enqueue_apply

        await enqueue_apply(db, device_id, force=True)

    await db.commit()

    # Removal must be pushed to NSO explicitly: a merge-PATCH on the next apply
    # would not drop the omitted entry, and a node-level DELETE can't address an
    # empty-string process-tag key.  PUT-replace the whole process-config list
    # (built from the full desired state) so FASTMAP reverts removed flex-algos.
    replaced = False
    if removed_keys:
        from nso_adapter.core.importer import get_nso_client
        from nso_adapter.nso.apply import (
            build_isis_interface_payload,
            build_isis_process_payload,
            replace_isis_service,
        )

        proc_rows = (
            (
                await db.execute(
                    select(IsisProcessIntent).where(
                        IsisProcessIntent.device_id == device_id,
                        IsisProcessIntent.accepted_at.is_not(None),
                    )
                )
            )
            .scalars()
            .all()
        )
        flex_rows = (
            (
                await db.execute(
                    select(IsisFlexAlgoIntent).where(
                        IsisFlexAlgoIntent.device_id == device_id,
                        IsisFlexAlgoIntent.accepted_at.is_not(None),
                    )
                )
            )
            .scalars()
            .all()
        )
        redist_rows = (
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
        iface_rows = (
            (
                await db.execute(
                    select(IsisInterfaceIntent).where(
                        IsisInterfaceIntent.device_id == device_id,
                        IsisInterfaceIntent.accepted_at.is_not(None),
                    )
                )
            )
            .scalars()
            .all()
        )
        level_rows = (
            (
                await db.execute(
                    select(IsisLevelIntent).where(
                        IsisLevelIntent.device_id == device_id,
                        IsisLevelIntent.accepted_at.is_not(None),
                    )
                )
            )
            .scalars()
            .all()
        )
        processes = build_isis_process_payload(proc_rows, redist_rows, flex_rows, level_rows)
        interfaces = build_isis_interface_payload(iface_rows)
        try:
            nso_client = get_nso_client(device.nso_instance)
            await replace_isis_service(nso_client, device.nso_device_name, interfaces, processes)
            replaced = True
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "isis_flex_algo.service_replace_failed",
                device_id=device_id,
                error=repr(exc),
            )

    return {"device_id": device_id, "flex_algo_count": count, "service_replaced": replaced}
