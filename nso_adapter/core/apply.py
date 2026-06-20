# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Apply worker — push accepted intent to NSO (Phase 2).

Follows the flow described in docs/nso-adapter.md §7a:
  1. Snapshot intent into job.context
  2. Mark each in-scope attribute as 'deploying'
  3. Commit each (interface, attribute) via NSO reconcile-commit service
  4. On success: status → in_sync, update last_apply_at
  5. On failure: status → apply_failed, capture error in last_apply_error

Concurrency: relies on the existing one-job-per-device rule in core/jobs.py.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import NamedTuple

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.nso.apply import NsoApplyError
from nso_adapter.store.models import (
    BfdIntent,
    BgpRouterIntent,
    DbInterface,
    Device,
    DeviceSettings,
    InterfaceAttrState,
    InterfaceIntent,
    InterfaceIpIntent,
    InterfaceMtuIntent,
    IsisFlexAlgoIntent,
    IsisInterfaceIntent,
    IsisProcessIntent,
    Job,
    JobStatus,
    JobType,
    L2SapIntent,
    LoggingHostIntent,
    OspfInstanceIntent,
    OspfInterfaceIntent,
    RedistributionIntent,
    RoutePolicyObjectIntent,
    SnmpCommunityIntent,
    SnmpHostIntent,
    SnmpSystemInfoIntent,
    SnmpV3UserIntent,
    StaticRouteIntent,
    SubinterfaceIntent,
    SviIntent,
    SyncState,
    VlanIntent,
)

logger = structlog.get_logger(__name__)

# Statuses eligible for apply (decision Q in plan — force=True pushes all of these)
_FORCE_ELIGIBLE = {
    SyncState.accepted,
    SyncState.apply_failed,
    SyncState.drifted,
    SyncState.in_sync,
}
# force=False only pushes these
_NO_FORCE_ELIGIBLE = {
    SyncState.accepted,
    SyncState.apply_failed,
    SyncState.drifted,
}


def _nokia_routed_kind(iface) -> str | None:
    """Derive the SR OS router context (base|ies|vprn) for a Nokia routed interface.

    The adapter's ``DbInterface.kind`` is the interface *type* (physical/logical/loopback/
    lag); the router context comes from ``service``/``vrf``:
      * VPRN — ``service`` set and ``vrf`` == ``service`` (VPRN addrs carry vrf=service-name)
      * IES  — ``service`` set, global table (vrf empty)
      * Base — no service
    Returns None for non-routed interfaces (physical ports, LAGs) and for non-Nokia
    devices (where ``kind`` is unset) so the IP lands via the normal port/interface path.
    """
    if iface.kind not in ("logical", "loopback"):
        return None
    if iface.service:
        return "vprn" if (iface.vrf and iface.vrf == iface.service) else "ies"
    return "base"


async def enqueue_apply(db: AsyncSession, device_id: int, force: bool = True) -> Job | None:
    """Create an apply job if no active job exists.  Returns Job or None if blocked."""
    from nso_adapter.core.jobs import get_active_job

    active = await get_active_job(device_id, db)
    if active:
        return None

    job = Job(job_type=JobType.apply, device_id=device_id, status=JobStatus.queued)
    db.add(job)
    await db.flush()
    return job


async def _diff_interface_attributes(db, nso_apply, client, device_name: str, ifaces: dict) -> str:
    """Accumulated description/enabled native delta across every accepted interface attr.

    One isolated dry-run per accepted (description|enabled) slice — a failing slice is
    logged and skipped so it never blocks the others.
    """
    attr_delta = ""
    for iface in ifaces.values():
        rows = (
            (await db.execute(select(InterfaceIntent).where(InterfaceIntent.interface_id == iface.id))).scalars().all()
        )
        for r in rows:
            if r.accepted_at is None or r.attribute not in ("description", "enabled"):
                continue
            try:
                delta = await nso_apply.apply_interface_attribute(
                    client=client,
                    device_name=device_name,
                    interface_name=iface.name,
                    attribute=r.attribute,
                    value=r.intent_value,
                    dry_run=True,
                )
            except Exception as exc:  # noqa: BLE001 — preview must never fail hard
                logger.warning(
                    "apply_diff.scope_failed",
                    scope="interface_attribute",
                    device=device_name,
                    interface=iface.name,
                    error=repr(exc),
                )
                continue
            if delta and delta.strip():
                attr_delta += delta
    return attr_delta


async def _diff_interface_ips(db, nso_apply, client, device_name: str, ifaces: dict) -> str:
    """Accumulated native IP delta — one isolated dry-run per interface carrying IP intent."""
    ip_rows = (
        (await db.execute(select(InterfaceIpIntent).where(InterfaceIpIntent.interface_id.in_(list(ifaces) or [-1]))))
        .scalars()
        .all()
    )
    by_iface: dict[int, list] = {}
    for r in ip_rows:
        by_iface.setdefault(r.interface_id, []).append(r)
    ip_delta = ""
    for iface_id, rows in by_iface.items():
        iface = ifaces[iface_id]
        rk = _nokia_routed_kind(iface)
        try:
            delta = await nso_apply.apply_interface_ips(
                client=client,
                device_name=device_name,
                interface_name=iface.name,
                ip_intent_rows=rows,
                kind=rk,
                service=iface.service if rk in ("ies", "vprn") else None,
                parent_binding=iface.parent_binding,
                encap_tag=iface.encap_tag,
                dry_run=True,
            )
        except Exception as exc:  # noqa: BLE001 — preview must never fail hard
            logger.warning(
                "apply_diff.scope_failed",
                scope="interface_ip",
                device=device_name,
                interface=iface.name,
                error=repr(exc),
            )
            continue
        if delta and delta.strip():
            ip_delta += delta
    return ip_delta


async def collect_apply_diff(db: AsyncSession, device_id: int) -> dict[str, str]:
    """Read-only preview: the per-scope native device diff the next Apply would push.

    For each scope, the accepted owned intent is dry-run against NSO (``?dry-run=native``)
    — NSO computes the device-native config it *would* push without committing anything.
    Returns ``{scope: native_delta}`` for scopes with a non-empty change (a scope already
    in sync yields an empty delta and is omitted). Never writes to NSO or the DB.

    Covers every scope ``run_apply`` pushes through the intent store: interface
    attributes/IPs, OSPF, IS-IS, BGP, route-policy, SNMP, static routes, logging, SVI,
    subinterfaces, VLANs, BFD, and L2 SAPs (LAG/switchport are pushed out-of-band by the
    plugin, not from this intent store, so they have no preview here). Each scope is a
    best-effort, isolated dry-run — one failing/slow scope never blocks the others.
    """
    from nso_adapter.core.importer import get_nso_client
    from nso_adapter.nso import apply as nso_apply

    device = await db.get(Device, device_id)
    if not device:
        return {}
    client = get_nso_client(device.nso_instance)
    device_name = device.nso_device_name
    diffs: dict[str, str] = {}

    async def _accepted(model) -> list:
        """All rows of *model* for this device that have been accepted (Apply-eligible)."""
        rows = (await db.execute(select(model).where(model.device_id == device_id))).scalars().all()
        return [r for r in rows if getattr(r, "accepted_at", None) is not None]

    async def _record(scope: str, coro) -> None:
        """Run one scope's dry-run; store a non-empty delta. Never raise."""
        try:
            delta = await coro
        except Exception as exc:  # noqa: BLE001 — preview must never fail hard
            logger.warning("apply_diff.scope_failed", scope=scope, device=device_name, error=repr(exc))
            return
        if delta and delta.strip():
            diffs[scope] = delta

    ifaces = {
        i.id: i
        for i in (await db.execute(select(DbInterface).where(DbInterface.device_id == device_id))).scalars().all()
    }

    # ── Interface attributes + IPs (each accumulates across several dry-runs) ─────
    attr_delta = await _diff_interface_attributes(db, nso_apply, client, device_name, ifaces)
    if attr_delta.strip():
        diffs["interface_attribute"] = attr_delta
    ip_delta = await _diff_interface_ips(db, nso_apply, client, device_name, ifaces)
    if ip_delta.strip():
        diffs["interface_ip"] = ip_delta

    # ── Redistribution rows split by destination protocol (shared by ospf/isis/bgp) ──
    redist = await _accepted(RedistributionIntent)
    redist_ospf = [r for r in redist if r.dest_protocol == "ospf"]
    redist_isis = [r for r in redist if r.dest_protocol == "isis"]
    redist_bgp = [r for r in redist if r.dest_protocol == "bgp"]

    # Collect every remaining scope's accepted rows (BGP relationships eager-loaded,
    # like run_apply, so the dry-run sees scopes/peers/afs).
    ospf_inst = await _accepted(OspfInstanceIntent)
    ospf_iface = await _accepted(OspfInterfaceIntent)
    isis_iface = await _accepted(IsisInterfaceIntent)
    isis_proc = await _accepted(IsisProcessIntent)
    isis_flex = await _accepted(IsisFlexAlgoIntent)
    bgp = await _accepted(BgpRouterIntent)
    if bgp:
        from nso_adapter.core.bgp_load import attach_bgp_relationships

        await attach_bgp_relationships(db, bgp)
    rp = await _accepted(RoutePolicyObjectIntent)
    snmp_comm = await _accepted(SnmpCommunityIntent)
    snmp_user = await _accepted(SnmpV3UserIntent)
    snmp_host = await _accepted(SnmpHostIntent)
    snmp_sysinfo_rows = await _accepted(SnmpSystemInfoIntent)
    snmp_sysinfo = snmp_sysinfo_rows[0] if snmp_sysinfo_rows else None
    sr = await _accepted(StaticRouteIntent)
    lg = await _accepted(LoggingHostIntent)
    svi = await _accepted(SviIntent)
    subif = await _accepted(SubinterfaceIntent)
    vlan = await _accepted(VlanIntent)
    bfd = await _accepted(BfdIntent)
    mtu = await _accepted(InterfaceMtuIntent)
    l2 = await _accepted(L2SapIntent)

    # (scope, trigger row-lists, lazy dry-run coroutine). A scope is previewed only when
    # at least one trigger list is non-empty; the coroutine is built lazily so skipped
    # scopes never construct an un-awaited dry-run.
    scopes = [
        (
            "ospf",
            [ospf_inst, ospf_iface, redist_ospf],
            lambda: nso_apply.apply_ospf_config(
                client=client,
                device_name=device_name,
                process_intent_rows=ospf_inst,
                interface_intent_rows=ospf_iface,
                redistribution_rows=redist_ospf,
                dry_run=True,
            ),
        ),
        (
            "isis",
            [isis_iface, isis_proc, redist_isis, isis_flex],
            lambda: nso_apply.apply_isis_interfaces(
                client=client,
                device_name=device_name,
                isis_intent_rows=isis_iface,
                isis_process_rows=isis_proc,
                redistribution_rows=redist_isis,
                flex_algo_rows=isis_flex,
                dry_run=True,
            ),
        ),
        (
            "bgp",
            [bgp, redist_bgp],
            lambda: nso_apply.apply_bgp_config(
                client=client,
                device_name=device_name,
                router_intent_rows=bgp,
                redistribution_rows=redist_bgp,
                dry_run=True,
            ),
        ),
        (
            "route_policy",
            [rp],
            lambda: nso_apply.apply_route_policy_config(
                client=client,
                device_name=device_name,
                intent_rows=rp,
                ned_id=device.ned_id,
                dry_run=True,
            ),
        ),
        (
            "snmp",
            [snmp_comm, snmp_user, snmp_host, [snmp_sysinfo] if snmp_sysinfo else []],
            lambda: nso_apply.apply_snmp_config(
                client=client,
                device_name=device_name,
                community_intents=snmp_comm,
                v3_user_intents=snmp_user,
                host_intents=snmp_host,
                system_info_intent=snmp_sysinfo,
                dry_run=True,
            ),
        ),
        (
            "static_route",
            [sr],
            lambda: nso_apply.apply_static_routes(
                client=client, device_name=device_name, route_intent_rows=sr, dry_run=True
            ),
        ),
        (
            "logging",
            [lg],
            lambda: nso_apply.apply_logging_config(
                client=client, device_name=device_name, host_intent_rows=lg, dry_run=True
            ),
        ),
        (
            "svi",
            [svi],
            lambda: nso_apply.apply_svi_config(
                client=client, device_name=device_name, svi_intent_rows=svi, dry_run=True
            ),
        ),
        (
            "subinterface",
            [subif],
            lambda: nso_apply.apply_subinterface_config(
                client=client, device_name=device_name, subif_intent_rows=subif, dry_run=True
            ),
        ),
        (
            "vlan",
            [vlan],
            lambda: nso_apply.apply_vlan_config(
                client=client, device_name=device_name, vlan_intent_rows=vlan, dry_run=True
            ),
        ),
        (
            "bfd",
            [bfd],
            lambda: nso_apply.apply_bfd_config(
                client=client, device_name=device_name, bfd_intent_rows=bfd, dry_run=True
            ),
        ),
        (
            "interface_mtu",
            [mtu],
            lambda: nso_apply.apply_mtu_config(
                client=client, device_name=device_name, mtu_intent_rows=mtu, dry_run=True
            ),
        ),
        (
            "l2_sap",
            [l2],
            lambda: nso_apply.apply_l2_saps(client=client, device_name=device_name, sap_intent_rows=l2, dry_run=True),
        ),
    ]
    for scope, triggers, make_coro in scopes:
        if any(triggers):
            await _record(scope, make_coro())

    return diffs


# ── run_apply: shared eligibility + per-scope batch-commit helpers ────────────
#
# An "apply pass" pushes one scope's accepted intent to NSO and stamps the
# outcome back onto the rows. Every scope shares the same eligibility filter and
# the same success/failure bookkeeping, so that logic lives in the helpers below
# and ``run_apply`` just wires the scopes together. Each scope commits as one
# unit: on success every row gets ``last_apply_at`` and a cleared error; on any
# failure every row records the error payload and the scope reports one item.


# Result-dict keys in the order run_apply has always emitted them (also the order
# failed items are reported in). Interface attributes + IPs come first and are
# handled out-of-band (per-item, not per-batch); these are the batch scopes.
_SCOPE_RESULT_ORDER = (
    "snmp",
    "static_route",
    "logging",
    "svi",
    "subinterface",
    "vlan",
    "bfd",
    "interface_mtu",
    "l2_sap",
    "isis",
    "bgp",
    "route_policy",
    "ospf",
)


class _Scope(NamedTuple):
    """One per-scope apply pass: which rows to stamp and how to push them."""

    key: str  # result-dict key + failed-item "type"
    log_label: str  # structlog event infix ("apply.<label>_failed")
    rows: list  # every row stamped on success/failure; empty ⇒ scope skipped
    make_coro: Callable[[], Awaitable]  # built lazily, only when rows is non-empty
    on_nso_error: Callable[[NsoApplyError], Awaitable] | None = None


def _is_eligible(row, force: bool) -> bool:
    """Report whether an accepted intent row is Apply-eligible.

    ``force=False`` additionally skips rows already applied cleanly (a non-null
    ``last_apply_at`` with no ``last_apply_error``) — only pending/failed rows go.
    """
    if row.accepted_at is None:
        return False
    if not force and row.last_apply_at is not None and row.last_apply_error is None:
        return False
    return True


async def _collect_eligible(db: AsyncSession, model, device_id: int, force: bool) -> list:
    """All Apply-eligible rows of *model* for this device (one device-scoped query)."""
    rows = (await db.execute(select(model).where(model.device_id == device_id))).scalars().all()
    return [r for r in rows if _is_eligible(r, force)]


async def _maybe_sync_from(db: AsyncSession, client, device_name: str, device_id: int) -> None:
    """Best-effort pre-apply sync-from (per-device gated by DeviceSettings).

    A timed-out or partial prior commit leaves NSO's CDB inconsistent with the device;
    the next apply is then refused ("device out of sync"). Re-reading the device first
    clears it. A failure here must not abort the apply — the per-scope verify still
    catches real problems. ``sync_before_apply=False`` skips it for NEDs that already
    sync on connect.
    """
    settings_row = (
        await db.execute(select(DeviceSettings).where(DeviceSettings.device_id == device_id))
    ).scalar_one_or_none()
    if settings_row is not None and not settings_row.sync_before_apply:
        return
    try:
        await client.sync_from(device_name)
        logger.info("apply.sync_from.done", device=device_name)
    except Exception as exc:
        logger.warning("apply.sync_from.failed", device=device_name, error=str(exc))


async def _collect_attr_eligibility(db: AsyncSession, ifaces: dict, force: bool) -> tuple[list, list]:
    """Snapshot every interface-attribute intent row; return (snapshot, eligible 3-tuples).

    Eligibility for attributes is keyed off the attr_state's sync_state (not last_apply_at):
    every accepted row is snapshotted, but only those whose state is in the force/no-force
    set are returned as ``(attr_state, intent_row, iface)`` for the per-attribute pass.
    """
    eligible_statuses = _FORCE_ELIGIBLE if force else _NO_FORCE_ELIGIBLE
    snapshot: list[dict] = []
    eligible: list[tuple] = []
    for iface in ifaces.values():
        intent_rows = (
            (await db.execute(select(InterfaceIntent).where(InterfaceIntent.interface_id == iface.id))).scalars().all()
        )
        for intent_row in intent_rows:
            attr_state = (
                await db.execute(
                    select(InterfaceAttrState).where(
                        InterfaceAttrState.interface_id == iface.id,
                        InterfaceAttrState.attribute == intent_row.attribute,
                    )
                )
            ).scalar_one_or_none()
            snapshot.append(
                {
                    "interface": iface.name,
                    "attribute": intent_row.attribute,
                    "intent_value": intent_row.intent_value,
                    "accepted_at": intent_row.accepted_at.isoformat() if intent_row.accepted_at else None,
                    "status_at_snapshot": attr_state.sync_state.value if attr_state else "unknown",
                }
            )
            if attr_state and attr_state.sync_state in eligible_statuses:
                eligible.append((attr_state, intent_row, iface))
    return snapshot, eligible


async def _collect_ip_eligibility(db: AsyncSession, ifaces: dict, force: bool) -> tuple[list, dict]:
    """Snapshot every IP intent row; return (snapshot, {iface_id: [eligible rows]})."""
    snapshot: list[dict] = []
    by_iface: dict[int, list] = {}
    for iface in ifaces.values():
        ip_rows = (
            (await db.execute(select(InterfaceIpIntent).where(InterfaceIpIntent.interface_id == iface.id)))
            .scalars()
            .all()
        )
        for row in ip_rows:
            snapshot.append(
                {
                    "interface": iface.name,
                    "address": row.address,
                    "family": row.family,
                    "secondary": row.secondary,
                    "vrf": row.vrf,
                    "accepted_at": row.accepted_at.isoformat() if row.accepted_at else None,
                }
            )
            if _is_eligible(row, force):
                by_iface.setdefault(iface.id, []).append(row)
    return snapshot, by_iface


async def _apply_attributes(eligible, apply_fn, *, client, device_name, job_id, now) -> tuple[int, int, list]:
    """Commit each (interface, attribute) individually and transition its attr_state.

    Unlike the batch scopes, a per-attribute failure isolates to that one attribute.
    Returns (in_sync, apply_failed, failures).
    """
    ok = 0
    failed = 0
    failures: list[dict] = []
    for attr_state, intent_row, iface in eligible:
        try:
            await apply_fn(
                client=client,
                device_name=device_name,
                interface_name=iface.name,
                attribute=intent_row.attribute,
                value=intent_row.intent_value,
            )
        except NsoApplyError as exc:
            logger.error(
                "apply.attribute_failed",
                job_id=job_id,
                device=device_name,
                interface=iface.name,
                attribute=intent_row.attribute,
                error=exc.message,
            )
            attr_state.sync_state = SyncState.apply_failed
            intent_row.last_apply_error = {"code": exc.code, "message": exc.message, "detail": exc.detail}
            failed += 1
            failures.append({"interface": iface.name, "attribute": intent_row.attribute, "error": exc.message})
        except Exception as exc:
            logger.exception(
                "apply.attribute_unexpected_error",
                job_id=job_id,
                interface=iface.name,
                attribute=intent_row.attribute,
            )
            attr_state.sync_state = SyncState.apply_failed
            intent_row.last_apply_error = {"code": "internal", "message": repr(exc), "detail": {}}
            failed += 1
            failures.append({"interface": iface.name, "attribute": intent_row.attribute, "error": repr(exc)})
        else:
            attr_state.sync_state = SyncState.in_sync
            intent_row.last_apply_at = now
            intent_row.last_apply_error = None
            ok += 1
    return ok, failed, failures


async def _apply_ips(by_iface, ifaces, apply_fn, *, client, device_name, job_id, now) -> tuple[int, int, list]:
    """Push IP intent one interface at a time (each interface is one commit unit).

    Returns (in_sync, apply_failed, failures); the counts are per-row, the failures
    per-interface.
    """
    ok = 0
    failed = 0
    failures: list[dict] = []
    for iface_id, ip_rows in by_iface.items():
        iface = ifaces[iface_id]
        routed_kind = _nokia_routed_kind(iface)
        try:
            await apply_fn(
                client=client,
                device_name=device_name,
                interface_name=iface.name,
                ip_intent_rows=ip_rows,
                kind=routed_kind,
                service=iface.service if routed_kind in ("ies", "vprn") else None,
                parent_binding=iface.parent_binding,
                encap_tag=iface.encap_tag,
            )
        except NsoApplyError as exc:
            logger.error("apply.ip_failed", job_id=job_id, device=device_name, interface=iface.name, error=exc.message)
            for row in ip_rows:
                row.last_apply_error = {"code": exc.code, "message": exc.message, "detail": exc.detail}
            failed += len(ip_rows)
            failures.append({"interface": iface.name, "error": exc.message})
        except Exception as exc:
            logger.exception("apply.ip_unexpected_error", job_id=job_id, interface=iface.name)
            for row in ip_rows:
                row.last_apply_error = {"code": "internal", "message": repr(exc), "detail": {}}
            failed += len(ip_rows)
            failures.append({"interface": iface.name, "error": repr(exc)})
        else:
            for row in ip_rows:
                row.last_apply_at = now
                row.last_apply_error = None
            ok += len(ip_rows)
    return ok, failed, failures


async def _apply_subif_ip_atomic(
    subif_rows, ip_by_iface, ifaces, *, client, device_name, job_id, now
) -> tuple[tuple[int, int, list], tuple[int, int, list]]:
    """Stage subinterface + interface-IP intent into ONE NSO transaction (atomic apply).

    The flagged (``NSO_ADAPTER_ATOMIC_APPLY``) replacement for the separate subif scope +
    per-interface IP commits: one ``apply_combined`` PATCH to ``/restconf/data`` so the
    subif unit and its IP land in a single device transaction (dissolving the greenfield
    ordering dependency — a Junos unit and its ``family inet address`` are created together).

    All-or-nothing: on a successful commit every subif + IP row is stamped ``last_apply_at``
    with the error cleared; on failure every row records the error. Returns
    ``(ip_outcome, subif_outcome)`` each as ``(in_sync, apply_failed, failures)`` so the
    caller feeds them into the existing per-row result model unchanged.
    """
    from nso_adapter.nso.apply import (
        apply_combined,
        build_interface_ip_entry,
        build_subif_interfaces,
    )

    modules: dict[str, list] = {}
    if subif_rows:
        modules["subinterface-reconciler:subif-config"] = [
            {"device": device_name, "interface": build_subif_interfaces(subif_rows)}
        ]
    ip_entries: list[dict] = []
    flat_ip_rows: list = []
    for iface_id, rows in ip_by_iface.items():
        iface = ifaces[iface_id]
        routed_kind = _nokia_routed_kind(iface)
        ip_entries.append(
            build_interface_ip_entry(
                device_name,
                iface.name,
                rows,
                kind=routed_kind,
                service=iface.service if routed_kind in ("ies", "vprn") else None,
                parent_binding=iface.parent_binding,
                encap_tag=iface.encap_tag,
            )
        )
        flat_ip_rows.extend(rows)
    if ip_entries:
        modules["interface-reconciler:interface-config"] = ip_entries

    all_rows = [*subif_rows, *flat_ip_rows]
    try:
        await apply_combined(client=client, device_name=device_name, modules=modules)
    except NsoApplyError as exc:
        logger.error("apply.atomic_subif_ip_failed", job_id=job_id, device=device_name, error=exc.message)
        err = {"code": exc.code, "message": exc.message, "detail": exc.detail}
        for row in all_rows:
            row.last_apply_error = err
        fail = [{"error": exc.message}]
        return (0, len(flat_ip_rows), fail if flat_ip_rows else []), (0, len(subif_rows), fail if subif_rows else [])
    except Exception as exc:
        logger.exception("apply.atomic_subif_ip_unexpected_error", job_id=job_id)
        err = {"code": "internal", "message": repr(exc), "detail": {}}
        for row in all_rows:
            row.last_apply_error = err
        fail = [{"error": repr(exc)}]
        return (0, len(flat_ip_rows), fail if flat_ip_rows else []), (0, len(subif_rows), fail if subif_rows else [])
    for row in all_rows:
        row.last_apply_at = now
        row.last_apply_error = None
    return (len(flat_ip_rows), 0, []), (len(subif_rows), 0, [])


async def _run_scope(log_label, coro, rows, *, job_id, device_name, now, on_nso_error=None) -> tuple[int, int, list]:
    """Push one scope's batch coroutine and stamp the outcome onto every row in *rows*.

    Returns (in_sync, apply_failed, failures). Success stamps last_apply_at and clears
    the error on every row; an NsoApplyError or any other exception records the error
    payload on every row and reports a single failure. ``on_nso_error`` is a best-effort
    side-effect (route-policy uses it to record a device-parser capability rejection).
    """
    try:
        await coro
    except NsoApplyError as exc:
        logger.error(f"apply.{log_label}_failed", job_id=job_id, device=device_name, error=exc.message)
        err = {"code": exc.code, "message": exc.message, "detail": exc.detail}
        for row in rows:
            row.last_apply_error = err
        if on_nso_error is not None:
            await on_nso_error(exc)
        return 0, len(rows), [{"error": exc.message}]
    except Exception as exc:
        logger.exception(f"apply.{log_label}_unexpected_error", job_id=job_id)
        err = {"code": "internal", "message": repr(exc), "detail": {}}
        for row in rows:
            row.last_apply_error = err
        return 0, len(rows), [{"error": repr(exc)}]
    for row in rows:
        row.last_apply_at = now
        row.last_apply_error = None
    return len(rows), 0, []


async def _finalize_job(
    db: AsyncSession,
    job: Job,
    job_id: int,
    device_id: int,
    any_eligible: bool,
    attr_outcome: tuple[int, int, list],
    ip_outcome: tuple[int, int, list],
    scope_outcomes: dict,
    scope_failures: dict,
) -> None:
    """Assemble job.result/status from the pass outcomes and commit.

    With nothing eligible the job succeeds with an all-zero result and returns early.
    Otherwise the per-scope counts are emitted; any failure flips the job to failed and
    collects the per-item errors.
    """
    if not any_eligible:
        logger.info("apply.nothing_eligible", job_id=job_id, device_id=device_id)
        job.status = JobStatus.succeeded
        job.result = {
            "attribute_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
            "ip_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
            "snmp_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
            "static_route_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
            "subinterface_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
            "vlan_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
            "bfd_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
            "interface_mtu_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
            "l2_sap_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
            "isis_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
            "bgp_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
            "route_policy_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
            "ospf_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
        }
        await db.commit()
        return

    attr_ok, attr_failed, attr_failures = attr_outcome
    ip_ok, ip_failed, ip_failures = ip_outcome

    result = {
        "attribute_count_by_outcome": {"in_sync": attr_ok, "apply_failed": attr_failed},
        "ip_count_by_outcome": {"in_sync": ip_ok, "apply_failed": ip_failed},
    }
    for key in _SCOPE_RESULT_ORDER:
        scope_ok, scope_failed = scope_outcomes[key]
        result[f"{key}_count_by_outcome"] = {"in_sync": scope_ok, "apply_failed": scope_failed}
    job.result = result

    total_failed = attr_failed + ip_failed + sum(failed for _ok, failed in scope_outcomes.values())
    if total_failed == 0:
        job.status = JobStatus.succeeded
    else:
        job.status = JobStatus.failed
        all_failed = [{"type": "attribute", **a} for a in attr_failures] + [{"type": "ip", **a} for a in ip_failures]
        for key in _SCOPE_RESULT_ORDER:
            all_failed.extend({"type": key, **a} for a in scope_failures.get(key, []))
        job.error = {
            "code": "nso_commit_failed",
            "message": f"{total_failed} item(s) failed to apply",
            "detail": {"items": all_failed},
        }
    await db.commit()
    logger.info(
        "apply.done",
        job_id=job_id,
        in_sync=attr_ok,
        apply_failed=attr_failed,
        ip_in_sync=ip_ok,
        ip_failed=ip_failed,
        snmp_in_sync=scope_outcomes["snmp"][0],
        snmp_failed=scope_outcomes["snmp"][1],
        sr_in_sync=scope_outcomes["static_route"][0],
        sr_failed=scope_outcomes["static_route"][1],
        l2_in_sync=scope_outcomes["l2_sap"][0],
        l2_failed=scope_outcomes["l2_sap"][1],
        isis_in_sync=scope_outcomes["isis"][0],
        isis_failed=scope_outcomes["isis"][1],
        bgp_in_sync=scope_outcomes["bgp"][0],
        bgp_failed=scope_outcomes["bgp"][1],
        rp_in_sync=scope_outcomes["route_policy"][0],
        rp_failed=scope_outcomes["route_policy"][1],
    )


async def _execute_apply(db: AsyncSession, job: Job, job_id: int, device_id: int, force: bool) -> None:
    """Run the apply body: sync-from, snapshot intent, push each scope, finalize the job.

    Raises on a missing device / NSO-client error so ``run_apply``'s outer handler can
    mark the job failed with an ``internal`` error.
    """
    from nso_adapter.core.importer import get_nso_client
    from nso_adapter.nso.apply import (
        apply_bfd_config,
        apply_bgp_config,
        apply_interface_attribute,
        apply_interface_ips,
        apply_isis_interfaces,
        apply_l2_saps,
        apply_logging_config,
        apply_mtu_config,
        apply_ospf_config,
        apply_route_policy_config,
        apply_snmp_config,
        apply_static_routes,
        apply_subinterface_config,
        apply_svi_config,
        apply_vlan_config,
        atomic_apply_enabled,
    )

    device = await db.get(Device, device_id)
    if not device:
        raise ValueError(f"Device {device_id} not found")
    client = get_nso_client(device.nso_instance)
    device_name = device.nso_device_name

    # ── Step 0: sync-from before apply (best-effort) ──
    await _maybe_sync_from(db, client, device_name, device_id)

    # ── Step 1: snapshot intent + collect every scope's eligible rows ──
    ifaces = {
        iface.id: iface
        for iface in (await db.execute(select(DbInterface).where(DbInterface.device_id == device_id))).scalars().all()
    }
    intent_snapshot, attr_eligible = await _collect_attr_eligibility(db, ifaces, force)
    ip_snapshot, ip_eligible_by_iface = await _collect_ip_eligibility(db, ifaces, force)

    snmp_comm = await _collect_eligible(db, SnmpCommunityIntent, device_id, force)
    snmp_user = await _collect_eligible(db, SnmpV3UserIntent, device_id, force)
    snmp_host = await _collect_eligible(db, SnmpHostIntent, device_id, force)
    snmp_sysinfo_rows = await _collect_eligible(db, SnmpSystemInfoIntent, device_id, force)
    snmp_sysinfo = snmp_sysinfo_rows[0] if snmp_sysinfo_rows else None
    snmp_rows = [*snmp_comm, *snmp_user, *snmp_host, *([snmp_sysinfo] if snmp_sysinfo else [])]

    sr_eligible = await _collect_eligible(db, StaticRouteIntent, device_id, force)
    logging_eligible = await _collect_eligible(db, LoggingHostIntent, device_id, force)
    svi_eligible = await _collect_eligible(db, SviIntent, device_id, force)
    subif_eligible = await _collect_eligible(db, SubinterfaceIntent, device_id, force)
    vlan_eligible = await _collect_eligible(db, VlanIntent, device_id, force)
    bfd_eligible = await _collect_eligible(db, BfdIntent, device_id, force)
    mtu_eligible = await _collect_eligible(db, InterfaceMtuIntent, device_id, force)
    l2_eligible = await _collect_eligible(db, L2SapIntent, device_id, force)
    isis_eligible = await _collect_eligible(db, IsisInterfaceIntent, device_id, force)
    isis_process_eligible = await _collect_eligible(db, IsisProcessIntent, device_id, force)
    isis_flex_eligible = await _collect_eligible(db, IsisFlexAlgoIntent, device_id, force)
    bgp_eligible = await _collect_eligible(db, BgpRouterIntent, device_id, force)
    if bgp_eligible:
        # Eagerly load BGP relationships for apply (avoids lazy-raise on the worker greenlet).
        from nso_adapter.core.bgp_load import attach_bgp_relationships

        await attach_bgp_relationships(db, bgp_eligible)
    rp_eligible = await _collect_eligible(db, RoutePolicyObjectIntent, device_id, force)
    ospf_instance_eligible = await _collect_eligible(db, OspfInstanceIntent, device_id, force)
    ospf_iface_eligible = await _collect_eligible(db, OspfInterfaceIntent, device_id, force)
    redist_eligible = await _collect_eligible(db, RedistributionIntent, device_id, force)
    redist_ospf = [r for r in redist_eligible if r.dest_protocol == "ospf"]
    redist_isis = [r for r in redist_eligible if r.dest_protocol == "isis"]
    redist_bgp = [r for r in redist_eligible if r.dest_protocol == "bgp"]

    job.context = {"force": force, "intent_snapshot": intent_snapshot, "ip_snapshot": ip_snapshot}
    now = datetime.now(UTC).replace(tzinfo=None)

    # ── Step 2: mark attribute states deploying ──
    for attr_state, _intent_row, _iface in attr_eligible:
        attr_state.sync_state = SyncState.deploying
    await db.commit()

    # ── Step 3–6: per-item attribute + IP passes ──
    attr_outcome = await _apply_attributes(
        attr_eligible, apply_interface_attribute, client=client, device_name=device_name, job_id=job_id, now=now
    )

    # Atomic apply (I3a): stage the subinterface + interface-IP pair into ONE NSO
    # transaction so a greenfield subif unit and its IP land together. When off, the
    # subif scope is applied below in the per-scope loop and IPs commit per-interface.
    atomic = atomic_apply_enabled() and bool(subif_eligible or ip_eligible_by_iface)
    subif_atomic_outcome: tuple[int, int, list] | None = None
    if atomic:
        ip_outcome, subif_atomic_outcome = await _apply_subif_ip_atomic(
            subif_eligible,
            ip_eligible_by_iface,
            ifaces,
            client=client,
            device_name=device_name,
            job_id=job_id,
            now=now,
        )
    else:
        ip_outcome = await _apply_ips(
            ip_eligible_by_iface,
            ifaces,
            apply_interface_ips,
            client=client,
            device_name=device_name,
            job_id=job_id,
            now=now,
        )

    async def _record_rp_capability(exc: NsoApplyError) -> None:
        # The device parser only rejects an unsupported construct on a real commit
        # (dry-run renders it). Record it so every box on this (ned, sw) is flagged.
        try:
            from nso_adapter.core.capability import (
                parse_rejected_construct,
                record_capability_rejection,
                refresh_device_capability,
            )

            info = await refresh_device_capability(db, client, device_name, device)
            scope, name = parse_rejected_construct(exc.message)
            if info and name:
                await record_capability_rejection(
                    db, info["ned_id"], info["sw_version"], scope, name, exc.message[:256]
                )
        except Exception:
            logger.debug("apply.capability_record_skipped", job_id=job_id)

    # ── Step 6b–6g: one batch commit per remaining scope ──
    scopes = [
        _Scope(
            "snmp",
            "snmp",
            snmp_rows,
            lambda: apply_snmp_config(
                client=client,
                device_name=device_name,
                community_intents=snmp_comm,
                v3_user_intents=snmp_user,
                host_intents=snmp_host,
                system_info_intent=snmp_sysinfo,
            ),
        ),
        _Scope(
            "static_route",
            "static_route",
            sr_eligible,
            lambda: apply_static_routes(client=client, device_name=device_name, route_intent_rows=sr_eligible),
        ),
        _Scope(
            "logging",
            "logging",
            logging_eligible,
            lambda: apply_logging_config(client=client, device_name=device_name, host_intent_rows=logging_eligible),
        ),
        _Scope(
            "svi",
            "svi",
            svi_eligible,
            lambda: apply_svi_config(client=client, device_name=device_name, svi_intent_rows=svi_eligible),
        ),
        _Scope(
            "subinterface",
            "subif",
            subif_eligible,
            lambda: apply_subinterface_config(client=client, device_name=device_name, subif_intent_rows=subif_eligible),
        ),
        _Scope(
            "vlan",
            "vlan",
            vlan_eligible,
            lambda: apply_vlan_config(client=client, device_name=device_name, vlan_intent_rows=vlan_eligible),
        ),
        _Scope(
            "bfd",
            "bfd",
            bfd_eligible,
            lambda: apply_bfd_config(client=client, device_name=device_name, bfd_intent_rows=bfd_eligible),
        ),
        _Scope(
            "interface_mtu",
            "interface_mtu",
            mtu_eligible,
            lambda: apply_mtu_config(client=client, device_name=device_name, mtu_intent_rows=mtu_eligible),
        ),
        _Scope(
            "l2_sap",
            "l2_sap",
            l2_eligible,
            lambda: apply_l2_saps(client=client, device_name=device_name, sap_intent_rows=l2_eligible),
        ),
        _Scope(
            "isis",
            "isis",
            [*isis_eligible, *isis_process_eligible, *redist_isis, *isis_flex_eligible],
            lambda: apply_isis_interfaces(
                client=client,
                device_name=device_name,
                isis_intent_rows=isis_eligible,
                isis_process_rows=isis_process_eligible,
                redistribution_rows=redist_isis,
                flex_algo_rows=isis_flex_eligible,
            ),
        ),
        _Scope(
            "bgp",
            "bgp",
            [*bgp_eligible, *redist_bgp],
            lambda: apply_bgp_config(
                client=client,
                device_name=device_name,
                router_intent_rows=bgp_eligible,
                redistribution_rows=redist_bgp,
            ),
        ),
        _Scope(
            "route_policy",
            "route_policy",
            rp_eligible,
            lambda: apply_route_policy_config(
                client=client, device_name=device_name, intent_rows=rp_eligible, ned_id=device.ned_id
            ),
            _record_rp_capability,
        ),
        _Scope(
            "ospf",
            "ospf",
            [*ospf_instance_eligible, *ospf_iface_eligible, *redist_ospf],
            lambda: apply_ospf_config(
                client=client,
                device_name=device_name,
                process_intent_rows=ospf_instance_eligible,
                interface_intent_rows=ospf_iface_eligible,
                redistribution_rows=redist_ospf,
            ),
        ),
    ]

    scope_outcomes: dict[str, tuple[int, int]] = {}
    scope_failures: dict[str, list] = {}
    for sc in scopes:
        # The subinterface scope is committed atomically with IPs above when the flag is on.
        if atomic and sc.key == "subinterface":
            continue
        if not sc.rows:
            scope_outcomes[sc.key] = (0, 0)
            continue
        scope_ok, scope_failed, fails = await _run_scope(
            sc.log_label,
            sc.make_coro(),
            sc.rows,
            job_id=job_id,
            device_name=device_name,
            now=now,
            on_nso_error=sc.on_nso_error,
        )
        scope_outcomes[sc.key] = (scope_ok, scope_failed)
        if fails:
            scope_failures[sc.key] = fails

    if subif_atomic_outcome is not None:
        s_ok, s_failed, s_fails = subif_atomic_outcome
        scope_outcomes["subinterface"] = (s_ok, s_failed)
        if s_fails:
            scope_failures["subinterface"] = s_fails

    # ── Step 7: finalize ──
    # "Nothing eligible" mirrors the historical flag set exactly (note: it keys off the
    # IS-IS *interface* list and the OSPF instance/interface lists, not the process/flex
    # rows), so the outcome is byte-identical to the pre-refactor worker.
    any_eligible = any(
        [
            attr_eligible,
            ip_eligible_by_iface,
            snmp_rows,
            sr_eligible,
            logging_eligible,
            svi_eligible,
            subif_eligible,
            vlan_eligible,
            bfd_eligible,
            mtu_eligible,
            l2_eligible,
            isis_eligible,
            bgp_eligible,
            rp_eligible,
            ospf_instance_eligible,
            ospf_iface_eligible,
            redist_eligible,
        ]
    )
    await _finalize_job(
        db, job, job_id, device_id, any_eligible, attr_outcome, ip_outcome, scope_outcomes, scope_failures
    )


async def run_apply(job_id: int, device_id: int, force: bool = True) -> None:
    """Background task: execute the apply for *device_id* (see module docstring §7a)."""
    from nso_adapter.store.db import get_session

    async for db in get_session():
        job = await db.get(Job, job_id)
        if not job:
            logger.error("apply.job_not_found", job_id=job_id)
            return
        job.status = JobStatus.running
        await db.commit()

        try:
            await _execute_apply(db, job, job_id, device_id, force)
        except Exception as exc:
            logger.exception("apply.unexpected_error", job_id=job_id, device_id=device_id)
            job.status = JobStatus.failed
            job.error = {"code": "internal", "message": repr(exc), "detail": {}}
            await db.commit()
