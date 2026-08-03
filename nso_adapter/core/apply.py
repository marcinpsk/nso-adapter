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

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import NamedTuple

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.core.claim import ClaimLostError
from nso_adapter.core.static_route_plan import authorized_clear_fields, build_plan
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
    IsisLevelIntent,
    IsisProcessIntent,
    Job,
    JobStatus,
    JobType,
    L2SapIntent,
    LoggingHostIntent,
    LoggingLevelsIntent,
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


def _nokia_attr_kind(iface) -> str | None:
    """SR OS context for a Nokia interface's description/admin-state write.

    Extends :func:`_nokia_routed_kind` (base|ies|vprn for an L3 routed interface) with ``lag``:
    a Nokia LAG's description/admin-state live under ``configure lag <lag-N>``, not a port and
    not a router interface. Physical ports (and non-Nokia interfaces) return ``None`` → the
    legacy ``configure port`` path. Distinct from ``_nokia_routed_kind`` because a LAG never
    carries an IP, so the IP path must keep returning ``None`` for it.
    """
    if iface.kind == "lag":
        return "lag"
    return _nokia_routed_kind(iface)


async def enqueue_apply(db: AsyncSession, device_id: int, force: bool = True) -> Job | None:
    """Create an apply job if no active job exists.  Returns Job or None if blocked.

    Also returns ``None`` on a store-only request (the plugin's intent re-sync,
    tracker #103): reconciling the intent store must never trigger a device commit,
    so the auto-apply enqueue is suppressed alongside the shrink-removal one.
    """
    from nso_adapter.core.jobs import admit_queued_job
    from nso_adapter.core.request_flags import STORE_ONLY

    if STORE_ONLY.get():
        logger.info("apply.skipped_store_only", device_id=device_id)
        return None

    # Atomic same-type QUEUED dedupe, inside a savepoint. Two properties matter to the
    # fifteen callers, all of which reach here with intent rows already mutated and
    # uncommitted: a conflict must not poison their transaction, and on a conflict the
    # queued winner is row-locked until they commit, so the worker cannot start it against a
    # snapshot older than the request that admitted it.
    #
    # A removal is enqueued BEFORE its apply by design, so rejecting on any active job
    # dropped the apply outright; and a running apply must not refuse its successor, because
    # the successor is what carries the newer intent.
    created, _winner = await admit_queued_job(db, device_id, JobType.apply)
    if created is None:
        return None
    await db.flush()
    return created


async def _diff_interface_attributes(db, nso_apply, client, device_name: str, ifaces: dict, fmt=True) -> str:
    """Accumulated description/enabled native delta across every accepted interface attr.

    One isolated dry-run per accepted (description|enabled) slice — a failing slice is
    logged and skipped so it never blocks the others.
    """
    attr_delta = ""
    for iface in ifaces.values():
        rows = (
            (await db.execute(select(InterfaceIntent).where(InterfaceIntent.interface_id == iface.id))).scalars().all()
        )
        rk = _nokia_attr_kind(iface)
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
                    kind=rk,
                    service=iface.service if rk in ("ies", "vprn") else None,
                    parent_binding=iface.parent_binding,
                    encap_tag=iface.encap_tag,
                    dry_run=fmt,
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


async def _diff_interface_ips(db, nso_apply, client, device_name: str, ifaces: dict, fmt=True) -> str:
    """Accumulated native IP delta — one isolated dry-run per interface carrying IP intent."""
    ip_rows = (
        (await db.execute(select(InterfaceIpIntent).where(InterfaceIpIntent.interface_id.in_(list(ifaces) or [-1]))))
        .scalars()
        .all()
    )
    by_iface: dict[int, list] = {}
    for r in ip_rows:
        if r.accepted_at is None:
            continue  # gate on accepted_at, like the attribute preview and real apply eligibility
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
                dry_run=fmt,
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


# ── #1396 R2 §4.1/§4.2/§4.8 — the guarded static-route PUT-replace ───────────
#
# One snapshot feeds BOTH the retained-entry computation and the collateral guard, and one
# body builder feeds both the real apply and the preview — that shared derivation is what
# makes the previewed payload byte-identical to the applied one (C2.6).

#: The scope fails with this code when the pre-PUT service read cannot be certified. A
#: destructive replace must never be built on a read that may be hiding both the tombstoned
#: entries it has to preserve and the collateral the guard has to see (§4.4).
SNAPSHOT_INCONCLUSIVE = "static_route_snapshot_inconclusive"


async def _static_route_snapshot(client, device, plan) -> tuple[dict | None, list[dict]]:
    """Read the live static-route service ONCE and derive the entries the PUT must retain.

    Returns ``(snapshot, retained)``. ``snapshot`` is the live instance body, or ``None``
    when the service is certifiably absent — nothing to retain and no orphan possible, so
    the PUT proceeds. Raises :class:`NsoApplyError` on an inconclusive read.

    *retained* are the live entries a tombstone still claims (by its own triple or by its
    ``deployed_key``) and that no body-rendered row re-asserts, kept **verbatim**: metric,
    tag and NED-specific leaves live only in the live copy, so reconstructing such an entry
    from the store triple would silently rewrite it.
    """
    from nso_adapter.core.static_route_plan import as_triple, triple_of
    from nso_adapter.nso.apply import _STATIC_ROUTE_SERVICE_PATH, static_route_entry_key

    state = await client.service_instance_state(_STATIC_ROUTE_SERVICE_PATH, device.nso_device_name)
    if state.inconclusive:
        raise NsoApplyError(
            SNAPSHOT_INCONCLUSIVE,
            f"static_route: could not certify the live service instance on {device.nso_device_name!r} "
            "— refusing to build a PUT-replace from an uncertified read",
            detail={"device": device.nso_device_name},
        )
    current = state.entry
    if not current:
        return current, []

    claimed: set[tuple[str, str, str]] = set()
    for tomb in plan.tombstones:
        claimed.add((tomb.vrf or "", tomb.prefix or "", tomb.next_hop or ""))
        deployed = as_triple(tomb.deployed_key)
        if deployed is not None:
            claimed.add(deployed)
    reasserted = {triple_of(row) for row in plan.rows}
    keep = claimed - reasserted  # a key a live row still renders needs no retention
    retained = [entry for entry in (current.get("route") or []) if static_route_entry_key(entry) in keep]
    return current, retained


async def _put_static_routes(client, device, plan, *, dry_run=False):
    """Send the guarded PUT-replace of the whole static-route service instance (§4.1).

    The guard sees the same snapshot the retained entries came from, and ``plan.allowed``
    names the keys it may watch disappear — the replacement predecessors this apply is
    delivering, plus (X4 belt) the tombstone keys the retention already re-asserts.
    """
    from nso_adapter.core.removal import _guarded_apply
    from nso_adapter.nso.apply import apply_static_routes

    current, retained = await _static_route_snapshot(client, device, plan)

    async def _apply(**kwargs):
        return await apply_static_routes(
            client=client,
            device_name=device.nso_device_name,
            route_intent_rows=plan.rows,
            extra_entries=retained,
            **kwargs,
        )

    if dry_run:
        # Preview parity (§4.8): the same body, rendered as a native PUT dry-run. No guard
        # (the delta NSO returns already shows what would be retracted), no writes, nothing
        # consumed — _send_service_config routes replace+dry_run through native_dry_run
        # with method="put".
        return await _apply(replace=True, dry_run=dry_run)
    context = {"removed": {"route": [list(key) for key in sorted(plan.allowed)]}}
    return await _guarded_apply(client, device, "static_route", context, _apply, current=current)


async def _enqueue_pending_clear_retract(db: AsyncSession, device, plan, *, reg=None) -> None:
    """Queue the networked retract a merge-PATCH apply structurally cannot deliver (§4.11).

    A cleared leaf only leaves the device on a networked PUT. In ``PUT`` mode this apply's
    own store-rendered body already omits it, so nothing is owed. In ``PATCH`` mode the
    renderer omits the leaf while the merge leaves it live — and reader-compare only checks
    the route KEY, so the row would otherwise be certified in sync over a stale value.

    Only the ``authorized`` half is deliverable (A1). A ``store_only`` clear was recorded by
    a request that may mutate the intent store but must never cause a device write; the
    apply's separate authorization covers the apply's OWN body, not a deletion job whose
    only purpose is to remove a leaf observed under ``?store_only=true``. Such an entry
    parks — the row stays unproven — until a later authorized push re-records the clear.

    Runs AFTER the apply's terminal transaction, so a plain failure here is logged, never
    raised: a second terminal write flipping a committed ``succeeded`` to ``failed`` would
    misreport rows the device really did accept. Nothing is lost either way — the carrier is
    store state, so the next apply re-derives exactly this decision.

    A LOST CLAIM is not that kind of failure and does propagate. The job this queues is a
    networked PUT; if the claim was revoked and reacquired while this apply was running, the
    successor may since have un-owned a route, and a stale retract queued behind its back
    would retract that deliberately detached config from the device. So the insert takes the
    claim lock first and holds it to COMMIT. An UNREGISTERED registration is the documented
    claimless lane and ``lock_claim`` no-ops on it — this transaction consumes no carrier and
    deletes no tombstone, so a claimless caller is not the programming error it would be there.
    """
    if plan.mode != "PATCH":
        return
    fields = sorted({f for row in plan.rows for f in authorized_clear_fields(row.pending_clear)})
    if not fields:
        return
    from nso_adapter.core.claim import ClaimRegistration, lock_claim
    from nso_adapter.core.removal import enqueue_removal

    try:
        await lock_claim(db, reg if reg is not None else ClaimRegistration())
        # retract=True, no removed/shrank: an un-own would make this a no-networking detach,
        # which can never deliver a clear. Ordinary admission, ordinary FIFO — no immediate
        # device write, so auto_apply pacing is untouched.
        job = await enqueue_removal(db, device_id=device.id, scope="static_route", retract=True)
        await db.commit()
    except ClaimLostError:
        await db.rollback()
        raise
    except Exception as exc:  # noqa: BLE001 — the apply is already finalized
        await db.rollback()
        logger.warning("static_route.pending_clear_retract_enqueue_failed", device_id=device.id, error=repr(exc))
        return
    logger.info(
        "static_route.pending_clear_retract_enqueued",
        device_id=device.id,
        job_id=getattr(job, "id", None),
        fields=fields,
    )


def _static_route_coro(client, device, plan, *, dry_run=False):
    """Build the static-route scope's coroutine for this plan — PUT-replace or today's merge."""
    from nso_adapter.nso.apply import apply_static_routes

    if plan.mode == "PUT":
        return _put_static_routes(client, device, plan, dry_run=dry_run)
    return apply_static_routes(
        client=client, device_name=device.nso_device_name, route_intent_rows=plan.rows, dry_run=dry_run
    )


async def collect_apply_diff(db: AsyncSession, device_id: int, outformat: str = "native") -> dict[str, str]:
    """Read-only preview: the per-scope native device diff the next Apply would push.

    For each scope, the accepted owned intent is dry-run against NSO — with
    ``outformat="native"`` (default) NSO renders the device-native config it *would*
    push; ``outformat="cli"`` renders the NED-uniform ``+``/``-`` tree diff instead
    (the apply-preview "diff -u" panel). Nothing is committed either way.
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
    # dry_run is bool|str down the apply stack: True = native, "cli" = tree diff.
    fmt = "cli" if outformat == "cli" else True
    diffs: dict[str, str] = {}

    async def _accepted(model) -> list:
        """All rows of *model* for this device that have been accepted (Apply-eligible)."""
        rows = (await db.execute(select(model).where(model.device_id == device_id))).scalars().all()
        return [r for r in rows if getattr(r, "accepted_at", None) is not None]

    async def _record(scope: str, coro) -> None:
        """Run one scope's dry-run; store a non-empty delta. Never raise.

        A scope that blows up is reported IN the preview rather than silently omitted: the
        operator approves the apply from this panel, and an empty entry reads as "nothing to
        do". A body-builder error (a vault_ref the writer cannot render, an unmappable enum)
        is exactly what will fail the real apply, so it must be visible here first.
        """
        try:
            delta = await coro
        except Exception as exc:  # noqa: BLE001 — preview must never fail hard
            logger.warning("apply_diff.scope_failed", scope=scope, device=device_name, error=repr(exc))
            reason = getattr(exc, "message", None) or repr(exc)
            diffs[scope] = f"!! preview unavailable for this scope: {reason}"
            return
        if delta and delta.strip():
            diffs[scope] = delta

    ifaces = {
        i.id: i
        for i in (await db.execute(select(DbInterface).where(DbInterface.device_id == device_id))).scalars().all()
    }

    # ── Interface attributes + IPs (each accumulates across several dry-runs) ─────
    attr_delta = await _diff_interface_attributes(db, nso_apply, client, device_name, ifaces, fmt)
    if attr_delta.strip():
        diffs["interface_attribute"] = attr_delta
    ip_delta = await _diff_interface_ips(db, nso_apply, client, device_name, ifaces, fmt)
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
    isis_levels = await _accepted(IsisLevelIntent)
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
    # The preview has always previewed ACCEPTED rows (the real apply pushes the eligible
    # subset) — passing them as the eligible list keeps PATCH-mode previews byte-identical
    # to today's, while PUT mode derives its rows from the store regardless.
    sr_plan = await build_plan(db, device, eligible_rows=sr)
    lg = await _accepted(LoggingHostIntent)
    lgl_rows = await _accepted(LoggingLevelsIntent)
    lgl = lgl_rows[0] if lgl_rows else None
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
                dry_run=fmt,
            ),
        ),
        (
            "isis",
            [isis_iface, isis_proc, redist_isis, isis_flex, isis_levels],
            lambda: nso_apply.apply_isis_interfaces(
                client=client,
                device_name=device_name,
                isis_intent_rows=isis_iface,
                isis_process_rows=isis_proc,
                redistribution_rows=redist_isis,
                flex_algo_rows=isis_flex,
                level_rows=isis_levels,
                dry_run=fmt,
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
                dry_run=fmt,
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
                dry_run=fmt,
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
                dry_run=fmt,
            ),
        ),
        (
            "static_route",
            [sr],
            # Preview parity (§4.8): the same classifier, the same rows, the same retained
            # tombstone entries — so a replacement-open device previews the very PUT the
            # apply would send. Read-only: build_plan writes nothing and consumes nothing.
            lambda: _static_route_coro(client, device, sr_plan, dry_run=fmt),
        ),
        (
            "logging",
            [lg, [lgl] if lgl else []],
            lambda: nso_apply.apply_logging_config(
                client=client, device_name=device_name, host_intent_rows=lg, levels_intent_row=lgl, dry_run=fmt
            ),
        ),
        (
            "svi",
            [svi],
            lambda: nso_apply.apply_svi_config(
                client=client, device_name=device_name, svi_intent_rows=svi, dry_run=fmt
            ),
        ),
        (
            "subinterface",
            [subif],
            lambda: nso_apply.apply_subinterface_config(
                client=client, device_name=device_name, subif_intent_rows=subif, dry_run=fmt
            ),
        ),
        (
            "vlan",
            [vlan],
            lambda: nso_apply.apply_vlan_config(
                client=client, device_name=device_name, vlan_intent_rows=vlan, dry_run=fmt
            ),
        ),
        (
            "bfd",
            [bfd],
            lambda: nso_apply.apply_bfd_config(
                client=client, device_name=device_name, bfd_intent_rows=bfd, dry_run=fmt
            ),
        ),
        (
            "interface_mtu",
            [mtu],
            lambda: nso_apply.apply_mtu_config(
                client=client, device_name=device_name, mtu_intent_rows=mtu, dry_run=fmt
            ),
        ),
        (
            "l2_sap",
            [l2],
            lambda: nso_apply.apply_l2_saps(client=client, device_name=device_name, sap_intent_rows=l2, dry_run=fmt),
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
        routed_kind = _nokia_attr_kind(iface)
        try:
            await apply_fn(
                client=client,
                device_name=device_name,
                interface_name=iface.name,
                attribute=intent_row.attribute,
                value=intent_row.intent_value,
                kind=routed_kind,
                service=iface.service if routed_kind in ("ies", "vprn") else None,
                parent_binding=iface.parent_binding,
                encap_tag=iface.encap_tag,
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
        except ClaimLostError:
            # Revocation is not a per-row failure: continuing the loop would push
            # further scopes under ownership this run has lost.
            raise
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
        except ClaimLostError:
            # Revocation is not a per-row failure: continuing the loop would push
            # further scopes under ownership this run has lost.
            raise
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


_IFACE_CONFIG_ROOT = "interface-reconciler:interface-config"


def _build_interface_config_entries(attr_eligible, ip_by_iface, ifaces, device_name: str) -> list[dict]:
    """Merge interface description/enabled + IP intent into one entry per interface.

    Both ride the same ``(device, interface-name)``-keyed interface-reconciler instance, so in
    a single atomic edit they MUST be one list item — two items with a duplicate key conflict.
    """
    from nso_adapter.nso.apply import _coerce_enabled_intent, build_interface_ip_entry

    by_name: dict[str, dict] = {}

    def _entry(name: str) -> dict:
        return by_name.setdefault(name, {"device": device_name, "interface-name": name})

    for _attr_state, intent_row, iface in attr_eligible:
        entry = _entry(iface.name)
        if intent_row.attribute == "description":
            entry["description"] = intent_row.intent_value if intent_row.intent_value is not None else ""
        elif intent_row.attribute == "enabled":
            # Shared strict coercion (raises on garbage) — same as the per-scope path, so a
            # corrupt value never silently disables the interface in the atomic body either.
            entry["enabled"] = _coerce_enabled_intent(intent_row.intent_value)

    for iface_id, rows in ip_by_iface.items():
        iface = ifaces[iface_id]
        routed_kind = _nokia_routed_kind(iface)
        ip_entry = build_interface_ip_entry(
            device_name,
            iface.name,
            rows,
            kind=routed_kind,
            service=iface.service if routed_kind in ("ies", "vprn") else None,
            parent_binding=iface.parent_binding,
            encap_tag=iface.encap_tag,
        )
        entry = _entry(iface.name)
        for key, value in ip_entry.items():
            if key not in ("device", "interface-name"):
                entry[key] = value

    return list(by_name.values())


_RP_ROOT = "route-policy-reconciler:route-policy-config"


def _device_error_message(exc) -> str | None:
    """Extract the device-parser error text from a failed atomic commit (``exc.detail['nso_error']``).

    Returns ``None`` when the failure was transport/internal (no device rejection) — so a
    transient failure (timeout, unreachable) never records a FALSE capability verdict. Only a
    real device rejection (the NED/device refused the commit) carries an ``nso_error`` payload.
    """
    nso_error = (getattr(exc, "detail", None) or {}).get("nso_error")
    if not isinstance(nso_error, dict):
        return None
    errors = (nso_error.get("ietf-restconf:errors") or {}).get("error") or []
    for e in errors:
        if isinstance(e, dict) and e.get("error-message"):
            return str(e["error-message"])
    return None


async def _localize_atomic_failure(client, device_name, modules, device_err) -> tuple[dict[str, str], tuple]:
    """Localise a failed atomic commit → ({offender root-key: its rejection message}, rp).

    ``rp`` is the route-policy ``(scope, name)`` construct parse. Two complementary signals: (1) a per-scope dry-run — a module the NED cannot compile re-runs
    to an inconclusive (``None``) delta in isolation; (2) the route-policy device-parser rejection,
    which renders clean in dry-run but is named in the *device* error (``device_err``). Each
    offender carries ITS OWN dry-run rejection message (H2): the combined-commit ``device_err``
    may describe a different module's failure, so per-construct attribution must read the
    message of the module that actually rejected. No recording here — the caller decides
    attribution (including the fall-back to all staged scopes) and whether it is a real device
    rejection before recording capability.
    """
    from nso_adapter.core.capability import parse_rejected_construct
    from nso_adapter.nso.apply import NsoApplyError, apply_combined

    offenders: dict[str, str] = {}
    for root_key, bodies in modules.items():
        try:
            # strict=True: only a CONCLUSIVE 4xx rejection in isolation flags this scope as an
            # offender. A transient/transport error (or 5xx) returns None / raises a non-NsoApplyError
            # — inconclusive, NOT an offender, so it never brands the scope a false 'unsupported'
            # that a later probe can't downgrade.
            await apply_combined(client, device_name, {root_key: bodies}, dry_run=True, strict=True)
        except NsoApplyError as exc:
            offenders[root_key] = str(exc.message or "")
        except Exception:  # noqa: BLE001 — transient/transport during localisation → inconclusive
            logger.debug("apply.localize.inconclusive", device=device_name, root_key=root_key)

    rp = parse_rejected_construct(device_err or "")
    if rp[1] and _RP_ROOT in modules:
        offenders.setdefault(_RP_ROOT, device_err or "")
    return offenders, rp


# Maps a staged module root-key → its result/scope key. interface-config is handled
# separately (it carries the attribute + IP scopes, which the result model splits out).
_ATOMIC_SCOPE_ROOTS: dict[str, str] = {
    "subinterface-reconciler:subif-config": "subinterface",
    "snmp-reconciler:snmp-config": "snmp",
    "static-route-reconciler:static-route-config": "static_route",
    "logging-reconciler:logging-config": "logging",
    "svi-reconciler:svi-config": "svi",
    "vlan-reconciler:vlan-config": "vlan",
    "bfd-reconciler:bfd-config": "bfd",
    "mtu-reconciler:mtu-config": "interface_mtu",
    "l2-sap-reconciler:l2-sap-config": "l2_sap",
    "isis-reconciler:isis-config": "isis",
    "bgp-reconciler:bgp-config": "bgp",
    "route-policy-reconciler:route-policy-config": "route_policy",
    "ospf-reconciler:ospf-config": "ospf",
}


def _capability_scopes_for(root_key: str) -> list[str]:
    """Capability-matrix scope name(s) for a staged module root-key ([] if not tracked).

    The merged interface-config module carries BOTH the interface_attribute and interface_ip
    scopes (collect_apply_diff / preflight treat them separately), so a rejection of it must
    record capability under both — else a preflight for interface_attribute sees a false
    'fully supported'.
    """
    if root_key == _IFACE_CONFIG_ROOT:
        return ["interface_attribute", "interface_ip"]
    scope = _ATOMIC_SCOPE_ROOTS.get(root_key)
    return [scope] if scope else []


async def _record_atomic_capability(db, client, device, device_name, offenders, exc, rp, device_err) -> None:
    """Record a capability rejection for the attributed offender scopes.

    A per-scope dry-run rejection means the NED cannot compile that scope's intent on this
    ``(ned, sw)``; a device-parser rejection (``device_err``) means the device itself refused
    the commit — both are real, reactive capability gaps, recorded at scope granularity (or
    fine-grained for route-policy when ``rp = (scope, name)`` parses). H2: the merged
    interface-config module carries TWO scopes — when its OWN rejection message names a
    construct, only the offending half is recorded (construct-named); an unattributable
    message falls back to the coarse both-scopes record. Detail prefers each offender's own
    localisation message over the combined ``device_err`` (which may describe a different
    module). The ``(ned, sw)`` key is read from the device row, learned + persisted via the
    capability probe only when not already known.
    """
    from nso_adapter.core.capability import (
        _clean_capability_key,
        parse_rejected_iface_construct,
        record_capability_rejection,
        refresh_device_capability,
    )

    # Guard both keys against the literal 'None' (see _clean_capability_key) so a device row
    # carrying a stringified-None ned_id never becomes a bogus capability key.
    ned_id = _clean_capability_key(device.ned_id)
    sw = _clean_capability_key(device.sw_version)
    if not ned_id:
        info = await refresh_device_capability(db, client, device_name, device)
        if info:
            ned_id, sw = info.get("ned_id", ""), info.get("sw_version", "")
    if not ned_id:
        return

    rp_scope, rp_name = rp
    for root_key in offenders:
        own_msg = offenders.get(root_key, "") if isinstance(offenders, dict) else ""
        detail = (own_msg or device_err or exc.message or "")[:256]
        if root_key == _RP_ROOT and rp_name:
            await record_capability_rejection(db, ned_id, sw, rp_scope, rp_name, detail)
            continue
        if root_key == _IFACE_CONFIG_ROOT:
            scope, name = parse_rejected_iface_construct(own_msg or device_err or exc.message or "")
            if scope:
                await record_capability_rejection(db, ned_id, sw, scope, name, detail)
                continue
        for scope in _capability_scopes_for(root_key):
            await record_capability_rejection(db, ned_id, sw, scope, scope, detail)


async def _clear_atomic_capability(db, device, modules) -> None:
    """Clear stale reactive capability rejections after a clean atomic commit.

    A successful commit proves every staged scope applies on this ``(ned, sw)`` — the strongest
    positive signal — so drop any coarse ``apply``-sourced ``unsupported`` recorded by an earlier
    failed apply. Without this the gap would stick forever (a probe cannot downgrade an
    apply-rejection). Best-effort; the ``(ned, sw)`` key is read from the device row (no probe).
    """
    from nso_adapter.core.capability import _clean_capability_key, clear_capability_rejections

    ned_id = _clean_capability_key(device.ned_id)
    if not ned_id:
        return
    sw = _clean_capability_key(device.sw_version)
    scopes: set[str] = set()
    for root_key in modules:
        scopes.update(_capability_scopes_for(root_key))
    await clear_capability_rejections(db, ned_id, sw, scopes)


async def _stage_atomic_modules(elig, client, device, device_name) -> tuple[dict, list, dict, dict]:
    """Build the combined ``/restconf/data`` body across every scope.

    Returns ``(modules, iface_entries, scope_rows, stage_errors)``. Each scope stages its
    body via ``stage=modules`` (reusing its own body-builder, no HTTP); the interface-config
    module merges attribute + IP intent per interface.

    A scope whose body cannot be BUILT (a malformed vault_ref, an unmappable enum) is
    isolated into *stage_errors* rather than raising: the fault is deterministic and local
    to that scope, so the rest of the apply still commits.
    """
    from nso_adapter.nso.apply import (
        apply_bfd_config,
        apply_bgp_config,
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
    )

    modules: dict[str, list] = {}
    iface_entries = _build_interface_config_entries(elig["attr"], elig["ip_by_iface"], elig["ifaces"], device_name)
    if iface_entries:
        modules[_IFACE_CONFIG_ROOT] = iface_entries

    stagers: list[tuple[str, list, object]] = [
        (
            "subinterface",
            elig["subif"],
            lambda: apply_subinterface_config(client, device_name, elig["subif"], stage=modules),
        ),
        (
            "snmp",
            elig["snmp_rows"],
            lambda: apply_snmp_config(
                client,
                device_name,
                elig["snmp_comm"],
                elig["snmp_user"],
                elig["snmp_host"],
                elig["snmp_sysinfo"],
                stage=modules,
            ),
        ),
        (
            "static_route",
            elig["static_route"],
            lambda: apply_static_routes(client, device_name, elig["static_route"], stage=modules),
        ),
        (
            "logging",
            [*elig["logging"], *([elig["logging_levels"]] if elig["logging_levels"] else [])],
            lambda: apply_logging_config(
                client, device_name, elig["logging"], levels_intent_row=elig["logging_levels"], stage=modules
            ),
        ),
        ("svi", elig["svi"], lambda: apply_svi_config(client, device_name, elig["svi"], stage=modules)),
        ("vlan", elig["vlan"], lambda: apply_vlan_config(client, device_name, elig["vlan"], stage=modules)),
        ("bfd", elig["bfd"], lambda: apply_bfd_config(client, device_name, elig["bfd"], stage=modules)),
        ("interface_mtu", elig["mtu"], lambda: apply_mtu_config(client, device_name, elig["mtu"], stage=modules)),
        ("l2_sap", elig["l2_sap"], lambda: apply_l2_saps(client, device_name, elig["l2_sap"], stage=modules)),
        (
            "isis",
            [*elig["isis_iface"], *elig["isis_proc"], *elig["redist_isis"], *elig["isis_flex"], *elig["isis_levels"]],
            lambda: apply_isis_interfaces(
                client,
                device_name,
                elig["isis_iface"],
                elig["isis_proc"],
                elig["redist_isis"],
                elig["isis_flex"],
                elig["isis_levels"],
                stage=modules,
            ),
        ),
        (
            "bgp",
            [*elig["bgp"], *elig["redist_bgp"]],
            lambda: apply_bgp_config(client, device_name, elig["bgp"], elig["redist_bgp"], stage=modules),
        ),
        (
            "route_policy",
            elig["rp"],
            lambda: apply_route_policy_config(client, device_name, elig["rp"], ned_id=device.ned_id, stage=modules),
        ),
        (
            "ospf",
            [*elig["ospf_inst"], *elig["ospf_iface"], *elig["redist_ospf"]],
            lambda: apply_ospf_config(
                client, device_name, elig["ospf_inst"], elig["ospf_iface"], elig["redist_ospf"], stage=modules
            ),
        ),
    ]
    scope_rows = {key: rows for key, rows, _fn in stagers}
    stage_errors: dict[str, NsoApplyError] = {}
    for key, rows, stage_fn in stagers:
        if not rows:
            continue
        try:
            await stage_fn()
        except NsoApplyError as exc:
            # This scope's BODY could not be built — a vault_ref that predates the
            # mount/path#key contract, an enum spelling the writer cannot map. Deterministic
            # and local to the scope: nothing was staged into `modules` (each builder
            # assembles its entry locally and only stages it at the very end), so drop the
            # offender and let the healthy scopes commit. Letting the raise escape failed the
            # ENTIRE job — interfaces, IPs, BGP, IS-IS — for one bad SNMP row.
            logger.error("apply.atomic_stage_failed", device=device_name, scope=key, error=exc.message)
            stage_errors[key] = exc
    return modules, iface_entries, scope_rows, stage_errors


def _stamp_attr_atomic(attr_eligible, commit_error, iface_failed, err, msg, now, snapshot) -> tuple[int, int, list]:
    """Stamp interface-attribute rows from the single atomic outcome.

    Pending (rolled-back, non-offender) attrs revert from ``deploying`` to their pre-apply
    snapshot state.
    """
    ok = failed = 0
    failures: list[dict] = []
    for attr_state, intent_row, iface in attr_eligible:
        if commit_error is None:
            attr_state.sync_state = SyncState.in_sync
            intent_row.last_apply_at = now
            intent_row.last_apply_error = None
            ok += 1
        elif iface_failed:
            attr_state.sync_state = SyncState.apply_failed
            intent_row.last_apply_error = err
            failed += 1
            failures.append({"interface": iface.name, "attribute": intent_row.attribute, "error": msg})
        else:
            attr_state.sync_state = snapshot[attr_state]
    return ok, failed, failures


def _stamp_ip_atomic(ip_rows_flat, commit_error, iface_failed, err, msg, now) -> tuple[int, int, list]:
    """Stamp IP rows from the single atomic outcome (pending rows untouched, retried next apply)."""
    if commit_error is None:
        for row in ip_rows_flat:
            row.last_apply_at = now
            row.last_apply_error = None
        return len(ip_rows_flat), 0, []
    if iface_failed:
        for row in ip_rows_flat:
            row.last_apply_error = err
        return 0, len(ip_rows_flat), ([{"error": msg}] if ip_rows_flat else [])
    return 0, 0, []


async def _atomic_reader_compare(
    client, device, scope_rows, scope_outcomes, scope_failures, *, job_id, device_name
) -> tuple[dict[str, str], dict[str, list[str]]]:
    """#108 presence check per staged scope after a clean atomic commit.

    Every scope committed in ONE transaction → ONE post-commit point → ONE batched
    device-state action for all checkable wire_names (r1-m3: no per-scope enlargement on
    the atomic path). Prepares each eligible scope (expected + Vault translation), fetches
    the sections whose translated set is non-empty in a single action, then classifies each
    scope independently. A batched-action raise → every checkable scope records ``error``
    (non-fatal). Mutates scope_outcomes/scope_failures for scopes with silently-dropped keys
    and returns ``(reader_compare, reader_compare_unverifiable)`` for job.result.
    """
    from nso_adapter.core.removal import _VERIFY_BATCH_TIMEOUT, _live_family_sections

    ned_id = getattr(device, "ned_id", None)
    preps: dict[str, object] = {}  # scope → prep tuple | None (uncheckable) | "error" (translate raised)
    for key, rows in scope_rows.items():
        s_ok, s_failed = scope_outcomes.get(key, (0, 0))
        if not rows or s_failed:
            continue
        try:
            preps[key] = await _reader_compare_prepare(key, rows, ned_id)
        except Exception as exc:  # noqa: BLE001 — a scope's translation must never fail the apply
            logger.warning("apply.reader_compare_error", job_id=job_id, device=device_name, scope=key, error=repr(exc))
            preps[key] = "error"

    wires = sorted({prep[3] for prep in preps.values() if isinstance(prep, tuple) and prep[0]})
    sections: dict[str, dict] = {}
    action_error: Exception | None = None
    if wires:
        try:
            sections = await _live_family_sections(client, device.nso_device_name, wires, timeout=_VERIFY_BATCH_TIMEOUT)
        except Exception as exc:  # noqa: BLE001 — a batched read failure fails no scope's apply
            action_error = exc
            logger.warning("apply.reader_compare_error", job_id=job_id, device=device_name, error=repr(exc))

    reader_compare: dict[str, str] = {}
    reader_compare_unverifiable: dict[str, list[str]] = {}
    for key, prep in preps.items():
        s_ok, _s_failed = scope_outcomes.get(key, (0, 0))
        if prep is None:
            continue  # structurally uncheckable → no entry
        if prep == "error":
            reader_compare[key] = "error"
            continue
        translated, unverifiable, spec, wire = prep
        if not translated:  # every key Vault-unverifiable → nothing to look for
            n_ok, n_failed, fails, status = s_ok, 0, [], "unknown"
        elif action_error is not None:
            n_ok, n_failed, fails, status = s_ok, 0, [], "error"
        else:
            try:
                # guarded (codex P2): a malformed 'ok' section must not let the walker's exception
                # escape and fail the whole atomic job — classify "error" for just this scope.
                n_ok, n_failed, fails, status = _classify_fetched_section(
                    key, translated, unverifiable, spec, sections[wire], s_ok, job_id=job_id, device_name=device_name
                )
            except Exception as exc:  # noqa: BLE001 — a read-side glitch never fails a good commit
                logger.warning(
                    "apply.reader_compare_error", job_id=job_id, device=device_name, scope=key, error=repr(exc)
                )
                n_ok, n_failed, fails, status = s_ok, 0, [], "error"
        reader_compare[key] = status
        if unverifiable:
            reader_compare_unverifiable[key] = unverifiable
        if n_failed:
            scope_outcomes[key] = (n_ok, n_failed)
            scope_failures.setdefault(key, []).extend(fails)
    return reader_compare, reader_compare_unverifiable


def _stamp_batch_scopes_atomic(scope_rows, offenders, commit_error, err, msg, now) -> tuple[dict, dict]:
    """Stamp every batch scope from the single atomic outcome → (scope_outcomes, scope_failures).

    Offending scopes fail; non-offending scopes are pending (rows untouched, retried next apply).
    """
    scope_outcomes: dict[str, tuple[int, int]] = {key: (0, 0) for key in _SCOPE_RESULT_ORDER}
    scope_failures: dict[str, list] = {}
    for root_key, scope_key in _ATOMIC_SCOPE_ROOTS.items():
        rows = scope_rows.get(scope_key) or []
        if not rows:
            continue
        if commit_error is None:
            for row in rows:
                row.last_apply_at = now
                row.last_apply_error = None
            scope_outcomes[scope_key] = (len(rows), 0)
        elif root_key in offenders:
            for row in rows:
                row.last_apply_error = err
            scope_outcomes[scope_key] = (0, len(rows))
            scope_failures[scope_key] = [{"error": msg}]
    return scope_outcomes, scope_failures


async def _run_atomic_apply(db, device, client, device_name, job, job_id, now, elig) -> None:
    """I3b atomic apply: stage every scope into one transaction and commit once.

    On success, stamp every row in_sync; on failure the whole transaction rolled back —
    localise the offending scope(s), fail those rows (+ record capability), and leave
    non-offending scopes pending (untouched → retried next apply).
    """
    from nso_adapter.nso.apply import apply_combined

    attr_eligible = elig["attr"]
    ip_rows_flat = [r for rows in elig["ip_by_iface"].values() for r in rows]

    # Snapshot attr states, then mark deploying (parity with the per-scope path); a pending
    # (rolled-back, non-offender) attr is reverted to its snapshot rather than left deploying.
    snapshot = {attr_state: attr_state.sync_state for attr_state, _ir, _if in attr_eligible}
    for attr_state, _ir, _if in attr_eligible:
        attr_state.sync_state = SyncState.deploying
    await db.commit()

    try:
        modules, iface_entries, scope_rows, stage_errors = await _stage_atomic_modules(
            elig, client, device, device_name
        )
    except Exception:
        # An UNEXPECTED error while building the combined body (before any commit) — a real
        # bug, not a scope's own bad intent, which _stage_atomic_modules isolates. Revert the
        # attrs we just marked 'deploying' so they aren't stuck forever, then re-raise so
        # run_apply fails the job with the real error.
        for attr_state, _ir, _if in attr_eligible:
            attr_state.sync_state = snapshot[attr_state]
        await db.commit()
        raise

    # Scopes whose body could not be built never entered the transaction; the rest still
    # commit. Keep them out of every stage that assumes a scope was pushed.
    staged_rows = {k: v for k, v in scope_rows.items() if k not in stage_errors}

    commit_error: NsoApplyError | None = None
    try:
        await apply_combined(client, device_name, modules)
    except NsoApplyError as exc:
        commit_error = exc
    except Exception as exc:  # noqa: BLE001 — surface as a job-level failure
        commit_error = NsoApplyError("internal", repr(exc))

    if commit_error is not None:
        logger.error("apply.atomic_failed", job_id=job_id, device=device_name, error=commit_error.message)
        device_err = _device_error_message(commit_error)
        offenders, rp = await _localize_atomic_failure(client, device_name, modules, device_err)
        # Capability (I2): record ONLY reliably-localised offenders — a per-scope dry-run the NED
        # cannot compile, or a parse_rejected_construct match (a known-unsupported construct named
        # in the device error). A generic device rejection is NOT a capability signal: it may be a
        # MISCONFIGURATION (e.g. a route-map referencing a prefix-list not included in the push),
        # not a NED limit — recording it would be a false "unsupported" verdict. Such failures
        # still fail the job + stamp last_apply_error (the operator sees the real device error).
        if offenders:
            try:
                await _record_atomic_capability(
                    db, client, device, device_name, offenders, commit_error, rp, device_err
                )
            except Exception:  # noqa: BLE001 — capability recording is best-effort
                logger.debug("apply.atomic.capability_record_skipped", job_id=job_id)
        if not offenders:  # could not localise → the whole rolled-back commit is the failure
            offenders = dict.fromkeys(modules.keys(), "")
        err = {"code": commit_error.code, "message": commit_error.message, "detail": commit_error.detail}
        msg = commit_error.message
    else:
        offenders, err, msg = {}, None, ""
        # Positive signal (I2): a clean commit clears any stale reactive 'unsupported' for the
        # applied scopes — a probe cannot downgrade an apply-rejection, so without this the gap
        # would stick forever even after the device is fixed / upgraded and the intent lands.
        try:
            await _clear_atomic_capability(db, device, modules)
        except Exception:  # noqa: BLE001 — capability bookkeeping is best-effort
            logger.debug("apply.atomic.capability_clear_skipped", job_id=job_id)

    iface_failed = (_IFACE_CONFIG_ROOT in offenders) if iface_entries else False
    attr_outcome = _stamp_attr_atomic(attr_eligible, commit_error, iface_failed, err, msg, now, snapshot)
    ip_outcome = _stamp_ip_atomic(ip_rows_flat, commit_error, iface_failed, err, msg, now)
    scope_outcomes, scope_failures = _stamp_batch_scopes_atomic(staged_rows, offenders, commit_error, err, msg, now)

    # A scope whose body could not be built failed on its own terms — it never reached the
    # device, so the commit outcome says nothing about it. Fail exactly its rows.
    for scope_key, stage_exc in stage_errors.items():
        rows = scope_rows.get(scope_key) or []
        stage_err = {"code": stage_exc.code, "message": stage_exc.message, "detail": stage_exc.detail}
        for row in rows:
            row.last_apply_error = stage_err
        scope_outcomes[scope_key] = (0, len(rows))
        scope_failures[scope_key] = [{"error": stage_exc.message}]

    # #108: a clean atomic commit rides the same FASTMAP writers — run the post-apply
    # presence check per staged scope and re-flag any silently-dropped keys. Unstaged
    # scopes are excluded: they were never pushed, so "not on the device" is not a drop.
    reader_compare: dict[str, str] = {}
    reader_compare_unverifiable: dict[str, list[str]] = {}
    if commit_error is None:
        reader_compare, reader_compare_unverifiable = await _atomic_reader_compare(
            client, device, staged_rows, scope_outcomes, scope_failures, job_id=job_id, device_name=device_name
        )

    await _finalize_job(
        db,
        job,
        job_id,
        device.id,
        True,
        attr_outcome,
        ip_outcome,
        scope_outcomes,
        scope_failures,
        reader_compare=reader_compare,
        reader_compare_unverifiable=reader_compare_unverifiable,
    )


# Scope → (store model name, residue YANG-list label, row → key tuple), guard grain.
# Key tuples are the store keys verbatim — the same store↔YANG key equivalence the
# removal path already relies on. bgp and route_policy have bespoke expansion below.
_READER_COMPARE_SPECS: dict[str, list] = {
    "static_route": [("StaticRouteIntent", "route", lambda r: (r.vrf, r.prefix, r.next_hop))],
    "vlan": [("VlanIntent", "vlan", lambda r: (r.vlan_id,))],
    "svi": [("SviIntent", "interface", lambda r: (r.interface_name,))],
    "subinterface": [("SubinterfaceIntent", "interface", lambda r: (r.interface_name,))],
    "bfd": [("BfdIntent", "interface", lambda r: (r.interface_name,))],
    "interface_mtu": [("InterfaceMtuIntent", "interface", lambda r: (r.interface_name,))],
    "logging": [("LoggingHostIntent", "host", lambda r: (r.address,))],
    "l2_sap": [("L2SapIntent", "sap", lambda r: (r.service_name, r.sap_id))],
    "isis": [
        ("IsisInterfaceIntent", "interface-config", lambda r: (r.interface_name, r.af)),
        ("IsisProcessIntent", "process-config", lambda r: (r.process_tag,)),
    ],
    "ospf": [
        ("OspfInstanceIntent", "process-config", lambda r: (r.process_id,)),
        ("OspfInterfaceIntent", "interface-config", lambda r: (r.interface_name,)),
    ],
    "snmp": [
        # SnmpCommunityIntent's intent key is the human-readable label, while the export keys a
        # community by sha256(community-string)[:16] — a digest of a secret the adapter never sees
        # (it pushes a Vault triple; NSO resolves it). Demanding the LABEL be present would stamp
        # reader_compare_missing on every successful SNMP apply, so the row used to be left out of
        # the check entirely — leaving the one scope where a silent drop is a missing CREDENTIAL as
        # the only scope the drop-detector did not cover.
        #
        # CR-A17: the adapter holds the vault_ref, so it can resolve the secret and compute that
        # same digest. The key is emitted as the label here and TRANSLATED in _translate_expected
        # (which drops the row when Vault cannot answer — unverifiable, never "missing").
        ("SnmpCommunityIntent", "community", lambda r: (r.label,)),
        ("SnmpV3UserIntent", "v3-user", lambda r: (r.username,)),
        ("SnmpHostIntent", "host", lambda r: (r.address,)),
    ],
}


def _unrenderable_community_list(row, ned_id: str | None) -> bool:
    """Report whether EVERY member of this community-list is unrepresentable on the NED.

    apply_route_policy_config skips members the NED cannot hold, so such an object is
    emitted as ``{"name": …, "entry": []}`` — an empty community-list has no renderable CLI
    form, never lands on the device, and therefore can never appear in the export. It is a
    deliberate, already-reported codec skip (the PUT hands the plugin `unsupported_members`
    so it can badge them "unsupported on <ned>"), NOT a silent writer drop.

    Deterministic — a pure function of member + dialect, the same verdict the apply path
    acts on — so no device read is needed to decide it.
    """
    from nso_adapter.core.community_dialect import community_dialect_for

    if row.family != "community_list":
        return False
    members = {e.get("community") for e in (row.entries or []) if isinstance(e, dict) and e.get("community")}
    if not members:
        return False
    return len(community_dialect_for(ned_id).unrepresentable_members(sorted(members))) == len(members)


def _reader_compare_expected(scope: str, rows, ned_id: str | None = None) -> list[tuple[object, str, tuple]]:
    """(intent row, YANG-list label, key tuple) for every checkable intended object (#108).

    Rows without a keyed reader presence are skipped: redistribution / flex-algo /
    level rows (nested non-keyed content, guard-grain parity), the snmp system-info
    scalar, and community-lists the NED cannot render at all
    (:func:`_unrenderable_community_list`).
    """
    from nso_adapter.core.removal import _ROUTE_POLICY_FAMILY_LISTS
    from nso_adapter.store import models as m

    if scope == "route_policy":
        return [
            (r, _ROUTE_POLICY_FAMILY_LISTS[r.family], (r.name,))
            for r in rows
            if isinstance(r, m.RoutePolicyObjectIntent)
            and r.family in _ROUTE_POLICY_FAMILY_LISTS
            and not _unrenderable_community_list(r, ned_id)
        ]
    if scope == "bgp":
        out: list[tuple[object, str, tuple]] = []
        for r in rows:
            if not isinstance(r, m.BgpRouterIntent):
                continue
            out.append((r, "router", (r.asn,)))
            for sc in r.scopes:  # eagerly loaded by attach_bgp_relationships
                for p in sc.peers:
                    out.append((r, "peer", (p.peer_address,)))
        return out
    out = []
    for model_name, label, keyfn in _READER_COMPARE_SPECS.get(scope, []):
        model = getattr(m, model_name)
        out.extend((r, label, keyfn(r)) for r in rows if isinstance(r, model))
    return out


async def _translate_expected(scope: str, expected: list[tuple[object, str, tuple]]) -> tuple[list, list[str]]:
    """Re-key the expected rows into the namespace the EXPORT uses (CR-A17).

    Identity for every grain but snmp/community, whose export key is ``sha256(secret)[:16]`` of a
    Vault-held community string. Returns ``(translatable, unverifiable_labels)``: a row whose key
    cannot be translated — no Vault provider, a Vault outage, a ref that no longer resolves — is
    DROPPED from the check rather than stamped ``reader_compare_missing``. Failing open is the only
    safe direction: a Vault blip must not permanently pin an SNMP scope apply_failed for a
    community that is sitting on the device exactly as intended.
    """
    from nso_adapter.core.removal import UNCOMPARABLE_LISTS
    from nso_adapter.core.snmp_verify import community_fingerprints

    if not any((scope, label) in UNCOMPARABLE_LISTS for _row, label, _key in expected):
        return expected, []

    refs = {
        str(row.label): row.vault_ref
        for row, label, _key in expected
        if (scope, label) == ("snmp", "community") and getattr(row, "vault_ref", None)
    }
    digests = await community_fingerprints(refs)

    out, unverifiable = [], []
    for row, label, key in expected:
        if (scope, label) not in UNCOMPARABLE_LISTS:
            out.append((row, label, key))
            continue
        digest = digests.get(str(getattr(row, "label", "")))
        if digest is None:
            unverifiable.append(f"{label} {list(key)}")
            continue
        out.append((row, label, (digest,)))
    return out, sorted(unverifiable)


def _reader_compare_checkable(scope, rows, ned_id) -> bool:
    """Whether *scope* has any keyed grain the post-apply presence check could verify.

    A cheap, Vault-free predicate (used to decide whether a budget-SKIPPED scope records
    ``unknown`` vs stays absent): a scope with no expected keyed grain — only nested
    non-keyed rows (redistribution / flex-algo / level) — is structurally uncheckable and
    keeps NO reader_compare entry, exactly as before (r3-M3).
    """
    from nso_adapter.core.removal import _RESIDUE_WIRE_NAMES, _guard_specs

    return bool(
        _reader_compare_expected(scope, rows, ned_id)
        and scope in _RESIDUE_WIRE_NAMES
        and _guard_specs().get(scope) is not None
    )


async def _reader_compare_prepare(scope, rows, ned_id):
    """Compute the translatable expected set for *scope* → ``(translated, unverifiable, spec, wire)``.

    Returns ``None`` when the scope is structurally uncheckable (no expected keyed grain / no
    envelope wire / no guard spec) — the caller records no reader_compare entry. Otherwise
    ``translated`` is the export-namespace expected set (may be empty when EVERY key is
    Vault-unverifiable → the caller records ``unknown`` and runs NO action, r2-m3), and
    ``unverifiable`` names the keys that could not be re-keyed (persisted symmetrically with
    the residue path's ``residue_unverifiable``). Runs the Vault translation (may block).
    """
    from nso_adapter.core.removal import _RESIDUE_WIRE_NAMES, _guard_specs

    expected = _reader_compare_expected(scope, rows, ned_id)
    spec = _guard_specs().get(scope)
    wire = _RESIDUE_WIRE_NAMES.get(scope)
    if not expected or spec is None or wire is None:
        return None
    translated, unverifiable = await _translate_expected(scope, expected)
    if unverifiable:
        # Named, never folded into "ok" silently: these keys were not checked at all.
        logger.warning("apply.reader_compare_unverifiable", scope=scope, keys=unverifiable)
    return translated, unverifiable, spec, wire


def _reader_compare_walk(scope, translated, unverifiable, section, spec, ok, *, job_id, device_name):
    """Walk an ``ok`` device-state *section* for the presence of every translated key.

    Present-all → ``ok``, unless some grain was ``unverifiable`` (never checked) → ``partial``
    (the r3-M2 fix for the mixed community+host false-green — ``partial`` beats ``ok`` but
    ``missing`` still beats ``partial``). A missing key stamps its rows ``reader_compare_missing``
    and fails the scope. Returns ``(ok, failed, fails, status)``.
    """
    from nso_adapter.core.removal import _norm_key, _reader_keys

    present = {gl.label: _reader_keys(scope, section, gl) for gl in spec.lists}
    row_by_id: dict[int, object] = {}
    missing: dict[int, list[str]] = {}
    for row, label, key in translated:
        if _norm_key(key) in present.get(label, set()):
            continue
        row_by_id[id(row)] = row
        missing.setdefault(id(row), []).append(f"{label} {list(key)}")
    if not missing:
        return ok, 0, [], ("partial" if unverifiable else "ok")
    fails = []
    for rid, keys in missing.items():
        msg = (
            f"post-apply device view is missing {', '.join(keys)} — the commit reported "
            f"success but the key(s) never landed (silent writer drop, #26 class)"
        )
        row_by_id[rid].last_apply_error = {
            "code": "reader_compare_missing",
            "message": msg,
            "detail": {"scope": scope},
        }
        fails.append({"error": msg})
    logger.error("apply.reader_compare_missing", job_id=job_id, device=device_name, scope=scope, missing=len(missing))
    return ok - len(missing), len(missing), fails, "missing"


def _classify_fetched_section(scope, translated, unverifiable, spec, section, ok, *, job_id, device_name):
    """Classify a CERTIFIED device-state *section* (status ok|unsupported|error) → (ok, failed, fails, status).

    ``error`` (the family read errored) → ``error``; ``unsupported`` (no export surface — absence
    proves nothing) → ``unknown``; ``ok`` → walk. Shared by the default per-scope path and the
    batched atomic path; the section is already status-terminal thanks to client certification.
    """
    from nso_adapter.core.removal import _verifier_section_status

    status = _verifier_section_status(section)
    if status == "error":
        logger.warning(
            "apply.reader_compare_error", job_id=job_id, device=device_name, scope=scope, error="section status=error"
        )
        return ok, 0, [], "error"
    if status == "unknown":  # the NED does not export this family
        logger.info("apply.reader_compare_unknown", job_id=job_id, device=device_name, scope=scope)
        return ok, 0, [], "unknown"
    return _reader_compare_walk(
        scope, translated, unverifiable, section, spec, ok, job_id=job_id, device_name=device_name
    )


async def _reader_compare_scope(client, device, scope, rows, *, ok, job_id, device_name, timeout):
    """Post-apply presence check (#108, the #26 silent-drop class) → (ok, failed, fails, status, unverifiable).

    ``_verify_native_or_raise`` re-diffs the committed payload against the CDB SERVICE
    tree — both sides sit behind the same FASTMAP writer, so a writer that silently
    drops an object is invisible (proven live on rg03, #26). This check reads the far
    side of the writer instead: the scope's device-state ``ACTION`` section — a fresh
    post-commit CDB extraction inside a whole-build txid bracket, read as soon as
    possible after the commit (the record-served facade is stale post-commit; the legacy
    subscriber-cache-backed getters can lag it). Every intended key must be present; a
    missing key stamps its rows ``reader_compare_missing`` (retryable — last_apply_error
    keeps them eligible) and fails the scope, so the plugin settles deploying→apply_failed
    on the immediate post-apply reconcile instead of waiting out stuck_deploying_grace_minutes.
    Status: "ok" / "partial" (checked keys present, some grain unverifiable) / "missing" /
    "unknown" (the NED has no export surface, or every key is Vault-unverifiable — absence
    proves nothing) / "error" (never fails a good apply on read trouble) / None (nothing
    checkable — e.g. only nested non-keyed rows in the batch). ``unverifiable`` names the keys
    that could not be re-keyed (recorded whenever non-empty, symmetric with residue).
    The ONE-family action runs inside the caller's wall-clock budget (*timeout*); an all-
    unverifiable scope returns "unknown" WITHOUT running it. NOT covered: NED/device-side
    divergence (needs check-sync) and redistribution rows (nested non-keyed, guard parity).
    """
    from nso_adapter.core.removal import _live_family_sections

    unverifiable: list[str] = []  # hoisted so the except still reports it (codex P3)
    try:
        prep = await _reader_compare_prepare(scope, rows, getattr(device, "ned_id", None))
        if prep is None:
            return ok, 0, [], None, []
        translated, unverifiable, spec, wire = prep
        if not translated:  # every key Vault-unverifiable → nothing to look for, run NO action
            return ok, 0, [], "unknown", unverifiable
        section = (await _live_family_sections(client, device.nso_device_name, [wire], timeout=timeout))[wire]
        # The walk stays INSIDE the try (codex P2): a malformed 'ok' section (a non-dict where a
        # keyed entry belongs) makes _reader_keys raise — that must classify "error", never escape
        # and turn a successful commit into an internal job failure.
        n_ok, n_failed, fails, status = _classify_fetched_section(
            scope, translated, unverifiable, spec, section, ok, job_id=job_id, device_name=device_name
        )
        return n_ok, n_failed, fails, status, unverifiable
    except Exception as exc:  # noqa: BLE001 — the check must never fail a good apply
        logger.warning("apply.reader_compare_error", job_id=job_id, device=device_name, scope=scope, error=repr(exc))
        return ok, 0, [], "error", unverifiable


async def _reader_compare_default_path(client, device, sc, scope_ok, *, remaining, ned_id, job_id, device_name):
    """Default-path per-scope verify under the HARD verify-time budget → (ok, failed, fails, status, unverifiable).

    ``remaining`` is the VERIFY budget still unspent (``_VERIFY_TOTAL_BUDGET`` minus the time already
    spent verifying earlier scopes — device COMMIT latency deliberately does NOT count, codex P1, so
    a slow early commit cannot starve later scopes of silent-drop detection). Budget spent → the scope
    is SKIPPED without running the action: a checkable scope records ``unknown`` (never silently
    absent), a structurally-uncheckable one keeps no entry. Otherwise the whole per-scope verify —
    translation + semaphore acquire + HTTP — runs inside a single ``asyncio.wait_for`` clipped to the
    remaining budget (so semaphore contention cannot push total default-path verify time past the
    budget), with the action's own timeout clipped to ``min(_VERIFY_PER_CALL_TIMEOUT, remaining)``. A
    cut verify → ``unknown``.
    """
    from nso_adapter.core.removal import _VERIFY_PER_CALL_TIMEOUT

    if remaining <= 0:
        status = "unknown" if _reader_compare_checkable(sc.key, sc.rows, ned_id) else None
        return scope_ok, 0, [], status, []
    try:
        return await asyncio.wait_for(
            _reader_compare_scope(
                client,
                device,
                sc.key,
                sc.rows,
                ok=scope_ok,
                job_id=job_id,
                device_name=device_name,
                timeout=min(_VERIFY_PER_CALL_TIMEOUT, remaining),
            ),
            timeout=remaining,
        )
    except TimeoutError:  # asyncio.TimeoutError is TimeoutError on 3.11+
        # The whole per-scope verify (translate + semaphore + HTTP) blew the budget; only a
        # checkable scope ever reaches the action, so a cut verify is always "unknown".
        return scope_ok, 0, [], "unknown", []


async def _run_scope(log_label, coro, rows, *, job_id, device_name, now, on_nso_error=None) -> tuple[int, int, list]:
    """Push one scope's batch coroutine and stamp the outcome onto every row in *rows*.

    Returns (in_sync, apply_failed, failures). Success stamps last_apply_at and clears
    the error on every row; an NsoApplyError or any other exception records the error
    payload on every row and reports a single failure. ``on_nso_error`` is a best-effort
    side-effect (route-policy uses it to record a device-parser capability rejection).

    A collateral block (the static-route PUT-replace, §4.1) gets its own clause ahead of
    the broad one: it is a REFUSAL with a machine-readable orphan report and a preview of
    the would-be device delta, and ``repr(exc)`` under ``code: "internal"`` would throw
    both away.
    """
    from nso_adapter.core.removal import RemovalBlockedError

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
    except ClaimLostError:
        # Revocation is not a runner error: recovery already owns the disposition.
        raise
    except RemovalBlockedError as exc:
        logger.error(f"apply.{log_label}_blocked_collateral", job_id=job_id, device=device_name, orphans=exc.orphans)
        err = {
            "code": "removal_blocked_collateral",
            "message": str(exc),
            "detail": {"orphans": exc.orphans, "preview": exc.preview},
        }
        for row in rows:
            row.last_apply_error = err
        # The preview rides the JOB failure too, not just the rows: it is the would-be device
        # delta the operator has to review before deciding to force the replacement, and
        # GET /jobs/{id} is where they read it (the removal path already reports it there).
        return (
            0,
            len(rows),
            [
                {
                    "error": str(exc),
                    "code": "removal_blocked_collateral",
                    "orphans": exc.orphans,
                    "preview": exc.preview,
                    "hint": (
                        "These service rows are not in the accepted intent this apply would "
                        "PUT-replace. Accept them into intent to keep them, or flush them "
                        "deliberately via POST /devices/{id}/actions/force-removal."
                    ),
                }
            ],
        )
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
    reader_compare: dict | None = None,
    reader_compare_unverifiable: dict | None = None,
) -> None:
    """Assemble job.result/status from the pass outcomes and commit.

    With nothing eligible the job succeeds with an all-zero result and returns early.
    Otherwise the per-scope counts are emitted (plus the per-scope post-apply
    reader_compare statuses, #108, and any reader_compare_unverifiable labels — the keys a
    scope's presence check could not verify, mirroring the residue path); any failure flips
    the job to failed and collects the per-item errors.
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
    if reader_compare:
        result["reader_compare"] = reader_compare
    if reader_compare_unverifiable:
        result["reader_compare_unverifiable"] = reader_compare_unverifiable
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


async def _execute_apply(db: AsyncSession, job: Job, job_id: int, device_id: int, force: bool, *, reg=None) -> None:
    """Run the apply body: sync-from, snapshot intent, push each scope, finalize the job.

    Raises on a missing device / NSO-client error so ``run_apply``'s outer handler can
    mark the job failed with an ``internal`` error.

    *reg* is the live claim registration, threaded down for the transactions R2 adds here.
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
    # #1396 R2 §3: ONE classifier decides the static-route mode and snapshots the rows the
    # body is built from, the rows this pass stamps, the keys the guard may see disappear
    # and the tombstones the body must retain.
    sr_plan = await build_plan(db, device, eligible_rows=sr_eligible)
    logging_eligible = await _collect_eligible(db, LoggingHostIntent, device_id, force)
    logging_levels_rows = await _collect_eligible(db, LoggingLevelsIntent, device_id, force)
    logging_levels = logging_levels_rows[0] if logging_levels_rows else None
    logging_rows = [*logging_eligible, *([logging_levels] if logging_levels else [])]
    svi_eligible = await _collect_eligible(db, SviIntent, device_id, force)
    subif_eligible = await _collect_eligible(db, SubinterfaceIntent, device_id, force)
    vlan_eligible = await _collect_eligible(db, VlanIntent, device_id, force)
    bfd_eligible = await _collect_eligible(db, BfdIntent, device_id, force)
    mtu_eligible = await _collect_eligible(db, InterfaceMtuIntent, device_id, force)
    l2_eligible = await _collect_eligible(db, L2SapIntent, device_id, force)
    isis_eligible = await _collect_eligible(db, IsisInterfaceIntent, device_id, force)
    isis_process_eligible = await _collect_eligible(db, IsisProcessIntent, device_id, force)
    isis_flex_eligible = await _collect_eligible(db, IsisFlexAlgoIntent, device_id, force)
    isis_level_eligible = await _collect_eligible(db, IsisLevelIntent, device_id, force)
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
    now = datetime.now(UTC)

    # EVERY collection the scope table below can push must be listed here. The IS-IS
    # sub-collections are eligible on their own (a per-level knob accepted on a device
    # whose interfaces are already in sync): when they were missing, the isis scope still
    # pushed — its _Scope rows list includes them — while _finalize_job took the "nothing
    # eligible" early-return and reported an all-zero SUCCESS for a commit the device had
    # rejected, which the plugin then settled deploying -> in_sync.
    any_eligible = any(
        [
            attr_eligible,
            ip_eligible_by_iface,
            snmp_rows,
            # From the PLAN, never the eligible list: in PUT mode a force=False apply can
            # have an empty eligible list and a non-empty body, and _finalize_job's
            # all-zero early success would then report a clean no-op AFTER a real PUT.
            sr_plan.rows,
            logging_rows,
            svi_eligible,
            subif_eligible,
            vlan_eligible,
            bfd_eligible,
            mtu_eligible,
            l2_eligible,
            isis_eligible,
            isis_process_eligible,
            isis_flex_eligible,
            isis_level_eligible,
            bgp_eligible,
            rp_eligible,
            ospf_instance_eligible,
            ospf_iface_eligible,
            redist_eligible,
        ]
    )

    # Atomic apply (I3b): stage EVERY scope's accepted intent into ONE NSO transaction and
    # commit once. Self-contained (stages, commits, stamps, finalises). When off, the
    # per-scope commit path below runs (per-item attr/IP + one batch commit per scope).
    if atomic_apply_enabled() and any_eligible:
        elig = {
            "ifaces": ifaces,
            "attr": attr_eligible,
            "ip_by_iface": ip_eligible_by_iface,
            "subif": subif_eligible,
            "snmp_rows": snmp_rows,
            "snmp_comm": snmp_comm,
            "snmp_user": snmp_user,
            "snmp_host": snmp_host,
            "snmp_sysinfo": snmp_sysinfo,
            "static_route": sr_eligible,
            "logging": logging_eligible,
            "logging_levels": logging_levels,
            "svi": svi_eligible,
            "vlan": vlan_eligible,
            "bfd": bfd_eligible,
            "mtu": mtu_eligible,
            "l2_sap": l2_eligible,
            "isis_iface": isis_eligible,
            "isis_proc": isis_process_eligible,
            "isis_flex": isis_flex_eligible,
            "isis_levels": isis_level_eligible,
            "bgp": bgp_eligible,
            "rp": rp_eligible,
            "ospf_inst": ospf_instance_eligible,
            "ospf_iface": ospf_iface_eligible,
            "redist_ospf": redist_ospf,
            "redist_isis": redist_isis,
            "redist_bgp": redist_bgp,
        }
        await _run_atomic_apply(db, device, client, device_name, job, job_id, now, elig)
        # §4.11 retry path, on BOTH apply implementations: the atomic path is a separate
        # early return with its own finalization, so wiring this only into the per-scope
        # loop below would leave atomic-mode applies enqueueing nothing.
        await _enqueue_pending_clear_retract(db, device, sr_plan, reg=reg)
        return

    # ── Step 2: mark attribute states deploying ──
    for attr_state, _intent_row, _iface in attr_eligible:
        attr_state.sync_state = SyncState.deploying
    await db.commit()

    # ── Step 3–6: per-item attribute + IP passes ──
    attr_outcome = await _apply_attributes(
        attr_eligible, apply_interface_attribute, client=client, device_name=device_name, job_id=job_id, now=now
    )
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
        except ClaimLostError:
            # A nested suppressor is as load-bearing as the runner boundary: swallowing
            # a revocation here lets the run continue under ownership it has lost.
            raise
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
            # plan.rows, not the eligible list: in PUT mode the body is every ACCEPTED row
            # (an eligible-only body retracts every accepted-and-clean route), so those are
            # exactly the rows this pass stamps. In PATCH mode the two are the same list.
            sr_plan.rows,
            lambda: _static_route_coro(client, device, sr_plan),
        ),
        _Scope(
            "logging",
            "logging",
            logging_rows,
            lambda: apply_logging_config(
                client=client,
                device_name=device_name,
                host_intent_rows=logging_eligible,
                levels_intent_row=logging_levels,
            ),
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
            [*isis_eligible, *isis_process_eligible, *redist_isis, *isis_flex_eligible, *isis_level_eligible],
            lambda: apply_isis_interfaces(
                client=client,
                device_name=device_name,
                isis_intent_rows=isis_eligible,
                isis_process_rows=isis_process_eligible,
                redistribution_rows=redist_isis,
                flex_algo_rows=isis_flex_eligible,
                level_rows=isis_level_eligible,
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

    # #108/1328: each scope commits then verifies immediately (default path, atomic-apply OFF)
    # — the per-scope action reads the far side of the FASTMAP writer as soon as possible after
    # ITS OWN commit (preserving the legacy immediate-per-scope timing, r2-M2). The action is
    # HEAVY (a live CDB build, not the cache-backed legacy GET), so a HARD budget bounds the total
    # default-path VERIFY time: once spent, remaining scopes skip to "unknown" rather than
    # serialising 60s each behind the shared 4-slot action semaphore (r3-M3/r4-M1). Only verify
    # time is charged — device COMMIT latency is excluded (codex P1), so a slow early commit can
    # never starve later scopes of silent-drop detection.
    from nso_adapter.core.removal import _VERIFY_TOTAL_BUDGET

    loop = asyncio.get_running_loop()
    verify_spent = 0.0
    device_ned_id = getattr(device, "ned_id", None)

    scope_outcomes: dict[str, tuple[int, int]] = {}
    scope_failures: dict[str, list] = {}
    reader_compare: dict[str, str] = {}
    reader_compare_unverifiable: dict[str, list[str]] = {}
    for sc in scopes:
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
        if scope_failed == 0:
            # #108: the commit reported success — require every intended key to be
            # present in the scope's device-state section (the #26 silent-drop class).
            verify_started = loop.time()
            scope_ok, scope_failed, fails, rc_status, rc_unver = await _reader_compare_default_path(
                client,
                device,
                sc,
                scope_ok,
                remaining=_VERIFY_TOTAL_BUDGET - verify_spent,
                ned_id=device_ned_id,
                job_id=job_id,
                device_name=device_name,
            )
            verify_spent += loop.time() - verify_started  # charge only verify time, not the commit
            if rc_status is not None:
                reader_compare[sc.key] = rc_status
            if rc_unver:
                reader_compare_unverifiable[sc.key] = rc_unver
        scope_outcomes[sc.key] = (scope_ok, scope_failed)
        if fails:
            scope_failures[sc.key] = fails

    # ── Step 7: finalize ── (any_eligible computed up front, before the atomic branch)
    await _finalize_job(
        db,
        job,
        job_id,
        device_id,
        any_eligible,
        attr_outcome,
        ip_outcome,
        scope_outcomes,
        scope_failures,
        reader_compare=reader_compare,
        reader_compare_unverifiable=reader_compare_unverifiable,
    )
    await _enqueue_pending_clear_retract(db, device, sr_plan, reg=reg)


async def _post_apply_refresh_and_notify(db: AsyncSession, device_id: int) -> None:
    """Re-read the just-applied surfaces back into the adapter read-mirror and notify the plugin.

    Apply commits config to NSO but — unlike ``sync_device`` — never refreshes the read-mirror
    (``GET /route-policy`` & friends serve cached DB rows) and never fires the sync-complete
    callback. So the plugin's own settle logic, which flips a ``deploying`` overlay row to
    ``in_sync`` only once the applied object is *present* in the adapter payload, reads a stale
    mirror on the immediate post-apply reconcile and re-marks the row ``deploying`` — it settles
    only on the next periodic sync (observed for route-policy on rg03: the row sat ``deploying``
    until the 15-min sync). Re-reading every surface that backs a ``deploying`` overlay — the
    routing surfaces (route-policy / IS-IS / OSPF / BGP / BFD / …) plus the L2/interface config
    surfaces (VLAN / SVI / subinterface / MTU) that ``sync_device`` does *not* fan out to — and
    then notifying the plugin lets those rows settle right after Apply.

    Best-effort: the Apply job is already finalized, so a refresh/notify failure must not fail it —
    the periodic sync remains the backstop.
    """
    from nso_adapter.core.importer import (
        get_netbox_client,
        get_nso_client,
        refresh_config_surfaces_for_device,
        refresh_routing_surfaces_for_device,
    )

    try:
        device = await db.get(Device, device_id)
        if device is None:
            return
        client = get_nso_client(device.nso_instance)
        await refresh_routing_surfaces_for_device(db, device, client, refresh_source="apply")
        await refresh_config_surfaces_for_device(db, device, client, refresh_source="apply")
        await db.commit()
        nb_client = get_netbox_client()
        if nb_client and device.netbox_device_id:
            await nb_client.notify_sync_complete(device.netbox_device_id)
    except ClaimLostError:
        # Revocation is not a runner error: recovery already owns the disposition.
        raise
    except Exception as exc:  # noqa: BLE001 — best-effort; never fail an already-finalized Apply
        logger.warning("apply.post_refresh_failed", device_id=device_id, error=repr(exc))


async def run_apply(job_id: int, device_id: int, force: bool = True, reg=None) -> None:
    """Background task: execute the apply for *device_id* (see module docstring §7a).

    *reg* is the worker's live ``ClaimRegistration``. R1 stopped it at the job runner, so
    nothing the apply wrote could be claim-scoped; R2's CAS and carrier transactions guard
    themselves with it.
    """
    from nso_adapter.store.db import get_session

    async for db in get_session():
        job = await db.get(Job, job_id)
        if not job:
            logger.error("apply.job_not_found", job_id=job_id)
            return
        job.status = JobStatus.running
        await db.commit()

        try:
            await _execute_apply(db, job, job_id, device_id, force, reg=reg)
        except ClaimLostError:
            # Revocation is not a runner error: recovery already owns the disposition.
            raise
        except Exception as exc:
            logger.exception("apply.unexpected_error", job_id=job_id, device_id=device_id)
            # Roll back first: if the failure came from a DB error the session is in a
            # needs-rollback state and the failed-status commit below would itself throw,
            # leaving the job stuck 'running' and masking the real error. Re-fetch the job
            # after rollback (it may have been expired) so the status change persists.
            await db.rollback()
            job = await db.get(Job, job_id)
            if job is not None:
                job.status = JobStatus.failed
                job.error = {"code": "internal", "message": repr(exc), "detail": {}}
                await db.commit()
        else:
            # Apply finalized (succeeded/partial/failed-on-device, no unexpected error): re-read
            # the applied surfaces into the mirror and notify the plugin so a 'deploying' row
            # settles on the immediate post-apply reconcile, not only on the next periodic sync.
            await _post_apply_refresh_and_notify(db, device_id)
