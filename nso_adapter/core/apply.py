# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Apply worker — push accepted intent to NSO (Phase 2, M5).

Follows the flow described in docs/nso-adapter.md §7a:
  1. Snapshot intent into job.context
  2. Mark each in-scope attribute as 'deploying'
  3. Commit each (interface, attribute) via NSO reconcile-commit service
  4. On success: status → in_sync, update last_apply_at
  5. On failure: status → apply_failed, capture error in last_apply_error

Concurrency: relies on the existing one-job-per-device rule in core/jobs.py.
"""

from __future__ import annotations

from datetime import UTC, datetime

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.store.models import (
    BgpAfIntent,
    BgpPeerAfIntent,
    BgpPeerIntent,
    BgpRouterIntent,
    BgpScopeIntent,
    DbInterface,
    Device,
    InterfaceAttrState,
    InterfaceIntent,
    InterfaceIpIntent,
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
    SviIntent,
    SyncState,
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
    """Derive the SR OS router context (base|ies|vprn) for a Nokia routed interface (M27).

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


async def run_apply(job_id: int, device_id: int, force: bool = True) -> None:
    """Background task: execute the apply for *device_id*."""
    from nso_adapter.core.importer import get_nso_client
    from nso_adapter.nso.apply import (
        NsoApplyError,
        apply_bgp_config,
        apply_interface_attribute,
        apply_interface_ips,
        apply_isis_interfaces,
        apply_l2_saps,
        apply_logging_config,
        apply_ospf_config,
        apply_route_policy_config,
        apply_snmp_config,
        apply_static_routes,
        apply_svi_config,
    )
    from nso_adapter.store.db import get_session

    async for db in get_session():
        job = await db.get(Job, job_id)
        if not job:
            logger.error("apply.job_not_found", job_id=job_id)
            return
        job.status = JobStatus.running
        await db.commit()

        try:
            device = await db.get(Device, device_id)
            if not device:
                raise ValueError(f"Device {device_id} not found")

            client = get_nso_client(device.nso_instance)
            device_name = device.nso_device_name

            # ── Step 1: snapshot intent ──────────────────────────────────────
            ifaces_result = await db.execute(select(DbInterface).where(DbInterface.device_id == device_id))
            ifaces = {iface.id: iface for iface in ifaces_result.scalars().all()}

            intent_snapshot: list[dict] = []
            eligible: list[tuple[InterfaceAttrState, InterfaceIntent]] = []

            eligible_statuses = _FORCE_ELIGIBLE if force else _NO_FORCE_ELIGIBLE

            for iface in ifaces.values():
                intent_rows = await db.execute(select(InterfaceIntent).where(InterfaceIntent.interface_id == iface.id))
                for intent_row in intent_rows.scalars().all():
                    # Find the attr_state for sync_state check
                    attr_state_result = await db.execute(
                        select(InterfaceAttrState).where(
                            InterfaceAttrState.interface_id == iface.id,
                            InterfaceAttrState.attribute == intent_row.attribute,
                        )
                    )
                    attr_state = attr_state_result.scalar_one_or_none()

                    # Include in snapshot regardless; eligibility filters apply
                    snapshot_entry = {
                        "interface": iface.name,
                        "attribute": intent_row.attribute,
                        "intent_value": intent_row.intent_value,
                        "accepted_at": intent_row.accepted_at.isoformat() if intent_row.accepted_at else None,
                        "status_at_snapshot": attr_state.sync_state.value if attr_state else "unknown",
                    }
                    intent_snapshot.append(snapshot_entry)

                    if attr_state and attr_state.sync_state in eligible_statuses:
                        eligible.append((attr_state, intent_row, iface))  # type: ignore[arg-type]

            # Snapshot IP intent rows for context
            ip_snapshot: list[dict] = []
            ip_eligible_by_iface: dict[int, list[InterfaceIpIntent]] = {}

            for iface in ifaces.values():
                ip_rows_result = await db.execute(
                    select(InterfaceIpIntent).where(InterfaceIpIntent.interface_id == iface.id)
                )
                ip_rows = ip_rows_result.scalars().all()
                for row in ip_rows:
                    ip_snapshot.append(
                        {
                            "interface": iface.name,
                            "address": row.address,
                            "family": row.family,
                            "secondary": row.secondary,
                            "vrf": row.vrf,
                            "accepted_at": row.accepted_at.isoformat() if row.accepted_at else None,
                        }
                    )
                    # Eligibility: accepted_at must be set; force=False also requires pending apply
                    if row.accepted_at is None:
                        continue
                    if not force and row.last_apply_at is not None and row.last_apply_error is None:
                        continue
                    ip_eligible_by_iface.setdefault(iface.id, []).append(row)

            # Snapshot SNMP intent rows for context
            snmp_comm_eligible: list[SnmpCommunityIntent] = []
            snmp_user_eligible: list[SnmpV3UserIntent] = []
            snmp_host_eligible: list[SnmpHostIntent] = []
            snmp_sysinfo_eligible: SnmpSystemInfoIntent | None = None
            snmp_has_eligible = False

            snmp_comm_result = await db.execute(
                select(SnmpCommunityIntent).where(SnmpCommunityIntent.device_id == device_id)
            )
            for row in snmp_comm_result.scalars().all():
                if row.accepted_at is None:
                    continue
                if not force and row.last_apply_at is not None and row.last_apply_error is None:
                    continue
                snmp_comm_eligible.append(row)
                snmp_has_eligible = True

            snmp_user_result = await db.execute(select(SnmpV3UserIntent).where(SnmpV3UserIntent.device_id == device_id))
            for row in snmp_user_result.scalars().all():
                if row.accepted_at is None:
                    continue
                if not force and row.last_apply_at is not None and row.last_apply_error is None:
                    continue
                snmp_user_eligible.append(row)
                snmp_has_eligible = True

            snmp_host_result = await db.execute(select(SnmpHostIntent).where(SnmpHostIntent.device_id == device_id))
            for row in snmp_host_result.scalars().all():
                if row.accepted_at is None:
                    continue
                if not force and row.last_apply_at is not None and row.last_apply_error is None:
                    continue
                snmp_host_eligible.append(row)
                snmp_has_eligible = True

            snmp_sysinfo_result = await db.execute(
                select(SnmpSystemInfoIntent).where(SnmpSystemInfoIntent.device_id == device_id)
            )
            sysinfo_row = snmp_sysinfo_result.scalar_one_or_none()
            if sysinfo_row and sysinfo_row.accepted_at is not None:
                if force or sysinfo_row.last_apply_at is None or sysinfo_row.last_apply_error is not None:
                    snmp_sysinfo_eligible = sysinfo_row
                    snmp_has_eligible = True

            # Collect static route intent rows
            sr_eligible: list[StaticRouteIntent] = []
            sr_result = await db.execute(select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id))
            for row in sr_result.scalars().all():
                if row.accepted_at is None:
                    continue
                if not force and row.last_apply_at is not None and row.last_apply_error is None:
                    continue
                sr_eligible.append(row)

            # Collect logging (remote-syslog) intent rows
            logging_eligible: list[LoggingHostIntent] = []
            logging_result = await db.execute(
                select(LoggingHostIntent).where(LoggingHostIntent.device_id == device_id)
            )
            for row in logging_result.scalars().all():
                if row.accepted_at is None:
                    continue
                if not force and row.last_apply_at is not None and row.last_apply_error is None:
                    continue
                logging_eligible.append(row)

            # Collect SVI/IRB intent rows (M35 write path)
            svi_eligible: list[SviIntent] = []
            svi_result = await db.execute(select(SviIntent).where(SviIntent.device_id == device_id))
            for row in svi_result.scalars().all():
                if row.accepted_at is None:
                    continue
                if not force and row.last_apply_at is not None and row.last_apply_error is None:
                    continue
                svi_eligible.append(row)

            # Collect L2 SAP intent rows (M37 P2b — Nokia SAP write path)
            l2_eligible: list[L2SapIntent] = []
            l2_result = await db.execute(select(L2SapIntent).where(L2SapIntent.device_id == device_id))
            for row in l2_result.scalars().all():
                if row.accepted_at is None:
                    continue
                if not force and row.last_apply_at is not None and row.last_apply_error is None:
                    continue
                l2_eligible.append(row)

            # Collect IS-IS interface intent rows
            isis_eligible: list[IsisInterfaceIntent] = []
            isis_result = await db.execute(
                select(IsisInterfaceIntent).where(IsisInterfaceIntent.device_id == device_id)
            )
            for row in isis_result.scalars().all():
                if row.accepted_at is None:
                    continue
                if not force and row.last_apply_at is not None and row.last_apply_error is None:
                    continue
                isis_eligible.append(row)

            # Collect IS-IS process intent rows (applied alongside interface rows)
            isis_process_eligible: list[IsisProcessIntent] = []
            isis_proc_result = await db.execute(
                select(IsisProcessIntent).where(IsisProcessIntent.device_id == device_id)
            )
            for row in isis_proc_result.scalars().all():
                if row.accepted_at is None:
                    continue
                if not force and row.last_apply_at is not None and row.last_apply_error is None:
                    continue
                isis_process_eligible.append(row)

            # Collect BGP router intent rows (eligibility at the router level)
            bgp_eligible: list[BgpRouterIntent] = []
            bgp_router_result = await db.execute(select(BgpRouterIntent).where(BgpRouterIntent.device_id == device_id))
            for row in bgp_router_result.scalars().all():
                if row.accepted_at is None:
                    continue
                if not force and row.last_apply_at is not None and row.last_apply_error is None:
                    continue
                bgp_eligible.append(row)

            # Eagerly load BGP relationships for apply (avoids lazy-raise)
            if bgp_eligible:
                bgp_router_ids = [r.id for r in bgp_eligible]
                scope_result = await db.execute(
                    select(BgpScopeIntent).where(BgpScopeIntent.router_id.in_(bgp_router_ids))
                )
                scopes_by_router: dict[int, list[BgpScopeIntent]] = {}
                all_scopes = scope_result.scalars().all()
                for s in all_scopes:
                    scopes_by_router.setdefault(s.router_id, []).append(s)

                scope_ids = [s.id for s in all_scopes]
                af_result = (
                    await db.execute(select(BgpAfIntent).where(BgpAfIntent.scope_id.in_(scope_ids)))
                    if scope_ids
                    else None
                )
                afs_by_scope: dict[int, list[BgpAfIntent]] = {}
                if af_result:
                    for af in af_result.scalars().all():
                        afs_by_scope.setdefault(af.scope_id, []).append(af)

                peer_result = (
                    await db.execute(select(BgpPeerIntent).where(BgpPeerIntent.scope_id.in_(scope_ids)))
                    if scope_ids
                    else None
                )
                peers_by_scope: dict[int, list[BgpPeerIntent]] = {}
                all_peers: list[BgpPeerIntent] = []
                if peer_result:
                    all_peers = peer_result.scalars().all()
                    for p in all_peers:
                        peers_by_scope.setdefault(p.scope_id, []).append(p)

                peer_ids = [p.id for p in all_peers]
                paf_result = (
                    await db.execute(select(BgpPeerAfIntent).where(BgpPeerAfIntent.peer_id.in_(peer_ids)))
                    if peer_ids
                    else None
                )
                pafs_by_peer: dict[int, list[BgpPeerAfIntent]] = {}
                if paf_result:
                    for paf in paf_result.scalars().all():
                        pafs_by_peer.setdefault(paf.peer_id, []).append(paf)

                for s in all_scopes:
                    s.__dict__["address_families"] = afs_by_scope.get(s.id, [])
                    s.__dict__["peers"] = peers_by_scope.get(s.id, [])
                for p in all_peers:
                    p.__dict__["peer_address_families"] = pafs_by_peer.get(p.id, [])
                for r in bgp_eligible:
                    r.__dict__["scopes"] = scopes_by_router.get(r.id, [])

            # Collect route-policy object intent rows
            rp_eligible: list[RoutePolicyObjectIntent] = []
            rp_result = await db.execute(
                select(RoutePolicyObjectIntent).where(RoutePolicyObjectIntent.device_id == device_id)
            )
            for row in rp_result.scalars().all():
                if row.accepted_at is None:
                    continue
                if not force and row.last_apply_at is not None and row.last_apply_error is None:
                    continue
                rp_eligible.append(row)

            # Collect OSPF instance intent rows
            ospf_instance_eligible: list[OspfInstanceIntent] = []
            ospf_inst_result = await db.execute(
                select(OspfInstanceIntent).where(OspfInstanceIntent.device_id == device_id)
            )
            for row in ospf_inst_result.scalars().all():
                if row.accepted_at is None:
                    continue
                if not force and row.last_apply_at is not None and row.last_apply_error is None:
                    continue
                ospf_instance_eligible.append(row)

            # Collect OSPF interface intent rows
            ospf_iface_eligible: list[OspfInterfaceIntent] = []
            ospf_iface_result = await db.execute(
                select(OspfInterfaceIntent).where(OspfInterfaceIntent.device_id == device_id)
            )
            for row in ospf_iface_result.scalars().all():
                if row.accepted_at is None:
                    continue
                if not force and row.last_apply_at is not None and row.last_apply_error is None:
                    continue
                ospf_iface_eligible.append(row)

            # Collect redistribution intent rows (keyed by dest_protocol)
            redist_eligible: list[RedistributionIntent] = []
            redist_result = await db.execute(
                select(RedistributionIntent).where(RedistributionIntent.device_id == device_id)
            )
            for row in redist_result.scalars().all():
                if row.accepted_at is None:
                    continue
                if not force and row.last_apply_at is not None and row.last_apply_error is None:
                    continue
                redist_eligible.append(row)

            redist_ospf = [r for r in redist_eligible if r.dest_protocol == "ospf"]
            redist_isis = [r for r in redist_eligible if r.dest_protocol == "isis"]
            redist_bgp = [r for r in redist_eligible if r.dest_protocol == "bgp"]

            job.context = {
                "force": force,
                "intent_snapshot": intent_snapshot,
                "ip_snapshot": ip_snapshot,
            }

            now = datetime.now(UTC).replace(tzinfo=None)

            # ── Step 2: mark attribute states as deploying ───────────────────
            for attr_state, _intent_row, _iface in eligible:
                attr_state.sync_state = SyncState.deploying
            await db.commit()

            # ── Step 3–5: commit each attribute ─────────────────────────────
            outcome_in_sync = 0
            outcome_failed = 0
            failed_attrs: list[dict] = []

            for attr_state, intent_row, iface in eligible:
                try:
                    await apply_interface_attribute(
                        client=client,
                        device_name=device_name,
                        interface_name=iface.name,
                        attribute=intent_row.attribute,
                        value=intent_row.intent_value,
                    )
                    attr_state.sync_state = SyncState.in_sync
                    intent_row.last_apply_at = now
                    intent_row.last_apply_error = None
                    outcome_in_sync += 1
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
                    intent_row.last_apply_error = {
                        "code": exc.code,
                        "message": exc.message,
                        "detail": exc.detail,
                    }
                    outcome_failed += 1
                    failed_attrs.append(
                        {
                            "interface": iface.name,
                            "attribute": intent_row.attribute,
                            "error": exc.message,
                        }
                    )
                except Exception as exc:
                    logger.exception(
                        "apply.attribute_unexpected_error",
                        job_id=job_id,
                        interface=iface.name,
                        attribute=intent_row.attribute,
                    )
                    attr_state.sync_state = SyncState.apply_failed
                    intent_row.last_apply_error = {
                        "code": "internal",
                        "message": repr(exc),
                        "detail": {},
                    }
                    outcome_failed += 1
                    failed_attrs.append(
                        {
                            "interface": iface.name,
                            "attribute": intent_row.attribute,
                            "error": repr(exc),
                        }
                    )

            # ── Step 6: IP intent pass ───────────────────────────────────────
            ip_outcome_ok = 0
            ip_outcome_failed = 0
            failed_ips: list[dict] = []

            for iface_id, ip_rows in ip_eligible_by_iface.items():
                iface = ifaces[iface_id]
                routed_kind = _nokia_routed_kind(iface)
                try:
                    await apply_interface_ips(
                        client=client,
                        device_name=device_name,
                        interface_name=iface.name,
                        ip_intent_rows=ip_rows,
                        kind=routed_kind,
                        service=iface.service if routed_kind in ("ies", "vprn") else None,
                        parent_binding=iface.parent_binding,
                        encap_tag=iface.encap_tag,
                    )
                    for row in ip_rows:
                        row.last_apply_at = now
                        row.last_apply_error = None
                    ip_outcome_ok += len(ip_rows)
                except NsoApplyError as exc:
                    logger.error(
                        "apply.ip_failed",
                        job_id=job_id,
                        device=device_name,
                        interface=iface.name,
                        error=exc.message,
                    )
                    for row in ip_rows:
                        row.last_apply_error = {
                            "code": exc.code,
                            "message": exc.message,
                            "detail": exc.detail,
                        }
                    ip_outcome_failed += len(ip_rows)
                    failed_ips.append({"interface": iface.name, "error": exc.message})
                except Exception as exc:
                    logger.exception(
                        "apply.ip_unexpected_error",
                        job_id=job_id,
                        interface=iface.name,
                    )
                    for row in ip_rows:
                        row.last_apply_error = {"code": "internal", "message": repr(exc), "detail": {}}
                    ip_outcome_failed += len(ip_rows)
                    failed_ips.append({"interface": iface.name, "error": repr(exc)})

            # ── Step 6b: SNMP intent pass ────────────────────────────────────
            snmp_outcome_ok = 0
            snmp_outcome_failed = 0
            snmp_failed: list[dict] = []

            if snmp_has_eligible:
                try:
                    await apply_snmp_config(
                        client=client,
                        device_name=device_name,
                        community_intents=snmp_comm_eligible,
                        v3_user_intents=snmp_user_eligible,
                        host_intents=snmp_host_eligible,
                        system_info_intent=snmp_sysinfo_eligible,
                    )
                    for row in snmp_comm_eligible:
                        row.last_apply_at = now
                        row.last_apply_error = None
                    for row in snmp_user_eligible:
                        row.last_apply_at = now
                        row.last_apply_error = None
                    for row in snmp_host_eligible:
                        row.last_apply_at = now
                        row.last_apply_error = None
                    if snmp_sysinfo_eligible:
                        snmp_sysinfo_eligible.last_apply_at = now
                        snmp_sysinfo_eligible.last_apply_error = None
                    snmp_outcome_ok = (
                        len(snmp_comm_eligible)
                        + len(snmp_user_eligible)
                        + len(snmp_host_eligible)
                        + (1 if snmp_sysinfo_eligible else 0)
                    )
                except NsoApplyError as exc:
                    logger.error(
                        "apply.snmp_failed",
                        job_id=job_id,
                        device=device_name,
                        error=exc.message,
                    )
                    err_payload = {"code": exc.code, "message": exc.message, "detail": exc.detail}
                    for row in snmp_comm_eligible:
                        row.last_apply_error = err_payload
                    for row in snmp_user_eligible:
                        row.last_apply_error = err_payload
                    for row in snmp_host_eligible:
                        row.last_apply_error = err_payload
                    if snmp_sysinfo_eligible:
                        snmp_sysinfo_eligible.last_apply_error = err_payload
                    snmp_outcome_failed = (
                        len(snmp_comm_eligible)
                        + len(snmp_user_eligible)
                        + len(snmp_host_eligible)
                        + (1 if snmp_sysinfo_eligible else 0)
                    )
                    snmp_failed.append({"error": exc.message})
                except Exception as exc:
                    logger.exception("apply.snmp_unexpected_error", job_id=job_id)
                    err_payload = {"code": "internal", "message": repr(exc), "detail": {}}
                    for row in snmp_comm_eligible:
                        row.last_apply_error = err_payload
                    for row in snmp_user_eligible:
                        row.last_apply_error = err_payload
                    for row in snmp_host_eligible:
                        row.last_apply_error = err_payload
                    if snmp_sysinfo_eligible:
                        snmp_sysinfo_eligible.last_apply_error = err_payload
                    snmp_outcome_failed = (
                        len(snmp_comm_eligible)
                        + len(snmp_user_eligible)
                        + len(snmp_host_eligible)
                        + (1 if snmp_sysinfo_eligible else 0)
                    )
                    snmp_failed.append({"error": repr(exc)})

            # ── Step 6c: static route intent pass ───────────────────────────
            sr_outcome_ok = 0
            sr_outcome_failed = 0
            sr_failed: list[dict] = []

            if sr_eligible:
                try:
                    await apply_static_routes(
                        client=client,
                        device_name=device_name,
                        route_intent_rows=sr_eligible,
                    )
                    for row in sr_eligible:
                        row.last_apply_at = now
                        row.last_apply_error = None
                    sr_outcome_ok = len(sr_eligible)
                except NsoApplyError as exc:
                    logger.error(
                        "apply.static_route_failed",
                        job_id=job_id,
                        device=device_name,
                        error=exc.message,
                    )
                    err_payload = {"code": exc.code, "message": exc.message, "detail": exc.detail}
                    for row in sr_eligible:
                        row.last_apply_error = err_payload
                    sr_outcome_failed = len(sr_eligible)
                    sr_failed.append({"error": exc.message})
                except Exception as exc:
                    logger.exception("apply.static_route_unexpected_error", job_id=job_id)
                    err_payload = {"code": "internal", "message": repr(exc), "detail": {}}
                    for row in sr_eligible:
                        row.last_apply_error = err_payload
                    sr_outcome_failed = len(sr_eligible)
                    sr_failed.append({"error": repr(exc)})

            # ── Step 6c1: logging (remote-syslog) intent pass ────────────────
            logging_outcome_ok = 0
            logging_outcome_failed = 0
            logging_failed: list[dict] = []

            if logging_eligible:
                try:
                    await apply_logging_config(
                        client=client,
                        device_name=device_name,
                        host_intent_rows=logging_eligible,
                    )
                    for row in logging_eligible:
                        row.last_apply_at = now
                        row.last_apply_error = None
                    logging_outcome_ok = len(logging_eligible)
                except NsoApplyError as exc:
                    logger.error(
                        "apply.logging_failed", job_id=job_id, device=device_name, error=exc.message
                    )
                    err_payload = {"code": exc.code, "message": exc.message, "detail": exc.detail}
                    for row in logging_eligible:
                        row.last_apply_error = err_payload
                    logging_outcome_failed = len(logging_eligible)
                    logging_failed.append({"error": exc.message})
                except Exception as exc:
                    logger.exception("apply.logging_unexpected_error", job_id=job_id)
                    err_payload = {"code": "internal", "message": repr(exc), "detail": {}}
                    for row in logging_eligible:
                        row.last_apply_error = err_payload
                    logging_outcome_failed = len(logging_eligible)
                    logging_failed.append({"error": repr(exc)})

            # ── Step 6c1b: SVI/IRB intent pass (M35) ─────────────────────────
            svi_outcome_ok = 0
            svi_outcome_failed = 0
            svi_failed: list[dict] = []

            if svi_eligible:
                try:
                    await apply_svi_config(client=client, device_name=device_name, svi_intent_rows=svi_eligible)
                    for row in svi_eligible:
                        row.last_apply_at = now
                        row.last_apply_error = None
                    svi_outcome_ok = len(svi_eligible)
                except NsoApplyError as exc:
                    logger.error("apply.svi_failed", job_id=job_id, device=device_name, error=exc.message)
                    err_payload = {"code": exc.code, "message": exc.message, "detail": exc.detail}
                    for row in svi_eligible:
                        row.last_apply_error = err_payload
                    svi_outcome_failed = len(svi_eligible)
                    svi_failed.append({"error": exc.message})
                except Exception as exc:
                    logger.exception("apply.svi_unexpected_error", job_id=job_id)
                    err_payload = {"code": "internal", "message": repr(exc), "detail": {}}
                    for row in svi_eligible:
                        row.last_apply_error = err_payload
                    svi_outcome_failed = len(svi_eligible)
                    svi_failed.append({"error": repr(exc)})

            # ── Step 6c2: L2 SAP intent pass (M37 P2b) ───────────────────────
            l2_outcome_ok = 0
            l2_outcome_failed = 0
            l2_failed: list[dict] = []

            if l2_eligible:
                try:
                    await apply_l2_saps(
                        client=client,
                        device_name=device_name,
                        sap_intent_rows=l2_eligible,
                    )
                    for row in l2_eligible:
                        row.last_apply_at = now
                        row.last_apply_error = None
                    l2_outcome_ok = len(l2_eligible)
                except NsoApplyError as exc:
                    logger.error(
                        "apply.l2_sap_failed",
                        job_id=job_id,
                        device=device_name,
                        error=exc.message,
                    )
                    err_payload = {"code": exc.code, "message": exc.message, "detail": exc.detail}
                    for row in l2_eligible:
                        row.last_apply_error = err_payload
                    l2_outcome_failed = len(l2_eligible)
                    l2_failed.append({"error": exc.message})
                except Exception as exc:
                    logger.exception("apply.l2_sap_unexpected_error", job_id=job_id)
                    err_payload = {"code": "internal", "message": repr(exc), "detail": {}}
                    for row in l2_eligible:
                        row.last_apply_error = err_payload
                    l2_outcome_failed = len(l2_eligible)
                    l2_failed.append({"error": repr(exc)})

            # ── Step 6d: IS-IS interface + process intent pass ───────────────
            isis_outcome_ok = 0
            isis_outcome_failed = 0
            isis_failed: list[dict] = []

            if isis_eligible or isis_process_eligible or redist_isis:
                try:
                    await apply_isis_interfaces(
                        client=client,
                        device_name=device_name,
                        isis_intent_rows=isis_eligible,
                        isis_process_rows=isis_process_eligible,
                        redistribution_rows=redist_isis,
                    )
                    for row in isis_eligible:
                        row.last_apply_at = now
                        row.last_apply_error = None
                    for row in isis_process_eligible:
                        row.last_apply_at = now
                        row.last_apply_error = None
                    for row in redist_isis:
                        row.last_apply_at = now
                        row.last_apply_error = None
                    isis_outcome_ok = len(isis_eligible) + len(isis_process_eligible) + len(redist_isis)
                except NsoApplyError as exc:
                    logger.error(
                        "apply.isis_failed",
                        job_id=job_id,
                        device=device_name,
                        error=exc.message,
                    )
                    err_payload = {"code": exc.code, "message": exc.message, "detail": exc.detail}
                    for row in isis_eligible:
                        row.last_apply_error = err_payload
                    for row in isis_process_eligible:
                        row.last_apply_error = err_payload
                    for row in redist_isis:
                        row.last_apply_error = err_payload
                    isis_outcome_failed = len(isis_eligible) + len(isis_process_eligible) + len(redist_isis)
                    isis_failed.append({"error": exc.message})
                except Exception as exc:
                    logger.exception("apply.isis_unexpected_error", job_id=job_id)
                    err_payload = {"code": "internal", "message": repr(exc), "detail": {}}
                    for row in isis_eligible:
                        row.last_apply_error = err_payload
                    for row in isis_process_eligible:
                        row.last_apply_error = err_payload
                    for row in redist_isis:
                        row.last_apply_error = err_payload
                    isis_outcome_failed = len(isis_eligible) + len(isis_process_eligible) + len(redist_isis)
                    isis_failed.append({"error": repr(exc)})

            # ── Step 6e: BGP intent pass ──────────────────────────────────────
            bgp_outcome_ok = 0
            bgp_outcome_failed = 0
            bgp_failed: list[dict] = []

            if bgp_eligible or redist_bgp:
                try:
                    await apply_bgp_config(
                        client=client,
                        device_name=device_name,
                        router_intent_rows=bgp_eligible,
                        redistribution_rows=redist_bgp,
                    )
                    for row in bgp_eligible:
                        row.last_apply_at = now
                        row.last_apply_error = None
                    for row in redist_bgp:
                        row.last_apply_at = now
                        row.last_apply_error = None
                    bgp_outcome_ok = len(bgp_eligible) + len(redist_bgp)
                except NsoApplyError as exc:
                    logger.error(
                        "apply.bgp_failed",
                        job_id=job_id,
                        device=device_name,
                        error=exc.message,
                    )
                    err_payload = {"code": exc.code, "message": exc.message, "detail": exc.detail}
                    for row in bgp_eligible:
                        row.last_apply_error = err_payload
                    for row in redist_bgp:
                        row.last_apply_error = err_payload
                    bgp_outcome_failed = len(bgp_eligible) + len(redist_bgp)
                    bgp_failed.append({"error": exc.message})
                except Exception as exc:
                    logger.exception("apply.bgp_unexpected_error", job_id=job_id)
                    err_payload = {"code": "internal", "message": repr(exc), "detail": {}}
                    for row in bgp_eligible:
                        row.last_apply_error = err_payload
                    for row in redist_bgp:
                        row.last_apply_error = err_payload
                    bgp_outcome_failed = len(bgp_eligible) + len(redist_bgp)
                    bgp_failed.append({"error": repr(exc)})

            # ── Step 6f: route-policy intent pass ────────────────────────────
            rp_outcome_ok = 0
            rp_outcome_failed = 0
            rp_failed: list[dict] = []

            if rp_eligible:
                try:
                    await apply_route_policy_config(
                        client=client,
                        device_name=device_name,
                        intent_rows=rp_eligible,
                    )
                    for row in rp_eligible:
                        row.last_apply_at = now
                        row.last_apply_error = None
                    rp_outcome_ok = len(rp_eligible)
                except NsoApplyError as exc:
                    logger.error(
                        "apply.route_policy_failed",
                        job_id=job_id,
                        device=device_name,
                        error=exc.message,
                    )
                    err_payload = {"code": exc.code, "message": exc.message, "detail": exc.detail}
                    for row in rp_eligible:
                        row.last_apply_error = err_payload
                    rp_outcome_failed = len(rp_eligible)
                    rp_failed.append({"error": exc.message})
                except Exception as exc:
                    logger.exception("apply.route_policy_unexpected_error", job_id=job_id)
                    err_payload = {"code": "internal", "message": repr(exc), "detail": {}}
                    for row in rp_eligible:
                        row.last_apply_error = err_payload
                    rp_outcome_failed = len(rp_eligible)
                    rp_failed.append({"error": repr(exc)})

            # ── Step 6g: OSPF intent pass ─────────────────────────────────────
            ospf_outcome_ok = 0
            ospf_outcome_failed = 0
            ospf_failed: list[dict] = []

            if ospf_instance_eligible or ospf_iface_eligible or redist_ospf:
                try:
                    await apply_ospf_config(
                        client=client,
                        device_name=device_name,
                        process_intent_rows=ospf_instance_eligible,
                        interface_intent_rows=ospf_iface_eligible,
                        redistribution_rows=redist_ospf,
                    )
                    for row in ospf_instance_eligible:
                        row.last_apply_at = now
                        row.last_apply_error = None
                    for row in ospf_iface_eligible:
                        row.last_apply_at = now
                        row.last_apply_error = None
                    for row in redist_ospf:
                        row.last_apply_at = now
                        row.last_apply_error = None
                    ospf_outcome_ok = len(ospf_instance_eligible) + len(ospf_iface_eligible) + len(redist_ospf)
                except NsoApplyError as exc:
                    logger.error(
                        "apply.ospf_failed",
                        job_id=job_id,
                        device=device_name,
                        error=exc.message,
                    )
                    err_payload = {"code": exc.code, "message": exc.message, "detail": exc.detail}
                    for row in ospf_instance_eligible:
                        row.last_apply_error = err_payload
                    for row in ospf_iface_eligible:
                        row.last_apply_error = err_payload
                    for row in redist_ospf:
                        row.last_apply_error = err_payload
                    ospf_outcome_failed = len(ospf_instance_eligible) + len(ospf_iface_eligible) + len(redist_ospf)
                    ospf_failed.append({"error": exc.message})
                except Exception as exc:
                    logger.exception("apply.ospf_unexpected_error", job_id=job_id)
                    err_payload = {"code": "internal", "message": repr(exc), "detail": {}}
                    for row in ospf_instance_eligible:
                        row.last_apply_error = err_payload
                    for row in ospf_iface_eligible:
                        row.last_apply_error = err_payload
                    for row in redist_ospf:
                        row.last_apply_error = err_payload
                    ospf_outcome_failed = len(ospf_instance_eligible) + len(ospf_iface_eligible) + len(redist_ospf)
                    ospf_failed.append({"error": repr(exc)})

            # ── Step 7: finalize job ─────────────────────────────────────────
            if (
                not eligible
                and not ip_eligible_by_iface
                and not snmp_has_eligible
                and not sr_eligible
                and not l2_eligible
                and not isis_eligible
                and not bgp_eligible
                and not rp_eligible
                and not ospf_instance_eligible
                and not ospf_iface_eligible
                and not redist_eligible
            ):
                logger.info("apply.nothing_eligible", job_id=job_id, device_id=device_id)
                job.status = JobStatus.succeeded
                job.result = {
                    "attribute_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
                    "ip_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
                    "snmp_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
                    "static_route_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
                    "l2_sap_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
                    "isis_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
                    "bgp_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
                    "route_policy_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
                    "ospf_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
                }
                await db.commit()
                return

            total_failed = (
                outcome_failed
                + ip_outcome_failed
                + snmp_outcome_failed
                + sr_outcome_failed
                + logging_outcome_failed
                + svi_outcome_failed
                + l2_outcome_failed
                + isis_outcome_failed
                + bgp_outcome_failed
                + rp_outcome_failed
                + ospf_outcome_failed
            )
            job.result = {
                "attribute_count_by_outcome": {
                    "in_sync": outcome_in_sync,
                    "apply_failed": outcome_failed,
                },
                "ip_count_by_outcome": {
                    "in_sync": ip_outcome_ok,
                    "apply_failed": ip_outcome_failed,
                },
                "snmp_count_by_outcome": {
                    "in_sync": snmp_outcome_ok,
                    "apply_failed": snmp_outcome_failed,
                },
                "static_route_count_by_outcome": {
                    "in_sync": sr_outcome_ok,
                    "apply_failed": sr_outcome_failed,
                },
                "logging_count_by_outcome": {
                    "in_sync": logging_outcome_ok,
                    "apply_failed": logging_outcome_failed,
                },
                "svi_count_by_outcome": {
                    "in_sync": svi_outcome_ok,
                    "apply_failed": svi_outcome_failed,
                },
                "l2_sap_count_by_outcome": {
                    "in_sync": l2_outcome_ok,
                    "apply_failed": l2_outcome_failed,
                },
                "isis_count_by_outcome": {
                    "in_sync": isis_outcome_ok,
                    "apply_failed": isis_outcome_failed,
                },
                "bgp_count_by_outcome": {
                    "in_sync": bgp_outcome_ok,
                    "apply_failed": bgp_outcome_failed,
                },
                "route_policy_count_by_outcome": {
                    "in_sync": rp_outcome_ok,
                    "apply_failed": rp_outcome_failed,
                },
                "ospf_count_by_outcome": {
                    "in_sync": ospf_outcome_ok,
                    "apply_failed": ospf_outcome_failed,
                },
            }
            if total_failed == 0:
                job.status = JobStatus.succeeded
            else:
                job.status = JobStatus.failed
                all_failed = (
                    [{"type": "attribute", **a} for a in failed_attrs]
                    + [{"type": "ip", **a} for a in failed_ips]
                    + [{"type": "snmp", **a} for a in snmp_failed]
                    + [{"type": "static_route", **a} for a in sr_failed]
                    + [{"type": "logging", **a} for a in logging_failed]
                    + [{"type": "svi", **a} for a in svi_failed]
                    + [{"type": "l2_sap", **a} for a in l2_failed]
                    + [{"type": "isis", **a} for a in isis_failed]
                    + [{"type": "bgp", **a} for a in bgp_failed]
                    + [{"type": "route_policy", **a} for a in rp_failed]
                    + [{"type": "ospf", **a} for a in ospf_failed]
                )
                job.error = {
                    "code": "nso_commit_failed",
                    "message": f"{total_failed} item(s) failed to apply",
                    "detail": {"items": all_failed},
                }
            await db.commit()
            logger.info(
                "apply.done",
                job_id=job_id,
                in_sync=outcome_in_sync,
                apply_failed=outcome_failed,
                ip_in_sync=ip_outcome_ok,
                ip_failed=ip_outcome_failed,
                snmp_in_sync=snmp_outcome_ok,
                snmp_failed=snmp_outcome_failed,
                sr_in_sync=sr_outcome_ok,
                sr_failed=sr_outcome_failed,
                l2_in_sync=l2_outcome_ok,
                l2_failed=l2_outcome_failed,
                isis_in_sync=isis_outcome_ok,
                isis_failed=isis_outcome_failed,
                bgp_in_sync=bgp_outcome_ok,
                bgp_failed=bgp_outcome_failed,
                rp_in_sync=rp_outcome_ok,
                rp_failed=rp_outcome_failed,
            )

        except Exception as exc:
            logger.exception("apply.unexpected_error", job_id=job_id, device_id=device_id)
            job.status = JobStatus.failed
            job.error = {"code": "internal", "message": repr(exc), "detail": {}}
            await db.commit()
