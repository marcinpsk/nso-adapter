# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Preflight (#1683), read-job discard and reduce-only authority retirement (#1613) for C9's cutover window.

A PARKED carrier (a key that no authorized positive row renders) loses its only payload source
when the legacy instances go, and seeding the key back would convert the owed deletion into adopted
intent, so the window refuses while one exists. After the window, the truthful authority record
is EMPTY: ``deauthorize_for_cutover`` only reduces authority and retires the provenance derived from it.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from typing import NamedTuple

import structlog
from sqlalchemy import MetaData, delete, exists, null, or_, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.core.claim import terminalize
from nso_adapter.core.generation import CROSSABLE_STATUSES, DEVICE_WRITING_JOB_TYPES
from nso_adapter.core.projection import projection_streams
from nso_adapter.store.models import (
    Base,
    DeploymentGeneration,
    DeviceProjectionStream,
    Job,
    JobStatus,
    JobType,
    StaticRouteIntent,
    StaticRouteTombstone,
    StreamPendingClear,
)

_RETIRE_SCHEMA = {
    # Clear authority, applied revisions, and the five-column preparation slot.
    "device_projection_stream": frozenset(
        "id device_id stream desired_revision authorized_revision applied_revision source_push_seq authorized_document prepared_revision prepared_tables prepared_deletions prepared_source_revision prepared_source_digest updated_at".split()
    ),
    # Clear deployed route identity and both pending-clear halves.
    "static_route_intent": frozenset(
        "id device_id route_id intent_generation deployed_key pending_clear vrf prefix next_hop interface_next_hop next_hop_vrf metric permanent tag name accepted_at last_apply_at last_apply_error".split()
    ),
    # Delete executable static-route removal carriers.
    "static_route_tombstone": frozenset(
        "id device_id route_id vrf prefix next_hop deployed_key marking job_id created_at".split()
    ),
    # Delete obsolete stream-clear obligations.
    "stream_pending_clear": frozenset("id device_id stream provenance revision recorded_at".split()),
}

_KEEP_SCHEMA = {
    # Desired intent survives for a later authorization.
    "bfd_intent": frozenset(
        "id device_id interface_name min_tx min_rx multiplier micro_bfd accepted_at last_apply_at last_apply_error".split()
    ),
    # Desired intent survives for a later authorization.
    "bgp_router_intent": frozenset("id device_id asn router_id accepted_at last_apply_at last_apply_error".split()),
    # Apply attempt history and responses are correlation only.
    "deployment_apply_attempt": frozenset(
        "id device_id selected admission_state http_status response created_at".split()
    ),
    # Settled or abandoned history cannot replay.
    "deployment_generation": frozenset(
        "id device_id seq mode status document digest allowed_removal_keys source_push_seq stream_revisions settlement_cohort removal_context apply_attempt_id job_id carrier_job_id carrier_job_status carrier_job_result carrier_job_error attempts last_error created_at updated_at settled_at".split()
    ),
    # Read mirror data does not authorize aggregate writes.
    "device_bfd_interface": frozenset(
        "id device_id interface_name bound_port min_tx min_rx multiplier micro_bfd enabled last_refreshed_at refresh_source".split()
    ),
    # Read mirror data does not authorize aggregate writes.
    "device_bgp_router": frozenset("id device_id asn router_id last_refreshed_at refresh_source".split()),
    # A settled claim is worker bookkeeping.
    "device_claim": frozenset("device_id claim_token purpose job_id acquired_at heartbeat_at".split()),
    # Management address observations do not authorize aggregate writes.
    "device_failover": frozenset(
        "id device_id primary_ip oob_ip active_address consecutive_failures consecutive_successes manual_override failback_blocked_reason oob_healthy oob_health_result oob_health_detail oob_health_checked_at last_probe_at last_probe_result last_probe_target last_probe_detail last_switch_at next_primary_probe_at next_oob_probe_at updated_at".split()
    ),
    # Keep generation sequences monotonic.
    "device_generation_counter": frozenset("device_id last_seq".split()),
    # Read mirror data does not authorize aggregate writes.
    "device_interface_mtu": frozenset(
        "id device_id interface_name mtu ip_mtu mpls_mtu bound_port last_refreshed_at refresh_source".split()
    ),
    # Read mirror data does not authorize aggregate writes.
    "device_isis_interface": frozenset(
        "id device_id interface_name af process_tag circuit_type network_type metric passive bound_port hello_auth_type hello_auth_present bfd_enabled frr_enabled frr_protection csnp_interval retransmit_interval lsp_interval mesh_group settings levels prefix_sids last_refreshed_at refresh_source".split()
    ),
    # Read mirror data does not authorize aggregate writes.
    "device_isis_process": frozenset(
        "id device_id process_tag net is_type metric_style overload_bit area_auth_type area_auth_present area_auth_key domain_auth_type domain_auth_present domain_auth_key spf_initial_wait spf_max_wait lsp_initial_wait lsp_max_wait lsp_lifetime lsp_refresh_interval lsp_mtu overload_on_startup overload_timeout te_enabled suppress_attached_bit ignore_attached_bit fast_reroute microloop_avoidance distance maximum_paths reference_bandwidth settings levels segment_routing_reported segment_routing_configured segment_routing flex_algos srv6_locators last_refreshed_at refresh_source".split()
    ),
    # Read mirror data does not authorize aggregate writes.
    "device_l2_sap": frozenset(
        "id device_id service_name service_type service_id sap_id port outer_tag inner_tag last_refreshed_at refresh_source".split()
    ),
    # Read mirror data does not authorize aggregate writes.
    "device_logging_host": frozenset(
        "id device_id address port severity facility transport vrf source last_refreshed_at refresh_source".split()
    ),
    # Read mirror data does not authorize aggregate writes.
    "device_logging_levels": frozenset(
        "id device_id console_severity monitor_severity module_severity last_refreshed_at refresh_source".split()
    ),
    # Read mirror data does not authorize aggregate writes.
    "device_ospf_instance": frozenset(
        "id device_id process_id router_id vrf areas enabled last_refreshed_at refresh_source".split()
    ),
    # Read mirror data does not authorize aggregate writes.
    "device_ospf_interface": frozenset(
        "id device_id interface_name process_id area_id passive priority cost network_type auth_type auth_present last_refreshed_at refresh_source".split()
    ),
    # Read mirror data does not authorize aggregate writes.
    "device_redistribution": frozenset(
        "id device_id dest_protocol dest_ref source_protocol source_ref route_map metric metric_type last_refreshed_at refresh_source".split()
    ),
    # Read mirror data does not authorize aggregate writes.
    "device_route_policy_as_path": frozenset("id device_id name content_hash last_refreshed_at refresh_source".split()),
    # Read mirror data does not authorize aggregate writes.
    "device_route_policy_community_list": frozenset(
        "id device_id name invert_match content_hash last_refreshed_at refresh_source".split()
    ),
    # Read mirror data does not authorize aggregate writes.
    "device_route_policy_prefix_list": frozenset(
        "id device_id name family content_hash last_refreshed_at refresh_source".split()
    ),
    # Read mirror data does not authorize aggregate writes.
    "device_route_policy_route_map": frozenset(
        "id device_id name content_hash last_refreshed_at refresh_source".split()
    ),
    # Operator settings do not carry device mutation provenance.
    "device_settings": frozenset("id device_id auto_apply sync_before_apply updated_at".split()),
    # Keep settlement sequences monotonic.
    "device_settle_counter": frozenset("device_id last_seq".split()),
    # Read mirror data does not authorize aggregate writes.
    "device_static_route": frozenset(
        "id device_id vrf prefix next_hop interface_next_hop next_hop_vrf metric permanent tag name last_refreshed_at refresh_source".split()
    ),
    # Read mirror data does not authorize aggregate writes.
    "device_subinterface": frozenset(
        "id device_id interface_name parent_interface dot1q_vlan sub_type vrf last_refreshed_at refresh_source".split()
    ),
    # Read mirror data does not authorize aggregate writes.
    "device_svi": frozenset(
        "id device_id interface_name vlan_id svi_type vrf last_refreshed_at refresh_source".split()
    ),
    # Read mirror data does not authorize aggregate writes.
    "device_switchport": frozenset(
        "id device_id interface_name mode untagged_vlan_id last_refreshed_at refresh_source".split()
    ),
    # Read mirror data does not authorize aggregate writes.
    "device_vlan": frozenset("id device_id vlan_id name last_refreshed_at refresh_source".split()),
    # A receipt correlates accepted pushes and preserves replay responses.
    "intent_push_receipt": frozenset(
        "id device_id section push_seq request_digest store_only delete_origin backfill_only response status_code generation_id created_at updated_at".split()
    ),
    # Read mirror data does not authorize aggregate writes.
    "interface_ip_address": frozenset(
        "id device_id interface_name address vrf family secondary bound_port last_refreshed_at refresh_source".split()
    ),
    # Desired intent survives for a later authorization.
    "interface_mtu_intent": frozenset(
        "id device_id interface_name mtu ip_mtu mpls_mtu accepted_at last_apply_at last_apply_error".split()
    ),
    # Read mirror and interface identity do not authorize aggregate writes.
    "interfaces": frozenset(
        "id device_id name netbox_interface_id nso_if_key parent_binding kind encap_tag vrf service".split()
    ),
    # Desired intent survives for a later authorization.
    "isis_flex_algo_intent": frozenset(
        "id device_id process_tag algo_id metric_type priority admin_group_exclude admin_group_include_any admin_group_include_all accepted_at last_apply_at last_apply_error".split()
    ),
    # Desired intent survives for a later authorization.
    "isis_interface_intent": frozenset(
        "id device_id interface_name af process_tag circuit_type network_type metric passive bfd_enabled frr_enabled frr_protection accepted_at last_apply_at last_apply_error".split()
    ),
    # Desired intent survives for a later authorization.
    "isis_level_intent": frozenset(
        "id device_id process_tag level wide_metrics_only labeled_preference disabled accepted_at last_apply_at last_apply_error".split()
    ),
    # Desired intent survives for a later authorization.
    "isis_process_intent": frozenset(
        "id device_id process_tag net is_type metric_style overload_bit area_auth_type area_auth_key domain_auth_type domain_auth_key fast_reroute microloop_avoidance accepted_at last_apply_at last_apply_error".split()
    ),
    # Terminal jobs are history; live jobs block the reset.
    "jobs": frozenset(
        "id job_type status coalescible device_id result error context created_at updated_at started_at heartbeat_at run_attempt settle_seq provision_attempt_id".split()
    ),
    # Desired intent survives for a later authorization.
    "l2_sap_intent": frozenset(
        "id device_id service_name service_type sap_id port outer_tag inner_tag accepted_at last_apply_at last_apply_error".split()
    ),
    # Read mirror data does not authorize aggregate writes.
    "lag_bundle_config": frozenset(
        "id device_id name lag_id min_links system_priority system_id timer admin_key vpc_sensitive last_refreshed_at refresh_source".split()
    ),
    # Desired intent survives for a later authorization.
    "lag_bundle_intent": frozenset(
        "id device_id name lag_id min_links system_priority system_id timer admin_key accepted_at last_apply_at last_apply_error".split()
    ),
    # Read mirror data does not authorize aggregate writes.
    "lag_interface": frozenset("id device_id name lag_id last_refreshed_at refresh_source".split()),
    # Desired intent survives for a later authorization.
    "logging_host_intent": frozenset(
        "id device_id address port severity facility transport vrf source accepted_at last_apply_at last_apply_error".split()
    ),
    # Desired intent survives for a later authorization.
    "logging_levels_intent": frozenset(
        "id device_id console_severity monitor_severity module_severity accepted_at last_apply_at last_apply_error".split()
    ),
    # Legacy scope settings do not authorize aggregate writes.
    "managed_scope": frozenset("id device_id attribute updated_at".split()),
    # Desired intent survives for a later authorization.
    "ospf_instance_intent": frozenset(
        "id device_id process_id router_id vrf areas enabled accepted_at last_apply_at last_apply_error".split()
    ),
    # Desired intent survives for a later authorization.
    "ospf_interface_intent": frozenset(
        "id device_id interface_name process_id area_id passive priority cost network_type auth_type auth_key accepted_at last_apply_at last_apply_error".split()
    ),
    # Desired intent survives for a later authorization.
    "redistribution_intent": frozenset(
        "id device_id dest_protocol dest_ref source_protocol source_ref route_map metric metric_type accepted_at last_apply_at last_apply_error".split()
    ),
    # Mirror diagnostics, including read_failures, do not authorize writes.
    "refresh_outcome": frozenset(
        "id device_id family refresh_source source_epoch read_outcome read_reason freshness read_failures started_at result succeeded row_count completed_at".split()
    ),
    # The mirror result pointer does not authorize writes.
    "refresh_outcome_pointer": frozenset("id device_id family attempt_id payload_revision updated_at".split()),
    # Desired intent survives for a later authorization.
    "route_policy_object_intent": frozenset(
        "id device_id family name entries invert_match accepted_at last_apply_at last_apply_error".split()
    ),
    # Read mirror data does not authorize aggregate writes.
    "snmp_community": frozenset("id device_id community_hash access acl last_refreshed_at refresh_source".split()),
    # Desired intent survives for a later authorization.
    "snmp_community_intent": frozenset(
        "id device_id label vault_ref access acl accepted_at last_apply_at last_apply_error".split()
    ),
    # Read mirror data does not authorize aggregate writes.
    "snmp_host": frozenset(
        "id device_id address version notify_type port username last_refreshed_at refresh_source".split()
    ),
    # Desired intent survives for a later authorization.
    "snmp_host_intent": frozenset(
        "id device_id address version notify_type community_or_user port accepted_at last_apply_at last_apply_error".split()
    ),
    # Desired or read-only state survives for later authorization.
    "snmp_system_info": frozenset("id device_id location contact last_refreshed_at refresh_source".split()),
    # Desired intent survives for a later authorization.
    "snmp_system_info_intent": frozenset(
        "id device_id location contact accepted_at last_apply_at last_apply_error".split()
    ),
    # Read mirror data does not authorize aggregate writes.
    "snmp_v3_user": frozenset(
        "id device_id username has_auth_secret has_priv_secret last_refreshed_at refresh_source".split()
    ),
    # Desired intent survives for a later authorization.
    "snmp_v3_user_intent": frozenset(
        "id device_id username group_name auth_protocol priv_protocol auth_vault_ref priv_vault_ref accepted_at last_apply_at last_apply_error".split()
    ),
    # Desired intent survives for a later authorization.
    "subinterface_intent": frozenset(
        "id device_id interface_name parent_interface dot1q_vlan sub_type vrf accepted_at last_apply_at last_apply_error".split()
    ),
    # Desired intent survives for a later authorization.
    "svi_intent": frozenset(
        "id device_id interface_name vlan_id svi_type vrf accepted_at last_apply_at last_apply_error".split()
    ),
    # Desired intent survives for a later authorization.
    "switchport_intent": frozenset(
        "id device_id interface_name mode untagged_vlan accepted_at last_apply_at last_apply_error".split()
    ),
    # Desired intent survives for a later authorization.
    "vlan_intent": frozenset("id device_id vlan_id name accepted_at last_apply_at last_apply_error".split()),
}


logger = structlog.get_logger(__name__)

#: Read over the AUTHORIZED fragment rather than live intent, so a store-only identity edit
#: cannot mask a carrier: only what an authorization froze counts as rendered. The one
#: departure from the design's literal SQL is the ``::jsonb`` cast, because
#: ``authorized_document`` is a ``json`` column and ``jsonb_array_elements`` needs ``jsonb``.
_PARKED_CARRIERS = text("""
WITH rendered AS (SELECT s.device_id, coalesce(r->>'vrf','') vrf, r->>'prefix' prefix, coalesce(r->>'next_hop','') next_hop
  FROM device_projection_stream s, LATERAL jsonb_array_elements(coalesce(s.authorized_document::jsonb->'static_route_intent','[]'::jsonb)) r
  WHERE s.stream='static_route' AND s.authorized_document IS NOT NULL)
SELECT t.device_id, t.id, t.vrf, t.prefix, t.next_hop, t.deployed_key FROM static_route_tombstone t
WHERE NOT EXISTS (SELECT 1 FROM rendered k WHERE k.device_id=t.device_id AND (k.vrf,k.prefix,k.next_hop)=(coalesce(t.vrf,''),t.prefix,coalesce(t.next_hop,'')))
   OR (t.deployed_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM rendered k WHERE k.device_id=t.device_id
       AND (k.vrf,k.prefix,k.next_hop)=(coalesce(t.deployed_key->>0,''),t.deployed_key->>1,coalesce(t.deployed_key->>2,''))))
ORDER BY t.device_id, t.id
""")


class ParkedCarrier(NamedTuple):
    """One carrier whose current or deployed key no authorized positive row renders."""

    device_id: int
    tombstone_id: int
    keys: tuple[tuple[str, str, str], ...]


class CutoverBlocked(RuntimeError):
    """The cutover window may not open: at least one device holds a parked carrier."""

    def __init__(self, parked: list[ParkedCarrier]):
        self.parked = parked
        named = ", ".join(
            f"device {carrier.device_id} tombstone {carrier.tombstone_id} has {len(carrier.keys)} parked key(s)"
            for carrier in parked
        )
        super().__init__(f"{len(parked)} static-route carrier(s) are parked and must drain first: {named}")


class CutoverStateBlocked(RuntimeError):
    """A generation or job can still execute its pre-cutover document."""

    def __init__(self, generations: list[tuple[int, int, str]], jobs: list[tuple[int | None, int, str]]):
        self.generations = generations
        self.jobs = jobs
        offenders = [
            f"device {device_id} generation {generation_id} ({status})"
            for device_id, generation_id, status in generations
        ]
        offenders.extend(f"device {device_id} job {job_id} ({status})" for device_id, job_id, status in jobs)
        super().__init__("cutover blocked by " + ", ".join(offenders))


class CutoverJobsBlocked(RuntimeError):
    """A live job is not a queued read, so the discard may not decide its outcome."""

    def __init__(self, jobs: list[tuple[int | None, int, str]]):
        self.jobs = jobs
        named = ", ".join(f"device {device_id} job {job_id} ({what})" for device_id, job_id, what in jobs)
        super().__init__(f"{len(jobs)} live job(s) are not discardable reads: {named}")


class CutoverSchemaBlocked(RuntimeError):
    """A device-scoped table or column has not been reviewed for the reset."""

    def __init__(self, table: str, column: str, *, missing: bool = False):
        self.table = table
        self.column = column
        reason = "missing reviewed" if missing else "unreviewed"
        super().__init__(f"cutover blocked by {reason} schema column {table}.{column}")


class DeviceCutoverReset(NamedTuple):
    """Counts of retired authority and obligations for one device."""

    device_id: int
    streams: int
    deployed_keys: int
    pending_clears: int
    tombstones: int
    stream_pending_clears: int


class CutoverReset(NamedTuple):
    """The post-cutover worklist for devices whose authority changed."""

    devices: tuple[DeviceCutoverReset, ...]


class DiscardedJob(NamedTuple):
    """One queued read job the window failed before the reset."""

    device_id: int
    job_id: int
    job_type: str


#: The success barrier's reads; provision is excluded because it creates the NSO device.
READ_JOB_TYPES = frozenset(JobType).difference(DEVICE_WRITING_JOB_TYPES, (JobType.provision,))

_DISCARDED_ERROR = {
    "code": "cutover_discarded",
    "message": "Queued read job discarded before the cutover authority reset",
    "detail": {},
}


def _refuse_unreviewed_schema(metadata: MetaData) -> None:
    """Check the hand-reviewed device table and column pin before any reset write."""
    reviewed = _RETIRE_SCHEMA | _KEEP_SCHEMA
    for table in metadata.tables.values():
        if "device_id" not in table.c:
            continue
        expected = reviewed.get(table.name)
        if expected is None:
            raise CutoverSchemaBlocked(table.name, "device_id")
        present = set(table.c.keys())
        if unreviewed := present - expected:
            raise CutoverSchemaBlocked(table.name, min(unreviewed))
        if missing := expected - present:
            raise CutoverSchemaBlocked(table.name, min(missing), missing=True)
    for name in reviewed.keys() - metadata.tables.keys():
        raise CutoverSchemaBlocked(name, "device_id", missing=True)


async def deauthorize_for_cutover(db: AsyncSession) -> CutoverReset:
    """Retire fleet authority and inherited obligations in the caller's transaction."""
    _refuse_unreviewed_schema(Base.metadata)
    # The device lock drains projection writers; table locks cover job and carrier writers.
    await db.execute(text("LOCK TABLE devices IN EXCLUSIVE MODE"))
    await db.execute(
        text(
            "LOCK TABLE deployment_generation, jobs, device_projection_stream, "
            "static_route_intent, static_route_tombstone, stream_pending_clear "
            "IN SHARE ROW EXCLUSIVE MODE"
        )
    )
    generations = (
        await db.execute(
            select(DeploymentGeneration.device_id, DeploymentGeneration.id, DeploymentGeneration.status)
            .where(DeploymentGeneration.status.not_in(CROSSABLE_STATUSES))
            .order_by(DeploymentGeneration.device_id, DeploymentGeneration.id)
        )
    ).all()
    jobs = (
        await db.execute(
            select(Job.device_id, Job.id, Job.status)
            .where(Job.status.in_((JobStatus.queued, JobStatus.running)))
            .order_by(Job.device_id, Job.id)
        )
    ).all()
    if generations or jobs:
        raise CutoverStateBlocked(
            [(device_id, generation_id, status.value) for device_id, generation_id, status in generations],
            [(device_id, job_id, status.value) for device_id, job_id, status in jobs],
        )
    parked = await parked_static_route_carriers(db)
    if parked:
        raise CutoverBlocked(parked)
    unknown_stream = (
        await db.execute(
            select(DeviceProjectionStream.device_id, DeviceProjectionStream.stream)
            .where(DeviceProjectionStream.stream.not_in(projection_streams()))
            .limit(1)
        )
    ).first()
    if unknown_stream is not None:
        raise CutoverSchemaBlocked("device_projection_stream", f"stream={unknown_stream.stream}")

    counts: defaultdict[int, Counter[str]] = defaultdict(Counter)
    changed_streams = await db.execute(
        update(DeviceProjectionStream)
        .where(
            DeviceProjectionStream.stream.in_(projection_streams()),
            or_(
                DeviceProjectionStream.authorized_document.is_not(None),
                DeviceProjectionStream.authorized_revision != 0,
                DeviceProjectionStream.applied_revision != 0,
                DeviceProjectionStream.prepared_revision.is_not(None),
                DeviceProjectionStream.prepared_tables.is_not(None),
                DeviceProjectionStream.prepared_deletions.is_not(None),
                DeviceProjectionStream.prepared_source_revision.is_not(None),
                DeviceProjectionStream.prepared_source_digest.is_not(None),
            ),
        )
        .values(
            authorized_document=null(),
            authorized_revision=0,
            applied_revision=0,
            prepared_revision=None,
            prepared_tables=null(),
            prepared_deletions=null(),
            prepared_source_revision=None,
            prepared_source_digest=None,
        )
        .returning(DeviceProjectionStream.device_id)
    )
    for (device_id,) in changed_streams:
        counts[device_id]["streams"] += 1
    for column, field in (
        (StaticRouteIntent.deployed_key, "deployed_keys"),
        (StaticRouteIntent.pending_clear, "pending_clears"),
    ):
        changed = await db.execute(
            update(StaticRouteIntent)
            .where(column.is_not(None))
            .values({column.key: null()})
            .returning(StaticRouteIntent.device_id)
        )
        for (device_id,) in changed:
            counts[device_id][field] += 1
    for model, field in ((StaticRouteTombstone, "tombstones"), (StreamPendingClear, "stream_pending_clears")):
        deleted = await db.execute(delete(model).returning(model.device_id))
        for (device_id,) in deleted:
            counts[device_id][field] += 1

    devices = tuple(
        DeviceCutoverReset(
            device_id,
            tally["streams"],
            tally["deployed_keys"],
            tally["pending_clears"],
            tally["tombstones"],
            tally["stream_pending_clears"],
        )
        for device_id, tally in sorted(counts.items())
    )
    logger.info(
        "cutover.authority_retired",
        devices=len(devices),
        streams=sum(device.streams for device in devices),
        deployed_keys=sum(device.deployed_keys for device in devices),
        pending_clears=sum(device.pending_clears for device in devices),
        tombstones=sum(device.tombstones for device in devices),
        stream_pending_clears=sum(device.stream_pending_clears for device in devices),
    )
    return CutoverReset(devices)


async def discard_queued_read_jobs(db: AsyncSession) -> tuple[DiscardedJob, ...]:
    """Fail every queued read job in the caller's transaction, or refuse while another job is live."""
    await db.execute(text("LOCK TABLE jobs IN SHARE ROW EXCLUSIVE MODE"))
    carries = exists().where(DeploymentGeneration.job_id == Job.id)
    live = (
        await db.execute(
            select(Job.device_id, Job.id, Job.job_type, Job.status, carries.label("carries"))
            .where(Job.status.in_((JobStatus.queued, JobStatus.running)))
            .order_by(Job.device_id, Job.id)
        )
    ).all()
    discard: list[DiscardedJob] = []
    blocked: list[tuple[int | None, int, str]] = []
    for device_id, job_id, job_type, status, carried in live:
        if status is JobStatus.queued and job_type in READ_JOB_TYPES and not carried:
            discard.append(DiscardedJob(device_id, job_id, job_type.value))
        else:
            carrying = " carrying a generation" if carried else ""
            blocked.append((device_id, job_id, f"{status.value} {job_type.value}{carrying}"))
    if blocked:
        raise CutoverJobsBlocked(blocked)
    for job in discard:
        written = await terminalize(
            db, job.job_id, status=JobStatus.failed, expect=JobStatus.queued, error=_DISCARDED_ERROR
        )
        if written is None:
            raise RuntimeError(f"job {job.job_id} left the queue under the jobs table lock")
    logger.info("cutover.read_jobs_discarded", jobs=len(discard))
    return tuple(discard)


async def parked_static_route_carriers(db: AsyncSession) -> list[ParkedCarrier]:
    """Return every carrier that would lose its payload source when the legacy instances go."""
    rows = (await db.execute(_PARKED_CARRIERS)).mappings().all()
    parked: list[ParkedCarrier] = []
    for row in rows:
        keys = [(row["vrf"] or "", row["prefix"] or "", row["next_hop"] or "")]
        deployed = row["deployed_key"]
        if deployed:
            key = (deployed[0] or "", deployed[1] or "", deployed[2] or "")
            if key not in keys:
                keys.append(key)
        parked.append(ParkedCarrier(row["device_id"], row["id"], tuple(keys)))
    return parked


async def refuse_cutover_while_carriers_are_parked(db: AsyncSession) -> None:
    """Run the drain preflight with workers stopped. Only an empty result opens the window."""
    parked = await parked_static_route_carriers(db)
    if parked:
        logger.error("cutover.drain_preflight_blocked", carriers=len(parked))
        raise CutoverBlocked(parked)
    logger.info("cutover.drain_preflight_clear")


__all__ = [
    "READ_JOB_TYPES",
    "CutoverBlocked",
    "CutoverJobsBlocked",
    "CutoverReset",
    "CutoverSchemaBlocked",
    "CutoverStateBlocked",
    "DeviceCutoverReset",
    "DiscardedJob",
    "ParkedCarrier",
    "deauthorize_for_cutover",
    "discard_queued_read_jobs",
    "parked_static_route_carriers",
    "refuse_cutover_while_carriers_are_parked",
]
