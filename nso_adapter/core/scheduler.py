# SPDX-License-Identifier: Apache-2.0
"""APScheduler setup — periodic sync poll + scope reconcile from NetBox plugin."""

from __future__ import annotations

from collections.abc import Callable
from typing import NamedTuple

import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from nso_adapter.config import get_config

logger = structlog.get_logger(__name__)
_scheduler: AsyncIOScheduler | None = None


async def _scheduled_sync_all() -> None:
    """Sync every device that has at least one attribute in scope.

    Enqueues a ``queued`` sync job per device; the durable worker pool
    (``core.worker``) drains them.  ``enqueue_job`` enforces the one-per-device
    constraint, so a device whose previous job is still queued/running is skipped.
    """
    from sqlalchemy import select

    from nso_adapter.core.jobs import enqueue_job
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device, JobType, ManagedScope

    async for db in get_session():
        result = await db.execute(select(Device).where(Device.id.in_(select(ManagedScope.device_id).distinct())))
        devices = result.scalars().all()
        for device in devices:
            try:
                job, created = await enqueue_job(device.id, JobType.sync, db)
                if created:
                    logger.info("scheduler.sync_enqueued", device_id=device.id, job_id=job.id)
                else:
                    logger.debug(
                        "scheduler.sync_skipped",
                        device_id=device.id,
                        reason="job_already_active",
                        job_id=job.id,
                    )
            except Exception as exc:
                logger.error("scheduler.sync_error", device_id=device.id, error=repr(exc))


async def _scheduled_scope_reconcile() -> None:
    """Self-healing path: reconcile managed scope from the NetBox plugin model.

    Aborts on any error — never interpret a NetBox outage as "everything deleted".
    On success, offboards devices present in the adapter but absent from the plugin.
    """
    from sqlalchemy import select

    from nso_adapter.bindings.netbox.scope import fetch_all_scope
    from nso_adapter.core.failover import upsert_failover_ips
    from nso_adapter.core.importer import get_netbox_client
    from nso_adapter.core.onboarding import offboard_device, set_scope
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device

    nb_client = get_netbox_client()
    if nb_client is None:
        logger.debug("scheduler.scope_reconcile.skipped", reason="no_netbox_client")
        return

    try:
        plugin_records = await fetch_all_scope(nb_client)
    except Exception as exc:
        logger.warning("scheduler.scope_reconcile.failed", error=repr(exc))
        return  # abort wholesale — do not interpret outage as empty scope

    plugin_by_nb_id = {r.netbox_device_id: r for r in plugin_records}

    async for db in get_session():
        # Select ids only (not ORM rows): the per-device commit/rollback below would expire
        # pre-loaded Device instances, and touching an expired attr later triggers a lazy
        # load that fails in the async greenlet context. Re-fetch a fresh row per iteration.
        device_ids = (await db.execute(select(Device.id).where(Device.netbox_device_id.is_not(None)))).scalars().all()

        for device_id in device_ids:
            # Isolate + commit per device: set_scope/upsert_failover_ips document "caller
            # commits" (and get_session never commits on exit), so each device's scope +
            # primary/OOB IPs must be committed here or they're silently discarded. Per-device
            # so one device raising (FK/constraint) can't abort the tick and skip every later
            # device — roll its partial/poisoned work back and carry on with the rest.
            try:
                device = await db.get(Device, device_id)
                if device is None:
                    continue
                plugin_rec = plugin_by_nb_id.get(device.netbox_device_id)
                if plugin_rec is None:
                    logger.warning(
                        "scheduler.scope_reconcile.offboarding",
                        device_id=device.id,
                        netbox_device_id=device.netbox_device_id,
                    )
                    await offboard_device(db, device)
                else:
                    await set_scope(db, device, plugin_rec.attributes)
                    await upsert_failover_ips(db, device, plugin_rec.primary_ip, plugin_rec.oob_ip)
                await db.commit()
            except Exception as exc:
                logger.warning("scheduler.scope_reconcile.device_failed", device_id=device_id, error=repr(exc))
                await db.rollback()


async def _scheduled_intent_reconcile() -> None:
    """Self-healing path: reconcile the adapter's intent mirror from the NetBox plugin.

    Reads /api/plugins/nso/interface-state/ for all accepted records and
    replaces the adapter's InterfaceIntent store accordingly (full replace
    per device, matching the PUT /intent semantics).

    Aborts on any error — never interpret a NetBox outage as "no intent".
    """
    from datetime import UTC, datetime

    from sqlalchemy import select

    from nso_adapter.bindings.netbox.intent import fetch_all_intent
    from nso_adapter.core.importer import get_netbox_client
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import DbInterface, Device, InterfaceIntent

    nb_client = get_netbox_client()
    if nb_client is None:
        logger.debug("scheduler.intent_reconcile.skipped", reason="no_netbox_client")
        return

    try:
        intent_records = await fetch_all_intent(nb_client)
    except Exception as exc:
        logger.warning("scheduler.intent_reconcile.failed", error=repr(exc))
        return  # abort wholesale

    # Group by netbox_device_id
    by_nb_device: dict[int, list] = {}
    for rec in intent_records:
        by_nb_device.setdefault(rec.netbox_device_id, []).append(rec)

    async for db in get_session():
        # ids only + re-fetch per iteration: the per-device commit/rollback expires pre-loaded
        # ORM rows, and a later expired-attr access does a lazy load that fails under asyncio.
        device_ids = (await db.execute(select(Device.id).where(Device.netbox_device_id.is_not(None)))).scalars().all()

        for device_id in device_ids:
            # Isolate + commit per device so one device raising (FK/constraint) can't abort the
            # tick and skip every later device — roll its partial work back and carry on.
            try:
                device = await db.get(Device, device_id)
                if device is None:
                    continue
                records = by_nb_device.get(device.netbox_device_id, [])

                # Load all interfaces for this device
                ifaces_result = await db.execute(select(DbInterface).where(DbInterface.device_id == device.id))
                iface_by_name = {i.name: i for i in ifaces_result.scalars().all()}

                # Full-replace: delete all existing intent rows for this device
                existing = await db.execute(
                    select(InterfaceIntent).where(
                        InterfaceIntent.interface_id.in_([i.id for i in iface_by_name.values()])
                    )
                )
                for row in existing.scalars().all():
                    await db.delete(row)
                await db.flush()

                now = datetime.now(UTC).replace(tzinfo=None)
                count = 0
                for rec in records:
                    iface = iface_by_name.get(rec.interface_name)
                    if iface is None:
                        logger.debug(
                            "scheduler.intent_reconcile.unknown_interface",
                            device_id=device.id,
                            interface=rec.interface_name,
                        )
                        continue
                    value = str(rec.intent_value) if rec.intent_value is not None else None
                    db.add(
                        InterfaceIntent(
                            interface_id=iface.id,
                            attribute=rec.attribute,
                            intent_value=value,
                            accepted_at=rec.accepted_at or now,
                        )
                    )
                    count += 1

                if count > 0:
                    logger.info(
                        "scheduler.intent_reconcile.updated",
                        device_id=device.id,
                        attribute_count=count,
                    )
                await db.commit()
            except Exception as exc:
                logger.warning("scheduler.intent_reconcile.device_failed", device_id=device.id, error=repr(exc))
                await db.rollback()


async def _scheduled_lag_topology_refresh() -> None:
    """Periodic fallback: refresh LAG topology for all managed devices."""
    from sqlalchemy import select

    from nso_adapter.core.importer import get_nso_client
    from nso_adapter.core.lag_topology import refresh_lag_topology_for_device
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device

    async for db in get_session():
        result = await db.execute(select(Device).where(Device.nso_device_name.is_not(None)))
        devices = result.scalars().all()
        for device in devices:
            try:
                nso_client = get_nso_client(device.nso_instance)
            except RuntimeError:
                logger.debug(
                    "scheduler.lag_topology.skipped",
                    device_id=device.id,
                    reason="no_nso_client",
                    instance=device.nso_instance,
                )
                continue
            await refresh_lag_topology_for_device(db, device, nso_client, refresh_source="poll")


async def _scheduled_lag_config_refresh() -> None:
    """Periodic fallback: refresh LAG config for all managed devices."""
    from sqlalchemy import select

    from nso_adapter.core.importer import get_nso_client
    from nso_adapter.core.lag_config import refresh_lag_config_for_device
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device

    async for db in get_session():
        result = await db.execute(select(Device).where(Device.nso_device_name.is_not(None)))
        devices = result.scalars().all()
        for device in devices:
            try:
                nso_client = get_nso_client(device.nso_instance)
            except RuntimeError:
                logger.debug(
                    "scheduler.lag_config.skipped",
                    device_id=device.id,
                    reason="no_nso_client",
                    instance=device.nso_instance,
                )
                continue
            await refresh_lag_config_for_device(db, device, nso_client, refresh_source="poll")


async def _scheduled_l2_service_refresh() -> None:
    """Periodic fallback: refresh Nokia L2 services (epipe/vpls + SAPs) for all managed devices."""
    from sqlalchemy import select

    from nso_adapter.core.importer import get_nso_client
    from nso_adapter.core.l2_service import refresh_l2_services_for_device
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device

    async for db in get_session():
        result = await db.execute(select(Device).where(Device.nso_device_name.is_not(None)))
        devices = result.scalars().all()
        for device in devices:
            try:
                nso_client = get_nso_client(device.nso_instance)
            except RuntimeError:
                logger.debug(
                    "scheduler.l2_service.skipped",
                    device_id=device.id,
                    reason="no_nso_client",
                    instance=device.nso_instance,
                )
                continue
            await refresh_l2_services_for_device(db, device, nso_client, refresh_source="poll")


async def _scheduled_interface_ip_refresh() -> None:
    """Periodic fallback: refresh interface IP addresses for all managed devices."""
    from sqlalchemy import select

    from nso_adapter.core.importer import get_nso_client
    from nso_adapter.core.interface_ip import refresh_interface_ips_for_device
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device

    async for db in get_session():
        result = await db.execute(select(Device).where(Device.nso_device_name.is_not(None)))
        devices = result.scalars().all()
        for device in devices:
            try:
                nso_client = get_nso_client(device.nso_instance)
            except RuntimeError:
                logger.debug(
                    "scheduler.interface_ip.skipped",
                    device_id=device.id,
                    reason="no_nso_client",
                    instance=device.nso_instance,
                )
                continue
            await refresh_interface_ips_for_device(db, device, nso_client, refresh_source="poll")


async def _scheduled_static_route_refresh() -> None:
    """Periodic fallback: refresh static routes for all managed devices."""
    from sqlalchemy import select

    from nso_adapter.core.importer import get_nso_client
    from nso_adapter.core.static_route import refresh_static_routes_for_device
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device

    async for db in get_session():
        result = await db.execute(select(Device).where(Device.nso_device_name.is_not(None)))
        devices = result.scalars().all()
        for device in devices:
            try:
                nso_client = get_nso_client(device.nso_instance)
            except RuntimeError:
                logger.debug(
                    "scheduler.static_route.skipped",
                    device_id=device.id,
                    reason="no_nso_client",
                    instance=device.nso_instance,
                )
                continue
            await refresh_static_routes_for_device(db, device, nso_client, refresh_source="poll")


async def _scheduled_isis_refresh() -> None:
    """Periodic fallback: refresh IS-IS interfaces for all managed devices."""
    from sqlalchemy import select

    from nso_adapter.core.importer import get_nso_client
    from nso_adapter.core.isis import refresh_isis_interfaces_for_device
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device

    async for db in get_session():
        result = await db.execute(select(Device).where(Device.nso_device_name.is_not(None)))
        devices = result.scalars().all()
        for device in devices:
            try:
                nso_client = get_nso_client(device.nso_instance)
            except RuntimeError:
                logger.debug(
                    "scheduler.isis.skipped",
                    device_id=device.id,
                    reason="no_nso_client",
                    instance=device.nso_instance,
                )
                continue
            await refresh_isis_interfaces_for_device(db, device, nso_client, refresh_source="poll")


async def _scheduled_bgp_refresh() -> None:
    """Periodic fallback: refresh BGP config for all managed devices."""
    from sqlalchemy import select

    from nso_adapter.core.bgp import refresh_bgp_config_for_device
    from nso_adapter.core.importer import get_nso_client
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device

    async for db in get_session():
        result = await db.execute(select(Device).where(Device.nso_device_name.is_not(None)))
        devices = result.scalars().all()
        for device in devices:
            try:
                nso_client = get_nso_client(device.nso_instance)
            except RuntimeError:
                logger.debug(
                    "scheduler.bgp.skipped",
                    device_id=device.id,
                    reason="no_nso_client",
                    instance=device.nso_instance,
                )
                continue
            await refresh_bgp_config_for_device(db, device, nso_client, refresh_source="poll")


async def _scheduled_ospf_refresh() -> None:
    """Periodic fallback: refresh OSPF config for all managed devices."""
    from sqlalchemy import select

    from nso_adapter.core.importer import get_nso_client
    from nso_adapter.core.ospf import refresh_ospf_for_device
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device

    async for db in get_session():
        result = await db.execute(select(Device).where(Device.nso_device_name.is_not(None)))
        devices = result.scalars().all()
        for device in devices:
            try:
                nso_client = get_nso_client(device.nso_instance)
            except RuntimeError:
                logger.debug(
                    "scheduler.ospf.skipped",
                    device_id=device.id,
                    reason="no_nso_client",
                    instance=device.nso_instance,
                )
                continue
            await refresh_ospf_for_device(db, device, nso_client, refresh_source="poll")


async def _scheduled_redistribution_refresh() -> None:
    """Periodic fallback: refresh redistribution config for all managed devices."""
    from sqlalchemy import select

    from nso_adapter.core.importer import get_nso_client
    from nso_adapter.core.redistribution import refresh_redistribution_for_device
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device

    async for db in get_session():
        result = await db.execute(select(Device).where(Device.nso_device_name.is_not(None)))
        devices = result.scalars().all()
        for device in devices:
            try:
                nso_client = get_nso_client(device.nso_instance)
            except RuntimeError:
                logger.debug(
                    "scheduler.redistribution.skipped",
                    device_id=device.id,
                    reason="no_nso_client",
                    instance=device.nso_instance,
                )
                continue
            await refresh_redistribution_for_device(db, device, nso_client, refresh_source="poll")


async def _scheduled_snmp_refresh() -> None:
    """Periodic fallback: refresh SNMP config for all managed devices.

    SNMP otherwise only refreshes on an SSE config-change event, so without this
    the mirror never populates/self-heals on a device that hasn't changed.
    """
    from sqlalchemy import select

    from nso_adapter.core.importer import get_nso_client
    from nso_adapter.core.snmp import refresh_snmp_config_for_device
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device

    async for db in get_session():
        result = await db.execute(select(Device).where(Device.nso_device_name.is_not(None)))
        devices = result.scalars().all()
        for device in devices:
            try:
                nso_client = get_nso_client(device.nso_instance)
            except RuntimeError:
                logger.debug(
                    "scheduler.snmp.skipped",
                    device_id=device.id,
                    reason="no_nso_client",
                    instance=device.nso_instance,
                )
                continue
            await refresh_snmp_config_for_device(db, device, nso_client, refresh_source="poll")


async def _scheduled_logging_refresh() -> None:
    """Periodic fallback: refresh logging/syslog config for all managed devices."""
    from sqlalchemy import select

    from nso_adapter.core.importer import get_nso_client
    from nso_adapter.core.logging_config import refresh_logging_config_for_device
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device

    async for db in get_session():
        result = await db.execute(select(Device).where(Device.nso_device_name.is_not(None)))
        for device in result.scalars().all():
            try:
                nso_client = get_nso_client(device.nso_instance)
            except RuntimeError:
                continue
            await refresh_logging_config_for_device(db, device, nso_client, refresh_source="poll")


async def _scheduled_route_policy_refresh() -> None:
    """Periodic fallback: refresh route-policy objects for all managed devices."""
    from sqlalchemy import select

    from nso_adapter.core.importer import get_nso_client
    from nso_adapter.core.route_policy import refresh_route_policy_for_device
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device

    async for db in get_session():
        result = await db.execute(select(Device).where(Device.nso_device_name.is_not(None)))
        devices = result.scalars().all()
        for device in devices:
            try:
                nso_client = get_nso_client(device.nso_instance)
            except RuntimeError:
                logger.debug(
                    "scheduler.route_policy.skipped",
                    device_id=device.id,
                    reason="no_nso_client",
                    instance=device.nso_instance,
                )
                continue
            await refresh_route_policy_for_device(db, device, nso_client, refresh_source="poll")


async def _scheduled_capability_refresh() -> None:
    """Daily: refresh the route-policy capability matrix (representable half) for the fleet.

    Probes each managed device; identical (ned_id, sw_version) boxes upsert the same rows
    (idempotent). The apply-failed hook keeps the accepted half current between probes.
    """
    from sqlalchemy import select

    from nso_adapter.core.capability import refresh_device_capability
    from nso_adapter.core.importer import get_nso_client
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device

    async for db in get_session():
        devices = (await db.execute(select(Device).where(Device.nso_device_name.is_not(None)))).scalars().all()
        for device in devices:
            try:
                nso_client = get_nso_client(device.nso_instance)
            except RuntimeError:
                continue
            try:
                await refresh_device_capability(db, nso_client, device.nso_device_name, device)
            except Exception as exc:  # noqa: BLE001 — one device must not abort the fleet refresh
                logger.warning("scheduler.capability.failed", device_id=device.id, error=str(exc))


async def _refresh_all_devices(refresh_fn, label: str) -> None:
    """Run *refresh_fn(db, device, nso_client, refresh_source='poll')* for every NSO device.

    Shared body for the L2/L3 interface-family poll jobs (VLAN-db + switchport,
     SVI/IRB, dot1q subinterface), which would otherwise be byte-for-byte
    copies of each other.
    """
    from sqlalchemy import select

    from nso_adapter.core.importer import get_nso_client
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device

    async for db in get_session():
        result = await db.execute(select(Device).where(Device.nso_device_name.is_not(None)))
        for device in result.scalars().all():
            try:
                nso_client = get_nso_client(device.nso_instance)
            except RuntimeError:
                logger.debug(
                    f"scheduler.{label}.skipped",
                    device_id=device.id,
                    reason="no_nso_client",
                    instance=device.nso_instance,
                )
                continue
            await refresh_fn(db, device, nso_client, refresh_source="poll")


async def _scheduled_vlan_refresh() -> None:
    """Periodic fallback: refresh the VLAN database for all managed devices."""
    from nso_adapter.core.vlan import refresh_vlan_database_for_device

    await _refresh_all_devices(refresh_vlan_database_for_device, "vlan")


async def _scheduled_switchport_refresh() -> None:
    """Periodic fallback: refresh L2 switchport config for all managed devices."""
    from nso_adapter.core.vlan import refresh_switchport_for_device

    await _refresh_all_devices(refresh_switchport_for_device, "switchport")


async def _scheduled_svi_refresh() -> None:
    """Periodic fallback: refresh SVIs/IRBs for all managed devices."""
    from nso_adapter.core.svi import refresh_svi_for_device

    await _refresh_all_devices(refresh_svi_for_device, "svi")


async def _scheduled_subinterface_refresh() -> None:
    """Periodic fallback: refresh dot1q subinterfaces for all managed devices."""
    from nso_adapter.core.subinterface import refresh_subinterface_for_device

    await _refresh_all_devices(refresh_subinterface_for_device, "subinterface")


async def _scheduled_interface_mtu_refresh() -> None:
    """Periodic fallback: refresh per-interface MTU for all managed devices (Phase 2b)."""
    from nso_adapter.core.interface_mtu import refresh_interface_mtu_for_device

    await _refresh_all_devices(refresh_interface_mtu_for_device, "interface_mtu")


async def _scheduled_topology_interfaces_refresh() -> None:
    """Ensure NetBox holds the LAG/channel/loopback interfaces bound_port correlation needs.

    A periodic reconcile for the interfaces the cfg.port feed never creates.

    Reads the adapter mirror (IS-IS / interface-IP / lag-topology, already
    refreshed by their own jobs) plus the attribute-sync DbInterface rows. Runs
    on its own interval so the source tables are populated first; idempotent.
    """
    from sqlalchemy import select

    from nso_adapter.core.importer import get_netbox_client
    from nso_adapter.core.topology_interfaces import ensure_topology_interfaces
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device

    nb_client = get_netbox_client()
    if nb_client is None:
        logger.debug("scheduler.topology_interfaces.skipped", reason="no_netbox_client")
        return

    async for db in get_session():
        result = await db.execute(select(Device).where(Device.netbox_device_id.is_not(None)))
        devices = result.scalars().all()
        for device in devices:
            try:
                await ensure_topology_interfaces(db, device, nb_client)
            except Exception as exc:
                logger.error("scheduler.topology_interfaces.error", device_id=device.id, error=repr(exc))


# Forward jitter applied to each probe's next due-time (fraction of the interval) so the fleet
# de-aligns instead of probing in lockstep. See the perf-spike writeup.
_FAILOVER_JITTER_FRACTION = 0.15


def _utcnow_naive():
    from datetime import UTC, datetime

    return datetime.now(UTC).replace(tzinfo=None)


async def _due_failover_device_ids(db, now) -> list[int]:
    """Device IDs whose primary- or OOB-probe is due (linked + plugin-sourced primary IP).

    Pre-filtering in SQL keeps the tick from spinning up a session/task per not-due device.
    """
    from sqlalchemy import and_, or_, select

    from nso_adapter.store.models import Device, DeviceFailover

    stmt = (
        select(Device.id)
        .join(DeviceFailover, DeviceFailover.device_id == Device.id)
        .where(
            Device.netbox_device_id.is_not(None),
            DeviceFailover.primary_ip.is_not(None),
            or_(
                DeviceFailover.next_primary_probe_at.is_(None),
                DeviceFailover.next_primary_probe_at <= now,
                and_(
                    DeviceFailover.oob_ip.is_not(None),
                    or_(
                        DeviceFailover.next_oob_probe_at.is_(None),
                        DeviceFailover.next_oob_probe_at <= now,
                    ),
                ),
            ),
        )
    )
    return list((await db.execute(stmt)).scalars().all())


async def _probe_one_failover_device(device_id: int, eff, now, flip_budget, sem) -> None:
    """Probe one device on its own DB session (safe under concurrency), bounded by *sem*.

    Each task owns its AsyncSession + commit so the gather'd probes don't share session state;
    the shared *flip_budget* caps disruptive flips across the whole tick.
    """
    from sqlalchemy import select

    from nso_adapter.core.failover import run_failover_tick
    from nso_adapter.core.importer import get_nso_client
    from nso_adapter.core.jobs import get_active_job
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device, DeviceFailover

    async with sem:
        async for db in get_session():
            pair = (
                await db.execute(
                    select(Device, DeviceFailover)
                    .join(DeviceFailover, DeviceFailover.device_id == Device.id)
                    .where(Device.id == device_id)
                )
            ).first()
            if pair is None:
                return
            device, fo = pair
            try:
                nso_client = get_nso_client(device.nso_instance)
            except RuntimeError:
                logger.debug("scheduler.failover.skipped", device_id=device_id, reason="no_nso_client")
                return
            try:
                active_job = await get_active_job(device.id, db)
                await run_failover_tick(
                    device,
                    fo,
                    nso_client,
                    eff,
                    now=now,
                    job_active=active_job is not None,
                    flip_budget=flip_budget,
                    jitter_fraction=_FAILOVER_JITTER_FRACTION,
                )
                await db.commit()
            except Exception as exc:
                await db.rollback()
                logger.warning("scheduler.failover.error", device_id=device_id, error=repr(exc))
            return


async def _scheduled_failover_probe() -> None:
    """Mgmt-IP failover base tick: probe due linked devices, switch primary↔OOB with hysteresis.

    Reads the live FailoverConfig each run (so a plugin settings change applies on the next tick
    without rescheduling APScheduler). Probes due devices CONCURRENTLY under a semaphore — the
    perf-spike load lever, since an unreachable connect blocks ~probe_timeout — and caps disruptive
    flips per tick via a shared budget. ``enabled=False`` makes the tick a no-op (live off-switch).
    """
    import asyncio

    from nso_adapter.core.failover import FlipBudget, get_effective_failover_config
    from nso_adapter.store.db import get_session

    cfg = get_config().scheduler
    now = _utcnow_naive()
    eff = None
    due_ids: list[int] = []
    async for db in get_session():
        eff = await get_effective_failover_config(db, cfg)
        if eff.enabled:
            due_ids = await _due_failover_device_ids(db, now)
        break
    if eff is None or not eff.enabled or not due_ids:
        return

    flip_budget = FlipBudget(eff.max_flips_per_tick)
    sem = asyncio.Semaphore(eff.probe_concurrency)
    await asyncio.gather(*(_probe_one_failover_device(did, eff, now, flip_budget, sem) for did in due_ids))


class _JobSpec(NamedTuple):
    """One periodic job's registration rule.

    ``enable_attr`` None → no enable flag (always-on jobs). ``gate_on_interval``
    True → only register when the resolved interval is > 0 (lets an interval of 0
    disable a job that has no dedicated enable flag).
    """

    fn: Callable
    job_id: str
    interval_attr: str
    enable_attr: str | None
    gate_on_interval: bool


# Declarative registry of every periodic job. Order = registration order.
_JOB_SPECS: tuple[_JobSpec, ...] = (
    _JobSpec(_scheduled_sync_all, "sync_all_devices", "poll_interval", None, False),
    _JobSpec(_scheduled_scope_reconcile, "scope_reconcile", "scope_reconcile_interval", None, False),
    _JobSpec(_scheduled_intent_reconcile, "intent_reconcile", "scope_reconcile_interval", None, False),
    _JobSpec(_scheduled_lag_topology_refresh, "lag_topology_refresh", "lag_topology_poll_interval", None, True),
    _JobSpec(_scheduled_lag_config_refresh, "lag_config_refresh", "lag_config_poll_interval", None, True),
    _JobSpec(
        _scheduled_interface_ip_refresh,
        "interface_ip_refresh",
        "interface_ip_poll_interval",
        "enable_interface_ip_sync",
        True,
    ),
    _JobSpec(
        _scheduled_static_route_refresh,
        "static_route_refresh",
        "static_route_poll_interval",
        "enable_static_routing_sync",
        True,
    ),
    _JobSpec(_scheduled_isis_refresh, "isis_refresh", "isis_poll_interval", "enable_isis_sync", True),
    _JobSpec(_scheduled_bgp_refresh, "bgp_refresh", "bgp_poll_interval", "enable_bgp_sync", True),
    _JobSpec(_scheduled_ospf_refresh, "ospf_refresh", "ospf_poll_interval", "enable_ospf_sync", True),
    _JobSpec(
        _scheduled_redistribution_refresh,
        "redistribution_refresh",
        "redistribution_poll_interval",
        "enable_redistribution_sync",
        True,
    ),
    _JobSpec(_scheduled_snmp_refresh, "snmp_refresh", "snmp_poll_interval", "enable_snmp_sync", True),
    _JobSpec(_scheduled_logging_refresh, "logging_refresh", "logging_poll_interval", "enable_logging_sync", True),
    _JobSpec(
        _scheduled_l2_service_refresh, "l2_service_refresh", "l2_service_poll_interval", "enable_l2_service_sync", True
    ),
    _JobSpec(_scheduled_vlan_refresh, "vlan_refresh", "vlan_poll_interval", "enable_vlan_sync", True),
    _JobSpec(
        _scheduled_switchport_refresh, "switchport_refresh", "switchport_poll_interval", "enable_switchport_sync", True
    ),
    _JobSpec(_scheduled_svi_refresh, "svi_refresh", "svi_poll_interval", "enable_svi_sync", True),
    _JobSpec(
        _scheduled_subinterface_refresh,
        "subinterface_refresh",
        "subinterface_poll_interval",
        "enable_subinterface_sync",
        True,
    ),
    _JobSpec(
        _scheduled_interface_mtu_refresh,
        "interface_mtu_refresh",
        "interface_mtu_poll_interval",
        "enable_interface_mtu_sync",
        True,
    ),
    _JobSpec(
        _scheduled_route_policy_refresh,
        "route_policy_refresh",
        "route_policy_poll_interval",
        "enable_route_policy_sync",
        True,
    ),
    _JobSpec(
        _scheduled_capability_refresh,
        "capability_refresh",
        "capability_refresh_interval",
        "enable_capability_refresh",
        True,
    ),
    _JobSpec(
        _scheduled_topology_interfaces_refresh,
        "topology_interfaces_refresh",
        "topology_interface_poll_interval",
        "enable_topology_interface_sync",
        True,
    ),
    _JobSpec(_scheduled_failover_probe, "failover_probe", "failover_base_tick", "enable_failover", True),
)


def start_scheduler() -> None:
    global _scheduler
    cfg = get_config()
    # Explicit job defaults (not the version-dependent APScheduler ones): never run the same
    # fleet refresh concurrently (max_instances=1); collapse a backlog of missed fires into a
    # single run (coalesce); and run a late/missed fire whenever the slot frees rather than
    # silently dropping it after the default 1s grace (misfire_grace_time=None).
    _scheduler = AsyncIOScheduler(job_defaults={"coalesce": True, "max_instances": 1, "misfire_grace_time": None})
    for spec in _JOB_SPECS:
        interval = getattr(cfg.scheduler, spec.interval_attr)
        enabled = spec.enable_attr is None or getattr(cfg.scheduler, spec.enable_attr)
        if enabled and (not spec.gate_on_interval or interval > 0):
            _scheduler.add_job(spec.fn, "interval", minutes=interval, id=spec.job_id)
    _scheduler.start()
    logger.info("scheduler.started")


def stop_scheduler() -> None:
    global _scheduler
    if _scheduler:
        # wait=False: don't block shutdown/reload on an in-flight sync (can take
        # ~30s). The process is going down; the lifespan's orphaned-job cleanup
        # marks any interrupted job as failed on the next start.
        _scheduler.shutdown(wait=False)
        _scheduler = None
