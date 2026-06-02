# SPDX-License-Identifier: Apache-2.0
"""APScheduler setup — periodic sync poll + scope reconcile from NetBox plugin."""

from __future__ import annotations

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
        result = await db.execute(select(Device).where(Device.netbox_device_id.is_not(None)))
        devices = result.scalars().all()

        for device in devices:
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
        result = await db.execute(select(Device).where(Device.netbox_device_id.is_not(None)))
        devices = result.scalars().all()

        for device in devices:
            records = by_nb_device.get(device.netbox_device_id, [])

            # Load all interfaces for this device
            ifaces_result = await db.execute(select(DbInterface).where(DbInterface.device_id == device.id))
            iface_by_name = {i.name: i for i in ifaces_result.scalars().all()}

            # Full-replace: delete all existing intent rows for this device
            existing = await db.execute(
                select(InterfaceIntent).where(InterfaceIntent.interface_id.in_([i.id for i in iface_by_name.values()]))
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
                await db.flush()
                logger.info(
                    "scheduler.intent_reconcile.updated",
                    device_id=device.id,
                    attribute_count=count,
                )

        await db.commit()


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


def start_scheduler() -> None:
    global _scheduler
    cfg = get_config()
    _scheduler = AsyncIOScheduler()
    _scheduler.add_job(
        _scheduled_sync_all,
        "interval",
        minutes=cfg.scheduler.poll_interval,
        id="sync_all_devices",
    )
    _scheduler.add_job(
        _scheduled_scope_reconcile,
        "interval",
        minutes=cfg.scheduler.scope_reconcile_interval,
        id="scope_reconcile",
    )
    _scheduler.add_job(
        _scheduled_intent_reconcile,
        "interval",
        minutes=cfg.scheduler.scope_reconcile_interval,
        id="intent_reconcile",
    )
    if cfg.scheduler.lag_topology_poll_interval > 0:
        _scheduler.add_job(
            _scheduled_lag_topology_refresh,
            "interval",
            minutes=cfg.scheduler.lag_topology_poll_interval,
            id="lag_topology_refresh",
        )
    if cfg.scheduler.enable_interface_ip_sync and cfg.scheduler.interface_ip_poll_interval > 0:
        _scheduler.add_job(
            _scheduled_interface_ip_refresh,
            "interval",
            minutes=cfg.scheduler.interface_ip_poll_interval,
            id="interface_ip_refresh",
        )
    if cfg.scheduler.enable_static_routing_sync and cfg.scheduler.static_route_poll_interval > 0:
        _scheduler.add_job(
            _scheduled_static_route_refresh,
            "interval",
            minutes=cfg.scheduler.static_route_poll_interval,
            id="static_route_refresh",
        )
    if cfg.scheduler.enable_isis_sync and cfg.scheduler.isis_poll_interval > 0:
        _scheduler.add_job(
            _scheduled_isis_refresh,
            "interval",
            minutes=cfg.scheduler.isis_poll_interval,
            id="isis_refresh",
        )
    if cfg.scheduler.enable_bgp_sync and cfg.scheduler.bgp_poll_interval > 0:
        _scheduler.add_job(
            _scheduled_bgp_refresh,
            "interval",
            minutes=cfg.scheduler.bgp_poll_interval,
            id="bgp_refresh",
        )
    if cfg.scheduler.enable_ospf_sync and cfg.scheduler.ospf_poll_interval > 0:
        _scheduler.add_job(
            _scheduled_ospf_refresh,
            "interval",
            minutes=cfg.scheduler.ospf_poll_interval,
            id="ospf_refresh",
        )
    if cfg.scheduler.enable_redistribution_sync and cfg.scheduler.redistribution_poll_interval > 0:
        _scheduler.add_job(
            _scheduled_redistribution_refresh,
            "interval",
            minutes=cfg.scheduler.redistribution_poll_interval,
            id="redistribution_refresh",
        )
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
