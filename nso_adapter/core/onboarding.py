# SPDX-License-Identifier: Apache-2.0
"""Device onboarding — create/validate device records and identity mapping.

Onboarding does NOT pre-flight NSO; if the device name is wrong the first sync
sets mapping_status = unmatched_device.
"""

from __future__ import annotations

import asyncio

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.config import get_config
from nso_adapter.store.models import ActiveAddress, DbInterface, Device, ManagedScope, MappingStatus

logger = structlog.get_logger(__name__)


async def _bootstrap_address(client, device_name: str, primary: str, oob_ip: str | None) -> tuple[str, dict | None]:
    """Reachability-aware initial management address.

    When failover is enabled and a fresh device's primary IP is unreachable but its OOB IP
    works, point NSO at the OOB address so the device is configurable immediately (it fails
    back to primary once the in-band address comes up). Returns ``(active_address, step|None)``.
    """
    cfg = get_config().scheduler
    if not (cfg.enable_failover and oob_ip and oob_ip != primary):
        return ActiveAddress.primary.value, None
    from nso_adapter.nso.actions import probe_reachable

    reachable, _detail, _elapsed = await probe_reachable(client, device_name, cfg.failover_probe_timeout)
    if reachable:
        return ActiveAddress.primary.value, {"step": "failover_bootstrap", "status": "primary"}
    try:
        await client.set_address(device_name, oob_ip)
        await client.disconnect(device_name)
    except Exception as exc:
        return ActiveAddress.primary.value, {"step": "failover_bootstrap", "status": "failed", "detail": repr(exc)}
    return ActiveAddress.oob.value, {
        "step": "failover_bootstrap",
        "status": "oob",
        "detail": f"primary {primary} unreachable; using OOB {oob_ip}",
    }


# Some devices (observed on IOS-XR) reset the FIRST southbound connection right
# after the node is created/unlocked; a single backed-off retry clears it.
_ONBOARD_RETRY_BACKOFF_SECONDS = 3.0


async def _once_with_retry(action, *, backoff: float = _ONBOARD_RETRY_BACKOFF_SECONDS, ok=None):
    """Run async *action*; on failure wait *backoff* and run it exactly once more.

    "Failure" = the action raised, or (when *ok* is given) it returned a value
    for which ``ok(value)`` is falsy — covers both fetch-host-keys (raises) and
    sync-from (returns a bool). The second attempt's exception/result propagates.
    """
    try:
        result = await action()
    except Exception:
        await asyncio.sleep(backoff)
        return await action()
    if ok is not None and not ok(result):
        await asyncio.sleep(backoff)
        return await action()
    return result


async def onboard_device(
    db: AsyncSession,
    nso_instance: str,
    nso_device_name: str,
    netbox_device_id: int,
) -> Device:
    """Onboard a device: link the NSO node (nso_instance + nso_device_name) to *netbox_device_id*.

    Creates a new mapped Device row, or ADOPTS an existing unlinked one for the same NSO node
    (a leftover provisioned into NSO without a NetBox link) by filling in netbox_device_id.
    Idempotent when the node is already linked to the same NetBox device.

    Raises:
        ValueError: if the NSO instance is unknown.
        LookupError: if netbox_device_id is already onboarded elsewhere, or the NSO node is already
            linked to a DIFFERENT NetBox device.

    """
    cfg = get_config()
    known_instances = {inst.name for inst in cfg.nso_instances}
    if nso_instance not in known_instances:
        raise ValueError(f"NSO instance {nso_instance!r} not found in config")

    # Is this exact NSO node (instance + name) already tracked by the adapter?
    existing = (
        await db.execute(
            select(Device).where(
                Device.nso_instance == nso_instance,
                Device.nso_device_name == nso_device_name,
            )
        )
    ).scalar_one_or_none()
    if existing is not None:
        # Already linked to THIS NetBox device → idempotent no-op (e.g. a re-fired manage signal).
        if existing.netbox_device_id == netbox_device_id:
            return existing
        # Linked to a DIFFERENT NetBox device → genuine conflict; never silently repoint it.
        if existing.netbox_device_id is not None:
            raise LookupError(
                f"NSO device {nso_device_name!r} on {nso_instance!r} is already onboarded "
                f"to NetBox device {existing.netbox_device_id}"
            )
        # Unlinked leftover — provisioned INTO NSO without a NetBox link (netbox_device_id NULL).
        # ADOPT it: fill the mapping in on the same row. Rejecting here left the plugin's onboard
        # POST failing with 409, which it swallowed, so the device never onboarded. The target
        # netbox_device_id must still be free (not held by some OTHER device row).
        dup_nb = (
            await db.execute(
                select(Device).where(Device.netbox_device_id == netbox_device_id, Device.id != existing.id)
            )
        ).scalar_one_or_none()
        if dup_nb is not None:
            raise LookupError(f"NetBox device {netbox_device_id} is already onboarded")
        existing.netbox_device_id = netbox_device_id
        existing.mapping_status = MappingStatus.mapped
        await db.commit()
        await db.refresh(existing)
        logger.info(
            "device.adopted", device_id=existing.id, nso_device=nso_device_name, netbox_device_id=netbox_device_id
        )
        return existing

    # New NSO node → the target netbox_device_id must not already be onboarded elsewhere.
    dup_nb = await db.execute(select(Device).where(Device.netbox_device_id == netbox_device_id))
    if dup_nb.scalar_one_or_none():
        raise LookupError(f"NetBox device {netbox_device_id} is already onboarded")

    device = Device(
        nso_instance=nso_instance,
        nso_device_name=nso_device_name,
        netbox_device_id=netbox_device_id,
        mapping_status=MappingStatus.mapped,
    )
    db.add(device)
    await db.commit()
    await db.refresh(device)
    logger.info("device.onboarded", device_id=device.id, nso_device=nso_device_name)
    return device


async def provision_nso_device(
    db: AsyncSession,
    *,
    nso_instance: str,
    device_name: str,
    address: str,
    ned_id: str,
    authgroup: str,
    netbox_device_id: int | None = None,
    ned_type: str | None = None,
    port: int | None = None,
    admin_state: str = "unlocked",
    do_sync: bool = True,
    oob_ip: str | None = None,
) -> dict:
    """Provision a device INTO NSO and bring it up, then map it in the adapter.

    Sequence (each step recorded; stops on a failure that blocks the next):
      1. create the device node (idempotent — skipped if it already exists)
      2. ssh fetch-host-keys (TOFU) — needs the device reachable
      3. set admin-state (unlocked)
      4. sync-from (pull running config into CDB) — non-fatal; normal sync retries
      5. create the adapter Device mapping row (if ``netbox_device_id`` given)

    ``ned_type`` (the NSO ``device-type`` transport) is derived from ``ned_id`` when
    not given; an explicit value that contradicts the ned_id raises ValueError
    (guards against onboarding a NETCONF NED as ``device-type cli``).

    On a blocking failure the device is left in NSO as-is for retry (no rollback).
    Returns ``{"ok": bool, "steps": [...], "device_id": int|None}``.
    """
    from nso_adapter.core.importer import get_nso_client
    from nso_adapter.nso.neds import resolve_device_type

    known = {inst.name for inst in get_config().nso_instances}
    if nso_instance not in known:
        raise ValueError(f"NSO instance {nso_instance!r} not found in config")

    device_type = resolve_device_type(ned_id, ned_type)

    client = get_nso_client(nso_instance)
    steps: list[dict] = []

    def _step(name: str, status: str, detail: str | None = None) -> None:
        entry = {"step": name, "status": status}
        if detail:
            entry["detail"] = detail
        steps.append(entry)

    def _result(ok: bool, device_id: int | None = None) -> dict:
        return {"ok": ok, "steps": steps, "device_id": device_id}

    # 1. create node (idempotent)
    try:
        if await client.device_exists(device_name):
            _step("create", "exists")
        else:
            await client.create_device(device_name, address, ned_id, authgroup, ned_type=device_type, port=port)
            _step("create", "ok", f"device-type={device_type}")
    except Exception as exc:
        _step("create", "failed", repr(exc))
        return _result(False)

    # 2. admin-state unlocked — blocking. MUST precede fetch-host-keys: a newly
    #    created device defaults to southbound-locked, which blocks ALL southbound
    #    traffic, so fetch-host-keys (and any connect) fails until it is unlocked.
    try:
        await client.set_admin_state(device_name, admin_state)
        _step("admin_state", "ok", admin_state)
    except Exception as exc:
        _step("admin_state", "failed", repr(exc))
        return _result(False)

    # 2b. reachability-aware address: bootstrap a fresh device over OOB if primary is
    #     unreachable (failover only). MUST precede fetch-host-keys so keys/sync use the
    #     reachable address. Best-effort — falls back to primary on any probe error.
    active_address, fo_step = await _bootstrap_address(client, device_name, address, oob_ip)
    if fo_step:
        steps.append(fo_step)

    # 3. fetch host keys (needs the device reachable AND unlocked) — blocking,
    #    with one backed-off retry for the first-connect reset.
    try:
        await _once_with_retry(lambda: client.fetch_host_keys(device_name))
        _step("fetch_host_keys", "ok")
    except Exception as exc:
        _step("fetch_host_keys", "failed", repr(exc))
        # If the bootstrap pinned NSO to the OOB address, don't strand the device: map it and
        # seed the failover row so the loop can fail it back to primary once in-band recovers.
        if active_address == ActiveAddress.oob.value:
            device_id = await _map_and_seed_failover(
                db, nso_instance, device_name, netbox_device_id, address, oob_ip, active_address, steps
            )
            return _result(False, device_id)
        return _result(False)

    # 4. sync-from — non-fatal (the adapter's normal sync will retry), but give
    #    it one backed-off retry too so onboarding usually lands fully synced.
    if do_sync:
        try:
            ok = await _once_with_retry(lambda: client.sync_from(device_name), ok=bool)
            _step("sync_from", "ok" if ok else "failed")
        except Exception as exc:
            _step("sync_from", "failed", repr(exc))

    # 5-6. adapter mapping row (so the read pipeline manages it henceforth) + failover row
    #      (IPs + bootstrapped address) so the failover loop can manage it.
    device_id = await _map_and_seed_failover(
        db, nso_instance, device_name, netbox_device_id, address, oob_ip, active_address, steps
    )

    logger.info("device.provisioned", nso_device=device_name, instance=nso_instance, steps=steps)
    return _result(True, device_id)


async def _map_and_seed_failover(
    db: AsyncSession,
    nso_instance: str,
    device_name: str,
    netbox_device_id: int | None,
    address: str,
    oob_ip: str | None,
    active_address: str,
    steps: list[dict],
) -> int | None:
    """Create the adapter mapping row and seed the failover row; return the device_id or None.

    Shared by the happy path and the OOB-bootstrap failure recovery so a device NSO was pinned
    to its OOB address is always handed to the failover loop — never stranded on OOB with no
    DeviceFailover row to fail it back once the in-band address recovers.
    """
    device_id = None
    if netbox_device_id is not None:
        try:
            row = await onboard_device(db, nso_instance, device_name, netbox_device_id)
            device_id = row.id
            steps.append({"step": "adapter_mapping", "status": "ok"})
        except LookupError as exc:
            steps.append({"step": "adapter_mapping", "status": "exists", "detail": repr(exc)})
    fo_seed = await _seed_onboarding_failover(db, device_id, address, oob_ip, active_address)
    if fo_seed:
        steps.append(fo_seed)
    return device_id


async def _seed_onboarding_failover(
    db: AsyncSession, device_id: int | None, primary: str, oob_ip: str | None, active_address: str
) -> dict | None:
    """Seed the failover row at onboarding (when enabled). Returns a step dict, or None."""
    if not (get_config().scheduler.enable_failover and device_id is not None and (oob_ip or primary)):
        return None
    from nso_adapter.core.failover import set_initial_failover_state

    try:
        await set_initial_failover_state(db, device_id, primary, oob_ip, active_address)
        await db.commit()
        return {"step": "failover_seed", "status": "ok", "detail": active_address}
    except Exception as exc:
        return {"step": "failover_seed", "status": "failed", "detail": repr(exc)}


async def rekey_device(
    db: AsyncSession,
    device: Device,
    nso_instance: str | None = None,
    nso_device_name: str | None = None,
) -> Device:
    """Re-key a device (change NSO instance or device name).

    Clears interface state; job history is retained.
    """
    from sqlalchemy import delete

    cfg = get_config()
    known_instances = {inst.name for inst in cfg.nso_instances}

    if nso_instance is not None and nso_instance not in known_instances:
        raise ValueError(f"NSO instance {nso_instance!r} not found in config")

    # Check that the target (instance, device_name) pair is not already claimed by another device.
    target_instance = nso_instance if nso_instance is not None else device.nso_instance
    target_name = nso_device_name if nso_device_name is not None else device.nso_device_name
    dup = await db.execute(
        select(Device).where(
            Device.nso_instance == target_instance,
            Device.nso_device_name == target_name,
            Device.id != device.id,
        )
    )
    if dup.scalar_one_or_none():
        raise LookupError(f"NSO device {target_name!r} on {target_instance!r} is already claimed by another device")

    if nso_instance is not None:
        device.nso_instance = nso_instance
    if nso_device_name is not None:
        device.nso_device_name = nso_device_name

    # Clear interface attr state, then interfaces, then managed scope — explicit to avoid lazy load
    iface_ids_result = await db.execute(select(DbInterface.id).where(DbInterface.device_id == device.id))
    iface_ids = list(iface_ids_result.scalars().all())
    if iface_ids:
        from nso_adapter.store.models import InterfaceAttrState

        await db.execute(delete(InterfaceAttrState).where(InterfaceAttrState.interface_id.in_(iface_ids)))
    await db.execute(delete(DbInterface).where(DbInterface.device_id == device.id))
    await db.execute(delete(ManagedScope).where(ManagedScope.device_id == device.id))

    device.ned_id = None
    device.mapping_status = MappingStatus.mapped
    device.last_sync_at = None
    device.last_sync_status = None

    await db.commit()
    await db.refresh(device)
    logger.info("device.rekeyed", device_id=device.id, nso_device=device.nso_device_name)
    return device


async def offboard_device(db: AsyncSession, device: Device) -> None:
    """Remove all adapter state for a device. Does not modify NetBox."""
    from sqlalchemy import delete, update

    from nso_adapter.store.models import InterfaceAttrState, Job

    # Delete in FK dependency order to avoid cascade-load on lazy="raise" relationships
    iface_ids_result = await db.execute(select(DbInterface.id).where(DbInterface.device_id == device.id))
    iface_ids = list(iface_ids_result.scalars().all())
    if iface_ids:
        await db.execute(delete(InterfaceAttrState).where(InterfaceAttrState.interface_id.in_(iface_ids)))
    await db.execute(delete(DbInterface).where(DbInterface.device_id == device.id))
    await db.execute(delete(ManagedScope).where(ManagedScope.device_id == device.id))
    # Null-out device_id on jobs so history is preserved (device_id is nullable by design)
    await db.execute(update(Job).where(Job.device_id == device.id).values(device_id=None))
    await db.delete(device)
    await db.commit()
    logger.info("device.offboarded", device_id=device.id)


async def set_scope(db: AsyncSession, device: Device, attributes: list[str]) -> list[ManagedScope]:
    """Replace the managed-scope attribute list for a device."""
    from sqlalchemy import select

    existing_result = await db.execute(select(ManagedScope).where(ManagedScope.device_id == device.id))
    existing = {s.attribute: s for s in existing_result.scalars().all()}
    desired = set(attributes)

    # Remove rows no longer in scope
    for attr, row in list(existing.items()):
        if attr not in desired:
            await db.delete(row)

    # Add new rows
    new_rows: list[ManagedScope] = []
    for attr in desired:
        if attr not in existing:
            row = ManagedScope(device_id=device.id, attribute=attr)
            db.add(row)
            new_rows.append(row)
        else:
            new_rows.append(existing[attr])

    await db.commit()

    # Re-query to get the final committed list
    result = await db.execute(select(ManagedScope).where(ManagedScope.device_id == device.id))
    return list(result.scalars().all())
