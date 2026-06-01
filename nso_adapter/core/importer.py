# SPDX-License-Identifier: Apache-2.0
"""Device sync importer — NSO → adapter DB → NetBox.

Sync flow (docs/nso-adapter.md §7):
  1. sync-from on NSO (refresh CDB from live device)
  2. Read managed attributes per interface
  3. Compute per-attribute compliance_status vs stored netbox_value
  4. NetBox binding writes NSO value onto dcim.Interface
  5. Persist interface_attr_state, update device.last_sync_*
"""

from __future__ import annotations

from datetime import datetime

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.config import get_config
from nso_adapter.core.compliance import compute_compliance_status
from nso_adapter.domain.models import Interface, InterfaceAttr
from nso_adapter.nso import actions as nso_actions
from nso_adapter.nso.client import NsoClient
from nso_adapter.store.models import (
    ComplianceStatus,
    DbInterface,
    Device,
    InterfaceAttrState,
    LastSyncStatus,
    ManagedScope,
    MappingStatus,
)

logger = structlog.get_logger(__name__)

_nso_clients: dict[str, NsoClient] = {}
_netbox_client = None  # set at startup via set_netbox_client


def _attrs_to_interface_list(data: dict | None) -> list[Interface]:
    """Convert NSO package interface-attributes oper-data to domain Interface objects.

    Skips malformed entries (missing ``interface-name``) with a warning log.
    Returns an empty list if *data* is None or has no ``interface`` key.
    """
    if data is None:
        return []
    result = []
    for entry in data.get("interface", []):
        name = entry.get("interface-name")
        if not name:
            logger.warning("interface-attrs: skipping malformed entry (no interface-name)", entry=entry)
            continue
        result.append(
            Interface(
                name=name,
                nso=InterfaceAttr(
                    description=entry.get("description"),
                    enabled=entry.get("enabled"),
                ),
                netbox=InterfaceAttr(description=None, enabled=None),
            )
        )
    return result


def register_nso_client(instance_name: str, client: NsoClient) -> None:
    _nso_clients[instance_name] = client


def get_nso_client(instance_name: str) -> NsoClient:
    if instance_name not in _nso_clients:
        raise RuntimeError(f"NSO client for {instance_name!r} not registered")
    return _nso_clients[instance_name]


def set_netbox_client(client) -> None:  # type: ignore[annotation-unchecked]
    global _netbox_client
    _netbox_client = client


def get_netbox_client():
    return _netbox_client


async def sync_device(device_id: int, db: AsyncSession) -> dict:
    """Full sync: NSO → DB → NetBox. Returns job result summary dict."""

    device = await db.get(Device, device_id)
    if not device:
        raise ValueError(f"Device {device_id} not found")

    client = get_nso_client(device.nso_instance)

    # Resolve NED ID if not yet set
    if not device.ned_id:
        device.ned_id = await client.get_device_ned_id(device.nso_device_name)
        if not device.ned_id:
            device.mapping_status = MappingStatus.unmatched_device
            device.last_sync_at = datetime.utcnow()
            device.last_sync_status = LastSyncStatus.failed
            await db.commit()
            raise ValueError(f"NSO device {device.nso_device_name!r} not found or has no NED ID")

    # Step 1: sync-from — refresh CDB from live device
    await nso_actions.sync_from(client, device.nso_device_name)

    # Step 2: read canonical interface attributes from NSO package oper-data
    attrs = await client.get_interface_attributes(device.nso_device_name)
    interfaces = _attrs_to_interface_list(attrs)

    # Determine which attributes are in scope
    scope_result = await db.execute(select(ManagedScope).where(ManagedScope.device_id == device_id))
    scope_attrs = [s.attribute for s in scope_result.scalars().all()]

    # Build lookup: interface name → DbInterface row
    result_rows = await db.execute(select(DbInterface).where(DbInterface.device_id == device_id))
    existing_ifaces: dict[str, DbInterface] = {row.name: row for row in result_rows.scalars().all()}

    # ── Phase 1: bulk interface inventory reconcile (plan Layer A) ──
    # Ensure every NSO-reported interface (incl. logical units as virtual
    # subinterfaces parented to their base) exists in NetBox in a few bulk
    # requests, instead of a GET+POST per interface. Returns name→nb_id.
    nb_client = get_netbox_client()
    nb_id_by_name: dict[str, int] = {}
    if nb_client and device.netbox_device_id:
        from nso_adapter.bindings.netbox.mapper import bulk_ensure_interfaces

        try:
            nb_id_by_name = await bulk_ensure_interfaces(
                nb_client, device.netbox_device_id, [i.name for i in interfaces]
            )
        except Exception as exc:
            logger.warning("netbox.bulk_ensure_failed", device_id=device_id, error=str(exc))

    interfaces_created = 0
    interfaces_written = 0
    changes_detected = 0
    # Batched NetBox attribute updates (Phase 2), merged per interface id so
    # description+enabled on one interface become a single PATCH row.
    attr_patches: dict[int, dict] = {}
    # Pending state updates keyed by NetBox interface id: applied ONLY after the
    # bulk PATCH confirms that id was written, so a failed/timed-out batch does
    # not falsely mark state as synced (which would skip it forever).
    pending_by_id: dict[int, list[tuple]] = {}

    for iface in interfaces:
        # Upsert DbInterface row
        db_iface = existing_ifaces.get(iface.name)
        if db_iface is None:
            db_iface = DbInterface(device_id=device_id, name=iface.name)
            db.add(db_iface)
            await db.flush()  # get id before upserting attr states
            interfaces_created += 1
        existing_ifaces[iface.name] = db_iface

        # Step 3: compute per-attribute compliance
        # Load existing attr_states
        attr_result = await db.execute(select(InterfaceAttrState).where(InterfaceAttrState.interface_id == db_iface.id))
        existing_attrs: dict[str, InterfaceAttrState] = {row.attribute: row for row in attr_result.scalars().all()}

        for attr in ("description", "enabled"):
            if attr not in scope_attrs:
                continue
            nso_val = iface.nso.description if attr == "description" else iface.nso.enabled
            nso_str = str(nso_val) if nso_val is not None else None

            attr_state = existing_attrs.get(attr)
            if attr_state is None:
                attr_state = InterfaceAttrState(interface_id=db_iface.id, attribute=attr)
                db.add(attr_state)

            prev_netbox_val = attr_state.netbox_value
            status = compute_compliance_status(nso_str, prev_netbox_val, attr_state.intent_value)
            if status == ComplianceStatus.changed:
                changes_detected += 1

            # Step 4 & 5: queue NetBox write (batched) + update state.
            # Phase 1 already created the interface; resolve its id by name.
            if nb_client and device.netbox_device_id:
                nb_id = nb_id_by_name.get(iface.name)
                if nb_id is not None:
                    if db_iface.netbox_interface_id is None:
                        db_iface.netbox_interface_id = nb_id
                    # Change detection: only enqueue a NetBox write when the value
                    # actually differs from what we last successfully wrote. Without
                    # this, every sync re-patches every interface (thousands of
                    # no-op writes) — which overwhelms NetBox and breeds lock
                    # contention. netbox_value is updated AFTER the bulk PATCH
                    # confirms the write (see Phase 2 flush), never optimistically,
                    # so a failed batch is safely retried next sync.
                    if prev_netbox_val != nso_str:
                        field_payload: dict = {}
                        if attr == "description":
                            field_payload["description"] = iface.nso.description or ""
                        elif iface.nso.enabled is not None:
                            field_payload["enabled"] = iface.nso.enabled
                        else:
                            continue  # NSO package didn't report enabled; skip write
                        attr_patches.setdefault(nb_id, {"id": nb_id}).update(field_payload)
                        pending_by_id.setdefault(nb_id, []).append((attr_state, nso_str))

            attr_state.nso_value = nso_str
            if attr_state.intent_value is not None:
                # Phase 2: intent has been deployed — use in_sync/drifted from compute_compliance_status.
                # Never downgrade to "imported" even if netbox_value == nso_str.
                attr_state.compliance_status = status
            else:
                # Phase 1: no intent yet — mark as "imported" when values match.
                # Note: netbox_value is only updated after the Phase 2 flush
                # confirms the write, so a just-written attr stays non-"imported"
                # until the next reconcile flips it (one-sync lag, self-heals).
                attr_state.compliance_status = (
                    ComplianceStatus.imported if attr_state.netbox_value == nso_str else status
                )
            attr_state.last_checked_at = datetime.utcnow()

    # ── Phase 2 flush: push queued attribute updates, batched + isolated ──
    # Mark netbox_value ONLY for ids the bulk PATCH confirms as written, so a
    # failed/timed-out batch is safely re-attempted on the next sync rather than
    # falsely recorded as in-sync.
    if nb_client and attr_patches:
        written = await nb_client.bulk_patch_interfaces(list(attr_patches.values()))
        for obj in written:
            for attr_state, nso_str in pending_by_id.get(obj["id"], []):
                attr_state.netbox_value = nso_str
                interfaces_written += 1

    # Update device sync state
    mapping_status = MappingStatus.mapped
    if not interfaces:
        mapping_status = MappingStatus.unmatched_interfaces
    device.mapping_status = mapping_status
    device.last_sync_at = datetime.utcnow()
    device.last_sync_status = LastSyncStatus.succeeded
    await db.commit()

    # Notify the netbox-nso-plugin so it refreshes its NSO*State display cache off
    # the request path. Best-effort — a callback failure must not fail the sync.
    if nb_client and device.netbox_device_id:
        try:
            await nb_client.notify_sync_complete(device.netbox_device_id)
        except Exception as exc:
            logger.warning("netbox.sync_complete_notify_failed", device_id=device_id, error=str(exc) or type(exc).__name__)

    summary = {
        "interfaces_written": interfaces_written,
        "interfaces_created": interfaces_created,
        "changes_detected": changes_detected,
    }
    logger.info("sync.done", device_id=device_id, **summary)
    return summary


async def check_compliance(device_id: int, db: AsyncSession) -> dict:
    """Re-read NSO config and recompute compliance WITHOUT writing to NetBox."""
    device = await db.get(Device, device_id)
    if not device:
        raise ValueError(f"Device {device_id} not found")

    client = get_nso_client(device.nso_instance)

    # compare-config re-reads from NSO CDB vs live device
    await nso_actions.compare_config(client, device.nso_device_name)

    attrs = await client.get_interface_attributes(device.nso_device_name)
    interfaces = _attrs_to_interface_list(attrs)

    scope_result2 = await db.execute(select(ManagedScope).where(ManagedScope.device_id == device_id))
    scope_attrs = [s.attribute for s in scope_result2.scalars().all()]
    changes_detected = 0

    for iface in interfaces:
        result_rows = await db.execute(
            select(DbInterface).where(DbInterface.device_id == device_id, DbInterface.name == iface.name)
        )
        db_iface = result_rows.scalar_one_or_none()
        if db_iface is None:
            continue

        for attr in ("description", "enabled"):
            if attr not in scope_attrs:
                continue
            nso_val = iface.nso.description if attr == "description" else iface.nso.enabled
            nso_str = str(nso_val) if nso_val is not None else None

            attr_result = await db.execute(
                select(InterfaceAttrState).where(
                    InterfaceAttrState.interface_id == db_iface.id,
                    InterfaceAttrState.attribute == attr,
                )
            )
            attr_state = attr_result.scalar_one_or_none()
            if attr_state is None:
                continue

            status = compute_compliance_status(nso_str, attr_state.netbox_value, attr_state.intent_value)
            if status == ComplianceStatus.changed:
                changes_detected += 1
            attr_state.nso_value = nso_str
            attr_state.compliance_status = status
            attr_state.last_checked_at = datetime.utcnow()

    device.last_sync_at = datetime.utcnow()
    await db.commit()
    return {"changes_detected": changes_detected}


async def discover_devices(db: AsyncSession) -> None:
    """Pull device list from all configured NSO instances and upsert into DB."""
    cfg = get_config()
    for inst in cfg.nso_instances:
        client = get_nso_client(inst.name)
        try:
            device_list = await client.list_devices()
        except Exception as exc:
            logger.error("discover.error", instance=inst.name, error=str(exc))
            continue
        for dev_data in device_list:
            name = dev_data.get("name")
            if not name:
                continue
            result = await db.execute(
                select(Device).where(
                    Device.nso_instance == inst.name,
                    Device.nso_device_name == name,
                )
            )
            if not result.scalar_one_or_none():
                db.add(Device(nso_instance=inst.name, nso_device_name=name))
    await db.commit()
