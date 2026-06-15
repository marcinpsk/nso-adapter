# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""GET /api/v1/devices/{id}/snmp-config and PUT /api/v1/devices/{id}/snmp-intent endpoints."""

from __future__ import annotations

from datetime import UTC, datetime

import structlog
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, verify_token
from nso_adapter.api.errors import api_error
from nso_adapter.store.models import (
    Device,
    DeviceSettings,
    SnmpCommunity,
    SnmpCommunityIntent,
    SnmpHost,
    SnmpHostIntent,
    SnmpSystemInfo,
    SnmpSystemInfoIntent,
    SnmpV3User,
    SnmpV3UserIntent,
)

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/v1/devices", tags=["snmp-config"])


@router.get("/{device_id}/snmp-config", dependencies=[Depends(verify_token)])
async def get_snmp_config(device_id: int, db: AsyncSession = Depends(get_db)):
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    communities_result = await db.execute(select(SnmpCommunity).where(SnmpCommunity.device_id == device_id))
    communities = communities_result.scalars().all()

    v3_users_result = await db.execute(select(SnmpV3User).where(SnmpV3User.device_id == device_id))
    v3_users = v3_users_result.scalars().all()

    hosts_result = await db.execute(select(SnmpHost).where(SnmpHost.device_id == device_id))
    hosts = hosts_result.scalars().all()

    system_info_result = await db.execute(select(SnmpSystemInfo).where(SnmpSystemInfo.device_id == device_id))
    system_info = system_info_result.scalar_one_or_none()

    all_rows = list(communities) + list(v3_users) + list(hosts)
    if system_info:
        all_rows.append(system_info)

    if not all_rows:
        return {
            "device_id": device_id,
            "last_refreshed_at": None,
            "refresh_source": "never",
            "communities": [],
            "v3_users": [],
            "hosts": [],
            "system_info": None,
        }

    latest = max(all_rows, key=lambda r: r.last_refreshed_at)

    return {
        "device_id": device_id,
        "last_refreshed_at": latest.last_refreshed_at.isoformat() + "Z",
        "refresh_source": latest.refresh_source,
        "communities": [{"community_hash": c.community_hash, "access": c.access, "acl": c.acl} for c in communities],
        "v3_users": [
            {
                "username": u.username,
                "has_auth_secret": u.has_auth_secret,
                "has_priv_secret": u.has_priv_secret,
            }
            for u in v3_users
        ],
        "hosts": [
            {
                "address": h.address,
                "version": h.version,
                "notify_type": h.notify_type,
                "port": h.port,
            }
            for h in hosts
        ],
        "system_info": ({"location": system_info.location, "contact": system_info.contact} if system_info else None),
    }


# ---------------------------------------------------------------------------
# PUT /{device_id}/snmp-intent
# ---------------------------------------------------------------------------


class SnmpCommunityEntry(BaseModel):
    label: str
    vault_ref: str  # "mount/path#key"
    access: str  # "RO" | "RW"
    acl: str | None = None
    accepted_at: datetime | None = None


class SnmpV3UserEntry(BaseModel):
    username: str
    auth_vault_ref: str | None = None
    priv_vault_ref: str | None = None
    accepted_at: datetime | None = None


class SnmpHostEntry(BaseModel):
    address: str
    version: str  # "1" | "2c" | "3"
    notify_type: str  # "trap" | "inform"
    community_or_user: str
    accepted_at: datetime | None = None


class SnmpSystemInfoEntry(BaseModel):
    location: str | None = None
    contact: str | None = None
    accepted_at: datetime | None = None


class SnmpIntentUpdate(BaseModel):
    communities: list[SnmpCommunityEntry] = []
    v3_users: list[SnmpV3UserEntry] = []
    hosts: list[SnmpHostEntry] = []
    system_info: SnmpSystemInfoEntry | None = None


@router.put("/{device_id}/snmp-intent", dependencies=[Depends(verify_token)])
async def put_snmp_intent(device_id: int, body: SnmpIntentUpdate, db: AsyncSession = Depends(get_db)):  # noqa: C901
    """Replace the adapter's SNMP intent mirror for this device atomically.

    Full-replace semantics: rows not present in the request body are deleted.
    ``accepted_at`` defaults to now if not supplied.  If ``auto_apply`` is
    enabled on the device, an apply job is enqueued after the upsert.
    """
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    now = datetime.now(UTC).replace(tzinfo=None)

    # ── Communities ──────────────────────────────────────────────────────────
    existing_comms_result = await db.execute(
        select(SnmpCommunityIntent).where(SnmpCommunityIntent.device_id == device_id)
    )
    existing_comms: dict[str, SnmpCommunityIntent] = {r.label: r for r in existing_comms_result.scalars().all()}
    new_comm_labels = {e.label for e in body.communities}
    removed_any = [label for label in existing_comms if label not in new_comm_labels]
    for label in list(removed_any):
        await db.delete(existing_comms[label])
    await db.flush()

    comm_count = 0
    for entry in body.communities:
        accepted = entry.accepted_at.replace(tzinfo=None) if entry.accepted_at else now
        if entry.label in existing_comms:
            row = existing_comms[entry.label]
            row.vault_ref = entry.vault_ref
            row.access = entry.access
            row.acl = entry.acl
            row.accepted_at = accepted
        else:
            row = SnmpCommunityIntent(
                device_id=device_id,
                label=entry.label,
                vault_ref=entry.vault_ref,
                access=entry.access,
                acl=entry.acl,
                accepted_at=accepted,
            )
            db.add(row)
        comm_count += 1

    # ── V3 Users ─────────────────────────────────────────────────────────────
    existing_users_result = await db.execute(select(SnmpV3UserIntent).where(SnmpV3UserIntent.device_id == device_id))
    existing_users: dict[str, SnmpV3UserIntent] = {r.username: r for r in existing_users_result.scalars().all()}
    new_usernames = {e.username for e in body.v3_users}
    removed_any += [u for u in existing_users if u not in new_usernames]
    for username in [u for u in existing_users if u not in new_usernames]:
        await db.delete(existing_users[username])
    await db.flush()

    user_count = 0
    for entry in body.v3_users:
        accepted = entry.accepted_at.replace(tzinfo=None) if entry.accepted_at else now
        if entry.username in existing_users:
            row = existing_users[entry.username]
            row.auth_vault_ref = entry.auth_vault_ref
            row.priv_vault_ref = entry.priv_vault_ref
            row.accepted_at = accepted
        else:
            row = SnmpV3UserIntent(
                device_id=device_id,
                username=entry.username,
                auth_vault_ref=entry.auth_vault_ref,
                priv_vault_ref=entry.priv_vault_ref,
                accepted_at=accepted,
            )
            db.add(row)
        user_count += 1

    # ── Hosts ─────────────────────────────────────────────────────────────────
    existing_hosts_result = await db.execute(select(SnmpHostIntent).where(SnmpHostIntent.device_id == device_id))
    existing_hosts: dict[str, SnmpHostIntent] = {r.address: r for r in existing_hosts_result.scalars().all()}
    new_addresses = {e.address for e in body.hosts}
    removed_any += [a for a in existing_hosts if a not in new_addresses]
    for address in [a for a in existing_hosts if a not in new_addresses]:
        await db.delete(existing_hosts[address])
    await db.flush()

    host_count = 0
    for entry in body.hosts:
        accepted = entry.accepted_at.replace(tzinfo=None) if entry.accepted_at else now
        if entry.address in existing_hosts:
            row = existing_hosts[entry.address]
            row.version = entry.version
            row.notify_type = entry.notify_type
            row.community_or_user = entry.community_or_user
            row.accepted_at = accepted
        else:
            row = SnmpHostIntent(
                device_id=device_id,
                address=entry.address,
                version=entry.version,
                notify_type=entry.notify_type,
                community_or_user=entry.community_or_user,
                accepted_at=accepted,
            )
            db.add(row)
        host_count += 1

    # ── System Info ───────────────────────────────────────────────────────────
    existing_sysinfo_result = await db.execute(
        select(SnmpSystemInfoIntent).where(SnmpSystemInfoIntent.device_id == device_id)
    )
    existing_sysinfo = existing_sysinfo_result.scalar_one_or_none()

    if body.system_info is None:
        if existing_sysinfo:
            await db.delete(existing_sysinfo)
            removed_any.append("system-info")
    else:
        accepted = body.system_info.accepted_at.replace(tzinfo=None) if body.system_info.accepted_at else now
        if existing_sysinfo:
            existing_sysinfo.location = body.system_info.location
            existing_sysinfo.contact = body.system_info.contact
            existing_sysinfo.accepted_at = accepted
        else:
            db.add(
                SnmpSystemInfoIntent(
                    device_id=device_id,
                    location=body.system_info.location,
                    contact=body.system_info.contact,
                    accepted_at=accepted,
                )
            )

    await db.flush()

    settings_result = await db.execute(select(DeviceSettings).where(DeviceSettings.device_id == device_id))
    settings = settings_result.scalar_one_or_none()
    total_count = comm_count + user_count + host_count + (1 if body.system_info else 0)
    if settings and settings.auto_apply and total_count > 0:
        from nso_adapter.core.apply import enqueue_apply

        await enqueue_apply(db, device_id, force=True)

    await db.commit()

    # Removal propagation: PUT-replace the snmp-reconciler instance with the full
    # remaining intent so a removed community/user/host/system-info is reverted.
    if removed_any:
        from nso_adapter.core.importer import get_nso_client
        from nso_adapter.nso.apply import apply_snmp_config

        comms = (
            (await db.execute(select(SnmpCommunityIntent).where(SnmpCommunityIntent.device_id == device_id)))
            .scalars()
            .all()
        )
        users = (
            (await db.execute(select(SnmpV3UserIntent).where(SnmpV3UserIntent.device_id == device_id))).scalars().all()
        )
        hosts = (await db.execute(select(SnmpHostIntent).where(SnmpHostIntent.device_id == device_id))).scalars().all()
        sysinfo = (
            await db.execute(select(SnmpSystemInfoIntent).where(SnmpSystemInfoIntent.device_id == device_id))
        ).scalar_one_or_none()
        try:
            nso_client = get_nso_client(device.nso_instance)
            await apply_snmp_config(nso_client, device.nso_device_name, comms, users, hosts, sysinfo, replace=True)
        except Exception as exc:  # noqa: BLE001
            logger.error("snmp_intent.replace_failed", device_id=device_id, error=repr(exc))

    logger.info(
        "snmp_intent.put.ok",
        device_id=device_id,
        community_count=comm_count,
        v3_user_count=user_count,
        host_count=host_count,
        has_system_info=body.system_info is not None,
    )
    return {
        "device_id": device_id,
        "community_count": comm_count,
        "v3_user_count": user_count,
        "host_count": host_count,
        "has_system_info": body.system_info is not None,
        "updated_at": now.isoformat() + "Z",
    }
