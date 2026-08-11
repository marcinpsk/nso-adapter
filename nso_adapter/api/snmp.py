# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""GET /api/v1/devices/{id}/snmp-config and PUT /api/v1/devices/{id}/snmp-intent endpoints."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Literal

import structlog
from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, get_read_db, verify_token
from nso_adapter.api.errors import RESP_401, RESP_404_DEVICE, RESP_409_PUSH_SEQ, RESP_422_VALIDATION, api_error
from nso_adapter.api.intent_push import admit_or_replay, get_intent_delivery
from nso_adapter.api.read_state import FamilyReadState, read_state_payload
from nso_adapter.api.timestamps import UtcInstant, iso_z
from nso_adapter.store import outcome_store
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


# ── Read-mirror response models (GET /snmp-config) ────────────────────────────
# EMIT-NULL shape: every key is always present and nullable values serialise as
# null (NOT omitted), so this endpoint deliberately does NOT use
# response_model_exclude_unset — the fixed shape is the contract.


class SnmpCommunityOut(BaseModel):
    community_hash: str
    access: str
    acl: str | None


class SnmpV3UserOut(BaseModel):
    username: str
    has_auth_secret: bool
    has_priv_secret: bool


class SnmpHostOut(BaseModel):
    address: str
    version: str | None
    notify_type: str | None
    port: int | None
    username: str | None


class SnmpSystemInfoOut(BaseModel):
    location: str | None
    contact: str | None


class SnmpConfigOut(BaseModel):
    device_id: int
    last_refreshed_at: str | None = None  # reader formats "<iso>Z"; None when never refreshed
    refresh_source: str  # legacy freshness (S5 retires it); read_state is the S4 truth
    read_state: FamilyReadState
    communities: list[SnmpCommunityOut]
    v3_users: list[SnmpV3UserOut]
    hosts: list[SnmpHostOut]
    system_info: SnmpSystemInfoOut | None


@router.get(
    "/{device_id}/snmp-config",
    dependencies=[Depends(verify_token)],
    response_model=SnmpConfigOut,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_422_VALIDATION},
)
async def get_snmp_config(device_id: int, db: AsyncSession = Depends(get_read_db)):
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    # Pointer first, rows second, one snapshot (S4 D2 — benign direction).
    read_state = read_state_payload(
        await outcome_store.get_current_outcome(db, device_id, "snmp"), source_epoch=device.source_epoch
    )

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
            "read_state": read_state,
            "communities": [],
            "v3_users": [],
            "hosts": [],
            "system_info": None,
        }

    latest = max(all_rows, key=lambda r: r.last_refreshed_at)

    return {
        "device_id": device_id,
        "last_refreshed_at": iso_z(latest.last_refreshed_at),
        "refresh_source": latest.refresh_source,
        "read_state": read_state,
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
                "username": h.username,
            }
            for h in hosts
        ],
        "system_info": ({"location": system_info.location, "contact": system_info.contact} if system_info else None),
    }


# ---------------------------------------------------------------------------
# PUT /{device_id}/snmp-intent
# ---------------------------------------------------------------------------


def _validated_vault_ref(value: str | None) -> str | None:
    """Reject a vault_ref the SNMP writer could never render into the mandatory triple.

    apply_snmp_config splits every ref into vault-mount/path/key and raises
    ``invalid_vault_ref`` on anything malformed — a mandatory leaf, so a bad ref cannot be
    skipped (dropping the element would delete it from the device on a replace apply). The
    field was an unvalidated ``str``, so the PUT returned 200 and the bad ref sat in the
    store failing EVERY apply forever, with the apply-diff preview swallowing the error so
    the operator saw no warning before hitting Apply. Fail at the boundary instead: the
    store must never hold intent the writer cannot render.
    """
    if value is None or value == "":
        return value
    from nso_adapter.secrets.refs import VaultRefError, parse_vault_ref

    try:
        parse_vault_ref(value, require_key=True)
    except VaultRefError as exc:
        raise ValueError(str(exc)) from exc
    return value


# The exact spellings _SNMP_VERSION / _SNMP_NOTIFY / _SNMP_ACCESS can map (nso/apply.py).
# Anything else raises mid-body-build and aborts the whole SNMP scope, so it must never
# reach the store.
SnmpVersion = Literal["1", "v1", "2", "2c", "v2c", "3", "v3"]
SnmpNotifyType = Literal["trap", "traps", "inform", "informs"]
SnmpAccess = Literal["ro", "RO", "rw", "RW"]


class SnmpCommunityEntry(BaseModel):
    label: str
    vault_ref: str  # "mount/path#key"
    access: SnmpAccess
    acl: str | None = None
    accepted_at: UtcInstant | None = None

    _check_vault_ref = field_validator("vault_ref")(_validated_vault_ref)


class SnmpV3UserEntry(BaseModel):
    username: str
    group: str | None = None
    # snmp-reconciler YANG enum spellings; an absent protocol disables that leg
    auth_protocol: Literal["md5", "sha", "sha-256", "sha-384", "sha-512"] | None = None
    priv_protocol: Literal["des", "3des", "aes-128", "aes-192", "aes-256"] | None = None
    auth_vault_ref: str | None = None
    priv_vault_ref: str | None = None
    accepted_at: UtcInstant | None = None

    _check_auth_ref = field_validator("auth_vault_ref")(_validated_vault_ref)
    _check_priv_ref = field_validator("priv_vault_ref")(_validated_vault_ref)


class SnmpHostEntry(BaseModel):
    address: str
    version: SnmpVersion
    notify_type: SnmpNotifyType
    community_or_user: str
    port: int | None = Field(default=None, ge=1, le=65535)  # absent = NED default 162
    accepted_at: UtcInstant | None = None


class SnmpSystemInfoEntry(BaseModel):
    location: str | None = None
    contact: str | None = None
    accepted_at: UtcInstant | None = None


class SnmpIntentUpdate(BaseModel):
    communities: list[SnmpCommunityEntry] = []
    v3_users: list[SnmpV3UserEntry] = []
    hosts: list[SnmpHostEntry] = []
    system_info: SnmpSystemInfoEntry | None = None


def _accepted_or_now(entry, now: datetime) -> datetime:
    """Resolve an entry's ``accepted_at``, defaulting to now.

    UTC is the ``UtcInstant`` annotation's guarantee — never re-normalize it here.
    """
    return entry.accepted_at or now


async def _sync_intent_collection(
    db: AsyncSession,
    model,
    device_id: int,
    *,
    key_attr: str,
    entries: list,
    now: datetime,
    apply_fields: Callable,
    make_row: Callable,
    capture: Callable | None = None,
) -> tuple[int, list[str], dict[str, object]]:
    """Full-replace one keyed SNMP intent collection for a device.

    Rows whose key is absent from *entries* are deleted; the rest are upserted
    (``apply_fields`` mutates an existing row, ``make_row`` builds a new one).
    Returns ``(upserted_count, removed_keys, captured)``.

    *capture* snapshots one value off each row that is about to be DELETED, before it is deleted —
    the removal worker runs long after this row is gone, so anything it needs from the row has to
    be lifted out here (CR-A17: a community's vault_ref, without which its removal cannot be
    verified on the device).
    """
    rows = (await db.execute(select(model).where(model.device_id == device_id))).scalars().all()
    existing = {getattr(r, key_attr): r for r in rows}
    new_keys = {getattr(e, key_attr) for e in entries}
    removed = [k for k in existing if k not in new_keys]
    captured = {k: capture(existing[k]) for k in removed} if capture else {}
    for k in removed:
        await db.delete(existing[k])
    await db.flush()

    count = 0
    for entry in entries:
        accepted = _accepted_or_now(entry, now)
        key = getattr(entry, key_attr)
        if key in existing:
            apply_fields(existing[key], entry, accepted)
        else:
            db.add(make_row(entry, accepted))
        count += 1
    return count, removed, captured


def _apply_community_fields(row: SnmpCommunityIntent, e: SnmpCommunityEntry, accepted: datetime) -> None:
    row.vault_ref = e.vault_ref
    row.access = e.access
    row.acl = e.acl
    row.accepted_at = accepted


def _apply_v3_user_fields(row: SnmpV3UserIntent, e: SnmpV3UserEntry, accepted: datetime) -> None:
    row.group_name = e.group
    row.auth_protocol = e.auth_protocol
    row.priv_protocol = e.priv_protocol
    row.auth_vault_ref = e.auth_vault_ref
    row.priv_vault_ref = e.priv_vault_ref
    row.accepted_at = accepted


def _apply_host_fields(row: SnmpHostIntent, e: SnmpHostEntry, accepted: datetime) -> None:
    row.version = e.version
    row.notify_type = e.notify_type
    row.community_or_user = e.community_or_user
    row.port = e.port
    row.accepted_at = accepted


async def _sync_system_info(db: AsyncSession, device_id: int, entry: SnmpSystemInfoEntry | None, now: datetime) -> bool:
    """Upsert or delete the singleton system-info intent. Returns True iff a row was deleted."""
    existing = (
        await db.execute(select(SnmpSystemInfoIntent).where(SnmpSystemInfoIntent.device_id == device_id))
    ).scalar_one_or_none()
    if entry is None:
        if existing:
            await db.delete(existing)
            return True
        return False
    accepted = _accepted_or_now(entry, now)
    if existing:
        existing.location = entry.location
        existing.contact = entry.contact
        existing.accepted_at = accepted
    else:
        db.add(
            SnmpSystemInfoIntent(
                device_id=device_id, location=entry.location, contact=entry.contact, accepted_at=accepted
            )
        )
    return False


class SnmpIntentResult(BaseModel):
    device_id: int
    community_count: int
    v3_user_count: int
    host_count: int
    has_system_info: bool
    updated_at: str  # "<iso>Z" stamped at write time


@router.put(
    "/{device_id}/snmp-intent",
    dependencies=[Depends(verify_token)],
    response_model=SnmpIntentResult,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_409_PUSH_SEQ, **RESP_422_VALIDATION},
)
async def put_snmp_intent(
    device_id: int,
    body: SnmpIntentUpdate,
    db: AsyncSession = Depends(get_db),
    delivery=Depends(get_intent_delivery),
):
    """Replace the adapter's SNMP intent mirror for this device atomically.

    Full-replace semantics: rows not present in the request body are deleted.
    ``accepted_at`` defaults to now if not supplied.  If ``auto_apply`` is
    enabled on the device, an apply job is enqueued after the upsert. Any
    removal triggers a replace-mode re-apply so the drop reverts on-device.
    """
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    # Every accepted write records its projection revision, store-only and
    # auto-apply-off included, and takes the device's projection lock before anything is
    # read (#1522 §G2). Only a promotion authorizes a deployment.
    from nso_adapter.core.generation import note_write
    from nso_adapter.core.receipt import record_response
    from nso_adapter.core.request_flags import PUSH_SEQ

    await note_write(db, device_id, delivery.stream, push_seq=PUSH_SEQ.get())
    if (replay := await admit_or_replay(db, device_id, delivery)) is not None:
        return replay

    now = datetime.now(UTC)

    comm_count, removed_comms, removed_comm_refs = await _sync_intent_collection(
        db,
        SnmpCommunityIntent,
        device_id,
        key_attr="label",
        entries=body.communities,
        now=now,
        apply_fields=_apply_community_fields,
        make_row=lambda e, acc: SnmpCommunityIntent(
            device_id=device_id, label=e.label, vault_ref=e.vault_ref, access=e.access, acl=e.acl, accepted_at=acc
        ),
        # CR-A17: lift the vault_ref off each row we are about to delete. The removal worker needs
        # it to compute the community's EXPORT key (sha256 of the secret) and so actually verify
        # that the community left the device — the row itself is gone by then.
        capture=lambda row: row.vault_ref,
    )
    user_count, removed_users, _ = await _sync_intent_collection(
        db,
        SnmpV3UserIntent,
        device_id,
        key_attr="username",
        entries=body.v3_users,
        now=now,
        apply_fields=_apply_v3_user_fields,
        make_row=lambda e, acc: SnmpV3UserIntent(
            device_id=device_id,
            username=e.username,
            group_name=e.group,
            auth_protocol=e.auth_protocol,
            priv_protocol=e.priv_protocol,
            auth_vault_ref=e.auth_vault_ref,
            priv_vault_ref=e.priv_vault_ref,
            accepted_at=acc,
        ),
    )
    host_count, removed_hosts, _ = await _sync_intent_collection(
        db,
        SnmpHostIntent,
        device_id,
        key_attr="address",
        entries=body.hosts,
        now=now,
        apply_fields=_apply_host_fields,
        make_row=lambda e, acc: SnmpHostIntent(
            device_id=device_id,
            address=e.address,
            version=e.version,
            notify_type=e.notify_type,
            community_or_user=e.community_or_user,
            port=e.port,
            accepted_at=acc,
        ),
    )
    removed = removed_comms + removed_users + removed_hosts

    if await _sync_system_info(db, device_id, body.system_info, now):
        removed.append("system-info")

    await db.flush()

    settings_result = await db.execute(select(DeviceSettings).where(DeviceSettings.device_id == device_id))
    settings = settings_result.scalar_one_or_none()
    total_count = comm_count + user_count + host_count + (1 if body.system_info else 0)
    if settings and settings.auto_apply and total_count > 0:
        from nso_adapter.core.apply import enqueue_apply

        await enqueue_apply(db, device_id, force=True, stream=delivery.stream)

    # A removal reverts on-device via an ASYNC removal job (like every other scope) — the
    # PUT no longer blocks on a full replace-mode device commit (which could stall the PUT
    # past the plugin client timeout). Enqueue before the commit so the job row lands with
    # the trimmed intent it will re-apply.
    if removed:
        from nso_adapter.core.removal import enqueue_removal

        # Thread the PER-COLLECTION removed keys (community/v3-user/host) so the
        # collateral guard can tell this intended retraction from an orphaned service
        # row — the merged `removed` list is namespace-ambiguous (a community and a
        # host may share a name) and system-info is a non-guarded scalar.
        await enqueue_removal(
            db,
            device_id,
            "snmp",
            promotes=(delivery.stream,),
            removed={"community": removed_comms, "v3-user": removed_users, "host": removed_hosts},
            vault_refs={label: ref for label, ref in removed_comm_refs.items() if ref},
        )

    result = {
        "device_id": device_id,
        "community_count": comm_count,
        "v3_user_count": user_count,
        "host_count": host_count,
        "has_system_info": body.system_info is not None,
        "updated_at": iso_z(now),
    }
    await record_response(db, device_id, delivery, result)
    await db.commit()

    logger.info(
        "snmp_intent.put.ok",
        device_id=device_id,
        community_count=comm_count,
        v3_user_count=user_count,
        host_count=host_count,
        has_system_info=body.system_info is not None,
    )
    return result
