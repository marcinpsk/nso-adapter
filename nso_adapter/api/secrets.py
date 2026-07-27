# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Secrets endpoints: Vault set / verify + brownfield community harvest.

The adapter is the only component that WRITES Vault; the NSO snmp-reconciler
reads refs at commit time and the plugin stores refs only. Plaintext transits
these endpoints transiently (``SecretStr`` bodies, no body logging) and is
never persisted, returned, or interpolated into errors — responses carry only
refs, field names, KV v2 versions and ``sha256[:16]`` fingerprints (the same
digest the read mirror publishes as the community identity).
"""

from __future__ import annotations

import anyio.to_thread
import structlog
from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, Field, SecretStr
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, verify_token
from nso_adapter.api.errors import (
    RESP_400,
    RESP_401,
    RESP_404,
    RESP_409,
    RESP_422_VALIDATION,
    RESP_501,
    RESP_502,
    api_error,
)
from nso_adapter.core import snmp_harvest
from nso_adapter.core.importer import get_nso_client
from nso_adapter.secrets.refs import VaultRef, VaultRefError, parse_vault_ref, secret_fingerprint
from nso_adapter.store.models import Device

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/v1", tags=["secrets"], dependencies=[Depends(verify_token)])


class SecretWriteRequest(BaseModel):
    vault_ref: str  # "mount/path" (multi-field) or "mount/path#key" (that one field)
    values: dict[str, SecretStr] = Field(min_length=1)


class SecretWriteOut(BaseModel):
    vault_ref: str
    version: int
    hashes: dict[str, str]  # field → sha256[:16] fingerprint


class SecretVerifyRequest(BaseModel):
    vault_ref: str


class SecretVerifyOut(BaseModel):
    vault_ref: str
    exists: bool
    fields: list[str]
    hashes: dict[str, str]
    version: int | None


class HarvestCommunityRequest(BaseModel):
    community_hash: str  # the read mirror's sha256[:16] community identity
    vault_ref: str  # "mount/path#key" target to write the plaintext to


class HarvestCommunityOut(BaseModel):
    vault_ref: str
    secret_hash: str
    version: int
    access: str
    acl: str | None


def _vault_provider(request: Request):
    """Return the app's secrets provider, or 501 if it cannot write Vault."""
    provider = getattr(request.app.state, "secrets", None)
    if provider is None or not hasattr(provider, "write_path"):
        raise api_error(
            501,
            "secrets_write_unsupported",
            "The configured secrets provider cannot write Vault (secrets.provider must be 'vault')",
        )
    return provider


def _parse_ref(reference: str) -> VaultRef:
    try:
        return parse_vault_ref(reference)
    except VaultRefError as exc:
        raise api_error(400, "invalid_vault_ref", str(exc)) from exc


async def _vault_op(operation, vault_ref: str):
    """Run a provider read/write OFF the event loop, mapping Vault failures to a 502.

    hvac is blocking (``requests`` — real sockets), so calling it straight from an
    ``async def`` handler freezes the single event-loop thread for the whole round-trip:
    every other adapter request hangs, ``/health`` stops answering (a container liveness
    probe can then kill the adapter mid-write), and the in-process scheduler tick driving
    failover probes and job dispatch stalls. ``write_path`` is a read-merge-write (two
    round-trips) plus a possible AppRole re-login on 403, so the freeze multiplies.

    hvac error text names the path and reason (e.g. 'permission denied' when the
    AppRole policy doesn't cover the ref) — never secret values.
    """
    try:
        return await anyio.to_thread.run_sync(operation)
    except Exception as exc:
        raise api_error(502, "vault_error", f"Vault operation failed for {vault_ref!r}: {exc}") from exc


@router.post(
    "/secrets",
    response_model=SecretWriteOut,
    responses={**RESP_401, **RESP_400, **RESP_422_VALIDATION, **RESP_501, **RESP_502},
)
async def set_secret(body: SecretWriteRequest, request: Request) -> SecretWriteOut:
    """Merge-write secret fields at the ref's Vault path; return version + fingerprints."""
    provider = _vault_provider(request)
    ref = _parse_ref(body.vault_ref)
    if ref.key is not None and set(body.values) != {ref.key}:
        raise api_error(
            400,
            "invalid_vault_ref",
            f"ref names key {ref.key!r} but values carry fields {sorted(body.values)!r}",
        )

    plain = {field: value.get_secret_value() for field, value in body.values.items()}
    version = await _vault_op(lambda: provider.write_path(ref.mount, ref.path, plain), body.vault_ref)
    hashes = {field: secret_fingerprint(value) for field, value in plain.items()}
    logger.info("secrets.set", vault_ref=body.vault_ref, fields=sorted(plain), version=version)
    return SecretWriteOut(vault_ref=body.vault_ref, version=version, hashes=hashes)


@router.post(
    "/secrets/verify",
    response_model=SecretVerifyOut,
    responses={**RESP_401, **RESP_400, **RESP_422_VALIDATION, **RESP_501, **RESP_502},
)
async def verify_secret(body: SecretVerifyRequest, request: Request) -> SecretVerifyOut:
    """Resolve a ref and return field names + fingerprints — never the values."""
    provider = _vault_provider(request)
    ref = _parse_ref(body.vault_ref)

    data, version = await _vault_op(lambda: provider.read_path_meta(ref.mount, ref.path), body.vault_ref)
    if ref.key is not None:
        data = {ref.key: data[ref.key]} if ref.key in data else {}
    if not data:
        version = None
    return SecretVerifyOut(
        vault_ref=body.vault_ref,
        exists=bool(data),
        fields=sorted(data),
        hashes={field: secret_fingerprint(value) for field, value in data.items()},
        version=version,
    )


@router.post(
    "/devices/{device_id}/secrets/harvest-community",
    response_model=HarvestCommunityOut,
    responses={
        **RESP_401,
        **RESP_400,
        **RESP_404,
        **RESP_409,
        **RESP_422_VALIDATION,
        **RESP_501,
        **RESP_502,
    },
)
async def harvest_community(
    device_id: int,
    body: HarvestCommunityRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> HarvestCommunityOut:
    """Adopt a device-held community string into Vault by its read-mirror fingerprint.

    Reads ONLY the targeted per-NED community subtree of NSO's config mirror,
    matches by ``sha256[:16]``, and writes the plaintext to the supplied ref.
    v3 secrets are never harvestable (engine-ID-localized); timos is excluded
    (SR OS stores communities hash2-obfuscated — live-confirmed).
    """
    provider = _vault_provider(request)
    ref = _parse_ref(body.vault_ref)
    if ref.key is None:
        raise api_error(400, "invalid_vault_ref", "harvest target ref must name a '#key'")

    device = await db.get(Device, device_id)
    if device is None:
        raise api_error(404, "not_found", f"device {device_id} not found")

    ned_id = device.ned_id or ""
    subpath = snmp_harvest.harvest_subpath(ned_id)
    if subpath is None:
        raise api_error(
            409,
            "harvest_unsupported_ned",
            f"NED {ned_id!r} is not harvest-capable (SR OS stores communities "
            "hash2-obfuscated — live-confirmed; v3 secrets are never harvestable)",
        )

    try:
        client = get_nso_client(device.nso_instance)
    except RuntimeError as exc:
        raise api_error(502, "nso_unavailable", str(exc)) from exc
    payload = await client.get_device_config_subtree(device.nso_device_name, subpath)

    found = snmp_harvest.find_community(ned_id, payload or {}, body.community_hash)
    if found is None:
        raise api_error(
            404,
            "community_not_found",
            f"no community with hash {body.community_hash!r} in the config mirror of "
            f"{device.nso_device_name!r} — if the device changed out-of-band, run sync-from and refresh first",
        )

    version = await _vault_op(lambda: provider.write_path(ref.mount, ref.path, {ref.key: found.secret}), body.vault_ref)
    logger.info(
        "secrets.harvest_community",
        device=device.nso_device_name,
        community_hash=body.community_hash,
        vault_ref=body.vault_ref,
        version=version,
    )
    return HarvestCommunityOut(
        vault_ref=body.vault_ref,
        secret_hash=secret_fingerprint(found.secret),
        version=version,
        access=found.access,
        acl=found.acl,
    )
