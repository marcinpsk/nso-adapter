# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Secrets endpoints: Vault set / verify + brownfield community harvest.

The adapter is the only component that WRITES Vault; the NSO snmp-reconciler
reads refs at commit time and the plugin stores refs only. Plaintext transits
these endpoints transiently (``SecretStr`` bodies, no body logging) and is
never persisted, returned, or interpolated into errors.

Neither is the REFERENCE, nor any component of it. ``vault_ref`` and the
``values`` keys are free-form caller strings that name a mount, a path and a
field, so a caller that pastes a secret into one would read it back out of the
answer and out of every log that recorded the answer. Every response and every
record carries an adapter-minted ``operation_id`` instead: it joins the record
to the answer, and it is ours. What Vault itself reports back (field names,
fingerprints, KV v2 versions) is not a caller echo and still travels.
"""

from __future__ import annotations

from uuid import uuid4

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
from nso_adapter.secrets.refs import (
    SECRET_FINGERPRINT_PATTERN,
    VaultRef,
    VaultRefError,
    parse_vault_ref,
    secret_fingerprint,
)
from nso_adapter.store.models import Device

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/v1", tags=["secrets"], dependencies=[Depends(verify_token)])


class SecretWriteRequest(BaseModel):
    vault_ref: str  # "mount/path" (multi-field) or "mount/path#key" (that one field)
    values: dict[str, SecretStr] = Field(min_length=1)


class SecretWriteOut(BaseModel):
    operation_id: str  # the adapter-minted handle joining this answer to its log record
    version: int


class SecretVerifyRequest(BaseModel):
    vault_ref: str


class SecretVerifyOut(BaseModel):
    operation_id: str
    exists: bool
    fields: list[str]
    hashes: dict[str, str]
    version: int | None


class HarvestCommunityRequest(BaseModel):
    # The read mirror's sha256[:16] community identity. Validated HERE: an unconstrained
    # field takes the community itself, and the caller then reads its own secret back out
    # of the refusal. The 422 for a non-fingerprint repeats no part of the submitted value.
    community_hash: str = Field(pattern=SECRET_FINGERPRINT_PATTERN)
    vault_ref: str  # "mount/path#key" target to write the plaintext to


class HarvestCommunityOut(BaseModel):
    operation_id: str
    secret_hash: str
    version: int
    access: str
    acl: str | None


def _operation_id() -> str:
    """Mint the correlation handle for one secrets operation.

    A reference names a mount, a path and a key, and the caller chooses all three, so no
    part of it may reach a log record or a response body. An operator still has to join a
    record to the answer the caller got, and this id is that join, so it carries the whole
    uuid4: a truncated handle collides and joins an answer to another operation's record.
    """
    return uuid4().hex


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
    """Parse a caller-supplied ref, answering 400 with the broken rule and not the input.

    The caller learns which part of the grammar it broke. It is never sent its own text
    back: a caller that put a secret in the ``vault_ref`` field would otherwise read it
    out of the error body and out of every log that recorded the response.
    """
    try:
        return parse_vault_ref(reference)
    except VaultRefError as exc:
        reason = exc.reason
    # Raised outside the handler: `from exc` (and `from None`) both keep the parser
    # exception on the chain, and its text repeats the reference.
    raise api_error(400, "invalid_vault_ref", reason)


async def _vault_op(operation):
    """Run a provider read/write OFF the event loop, mapping Vault failures to a 502.

    hvac is blocking (``requests`` — real sockets), so calling it straight from an
    ``async def`` handler freezes the single event-loop thread for the whole round-trip:
    every other adapter request hangs, ``/health`` stops answering (a container liveness
    probe can then kill the adapter mid-write), and the in-process scheduler tick driving
    failover probes and job dispatch stalls. ``write_path`` is a read-merge-write (two
    round-trips) plus a possible AppRole re-login on 403, so the freeze multiplies.

    The provider's own text can repeat the request URL and the payload, and the ref names a
    mount, a path and a key, so the 502 carries the failure TYPE alone. The caller already
    knows which ref it sent.
    """
    try:
        return await anyio.to_thread.run_sync(operation)
    except Exception as exc:  # noqa: BLE001 — every provider failure is the same 502
        failure = type(exc).__name__
    # Raised outside the handler: `from None` would still leave the provider's exception
    # reachable on __context__, and a formatted traceback prints it.
    raise api_error(502, "vault_error", f"The Vault operation failed ({failure})")


@router.post(
    "/secrets",
    response_model=SecretWriteOut,
    responses={**RESP_401, **RESP_400, **RESP_422_VALIDATION, **RESP_501, **RESP_502},
)
async def set_secret(body: SecretWriteRequest, request: Request) -> SecretWriteOut:
    """Merge-write secret fields at the ref's Vault path; return the new KV v2 version."""
    provider = _vault_provider(request)
    ref = _parse_ref(body.vault_ref)
    if ref.key is not None and set(body.values) != {ref.key}:
        # Both halves of the mismatch are the caller's own strings, so the refusal states the
        # rule. The caller holds the ref and the field names it sent and needs neither back.
        raise api_error(
            400,
            "invalid_vault_ref",
            "a vault_ref ending in '#<key>' requires values to carry exactly that one field",
        )

    plain = {field: value.get_secret_value() for field, value in body.values.items()}
    version = await _vault_op(lambda: provider.write_path(ref.mount, ref.path, plain))
    operation_id = _operation_id()
    logger.info("secrets.set", operation_id=operation_id, version=version)
    return SecretWriteOut(operation_id=operation_id, version=version)


@router.post(
    "/secrets/verify",
    response_model=SecretVerifyOut,
    responses={**RESP_401, **RESP_400, **RESP_422_VALIDATION, **RESP_501, **RESP_502},
)
async def verify_secret(body: SecretVerifyRequest, request: Request) -> SecretVerifyOut:
    """Resolve a ref and return field names + fingerprints — never the values."""
    provider = _vault_provider(request)
    ref = _parse_ref(body.vault_ref)

    data, version = await _vault_op(lambda: provider.read_path_meta(ref.mount, ref.path))
    if ref.key is not None:
        data = {ref.key: data[ref.key]} if ref.key in data else {}
    if not data:
        version = None
    operation_id = _operation_id()
    # The field names and fingerprints are what VAULT holds, not what the caller sent, and the
    # verify exists to report them. Nothing of the submitted ref is echoed.
    logger.info("secrets.verify", operation_id=operation_id, exists=bool(data), version=version)
    return SecretVerifyOut(
        operation_id=operation_id,
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

    unavailable = None
    try:
        client = get_nso_client(device.nso_instance)
    except RuntimeError:
        # Adapter-authored, like the same refusal in api/capability.py. The caught text is
        # not repeated and not chained: a raise inside the handler attaches it either way.
        unavailable = api_error(502, "nso_unavailable", f"No NSO client for instance {device.nso_instance!r}")
    if unavailable is not None:
        raise unavailable
    payload = await client.get_device_config_subtree(device.nso_device_name, subpath)

    found = snmp_harvest.find_community(ned_id, payload or {}, body.community_hash)
    if found is None:
        # The adapter's own device id, never the NSO name; the fingerprint is the caller's.
        raise api_error(
            404,
            "community_not_found",
            f"no community with the requested fingerprint in the config mirror of device {device.id}. "
            "If the device changed out-of-band, run sync-from and refresh first",
        )

    version = await _vault_op(lambda: provider.write_path(ref.mount, ref.path, {ref.key: found.secret}))
    operation_id = _operation_id()
    # The device is the adapter's own id and the hash is a fingerprint; no part of the ref.
    logger.info(
        "secrets.harvest_community",
        operation_id=operation_id,
        device_id=device.id,
        community_hash=body.community_hash,
        version=version,
    )
    return HarvestCommunityOut(
        operation_id=operation_id,
        secret_hash=secret_fingerprint(found.secret),
        version=version,
        access=found.access,
        acl=found.acl,
    )
