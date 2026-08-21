# SPDX-License-Identifier: Apache-2.0
"""Device onboarding — create/validate device records and identity mapping.

Onboarding does NOT pre-flight NSO; if the device name is wrong the first sync
sets mapping_status = unmatched_device.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from contextlib import suppress
from typing import Any

import structlog
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.config import get_config
from nso_adapter.core.claim import (
    CLAIM_WAIT_POLL_INTERVAL_S,
    ClaimLostError,
    ClaimRegistration,
    ClaimUnavailableError,
    acquire_claim_resolving,
    lock_claim,
    release_claim,
    resolve_claim_by_token,
)
from nso_adapter.core.families import ALL_FAMILY_KEYS
from nso_adapter.store import outcome_store
from nso_adapter.store.device_settle import create_counter
from nso_adapter.store.models import (
    ActiveAddress,
    Base,
    DbInterface,
    Device,
    DeviceClaim,
    ManagedScope,
    MappingStatus,
    RefreshOutcomePointer,
)

logger = structlog.get_logger(__name__)

_READ_MIRROR_ROOTS = (
    "interfaces",
    "lag_interface",
    "lag_bundle_config",
    "device_vlan",
    "device_switchport",
    "interface_ip_address",
    "snmp_community",
    "snmp_v3_user",
    "snmp_host",
    "device_logging_host",
    "device_logging_levels",
    "snmp_system_info",
    "device_static_route",
    "device_svi",
    "device_subinterface",
    "device_interface_mtu",
    "device_l2_sap",
    "device_isis_interface",
    "device_isis_process",
    "device_bfd_interface",
    "device_bgp_router",
    "device_route_policy_prefix_list",
    "device_route_policy_community_list",
    "device_route_policy_as_path",
    "device_route_policy_route_map",
    "device_ospf_interface",
    "device_ospf_instance",
    "device_redistribution",
)


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
    *,
    reg: ClaimRegistration | None = None,
    job_id: int | None = None,
) -> Device:
    """Onboard a device: link the NSO node (nso_instance + nso_device_name) to *netbox_device_id*.

    Creates a new mapped Device row, or ADOPTS an existing unlinked one for the same NSO node
    (a leftover provisioned into NSO without a NetBox link) by filling in netbox_device_id.
    Idempotent when the node is already linked to the same NetBox device.

    *reg* switches on provision mode: the device claim is acquired here, registered into the
    caller's live :class:`ClaimRegistration`, and left HELD so provision's post-map phase
    runs guarded. Without it — the plugin's mapping POST — nothing about this function
    changes. See :func:`_onboard_under_claim`.

    Raises:
        ValueError: if the NSO instance is unknown.
        LookupError: if netbox_device_id is already onboarded elsewhere, or the NSO node is already
            linked to a DIFFERENT NetBox device.
        ClaimUnavailableError: provision mode only — the device stayed claimed for the whole
            wait budget, so the mapping is refused rather than performed unserialized.

    """
    cfg = get_config()
    known_instances = {inst.name for inst in cfg.nso_instances}
    if nso_instance not in known_instances:
        raise ValueError(f"NSO instance {nso_instance!r} not found in config")

    if reg is not None:
        return await _onboard_under_claim(db, nso_instance, nso_device_name, netbox_device_id, reg, job_id)

    # Is this exact NSO node (instance + name) already tracked by the adapter?
    # FOR UPDATE: adoption of an unlinked row is a read-then-update, so without the row lock two
    # callers can both read netbox_device_id NULL, both find their (different) target ids free,
    # and both claim the same row — the last commit silently repointing ownership. The unique
    # constraints cannot catch that: only one row exists and it is an UPDATE, not an insert.
    existing = (
        await db.execute(
            select(Device)
            .where(
                Device.nso_instance == nso_instance,
                Device.nso_device_name == nso_device_name,
            )
            .with_for_update()
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
    dup_nb_result = await db.execute(select(Device).where(Device.netbox_device_id == netbox_device_id))
    if dup_nb_result.scalar_one_or_none():
        raise LookupError(f"NetBox device {netbox_device_id} is already onboarded")

    device = Device(
        nso_instance=nso_instance,
        nso_device_name=nso_device_name,
        netbox_device_id=netbox_device_id,
        mapping_status=MappingStatus.mapped,
    )
    db.add(device)
    try:
        # The settle counter is created WITH the device, in this same transaction: a terminal
        # write may never create it (Appendix S §3.3), so every insert site owes one.
        await db.flush()
        await create_counter(db, device.id)
        await db.commit()
    except IntegrityError:
        # Lost a race with a concurrent onboard of the same device. The checks above are
        # select-then-insert, so both callers can find nothing and both insert; the DB
        # constraints (uq_device_nso_identity / uq_device_netbox_device_id) are what actually
        # decide. Re-read the winner and return it — onboarding is idempotent by contract, and
        # a duplicate row here would be permanent (the scope reconcile keeps every row it sees).
        await db.rollback()
        winner = (
            await db.execute(
                select(Device).where(
                    Device.nso_instance == nso_instance,
                    Device.nso_device_name == nso_device_name,
                )
            )
        ).scalar_one_or_none()
        if winner is None or winner.netbox_device_id not in (None, netbox_device_id):
            # The conflict was on netbox_device_id instead: another NSO node claimed it.
            raise LookupError(f"NetBox device {netbox_device_id} is already onboarded") from None
        logger.info("device.onboard_race_resolved", device_id=winner.id, nso_device=nso_device_name)
        return winner
    await db.refresh(device)
    logger.info("device.onboarded", device_id=device.id, nso_device=nso_device_name)
    return device


async def _onboard_under_claim(
    db: AsyncSession,
    nso_instance: str,
    nso_device_name: str,
    netbox_device_id: int,
    reg: ClaimRegistration,
    job_id: int | None,
) -> Device:
    """Map the device with its claim already held, and leave the claim held.

    Provision keeps doing device work after this returns — the failover seed and the
    comprehensive mirror fill — so the window from the mapping to the end of that work is
    exactly where a rival sync, failover tick or teardown used to interleave on a device
    that had just become visible with no claim.

    Acquisition differs by how the Device becomes visible:

    * **fresh insert** — the ``Device`` and its ``device_claim`` row go in ONE transaction;
    * **exact pair / adoption / lost-insert winner** — the Device already exists and another
      session may be holding it, so discovery is NON-LOCKING, that transaction ends, the
      claim is acquired in its own committed transaction, and only then is the row re-read
      and revalidated under ``claim -> devices``.

    Carrying a ``FOR UPDATE`` from the discovery into the acquisition is the shape that does
    not work (R18-B2): the claim INSERT validates its ``devices`` FK by taking FOR KEY SHARE
    on that row, so it blocks on our own lock.

    The wait is the OQ6 intent-claim budget, and a timeout raises rather than proceeding
    unserialized — with nothing written, including on the adoption branch, which is why the
    adoption must not commit before the claim exists.
    """
    deadline = time.monotonic() + get_config().intent_claim_wait_seconds
    lost_insert = False
    while True:
        # NON-LOCKING, and the transaction ends before the acquisition.
        existing_id = await db.scalar(
            select(Device.id).where(
                Device.nso_instance == nso_instance,
                Device.nso_device_name == nso_device_name,
            )
        )
        await db.rollback()

        if existing_id is None:
            if lost_insert:
                # An insert lost, yet the pair still does not exist: the conflict was on
                # netbox_device_id, so another NSO node holds it. Retrying cannot help.
                raise LookupError(f"NetBox device {netbox_device_id} is already onboarded")
            device = await _insert_device_with_claim(db, nso_instance, nso_device_name, netbox_device_id, reg, job_id)
            if device is not None:
                return device
            # Lost the insert: the winner is now an ordinary existing Device. Re-discover it
            # rather than assuming, since the winner may carry a different mapping.
            lost_insert = True
            continue

        acquired = await acquire_claim_resolving(
            existing_id, "job", job_id=job_id, lock_timeout_ms=_remaining_ms(deadline), adopt=reg
        )
        if acquired is not None:
            try:
                device = await _link_existing_under_claim(db, acquired, existing_id, netbox_device_id, reg)
            except BaseException:
                # Only while UNREGISTERED: once the run owns the claim the worker's outer
                # path owns the release, and releasing here would strand the job. Roll the
                # caller's transaction back first — a body that died after `lock_claim` left
                # a FOR UPDATE pending in it, and the release would wait on our own lock.
                if not reg.registered:
                    with suppress(Exception):
                        await db.rollback()
                    await release_claim(acquired)
                raise
            if device is not None:
                return device
            # Torn down between discovery and the claim (M6.9s i): the operator issued both
            # operations, and a genuinely fresh onboarding is the correct outcome.
            await release_claim(acquired)

        if time.monotonic() >= deadline:
            logger.warning(
                "device.mapping_claim_timeout",
                nso_device=nso_device_name,
                waited_s=get_config().intent_claim_wait_seconds,
            )
            raise ClaimUnavailableError(f"NSO device {nso_device_name!r} is claimed by another operation")
        # Never past the deadline: the budget is the whole wait, polling included.
        await asyncio.sleep(min(CLAIM_WAIT_POLL_INTERVAL_S, max(0.0, deadline - time.monotonic())))


def _remaining_ms(deadline: float) -> int:
    """Return what is left of the OQ6 budget, as a server-side lock bound.

    Without it the budget bounds only the polling, not the one wait the acquisition itself
    can make: ``ON CONFLICT DO NOTHING`` blocks on a rival's uncommitted insertion. Floored
    at 1ms rather than 0, which PostgreSQL reads as "no timeout at all" — an expired budget
    must still make an uncontended attempt, just never a waiting one.
    """
    return max(1, int((deadline - time.monotonic()) * 1000))


async def _insert_device_with_claim(
    db: AsyncSession,
    nso_instance: str,
    nso_device_name: str,
    netbox_device_id: int,
    reg: ClaimRegistration,
    job_id: int | None,
) -> Device | None:
    """Insert the Device and its claim together; None when another writer won the race.

    Both rows are NEW: no rival can hold a lock on either and neither is observable until
    the commit, so the within-transaction ``devices`` → ``device_claim`` order the FK
    requires is not a §3.9 inversion — §3.9 orders locks taken on rows that ALREADY exist.
    Splitting them would reopen, on the one branch that can avoid it entirely, the window
    this whole guard exists to close.

    Registration follows the COMMIT with no await in between, deliberately: anything that
    can raise or be cancelled there would leave a durable claim that the worker still reads
    as claimless — no guard, no release, no recovery until the reaper.
    """
    dup_nb = await db.scalar(select(Device.id).where(Device.netbox_device_id == netbox_device_id))
    if dup_nb is not None:
        await db.rollback()
        raise LookupError(f"NetBox device {netbox_device_id} is already onboarded")

    token = uuid.uuid4().hex
    device = Device(
        nso_instance=nso_instance,
        nso_device_name=nso_device_name,
        netbox_device_id=netbox_device_id,
        mapping_status=MappingStatus.mapped,
    )
    db.add(device)
    try:
        await db.flush()
        db.add(DeviceClaim(device_id=device.id, claim_token=token, purpose="job", job_id=job_id))
        # Same transaction as the device, like every other insert site (Appendix S §3.3).
        await create_counter(db, device.id)
        await db.commit()
    except IntegrityError:
        # Provably before COMMIT: a concurrent onboard of the same node or netbox id won.
        await db.rollback()
        return None
    except BaseException as exc:
        # In doubt: the COMMIT may have landed with both rows, and a CANCELLATION delivered
        # at that await is the same state as a lost ack. Read the durable answer.
        with suppress(Exception):
            await db.rollback()
        resolved = await resolve_claim_by_token(token)
        if resolved is None:
            raise
        reg.register(*resolved.identity())
        if not isinstance(exc, Exception):
            # A cancellation still has to propagate — the ownership was just handed to the
            # registration, so the worker's claimed terminal path releases it.
            raise
        # The claim is durably there, so the Device is too — its FK cascades the claim away.
        return await db.scalar(select(Device).where(Device.id == resolved.device_id))

    reg.register(device.id, token)
    await db.refresh(device)
    logger.info("device.onboarded", device_id=device.id, nso_device=nso_device_name, claimed=True)
    return device


async def _link_existing_under_claim(
    db: AsyncSession,
    acquired: ClaimRegistration,
    device_id: int,
    netbox_device_id: int,
    reg: ClaimRegistration,
) -> Device | None:
    """Re-read and revalidate the existing Device under the claim. None if it vanished.

    The adoption branch is the only one that writes, and it writes HERE rather than during
    discovery: a mapping committed before the claim existed would be a device write the
    acquisition timeout is supposed to have prevented.

    Registers *reg* — with no await between the decision and the registration — on every
    path that keeps the claim, so the run and the worker never disagree about ownership.
    Every path that gives the claim back leaves *reg* unregistered.
    """
    await lock_claim(db, acquired)  # claim -> devices, per §3.9
    existing = await db.scalar(select(Device).where(Device.id == device_id).with_for_update())
    if existing is None:
        await db.rollback()
        return None
    # Snapshotted: ending the transaction below expires the instance, and an implicit lazy
    # load on an async session raises MissingGreenlet instead of the intended error.
    linked_to, nso_device_name, nso_instance = (
        existing.netbox_device_id,
        existing.nso_device_name,
        existing.nso_instance,
    )

    # Already linked to THIS NetBox device → idempotent no-op; nothing to write.
    if linked_to == netbox_device_id:
        reg.register(*acquired.identity())
        await db.rollback()
        return await db.get(Device, device_id)
    # Linked to a DIFFERENT NetBox device → genuine conflict; never silently repoint it.
    if linked_to is not None:
        await db.rollback()
        raise LookupError(
            f"NSO device {nso_device_name!r} on {nso_instance!r} is already onboarded to NetBox device {linked_to}"
        )
    dup_nb = await db.scalar(
        select(Device.id).where(Device.netbox_device_id == netbox_device_id, Device.id != device_id)
    )
    if dup_nb is not None:
        await db.rollback()
        raise LookupError(f"NetBox device {netbox_device_id} is already onboarded")

    existing.netbox_device_id = netbox_device_id
    existing.mapping_status = MappingStatus.mapped
    await db.commit()
    reg.register(*acquired.identity())
    await db.refresh(existing)
    logger.info(
        "device.adopted",
        device_id=device_id,
        nso_device=nso_device_name,
        netbox_device_id=netbox_device_id,
        claimed=True,
    )
    return existing


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
    reg: ClaimRegistration | None = None,
    job_id: int | None = None,
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

    Steps 1-4 are CDB/NSO work against no adapter Device and run on the CLAIMLESS lane.
    From step 5 on the run holds the device claim — acquired and registered into *reg*
    inside :func:`onboard_device` — and every commit after it is guarded. The claim is
    deliberately NOT released here: ``_run_provision`` still has to write the job's terminal
    status, result and ``device_id`` link, and releasing at the refresh boundary lets a
    teardown delete the device out from under that final commit.

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
                db,
                nso_instance,
                device_name,
                netbox_device_id,
                address,
                oob_ip,
                active_address,
                steps,
                reg=reg,
                job_id=job_id,
            )
            return _result(False, device_id)
        return _result(False)

    # 4. sync-from — non-fatal (the adapter's normal sync will retry), but give
    #    it one backed-off retry too so onboarding usually lands fully synced.
    sync_ok = False
    if do_sync:
        try:
            sync_ok = bool(await _once_with_retry(lambda: client.sync_from(device_name), ok=bool))
            _step("sync_from", "ok" if sync_ok else "failed")
        except Exception as exc:
            _step("sync_from", "failed", repr(exc))

    # 5-6. adapter mapping row (so the read pipeline manages it henceforth) + failover row
    #      (IPs + bootstrapped address) so the failover loop can manage it.
    device_id = await _map_and_seed_failover(
        db,
        nso_instance,
        device_name,
        netbox_device_id,
        address,
        oob_ip,
        active_address,
        steps,
        reg=reg,
        job_id=job_id,
    )

    # 7. A2: fill the read-mirror immediately so a freshly-onboarded device's IP/LAG/L2/... show up
    #    on the tab NOW, not on the next per-family poll (0–60 min for IP, up to 300 min for most).
    #    Gated on a SUCCESSFUL sync-from: reading the export before the CDB is populated returns an
    #    empty/404 body that would commit an empty mirror (the onboarding empty-wipe race). Best-effort
    #    — a surface read failure must never fail provisioning; the normal poll/sync heals it later.
    if sync_ok and device_id is not None:
        await _initial_mirror_refresh(db, device_id, client, reg=reg)

    logger.info("device.provisioned", nso_device=device_name, instance=nso_instance, steps=steps)
    return _result(True, device_id)


async def _guard(db: AsyncSession, reg: ClaimRegistration | None) -> None:
    """Take the claim's row lock before this transaction's first effectful statement.

    A no-op both when there is no registration (the plugin's mapping path, direct calls)
    and while one is unregistered (the claimless half of a provision), which is what lets
    the post-map helpers call it unconditionally instead of branching.
    """
    if reg is not None:
        await lock_claim(db, reg)


async def _initial_mirror_refresh(
    db: AsyncSession, device_id: int, client, *, reg: ClaimRegistration | None = None
) -> None:
    """Best-effort comprehensive read-mirror fill for a freshly-provisioned device (A2).

    Runs under the device claim in provision mode: a revocation mid-refresh must stop the
    writes rather than let a replaced holder keep filling the mirror. The guard is taken
    twice on purpose — the per-family refreshes commit inside
    ``refresh_all_surfaces_for_device``, so the final commit here is a NEW transaction and
    owes its own row lock.
    """
    from nso_adapter.core.importer import refresh_all_surfaces_for_device

    try:
        await _guard(db, reg)
        device = await db.get(Device, device_id)
        if device is None:
            return
        degraded, _supplier = await refresh_all_surfaces_for_device(
            db, device, client, refresh_source="onboard", atomic=True
        )
        await _guard(db, reg)
        await db.commit()
        if degraded:
            logger.warning("device.onboard_mirror.partial", device_id=device_id, degraded_surfaces=sorted(degraded))
        else:
            logger.info("device.onboard_mirror.done", device_id=device_id)
    except ClaimLostError:
        # Revocation is not a runner error: recovery already owns the disposition.
        raise
    except Exception as exc:  # noqa: BLE001 — never fail provisioning on a mirror-read hiccup
        await db.rollback()
        logger.warning("device.onboard_mirror.failed", device_id=device_id, error=repr(exc))


async def _map_and_seed_failover(
    db: AsyncSession,
    nso_instance: str,
    device_name: str,
    netbox_device_id: int | None,
    address: str,
    oob_ip: str | None,
    active_address: str,
    steps: list[dict],
    *,
    reg: ClaimRegistration | None = None,
    job_id: int | None = None,
) -> int | None:
    """Create the adapter mapping row and seed the failover row; return the device_id or None.

    Shared by the happy path and the OOB-bootstrap failure recovery so a device NSO was pinned
    to its OOB address is always handed to the failover loop — never stranded on OOB with no
    DeviceFailover row to fail it back once the in-band address recovers.

    A :class:`ClaimUnavailableError` from the mapping propagates: the provision fails
    retryably rather than continuing into the post-map phase unserialized. Nothing has been
    written to the device at that point, on any branch.
    """
    device_id = None
    if netbox_device_id is not None:
        try:
            row = await onboard_device(db, nso_instance, device_name, netbox_device_id, reg=reg, job_id=job_id)
            device_id = row.id
            steps.append({"step": "adapter_mapping", "status": "ok"})
        except LookupError as exc:
            steps.append({"step": "adapter_mapping", "status": "exists", "detail": repr(exc)})
    fo_seed = await _seed_onboarding_failover(db, device_id, address, oob_ip, active_address, reg=reg)
    if fo_seed:
        steps.append(fo_seed)
    return device_id


async def _seed_onboarding_failover(
    db: AsyncSession,
    device_id: int | None,
    primary: str,
    oob_ip: str | None,
    active_address: str,
    *,
    reg: ClaimRegistration | None = None,
) -> dict | None:
    """Seed the failover row at onboarding (when enabled). Returns a step dict, or None."""
    if not (get_config().scheduler.enable_failover and device_id is not None and (oob_ip or primary)):
        return None
    from nso_adapter.core.failover import set_initial_failover_state

    try:
        await _guard(db, reg)  # device state, committed below: guarded like every other write
        await set_initial_failover_state(db, device_id, primary, oob_ip, active_address)
        await db.commit()
        return {"step": "failover_seed", "status": "ok", "detail": active_address}
    except ClaimLostError:
        # Revocation is not a runner error: recovery already owns the disposition.
        raise
    except Exception as exc:
        # Best-effort means the STEP is reported and provisioning continues — but the failed
        # transaction has to go, or the mirror refresh and the runner's terminal write both
        # die of PendingRollbackError on a device that mapped perfectly well.
        await db.rollback()
        return {"step": "failover_seed", "status": "failed", "detail": repr(exc)}


async def rekey_device(
    db: AsyncSession,
    device: Device,
    nso_instance: str | None = None,
    nso_device_name: str | None = None,
) -> Device:
    """Atomically change source identity and invalidate every read publication."""
    from sqlalchemy import delete, update

    cfg = get_config()
    known_instances = {inst.name for inst in cfg.nso_instances}

    if nso_instance is not None and nso_instance not in known_instances:
        raise ValueError(f"NSO instance {nso_instance!r} not found in config")

    device_id = device.id
    for family in ALL_FAMILY_KEYS:
        await outcome_store.acquire_family_fence(db, device_id, family)
    # Another rekey may have committed while this request was waiting for the
    # canonical all-family fence. Re-read identity and generation under the fence.
    await db.refresh(device)

    target_instance = nso_instance if nso_instance is not None else device.nso_instance
    target_name = nso_device_name if nso_device_name is not None else device.nso_device_name
    if (target_instance, target_name) == (device.nso_instance, device.nso_device_name):
        await db.commit()  # release transaction-scoped advisory locks
        await db.refresh(device)
        return device
    # Check that the target pair is not already claimed while publication is fenced.
    dup = await db.execute(
        select(Device).where(
            Device.nso_instance == target_instance,
            Device.nso_device_name == target_name,
            Device.id != device.id,
        )
    )
    if dup.scalar_one_or_none():
        raise LookupError(f"NSO device {target_name!r} on {target_instance!r} is already claimed by another device")

    device.nso_instance = target_instance
    device.nso_device_name = target_name
    device.source_epoch += 1

    # Child rows use ON DELETE CASCADE where applicable. Interfaces retain the
    # established explicit cleanup because their oldest FKs predate DB cascades.
    iface_ids_result = await db.execute(select(DbInterface.id).where(DbInterface.device_id == device.id))
    iface_ids = list(iface_ids_result.scalars().all())
    if iface_ids:
        from nso_adapter.store.models import InterfaceAttrState, InterfaceIntent, InterfaceIpIntent

        await db.execute(delete(InterfaceAttrState).where(InterfaceAttrState.interface_id.in_(iface_ids)))
        intent_iface_ids = set(
            (await db.execute(select(InterfaceIntent.interface_id).where(InterfaceIntent.interface_id.in_(iface_ids))))
            .scalars()
            .all()
        )
        intent_iface_ids.update(
            (
                await db.execute(
                    select(InterfaceIpIntent.interface_id).where(InterfaceIpIntent.interface_id.in_(iface_ids))
                )
            )
            .scalars()
            .all()
        )
        # Interface intents are operator-owned state, not a read mirror. Keep their
        # minimal interface identity anchor so the next source read reuses the
        # same row by name and the intent/history survives the rekey.
        if intent_iface_ids:
            await db.execute(
                delete(DbInterface).where(
                    DbInterface.device_id == device.id,
                    DbInterface.id.not_in(intent_iface_ids),
                )
            )
            await db.execute(
                update(DbInterface)
                .where(DbInterface.id.in_(intent_iface_ids))
                .values(parent_binding=None, kind=None, encap_tag=None, vrf=None, service=None)
            )
        else:
            await db.execute(delete(DbInterface).where(DbInterface.device_id == device.id))
    for table_name in _READ_MIRROR_ROOTS:
        if table_name == "interfaces":
            continue  # handled above so operator-owned interface-intent anchors survive
        table = Base.metadata.tables[table_name]
        await db.execute(delete(table).where(table.c.device_id == device.id))
    await db.execute(delete(RefreshOutcomePointer).where(RefreshOutcomePointer.device_id == device.id))
    await db.execute(delete(ManagedScope).where(ManagedScope.device_id == device.id))

    device.ned_id = None
    device.sw_version = None
    device.mapping_status = MappingStatus.mapped
    device.last_sync_at = None
    device.last_sync_status = None
    device.degraded_surfaces = None

    await db.commit()
    await db.refresh(device)
    logger.info("device.rekeyed", device_id=device.id, nso_device=device.nso_device_name)
    return device


def intent_root_models() -> list[Any]:
    """Return the direct intent roots on ``Device``, derived from the mapper.

    Never hard-coded: a family added later joins this list automatically instead of
    silently reintroducing a cascade that fires after the job null-out. The mapper alone
    is provably incomplete, though — ``InterfaceIntent`` and ``InterfaceIpIntent`` hang off
    ``DbInterface``, and teardown handles those separately.
    """
    return [
        rel.mapper.class_
        for rel in Device.__mapper__.relationships
        if rel.mapper.class_.__tablename__.endswith("_intent")
    ]


async def offboard_device(db: AsyncSession, device: Device) -> None:
    """Remove all adapter state for a device. Does not modify NetBox.

    Holds the device claim for the whole teardown, so it can never dismantle a device a
    runner (or the tombstone sweeper) is working on, and takes its locks in §3.9's order:
    ``device_claim -> devices -> intent/children -> jobs``. Deleting the intent roots
    explicitly, BEFORE ``jobs``, is what removes the deadlock against an intent endpoint
    that holds an intent row and reaches for the queued apply winner — leaving them to
    ``db.delete(device)``'s cascade puts them after the job null-out.

    Raises :class:`ClaimUnavailableError` when the device stays claimed for the whole
    wait budget.
    """
    from sqlalchemy import delete, update

    from nso_adapter.core.claim import (
        acquire_claim_or_refuse,
        terminalize_offboard_orphans_bulk,
        terminalize_queued_bulk,
    )
    from nso_adapter.store.models import (
        InterfaceAttrState,
        InterfaceIntent,
        InterfaceIpIntent,
        Job,
    )

    device_id = device.id
    reg = await acquire_claim_or_refuse(device_id, "teardown", timeout_s=get_config().intent_claim_wait_seconds)
    # The device delete cascades the claim row away with it, so a release afterwards would
    # find nothing and report a lost claim. Released here only when the teardown did NOT
    # get that far.
    claim_survives = True
    try:
        await lock_claim(db, reg)  # the guard, held to commit
        await db.execute(select(Device.id).where(Device.id == device_id).with_for_update())

        for model in intent_root_models():
            await db.execute(delete(model).where(model.device_id == device_id))

        # Delete in FK dependency order to avoid cascade-load on lazy="raise" relationships
        iface_ids_result = await db.execute(select(DbInterface.id).where(DbInterface.device_id == device_id))
        iface_ids = list(iface_ids_result.scalars().all())
        if iface_ids:
            await db.execute(delete(InterfaceAttrState).where(InterfaceAttrState.interface_id.in_(iface_ids)))
            # Restrictive FK (no ondelete) and the interface delete below is bulk SQL, which
            # bypasses the ORM relationship cascade — without this the delete raises.
            await db.execute(delete(InterfaceIntent).where(InterfaceIntent.interface_id.in_(iface_ids)))
            # This one's FK does carry ON DELETE CASCADE; deleted explicitly anyway so the
            # asymmetry with InterfaceIntent above is not read as an oversight.
            await db.execute(delete(InterfaceIpIntent).where(InterfaceIpIntent.interface_id.in_(iface_ids)))
        await db.execute(delete(DbInterface).where(DbInterface.device_id == device_id))
        await db.execute(delete(ManagedScope).where(ManagedScope.device_id == device_id))
        await db.flush()

        # Jobs last, and QUEUED rows are terminalized before the null-out: nulling a queued
        # job manufactures a non-provision claimless job, which breaks the worker's
        # claimless bypass — it would dispatch it with device_id=None against a device that
        # no longer exists.
        await terminalize_queued_bulk(
            db,
            device_id,
            error={
                "code": "device_offboarded",
                "message": "The device was offboarded before this job ran",
                "detail": {},
            },
        )
        # The teardown claim proves residual non-terminal jobs are orphaned executions.
        await terminalize_offboard_orphans_bulk(
            db,
            device_id,
            error={
                "code": "device_offboarded_orphan",
                "message": "The device was offboarded after this job lost its worker",
                "detail": {},
            },
        )
        # Null-out device_id on jobs so history is preserved (device_id is nullable by design)
        await db.execute(update(Job).where(Job.device_id == device_id).values(device_id=None))
        await db.delete(device)
        await db.commit()
        claim_survives = False
    finally:
        if claim_survives:
            # The body may have died with the guard's FOR UPDATE still pending in *db*;
            # a release through a fresh session would wait on our own lock.
            with suppress(Exception):
                await db.rollback()
            await release_claim(reg)
    logger.info("device.offboarded", device_id=device_id)


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
