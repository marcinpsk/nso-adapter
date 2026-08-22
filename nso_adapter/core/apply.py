# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Apply worker — push accepted intent to NSO (Phase 2).

Follows the flow described in docs/nso-adapter.md §7a:
  1. Snapshot intent into job.context
  2. Mark each in-scope attribute as 'deploying'
  3. Commit each (interface, attribute) via NSO reconcile-commit service
  4. On success: status → in_sync, update last_apply_at
  5. On failure: status → apply_failed, capture error in last_apply_error

DeviceClaim serializes execution. Auto-Apply admission joins only queued coalescible
Apply jobs, so running Apply jobs and other types permit successors.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime
from typing import Any, NamedTuple

import structlog
from sqlalchemy import inspect as sa_inspect
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.core.claim import BookkeepingOutcomeUnknown, ClaimLostError, JobError, internal_error, terminalize
from nso_adapter.core.generation import executing_generation, generation_execution_sections, note_write
from nso_adapter.core.projection import (
    INTERFACE_ATTRIBUTE_ELIGIBLE_STATES,
    hydrate_interface_execution,
    hydrate_section,
    intent_state,
    section_models,
)
from nso_adapter.core.static_route_plan import (
    SrPlan,
    authorized_clear_fields,
    build_plan,
    hydrate_static_route_apply_plan,
    recorded_static_route_apply_mode,
)
from nso_adapter.nso.apply import NsoApplyError
from nso_adapter.store.models import (
    BfdIntent,
    BgpRouterIntent,
    DbInterface,
    Device,
    DeviceSettings,
    InterfaceAttrState,
    InterfaceIntent,
    InterfaceIpIntent,
    InterfaceMtuIntent,
    IsisFlexAlgoIntent,
    IsisInterfaceIntent,
    IsisLevelIntent,
    IsisProcessIntent,
    Job,
    JobStatus,
    JobType,
    L2SapIntent,
    LoggingHostIntent,
    LoggingLevelsIntent,
    OspfInstanceIntent,
    OspfInterfaceIntent,
    RedistributionIntent,
    RoutePolicyObjectIntent,
    SnmpCommunityIntent,
    SnmpHostIntent,
    SnmpSystemInfoIntent,
    SnmpV3UserIntent,
    StaticRouteIntent,
    SubinterfaceIntent,
    SviIntent,
    SyncState,
    VlanIntent,
)

logger = structlog.get_logger(__name__)

# Statuses eligible for apply (decision Q in plan — force=True pushes all of these)
_FORCE_ELIGIBLE = set(INTERFACE_ATTRIBUTE_ELIGIBLE_STATES)
# force=False only pushes these
_NO_FORCE_ELIGIBLE = {
    SyncState.accepted,
    SyncState.apply_failed,
    SyncState.drifted,
}


def _nokia_routed_kind(iface) -> str | None:
    """Derive the SR OS router context (base|ies|vprn) for a Nokia routed interface.

    The adapter's ``DbInterface.kind`` is the interface *type* (physical/logical/loopback/
    lag); the router context comes from ``service``/``vrf``:
      * VPRN — ``service`` set and ``vrf`` == ``service`` (VPRN addrs carry vrf=service-name)
      * IES  — ``service`` set, global table (vrf empty)
      * Base — no service
    Returns None for non-routed interfaces (physical ports, LAGs) and for non-Nokia
    devices (where ``kind`` is unset) so the IP lands via the normal port/interface path.
    """
    if iface.kind not in ("logical", "loopback"):
        return None
    if iface.service:
        return "vprn" if (iface.vrf and iface.vrf == iface.service) else "ies"
    return "base"


def _nokia_attr_kind(iface) -> str | None:
    """SR OS context for a Nokia interface's description/admin-state write.

    Extends :func:`_nokia_routed_kind` (base|ies|vprn for an L3 routed interface) with ``lag``:
    a Nokia LAG's description/admin-state live under ``configure lag <lag-N>``, not a port and
    not a router interface. Physical ports (and non-Nokia interfaces) return ``None`` → the
    legacy ``configure port`` path. Distinct from ``_nokia_routed_kind`` because a LAG never
    carries an IP, so the IP path must keep returning ``None`` for it.
    """
    if iface.kind == "lag":
        return "lag"
    return _nokia_routed_kind(iface)


async def enqueue_apply(
    db: AsyncSession,
    device_id: int,
    force: bool = True,
    *,
    stream: str,
    settlement_cohort: int | None = None,
) -> Job | None:
    """Create or join a queued coalescible Apply carrier for a new generation.

    *stream* names the endpoint lane this write touched — the promotion protocol's unit
    (#1522 §G2). It is a required keyword, not an optional one: a call site that cannot say
    which lane it mutated cannot record a revision for it, and a default would silently
    attribute every such write to one family. It is the ENDPOINT's stream, never the
    document section: promoting ``interface_config`` for an address push would authorize the
    interface attributes a store-only repair left behind (#103).

    *settlement_cohort* groups this generation with other generations created by the same
    request. It stays NULL when this is the request's only promoted generation.

    Returns the new job, or ``None`` when admission finds a queued winner. It also returns
    ``None`` on a store-only request (the plugin's intent re-sync,
    tracker #103): reconciling the intent store must never trigger a device commit,
    so the auto-apply enqueue is suppressed alongside the shrink-removal one. The
    stream's ``desired_revision`` is still bumped — the store DID change — but nothing is
    promoted and no generation exists to deploy it.

    Note the callers gate this on the device's ``auto_apply``, so a device with auto-apply
    OFF records no revision here. Nothing is lost while the adapter deploys nothing for
    such a device; #1522 §H4's manual-Apply protocol is what needs the bump at the mutation
    site, and moving it there belongs with that change.
    """
    from nso_adapter.core.generation import attach_to_job, create_generation
    from nso_adapter.core.jobs import admit_coalescible_job
    from nso_adapter.core.request_flags import STORE_ONLY
    from nso_adapter.store.models import GenerationMode

    if STORE_ONLY.get():
        logger.info("apply.skipped_store_only", device_id=device_id)
        return None

    # The promotion and its immutable document, in THIS transaction and under the projection
    # lock note_write already took: the document is the state that authorized the job, not
    # whatever the store holds when a worker eventually picks it up.
    generation = await create_generation(
        db,
        device_id,
        streams=(stream,),
        mode=GenerationMode.networked,
        settlement_cohort=settlement_cohort,
    )

    # Atomic same-type QUEUED dedupe, inside a savepoint. Two properties matter to the
    # fifteen callers, all of which reach here with intent rows already mutated and
    # uncommitted: a conflict must not poison their transaction, and on a conflict the
    # queued winner is row-locked until they commit, so the worker cannot start it against a
    # snapshot older than the request that admitted it.
    #
    # A removal is enqueued BEFORE its apply by design, so rejecting on any active job
    # dropped the apply outright; and a running apply must not refuse its successor, because
    # the successor is what carries the newer intent.
    created, winner = await admit_coalescible_job(db, device_id, JobType.apply)
    job = created or winner
    if job is not None:
        # A refused attachment is not an error: the generation is not contiguous with what
        # that job already carries, so it waits for a job of its own (advance_device_generations).
        await attach_to_job(db, generation, job)
    else:
        logger.error("apply.generation_unattached", device_id=device_id, seq=generation.seq)
    if created is None:
        return None
    await db.flush()
    return created


async def _diff_interface_attributes(db, nso_apply, client, device_name: str, ifaces: dict, fmt=True) -> str:
    """Accumulated description/enabled native delta across every accepted interface attr.

    One isolated dry-run per accepted (description|enabled) slice — a failing slice is
    logged and skipped so it never blocks the others.
    """
    attr_delta = ""
    for iface in ifaces.values():
        rows = (
            (await db.execute(select(InterfaceIntent).where(InterfaceIntent.interface_id == iface.id))).scalars().all()
        )
        rk = _nokia_attr_kind(iface)
        for r in rows:
            if r.accepted_at is None or r.attribute not in ("description", "enabled"):
                continue
            try:
                delta = await nso_apply.apply_interface_attribute(
                    client=client,
                    device_name=device_name,
                    interface_name=iface.name,
                    attribute=r.attribute,
                    value=r.intent_value,
                    kind=rk,
                    service=iface.service if rk in ("ies", "vprn") else None,
                    parent_binding=iface.parent_binding,
                    encap_tag=iface.encap_tag,
                    dry_run=fmt,
                )
            except Exception as exc:  # noqa: BLE001 — preview must never fail hard
                logger.warning(
                    "apply_diff.scope_failed",
                    scope="interface_attribute",
                    device=device_name,
                    interface=iface.name,
                    error=repr(exc),
                )
                continue
            if delta and delta.strip():
                attr_delta += delta
    return attr_delta


async def _diff_interface_ips(db, nso_apply, client, device_name: str, ifaces: dict, fmt=True) -> str:
    """Accumulated native IP delta — one isolated dry-run per interface carrying IP intent."""
    ip_rows = (
        (await db.execute(select(InterfaceIpIntent).where(InterfaceIpIntent.interface_id.in_(list(ifaces) or [-1]))))
        .scalars()
        .all()
    )
    by_iface: dict[int, list] = {}
    for r in ip_rows:
        if r.accepted_at is None:
            continue  # gate on accepted_at, like the attribute preview and real apply eligibility
        by_iface.setdefault(r.interface_id, []).append(r)
    ip_delta = ""
    for iface_id, rows in by_iface.items():
        iface = ifaces[iface_id]
        rk = _nokia_routed_kind(iface)
        try:
            delta = await nso_apply.apply_interface_ips(
                client=client,
                device_name=device_name,
                interface_name=iface.name,
                ip_intent_rows=rows,
                kind=rk,
                service=iface.service if rk in ("ies", "vprn") else None,
                parent_binding=iface.parent_binding,
                encap_tag=iface.encap_tag,
                dry_run=fmt,
            )
        except Exception as exc:  # noqa: BLE001 — preview must never fail hard
            logger.warning(
                "apply_diff.scope_failed",
                scope="interface_ip",
                device=device_name,
                interface=iface.name,
                error=repr(exc),
            )
            continue
        if delta and delta.strip():
            ip_delta += delta
    return ip_delta


# ── #1396 R2 §4.1/§4.2/§4.8 — the guarded static-route PUT-replace ───────────
#
# One snapshot feeds BOTH the retained-entry computation and the collateral guard, and one
# body builder feeds both the real apply and the preview — that shared derivation is what
# makes the previewed payload byte-identical to the applied one (C2.6).

#: The scope fails with this code when the pre-PUT service read cannot be certified. A
#: destructive replace must never be built on a read that may be hiding both the tombstoned
#: entries it has to preserve and the collateral the guard has to see (§4.4).
SNAPSHOT_INCONCLUSIVE = "static_route_snapshot_inconclusive"


async def _static_route_snapshot(client, device, plan) -> tuple[dict | None, list[dict]]:
    """Read the live static-route service ONCE and derive the entries the PUT must retain.

    Returns ``(snapshot, retained)``. ``snapshot`` is the live instance body, or ``None``
    when the service is certifiably absent — nothing to retain and no orphan possible, so
    the PUT proceeds. Raises :class:`NsoApplyError` on an inconclusive read.

    *retained* are the live entries a tombstone still claims (by its own triple or by its
    ``deployed_key``) and that no body-rendered row re-asserts, kept **verbatim**: metric,
    tag and NED-specific leaves live only in the live copy, so reconstructing such an entry
    from the store triple would silently rewrite it.
    """
    from nso_adapter.core.static_route_plan import as_triple, triple_of
    from nso_adapter.nso.apply import _STATIC_ROUTE_SERVICE_PATH, static_route_entry_key

    state = await client.service_instance_state(_STATIC_ROUTE_SERVICE_PATH, device.nso_device_name)
    if state.inconclusive:
        raise NsoApplyError(
            SNAPSHOT_INCONCLUSIVE,
            f"static_route: could not certify the live service instance on {device.nso_device_name!r} "
            "— refusing to build a PUT-replace from an uncertified read",
            detail={"device": device.nso_device_name},
        )
    current = state.entry
    if not current:
        return current, []

    claimed: set[tuple[str, str, str]] = set()
    for tomb in plan.tombstones:
        claimed.add((tomb.vrf or "", tomb.prefix or "", tomb.next_hop or ""))
        deployed = as_triple(tomb.deployed_key)
        if deployed is not None:
            claimed.add(deployed)
    reasserted = {triple_of(row) for row in plan.rows}
    keep = claimed - reasserted  # a key a live row still renders needs no retention
    retained = [entry for entry in (current.get("route") or []) if static_route_entry_key(entry) in keep]
    return current, retained


async def _put_static_routes(client, device, plan, *, dry_run=False, outbox: dict | None = None):
    """Send the guarded PUT-replace of the whole static-route service instance (§4.1).

    The guard sees the same snapshot the retained entries came from, and ``plan.allowed``
    names the keys it may watch disappear — the replacement predecessors this apply is
    delivering, plus (X4 belt) the tombstone keys the retention already re-asserts.
    """
    from nso_adapter.core.removal import _guarded_apply
    from nso_adapter.core.static_route_plan import triple_of
    from nso_adapter.nso.apply import apply_static_routes, static_route_entry_key

    current, retained = await _static_route_snapshot(client, device, plan)

    async def _apply(**kwargs):
        return await apply_static_routes(
            client=client,
            device_name=device.nso_device_name,
            route_intent_rows=plan.rows,
            extra_entries=retained,
            **kwargs,
        )

    if dry_run:
        # Preview parity (§4.8): the same body, rendered as a native PUT dry-run. No guard
        # (the delta NSO returns already shows what would be retracted), no writes, nothing
        # consumed — _send_service_config routes replace+dry_run through native_dry_run
        # with method="put".
        return await _apply(replace=True, dry_run=dry_run)
    context = {"removed": {"route": [list(key) for key in sorted(plan.allowed)]}}
    verdict = await _guarded_apply(client, device, "static_route", context, _apply, current=current)
    if outbox is not None:
        outbox["verify"] = verdict
        # Exactly what the body carried: the rendered rows plus the tombstone entries kept
        # verbatim. The residue check subtracts these — a key still on the device because
        # this very PUT re-asserted it is intent, not a survivor (C3.8).
        outbox["sent_keys"] = {triple_of(row) for row in plan.rows} | {
            static_route_entry_key(entry) for entry in retained
        }
    return verdict


async def _enqueue_pending_clear_retract(db: AsyncSession, device, plan, *, reg=None) -> None:
    """Queue the networked retract a merge-PATCH apply structurally cannot deliver (§4.11).

    A cleared leaf only leaves the device on a networked PUT. In ``PUT`` mode this apply's
    own store-rendered body already omits it, so nothing is owed. In ``PATCH`` mode the
    renderer omits the leaf while the merge leaves it live — and reader-compare only checks
    the route KEY, so the row would otherwise be certified in sync over a stale value.

    Only the ``authorized`` half is deliverable (A1). A ``store_only`` clear was recorded by
    a request that may mutate the intent store but must never cause a device write; the
    apply's separate authorization covers the apply's OWN body, not a deletion job whose
    only purpose is to remove a leaf observed under ``?store_only=true``. Such an entry
    parks — the row stays unproven — until a later authorized push re-records the clear.

    Runs AFTER the apply's terminal transaction, so a plain failure here is logged, never
    raised: a second terminal write flipping a committed ``succeeded`` to ``failed`` would
    misreport rows the device really did accept. Nothing is lost either way — the carrier is
    store state, so the next apply re-derives exactly this decision.

    A LOST CLAIM is not that kind of failure and does propagate. The job this queues is a
    networked PUT; if the claim was revoked and reacquired while this apply was running, the
    successor may since have un-owned a route, and a stale retract queued behind its back
    would retract that deliberately detached config from the device. So the insert takes the
    claim lock first and holds it to COMMIT. An UNREGISTERED registration is the documented
    claimless lane and ``lock_claim`` no-ops on it — this transaction consumes no carrier and
    deletes no tombstone, so a claimless caller is not the programming error it would be there.
    """
    if plan.mode != "PATCH":
        return
    fields = sorted({f for row in plan.rows for f in authorized_clear_fields(row.pending_clear)})
    if not fields:
        return
    from nso_adapter.core.claim import ClaimRegistration, lock_claim
    from nso_adapter.core.removal import enqueue_removal

    try:
        await lock_claim(db, reg if reg is not None else ClaimRegistration())
        # retract=True, no removed/shrank: an un-own would make this a no-networking detach,
        # which can never deliver a clear. Ordinary admission, ordinary FIFO — no immediate
        # device write, so auto_apply pacing is untouched.
        # Recorded as a write of its own: this retract is derived from carrier state during
        # the run, so no request wrote the revision it promotes (#1522 §G2).
        await note_write(db, device.id, "static_route")
        job = await enqueue_removal(
            db,
            device_id=device.id,
            scope="static_route",
            # A pure clear deletes nothing, so it carries no deletion marking and nothing
            # of this run's un-owns can defer it: it exists only to network the clear.
            marking=None,
            defer_retract=False,
            promotes=("static_route",),
            retract=True,
        )
        await db.commit()
    except ClaimLostError:
        await db.rollback()
        raise
    except Exception as exc:  # noqa: BLE001 — the apply is already finalized
        await db.rollback()
        logger.warning("static_route.pending_clear_retract_enqueue_failed", device_id=device.id, error=repr(exc))
        return
    logger.info(
        "static_route.pending_clear_retract_enqueued",
        device_id=device.id,
        job_id=getattr(job, "id", None),
        fields=fields,
    )


def _static_route_coro(client, device, plan, *, dry_run=False, outbox: dict | None = None):
    """Build the static-route scope's coroutine for this plan — PUT-replace or today's merge.

    *outbox* collects what the send learned and the caller's bookkeeping needs: the §4.4
    proof ``verify`` verdict, and ``sent_keys`` — every route key the body actually carried,
    rendered rows plus verbatim tombstone retention. The residue check subtracts those: a
    predecessor key this apply deliberately re-asserted (a sibling row reclaimed it, or a
    tombstone still owns its entry) is not residue, it is intent (C3.8).
    """
    from nso_adapter.core.static_route_plan import triple_of
    from nso_adapter.nso.apply import apply_static_routes

    async def _run():
        if plan.mode == "PUT":
            return await _put_static_routes(client, device, plan, dry_run=dry_run, outbox=outbox)
        verdict = await apply_static_routes(
            client=client, device_name=device.nso_device_name, route_intent_rows=plan.rows, dry_run=dry_run
        )
        if outbox is not None:
            outbox["verify"] = verdict
            outbox["sent_keys"] = {triple_of(row) for row in plan.rows}
        return verdict

    return _run()


# ── #1396 R2 §4.4-§4.6 — proof, residue enforcement, CAS, per-route results ──

#: The scope failure a surviving predecessor key raises. Distinct from the writer-drop code:
#: the intent DID land, and what failed is the retraction of what it replaced.
RESIDUE_FOUND_CODE = "static_route_residue_found"

#: Per-route outcomes (§4.5). ``unproven`` is the honest third state R2 adds — the write was
#: accepted and nothing proves it, so nothing may be consumed and no green may be reported.
SR_IN_SYNC = "in_sync"
SR_APPLY_FAILED = "apply_failed"
SR_UNPROVEN = "unproven"


class SrProof(NamedTuple):
    """Everything the post-write reads established, before any of it is acted on."""

    #: The commit's native-verify verdict, or ``None`` when the send returned none at all.
    verify: str | None
    #: Per-row reader-compare evidence, ``{row pk: present|missing}``. Absent ⇒ unverifiable.
    evidence: dict[int, str]
    #: ``clean|found|unsupported|error`` over the consumed predecessor keys; ``None`` when no
    #: key was consumed, so no residue read was owed. Only ``clean`` may consume anything;
    #: ``found`` fails the scope, and the other two are inconclusive (§6/OQ-R2-1).
    residue: str | None
    #: The consumed predecessor keys still present on the device.
    survivors: list[tuple[str, str, str]]
    #: The device-state ``static-route`` entries by key — §4.11's per-field evidence plane.
    entries: dict[tuple[str, str, str], dict]


def static_route_fingerprint(row) -> str:
    """SHA-256 over the EXACT wire entry sent for *row*.

    Hashes the renderer's output, not a hand-picked field list, so the fingerprint cannot
    drift from the payload: every leaf the body carries moves it, and a store field with no
    wire form (``name``) does not.
    """
    from nso_adapter.nso.apply import static_route_entry

    encoded = json.dumps(static_route_entry(row), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest()


async def _static_route_device_state(client, device) -> tuple[str, dict]:
    """Read the certified ``static-route`` device-state section ONCE → ``(status, entries)``.

    One read serves both post-write consumers: the residue check over the consumed
    predecessor keys, and §4.11's per-field evidence for the clear carrier. ``status`` is
    ``ok`` / ``unsupported`` (the NED exports no such section — absence proves nothing) /
    ``error``; only ``ok`` yields entries, and only entries can consume anything.
    """
    from nso_adapter.core.removal import _VERIFY_BATCH_TIMEOUT, _live_family_sections, _verifier_section_status

    try:
        sections = await _live_family_sections(
            client, device.nso_device_name, ["static-route"], timeout=_VERIFY_BATCH_TIMEOUT
        )
        section = sections["static-route"]
        status = _verifier_section_status(section)
        if status != "ok":
            return ("unsupported" if status == "unknown" else "error"), {}
        entries: dict[tuple[str, str, str], dict] = {}
        for entry in section.get("route") or []:
            if isinstance(entry, dict):
                key = (
                    str(entry.get("vrf") or ""),
                    str(entry.get("prefix") or ""),
                    str(entry.get("next-hop") or ""),
                )
                entries[key] = entry
        return "ok", entries
    except ClaimLostError:
        # Revocation is not a read failure: swallowing it here would let a revoked holder
        # carry on to the bookkeeping under ownership it no longer has.
        raise
    except Exception as exc:  # noqa: BLE001 — a read-side failure is inconclusive, never a green
        logger.warning("static_route.device_state_read_failed", device_id=device.id, error=repr(exc))
        return "error", {}


async def _static_route_proof(client, device, plan, *, verify, evidence, consumed_keys, want_fields) -> SrProof:
    """Gather §4.4's evidence for this apply. Reads only — nothing is consumed here.

    The device-state read runs only when something depends on it: a consumed predecessor key
    to look for, or a clear carrier to prove empty. A plain merge-PATCH apply of never-edited
    rows therefore costs exactly what it costs today.
    """
    if not consumed_keys and not want_fields:
        return SrProof(verify, evidence, None, [], {})
    status, entries = await _static_route_device_state(client, device)
    residue: str | None = None
    survivors: list[tuple[str, str, str]] = []
    if consumed_keys:
        if status != "ok":
            residue = status
        else:
            survivors = sorted(key for key in consumed_keys if key in entries)
            residue = "found" if survivors else "clean"
            if survivors:
                logger.error(
                    "static_route.residue_found",
                    device_id=device.id,
                    survivors=[list(k) for k in survivors],
                )
    return SrProof(verify, evidence, residue, survivors, entries)


def _sr_row_proven(row, proof: SrProof, *, conclusive: bool) -> bool:
    """Whether *row*'s own key is proven landed — the CAS precondition (§4.4's table)."""
    return conclusive and proof.evidence.get(row.id) == "present"


async def _static_route_bookkeeping(
    db: AsyncSession,
    device,
    plan,
    proof: SrProof,
    *,
    put_delivered: bool,
    job_id: int,
    reg=None,
    send_failed: bool,
    stamp_of: dict | None = None,
) -> tuple[list[dict], tuple[int, int] | None, list[dict]]:
    """Consume, CAS and record — the ONE transaction §4.6 requires, minus its commit.

    Everything written here rides the caller's terminal transaction (row stamps, per-route
    results, job status), because a split leaves a closed replacement under a failed apply.
    The claim lock is taken FIRST and held to that commit: a revoked holder must not close a
    replacement or empty a carrier on behalf of a claim it no longer owns.

    *put_delivered* says whether a networked PUT actually carried the store-rendered body.
    Only then does the body omit a cleared leaf, so only then may a clear carrier be
    consumed — and only then can a replacement close at all. A merge-PATCH adds the new
    triple and leaves the predecessor live, so CASing over it would close the replacement
    while the old route is still on the device, permanently (C2.7).

    Returns ``(results, adjusted_scope_outcome | None, extra_failures)``.
    """
    from nso_adapter.core.claim import ClaimRegistration, lock_claim
    from nso_adapter.core.static_route_plan import pending_clear_fields, triple_of
    from nso_adapter.nso.apply import VERIFY_CONCLUSIVE

    # An unregistered registration is the documented claimless lane and lock_claim no-ops on
    # it — the same reading C1 shipped for the follow-on enqueue. A REGISTERED one that has
    # been revoked raises, and that propagates: recovery owns the disposition from there.
    #
    # no_autoflush is the lock ORDER, not tidiness: the scope pass has already dirtied intent
    # rows, and lock_claim's ORM SELECT would autoflush them first — taking intent-row locks
    # before the claim lock, the exact reverse of the order every claimed writer uses, which
    # is a deadlock against a successor holding the claim and waiting on those rows. The
    # stamps flush at COMMIT instead, behind the lock.
    with db.no_autoflush:
        await lock_claim(db, reg if reg is not None else ClaimRegistration())

    conclusive = proof.verify == VERIFY_CONCLUSIVE
    residue_blocks = proof.residue is not None and proof.residue != "clean"
    residue_found = proof.residue == "found"
    cas_by_row = {c.row_id: c for c in plan.cas}

    # A predecessor this apply was supposed to retract is still on the device. The intent
    # landed; what failed is the retraction — so the scope fails and NOTHING is consumed.
    # Built BEFORE the record: the per-route `error` is read off the row, so stamping the
    # residue verdict afterwards would report every route failed with no error at all.
    residue_message = ""
    residue_err: dict | None = None
    if residue_found:
        residue_message = (
            "static_route: the replaced route(s) "
            f"{[list(k) for k in proof.survivors]} are still on the device after the replace — "
            "the predecessor was not retracted, so the replacement stays open"
        )
        residue_err = {
            "code": RESIDUE_FOUND_CODE,
            "message": residue_message,
            "detail": {"residue": [list(k) for k in proof.survivors]},
        }

    results: list[dict] = []
    for row in plan.rows:
        stamp = row if stamp_of is None else stamp_of.get(_stamp_key(row))
        outcome = SR_UNPROVEN
        if send_failed or residue_found:
            outcome = SR_APPLY_FAILED
        elif proof.evidence.get(row.id) == "missing":
            outcome = SR_APPLY_FAILED
        elif _sr_row_proven(row, proof, conclusive=conclusive) and not residue_blocks:
            outcome = await _settle_proven_row(
                db,
                device,
                plan,
                proof,
                row,
                stamp=stamp,
                put_delivered=put_delivered,
                cas=cas_by_row.get(row.id),
            )
        if residue_err is not None and stamp is not None:
            stamp.last_apply_error = residue_err
        if outcome is SR_UNPROVEN:
            logger.warning(
                "static_route.route_unproven",
                job_id=job_id,
                device_id=device.id,
                row_id=row.id,
                route_id=row.route_id,
                verify=proof.verify,
                residue=proof.residue,
                evidence=proof.evidence.get(row.id, "unverifiable"),
                pending_clear=sorted(pending_clear_fields(row.pending_clear)),
            )
        results.append(
            {
                "route_id": row.route_id,
                "row_id": row.id,
                "key": list(triple_of(row)),
                "fingerprint": static_route_fingerprint(row),
                # R3 §4.5: the generation this verdict is about, and this route's OWN error.
                # Without them the consumer can only settle on presence and can only report
                # one shared message for every failed route of the scope.
                #
                # Scoped to `apply_failed` because the column outlives a pass: an atomic
                # commit that rolls back in a SIBLING scope leaves the static rows untouched
                # and `unproven`, still carrying an earlier apply's error — reporting it here
                # would date a superseded generation's failure to this one. Every
                # `apply_failed` outcome had its error written by THIS pass (send failure,
                # reader-compare miss, residue or a stage error), so nothing is lost.
                "generation": row.intent_generation,
                "outcome": outcome,
                "error": (
                    residue_err or row.last_apply_error or (stamp.last_apply_error if stamp is not None else None)
                )
                if outcome == SR_APPLY_FAILED
                else None,
            }
        )

    if residue_err is None:
        return results, None, []
    return results, (0, len(plan.rows)), [{"error": residue_message, "code": RESIDUE_FOUND_CODE}]


async def _settle_proven_row(db, device, plan, proof: SrProof, row, *, stamp, put_delivered: bool, cas) -> str:
    """CAS and consume for one row whose own key is proven present → its outcome.

    Two things can still block ``in_sync`` after the key proof: an undelivered replacement
    (a merge-PATCH left the predecessor live) and a clear carrier the per-field evidence
    cannot empty. Both mean the device holds something the store says it should not.
    """
    from nso_adapter.core.static_route_plan import (
        AUTHORIZED,
        STORE_ONLY,
        leaf_is_neutral,
        pending_clear_fields,
        replacement_open,
        triple_of,
    )
    from nso_adapter.store.static_route_store import CAS_ROW, CAS_TOMBSTONE, cas_deployed_key

    if stamp is None:
        # A successor changed or deleted this row before the selected generation ran.
        return SR_UNPROVEN
    if replacement_open(row) and not put_delivered:
        # The merge added the new triple and left the predecessor. Recording the new triple
        # as deployed would destroy the only pointer to what is still on the device.
        return SR_UNPROVEN

    if cas is not None:
        verdict = await cas_deployed_key(
            db,
            device_id=device.id,
            row_id=cas.row_id,
            route_id=cas.route_id,
            sent_triple=cas.sent_triple,
            expected_old=cas.expected_old,
            tombstone_id_watermark=plan.tombstone_id_watermark,
        )
        if verdict not in (CAS_ROW, CAS_TOMBSTONE):
            # Another session moved the row (or its carrier is ambiguous): no authority was
            # granted, so this apply proved nothing it may report as settled.
            return SR_UNPROVEN

    pending = pending_clear_fields(row.pending_clear)
    if not pending:
        return SR_IN_SYNC
    # The immutable document records the carrier as it stood when this generation was
    # created. A preceding removal generation can prove and consume some or all of that
    # carrier before this apply runs. The matching live stamp is safe evidence of that
    # predecessor outcome: the document/live join already compared every intent field and
    # authorization stamp, while pending_clear is apply bookkeeping by design.
    fulfilled = pending - pending_clear_fields(stamp.pending_clear)
    if not put_delivered:
        # Only the PUT path delivers a clear: the merge body omits the leaf but the merge
        # never drops one. A preceding removal can already have fulfilled the obligation.
        return SR_IN_SYNC if pending == fulfilled else SR_UNPROVEN
    entry = proof.entries.get(triple_of(row))
    device_proven = {field for field in pending - fulfilled if entry is not None and leaf_is_neutral(field, entry)}
    if device_proven:
        carrier = stamp.pending_clear or {}
        remaining_auth = sorted({*(carrier.get(AUTHORIZED) or ())} - device_proven)
        remaining_store = sorted({*(carrier.get(STORE_ONLY) or ())} - device_proven)
        stamp.pending_clear = (
            {AUTHORIZED: remaining_auth, STORE_ONLY: remaining_store} if (remaining_auth or remaining_store) else None
        )
        logger.info(
            "static_route.pending_clear_consumed",
            device_id=device.id,
            row_id=row.id,
            fields=sorted(device_proven),
        )
    proven = fulfilled | device_proven
    return SR_IN_SYNC if pending == proven else SR_UNPROVEN


async def _settle_static_routes(
    db: AsyncSession,
    device,
    client,
    plan,
    *,
    job_id: int,
    outbox: dict,
    evidence: dict[int, str],
    put_delivered: bool,
    send_failed: bool,
    scope_outcomes: dict,
    scope_failures: dict,
    reg=None,
    stamp_of: dict | None = None,
) -> list[dict] | None:
    """Run §4.4's proof and §4.5/§4.6's bookkeeping for the static-route scope.

    Shared by both apply implementations, because the atomic path is a separate early return
    with its own finalization — wiring this into the per-scope loop alone would leave every
    atomic apply CASing nothing and reporting no per-route outcome at all.

    *put_delivered* is False on the atomic path even for a ``PUT`` plan: atomic staging is
    merge-PATCH only and explicitly ignores ``replace`` (G4), so no store-rendered body was
    ever PUT and nothing may close a replacement or consume a clear. Returns ``None`` when
    the device has no static-route rows in this pass, so ``job.result`` gains no empty key.

    *send_failed* is the SEND's own verdict, captured before reader-compare folds its
    per-row findings into the same counter. The two are different facts: a failed send means
    nothing landed for anyone, while a reader-compare miss is per row — and reading the
    merged counter would make one silently-dropped route block its proven sibling's CAS,
    which is exactly the aggregate-instead-of-evidence mistake §4.4 rules out.
    """
    if not plan.rows:
        return None
    consumed = set() if send_failed or not put_delivered else _static_route_consumed_keys(plan, outbox.get("sent_keys"))
    want_fields = bool(put_delivered and not send_failed and any(_static_route_pending(row) for row in plan.rows))
    proof = await _static_route_proof(
        client,
        device,
        plan,
        verify=outbox.get("verify"),
        evidence=evidence,
        consumed_keys=consumed,
        want_fields=want_fields,
    )
    results, adjusted, extra_fails = await _static_route_bookkeeping(
        db,
        device,
        plan,
        proof,
        put_delivered=put_delivered,
        job_id=job_id,
        reg=reg,
        send_failed=send_failed,
        stamp_of=stamp_of,
    )
    if adjusted is not None:
        scope_outcomes["static_route"] = adjusted
        scope_failures.setdefault("static_route", []).extend(extra_fails)
    return results


def _static_route_pending(row) -> bool:
    """Whether *row* still owes a clear — either carrier half blocks a proven ``in_sync``."""
    from nso_adapter.core.static_route_plan import pending_clear_fields

    return bool(pending_clear_fields(row.pending_clear))


def _static_route_consumed_keys(plan, sent_keys) -> set:
    """Return the predecessor keys this apply claims to have retracted — the residue set (§4.4).

    A key the body re-asserted is excluded: another live row reclaimed it, or a tombstone
    still owns its entry verbatim. Finding it on the device afterwards is then the intended
    outcome, not residue (C3.8).
    """
    from nso_adapter.core.static_route_plan import as_triple, replacement_open

    consumed = set()
    for row in plan.rows:
        if replacement_open(row):
            old = as_triple(row.deployed_key)
            if old is not None:
                consumed.add(old)
    return consumed - set(sent_keys or ())


async def collect_apply_diff(db: AsyncSession, device_id: int, outformat: str = "native") -> dict[str, str]:
    """Read-only preview: the per-scope native device diff the next Apply would push.

    For each scope, the accepted owned intent is dry-run against NSO — with
    ``outformat="native"`` (default) NSO renders the device-native config it *would*
    push; ``outformat="cli"`` renders the NED-uniform ``+``/``-`` tree diff instead
    (the apply-preview "diff -u" panel). Nothing is committed either way.
    Returns ``{scope: native_delta}`` for scopes with a non-empty change (a scope already
    in sync yields an empty delta and is omitted). Never writes to NSO or the DB.

    Covers every scope ``run_apply`` pushes through the intent store: interface
    attributes/IPs, OSPF, IS-IS, BGP, route-policy, SNMP, static routes, logging, SVI,
    subinterfaces, VLANs, BFD, and L2 SAPs (LAG/switchport are pushed out-of-band by the
    plugin, not from this intent store, so they have no preview here). Each scope is a
    best-effort, isolated dry-run — one failing/slow scope never blocks the others.
    """
    from nso_adapter.core.importer import get_nso_client
    from nso_adapter.nso import apply as nso_apply

    device = await db.get(Device, device_id)
    if not device:
        return {}
    client = get_nso_client(device.nso_instance)
    device_name = device.nso_device_name
    # dry_run is bool|str down the apply stack: True = native, "cli" = tree diff.
    fmt: bool | str = "cli" if outformat == "cli" else True
    diffs: dict[str, str] = {}

    async def _accepted(model) -> list:
        """All rows of *model* for this device that have been accepted (Apply-eligible)."""
        rows = (await db.execute(select(model).where(model.device_id == device_id))).scalars().all()
        return [r for r in rows if getattr(r, "accepted_at", None) is not None]

    async def _record(scope: str, coro) -> None:
        """Run one scope's dry-run; store a non-empty delta. Never raise.

        A scope that blows up is reported IN the preview rather than silently omitted: the
        operator approves the apply from this panel, and an empty entry reads as "nothing to
        do". A body-builder error (a vault_ref the writer cannot render, an unmappable enum)
        is exactly what will fail the real apply, so it must be visible here first.
        """
        try:
            delta = await coro
        except Exception as exc:  # noqa: BLE001 — preview must never fail hard
            logger.warning("apply_diff.scope_failed", scope=scope, device=device_name, error=repr(exc))
            reason = getattr(exc, "message", None) or repr(exc)
            diffs[scope] = f"!! preview unavailable for this scope: {reason}"
            return
        if delta and delta.strip():
            diffs[scope] = delta

    ifaces = {
        i.id: i
        for i in (await db.execute(select(DbInterface).where(DbInterface.device_id == device_id))).scalars().all()
    }

    # ── Interface attributes + IPs (each accumulates across several dry-runs) ─────
    attr_delta = await _diff_interface_attributes(db, nso_apply, client, device_name, ifaces, fmt)
    if attr_delta.strip():
        diffs["interface_attribute"] = attr_delta
    ip_delta = await _diff_interface_ips(db, nso_apply, client, device_name, ifaces, fmt)
    if ip_delta.strip():
        diffs["interface_ip"] = ip_delta

    # ── Redistribution rows split by destination protocol (shared by ospf/isis/bgp) ──
    redist = await _accepted(RedistributionIntent)
    redist_ospf = [r for r in redist if r.dest_protocol == "ospf"]
    redist_isis = [r for r in redist if r.dest_protocol == "isis"]
    redist_bgp = [r for r in redist if r.dest_protocol == "bgp"]

    # Collect every remaining scope's accepted rows (BGP relationships eager-loaded,
    # like run_apply, so the dry-run sees scopes/peers/afs).
    ospf_inst = await _accepted(OspfInstanceIntent)
    ospf_iface = await _accepted(OspfInterfaceIntent)
    isis_iface = await _accepted(IsisInterfaceIntent)
    isis_proc = await _accepted(IsisProcessIntent)
    isis_flex = await _accepted(IsisFlexAlgoIntent)
    isis_levels = await _accepted(IsisLevelIntent)
    bgp = await _accepted(BgpRouterIntent)
    if bgp:
        from nso_adapter.core.bgp_load import attach_bgp_relationships

        await attach_bgp_relationships(db, bgp)
    rp = await _accepted(RoutePolicyObjectIntent)
    snmp_comm = await _accepted(SnmpCommunityIntent)
    snmp_user = await _accepted(SnmpV3UserIntent)
    snmp_host = await _accepted(SnmpHostIntent)
    snmp_sysinfo_rows = await _accepted(SnmpSystemInfoIntent)
    snmp_sysinfo = snmp_sysinfo_rows[0] if snmp_sysinfo_rows else None
    sr = await _accepted(StaticRouteIntent)
    # The preview has always previewed ACCEPTED rows (the real apply pushes the eligible
    # subset) — passing them as the eligible list keeps PATCH-mode previews byte-identical
    # to today's, while PUT mode derives its rows from the store regardless.
    sr_plan = await build_plan(db, device, eligible_rows=sr)
    lg = await _accepted(LoggingHostIntent)
    lgl_rows = await _accepted(LoggingLevelsIntent)
    lgl = lgl_rows[0] if lgl_rows else None
    svi = await _accepted(SviIntent)
    subif = await _accepted(SubinterfaceIntent)
    vlan = await _accepted(VlanIntent)
    bfd = await _accepted(BfdIntent)
    mtu = await _accepted(InterfaceMtuIntent)
    l2 = await _accepted(L2SapIntent)

    # (scope, trigger row-lists, lazy dry-run coroutine). A scope is previewed only when
    # at least one trigger list is non-empty; the coroutine is built lazily so skipped
    # scopes never construct an un-awaited dry-run.
    scopes = [
        (
            "ospf",
            [ospf_inst, ospf_iface, redist_ospf],
            lambda: nso_apply.apply_ospf_config(
                client=client,
                device_name=device_name,
                process_intent_rows=ospf_inst,
                interface_intent_rows=ospf_iface,
                redistribution_rows=redist_ospf,
                dry_run=fmt,
            ),
        ),
        (
            "isis",
            [isis_iface, isis_proc, redist_isis, isis_flex, isis_levels],
            lambda: nso_apply.apply_isis_interfaces(
                client=client,
                device_name=device_name,
                isis_intent_rows=isis_iface,
                isis_process_rows=isis_proc,
                redistribution_rows=redist_isis,
                flex_algo_rows=isis_flex,
                level_rows=isis_levels,
                dry_run=fmt,
            ),
        ),
        (
            "bgp",
            [bgp, redist_bgp],
            lambda: nso_apply.apply_bgp_config(
                client=client,
                device_name=device_name,
                router_intent_rows=bgp,
                redistribution_rows=redist_bgp,
                dry_run=fmt,
            ),
        ),
        (
            "route_policy",
            [rp],
            lambda: nso_apply.apply_route_policy_config(
                client=client,
                device_name=device_name,
                intent_rows=rp,
                ned_id=device.ned_id,
                dry_run=fmt,
            ),
        ),
        (
            "snmp",
            [snmp_comm, snmp_user, snmp_host, [snmp_sysinfo] if snmp_sysinfo else []],
            lambda: nso_apply.apply_snmp_config(
                client=client,
                device_name=device_name,
                community_intents=snmp_comm,
                v3_user_intents=snmp_user,
                host_intents=snmp_host,
                system_info_intent=snmp_sysinfo,
                dry_run=fmt,
            ),
        ),
        (
            "static_route",
            [sr],
            # Preview parity (§4.8): the same classifier, the same rows, the same retained
            # tombstone entries — so a replacement-open device previews the very PUT the
            # apply would send. Read-only: build_plan writes nothing and consumes nothing.
            lambda: _static_route_coro(client, device, sr_plan, dry_run=fmt),
        ),
        (
            "logging",
            [lg, [lgl] if lgl else []],
            lambda: nso_apply.apply_logging_config(
                client=client, device_name=device_name, host_intent_rows=lg, levels_intent_row=lgl, dry_run=fmt
            ),
        ),
        (
            "svi",
            [svi],
            lambda: nso_apply.apply_svi_config(
                client=client, device_name=device_name, svi_intent_rows=svi, dry_run=fmt
            ),
        ),
        (
            "subinterface",
            [subif],
            lambda: nso_apply.apply_subinterface_config(
                client=client, device_name=device_name, subif_intent_rows=subif, dry_run=fmt
            ),
        ),
        (
            "vlan",
            [vlan],
            lambda: nso_apply.apply_vlan_config(
                client=client, device_name=device_name, vlan_intent_rows=vlan, dry_run=fmt
            ),
        ),
        (
            "bfd",
            [bfd],
            lambda: nso_apply.apply_bfd_config(
                client=client, device_name=device_name, bfd_intent_rows=bfd, dry_run=fmt
            ),
        ),
        (
            "interface_mtu",
            [mtu],
            lambda: nso_apply.apply_mtu_config(
                client=client, device_name=device_name, mtu_intent_rows=mtu, dry_run=fmt
            ),
        ),
        (
            "l2_sap",
            [l2],
            lambda: nso_apply.apply_l2_saps(client=client, device_name=device_name, sap_intent_rows=l2, dry_run=fmt),
        ),
    ]
    for scope, triggers, make_coro in scopes:
        if any(triggers):
            await _record(scope, make_coro())

    return diffs


# ── run_apply: shared eligibility + per-scope batch-commit helpers ────────────
#
# An "apply pass" pushes one scope's accepted intent to NSO and stamps the
# outcome back onto the rows. Every scope shares the same eligibility filter and
# the same success/failure bookkeeping, so that logic lives in the helpers below
# and ``run_apply`` just wires the scopes together. Each scope commits as one
# unit: on success every row gets ``last_apply_at`` and a cleared error; on any
# failure every row records the error payload and the scope reports one item.


# Result-dict keys in the order run_apply has always emitted them (also the order
# failed items are reported in). Interface attributes + IPs come first and are
# handled out-of-band (per-item, not per-batch); these are the batch scopes.
_SCOPE_RESULT_ORDER = (
    "snmp",
    "static_route",
    "logging",
    "svi",
    "subinterface",
    "vlan",
    "bfd",
    "interface_mtu",
    "l2_sap",
    "isis",
    "bgp",
    "route_policy",
    "ospf",
)


class _Scope(NamedTuple):
    """One per-scope apply pass: which rows to stamp and how to push them."""

    key: str  # result-dict key + failed-item "type"
    log_label: str  # structlog event infix ("apply.<label>_failed")
    rows: list  # every row stamped on success/failure
    make_coro: Callable[[], Awaitable]  # built lazily, only when the scope sends something
    on_nso_error: Callable[[NsoApplyError], Awaitable] | None = None
    #: What the BODY carries, when that is not ``rows``. A document-executed scope pushes
    #: the generation's hydrated document and stamps the live rows it carried, and those two
    #: lists differ whenever a successor moved a row (#1522 §G1).
    push: list | None = None
    #: Where a finding about a PUSHED row is recorded: ``{row id -> the live row}``. ``None``
    #: for a generationless scope, where the pushed row IS the live row. A pushed row absent from
    #: the map has no live counterpart left — it still fails the scope, it simply leaves no
    #: bookkeeping behind, because a transient hydrated row's stamp goes nowhere.
    stamp_of: dict | None = None

    @property
    def sent(self) -> list:
        """Return the rows this scope's body carries. Equal to ``rows`` for a generationless scope.

        Whether the scope RUNS is decided by this and never by ``rows``, and so is what the
        post-apply presence check looks for: an empty stamp list means the deployment records
        nothing, never that it sent nothing. Verifying ``rows`` let a successor-rewritten
        scope expect no keys at all, so NSO could drop the pushed ones and the run still
        settled (#1558 rework 3, finding 2).
        """
        return self.rows if self.push is None else self.push


def _is_eligible(row, force: bool) -> bool:
    """Report whether an accepted intent row is Apply-eligible.

    ``force=False`` additionally skips rows already applied cleanly (a non-null
    ``last_apply_at`` with no ``last_apply_error``) — only pending/failed rows go.
    """
    if row.accepted_at is None:
        return False
    if not force and row.last_apply_at is not None and row.last_apply_error is None:
        return False
    return True


async def _collect_eligible(db: AsyncSession, model, device_id: int, force: bool) -> list:
    """All Apply-eligible rows of *model* for this device (one device-scoped query)."""
    if model is InterfaceIpIntent:
        stmt = (
            select(InterfaceIpIntent)
            .join(DbInterface, InterfaceIpIntent.interface_id == DbInterface.id)
            .where(DbInterface.device_id == device_id)
        )
    else:
        stmt = select(model).where(model.device_id == device_id)
    rows = (await db.execute(stmt)).scalars().all()
    return [r for r in rows if _is_eligible(r, force)]


class _Rows(NamedTuple):
    """One model's contribution to an apply pass, split by what each half is FOR.

    *push* is what reaches the device AND what the post-apply presence check looks for;
    *stamp* is what records the outcome. They are the same list for a generationless job. For
    a generated one they differ on purpose: *push* is rebuilt from the generation's
    immutable document (transient rows that must never reach the store), while *stamp* is the
    LIVE rows this deployment actually carried — a row the successor added, changed or
    re-authorized after this generation was cut is not stamped by a deployment that never
    carried it.

    *stamp_of* joins the two: pushed-row model and id -> the live row a finding about it
    is recorded on. The model is part of the key because aggregate sections contain tables
    whose primary-key sequences overlap. ``None`` means the two lists are the same objects.
    """

    push: list
    stamp: list
    stamp_of: dict | None = None


def _stamp_key(row) -> tuple[type, object]:
    return type(row), getattr(row, "id", None)


def _stamp_join(document_rows: Sequence, live_rows: Sequence) -> dict[tuple[type, object], Any]:
    """Map document rows to live rows that still hold the same intent."""
    live_by_id = {row.id: row for row in live_rows}
    return {
        _stamp_key(row): live
        for row in document_rows
        if (live := live_by_id.get(row.id)) is not None and intent_state(live) == intent_state(row)
    }


def _combine_rows(*groups: _Rows) -> _Rows:
    """Combine one section's model collections without losing their stamp joins."""
    push = [row for group in groups for row in group.push]
    stamp = [row for group in groups for row in group.stamp]
    if all(group.stamp_of is None for group in groups):
        return _Rows(push=push, stamp=stamp)
    stamp_of = {
        key: row
        for group in groups
        for key, row in (group.stamp_of or {_stamp_key(live): live for live in group.stamp}).items()
    }
    return _Rows(push=push, stamp=stamp, stamp_of=stamp_of)


def _reject_transient_stamps(scope_label: str, rows) -> None:
    """Refuse to stamp a document-hydrated row. Both apply implementations call this.

    A hydrated row is TRANSIENT: setting ``last_apply_at`` on it writes to an object no
    session will ever flush, so the stamp vanishes and the row stays pending for ever while
    the job reports success. A scope that joins :data:`DOCUMENT_EXECUTED_SECTIONS` must pass
    its LIVE rows here; this makes getting that wrong a loud failure on the first apply
    instead of a silent one, on the per-scope path AND on the atomic one.
    """
    transient = [row for row in rows if sa_inspect(row).transient]
    if transient:
        raise RuntimeError(
            f"scope {scope_label!r} offered {len(transient)} transient row(s) to stamp — "
            "pass the live rows the generation's document carried, not the hydrated ones"
        )


class _Projection:
    """Where one apply run reads the rows it deploys (#1522 §G1).

    Built once per run. Every generated section comes from the executing generation's
    stored document, so a successor committing between the worker's ``running`` commit and
    this read cannot be deployed under the wrong generation's identity.

    A ``document`` of None is the one job that carries no generation: an Apply on a device
    nothing was ever written for. Its live read returns nothing either, so the two agree.
    """

    def __init__(
        self,
        db: AsyncSession,
        device_id: int,
        force: bool,
        document: dict | None,
        sections: frozenset[str] | None = None,
    ):
        self._db = db
        self._device_id = device_id
        self._force = force
        self._document = document
        self._sections = sections
        self._hydrated: dict[str, dict[type, list]] = {}
        self._live: dict[tuple[type, bool], list] = {}

    def _document_rows(self, section: str) -> dict[type, list]:
        if self._document is None:
            raise RuntimeError("document rows requested without a deployment document")
        if section not in self._hydrated:
            self._hydrated[section] = hydrate_section(self._document, section)
        return self._hydrated[section]

    async def collect(self, model, *, section: str, force: bool | None = None) -> _Rows:
        if model not in section_models({section}):
            raise ValueError(f"{model.__name__} does not belong to projection section {section!r}")
        if self._sections is not None and section not in self._sections:
            return _Rows(push=[], stamp=[])
        effective_force = self._force if force is None else force
        live_key = (model, effective_force)
        if live_key not in self._live:
            self._live[live_key] = await _collect_eligible(self._db, model, self._device_id, effective_force)
        live = self._live[live_key]
        if model is RedistributionIntent:
            live = [row for row in live if row.dest_protocol == section]
        if self._document is None:
            return _Rows(push=live, stamp=live)
        if self._sections is None and section not in self._document:
            return _Rows(push=[], stamp=[])
        document_rows = self._document_rows(section).get(model, [])
        push = [row for row in document_rows if _is_eligible(row, effective_force)]
        # Matched on CONTENT, not on the id alone. A successor push rewrites a row in place,
        # so the id it kept says nothing about whether this document carried what the row now
        # holds; stamping on the id would report the successor's intent as applied by a
        # deployment that never sent it. A row whose content moved simply stays pending and
        # the successor's own generation stamps it.
        stamp_of = _stamp_join(push, live)
        stamp = [row for row in live if _stamp_key(row) in stamp_of]
        return _Rows(push=push, stamp=stamp, stamp_of=stamp_of)


async def _maybe_sync_from(db: AsyncSession, client, device_name: str, device_id: int) -> None:
    """Best-effort pre-apply sync-from (per-device gated by DeviceSettings).

    A timed-out or partial prior commit leaves NSO's CDB inconsistent with the device;
    the next apply is then refused ("device out of sync"). Re-reading the device first
    clears it. A failure here must not abort the apply — the per-scope verify still
    catches real problems. ``sync_before_apply=False`` skips it for NEDs that already
    sync on connect.
    """
    settings_row = (
        await db.execute(select(DeviceSettings).where(DeviceSettings.device_id == device_id))
    ).scalar_one_or_none()
    if settings_row is not None and not settings_row.sync_before_apply:
        return
    try:
        await client.sync_from(device_name)
        logger.info("apply.sync_from.done", device=device_name)
    except Exception as exc:
        logger.warning("apply.sync_from.failed", device=device_name, error=str(exc))


async def _collect_attr_eligibility(db: AsyncSession, ifaces: dict, force: bool) -> tuple[list, list]:
    """Snapshot every interface-attribute intent row; return (snapshot, eligible sends).

    Eligibility for attributes is keyed off the attr_state's sync_state (not last_apply_at):
    every accepted row is snapshotted, but only those whose state is in the force/no-force
    set are returned for the per-attribute pass.
    """
    eligible_statuses = _FORCE_ELIGIBLE if force else _NO_FORCE_ELIGIBLE
    snapshot: list[dict] = []
    eligible: list[tuple] = []
    for iface in ifaces.values():
        intent_rows = (
            (await db.execute(select(InterfaceIntent).where(InterfaceIntent.interface_id == iface.id))).scalars().all()
        )
        for intent_row in intent_rows:
            attr_state = (
                await db.execute(
                    select(InterfaceAttrState).where(
                        InterfaceAttrState.interface_id == iface.id,
                        InterfaceAttrState.attribute == intent_row.attribute,
                    )
                )
            ).scalar_one_or_none()
            snapshot.append(
                {
                    "interface": iface.name,
                    "attribute": intent_row.attribute,
                    "intent_value": intent_row.intent_value,
                    "accepted_at": intent_row.accepted_at.isoformat() if intent_row.accepted_at else None,
                    "status_at_snapshot": attr_state.sync_state.value if attr_state else "unknown",
                }
            )
            if attr_state and attr_state.sync_state in eligible_statuses:
                eligible.append(_AttributeApply(attr_state, intent_row, iface, intent_row))
    return snapshot, eligible


async def _collect_ip_eligibility(db: AsyncSession, ifaces: dict, force: bool) -> tuple[list, dict]:
    """Snapshot every IP intent row; return (snapshot, {iface_id: [eligible rows]})."""
    snapshot: list[dict] = []
    by_iface: dict[int, list] = {}
    for iface in ifaces.values():
        ip_rows = (
            (await db.execute(select(InterfaceIpIntent).where(InterfaceIpIntent.interface_id == iface.id)))
            .scalars()
            .all()
        )
        for row in ip_rows:
            snapshot.append(
                {
                    "interface": iface.name,
                    "address": row.address,
                    "family": row.family,
                    "secondary": row.secondary,
                    "vrf": row.vrf,
                    "accepted_at": row.accepted_at.isoformat() if row.accepted_at else None,
                }
            )
            if _is_eligible(row, force):
                by_iface.setdefault(iface.id, []).append(row)
    return snapshot, by_iface


class _AttributeApply(NamedTuple):
    """One recorded attribute send and the live rows it may stamp."""

    state: InterfaceAttrState | None
    push: InterfaceIntent
    interface: DbInterface
    stamp: InterfaceIntent | None


async def _collect_document_interface(
    db: AsyncSession,
    source: _Projection,
    document: dict,
) -> tuple[dict, list[dict], list[_AttributeApply], list[dict], dict, _Rows]:
    """Hydrate interface rows and consume their creation-time execution context."""
    execution = hydrate_interface_execution(document)
    document_attr_rows = source._document_rows("interface_config").get(InterfaceIntent, [])
    interface_ids = list(execution.interfaces)
    live_attr_rows = (
        (await db.execute(select(InterfaceIntent).where(InterfaceIntent.interface_id.in_(interface_ids))))
        .scalars()
        .all()
        if interface_ids
        else []
    )
    attr_stamp_of = _stamp_join(document_attr_rows, live_attr_rows)
    ip_rows = await source.collect(InterfaceIpIntent, section="interface_config")
    states = (
        (await db.execute(select(InterfaceAttrState).where(InterfaceAttrState.interface_id.in_(interface_ids))))
        .scalars()
        .all()
        if interface_ids
        else []
    )
    state_by_key = {(state.interface_id, state.attribute): state for state in states}
    eligible: list[_AttributeApply] = []
    intent_snapshot: list[dict] = []
    for row in document_attr_rows:
        key = (row.interface_id, row.attribute)
        iface = execution.interfaces.get(row.interface_id)
        if iface is None:
            raise ValueError(f"interface_config document has no context for interface id {row.interface_id}")
        is_eligible = key in execution.eligible_attributes
        intent_snapshot.append(
            {
                "interface": iface.name,
                "attribute": row.attribute,
                "intent_value": row.intent_value,
                "accepted_at": row.accepted_at.isoformat() if row.accepted_at else None,
                "status_at_snapshot": "eligible" if is_eligible else "ineligible",
            }
        )
        if not is_eligible:
            continue
        stamp = attr_stamp_of.get(_stamp_key(row))
        eligible.append(_AttributeApply(state_by_key.get(key) if stamp is not None else None, row, iface, stamp))

    ip_by_iface: dict[int, list] = {}
    ip_snapshot: list[dict] = []
    for row in ip_rows.push:
        iface = execution.interfaces.get(row.interface_id)
        if iface is None:
            raise ValueError(f"interface_config document has no context for interface id {row.interface_id}")
        ip_by_iface.setdefault(row.interface_id, []).append(row)
        ip_snapshot.append(
            {
                "interface": iface.name,
                "address": row.address,
                "family": row.family,
                "secondary": row.secondary,
                "vrf": row.vrf,
                "accepted_at": row.accepted_at.isoformat() if row.accepted_at else None,
            }
        )
    return execution.interfaces, intent_snapshot, eligible, ip_snapshot, ip_by_iface, ip_rows


async def _collect_interface_apply_rows(
    db: AsyncSession,
    source: _Projection,
    generation,
    execution_sections: frozenset[str] | None,
    device_id: int,
    force: bool,
) -> tuple[dict, list[dict], list, list[dict], dict, dict | None]:
    """Select no rows, generationless live rows, or the recorded interface document."""
    if execution_sections is not None and "interface_config" not in execution_sections:
        return {}, [], [], [], {}, None
    if generation is not None:
        ifaces, intent_snapshot, attrs, ip_snapshot, ips, ip_rows = await _collect_document_interface(
            db,
            source,
            generation.document,
        )
        return ifaces, intent_snapshot, attrs, ip_snapshot, ips, ip_rows.stamp_of
    ifaces = {
        iface.id: iface
        for iface in (await db.execute(select(DbInterface).where(DbInterface.device_id == device_id))).scalars().all()
    }
    intent_snapshot, attrs = await _collect_attr_eligibility(db, ifaces, force)
    ip_snapshot, ips = await _collect_ip_eligibility(db, ifaces, force)
    return ifaces, intent_snapshot, attrs, ip_snapshot, ips, None


async def _apply_attributes(eligible, apply_fn, *, client, device_name, job_id, now) -> tuple[int, int, list]:
    """Commit each (interface, attribute) individually and transition its attr_state.

    Unlike the batch scopes, a per-attribute failure isolates to that one attribute.
    Returns (in_sync, apply_failed, failures).
    """
    ok = 0
    failed = 0
    failures: list[dict] = []
    for item in eligible:
        attr_state, intent_row, iface, stamp = item
        routed_kind = _nokia_attr_kind(iface)
        try:
            await apply_fn(
                client=client,
                device_name=device_name,
                interface_name=iface.name,
                attribute=intent_row.attribute,
                value=intent_row.intent_value,
                kind=routed_kind,
                service=iface.service if routed_kind in ("ies", "vprn") else None,
                parent_binding=iface.parent_binding,
                encap_tag=iface.encap_tag,
            )
        except NsoApplyError as exc:
            logger.error(
                "apply.attribute_failed",
                job_id=job_id,
                device=device_name,
                interface=iface.name,
                attribute=intent_row.attribute,
                error=exc.message,
            )
            if attr_state is not None and stamp is not None:
                attr_state.sync_state = SyncState.apply_failed
                stamp.last_apply_error = {"code": exc.code, "message": exc.message, "detail": exc.detail}
            failed += 1
            failures.append({"interface": iface.name, "attribute": intent_row.attribute, "error": exc.message})
        except ClaimLostError:
            # Revocation is not a per-row failure: continuing the loop would push
            # further scopes under ownership this run has lost.
            raise
        except Exception as exc:
            logger.exception(
                "apply.attribute_unexpected_error",
                job_id=job_id,
                interface=iface.name,
                attribute=intent_row.attribute,
            )
            if attr_state is not None and stamp is not None:
                attr_state.sync_state = SyncState.apply_failed
                stamp.last_apply_error = internal_error(exc)
            failed += 1
            failures.append(
                {"interface": iface.name, "attribute": intent_row.attribute, "error": internal_error(exc)["message"]}
            )
        else:
            if attr_state is not None and stamp is not None:
                attr_state.sync_state = SyncState.in_sync
                stamp.last_apply_at = now
                stamp.last_apply_error = None
            ok += 1
    return ok, failed, failures


async def _apply_ips(
    by_iface,
    ifaces,
    apply_fn,
    *,
    client,
    device_name,
    job_id,
    now,
    stamp_of: dict | None = None,
) -> tuple[int, int, list]:
    """Push IP intent one interface at a time (each interface is one commit unit).

    Returns (in_sync, apply_failed, failures); the counts are per-row, the failures
    per-interface.
    """
    ok = 0
    failed = 0
    failures: list[dict] = []
    for iface_id, ip_rows in by_iface.items():
        iface = ifaces[iface_id]
        routed_kind = _nokia_routed_kind(iface)
        try:
            await apply_fn(
                client=client,
                device_name=device_name,
                interface_name=iface.name,
                ip_intent_rows=ip_rows,
                kind=routed_kind,
                service=iface.service if routed_kind in ("ies", "vprn") else None,
                parent_binding=iface.parent_binding,
                encap_tag=iface.encap_tag,
            )
        except NsoApplyError as exc:
            logger.error("apply.ip_failed", job_id=job_id, device=device_name, interface=iface.name, error=exc.message)
            for row in ip_rows:
                stamp = row if stamp_of is None else stamp_of.get(_stamp_key(row))
                if stamp is not None:
                    stamp.last_apply_error = {"code": exc.code, "message": exc.message, "detail": exc.detail}
            failed += len(ip_rows)
            failures.append({"interface": iface.name, "error": exc.message})
        except ClaimLostError:
            # Revocation is not a per-row failure: continuing the loop would push
            # further scopes under ownership this run has lost.
            raise
        except Exception as exc:
            logger.exception("apply.ip_unexpected_error", job_id=job_id, interface=iface.name)
            for row in ip_rows:
                stamp = row if stamp_of is None else stamp_of.get(_stamp_key(row))
                if stamp is not None:
                    stamp.last_apply_error = internal_error(exc)
            failed += len(ip_rows)
            failures.append({"interface": iface.name, "error": internal_error(exc)["message"]})
        else:
            for row in ip_rows:
                stamp = row if stamp_of is None else stamp_of.get(_stamp_key(row))
                if stamp is not None:
                    stamp.last_apply_at = now
                    stamp.last_apply_error = None
            ok += len(ip_rows)
    return ok, failed, failures


_IFACE_CONFIG_ROOT = "interface-reconciler:interface-config"


def _build_interface_config_entries(attr_eligible, ip_by_iface, ifaces, device_name: str) -> list[dict]:
    """Merge interface description/enabled + IP intent into one entry per interface.

    Both ride the same ``(device, interface-name)``-keyed interface-reconciler instance, so in
    a single atomic edit they MUST be one list item — two items with a duplicate key conflict.
    """
    from nso_adapter.nso.apply import _coerce_enabled_intent, build_interface_ip_entry

    by_name: dict[str, dict] = {}

    def _entry(name: str) -> dict:
        return by_name.setdefault(name, {"device": device_name, "interface-name": name})

    for _attr_state, intent_row, iface, _stamp in attr_eligible:
        entry = _entry(iface.name)
        if intent_row.attribute == "description":
            entry["description"] = intent_row.intent_value if intent_row.intent_value is not None else ""
        elif intent_row.attribute == "enabled":
            # Shared strict coercion (raises on garbage) — same as the per-scope path, so a
            # corrupt value never silently disables the interface in the atomic body either.
            entry["enabled"] = _coerce_enabled_intent(intent_row.intent_value)

    for iface_id, rows in ip_by_iface.items():
        iface = ifaces[iface_id]
        routed_kind = _nokia_routed_kind(iface)
        ip_entry = build_interface_ip_entry(
            device_name,
            iface.name,
            rows,
            kind=routed_kind,
            service=iface.service if routed_kind in ("ies", "vprn") else None,
            parent_binding=iface.parent_binding,
            encap_tag=iface.encap_tag,
        )
        entry = _entry(iface.name)
        for key, value in ip_entry.items():
            if key not in ("device", "interface-name"):
                entry[key] = value

    return list(by_name.values())


_RP_ROOT = "route-policy-reconciler:route-policy-config"
_SR_ROOT = "static-route-reconciler:static-route-config"


def _device_error_message(exc) -> str | None:
    """Extract the device-parser error text from a failed atomic commit (``exc.detail['nso_error']``).

    Returns ``None`` when the failure was transport/internal (no device rejection) — so a
    transient failure (timeout, unreachable) never records a FALSE capability verdict. Only a
    real device rejection (the NED/device refused the commit) carries an ``nso_error`` payload.
    """
    nso_error = (getattr(exc, "detail", None) or {}).get("nso_error")
    if not isinstance(nso_error, dict):
        return None
    errors = (nso_error.get("ietf-restconf:errors") or {}).get("error") or []
    for e in errors:
        if isinstance(e, dict) and e.get("error-message"):
            return str(e["error-message"])
    return None


async def _localize_atomic_failure(client, device_name, modules, device_err) -> tuple[dict[str, str], tuple]:
    """Localise a failed atomic commit → ({offender root-key: its rejection message}, rp).

    ``rp`` is the route-policy ``(scope, name)`` construct parse. Two complementary signals: (1) a per-scope dry-run — a module the NED cannot compile re-runs
    to an inconclusive (``None``) delta in isolation; (2) the route-policy device-parser rejection,
    which renders clean in dry-run but is named in the *device* error (``device_err``). Each
    offender carries ITS OWN dry-run rejection message (H2): the combined-commit ``device_err``
    may describe a different module's failure, so per-construct attribution must read the
    message of the module that actually rejected. No recording here — the caller decides
    attribution (including the fall-back to all staged scopes) and whether it is a real device
    rejection before recording capability.
    """
    from nso_adapter.core.capability import parse_rejected_construct
    from nso_adapter.nso.apply import NsoApplyError, apply_combined

    offenders: dict[str, str] = {}
    for root_key, bodies in modules.items():
        try:
            # strict=True: only a CONCLUSIVE 4xx rejection in isolation flags this scope as an
            # offender. A transient/transport error (or 5xx) returns None / raises a non-NsoApplyError
            # — inconclusive, NOT an offender, so it never brands the scope a false 'unsupported'
            # that a later probe can't downgrade.
            await apply_combined(client, device_name, {root_key: bodies}, dry_run=True, strict=True)
        except NsoApplyError as exc:
            offenders[root_key] = str(exc.message or "")
        except Exception:  # noqa: BLE001 — transient/transport during localisation → inconclusive
            logger.debug("apply.localize.inconclusive", device=device_name, root_key=root_key)

    rp = parse_rejected_construct(device_err or "")
    if rp[1] and _RP_ROOT in modules:
        offenders.setdefault(_RP_ROOT, device_err or "")
    return offenders, rp


# Maps a staged module root-key → its result/scope key. interface-config is handled
# separately (it carries the attribute + IP scopes, which the result model splits out).
_ATOMIC_SCOPE_ROOTS: dict[str, str] = {
    "subinterface-reconciler:subif-config": "subinterface",
    "snmp-reconciler:snmp-config": "snmp",
    _SR_ROOT: "static_route",
    "logging-reconciler:logging-config": "logging",
    "svi-reconciler:svi-config": "svi",
    "vlan-reconciler:vlan-config": "vlan",
    "bfd-reconciler:bfd-config": "bfd",
    "mtu-reconciler:mtu-config": "interface_mtu",
    "l2-sap-reconciler:l2-sap-config": "l2_sap",
    "isis-reconciler:isis-config": "isis",
    "bgp-reconciler:bgp-config": "bgp",
    "route-policy-reconciler:route-policy-config": "route_policy",
    "ospf-reconciler:ospf-config": "ospf",
}


def _capability_scopes_for(root_key: str) -> list[str]:
    """Capability-matrix scope name(s) for a staged module root-key ([] if not tracked).

    The merged interface-config module carries BOTH the interface_attribute and interface_ip
    scopes (collect_apply_diff / preflight treat them separately), so a rejection of it must
    record capability under both — else a preflight for interface_attribute sees a false
    'fully supported'.
    """
    if root_key == _IFACE_CONFIG_ROOT:
        return ["interface_attribute", "interface_ip"]
    scope = _ATOMIC_SCOPE_ROOTS.get(root_key)
    return [scope] if scope else []


async def _record_atomic_capability(db, client, device, device_name, offenders, exc, rp, device_err) -> None:
    """Record a capability rejection for the attributed offender scopes.

    A per-scope dry-run rejection means the NED cannot compile that scope's intent on this
    ``(ned, sw)``; a device-parser rejection (``device_err``) means the device itself refused
    the commit — both are real, reactive capability gaps, recorded at scope granularity (or
    fine-grained for route-policy when ``rp = (scope, name)`` parses). H2: the merged
    interface-config module carries TWO scopes — when its OWN rejection message names a
    construct, only the offending half is recorded (construct-named); an unattributable
    message falls back to the coarse both-scopes record. Detail prefers each offender's own
    localisation message over the combined ``device_err`` (which may describe a different
    module). The ``(ned, sw)`` key is read from the device row, learned + persisted via the
    capability probe only when not already known.
    """
    from nso_adapter.core.capability import (
        _clean_capability_key,
        parse_rejected_iface_construct,
        record_capability_rejection,
        refresh_device_capability,
    )

    # Guard both keys against the literal 'None' (see _clean_capability_key) so a device row
    # carrying a stringified-None ned_id never becomes a bogus capability key.
    ned_id = _clean_capability_key(device.ned_id)
    sw = _clean_capability_key(device.sw_version)
    if not ned_id:
        info = await refresh_device_capability(db, client, device_name, device)
        if info:
            ned_id, sw = info.get("ned_id", ""), info.get("sw_version", "")
    if not ned_id:
        return

    rp_scope, rp_name = rp
    for root_key in offenders:
        own_msg = offenders.get(root_key, "") if isinstance(offenders, dict) else ""
        detail = (own_msg or device_err or exc.message or "")[:256]
        if root_key == _RP_ROOT and rp_name:
            await record_capability_rejection(db, ned_id, sw, rp_scope, rp_name, detail)
            continue
        if root_key == _IFACE_CONFIG_ROOT:
            scope, name = parse_rejected_iface_construct(own_msg or device_err or exc.message or "")
            if scope:
                await record_capability_rejection(db, ned_id, sw, scope, name, detail)
                continue
        for scope in _capability_scopes_for(root_key):
            await record_capability_rejection(db, ned_id, sw, scope, scope, detail)


async def _clear_atomic_capability(db, device, modules) -> None:
    """Clear stale reactive capability rejections after a clean atomic commit.

    A successful commit proves every staged scope applies on this ``(ned, sw)`` — the strongest
    positive signal — so drop any coarse ``apply``-sourced ``unsupported`` recorded by an earlier
    failed apply. Without this the gap would stick forever (a probe cannot downgrade an
    apply-rejection). Best-effort; the ``(ned, sw)`` key is read from the device row (no probe).
    """
    from nso_adapter.core.capability import _clean_capability_key, clear_capability_rejections

    ned_id = _clean_capability_key(device.ned_id)
    if not ned_id:
        return
    sw = _clean_capability_key(device.sw_version)
    scopes: set[str] = set()
    for root_key in modules:
        scopes.update(_capability_scopes_for(root_key))
    await clear_capability_rejections(db, ned_id, sw, scopes)


class _AtomicRows(NamedTuple):
    """The atomic path's three per-scope collections (#1558 rework 3, finding 2).

    *sent* is what each staged scope's body carried, *stamp* the live rows that record its
    outcome, *stamp_of* the ``{scope: {sent row id -> live row}}`` join. Replacing the staged
    rows with the stamp rows made a successor-rewritten scope verify an empty key set, so a
    dropped push settled.
    """

    sent: dict[str, list]
    stamp: dict[str, list]
    stamp_of: dict[str, dict]


async def _stage_atomic_modules(
    elig, client, device, device_name, *, sr_plan=None
) -> tuple[dict, list, _AtomicRows, dict]:
    """Build the combined ``/restconf/data`` body across every scope.

    Returns ``(modules, iface_entries, rows, stage_errors)``. Each scope stages its
    body via ``stage=modules`` (reusing its own body-builder, no HTTP); the interface-config
    module merges attribute + IP intent per interface.

    A scope whose body cannot be BUILT (a malformed vault_ref, an unmappable enum) is
    isolated into *stage_errors* rather than raising: the fault is deterministic and local
    to that scope, so the rest of the apply still commits.

    R2 §4.9: a ``PUT``-mode static-route plan is EXCLUDED from the combined body — staging
    is merge-PATCH only and explicitly ignores ``replace`` (G4), so staging it would send
    the new triple and leave the predecessor live while reporting success. The scope leaves
    ``scope_rows`` too, so nothing downstream stamps or reader-compares rows this
    transaction never carried; a follow-on PUT delivers them after the commit.

    ``scope_rows`` is the STAMP half and is not always the staged half (#1522 §G1): a
    document-executed scope stages the generation's hydrated document and stamps the LIVE
    rows it carried, which ``elig["stamp"]`` supplies per scope. Whether a scope is staged at
    all is still decided by what it PUSHES — an empty stamp list means this deployment
    records nothing, never that it sends nothing.
    """
    from nso_adapter.nso.apply import (
        apply_bfd_config,
        apply_bgp_config,
        apply_isis_interfaces,
        apply_l2_saps,
        apply_logging_config,
        apply_mtu_config,
        apply_ospf_config,
        apply_route_policy_config,
        apply_snmp_config,
        apply_static_routes,
        apply_subinterface_config,
        apply_svi_config,
        apply_vlan_config,
    )

    modules: dict[str, list] = {}
    iface_entries = _build_interface_config_entries(elig["attr"], elig["ip_by_iface"], elig["ifaces"], device_name)
    if iface_entries:
        modules[_IFACE_CONFIG_ROOT] = iface_entries

    stagers: list[tuple[str, list, Callable[[], Awaitable[Any]]]] = [
        (
            "subinterface",
            elig["subif"],
            lambda: apply_subinterface_config(client, device_name, elig["subif"], stage=modules),
        ),
        (
            "snmp",
            elig["snmp_rows"],
            lambda: apply_snmp_config(
                client,
                device_name,
                elig["snmp_comm"],
                elig["snmp_user"],
                elig["snmp_host"],
                elig["snmp_sysinfo"],
                stage=modules,
            ),
        ),
        (
            "static_route",
            elig["static_route"],
            lambda: apply_static_routes(client, device_name, elig["static_route"], stage=modules),
        ),
        (
            "logging",
            [*elig["logging"], *([elig["logging_levels"]] if elig["logging_levels"] else [])],
            lambda: apply_logging_config(
                client, device_name, elig["logging"], levels_intent_row=elig["logging_levels"], stage=modules
            ),
        ),
        ("svi", elig["svi"], lambda: apply_svi_config(client, device_name, elig["svi"], stage=modules)),
        ("vlan", elig["vlan"], lambda: apply_vlan_config(client, device_name, elig["vlan"], stage=modules)),
        ("bfd", elig["bfd"], lambda: apply_bfd_config(client, device_name, elig["bfd"], stage=modules)),
        ("interface_mtu", elig["mtu"], lambda: apply_mtu_config(client, device_name, elig["mtu"], stage=modules)),
        ("l2_sap", elig["l2_sap"], lambda: apply_l2_saps(client, device_name, elig["l2_sap"], stage=modules)),
        (
            "isis",
            [*elig["isis_iface"], *elig["isis_proc"], *elig["redist_isis"], *elig["isis_flex"], *elig["isis_levels"]],
            lambda: apply_isis_interfaces(
                client,
                device_name,
                elig["isis_iface"],
                elig["isis_proc"],
                elig["redist_isis"],
                elig["isis_flex"],
                elig["isis_levels"],
                stage=modules,
            ),
        ),
        (
            "bgp",
            [*elig["bgp"], *elig["redist_bgp"]],
            lambda: apply_bgp_config(client, device_name, elig["bgp"], elig["redist_bgp"], stage=modules),
        ),
        (
            "route_policy",
            elig["rp"],
            lambda: apply_route_policy_config(client, device_name, elig["rp"], ned_id=device.ned_id, stage=modules),
        ),
        (
            "ospf",
            [*elig["ospf_inst"], *elig["ospf_iface"], *elig["redist_ospf"]],
            lambda: apply_ospf_config(
                client, device_name, elig["ospf_inst"], elig["ospf_iface"], elig["redist_ospf"], stage=modules
            ),
        ),
    ]
    if sr_plan is not None and sr_plan.mode == "PUT":
        stagers = [entry for entry in stagers if entry[0] != "static_route"]
    # Three collections, not two: what the body carried (``sent_rows``, what the presence
    # check must look for), what records the outcome (``scope_rows``) and how one maps onto
    # the other (``stamp_of``). Keyed access, not a default: ``elig`` has ONE producer, and a
    # missing key here would silently stamp the hydrated rows again.
    stamp_rows = elig["stamp"]
    stamp_of = elig["stamp_of"]
    sent_rows = {key: rows for key, rows, _fn in stagers}
    scope_rows = {key: stamp_rows.get(key, rows) for key, rows, _fn in stagers}
    stage_errors: dict[str, NsoApplyError] = {}
    for key, rows, stage_fn in stagers:
        if not rows:
            continue
        try:
            await stage_fn()
        except NsoApplyError as exc:
            # This scope's BODY could not be built — a vault_ref that predates the
            # mount/path#key contract, an enum spelling the writer cannot map. Deterministic
            # and local to the scope: nothing was staged into `modules` (each builder
            # assembles its entry locally and only stages it at the very end), so drop the
            # offender and let the healthy scopes commit. Letting the raise escape failed the
            # ENTIRE job — interfaces, IPs, BGP, IS-IS — for one bad SNMP row.
            logger.error("apply.atomic_stage_failed", device=device_name, scope=key, error=exc.message)
            stage_errors[key] = exc
    return modules, iface_entries, _AtomicRows(sent_rows, scope_rows, stamp_of), stage_errors


def _stamp_attr_atomic(attr_eligible, commit_error, iface_failed, err, msg, now, snapshot) -> tuple[int, int, list]:
    """Stamp interface-attribute rows from the single atomic outcome.

    Pending (rolled-back, non-offender) attrs revert from ``deploying`` to their pre-apply
    snapshot state.
    """
    ok = failed = 0
    failures: list[dict] = []
    for attr_state, intent_row, iface, stamp in attr_eligible:
        if commit_error is None:
            if attr_state is not None and stamp is not None:
                attr_state.sync_state = SyncState.in_sync
                stamp.last_apply_at = now
                stamp.last_apply_error = None
            ok += 1
        elif iface_failed:
            if attr_state is not None and stamp is not None:
                attr_state.sync_state = SyncState.apply_failed
                stamp.last_apply_error = err
            failed += 1
            failures.append({"interface": iface.name, "attribute": intent_row.attribute, "error": msg})
        elif attr_state is not None:
            attr_state.sync_state = snapshot[attr_state]
    return ok, failed, failures


def _stamp_ip_atomic(ip_rows_flat, commit_error, iface_failed, err, msg, now, stamp_of=None) -> tuple[int, int, list]:
    """Stamp IP rows from the single atomic outcome (pending rows untouched, retried next apply)."""
    if commit_error is None:
        for row in ip_rows_flat:
            stamp = row if stamp_of is None else stamp_of.get(_stamp_key(row))
            if stamp is not None:
                stamp.last_apply_at = now
                stamp.last_apply_error = None
        return len(ip_rows_flat), 0, []
    if iface_failed:
        for row in ip_rows_flat:
            stamp = row if stamp_of is None else stamp_of.get(_stamp_key(row))
            if stamp is not None:
                stamp.last_apply_error = err
        return 0, len(ip_rows_flat), ([{"error": msg}] if ip_rows_flat else [])
    return 0, 0, []


async def _atomic_reader_compare(
    client, device, sent_rows, scope_outcomes, scope_failures, *, job_id, device_name, stamp_of=None
) -> tuple[dict[str, str], dict[str, list[str]], dict[str, dict[int, str]]]:
    """#108 presence check per staged scope after a clean atomic commit.

    *sent_rows* is what each staged scope's BODY carried and *stamp_of* maps those rows onto
    the live rows a finding is recorded on — the stamp rows are not the check's input, or a
    successor-rewritten scope would be checked for no keys at all.

    Every scope committed in ONE transaction → ONE post-commit point → ONE batched
    device-state action for all checkable wire_names (r1-m3: no per-scope enlargement on
    the atomic path). Prepares each eligible scope (expected + Vault translation), fetches
    the sections whose translated set is non-empty in a single action, then classifies each
    scope independently. A batched-action raise → every checkable scope records ``error``
    (non-fatal). Mutates scope_outcomes/scope_failures for scopes with silently-dropped keys
    and returns ``(reader_compare, reader_compare_unverifiable, evidence_by_scope)`` — the
    last being R2 §4.4's per-row map, which the static-route bookkeeping reads instead of the
    aggregate.
    """
    from nso_adapter.core.removal import _VERIFY_BATCH_TIMEOUT, _live_family_sections

    ned_id = getattr(device, "ned_id", None)
    preps: dict[str, Any] = {}  # scope → prep tuple | None (uncheckable) | "error" (translate raised)
    for key, rows in sent_rows.items():
        s_ok, s_failed = scope_outcomes.get(key, (0, 0))
        if not rows or s_failed:
            continue
        try:
            preps[key] = await _reader_compare_prepare(key, rows, ned_id)
        except Exception as exc:  # noqa: BLE001 — a scope's translation must never fail the apply
            logger.warning("apply.reader_compare_error", job_id=job_id, device=device_name, scope=key, error=repr(exc))
            preps[key] = "error"

    wires = sorted({prep[3] for prep in preps.values() if isinstance(prep, tuple) and prep[0]})
    sections: dict[str, dict] = {}
    action_error: Exception | None = None
    if wires:
        try:
            sections = await _live_family_sections(client, device.nso_device_name, wires, timeout=_VERIFY_BATCH_TIMEOUT)
        except Exception as exc:  # noqa: BLE001 — a batched read failure fails no scope's apply
            action_error = exc
            logger.warning("apply.reader_compare_error", job_id=job_id, device=device_name, error=repr(exc))

    reader_compare: dict[str, str] = {}
    reader_compare_unverifiable: dict[str, list[str]] = {}
    evidence_by_scope: dict[str, dict[int, str]] = {}
    for key, prep in preps.items():
        s_ok, _s_failed = scope_outcomes.get(key, (0, 0))
        if prep is None:
            continue  # structurally uncheckable → no entry
        if prep == "error":
            reader_compare[key] = "error"
            continue
        translated, unverifiable, spec, wire = prep
        evidence: dict[int, str] = {}
        fails: list[Any]
        if not translated:  # every key Vault-unverifiable → nothing to look for
            n_ok, n_failed, fails, status = s_ok, 0, [], "unknown"
        elif action_error is not None:
            n_ok, n_failed, fails, status = s_ok, 0, [], "error"
        else:
            try:
                # guarded (codex P2): a malformed 'ok' section must not let the walker's exception
                # escape and fail the whole atomic job — classify "error" for just this scope.
                n_ok, n_failed, fails, status, evidence = _classify_fetched_section(
                    key,
                    translated,
                    unverifiable,
                    spec,
                    sections[wire],
                    s_ok,
                    job_id=job_id,
                    device_name=device_name,
                    stamp_of=(stamp_of or {}).get(key),
                )
            except Exception as exc:  # noqa: BLE001 — a read-side glitch never fails a good commit
                logger.warning(
                    "apply.reader_compare_error", job_id=job_id, device=device_name, scope=key, error=repr(exc)
                )
                n_ok, n_failed, fails, status, evidence = s_ok, 0, [], "error", {}
        reader_compare[key] = status
        evidence_by_scope[key] = evidence
        if unverifiable:
            reader_compare_unverifiable[key] = unverifiable
        if n_failed:
            scope_outcomes[key] = (n_ok, n_failed)
            scope_failures.setdefault(key, []).extend(fails)
    return reader_compare, reader_compare_unverifiable, evidence_by_scope


async def _atomic_commit(
    db, client, device, device_name, modules, *, job_id: int
) -> tuple[NsoApplyError | None, str | None, dict, dict | None, str]:
    """Commit the combined transaction and localise its failure → the stamping inputs.

    Returns ``(commit_error, combined_verify, offenders, err, msg)``. *combined_verify* is
    the commit's §4.4 proof verdict, shared by every scope staged into it (G39).
    """
    from nso_adapter.nso.apply import apply_combined

    commit_error: NsoApplyError | None = None
    combined_verify: str | None = None
    if modules:
        try:
            combined_verify = await apply_combined(client, device_name, modules)
        except NsoApplyError as exc:
            commit_error = exc
        except Exception as exc:  # noqa: BLE001 — surface as a job-level failure
            commit_error = NsoApplyError("internal", repr(exc))
    else:
        # Nothing reached the combined body: every eligible scope either failed to stage or
        # left the transaction (a PUT-mode static-route plan, §4.9). An empty PATCH is a
        # pointless round trip whose verify verdict would then be attributed to a scope the
        # transaction never carried. C2 handed this case over: with atomic on, PUT mode and
        # force=False, `any_eligible` comes from plan.rows and the eligible list can be
        # empty — unreachable while the worker passes force=True, and it must stay so.
        logger.info("apply.atomic_nothing_staged", job_id=job_id, device=device_name)

    if commit_error is None:
        # Positive signal (I2): a clean commit clears any stale reactive 'unsupported' for the
        # applied scopes — a probe cannot downgrade an apply-rejection, so without this the gap
        # would stick forever even after the device is fixed / upgraded and the intent lands.
        try:
            await _clear_atomic_capability(db, device, modules)
        except Exception:  # noqa: BLE001 — capability bookkeeping is best-effort
            logger.debug("apply.atomic.capability_clear_skipped", job_id=job_id)
        return None, combined_verify, {}, None, ""

    logger.error("apply.atomic_failed", job_id=job_id, device=device_name, error=commit_error.message)
    device_err = _device_error_message(commit_error)
    offenders, rp = await _localize_atomic_failure(client, device_name, modules, device_err)
    # Capability (I2): record ONLY reliably-localised offenders — a per-scope dry-run the NED
    # cannot compile, or a parse_rejected_construct match (a known-unsupported construct named
    # in the device error). A generic device rejection is NOT a capability signal: it may be a
    # MISCONFIGURATION (e.g. a route-map referencing a prefix-list not included in the push),
    # not a NED limit — recording it would be a false "unsupported" verdict. Such failures
    # still fail the job + stamp last_apply_error (the operator sees the real device error).
    if offenders:
        try:
            await _record_atomic_capability(db, client, device, device_name, offenders, commit_error, rp, device_err)
        except Exception:  # noqa: BLE001 — capability recording is best-effort
            logger.debug("apply.atomic.capability_record_skipped", job_id=job_id)
    if not offenders:  # could not localise → the whole rolled-back commit is the failure
        offenders = dict.fromkeys(modules.keys(), "")
    err = {"code": commit_error.code, "message": commit_error.message, "detail": commit_error.detail}
    return commit_error, combined_verify, offenders, err, commit_error.message


async def _static_route_followon_put(
    client,
    device,
    device_name,
    plan,
    *,
    job_id: int,
    now,
    outbox: dict,
    scope_outcomes: dict,
    scope_failures: dict,
    reader_compare: dict,
    reader_compare_unverifiable: dict,
    stamp_rows: list,
    stamp_of: dict | None,
) -> tuple[bool, dict[int, str]]:
    """Deliver the ``PUT``-mode replacement the combined transaction cannot (§4.9).

    Runs only after ``apply_combined`` committed cleanly, and is the whole reason a
    ``PUT``-mode plan is excluded from that commit: staging ignores ``replace`` (G4), so
    without this the atomic path would merge the new triple, leave the predecessor on the
    device and close nothing — an honest ``unproven`` at best, a false green at worst.

    The scope's outcome, failures, reader-compare status and per-row evidence are produced
    exactly as the per-scope loop produces them, so the bookkeeping that follows cannot tell
    the two implementations apart. Returns ``(send_failed, evidence)`` — the send's OWN
    verdict, captured before reader-compare folds its per-row findings into the same counter.

    **Documented loss**: the replacement is NOT transactional with the other scopes. The
    combined commit has already landed when this PUT runs, so a failure here fails the job
    and stamps the static rows while every other scope stays applied.
    """
    from nso_adapter.core.removal import _VERIFY_PER_CALL_TIMEOUT

    scope_ok, scope_failed, fails = await _run_scope(
        "static_route",
        _static_route_coro(client, device, plan, outbox=outbox),
        stamp_rows,
        sent_rows=plan.rows,
        job_id=job_id,
        device_name=device_name,
        now=now,
    )
    send_failed = scope_failed != 0
    evidence: dict[int, str] = {}
    if not send_failed:
        scope_ok, scope_failed, fails, status, unverifiable, evidence = await _reader_compare_scope(
            client,
            device,
            "static_route",
            plan.rows,
            ok=scope_ok,
            job_id=job_id,
            device_name=device_name,
            timeout=_VERIFY_PER_CALL_TIMEOUT,
            stamp_of=stamp_of,
        )
        if status is not None:
            reader_compare["static_route"] = status
        if unverifiable:
            reader_compare_unverifiable["static_route"] = unverifiable
    scope_outcomes["static_route"] = (scope_ok, scope_failed)
    if fails:
        scope_failures.setdefault("static_route", []).extend(fails)
    return send_failed, evidence


def _stamp_batch_scopes_atomic(sent_rows, stamp_rows, offenders, commit_error, err, msg, now) -> tuple[dict, dict]:
    """Stamp every batch scope from the single atomic outcome → (scope_outcomes, scope_failures).

    Offending scopes fail; non-offending scopes are pending (rows untouched, retried next apply).
    """
    scope_outcomes: dict[str, tuple[int, int]] = {key: (0, 0) for key in _SCOPE_RESULT_ORDER}
    scope_failures: dict[str, list] = {}
    for root_key, scope_key in _ATOMIC_SCOPE_ROOTS.items():
        sent = sent_rows.get(scope_key) or []
        if not sent:
            continue
        stamps = stamp_rows.get(scope_key) or []
        _reject_transient_stamps(scope_key, stamps)
        if commit_error is None:
            for row in stamps:
                row.last_apply_at = now
                row.last_apply_error = None
            scope_outcomes[scope_key] = (len(sent), 0)
        elif root_key in offenders:
            for row in sent:
                row.last_apply_error = err
            for row in stamps:
                row.last_apply_error = err
            scope_outcomes[scope_key] = (0, len(sent))
            scope_failures[scope_key] = [{"error": msg}]
    return scope_outcomes, scope_failures


async def _run_atomic_apply(db, device, client, device_name, job, job_id, now, elig, *, sr_plan=None, reg=None) -> None:
    """I3b atomic apply: stage every scope into one transaction and commit once.

    On success, stamp every row in_sync; on failure the whole transaction rolled back —
    localise the offending scope(s), fail those rows (+ record capability), and leave
    non-offending scopes pending (untouched → retried next apply).

    R2 §4.4: the combined commit's verify verdict is threaded out of ``apply_combined``
    rather than discarded, and the static-route bookkeeping runs here too. Staging is
    merge-PATCH only and ignores ``replace`` (G4), so a ``PATCH``-mode plan delivers no
    replacement and no clear — ``put_delivered=False``, which is what keeps such an apply
    from closing a replacement the device never received.

    R2 §4.9: a ``PUT``-mode plan is instead excluded from the combined transaction and
    delivered by a follow-on PUT once that transaction has committed. The replacement is
    therefore NOT atomic with the other scopes — a deliberate, tested loss.
    """
    attr_eligible = elig["attr"]
    ip_rows_flat = [r for rows in elig["ip_by_iface"].values() for r in rows]

    # Snapshot attr states, then mark deploying (parity with the per-scope path); a pending
    # (rolled-back, non-offender) attr is reverted to its snapshot rather than left deploying.
    attr_stamps = [item.stamp for item in attr_eligible if item.stamp is not None]
    ip_stamps = list((elig.get("ip_stamp_of") or {}).values())
    _reject_transient_stamps("interface_config", [*attr_stamps, *ip_stamps])
    snapshot = {
        item.state: item.state.sync_state for item in attr_eligible if item.state is not None and item.stamp is not None
    }
    for attr_state in snapshot:
        attr_state.sync_state = SyncState.deploying
    await db.commit()

    sr_put_mode = sr_plan is not None and sr_plan.mode == "PUT"

    try:
        modules, iface_entries, rows, stage_errors = await _stage_atomic_modules(
            elig, client, device, device_name, sr_plan=sr_plan
        )
    except Exception:
        # An UNEXPECTED error while building the combined body (before any commit) — a real
        # bug, not a scope's own bad intent, which _stage_atomic_modules isolates. Revert the
        # attrs we just marked 'deploying' so they aren't stuck forever, then re-raise so
        # run_apply fails the job with the real error.
        for attr_state, state in snapshot.items():
            attr_state.sync_state = state
        await db.commit()
        raise

    # Scopes whose body could not be built never entered the transaction; the rest still
    # commit. Keep them out of every stage that assumes a scope was pushed.
    staged_stamp = {k: v for k, v in rows.stamp.items() if k not in stage_errors}
    staged_sent = {k: v for k, v in rows.sent.items() if k not in stage_errors}

    commit_error, combined_verify, offenders, err, msg = await _atomic_commit(
        db, client, device, device_name, modules, job_id=job_id
    )

    iface_failed = (_IFACE_CONFIG_ROOT in offenders) if iface_entries else False
    attr_outcome = _stamp_attr_atomic(attr_eligible, commit_error, iface_failed, err, msg, now, snapshot)
    ip_outcome = _stamp_ip_atomic(
        ip_rows_flat,
        commit_error,
        iface_failed,
        err,
        msg,
        now,
        stamp_of=elig.get("ip_stamp_of"),
    )
    scope_outcomes, scope_failures = _stamp_batch_scopes_atomic(
        staged_sent, staged_stamp, offenders, commit_error, err, msg, now
    )

    # A scope whose body could not be built failed on its own terms — it never reached the
    # device, so the commit outcome says nothing about it. Fail exactly its rows.
    for scope_key, stage_exc in stage_errors.items():
        stamped = rows.stamp.get(scope_key) or []
        _reject_transient_stamps(scope_key, stamped)
        stage_err = {"code": stage_exc.code, "message": stage_exc.message, "detail": stage_exc.detail}
        for row in rows.sent.get(scope_key) or []:
            row.last_apply_error = stage_err
        for row in stamped:
            row.last_apply_error = stage_err
        # Counted against what the body WOULD have carried: a scope whose live rows a
        # successor rewrote stamps none of them, and a (0, 0) outcome is a silent success.
        scope_outcomes[scope_key] = (0, len(rows.sent.get(scope_key) or []))
        scope_failures[scope_key] = [{"error": stage_exc.message}]

    # #108: a clean atomic commit rides the same FASTMAP writers — run the post-apply
    # presence check per staged scope and re-flag any silently-dropped keys. Unstaged
    # scopes are excluded: they were never pushed, so "not on the device" is not a drop.
    reader_compare: dict[str, str] = {}
    reader_compare_unverifiable: dict[str, list[str]] = {}
    evidence_by_scope: dict[str, dict[int, str]] = {}
    # Captured BEFORE reader-compare folds its per-row findings into the same counter.
    sr_send_failed = bool(scope_outcomes.get("static_route", (0, 0))[1])
    if commit_error is None:
        reader_compare, reader_compare_unverifiable, evidence_by_scope = await _atomic_reader_compare(
            client,
            device,
            staged_sent,
            scope_outcomes,
            scope_failures,
            job_id=job_id,
            device_name=device_name,
            stamp_of=rows.stamp_of,
        )

    # §4.9's follow-on: the PUT-mode replacement the combined transaction could not carry.
    # Only after a CLEAN commit — a rolled-back transaction leaves every non-offending scope
    # pending, and the static rows are no different for having been excluded from it.
    sr_outbox: dict = {"verify": combined_verify, "sent_keys": None}
    sr_evidence = evidence_by_scope.get("static_route", {})
    sr_put_delivered = False
    if sr_put_mode and commit_error is None:
        sr_outbox = {}
        sr_send_failed, sr_evidence = await _static_route_followon_put(
            client,
            device,
            device_name,
            sr_plan,
            job_id=job_id,
            now=now,
            outbox=sr_outbox,
            scope_outcomes=scope_outcomes,
            scope_failures=scope_failures,
            reader_compare=reader_compare,
            reader_compare_unverifiable=reader_compare_unverifiable,
            stamp_rows=elig["stamp"].get("static_route") or [],
            stamp_of=rows.stamp_of.get("static_route"),
        )
        sr_put_delivered = True

    sr_results = None
    if sr_plan is not None:
        sr_results = await _settle_static_routes(
            db,
            device,
            client,
            sr_plan,
            job_id=job_id,
            outbox=sr_outbox,
            evidence=sr_evidence,
            put_delivered=sr_put_delivered,
            send_failed=sr_send_failed,
            scope_outcomes=scope_outcomes,
            scope_failures=scope_failures,
            reg=reg,
            stamp_of=rows.stamp_of.get("static_route"),
        )

    await _finalize_job(
        db,
        job_id,
        device.id,
        True,
        attr_outcome,
        ip_outcome,
        scope_outcomes,
        scope_failures,
        reader_compare=reader_compare,
        reader_compare_unverifiable=reader_compare_unverifiable,
        static_route_results=sr_results,
        reg=reg,
    )

    if sr_put_delivered and not sr_send_failed:
        # The scope that LEFT the combined body still owes its capability bookkeeping:
        # `_clear_atomic_capability` only sees the roots that rode the commit, so without this
        # a stale apply-sourced `unsupported` for static_route would stick forever (a probe
        # cannot downgrade one) and /apply/preflight would keep warning about a scope that now
        # applies cleanly. Deferred past the terminal transaction because the clear COMMITS —
        # inline it would split the one transaction §4.6 requires.
        try:
            await _clear_atomic_capability(db, device, [_SR_ROOT])
        except Exception:  # noqa: BLE001 — capability bookkeeping is best-effort
            logger.debug("apply.atomic.capability_clear_skipped", job_id=job_id)


# Scope → (store model name, residue YANG-list label, row → key tuple), guard grain.
# Key tuples are the store keys verbatim — the same store↔YANG key equivalence the
# removal path already relies on. bgp and route_policy have bespoke expansion below.
_READER_COMPARE_SPECS: dict[str, list] = {
    "static_route": [("StaticRouteIntent", "route", lambda r: (r.vrf, r.prefix, r.next_hop))],
    "vlan": [("VlanIntent", "vlan", lambda r: (r.vlan_id,))],
    "svi": [("SviIntent", "interface", lambda r: (r.interface_name,))],
    "subinterface": [("SubinterfaceIntent", "interface", lambda r: (r.interface_name,))],
    "bfd": [("BfdIntent", "interface", lambda r: (r.interface_name,))],
    "interface_mtu": [("InterfaceMtuIntent", "interface", lambda r: (r.interface_name,))],
    "logging": [("LoggingHostIntent", "host", lambda r: (r.address,))],
    "l2_sap": [("L2SapIntent", "sap", lambda r: (r.service_name, r.sap_id))],
    "isis": [
        ("IsisInterfaceIntent", "interface-config", lambda r: (r.interface_name, r.af)),
        ("IsisProcessIntent", "process-config", lambda r: (r.process_tag,)),
    ],
    "ospf": [
        ("OspfInstanceIntent", "process-config", lambda r: (r.process_id,)),
        ("OspfInterfaceIntent", "interface-config", lambda r: (r.interface_name,)),
    ],
    "snmp": [
        # SnmpCommunityIntent's intent key is the human-readable label, while the export keys a
        # community by sha256(community-string)[:16] — a digest of a secret the adapter never sees
        # (it pushes a Vault triple; NSO resolves it). Demanding the LABEL be present would stamp
        # reader_compare_missing on every successful SNMP apply, so the row used to be left out of
        # the check entirely — leaving the one scope where a silent drop is a missing CREDENTIAL as
        # the only scope the drop-detector did not cover.
        #
        # CR-A17: the adapter holds the vault_ref, so it can resolve the secret and compute that
        # same digest. The key is emitted as the label here and TRANSLATED in _translate_expected
        # (which drops the row when Vault cannot answer — unverifiable, never "missing").
        ("SnmpCommunityIntent", "community", lambda r: (r.label,)),
        ("SnmpV3UserIntent", "v3-user", lambda r: (r.username,)),
        ("SnmpHostIntent", "host", lambda r: (r.address,)),
    ],
}


def _unrenderable_community_list(row, ned_id: str | None) -> bool:
    """Report whether EVERY member of this community-list is unrepresentable on the NED.

    apply_route_policy_config skips members the NED cannot hold, so such an object is
    emitted as ``{"name": …, "entry": []}`` — an empty community-list has no renderable CLI
    form, never lands on the device, and therefore can never appear in the export. It is a
    deliberate, already-reported codec skip (the PUT hands the plugin `unsupported_members`
    so it can badge them "unsupported on <ned>"), NOT a silent writer drop.

    Deterministic — a pure function of member + dialect, the same verdict the apply path
    acts on — so no device read is needed to decide it.
    """
    from nso_adapter.core.community_dialect import community_dialect_for

    if row.family != "community_list":
        return False
    members = {e.get("community") for e in (row.entries or []) if isinstance(e, dict) and e.get("community")}
    if not members:
        return False
    return len(community_dialect_for(ned_id).unrepresentable_members(sorted(members))) == len(members)


def _reader_compare_expected(scope: str, rows, ned_id: str | None = None) -> list[tuple[Any, str, tuple]]:
    """(intent row, YANG-list label, key tuple) for every checkable intended object (#108).

    Rows without a keyed reader presence are skipped: redistribution / flex-algo /
    level rows (nested non-keyed content, guard-grain parity), the snmp system-info
    scalar, and community-lists the NED cannot render at all
    (:func:`_unrenderable_community_list`).
    """
    from nso_adapter.core.removal import _ROUTE_POLICY_FAMILY_LISTS
    from nso_adapter.store import models as m

    if scope == "route_policy":
        return [
            (r, _ROUTE_POLICY_FAMILY_LISTS[r.family], (r.name,))
            for r in rows
            if isinstance(r, m.RoutePolicyObjectIntent)
            and r.family in _ROUTE_POLICY_FAMILY_LISTS
            and not _unrenderable_community_list(r, ned_id)
        ]
    if scope == "bgp":
        out: list[tuple[Any, str, tuple]] = []
        for r in rows:
            if not isinstance(r, m.BgpRouterIntent):
                continue
            out.append((r, "router", (r.asn,)))
            for sc in r.scopes:  # eagerly loaded by attach_bgp_relationships
                for p in sc.peers:
                    out.append((r, "peer", (p.peer_address,)))
        return out
    out = []
    for model_name, label, keyfn in _READER_COMPARE_SPECS.get(scope, []):
        model = getattr(m, model_name)
        out.extend((r, label, keyfn(r)) for r in rows if isinstance(r, model))
    return out


async def _translate_expected(scope: str, expected: list[tuple[Any, str, tuple]]) -> tuple[list, list[str]]:
    """Re-key the expected rows into the namespace the EXPORT uses (CR-A17).

    Identity for every grain but snmp/community, whose export key is ``sha256(secret)[:16]`` of a
    Vault-held community string. Returns ``(translatable, unverifiable_labels)``: a row whose key
    cannot be translated — no Vault provider, a Vault outage, a ref that no longer resolves — is
    DROPPED from the check rather than stamped ``reader_compare_missing``. Failing open is the only
    safe direction: a Vault blip must not permanently pin an SNMP scope apply_failed for a
    community that is sitting on the device exactly as intended.
    """
    from nso_adapter.core.removal import UNCOMPARABLE_LISTS
    from nso_adapter.core.snmp_verify import community_fingerprints

    if not any((scope, label) in UNCOMPARABLE_LISTS for _row, label, _key in expected):
        return expected, []

    refs = {
        str(row.label): row.vault_ref
        for row, label, _key in expected
        if (scope, label) == ("snmp", "community") and getattr(row, "vault_ref", None)
    }
    digests = await community_fingerprints(refs)

    out, unverifiable = [], []
    for row, label, key in expected:
        if (scope, label) not in UNCOMPARABLE_LISTS:
            out.append((row, label, key))
            continue
        digest = digests.get(str(getattr(row, "label", "")))
        if digest is None:
            unverifiable.append(f"{label} {list(key)}")
            continue
        out.append((row, label, (digest,)))
    return out, sorted(unverifiable)


def _reader_compare_checkable(scope, rows, ned_id) -> bool:
    """Whether *scope* has any keyed grain the post-apply presence check could verify.

    A cheap, Vault-free predicate (used to decide whether a budget-SKIPPED scope records
    ``unknown`` vs stays absent): a scope with no expected keyed grain — only nested
    non-keyed rows (redistribution / flex-algo / level) — is structurally uncheckable and
    keeps NO reader_compare entry, exactly as before (r3-M3).
    """
    from nso_adapter.core.removal import _RESIDUE_WIRE_NAMES, _guard_specs

    return bool(
        _reader_compare_expected(scope, rows, ned_id)
        and scope in _RESIDUE_WIRE_NAMES
        and _guard_specs().get(scope) is not None
    )


async def _reader_compare_prepare(scope, rows, ned_id):
    """Compute the translatable expected set for *scope* → ``(translated, unverifiable, spec, wire)``.

    Returns ``None`` when the scope is structurally uncheckable (no expected keyed grain / no
    envelope wire / no guard spec) — the caller records no reader_compare entry. Otherwise
    ``translated`` is the export-namespace expected set (may be empty when EVERY key is
    Vault-unverifiable → the caller records ``unknown`` and runs NO action, r2-m3), and
    ``unverifiable`` names the keys that could not be re-keyed (persisted symmetrically with
    the residue path's ``residue_unverifiable``). Runs the Vault translation (may block).
    """
    from nso_adapter.core.removal import _RESIDUE_WIRE_NAMES, _guard_specs

    expected = _reader_compare_expected(scope, rows, ned_id)
    spec = _guard_specs().get(scope)
    wire = _RESIDUE_WIRE_NAMES.get(scope)
    if not expected or spec is None or wire is None:
        return None
    translated, unverifiable = await _translate_expected(scope, expected)
    if unverifiable:
        # Named, never folded into "ok" silently: these keys were not checked at all.
        logger.warning("apply.reader_compare_unverifiable", scope=scope, keys=unverifiable)
    return translated, unverifiable, spec, wire


def _reader_compare_walk(
    scope: Any,
    translated: Any,
    unverifiable: Any,
    section: Any,
    spec: Any,
    ok: Any,
    *,
    job_id: Any,
    device_name: Any,
    stamp_of: Any = None,
) -> tuple[Any, Any, Any, Any, Any]:
    """Walk an ``ok`` device-state *section* for the presence of every translated key.

    Present-all → ``ok``, unless some grain was ``unverifiable`` (never checked) → ``partial``
    (the r3-M2 fix for the mixed community+host false-green — ``partial`` beats ``ok`` but
    ``missing`` still beats ``partial``). A missing key stamps its rows ``reader_compare_missing``
    and fails the scope. Returns ``(ok, failed, fails, status, evidence)``.

    *translated* names what the deployment SENT. *stamp_of* maps a sent row's model and id
    onto the live row that records a finding about it. It is ``None`` when the sent row IS
    the live row. A sent row with no live counterpart still FAILS the
    scope; it just leaves no ``last_apply_error`` behind, because stamping the transient
    hydrated row would write to an object no session flushes.

    *evidence* is the R2 §4.4 PER-ROW map ``{row pk: "present" | "missing"}``, returned
    alongside the unchanged aggregate. The aggregate collapses a two-row walk with one
    missing key into ``missing`` for BOTH, so a consumer that reads it CASes neither row —
    while the rule that a proven sibling must still CAS demands the opposite. A row absent
    from the map was never checked and its consumer must treat it as unverifiable.
    """
    from nso_adapter.core.removal import _norm_key, _reader_keys

    present = {gl.label: _reader_keys(scope, section, gl) for gl in spec.lists}
    row_by_id: dict[int, Any] = {}
    missing: dict[int, list[str]] = {}
    evidence: dict[int, str] = {}
    for row, label, key in translated:
        pk = getattr(row, "id", None)
        if _norm_key(key) in present.get(label, set()):
            # setdefault, never a plain assignment: a row can contribute several grains
            # (an IS-IS interface per address family), and one present grain must not
            # overwrite a sibling grain already found missing.
            if pk is not None:
                evidence.setdefault(pk, "present")
            continue
        if pk is not None:
            evidence[pk] = "missing"
        row_by_id[id(row)] = row
        missing.setdefault(id(row), []).append(f"{label} {list(key)}")
    if not missing:
        return ok, 0, [], ("partial" if unverifiable else "ok"), evidence
    fails = []
    for rid, keys in missing.items():
        msg = (
            f"post-apply device view is missing {', '.join(keys)} — the commit reported "
            f"success but the key(s) never landed (silent writer drop, #26 class)"
        )
        sent = row_by_id[rid]
        current_error = {
            "code": "reader_compare_missing",
            "message": msg,
            "detail": {"scope": scope},
        }
        # The sent row can be document-hydrated and have no live stamp after a successor
        # rewrite. Keep this pass's error on that in-memory carrier for the per-route result.
        sent.last_apply_error = current_error
        target = sent if stamp_of is None else stamp_of.get(_stamp_key(sent))
        if target is not None:
            target.last_apply_error = current_error
        fails.append({"error": msg})
    logger.error("apply.reader_compare_missing", job_id=job_id, device=device_name, scope=scope, missing=len(missing))
    # Clamped: ``ok`` counts rows this pass STAMPED, and a successor-rewritten scope stamps
    # none while still sending — and failing — several. The failure is carried by the count
    # beside it, never by a negative in_sync.
    return max(ok - len(missing), 0), len(missing), fails, "missing", evidence


def _classify_fetched_section(
    scope, translated, unverifiable, spec, section, ok, *, job_id, device_name, stamp_of=None
):
    """Classify a CERTIFIED device-state *section* → (ok, failed, fails, status, evidence).

    ``error`` (the family read errored) → ``error``; ``unsupported`` (no export surface — absence
    proves nothing) → ``unknown``; ``ok`` → walk. Shared by the default per-scope path and the
    batched atomic path; the section is already status-terminal thanks to client certification.
    An un-walked section yields an EMPTY per-row evidence map: nothing was checked, so no row
    may be reported present (R2 §4.4).
    """
    from nso_adapter.core.removal import _verifier_section_status

    status = _verifier_section_status(section)
    if status == "error":
        logger.warning(
            "apply.reader_compare_error", job_id=job_id, device=device_name, scope=scope, error="section status=error"
        )
        return ok, 0, [], "error", {}
    if status == "unknown":  # the NED does not export this family
        logger.info("apply.reader_compare_unknown", job_id=job_id, device=device_name, scope=scope)
        return ok, 0, [], "unknown", {}
    return _reader_compare_walk(
        scope, translated, unverifiable, section, spec, ok, job_id=job_id, device_name=device_name, stamp_of=stamp_of
    )


async def _reader_compare_scope(
    client: Any,
    device: Any,
    scope: Any,
    rows: Any,
    *,
    ok: Any,
    job_id: Any,
    device_name: Any,
    timeout: Any,
    stamp_of: Any = None,
) -> tuple[Any, Any, Any, Any, Any, Any]:
    """Post-apply presence check (#108) → (ok, failed, fails, status, unverifiable, evidence).

    ``_verify_native_or_raise`` re-diffs the committed payload against the CDB SERVICE
    tree — both sides sit behind the same FASTMAP writer, so a writer that silently
    drops an object is invisible (proven live on rg03, #26). This check reads the far
    side of the writer instead: the scope's device-state ``ACTION`` section — a fresh
    post-commit CDB extraction inside a whole-build txid bracket, read as soon as
    possible after the commit (the record-served facade is stale post-commit; the legacy
    subscriber-cache-backed getters can lag it). Every intended key must be present; a
    missing key stamps its rows ``reader_compare_missing`` (retryable — last_apply_error
    keeps them eligible) and fails the scope, so the plugin settles deploying→apply_failed
    on the immediate post-apply reconcile instead of waiting out stuck_deploying_grace_minutes.
    Status: "ok" / "partial" (checked keys present, some grain unverifiable) / "missing" /
    "unknown" (the NED has no export surface, or every key is Vault-unverifiable — absence
    proves nothing) / "error" (never fails a good apply on read trouble) / None (nothing
    checkable — e.g. only nested non-keyed rows in the batch). ``unverifiable`` names the keys
    that could not be re-keyed (recorded whenever non-empty, symmetric with residue).
    The ONE-family action runs inside the caller's wall-clock budget (*timeout*); an all-
    unverifiable scope returns "unknown" WITHOUT running it. NOT covered: NED/device-side
    divergence (needs check-sync) and redistribution rows (nested non-keyed, guard parity).
    """
    from nso_adapter.core.removal import _live_family_sections

    unverifiable: list[str] = []  # hoisted so the except still reports it (codex P3)
    try:
        prep = await _reader_compare_prepare(scope, rows, getattr(device, "ned_id", None))
        if prep is None:
            return ok, 0, [], None, [], {}
        translated, unverifiable, spec, wire = prep
        if not translated:  # every key Vault-unverifiable → nothing to look for, run NO action
            return ok, 0, [], "unknown", unverifiable, {}
        section = (await _live_family_sections(client, device.nso_device_name, [wire], timeout=timeout))[wire]
        # The walk stays INSIDE the try (codex P2): a malformed 'ok' section (a non-dict where a
        # keyed entry belongs) makes _reader_keys raise — that must classify "error", never escape
        # and turn a successful commit into an internal job failure.
        n_ok, n_failed, fails, status, evidence = _classify_fetched_section(
            scope,
            translated,
            unverifiable,
            spec,
            section,
            ok,
            job_id=job_id,
            device_name=device_name,
            stamp_of=stamp_of,
        )
        return n_ok, n_failed, fails, status, unverifiable, evidence
    except Exception as exc:  # noqa: BLE001 — the check must never fail a good apply
        logger.warning("apply.reader_compare_error", job_id=job_id, device=device_name, scope=scope, error=repr(exc))
        return ok, 0, [], "error", unverifiable, {}


async def _reader_compare_default_path(client, device, sc, scope_ok, *, remaining, ned_id, job_id, device_name):
    """Default-path per-scope verify under the HARD budget → (ok, failed, fails, status, unverifiable, evidence).

    ``remaining`` is the VERIFY budget still unspent (``_VERIFY_TOTAL_BUDGET`` minus the time already
    spent verifying earlier scopes — device COMMIT latency deliberately does NOT count, codex P1, so
    a slow early commit cannot starve later scopes of silent-drop detection). Budget spent → the scope
    is SKIPPED without running the action: a checkable scope records ``unknown`` (never silently
    absent), a structurally-uncheckable one keeps no entry. Otherwise the whole per-scope verify —
    translation + semaphore acquire + HTTP — runs inside a single ``asyncio.wait_for`` clipped to the
    remaining budget (so semaphore contention cannot push total default-path verify time past the
    budget), with the action's own timeout clipped to ``min(_VERIFY_PER_CALL_TIMEOUT, remaining)``. A
    cut verify → ``unknown``.
    """
    from nso_adapter.core.removal import _VERIFY_PER_CALL_TIMEOUT

    if remaining <= 0:
        status = "unknown" if _reader_compare_checkable(sc.key, sc.sent, ned_id) else None
        return scope_ok, 0, [], status, [], {}
    try:
        return await asyncio.wait_for(
            _reader_compare_scope(
                client,
                device,
                sc.key,
                # What was SENT, never what will be stamped: a successor-rewritten scope
                # stamps nothing and would otherwise be checked for nothing (finding 2).
                sc.sent,
                ok=scope_ok,
                job_id=job_id,
                device_name=device_name,
                timeout=min(_VERIFY_PER_CALL_TIMEOUT, remaining),
                stamp_of=sc.stamp_of,
            ),
            timeout=remaining,
        )
    except TimeoutError:  # asyncio.TimeoutError is TimeoutError on 3.11+
        # The whole per-scope verify (translate + semaphore + HTTP) blew the budget; only a
        # checkable scope ever reaches the action, so a cut verify is always "unknown".
        return scope_ok, 0, [], "unknown", [], {}


async def _run_scope(
    log_label, coro, rows, *, sent_rows=None, job_id, device_name, now, on_nso_error=None
) -> tuple[int, int, list]:
    """Push one scope's batch coroutine and stamp the outcome onto every row in *rows*.

    Returns (in_sync, apply_failed, failures), counted from *sent_rows*. Success stamps
    last_apply_at and clears the error on every row; an NsoApplyError or any other
    exception records the error payload on every row and reports a single failure.
    ``on_nso_error`` is a best-effort side-effect (route-policy uses it to record a
    device-parser capability rejection).

    A collateral block (the static-route PUT-replace, §4.1) gets its own clause ahead of
    the broad one: it is a REFUSAL with a machine-readable orphan report and a preview of
    the would-be device delta, and ``repr(exc)`` under ``code: "internal"`` would throw
    both away.
    """
    from nso_adapter.core.removal import RemovalBlockedError

    _reject_transient_stamps(log_label, rows)
    accounted_rows = rows if sent_rows is None else sent_rows

    def _record_current_error(err: dict) -> None:
        # ``rows`` are the live stamps. ``accounted_rows`` are what the body sent and can
        # be immutable document rows with no live counterpart. The result needs the latter
        # even when there is deliberately nothing safe to persist.
        for row in rows:
            row.last_apply_error = err
        for row in accounted_rows:
            row.last_apply_error = err

    try:
        await coro
    except NsoApplyError as exc:
        logger.error(f"apply.{log_label}_failed", job_id=job_id, device=device_name, error=exc.message)
        err = {"code": exc.code, "message": exc.message, "detail": exc.detail}
        _record_current_error(err)
        if on_nso_error is not None:
            await on_nso_error(exc)
        return 0, len(accounted_rows), [{"error": exc.message}]
    except ClaimLostError:
        # Revocation is not a runner error: recovery already owns the disposition.
        raise
    except RemovalBlockedError as exc:
        logger.error(f"apply.{log_label}_blocked_collateral", job_id=job_id, device=device_name, orphans=exc.orphans)
        err = {
            "code": "removal_blocked_collateral",
            "message": str(exc),
            "detail": {"orphans": exc.orphans, "preview": exc.preview},
        }
        _record_current_error(err)
        # The preview rides the JOB failure too, not just the rows: it is the would-be device
        # delta the operator has to review before deciding to force the replacement, and
        # GET /jobs/{id} is where they read it (the removal path already reports it there).
        return (
            0,
            len(accounted_rows),
            [
                {
                    "error": str(exc),
                    "code": "removal_blocked_collateral",
                    "orphans": exc.orphans,
                    "preview": exc.preview,
                    "hint": (
                        "These service rows are not in the accepted intent this apply would "
                        "PUT-replace. Accept them into intent to keep them, or flush them "
                        "deliberately via POST /devices/{id}/actions/force-removal."
                    ),
                }
            ],
        )
    except Exception as exc:
        logger.exception(f"apply.{log_label}_unexpected_error", job_id=job_id)
        err = internal_error(exc)
        _record_current_error(err)
        return 0, len(accounted_rows), [{"error": internal_error(exc)["message"]}]
    for row in rows:
        row.last_apply_at = now
        row.last_apply_error = None
    return len(accounted_rows), 0, []


async def _record_rp_capability_now(db, client, device, device_name, errors, *, job_id: int) -> None:
    """Record the route-policy scope's device-parser rejections — AFTER the terminal commit.

    The device parser only rejects an unsupported construct on a real commit (a dry-run
    renders it), so this is the only place the fact can be learned. It commits on the apply's
    own session, which is why it must not run mid-loop: route-policy is pushed after static
    routes, so a commit there would land an earlier scope's row stamps without the CAS,
    per-route results and terminal status §4.6 requires to be one transaction. Nothing is
    lost by waiting — it records a ``(ned, sw)`` fact, not this job's outcome.
    """
    from nso_adapter.core.capability import (
        parse_rejected_construct,
        record_capability_rejection,
        refresh_device_capability,
    )

    for exc in errors:
        try:
            info = await refresh_device_capability(db, client, device_name, device)
            scope, name = parse_rejected_construct(exc.message)
            if info and name:
                await record_capability_rejection(
                    db, info["ned_id"], info["sw_version"], scope, name, exc.message[:256]
                )
        except ClaimLostError:
            # A nested suppressor is as load-bearing as the runner boundary: swallowing a
            # revocation here lets the run continue under ownership it has lost.
            raise
        except Exception:  # noqa: BLE001 — capability recording is best-effort
            logger.debug("apply.capability_record_skipped", job_id=job_id)


async def _commit_terminal(db: AsyncSession, job_id: int) -> None:
    """Commit the apply's terminal transaction under the three-state contract (R2 §4.6).

    ``_commit_outcome`` classifies a raising COMMIT as UNKNOWN by construction — PostgreSQL
    may or may not have applied it and the client cannot tell. Writing a second terminal
    status on that reading is the bug, not the recovery: if the commit landed, the job is
    already terminal with its CAS and per-route results intact. Raising instead hands the
    decision to claim recovery, which re-dispositions only a job still ``running`` (G38).
    """
    from nso_adapter.core.claim import ClaimOutcome, _commit_outcome

    outcome = await _commit_outcome(db)
    if outcome is not ClaimOutcome.COMMIT_ACKNOWLEDGED:
        logger.error("apply.terminal_commit_outcome_unknown", job_id=job_id, outcome=outcome.value)
        raise BookkeepingOutcomeUnknown(f"apply job {job_id}: terminal commit outcome is {outcome.value}")


async def _write_terminal(
    db: AsyncSession, job_id: int, status: JobStatus, result: dict | None, error: dict | None, reg
) -> bool:
    """Write the apply's terminal status under its ownership predicate. False on a refusal.

    A refusal means another execution owns this job — recovery re-dispositioned it while
    this run was in flight. The per-route results and CAS in this transaction belong to
    that decision, not to ours, so the transaction is discarded rather than committed
    under a status we were refused.
    """
    write = await terminalize(
        db,
        job_id,
        status=status,
        expect=JobStatus.running,
        run_attempt=reg.run_attempt if reg is not None else None,
        result=result,
        error=error,
    )
    if write is None:
        await db.rollback()
        return False
    return True


async def _finalize_job(
    db: AsyncSession,
    job_id: int,
    device_id: int,
    any_eligible: bool,
    attr_outcome: tuple[int, int, list],
    ip_outcome: tuple[int, int, list],
    scope_outcomes: dict,
    scope_failures: dict,
    reader_compare: dict | None = None,
    reader_compare_unverifiable: dict | None = None,
    static_route_results: list | None = None,
    reg=None,
) -> None:
    """Assemble job.result/status from the pass outcomes and commit.

    With nothing eligible the job succeeds with an all-zero result and returns early.
    Otherwise the per-scope counts are emitted (plus the per-scope post-apply
    reader_compare statuses, #108, and any reader_compare_unverifiable labels — the keys a
    scope's presence check could not verify, mirroring the residue path); any failure flips
    the job to failed and collects the per-item errors.

    *static_route_results* is R2 §4.5's per-route record. It rides here rather than in a
    commit of its own because this IS the terminal transaction: the CAS, the row stamps, the
    results and the status must land together or not at all (§4.6).

    The commit goes through the three-state contract. A COMMIT that raises may still have
    been applied, and the caller's fallback — roll back, write ``failed`` in a SECOND
    transaction — would then produce exactly the torn state the atomicity rule exists to
    prevent: a consumed carrier or a closed replacement under a failed job. So an unknown
    outcome raises :class:`BookkeepingOutcomeUnknown` instead, and recovery decides.
    """
    if not any_eligible:
        logger.info("apply.nothing_eligible", job_id=job_id, device_id=device_id)
        empty_result = {
            "attribute_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
            "ip_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
            "snmp_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
            "static_route_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
            "subinterface_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
            "vlan_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
            "bfd_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
            "interface_mtu_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
            "l2_sap_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
            "isis_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
            "bgp_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
            "route_policy_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
            "ospf_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
        }
        if not await _write_terminal(db, job_id, JobStatus.succeeded, empty_result, None, reg):
            return
        await _commit_terminal(db, job_id)
        return

    attr_ok, attr_failed, attr_failures = attr_outcome
    ip_ok, ip_failed, ip_failures = ip_outcome

    result: dict[str, Any] = {
        "attribute_count_by_outcome": {"in_sync": attr_ok, "apply_failed": attr_failed},
        "ip_count_by_outcome": {"in_sync": ip_ok, "apply_failed": ip_failed},
    }
    for key in _SCOPE_RESULT_ORDER:
        scope_ok, scope_failed = scope_outcomes[key]
        result[f"{key}_count_by_outcome"] = {"in_sync": scope_ok, "apply_failed": scope_failed}
    if reader_compare:
        result["reader_compare"] = reader_compare
    if reader_compare_unverifiable:
        result["reader_compare_unverifiable"] = reader_compare_unverifiable
    if static_route_results is not None:
        result["static_route_results"] = static_route_results

    total_failed = attr_failed + ip_failed + sum(failed for _ok, failed in scope_outcomes.values())
    error = None
    if total_failed == 0:
        status = JobStatus.succeeded
    else:
        status = JobStatus.failed
        all_failed = [{"type": "attribute", **a} for a in attr_failures] + [{"type": "ip", **a} for a in ip_failures]
        for key in _SCOPE_RESULT_ORDER:
            all_failed.extend({"type": key, **a} for a in scope_failures.get(key, []))
        error = {
            "code": "nso_commit_failed",
            "message": f"{total_failed} item(s) failed to apply",
            "detail": {"items": all_failed},
        }
    if not await _write_terminal(db, job_id, status, result, error, reg):
        return
    await _commit_terminal(db, job_id)
    logger.info(
        "apply.done",
        job_id=job_id,
        in_sync=attr_ok,
        apply_failed=attr_failed,
        ip_in_sync=ip_ok,
        ip_failed=ip_failed,
        snmp_in_sync=scope_outcomes["snmp"][0],
        snmp_failed=scope_outcomes["snmp"][1],
        sr_in_sync=scope_outcomes["static_route"][0],
        sr_failed=scope_outcomes["static_route"][1],
        l2_in_sync=scope_outcomes["l2_sap"][0],
        l2_failed=scope_outcomes["l2_sap"][1],
        isis_in_sync=scope_outcomes["isis"][0],
        isis_failed=scope_outcomes["isis"][1],
        bgp_in_sync=scope_outcomes["bgp"][0],
        bgp_failed=scope_outcomes["bgp"][1],
        rp_in_sync=scope_outcomes["route_policy"][0],
        rp_failed=scope_outcomes["route_policy"][1],
    )


def _refuse_unverifiable_recorded_put(generation, execution_sections) -> None:
    """Refuse a recorded destructive PUT when verification is disabled at execution."""
    from nso_adapter.nso import apply as nso_apply

    if generation is None or "static_route" not in (execution_sections or ()):
        return
    if recorded_static_route_apply_mode(generation.document) == "PUT" and not nso_apply.VERIFY_AFTER_APPLY:
        raise JobError(
            "static_route_put_verify_disabled",
            "Static-route PUT verification is disabled at worker execution. "
            "The recorded destructive replace was not sent.",
        )


async def _execute_apply(db: AsyncSession, job: Job, job_id: int, device_id: int, force: bool, *, reg=None) -> None:
    """Run the apply body: sync-from, snapshot intent, push each scope, finalize the job.

    Raises on a missing device / NSO-client error so ``run_apply``'s outer handler can
    mark the job failed with an ``internal`` error.

    *reg* is the live claim registration, threaded down for the transactions R2 adds here.
    """
    from nso_adapter.core.importer import get_nso_client
    from nso_adapter.nso.apply import (
        apply_bfd_config,
        apply_bgp_config,
        apply_interface_attribute,
        apply_interface_ips,
        apply_isis_interfaces,
        apply_l2_saps,
        apply_logging_config,
        apply_mtu_config,
        apply_ospf_config,
        apply_route_policy_config,
        apply_snmp_config,
        apply_subinterface_config,
        apply_svi_config,
        apply_vlan_config,
        atomic_apply_enabled,
    )

    device = await db.get(Device, device_id)
    if not device:
        raise ValueError(f"Device {device_id} not found")

    # WHAT this run deploys is decided by the generation the job carries, not by the store as
    # it stands now (#1522 §G1). Between the worker committing `running` and this point a
    # successor push can commit; without the stored document it would be deployed here, under
    # this generation's identity and settled as this generation's revision.
    generation = await executing_generation(db, job_id)
    execution_sections = await generation_execution_sections(db, job_id)
    _refuse_unverifiable_recorded_put(generation, execution_sections)

    client = get_nso_client(device.nso_instance)
    device_name = device.nso_device_name

    # ── Step 0: sync-from before apply (best-effort) ──
    await _maybe_sync_from(db, client, device_name, device_id)

    source = _Projection(
        db,
        device_id,
        force,
        generation.document if generation is not None else None,
        execution_sections,
    )

    # ── Step 1: snapshot intent + collect every scope this job is allowed to execute ──
    (
        ifaces,
        intent_snapshot,
        attr_eligible,
        ip_snapshot,
        ip_eligible_by_iface,
        interface_ip_stamp_of,
    ) = await _collect_interface_apply_rows(
        db,
        source,
        generation,
        execution_sections,
        device_id,
        force,
    )

    snmp_comm_rows = await source.collect(SnmpCommunityIntent, section="snmp")
    snmp_user_rows = await source.collect(SnmpV3UserIntent, section="snmp")
    snmp_host_rows = await source.collect(SnmpHostIntent, section="snmp")
    snmp_sysinfo_rows = await source.collect(SnmpSystemInfoIntent, section="snmp")
    snmp_rows = _combine_rows(snmp_comm_rows, snmp_user_rows, snmp_host_rows, snmp_sysinfo_rows)
    snmp_comm = snmp_comm_rows.push
    snmp_user = snmp_user_rows.push
    snmp_host = snmp_host_rows.push
    snmp_sysinfo = snmp_sysinfo_rows.push[0] if snmp_sysinfo_rows.push else None

    sr_eligible_rows = await source.collect(StaticRouteIntent, section="static_route")
    sr_eligible = sr_eligible_rows.push
    if generation is not None and "static_route" in (execution_sections or ()):
        sr_all_rows = await source.collect(StaticRouteIntent, section="static_route", force=True)
        sr_plan = hydrate_static_route_apply_plan(generation.document, eligible_rows=sr_eligible)
        sr_stamp_of = sr_all_rows.stamp_of
        sr_stamp_rows = [
            stamp for row in sr_plan.rows if (stamp := (sr_stamp_of or {}).get(_stamp_key(row))) is not None
        ]
    elif generation is None:
        # Generationless jobs predate document execution and still serve explicit local runs.
        sr_plan = await build_plan(db, device, eligible_rows=sr_eligible)
        sr_stamp_rows = sr_plan.rows
        sr_stamp_of = None
    else:
        sr_plan = SrPlan("PATCH", [], set(), [], [], 0)
        sr_stamp_rows = []
        sr_stamp_of = {}
    # What the static-route send learned, for §4.4's proof: the verify verdict and the exact
    # route keys the body carried. Filled by the scope coroutine, read after it returns.
    sr_outbox: dict = {}
    logging_host_rows = await source.collect(LoggingHostIntent, section="logging")
    logging_level_rows = await source.collect(LoggingLevelsIntent, section="logging")
    logging_rows = _combine_rows(logging_host_rows, logging_level_rows)
    logging_eligible = logging_host_rows.push
    logging_levels = logging_level_rows.push[0] if logging_level_rows.push else None
    svi_rows = await source.collect(SviIntent, section="svi")
    subif_rows = await source.collect(SubinterfaceIntent, section="subinterface")
    vlan_rows = await source.collect(VlanIntent, section="vlan")
    bfd_rows = await source.collect(BfdIntent, section="bfd")
    mtu_rows = await source.collect(InterfaceMtuIntent, section="interface_mtu")
    l2_rows = await source.collect(L2SapIntent, section="l2_sap")
    isis_iface_rows = await source.collect(IsisInterfaceIntent, section="isis")
    isis_process_rows = await source.collect(IsisProcessIntent, section="isis")
    isis_flex_rows = await source.collect(IsisFlexAlgoIntent, section="isis")
    isis_level_rows = await source.collect(IsisLevelIntent, section="isis")
    redist_isis_rows = await source.collect(RedistributionIntent, section="isis")
    isis_rows = _combine_rows(
        isis_iface_rows,
        isis_process_rows,
        redist_isis_rows,
        isis_flex_rows,
        isis_level_rows,
    )
    bgp_router_rows = await source.collect(BgpRouterIntent, section="bgp")
    if bgp_router_rows.push and not sa_inspect(bgp_router_rows.push[0]).transient:
        # Live BGP rows still need their relationship collections loaded.
        from nso_adapter.core.bgp_load import attach_bgp_relationships

        await attach_bgp_relationships(db, bgp_router_rows.push)
    redist_bgp_rows = await source.collect(RedistributionIntent, section="bgp")
    bgp_rows = _combine_rows(bgp_router_rows, redist_bgp_rows)
    bgp_eligible = bgp_router_rows.push
    redist_bgp = redist_bgp_rows.push
    rp_rows = await source.collect(RoutePolicyObjectIntent, section="route_policy")
    ospf_instance_rows = await source.collect(OspfInstanceIntent, section="ospf")
    ospf_iface_rows = await source.collect(OspfInterfaceIntent, section="ospf")
    redist_ospf_rows = await source.collect(RedistributionIntent, section="ospf")
    ospf_rows = _combine_rows(ospf_instance_rows, ospf_iface_rows, redist_ospf_rows)

    svi_eligible = svi_rows.push
    subif_eligible = subif_rows.push
    bfd_eligible = bfd_rows.push
    mtu_eligible = mtu_rows.push
    l2_eligible = l2_rows.push
    isis_eligible = isis_iface_rows.push
    isis_process_eligible = isis_process_rows.push
    isis_flex_eligible = isis_flex_rows.push
    isis_level_eligible = isis_level_rows.push
    redist_isis = redist_isis_rows.push
    rp_eligible = rp_rows.push
    ospf_instance_eligible = ospf_instance_rows.push
    ospf_iface_eligible = ospf_iface_rows.push
    redist_ospf = redist_ospf_rows.push

    job.context = {"force": force, "intent_snapshot": intent_snapshot, "ip_snapshot": ip_snapshot}
    now = datetime.now(UTC)

    # EVERY collection the scope table below can push must be listed here. The IS-IS
    # sub-collections are eligible on their own (a per-level knob accepted on a device
    # whose interfaces are already in sync): when they were missing, the isis scope still
    # pushed — its _Scope rows list includes them — while _finalize_job took the "nothing
    # eligible" early-return and reported an all-zero SUCCESS for a commit the device had
    # rejected, which the plugin then settled deploying -> in_sync.
    any_eligible = any(
        [
            attr_eligible,
            ip_eligible_by_iface,
            snmp_rows.push,
            # From the PLAN, never the eligible list: in PUT mode a force=False apply can
            # have an empty eligible list and a non-empty body, and _finalize_job's
            # all-zero early success would then report a clean no-op AFTER a real PUT.
            sr_plan.rows,
            logging_rows.push,
            svi_eligible,
            subif_eligible,
            vlan_rows.push,
            bfd_eligible,
            mtu_eligible,
            l2_eligible,
            isis_eligible,
            isis_process_eligible,
            isis_flex_eligible,
            isis_level_eligible,
            bgp_eligible,
            rp_eligible,
            ospf_instance_eligible,
            ospf_iface_eligible,
            redist_ospf,
            redist_isis,
            redist_bgp,
        ]
    )

    # Atomic apply (I3b): stage EVERY scope's accepted intent into ONE NSO transaction and
    # commit once. Self-contained (stages, commits, stamps, finalises). When off, the
    # per-scope commit path below runs (per-item attr/IP + one batch commit per scope).
    if atomic_apply_enabled() and any_eligible:
        elig = {
            "ifaces": ifaces,
            "attr": attr_eligible,
            "ip_by_iface": ip_eligible_by_iface,
            "ip_stamp_of": interface_ip_stamp_of,
            "subif": subif_eligible,
            "snmp_rows": snmp_rows.push,
            "snmp_comm": snmp_comm,
            "snmp_user": snmp_user,
            "snmp_host": snmp_host,
            "snmp_sysinfo": snmp_sysinfo,
            "static_route": sr_plan.rows,
            "logging": logging_eligible,
            "logging_levels": logging_levels,
            "svi": svi_eligible,
            "vlan": vlan_rows.push,
            "bfd": bfd_eligible,
            "mtu": mtu_eligible,
            "l2_sap": l2_eligible,
            "isis_iface": isis_eligible,
            "isis_proc": isis_process_eligible,
            "isis_flex": isis_flex_eligible,
            "isis_levels": isis_level_eligible,
            "bgp": bgp_eligible,
            "rp": rp_eligible,
            "ospf_inst": ospf_instance_eligible,
            "ospf_iface": ospf_iface_eligible,
            "redist_ospf": redist_ospf,
            "redist_isis": redist_isis,
            "redist_bgp": redist_bgp,
            # The body uses the document rows. Bookkeeping uses matching live rows.
            "stamp": {
                "snmp": snmp_rows.stamp,
                "static_route": sr_stamp_rows,
                "logging": logging_rows.stamp,
                "svi": svi_rows.stamp,
                "subinterface": subif_rows.stamp,
                "vlan": vlan_rows.stamp,
                "bfd": bfd_rows.stamp,
                "interface_mtu": mtu_rows.stamp,
                "l2_sap": l2_rows.stamp,
                "isis": isis_rows.stamp,
                "bgp": bgp_rows.stamp,
                "route_policy": rp_rows.stamp,
                "ospf": ospf_rows.stamp,
            },
            "stamp_of": {
                "snmp": snmp_rows.stamp_of,
                "static_route": sr_stamp_of,
                "logging": logging_rows.stamp_of,
                "svi": svi_rows.stamp_of,
                "subinterface": subif_rows.stamp_of,
                "vlan": vlan_rows.stamp_of,
                "bfd": bfd_rows.stamp_of,
                "interface_mtu": mtu_rows.stamp_of,
                "l2_sap": l2_rows.stamp_of,
                "isis": isis_rows.stamp_of,
                "bgp": bgp_rows.stamp_of,
                "route_policy": rp_rows.stamp_of,
                "ospf": ospf_rows.stamp_of,
            },
        }
        await _run_atomic_apply(db, device, client, device_name, job, job_id, now, elig, sr_plan=sr_plan, reg=reg)
        # §4.11 retry path, on BOTH apply implementations: the atomic path is a separate
        # early return with its own finalization, so wiring this only into the per-scope
        # loop below would leave atomic-mode applies enqueueing nothing.
        await _enqueue_pending_clear_retract(db, device, sr_plan, reg=reg)
        return

    # ── Step 2: mark attribute states deploying ──
    attr_stamps = [item.stamp for item in attr_eligible if item.stamp is not None]
    ip_stamps = list((interface_ip_stamp_of or {}).values())
    _reject_transient_stamps("interface_config", [*attr_stamps, *ip_stamps])
    for item in attr_eligible:
        if item.state is not None and item.stamp is not None:
            item.state.sync_state = SyncState.deploying
    await db.commit()

    # ── Step 3–6: per-item attribute + IP passes ──
    attr_outcome = await _apply_attributes(
        attr_eligible, apply_interface_attribute, client=client, device_name=device_name, job_id=job_id, now=now
    )
    ip_outcome = await _apply_ips(
        ip_eligible_by_iface,
        ifaces,
        apply_interface_ips,
        client=client,
        device_name=device_name,
        job_id=job_id,
        now=now,
        stamp_of=interface_ip_stamp_of,
    )

    deferred_rp_capability: list[NsoApplyError] = []

    async def _record_rp_capability(exc: NsoApplyError) -> None:
        # DEFERRED past the terminal transaction, not skipped. Capability recording commits
        # on THIS session, and route-policy runs after static routes — so a commit here would
        # land an earlier scope's row stamps without the CAS, per-route results and status
        # that §4.6 requires to be one transaction. It records a (ned, sw) fact, not this
        # job's outcome, so its timing is free.
        deferred_rp_capability.append(exc)

    # ── Step 6b–6g: one batch commit per remaining scope ──
    scopes = [
        _Scope(
            "snmp",
            "snmp",
            snmp_rows.stamp,
            lambda: apply_snmp_config(
                client=client,
                device_name=device_name,
                community_intents=snmp_comm,
                v3_user_intents=snmp_user,
                host_intents=snmp_host,
                system_info_intent=snmp_sysinfo,
            ),
            push=snmp_rows.push,
            stamp_of=snmp_rows.stamp_of,
        ),
        _Scope(
            "static_route",
            "static_route",
            # plan.rows, not the eligible list: in PUT mode the body is every ACCEPTED row
            # (an eligible-only body retracts every accepted-and-clean route). Only matching
            # live rows receive bookkeeping for the recorded body.
            sr_stamp_rows,
            lambda: _static_route_coro(client, device, sr_plan, outbox=sr_outbox),
            push=sr_plan.rows,
            stamp_of=sr_stamp_of,
        ),
        _Scope(
            "logging",
            "logging",
            logging_rows.stamp,
            lambda: apply_logging_config(
                client=client,
                device_name=device_name,
                host_intent_rows=logging_eligible,
                levels_intent_row=logging_levels,
            ),
            push=logging_rows.push,
            stamp_of=logging_rows.stamp_of,
        ),
        _Scope(
            "svi",
            "svi",
            svi_rows.stamp,
            lambda: apply_svi_config(client=client, device_name=device_name, svi_intent_rows=svi_eligible),
            push=svi_rows.push,
            stamp_of=svi_rows.stamp_of,
        ),
        _Scope(
            "subinterface",
            "subif",
            subif_rows.stamp,
            lambda: apply_subinterface_config(client=client, device_name=device_name, subif_intent_rows=subif_eligible),
            push=subif_rows.push,
            stamp_of=subif_rows.stamp_of,
        ),
        _Scope(
            "vlan",
            "vlan",
            # push ≠ stamp here: the body is the executing generation's document, the stamps
            # go on the live rows that document carried (#1522 §G1).
            vlan_rows.stamp,
            lambda: apply_vlan_config(client=client, device_name=device_name, vlan_intent_rows=vlan_rows.push),
            push=vlan_rows.push,
            stamp_of=vlan_rows.stamp_of,
        ),
        _Scope(
            "bfd",
            "bfd",
            bfd_rows.stamp,
            lambda: apply_bfd_config(client=client, device_name=device_name, bfd_intent_rows=bfd_eligible),
            push=bfd_rows.push,
            stamp_of=bfd_rows.stamp_of,
        ),
        _Scope(
            "interface_mtu",
            "interface_mtu",
            mtu_rows.stamp,
            lambda: apply_mtu_config(client=client, device_name=device_name, mtu_intent_rows=mtu_eligible),
            push=mtu_rows.push,
            stamp_of=mtu_rows.stamp_of,
        ),
        _Scope(
            "l2_sap",
            "l2_sap",
            l2_rows.stamp,
            lambda: apply_l2_saps(client=client, device_name=device_name, sap_intent_rows=l2_eligible),
            push=l2_rows.push,
            stamp_of=l2_rows.stamp_of,
        ),
        _Scope(
            "isis",
            "isis",
            isis_rows.stamp,
            lambda: apply_isis_interfaces(
                client=client,
                device_name=device_name,
                isis_intent_rows=isis_eligible,
                isis_process_rows=isis_process_eligible,
                redistribution_rows=redist_isis,
                flex_algo_rows=isis_flex_eligible,
                level_rows=isis_level_eligible,
            ),
            push=isis_rows.push,
            stamp_of=isis_rows.stamp_of,
        ),
        _Scope(
            "bgp",
            "bgp",
            bgp_rows.stamp,
            lambda: apply_bgp_config(
                client=client,
                device_name=device_name,
                router_intent_rows=bgp_eligible,
                redistribution_rows=redist_bgp,
            ),
            push=bgp_rows.push,
            stamp_of=bgp_rows.stamp_of,
        ),
        _Scope(
            "route_policy",
            "route_policy",
            rp_rows.stamp,
            lambda: apply_route_policy_config(
                client=client, device_name=device_name, intent_rows=rp_eligible, ned_id=device.ned_id
            ),
            on_nso_error=_record_rp_capability,
            push=rp_rows.push,
            stamp_of=rp_rows.stamp_of,
        ),
        _Scope(
            "ospf",
            "ospf",
            ospf_rows.stamp,
            lambda: apply_ospf_config(
                client=client,
                device_name=device_name,
                process_intent_rows=ospf_instance_eligible,
                interface_intent_rows=ospf_iface_eligible,
                redistribution_rows=redist_ospf,
            ),
            push=ospf_rows.push,
            stamp_of=ospf_rows.stamp_of,
        ),
    ]

    # #108/1328: each scope commits then verifies immediately (default path, atomic-apply OFF)
    # — the per-scope action reads the far side of the FASTMAP writer as soon as possible after
    # ITS OWN commit (preserving the legacy immediate-per-scope timing, r2-M2). The action is
    # HEAVY (a live CDB build, not the cache-backed legacy GET), so a HARD budget bounds the total
    # default-path VERIFY time: once spent, remaining scopes skip to "unknown" rather than
    # serialising 60s each behind the shared 4-slot action semaphore (r3-M3/r4-M1). Only verify
    # time is charged — device COMMIT latency is excluded (codex P1), so a slow early commit can
    # never starve later scopes of silent-drop detection.
    from nso_adapter.core.removal import _VERIFY_TOTAL_BUDGET

    loop = asyncio.get_running_loop()
    verify_spent = 0.0
    device_ned_id = getattr(device, "ned_id", None)

    scope_outcomes: dict[str, tuple[int, int]] = {}
    scope_failures: dict[str, list] = {}
    reader_compare: dict[str, str] = {}
    reader_compare_unverifiable: dict[str, list[str]] = {}
    evidence_by_scope: dict[str, dict[int, str]] = {}
    send_failed_by_scope: dict[str, bool] = {}
    for sc in scopes:
        if not sc.sent:
            scope_outcomes[sc.key] = (0, 0)
            continue
        scope_ok, scope_failed, fails = await _run_scope(
            sc.log_label,
            sc.make_coro(),
            sc.rows,
            sent_rows=sc.sent,
            job_id=job_id,
            device_name=device_name,
            now=now,
            on_nso_error=sc.on_nso_error,
        )
        # The SEND's own verdict, before reader-compare folds per-row findings into the same
        # counter: "nothing landed" and "one row of several is missing" are different facts.
        send_failed_by_scope[sc.key] = scope_failed != 0
        if scope_failed == 0:
            # #108: the commit reported success — require every intended key to be
            # present in the scope's device-state section (the #26 silent-drop class).
            verify_started = loop.time()
            scope_ok, scope_failed, fails, rc_status, rc_unver, rc_evidence = await _reader_compare_default_path(
                client,
                device,
                sc,
                scope_ok,
                remaining=_VERIFY_TOTAL_BUDGET - verify_spent,
                ned_id=device_ned_id,
                job_id=job_id,
                device_name=device_name,
            )
            verify_spent += loop.time() - verify_started  # charge only verify time, not the commit
            if rc_status is not None:
                reader_compare[sc.key] = rc_status
            if rc_unver:
                reader_compare_unverifiable[sc.key] = rc_unver
            evidence_by_scope[sc.key] = rc_evidence
        scope_outcomes[sc.key] = (scope_ok, scope_failed)
        if fails:
            scope_failures[sc.key] = fails

    # ── Step 6h: R2 §4.4-§4.6 — prove, CAS, consume, record (per-scope path) ──
    sr_results = await _settle_static_routes(
        db,
        device,
        client,
        sr_plan,
        job_id=job_id,
        outbox=sr_outbox,
        evidence=evidence_by_scope.get("static_route", {}),
        put_delivered=sr_plan.mode == "PUT",
        send_failed=send_failed_by_scope.get("static_route", False),
        scope_outcomes=scope_outcomes,
        scope_failures=scope_failures,
        reg=reg,
        stamp_of=sr_stamp_of,
    )

    # ── Step 7: finalize ── (any_eligible computed up front, before the atomic branch)
    await _finalize_job(
        db,
        job_id,
        device_id,
        any_eligible,
        attr_outcome,
        ip_outcome,
        scope_outcomes,
        scope_failures,
        reader_compare=reader_compare,
        reader_compare_unverifiable=reader_compare_unverifiable,
        static_route_results=sr_results,
        reg=reg,
    )
    await _enqueue_pending_clear_retract(db, device, sr_plan, reg=reg)
    await _record_rp_capability_now(db, client, device, device_name, deferred_rp_capability, job_id=job_id)


async def _post_apply_refresh_and_notify(db: AsyncSession, device_id: int) -> None:
    """Re-read the just-applied surfaces back into the adapter read-mirror and notify the plugin.

    Apply commits config to NSO but — unlike ``sync_device`` — never refreshes the read-mirror
    (``GET /route-policy`` & friends serve cached DB rows) and never fires the sync-complete
    callback. So the plugin's own settle logic, which flips a ``deploying`` overlay row to
    ``in_sync`` only once the applied object is *present* in the adapter payload, reads a stale
    mirror on the immediate post-apply reconcile and re-marks the row ``deploying`` — it settles
    only on the next periodic sync (observed for route-policy on rg03: the row sat ``deploying``
    until the 15-min sync). Re-reading every surface that backs a ``deploying`` overlay — the
    routing surfaces (route-policy / IS-IS / OSPF / BGP / BFD / …) plus the L2/interface config
    surfaces (VLAN / SVI / subinterface / MTU) that ``sync_device`` does *not* fan out to — and
    then notifying the plugin lets those rows settle right after Apply.

    Best-effort: the Apply job is already finalized, so a refresh/notify failure must not fail it —
    the periodic sync remains the backstop.
    """
    from nso_adapter.core.importer import (
        get_netbox_client,
        get_nso_client,
        refresh_config_surfaces_for_device,
        refresh_routing_surfaces_for_device,
    )

    try:
        device = await db.get(Device, device_id)
        if device is None:
            return
        client = get_nso_client(device.nso_instance)
        await refresh_routing_surfaces_for_device(db, device, client, refresh_source="apply")
        await refresh_config_surfaces_for_device(db, device, client, refresh_source="apply")
        await db.commit()
        nb_client = get_netbox_client()
        if nb_client and device.netbox_device_id:
            await nb_client.notify_sync_complete(device.netbox_device_id)
    except ClaimLostError:
        # Revocation is not a runner error: recovery already owns the disposition.
        raise
    except Exception as exc:  # noqa: BLE001 — best-effort; never fail an already-finalized Apply
        logger.warning("apply.post_refresh_failed", device_id=device_id, error=repr(exc))


async def run_apply(job_id: int, device_id: int, force: bool = True, reg=None) -> None:
    """Background task: execute the apply for *device_id* (see module docstring §7a).

    *reg* is the worker's live ``ClaimRegistration``. R1 stopped it at the job runner, so
    nothing the apply wrote could be claim-scoped; R2's CAS and carrier transactions guard
    themselves with it.
    """
    from nso_adapter.store.db import session

    async with session() as db:
        job = await db.get(Job, job_id)
        if not job:
            logger.error("apply.job_not_found", job_id=job_id)
            return

        try:
            await _execute_apply(db, job, job_id, device_id, force, reg=reg)
        except ClaimLostError:
            # Revocation is not a runner error: recovery already owns the disposition.
            raise
        except BookkeepingOutcomeUnknown:
            # The terminal commit may have landed. Writing `failed` here would flip a job
            # whose CAS and per-route results are already committed — the exact torn state
            # §4.6's single transaction exists to prevent. Nothing further is written, the
            # post-apply refresh is skipped, and claim recovery decides (G38).
            raise
        except JobError as exc:
            logger.warning("apply.refused", job_id=job_id, device_id=device_id, code=exc.error["code"])
            await db.rollback()
            if await _write_terminal(db, job_id, JobStatus.failed, None, exc.error, reg):
                await _commit_terminal(db, job_id)
        except Exception as exc:
            logger.exception("apply.unexpected_error", job_id=job_id, device_id=device_id)
            # Roll back first: if the failure came from a DB error the session is in a
            # needs-rollback state and the failed-status commit below would itself throw,
            # leaving the job stuck 'running' and masking the real error. Re-fetch the job
            # after rollback (it may have been expired) so the status change persists.
            await db.rollback()
            # Commit only a landed write; on a refusal _write_terminal has already rolled back.
            if await _write_terminal(db, job_id, JobStatus.failed, None, internal_error(exc), reg):
                await db.commit()
        else:
            # Apply finalized (succeeded/partial/failed-on-device, no unexpected error): re-read
            # the applied surfaces into the mirror and notify the plugin so a 'deploying' row
            # settles on the immediate post-apply reconcile, not only on the next periodic sync.
            await _post_apply_refresh_and_notify(db, device_id)
