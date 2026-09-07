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

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from functools import cache
from typing import Any, NamedTuple

import structlog
from sqlalchemy import inspect as sa_inspect
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.core.claim import BookkeepingOutcomeUnknown, ClaimLostError, JobError, internal_error, terminalize
from nso_adapter.core.community_dialect import community_dialect_for
from nso_adapter.core.generation import executing_generation, generation_execution_sections
from nso_adapter.core.projection import (
    NoComparison,
    TableCompare,
    hydrate_interface_execution,
    hydrate_section,
    intent_state,
    section_context,
    section_models,
    section_registry,
)
from nso_adapter.core.static_route_plan import (
    SrPlan,
    hydrate_static_route_apply_plan,
    recorded_static_route_apply_mode,
)
from nso_adapter.nso.apply import NsoApplyError
from nso_adapter.store.models import (
    DbInterface,
    Device,
    DeviceSettings,
    InterfaceAttrState,
    InterfaceIntent,
    InterfaceIpIntent,
    Job,
    JobStatus,
    JobType,
    RedistributionIntent,
    StaticRouteIntent,
    SyncState,
)

logger = structlog.get_logger(__name__)

#: The apply preview's one key. One document is one transaction, so it renders one delta;
#: the key names the write, not a family.
PREVIEW_KEY = "device_intent"


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


# ── #1396 R2 §4.1/§4.2/§4.8 — the guarded static-route PUT-replace ───────────
#
# One snapshot feeds BOTH the retained-entry computation and the collateral guard, and one
# body builder feeds both the real apply and the preview — that shared derivation is what
# makes the previewed payload byte-identical to the applied one (C2.6).

#: The scope fails with this code when the pre-PUT service read cannot be certified. A
#: destructive replace must never be built on a read that may be hiding both the tombstoned
#: entries it has to preserve and the collateral the guard has to see (§4.4).
SNAPSHOT_INCONCLUSIVE = "static_route_snapshot_inconclusive"


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

    *put_delivered* is now simply "the document committed": one PUT IS the replacement, so a
    clean commit delivered it and a failed one delivered nothing. Returns ``None`` when the
    device has no static-route rows in this pass, so ``job.result`` gains no empty key.

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


# ── the aggregate document: rows in, one container body per family out ───────


def _carried(rows: list) -> list:
    """Return the rows a body may assert: an intent row must be accepted, a carrier has no such column."""
    return [row for row in rows if not hasattr(row, "accepted_at") or row.accepted_at is not None]


def section_execution(document: dict, section: str, proof: Any = None):
    """Return the frozen facts *section*'s encoder reads besides its rows (memo A9).

    The NED id and the dialect come from the section's own context and from no device row,
    so a retry, a reissue and an unrelated force-removal all encode the same bytes.
    """
    from nso_adapter.core.community_dialect import community_dialect_by_name
    from nso_adapter.nso.apply import SectionExecution

    context = section_context(document, section)
    return SectionExecution(context["ned_id"], community_dialect_by_name(context["dialect"]), proof)


def _hydrated_proof(document: dict, section: str):
    """Return the proof *section*'s encoder reads, rebuilt from the document alone."""
    return hydrate_interface_execution(document) if section == "interface_config" else None


def encode_section(document: dict, section: str, *, rows: Mapping[str, list] | None = None, proof: Any = None) -> dict:
    """Encode ONE section into its YANG container body.

    *rows* lets an executing deployment hand in the rows it already collected (the
    static-route plan's own rows); everything else is read from the document. Nothing here
    reads a live intent row or the device row.
    """
    from nso_adapter.core.projection import section_rows_by_table
    from nso_adapter.nso.apply import refuse_gated_local_levels

    entry = section_registry()[section]
    source = rows if rows is not None else section_rows_by_table(document, section)
    table_rows = {table: _carried(table_rows) for table, table_rows in source.items()}
    if section == "logging":
        # A send-boundary refusal, never the encoder's: one document must encode the same
        # bytes in every process, and a weaker host-only body would stamp the levels row
        # in_sync with no severity landing.
        refuse_gated_local_levels(table_rows)
    if proof is None:
        proof = _hydrated_proof(document, section)
    return entry.encode(table_rows, section_execution(document, section, proof))


def overlay_retained_routes(body: dict, retained: list[dict]) -> set:
    """Add the entries an unconsumed carrier still claims to a static-route body → its keys.

    Kept VERBATIM: metric, tag and NED-specific leaves live only in the live copy, so
    rebuilding such an entry from a store triple would silently rewrite it. A rendered row
    always wins on a key collision — the store is the authority for a route it still owns.
    """
    from nso_adapter.nso.apply import static_route_entry_key

    routes = body["route"]
    keys = {static_route_entry_key(entry) for entry in routes}
    for entry in retained:
        key = static_route_entry_key(entry)
        if key in keys:
            continue
        keys.add(key)
        routes.append(entry)
    return keys


def encode_device_document(document: dict) -> dict[str, dict]:
    """Encode every section the document carries into ``{YANG container: body}``.

    The registry binds each section to its container and its encoder, so this walk is the
    whole family fan-out: a family the document does not carry is absent from the body and,
    under a full-document PUT, therefore owns nothing.
    """
    registry = section_registry()
    return {
        registry[section].container: encode_section(document, section) for section in registry if section in document
    }


class DeviceBody(NamedTuple):
    """One device's whole PUT body, plus what building it learned.

    *static_route* is the certified verdict the retention read returned, or ``None`` when no
    certified read happened. The collateral guard takes its snapshot from it, so the retained
    entries and the guard see ONE read (R2 §4.1).
    """

    containers: dict[str, dict]
    sent_route_keys: set | None
    errors: dict[str, NsoApplyError]
    static_route: Any = None

    @property
    def snapshot(self):
        """The live instance the guard compares against, or the take-your-own sentinel."""
        from nso_adapter.core.removal import _NO_SNAPSHOT

        return _NO_SNAPSHOT if self.static_route is None else self.static_route.instance


def _operation_selected_routes(document: dict) -> frozenset:
    """Return the static-route keys THIS operation is authorized to remove, from its plane."""
    from nso_adapter.core.projection import section_operation
    from nso_adapter.core.static_route_plan import as_triple

    removal = section_operation(document, "static_route").get("removal") or {}
    return frozenset(key for raw in (removal.get("authorized_removal_keys") or []) if (key := as_triple(raw)))


async def build_device_containers(
    client,
    device,
    document: dict,
    *,
    rows_by_section: Mapping[str, Mapping[str, list]] | None = None,
    proof_by_section: Mapping[str, Any] | None = None,
    static_route_plan: SrPlan | None = None,
    retain_static_routes: bool = True,
) -> DeviceBody:
    """Build one device's whole PUT body → ``({container: body}, route keys sent, build errors)``.

    The ONE body builder every sender uses: apply, removal and the preview all encode the same
    document the same way, so a preview cannot show a body the commit would not send.

    The static-route section is the ratified exception to encoding from the document alone
    (#1683): its body additionally carries, verbatim, the live certified entries of the keys
    the frozen plan retains. An uncertified read refuses the send; certified absence retains
    nothing.

    ``retain_static_routes=False`` is the operator's FLUSH: a force-selected static-route
    section emits the document's rows and no retained entries, because a force-removal carries
    no removal authority and the formula would otherwise preserve every carrier-claimed key —
    the opposite of what the override promises.

    A family whose body cannot be BUILT (a malformed vault_ref, an unmappable enum, an
    uncertifiable retention read) is returned in *errors* rather than raised, and the caller
    decides. It is never silently omitted: under a full-document PUT an omitted family is a
    RETRACTED family, so a body with a build error is not sendable at all.
    """
    from nso_adapter.core.static_route_plan import as_triple, triple_of
    from nso_adapter.core.static_route_reader import certified_static_route_section
    from nso_adapter.nso.apply import static_route_entry_key

    registry = section_registry()
    containers: dict[str, dict] = {}
    errors: dict[str, NsoApplyError] = {}
    sent_route_keys: set | None = None
    certified: Any = None
    for section, entry in registry.items():
        if section not in document:
            continue
        try:
            body = encode_section(
                document,
                section,
                rows=(rows_by_section or {}).get(section),
                proof=(proof_by_section or {}).get(section),
            )
            if section == "static_route":
                retained: list[dict] = []
                if retain_static_routes:
                    plan = static_route_plan or hydrate_static_route_apply_plan(document)
                    certified = await certified_static_route_section(client, device)
                    if certified.inconclusive:
                        raise NsoApplyError(
                            SNAPSHOT_INCONCLUSIVE,
                            f"static_route: could not certify the live service instance on {device.nso_device_name!r}; "
                            "refusing to build a device-intent PUT from an uncertified read",
                            detail={"device": device.nso_device_name},
                        )
                    claimed = {triple_of(tomb) for tomb in plan.tombstones}
                    claimed.update(key for tomb in plan.tombstones if (key := as_triple(tomb.deployed_key)) is not None)
                    keep = claimed - {triple_of(row) for row in plan.rows} - _operation_selected_routes(document)
                    retained = [entry for entry in certified.routes if static_route_entry_key(entry) in keep]
                sent_route_keys = overlay_retained_routes(body, retained)
        except NsoApplyError as exc:
            logger.error(
                "apply.section_build_failed", device=device.nso_device_name, section=section, error=exc.message
            )
            errors[section] = exc
            continue
        containers[entry.container] = body
    return DeviceBody(containers, sent_route_keys, errors, certified)


async def collect_apply_diff(db: AsyncSession, device_id: int, outformat: str = "native") -> dict[str, str]:
    """Read-only preview: the native device delta the device's next deployment would push.

    The preview is bound to the DOCUMENT being committed — the device's executable
    generation head — never to a live-store estimate: store-only intent never reaches the
    device, so previewing it would show a diff the commit cannot produce. With
    ``outformat="native"`` NSO renders the device-native config the PUT would push;
    ``outformat="cli"`` renders the NED-uniform ``+``/``-`` tree diff (the "diff -u" panel).
    Nothing is committed either way.

    One document is one transaction, so there is one delta: the per-family split the
    reconcilers gave for free is not reconstructible from a single dry-run, and inventing one
    would mean sixteen dry-runs of documents the adapter would never send. An empty delta
    (the device already holds the document) returns ``{}``; a device with nothing authorized
    to deploy reports the preview UNAVAILABLE rather than an empty one, which would read as
    "nothing to do".
    """
    from nso_adapter.core.generation import executable_head, executing_generation, execution_policy
    from nso_adapter.core.importer import get_nso_client
    from nso_adapter.nso.apply import apply_device_intent

    device = await db.get(Device, device_id)
    if not device:
        return {}
    generation = await executable_head(db, device_id)
    if generation is None:
        return {PREVIEW_KEY: "!! preview unavailable: this device has no generation to deploy"}
    if generation.job_id is not None:
        generation = await executing_generation(db, generation.job_id)
        if generation is None:
            return {PREVIEW_KEY: "!! preview unavailable: job carries no generation"}
    client = get_nso_client(device.nso_instance)
    # dry_run is bool|str down the sender: True = native, "cli" = tree diff.
    fmt: bool | str = "cli" if outformat == "cli" else True
    try:
        policy = execution_policy(generation)
        body = await build_device_containers(
            client, device, generation.document, retain_static_routes=policy.retain_static_routes
        )
        if body.errors:
            raise next(iter(body.errors.values()))
        delta = await apply_device_intent(
            client, device.nso_device_name, body.containers, dry_run=fmt, no_networking=policy.no_networking
        )
    except Exception as exc:  # noqa: BLE001 — the preview must never fail hard
        logger.warning("apply_diff.failed", device=device.nso_device_name, error=repr(exc))
        reason = getattr(exc, "message", None) or repr(exc)
        return {PREVIEW_KEY: f"!! preview unavailable: {reason}"}
    if delta is None:
        return {PREVIEW_KEY: "!! preview unavailable: NSO dry-run was inconclusive"}
    return {PREVIEW_KEY: delta} if delta.strip() else {}


# ── run_apply: shared eligibility + per-scope batch-commit helpers ────────────
#
# An "apply pass" pushes one scope's accepted intent to NSO and stamps the
# outcome back onto the rows. Every scope shares the same eligibility filter and
# the same success/failure bookkeeping, so that logic lives in the helpers below
# and ``run_apply`` just wires the scopes together. Each scope commits as one
# unit: on success every row gets ``last_apply_at`` and a cleared error; on any
# failure every row records the error payload and the scope reports one item.


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
    *stamp* is what records the outcome. For document execution they differ on purpose:
    *push* is rebuilt from the generation's immutable document (transient rows that must
    never reach the store), while *stamp* is the LIVE rows this deployment actually carried.
    A row the successor changed is not stamped by a deployment that never carried it.

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

    The worker refuses an Apply without a generation before constructing this source. Every
    selected section is therefore hydrated from one immutable document.
    """

    def __init__(
        self,
        db: AsyncSession,
        device_id: int,
        force: bool,
        document: dict,
        sections: frozenset[str],
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
        if section not in self._sections:
            return _Rows(push=[], stamp=[])
        effective_force = self._force if force is None else force
        live_key = (model, effective_force)
        if live_key not in self._live:
            self._live[live_key] = await _collect_eligible(self._db, model, self._device_id, effective_force)
        live = self._live[live_key]
        if model is RedistributionIntent:
            live = [row for row in live if row.dest_protocol == section]
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


class _AttributeApply(NamedTuple):
    """One recorded attribute send and the live rows it may stamp."""

    state: InterfaceAttrState | None
    push: InterfaceIntent
    interface: DbInterface
    stamp: InterfaceIntent | None


class _InterfaceApply(NamedTuple):
    """The interface section's execution halves: what the body carries and what it stamps.

    The attribute and address halves keep separate bookkeeping (two result counters, two
    capability scopes) although they ride ONE keyed interface entry on the wire.
    """

    execution: Any  # projection.InterfaceExecution — the frozen writer context + eligibility
    attributes: list  # _AttributeApply, the eligible attribute rows and their live stamps
    ip_by_iface: dict
    ip_stamp_of: dict | None
    intent_snapshot: list
    ip_snapshot: list

    @property
    def ip_rows(self) -> list:
        return [row for rows in self.ip_by_iface.values() for row in rows]


_NO_INTERFACE = _InterfaceApply(None, [], {}, None, [], [])


async def _collect_document_interface(db: AsyncSession, source: _Projection, document: dict) -> _InterfaceApply:
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
    return _InterfaceApply(execution, eligible, ip_by_iface, ip_rows.stamp_of, intent_snapshot, ip_snapshot)


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


#: The container the route-policy family writes under. Its rejections are the one class the
#: device names in the COMMIT error while every dry-run renders clean.
_RP_CONTAINER = section_registry()["route_policy"].container

#: The family a refusal from the aggregate names for itself, from the ratified refusal shape
#: ``device-intent: refused [family=<container> field=<field_id>]: <reason>``.
_REFUSED_FAMILY = re.compile(r"device-intent:\s*refused\s*\[\s*family=([A-Za-z0-9._-]+)")


@cache
def _section_by_container() -> dict[str, str]:
    """YANG container -> the document section that owns it, off the registry."""
    return {entry.container: section for section, entry in section_registry().items()}


def _refused_family(message: str | None, containers) -> str | None:
    """Return the container a refusal names itself, when the document carries it."""
    match = _REFUSED_FAMILY.search(message or "")
    family = match.group(1) if match else None
    return family if family in containers else None


async def _localize_document_failure(client, device_name, containers, device_err) -> tuple[dict[str, str], tuple]:
    """Localise a failed document commit → ({offender container: its rejection message}, rp).

    ``rp`` is the route-policy ``(scope, name)`` construct parse. Three signals, cheapest
    first: (1) the aggregate's own refusal names its family in the message; (2) the
    empty-one-family dry-run — the family whose REMOVAL lets the document pass is the one the
    device rejected; (3) the route-policy device-parser rejection, which renders clean in
    dry-run and is only named in the *device* error. A dry-run that stays rejected, or comes
    back inconclusive, accuses nobody: a transient blip must never brand a family a false
    ``unsupported`` that a later probe cannot downgrade.

    The loop runs ONLY when the whole document's own dry-run reproduces the failure. A
    commit that fails for a reason dry-run cannot see — a transport blip, or a device-side
    misconfiguration that renders clean — lets EVERY trial pass, and every family would be
    named an offender: sixteen false ``unsupported`` verdicts per ``(ned, sw)`` from one
    timeout. Not reproducible means not attributable.

    No recording here — the caller decides attribution (including the fall-back to every
    family in the document) and whether this was a real device rejection.
    """
    from nso_adapter.core.capability import parse_rejected_construct
    from nso_adapter.nso.apply import NsoApplyError, apply_device_intent

    rp = parse_rejected_construct(device_err or "")
    named = _refused_family(device_err, containers)
    if named is not None:
        return {named: device_err or ""}, rp

    def _unattributable():
        logger.info("apply.localize.not_reproducible", device=device_name)
        return ({_RP_CONTAINER: device_err or ""} if rp[1] and _RP_CONTAINER in containers else {}), rp

    try:
        await apply_device_intent(client, device_name, containers, dry_run=True, strict=True)
    except NsoApplyError:
        pass  # the document is conclusively rejected as it stands: the loop can attribute it
    except Exception:  # noqa: BLE001 — a transport blip reproduces nothing, and must not escape
        logger.debug("apply.localize.inconclusive", device=device_name)
        return _unattributable()
    else:
        return _unattributable()

    offenders: dict[str, str] = {}
    for container in containers:
        trial = {name: body for name, body in containers.items() if name != container}
        try:
            delta = await apply_device_intent(client, device_name, trial, dry_run=True, strict=True)
        except NsoApplyError:
            continue  # still rejected without this family — not the offender
        except Exception:  # noqa: BLE001 — transient/transport during localisation → inconclusive
            logger.debug("apply.localize.inconclusive", device=device_name, family=container)
            continue
        if delta is None:  # inconclusive, not a clean pass
            continue
        offenders[container] = device_err or ""

    if rp[1] and _RP_CONTAINER in containers:
        offenders.setdefault(_RP_CONTAINER, device_err or "")
    return offenders, rp


def _capability_scopes_for(container: str) -> list[str]:
    """Capability-matrix scope name(s) for a YANG container ([] if not tracked).

    Off the registry's ``capability_scopes``: the merged interface container carries BOTH the
    interface_attribute and interface_ip scopes (collect_apply_diff / preflight treat them
    separately), so a rejection of it records capability under both — else a preflight for
    interface_attribute sees a false "fully supported".
    """
    section = _section_by_container().get(container)
    return list(section_registry()[section].capability_scopes) if section else []


async def _record_atomic_capability(db, client, device, device_name, offenders, exc, rp, device_err) -> None:
    """Record a capability rejection for the attributed offender families.

    A family whose REMOVAL lets the document compile is one the NED cannot compile on this
    ``(ned, sw)``; a device-parser rejection (``device_err``) means the device itself refused
    the commit — both are real, reactive capability gaps, recorded at scope granularity (or
    fine-grained for route-policy when ``rp = (scope, name)`` parses). H2: the interface
    container carries TWO scopes — when the rejection message names a construct, only the
    offending half is recorded (construct-named); an unattributable message falls back to the
    coarse both-scopes record. The ``(ned, sw)`` key is read from the device row, learned +
    persisted via the capability probe only when not already known.
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
    iface_container = section_registry()["interface_config"].container
    for container in offenders:
        own_msg = offenders.get(container, "") if isinstance(offenders, dict) else ""
        detail = (own_msg or device_err or exc.message or "")[:256]
        if container == _RP_CONTAINER and rp_name:
            await record_capability_rejection(db, ned_id, sw, rp_scope, rp_name, detail)
            continue
        if container == iface_container:
            scope, name = parse_rejected_iface_construct(own_msg or device_err or exc.message or "")
            if scope:
                await record_capability_rejection(db, ned_id, sw, scope, name, detail)
                continue
        for scope in _capability_scopes_for(container):
            await record_capability_rejection(db, ned_id, sw, scope, scope, detail)


async def _clear_atomic_capability(db, device, containers) -> None:
    """Clear stale reactive capability rejections after a clean document commit.

    A successful commit proves every family in the document applies on this ``(ned, sw)`` —
    the strongest positive signal — so drop any coarse ``apply``-sourced ``unsupported``
    recorded by an earlier failed apply. Without this the gap would stick forever (a probe
    cannot downgrade an apply-rejection). Best-effort; the ``(ned, sw)`` key is read from the
    device row (no probe).
    """
    from nso_adapter.core.capability import _clean_capability_key, clear_capability_rejections

    ned_id = _clean_capability_key(device.ned_id)
    if not ned_id:
        return
    sw = _clean_capability_key(device.sw_version)
    scopes: set[str] = set()
    for container in containers:
        scopes.update(_capability_scopes_for(container))
    await clear_capability_rejections(db, ned_id, sw, scopes)


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


async def _document_reader_compare(
    client, device, sections, outcomes, failures, *, job_id, device_name
) -> tuple[dict[str, str], dict[str, list[str]], dict[str, dict[int, str]]]:
    """#108 presence check per family after a clean commit.

    Each family's expected keys come from what its body CARRIED and from the section's own
    frozen context, and ``stamp_of`` maps those rows onto the live rows a finding is recorded
    on — the stamp rows are not the check's input, or a successor-rewritten family would be
    checked for no keys at all.

    One document is one transaction is ONE post-commit point, so every checkable family is
    read in a single batched device-state action. A batched-action raise → every checkable
    family records ``error`` (non-fatal). Mutates *outcomes* / *failures* for families with
    silently-dropped keys and returns ``(reader_compare, reader_compare_unverifiable,
    evidence_by_section)`` — the last being R2 §4.4's per-row map, which the static-route
    bookkeeping reads instead of the aggregate.
    """
    from nso_adapter.core.removal import _VERIFY_BATCH_TIMEOUT, _live_family_sections

    registry = section_registry()
    preps: dict[str, Any] = {}  # section → prep tuple | None (uncheckable) | "error" (translate raised)
    for section, apply_rows in sections.items():
        key = registry[section].result_keys[0]
        _ok, failed = outcomes.get(key, (0, 0))
        if not apply_rows.sent or failed:
            continue
        try:
            preps[section] = await _reader_compare_prepare(section, apply_rows.sent, apply_rows.ned_id)
        except Exception as exc:  # noqa: BLE001 — a family's translation must never fail the apply
            logger.warning(
                "apply.reader_compare_error", job_id=job_id, device=device_name, scope=section, error=repr(exc)
            )
            preps[section] = "error"

    wires = sorted({prep[3] for prep in preps.values() if isinstance(prep, tuple) and prep[0]})
    fetched: dict[str, dict] = {}
    action_error: Exception | None = None
    if wires:
        try:
            fetched = await _live_family_sections(client, device.nso_device_name, wires, timeout=_VERIFY_BATCH_TIMEOUT)
        except Exception as exc:  # noqa: BLE001 — a batched read failure fails no family's apply
            action_error = exc
            logger.warning("apply.reader_compare_error", job_id=job_id, device=device_name, error=repr(exc))

    reader_compare: dict[str, str] = {}
    reader_compare_unverifiable: dict[str, list[str]] = {}
    evidence_by_section: dict[str, dict[int, str]] = {}
    for section, prep in preps.items():
        key = registry[section].result_keys[0]
        s_ok, _s_failed = outcomes.get(key, (0, 0))
        if prep is None:
            continue  # structurally uncheckable → no entry
        if prep == "error":
            reader_compare[section] = "error"
            continue
        translated, unverifiable, lists, wire = prep
        evidence: dict[int, str] = {}
        fails: list[Any]
        if not translated:  # every key Vault-unverifiable → nothing to look for
            n_ok, n_failed, fails, status = s_ok, 0, [], "unknown"
        elif action_error is not None:
            n_ok, n_failed, fails, status = s_ok, 0, [], "error"
        else:
            try:
                # guarded (codex P2): a malformed 'ok' section must not let the walker's exception
                # escape and fail the whole job — classify "error" for just this family.
                n_ok, n_failed, fails, status, evidence = _classify_fetched_section(
                    section,
                    translated,
                    unverifiable,
                    lists,
                    fetched[wire],
                    s_ok,
                    job_id=job_id,
                    device_name=device_name,
                    stamp_of=sections[section].stamp_of,
                )
            except Exception as exc:  # noqa: BLE001 — a read-side glitch never fails a good commit
                logger.warning(
                    "apply.reader_compare_error", job_id=job_id, device=device_name, scope=section, error=repr(exc)
                )
                n_ok, n_failed, fails, status, evidence = s_ok, 0, [], "error", {}
        reader_compare[section] = status
        evidence_by_section[section] = evidence
        if unverifiable:
            reader_compare_unverifiable[section] = unverifiable
        if n_failed:
            outcomes[key] = (n_ok, n_failed)
            failures.setdefault(key, []).extend(fails)
    return reader_compare, reader_compare_unverifiable, evidence_by_section


class _SectionApply(NamedTuple):
    """One document section as the sender needs it and as its bookkeeping records it.

    *rows* is what the encoder reads (table name -> rows), *sent* every row the body carries,
    *stamp* the LIVE rows this deployment records its outcome on, and *stamp_of* the join
    between them. The two lists differ whenever a successor rewrote a row this document
    carried (#1522 §G1), and a section the job does not EXECUTE stamps nothing at all while
    its rows still ride the body — under a full-document PUT, omitting them would retract them.
    """

    rows: dict[str, list]
    sent: list
    stamp: list
    stamp_of: dict
    #: The NED id this section was FROZEN with. Every comparison that is NED-conditioned
    #: reads it here, never from the live device row.
    ned_id: str | None


class _ApplyPlan(NamedTuple):
    """Everything one deployment sends and stamps, resolved before the first device call."""

    device_id: int
    document: dict
    sections: dict[str, _SectionApply]
    interface: _InterfaceApply
    static_route: SrPlan | None
    #: The keys this deployment may drop, per section and guarded list — the guard's authority.
    allowed: dict
    any_eligible: bool


@cache
def _result_keys() -> tuple[str, ...]:
    """Every job-result counter name, in registry order (memo A8)."""
    return tuple(key for entry in section_registry().values() for key in entry.result_keys)


def _stampable(spec) -> bool:
    """Whether a section's table is one an apply stamps and counts on its own.

    A child table records through its parent and a lifecycle carrier is settlement state, not
    intent: neither is device-scoped, so neither is collected or counted here.
    """
    return spec.parent is None and not spec.lifecycle


async def _collect_sections(
    db: AsyncSession,
    source: _Projection,
    document: dict,
    *,
    static_route_plan: SrPlan | None,
) -> dict[str, _SectionApply]:
    """Collect every section the document carries: the body's rows and their live stamps.

    The registry is the walk. A section the job does not execute contributes its rows and no
    bookkeeping; ``_Projection`` decides that, so the two halves cannot drift apart.
    """
    from nso_adapter.core.projection import section_rows_by_table
    from nso_adapter.store.models import StaticRouteTombstone

    sections: dict[str, _SectionApply] = {}
    for section, entry in section_registry().items():
        if section not in document:
            continue
        rows = section_rows_by_table(document, section)
        if section == "static_route" and static_route_plan is not None:
            # The frozen plan owns which rows and which carriers this document executes.
            rows = {
                **rows,
                StaticRouteIntent.__tablename__: list(static_route_plan.rows),
                StaticRouteTombstone.__tablename__: list(static_route_plan.tombstones),
            }
        sent: list = []
        stamp: list = []
        stamp_of: dict = {}
        if section != "interface_config":  # the interface halves keep their own bookkeeping
            for spec in entry.tables:
                if not _stampable(spec):
                    continue
                collected = await source.collect(spec.model, section=section)
                sent.extend(collected.push)
                stamp.extend(collected.stamp)
                stamp_of.update(collected.stamp_of or {})
        if section == "static_route" and static_route_plan is not None:
            # The SAME objects the body carried and the settlement reads. Two hydrations of
            # one document produce equal rows that are not the same objects, so a send error
            # stamped on one is invisible to the per-route record built from the other.
            sent = list(static_route_plan.rows)
        sections[section] = _SectionApply(rows, sent, stamp, stamp_of, section_context(document, section)["ned_id"])
    return sections


def _revert_deploying(snapshot: dict) -> None:
    """Put the attribute states this run marked ``deploying`` back where they were."""
    for attr_state, state in snapshot.items():
        attr_state.sync_state = state


async def _finalize_unsent(db, plan: _ApplyPlan, build_errors: dict, *, job_id: int, reg) -> None:
    """Fail the families whose body could not be built. Nothing reached the device."""
    registry = section_registry()
    outcomes: dict[str, tuple[int, int]] = dict.fromkeys(_result_keys(), (0, 0))
    failures: dict[str, list] = {}
    for section, exc in build_errors.items():
        error = {"code": exc.code, "message": exc.message, "detail": exc.detail}
        if section == "interface_config":
            for item in plan.interface.attributes:
                if item.stamp is not None:
                    item.stamp.last_apply_error = error
            for row in (plan.interface.ip_stamp_of or {}).values():
                row.last_apply_error = error
            outcomes["attribute"] = (0, len(plan.interface.attributes))
            outcomes["ip"] = (0, len(plan.interface.ip_rows))
            failures["attribute"] = [{"error": exc.message}]
            continue
        apply_rows = plan.sections[section]
        _reject_transient_stamps(section, apply_rows.stamp)
        for row in apply_rows.sent:
            row.last_apply_error = error
        for row in apply_rows.stamp:
            row.last_apply_error = error
        key = registry[section].result_keys[0]
        # Counted against what the body WOULD have carried: a family whose live rows a
        # successor rewrote stamps none of them, and a (0, 0) outcome is a silent success.
        outcomes[key] = (0, len(apply_rows.sent))
        failures[key] = [{"error": exc.message}]
    await _finalize_job(db, job_id, plan.device_id, True, outcomes, failures, reg=reg, document_failed=True)


async def _commit_document(
    db, client, device, device_name, body, *, allowed: dict, job_id: int
) -> tuple[NsoApplyError | None, str | None, dict, dict | None, str]:
    """PUT the device's document behind the guard and localise its failure → stamping inputs.

    Returns ``(commit_error, verify, offenders, err, msg)``. *verify* is the commit's §4.4
    proof verdict, shared by every family in the document because they landed in ONE
    transaction (G39).

    The collateral guard runs HERE too, not only on a removal: one PUT makes every omission a
    retraction, so an apply can flush an orphaned service row exactly as a removal can. A
    blocked write sent nothing, so it fails the job without accusing any single family.
    """
    from nso_adapter.core.removal import RemovalBlockedError, guarded_device_write

    containers = body.containers
    commit_error: NsoApplyError | None = None
    verify: str | None = None
    blocked = False
    try:
        verify = await guarded_device_write(client, device, containers, allowed=allowed, current=body.snapshot)
    except RemovalBlockedError as exc:
        logger.error("apply.blocked_collateral", job_id=job_id, device=device_name, orphans=exc.orphans)
        commit_error = NsoApplyError(
            "removal_blocked_collateral", str(exc), detail={"orphans": exc.orphans, "preview": exc.preview}
        )
        blocked = True
    except NsoApplyError as exc:
        commit_error = exc
    except Exception as exc:  # noqa: BLE001 — surface as a job-level failure
        # The TYPE only: exception text can carry credentials (a RESTCONF error echoes the
        # request, an httpx error its headers) and this payload is persisted on every row.
        logger.error("apply.commit_internal_error", job_id=job_id, device=device_name, error=repr(exc))
        internal = internal_error(exc)
        commit_error = NsoApplyError("internal", internal["message"], detail=internal["detail"])

    if commit_error is None:
        # Positive signal (I2): a clean commit clears any stale reactive 'unsupported' for the
        # families in the document — a probe cannot downgrade an apply-rejection, so without
        # this the gap would stick forever even after the device is fixed and the intent lands.
        try:
            await _clear_atomic_capability(db, device, containers)
        except Exception:  # noqa: BLE001 — capability bookkeeping is best-effort
            logger.debug("apply.atomic.capability_clear_skipped", job_id=job_id)
        return None, verify, {}, None, ""

    logger.error("apply.atomic_failed", job_id=job_id, device=device_name, error=commit_error.message)
    device_err = _device_error_message(commit_error)
    # A guard refusal never reached the device, so there is nothing to localise and no
    # capability verdict to draw: the whole unsent document is the failure.
    offenders, rp = (
        ({}, (None, None)) if blocked else await _localize_document_failure(client, device_name, containers, device_err)
    )
    # Capability (I2): record ONLY reliably-localised offenders — a family whose removal lets
    # the document compile, a refusal that names its own family, or a parse_rejected_construct
    # match. A generic device rejection is NOT a capability signal: it may be a MISCONFIGURATION
    # (a route-map referencing a prefix-list the push does not carry), not a NED limit, and
    # recording it would be a false "unsupported" verdict. Such failures still fail the job and
    # stamp last_apply_error, so the operator sees the real device error.
    if offenders:
        try:
            await _record_atomic_capability(db, client, device, device_name, offenders, commit_error, rp, device_err)
        except Exception:  # noqa: BLE001 — capability recording is best-effort
            logger.debug("apply.atomic.capability_record_skipped", job_id=job_id)
    if not offenders:  # could not localise → the whole rolled-back commit is the failure
        offenders = dict.fromkeys(containers, "")
    message = commit_error.message
    if offenders:
        message = f"{message}; blocked by {', '.join(sorted(offenders))} refusal: {device_err or message}"
    err = {"code": commit_error.code, "message": message, "detail": commit_error.detail}
    return commit_error, verify, offenders, err, message


def _stamp_batch_sections(sections, offenders, commit_error, err, msg, now) -> tuple[dict, dict]:
    """Stamp every batch family from the single commit outcome → (outcomes, failures).

    Keyed by result key, in registry order. Every transmitted family fails when the
    transaction rolls back. Localization identifies the culprit only.
    """
    registry = section_registry()
    outcomes: dict[str, tuple[int, int]] = {}
    failures: dict[str, list] = {}
    for section, apply_rows in sections.items():
        if section == "interface_config":
            continue  # its two counters come from the per-attribute and per-address halves
        key = registry[section].result_keys[0]
        outcomes.setdefault(key, (0, 0))
        if not apply_rows.sent:
            continue
        _reject_transient_stamps(section, apply_rows.stamp)
        if commit_error is None:
            for row in apply_rows.stamp:
                row.last_apply_at = now
                row.last_apply_error = None
            outcomes[key] = (len(apply_rows.sent), 0)
        else:
            for row in apply_rows.sent:
                row.last_apply_error = err
            for row in apply_rows.stamp:
                row.last_apply_error = err
            outcomes[key] = (0, len(apply_rows.sent))
            failures[key] = [{"error": msg}]
    return outcomes, failures


async def _run_document_apply(db, device, client, device_name, job, job_id, now, plan, *, reg=None) -> None:
    """Deploy one device's document: encode every family, PUT once, stamp the one outcome.

    On success every row the body carried is stamped in_sync; on failure the whole
    transaction rolled back. Fail every transmitted row and localize only for attribution
    and capability recording.

    §4.4: the commit's verify verdict is threaded out of the sender rather than discarded and
    is shared by every family. §4.9's PATCH-versus-PUT split is gone with the per-family
    services: one PUT IS the replacement, so a static-route replacement is delivered in the
    same transaction as everything else instead of after it.
    """
    attr_eligible = plan.interface.attributes
    ip_rows_flat = [row for rows in plan.interface.ip_by_iface.values() for row in rows]

    # Snapshot attr states, then mark deploying; a pending (rolled-back, non-offender) attr is
    # reverted to its snapshot rather than left deploying.
    attr_stamps = [item.stamp for item in attr_eligible if item.stamp is not None]
    ip_stamps = list((plan.interface.ip_stamp_of or {}).values())
    _reject_transient_stamps("interface_config", [*attr_stamps, *ip_stamps])
    snapshot = {
        item.state: item.state.sync_state for item in attr_eligible if item.state is not None and item.stamp is not None
    }
    for attr_state in snapshot:
        attr_state.sync_state = SyncState.deploying
    await db.commit()

    try:
        body = await build_device_containers(
            client,
            device,
            plan.document,
            rows_by_section={section: rows.rows for section, rows in plan.sections.items()},
            proof_by_section={"interface_config": plan.interface.execution},
            static_route_plan=plan.static_route,
        )
    except Exception:
        # An UNEXPECTED error while building the body (before any commit) — a real bug, not a
        # family's own bad intent, which the builder isolates. Revert the attrs just marked
        # 'deploying' so they are not stuck forever, then re-raise so run_apply fails the job
        # with the real error.
        _revert_deploying(snapshot)
        await db.commit()
        raise

    if body.errors:
        # NOTHING was sent: an omitted family is a retracted family under a full-document PUT,
        # so a body that could not be built whole is not sendable at all. Fail exactly the
        # families that could not be built and leave every other row pending.
        _revert_deploying(snapshot)
        await _finalize_unsent(db, plan, body.errors, job_id=job_id, reg=reg)
        return

    commit_error, verify, offenders, err, msg = await _commit_document(
        db, client, device, device_name, body, allowed=plan.allowed, job_id=job_id
    )

    iface_container = section_registry()["interface_config"].container
    iface_failed = commit_error is not None and iface_container in body.containers
    attr_outcome = _stamp_attr_atomic(attr_eligible, commit_error, iface_failed, err, msg, now, snapshot)
    ip_outcome = _stamp_ip_atomic(
        ip_rows_flat, commit_error, iface_failed, err, msg, now, stamp_of=plan.interface.ip_stamp_of
    )
    outcomes, failures = _stamp_batch_sections(plan.sections, offenders, commit_error, err, msg, now)
    outcomes["attribute"] = attr_outcome[:2]
    outcomes["ip"] = ip_outcome[:2]
    if attr_outcome[2]:
        failures["attribute"] = list(attr_outcome[2])
    if ip_outcome[2]:
        failures["ip"] = list(ip_outcome[2])

    # The SEND's own verdict, before reader-compare folds per-row findings into the same
    # counter: "nothing landed" and "one row of several is missing" are different facts, and
    # reading the merged counter would make one dropped route block its proven sibling's CAS.
    sr_key = section_registry()["static_route"].result_keys[0]
    sr_send_failed = bool(outcomes.get(sr_key, (0, 0))[1])

    # #108: the document rides the same FASTMAP writers — run the post-apply presence check
    # per family and re-flag any silently-dropped keys. A family the body could not carry is
    # excluded: it was never pushed, so "not on the device" is not a drop.
    reader_compare: dict[str, str] = {}
    reader_compare_unverifiable: dict[str, list[str]] = {}
    evidence_by_section: dict[str, dict[int, str]] = {}
    if commit_error is None:
        reader_compare, reader_compare_unverifiable, evidence_by_section = await _document_reader_compare(
            client,
            device,
            plan.sections,
            outcomes,
            failures,
            job_id=job_id,
            device_name=device_name,
        )

    sr_results = None
    if plan.static_route is not None:
        sr_results = await _settle_static_routes(
            db,
            device,
            client,
            plan.static_route,
            job_id=job_id,
            outbox={"verify": verify, "sent_keys": body.sent_route_keys},
            evidence=evidence_by_section.get("static_route", {}),
            # One PUT is the replacement: the document either landed or nothing did.
            put_delivered=commit_error is None,
            send_failed=sr_send_failed,
            scope_outcomes=outcomes,
            scope_failures=failures,
            reg=reg,
            stamp_of=plan.sections["static_route"].stamp_of if "static_route" in plan.sections else None,
        )

    await _finalize_job(
        db,
        job_id,
        device.id,
        plan.any_eligible,
        outcomes,
        failures,
        reader_compare=reader_compare,
        reader_compare_unverifiable=reader_compare_unverifiable,
        static_route_results=sr_results,
        reg=reg,
        document_failed=commit_error is not None,
    )


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
    if row.family != "community_list":
        return False
    members = {e.get("community") for e in (row.entries or []) if isinstance(e, dict) and e.get("community")}
    if not members:
        return False
    return len(community_dialect_for(ned_id).unrepresentable_members(sorted(members))) == len(members)


def _reader_compare_expected(section: str, rows, ned_id: str | None = None) -> list[tuple[Any, str, tuple]]:
    """(intent row, YANG-list label, key tuple) for every checkable intended object (#108).

    Dispatches on the section's registry ``verify`` disposition (memo A8): a table comparison
    reads the registry's own entries, the two bespoke expansions keep their key expansion, and
    a section with no post-apply comparison expects nothing.

    Rows without a keyed reader presence are skipped: redistribution / flex-algo / level rows
    (nested non-keyed content, guard-grain parity), the snmp system-info scalar, and
    community-lists the NED cannot render at all (:func:`_unrenderable_community_list`).
    """
    from nso_adapter.core.removal import _ROUTE_POLICY_FAMILY_LISTS
    from nso_adapter.store import models as m

    verify = section_registry()[section].verify
    if isinstance(verify, NoComparison):
        return []
    if isinstance(verify, TableCompare):
        out: list[tuple[Any, str, tuple]] = []
        for model, label, keyfn in verify.entries:
            out.extend((r, label, keyfn(r)) for r in rows if isinstance(r, model))
        return out
    if section == "route_policy":
        return [
            (r, _ROUTE_POLICY_FAMILY_LISTS[r.family], (r.name,))
            for r in rows
            if isinstance(r, m.RoutePolicyObjectIntent)
            and r.family in _ROUTE_POLICY_FAMILY_LISTS
            and not _unrenderable_community_list(r, ned_id)
        ]
    if section == "bgp":
        expanded: list[tuple[Any, str, tuple]] = []
        for r in rows:
            if not isinstance(r, m.BgpRouterIntent):
                continue
            expanded.append((r, "router", (r.asn,)))
            for sc in r.scopes:  # the document hydrates the whole router tree
                for p in sc.peers:
                    expanded.append((r, "peer", (p.peer_address,)))
        return expanded
    raise RuntimeError(f"section {section!r} declares a bespoke expansion that has no implementation")


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


async def _reader_compare_prepare(scope, rows, ned_id):
    """Compute the translatable expected set for *scope* → ``(translated, unverifiable, lists, wire)``.

    Returns ``None`` when the section is structurally uncheckable (no expected keyed grain, no
    envelope wire, no guarded list) — the caller records no reader_compare entry. Otherwise
    ``translated`` is the export-namespace expected set (may be empty when EVERY key is
    Vault-unverifiable → the caller records ``unknown`` and runs NO action, r2-m3), and
    ``unverifiable`` names the keys that could not be re-keyed (persisted symmetrically with
    the residue path's ``residue_unverifiable``). Runs the Vault translation (may block).
    """
    from nso_adapter.core.removal import residue_wire_name, section_guard_lists

    expected = _reader_compare_expected(scope, rows, ned_id)
    lists = section_guard_lists(scope)
    wire = residue_wire_name(scope)
    if not expected or not lists or wire is None:
        return None
    translated, unverifiable = await _translate_expected(scope, expected)
    if unverifiable:
        # Named, never folded into "ok" silently: these keys were not checked at all.
        logger.warning("apply.reader_compare_unverifiable", scope=scope, keys=unverifiable)
    return translated, unverifiable, lists, wire


def _reader_compare_walk(
    scope: Any,
    translated: Any,
    unverifiable: Any,
    section: Any,
    lists: Any,
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

    present = {gl.label: _reader_keys(scope, section, gl) for gl in lists}
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
    scope, translated, unverifiable, lists, section, ok, *, job_id, device_name, stamp_of=None
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
        scope, translated, unverifiable, section, lists, ok, job_id=job_id, device_name=device_name, stamp_of=stamp_of
    )


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
    outcomes: dict[str, tuple[int, int]],
    failures: dict[str, list],
    reader_compare: dict | None = None,
    reader_compare_unverifiable: dict | None = None,
    static_route_results: list | None = None,
    reg=None,
    document_failed: bool = False,
) -> None:
    """Assemble job.result/status from the deployment's outcomes and commit.

    The counters are the registry's ``result_keys``, in registry order (memo A8): one
    ``<key>_count_by_outcome`` each, names unchanged, so a new family gets its counter from
    the registry instead of a hand-kept list. With nothing eligible the job succeeds with an
    all-zero result and returns early.

    Also emitted: the per-family post-apply ``reader_compare`` statuses (#108) and any
    ``reader_compare_unverifiable`` labels — the keys a family's presence check could not
    verify, mirroring the residue path. Any failure flips the job to failed and collects the
    per-item errors.

    *static_route_results* is R2 §4.5's per-route record. It rides here rather than in a
    commit of its own because this IS the terminal transaction: the CAS, the row stamps, the
    results and the status must land together or not at all (§4.6).

    The commit goes through the three-state contract. A COMMIT that raises may still have
    been applied, and the caller's fallback — roll back, write ``failed`` in a SECOND
    transaction — would then produce exactly the torn state the atomicity rule exists to
    prevent: a consumed carrier or a closed replacement under a failed job. So an unknown
    outcome raises :class:`BookkeepingOutcomeUnknown` instead, and recovery decides.
    """
    keys = _result_keys()
    if not any_eligible and not document_failed:
        logger.info("apply.nothing_eligible", job_id=job_id, device_id=device_id)
        empty_result = {f"{key}_count_by_outcome": {"in_sync": 0, "apply_failed": 0} for key in keys}
        if not await _write_terminal(db, job_id, JobStatus.succeeded, empty_result, None, reg):
            return
        await _commit_terminal(db, job_id)
        return

    result: dict[str, Any] = {}
    for key in keys:
        key_ok, key_failed = outcomes.get(key, (0, 0))
        result[f"{key}_count_by_outcome"] = {"in_sync": key_ok, "apply_failed": key_failed}
    if reader_compare:
        result["reader_compare"] = reader_compare
    if reader_compare_unverifiable:
        result["reader_compare_unverifiable"] = reader_compare_unverifiable
    if static_route_results is not None:
        result["static_route_results"] = static_route_results

    total_failed = sum(failed for _ok, failed in outcomes.values())
    error = None
    if total_failed == 0 and not document_failed:
        status = JobStatus.succeeded
    else:
        status = JobStatus.failed
        all_failed = [{"type": key, **item} for key in keys for item in failures.get(key, [])]
        error = {
            "code": "nso_commit_failed",
            "message": "Device document failed to apply"
            if document_failed
            else f"{total_failed} item(s) failed to apply",
            "detail": {"items": all_failed},
        }
    if not await _write_terminal(db, job_id, status, result, error, reg):
        return
    await _commit_terminal(db, job_id)
    logger.info(
        "apply.done",
        job_id=job_id,
        device_id=device_id,
        failed=total_failed,
        counts={key: outcomes.get(key, (0, 0)) for key in keys if outcomes.get(key, (0, 0)) != (0, 0)},
    )


def _refuse_unverifiable_recorded_put(generation, execution_sections) -> None:
    """Refuse a recorded destructive replacement when verification is disabled at execution.

    ``mode`` no longer picks a transport — every send is the whole document — so what it
    records is whether this document DELIVERS a replacement: a predecessor key it drops and
    then CASes closed. Sending one with no proof channel would close the replacement while
    the predecessor may still be on the device, so the job refuses instead (§4.4).
    """
    from nso_adapter.core.static_route_plan import PUT_REFUSED_EVENT
    from nso_adapter.nso import apply as nso_apply

    if "static_route" not in execution_sections:
        return
    if recorded_static_route_apply_mode(generation.document) == "PUT" and not nso_apply.VERIFY_AFTER_APPLY:
        logger.warning(PUT_REFUSED_EVENT, device_id=generation.device_id)
        raise JobError(
            "static_route_put_verify_disabled",
            "Static-route replacement verification is disabled at worker execution. "
            "The recorded destructive replace was not sent.",
        )


async def _required_apply_generation(db: AsyncSession, job_id: int):
    """Return one Apply carrier's generation and execution boundary, or fail closed."""
    generation = await executing_generation(db, job_id)
    if generation is None:
        raise JobError(
            "apply_generation_missing",
            f"Apply job {job_id} carries no generation to deploy.",
        )
    execution_sections = await generation_execution_sections(db, job_id)
    if execution_sections is None:  # pragma: no cover - the generation above came from this job
        raise RuntimeError(f"apply job {job_id} lost its generation while selecting execution sections")
    return generation, execution_sections


async def _execute_apply(db: AsyncSession, job: Job, job_id: int, device_id: int, force: bool, *, reg=None) -> None:
    """Run the apply body: sync-from, collect the document, PUT it once, finalize the job.

    Raises on a missing device / NSO-client error so ``run_apply``'s outer handler can mark
    the job failed with an ``internal`` error.

    *reg* is the live claim registration, threaded down for the transactions R2 adds here.
    """
    from nso_adapter.core.importer import get_nso_client
    from nso_adapter.core.removal import guard_allowed

    device = await db.get(Device, device_id)
    if not device:
        raise ValueError(f"Device {device_id} not found")

    # WHAT this run deploys is decided by the generation the job carries, not by the store as
    # it stands now (#1522 §G1). Between the worker committing `running` and this point a
    # successor push can commit; without the stored document it would be deployed here, under
    # this generation's identity and settled as this generation's revision.
    generation, execution_sections = await _required_apply_generation(db, job_id)
    _refuse_unverifiable_recorded_put(generation, execution_sections)

    client = get_nso_client(device.nso_instance)
    device_name = device.nso_device_name

    # ── Step 0: sync-from before apply (best-effort) ──
    await _maybe_sync_from(db, client, device_name, device_id)

    document = generation.document
    source = _Projection(db, device_id, force, document, execution_sections)

    # ── Step 1: the document's own rows, plus the live rows this run may stamp ──
    interface = (
        await _collect_document_interface(db, source, document) if "interface_config" in document else _NO_INTERFACE
    )
    # Hydrated whenever the document CARRIES the section, executed or not: the body asserts
    # every family of the device, so a section this job does not settle still rides it.
    sr_plan = hydrate_static_route_apply_plan(document) if "static_route" in document else None
    sections = await _collect_sections(db, source, document, static_route_plan=sr_plan)

    job.context = {
        "force": force,
        "intent_snapshot": interface.intent_snapshot,
        "ip_snapshot": interface.ip_snapshot,
    }
    now = datetime.now(UTC)

    # Whether this job has anything to DO, never what the body carries: a job whose executed
    # sections carry no eligible row records nothing and touches no device. From the static
    # route PLAN, never an eligible list — in a replacement the body is every accepted row.
    any_eligible = bool(
        interface.attributes
        or interface.ip_by_iface
        or any(rows.sent for section, rows in sections.items() if section in execution_sections)
    )

    plan = _ApplyPlan(
        device_id=device_id,
        document=document,
        sections=sections,
        interface=interface,
        static_route=sr_plan if "static_route" in execution_sections else None,
        allowed=guard_allowed(generation, route_keys=sr_plan.allowed if sr_plan is not None else None),
        any_eligible=any_eligible,
    )
    if not any_eligible:
        await _finalize_job(db, job_id, device_id, False, {}, {}, reg=reg)
        return
    await _run_document_apply(db, device, client, device_name, job, job_id, now, plan, reg=reg)


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
