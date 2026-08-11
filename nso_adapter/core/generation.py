# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Deployment generations: the ordered, immutable unit of device deployment (#1522 §G1/§G2/§H2).

Every accepted projection write takes the device's projection lock and bumps the touched
STREAM's ``desired_revision`` (:func:`note_write`, called at the MUTATION SITE). A stream is
one intent-PUT endpoint's lane, and it owns an explicit set of intent tables
(:mod:`core.projection`) — sixteen streams compose fourteen document sections. A write
that is not store-only additionally PROMOTES — copies ``desired_revision`` into
``authorized_revision`` — and creates one immutable deployment generation: the exact
document, its mode (``networked`` or ``detach``), a digest over both, the keys the document
is allowed to drop, and the ``X-Push-Seq`` that authorized it. Jobs carry generations and
execute the document they carry; they never rebuild the deployment from a store that has
moved on since.

Four rules the rest of the adapter leans on:

* **Order.** ``seq`` is allocated from :class:`DeviceGenerationCounter` under a row lock
  held to COMMIT, so per device the sequence equals the COMMIT order of the mutations that
  created the generations. Nothing else can convert allocation order into commit order.
* **The document is COMPLETE.** A generation's document composes every section from its
  streams' fragments: the just-promoted streams contribute a fresh snapshot, every other
  stream its LAST AUTHORIZED fragment. The deployment is a full-document write with removal
  by omission, so a document naming only the family that changed would read as "delete
  everything else"; composing per STREAM rather than per section is what stops one lane's
  push from carrying its sibling lane's un-promoted store-only repair (#103).
* **Success barrier (§H2).** Generation N+1 may not start until N is ``settled``. A
  ``failed`` or ``outcome_unknown`` head blocks every successor until :func:`retry_generation`
  re-admits it with the same document, mode and digest, or :func:`reconcile_generation`
  abandons it. Enforced twice on purpose: at admission, and again in the worker when it
  starts a job (:func:`job_admissible`) — the worker used to start whatever was queued.
* **Modes never coalesce.** Adjacent generations of the SAME mode may share one job (the
  queued-winner coalescing that already exists), and the run deploys the LAST of them,
  which supersedes its predecessors because each document is complete. A networked job
  absorbing a detach would retract exactly the config the detach exists to leave alone
  (#106), so that attachment is REFUSED rather than reordered.

**On the isolation level.** §G1 places generation creation in a REPEATABLE READ
transaction. What that buys is a document built from one consistent snapshot of the
projection, atomically with the mutation that authorized it. This module buys the same
guarantee with the projection lock instead: every accepted write takes it before reading
anything, and holds it to COMMIT, so no other projection writer can commit inside the
window. On PostgreSQL the two are not interchangeable for a WRITER — under REPEATABLE READ
the second writer of a row does not wait and proceed, it aborts with a serialization
failure, so the literal reading additionally requires whole-request retry at every intent
endpoint. The lock is the mechanism, and the difference is recorded rather than papered over.
"""

from __future__ import annotations

import hashlib
import json
from contextvars import ContextVar
from datetime import UTC, datetime

import structlog
from sqlalchemy import select
from sqlalchemy import update as sa_update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.core.projection import projection_streams, snapshot_stream, stream_section
from nso_adapter.store.db import execute_dml
from nso_adapter.store.models import (
    SETTLEMENT_COHORT_SEQUENCE,
    DeploymentGeneration,
    DeviceGenerationCounter,
    DeviceProjectionStream,
    GenerationMode,
    GenerationStatus,
    Job,
    JobStatus,
    JobType,
)

logger = structlog.get_logger(__name__)

#: Statuses a successor may cross. Everything else is a head that blocks.
_CROSSABLE = (GenerationStatus.settled, GenerationStatus.abandoned)
#: A job that is still going to run (or is running) covers its generations.
_LIVE_JOB = (JobStatus.queued, JobStatus.running)


class DeviceProjectionGone(RuntimeError):
    """The device was offboarded under a projection write. Nothing may be promoted for it."""


class GenerationModeConflict(RuntimeError):
    """A generation was offered to a job carrying the other mode.

    Hard failure, never a silent reorder: the two modes are different device operations and
    one job commits with one ``no-networking`` setting.
    """


def _now() -> datetime:
    return datetime.now(UTC)


def digest_document(mode: GenerationMode, document: dict, allowed_removal_keys: dict) -> str:
    """Return the sha256 a retry must reproduce byte for byte.

    Canonical JSON (sorted keys, no incidental whitespace) so two equal documents built by
    different code paths digest alike, and the mode is inside the hash because the same
    bytes committed ``no-networking`` are a different deployment.
    """
    payload = json.dumps(
        {"mode": mode.value, "document": document, "allowed_removal_keys": allowed_removal_keys},
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(payload.encode()).hexdigest()


async def lock_projection(db: AsyncSession, device_id: int) -> None:
    """Serialize this device's projection against every other writer. Caller commits.

    Taken by EVERY accepted write, store-only included: a store-only repair that slipped in
    between two of the document's SELECTs would put state into a generation nobody
    authorized. The lock is the counter row, so the same acquisition that orders the
    writers also orders the sequence they allocate.

    The row is created lazily under a SAVEPOINT — this is never a terminal transaction, so
    the FK check's ``FOR KEY SHARE`` on ``devices`` closes no cycle here (contrast
    :mod:`store.device_settle`, where a lazy create WOULD deadlock against offboard).
    """
    try:
        async with db.begin_nested():
            await db.execute(
                pg_insert(DeviceGenerationCounter)
                .values(device_id=device_id, last_seq=0)
                .on_conflict_do_nothing(index_elements=["device_id"])
            )
    except IntegrityError:
        # The FK found no device: it was offboarded under this write. `from None` keeps the
        # driver's request-echoing exception out of the traceback.
        raise DeviceProjectionGone(f"device {device_id} no longer exists") from None
    held = await db.scalar(
        select(DeviceGenerationCounter.device_id)
        .where(DeviceGenerationCounter.device_id == device_id)
        .with_for_update()
    )
    if held is None:
        raise DeviceProjectionGone(f"device {device_id} no longer exists")


async def note_write(db: AsyncSession, device_id: int, stream: str, *, push_seq: int | None = None) -> int:
    """Record one accepted write to *stream* and return its new ``desired_revision``.

    Called ONCE per accepted mutation, at the MUTATION SITE — store-only, backfill,
    auto-apply-off and normal alike — because ``desired_revision`` is what the store HOLDS,
    not what any operator authorized. It also takes the device's projection lock, so the
    caller holds it before it reads anything the document will be built from.

    The unit is the ENDPOINT's stream, never the document section: two endpoints share the
    interface document and two share the IS-IS one, and one revision counter for a shared
    pair reads each lane's writes as the other's (#103).

    Deliberately NOT called by the enqueue choke points. One push can reach both of them
    (a shrink and a growth in the same body) having produced ONE store state; a second bump
    there would leave the removal's generation carrying a revision no separate write stands
    behind, and would leave that revision looking un-authorized for good.
    """
    if stream not in projection_streams():
        raise ValueError(f"unknown projection stream {stream!r}")
    await lock_projection(db, device_id)
    values = {
        "device_id": device_id,
        "stream": stream,
        "desired_revision": 1,
        "source_push_seq": push_seq,
        "updated_at": _now(),
    }
    stmt = (
        pg_insert(DeviceProjectionStream)
        .values(**values)
        .on_conflict_do_update(
            index_elements=["device_id", "stream"],
            set_={
                "desired_revision": DeviceProjectionStream.desired_revision + 1,
                "source_push_seq": push_seq,
                "updated_at": _now(),
            },
        )
        .returning(DeviceProjectionStream.desired_revision)
    )
    revision = await db.scalar(stmt)
    if revision is None:  # INSERT .. ON CONFLICT DO UPDATE .. RETURNING always returns one row
        raise RuntimeError(f"projection revision update returned no row for device {device_id}, stream {stream!r}")
    return revision


async def _next_seq(db: AsyncSession, device_id: int) -> int:
    """Take the device's next generation sequence. The projection lock is already held."""
    seq = await db.scalar(
        sa_update(DeviceGenerationCounter)
        .where(DeviceGenerationCounter.device_id == device_id)
        .values(last_seq=DeviceGenerationCounter.last_seq + 1)
        .returning(DeviceGenerationCounter.last_seq)
        .execution_options(synchronize_session=False)
    )
    if seq is None:  # pragma: no cover — lock_projection proved the row exists
        raise DeviceProjectionGone(f"device {device_id} lost its generation counter")
    return seq


async def allocate_settlement_cohort(db: AsyncSession) -> int:
    """Reserve one globally unique identifier for a request settlement group."""
    cohort = await db.scalar(select(SETTLEMENT_COHORT_SEQUENCE.next_value()))
    if cohort is None:  # pragma: no cover - PostgreSQL nextval always returns one value
        raise RuntimeError("the settlement cohort sequence returned no value")
    return cohort


def _compose_document(fragments: dict[str, dict]) -> dict:
    """Compose ``{stream: fragment}`` into the ``{section: {table: rows}}`` outbound document.

    Streams own disjoint tables inside a section, so a section's tables are the union of its
    streams' fragments and no fragment can overwrite a sibling's table.
    """
    document: dict[str, dict] = {}
    for stream, fragment in sorted(fragments.items()):
        if fragment:
            document.setdefault(stream_section(stream), {}).update(fragment)
    return document


async def _authorized_fragments(db: AsyncSession, device_id: int) -> dict[str, dict]:
    """Every stream's last-authorized fragment for this device. Unpromoted lanes are absent."""
    rows = (
        (await db.execute(select(DeviceProjectionStream).where(DeviceProjectionStream.device_id == device_id)))
        .scalars()
        .all()
    )
    return {row.stream: row.authorized_document for row in rows if row.authorized_document}


async def _compose_authorized_document(db: AsyncSession, device_id: int, promoted: dict[str, dict]) -> dict:
    """Overlay the just-promoted streams on the last-authorized fragment of every other one.

    A generation's document is the COMPLETE outbound device document, never only the family
    the push touched. The deployment is a full-document write with removal by omission, so a
    document carrying only ``vlan`` says "this device has vlans and nothing else" — the
    other families would be omitted, which reads as delete.

    "Last authorized", not "current", and per STREAM rather than per section: a store-only
    repair that was never promoted must not ride out on the next unrelated push (#103), and
    that includes a repair delivered to the SIBLING lane of the stream being promoted — an
    ``interface_config`` store-only write followed by a normal ``ip`` push. Streams never
    promoted contribute nothing; the adapter has authorized nothing there.
    """
    fragments = await _authorized_fragments(db, device_id)
    fragments.update(promoted)
    return _compose_document(fragments)


#: The LAST generation the in-flight request enqueued, or ``None`` when it enqueued none.
#: Written at the single generation-write point below, read once by
#: :func:`core.receipt.record_response` through :func:`consume_last_enqueued_generation_id`.
#: Request-scoped like the flags in :mod:`core.request_flags`, and for the same reason: the
#: intent PUTs would otherwise each have to thread a return value back to the
#: receipt. The action, retry and worker paths create generations and never record a
#: response, so their value simply dies with their request context.
LAST_ENQUEUED_GENERATION_ID: ContextVar[int | None] = ContextVar("last_enqueued_generation_id", default=None)


def consume_last_enqueued_generation_id() -> int | None:
    """Take the id of the last generation this request enqueued, clearing it.

    Consuming rather than reading is what keeps a later delivery in the same context from
    inheriting an earlier one's link.
    """
    generation_id = LAST_ENQUEUED_GENERATION_ID.get()
    LAST_ENQUEUED_GENERATION_ID.set(None)
    return generation_id


async def create_generation(
    db: AsyncSession,
    device_id: int,
    *,
    streams: tuple[str, ...],
    mode: GenerationMode,
    allowed_removal_keys: dict | None = None,
    document: dict | None = None,
    removal_context: dict | None = None,
    settlement_cohort: int | None = None,
) -> DeploymentGeneration:
    """Promote *streams* and store the immutable generation they authorize. Caller commits.

    Refuses a store-only request outright rather than quietly producing nothing: the two
    choke points check the flag themselves, so reaching here under it is a wiring bug and a
    silent no-op would look exactly like a delivered deployment.

    *document* overrides the composition below with a complete document built elsewhere — the
    seam #1522's aggregate device-intent builder plugs into. *removal_context* is the job
    context a removal's generation must keep so a retry can rebuild the job that executes it.
    """
    from nso_adapter.core.request_flags import STORE_ONLY

    if STORE_ONLY.get():
        raise RuntimeError("create_generation reached under a store-only request — store-only never promotes")
    if not streams:
        raise ValueError("a generation must name at least one projection stream")
    await lock_projection(db, device_id)

    stream_revisions: dict[str, int] = {}
    source_push_seq: dict[str, int | None] = {}
    for stream in sorted(set(streams)):
        row = (
            await db.execute(
                sa_update(DeviceProjectionStream)
                .where(DeviceProjectionStream.device_id == device_id, DeviceProjectionStream.stream == stream)
                .values(authorized_revision=DeviceProjectionStream.desired_revision, updated_at=_now())
                .returning(DeviceProjectionStream.desired_revision, DeviceProjectionStream.source_push_seq)
                .execution_options(synchronize_session=False)
            )
        ).one_or_none()
        if row is None:
            # Promotion with no accepted write behind it. Every caller records its write
            # first; arriving here means a producer skipped note_write, and promoting
            # revision 0 would authorize a document nobody asked for.
            raise RuntimeError(f"device {device_id} stream {stream!r} has no accepted write to promote")
        stream_revisions[stream] = row.desired_revision
        source_push_seq[stream] = row.source_push_seq

    promoted = {stream: await snapshot_stream(db, device_id, stream) for stream in sorted(stream_revisions)}
    # The promoted streams become the new last-authorized fragments, so the NEXT generation of
    # any other lane composes THIS state in rather than whatever the store drifts to.
    for stream, fragment in promoted.items():
        await db.execute(
            sa_update(DeviceProjectionStream)
            .where(DeviceProjectionStream.device_id == device_id, DeviceProjectionStream.stream == stream)
            .values(authorized_document=fragment, updated_at=_now())
            .execution_options(synchronize_session=False)
        )
    body = document if document is not None else await _compose_authorized_document(db, device_id, promoted)
    return await _store_generation(
        db,
        device_id,
        mode=mode,
        document=body,
        allowed_removal_keys=allowed_removal_keys or {},
        source_push_seq=source_push_seq,
        stream_revisions=stream_revisions,
        removal_context=removal_context,
        settlement_cohort=settlement_cohort,
    )


async def _store_generation(
    db: AsyncSession,
    device_id: int,
    *,
    mode: GenerationMode,
    document: dict,
    allowed_removal_keys: dict,
    source_push_seq: dict,
    stream_revisions: dict,
    removal_context: dict | None,
    settlement_cohort: int | None,
) -> DeploymentGeneration:
    """Allocate the sequence and write the immutable row. The projection lock is held."""
    generation = DeploymentGeneration(
        device_id=device_id,
        seq=await _next_seq(db, device_id),
        mode=mode,
        status=GenerationStatus.pending,
        document=document,
        digest=digest_document(mode, document, allowed_removal_keys),
        allowed_removal_keys=allowed_removal_keys,
        source_push_seq=source_push_seq,
        stream_revisions=stream_revisions,
        removal_context=removal_context,
        settlement_cohort=settlement_cohort,
    )
    db.add(generation)
    await db.flush()
    # The one place a generation row is written, so every path that creates one — both
    # create_generation callers and the reissue — records its provenance here.
    LAST_ENQUEUED_GENERATION_ID.set(generation.id)
    logger.info(
        "generation.created",
        device_id=device_id,
        generation_id=generation.id,
        seq=generation.seq,
        mode=mode.value,
        streams=sorted(stream_revisions),
        document_sections=sorted(document),
        digest=generation.digest[:12],
    )
    return generation


async def authorized_streams(db: AsyncSession, device_id: int) -> tuple[str, ...]:
    """Every stream this device has a desired state for. The operator's Apply promotes them.

    ``desired_revision > 0`` and not ``authorized_revision``: a manual Apply IS the
    authorization for what the store holds, which is exactly what the current apply job
    pushes. The plugin-side selection of WHICH revisions to promote is §H4, slice 2.
    """
    rows = (
        (
            await db.execute(
                select(DeviceProjectionStream.stream).where(
                    DeviceProjectionStream.device_id == device_id,
                    DeviceProjectionStream.desired_revision > 0,
                )
            )
        )
        .scalars()
        .all()
    )
    return tuple(sorted(rows))


async def create_reissue_generation(
    db: AsyncSession,
    device_id: int,
    *,
    mode: GenerationMode,
    removal_context: dict | None = None,
    allowed_removal_keys: dict | None = None,
) -> DeploymentGeneration:
    """Order a NEW deployment of the state that is ALREADY authorized. Promotes nothing.

    Three producers re-issue a device write for an authority an earlier push already
    authorized: the tombstone sweeper and the static-route reclaimer (a deletion with no
    proof of delivery) and the operator's force-removal (the same scope again, with the
    collateral guard off). There is no new operator intent behind any of them, so nothing may
    be promoted; what they need is a place in the device's ordered chain, so the write cannot
    cross a blocked head and cannot be crossed.

    None of them is a request-scoped intent delivery, so ``?store_only=true`` is not checked
    here: the force-removal action is exempt from it at its own choke point
    (:func:`core.removal.enqueue_removal`) and the two scheduled producers carry no request.

    It therefore settles NOTHING: ``stream_revisions`` is empty. The document composes every
    authorized stream so the write stays complete, but the job behind it executes ONE
    removal context's scope, and settlement advances exactly what a generation lists. Listing
    every authorized revision let a static-route reissue certify a VLAN revision whose own
    deployment had failed or been abandoned — a lane marked applied by a write that never
    carried it. ``source_push_seq`` stays: it is provenance, not a settlement target.
    """
    await lock_projection(db, device_id)
    rows = (
        (await db.execute(select(DeviceProjectionStream).where(DeviceProjectionStream.device_id == device_id)))
        .scalars()
        .all()
    )
    fragments = {row.stream: row.authorized_document for row in rows if row.authorized_document}
    return await _store_generation(
        db,
        device_id,
        mode=mode,
        document=_compose_document(fragments),
        allowed_removal_keys=allowed_removal_keys or {},
        source_push_seq={row.stream: row.source_push_seq for row in rows if row.authorized_document},
        stream_revisions={},
        removal_context=removal_context,
        settlement_cohort=None,
    )


async def _job_generations(db: AsyncSession, job_id: int) -> list[DeploymentGeneration]:
    return list(
        (
            await db.execute(
                select(DeploymentGeneration)
                .where(DeploymentGeneration.job_id == job_id)
                .order_by(DeploymentGeneration.seq)
            )
        )
        .scalars()
        .all()
    )


class GenerationTampered(RuntimeError):
    """A stored generation's digest no longer matches its document. It is not executed."""


async def executing_generation(db: AsyncSession, job_id: int) -> DeploymentGeneration | None:
    """Return the generation a job must deploy, digest verified. ``None`` if it carries none.

    A job may carry several ADJACENT generations of the same mode (the queued-winner
    coalescing that already exists). Each document is the COMPLETE outbound device document,
    so the highest ``seq`` supersedes its predecessors and IS what the run deploys; the
    earlier ones settle with it because that one write also establishes their state.

    Raises :class:`GenerationTampered` rather than executing a document whose digest does not
    reproduce. The database trigger makes that unreachable from SQL, so it is the last line
    against a restore, a manual repair, or a bug that bypasses the ORM.
    """
    carried = await _job_generations(db, job_id)
    if not carried:
        return None
    generation = carried[-1]
    expected = digest_document(generation.mode, generation.document, generation.allowed_removal_keys or {})
    if expected != generation.digest:
        raise GenerationTampered(
            f"generation {generation.id} (device {generation.device_id} seq {generation.seq}) digest "
            f"{generation.digest[:12]} does not match its document"
        )
    return generation


async def _blocking_predecessor(db: AsyncSession, device_id: int, seq: int) -> DeploymentGeneration | None:
    """Return the earliest generation before *seq* that a successor may not cross."""
    return await db.scalar(
        select(DeploymentGeneration)
        .where(
            DeploymentGeneration.device_id == device_id,
            DeploymentGeneration.seq < seq,
            DeploymentGeneration.status.not_in(_CROSSABLE),
        )
        .order_by(DeploymentGeneration.seq)
        .limit(1)
    )


async def attach_to_job(db: AsyncSession, generation: DeploymentGeneration, job: Job) -> bool:
    """Bind *generation* to *job* when the job can carry it. Caller commits.

    Returns False (and binds nothing) when the job already carries a generation that is not
    this one's immediate predecessor: executing that job would deploy this document BEFORE
    the generation in between, which is precisely the strict order §G1 exists to hold. The
    generation stays ``pending`` and unattached; :func:`advance_device_generations` gives it
    a job of its own once the barrier clears.

    Raises :class:`GenerationModeConflict` when the job carries the other mode — coalescing
    across the networked/detach boundary is forbidden, and there is no correct reordering.

    An EMPTY job always accepts: it was created for this generation, and whether it may
    START is the barrier's question (:func:`job_admissible`), asked again when a worker
    picks it up — not this one.
    """
    carried = await _job_generations(db, job.id)
    if carried:
        if carried[-1].mode is not generation.mode:
            raise GenerationModeConflict(
                f"job {job.id} carries mode {carried[-1].mode.value}; generation {generation.id} is "
                f"{generation.mode.value}"
            )
        if carried[-1].seq != generation.seq - 1:
            logger.info(
                "generation.attach_declined_noncontiguous",
                job_id=job.id,
                generation_id=generation.id,
                job_head_seq=carried[-1].seq,
                seq=generation.seq,
            )
            return False
    generation.job_id = job.id
    await db.flush()
    return True


async def executable_head(db: AsyncSession, device_id: int) -> DeploymentGeneration | None:
    """Return the device's oldest generation that is not settled or abandoned."""
    return await db.scalar(
        select(DeploymentGeneration)
        .where(DeploymentGeneration.device_id == device_id, DeploymentGeneration.status.not_in(_CROSSABLE))
        .order_by(DeploymentGeneration.seq)
        .limit(1)
    )


async def _locked_executable_head(db: AsyncSession, device_id: int) -> DeploymentGeneration | None:
    """Re-read and lock the executable head after the caller takes the projection lock."""
    return await db.scalar(
        select(DeploymentGeneration)
        .where(DeploymentGeneration.device_id == device_id, DeploymentGeneration.status.not_in(_CROSSABLE))
        .order_by(DeploymentGeneration.seq)
        .limit(1)
        .with_for_update()
        .execution_options(populate_existing=True)
    )


async def _require_locked_executable_head(db: AsyncSession, generation: DeploymentGeneration) -> DeploymentGeneration:
    """Return *generation* as the locked executable head, or refuse the operator exit."""
    await lock_projection(db, generation.device_id)
    head = await _locked_executable_head(db, generation.device_id)
    if head is None or head.id != generation.id:
        raise GenerationNotBlocked(f"generation {generation.id} is not the executable head")
    return head


#: Job types that write to the device. Everything else is a read and is never barred.
_DEVICE_WRITING = (JobType.apply, JobType.removal)
#: A head in one of these has not settled and never will without a retry or a reconcile.
BLOCKED_STATUSES = (GenerationStatus.failed, GenerationStatus.outcome_unknown)


async def job_admissible(db: AsyncSession, job_id: int, device_id: int) -> bool:
    """Ask the success barrier whether a worker may start *job_id* now (§H2).

    A READ carrying no generation is unaffected: a sync never deploys a document and must
    not queue behind a blocked write. A device-WRITING job carrying no generation is a
    different matter — an Apply on a device nothing was ever written for, or a job whose
    generation was abandoned — and it may not cross a blocked head either, or it deploys
    over a device state nobody established.
    """
    carried = await _job_generations(db, job_id)
    if not carried:
        job_type = await db.scalar(select(Job.job_type).where(Job.id == job_id))
        if job_type not in _DEVICE_WRITING:
            return True
        blocked = await db.scalar(
            select(DeploymentGeneration.seq)
            .where(DeploymentGeneration.device_id == device_id, DeploymentGeneration.status.in_(BLOCKED_STATUSES))
            .limit(1)
        )
        if blocked is None:
            return True
        logger.warning(
            "generation.blocked_generationless_write",
            job_id=job_id,
            device_id=device_id,
            job_type=str(job_type),
            blocked_by_seq=blocked,
        )
        return False
    blocker = await _blocking_predecessor(db, device_id, carried[0].seq)
    if blocker is None:
        return True
    logger.warning(
        "generation.blocked_by_predecessor",
        job_id=job_id,
        device_id=device_id,
        seq=carried[0].seq,
        blocked_by_seq=blocker.seq,
        blocked_by_status=blocker.status.value,
    )
    return False


async def mark_job_generations_running(db: AsyncSession, job_id: int) -> None:
    """Move a starting job's generations to ``running``. Caller commits."""
    await db.execute(
        sa_update(DeploymentGeneration)
        .where(DeploymentGeneration.job_id == job_id, DeploymentGeneration.status == GenerationStatus.pending)
        .values(status=GenerationStatus.running, attempts=DeploymentGeneration.attempts + 1, updated_at=_now())
        .execution_options(synchronize_session=False)
    )


async def settle_job_generations(
    db: AsyncSession,
    job_id: int,
    *,
    outcome: GenerationStatus,
    error: dict | None = None,
) -> None:
    """Record what the job's execution proved about its generations. Caller commits.

    Runs INSIDE the terminal transaction, so a generation can never be settled by a status
    write that was rolled back. On success each carried stream is stamped applied at THE
    REVISION THE GENERATION CARRIED — never at the store's current revision, which may
    already hold writes this deployment never contained (§G2). A marking-split removal can
    put the same stream revision in two generations. That revision is stamped only after
    every generation carrying it has settled successfully.
    """
    carried = await _job_generations(db, job_id)
    if not carried:
        return
    now = _now()
    values: dict = {"status": outcome, "updated_at": now}
    if outcome is GenerationStatus.settled:
        values["settled_at"] = now
        values["last_error"] = None
    elif error is not None:
        values["last_error"] = error
    await db.execute(
        sa_update(DeploymentGeneration)
        .where(DeploymentGeneration.id.in_([g.id for g in carried]))
        .values(**values)
        .execution_options(synchronize_session=False)
    )
    if outcome is not GenerationStatus.settled:
        logger.warning(
            "generation.unsettled",
            job_id=job_id,
            outcome=outcome.value,
            seqs=[g.seq for g in carried],
        )
        return
    targets = {
        (generation.device_id, stream, revision, generation.settlement_cohort)
        for generation in carried
        for stream, revision in (generation.stream_revisions or {}).items()
    }
    for device_id, stream, revision, settlement_cohort in sorted(
        targets,
        key=lambda target: (target[0], target[1], target[2], target[3] is not None, target[3] or 0),
    ):
        conditions = [
            DeviceProjectionStream.device_id == device_id,
            DeviceProjectionStream.stream == stream,
            DeviceProjectionStream.applied_revision < revision,
        ]
        if settlement_cohort is not None:
            unsettled_sibling = (
                select(DeploymentGeneration.id)
                .where(
                    DeploymentGeneration.device_id == device_id,
                    DeploymentGeneration.status != GenerationStatus.settled,
                    DeploymentGeneration.stream_revisions[stream].as_string() == str(revision),
                    DeploymentGeneration.settlement_cohort == settlement_cohort,
                )
                .exists()
            )
            conditions.append(~unsettled_sibling)
        await db.execute(
            sa_update(DeviceProjectionStream)
            .where(*conditions)
            .values(applied_revision=revision, updated_at=now)
            .execution_options(synchronize_session=False)
        )


async def requeue_job_generations(db: AsyncSession, job_id: int) -> None:
    """Return an interrupted job's generations to ``pending``. Caller commits.

    Only for a job recovery put back on the queue: the SAME job will re-run the SAME
    documents, so the generations are not blocked, merely un-started.
    """
    await db.execute(
        sa_update(DeploymentGeneration)
        .where(DeploymentGeneration.job_id == job_id, DeploymentGeneration.status == GenerationStatus.running)
        .values(status=GenerationStatus.pending, updated_at=_now())
        .execution_options(synchronize_session=False)
    )


def _job_for(generation: DeploymentGeneration) -> Job:
    """Build the job that executes *generation*, from the generation alone.

    A removal's context lives on the GENERATION, so a retry never has to read it back off
    the job that failed carrying it — which may have been requeued, superseded, or had its
    context rewritten by its own run.
    """
    if generation.removal_context:
        return Job(
            job_type=JobType.removal,
            device_id=generation.device_id,
            status=JobStatus.queued,
            context=dict(generation.removal_context),
        )
    if generation.mode is GenerationMode.detach:
        # Unreachable through enqueue_removal, which always records the context. Raising
        # beats the alternative: an apply job would NETWORK the very retraction the detach
        # exists to keep off the device.
        raise RuntimeError(f"detach generation {generation.id} carries no removal context")
    return Job(job_type=JobType.apply, device_id=generation.device_id, status=JobStatus.queued)


async def _queue_job_for(db: AsyncSession, generation: DeploymentGeneration) -> Job:
    """Give *generation* a queued job, TAKING OVER the successors' one if one exists.

    At most one queued job per (device, type) exists — the admission dedupe index — so this
    cannot simply insert another when a successor already holds one. It takes that job over
    instead: the successors it carried go back to unattached ``pending`` and get their own
    job once this head settles. Which is the barrier: the head runs first, alone, with its
    own document.
    """
    spec = _job_for(generation)
    existing = await db.scalar(
        select(Job)
        .where(
            Job.device_id == generation.device_id,
            Job.job_type == spec.job_type,
            Job.status == JobStatus.queued,
            select(DeploymentGeneration.id).where(DeploymentGeneration.job_id == Job.id).exists(),
        )
        # Removals are exempt from the queued-job dedupe index, so several can be queued at
        # once; ordered so the takeover target is the OLDEST rather than whatever the planner
        # happened to return, and so two concurrent callers contend on the same row.
        .order_by(Job.created_at, Job.id)
        .limit(1)
        .with_for_update()
    )
    if existing is None:
        db.add(spec)
        await db.flush()
        return spec
    released = await execute_dml(
        db,
        sa_update(DeploymentGeneration)
        .where(DeploymentGeneration.job_id == existing.id, DeploymentGeneration.id != generation.id)
        .values(job_id=None, updated_at=_now())
        .execution_options(synchronize_session=False),
    )
    if spec.context is not None:
        # A removal's context IS its operation. An apply's is written by the run itself, so
        # there is nothing to carry over and blanking it would drop the previous run's audit.
        existing.context = spec.context
    await db.flush()
    logger.info(
        "generation.took_over_queued_job",
        job_id=existing.id,
        generation_id=generation.id,
        released=released.rowcount,
    )
    return existing


class GenerationNotBlocked(RuntimeError):
    """The generation is not (or is no longer) a blocked head, so neither exit applies."""


async def _claim_blocked_head(db: AsyncSession, generation_id: int, outcome: GenerationStatus) -> None:
    """Move a blocked head to *outcome*, or raise. The compare-and-set both exits share.

    The transition IS the claim. The two operator exits are separate HTTP requests that both
    read "the current head", so a plain assignment lets a retry and an abandon — or two
    retries — each act on the same generation: one duplicates a removal, the other executes
    a deployment the operator gave up on. Only one ``failed``/``outcome_unknown`` row can
    leave that state.
    """
    claimed = await db.scalar(
        sa_update(DeploymentGeneration)
        .where(DeploymentGeneration.id == generation_id, DeploymentGeneration.status.in_(BLOCKED_STATUSES))
        .values(status=outcome, updated_at=_now())
        .returning(DeploymentGeneration.id)
        .execution_options(synchronize_session=False)
    )
    if claimed is None:
        raise GenerationNotBlocked(f"generation {generation_id} is no longer a blocked head")


async def retry_generation(db: AsyncSession, generation_id: int) -> Job | None:
    """Re-admit a blocked head as a NEW job carrying the SAME document, mode and digest (§H2).

    The explicit retry the success barrier needs: a ``failed`` or ``outcome_unknown`` head
    blocks every successor, and the only two ways out are this and
    :func:`reconcile_generation`'s abandon. Nothing is rebuilt — the job re-executes the
    stored document, so a retry of a detach stays a detach and a retry after the store moved
    on still deploys what was authorized.

    Returns the new job, or ``None`` when the generation does not exist. Raises
    :class:`GenerationNotBlocked` if it is not a blocked head: retrying a ``pending`` one
    would double-queue it, and retrying a ``settled`` one would re-deploy a state the device
    already reached. The status claim happens BEFORE the job is built, so a lost race leaves
    no job behind.
    """
    generation = await db.get(DeploymentGeneration, generation_id)
    if generation is None:
        return None
    generation = await _require_locked_executable_head(db, generation)
    if generation.status not in BLOCKED_STATUSES:
        raise GenerationNotBlocked(f"generation {generation_id} is {generation.status.value}, not a blocked head")
    # Digest-verified here too: a retry is precisely the moment the stored bytes are trusted.
    expected = digest_document(generation.mode, generation.document, generation.allowed_removal_keys or {})
    if expected != generation.digest:
        raise GenerationTampered(f"generation {generation_id} digest does not match its document")
    await _claim_blocked_head(db, generation_id, GenerationStatus.pending)
    job = await _queue_job_for(db, generation)
    generation.status = GenerationStatus.pending
    generation.job_id = job.id
    generation.updated_at = _now()
    await db.flush()
    logger.warning(
        "generation.retry_admitted",
        generation_id=generation_id,
        device_id=generation.device_id,
        seq=generation.seq,
        mode=generation.mode.value,
        job_id=job.id,
        attempts=generation.attempts,
    )
    return job


async def reconcile_generation(db: AsyncSession, generation_id: int) -> Job | None:
    """Abandon a blocked head and return the released successor's live job. Caller commits.

    The explicit exit §H2 names. Only a ``failed`` or ``outcome_unknown`` generation can be
    abandoned: settling one by decree would claim a device write that never happened, and
    abandoning a pending one would drop authorized intent on the floor.

    The chain is advanced in THIS transaction. Nothing else would: advancement otherwise runs
    only when a worker finishes a job or the process restarts, and abandoning the blocker is
    exactly the case where no job is going to finish. A successor left unattached — a
    generation that lost its job to noncontiguous coalescing — would then sit ``pending`` with
    nothing to deploy it until an unrelated push or a restart happened along.
    """
    generation = await db.get(DeploymentGeneration, generation_id)
    if generation is None:
        return None
    generation = await _require_locked_executable_head(db, generation)
    if generation.status not in BLOCKED_STATUSES:
        raise GenerationNotBlocked(f"generation {generation_id} is {generation.status.value}, not a blocked head")
    await _claim_blocked_head(db, generation_id, GenerationStatus.abandoned)
    generation.status = GenerationStatus.abandoned
    generation.updated_at = _now()
    await db.flush()
    logger.warning(
        "generation.abandoned",
        generation_id=generation_id,
        device_id=generation.device_id,
        seq=generation.seq,
    )
    await advance_generations_locked(db, generation.device_id)
    successor = await executable_head(db, generation.device_id)
    if successor is None or successor.status is not GenerationStatus.pending:
        return None
    return await _live_job(db, successor.job_id)


async def _live_job(db: AsyncSession, job_id: int | None) -> Job | None:
    if job_id is None:
        return None
    return await db.scalar(select(Job).where(Job.id == job_id, Job.status.in_(_LIVE_JOB)))


async def advance_generations_locked(db: AsyncSession, device_id: int) -> int:
    """Give the device's executable head a job when it has none. Caller commits.

    The advancement itself, with no transaction of its own, so the two callers can each own
    their boundary: :func:`advance_device_generations` runs it after a job finishes and at
    startup, and :func:`reconcile_generation` runs it in the transaction that abandons the
    blocker — the one case where no job is going to finish and release the successor.

    Idempotent by construction: it only acts on a head that is ``pending`` and uncovered.

    A REMOVAL head gets a job built from its OWN ``removal_context`` — never the shared
    queued apply. The test used to be the ``detach`` mode alone, which is the wrong question:
    a delete-origin removal is ``networked``, and handing it an apply job produced a device
    write that pushes the surviving intent and settles the generation without ever deleting
    the entry the operator removed. Mode decides how the removal commits (#106); the
    presence of a removal context decides that it IS one.
    """
    from nso_adapter.core.jobs import admit_queued_job

    head = await executable_head(db, device_id)
    if head is None or head.status is not GenerationStatus.pending:
        return 0
    if await _live_job(db, head.job_id) is not None:
        return 0
    if head.job_id is not None:
        head.job_id = None
        await db.flush()
    if head.removal_context or head.mode is GenerationMode.detach:
        removal_job = await _queue_job_for(db, head)
        head.job_id = removal_job.id
        await db.flush()
        logger.info(
            "generation.advanced_removal",
            device_id=device_id,
            job_id=removal_job.id,
            seq=head.seq,
            mode=head.mode.value,
        )
        return 1
    created, winner = await admit_queued_job(db, device_id, JobType.apply)
    job = created or winner
    if job is None:  # pragma: no cover — sustained admission contention
        # Nothing of ours is half-written: admit_queued_job works inside a SAVEPOINT it has
        # already released. The head stays unattached and the next advancement retries it.
        logger.warning("generation.advance_no_job", device_id=device_id, seq=head.seq)
        return 0
    attached = 0
    following = (
        (
            await db.execute(
                select(DeploymentGeneration)
                .where(
                    DeploymentGeneration.device_id == device_id,
                    DeploymentGeneration.seq >= head.seq,
                    DeploymentGeneration.status == GenerationStatus.pending,
                )
                .order_by(DeploymentGeneration.seq)
            )
        )
        .scalars()
        .all()
    )
    for generation in following:
        if generation.job_id is not None and generation.job_id != job.id:
            break
        try:
            if not await attach_to_job(db, generation, job):
                break
        except GenerationModeConflict:
            break
        attached += 1
    if attached:
        logger.info("generation.advanced", device_id=device_id, job_id=job.id, attached=attached)
    return attached


async def advance_device_generations(device_id: int) -> int:
    """Run :func:`advance_generations_locked` in its own transaction.

    Called after every job finishes, at startup, and after a push whose own admission left
    a generation unattached.
    """
    from nso_adapter.store.db import get_session

    async for db in get_session():
        await lock_projection(db, device_id)
        attached = await advance_generations_locked(db, device_id)
        await db.commit()
        return attached
    return 0


async def recover_generations() -> int:
    """Reconcile generations a dead process left behind, then re-admit the heads. Startup only.

    A generation still ``running`` when no process is running it is the outcome-unknown case
    (§H2): the document may or may not have reached the device, so it blocks its successors
    instead of being assumed either way, until it settles on a retry or is reconciled.
    Everything ``pending`` and uncovered is then handed a job, so a restart resumes the
    chain instead of stranding it.
    """
    from nso_adapter.store.db import get_session

    stranded: list[int] = []
    devices: list[int] = []
    async for db in get_session():
        rows = (
            (
                await db.execute(
                    select(DeploymentGeneration)
                    .outerjoin(Job, Job.id == DeploymentGeneration.job_id)
                    .where(
                        DeploymentGeneration.status == GenerationStatus.running,
                        (Job.id.is_(None)) | (Job.status != JobStatus.running),
                    )
                )
            )
            .scalars()
            .all()
        )
        for generation in rows:
            generation.status = GenerationStatus.outcome_unknown
            generation.updated_at = _now()
            stranded.append(generation.device_id)
        devices = list(
            (
                await db.execute(
                    select(DeploymentGeneration.device_id)
                    .where(DeploymentGeneration.status == GenerationStatus.pending)
                    .distinct()
                )
            )
            .scalars()
            .all()
        )
        await db.commit()
        break
    if stranded:
        logger.error("generation.outcome_unknown_on_restart", count=len(stranded), devices=sorted(set(stranded)))
    for device_id in devices:
        await advance_device_generations(device_id)
    return len(stranded)


__all__ = [
    "BLOCKED_STATUSES",
    "DeviceProjectionGone",
    "GenerationModeConflict",
    "GenerationNotBlocked",
    "GenerationTampered",
    "advance_device_generations",
    "advance_generations_locked",
    "allocate_settlement_cohort",
    "attach_to_job",
    "authorized_streams",
    "consume_last_enqueued_generation_id",
    "create_generation",
    "create_reissue_generation",
    "digest_document",
    "executable_head",
    "executing_generation",
    "job_admissible",
    "lock_projection",
    "mark_job_generations_running",
    "note_write",
    "recover_generations",
    "reconcile_generation",
    "requeue_job_generations",
    "retry_generation",
    "settle_job_generations",
]
