# SPDX-License-Identifier: Apache-2.0
"""Job enqueue helpers and background task runners.

One job per device runs at a time — a second request while a job is
queued/running returns the existing job id for 409 handling in the API layer.

Execution is handled by the durable worker pool (``core.worker``): ``enqueue_job``
only inserts a ``queued`` row; a worker claims and runs it.
"""

from __future__ import annotations

import asyncio

import structlog
from sqlalchemy import exists, literal_column, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.core.claim import (
    UNSET,
    BookkeepingOutcomeUnknown,
    ClaimLostError,
    ClaimRegistration,
    ClaimUnavailableError,
    JobError,
    error_envelope,
    lock_claim,
    terminalize,
)
from nso_adapter.store.models import Job, JobStatus, JobType

logger = structlog.get_logger(__name__)


async def get_queued_job_of_type(device_id: int, job_type: JobType, db: AsyncSession) -> Job | None:
    """Return the queued job of *job_type* — the exact cause of a same-type refusal.

    This is the only correct answer for a 409: telling a caller about some unrelated job
    that happens to be active gives it nothing to wait on or retry against. Ordered so two
    exempt rows (removals) still yield a deterministic answer rather than raising.
    """
    result = await db.execute(
        select(Job)
        .where(Job.device_id == device_id, Job.job_type == job_type, Job.status == JobStatus.queued)
        .order_by(Job.created_at, Job.id)
        .limit(1)
    )
    return result.scalar_one_or_none()


async def has_any_active_job(device_id: int, db: AsyncSession) -> bool:
    """Is anything queued or running for this device? A cheap EXISTS pre-filter.

    Deliberately a boolean and not a row: several rows can legitimately match (removals are
    exempt from queued uniqueness, one per scope), so anything returning "the" active job is
    unsound. Note it is only a PRE-filter for serialization decisions — an apply goes
    terminal while its claim is still held through the post-apply refresh, so a job-status
    query reads "idle" while the device is genuinely busy. The claim is the real gate.
    """
    return bool(
        await db.scalar(
            select(
                exists().where(
                    Job.device_id == device_id,
                    Job.status.in_([JobStatus.queued, JobStatus.running]),
                )
            )
        )
    )


async def get_head_queued_job(device_id: int, db: AsyncSession) -> Job | None:
    """Return the oldest queued job for this device: the per-device FIFO head.

    Worker-internal.
    """
    result = await db.execute(
        select(Job)
        .where(Job.device_id == device_id, Job.status == JobStatus.queued)
        .order_by(Job.created_at, Job.id)
        .limit(1)
    )
    return result.scalar_one_or_none()


# The dedupe index's predicate, verbatim. ON CONFLICT infers the arbiter from the index
# columns PLUS this predicate; PostgreSQL requires it to imply the index's own. The index
# is not a constraint, so `ON CONFLICT ON CONSTRAINT <name>` raises InvalidObjectDefinition
# rather than returning empty — which is the 500 atomic admission exists to prevent.
_QUEUED_DEDUPE_PREDICATE = text("status = 'queued' AND job_type <> 'removal'")

# Bounds applied ONLY to a transaction that ends up holding the queued-winner row lock.
# Production declares no statement or transaction timeout on the engine, so a hung request
# holding that row would starve the job indefinitely. SET LOCAL scopes both to this
# transaction and needs no reset before the connection returns to the pool — a session-level
# value would, and a short idle-transaction bound set session-wide would also kill the
# capability refresh, which legitimately holds one transaction across a 120s NSO action.
_WINNER_LOCK_STATEMENT_TIMEOUT_MS = 60_000
_WINNER_LOCK_IDLE_TX_TIMEOUT_MS = 120_000

_ADMISSION_RETRIES = 3


async def _lock_queued_winner(db: AsyncSession, device_id: int, job_type: JobType) -> Job | None:
    """Row-lock the queued job that won admission, and hold it to the caller's commit.

    That lock is the handoff guarantee (F6): the worker cannot start the winner until the
    caller's own intent mutation is visible, so the job it runs can never carry a snapshot
    older than the request that admitted it.

    Zero rows means the winner is no longer queued — it went running or terminal between the
    failed insert and this lookup — and the caller should retry admission to create a
    successor. ``FOR UPDATE`` re-checks the predicate after acquiring the lock, so this
    cannot return a row that has since changed status.
    """
    return await db.scalar(
        select(Job)
        .where(Job.device_id == device_id, Job.job_type == job_type, Job.status == JobStatus.queued)
        .order_by(Job.created_at, Job.id)
        .limit(1)
        .with_for_update()
    )


async def admit_queued_job(
    db: AsyncSession,
    device_id: int | None,
    job_type: JobType,
    *,
    context: dict | None = None,
) -> tuple[Job | None, Job | None]:
    """Atomically admit a queued job of *job_type*, or hand back the queued winner.

    Returns ``(created, winner)`` with exactly one of them set. The insert runs inside a
    SAVEPOINT so a conflict cannot poison the caller's transaction — every auto-apply
    endpoint calls this with intent rows already mutated and uncommitted, and losing those
    is a silent data loss, not a retryable error.

    Does NOT commit: the caller owns its transaction boundary.
    """
    for _attempt in range(_ADMISSION_RETRIES):
        values: dict = {"job_type": job_type, "device_id": device_id, "status": JobStatus.queued}
        if context is not None:
            values["context"] = context

        async with db.begin_nested():
            job_id = await db.scalar(
                pg_insert(Job)
                .values(**values)
                .on_conflict_do_nothing(
                    index_elements=[Job.device_id, Job.job_type],
                    index_where=_QUEUED_DEDUPE_PREDICATE,
                )
                .returning(Job.id)
            )

        if job_id is not None:
            created = await db.get(Job, job_id)
            return created, None

        # Lost to an existing queued row. Bound this transaction before parking on its lock.
        await db.execute(text(f"SET LOCAL statement_timeout = '{_WINNER_LOCK_STATEMENT_TIMEOUT_MS}ms'"))
        await db.execute(text(f"SET LOCAL idle_in_transaction_session_timeout = '{_WINNER_LOCK_IDLE_TX_TIMEOUT_MS}ms'"))
        winner = await _lock_queued_winner(db, device_id, job_type)
        if winner is not None:
            return None, winner
        # The winner started running: our caller's intent is newer than its snapshot, so a
        # successor is the correct answer — never "blocked".
        logger.debug("job.admission.winner_started", device_id=device_id, job_type=str(job_type))

    logger.warning("job.admission.retries_exhausted", device_id=device_id, job_type=str(job_type))
    return None, None


async def enqueue_job(
    device_id: int,
    job_type: JobType,
    db: AsyncSession,
) -> tuple[Job, bool]:
    """Create a queued job.  Returns (job, created).

    If an active job already exists for the device, returns that job with
    created=False so the caller can return 409.  The durable worker pool
    (``core.worker``) claims and runs the job.
    """
    if job_type not in _JOB_RUNNERS:
        raise ValueError(f"No runner registered for job type {job_type!r}")

    if job_type is JobType.apply and device_id is not None:
        # §H4's atomic-under-lock boundary, taken BEFORE anything is selected: WHICH streams
        # this Apply promotes, the document they authorize and the job that carries it are
        # one unit against every other projection writer. Selecting first and locking inside
        # create_generation left a window in which a stream committed after the selection
        # was promoted by nobody — its intent stayed undeployed with no record of why.
        # It also fixes the lock order on this path: every other producer takes the
        # projection lock before it touches a job row.
        from nso_adapter.core.generation import lock_projection

        await lock_projection(db, device_id)

    # Atomic same-type queued dedupe. No check-then-insert: the DB decides, so no TOCTOU
    # window exists and a conflict never surfaces as an IntegrityError the caller must
    # recover from. A queued removal does not refuse a sync, and a running job does not
    # refuse its own successor — the device claim serializes execution.
    created, winner = await admit_queued_job(db, device_id, job_type)
    if winner is not None:
        logger.debug("job.enqueue.race_lost", device_id=device_id, winner_id=winner.id)
        if job_type is JobType.apply and device_id is not None:
            attached = await _authorize_apply_job(db, device_id, winner)
            if not attached:
                logger.debug(
                    "job.enqueue.winner_cannot_carry_generation",
                    device_id=device_id,
                    winner_id=winner.id,
                )
        await db.commit()  # release the winner lock; this helper owns its transaction
        return winner, False
    if created is None:  # pragma: no cover - retries exhausted under sustained contention
        raise RuntimeError(f"could not admit a {job_type} job for device {device_id}")

    if job_type is JobType.apply and device_id is not None:
        if not await _authorize_apply_job(db, device_id, created):  # pragma: no cover - an empty job always accepts
            raise RuntimeError(f"new apply job {created.id} refused its first generation")

    # This helper owns the outer commit and its API/scheduler callers depend on that:
    # get_db does not auto-commit.
    await db.commit()
    await db.refresh(created)
    return created, True


async def _authorize_apply_job(db: AsyncSession, device_id: int, job: Job) -> bool:
    """Give an operator-triggered Apply its deployment generation (#1522 §G1/§H4).

    Here rather than in the API handler, and inside the SAME transaction as the job insert:
    an apply job with no generation is a device write with no place in the device's ordered
    chain — free to cross a blocked head, and settling nothing when it succeeds.

    Pressing Apply IS the authorization for what the store holds; that is already what the
    apply job pushes. So every stream with a desired state is promoted and the resulting
    complete document becomes what the job deploys. The plugin-side selection of WHICH
    revisions an Apply promotes (§H4) narrows this set in slice 2; it does not change the
    shape.

    The caller already holds the device's projection lock, so the selection below and the
    promotion that follows read one snapshot.
    """
    from nso_adapter.core.generation import attach_to_job, authorized_streams, create_generation
    from nso_adapter.store.models import GenerationMode

    streams = await authorized_streams(db, device_id)
    if not streams:
        # Nothing has ever been written for this device: the apply has nothing to deploy, and
        # a generation over an empty document would order a device write that does not exist.
        return True
    async with db.begin_nested() as authorization:
        generation = await create_generation(db, device_id, streams=streams, mode=GenerationMode.networked)
        if not await attach_to_job(db, generation, job):
            # The action returns 409 naming *job*. If that job cannot carry this generation,
            # leave the projection unpromoted so the response remains a truthful refusal.
            await authorization.rollback()
            return False
    return True


# The provision dedupe index's expressions and predicate, verbatim and defined ONCE: the
# same two constants are the ON CONFLICT inference target and the lookup's filter, so the
# lookup can never drift from what the database actually enforces. literal_column, not text:
# ON CONFLICT infers from column EXPRESSIONS, and a TextClause is not one.
_PROVISION_PAIR_ELEMENTS = (
    literal_column("(context ->> 'nso_instance')"),
    literal_column("(context ->> 'device_name')"),
)
_PROVISION_DEDUPE_PREDICATE = text("status IN ('queued', 'running') AND job_type = 'provision'")


async def get_active_provision_job(nso_instance: str, device_name: str, db: AsyncSession) -> Job | None:
    """Return the queued/running provision job for (instance, device_name), or None.

    Provision jobs run *before* the adapter ``Device`` row exists, so they carry no
    ``device_id`` — the de-dup key lives in ``Job.context`` instead. Scoped to the
    two context fields that uniquely identify the in-flight onboarding.

    Ordered and limited rather than ``scalar_one_or_none``: rows admitted before
    ``uq_job_active_provision_pair`` existed can still be duplicated, and a lookup that
    raises on them would take out every subsequent onboarding of that node.
    """
    return await db.scalar(
        select(Job)
        .where(
            _PROVISION_DEDUPE_PREDICATE,
            _PROVISION_PAIR_ELEMENTS[0] == nso_instance,
            _PROVISION_PAIR_ELEMENTS[1] == device_name,
        )
        .order_by(Job.created_at, Job.id)
        .limit(1)
    )


async def enqueue_provision_job(params: dict, db: AsyncSession) -> tuple[Job, bool]:
    """Create a queued provision (device-onboarding) job.  Returns (job, created).

    Unlike :func:`enqueue_job`, a provision runs before the device exists, so the job has
    ``device_id=None`` and carries its parameters in ``context``; de-dup is on
    (nso_instance, device_name) so a double-click returns the in-flight job
    (created=False) instead of provisioning twice.

    The DB decides, not a preceding lookup. A check-then-insert let two concurrent requests
    for the same node both find nothing and both admit — and nothing downstream would have
    stopped them, because the two runners onboard the same NSO node with no claim between
    them until each reaches its own mapping. The loser now loses on the index conflict and
    is handed the winner's job.

    Zero rows with no active job means the winner reached a terminal status between the two
    statements; a fresh admission is then the correct answer, not "blocked".
    """
    for _attempt in range(_ADMISSION_RETRIES):
        async with db.begin_nested():
            job_id = await db.scalar(
                pg_insert(Job)
                .values(job_type=JobType.provision, device_id=None, status=JobStatus.queued, context=params)
                .on_conflict_do_nothing(
                    index_elements=_PROVISION_PAIR_ELEMENTS,
                    index_where=_PROVISION_DEDUPE_PREDICATE,
                )
                .returning(Job.id)
            )
        if job_id is not None:
            await db.commit()
            job = await db.get(Job, job_id)
            return job, True

        active = await get_active_provision_job(params["nso_instance"], params["device_name"], db)
        if active is not None:
            return active, False
        logger.debug("job.provision_admission.winner_finished", device_name=params.get("device_name"))

    logger.warning("job.provision_admission.retries_exhausted", device_name=params.get("device_name"))
    raise RuntimeError(f"could not admit a provision job for {params.get('device_name')!r}")


# ── Job runners ───────────────────────────────────────────────────────────────
#
# Every runner takes the worker's live ClaimRegistration, uniformly, so the worker never
# has to decide by job type which runners want one. Only provision reads it today: it is
# the one runner that starts claimless and acquires its claim mid-run.


async def _mark_job_failed(
    db: AsyncSession,
    job_id: int,
    error: dict,
    reg: ClaimRegistration | None = None,
) -> None:
    """Record a terminal ``failed`` status on *job_id*, tolerating a poisoned session.

    A DB-origin error inside a runner's ``try`` leaves the AsyncSession in a
    needs-rollback state; committing the failed status without rolling back first
    would itself re-raise (PendingRollbackError) and strand the job in ``running``.
    Roll back, re-fetch the (possibly expired) job, then commit the terminal status.
    Same fix as :func:`core.apply.run_apply` (finding #11); shared so the other
    runners stay consistent.

    This is an effectful transaction on a claimed device, so it takes the claim row lock
    when *reg* is supplied — after the rollback, before the re-fetch, leaving the
    rollback-first contract intact. Without the lock the concrete failure is: recovery
    revokes a stale sync's claim and requeues the job, the old runner raises
    ``ClaimLostError``, its wrapper converts that into a call here, and this write
    overwrites recovery's disposition — or a fresh worker's ``running``.

    The compare-and-set on ``(running, run_attempt)`` is the rest of that guard, and it is
    what covers the claimless lane, which has no claim row to lock at all.
    """
    await db.rollback()
    if reg is not None:
        try:
            await lock_claim(db, reg)
        except ClaimLostError:
            logger.warning("job.mark_failed_claim_lost", job_id=job_id, device_id=reg.device_id)
            return
    await terminalize(
        db,
        job_id,
        status=JobStatus.failed,
        expect=JobStatus.running,
        run_attempt=reg.run_attempt if reg is not None else None,
        error=error,
    )
    await db.commit()


async def _run_with_db(
    job_id: int,
    device_id: int,
    coro_factory,
    *,
    timeout: float = 600.0,
    reg: ClaimRegistration | None = None,
) -> None:
    """Run *coro_factory* under a total job budget and write its terminal status.

    *reg* names the execution this run belongs to. Without it the terminal write has no
    way to say which run it reports, and an abandoned runner could suppress the rerun
    recovery mandated or clobber the successor that replaced it.
    """
    from nso_adapter.store.db import get_session

    # Total job timeout, default 10 minutes: guards against NSO hung connections that
    # outlast the per-request httpx timeout (e.g. TCP keepalive issues or mid-response
    # stalls). The comprehensive runners (sync_now, sync_from_nso) pass 900s — their
    # legal child waits alone sum to ~720s (NED resolve 30 + sync-from 120 + attrs
    # 30+180 escalation + atomic action 360; codex S5a R1-F3).
    async for db in get_session():
        if await db.scalar(select(Job.id).where(Job.id == job_id)) is None:
            return
        logger.info("job.budget", job_id=job_id, device_id=device_id, timeout=timeout)
        try:
            result = await asyncio.wait_for(coro_factory(device_id, db), timeout=timeout)
            write = await terminalize(
                db,
                job_id,
                status=JobStatus.succeeded,
                expect=JobStatus.running,
                run_attempt=reg.run_attempt if reg is not None else None,
                result=result,
            )
            if write is not None:
                await db.commit()
            else:
                # Refused: recovery re-dispositioned the job mid-run. The session's writes
                # belong to an execution that lost ownership, so the transaction is discarded.
                await db.rollback()
        except TimeoutError:
            logger.error("job.timeout", job_id=job_id, device_id=device_id, timeout=timeout)
            await _mark_job_failed(
                db,
                job_id,
                {"code": "timeout", "message": f"Job exceeded {int(timeout)}s timeout", "detail": {}},
                reg,
            )
        except ClaimLostError:
            # Revocation is not a runner error: recovery already owns the disposition.
            raise
        except BookkeepingOutcomeUnknown:
            # R2 §4.6 / Appendix S §3.3: the terminal transaction did not complete after its
            # effect was performed. A second write here reports a failure for work that
            # really landed; recovery re-dispositions a job still `running` instead.
            raise
        except Exception as exc:
            logger.exception("job.failed", job_id=job_id, device_id=device_id, error=repr(exc))
            await _mark_job_failed(db, job_id, error_envelope(exc), reg)


async def _run_sync(job_id: int, device_id: int, reg: ClaimRegistration | None = None) -> None:
    from nso_adapter.core.importer import sync_device

    logger.info("job.sync.start", job_id=job_id, device_id=device_id)
    await _run_with_db(job_id, device_id, sync_device, reg=reg)


async def _run_sync_now(job_id: int, device_id: int, reg: ClaimRegistration | None = None) -> None:
    """Operator Sync-Now: grain-c atomic AND comprehensive (S5a).

    One atomic ``device-state-read`` covering ALL surfaces, not just the lean routing
    set — the doc families have no other read source on quiet devices.
    """
    from nso_adapter.core.importer import sync_device

    logger.info("job.sync_now.start", job_id=job_id, device_id=device_id)

    async def _atomic_sync(device_id_: int, db) -> dict:
        return await sync_device(device_id_, db, atomic=True, comprehensive=True)

    await _run_with_db(job_id, device_id, _atomic_sync, timeout=900.0, reg=reg)


async def _run_sync_from_nso(job_id: int, device_id: int, reg: ClaimRegistration | None = None) -> None:
    """Operator "Sync from NSO" (S5a B): comprehensive CDB-only mirror read.

    All surfaces from ONE atomic ``device-state-read`` — NO device ``sync-from``, no
    ``last_sync_*`` writes (those describe device-reread truthfulness, which this job
    does not perform). A TOTAL supplier failure (the one read itself failed — nothing
    refreshed) FAILS the job honestly instead of reporting green success (codex R1-F2).
    """
    from nso_adapter.core.importer import get_netbox_client, get_nso_client, refresh_all_surfaces_for_device
    from nso_adapter.store.models import Device

    logger.info("job.sync_from_nso.start", job_id=job_id, device_id=device_id)

    async def _mirror_read(device_id_: int, db) -> dict:
        device = await db.get(Device, device_id_)
        if not device:
            raise JobError("not_found", f"Device {device_id_} not found")
        client = get_nso_client(device.nso_instance)
        failed, supplier_outcome = await refresh_all_surfaces_for_device(
            db, device, client, refresh_source="sync_from_nso", atomic=True
        )
        if supplier_outcome is not None:
            raise JobError("read_unavailable", "NSO read unavailable — nothing refreshed; last-known data kept")
        nb_client = get_netbox_client()
        if nb_client and device.netbox_device_id:
            try:
                await nb_client.notify_sync_complete(device.netbox_device_id)
            except Exception as exc:  # noqa: BLE001 — best-effort; the mirror is refreshed
                logger.warning(
                    "netbox.sync_complete_notify_failed", device_id=device_id_, error=str(exc) or type(exc).__name__
                )
        return {"degraded_surfaces": sorted(failed)}

    await _run_with_db(job_id, device_id, _mirror_read, timeout=900.0, reg=reg)


async def _run_detect_drift(job_id: int, device_id: int, reg: ClaimRegistration | None = None) -> None:
    from nso_adapter.core.importer import detect_drift

    logger.info("job.detect_drift.start", job_id=job_id, device_id=device_id)
    await _run_with_db(job_id, device_id, detect_drift, reg=reg)


async def _run_connect(job_id: int, device_id: int, reg: ClaimRegistration | None = None) -> None:
    from nso_adapter.core.importer import get_nso_client
    from nso_adapter.nso.actions import connect
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device

    _JOB_TIMEOUT = 600.0

    logger.info("job.connect.start", job_id=job_id, device_id=device_id)

    async for db in get_session():
        if await db.scalar(select(Job.id).where(Job.id == job_id)) is None:
            return
        try:
            device = await db.get(Device, device_id)
            if not device:
                raise JobError("not_found", f"Device {device_id} not found")
            client = get_nso_client(device.nso_instance)

            async def _do_connect(dev_id: int, _db) -> dict:
                output = await connect(client, device.nso_device_name)
                return {"output": output}

            result = await asyncio.wait_for(_do_connect(device_id, db), timeout=_JOB_TIMEOUT)
            write = await terminalize(
                db,
                job_id,
                status=JobStatus.succeeded,
                expect=JobStatus.running,
                run_attempt=reg.run_attempt if reg is not None else None,
                result=result,
            )
            if write is not None:
                await db.commit()
            else:
                # Refused: same discard rule as _run_with_db above.
                await db.rollback()
        except TimeoutError:
            logger.error("job.connect.timeout", job_id=job_id, timeout=_JOB_TIMEOUT)
            await _mark_job_failed(
                db,
                job_id,
                {"code": "timeout", "message": f"Connect exceeded {int(_JOB_TIMEOUT)}s timeout", "detail": {}},
                reg,
            )
        except ClaimLostError:
            # Revocation is not a runner error: recovery already owns the disposition.
            raise
        except BookkeepingOutcomeUnknown:
            # R2 §4.6 / Appendix S §3.3: the terminal transaction did not complete after its
            # effect was performed. A second write here reports a failure for work that
            # really landed; recovery re-dispositions a job still `running` instead.
            raise
        except Exception as exc:
            logger.exception("job.connect.failed", job_id=job_id, error=repr(exc))
            await _mark_job_failed(db, job_id, error_envelope(exc), reg)


async def _run_apply(job_id: int, device_id: int, reg: ClaimRegistration | None = None) -> None:
    from nso_adapter.core.apply import run_apply

    logger.info("job.apply.start", job_id=job_id, device_id=device_id)
    # The claim registration goes THROUGH to the runner: R1 kept it here, so no write the
    # runner makes could be claim-scoped. R2's carrier/CAS transactions need the token.
    await run_apply(job_id, device_id, force=True, reg=reg)


async def _run_removal(job_id: int, device_id: int, reg: ClaimRegistration | None = None) -> None:
    from nso_adapter.core.removal import run_removal

    logger.info("job.removal.start", job_id=job_id, device_id=device_id)
    await run_removal(job_id, device_id, reg=reg)


async def _notify_provision_complete(job_id: int) -> None:
    """Best-effort: tell the plugin a provision job finished so it advances the onboarding row.

    Fire-and-forget — a callback failure must not fail the job; the plugin's device-tab self-heal
    and hourly sweep still catch a missed notification. No-op when the NetBox client is unset
    (e.g. tests, or a deployment without the plugin callback wired).
    """
    from nso_adapter.core.importer import get_netbox_client

    nb = get_netbox_client()
    if nb is None:
        return
    try:
        await nb.notify_provision_complete(job_id)
    except Exception as exc:  # noqa: BLE001 - best-effort callback; never fail the job on it
        logger.warning("netbox.provision_complete_notify_failed", job_id=job_id, error=str(exc) or type(exc).__name__)


async def _run_provision(job_id: int, device_id: int | None, reg: ClaimRegistration | None = None) -> None:
    """Run a queued device-onboarding job from its stored ``context`` parameters.

    Provision has no ``device_id`` (the adapter Device row may be created mid-job);
    the parameters live in ``Job.context``. The whole create→fetch-keys→unlock→
    sync-from sequence can be slow (probe-then-OOB-bootstrap + a full sync-from), so it
    runs under the same 600s guard as the other long jobs. The provision core never
    raises for a blocking step — it returns ``{ok: False, steps: [...]}`` — so the job
    *succeeds* (the work ran) and the caller inspects ``result.ok``; only an unexpected
    crash or the timeout marks the job failed.

    The only runner that starts CLAIMLESS and acquires mid-run, which is why it is handed
    the worker's live ``ClaimRegistration``: everything from the mapping onwards runs under
    the device claim, and the heartbeat and terminal writer read that same object live.
    Contention on the device is one such honest failure — see ``device_busy`` below.
    """
    from nso_adapter.core.onboarding import provision_nso_device
    from nso_adapter.store.db import get_session

    _JOB_TIMEOUT = 600.0
    logger.info("job.provision.start", job_id=job_id)

    async for db in get_session():
        context = await db.scalar(select(Job.context).where(Job.id == job_id))
        if context is None and await db.scalar(select(Job.id).where(Job.id == job_id)) is None:
            return
        params = dict(context or {})
        try:
            result = await asyncio.wait_for(
                provision_nso_device(db, **params, reg=reg, job_id=job_id), timeout=_JOB_TIMEOUT
            )
            # The terminal write is an effect performed on behalf of the claim once the run
            # has one, so it takes the row lock like every other guarded write. Without it a
            # run whose stale claim was revoked, and whose job recovery already
            # re-dispositioned, still commits `succeeded` over that disposition.
            if reg is not None:
                await lock_claim(db, reg)
            # The device this run created is attached in the SAME statement as the terminal
            # status. UNSET means "leave it attached" — never "set NULL".
            provisioned_device_id = result.get("device_id")
            write = await terminalize(
                db,
                job_id,
                status=JobStatus.succeeded,
                expect=JobStatus.running,
                run_attempt=reg.run_attempt if reg is not None else None,
                result=result,
                set_device_id=provisioned_device_id if provisioned_device_id is not None else UNSET,
            )
            if write is not None:
                await db.commit()
            else:
                # Refused: recovery re-dispositioned the job mid-run. The session's writes
                # belong to an execution that lost ownership, so the transaction is discarded.
                await db.rollback()
        except ClaimUnavailableError as exc:
            # The device stayed claimed for the whole OQ6 budget, so the mapping was refused
            # and NOTHING was written. Honestly retryable — and terminal, so the pair's
            # admission slot frees for the retry. Necessarily still claimless: this is
            # raised by the acquisition itself, which is exactly why the registration must
            # ride along — with no claim row to lock, the attempt on it is the only proof
            # this write belongs to THIS run and not to the successor that replaced it.
            logger.warning("job.provision.device_busy", job_id=job_id, error=repr(exc))
            await _mark_job_failed(
                db,
                job_id,
                {
                    "code": "device_busy",
                    "message": "The device is busy — another operation holds it",
                    "detail": {"reason": "claim_unavailable", "retryable": True},
                },
                reg,
            )
        except TimeoutError:
            logger.error("job.provision.timeout", job_id=job_id, timeout=_JOB_TIMEOUT)
            await _mark_job_failed(
                db,
                job_id,
                {"code": "timeout", "message": f"Provision exceeded {int(_JOB_TIMEOUT)}s timeout", "detail": {}},
                reg,
            )
        except ClaimLostError:
            # Revocation is not a runner error: recovery already owns the disposition.
            raise
        except BookkeepingOutcomeUnknown:
            # R2 §4.6 / Appendix S §3.3: the terminal transaction did not complete after its
            # effect was performed. A second write here reports a failure for work that
            # really landed; recovery re-dispositions a job still `running` instead.
            raise
        except Exception as exc:
            logger.exception("job.provision.failed", job_id=job_id, error=repr(exc))
            await _mark_job_failed(db, job_id, error_envelope(exc), reg)

    # Tell the plugin the provision job reached a terminal state (any branch above) so it advances
    # the gated onboarding row off the dashboard-poll path. Best-effort — the plugin's device-tab
    # self-heal and hourly sweep still catch a missed callback.
    await _notify_provision_complete(job_id)


_JOB_RUNNERS = {
    JobType.sync: _run_sync,
    JobType.sync_now: _run_sync_now,
    JobType.sync_from_nso: _run_sync_from_nso,
    JobType.detect_drift: _run_detect_drift,
    JobType.connect: _run_connect,
    JobType.apply: _run_apply,
    JobType.removal: _run_removal,
    JobType.provision: _run_provision,
}
