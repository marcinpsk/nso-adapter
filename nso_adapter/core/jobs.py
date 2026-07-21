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
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.store.models import Job, JobStatus, JobType

logger = structlog.get_logger(__name__)


async def get_active_job(device_id: int, db: AsyncSession) -> Job | None:
    """Return the currently queued/running job for *device_id*, or None."""
    result = await db.execute(
        select(Job).where(
            Job.device_id == device_id,
            Job.status.in_([JobStatus.queued, JobStatus.running]),
        )
    )
    return result.scalar_one_or_none()


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

    active = await get_active_job(device_id, db)
    if active:
        return active, False

    job = Job(job_type=job_type, device_id=device_id, status=JobStatus.queued)
    db.add(job)
    try:
        await db.commit()
    except IntegrityError:
        # Lost the check-then-insert race: a concurrent enqueue committed the active
        # job for this device first, and uq_job_active_per_device rejected ours. Recover
        # by returning the winner instead of surfacing a 500.
        await db.rollback()
        winner = await get_active_job(device_id, db)
        if winner is not None:
            logger.debug("job.enqueue.race_lost", device_id=device_id, winner_id=winner.id)
            return winner, False
        raise
    await db.refresh(job)
    return job, True


async def get_active_provision_job(nso_instance: str, device_name: str, db: AsyncSession) -> Job | None:
    """Return the queued/running provision job for (instance, device_name), or None.

    Provision jobs run *before* the adapter ``Device`` row exists, so they carry no
    ``device_id`` — the de-dup key lives in ``Job.context`` instead. Scoped to the
    two context fields that uniquely identify the in-flight onboarding.
    """
    result = await db.execute(
        select(Job).where(
            Job.job_type == JobType.provision,
            Job.status.in_([JobStatus.queued, JobStatus.running]),
        )
    )
    for job in result.scalars().all():
        ctx = job.context or {}
        if ctx.get("nso_instance") == nso_instance and ctx.get("device_name") == device_name:
            return job
    return None


async def enqueue_provision_job(params: dict, db: AsyncSession) -> tuple[Job, bool]:
    """Create a queued provision (device-onboarding) job.  Returns (job, created).

    Unlike :func:`enqueue_job`, a provision runs before the device exists, so the job
    has ``device_id=None`` and carries its parameters in ``context``; de-dup is on
    (nso_instance, device_name) via :func:`get_active_provision_job` so a double-click
    returns the in-flight job (created=False) instead of provisioning twice.
    """
    active = await get_active_provision_job(params["nso_instance"], params["device_name"], db)
    if active:
        return active, False

    job = Job(job_type=JobType.provision, device_id=None, status=JobStatus.queued, context=params)
    db.add(job)
    await db.commit()
    await db.refresh(job)
    return job, True


# ── Job runners ───────────────────────────────────────────────────────────────


async def _mark_job_failed(db: AsyncSession, job_id: int, error: dict) -> None:
    """Record a terminal ``failed`` status on *job_id*, tolerating a poisoned session.

    A DB-origin error inside a runner's ``try`` leaves the AsyncSession in a
    needs-rollback state; committing the failed status without rolling back first
    would itself re-raise (PendingRollbackError) and strand the job in ``running``.
    Roll back, re-fetch the (possibly expired) job, then commit the terminal status.
    Same fix as :func:`core.apply.run_apply` (finding #11); shared so the other
    runners stay consistent.
    """
    await db.rollback()
    job = await db.get(Job, job_id)
    if job is not None:
        job.status = JobStatus.failed
        job.error = error
        await db.commit()


async def _run_with_db(job_id: int, device_id: int, coro_factory) -> None:
    from nso_adapter.store.db import get_session

    # Total job timeout: 10 minutes.  This guards against NSO hung connections
    # that outlast the per-request httpx timeout (e.g. TCP keepalive issues or
    # mid-response stalls that don't trigger the read timeout).
    _JOB_TIMEOUT = 600.0

    async for db in get_session():
        job = await db.get(Job, job_id)
        if not job:
            return
        job.status = JobStatus.running
        await db.commit()
        try:
            result = await asyncio.wait_for(coro_factory(device_id, db), timeout=_JOB_TIMEOUT)
            job.status = JobStatus.succeeded
            job.result = result
            await db.commit()
        except TimeoutError:
            logger.error("job.timeout", job_id=job_id, device_id=device_id, timeout=_JOB_TIMEOUT)
            await _mark_job_failed(
                db,
                job_id,
                {"code": "timeout", "message": f"Job exceeded {int(_JOB_TIMEOUT)}s timeout", "detail": {}},
            )
        except Exception as exc:
            logger.exception("job.failed", job_id=job_id, device_id=device_id, error=repr(exc))
            await _mark_job_failed(db, job_id, {"code": "internal", "message": repr(exc), "detail": {}})


async def _run_sync(job_id: int, device_id: int) -> None:
    from nso_adapter.core.importer import sync_device

    logger.info("job.sync.start", job_id=job_id, device_id=device_id)
    await _run_with_db(job_id, device_id, sync_device)


async def _run_sync_now(job_id: int, device_id: int) -> None:
    """Operator Sync-Now: the sync body on READSEM grain c (one atomic device-state-read)."""
    from nso_adapter.core.importer import sync_device

    logger.info("job.sync_now.start", job_id=job_id, device_id=device_id)

    async def _atomic_sync(device_id_: int, db) -> dict:
        return await sync_device(device_id_, db, atomic=True)

    await _run_with_db(job_id, device_id, _atomic_sync)


async def _run_detect_drift(job_id: int, device_id: int) -> None:
    from nso_adapter.core.importer import detect_drift

    logger.info("job.detect_drift.start", job_id=job_id, device_id=device_id)
    await _run_with_db(job_id, device_id, detect_drift)


async def _run_connect(job_id: int, device_id: int) -> None:
    from nso_adapter.core.importer import get_nso_client
    from nso_adapter.nso.actions import connect
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device

    _JOB_TIMEOUT = 600.0

    logger.info("job.connect.start", job_id=job_id, device_id=device_id)

    async for db in get_session():
        job = await db.get(Job, job_id)
        if not job:
            return
        job.status = JobStatus.running
        await db.commit()
        try:
            device = await db.get(Device, device_id)
            if not device:
                raise ValueError(f"Device {device_id} not found")
            client = get_nso_client(device.nso_instance)

            async def _do_connect(dev_id: int, _db) -> dict:
                output = await connect(client, device.nso_device_name)
                return {"output": output}

            result = await asyncio.wait_for(_do_connect(device_id, db), timeout=_JOB_TIMEOUT)
            job.status = JobStatus.succeeded
            job.result = result
            await db.commit()
        except TimeoutError:
            logger.error("job.connect.timeout", job_id=job_id, timeout=_JOB_TIMEOUT)
            await _mark_job_failed(
                db,
                job_id,
                {"code": "timeout", "message": f"Connect exceeded {int(_JOB_TIMEOUT)}s timeout", "detail": {}},
            )
        except Exception as exc:
            logger.exception("job.connect.failed", job_id=job_id, error=repr(exc))
            await _mark_job_failed(db, job_id, {"code": "internal", "message": repr(exc), "detail": {}})


async def _run_apply(job_id: int, device_id: int) -> None:
    from nso_adapter.core.apply import run_apply

    logger.info("job.apply.start", job_id=job_id, device_id=device_id)
    await run_apply(job_id, device_id, force=True)


async def _run_removal(job_id: int, device_id: int) -> None:
    from nso_adapter.core.removal import run_removal

    logger.info("job.removal.start", job_id=job_id, device_id=device_id)
    await run_removal(job_id, device_id)


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


async def _run_provision(job_id: int, device_id: int | None) -> None:
    """Run a queued device-onboarding job from its stored ``context`` parameters.

    Provision has no ``device_id`` (the adapter Device row may be created mid-job);
    the parameters live in ``Job.context``. The whole create→fetch-keys→unlock→
    sync-from sequence can be slow (probe-then-OOB-bootstrap + a full sync-from), so it
    runs under the same 600s guard as the other long jobs. The provision core never
    raises for a blocking step — it returns ``{ok: False, steps: [...]}`` — so the job
    *succeeds* (the work ran) and the caller inspects ``result.ok``; only an unexpected
    crash or the timeout marks the job failed.
    """
    from nso_adapter.core.onboarding import provision_nso_device
    from nso_adapter.store.db import get_session

    _JOB_TIMEOUT = 600.0
    logger.info("job.provision.start", job_id=job_id)

    async for db in get_session():
        job = await db.get(Job, job_id)
        if not job:
            return
        params = dict(job.context or {})
        job.status = JobStatus.running
        await db.commit()
        try:
            result = await asyncio.wait_for(provision_nso_device(db, **params), timeout=_JOB_TIMEOUT)
            job.status = JobStatus.succeeded
            job.result = result
            # Link the job to the device it created so history/lookup works post-onboard.
            if result.get("device_id") is not None:
                job.device_id = result["device_id"]
            await db.commit()
        except TimeoutError:
            logger.error("job.provision.timeout", job_id=job_id, timeout=_JOB_TIMEOUT)
            await _mark_job_failed(
                db,
                job_id,
                {"code": "timeout", "message": f"Provision exceeded {int(_JOB_TIMEOUT)}s timeout", "detail": {}},
            )
        except Exception as exc:
            logger.exception("job.provision.failed", job_id=job_id, error=repr(exc))
            await _mark_job_failed(db, job_id, {"code": "internal", "message": repr(exc), "detail": {}})

    # Tell the plugin the provision job reached a terminal state (any branch above) so it advances
    # the gated onboarding row off the dashboard-poll path. Best-effort — the plugin's device-tab
    # self-heal and hourly sweep still catch a missed callback.
    await _notify_provision_complete(job_id)


_JOB_RUNNERS = {
    JobType.sync: _run_sync,
    JobType.sync_now: _run_sync_now,
    JobType.detect_drift: _run_detect_drift,
    JobType.connect: _run_connect,
    JobType.apply: _run_apply,
    JobType.removal: _run_removal,
    JobType.provision: _run_provision,
}
