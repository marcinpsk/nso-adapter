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
    await db.commit()
    await db.refresh(job)
    return job, True


# ── Job runners ───────────────────────────────────────────────────────────────


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
        except TimeoutError:
            logger.error("job.timeout", job_id=job_id, device_id=device_id, timeout=_JOB_TIMEOUT)
            job.status = JobStatus.failed
            job.error = {
                "code": "timeout",
                "message": f"Job exceeded {int(_JOB_TIMEOUT)}s timeout",
                "detail": {},
            }
        except Exception as exc:
            logger.exception("job.failed", job_id=job_id, device_id=device_id, error=repr(exc))
            job.status = JobStatus.failed
            job.error = {"code": "internal", "message": repr(exc), "detail": {}}
        finally:
            await db.commit()


async def _run_sync(job_id: int, device_id: int) -> None:
    from nso_adapter.core.importer import sync_device

    logger.info("job.sync.start", job_id=job_id, device_id=device_id)
    await _run_with_db(job_id, device_id, sync_device)


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
        except TimeoutError:
            logger.error("job.connect.timeout", job_id=job_id, timeout=_JOB_TIMEOUT)
            job.status = JobStatus.failed
            job.error = {
                "code": "timeout",
                "message": f"Connect exceeded {int(_JOB_TIMEOUT)}s timeout",
                "detail": {},
            }
        except Exception as exc:
            logger.exception("job.connect.failed", job_id=job_id, error=repr(exc))
            job.status = JobStatus.failed
            job.error = {"code": "internal", "message": repr(exc), "detail": {}}
        finally:
            await db.commit()


async def _run_apply(job_id: int, device_id: int) -> None:
    from nso_adapter.core.apply import run_apply

    logger.info("job.apply.start", job_id=job_id, device_id=device_id)
    await run_apply(job_id, device_id, force=True)


async def _run_removal(job_id: int, device_id: int) -> None:
    from nso_adapter.core.removal import run_removal

    logger.info("job.removal.start", job_id=job_id, device_id=device_id)
    await run_removal(job_id, device_id)


_JOB_RUNNERS = {
    JobType.sync: _run_sync,
    JobType.detect_drift: _run_detect_drift,
    JobType.connect: _run_connect,
    JobType.apply: _run_apply,
    JobType.removal: _run_removal,
}
