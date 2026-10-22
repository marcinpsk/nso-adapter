# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Run directly seeded execution fixtures with a real device claim."""

from nso_adapter.core.apply import run_apply as apply_runner
from nso_adapter.core.claim import BookkeepingOutcomeUnknown, ClaimLostError, acquire_claim, release_claim
from nso_adapter.core.removal import run_removal as removal_runner
from nso_adapter.nso import apply as nso_apply
from nso_adapter.nso.refusals import OrdinaryRejection, Unavailable
from nso_adapter.store.models import Job
from tests.conftest import session

REAL_SENDER = nso_apply.apply_device_intent


async def _run(runner, job_id, device_id, reg, **kwargs):
    async with session() as db:
        job = await db.get(Job, job_id)
    if job is None or job.device_id != device_id:
        return await runner(job_id, device_id, reg=reg, **kwargs)
    own_claim = reg is None
    claim = await acquire_claim(device_id, "job", job_id=job_id) if own_claim else reg
    assert claim is not None
    claim.run_attempt = reg.run_attempt if reg is not None and reg.run_attempt is not None else job.run_attempt
    preserve_claim = False
    sender = nso_apply.apply_device_intent

    async def checkpointed_sender(*args, checkpoint=None, **params):
        try:
            verdict = await sender(*args, **params)
        except nso_apply.NsoApplyError as exc:
            classification = exc.classification or (Unavailable() if exc.code == "internal" else OrdinaryRejection())
            exc.classification = classification
            if checkpoint is not None:
                await checkpoint(exc.sender_phase, classification, False)
            raise
        except Exception:
            if checkpoint is not None:
                await checkpoint("commit", Unavailable(), False)
            raise
        if checkpoint is not None:
            await checkpoint("commit", None, True)
            await checkpoint("post_commit_verification", None, False)
        return (
            verdict
            if verdict in {nso_apply.VERIFY_CONCLUSIVE, nso_apply.VERIFY_INCONCLUSIVE, nso_apply.VERIFY_DISABLED}
            else nso_apply.VERIFY_CONCLUSIVE
        )

    if sender is not REAL_SENDER:
        nso_apply.apply_device_intent = checkpointed_sender
    try:
        await runner(job_id, device_id, reg=claim, **kwargs)
    except (BookkeepingOutcomeUnknown, ClaimLostError):
        preserve_claim = True
        raise
    finally:
        nso_apply.apply_device_intent = sender
        if own_claim and not preserve_claim:
            await release_claim(claim)


async def run_apply(job_id, device_id, reg=None):
    await _run(apply_runner, job_id, device_id, reg)


async def run_removal(job_id, device_id, reg=None):
    await _run(removal_runner, job_id, device_id, reg)
