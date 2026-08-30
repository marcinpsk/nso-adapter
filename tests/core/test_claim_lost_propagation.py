# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""``ClaimLostError`` must reach the worker, not be converted into a job failure.

Every broad ``except Exception`` on a claimed path is a place where a revocation looks
like a benign outcome: the wrapper writes ``failed`` over the disposition recovery
already made, or a suppressor swallows it and the run continues into further device
writes under ownership it no longer has.

Two halves, and the structural one is necessary but NOT sufficient on its own:

* behavioral — drive the handler and assert the exception propagates and nothing is
  written. Done here for the helpers that can be driven without a live NSO;
* structural — assert every site in the inventory carries an ``except ClaimLostError``
  BEFORE its broad clause, so a newly added handler cannot quietly omit it.

Wiring the guard into the runners themselves (so a real revoked apply/removal/sync
raises from its own inner commits) is the next slice; until then these handlers cannot
see a real ClaimLostError end-to-end, which is why the structural half carries the
inventory.
"""

from __future__ import annotations

import ast
import pathlib
from datetime import UTC, datetime, timedelta

import pytest
import sqlalchemy as sa

from nso_adapter.core.claim import ClaimLostError, ClaimRegistration, acquire_claim, claim_stale_cutoff
from tests.conftest import seed_device, session

pytestmark = pytest.mark.anyio

_ROOT = pathlib.Path(__file__).resolve().parents[2] / "nso_adapter"

# (module, enclosing function, the log event that identifies the broad handler).
# Named rather than derived: the point is that THESE sites are covered, and a rename
# should fail the test rather than silently shrink the inventory.
INVENTORY = [
    ("core/apply.py", "_run_scope", "{log_label}_unexpected_error"),
    ("core/apply.py", None, "apply.attribute_unexpected_error"),
    ("core/apply.py", None, "apply.ip_unexpected_error"),
    ("core/apply.py", "_post_apply_refresh_and_notify", "apply.post_refresh_failed"),
    ("core/apply.py", "run_apply", "apply.unexpected_error"),
    ("core/apply.py", "_record_rp_capability", "apply.capability_record_skipped"),
    ("core/removal.py", "run_removal", "removal.failed"),
    ("core/removal.py", "run_removal", "removal.followup_sync_enqueue_failed"),
    ("core/jobs.py", "_run_with_db", "job.failed"),
    ("core/jobs.py", "_run_connect", "job.connect.failed"),
    ("core/jobs.py", "_run_provision", "job.provision.failed"),
    ("core/onboarding.py", "_initial_mirror_refresh", "device.onboard_mirror.failed"),
    ("core/onboarding.py", "_seed_onboarding_failover", "failover_seed"),
    ("core/refresh_engine.py", None, "outcome.read_record_failed"),
    ("core/refresh_engine.py", None, "outcome.result_record_failed"),
]


def _handlers(path: pathlib.Path):
    """Yield every ``try`` node in the module together with its handler list."""
    tree = ast.parse(path.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.Try):
            yield node


def _mentions(node: ast.AST, needle: str) -> bool:
    return needle in ast.unparse(node)


@pytest.mark.parametrize(("module", "function", "marker"), INVENTORY, ids=[f"{m}:{k}" for m, _f, k in INVENTORY])
def test_broad_handler_reraises_claim_lost_first(module, function, marker):
    """The ``except ClaimLostError`` clause must come BEFORE the broad one.

    Ordering is the whole point: Python matches handlers top to bottom, so a re-raise
    placed after ``except Exception`` never runs.
    """
    path = _ROOT / module
    matching = [t for t in _handlers(path) if any(_mentions(h, marker) for h in t.handlers)]
    assert matching, f"no try/except in {module} mentions {marker!r} — did the log event get renamed?"

    for try_node in matching:
        names = []
        for handler in try_node.handlers:
            if handler.type is None:
                names.append("bare")
            else:
                names.append(ast.unparse(handler.type))
        assert "ClaimLostError" in names, f"{module} handler for {marker!r} does not re-raise ClaimLostError: {names}"

        claim_at = names.index("ClaimLostError")
        broad = [i for i, n in enumerate(names) if n in {"Exception", "bare"}]
        assert broad, f"{module}:{marker} has no broad clause — inventory entry is stale"
        assert claim_at < min(broad), (
            f"{module} re-raises ClaimLostError AFTER its broad clause, so it never runs: {names}"
        )

        handler = try_node.handlers[claim_at]
        assert any(isinstance(stmt, ast.Raise) for stmt in handler.body), (
            f"{module}:{marker} catches ClaimLostError without re-raising it"
        )


# ── behavioral: the helpers that can be driven without a live NSO ────────────


async def test_run_with_db_propagates_claim_lost_without_failing_the_job(adapter_client):
    """M9's shape for sync/sync_now/sync_from_nso/detect_drift — all four go through here.

    Against the unguarded wrapper the job flips to ``failed``, clobbering whatever
    recovery decided.
    """
    from nso_adapter.core.jobs import _run_with_db
    from nso_adapter.store.models import Job, JobStatus, JobType

    device_id = await seed_device(nso_device_name="cl-rwd", netbox_device_id=9990)
    async with session() as db:
        job = Job(
            job_type=JobType.sync,
            device_id=device_id,
            status=JobStatus.queued,
            coalescible=True,
            context={},
        )
        db.add(job)
        await db.commit()
        job_id = job.id

    async def _revoked(_device_id, _db):
        raise ClaimLostError("revoked mid-run")

    with pytest.raises(ClaimLostError):
        await _run_with_db(job_id, device_id, _revoked)

    async with session() as db:
        # NOT failed: the runner's own view is stale, and recovery owns the disposition.
        assert (await db.get(Job, job_id)).status is not JobStatus.failed


async def test_mark_job_failed_cannot_overwrite_recovery(adapter_client):
    """M9.11 — with a stale token the shared terminal writer must write nothing."""
    from nso_adapter.core.claim import revoke_stale_claims
    from nso_adapter.core.jobs import _mark_job_failed
    from nso_adapter.store.models import DeviceClaim, Job, JobStatus, JobType

    device_id = await seed_device(nso_device_name="cl-mjf", netbox_device_id=9991)
    async with session() as db:
        job = Job(
            job_type=JobType.sync,
            device_id=device_id,
            status=JobStatus.running,
            coalescible=True,
            context={},
        )
        db.add(job)
        await db.commit()
        job_id = job.id

    reg = await acquire_claim(device_id, "job", job_id=job_id)
    async with session() as db:
        await db.execute(
            sa.update(DeviceClaim)
            .where(DeviceClaim.device_id == device_id)
            .values(heartbeat_at=datetime.now(UTC) - timedelta(seconds=claim_stale_cutoff() + 60))
        )
        await db.commit()
    await revoke_stale_claims()

    async with session() as db:
        assert (await db.get(Job, job_id)).status is JobStatus.queued  # recovery's disposition
        await _mark_job_failed(db, job_id, {"code": "internal", "message": "stale", "detail": {}}, reg)

    async with session() as db:
        assert (await db.get(Job, job_id)).status is JobStatus.queued, "the revoked runner overwrote recovery"


async def test_mark_job_failed_still_writes_for_the_holder(adapter_client):
    """The guard must not break the ordinary path."""
    from nso_adapter.core.jobs import _mark_job_failed
    from nso_adapter.store.models import Job, JobStatus, JobType

    device_id = await seed_device(nso_device_name="cl-mjf-ok", netbox_device_id=9992)
    async with session() as db:
        job = Job(
            job_type=JobType.sync,
            device_id=device_id,
            status=JobStatus.running,
            coalescible=True,
            context={},
        )
        db.add(job)
        await db.commit()
        job_id = job.id
    reg = await acquire_claim(device_id, "job", job_id=job_id)

    async with session() as db:
        await _mark_job_failed(db, job_id, {"code": "internal", "message": "real", "detail": {}}, reg)
    async with session() as db:
        assert (await db.get(Job, job_id)).status is JobStatus.failed


async def test_mark_job_failed_without_a_registration_is_unchanged(adapter_client):
    """The claimless lane keeps today's behavior."""
    from nso_adapter.core.jobs import _mark_job_failed
    from nso_adapter.store.models import Job, JobStatus, JobType

    job_id = None
    async with session() as db:
        job = Job(
            job_type=JobType.provision,
            device_id=None,
            status=JobStatus.running,
            coalescible=False,
            context={},
        )
        db.add(job)
        await db.commit()
        job_id = job.id
        await _mark_job_failed(db, job_id, {"code": "internal", "message": "x", "detail": {}})
    async with session() as db:
        assert (await db.get(Job, job_id)).status is JobStatus.failed


async def test_worker_mark_failed_cannot_overwrite_recovery(adapter_client):
    """M9.10 — the worker's last-resort writer, driven with a stale token directly."""
    from nso_adapter.core import worker as worker_mod
    from nso_adapter.store.models import Job, JobStatus, JobType

    device_id = await seed_device(nso_device_name="cl-wmf", netbox_device_id=9993)
    async with session() as db:
        job = Job(
            job_type=JobType.sync,
            device_id=device_id,
            status=JobStatus.queued,
            coalescible=True,
            context={},
        )
        db.add(job)
        await db.commit()
        job_id = job.id

    stale = ClaimRegistration(device_id, "a-token-nobody-holds")
    await worker_mod._mark_failed(job_id, "internal", "machinery fault", stale)

    async with session() as db:
        assert (await db.get(Job, job_id)).status is JobStatus.queued
