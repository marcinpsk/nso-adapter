# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Appendix S, chunk S3: the ascending settlement feed and its validation.

The consumer walks one device's terminal jobs in the order they became true, from a
durable cursor. That is a second reading of ``GET /api/v1/jobs``, not a replacement: the
default page stays exactly what the three existing consumers read today, and the
ascending page is reached only by asking for it.

Two properties carry the feed. Ordering is ``settle_seq`` alone — ``(device_id,
settle_seq)`` is unique, so there is nothing to break a tie on. Invisibility of queued and
running jobs is not a status filter but the cursor predicate itself: ``settle_seq >
:cursor`` is NULL-false, so a job that has not settled cannot be paged over and skipped.

Validation fails fast rather than coercing: a per-device sequence is meaningless without a
device, and a clamped limit hides the caller's bug.
"""

from __future__ import annotations

import pytest

from nso_adapter.core.claim import terminalize
from nso_adapter.store.meta import get_store_incarnation
from nso_adapter.store.models import Job, JobStatus, JobType
from tests.conftest import VALID_TOKEN, seed_device, session

pytestmark = pytest.mark.anyio

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


async def _terminal_job(device_id: int, *, job_type: JobType = JobType.removal) -> int:
    """A job that ran and settled, through the real allocator — so it carries a sequence."""
    async with session() as db:
        job = Job(job_type=job_type, device_id=device_id, status=JobStatus.running, run_attempt=1)
        db.add(job)
        await db.flush()
        await terminalize(db, job.id, status=JobStatus.succeeded, expect=JobStatus.running, run_attempt=1)
        await db.commit()
        return job.id


async def _pending_job(device_id: int, *, status: JobStatus, job_type: JobType) -> int:
    async with session() as db:
        job = Job(job_type=job_type, device_id=device_id, status=status, run_attempt=0)
        db.add(job)
        await db.commit()
        return job.id


async def _walk(client, device_id: int, *, limit: int = 1) -> list[dict]:
    """Page the ascending feed from the start until it is exhausted."""
    rows: list[dict] = []
    cursor = 0
    while True:
        resp = await client.get(
            "/api/v1/jobs",
            params={"device_id": device_id, "order": "asc", "after_settle_seq": cursor, "limit": limit},
            headers=AUTH,
        )
        assert resp.status_code == 200, resp.text
        page = resp.json()
        if not page:
            return rows
        rows.extend(page)
        cursor = page[-1]["settle_seq"]


# ── S3.1 (P0.6): a job that has not settled cannot be paged over ──────────────


async def test_non_terminal_jobs_are_invisible_to_the_ascending_feed(adapter_client):
    """S3.1 — queued and running siblings never appear in an ascending page.

    Forbidden: either of them served. A consumer advances its cursor past every row a page
    hands it, so a queued job appearing there would be consumed as a result it does not
    have, and its real result — allocated later, at a HIGHER sequence — would land behind
    the cursor and be lost.

    The ordering is asserted on ``settle_seq``, not on insertion order: the feed's contract
    is the sequence, and ``created_at`` is transaction time.
    """
    device_id = await seed_device(nso_device_name="feed-visibility", netbox_device_id=8301)
    first = await _terminal_job(device_id)
    second = await _terminal_job(device_id)
    queued = await _pending_job(device_id, status=JobStatus.queued, job_type=JobType.sync)
    running = await _pending_job(device_id, status=JobStatus.running, job_type=JobType.apply)

    for params in (
        {"device_id": device_id, "order": "asc"},
        {"device_id": device_id, "order": "asc", "after_settle_seq": 0},
    ):
        resp = await adapter_client.get("/api/v1/jobs", params=params, headers=AUTH)
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert [r["id"] for r in body] == [first, second]
        assert [r["settle_seq"] for r in body] == [1, 2]
        assert queued not in [r["id"] for r in body]
        assert running not in [r["id"] for r in body]

    # And the cursor pages forward, rather than re-serving from the start.
    resp = await adapter_client.get(
        "/api/v1/jobs",
        params={"device_id": device_id, "order": "asc", "after_settle_seq": 1},
        headers=AUTH,
    )
    assert [r["id"] for r in resp.json()] == [second]


# ── S3.2 (§9 item 4): a per-device sequence needs a device ────────────────────


async def test_ascending_and_cursor_requests_require_a_device_scope(adapter_client):
    """S3.2 — ``order=asc`` and ``after_settle_seq`` both demand ``device_id``.

    Forbidden: serving either unscoped. Sequences are allocated per device, so an unscoped
    ascending page interleaves two devices' independent sequences into one order that is
    wrong for both, and a consumer would advance its cursor over the other device's rows.
    Reject, never coerce to a device.
    """
    device_id = await seed_device(nso_device_name="feed-scope", netbox_device_id=8302)
    await _terminal_job(device_id)

    for params in ({"order": "asc"}, {"after_settle_seq": 1}, {"order": "asc", "after_settle_seq": 1}):
        resp = await adapter_client.get("/api/v1/jobs", params=params, headers=AUTH)
        assert resp.status_code == 422, (params, resp.text)
        error = resp.json()["error"]
        assert error["code"] == "validation_error"
        assert "device_id" in error["message"]

    scoped = await adapter_client.get(
        "/api/v1/jobs",
        params={"device_id": device_id, "order": "asc", "after_settle_seq": 0},
        headers=AUTH,
    )
    assert scoped.status_code == 200, scoped.text


# ── S3.3: the limit is validated, not clamped ────────────────────────────────


async def test_limit_is_validated_not_clamped(adapter_client):
    """S3.3 — ``limit`` outside 1..500 is a 422, never a silent clamp.

    Forbidden: clamping. A caller asking for 5000 rows and receiving 500 believes it has
    the whole page and advances its cursor accordingly; a caller asking for 0 receives an
    empty page that looks like "nothing to settle". Both hide the bug that produced them.
    """
    device_id = await seed_device(nso_device_name="feed-limit", netbox_device_id=8303)
    first = await _terminal_job(device_id)
    await _terminal_job(device_id)

    for limit in (0, -1, 501, 5000):
        resp = await adapter_client.get(
            "/api/v1/jobs",
            params={"device_id": device_id, "order": "asc", "limit": limit},
            headers=AUTH,
        )
        assert resp.status_code == 422, (limit, resp.text)
        assert resp.json()["error"]["code"] == "validation_error"

    # The bounds themselves are served, and a limit inside them really truncates.
    inside = await adapter_client.get(
        "/api/v1/jobs",
        params={"device_id": device_id, "order": "asc", "limit": 1},
        headers=AUTH,
    )
    assert inside.status_code == 200, inside.text
    assert [r["id"] for r in inside.json()] == [first]
    for limit in (1, 500):
        resp = await adapter_client.get("/api/v1/jobs", params={"limit": limit}, headers=AUTH)
        assert resp.status_code == 200, (limit, resp.text)


# ── S3.4b (from S1.3): a refused terminal write leaves one entry, at one seq ──


async def test_a_rejected_terminal_write_leaves_exactly_one_feed_entry(adapter_client, monkeypatch):
    """S3.4b — the row S1.3 protects appears once in the feed, under one sequence.

    S1.3 proves no column changes when a stale runner writes over an already-terminal job.
    That is the row-level half; this is the feed-level half, and it is the one the consumer
    depends on: a second sequence for the same job would be settled twice, and the cursor
    would carry the later value, so the interleaved results of other jobs would be skipped.

    A refused compare-and-set allocates nothing (S2.7), which is what makes the two halves
    one property.
    """
    from nso_adapter.core import jobs as jobs_mod
    from tests.core.test_settle_token import _ok_sync, _queue, _start_run

    device_id = await seed_device(nso_device_name="feed-once", netbox_device_id=8304)
    await _terminal_job(device_id)  # a sibling ahead of it in the sequence
    job_id = await _queue(device_id, JobType.sync)
    _jid, _dev, _jt, reg = await _start_run(device_id, job_id)

    async with session() as db:
        landed = await terminalize(
            db,
            job_id,
            status=JobStatus.succeeded,
            expect=JobStatus.running,
            run_attempt=reg.run_attempt,
            result={"landed": "first"},
        )
        await db.commit()
    assert landed is not None

    # The abandoned runner now finishes and tries to write its own terminal state.
    monkeypatch.setattr("nso_adapter.core.importer.sync_device", _ok_sync)
    await jobs_mod._run_sync(job_id, device_id, reg)

    rows = await _walk(adapter_client, device_id)
    entries = [r for r in rows if r["id"] == job_id]
    assert len(entries) == 1
    assert entries[0]["result"] == {"landed": "first"}
    assert [r["settle_seq"] for r in rows] == [1, 2]


# ── S3.6 (r2-M5): the incarnation rides the response, not the rows ───────────


async def test_the_store_incarnation_header_is_present_on_an_empty_page(adapter_client):
    """S3.6 — ``X-Store-Incarnation`` is on every page, including an empty one.

    Forbidden: carrying the incarnation per row. An empty page is exactly what a cursor
    left over from a previous store produces, so a per-row signal tells the consumer
    nothing in the one state where it decides whether to reset. Forbidden too: an envelope
    around the rows, which would change the default body the existing consumers read.
    """
    device_id = await seed_device(nso_device_name="feed-header", netbox_device_id=8305)
    await _terminal_job(device_id)
    live = get_store_incarnation()[0]

    empty = await adapter_client.get(
        "/api/v1/jobs",
        params={"device_id": device_id, "order": "asc", "after_settle_seq": 999},
        headers=AUTH,
    )
    assert empty.status_code == 200, empty.text
    assert empty.json() == []
    assert empty.headers["X-Store-Incarnation"] == live

    default = await adapter_client.get("/api/v1/jobs", headers=AUTH)
    assert default.status_code == 200, default.text
    assert default.headers["X-Store-Incarnation"] == live
    body = default.json()
    assert isinstance(body, list) and body, "the default page is a bare list of rows"
