# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Unit tests for core/jobs.py — enqueue_job, _run_with_db, _run_connect, runners."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from sqlalchemy import select

from nso_adapter.core.jobs import (
    _run_apply,
    _run_connect,
    _run_detect_drift,
    _run_provision,
    _run_sync,
    _run_with_db,
    enqueue_job,
    get_active_job,
)
from nso_adapter.nso.client import NsoClient
from nso_adapter.store.db import get_session
from nso_adapter.store.models import Device, Job, JobStatus, JobType


async def _seed_device(nso_device_name: str = "test-rtr", netbox_id: int = 1) -> int:
    """Insert a device and return its id."""
    async for db in get_session():
        d = Device(nso_instance="nso-dev", nso_device_name=nso_device_name, netbox_device_id=netbox_id)
        db.add(d)
        await db.commit()
        await db.refresh(d)
        return d.id
    raise RuntimeError("no session")


async def _seed_job(device_id: int, status: JobStatus = JobStatus.queued) -> int:
    """Insert a job and return its id."""
    async for db in get_session():
        j = Job(job_type=JobType.sync, device_id=device_id, status=status)
        db.add(j)
        await db.commit()
        await db.refresh(j)
        return j.id
    raise RuntimeError("no session")


# ── JobType enum invariant ──────────────────────────────────────────────────────


def test_job_type_value_equals_name_for_all_members():
    """Every JobType value must equal its name: SQLAlchemy's Enum(JobType) persists the member
    NAME, so a divergent value (detect_drift = "detect-drift") silently misses a raw-value
    filter/write against the DB or DataErrors (s3-9)."""
    diverged = {m.name: m.value for m in JobType if m.value != m.name}
    assert not diverged, f"JobType members whose value != name: {diverged}"


# ── get_active_job ────────────────────────────────────────────────────────────


async def test_get_active_job_returns_queued(adapter_client):
    """Returns queued job for device."""
    device_id = await _seed_device("rtr-01", 11)
    job_id = await _seed_job(device_id, JobStatus.queued)

    async for db in get_session():
        result = await get_active_job(device_id, db)
        assert result is not None
        assert result.id == job_id
        break


async def test_get_active_job_returns_none_when_succeeded(adapter_client):
    """Returns None when all jobs are in terminal states."""
    device_id = await _seed_device("rtr-02", 12)
    await _seed_job(device_id, JobStatus.succeeded)

    async for db in get_session():
        result = await get_active_job(device_id, db)
        assert result is None
        break


async def test_get_active_job_returns_running_job(adapter_client):
    """Returns running job (not just queued)."""
    device_id = await _seed_device("rtr-03", 13)
    await _seed_job(device_id, JobStatus.running)

    async for db in get_session():
        result = await get_active_job(device_id, db)
        assert result is not None
        assert result.status == JobStatus.running
        break


# ── enqueue_job ───────────────────────────────────────────────────────────────


async def test_enqueue_job_creates_new_job(adapter_client):
    """enqueue_job creates a queued job for the worker pool to drain."""
    device_id = await _seed_device("rtr-04", 14)

    async for db in get_session():
        job, created = await enqueue_job(device_id, JobType.sync, db)
        assert created is True
        assert job.status == JobStatus.queued
        break


async def test_enqueue_job_returns_existing_when_active(adapter_client):
    """enqueue_job returns existing active job with created=False."""
    device_id = await _seed_device("rtr-05", 15)
    existing_id = await _seed_job(device_id, JobStatus.queued)

    async for db in get_session():
        job, created = await enqueue_job(device_id, JobType.sync, db)
        assert created is False
        assert job.id == existing_id
        break


async def test_active_job_partial_unique_index_rejects_second(adapter_client):
    """s3-17: the DB enforces at most one active (queued/running) job per device, so a
    TOCTOU race between the enqueue check and the insert cannot materialise two active jobs."""
    from sqlalchemy.exc import IntegrityError

    device_id = await _seed_device("dup-active", 4100)
    await _seed_job(device_id, JobStatus.queued)

    async for db in get_session():
        db.add(Job(job_type=JobType.sync, device_id=device_id, status=JobStatus.running))
        with pytest.raises(IntegrityError):
            await db.commit()
        await db.rollback()
        break


async def test_active_job_index_exempts_removal(adapter_client):
    """s3-17: removal jobs are intentionally per-scope (enqueue_removal queues one each for
    bgp/isis/snmp/…), so the one-active-per-device index must NOT block a second active
    removal for the same device."""
    device_id = await _seed_device("removal-multi", 4103)

    async for db in get_session():
        db.add(Job(job_type=JobType.removal, device_id=device_id, status=JobStatus.queued, context={"scope": "bgp"}))
        db.add(Job(job_type=JobType.removal, device_id=device_id, status=JobStatus.queued, context={"scope": "isis"}))
        await db.commit()  # no IntegrityError — removal is exempt from the active-job index
        actives = (
            (await db.execute(select(Job).where(Job.device_id == device_id, Job.job_type == JobType.removal)))
            .scalars()
            .all()
        )
        assert len(actives) == 2
        break


async def test_active_job_index_allows_new_after_terminal(adapter_client):
    """A finished (succeeded/failed) job must not block a fresh active job for the device."""
    device_id = await _seed_device("dup-terminal", 4101)
    await _seed_job(device_id, JobStatus.succeeded)

    async for db in get_session():
        db.add(Job(job_type=JobType.sync, device_id=device_id, status=JobStatus.queued))
        await db.commit()  # no IntegrityError — succeeded job is not "active"
        actives = (
            (await db.execute(select(Job).where(Job.device_id == device_id, Job.status == JobStatus.queued)))
            .scalars()
            .all()
        )
        assert len(actives) == 1
        break


async def test_enqueue_job_recovers_from_lost_race(adapter_client, monkeypatch):
    """s3-17: when the pre-insert check loses the race (stale read → None) and the unique
    index rejects the duplicate insert, enqueue_job re-reads and returns the winning active
    job (created=False) instead of surfacing the IntegrityError as a 500."""
    from nso_adapter.core import jobs as jobs_mod

    device_id = await _seed_device("race", 4102)
    existing_id = await _seed_job(device_id, JobStatus.queued)

    real = jobs_mod.get_active_job
    calls = {"n": 0}

    async def flaky(dev_id, db):
        calls["n"] += 1
        if calls["n"] == 1:
            return None  # simulate the stale read that lost the TOCTOU race
        return await real(dev_id, db)

    monkeypatch.setattr(jobs_mod, "get_active_job", flaky)

    async for db in get_session():
        job, created = await enqueue_job(device_id, JobType.sync, db)
        assert created is False
        assert job.id == existing_id
        actives = (
            (
                await db.execute(
                    select(Job).where(
                        Job.device_id == device_id,
                        Job.status.in_([JobStatus.queued, JobStatus.running]),
                    )
                )
            )
            .scalars()
            .all()
        )
        assert len(actives) == 1
        break


async def test_enqueue_job_raises_on_unknown_type(adapter_client):
    """enqueue_job raises ValueError for unregistered job type."""
    device_id = await _seed_device("rtr-06", 16)

    async for db in get_session():
        with patch("nso_adapter.core.jobs._JOB_RUNNERS", {}):
            with pytest.raises(ValueError, match="No runner registered"):
                await enqueue_job(device_id, JobType.sync, db)
        break


# ── _run_with_db ──────────────────────────────────────────────────────────────


async def test_run_with_db_success(adapter_client):
    """_run_with_db marks job succeeded and stores result."""
    device_id = await _seed_device("rtr-10", 20)
    job_id = await _seed_job(device_id, JobStatus.queued)

    async def success_factory(dev_id, db):
        return {"outcome": "ok"}

    await _run_with_db(job_id, device_id, success_factory)

    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        assert job.result == {"outcome": "ok"}
        break


async def test_run_with_db_failure(adapter_client):
    """_run_with_db marks job failed on exception."""
    device_id = await _seed_device("rtr-11", 21)
    job_id = await _seed_job(device_id, JobStatus.queued)

    async def fail_factory(dev_id, db):
        raise RuntimeError("something broke")

    await _run_with_db(job_id, device_id, fail_factory)

    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert "something broke" in job.error["message"]
        break


async def test_run_with_db_timeout(adapter_client):
    """_run_with_db marks job failed on timeout."""
    device_id = await _seed_device("rtr-12", 22)
    job_id = await _seed_job(device_id, JobStatus.queued)

    async def slow_factory(dev_id, db):
        await asyncio.sleep(9999)

    # Patch asyncio.wait_for in the module namespace to raise TimeoutError
    async def mock_wait_for(coro, timeout):
        coro.close()
        raise TimeoutError

    with patch("asyncio.wait_for", side_effect=mock_wait_for):
        await _run_with_db(job_id, device_id, slow_factory)

    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.error["code"] == "timeout"
        break


async def test_run_with_db_job_not_found(adapter_client):
    """_run_with_db returns early when job doesn't exist."""
    device_id = await _seed_device("rtr-13", 23)

    async def success_factory(dev_id, db):
        return {"outcome": "ok"}

    # Should not raise — just return early
    await _run_with_db(99999, device_id, success_factory)


async def test_run_with_db_marks_failed_even_when_session_poisoned(adapter_client):
    """A DB-origin error inside the factory poisons the session (needs-rollback); the failure
    handler must rollback before the failed-status commit, or that commit itself re-raises and
    the job is stranded 'running' (s3-5 — same fix as run_apply #11)."""
    device_id = await _seed_device("rtr-poison-db", 71)
    job_id = await _seed_job(device_id, JobStatus.queued)

    async def poison_factory(dev_id, db):
        # A duplicate PK insert → IntegrityError → AsyncSession enters needs-rollback,
        # exactly like a failed flush mid-sync.
        db.add(Job(id=job_id, job_type=JobType.sync, device_id=dev_id, status=JobStatus.queued))
        await db.flush()

    await _run_with_db(job_id, device_id, poison_factory)

    async for db in get_session():
        assert (await db.get(Job, job_id)).status == JobStatus.failed
        break


# ── _run_sync and _run_detect_drift ───────────────────────────────────────


async def test_run_sync_calls_run_with_db(adapter_client):
    """_run_sync delegates to _run_with_db with sync_device."""
    device_id = await _seed_device("rtr-20", 30)
    job_id = await _seed_job(device_id)

    with patch("nso_adapter.core.jobs._run_with_db", new_callable=AsyncMock) as mock_run:
        await _run_sync(job_id, device_id)
        mock_run.assert_called_once()
        assert mock_run.call_args[0][0] == job_id
        assert mock_run.call_args[0][1] == device_id


async def test_run_sync_now_uses_the_900s_budget(adapter_client):
    """A2 (codex R1-F3): the comprehensive runner's legal child waits sum to ~720s — the
    default 600s budget would cancel a slow-but-valid whale run."""
    from nso_adapter.core.jobs import _run_sync_now

    device_id = await _seed_device()
    job_id = await _seed_job(device_id)

    with patch("nso_adapter.core.jobs._run_with_db", new_callable=AsyncMock) as rwd:
        await _run_sync_now(job_id, device_id)

    rwd.assert_awaited_once()
    assert rwd.await_args.kwargs.get("timeout") == 900.0


async def test_sync_now_timeout_leaves_last_sync_pair_untouched(adapter_client):
    """A2 regression (codex R2-F8, red against the pre-move importer.py:824): a job-budget
    cancel mid-fan-out must NOT leave an ADVANCED last_sync_at under the OLD status — the
    operator would see a fresh timestamp, a stale status, and a failed job."""
    from datetime import datetime

    from nso_adapter.core import importer as imp
    from nso_adapter.core.importer import sync_device
    from nso_adapter.store.models import LastSyncStatus

    prior_ts = datetime(2026, 1, 1, 12, 0, 0)
    async for db in get_session():
        d = Device(
            nso_instance="nso-dev",
            nso_device_name="t-rtr",
            netbox_device_id=1,
            ned_id="cisco-ios-cli-6.95",
            last_sync_at=prior_ts,
            last_sync_status=LastSyncStatus.succeeded,
        )
        db.add(d)
        await db.commit()
        await db.refresh(d)
        device_id = d.id
        break
    job_id = await _seed_job(device_id)

    client = AsyncMock(spec=NsoClient)
    client.get_device_ned_id = AsyncMock(return_value="cisco-ios-cli-6.95")
    client.get_device_state_section = AsyncMock(return_value={"status": "ok", "device-name": "t-rtr", "interface": []})
    imp._nso_clients["nso-dev"] = client
    imp._netbox_client = None

    async def _stalled_fanout(*a, **k):
        await asyncio.sleep(30)
        return []

    async def _factory(device_id_: int, db) -> dict:
        return await sync_device(device_id_, db, atomic=True, comprehensive=True)

    with (
        patch("nso_adapter.core.importer.nso_actions.sync_from", new_callable=AsyncMock),
        patch("nso_adapter.core.importer.refresh_all_surfaces_for_device", side_effect=_stalled_fanout),
    ):
        await _run_with_db(job_id, device_id, _factory, timeout=0.5)

    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.error["code"] == "timeout"
        device = await db.get(Device, device_id)
        assert device.last_sync_status == LastSyncStatus.succeeded
        assert device.last_sync_at == prior_ts, "timestamp must not advance on a cancelled sync"
        break


async def test_run_sync_from_nso_reads_all_surfaces_without_device_contact(adapter_client):
    """S5a B: the runner is a comprehensive CDB-only mirror read — refresh_all_surfaces
    atomic, NO device sync-from, no last_sync_* writes; notify best-effort."""
    from nso_adapter.core import importer as imp
    from nso_adapter.core.jobs import _run_sync_from_nso
    from nso_adapter.nso.client import NsoClient as _NsoClient

    device_id = await _seed_device("sfn-rtr", 91)
    job_id = await _seed_job(device_id)

    client = AsyncMock(spec=_NsoClient)
    imp._nso_clients["nso-dev"] = client
    nb = AsyncMock()
    imp._netbox_client = nb

    with (
        patch(
            "nso_adapter.core.importer.refresh_all_surfaces_for_device",
            new_callable=AsyncMock,
            return_value=(["bgp"], None),
        ) as fanout,
        patch("nso_adapter.core.importer.nso_actions.sync_from", new_callable=AsyncMock) as sf,
    ):
        await _run_sync_from_nso(job_id, device_id)

    fanout.assert_awaited_once()
    assert fanout.await_args.kwargs.get("atomic") is True
    assert fanout.await_args.kwargs.get("refresh_source") == "sync_from_nso"
    sf.assert_not_awaited()  # pins the no-device-round-trip contract

    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        assert job.result == {"degraded_surfaces": ["bgp"]}
        device = await db.get(Device, device_id)
        assert device.last_sync_at is None  # grain (b) never claims a device sync
        break
    nb.notify_sync_complete.assert_awaited_once()


async def test_run_sync_from_nso_fails_job_on_total_supplier_failure(adapter_client):
    """S5a B (codex R1-F2/R2-F4): an export/action outage refreshed NOTHING — the job
    must FAIL with an honest message, never report green success."""
    from nso_adapter.core import importer as imp
    from nso_adapter.core.jobs import _run_sync_from_nso
    from nso_adapter.nso.client import NsoClient as _NsoClient
    from nso_adapter.nso.read_outcome import Unavailable, UnavailableReason

    device_id = await _seed_device("sfn-rtr-down", 92)
    job_id = await _seed_job(device_id)

    client = AsyncMock(spec=_NsoClient)
    imp._nso_clients["nso-dev"] = client
    imp._netbox_client = None

    supplier = Unavailable(UnavailableReason.read_error, detail="action boom")
    all_kept = [
        "static_route",
        "isis",
        "bgp",
        "ospf",
        "redistribution",
        "route_policy",
        "snmp",
        "logging",
        "bfd",
        "interface_ip",
        "vlan",
        "svi",
        "subinterface",
        "interface_mtu",
        "switchport",
        "l2_service",
        "lag_topology",
        "lag_config",
    ]
    with patch(
        "nso_adapter.core.importer.refresh_all_surfaces_for_device",
        new_callable=AsyncMock,
        return_value=(all_kept, supplier),
    ):
        await _run_sync_from_nso(job_id, device_id)

    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert "nothing refreshed" in job.error["message"].lower()
        break


async def test_run_sync_now_requests_comprehensive_atomic(adapter_client):
    """A1 (READSEM S5a, codex R1-F9): the RUNNER must pass BOTH flags — a direct
    sync_device test alone stays green if the runner edit is forgotten."""
    from nso_adapter.core.jobs import _run_sync_now

    device_id = await _seed_device()
    job_id = await _seed_job(device_id)

    with patch("nso_adapter.core.importer.sync_device", new_callable=AsyncMock, return_value={}) as sd:
        await _run_sync_now(job_id, device_id)

    sd.assert_awaited_once()
    assert sd.await_args.kwargs.get("atomic") is True
    assert sd.await_args.kwargs.get("comprehensive") is True


async def test_run_detect_drift_calls_run_with_db(adapter_client):
    """_run_detect_drift delegates to _run_with_db."""
    device_id = await _seed_device("rtr-21", 31)
    job_id = await _seed_job(device_id)

    with patch("nso_adapter.core.jobs._run_with_db", new_callable=AsyncMock) as mock_run:
        await _run_detect_drift(job_id, device_id)
        mock_run.assert_called_once()
        assert mock_run.call_args[0][0] == job_id


# ── _run_connect ──────────────────────────────────────────────────────────────


def _nso_client_for_connect(output: dict) -> MagicMock:
    """A spec'd NsoClient (real external HTTP boundary) whose pooled client yields a REAL
    httpx.Response, so nso.actions.connect runs end-to-end: URL build from _base, the
    `async with client._client(...)` pool, raise_for_status, and the tailf-ncs:output
    extraction. Only the HTTP socket is faked — bound to NsoClient via spec=."""
    client = MagicMock(spec=NsoClient)
    client._base = "http://nso"  # instance attr — set explicitly (spec= permits SET)
    client._action_timeout = 5.0
    resp = httpx.Response(
        200,
        json={"tailf-ncs:output": output},
        request=httpx.Request("POST", "http://nso/connect"),
    )
    http = AsyncMock()
    http.post.return_value = resp
    client._client.return_value.__aenter__.return_value = http
    return client


async def test_run_connect_success(adapter_client):
    """_run_connect runs the real connect action and threads its output onto the job."""
    device_id = await _seed_device("rtr-30", 40)
    job_id = await _seed_job(device_id)

    client = _nso_client_for_connect({"result": "connected"})
    with patch("nso_adapter.core.importer.get_nso_client", return_value=client):
        await _run_connect(job_id, device_id)

    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        # The real connect extracts tailf-ncs:output; _run_connect stores it under "output".
        assert job.result == {"output": {"result": "connected"}}
        break


async def test_run_connect_device_not_found(adapter_client):
    """_run_connect marks job failed when get_nso_client raises."""
    device_id = await _seed_device("rtr-31", 41)
    job_id = await _seed_job(device_id)

    with patch("nso_adapter.core.importer.get_nso_client", side_effect=KeyError("nso-dev not found")):
        await _run_connect(job_id, device_id)

    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        break


async def test_run_connect_device_not_in_db(adapter_client):
    """_run_connect marks job failed when device_id doesn't exist in DB."""
    # Seed a device just to have the job FK work, then use non-existent device_id
    device_id = await _seed_device("rtr-34", 44)
    job_id = await _seed_job(device_id)
    non_existent_device_id = 99998

    await _run_connect(job_id, non_existent_device_id)

    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert "not found" in job.error["message"]
        break


async def test_run_connect_job_not_found(adapter_client):
    """_run_connect returns early when the job id doesn't exist in DB."""
    device_id = await _seed_device("rtr-32", 42)
    # Don't seed a job — use a non-existent job_id
    await _run_connect(99999, device_id)  # should not raise


async def test_run_connect_timeout(adapter_client):
    """_run_connect marks job failed on timeout."""
    device_id = await _seed_device("rtr-33", 43)
    job_id = await _seed_job(device_id)

    async def mock_wait_for(coro, timeout):
        coro.close()
        raise TimeoutError

    # wait_for short-circuits before connect touches the client, so it's only a spec'd
    # NsoClient placeholder here (bound so a renamed member still couldn't be fabricated).
    client = MagicMock(spec=NsoClient)
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=client),
        patch("asyncio.wait_for", side_effect=mock_wait_for),
    ):
        await _run_connect(job_id, device_id)

    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.error["code"] == "timeout"
        break


async def test_run_connect_marks_failed_even_when_session_poisoned(adapter_client):
    """Same poisoned-session guard for _run_connect (s3-5). The connect boundary doesn't take
    the runner's db, so a capturing get_session wrapper hands the poison stub the real session
    to fail a flush on — proving the runner rolls back before committing the failed status."""
    device_id = await _seed_device("rtr-poison-c", 72)
    job_id = await _seed_job(device_id, JobStatus.queued)

    from nso_adapter.store import db as db_mod

    real_get_session = db_mod.get_session
    captured: dict = {}

    async def capturing_get_session():
        async for db in real_get_session():
            captured["db"] = db
            yield db

    async def poison_connect(client, name):
        db = captured["db"]
        db.add(Job(id=job_id, job_type=JobType.connect, device_id=device_id, status=JobStatus.queued))
        await db.flush()  # duplicate PK → session poisoned

    with (
        patch("nso_adapter.store.db.get_session", capturing_get_session),
        patch("nso_adapter.core.importer.get_nso_client", return_value=object()),
        patch("nso_adapter.nso.actions.connect", poison_connect),
    ):
        await _run_connect(job_id, device_id)

    async for db in get_session():
        assert (await db.get(Job, job_id)).status == JobStatus.failed
        break


# ── _run_apply ────────────────────────────────────────────────────────────────


async def test_run_apply_calls_run_apply(adapter_client):
    """_run_apply delegates to core.apply.run_apply."""
    device_id = await _seed_device("rtr-40", 50)
    job_id = await _seed_job(device_id)

    with patch("nso_adapter.core.apply.run_apply", new_callable=AsyncMock) as mock_run:
        await _run_apply(job_id, device_id)
        mock_run.assert_called_once_with(job_id, device_id, force=True)


# ── _run_provision ──────────────────────────────────────────────────────────────


async def test_run_provision_marks_failed_even_when_session_poisoned(adapter_client):
    """Same poisoned-session guard for _run_provision (s3-5). provision_nso_device takes the
    runner's db, so a poisoning stub leaves the session needs-rollback; the runner must
    rollback before committing the failed status or the provision job hangs 'running'."""
    async for db in get_session():
        j = Job(
            job_type=JobType.provision,
            device_id=None,
            status=JobStatus.queued,
            context={"nso_instance": "nso-dev", "device_name": "prov-poison"},
        )
        db.add(j)
        await db.commit()
        await db.refresh(j)
        job_id = j.id
        break

    async def poison_provision(db, **params):
        db.add(Job(id=job_id, job_type=JobType.provision, status=JobStatus.queued))
        await db.flush()  # duplicate PK → session poisoned

    with patch("nso_adapter.core.onboarding.provision_nso_device", poison_provision):
        await _run_provision(job_id, None)

    async for db in get_session():
        assert (await db.get(Job, job_id)).status == JobStatus.failed
        break


class _FakeNb:
    """Real-shape async fake of the NetBox client — records provision-complete callbacks."""

    def __init__(self):
        self.calls = []

    async def notify_provision_complete(self, job_id):
        self.calls.append(job_id)


async def _queue_provision_job(device_name: str) -> int:
    job_id = 0
    async for db in get_session():
        j = Job(
            job_type=JobType.provision,
            device_id=None,
            status=JobStatus.queued,
            context={"nso_instance": "nso-dev", "device_name": device_name},
        )
        db.add(j)
        await db.commit()
        await db.refresh(j)
        job_id = j.id
        break
    return job_id


async def test_run_provision_notifies_plugin_on_success(adapter_client):
    """A successful provision fires the plugin provision-complete callback with the job id."""
    job_id = await _queue_provision_job("prov-notify-ok")
    fake = _FakeNb()

    async def ok_provision(db, **params):
        return {"ok": True, "steps": [{"step": "create", "status": "ok"}], "device_id": None}

    with (
        patch("nso_adapter.core.onboarding.provision_nso_device", ok_provision),
        patch("nso_adapter.core.importer.get_netbox_client", lambda: fake),
    ):
        await _run_provision(job_id, None)

    async for db in get_session():
        assert (await db.get(Job, job_id)).status == JobStatus.succeeded
        break
    assert fake.calls == [job_id]


async def test_run_provision_notifies_plugin_on_failure(adapter_client):
    """Even when provisioning fails, the runner still fires the callback so the plugin marks it failed."""
    job_id = await _queue_provision_job("prov-notify-fail")
    fake = _FakeNb()

    async def boom_provision(db, **params):
        raise RuntimeError("nso unreachable")

    with (
        patch("nso_adapter.core.onboarding.provision_nso_device", boom_provision),
        patch("nso_adapter.core.importer.get_netbox_client", lambda: fake),
    ):
        await _run_provision(job_id, None)

    async for db in get_session():
        assert (await db.get(Job, job_id)).status == JobStatus.failed
        break
    assert fake.calls == [job_id]
