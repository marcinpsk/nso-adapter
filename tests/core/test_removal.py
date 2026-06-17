# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Unit tests for core/removal.py — async removal propagation.

Removal no longer runs the device commit inline in the intent PUT; it enqueues a
``removal`` job that the worker runs in the background. These tests cover the
enqueue path, the back-compat shim, and the job runner's scope dispatch — all
against the REAL in-memory DB and real intent/Job rows (so the SQLAlchemy
``select(...).where(...)`` filters actually run); only the NSO apply boundary is
stubbed with a spy.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select

from nso_adapter.core import removal as removal_mod
from nso_adapter.core.removal import enqueue_removal, replace_on_removal
from nso_adapter.store.db import get_session
from nso_adapter.store.models import (
    Device,
    Job,
    JobStatus,
    JobType,
    OspfInstanceIntent,
    OspfInterfaceIntent,
    RedistributionIntent,
    VlanIntent,
)

_NOW = datetime.now(UTC).replace(tzinfo=None)

# An opaque NSO-client token: removal threads it straight to the apply boundary
# (which these tests stub), so it is never dereferenced here — a plain sentinel,
# not a mock, makes that pass-through explicit.
_CLIENT = object()


async def _seed_device(*, nso_device_name: str = "sw3", netbox_device_id: int = 42) -> int:
    async for db in get_session():
        d = Device(nso_instance="nso-dev", nso_device_name=nso_device_name, netbox_device_id=netbox_device_id)
        db.add(d)
        await db.commit()
        await db.refresh(d)
        return d.id
    raise RuntimeError("no session")


async def _seed_removal_job(device_id: int, scope: str = "vlan") -> int:
    async for db in get_session():
        j = Job(job_type=JobType.removal, device_id=device_id, status=JobStatus.queued, context={"scope": scope})
        db.add(j)
        await db.commit()
        await db.refresh(j)
        return j.id
    raise RuntimeError("no session")


# ── replace_on_removal (back-compat shim) ─────────────────────────────────────


async def test_replace_on_removal_noop_when_nothing_removed(adapter_client):
    """No removals → no job enqueued, returns False."""
    device_id = await _seed_device()
    async for db in get_session():
        device = await db.get(Device, device_id)
        result = await replace_on_removal(db, device, [], VlanIntent)
        assert result is False
        assert (await db.execute(select(Job))).scalars().all() == []
        break


async def test_replace_on_removal_enqueues_job_and_commits(adapter_client):
    """On removal, a `removal` job for the model's scope is enqueued + committed."""
    device_id = await _seed_device()
    async for db in get_session():
        device = await db.get(Device, device_id)
        ok = await replace_on_removal(db, device, [3366], VlanIntent)
        assert ok is True
        break

    # Re-read in a fresh session to prove it was committed, not merely flushed.
    async for db in get_session():
        jobs = (await db.execute(select(Job))).scalars().all()
        assert len(jobs) == 1
        job = jobs[0]
        assert job.job_type == JobType.removal
        assert job.device_id == device_id
        assert job.context == {"scope": "vlan"}
        assert job.status == JobStatus.queued
        break


async def test_replace_on_removal_unknown_model_returns_false(adapter_client):
    """An unmapped store model never enqueues (and never crashes the request)."""
    device_id = await _seed_device()

    class _Unmapped:
        pass

    async for db in get_session():
        device = await db.get(Device, device_id)
        ok = await replace_on_removal(db, device, [1], _Unmapped)
        assert ok is False
        assert (await db.execute(select(Job))).scalars().all() == []
        break


# ── enqueue_removal ───────────────────────────────────────────────────────────


async def test_enqueue_removal_rejects_unknown_scope(adapter_client):
    async for db in get_session():
        with pytest.raises(ValueError, match="Unknown removal scope"):
            await enqueue_removal(db, 1, "bogus")
        break


async def test_enqueue_removal_creates_job_for_each_valid_scope(adapter_client):
    """Every reconciler scope (incl ospf/bgp) maps to a removal job."""
    from nso_adapter.core.removal import VALID_REMOVAL_SCOPES

    device_id = await _seed_device()
    for scope in VALID_REMOVAL_SCOPES:
        async for db in get_session():
            job = await enqueue_removal(db, device_id, scope)
            await db.commit()
            assert job.job_type == JobType.removal
            assert job.context == {"scope": scope}
            break

    # Every scope produced a real persisted removal job.
    async for db in get_session():
        scopes = {j.context["scope"] for j in (await db.execute(select(Job))).scalars().all()}
        assert scopes == VALID_REMOVAL_SCOPES
        break


# ── _dispatch_scope ───────────────────────────────────────────────────────────


async def test_dispatch_scope_simple_calls_apply_replace_true(adapter_client):
    """A simple scope fetches ONLY accepted rows and calls its apply with replace=True."""
    device_id = await _seed_device(nso_device_name="sw3")
    async for db in get_session():
        db.add(VlanIntent(device_id=device_id, vlan_id=10, accepted_at=_NOW))
        db.add(VlanIntent(device_id=device_id, vlan_id=20, accepted_at=None))  # not accepted → excluded
        await db.commit()
        break

    apply_fn = AsyncMock()
    async for db in get_session():
        device = await db.get(Device, device_id)
        with patch("nso_adapter.nso.apply.apply_vlan_config", apply_fn):
            await removal_mod._dispatch_scope(db, device, _CLIENT, "vlan")
        break

    apply_fn.assert_awaited_once()
    args, kwargs = apply_fn.await_args
    assert args[0] is _CLIENT
    assert args[1] == "sw3"
    assert [r.vlan_id for r in args[2]] == [10]  # the accepted_at filter dropped vlan 20
    assert kwargs == {"replace": True}


async def test_dispatch_scope_ospf_uses_multi_row_apply(adapter_client):
    """OSPF dispatch fetches instances+interfaces+redist(ospf only) and applies replace=True."""
    device_id = await _seed_device(nso_device_name="ra1")
    async for db in get_session():
        db.add(OspfInstanceIntent(device_id=device_id, process_id="1", vrf=""))
        db.add(OspfInterfaceIntent(device_id=device_id, interface_name="Gi0/0", passive=False))
        db.add(RedistributionIntent(device_id=device_id, dest_protocol="ospf", source_protocol="connected"))
        db.add(RedistributionIntent(device_id=device_id, dest_protocol="bgp", source_protocol="connected"))
        await db.commit()
        break

    apply_fn = AsyncMock()
    async for db in get_session():
        device = await db.get(Device, device_id)
        with patch("nso_adapter.nso.apply.apply_ospf_config", apply_fn):
            await removal_mod._dispatch_scope(db, device, _CLIENT, "ospf")
        break

    apply_fn.assert_awaited_once()
    args, kwargs = apply_fn.await_args
    # apply_ospf_config(client, name, insts, ifaces, redist, replace=True)
    assert args[0] is _CLIENT and args[1] == "ra1"
    assert [i.process_id for i in args[2]] == ["1"]
    assert [i.interface_name for i in args[3]] == ["Gi0/0"]
    assert [r.dest_protocol for r in args[4]] == ["ospf"]  # the bgp redist row is filtered out
    assert kwargs == {"replace": True}


async def test_dispatch_scope_unknown_raises(adapter_client):
    device_id = await _seed_device()
    async for db in get_session():
        device = await db.get(Device, device_id)
        with pytest.raises(ValueError, match="Unknown removal scope"):
            await removal_mod._dispatch_scope(db, device, _CLIENT, "nope")
        break


# ── run_removal (job runner) ──────────────────────────────────────────────────


async def test_run_removal_dispatches_and_marks_succeeded(adapter_client):
    """run_removal runs the scope handler and marks the real job succeeded."""
    from nso_adapter.core.removal import run_removal

    device_id = await _seed_device(nso_device_name="sw3")
    job_id = await _seed_removal_job(device_id, "vlan")

    disp = AsyncMock()
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=_CLIENT),
        patch("nso_adapter.core.removal._dispatch_scope", new=disp),
    ):
        await run_removal(job_id=job_id, device_id=device_id)

    disp.assert_awaited_once()
    # Dispatched with the real device, the resolved client, and the scope from job.context.
    args = disp.await_args.args
    assert args[1].id == device_id and args[2] is _CLIENT and args[3] == "vlan"

    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        assert job.result == {"scope": "vlan"}
        break


async def test_run_removal_records_failure(adapter_client):
    """A handler error is recorded on the real job, not raised."""
    from nso_adapter.core.removal import run_removal

    device_id = await _seed_device(nso_device_name="sw3")
    job_id = await _seed_removal_job(device_id, "vlan")

    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=_CLIENT),
        patch("nso_adapter.core.removal._dispatch_scope", new=AsyncMock(side_effect=RuntimeError("boom"))),
    ):
        await run_removal(job_id=job_id, device_id=device_id)

    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.error["code"] == "removal_failed"
        break
