# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Unit tests for core/removal.py — async removal propagation.

Removal no longer runs the device commit inline in the intent PUT; it enqueues a
``removal`` job that the worker runs in the background. These tests cover the
enqueue path, the back-compat shim, and the job runner's scope dispatch.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from nso_adapter.core import removal as removal_mod
from nso_adapter.core.removal import enqueue_removal, replace_on_removal


@pytest.mark.asyncio
async def test_replace_on_removal_noop_when_nothing_removed():
    """No removals → no job enqueued, returns False, no commit."""
    db = MagicMock()
    db.add = MagicMock()
    db.commit = AsyncMock()
    result = await replace_on_removal(db, SimpleNamespace(id=1), [], object)
    assert result is False
    db.add.assert_not_called()
    db.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_replace_on_removal_enqueues_job_and_commits():
    """On removal, a `removal` job for the model's scope is enqueued + committed."""
    from nso_adapter.store.models import JobType, VlanIntent

    db = MagicMock()
    db.add = MagicMock()
    db.flush = AsyncMock()
    db.commit = AsyncMock()
    device = SimpleNamespace(id=7)

    ok = await replace_on_removal(db, device, [3366], VlanIntent)

    assert ok is True
    db.add.assert_called_once()
    job = db.add.call_args[0][0]
    assert job.job_type == JobType.removal
    assert job.device_id == 7
    assert job.context == {"scope": "vlan"}
    db.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_replace_on_removal_unknown_model_returns_false():
    """An unmapped store model never enqueues (and never crashes the request)."""
    db = MagicMock()
    db.add = MagicMock()
    db.commit = AsyncMock()

    class _Unmapped:
        pass

    ok = await replace_on_removal(db, SimpleNamespace(id=1), [1], _Unmapped)
    assert ok is False
    db.add.assert_not_called()


@pytest.mark.asyncio
async def test_enqueue_removal_rejects_unknown_scope():
    db = MagicMock()
    with pytest.raises(ValueError, match="Unknown removal scope"):
        await enqueue_removal(db, 1, "bogus")


@pytest.mark.asyncio
async def test_enqueue_removal_creates_job_for_each_valid_scope():
    """Every reconciler scope (incl ospf/bgp) maps to a removal job."""
    from nso_adapter.core.removal import VALID_REMOVAL_SCOPES
    from nso_adapter.store.models import JobType

    for scope in VALID_REMOVAL_SCOPES:
        db = MagicMock()
        db.add = MagicMock()
        db.flush = AsyncMock()
        job = await enqueue_removal(db, 5, scope)
        assert job.job_type == JobType.removal
        assert job.context == {"scope": scope}


@pytest.mark.asyncio
async def test_dispatch_scope_simple_calls_apply_replace_true():
    """A simple scope fetches accepted rows and calls its apply with replace=True."""
    remaining = [SimpleNamespace(vlan_id=10)]
    scalars = MagicMock()
    scalars.all.return_value = remaining
    result_obj = MagicMock()
    result_obj.scalars.return_value = scalars
    db = MagicMock()
    db.execute = AsyncMock(return_value=result_obj)
    device = SimpleNamespace(id=1, nso_device_name="sw3")

    apply_fn = AsyncMock()
    with patch("nso_adapter.nso.apply.apply_vlan_config", apply_fn):
        await removal_mod._dispatch_scope(db, device, "CLIENT", "vlan")
    apply_fn.assert_awaited_once_with("CLIENT", "sw3", remaining, replace=True)


@pytest.mark.asyncio
async def test_dispatch_scope_ospf_uses_multi_row_apply():
    """OSPF dispatch fetches instances+interfaces+redist and applies with replace=True."""
    scalars = MagicMock()
    scalars.all.return_value = []
    result_obj = MagicMock()
    result_obj.scalars.return_value = scalars
    db = MagicMock()
    db.execute = AsyncMock(return_value=result_obj)
    device = SimpleNamespace(id=1, nso_device_name="ra1")

    apply_fn = AsyncMock()
    with patch("nso_adapter.nso.apply.apply_ospf_config", apply_fn):
        await removal_mod._dispatch_scope(db, device, "CLIENT", "ospf")
    apply_fn.assert_awaited_once()
    assert apply_fn.await_args.kwargs["replace"] is True


@pytest.mark.asyncio
async def test_dispatch_scope_unknown_raises():
    with pytest.raises(ValueError, match="Unknown removal scope"):
        await removal_mod._dispatch_scope(MagicMock(), SimpleNamespace(id=1), "C", "nope")


@pytest.mark.asyncio
async def test_run_removal_dispatches_and_marks_succeeded():
    """run_removal runs the scope handler and marks the job succeeded."""
    from nso_adapter.store.models import JobStatus

    job = SimpleNamespace(status=None, context={"scope": "vlan"}, result=None, error=None)
    device = SimpleNamespace(id=1, nso_instance="nso-dev", nso_device_name="sw3")

    db = MagicMock()
    db.get = AsyncMock(side_effect=[job, device])
    db.commit = AsyncMock()

    async def _fake_session():
        yield db

    with (
        patch("nso_adapter.store.db.get_session", _fake_session),
        patch("nso_adapter.core.importer.get_nso_client", return_value="CLIENT"),
        patch("nso_adapter.core.removal._dispatch_scope", new=AsyncMock()) as disp,
    ):
        from nso_adapter.core.removal import run_removal

        await run_removal(job_id=99, device_id=1)

    disp.assert_awaited_once()
    assert job.status == JobStatus.succeeded
    assert job.result == {"scope": "vlan"}


@pytest.mark.asyncio
async def test_run_removal_records_failure():
    """A handler error is recorded on the job, not raised."""
    from nso_adapter.store.models import JobStatus

    job = SimpleNamespace(status=None, context={"scope": "vlan"}, result=None, error=None)
    device = SimpleNamespace(id=1, nso_instance="nso-dev", nso_device_name="sw3")
    db = MagicMock()
    db.get = AsyncMock(side_effect=[job, device])
    db.commit = AsyncMock()

    async def _fake_session():
        yield db

    with (
        patch("nso_adapter.store.db.get_session", _fake_session),
        patch("nso_adapter.core.importer.get_nso_client", return_value="CLIENT"),
        patch("nso_adapter.core.removal._dispatch_scope", new=AsyncMock(side_effect=RuntimeError("boom"))),
    ):
        from nso_adapter.core.removal import run_removal

        await run_removal(job_id=99, device_id=1)

    assert job.status == JobStatus.failed
    assert job.error["code"] == "removal_failed"
