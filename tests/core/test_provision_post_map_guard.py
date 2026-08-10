# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""M6.9l — provision's post-map phase runs under the device claim.

Provision is the one runner that starts claimless and acquires mid-run. Everything up to
the sync-from is CDB/NSO work against no adapter Device; the moment ``onboard_device``
makes a Device visible, the run holds that device's claim and keeps it through the failover
seed and the comprehensive mirror fill. Without it a scheduled sync, a failover tick or a
teardown claims the freshly-visible device and interleaves with the refresh.

Nothing about today's provisioning moved: the inline refresh is still inline and still
gated on a successful ``sync_from``. What changed is who may touch the device while it runs.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import pytest
import sqlalchemy as sa

from nso_adapter.config import get_config
from nso_adapter.core import onboarding as onboarding_mod
from nso_adapter.core.claim import ClaimLostError, ClaimRegistration, ClaimUnavailableError, acquire_claim
from nso_adapter.nso.client import NsoClient
from tests.conftest import seed_device, session

pytestmark = pytest.mark.anyio

_INSTANCE = "nso-dev"
_NED = "cisco-ios-cli-6.114:cisco-ios-cli-6.114"


def _client(*, sync: bool = True) -> AsyncMock:
    c = AsyncMock(spec=NsoClient)
    c.device_exists.return_value = False
    c.sync_from.return_value = sync
    return c


class _BarrierRefresh:
    """The comprehensive refresh, held open so rivals can be tried against a live claim.

    It commits before parking, exactly as the real per-family refreshes do — which is what
    makes the run's final commit a NEW transaction, and therefore what makes the second
    guard load-bearing. A stand-in that never commits would hold the guard's row lock for
    the whole barrier and prove the wrong thing.
    """

    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.calls = 0

    async def __call__(self, db, device, client, **kwargs):
        self.calls += 1
        await db.commit()
        self.entered.set()
        await self.release.wait()
        return [], None


async def _seed_provision_job() -> int:
    from nso_adapter.store.models import Job, JobStatus, JobType

    async with session() as db:
        job = Job(job_type=JobType.provision, device_id=None, status=JobStatus.running, context={})
        db.add(job)
        await db.commit()
        return job.id


async def _seed_unlinked_device(name: str) -> int:
    """A leftover provisioned into NSO with no NetBox link — the adoption branch."""
    from nso_adapter.store.models import Device

    async with session() as db:
        device = Device(nso_instance=_INSTANCE, nso_device_name=name, netbox_device_id=None)
        db.add(device)
        await db.commit()
        return device.id


async def _device_by_name(name: str):
    from nso_adapter.store.models import Device

    async with session() as db:
        return await db.scalar(sa.select(Device).where(Device.nso_device_name == name))


async def _claim_row(device_id: int):
    from nso_adapter.store.models import DeviceClaim

    async with session() as db:
        return await db.get(DeviceClaim, device_id)


async def _provision(db, *, name, netbox_device_id, reg, job_id, refresh, client=None, sync=True):
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=client or _client(sync=sync)),
        patch("nso_adapter.core.importer.refresh_all_surfaces_for_device", refresh),
    ):
        return await onboarding_mod.provision_nso_device(
            db,
            nso_instance=_INSTANCE,
            device_name=name,
            address="10.0.0.9",
            ned_id=_NED,
            authgroup="network",
            netbox_device_id=netbox_device_id,
            reg=reg,
            job_id=job_id,
        )


# ── M6.9l(b): the refresh runs under the claim, on every acquisition branch ──


@pytest.mark.parametrize("branch", ["fresh", "exact_pair", "adoption"])
async def test_post_map_refresh_runs_under_the_claim(adapter_client_with_nso, monkeypatch, branch):
    """Every rival that could touch the device is excluded for the WHOLE refresh.

    Against an unguarded post-map phase each of these succeeds: the Device is committed and
    visible, and nothing holds it until the run ends.
    """
    from nso_adapter.core.onboarding import offboard_device
    from nso_adapter.store.models import Device, MappingStatus

    monkeypatch.setattr(get_config(), "intent_claim_wait_seconds", 0.3)
    name = f"pg-{branch}"
    netbox_device_id = 7100 + len(branch)
    if branch == "exact_pair":
        await seed_device(nso_device_name=name, netbox_device_id=netbox_device_id, attributes=[])
    elif branch == "adoption":
        await _seed_unlinked_device(name)

    reg = ClaimRegistration()
    job_id = await _seed_provision_job()
    refresh = _BarrierRefresh()

    async with session() as db:
        task = asyncio.create_task(
            _provision(db, name=name, netbox_device_id=netbox_device_id, reg=reg, job_id=job_id, refresh=refresh)
        )
        try:
            await asyncio.wait_for(refresh.entered.wait(), timeout=20)

            assert reg.registered, "the run reached its post-map phase without a claim"
            device_id = reg.device_id
            # A rival claimed sync and a failover tick both lose at the database.
            assert await acquire_claim(device_id, "job") is None
            assert await acquire_claim(device_id, "failover") is None
            # And a teardown waits its budget out rather than dismantling a live onboarding.
            with pytest.raises(ClaimUnavailableError):
                async with session() as other:
                    await offboard_device(other, await other.get(Device, device_id))
        finally:
            refresh.release.set()
        result = await asyncio.wait_for(task, timeout=20)

    assert result["ok"] is True
    assert result["device_id"] == reg.device_id
    assert refresh.calls == 1, "the inline refresh stopped running"

    device = await _device_by_name(name)
    assert device.netbox_device_id == netbox_device_id
    assert device.mapping_status is MappingStatus.mapped
    # The claim OUTLIVES the refresh: _run_provision still owes the terminal status, the
    # result and the device_id link, and a teardown between the two would break that commit.
    claim = await _claim_row(reg.device_id)
    assert claim is not None and claim.claim_token == reg.token
    assert claim.job_id == job_id, "a revoked claim with no job recorded cannot re-disposition it"


async def test_the_sync_ok_gate_on_the_refresh_is_unchanged(adapter_client_with_nso):
    """A failed sync-from still skips the refresh — the empty-wipe race guard, preserved."""
    reg = ClaimRegistration()
    job_id = await _seed_provision_job()
    refresh = _BarrierRefresh()
    refresh.release.set()

    async with session() as db:
        result = await _provision(
            db, name="pg-nosync", netbox_device_id=7150, reg=reg, job_id=job_id, refresh=refresh, sync=False
        )

    assert result["ok"] is True
    assert refresh.calls == 0
    # Still claimed: the mapping happened, so the guard applies to everything after it.
    assert reg.registered
    assert await _claim_row(reg.device_id) is not None


async def test_a_revocation_mid_refresh_stops_the_writes(adapter_client_with_nso):
    """The refresh commits per family, so the final commit is its own transaction and owes
    its own guard. Without the second lock a replaced holder finishes filling the mirror."""
    from nso_adapter.store.models import DeviceClaim

    reg = ClaimRegistration()
    job_id = await _seed_provision_job()
    refresh = _BarrierRefresh()

    async with session() as db:
        task = asyncio.create_task(
            _provision(db, name="pg-revoked", netbox_device_id=7160, reg=reg, job_id=job_id, refresh=refresh)
        )
        await asyncio.wait_for(refresh.entered.wait(), timeout=20)
        async with session() as other:
            await other.execute(sa.delete(DeviceClaim).where(DeviceClaim.device_id == reg.device_id))
            await other.commit()
        refresh.release.set()

        with pytest.raises(ClaimLostError):
            await asyncio.wait_for(task, timeout=20)


async def test_the_failover_seed_is_guarded_too(adapter_client_with_nso, monkeypatch):
    """It commits device state between the mapping and the refresh, so it is not exempt."""
    from nso_adapter.store.models import DeviceClaim, DeviceFailover

    monkeypatch.setattr(get_config().scheduler, "enable_failover", True)
    reg = ClaimRegistration()
    job_id = await _seed_provision_job()
    refresh = _BarrierRefresh()
    refresh.release.set()

    original = onboarding_mod._insert_device_with_claim

    async def _revoke_after_mapping(*args, **kwargs):
        device = await original(*args, **kwargs)
        async with session() as other:
            await other.execute(sa.delete(DeviceClaim).where(DeviceClaim.device_id == device.id))
            await other.commit()
        return device

    monkeypatch.setattr(onboarding_mod, "_insert_device_with_claim", _revoke_after_mapping)

    async with session() as db:
        with pytest.raises(ClaimLostError):
            await _provision(db, name="pg-fo", netbox_device_id=7170, reg=reg, job_id=job_id, refresh=refresh)

    async with session() as db:
        seeded = await db.scalar(sa.select(DeviceFailover).where(DeviceFailover.device_id == reg.device_id))
    assert seeded is None, "a revoked run seeded the failover row anyway"


async def test_fresh_device_and_claim_commit_together(adapter_client_with_nso, monkeypatch):
    """M6.9l(b) — ONE transaction: kill the mapping between the two rows and NEITHER exists.

    The failure is injected at the claim, after the Device row has been written. An
    implementation that commits the Device and then acquires leaves it behind here — exactly
    the unclaimed-and-visible window, on the one branch that can avoid it entirely.
    """
    from nso_adapter.store.models import DeviceClaim

    def _connection_lost(**_kwargs):
        raise RuntimeError("connection lost before the claim was written")

    monkeypatch.setattr(onboarding_mod, "DeviceClaim", _connection_lost)

    reg = ClaimRegistration()
    job_id = await _seed_provision_job()
    refresh = _BarrierRefresh()
    refresh.release.set()

    async with session() as db:
        with pytest.raises(RuntimeError):
            await _provision(db, name="pg-atomic", netbox_device_id=7180, reg=reg, job_id=job_id, refresh=refresh)

    monkeypatch.undo()
    assert not reg.registered
    assert await _device_by_name("pg-atomic") is None, "the Device survived a failed claim write"
    async with session() as db:
        assert (await db.execute(sa.select(DeviceClaim))).first() is None


# ── M6.9l(c): the acquisition timeout ────────────────────────────────────────


async def test_claim_timeout_fails_provision_retryably(adapter_client_with_nso, monkeypatch):
    """A held claim must refuse the mapping, not proceed unserialized — and leave NO write.

    Driven through the real runner, because the honest-failure envelope is what the operator
    and the plugin actually see.
    """
    from nso_adapter.core.jobs import _JOB_RUNNERS
    from nso_adapter.store.models import Device, Job, JobStatus, JobType

    monkeypatch.setattr(get_config(), "intent_claim_wait_seconds", 0.3)
    device_id = await _seed_unlinked_device("pg-busy")
    rival = await acquire_claim(device_id, "intent_put")
    assert rival is not None

    async with session() as db:
        job = Job(
            job_type=JobType.provision,
            device_id=None,
            status=JobStatus.running,
            run_attempt=1,
            context={
                "nso_instance": _INSTANCE,
                "device_name": "pg-busy",
                "address": "10.0.0.9",
                "ned_id": _NED,
                "authgroup": "network",
                "netbox_device_id": 7190,
            },
        )
        db.add(job)
        await db.commit()
        job_id = job.id

    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=_client()),
        patch("nso_adapter.core.importer.refresh_all_surfaces_for_device", AsyncMock(return_value=([], None))),
    ):
        await _JOB_RUNNERS[JobType.provision](job_id, None, ClaimRegistration())

    async with session() as db:
        job = await db.get(Job, job_id)
        device = await db.get(Device, device_id)
    assert job.status is JobStatus.failed
    assert job.error["code"] == "device_busy"
    assert job.error["detail"]["reason"] == "claim_unavailable"
    assert job.error["detail"]["retryable"] is True
    # B3: the adoption must not have written before the claim.
    assert device.netbox_device_id is None, "a half-adopted row survived a refused mapping"
    assert (await _claim_row(device_id)).claim_token == rival.token


async def test_a_refused_terminal_write_discards_the_provision_transaction(adapter_client_with_nso):
    """S1 — a stale runner's success path must not commit under a refused CAS.

    The job's ``run_attempt`` moved on while this run was in flight (recovery re-dispatched
    the job to a successor), so the terminal CAS is refused. The device and claim committed
    mid-run and stay — but any write still riding the FINAL transaction when terminalize is
    reached (modeled by a mirror-refresh stand-in that stamps the device without
    committing) belongs to an execution that lost its ownership. Forbidden: committing
    that transaction anyway.
    """
    from nso_adapter.core.jobs import _JOB_RUNNERS
    from nso_adapter.store.models import Device, Job, JobStatus, JobType

    async def _tail_write(db, device_id, client, *, reg=None):
        device = await db.get(Device, device_id)
        device.sw_version = "tail-write"

    async with session() as db:
        job = Job(
            job_type=JobType.provision,
            device_id=None,
            status=JobStatus.running,
            run_attempt=2,
            context={
                "nso_instance": _INSTANCE,
                "device_name": "pg-stale-success",
                "address": "10.0.0.9",
                "ned_id": _NED,
                "authgroup": "network",
                "netbox_device_id": 7191,
            },
        )
        db.add(job)
        await db.commit()
        job_id = job.id

    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=_client()),
        patch("nso_adapter.core.onboarding._initial_mirror_refresh", _tail_write),
    ):
        await _JOB_RUNNERS[JobType.provision](job_id, None, ClaimRegistration(run_attempt=1))

    async with session() as db:
        job = await db.get(Job, job_id)
    device = await _device_by_name("pg-stale-success")
    assert job.status is JobStatus.running, "the refused write must leave the successor's row alone"
    assert job.run_attempt == 2
    assert job.result is None and job.settle_seq is None
    assert device is not None, "the mid-run device+claim commit is durable by design"
    assert device.sw_version is None, "the discarded transaction leaked a tail write"


# ── M6.9s: teardown vs provision is serialization, not arbitration ───────────


async def test_a_teardown_before_the_mapping_yields_a_fresh_onboarding(adapter_client_with_nso):
    """M6.9s(i) — no Device left, so the mapping is a genuinely fresh one, not an error."""
    from nso_adapter.core.onboarding import offboard_device
    from nso_adapter.store.models import Device

    original_id = await seed_device(nso_device_name="pg-torn", netbox_device_id=7200, attributes=[])

    client = _client()

    async def _teardown_then_sync(_name):
        async with session() as other:
            await offboard_device(other, await other.get(Device, original_id))
        return True

    client.sync_from.side_effect = _teardown_then_sync

    reg = ClaimRegistration()
    job_id = await _seed_provision_job()
    refresh = _BarrierRefresh()
    refresh.release.set()

    async with session() as db:
        result = await _provision(
            db, name="pg-torn", netbox_device_id=7200, reg=reg, job_id=job_id, refresh=refresh, client=client
        )

    assert result["ok"] is True
    assert result["device_id"] != original_id, "the mapping reused a torn-down device"
    assert reg.registered and reg.device_id == result["device_id"]


async def test_a_device_that_vanishes_before_the_claim_is_retried_as_fresh(adapter_client_with_nso, monkeypatch):
    """The bounded retry on the OQ6 budget: discovery saw a device the acquisition cannot."""
    from nso_adapter.core.onboarding import offboard_device
    from nso_adapter.store.models import Device

    monkeypatch.setattr(get_config(), "intent_claim_wait_seconds", 5.0)
    original_id = await seed_device(nso_device_name="pg-vanish", netbox_device_id=7210, attributes=[])

    original = onboarding_mod.acquire_claim_resolving
    fired = {"n": 0}

    async def _tear_down_then_acquire(device_id, purpose, **kwargs):
        # Exactly once, between the non-locking discovery and the acquisition.
        fired["n"] += 1
        if fired["n"] == 1:
            async with session() as other:
                await offboard_device(other, await other.get(Device, device_id))
        return await original(device_id, purpose, **kwargs)

    monkeypatch.setattr(onboarding_mod, "acquire_claim_resolving", _tear_down_then_acquire)

    reg = ClaimRegistration()
    job_id = await _seed_provision_job()
    refresh = _BarrierRefresh()
    refresh.release.set()

    async with session() as db:
        result = await _provision(db, name="pg-vanish", netbox_device_id=7210, reg=reg, job_id=job_id, refresh=refresh)

    assert fired["n"] == 1
    assert result["ok"] is True and result["device_id"] != original_id


async def test_a_taken_netbox_id_is_refused_and_leaks_no_claim(adapter_client_with_nso):
    """The mapping conflict is reported, not retried — and no claim survives the refusal."""
    from nso_adapter.store.models import DeviceClaim

    await seed_device(nso_device_name="pg-holder", netbox_device_id=7240, attributes=[])

    reg = ClaimRegistration()
    job_id = await _seed_provision_job()
    refresh = _BarrierRefresh()
    refresh.release.set()

    async with session() as db:
        result = await _provision(db, name="pg-taken", netbox_device_id=7240, reg=reg, job_id=job_id, refresh=refresh)

    mapping = next(step for step in result["steps"] if step["step"] == "adapter_mapping")
    assert mapping["status"] == "exists"
    assert result["device_id"] is None
    assert not reg.registered
    assert await _device_by_name("pg-taken") is None
    async with session() as db:
        assert (await db.execute(sa.select(DeviceClaim))).first() is None


async def test_a_pair_mapped_elsewhere_is_reported_and_leaks_no_claim(adapter_client_with_nso):
    """The node is already linked to a DIFFERENT NetBox device: report it, never repoint it.

    The conflict is detected under the claim, after the read transaction ends, so the
    message must be built from values snapshotted while the instance was still live — an
    implicit lazy load on an expired one raises MissingGreenlet and turns a clean
    ``adapter_mapping: exists`` into an internal failure.
    """
    from nso_adapter.store.models import DeviceClaim

    await seed_device(nso_device_name="pg-elsewhere", netbox_device_id=7250, attributes=[])

    reg = ClaimRegistration()
    job_id = await _seed_provision_job()
    refresh = _BarrierRefresh()
    refresh.release.set()

    async with session() as db:
        result = await _provision(
            db, name="pg-elsewhere", netbox_device_id=7251, reg=reg, job_id=job_id, refresh=refresh
        )

    mapping = next(step for step in result["steps"] if step["step"] == "adapter_mapping")
    assert mapping["status"] == "exists"
    assert "7250" in mapping["detail"]
    assert not reg.registered
    async with session() as db:
        assert (await db.execute(sa.select(DeviceClaim))).first() is None


async def test_a_failure_right_after_the_claim_commits_still_registers_it(adapter_client_with_nso):
    """Registration must follow the durable COMMIT immediately, not the bookkeeping after it.

    Anything that can raise in between leaves a claim in the table that the worker still
    reads as claimless: no guard, no release, and no recovery until the reaper.
    """
    reg = ClaimRegistration()
    job_id = await _seed_provision_job()
    refresh = _BarrierRefresh()
    refresh.release.set()

    async with session() as db:
        real_refresh = db.refresh

        async def _fail_once(instance, *args, **kwargs):
            db.refresh = real_refresh
            raise RuntimeError("the connection dropped after the mapping committed")

        db.refresh = _fail_once
        with pytest.raises(RuntimeError):
            await _provision(db, name="pg-late", netbox_device_id=7270, reg=reg, job_id=job_id, refresh=refresh)

    assert reg.registered, "the claim is durable but the run still looks claimless"
    assert (await _claim_row(reg.device_id)) is not None, "the release is the worker's, not the mapping's"


async def test_a_cancellation_at_the_fresh_commit_hands_over_the_claim(adapter_client_with_nso):
    """A cancel delivered AT the claim-producing COMMIT is the same in-doubt state as a lost
    acknowledgement: the rows may be durable. It must be resolved and handed to the
    registration — and the cancel must still propagate, or the worker's drain breaks.
    """
    from nso_adapter.store.models import DeviceClaim

    reg = ClaimRegistration()
    job_id = await _seed_provision_job()
    refresh = _BarrierRefresh()
    refresh.release.set()

    async with session() as db:
        real_commit = db.commit
        fired = {"n": 0}

        async def _commit_then_cancel() -> None:
            fired["n"] += 1
            await real_commit()
            if fired["n"] == 1:
                raise asyncio.CancelledError()

        db.commit = _commit_then_cancel
        with pytest.raises(asyncio.CancelledError):
            await _provision(db, name="pg-cancel", netbox_device_id=7280, reg=reg, job_id=job_id, refresh=refresh)

    device = await _device_by_name("pg-cancel")
    assert device is not None, "the mapping committed, so both rows are durable"
    assert reg.registered and reg.device_id == device.id, "a durable claim was left looking unowned"
    async with session() as db:
        claim = await db.get(DeviceClaim, device.id)
    assert claim is not None and claim.claim_token == reg.token


async def test_a_cancellation_at_the_existing_device_acquisition_hands_over_the_claim(adapter_client_with_nso):
    """Same seam on the branch that acquires in a transaction of its own."""
    from nso_adapter.core import claim as claim_mod
    from nso_adapter.store.models import Device, DeviceClaim

    device_id = await _seed_unlinked_device("pg-cancel2")
    reg = ClaimRegistration()
    job_id = await _seed_provision_job()
    refresh = _BarrierRefresh()
    refresh.release.set()

    original = claim_mod.acquire_claim

    async def _acquire_then_cancel(*args, **kwargs):
        await original(*args, **kwargs)
        raise asyncio.CancelledError()

    with patch.object(claim_mod, "acquire_claim", _acquire_then_cancel):
        async with session() as db:
            with pytest.raises(asyncio.CancelledError):
                await _provision(db, name="pg-cancel2", netbox_device_id=7290, reg=reg, job_id=job_id, refresh=refresh)

    assert reg.registered and reg.device_id == device_id
    async with session() as db:
        claim = await db.get(DeviceClaim, device_id)
        assert claim is not None and claim.claim_token == reg.token
        # The adoption never ran: the cancel landed at the acquisition.
        assert (await db.get(Device, device_id)).netbox_device_id is None


async def test_a_failed_failover_seed_does_not_poison_the_rest_of_the_run(adapter_client_with_nso, monkeypatch):
    """Best-effort means the STEP fails, not the run: a failed transaction left behind kills
    the mirror refresh and the runner's terminal write on a device that mapped fine."""
    monkeypatch.setattr(get_config().scheduler, "enable_failover", True)

    async def _poison(db, *args, **kwargs):
        await db.execute(sa.text("SELECT 1 / 0"))  # a real error, leaving a real failed txn

    monkeypatch.setattr("nso_adapter.core.failover.set_initial_failover_state", _poison)

    reg = ClaimRegistration()
    job_id = await _seed_provision_job()
    refresh = _BarrierRefresh()
    refresh.release.set()

    async with session() as db:
        result = await _provision(db, name="pg-poison", netbox_device_id=7300, reg=reg, job_id=job_id, refresh=refresh)

    seed = next(step for step in result["steps"] if step["step"] == "failover_seed")
    assert seed["status"] == "failed"
    assert result["ok"] is True
    assert refresh.calls == 1, "the poisoned session took the mirror refresh down with it"


async def test_the_terminal_write_is_guarded_once_the_run_is_claimed(adapter_client_with_nso):
    """A revoked provision must not commit `succeeded` over recovery's disposition.

    The runner's terminal transaction writes status, result and the device_id link on behalf
    of the claim, so it takes the row lock like every other guarded write.
    """
    from nso_adapter.core.jobs import _JOB_RUNNERS
    from nso_adapter.store.models import DeviceClaim, Job, JobStatus, JobType

    async with session() as db:
        job = Job(
            job_type=JobType.provision,
            device_id=None,
            status=JobStatus.queued,
            context={
                "nso_instance": _INSTANCE,
                "device_name": "pg-terminal",
                "address": "10.0.0.9",
                "ned_id": _NED,
                "authgroup": "network",
                "netbox_device_id": 7260,
            },
        )
        db.add(job)
        await db.commit()
        job_id = job.id

    reg = ClaimRegistration()

    async def _revoke_after_the_refresh(db, device_id, client, *, reg=None):
        # The last step before the runner writes its terminal status: a stale claim revoked
        # here, with the job already re-dispositioned, is exactly what the guard must catch.
        async with session() as other:
            await other.execute(sa.delete(DeviceClaim).where(DeviceClaim.device_id == device_id))
            await other.execute(sa.update(Job).where(Job.id == job_id).values(status=JobStatus.queued))
            await other.commit()

    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=_client()),
        patch("nso_adapter.core.onboarding._initial_mirror_refresh", _revoke_after_the_refresh),
        pytest.raises(ClaimLostError),
    ):
        await _JOB_RUNNERS[JobType.provision](job_id, None, reg)

    async with session() as db:
        assert (await db.get(Job, job_id)).status is JobStatus.queued, "the revoked run overwrote the disposition"


async def test_a_lost_insert_acquires_on_the_winner(adapter_client_with_nso, monkeypatch):
    """The fourth branch: two onboardings of one node, and the loser must not run unclaimed."""
    from nso_adapter.store.models import Device

    original = onboarding_mod._insert_device_with_claim
    fired = {"n": 0}

    async def _let_a_rival_win_first(db, nso_instance, nso_device_name, netbox_device_id, reg, job_id):
        fired["n"] += 1
        if fired["n"] == 1:
            async with session() as other:
                other.add(Device(nso_instance=nso_instance, nso_device_name=nso_device_name, netbox_device_id=None))
                await other.commit()
        return await original(db, nso_instance, nso_device_name, netbox_device_id, reg, job_id)

    monkeypatch.setattr(onboarding_mod, "_insert_device_with_claim", _let_a_rival_win_first)

    reg = ClaimRegistration()
    job_id = await _seed_provision_job()
    refresh = _BarrierRefresh()
    refresh.release.set()

    async with session() as db:
        result = await _provision(db, name="pg-lost", netbox_device_id=7220, reg=reg, job_id=job_id, refresh=refresh)

    assert fired["n"] == 1, "the insert never lost"
    assert result["ok"] is True
    winner = await _device_by_name("pg-lost")
    # The loser adopted the winner's row under the claim rather than inserting a duplicate.
    assert result["device_id"] == winner.id
    assert winner.netbox_device_id == 7220
    assert reg.registered and reg.device_id == winner.id


# ── the plugin's mapping path is untouched (M6.9l a) ─────────────────────────


async def test_the_mapping_endpoint_takes_no_claim(adapter_client_with_nso):
    """Path A: provision without a netbox id, then the plugin's mapping POST.

    A preservation pin — the mapping creates the Device, enqueues nothing, runs no inline
    refresh (there is none on that path) and takes no claim.
    """
    from nso_adapter.store.models import DeviceClaim, Job

    reg = ClaimRegistration()
    job_id = await _seed_provision_job()
    refresh = _BarrierRefresh()
    refresh.release.set()

    async with session() as db:
        result = await _provision(db, name="pg-patha", netbox_device_id=None, reg=reg, job_id=job_id, refresh=refresh)

    assert result["ok"] is True and result["device_id"] is None
    assert not reg.registered, "a provision with no mapping must stay on the claimless lane"
    assert refresh.calls == 0

    async with session() as db:
        device = await onboarding_mod.onboard_device(db, _INSTANCE, "pg-patha", 7230)
        device_id = device.id

    async with session() as db:
        assert await db.get(DeviceClaim, device_id) is None
        jobs = (await db.execute(sa.select(Job).where(Job.device_id == device_id))).scalars().all()
        assert jobs == []
