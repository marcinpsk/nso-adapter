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
    IsisFlexAlgoIntent,
    IsisInterfaceIntent,
    IsisLevelIntent,
    IsisProcessIntent,
    Job,
    JobStatus,
    JobType,
    OspfInstanceIntent,
    OspfInterfaceIntent,
    RedistributionIntent,
    RoutePolicyObjectIntent,
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


async def _seed_removal_job(device_id: int, scope: str = "vlan", context_extra: dict | None = None) -> int:
    async for db in get_session():
        j = Job(
            job_type=JobType.removal,
            device_id=device_id,
            status=JobStatus.queued,
            context={"scope": scope, **(context_extra or {})},
        )
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
        assert job.context == {"scope": "vlan", "removed": {"vlan": [3366]}, "detach": True}
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
            assert job.context == {"scope": scope, "detach": True}
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
    client = _guard_client(None)  # no service instance in NSO → collateral guard no-ops
    async for db in get_session():
        device = await db.get(Device, device_id)
        with patch("nso_adapter.nso.apply.apply_vlan_config", apply_fn):
            await removal_mod._dispatch_scope(db, device, client, "vlan")
        break

    apply_fn.assert_awaited_once()
    args, kwargs = apply_fn.await_args
    assert args[0] is client
    assert args[1] == "sw3"
    assert [r.vlan_id for r in args[2]] == [10]  # the accepted_at filter dropped vlan 20
    assert kwargs == {"replace": True}


async def test_dispatch_scope_ospf_uses_multi_row_apply(adapter_client):
    """OSPF dispatch fetches ONLY accepted instances+interfaces+redist(ospf only), replace=True.

    A PUT-replace re-asserts the full desired state, so it must never include
    not-yet-accepted (imported/staged) rows — that would deploy un-reviewed config.
    """
    device_id = await _seed_device(nso_device_name="ra1")
    async for db in get_session():
        db.add(OspfInstanceIntent(device_id=device_id, process_id="1", vrf="", accepted_at=_NOW))
        db.add(OspfInstanceIntent(device_id=device_id, process_id="9", vrf="", accepted_at=None))  # excluded
        db.add(OspfInterfaceIntent(device_id=device_id, interface_name="Gi0/0", passive=False, accepted_at=_NOW))
        db.add(OspfInterfaceIntent(device_id=device_id, interface_name="Gi0/9", passive=False, accepted_at=None))
        db.add(
            RedistributionIntent(
                device_id=device_id, dest_protocol="ospf", source_protocol="connected", accepted_at=_NOW
            )
        )
        db.add(
            RedistributionIntent(device_id=device_id, dest_protocol="ospf", source_protocol="static", accepted_at=None)
        )  # excluded
        db.add(
            RedistributionIntent(
                device_id=device_id, dest_protocol="bgp", source_protocol="connected", accepted_at=_NOW
            )
        )
        await db.commit()
        break

    apply_fn = AsyncMock()
    client = _guard_client(None)  # no service instance in NSO → collateral guard no-ops
    async for db in get_session():
        device = await db.get(Device, device_id)
        with patch("nso_adapter.nso.apply.apply_ospf_config", apply_fn):
            await removal_mod._dispatch_scope(db, device, client, "ospf")
        break

    apply_fn.assert_awaited_once()
    args, kwargs = apply_fn.await_args
    # apply_ospf_config(client, name, insts, ifaces, redist, replace=True)
    assert args[0] is client and args[1] == "ra1"
    assert [i.process_id for i in args[2]] == ["1"]  # un-accepted process 9 filtered out
    assert [i.interface_name for i in args[3]] == ["Gi0/0"]  # un-accepted Gi0/9 filtered out
    # only the accepted ospf redist row survives (bgp + un-accepted ospf filtered)
    assert [(r.dest_protocol, r.source_protocol) for r in args[4]] == [("ospf", "connected")]
    assert kwargs == {"replace": True}


async def test_isis_in_valid_removal_scopes():
    """IS-IS must be a recognised removal scope (else enqueue_removal rejects it)."""
    assert "isis" in removal_mod.VALID_REMOVAL_SCOPES


async def test_dispatch_scope_isis_uses_multi_row_apply(adapter_client):
    """IS-IS dispatch fetches ONLY accepted iface/proc/redist(isis)/flex/level, replace=True.

    A PUT-replace re-asserts the full desired state, so it must never include
    not-yet-accepted rows, and must scope redistribution to dest_protocol=isis.
    """
    device_id = await _seed_device(nso_device_name="ra1")
    async for db in get_session():
        db.add(
            IsisInterfaceIntent(
                device_id=device_id, interface_name="system", af="ipv4", process_tag="0", passive=True, accepted_at=_NOW
            )
        )
        db.add(
            IsisInterfaceIntent(
                device_id=device_id, interface_name="lag1", af="ipv4", process_tag="0", passive=False, accepted_at=None
            )
        )  # excluded (un-accepted)
        db.add(IsisProcessIntent(device_id=device_id, process_tag="0", net="49.0001.00", accepted_at=_NOW))
        db.add(IsisFlexAlgoIntent(device_id=device_id, process_tag="0", algo_id=128, accepted_at=_NOW))
        db.add(IsisLevelIntent(device_id=device_id, process_tag="0", level=2, accepted_at=_NOW))
        db.add(
            RedistributionIntent(
                device_id=device_id, dest_protocol="isis", source_protocol="connected", accepted_at=_NOW
            )
        )
        db.add(
            RedistributionIntent(device_id=device_id, dest_protocol="bgp", source_protocol="static", accepted_at=_NOW)
        )  # excluded (bgp)
        await db.commit()
        break

    apply_fn = AsyncMock()
    client = _guard_client(None)  # no service instance in NSO → collateral guard no-ops
    async for db in get_session():
        device = await db.get(Device, device_id)
        with patch("nso_adapter.nso.apply.apply_isis_interfaces", apply_fn):
            await removal_mod._dispatch_scope(db, device, client, "isis")
        break

    apply_fn.assert_awaited_once()
    args, kwargs = apply_fn.await_args
    # apply_isis_interfaces(client, name, ifaces, procs, redist, flex, levels, replace=True)
    assert args[0] is client and args[1] == "ra1"
    assert [i.interface_name for i in args[2]] == ["system"]  # un-accepted lag1 filtered out
    assert [p.process_tag for p in args[3]] == ["0"]
    assert [(r.dest_protocol, r.source_protocol) for r in args[4]] == [("isis", "connected")]  # bgp filtered
    assert [f.algo_id for f in args[5]] == [128]
    assert [lv.level for lv in args[6]] == [2]
    assert kwargs == {"replace": True}


async def test_dispatch_scope_route_policy_passes_ned_id(adapter_client):
    """Route-policy removal MUST thread the device's ned_id so community members are
    translated to the device's NED dialect (identity dialect on ned_id=None pushes the
    wrong wire form / fails to skip unrepresentable members)."""
    device_id = await _seed_device(nso_device_name="ra1")
    async for db in get_session():
        device = await db.get(Device, device_id)
        device.ned_id = "cisco-iosxr-nc-7.3"
        db.add(RoutePolicyObjectIntent(device_id=device_id, family="rpl", name="RP-IN", entries=[], accepted_at=_NOW))
        await db.commit()
        break

    apply_fn = AsyncMock()
    client = _guard_client(None)  # no service instance in NSO → collateral guard no-ops
    async for db in get_session():
        device = await db.get(Device, device_id)
        with patch("nso_adapter.nso.apply.apply_route_policy_config", apply_fn):
            await removal_mod._dispatch_scope(db, device, client, "route_policy")
        break

    apply_fn.assert_awaited_once()
    args, kwargs = apply_fn.await_args
    assert args[0] is client and args[1] == "ra1"
    assert [r.name for r in args[2]] == ["RP-IN"]
    assert kwargs.get("ned_id") == "cisco-iosxr-nc-7.3"
    assert kwargs.get("replace") is True


async def test_dispatch_interface_config_puts_remaining_and_deletes_empty(adapter_client):
    """interface_config removal PUT-replaces an interface that still has accepted intent, and
    DELETEs one with none — so a removed IP is reverted on the device (#5)."""
    from nso_adapter.store.models import DbInterface, InterfaceIpIntent

    device_id = await _seed_device(nso_device_name="sw3")
    async for db in get_session():
        keep = DbInterface(device_id=device_id, name="Gi0/0")  # still has an accepted IP → PUT
        gone = DbInterface(device_id=device_id, name="Gi0/1")  # no remaining intent → DELETE
        db.add(keep)
        db.add(gone)
        await db.flush()
        db.add(InterfaceIpIntent(interface_id=keep.id, address="10.0.0.1/24", family="ipv4", vrf="", accepted_at=_NOW))
        await db.commit()
        break

    replace_fn = AsyncMock()
    delete_fn = AsyncMock()
    async for db in get_session():
        device = await db.get(Device, device_id)
        with (
            patch("nso_adapter.nso.apply.replace_interface_config", replace_fn),
            patch("nso_adapter.nso.apply.delete_interface_config", delete_fn),
        ):
            await removal_mod._dispatch_scope(
                db, device, _CLIENT, "interface_config", {"interfaces": ["Gi0/0", "Gi0/1"]}
            )
        break

    replace_fn.assert_awaited_once()
    assert replace_fn.await_args.args[1] == "sw3" and replace_fn.await_args.args[2] == "Gi0/0"
    delete_fn.assert_awaited_once()
    assert delete_fn.await_args.args[1] == "sw3" and delete_fn.await_args.args[2] == "Gi0/1"


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
        assert job.result["scope"] == "vlan"
        # No `removed` keys in this job's context → nothing to residue-check (#104); the
        # opaque client's reader surface is never touched. Report that honestly — a job
        # that never read the device must not claim the device came back clean.
        assert job.result["residue_check"] == "unsupported"
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


async def test_run_removal_marks_failed_even_when_session_poisoned(adapter_client):
    """A DB-origin error in dispatch poisons the session (needs-rollback); the failure handler
    must rollback before the failed-status commit, or that commit re-raises and the job is
    stranded 'running' (s3-5 — same fix as run_apply #11)."""
    from nso_adapter.core.removal import run_removal

    device_id = await _seed_device(nso_device_name="sw-poison")
    job_id = await _seed_removal_job(device_id, "vlan")

    async def poison(db, device, client, scope, context=None):
        # A duplicate PK insert → IntegrityError → AsyncSession enters needs-rollback,
        # exactly like a failed flush during the PUT-replace's row bookkeeping.
        db.add(Job(id=job_id, job_type=JobType.removal, device_id=device.id, status=JobStatus.queued))
        await db.flush()

    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=_CLIENT),
        patch("nso_adapter.core.removal._dispatch_scope", new=poison),
    ):
        await run_removal(job_id=job_id, device_id=device_id)

    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.error["code"] == "removal_failed"
        break


# ── isis removal collateral guard (the ra1 lo0 incident, 2026-07-09) ──────────
#
# The 714bd93 PUT-replace retract flushed 4 ORPHANED service rows (PATCH-no-op-era
# debris whose intent deletions never reached NSO) off the live router — lo0 left
# ra1's IGP for ~42h. The guard compares NSO's current service rows against the
# remaining snapshot + the trigger's just-removed keys BEFORE committing; anything
# beyond that is collateral → block with a dry-run preview in the failure detail.


def _isis_client(service_config):
    """A spec'd NSO-client fake for the guard: only the (generic) service GET is primed."""
    return _guard_client(service_config)


# The staged body the guard diffs against for the seeded ("system", "ipv4") snapshot —
# what the REAL apply_isis_interfaces would build from the remaining accepted rows.
_ISIS_STAGED_SYSTEM = {"interface-config": [{"interface-name": "system", "af": "ipv4"}]}


async def _seed_isis_intent(device_id: int, *ifaces: tuple[str, str]):
    async for db in get_session():
        for name, af in ifaces:
            db.add(
                IsisInterfaceIntent(
                    device_id=device_id, interface_name=name, af=af, process_tag="0", passive=True, accepted_at=_NOW
                )
            )
        await db.commit()
        return


async def _run_guarded_removal(device_id: int, job_id: int, client, apply_fn):
    with (
        patch("nso_adapter.nso.apply.apply_isis_interfaces", apply_fn),
        patch("nso_adapter.core.importer.get_nso_client", return_value=client),
    ):
        await removal_mod.run_removal(job_id, device_id)


async def test_isis_removal_blocked_on_orphaned_service_rows(adapter_client):
    """An NSO service interface row that is neither in the remaining snapshot nor in
    the trigger's just-removed set is an orphan; the job must BLOCK, attach the
    dry-run preview, and commit NOTHING."""
    device_id = await _seed_device(nso_device_name="ra1-guard")
    await _seed_isis_intent(device_id, ("system", "ipv4"))
    client = _isis_client(
        {
            "device": "ra1-guard",
            "interface-config": [
                {"interface-name": "system", "af": "ipv4"},
                {"interface-name": "lo0", "af": "ipv4"},  # orphan — nobody just removed it
            ],
        }
    )
    job_id = await _seed_removal_job(device_id, scope="isis")
    apply_fn = _staging_apply(_ISIS_STAGED_SYSTEM, preview="- interface lo0 (native preview)")
    await _run_guarded_removal(device_id, job_id, client, apply_fn)
    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.error["code"] == "removal_blocked_collateral"
        assert job.error["detail"]["orphans"] == {"interface-config": [["lo0", "ipv4"]]}
        assert job.error["detail"]["preview"] == "- interface lo0 (native preview)"
        break
    # stage + dry-run preview only — nothing was committed
    committed = [c for c in apply_fn.await_args_list if c.kwargs.get("stage") is None and not c.kwargs.get("dry_run")]
    assert committed == []


async def test_isis_removal_orphaned_process_blocks(adapter_client):
    """A service process-config row beyond the snapshot is collateral too — retracting
    it would drop the whole `router isis <tag>` process from the device."""
    device_id = await _seed_device(nso_device_name="ra1-guard-proc")
    await _seed_isis_intent(device_id, ("system", "ipv4"))
    client = _isis_client(
        {
            "device": "ra1-guard-proc",
            "interface-config": [{"interface-name": "system", "af": "ipv4"}],
            "process-config": [{"process-tag": "OLD"}],
        }
    )
    job_id = await _seed_removal_job(device_id, scope="isis")
    apply_fn = _staging_apply(_ISIS_STAGED_SYSTEM, preview="preview")
    await _run_guarded_removal(device_id, job_id, client, apply_fn)
    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.error["detail"]["orphans"] == {"process-config": [["OLD"]]}
        break


async def test_isis_removal_proceeds_when_extra_row_was_just_removed(adapter_client):
    """The trigger threads the keys it just deleted; those are EXPECTED retractions
    (the whole point of the removal job), not collateral."""
    device_id = await _seed_device(nso_device_name="ra1-legit")
    await _seed_isis_intent(device_id, ("system", "ipv4"))
    client = _isis_client(
        {
            "device": "ra1-legit",
            "interface-config": [
                {"interface-name": "system", "af": "ipv4"},
                {"interface-name": "lag1", "af": "ipv4"},  # just removed by the operator
            ],
        }
    )
    # legacy pre-#90 context shape — jobs queued before the generalization must still pass
    job_id = await _seed_removal_job(device_id, scope="isis", context_extra={"removed_interfaces": [["lag1", "ipv4"]]})
    apply_fn = _staging_apply(_ISIS_STAGED_SYSTEM)
    await _run_guarded_removal(device_id, job_id, client, apply_fn)
    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        break
    final = apply_fn.await_args_list[-1]
    assert final.kwargs == {"replace": True}


async def test_isis_removal_force_skips_guard(adapter_client):
    """The operator override (actions/force-removal) flushes orphans on purpose."""
    device_id = await _seed_device(nso_device_name="ra1-force")
    await _seed_isis_intent(device_id, ("system", "ipv4"))
    client = _isis_client(
        {
            "device": "ra1-force",
            "interface-config": [
                {"interface-name": "system", "af": "ipv4"},
                {"interface-name": "lo0", "af": "ipv4"},
            ],
        }
    )
    job_id = await _seed_removal_job(device_id, scope="isis", context_extra={"force": True})
    apply_fn = AsyncMock()
    await _run_guarded_removal(device_id, job_id, client, apply_fn)
    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        break
    apply_fn.assert_awaited_once()
    assert apply_fn.await_args.kwargs == {"replace": True}


async def test_isis_removal_without_service_instance_proceeds(adapter_client):
    """No isis-config instance in NSO (404 → None) → nothing to guard, plain replace."""
    device_id = await _seed_device(nso_device_name="ra1-fresh")
    await _seed_isis_intent(device_id, ("system", "ipv4"))
    client = _isis_client(None)
    job_id = await _seed_removal_job(device_id, scope="isis")
    apply_fn = AsyncMock()
    await _run_guarded_removal(device_id, job_id, client, apply_fn)
    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        break
    apply_fn.assert_awaited_once()


# ── generalized collateral guard (#90) — every PUT-replace scope ──────────────
#
# The same orphan-flush risk exists in every scope whose removal PUT-replaces a
# device-keyed service instance (ospf/bgp/snmp + the _SIMPLE_TARGETS). The guard
# generalizes: stage the would-be PUT body (no HTTP), diff its keyed YANG lists
# against the CURRENT service instance, allow only the trigger's just-removed
# keys (context["removed"] = {yang-list: [keys]}), block anything beyond.


def _guard_client(service_config=None):
    """A spec'd NSO-client fake for the generic guard: only the service GET is primed."""
    from nso_adapter.nso.client import NsoClient

    client = AsyncMock(spec=NsoClient)
    client.get_service_config.return_value = service_config
    return client


def _staging_apply(entry: dict, preview: str = "native preview"):
    """An apply spy honouring the ``stage`` contract of _send_service_config.

    The guard builds the would-be PUT body via ``apply(stage=...)``; a bare
    AsyncMock would leave the stage empty and make every current row look like
    an orphan. The spy records calls like an AsyncMock and stages *entry*.
    """

    async def _impl(*args, **kwargs):
        if kwargs.get("stage") is not None:
            kwargs["stage"]["x:config"] = [entry]
            return None
        if kwargs.get("dry_run"):
            return preview
        return None

    return AsyncMock(side_effect=_impl)


async def _run_removal_with(scope: str, apply_target: str, device_id: int, job_id: int, client, apply_fn):
    with (
        patch(f"nso_adapter.nso.apply.{apply_target}", apply_fn),
        patch("nso_adapter.core.importer.get_nso_client", return_value=client),
    ):
        await removal_mod.run_removal(job_id, device_id)


async def test_snmp_removal_blocked_on_orphaned_community(adapter_client):
    """An snmp service community that is neither in the staged body nor just-removed
    is collateral — the job blocks with the generic orphans detail."""
    device_id = await _seed_device(nso_device_name="sw-snmp-guard")
    client = _guard_client(
        {
            "device": "sw-snmp-guard",
            "community": [{"name": "ops"}, {"name": "legacy"}],
            "host": [{"address": "10.0.0.9"}],
        }
    )
    job_id = await _seed_removal_job(device_id, scope="snmp")
    apply_fn = _staging_apply(
        {"device": "sw-snmp-guard", "community": [{"name": "ops"}], "host": [{"address": "10.0.0.9"}]}
    )
    await _run_removal_with("snmp", "apply_snmp_config", device_id, job_id, client, apply_fn)
    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.error["code"] == "removal_blocked_collateral"
        assert job.error["detail"]["orphans"] == {"community": [["legacy"]]}
        assert job.error["detail"]["preview"] == "native preview"
        break
    # stage + dry-run preview only — nothing committed
    committed = [c for c in apply_fn.await_args_list if c.kwargs.get("stage") is None and not c.kwargs.get("dry_run")]
    assert committed == []


async def test_snmp_removal_passes_when_removed_threaded(adapter_client):
    """The trigger's just-removed community is an EXPECTED retraction, not collateral."""
    device_id = await _seed_device(nso_device_name="sw-snmp-legit")
    client = _guard_client({"device": "sw-snmp-legit", "community": [{"name": "ops"}, {"name": "legacy"}]})
    job_id = await _seed_removal_job(device_id, scope="snmp", context_extra={"removed": {"community": ["legacy"]}})
    apply_fn = _staging_apply({"device": "sw-snmp-legit", "community": [{"name": "ops"}]})
    await _run_removal_with("snmp", "apply_snmp_config", device_id, job_id, client, apply_fn)
    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        break
    final = apply_fn.await_args_list[-1]
    assert final.kwargs.get("replace") is True and final.kwargs.get("stage") is None


async def test_vlan_removal_blocked_on_orphan_vid_normalizes_ints(adapter_client):
    """vlan-id ints (NSO JSON) and store ints compare as strings — no false pass/block."""
    device_id = await _seed_device(nso_device_name="sw-vlan-guard")
    client = _guard_client({"device": "sw-vlan-guard", "vlan": [{"vlan-id": 10}, {"vlan-id": 99}]})
    job_id = await _seed_removal_job(device_id, scope="vlan")
    apply_fn = _staging_apply({"device": "sw-vlan-guard", "vlan": [{"vlan-id": 10}]})
    await _run_removal_with("vlan", "apply_vlan_config", device_id, job_id, client, apply_fn)
    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.error["detail"]["orphans"] == {"vlan": [["99"]]}
        break


async def test_bgp_removal_blocked_on_nested_orphan_peer(adapter_client):
    """bgp peers live two lists deep (router/scope/peer); an orphan peer still blocks."""
    device_id = await _seed_device(nso_device_name="sw-bgp-guard")
    service = {
        "device": "sw-bgp-guard",
        "router": [
            {
                "asn": "64500",
                "scope": [{"vrf": "", "peer": [{"peer-address": "192.0.2.1"}, {"peer-address": "192.0.2.9"}]}],
            }
        ],
    }
    staged = {
        "device": "sw-bgp-guard",
        "router": [{"asn": "64500", "scope": [{"vrf": "", "peer": [{"peer-address": "192.0.2.1"}]}]}],
    }
    client = _guard_client(service)
    job_id = await _seed_removal_job(device_id, scope="bgp")
    apply_fn = _staging_apply(staged)
    await _run_removal_with("bgp", "apply_bgp_config", device_id, job_id, client, apply_fn)
    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.error["detail"]["orphans"] == {"peer": [["192.0.2.9"]]}
        break


async def test_bgp_removal_passes_with_removed_peer_threaded(adapter_client):
    device_id = await _seed_device(nso_device_name="sw-bgp-legit")
    service = {
        "device": "sw-bgp-legit",
        "router": [{"asn": "64500", "scope": [{"vrf": "", "peer": [{"peer-address": "192.0.2.9"}]}]}],
    }
    staged = {"device": "sw-bgp-legit", "router": [{"asn": "64500", "scope": [{"vrf": "", "peer": []}]}]}
    client = _guard_client(service)
    job_id = await _seed_removal_job(device_id, scope="bgp", context_extra={"removed": {"peer": ["192.0.2.9"]}})
    apply_fn = _staging_apply(staged)
    await _run_removal_with("bgp", "apply_bgp_config", device_id, job_id, client, apply_fn)
    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        break


async def test_static_route_removal_compound_key(adapter_client):
    """static routes are keyed (vrf, prefix, next-hop); the compound key must round-trip
    through the removed context (JSON arrays) and match the YANG entry fields."""
    device_id = await _seed_device(nso_device_name="sw-sr-guard")
    service = {
        "device": "sw-sr-guard",
        "route": [
            {"vrf": "", "prefix": "10.0.0.0/24", "next-hop": "192.0.2.1"},
            {"vrf": "", "prefix": "10.9.0.0/24", "next-hop": "192.0.2.9"},
        ],
    }
    staged = {"device": "sw-sr-guard", "route": [{"vrf": "", "prefix": "10.0.0.0/24", "next-hop": "192.0.2.1"}]}
    client = _guard_client(service)
    job_id = await _seed_removal_job(
        device_id, scope="static_route", context_extra={"removed": {"route": [["", "10.9.0.0/24", "192.0.2.9"]]}}
    )
    apply_fn = _staging_apply(staged)
    await _run_removal_with("static_route", "apply_static_routes", device_id, job_id, client, apply_fn)
    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        break

    # same setup WITHOUT the removed context → the dropped route is collateral
    job2 = await _seed_removal_job(device_id, scope="static_route")
    apply_fn2 = _staging_apply(staged)
    await _run_removal_with("static_route", "apply_static_routes", device_id, job2, client, apply_fn2)
    async for db in get_session():
        job = await db.get(Job, job2)
        assert job.status == JobStatus.failed
        assert job.error["detail"]["orphans"] == {"route": [["", "10.9.0.0/24", "192.0.2.9"]]}
        break


async def test_generic_force_skips_guard_and_service_get(adapter_client):
    """force=true (operator override) commits without even reading the service."""
    device_id = await _seed_device(nso_device_name="sw-vlan-force")
    client = _guard_client({"device": "sw-vlan-force", "vlan": [{"vlan-id": 99}]})
    job_id = await _seed_removal_job(device_id, scope="vlan", context_extra={"force": True})
    apply_fn = AsyncMock()
    await _run_removal_with("vlan", "apply_vlan_config", device_id, job_id, client, apply_fn)
    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        break
    apply_fn.assert_awaited_once()
    assert apply_fn.await_args.kwargs == {"replace": True}
    client.get_service_config.assert_not_awaited()


async def test_generic_no_service_instance_skips_guard(adapter_client):
    """No service instance in NSO (404 → None) → nothing to guard, plain replace."""
    device_id = await _seed_device(nso_device_name="sw-logging-fresh")
    client = _guard_client(None)
    job_id = await _seed_removal_job(device_id, scope="logging")
    apply_fn = AsyncMock()
    await _run_removal_with("logging", "apply_logging_config", device_id, job_id, client, apply_fn)
    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        break
    apply_fn.assert_awaited_once()
    assert apply_fn.await_args.kwargs == {"replace": True}


async def test_replace_on_removal_threads_removed_keys(adapter_client):
    """The shim maps each simple scope's removed store keys onto its YANG list."""
    device_id = await _seed_device(nso_device_name="sw-shim")
    async for db in get_session():
        device = await db.get(Device, device_id)
        ok = await replace_on_removal(db, device, [3366, 3377], VlanIntent)
        assert ok is True
        break
    async for db in get_session():
        job = (await db.execute(select(Job))).scalars().one()
        assert job.context == {"scope": "vlan", "removed": {"vlan": [3366, 3377]}, "detach": True}
        break


async def test_replace_on_removal_maps_route_policy_families(adapter_client):
    """route_policy removed keys are (family, name); the shim buckets them into the
    per-family YANG lists (community_list → community-list etc.)."""
    device_id = await _seed_device(nso_device_name="sw-shim-rp")
    async for db in get_session():
        device = await db.get(Device, device_id)
        ok = await replace_on_removal(
            db, device, [("community_list", "cnad-test"), ("route_map", "RM-IN")], RoutePolicyObjectIntent
        )
        assert ok is True
        break
    async for db in get_session():
        job = (await db.execute(select(Job))).scalars().one()
        assert job.context == {
            "scope": "route_policy",
            "removed": {"community-list": ["cnad-test"], "route-map": ["RM-IN"]},
            "detach": True,
        }
        break


async def test_enqueue_removal_serializes_removed_tuples(adapter_client):
    """Tuple keys (compound) become JSON-safe arrays in the job context."""
    device_id = await _seed_device(nso_device_name="sw-enq")
    async for db in get_session():
        job = await enqueue_removal(
            db, device_id, "static_route", removed={"route": [("", "10.0.0.0/24", "192.0.2.1")]}
        )
        await db.commit()
        assert job.context == {
            "scope": "static_route",
            "removed": {"route": [["", "10.0.0.0/24", "192.0.2.1"]]},
            "detach": True,
        }
        break


# ── #104-A: residue-after-retract detection + immediate follow-up sync ────────
#
# FASTMAP's reverse diff keeps service-created entries that picked up foreign leaves
# (sw03 Vlan987: a sync between apply and removal imported the device-rendered
# ``no ip address`` into the CDB entry), so a removal can report SUCCESS while its
# keys survive on the device. run_removal must re-read the scope's device-tree view
# (network-state-export reader = data-provider, computed at GET time) and surface
# survivors in the job result, then enqueue a sync so the reappeared rows show as
# unowned mirrors immediately.


class _ReaderClient:
    """Real-shape fake of the reader surface: canned network-state-export entries.

    Method names MUST be real NsoClient method names —
    test_residue_readers_resolve_on_the_real_client pins both this fake and the
    _RESIDUE_READERS mapping to the real surface. (#104-A shipped bfd/l2_sap
    pointing at nonexistent get_bfd/get_l2_service and the fake matched the typo,
    so every real bfd/l2 removal silently degraded to residue_check='error'.)
    """

    def __init__(self, *, raise_on_read=False, **entries):
        self._entries = entries  # short scope tag → canned reader entry
        self._raise = raise_on_read
        self.reads = 0

    def _read(self, tag):
        self.reads += 1
        if self._raise:
            raise RuntimeError("reader down")
        return self._entries.get(tag)

    async def get_svi(self, device_name):
        return self._read("svi")

    async def get_l2_services(self, device_name):
        return self._read("l2")

    async def get_bfd_config(self, device_name):
        return self._read("bfd")

    async def get_bgp_config(self, device_name):
        return self._read("bgp")

    async def get_isis_interfaces(self, device_name):
        return self._read("isis")

    async def get_ospf(self, device_name):
        return self._read("ospf")

    async def get_route_policy(self, device_name):
        return self._read("route_policy")

    async def get_snmp_config(self, device_name):
        return self._read("snmp")

    async def get_interface_ips(self, device_name):
        return self._read("interface_ips")


async def _run(job_id: int, device_id: int, client) -> None:
    from nso_adapter.core.removal import run_removal

    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=client),
        patch("nso_adapter.core.removal._dispatch_scope", new=AsyncMock()),
    ):
        await run_removal(job_id=job_id, device_id=device_id)


async def _job_after(job_id: int) -> Job:
    async for db in get_session():
        return await db.get(Job, job_id)
    raise RuntimeError("no session")


async def test_run_removal_reports_residue_when_removed_key_survives(adapter_client):
    """The sw03 Vlan987 case: removal succeeds but the device tree still has the key."""
    device_id = await _seed_device(nso_device_name="sw3")
    job_id = await _seed_removal_job(device_id, "svi", {"removed": {"interface": [["Vlan987"]]}})
    client = _ReaderClient(svi={"interface": [{"interface-name": "Vlan987", "vlan-id": 987}]})

    await _run(job_id, device_id, client)

    job = await _job_after(job_id)
    assert job.status == JobStatus.succeeded
    assert job.result["residue_check"] == "found"
    assert job.result["residue"] == {"interface": [["Vlan987"]]}


async def test_run_removal_residue_clean_when_key_gone(adapter_client):
    device_id = await _seed_device(nso_device_name="sw3")
    job_id = await _seed_removal_job(device_id, "svi", {"removed": {"interface": [["Vlan987"]]}})
    client = _ReaderClient(svi={"interface": [{"interface-name": "Vlan100", "vlan-id": 100}]})

    await _run(job_id, device_id, client)

    job = await _job_after(job_id)
    assert job.result["residue_check"] == "clean"
    assert "residue" not in job.result


async def test_run_removal_residue_handles_nested_l2_reader_shape(adapter_client):
    """The l2 reader nests sap[] under service[] — the compound key must still match."""
    device_id = await _seed_device(nso_device_name="ra1")
    job_id = await _seed_removal_job(device_id, "l2_sap", {"removed": {"sap": [["TL", "lag-60:3999"]]}})
    client = _ReaderClient(l2={"service": [{"service-name": "TL", "sap": [{"sap-id": "lag-60:3999"}]}]})

    await _run(job_id, device_id, client)

    job = await _job_after(job_id)
    assert job.result["residue_check"] == "found"
    assert job.result["residue"] == {"sap": [["TL", "lag-60:3999"]]}


async def test_run_removal_residue_unsupported_scope_is_transparent(adapter_client):
    """Jobs without captured removed values must say so — never silently claim clean.

    An interface_config job whose context has no ``removed`` values (a legacy queue
    row from before #104 phase-3, or an actions/force-removal with no trigger-computed
    values) cannot be value-checked, so the check reports unsupported, not clean.
    """
    device_id = await _seed_device(nso_device_name="sw3")
    job_id = await _seed_removal_job(device_id, "interface_config", {"interfaces": ["GigabitEthernet0/1"]})

    await _run(job_id, device_id, _ReaderClient())

    job = await _job_after(job_id)
    assert job.status == JobStatus.succeeded
    assert job.result["residue_check"] == "unsupported"


async def test_force_removal_residue_never_silently_clean(adapter_client):
    """The generic residue path must honour the same "never a silent clean" contract as
    _interface_config_residue — on the ONE path where a survivor matters most.

    actions/force-removal enqueues with no ``removed`` keys (nothing was trigger-deleted;
    the operator is deliberately flushing orphans). The generic check short-circuited to
    {} -> residue_check="clean" WITHOUT ever reading the device: a deliberate flush against
    a FASTMAP service that already surprised us once, reported verified when nothing was
    checked. It must report unsupported, and must not pretend to have read the device.
    """
    device_id = await _seed_device(nso_device_name="rg03")
    job_id = await _seed_removal_job(device_id, "bgp", {"force": True})
    client = _ReaderClient(bgp={"router": [{"asn": 65000}]})

    await _run(job_id, device_id, client)

    job = await _job_after(job_id)
    assert job.status == JobStatus.succeeded
    assert job.result["residue_check"] == "unsupported"  # NOT "clean"
    assert client.reads == 0  # the device was never read — so nothing was verified


async def test_cleared_scalar_retract_residue_is_unsupported(adapter_client):
    """A cleared-scalar retract removes no KEY, so there is nothing for the key-grain
    residue check to look for — say so rather than claim the device came back clean."""
    device_id = await _seed_device(nso_device_name="ra1")
    job_id = await _seed_removal_job(device_id, "isis", {})
    client = _ReaderClient(isis={"interface": [{"interface-name": "Gi0/0", "af": "ipv4"}]})

    await _run(job_id, device_id, client)

    job = await _job_after(job_id)
    assert job.result["residue_check"] == "unsupported"
    assert client.reads == 0


def test_uncomparable_grains_are_never_reader_compared():
    """A grain in UNCOMPARABLE_LISTS must not appear in _READER_COMPARE_SPECS.

    The two registries encode the same key-grain truth, and they had already diverged: the
    compare demanded a community be "present" under its intent LABEL while the export names
    it by a SHA-256 of the secret, so every successful SNMP apply was failed. Pin them
    together so re-adding an un-keyable grain to either side fails here, loudly, rather than
    on a live device.
    """
    from nso_adapter.core.apply import _READER_COMPARE_SPECS
    from nso_adapter.core.removal import UNCOMPARABLE_LISTS

    compared = {(scope, label) for scope, specs in _READER_COMPARE_SPECS.items() for _, label, _ in specs}
    assert not (compared & UNCOMPARABLE_LISTS), (
        f"these grains cannot be key-matched against the export: {sorted(compared & UNCOMPARABLE_LISTS)}"
    )


def test_residue_readers_resolve_on_the_real_client():
    """Every _RESIDUE_READERS target must be a real NsoClient coroutine — and so must
    every get_* method on the test fake. #104-A shipped bfd→get_bfd and
    l2_sap→get_l2_service (neither exists); the fake carried the same typo, so the
    suite stayed green while real removals degraded to residue_check='error'."""
    import inspect

    from nso_adapter.core.removal import _RESIDUE_READERS
    from nso_adapter.nso.client import NsoClient

    for scope, name in _RESIDUE_READERS.items():
        fn = getattr(NsoClient, name, None)
        assert fn is not None and inspect.iscoroutinefunction(fn), f"{scope} → {name} is not a real NsoClient reader"
    for name in dir(_ReaderClient):
        if name.startswith("get_"):
            assert hasattr(NsoClient, name), f"_ReaderClient.{name} drifted off the real NsoClient surface"


async def test_run_removal_residue_bgp_router_and_peer(adapter_client):
    """#104 phase-2: bgp residue matches the guard grain — router by asn, peers
    flattened across router→scope→peer (the reader nests identically)."""
    device_id = await _seed_device(nso_device_name="rg3")
    job_id = await _seed_removal_job(
        device_id, "bgp", {"removed": {"router": [["65100"]], "peer": [["10.0.0.7"], ["10.0.0.9"]]}}
    )
    client = _ReaderClient(
        bgp={
            "router": [
                {
                    "asn": 65100,  # ints in NSO JSON — must still match the string trigger key
                    "scope": [{"vrf": "GRT", "peer": [{"peer-address": "10.0.0.7"}]}],
                }
            ]
        }
    )

    await _run(job_id, device_id, client)

    job = await _job_after(job_id)
    assert job.result["residue_check"] == "found"
    assert job.result["residue"] == {"router": [["65100"]], "peer": [["10.0.0.7"]]}


async def test_run_removal_residue_isis_renamed_reader_lists(adapter_client):
    """#104 phase-2: the isis export lists are named interface/process while the
    service (and trigger) say interface-config/process-config — the translation
    must land on the right lists, including the compound (interface-name, af) key."""
    device_id = await _seed_device(nso_device_name="ra1")
    job_id = await _seed_removal_job(
        device_id,
        "isis",
        {"removed": {"interface-config": [["ge-0/0/0", "ipv4"], ["ge-0/0/1", "ipv4"]], "process-config": [["CORE"]]}},
    )
    client = _ReaderClient(
        isis={
            "process": [{"process-tag": "CORE"}],
            "interface": [{"interface-name": "ge-0/0/0", "af": "ipv4"}],
        }
    )

    await _run(job_id, device_id, client)

    job = await _job_after(job_id)
    assert job.result["residue_check"] == "found"
    assert job.result["residue"] == {"interface-config": [["ge-0/0/0", "ipv4"]], "process-config": [["CORE"]]}


async def test_run_removal_residue_ospf_instance_rename_and_clean(adapter_client):
    """#104 phase-2: ospf process-config maps onto the export's `instance` list;
    a device tree with none of the removed keys reports clean."""
    device_id = await _seed_device(nso_device_name="rg3")
    job_id = await _seed_removal_job(
        device_id, "ospf", {"removed": {"interface-config": [["GigabitEthernet0/1"]], "process-config": [["1"]]}}
    )
    clean_client = _ReaderClient(ospf={"instance": [{"process-id": 2}], "interface": []})

    await _run(job_id, device_id, clean_client)

    job = await _job_after(job_id)
    assert job.result["residue_check"] == "clean"
    assert "residue" not in job.result


async def test_run_removal_residue_ospf_found_via_instance_list(adapter_client):
    device_id = await _seed_device(nso_device_name="rg3")
    job_id = await _seed_removal_job(device_id, "ospf", {"removed": {"process-config": [["1"]]}})
    client = _ReaderClient(ospf={"instance": [{"process-id": 1}]})

    await _run(job_id, device_id, client)

    job = await _job_after(job_id)
    assert job.result["residue_check"] == "found"
    assert job.result["residue"] == {"process-config": [["1"]]}


def _community_export_name(secret: str) -> str:
    """The export's community key: sha256 of the community STRING, first 16 hex chars.

    network-state-export.yang:597 — "Stable opaque identifier (first 16 hex chars of
    SHA-256 of the community string)". The intent's key is the human-readable LABEL, and
    the adapter never sees the secret (it pushes a Vault triple; NSO resolves it), so the
    two namespaces can never be intersected.
    """
    import hashlib

    return hashlib.sha256(secret.encode()).hexdigest()[:16]


async def test_run_removal_residue_snmp_three_lists(adapter_client):
    """#104 phase-2: snmp residue checks v3-user/host by key; community CANNOT be keyed.

    The old fake keyed the exported community by the intent LABEL ("public"), which the
    real export never emits — so the suite stayed green while the check was structurally
    incapable of matching. Address-keyed hosts and username-keyed v3-users are correct.
    """
    device_id = await _seed_device(nso_device_name="sw3")
    job_id = await _seed_removal_job(
        device_id,
        "snmp",
        {"removed": {"v3-user": [["nms"]], "host": [["198.18.5.9"]]}},
    )
    client = _ReaderClient(
        snmp={
            "community": [{"name": _community_export_name("public")}],
            "v3-user": [{"username": "nms"}],
            "host": [{"address": "198.18.5.10"}],
        }
    )

    await _run(job_id, device_id, client)

    job = await _job_after(job_id)
    assert job.result["residue_check"] == "found"
    assert job.result["residue"] == {"v3-user": [["nms"]]}  # host 198.18.5.9 really is gone


async def test_run_removal_residue_snmp_community_is_never_falsely_clean(adapter_client):
    """A community that SURVIVES the retract must never be reported clean.

    The check intersected the removed LABELS with the export's SHA-256 fingerprints — empty
    by construction — so it reported "clean" while the credential was still live on the
    router: a silent false-clean on a security-relevant removal, on the very check built to
    surface it. The grain is unverifiable, so say so.
    """
    device_id = await _seed_device(nso_device_name="sw3")
    job_id = await _seed_removal_job(device_id, "snmp", {"removed": {"community": [["ro-community"]]}})
    # The community is STILL on the device — the export shows its hashed identity.
    client = _ReaderClient(snmp={"community": [{"name": _community_export_name("s3cr3t")}], "v3-user": [], "host": []})

    await _run(job_id, device_id, client)

    job = await _job_after(job_id)
    assert job.result["residue_check"] != "clean"
    assert job.result["residue_check"] == "unsupported"  # nothing checkable was checked
    assert job.result["residue_unverifiable"] == ["community"]


async def test_run_removal_residue_snmp_mixed_reports_partial(adapter_client):
    """A checkable list came back clean but the community could not be checked at all —
    that is not a clean bill of health for the removal."""
    device_id = await _seed_device(nso_device_name="sw3")
    job_id = await _seed_removal_job(
        device_id, "snmp", {"removed": {"community": [["ro-community"]], "host": [["198.18.5.9"]]}}
    )
    client = _ReaderClient(snmp={"community": [], "v3-user": [], "host": [{"address": "198.18.5.10"}]})

    await _run(job_id, device_id, client)

    job = await _job_after(job_id)
    assert job.result["residue_check"] == "partial"
    assert job.result["residue_unverifiable"] == ["community"]


async def test_run_removal_residue_route_policy_per_family_lists(adapter_client):
    """#104 phase-2: route-policy residue matches per family list (route-map vs
    prefix-list etc.), same bucketing the trigger's _removed_map produced."""
    device_id = await _seed_device(nso_device_name="rg3")
    job_id = await _seed_removal_job(
        device_id, "route_policy", {"removed": {"route-map": [["RM-X"]], "prefix-list": [["PL-Y"]]}}
    )
    client = _ReaderClient(
        route_policy={
            "route-map": [{"name": "RM-X", "entry": [{"sequence": 10}]}],
            "prefix-list": [],
        }
    )

    await _run(job_id, device_id, client)

    job = await _job_after(job_id)
    assert job.result["residue_check"] == "found"
    assert job.result["residue"] == {"route-map": [["RM-X"]]}


async def test_run_removal_residue_bfd_uses_real_reader_name(adapter_client):
    """#104-A regression: the bfd mapping pointed at nonexistent get_bfd, so every
    bfd residue check errored. With the real get_bfd_config it must work."""
    device_id = await _seed_device(nso_device_name="sw3")
    job_id = await _seed_removal_job(device_id, "bfd", {"removed": {"interface": [["ge-0/0/2"]]}})
    client = _ReaderClient(bfd={"interface": [{"interface-name": "ge-0/0/2"}]})

    await _run(job_id, device_id, client)

    job = await _job_after(job_id)
    assert job.result["residue_check"] == "found"
    assert job.result["residue"] == {"interface": [["ge-0/0/2"]]}


# ── #104 phase-3: interface_config residue at VALUE grain ─────────────────────
#
# interface_config removal is per-instance (PUT-replace/DELETE of one
# (device, interface) service entry) retracting address/attribute VALUES, not
# keyed rows — so its residue check intersects the trigger's just-removed
# (interface, address, vrf) triples with the interface-ip export view.


async def test_run_removal_residue_interface_config_value_survives(adapter_client):
    """A delete-origin IP retraction whose address SURVIVES on the device is reported.

    Whether it survived as a kept-adopted leaf or a husk entry, the operator
    deleted the IP in NetBox and would otherwise believe it left the device —
    transparency over a silent 'clean' (intent-integrity principle).
    """
    device_id = await _seed_device(nso_device_name="rm-ic-1")
    job_id = await _seed_removal_job(
        device_id,
        "interface_config",
        {"interfaces": ["Gi0/3"], "removed": {"address": [["Gi0/3", "10.0.0.2/24", ""]]}},
    )
    client = _ReaderClient(
        interface_ips={
            "interface": [
                {
                    "interface-name": "Gi0/3",
                    "address": [
                        {"address": "10.0.0.1/24", "vrf": "", "family": "ipv4"},
                        # explicit null vrf, as the export can emit — must compare as ""
                        {"address": "10.0.0.2/24", "vrf": None, "family": "ipv4"},
                    ],
                }
            ]
        }
    )

    await _run(job_id, device_id, client)

    job = await _job_after(job_id)
    assert job.status == JobStatus.succeeded
    assert job.result["residue_check"] == "found"
    assert job.result["residue"] == {"address": [["Gi0/3", "10.0.0.2/24", ""]]}


async def test_run_removal_residue_interface_config_value_gone_is_clean(adapter_client):
    """The removed address absent from ITS interface is clean — a same-address
    entry on a different interface must not count as residue."""
    device_id = await _seed_device(nso_device_name="rm-ic-2")
    job_id = await _seed_removal_job(
        device_id,
        "interface_config",
        {"interfaces": ["Gi0/3"], "removed": {"address": [["Gi0/3", "10.0.0.2/24", ""]]}},
    )
    client = _ReaderClient(
        interface_ips={
            "interface": [
                {"interface-name": "Gi0/3", "address": [{"address": "10.0.0.1/24", "vrf": ""}]},
                {"interface-name": "Gi0/4", "address": [{"address": "10.0.0.2/24", "vrf": ""}]},
            ]
        }
    )

    await _run(job_id, device_id, client)

    job = await _job_after(job_id)
    assert job.result["residue_check"] == "clean"
    assert "residue" not in job.result


async def test_run_removal_residue_interface_config_normalizes_ipv6(adapter_client):
    """Intent form and export form of one address may differ textually (IPv6
    case/zero-compression) — compare at ip_interface grain, report the trigger's
    original form."""
    device_id = await _seed_device(nso_device_name="rm-ic-3")
    job_id = await _seed_removal_job(
        device_id,
        "interface_config",
        {"interfaces": ["ge-0/0/1"], "removed": {"address": [["ge-0/0/1", "2001:DB8:0:0::1/64", "CUST"]]}},
    )
    client = _ReaderClient(
        interface_ips={
            "interface": [
                {
                    "interface-name": "ge-0/0/1",
                    "address": [{"address": "2001:db8::1/64", "vrf": "CUST", "family": "ipv6"}],
                }
            ]
        }
    )

    await _run(job_id, device_id, client)

    job = await _job_after(job_id)
    assert job.result["residue_check"] == "found"
    assert job.result["residue"] == {"address": [["ge-0/0/1", "2001:DB8:0:0::1/64", "CUST"]]}


async def test_run_removal_residue_reader_error_is_nonfatal(adapter_client):
    device_id = await _seed_device(nso_device_name="sw3")
    job_id = await _seed_removal_job(device_id, "svi", {"removed": {"interface": [["Vlan987"]]}})

    await _run(job_id, device_id, _ReaderClient(raise_on_read=True))

    job = await _job_after(job_id)
    assert job.status == JobStatus.succeeded
    assert job.result["residue_check"] == "error"


async def test_run_removal_without_removed_context_skips_reader(adapter_client):
    """With no captured keys the reader is never called — so the result must be
    "unsupported", not "clean". Asserting reads == 0 alongside "clean" was asserting the
    bug: a verdict of "the device came back clean" on a device that was never read."""
    device_id = await _seed_device(nso_device_name="sw3")
    job_id = await _seed_removal_job(device_id, "svi")
    client = _ReaderClient(svi={"interface": [{"interface-name": "Vlan987"}]})

    await _run(job_id, device_id, client)

    job = await _job_after(job_id)
    assert job.result["residue_check"] == "unsupported"
    assert client.reads == 0


async def test_run_removal_enqueues_followup_sync(adapter_client):
    """Option A: a successful removal enqueues an immediate sync so reappeared rows
    surface as unowned mirrors right away, not at the next poll cycle."""
    device_id = await _seed_device(nso_device_name="sw3")
    job_id = await _seed_removal_job(device_id, "svi", {"removed": {"interface": [["Vlan987"]]}})

    await _run(job_id, device_id, _ReaderClient(svi={"interface": []}))

    async for db in get_session():
        sync_jobs = (
            (
                await db.execute(
                    select(Job).where(
                        Job.device_id == device_id,
                        Job.job_type == JobType.sync,
                        Job.status == JobStatus.queued,
                    )
                )
            )
            .scalars()
            .all()
        )
        assert len(sync_jobs) == 1
        break


async def test_run_removal_failure_skips_residue_and_sync(adapter_client):
    """A failed removal changed nothing on the device — no residue claim, no sync."""
    from nso_adapter.core.removal import run_removal

    device_id = await _seed_device(nso_device_name="sw3")
    job_id = await _seed_removal_job(device_id, "svi", {"removed": {"interface": [["Vlan987"]]}})

    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=_ReaderClient()),
        patch("nso_adapter.core.removal._dispatch_scope", new=AsyncMock(side_effect=RuntimeError("boom"))),
    ):
        await run_removal(job_id=job_id, device_id=device_id)

    job = await _job_after(job_id)
    assert job.status == JobStatus.failed
    assert not (job.result or {}).get("residue_check")
    async for db in get_session():
        sync_jobs = (
            (await db.execute(select(Job).where(Job.device_id == device_id, Job.job_type == JobType.sync)))
            .scalars()
            .all()
        )
        assert sync_jobs == []
        break


# ── #106: un-own = detach (drop governance, never touch the device) ───────────
#
# A PUT-replace removal of an ADOPTED entry plays FASTMAP's reverse diff against
# the live device (rg03: the adopted redistribute's route-map filter would have
# been stripped). Only a NetBox object DELETION may retract for real — the plugin
# marks those pushes ?delete_origin=true; every unmarked shrink (an un-own) runs
# the replace with no-networking + sync-from so the device keeps the config as
# unowned brownfield.


async def test_enqueue_removal_unmarked_shrink_defaults_to_detach(adapter_client):
    device_id = await _seed_device(nso_device_name="sw-detach")
    async for db in get_session():
        job = await enqueue_removal(db, device_id, "svi", removed={"interface": [["Vlan9"]]})
        await db.commit()
        assert job.context["detach"] is True
        break


async def test_enqueue_removal_delete_origin_is_real_retraction(adapter_client):
    from nso_adapter.core.request_flags import DELETE_ORIGIN

    device_id = await _seed_device(nso_device_name="sw-detach")
    token = DELETE_ORIGIN.set(True)
    try:
        async for db in get_session():
            job = await enqueue_removal(db, device_id, "svi", removed={"interface": [["Vlan9"]]})
            await db.commit()
            assert "detach" not in (job.context or {})
            break
    finally:
        DELETE_ORIGIN.reset(token)


async def test_enqueue_removal_force_is_real_retraction(adapter_client):
    device_id = await _seed_device(nso_device_name="sw-detach")
    async for db in get_session():
        job = await enqueue_removal(db, device_id, "svi", removed={"interface": [["Vlan9"]]}, force=True)
        await db.commit()
        assert job.context.get("force") is True
        assert "detach" not in job.context
        break


async def test_run_removal_detach_syncs_from_and_skips_residue(adapter_client):
    """Detach: device untouched → the removed keys are EXPECTED on the device (they
    must not be reported as residue), and CDB must be re-aligned via sync-from."""
    device_id = await _seed_device(nso_device_name="sw3")
    job_id = await _seed_removal_job(device_id, "svi", {"removed": {"interface": [["Vlan987"]]}, "detach": True})
    client = _ReaderClient(svi={"interface": [{"interface-name": "Vlan987"}]})
    sync_from = AsyncMock(return_value={"result": True})

    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=client),
        patch("nso_adapter.core.removal._dispatch_scope", new=AsyncMock()),
        patch("nso_adapter.nso.actions.sync_from", new=sync_from),
    ):
        from nso_adapter.core.removal import run_removal

        await run_removal(job_id=job_id, device_id=device_id)

    job = await _job_after(job_id)
    assert job.status == JobStatus.succeeded
    assert job.result["detach"] is True
    assert job.result["residue_check"] == "skipped_detach"
    assert "residue" not in job.result
    assert client.reads == 0
    sync_from.assert_awaited_once()
    async for db in get_session():
        sync_jobs = (
            (
                await db.execute(
                    select(Job).where(
                        Job.device_id == device_id,
                        Job.job_type == JobType.sync,
                        Job.status == JobStatus.queued,
                    )
                )
            )
            .scalars()
            .all()
        )
        assert len(sync_jobs) == 1  # the follow-up sync still refreshes the mirrors
        break


async def test_run_removal_real_retraction_does_not_sync_from(adapter_client):
    device_id = await _seed_device(nso_device_name="sw3")
    job_id = await _seed_removal_job(device_id, "svi", {"removed": {"interface": [["Vlan987"]]}})
    sync_from = AsyncMock(return_value={"result": True})

    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=_ReaderClient(svi={"interface": []})),
        patch("nso_adapter.core.removal._dispatch_scope", new=AsyncMock()),
        patch("nso_adapter.nso.actions.sync_from", new=sync_from),
    ):
        from nso_adapter.core.removal import run_removal

        await run_removal(job_id=job_id, device_id=device_id)

    job = await _job_after(job_id)
    assert job.result["residue_check"] == "clean"
    sync_from.assert_not_awaited()


async def test_guarded_apply_detach_skips_collateral_guard(adapter_client):
    """Detach drops governance of the whole instance without device writes, so the
    orphan guard (which protects device config from a real PUT-replace flush) must
    stand down — otherwise every un-own on an instance with un-adopted siblings
    blocks forever (the rg03 static sibling condition)."""
    device_id = await _seed_device(nso_device_name="sw-detach")
    async for db in get_session():
        device = await db.get(Device, device_id)
        break
    client = _guard_client(service_config={"vlan": [{"vlan-id": 100}, {"vlan-id": 200}]})
    apply_fn = _staging_apply({"vlan": [{"vlan-id": 100}]})  # 200 would be an orphan

    await removal_mod._guarded_apply(client, device, "vlan", {"detach": True}, apply_fn)

    # No RemovalBlockedError raised; the replace ran exactly once, with no dry-run block.
    replace_calls = [c for c in apply_fn.await_args_list if c.kwargs.get("replace") and not c.kwargs.get("dry_run")]
    assert len(replace_calls) == 1


async def test_send_service_config_detach_replace_adds_no_networking(adapter_client):
    """The detach replace must commit with no-networking so nothing reaches the device."""
    import httpx

    from nso_adapter.nso import apply as nso_apply
    from nso_adapter.nso.client import NsoClient

    recorded: list[str] = []

    class _Transport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request):
            recorded.append(str(request.url))
            return httpx.Response(204)

    class _Client(NsoClient):
        def __init__(self):
            pass

        _base = "http://nso-test"
        _action_timeout = 5

        def _client(self, timeout=None):
            return httpx.AsyncClient(transport=_Transport())

    token = nso_apply.DETACH_REPLACE.set(True)
    try:
        await nso_apply._send_service_config(
            _Client(),
            "/restconf/data/vlan-reconciler:vlan-config",
            "vlan-reconciler:vlan-config",
            "sw-detach",
            {"device": "sw-detach", "vlan": []},
            scope="vlan",
            replace=True,
        )
    finally:
        nso_apply.DETACH_REPLACE.reset(token)

    commit_url = recorded[0]
    assert "no-networking" in commit_url


async def test_delete_interface_config_detach_adds_no_networking(adapter_client):
    """interface_config removal can DELETE the whole instance — under detach that
    DELETE must also commit with no-networking (FASTMAP would otherwise revert
    everything the service created ON THE DEVICE)."""
    import httpx

    from nso_adapter.nso import apply as nso_apply
    from nso_adapter.nso.client import NsoClient

    recorded: list[str] = []

    class _Transport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request):
            recorded.append(str(request.url))
            return httpx.Response(204)

    class _Client(NsoClient):
        def __init__(self):
            pass

        _base = "http://nso-test"
        _action_timeout = 5

        def _client(self, timeout=None):
            return httpx.AsyncClient(transport=_Transport())

    token = nso_apply.DETACH_REPLACE.set(True)
    try:
        await nso_apply.delete_interface_config(_Client(), "sw-detach", "Gi0/1")
    finally:
        nso_apply.DETACH_REPLACE.reset(token)

    assert "no-networking" in recorded[0]


# ── is_cleared / lost_content: the two "a merge-PATCH cannot express this" predicates ──


@pytest.mark.parametrize(
    ("before", "after", "expected"),
    [
        ("RM-IN", None, True),  # nullable column blanked
        ("warning", "", True),  # NOT NULL column with default="" blanked
        (10, None, True),
        (None, "RM-IN", False),  # a grow is not a clear
        ("", "RM-IN", False),
        ("RM-IN", "RM-OUT", False),  # a value CHANGE merges fine
        (True, False, False),  # a toggle-off is emitted explicitly — not a clear
        (False, True, False),
        (0, None, True),  # 0 is a value, and omitting it would leave the old one
    ],
)
def test_is_cleared(before, after, expected):
    from nso_adapter.core.removal import is_cleared

    assert is_cleared(before, after) is expected


@pytest.mark.parametrize(
    ("before", "after", "expected"),
    [
        # keyed list entries: a dropped term is not deletable by a merge
        ([{"sequence": 10}, {"sequence": 20}], [{"sequence": 10}], True),
        ([{"sequence": 10}], [{"sequence": 10}, {"sequence": 20}], False),  # grow
        ([{"sequence": 10}], [{"sequence": 10}], False),  # republish
        # a leaf-list MERGES, so losing (or swapping) a member needs a replace
        ({"match": ["A", "B"]}, {"match": ["A"]}, True),
        ({"match": ["A"]}, {"match": ["A", "B"]}, False),
        ({"match": ["A"]}, {"match": ["B"]}, True),
        # a blanked leaf inside a surviving entry
        ([{"sequence": 10, "set": {"med": 5}}], [{"sequence": 10}], True),
        # a rewritten blob is safe: the merge overwrites the leaf, so FASTMAP reverts
        # whatever the OLD value created
        ([{"sequence": 10, "set": {"med": 5}}], [{"sequence": 10, "set": {"med": 9}}], False),
        # ...but a blob that loses a key inside it is a real shrink
        ([{"sequence": 10, "set": {"med": 5, "lp": 1}}], [{"sequence": 10, "set": {"med": 5}}], True),
        (None, [{"sequence": 10}], False),  # nothing existed before
        ([], [{"sequence": 10}], False),
    ],
)
def test_lost_content(before, after, expected):
    from nso_adapter.core.removal import lost_content

    assert lost_content(before, after) is expected
