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
        assert job.context == {"scope": "vlan", "removed": {"vlan": [3366]}}
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
        assert job.context == {"scope": "vlan", "removed": {"vlan": [3366, 3377]}}
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
        assert job.context == {"scope": "static_route", "removed": {"route": [["", "10.0.0.0/24", "192.0.2.1"]]}}
        break
