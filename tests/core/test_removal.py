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

import inspect
from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select

from nso_adapter.core import removal as removal_mod
from nso_adapter.core.removal import enqueue_removal, replace_on_removal
from nso_adapter.store.device_settle import create_counter
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
from tests.conftest import SNMP_COMMUNITY as _COMMUNITY
from tests.conftest import SNMP_VAULT_REF as _REF
from tests.conftest import note_projection_write, session

_NOW = datetime.now(UTC)

# An opaque NSO-client token: removal threads it straight to the apply boundary
# (which these tests stub), so it is never dereferenced here — a plain sentinel,
# not a mock, makes that pass-through explicit.
_CLIENT = object()


async def _seed_device(*, nso_device_name: str = "sw3", netbox_device_id: int = 42) -> int:
    async with session() as db:
        d = Device(nso_instance="nso-dev", nso_device_name=nso_device_name, netbox_device_id=netbox_device_id)
        db.add(d)
        await db.flush()
        await create_counter(db, d.id)
        await db.commit()
        await db.refresh(d)
        return d.id


async def _seed_removal_job(device_id: int, scope: str = "vlan", context_extra: dict | None = None) -> int:
    """Seed a started removal job WITH the generation it deploys, as enqueue_removal does.

    A removal job without a generation is not a state production can reach (#1522 §G1): the
    body a PUT-replace asserts comes from the generation's document for a document-executed
    scope, and a job with none has nothing to deploy.
    """
    from nso_adapter.core.generation import (
        attach_to_job,
        create_generation,
        create_reissue_generation,
        note_write,
    )
    from nso_adapter.core.projection import section_streams
    from nso_adapter.store.models import GenerationMode

    context = {"scope": scope, **(context_extra or {})}
    stream = section_streams(scope)[0]
    mode = GenerationMode.detach if context.get("detach") else GenerationMode.networked
    async with session() as db:
        if context.get("force"):
            # A force removal PROMOTES NOTHING, so it takes the reissue branch of
            # enqueue_removal: no accepted write stands behind it (hence no note_write),
            # stream_revisions stays empty, and it carries no allowed_removal_keys.
            generation = await create_reissue_generation(db, device_id, mode=mode, removal_context=context)
        else:
            await note_write(db, device_id, stream)
            generation = await create_generation(
                db,
                device_id,
                streams=(stream,),
                mode=mode,
                allowed_removal_keys=context.get("removed") or {},
                removal_context=context,
            )
        # Started, at attempt 1: run_removal is invoked directly, so nothing else performs
        # the worker head's queued -> running transition its terminal CAS expects.
        j = Job(
            job_type=JobType.removal,
            device_id=device_id,
            status=JobStatus.running,
            run_attempt=1,
            context=context,
        )
        db.add(j)
        await db.flush()
        await attach_to_job(db, generation, j)
        await db.commit()
        await db.refresh(j)
        return j.id


# ── replace_on_removal (back-compat shim) ─────────────────────────────────────


async def test_replace_on_removal_noop_when_nothing_removed(adapter_client):
    """No removals → no job enqueued, returns False."""
    device_id = await _seed_device()
    async with session() as db:
        device = await db.get(Device, device_id)
        result = await replace_on_removal(db, device, [], VlanIntent)
        assert result is False
        assert (await db.execute(select(Job))).scalars().all() == []


async def test_replace_on_removal_enqueues_job_and_commits(adapter_client):
    """On removal, a `removal` job for the model's scope is enqueued + committed."""
    device_id = await _seed_device()
    async with session() as db:
        device = await db.get(Device, device_id)
        await note_projection_write(db, device_id, "vlan")
        ok = await replace_on_removal(db, device, [3366], VlanIntent)
        assert ok is True
        await db.commit()  # the shim no longer commits: the caller owns the boundary

    # Re-read in a fresh session to prove it was committed, not merely flushed.
    async with session() as db:
        jobs = (await db.execute(select(Job))).scalars().all()
        assert len(jobs) == 1
        job = jobs[0]
        assert job.job_type == JobType.removal
        assert job.device_id == device_id
        assert job.context == {"scope": "vlan", "removed": {"vlan": [3366]}, "detach": True}
        assert job.status == JobStatus.queued


async def test_replace_on_removal_unknown_model_returns_false(adapter_client):
    """An unmapped store model never enqueues (and never crashes the request)."""
    device_id = await _seed_device()

    class _Unmapped:
        pass

    async with session() as db:
        device = await db.get(Device, device_id)
        ok = await replace_on_removal(db, device, [1], _Unmapped)
        assert ok is False
        assert (await db.execute(select(Job))).scalars().all() == []


# ── enqueue_removal ───────────────────────────────────────────────────────────


def test_enqueue_removal_requires_a_promotion_disposition():
    parameter = inspect.signature(enqueue_removal).parameters["promotes"]

    assert parameter.default is inspect.Parameter.empty


async def test_enqueue_removal_rejects_unknown_scope(adapter_client):
    async with session() as db:
        with pytest.raises(ValueError, match="Unknown removal scope"):
            await enqueue_removal(db, 1, "bogus", marking="detach", defer_retract=False, promotes=("vlan",))


async def test_enqueue_removal_creates_job_for_each_valid_scope(adapter_client):
    """Every reconciler scope (incl ospf/bgp) maps to a removal job."""
    from nso_adapter.core.projection import section_streams
    from nso_adapter.core.removal import VALID_REMOVAL_SCOPES

    device_id = await _seed_device()
    for scope in VALID_REMOVAL_SCOPES:
        async with session() as db:
            stream = section_streams(scope)[0]
            await note_projection_write(db, device_id, stream)
            job = await enqueue_removal(
                db,
                device_id,
                scope,
                marking="detach",
                defer_retract=False,
                promotes=(stream,),
            )
            await db.commit()
            assert job.job_type == JobType.removal
            assert job.context == {"scope": scope, "detach": True}

    # Every scope produced a real persisted removal job.
    async with session() as db:
        scopes = {j.context["scope"] for j in (await db.execute(select(Job))).scalars().all()}
        assert scopes == VALID_REMOVAL_SCOPES


# ── _dispatch_scope ───────────────────────────────────────────────────────────


async def test_dispatch_scope_simple_calls_apply_replace_true(adapter_client):
    """A simple scope fetches ONLY accepted rows and calls its apply with replace=True."""
    device_id = await _seed_device(nso_device_name="sw3")
    async with session() as db:
        db.add(VlanIntent(device_id=device_id, vlan_id=10, accepted_at=_NOW))
        db.add(VlanIntent(device_id=device_id, vlan_id=20, accepted_at=None))  # not accepted → excluded
        await db.commit()

    apply_fn = AsyncMock()
    client = _guard_client(None)  # no service instance in NSO → collateral guard no-ops
    async with session() as db:
        device = await db.get(Device, device_id)
        with patch("nso_adapter.nso.apply.apply_vlan_config", apply_fn):
            await removal_mod._dispatch_scope(db, device, client, "vlan")

    apply_fn.assert_awaited_once()
    args, kwargs = apply_fn.await_args
    assert args[0] is client
    assert args[1] == "sw3"
    assert [r.vlan_id for r in args[2]] == [10]  # the accepted_at filter dropped vlan 20
    assert kwargs == {"replace": True}


async def test_dispatch_scope_logging_carries_accepted_levels(adapter_client):
    """The logging PUT-replace must re-assert the ACCEPTED local-levels intent alongside
    the remaining hosts — otherwise any host removal would FASTMAP-retract the owned
    severities (on NX that DISABLES the destination, not a benign revert)."""
    from nso_adapter.store.models import LoggingHostIntent, LoggingLevelsIntent

    device_id = await _seed_device(nso_device_name="nx-t11")
    async with session() as db:
        db.add(LoggingHostIntent(device_id=device_id, address="10.9.2.1", accepted_at=_NOW))
        db.add(LoggingLevelsIntent(device_id=device_id, console_severity="CRITICAL", accepted_at=_NOW))
        await db.commit()

    apply_fn = AsyncMock()
    client = _guard_client(None)  # no service instance in NSO → collateral guard no-ops
    async with session() as db:
        device = await db.get(Device, device_id)
        with patch("nso_adapter.nso.apply.apply_logging_config", apply_fn):
            await removal_mod._dispatch_scope(db, device, client, "logging")

    apply_fn.assert_awaited_once()
    args, kwargs = apply_fn.await_args
    assert [r.address for r in args[2]] == ["10.9.2.1"]
    assert kwargs["replace"] is True
    assert kwargs["levels_intent_row"] is not None
    assert kwargs["levels_intent_row"].console_severity == "CRITICAL"


async def test_dispatch_scope_logging_gate_off_refuses_not_retracts(adapter_client, monkeypatch):
    """Gate OFF + owned levels: the REAL builder refuses the replace instead of
    committing a levels-less body that would FASTMAP-retract the owned severities
    (NX destination disable). The removal job then fails honestly (removal_failed)."""
    from nso_adapter.nso.apply import NsoApplyError
    from nso_adapter.store.models import LoggingHostIntent, LoggingLevelsIntent

    monkeypatch.delenv("NSO_ADAPTER_LOGGING_LOCAL_LEVELS_WRITE", raising=False)
    device_id = await _seed_device(nso_device_name="nx-t13", netbox_device_id=44)
    async with session() as db:
        db.add(LoggingHostIntent(device_id=device_id, address="10.9.2.3", accepted_at=_NOW))
        db.add(LoggingLevelsIntent(device_id=device_id, console_severity="CRITICAL", accepted_at=_NOW))
        await db.commit()

    client = _guard_client(None)  # no service instance → guard no-ops, plain replace
    async with session() as db:
        device = await db.get(Device, device_id)
        with pytest.raises(NsoApplyError, match="NSO_ADAPTER_LOGGING_LOCAL_LEVELS_WRITE"):
            await removal_mod._dispatch_scope(db, device, client, "logging")


async def test_dispatch_scope_logging_excludes_unaccepted_levels(adapter_client):
    """A not-yet-accepted levels intent must never ride a PUT-replace (un-reviewed config)."""
    from nso_adapter.store.models import LoggingHostIntent, LoggingLevelsIntent

    device_id = await _seed_device(nso_device_name="nx-t12", netbox_device_id=43)
    async with session() as db:
        db.add(LoggingHostIntent(device_id=device_id, address="10.9.2.2", accepted_at=_NOW))
        db.add(LoggingLevelsIntent(device_id=device_id, console_severity="ERROR", accepted_at=None))
        await db.commit()

    apply_fn = AsyncMock()
    client = _guard_client(None)
    async with session() as db:
        device = await db.get(Device, device_id)
        with patch("nso_adapter.nso.apply.apply_logging_config", apply_fn):
            await removal_mod._dispatch_scope(db, device, client, "logging")

    apply_fn.assert_awaited_once()
    assert apply_fn.await_args.kwargs["levels_intent_row"] is None


async def test_dispatch_scope_ospf_uses_multi_row_apply(adapter_client):
    """OSPF dispatch fetches ONLY accepted instances+interfaces+redist(ospf only), replace=True.

    A PUT-replace re-asserts the full desired state, so it must never include
    not-yet-accepted (imported/staged) rows — that would deploy un-reviewed config.
    """
    device_id = await _seed_device(nso_device_name="ra1")
    async with session() as db:
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

    apply_fn = AsyncMock()
    client = _guard_client(None)  # no service instance in NSO → collateral guard no-ops
    async with session() as db:
        device = await db.get(Device, device_id)
        with patch("nso_adapter.nso.apply.apply_ospf_config", apply_fn):
            await removal_mod._dispatch_scope(db, device, client, "ospf")

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
    async with session() as db:
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

    apply_fn = AsyncMock()
    client = _guard_client(None)  # no service instance in NSO → collateral guard no-ops
    async with session() as db:
        device = await db.get(Device, device_id)
        with patch("nso_adapter.nso.apply.apply_isis_interfaces", apply_fn):
            await removal_mod._dispatch_scope(db, device, client, "isis")

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
    async with session() as db:
        device = await db.get(Device, device_id)
        device.ned_id = "cisco-iosxr-nc-7.3"
        db.add(RoutePolicyObjectIntent(device_id=device_id, family="rpl", name="RP-IN", entries=[], accepted_at=_NOW))
        await db.commit()

    apply_fn = AsyncMock()
    client = _guard_client(None)  # no service instance in NSO → collateral guard no-ops
    async with session() as db:
        device = await db.get(Device, device_id)
        with patch("nso_adapter.nso.apply.apply_route_policy_config", apply_fn):
            await removal_mod._dispatch_scope(db, device, client, "route_policy")

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
    async with session() as db:
        keep = DbInterface(device_id=device_id, name="Gi0/0")  # still has an accepted IP → PUT
        gone = DbInterface(device_id=device_id, name="Gi0/1")  # no remaining intent → DELETE
        db.add(keep)
        db.add(gone)
        await db.flush()
        db.add(InterfaceIpIntent(interface_id=keep.id, address="10.0.0.1/24", family="ipv4", vrf="", accepted_at=_NOW))
        await db.commit()

    replace_fn = AsyncMock()
    delete_fn = AsyncMock()
    async with session() as db:
        device = await db.get(Device, device_id)
        with (
            patch("nso_adapter.nso.apply.replace_interface_config", replace_fn),
            patch("nso_adapter.nso.apply.delete_interface_config", delete_fn),
        ):
            await removal_mod._dispatch_scope(
                db, device, _CLIENT, "interface_config", {"interfaces": ["Gi0/0", "Gi0/1"]}
            )

    replace_fn.assert_awaited_once()
    assert replace_fn.await_args.args[1] == "sw3" and replace_fn.await_args.args[2] == "Gi0/0"
    delete_fn.assert_awaited_once()
    assert delete_fn.await_args.args[1] == "sw3" and delete_fn.await_args.args[2] == "Gi0/1"


async def test_dispatch_scope_unknown_raises(adapter_client):
    device_id = await _seed_device()
    async with session() as db:
        device = await db.get(Device, device_id)
        with pytest.raises(ValueError, match="Unknown removal scope"):
            await removal_mod._dispatch_scope(db, device, _CLIENT, "nope")


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

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        assert job.result["scope"] == "vlan"
        # No `removed` keys in this job's context → nothing to residue-check (#104); the
        # opaque client's reader surface is never touched. Report that honestly — a job
        # that never read the device must not claim the device came back clean.
        assert job.result["residue_check"] == "unsupported"


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

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.error["code"] == "removal_failed"


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

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.error["code"] == "removal_failed"


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
    async with session() as db:
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
    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.error["code"] == "removal_blocked_collateral"
        assert job.error["detail"]["orphans"] == {"interface-config": [["lo0", "ipv4"]]}
        assert job.error["detail"]["preview"] == "- interface lo0 (native preview)"
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
    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.error["detail"]["orphans"] == {"process-config": [["OLD"]]}


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
    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
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
    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
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
    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
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
    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.error["code"] == "removal_blocked_collateral"
        assert job.error["detail"]["orphans"] == {"community": [["legacy"]]}
        assert job.error["detail"]["preview"] == "native preview"
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
    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
    final = apply_fn.await_args_list[-1]
    assert final.kwargs.get("replace") is True and final.kwargs.get("stage") is None


async def test_vlan_removal_blocked_on_orphan_vid_normalizes_ints(adapter_client):
    """vlan-id ints (NSO JSON) and store ints compare as strings — no false pass/block."""
    device_id = await _seed_device(nso_device_name="sw-vlan-guard")
    client = _guard_client({"device": "sw-vlan-guard", "vlan": [{"vlan-id": 10}, {"vlan-id": 99}]})
    job_id = await _seed_removal_job(device_id, scope="vlan")
    apply_fn = _staging_apply({"device": "sw-vlan-guard", "vlan": [{"vlan-id": 10}]})
    await _run_removal_with("vlan", "apply_vlan_config", device_id, job_id, client, apply_fn)
    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.error["detail"]["orphans"] == {"vlan": [["99"]]}


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
    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.error["detail"]["orphans"] == {"peer": [["192.0.2.9"]]}


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
    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded


async def test_static_route_removal_no_longer_blocks_on_collateral(adapter_client):
    """#1396 R2 §4.3/OQ-R2-3 — the DOCUMENTED behavior change, asserted rather than assumed.

    Until R2 this scope built a store-assertive PUT and blocked (``removal_blocked_collateral``)
    on any service row the store no longer asserts. R2 makes static-route removal bodies
    live-service-relative, so such a body cannot flush anything and the guard degenerates to
    "we dropped exactly what we authorized": the unrelated row is RETAINED and named on the job
    instead. The full branch matrix lives in ``tests/core/test_static_route_removal.py``; this
    pin exists so the change cannot happen silently for the other twelve scopes' neighbours.
    """
    from tests.core.test_static_route_removal import SrFake, run_removal_job, seed_removal_job, sr_client, wire

    survivor = ("", "10.0.0.0/24", "192.0.2.1")
    dropped = ("", "10.9.0.0/24", "192.0.2.9")
    device_id = await _seed_device(nso_device_name="sw-sr-guard")
    fake = SrFake("sw-sr-guard", service=[wire(survivor), wire(dropped)])

    # No `removed` context at all — pre-R2 this was the collateral case that BLOCKED.
    job_id = await seed_removal_job(device_id, {})
    job = await run_removal_job(device_id, job_id, sr_client(fake))
    assert job.status == JobStatus.succeeded
    assert job.result["superseded"] is True, "nothing authorized and no clear ⇒ no PUT at all"
    assert fake.service_keys == {survivor, dropped}

    # With the compound key threaded through the context, exactly that key is dropped and the
    # unrelated one is retained and reported.
    job2 = await seed_removal_job(device_id, {"removed": {"route": [list(dropped)]}})
    job = await run_removal_job(device_id, job2, sr_client(fake))
    assert job.status == JobStatus.succeeded
    assert fake.service_keys == {survivor}
    assert job.result["retained_orphans"] == [list(survivor)]


async def test_generic_force_skips_guard_and_service_get(adapter_client):
    """force=true (operator override) commits without even reading the service."""
    device_id = await _seed_device(nso_device_name="sw-vlan-force")
    client = _guard_client({"device": "sw-vlan-force", "vlan": [{"vlan-id": 99}]})
    job_id = await _seed_removal_job(device_id, scope="vlan", context_extra={"force": True})
    apply_fn = AsyncMock()
    await _run_removal_with("vlan", "apply_vlan_config", device_id, job_id, client, apply_fn)
    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
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
    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
    apply_fn.assert_awaited_once()
    assert apply_fn.await_args.kwargs == {"replace": True, "levels_intent_row": None}


async def test_replace_on_removal_threads_removed_keys(adapter_client):
    """The shim maps each simple scope's removed store keys onto its YANG list."""
    device_id = await _seed_device(nso_device_name="sw-shim")
    async with session() as db:
        device = await db.get(Device, device_id)
        await note_projection_write(db, device_id, "vlan")
        ok = await replace_on_removal(db, device, [3366, 3377], VlanIntent)
        assert ok is True
        await db.commit()
    async with session() as db:
        job = (await db.execute(select(Job))).scalars().one()
        assert job.context == {"scope": "vlan", "removed": {"vlan": [3366, 3377]}, "detach": True}


async def test_replace_on_removal_maps_route_policy_families(adapter_client):
    """route_policy removed keys are (family, name); the shim buckets them into the
    per-family YANG lists (community_list → community-list etc.)."""
    device_id = await _seed_device(nso_device_name="sw-shim-rp")
    async with session() as db:
        device = await db.get(Device, device_id)
        await note_projection_write(db, device_id, "route_policy")
        ok = await replace_on_removal(
            db, device, [("community_list", "example-comm"), ("route_map", "RM-IN")], RoutePolicyObjectIntent
        )
        assert ok is True
        await db.commit()
    async with session() as db:
        job = (await db.execute(select(Job))).scalars().one()
        assert job.context == {
            "scope": "route_policy",
            "removed": {"community-list": ["example-comm"], "route-map": ["RM-IN"]},
            "detach": True,
        }


async def test_enqueue_removal_serializes_removed_tuples(adapter_client):
    """Tuple keys (compound) become JSON-safe arrays in the job context."""
    device_id = await _seed_device(nso_device_name="sw-enq")
    async with session() as db:
        await note_projection_write(db, device_id, "static_route")
        job = await enqueue_removal(
            db,
            device_id,
            "static_route",
            marking="detach",
            defer_retract=False,
            promotes=("static_route",),
            removed={"route": [("", "10.0.0.0/24", "192.0.2.1")]},
        )
        await db.commit()
        assert job.context == {
            "scope": "static_route",
            "removed": {"route": [["", "10.0.0.0/24", "192.0.2.1"]]},
            "detach": True,
        }


# ── #104-A: residue-after-retract detection + immediate follow-up sync ────────
#
# FASTMAP's reverse diff keeps service-created entries that picked up foreign leaves
# (sw03 Vlan987: a sync between apply and removal imported the device-rendered
# ``no ip address`` into the CDB entry), so a removal can report SUCCESS while its
# keys survive on the device. run_removal must re-read the scope's device-tree view
# (network-state-export reader = data-provider, computed at GET time) and surface
# survivors in the job result, then enqueue a sync so the reappeared rows show as
# unowned mirrors immediately.


# short scope tag → device-state envelope SECTION (wire) name, mirroring
# removal._RESIDUE_WIRE_NAMES. Residue tests seed canned data by the short tag; the fake
# serves it as the ACTION's certified per-section output keyed by the wire name.
_TAG_TO_WIRE = {
    "svi": "svi",
    "subinterface": "subinterface",
    "static_route": "static-route",
    "vlan": "vlan-database",
    "logging": "logging-config",
    "interface_mtu": "interface-mtu",
    "bfd": "bfd-config",
    "l2": "l2-service",
    "bgp": "bgp-config",
    "isis": "isis-interface",
    "ospf": "ospf-config",
    "route_policy": "route-policy",
    "snmp": "snmp-config",
    "interface_ips": "interface-ip",
}


class _ReaderClient:
    """Real-shape fake of the device-state-read ACTION surface (READSEM 1328).

    Exposes exactly one coroutine — ``run_device_state_read`` — matching the real
    NsoClient method the residue check now calls. Canned per-family data is passed by the
    short scope tag (``svi=…``, ``snmp=…``) and served as the action's CERTIFIED output:
    ``{"atomic": True, "device-name": name, <wire>: <section>}`` with a terminal per-section
    ``status`` — defaulting to ``ok`` when the canned dict omits one, so a test forces
    ``unsupported``/``error`` by passing ``{"status": …}`` verbatim. A supported family with
    no canned data reads as authoritative-empty (``status=ok``, no list keys — the shape
    RESTCONF emits for a genuinely empty family). ``reads`` counts action calls, so the
    pre-check short-circuits (force-removal, cleared-scalar retract) still assert ``reads==0``.

    ``test_residue_wire_names_match_the_envelope_sections`` pins the wire mapping to the real
    envelope section set; certification itself is exercised end-to-end (through the real
    transport) in tests/nso/test_device_state_client.py — a method fake bypasses it.
    """

    def __init__(self, *, raise_on_read=False, **entries):
        self._sections = {
            _TAG_TO_WIRE[tag]: (data if "status" in data else {"status": "ok", **data}) for tag, data in entries.items()
        }
        self._raise = raise_on_read
        self.reads = 0

    async def run_device_state_read(self, device_name, families, *, timeout=None):
        self.reads += 1
        if self._raise:
            raise RuntimeError("reader down")
        return {
            "atomic": True,
            "device-name": device_name,
            **{wire: self._sections.get(wire, {"status": "ok"}) for wire in families},
        }


async def _run(job_id: int, device_id: int, client) -> None:
    from nso_adapter.core.removal import run_removal

    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=client),
        patch("nso_adapter.core.removal._dispatch_scope", new=AsyncMock()),
    ):
        await run_removal(job_id=job_id, device_id=device_id)


async def _job_after(job_id: int) -> Job:
    async with session() as db:
        return await db.get(Job, job_id)


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


def test_an_uncomparable_grain_is_only_reader_compared_if_it_can_be_TRANSLATED():
    """CR-A17 relaxed this rule — but only by exactly one notch, and it must not slip further.

    It used to read "a grain in UNCOMPARABLE_LISTS must NEVER appear in _READER_COMPARE_SPECS",
    because the two registries encode the same key-grain truth and had already diverged: the
    compare demanded a community be present under its intent LABEL while the export names it by a
    SHA-256 of the secret, so every successful SNMP apply was failed.

    The grain is comparable now — but only because `_KEY_TRANSLATORS` knows how to re-key it. The
    invariant that actually matters is therefore: an un-keyable grain may be reader-compared IF AND
    ONLY IF something can translate its key. Adding one to _READER_COMPARE_SPECS without a
    translator resurrects the original bug (every key "missing", the scope permanently
    apply_failed); registering one whose translator can never resolve resurrects the other (a
    fabricated verdict). Both fail here, loudly, instead of on a live device.
    """
    from nso_adapter.core.apply import _READER_COMPARE_SPECS
    from nso_adapter.core.removal import _KEY_TRANSLATORS, UNCOMPARABLE_LISTS

    compared = {(scope, label) for scope, specs in _READER_COMPARE_SPECS.items() for _, label, _ in specs}
    untranslatable = (compared & UNCOMPARABLE_LISTS) - set(_KEY_TRANSLATORS)
    assert not untranslatable, (
        f"these grains are reader-compared but cannot be key-matched against the export: "
        f"{sorted(untranslatable)} — every intended key would be reported missing"
    )
    assert set(_KEY_TRANSLATORS) <= UNCOMPARABLE_LISTS, (
        "a translator for a grain that already shares the export's namespace re-keys a key that "
        "was fine — it can only make the comparison wrong"
    )


# removal scope → the FamilySpec surface name whose wire_name the residue read must target.
# The mirror engine reads each family through FamilySpec.wire_name, so pinning
# _RESIDUE_WIRE_NAMES to that registry means a section rename breaks the residue check and
# the mirror together (never silently). This is the #104-A trap — a wire that the action
# would 404 on, matched by a fake that carried the same typo — foreclosed at the contract level.
_SCOPE_TO_SURFACE = {
    "svi": "svi",
    "subinterface": "subinterface",
    "static_route": "static_route",
    "vlan": "vlan",
    "logging": "logging",
    "interface_mtu": "interface_mtu",
    "bfd": "bfd",
    "l2_sap": "l2_service",
    "bgp": "bgp",
    "isis": "isis",
    "ospf": "ospf",
    "route_policy": "route_policy",
    "snmp": "snmp",
    "interface_config": "interface_ip",
}


def test_residue_wire_names_match_the_envelope_sections():
    """Every _RESIDUE_WIRE_NAMES value must equal the FamilySpec.wire_name the mirror reads,
    and the fake must expose the real action method. #104-A shipped bfd→get_bfd and
    l2_sap→get_l2_service (neither existed) and the fake carried the same typo, so the suite
    stayed green while real removals degraded to residue_check='error'. Its reborn form is a
    wire-name typo the action would 404 on — pinned here to ground truth."""
    import inspect

    from nso_adapter.core.importer import _projectable_spec
    from nso_adapter.core.removal import _RESIDUE_WIRE_NAMES
    from nso_adapter.nso.client import NsoClient

    assert set(_RESIDUE_WIRE_NAMES) == set(_SCOPE_TO_SURFACE)  # no scope drifts out of coverage
    for scope, wire in _RESIDUE_WIRE_NAMES.items():
        spec = _projectable_spec(_SCOPE_TO_SURFACE[scope])
        assert spec is not None, f"{scope} → surface {_SCOPE_TO_SURFACE[scope]!r} has no FamilySpec"
        assert spec.wire_name == wire, f"{scope}: residue wire {wire!r} != mirror wire_name {spec.wire_name!r}"
    # The residue read and the fake both go through the real action method, not a getter.
    action = getattr(NsoClient, "run_device_state_read", None)
    assert action is not None and inspect.iscoroutinefunction(action)
    assert inspect.iscoroutinefunction(getattr(_ReaderClient, "run_device_state_read", None))


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


async def test_run_removal_residue_bfd_reads_the_bfd_config_section(adapter_client):
    """#104-A regression, READSEM 1328 form: the bfd residue read must target the real
    ``bfd-config`` envelope section (a typo'd wire would 404 on the action and degrade
    every bfd removal to residue_check='error')."""
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


async def test_run_removal_residue_interface_config_unsupported_section(adapter_client):
    """READSEM 1328, value-grain path: a NED with no interface-ip surface (status=unsupported)
    reports residue_check="unsupported" — the value compare cannot run, so never a silent clean."""
    device_id = await _seed_device(nso_device_name="rm-ic-4")
    job_id = await _seed_removal_job(
        device_id,
        "interface_config",
        {"interfaces": ["Gi0/3"], "removed": {"address": [["Gi0/3", "10.0.0.2/24", ""]]}},
    )
    client = _ReaderClient(interface_ips={"status": "unsupported"})

    await _run(job_id, device_id, client)

    job = await _job_after(job_id)
    assert job.status == JobStatus.succeeded
    assert job.result["residue_check"] == "unsupported"
    assert client.reads == 1


async def test_run_removal_residue_reader_error_is_nonfatal(adapter_client):
    device_id = await _seed_device(nso_device_name="sw3")
    job_id = await _seed_removal_job(device_id, "svi", {"removed": {"interface": [["Vlan987"]]}})

    await _run(job_id, device_id, _ReaderClient(raise_on_read=True))

    job = await _job_after(job_id)
    assert job.status == JobStatus.succeeded
    assert job.result["residue_check"] == "error"


async def test_run_removal_residue_unsupported_section_is_transparent(adapter_client):
    """READSEM 1328: a section the NED does not export (status=unsupported) reports
    residue_check="unsupported" — the honest verdict the legacy None→{}→"clean" fabricated.

    The device WAS read (reads==1), but the family has no export surface here, so the
    check truly could not run — never a clean bill on config that might still be live.
    """
    device_id = await _seed_device(nso_device_name="sw3")
    job_id = await _seed_removal_job(device_id, "svi", {"removed": {"interface": [["Vlan987"]]}})
    client = _ReaderClient(svi={"status": "unsupported"})

    await _run(job_id, device_id, client)

    job = await _job_after(job_id)
    assert job.status == JobStatus.succeeded
    assert job.result["residue_check"] == "unsupported"
    assert client.reads == 1  # the action DID run — the NED just has no surface for this family


async def test_run_removal_residue_error_section_is_nonfatal(adapter_client):
    """READSEM 1328: a section whose family read errored (status=error) reports
    residue_check="error" and never fails the removal — the action's terminal error
    status must not read as a clean/found verdict."""
    device_id = await _seed_device(nso_device_name="sw3")
    job_id = await _seed_removal_job(device_id, "svi", {"removed": {"interface": [["Vlan987"]]}})
    client = _ReaderClient(svi={"status": "error", "error-reason": "extract failed"})

    await _run(job_id, device_id, client)

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

    async with session() as db:
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
    async with session() as db:
        sync_jobs = (
            (await db.execute(select(Job).where(Job.device_id == device_id, Job.job_type == JobType.sync)))
            .scalars()
            .all()
        )
        assert sync_jobs == []


# ── #106: un-own = detach (drop governance, never touch the device) ───────────
#
# A PUT-replace removal of an ADOPTED entry plays FASTMAP's reverse diff against
# the live device (rg03: the adopted redistribute's route-map filter would have
# been stripped). Only a NetBox object DELETION may retract for real — the plugin
# marks those pushes ?delete_origin=true; every unmarked shrink (an un-own) runs
# the replace with no-networking + sync-from so the device keeps the config as
# unowned brownfield.


async def test_enqueue_removal_unmarked_shrink_defaults_to_detach(adapter_client):
    from nso_adapter.core.removal import query_flag_marking

    device_id = await _seed_device(nso_device_name="sw-detach")
    async with session() as db:
        await note_projection_write(db, device_id, "svi")
        marks = query_flag_marking(deletes=True)
        job = await enqueue_removal(
            db,
            device_id,
            "svi",
            marking=marks.marking,
            defer_retract=marks.defer_retract,
            promotes=("svi",),
            removed={"interface": [["Vlan9"]]},
        )
        await db.commit()
        assert job.context["detach"] is True


async def test_enqueue_removal_delete_origin_is_real_retraction(adapter_client):
    from nso_adapter.core.removal import query_flag_marking
    from nso_adapter.core.request_flags import DELETE_ORIGIN

    device_id = await _seed_device(nso_device_name="sw-detach")
    token = DELETE_ORIGIN.set(True)
    try:
        async with session() as db:
            await note_projection_write(db, device_id, "svi")
            marks = query_flag_marking(deletes=True)
            job = await enqueue_removal(
                db,
                device_id,
                "svi",
                marking=marks.marking,
                defer_retract=marks.defer_retract,
                promotes=("svi",),
                removed={"interface": [["Vlan9"]]},
            )
            await db.commit()
            assert "detach" not in (job.context or {})
    finally:
        DELETE_ORIGIN.reset(token)


async def test_enqueue_removal_force_is_real_retraction(adapter_client):
    device_id = await _seed_device(nso_device_name="sw-detach")
    async with session() as db:
        job = await enqueue_removal(
            db,
            device_id,
            "svi",
            marking=None,
            defer_retract=False,
            promotes=(),
            removed={"interface": [["Vlan9"]]},
            force=True,
        )
        await db.commit()
        assert job.context.get("force") is True
        assert "detach" not in job.context


async def test_enqueue_removal_force_refuses_to_promote(adapter_client):
    """A force flush re-deploys authorized state; a caller naming streams contradicts that.

    Silently ignoring them is the shape of the bug: the promotion would authorize (and its
    settlement certify) whatever the named lanes hold, none of which this job sends.
    """
    device_id = await _seed_device(nso_device_name="sw-force-promotes")
    async with session() as db:
        await note_projection_write(db, device_id, "svi")
        with pytest.raises(ValueError, match="promotes nothing"):
            await enqueue_removal(
                db, device_id, "svi", marking=None, defer_retract=False, promotes=("svi",), force=True
            )


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
    async with session() as db:
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
    async with session() as db:
        device = await db.get(Device, device_id)
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
    "emission_field",
    [
        ("isis_interface_intent", "bfd_enabled"),
        ("isis_interface_intent", "frr_enabled"),
        ("isis_process_intent", "overload_bit"),
        ("isis_process_intent", "microloop_avoidance"),
        ("isis_level_intent", "wide_metrics_only"),
        ("isis_level_intent", "disabled"),
    ],
)
def test_false_to_none_is_a_clear_for_explicit_false_isis_fields(emission_field):
    from nso_adapter.core.removal import is_cleared

    assert is_cleared(False, None, emission_field=emission_field) is True


def test_false_to_none_stays_an_update_for_ospf_enabled():
    from nso_adapter.core.removal import is_cleared

    assert is_cleared(False, None, emission_field=("ospf_instance_intent", "enabled")) is False


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


# ── CR-A17: an SNMP community's retraction is verifiable after all ────────────────────────────
#
# The residue check's job is to answer "is the key I just removed actually OFF the device?". For
# every scope but one it does. snmp/community abstained, because the intent keys a community by its
# human-readable LABEL while the export keys it by sha256(community-string)[:16] — a digest of a
# secret the adapter never sees (it pushes a Vault triple and NSO resolves it). Two namespaces, no
# intersection, so the grain was reported unverifiable: honest, but it left the ONE scope where a
# survivor is a live credential as the only scope neither integrity check covered. A FASTMAP retract
# that left a community on the router was caught by nothing.
#
# The adapter holds the vault_ref. So: resolve the secret, hash it the same way, compare digests.
# What it must NEVER do is fabricate a verdict when Vault cannot answer — a clean bill on a
# credential that is still live is far worse than admitting the check did not run.


async def _seed_snmp_removal(device_name: str, *, refs: dict | None = None) -> tuple[int, int]:
    device_id = await _seed_device(nso_device_name=device_name)
    context = {"removed": {"community": [["prod-ro"]]}}
    if refs is not None:
        context["vault_refs"] = refs
    job_id = await _seed_removal_job(device_id, "snmp", context)
    return device_id, job_id


async def test_a_removed_community_STILL_on_the_device_is_now_FOUND(adapter_client, vault):
    """The bug this closes. The community was retracted in NetBox, the replace commit reported
    success — and the community is still on the router, ready to be used. Previously: "partial",
    grain unverifiable, nobody the wiser.
    """
    provider = vault()
    device_id, job_id = await _seed_snmp_removal("sw3", refs={"prod-ro": _REF})
    # the device still carries it, under its hashed export identity
    client = _ReaderClient(snmp={"community": [{"name": _community_export_name(_COMMUNITY), "access": "ro"}]})

    await _run(job_id, device_id, client)

    job = await _job_after(job_id)
    assert job.result["residue_check"] == "found"
    assert job.result["residue"] == {"community": [["prod-ro"]]}
    assert "residue_unverifiable" not in job.result, "the grain IS verifiable now — say so"
    assert provider.reads == 1


async def test_a_removed_community_that_actually_LEFT_is_CLEAN(adapter_client, vault):
    """The other half. A clean bill is only allowed once the check has actually run — and now it
    can, so the operator gets a real "it's gone" instead of a shrug.
    """
    vault()
    device_id, job_id = await _seed_snmp_removal("sw3", refs={"prod-ro": _REF})
    # some OTHER community remains; ours is gone
    client = _ReaderClient(snmp={"community": [{"name": _community_export_name("a-different-one"), "access": "rw"}]})

    await _run(job_id, device_id, client)

    job = await _job_after(job_id)
    assert job.result["residue_check"] == "clean"
    assert "residue" not in job.result
    assert "residue_unverifiable" not in job.result


async def test_VAULT_DOWN_reports_unverifiable_and_NEVER_clean(adapter_client, vault):
    """The rule that makes the rest of this safe to trust.

    If Vault cannot answer, the digest cannot be computed, so the comparison cannot run. The
    intersection of an empty key-set with the device's communities is empty — which, folded into
    the verdict, would read as "clean": a fabricated all-clear on a credential that may well still
    be live on the router. Fail OPEN, back to exactly where this grain already was.
    """
    vault(fail=True)
    device_id, job_id = await _seed_snmp_removal("sw3", refs={"prod-ro": _REF})
    client = _ReaderClient(snmp={"community": [{"name": _community_export_name(_COMMUNITY), "access": "ro"}]})

    await _run(job_id, device_id, client)

    job = await _job_after(job_id)
    assert job.result["residue_check"] != "clean"
    assert job.result["residue_unverifiable"] == ["community"]


async def test_NO_vault_provider_is_unverifiable_too(adapter_client):
    """The local/env secrets provider has no mount-explicit read. Same verdict as an outage."""
    device_id, job_id = await _seed_snmp_removal("sw3", refs={"prod-ro": _REF})
    client = _ReaderClient(snmp={"community": [{"name": _community_export_name(_COMMUNITY)}]})

    await _run(job_id, device_id, client)

    job = await _job_after(job_id)
    assert job.result["residue_check"] != "clean"
    assert job.result["residue_unverifiable"] == ["community"]


async def test_a_job_queued_BEFORE_this_existed_carries_no_refs_and_stays_unverifiable(adapter_client, vault):
    """Back-compat: a removal job already in the queue has no `vault_refs` in its context. It must
    degrade to the old verdict, not to a fabricated clean one.
    """
    vault()
    device_id, job_id = await _seed_snmp_removal("sw3", refs=None)  # the OLD context shape
    client = _ReaderClient(snmp={"community": [{"name": _community_export_name(_COMMUNITY)}]})

    await _run(job_id, device_id, client)

    job = await _job_after(job_id)
    assert job.result["residue_check"] != "clean"
    assert job.result["residue_unverifiable"] == ["community"]


async def test_a_community_whose_ref_no_longer_resolves_does_not_taint_the_OTHER_grains(adapter_client, vault):
    """Mixed removal: a community (unresolvable) plus a v3-user (plain key). The user's grain is
    still checked and still reports its survivor — one grain going dark must not blind the others.
    """
    vault()
    device_id = await _seed_device(nso_device_name="sw3")
    job_id = await _seed_removal_job(
        device_id,
        "snmp",
        {
            "removed": {"community": [["prod-ro"]], "v3-user": [["netmon"]]},
            "vault_refs": {"prod-ro": "network/no/such/path#community"},  # resolves to nothing
        },
    )
    client = _ReaderClient(snmp={"community": [], "v3-user": [{"username": "netmon"}]})

    await _run(job_id, device_id, client)

    job = await _job_after(job_id)
    assert job.result["residue_check"] == "found"
    assert job.result["residue"] == {"v3-user": [["netmon"]]}
    assert job.result["residue_unverifiable"] == ["community"]


async def test_the_vault_read_never_runs_on_the_EVENT_LOOP_thread(adapter_client, vault):
    """CR-A13, the constraint CR-A17 had to be built around.

    hvac is blocking `requests`. Resolving a ref straight from an `async def` freezes the single
    event-loop thread for the whole Vault round-trip: every other adapter request hangs, /health
    stops answering (a container liveness probe can then kill the adapter mid-write), and the
    in-process scheduler tick driving failover probes and job dispatch stalls. On a Vault that is
    slow rather than down — the nastier case — that is a stall, not an error.

    So the read is proven to happen OFF the loop, not merely asserted to in a comment.
    """
    import threading

    provider = vault()
    device_id, job_id = await _seed_snmp_removal("sw3", refs={"prod-ro": _REF})

    await _run(job_id, device_id, _ReaderClient(snmp={"community": []}))

    assert provider.reads == 1
    assert provider.read_threads == [provider.read_threads[0]]
    assert provider.read_threads[0] != threading.get_ident(), "the Vault read ran ON the event loop"
