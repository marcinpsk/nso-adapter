# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""#1396 R2 chunk C1 — planner, renderer, guard snapshot and claim/job_id threading.

Pins C1.1-C1.9 of the R2 brief. Every plan case runs against a real PostgreSQL clone and
real ``StaticRouteIntent`` / ``StaticRouteTombstone`` rows; the NSO client is the only
fake, and the guard cases use the same spec'd fake the shipped guard tests use.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import pytest
import sqlalchemy as sa

from nso_adapter.core import removal as removal_mod
from nso_adapter.core.claim import ClaimLostError, ClaimRegistration, acquire_claim, lock_claim
from nso_adapter.core.static_route_plan import (
    SR_CLEAR_FIELDS,
    build_plan,
    fence_open,
    hydrate_static_route_apply_plan,
    replacement_open,
)
from nso_adapter.nso.apply import apply_static_routes, static_route_entry
from tests.conftest import seed_device, session

pytestmark = pytest.mark.anyio

_NOW = datetime(2026, 6, 1, tzinfo=UTC)

A = ("", "10.0.0.0/24", "192.0.2.1")
B = ("", "10.0.1.0/24", "192.0.2.2")
C = ("", "10.0.2.0/24", "192.0.2.3")

_SR_ROOT = "static-route-reconciler:static-route-config"


async def _seed_rows(device_id: int, specs: list[dict]) -> dict[tuple, int]:
    """Insert intent rows; return ``{triple: row id}``."""
    from nso_adapter.store.models import StaticRouteIntent

    out: dict[tuple, int] = {}
    async with session() as db:
        for spec in specs:
            vrf, prefix, next_hop = spec["triple"]
            row = StaticRouteIntent(
                device_id=device_id,
                vrf=vrf,
                prefix=prefix,
                next_hop=next_hop,
                route_id=spec.get("route_id"),
                deployed_key=spec.get("deployed_key"),
                accepted_at=spec.get("accepted_at", _NOW),
                last_apply_at=spec.get("last_apply_at"),
            )
            db.add(row)
            await db.flush()
            out[spec["triple"]] = row.id
        await db.commit()
    return out


async def _seed_tombstone(
    device_id: int, triple: tuple, *, route_id: int = 99, deployed_key=None, marking="delete_origin"
) -> int:
    from nso_adapter.store.models import StaticRouteTombstone

    vrf, prefix, next_hop = triple
    async with session() as db:
        tomb = StaticRouteTombstone(
            device_id=device_id,
            route_id=route_id,
            vrf=vrf,
            prefix=prefix,
            next_hop=next_hop,
            deployed_key=deployed_key,
            marking=marking,
        )
        db.add(tomb)
        await db.commit()
        return tomb.id


async def _plan(device_id: int, *, force: bool = True):
    """Build the plan the way a real apply does — eligible rows from the real collector."""
    from nso_adapter.core.apply import _collect_eligible
    from nso_adapter.store.models import Device, StaticRouteIntent

    async with session() as db:
        device = await db.get(Device, device_id)
        eligible = await _collect_eligible(db, StaticRouteIntent, device_id, force)
        return await build_plan(db, device, eligible_rows=eligible), [r.id for r in eligible]


def _triples(rows) -> set[tuple]:
    return {(r.vrf, r.prefix, r.next_hop) for r in rows}


# ── C1.1 / C1.2 — the mode predicate ─────────────────────────────────────────


async def test_c1_1_a_route_id_less_sibling_shuts_the_fence(adapter_client):
    """C1.1 — one NULL ``route_id`` anywhere on the device forbids PUT mode.

    The fence is per DEVICE, not per row: the replacement-open row here is fully
    identified, and a per-row reading would happily PUT-replace while a sibling triple
    was never correlated with any NetBox route pk.
    """
    device_id = await seed_device(nso_device_name="sr-plan-fence", netbox_device_id=7001)
    await _seed_rows(
        device_id,
        [
            {"triple": A, "route_id": None},
            {"triple": B, "route_id": 2, "deployed_key": list(C)},
        ],
    )
    plan, _ = await _plan(device_id)
    assert plan.mode == "PATCH"

    # Discriminating variant: backfill the NULL and the very same store flips to PUT.
    from nso_adapter.store.models import StaticRouteIntent

    async with session() as db:
        await db.execute(
            sa.update(StaticRouteIntent)
            .where(StaticRouteIntent.device_id == device_id, StaticRouteIntent.route_id.is_(None))
            .values(route_id=1)
        )
        await db.commit()
    plan, _ = await _plan(device_id)
    assert plan.mode == "PUT"


async def test_c1_2_no_open_replacement_stays_patch(adapter_client):
    """C1.2 — a fully-delivered device merge-PATCHes; one stale predecessor flips it."""
    device_id = await seed_device(nso_device_name="sr-plan-clean", netbox_device_id=7002)
    await _seed_rows(
        device_id,
        [
            {"triple": A, "route_id": 1, "deployed_key": list(A)},
            {"triple": B, "route_id": 2, "deployed_key": list(B)},
        ],
    )
    plan, _ = await _plan(device_id)
    assert plan.mode == "PATCH"
    assert plan.allowed == set()

    from nso_adapter.store.models import StaticRouteIntent

    async with session() as db:
        await db.execute(
            sa.update(StaticRouteIntent)
            .where(StaticRouteIntent.device_id == device_id, StaticRouteIntent.route_id == 2)
            .values(deployed_key=list(C))
        )
        await db.commit()
    plan, _ = await _plan(device_id)
    assert plan.mode == "PUT"
    assert C in plan.allowed


async def test_c1_2b_put_is_refused_when_verification_is_disabled(adapter_client, monkeypatch):
    """§4.4 — a destructive replace whose proof is structurally unavailable must not run."""
    from nso_adapter.nso import apply as nso_apply

    device_id = await seed_device(nso_device_name="sr-plan-noverify", netbox_device_id=7003)
    await _seed_rows(device_id, [{"triple": B, "route_id": 2, "deployed_key": list(A)}])
    monkeypatch.setattr(nso_apply, "VERIFY_AFTER_APPLY", False)
    plan, _ = await _plan(device_id)
    assert plan.mode == "PATCH"
    assert plan.allowed == set()

    monkeypatch.setattr(nso_apply, "VERIFY_AFTER_APPLY", True)
    plan, _ = await _plan(device_id)
    assert plan.mode == "PUT"


# ── C1.3 / C1.4 — plan.rows is the single source of truth ────────────────────


async def test_c1_3_put_rows_are_every_accepted_row_force_independent(adapter_client):
    """C1.3 — the row needing replacement keeps its ``last_apply_at``, so ``force=False``
    filters it out of the eligible list. A PUT built from that list would retract the very
    route it exists to replace, and every accepted-and-clean sibling with it.
    """
    device_id = await seed_device(nso_device_name="sr-plan-rows", netbox_device_id=7004)
    await _seed_rows(
        device_id,
        [
            # replacement open AND already applied cleanly ⇒ not eligible under force=False
            {"triple": B, "route_id": 2, "deployed_key": list(A), "last_apply_at": _NOW},
            # accepted and clean sibling ⇒ also filtered out under force=False
            {"triple": C, "route_id": 3, "deployed_key": list(C), "last_apply_at": _NOW},
        ],
    )
    plan_soft, eligible_soft = await _plan(device_id, force=False)
    assert eligible_soft == [], "setup broken: the rows must be ineligible under force=False"
    assert plan_soft.mode == "PUT"
    assert _triples(plan_soft.rows) == {B, C}

    plan_hard, _ = await _plan(device_id, force=True)
    assert [r.id for r in plan_hard.rows] == [r.id for r in plan_soft.rows]


async def test_c1_4_any_eligible_derived_from_plan_rows_is_true(adapter_client):
    """C1.4 (plan half) — ``plan.rows`` is non-empty exactly where the old eligible list is
    empty, so a caller deriving ``any_eligible`` from it cannot take the all-zero early
    success after a real PUT. Wiring it into ``_execute_apply`` is C2's site.
    """
    device_id = await seed_device(nso_device_name="sr-plan-anyelig", netbox_device_id=7005)
    await _seed_rows(device_id, [{"triple": B, "route_id": 2, "deployed_key": list(A), "last_apply_at": _NOW}])
    plan, eligible = await _plan(device_id, force=False)
    assert eligible == []
    assert bool(plan.rows) is True
    assert [c.row_id for c in plan.cas] == [r.id for r in plan.rows]
    assert plan.cas[0].expected_old == list(A)
    assert plan.cas[0].sent_triple == B


async def test_c1_4b_patch_rows_are_the_eligible_list_verbatim(adapter_client):
    """PATCH mode must not silently widen the body to every accepted row."""
    device_id = await seed_device(nso_device_name="sr-plan-patchrows", netbox_device_id=7006)
    ids = await _seed_rows(
        device_id,
        [
            {"triple": A, "route_id": 1, "deployed_key": list(A), "last_apply_at": _NOW},
            {"triple": B, "route_id": 2},
        ],
    )
    plan, eligible = await _plan(device_id, force=False)
    assert plan.mode == "PATCH"
    assert eligible == [ids[B]]
    assert [r.id for r in plan.rows] == [ids[B]]


# ── C1.5 — REPLACEMENT_OPEN is element-wise ──────────────────────────────────


class _Row:
    """A minimal row stand-in for the pure predicates (no DB identity involved)."""

    def __init__(self, vrf, prefix, next_hop, deployed_key=None, route_id=1):
        self.vrf, self.prefix, self.next_hop = vrf, prefix, next_hop
        self.deployed_key = deployed_key
        self.route_id = route_id


def test_c1_5_replacement_open_compares_by_value_not_identity():
    """C1.5 — an ``A -> B -> A`` round trip is NOT an open replacement.

    ``deployed_key`` is a freshly deserialized JSON list, never the same object as the
    current triple, so any identity/`is` comparison reports every applied row as open and
    PUT-replaces the whole fleet.
    """
    same = _Row(*A, deployed_key=list(A))
    assert replacement_open(same) is False

    for index in range(3):
        drifted = list(A)
        drifted[index] = "zzz"
        assert replacement_open(_Row(*A, deployed_key=drifted)) is True, f"element {index} ignored"

    assert replacement_open(_Row(*A, deployed_key=None)) is False


def test_c1_5b_fence_open_is_device_wide():
    rows = [_Row(*A, route_id=1), _Row(*B, route_id=None)]
    assert fence_open(rows) is False
    assert fence_open([_Row(*A, route_id=1)]) is True
    assert fence_open([]) is True


# ── C1.6 — the one renderer ──────────────────────────────────────────────────


class _RenderRow:
    def __init__(self, **kw):
        self.vrf = kw.get("vrf", "")
        self.prefix = kw.get("prefix", "10.0.0.0/24")
        self.next_hop = kw.get("next_hop", "192.0.2.1")
        self.interface_next_hop = kw.get("interface_next_hop")
        self.next_hop_vrf = kw.get("next_hop_vrf")
        self.metric = kw.get("metric")
        self.permanent = kw.get("permanent")
        self.tag = kw.get("tag")
        self.name = kw.get("name")


def test_c1_6_static_route_entry_matches_the_captured_wire_dict():
    """C1.6 — the exact pre-refactor wire dict, asserted literally.

    Renderer-vs-renderer equality would pass while both drifted; these are the dicts the
    shipped loop produced, transcribed by hand.
    """
    everything = _RenderRow(
        vrf="RED",
        prefix="10.5.0.0/24",
        next_hop="192.0.2.9",
        interface_next_hop="GigabitEthernet0/0",
        next_hop_vrf="BLUE",
        metric=10,
        permanent=True,
        tag=100,
        name="to-core",
    )
    assert static_route_entry(everything) == {
        "vrf": "RED",
        "prefix": "10.5.0.0/24",
        "next-hop": "192.0.2.9",
        "interface-next-hop": "GigabitEthernet0/0",
        "next-hop-vrf": "BLUE",
        "metric": 10,
        "permanent": True,
        "tag": 100,
    }

    nothing = _RenderRow(name="still-not-emitted")
    assert static_route_entry(nothing) == {"vrf": "", "prefix": "10.0.0.0/24", "next-hop": "192.0.2.1"}


def test_c1_6b_omission_rules_are_not_falsiness():
    """``permanent=False`` stays omitted; ``metric=0`` / ``tag=0`` are emitted."""
    row = _RenderRow(permanent=False, metric=0, tag=0, interface_next_hop="", next_hop_vrf="")
    assert static_route_entry(row) == {
        "vrf": "",
        "prefix": "10.0.0.0/24",
        "next-hop": "192.0.2.1",
        "metric": 0,
        "tag": 0,
    }


# ── C1.7 — extra_entries ─────────────────────────────────────────────────────


async def _staged_body(rows, extra_entries=None) -> dict:
    from nso_adapter.nso.client import NsoClient

    client = AsyncMock(spec=NsoClient)
    stage: dict[str, list] = {}
    await apply_static_routes(client, "sr-extra", rows, extra_entries=extra_entries, replace=True, stage=stage)
    return stage[_SR_ROOT][0]


async def test_c1_7_extra_entries_ride_verbatim_and_never_override_a_rendered_row():
    """C1.7 — retention appends what the store cannot express, and loses key collisions.

    ``A'`` is a live copy of a route the store still owns; letting it win would deploy
    whatever stale leaves the device happened to hold over the accepted intent.
    """
    rendered_a = _RenderRow(prefix=A[1], next_hop=A[2], metric=10)
    stale_a = {"vrf": "", "prefix": A[1], "next-hop": A[2], "metric": 999}
    verbatim_c = {"vrf": "", "prefix": C[1], "next-hop": C[2], "tag": 7, "bfd-fast-detect": {"minimum": 50}}

    body = await _staged_body([rendered_a], extra_entries=[stale_a, verbatim_c])
    assert body["route"] == [
        {"vrf": "", "prefix": A[1], "next-hop": A[2], "metric": 10},
        verbatim_c,
    ]
    # the leaf the store has no column for survived byte-for-byte
    assert body["route"][1]["bfd-fast-detect"] == {"minimum": 50}


async def test_c1_7b_no_extra_entries_is_todays_body():
    body = await _staged_body([_RenderRow()])
    assert body == {"device": "sr-extra", "route": [{"vrf": "", "prefix": "10.0.0.0/24", "next-hop": "192.0.2.1"}]}


# ── C1.8 — the guard snapshot parameter ──────────────────────────────────────


def _guard_client(service_config=None):
    from nso_adapter.nso.client import NsoClient

    client = AsyncMock(spec=NsoClient)
    client.get_service_config.return_value = service_config
    return client


class _Device:
    id = 1
    nso_device_name = "sr-guard"
    ned_id = "cisco-ios-cli-6.95"


@pytest.mark.parametrize(
    ("supplied", "label"),
    [
        ({"device": "sr-guard", "route": [{"vrf": "", "prefix": A[1], "next-hop": A[2]}]}, "a real snapshot"),
        (None, "the absent-service snapshot"),
    ],
)
async def test_c1_8_supplied_snapshot_suppresses_the_internal_get(supplied, label):
    """C1.8 — ``current=`` is a sentinel default, so even ``None`` suppresses the GET.

    The ``None`` case is the one a naive ``if current is None: GET`` gets wrong: ``None``
    is a valid snapshot meaning "no service instance", and re-reading it defeats the
    one-snapshot contract exactly where a second read is most likely to disagree.
    """
    client = _guard_client(
        {"device": "sr-guard", "route": [{"vrf": "", "prefix": "10.9.9.0/24", "next-hop": "1.2.3.4"}]}
    )

    async def _apply(**kwargs):
        if kwargs.get("stage") is not None:
            kwargs["stage"][_SR_ROOT] = [
                {"device": "sr-guard", "route": [{"vrf": "", "prefix": A[1], "next-hop": A[2]}]}
            ]
        return

    await removal_mod._guarded_apply(
        client, _Device(), "static_route", {}, AsyncMock(side_effect=_apply), current=supplied
    )
    client.get_service_config.assert_not_awaited(), label


async def test_c1_8b_other_scopes_still_read_the_service_themselves():
    """The twelve scopes that pass nothing keep today's internal GET."""
    client = _guard_client(None)
    await removal_mod._guarded_apply(client, _Device(), "vlan", {}, AsyncMock())
    client.get_service_config.assert_awaited_once()


async def test_c1_8c_a_supplied_snapshot_still_drives_the_guard():
    """Handing the snapshot in must not disable the collateral check."""
    client = _guard_client(None)  # would look clean if the helper re-read
    supplied = {
        "device": "sr-guard",
        "route": [
            {"vrf": "", "prefix": A[1], "next-hop": A[2]},
            {"vrf": "", "prefix": C[1], "next-hop": C[2]},  # orphan
        ],
    }

    async def _apply(**kwargs):
        if kwargs.get("stage") is not None:
            kwargs["stage"][_SR_ROOT] = [
                {"device": "sr-guard", "route": [{"vrf": "", "prefix": A[1], "next-hop": A[2]}]}
            ]
        return "preview"

    with pytest.raises(removal_mod.RemovalBlockedError) as excinfo:
        await removal_mod._guarded_apply(
            client, _Device(), "static_route", {}, AsyncMock(side_effect=_apply), current=supplied
        )
    assert excinfo.value.orphans == {"route": [["", C[1], C[2]]]}
    client.get_service_config.assert_not_awaited()


# ── C1.9 — claim + job_id threading ──────────────────────────────────────────


async def test_c1_9_runners_forward_the_worker_registration(adapter_client):
    """The worker's live registration must reach ``run_apply`` / ``run_removal``.

    R1 stopped it at ``_run_apply`` / ``_run_removal``, so no write a runner made could be
    claim-scoped or tombstone-correlated — X1's blocker.
    """
    from nso_adapter.core.jobs import _run_apply, _run_removal

    reg = ClaimRegistration(11, "a-token")
    with patch("nso_adapter.core.apply.run_apply", new_callable=AsyncMock) as run_apply_mock:
        await _run_apply(1, 11, reg)
    assert run_apply_mock.await_args.kwargs["reg"] is reg

    with patch("nso_adapter.core.removal.run_removal", new_callable=AsyncMock) as run_removal_mock:
        await _run_removal(2, 11, reg)
    assert run_removal_mock.await_args.kwargs["reg"] is reg


async def test_c1_9b_dispatch_scope_receives_the_job_id_and_the_registration(adapter_client):
    """``_dispatch_scope`` had neither (G13), so no R2 write could be job-correlated."""
    from nso_adapter.store.models import Job, JobStatus, JobType

    device_id = await seed_device(nso_device_name="sr-thread", netbox_device_id=7010)
    async with session() as db:
        job = Job(
            job_type=JobType.removal,
            device_id=device_id,
            status=JobStatus.queued,
            coalescible=False,
            context={"scope": "vlan"},
        )
        db.add(job)
        await db.commit()
        job_id = job.id

    reg = await acquire_claim(device_id, "job", job_id=job_id)
    seen = {}

    async def _capture(_db, _device, _client, _scope, _context=None, *, job_id=None, reg=None):
        seen["job_id"] = job_id
        seen["reg"] = reg

    with (
        patch.object(removal_mod, "_dispatch_scope", _capture),
        patch("nso_adapter.core.importer.get_nso_client", return_value=_guard_client(None)),
    ):
        await removal_mod.run_removal(job_id, device_id, reg=reg)

    assert seen["job_id"] == job_id
    assert seen["reg"] is reg
    assert reg.registered is True, "a consuming path needs a REGISTERED claim, not a placeholder"


async def test_c1_9c_a_revoked_claim_propagates_instead_of_failing_the_job(adapter_client):
    """A revocation inside the scope must not be written back as a job failure.

    Driven through the real ``lock_claim`` with the real registration the runner was
    handed; recovery already owns the disposition.
    """
    from nso_adapter.store.models import DeviceClaim, Job, JobStatus, JobType

    device_id = await seed_device(nso_device_name="sr-revoked", netbox_device_id=7011)
    async with session() as db:
        job = Job(
            job_type=JobType.removal,
            device_id=device_id,
            status=JobStatus.queued,
            coalescible=False,
            context={"scope": "vlan"},
        )
        db.add(job)
        await db.commit()
        job_id = job.id

    reg = await acquire_claim(device_id, "job", job_id=job_id)

    async def _revoked_then_lock(db, _device, _client, _scope, _context=None, *, job_id=None, reg=None):
        await db.execute(sa.delete(DeviceClaim).where(DeviceClaim.device_id == reg.device_id))
        await db.commit()
        await lock_claim(db, reg)

    with (
        patch.object(removal_mod, "_dispatch_scope", _revoked_then_lock),
        patch("nso_adapter.core.importer.get_nso_client", return_value=_guard_client(None)),
        pytest.raises(ClaimLostError),
    ):
        await removal_mod.run_removal(job_id, device_id, reg=reg)

    async with session() as db:
        assert (await db.get(Job, job_id)).status is not JobStatus.failed


async def test_c1_9d_lock_claim_refuses_a_missing_registration():
    """G20 — ``reg=None`` on a consuming path is a programming error, not a no-op.

    Only an UNREGISTERED ``ClaimRegistration`` is the claimless lane; ``None`` must never
    read as "nothing to guard", or R2's carrier writes would commit unguarded.
    """
    with pytest.raises(AttributeError):
        await lock_claim(object(), None)  # type: ignore[arg-type]

    unregistered = ClaimRegistration(1, None)
    assert unregistered.registered is False


# ── the clear-field list cannot drift from the endpoint's state fields ───────


def test_clear_fields_are_the_state_fields_minus_name():
    """A field added to the endpoint's before-image must be classified deliberately."""
    from nso_adapter.api.static_route import _STATE_FIELDS

    assert SR_CLEAR_FIELDS == tuple(f for f in _STATE_FIELDS if f != "name")


# ── the plan's tombstone snapshot ────────────────────────────────────────────


async def test_plan_snapshots_tombstones_and_the_watermark(adapter_client):
    """``allowed`` carries the X4 belt and the CAS fallback gets its watermark."""
    device_id = await seed_device(nso_device_name="sr-plan-tombs", netbox_device_id=7012)
    await _seed_rows(device_id, [{"triple": B, "route_id": 2, "deployed_key": list(A)}])
    tomb_id = await _seed_tombstone(device_id, C, deployed_key=list(A))

    plan, _ = await _plan(device_id)
    assert plan.mode == "PUT"
    assert plan.tombstone_ids == [tomb_id]
    assert plan.tombstone_id_watermark == tomb_id
    assert plan.allowed == {A, C}


async def test_generation_records_the_complete_static_route_apply_plan(adapter_client):
    """Generation creation freezes every fact that selects PATCH versus PUT."""
    from nso_adapter.core.generation import create_generation, note_write
    from nso_adapter.store.models import GenerationMode

    device_id = await seed_device(nso_device_name="sr-recorded-plan", netbox_device_id=7014)
    ids = await _seed_rows(device_id, [{"triple": B, "route_id": 2, "deployed_key": list(A)}])
    tomb_id = await _seed_tombstone(device_id, C, deployed_key=list(A))

    async with session() as db:
        await note_write(db, device_id, "static_route")
        generation = await create_generation(
            db,
            device_id,
            streams=("static_route",),
            mode=GenerationMode.networked,
        )
        await db.commit()

    recorded = generation.document["static_route"]["_execution"]["apply"]
    assert recorded == {
        "mode": "PUT",
        "row_ids": [ids[B]],
        "allowed_removal_keys": [list(A), list(C)],
        "tombstone_ids": [tomb_id],
        "cas": [
            {
                "row_id": ids[B],
                "route_id": 2,
                "sent_triple": list(B),
                "expected_old": list(A),
            }
        ],
        "tombstone_id_watermark": tomb_id,
    }


async def test_recorded_plan_rejects_a_malformed_sent_triple(adapter_client):
    from nso_adapter.core.generation import create_generation, note_write
    from nso_adapter.store.models import GenerationMode

    device_id = await seed_device(nso_device_name="sr-malformed-recorded-plan", netbox_device_id=7015)
    await _seed_rows(device_id, [{"triple": B, "route_id": 2, "deployed_key": list(A)}])

    async with session() as db:
        await note_write(db, device_id, "static_route")
        generation = await create_generation(
            db,
            device_id,
            streams=("static_route",),
            mode=GenerationMode.networked,
        )
        document = generation.document

    document["static_route"]["_execution"]["apply"]["cas"][0]["sent_triple"] = list(B[:2])

    # Pinned: the plan raises from four independent checks, and the CAS-coordinate one is a
    # plausible alternative source with no eligible rows.
    with pytest.raises(ValueError, match="must contain three values"):
        hydrate_static_route_apply_plan(document, eligible_rows=[])


async def test_plan_writes_nothing(adapter_client):
    """``build_plan`` is read-only — no stamping, no consumption, no HTTP."""
    from nso_adapter.store.models import Device, StaticRouteIntent

    device_id = await seed_device(nso_device_name="sr-plan-readonly", netbox_device_id=7013)
    await _seed_rows(device_id, [{"triple": B, "route_id": 2, "deployed_key": list(A)}])
    await _seed_tombstone(device_id, C)

    def _snapshot(rows):
        return [{c.name: getattr(r, c.name) for c in StaticRouteIntent.__table__.columns} for r in rows]

    async with session() as db:
        before = _snapshot(
            (await db.execute(sa.select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id)))
            .scalars()
            .all()
        )
        device = await db.get(Device, device_id)
        await build_plan(db, device, eligible_rows=[])
        await db.rollback()

    async with session() as db:
        after = _snapshot(
            (await db.execute(sa.select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id)))
            .scalars()
            .all()
        )
        assert after == before
        assert (
            await db.scalar(
                sa.text("SELECT count(*) FROM static_route_tombstone WHERE device_id = :d").bindparams(d=device_id)
            )
            == 1
        )


def test_the_carrier_accessors_read_an_absent_carrier_as_empty():
    """``pending_clear`` is NULL on almost every row; both readers must say "nothing"."""
    from nso_adapter.core.static_route_plan import authorized_clear_fields, pending_clear_fields

    for carrier in (None, {}, {"authorized": [], "store_only": []}):
        assert pending_clear_fields(carrier) == set()
        assert authorized_clear_fields(carrier) == set()


# ── the one clear-candidate rule (creation-time plan and live reissue) ───────


class _ClearRow:
    _ids = iter(range(1, 1000))

    def __init__(self, triple, *, pending_clear=None, deployed_key=None, **leaves):
        self.id = next(self._ids)
        self.vrf, self.prefix, self.next_hop = triple
        self.pending_clear = pending_clear
        self.deployed_key = deployed_key
        for name, value in leaves.items():
            setattr(self, name, value)


def test_candidate_clear_fields_pins_the_wire_set_rules():
    """permanent True->False is a clear; metric at 0 is wire-set; open replacements wait."""
    from nso_adapter.core.static_route_plan import candidate_clear_fields

    cleared = _ClearRow(A, pending_clear={"authorized": ["permanent"]}, permanent=False)
    assert candidate_clear_fields(cleared) == ("permanent",)

    metric_zero = _ClearRow(A, pending_clear={"authorized": ["metric"]}, metric=0)
    assert candidate_clear_fields(metric_zero) == ()

    metric_gone = _ClearRow(A, pending_clear={"authorized": ["metric"]}, metric=None)
    assert candidate_clear_fields(metric_gone) == ("metric",)

    open_replacement = _ClearRow(B, pending_clear={"authorized": ["permanent"]}, permanent=False, deployed_key=list(C))
    assert candidate_clear_fields(open_replacement) == ()

    store_only = _ClearRow(A, pending_clear={"store_only": ["permanent"]}, permanent=False)
    assert candidate_clear_fields(store_only) == ()


def test_clears_suppressed_matches_the_two_removal_modes():
    from nso_adapter.core.static_route_plan import clears_suppressed

    assert clears_suppressed({}) is False
    assert clears_suppressed({"detach": True}) is True
    assert clears_suppressed({"retract_deferred": True}) is True


async def test_promoted_and_live_reissue_plans_carry_identical_clears(adapter_client):
    """Drift guard: the creation-time classifier and the live reissue path share one rule."""
    from nso_adapter.core.projection import EXECUTION_KEY
    from nso_adapter.core.removal import _sr_execution_plan
    from nso_adapter.core.static_route_plan import (
        _serialize_removal_plan,
        classify_removal_plan,
        hydrate_static_route_removal_plan,
    )
    from nso_adapter.store.models import Device, StaticRouteIntent

    device_id = await seed_device(nso_device_name="sr-clear-parity", netbox_device_id=9891)
    ids = await _seed_rows(
        device_id,
        [
            {"triple": A, "route_id": 1, "deployed_key": list(A)},
            {"triple": B, "route_id": 2, "deployed_key": list(C)},
            {"triple": C, "route_id": 3, "deployed_key": list(C)},
        ],
    )
    async with session() as db:
        carriers = {
            ids[A]: ({"authorized": ["permanent"]}, {"permanent": False}),
            ids[B]: ({"authorized": ["permanent"]}, {"permanent": False}),
            ids[C]: ({"authorized": ["metric"]}, {"metric": 0}),
        }
        for row_id, (carrier, leaves) in carriers.items():
            row = await db.get(StaticRouteIntent, row_id)
            row.pending_clear = carrier
            for name, value in leaves.items():
                setattr(row, name, value)
        await db.commit()

    async with session() as db:
        device = await db.get(Device, device_id)
        live = await _sr_execution_plan(db, device, {}, job_id=None)
        rows = (
            (
                await db.execute(
                    sa.select(StaticRouteIntent)
                    .where(StaticRouteIntent.device_id == device_id)
                    .order_by(StaticRouteIntent.id)
                )
            )
            .scalars()
            .all()
        )
        promoted = classify_removal_plan(rows, [], allowed_removal_keys={}, context={})

    document = {"static_route": {EXECUTION_KEY: {"removal": _serialize_removal_plan(promoted)}}}
    hydrated = hydrate_static_route_removal_plan(document)
    assert hydrated.clears == promoted.clears
    assert live.clears == promoted.clears
    assert [(clear.key, clear.fields) for clear in promoted.clears] == [(A, ("permanent",))]
