# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""#1396 R2 chunk C1 — planner, renderer, guard snapshot and claim/job_id threading.

Pins C1.1-C1.9 of the R2 brief. Every plan case runs against a real PostgreSQL clone and
real ``StaticRouteIntent`` / ``StaticRouteTombstone`` rows; the NSO client is the only
fake, and the guard cases use the same spec'd fake the shipped guard tests use.
"""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
import sqlalchemy as sa

from nso_adapter.core import removal as removal_mod
from nso_adapter.core.claim import ClaimLostError, ClaimRegistration, acquire_claim, lock_claim
from nso_adapter.core.static_route_plan import (
    SR_CLEAR_FIELDS,
    _serialize_apply_plan,
    fence_open,
    hydrate_static_route_apply_plan,
    replacement_open,
)
from nso_adapter.nso.apply import static_route_entry
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
    """Classify the device's live rows the way authorization freezes them.

    The eligible list is returned beside the plan because several cases pin that the BODY is
    no longer a function of it: one document carries every accepted row, so an eligible-only
    body would retract the accepted-and-clean siblings.
    """
    from nso_adapter.core.apply import _collect_eligible
    from nso_adapter.core.static_route_plan import classify_apply_plan
    from nso_adapter.store.models import StaticRouteIntent, StaticRouteTombstone

    async with session() as db:
        rows = list(
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
        tombstones = list(
            (
                await db.execute(
                    sa.select(StaticRouteTombstone)
                    .where(StaticRouteTombstone.device_id == device_id)
                    .order_by(StaticRouteTombstone.id)
                )
            )
            .scalars()
            .all()
        )
        eligible = await _collect_eligible(db, StaticRouteIntent, device_id, force)
        return classify_apply_plan(rows, tombstones, device_id=device_id), [r.id for r in eligible]


def _triples(rows) -> set[tuple]:
    return {(r.vrf, r.prefix, r.next_hop) for r in rows}


# ── C1.1 / C1.2 — the mode predicate ─────────────────────────────────────────


async def test_c1_1_the_fence_no_longer_decides_whether_a_replacement_needs_proof(adapter_client):
    """C1.1 — the fence gated a transport choice that no longer exists.

    It forbade PUT mode so a device whose triples were never correlated with a NetBox route
    pk could not claim deletion authority. One document drops the predecessor either way, so
    a fence-dependent record would only drop the PROOF requirement on the devices least able
    to justify the write. The fence still decides whether a removed row earns a deletion
    record, at the intent endpoint, and that is untouched.
    """
    device_id = await seed_device(nso_device_name="sr-plan-fence", netbox_device_id=7001)
    await _seed_rows(
        device_id,
        [
            {"triple": A, "route_id": None},
            {"triple": B, "route_id": 2, "deployed_key": list(C)},
        ],
    )
    assert fence_open([]) is True, "the predicate itself is unchanged"
    async with session() as db:
        from nso_adapter.store.models import StaticRouteIntent

        seeded = list(
            (await db.execute(sa.select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id)))
            .scalars()
            .all()
        )
    assert fence_open(seeded) is False, "the seeded NULL route_id shuts this device's fence"
    plan, _ = await _plan(device_id)
    assert plan.mode == "PUT", "a shut fence cannot make an undelivered identity edit unprovable"
    assert C in plan.allowed, "the body drops the predecessor, so the authority must name it"

    # Discriminating variant: backfilling the NULL changes nothing about this record.
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


async def test_c1_2b_classification_is_pure_and_the_worker_refuses_an_unprovable_replacement(
    adapter_client, monkeypatch
):
    """§4.4 — a destructive replace whose proof is unavailable must not run, and the REFUSAL
    is the worker's, not the classifier's.

    There is one transport now, so there is no weaker mode to fall back to: classification
    stays a pure function of the rows (the same plan with verification on or off), and
    ``_refuse_unverifiable_recorded_put`` fails the job before anything is sent.
    """
    from nso_adapter.core.apply import _refuse_unverifiable_recorded_put
    from nso_adapter.core.claim import JobError
    from nso_adapter.nso import apply as nso_apply

    device_id = await seed_device(nso_device_name="sr-plan-noverify", netbox_device_id=7003)
    await _seed_rows(device_id, [{"triple": B, "route_id": 2, "deployed_key": list(A)}])
    monkeypatch.setattr(nso_apply, "VERIFY_AFTER_APPLY", False)
    plan, _ = await _plan(device_id)
    assert plan.mode == "PUT", "the plan records that this document delivers a replacement"
    assert A in plan.allowed

    generation = SimpleNamespace(
        device_id=device_id,
        document={
            "static_route": {
                "static_route_intent": [],
                "_execution": {
                    "context": {"ned_id": None, "dialect": "identity"},
                    "proof": {"apply": _serialize_apply_plan(plan)},
                },
            }
        },
    )
    with pytest.raises(JobError) as excinfo:
        _refuse_unverifiable_recorded_put(generation)
    assert excinfo.value.error["code"] == "static_route_put_verify_disabled"

    monkeypatch.setattr(nso_apply, "VERIFY_AFTER_APPLY", True)
    _refuse_unverifiable_recorded_put(generation)


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


async def test_c1_4b_the_body_is_every_accepted_row_even_with_no_replacement_open(adapter_client):
    """One document is complete desired state, so an eligible-only body is never built.

    Under the per-family merge this device took the eligible list verbatim. The aggregate
    replaces the whole family, so a body holding only the pending row would RETRACT the
    accepted-and-clean sibling the operator never touched.
    """
    device_id = await seed_device(nso_device_name="sr-plan-patchrows", netbox_device_id=7006)
    ids = await _seed_rows(
        device_id,
        [
            {"triple": A, "route_id": 1, "deployed_key": list(A), "last_apply_at": _NOW},
            {"triple": B, "route_id": 2},
        ],
    )
    plan, eligible = await _plan(device_id, force=False)
    assert plan.mode == "PATCH", "no row carries an undelivered identity edit"
    assert eligible == [ids[B]]
    assert [r.id for r in plan.rows] == [ids[A], ids[B]]


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


def test_static_route_projection_state_uses_the_wire_renderer_for_serialized_rows():
    """ORM and document rows compare by emitted leaves, not stored metadata."""
    from nso_adapter.core.projection import projection_row_state

    orm_row = _RenderRow(
        vrf="RED",
        prefix="10.5.0.0/24",
        next_hop="192.0.2.9",
        interface_next_hop="GigabitEthernet0/0",
        next_hop_vrf="BLUE",
        metric=10,
        permanent=True,
        tag=100,
        name="old name",
    )
    document_row = vars(orm_row) | {
        "id": 1,
        "device_id": 2,
        "route_id": 3,
        "intent_generation": 4,
        "accepted_at": "2026-08-30T12:00:00+00:00",
        "pending_clear": None,
    }
    expected = static_route_entry(orm_row)

    assert static_route_entry(document_row) == expected
    assert projection_row_state("static_route_intent", document_row) == expected

    metadata_edit = document_row | {"name": "new name", "route_id": 30, "intent_generation": 40}
    assert projection_row_state("static_route_intent", metadata_edit) == expected
    assert projection_row_state("static_route_intent", document_row | {"metric": 20}) != expected


# ── C1.7 — extra_entries ─────────────────────────────────────────────────────


def _retained_body(rows, extra_entries=None) -> dict:
    """The static-route container the sender builds: the document's rows plus retention."""
    from nso_adapter.core.apply import overlay_retained_routes
    from nso_adapter.nso.apply import _CONTEXT_FREE_EXECUTION, encode_static_route

    body = encode_static_route({"static_route_intent": rows}, _CONTEXT_FREE_EXECUTION)
    overlay_retained_routes(body, extra_entries or [])
    return body


def test_c1_7_retained_entries_ride_verbatim_and_never_override_a_rendered_row():
    """C1.7 — retention appends what the store cannot express, and loses key collisions.

    ``A'`` is a live copy of a route the store still owns; letting it win would deploy
    whatever stale leaves the device happened to hold over the accepted intent.
    """
    rendered_a = _RenderRow(prefix=A[1], next_hop=A[2], metric=10)
    stale_a = {"vrf": "", "prefix": A[1], "next-hop": A[2], "metric": 999}
    verbatim_c = {"vrf": "", "prefix": C[1], "next-hop": C[2], "tag": 7, "bfd-fast-detect": {"minimum": 50}}

    body = _retained_body([rendered_a], extra_entries=[stale_a, verbatim_c])
    assert body["route"] == [
        {"vrf": "", "prefix": A[1], "next-hop": A[2], "metric": 10},
        verbatim_c,
    ]
    # the leaf the store has no column for survived byte-for-byte
    assert body["route"][1]["bfd-fast-detect"] == {"minimum": 50}


def test_c1_7b_no_retained_entries_is_the_documents_own_body():
    body = _retained_body([_RenderRow()])
    assert body == {"route": [{"vrf": "", "prefix": "10.0.0.0/24", "next-hop": "192.0.2.1"}]}


# ── C1.8 — the guard snapshot parameter ──────────────────────────────────────


def _guard_client(service_config=None):
    from nso_adapter.nso.client import NsoClient, ServiceInstanceState

    client = AsyncMock(spec=NsoClient)
    client.service_instance_state.return_value = ServiceInstanceState(
        "absent" if service_config is None else "present", service_config
    )
    return client


class _Device:
    id = 1
    nso_device_name = "sr-guard"
    ned_id = "cisco-ios-cli-6.95"


def _instance(*keys) -> dict:
    return {
        "device": "sr-guard",
        "static-route": {"route": [{"vrf": k[0], "prefix": k[1], "next-hop": k[2]} for k in keys]},
    }


def _containers(*keys) -> dict:
    return {"static-route": {"route": [{"vrf": k[0], "prefix": k[1], "next-hop": k[2]} for k in keys]}}


@pytest.mark.parametrize(
    ("supplied", "label"),
    [(_instance(A), "a real snapshot"), (None, "the absent-service snapshot")],
)
async def test_c1_8_supplied_snapshot_suppresses_the_internal_get(supplied, label):
    """C1.8 — ``current=`` is a sentinel default, so even ``None`` suppresses the GET.

    The ``None`` case is the one a naive ``if current is None: GET`` gets wrong: ``None``
    is a valid snapshot meaning "no service instance", and re-reading it defeats the
    one-snapshot contract exactly where a second read is most likely to disagree.
    """
    client = _guard_client(_instance(("", "10.9.9.0/24", "1.2.3.4")))

    with patch("nso_adapter.nso.apply.apply_device_intent", new_callable=AsyncMock):
        await removal_mod.guarded_device_write(client, _Device(), _containers(A), allowed={}, current=supplied)
    client.service_instance_state.assert_not_awaited(), label


async def test_c1_8b_a_send_with_no_snapshot_reads_the_instance_itself():
    """A document with no static-route section takes no certified read, so the guard reads."""
    client = _guard_client(None)
    with patch("nso_adapter.nso.apply.apply_device_intent", new_callable=AsyncMock):
        await removal_mod.guarded_device_write(client, _Device(), {"vlan": {"vlan": []}}, allowed={})
    client.service_instance_state.assert_awaited_once()


async def test_c1_8c_a_supplied_snapshot_still_drives_the_guard():
    """Handing the snapshot in must not disable the collateral check."""
    client = _guard_client(None)  # would look clean if the helper re-read
    with pytest.raises(removal_mod.RemovalBlockedError) as excinfo:
        await removal_mod.guarded_device_write(client, _Device(), _containers(A), allowed={}, current=_instance(A, C))
    # Scope-qualified: the guard is device-wide, and two families both have a `host` list.
    assert excinfo.value.orphans == {"static_route/route": [["", C[1], C[2]]]}
    client.service_instance_state.assert_not_awaited()


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
    from tests.core.removal_helpers import authorize_stream

    device_id = await seed_device(nso_device_name="sr-thread", netbox_device_id=7010)
    await authorize_stream(device_id, "vlan")
    async with session() as db:
        job = await removal_mod.enqueue_removal(
            db, device_id, "vlan", marking=None, defer_retract=False, promotes=(), force=True
        )
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
    from nso_adapter.store.models import DeviceClaim, Job, JobStatus
    from tests.core.removal_helpers import authorize_stream

    device_id = await seed_device(nso_device_name="sr-revoked", netbox_device_id=7011)
    await authorize_stream(device_id, "vlan")
    async with session() as db:
        job = await removal_mod.enqueue_removal(
            db, device_id, "vlan", marking=None, defer_retract=False, promotes=(), force=True
        )
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

    recorded = generation.document["static_route"]["_execution"]["proof"]["apply"]
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

    document["static_route"]["_execution"]["proof"]["apply"]["cas"][0]["sent_triple"] = list(B[:2])

    # Pinned: the plan raises from four independent checks, and the CAS-coordinate one is a
    # plausible alternative source with no eligible rows.
    with pytest.raises(ValueError, match="must contain three values"):
        hydrate_static_route_apply_plan(document)


async def test_plan_writes_nothing(adapter_client):
    """Classification is read-only — no stamping, no consumption, no HTTP."""
    from nso_adapter.core.static_route_plan import classify_apply_plan
    from nso_adapter.store.models import StaticRouteIntent

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
        rows = list(
            (await db.execute(sa.select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id)))
            .scalars()
            .all()
        )
        classify_apply_plan(rows, [], device_id=device_id)
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


async def test_a_frozen_removal_plan_round_trips_its_clears(adapter_client):
    """Drift guard: what the creation-time classifier records is exactly what execution reads.

    There is no second classifier to drift from any more: a reissue reads the operation plane
    its own creation wrote, so the only rule left is that serialization round-trips.
    """
    from nso_adapter.core.projection import EXECUTION_KEY, snapshot_stream
    from nso_adapter.core.static_route_plan import (
        _serialize_removal_plan,
        classify_removal_plan,
        hydrate_static_route_removal_plan,
    )
    from nso_adapter.store.models import StaticRouteIntent

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
        tables = await snapshot_stream(db, device_id, "static_route")

    document = {
        "static_route": {
            **tables,
            EXECUTION_KEY: {
                "context": {"ned_id": None, "dialect": "identity"},
                "operation": {"removal": _serialize_removal_plan(promoted)},
            },
        }
    }
    hydrated = hydrate_static_route_removal_plan(document)
    assert hydrated.clears == promoted.clears
    assert [(clear.key, clear.fields) for clear in promoted.clears] == [(A, ("permanent",))]


async def test_retaining_a_replacement_row_requires_put_verification(adapter_client):
    from nso_adapter.core.generation import _retain_rows
    from nso_adapter.core.projection import EXECUTION_KEY, fragment_tables
    from nso_adapter.store.models import StaticRouteIntent
    from tests.core.projection_helpers import freeze_snapshot

    device_id = await seed_device(nso_device_name="retained-replacement")
    await _seed_rows(device_id, [{"triple": B, "route_id": 2, "deployed_key": list(A)}])
    async with session() as db:
        source = await freeze_snapshot(db, device_id, "static_route")
        await db.execute(sa.delete(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id))
        desired = await freeze_snapshot(db, device_id, "static_route")
    assert desired[EXECUTION_KEY]["proof"]["apply"]["mode"] == "PATCH"
    retained = _retain_rows(desired, fragment_tables(source), "static_route", source)
    plan = retained[EXECUTION_KEY]["proof"]["apply"]
    assert plan["allowed_removal_keys"] == [list(A)]
    assert plan["mode"] == "PUT"
