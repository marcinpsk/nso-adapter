# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""#1396 R2 chunk C4 — the removal branches, supersession, the consumption proofs.

Pins C4.1-C4.21, C4.24-C4.29, C4.31, C4.32, plus the halves the amendments reassigned here:
A2(ii) — C2.9b's detach and networked-removal arms; A2(iii) — the duplicate retract a single
authorized clear can queue; A3(i) — C3.5's carrier-owning removal counter-case; and the
device half of C1.13. The reclaimer (C4.22, C4.23) is in ``test_static_route_reclaim.py``.

Every case drives the REAL ``run_removal`` against a real PostgreSQL clone. The only fake is
the RESTCONF boundary, and it is STATEFUL: a networked PUT rewrites both the service instance
and the device section (FASTMAP-style — an entry the service used to own and no longer does is
reverted, brownfield entries the service never owned are left alone), while a ``no-networking``
PUT rewrites only the service. So "what is left on the device" is an observed fact here, not an
assertion about a call graph.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from sqlalchemy import select

from nso_adapter.core.claim import acquire_claim, release_claim
from nso_adapter.store.models import Job, JobStatus, JobType, StaticRouteIntent, StaticRouteTombstone
from tests.conftest import seed_device, session, start_job
from tests.core.test_static_route_put import A, B, C, D, seed_apply_job, seed_rows, wire

pytestmark = pytest.mark.anyio

_SR_ROOT = "static-route-reconciler:static-route-config"
_NOW = datetime(2026, 6, 1, tzinfo=UTC)


def key_of(entry: dict) -> tuple[str, str, str]:
    return (entry.get("vrf") or "", entry.get("prefix") or "", entry.get("next-hop") or "")


# ── the stateful RESTCONF substrate ──────────────────────────────────────────


class SrFake:
    """The static-route service instance and the device section, as one moving state."""

    def __init__(
        self,
        device_name: str,
        *,
        service: list[dict] | None,
        device: list[dict] | None = None,
        service_status: str = "present",
        section_status: str = "ok",
        dry_run_status: int = 200,
    ):
        self.device_name = device_name
        self.service = None if service is None else [dict(e) for e in service]
        self.device = [dict(e) for e in (device if device is not None else (service or []))]
        self.service_status = service_status
        self.section_status = section_status
        self.dry_run_status = dry_run_status
        self.calls: list[dict] = []

    # ── reads ──
    def state(self):
        from nso_adapter.nso.client import ServiceInstanceState

        if self.service_status == "inconclusive":
            return ServiceInstanceState("inconclusive", None)
        if self.service_status == "absent" or self.service is None:
            return ServiceInstanceState("absent", None)
        return ServiceInstanceState("present", {"device": self.device_name, "route": [dict(e) for e in self.service]})

    def section(self) -> dict:
        if self.section_status != "ok":
            return {"status": self.section_status}
        return {"status": "ok", "route": [dict(e) for e in self.device]}

    # ── the write boundary ──
    async def handle(self, method: str, url: str, content=None, headers=None):
        body = json.loads(content) if content else None
        dry = "dry-run=" in url
        no_net = "no-networking" in url
        self.calls.append({"method": method, "url": url, "body": body, "dry_run": dry, "no_networking": no_net})
        request = httpx.Request(method.upper(), url)
        if dry:
            if self.dry_run_status != 200:
                return httpx.Response(self.dry_run_status, request=request, json={"errors": "boom"})
            return httpx.Response(
                200,
                request=request,
                json={"dry-run-result": {"native": {"device": [{"name": self.device_name, "data": ""}]}}},
            )
        if body and _SR_ROOT in body:
            routes = [dict(e) for e in (body[_SR_ROOT][0].get("route") or [])]
            owned = {key_of(e) for e in (self.service or [])}
            new = {key_of(e) for e in routes}
            if not no_net:
                # FASTMAP's reverse diff: what the service owned and no longer asserts goes
                # away; everything else on the device stays exactly as it was.
                by_key = {key_of(e): e for e in self.device if key_of(e) not in (owned - new)}
                for entry in routes:
                    by_key[key_of(entry)] = dict(entry)
                self.device = list(by_key.values())
            self.service = routes
            self.service_status = "present"
        return httpx.Response(204, request=request, text="")

    # ── views ──
    @property
    def writes(self) -> list[dict]:
        return [c for c in self.calls if not c["dry_run"] and c["body"] and _SR_ROOT in c["body"]]

    def sent_routes(self, index: int = -1) -> list[dict]:
        return self.writes[index]["body"][_SR_ROOT][0]["route"]

    def sent_keys(self, index: int = -1) -> set[tuple[str, str, str]]:
        return {key_of(e) for e in self.sent_routes(index)}

    @property
    def service_keys(self) -> set[tuple[str, str, str]]:
        return {key_of(e) for e in (self.service or [])}

    @property
    def device_keys(self) -> set[tuple[str, str, str]]:
        return {key_of(e) for e in self.device}

    def device_entry(self, triple) -> dict | None:
        return next((e for e in self.device if key_of(e) == triple), None)


def sr_client(fake: SrFake):
    from nso_adapter.nso.client import NsoClient

    http = AsyncMock()
    for method in ("get", "put", "patch", "post"):

        def _bind(m=method):
            async def _call(url, content=None, headers=None, **kwargs):
                return await fake.handle(m, url, content, headers)

            return _call

        getattr(http, method).side_effect = _bind()

    client = MagicMock(spec=NsoClient)
    client._base = "http://nso"
    client._action_timeout = 120.0
    cm = client._client.return_value
    cm.__aenter__.return_value = http
    cm.__aexit__.return_value = False
    client.service_instance_state = AsyncMock(side_effect=lambda _path, _device: fake.state())
    # Deliberately clean-looking: a path that wrongly falls back to the UNCERTIFIED reader is
    # caught by the assertions rather than hidden by it.
    client.get_service_config = AsyncMock(return_value=None)
    client.run_device_state_read = AsyncMock(
        side_effect=lambda _d, _wires, timeout=None: {"static-route": fake.section()}
    )
    return client


# ── seeding / running ────────────────────────────────────────────────────────


async def seed_removal_job(device_id: int, context: dict) -> int:
    async with session() as db:
        # Started, at attempt 1: see seed_apply_job in test_static_route_put.
        job = Job(
            job_type=JobType.removal,
            device_id=device_id,
            status=JobStatus.running,
            run_attempt=1,
            context={"scope": "static_route", **context},
        )
        db.add(job)
        await db.commit()
        return job.id


async def seed_tomb(
    device_id: int,
    triple,
    *,
    job_id: int | None = None,
    route_id: int = 99,
    deployed_key=None,
    marking: str = "delete_origin",
) -> int:
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
            job_id=job_id,
        )
        db.add(tomb)
        await db.commit()
        return tomb.id


async def run_removal_job(device_id: int, job_id: int, client, *, reg=None, sync_from=None) -> Job:
    from nso_adapter.core.removal import run_removal

    attempt = await start_job(job_id)  # the head transition a directly-invoked runner does not do
    own_claim = reg is None
    if own_claim:
        reg = await acquire_claim(device_id, "job", job_id=job_id)
    if reg is not None and reg.run_attempt is None:
        # The worker hands the runner the attempt its execution was started at; without it
        # the removal's terminal CAS falls back to status-only and its fence is never run.
        reg.run_attempt = attempt
    sync = sync_from if sync_from is not None else AsyncMock(return_value={})
    try:
        with (
            patch("nso_adapter.core.importer.get_nso_client", return_value=client),
            patch("nso_adapter.nso.actions.sync_from", new=sync),
        ):
            await run_removal(job_id=job_id, device_id=device_id, reg=reg)
    finally:
        if own_claim:
            await release_claim(reg)
    async with session() as db:
        return await db.get(Job, job_id)


async def run_apply_job(device_id: int, client, *, reg=None) -> Job:
    from nso_adapter.core.apply import run_apply

    job_id = await seed_apply_job(device_id)
    own_claim = reg is None
    if own_claim:
        reg = await acquire_claim(device_id, "job", job_id=job_id)
    try:
        with (
            patch("nso_adapter.core.importer.get_nso_client", return_value=client),
            patch("nso_adapter.core.apply._post_apply_refresh_and_notify", new=AsyncMock()),
        ):
            await run_apply(job_id=job_id, device_id=device_id, force=True, reg=reg)
    finally:
        if own_claim:
            await release_claim(reg)
    async with session() as db:
        return await db.get(Job, job_id)


async def tombstone_ids(device_id: int) -> list[int]:
    async with session() as db:
        rows = (
            (
                await db.execute(
                    select(StaticRouteTombstone)
                    .where(StaticRouteTombstone.device_id == device_id)
                    .order_by(StaticRouteTombstone.id)
                )
            )
            .scalars()
            .all()
        )
        return [r.id for r in rows]


async def carriers(device_id: int) -> dict[tuple, dict | None]:
    async with session() as db:
        rows = (
            (await db.execute(select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id)))
            .scalars()
            .all()
        )
        return {(r.vrf, r.prefix, r.next_hop): r.pending_clear for r in rows}


async def deployed(device_id: int) -> dict[tuple, list | None]:
    async with session() as db:
        rows = (
            (await db.execute(select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id)))
            .scalars()
            .all()
        )
        return {(r.vrf, r.prefix, r.next_hop): r.deployed_key for r in rows}


# ── C4.1/C4.2 — the body is live-relative, never store-assertive ─────────────


async def test_c4_1_body_is_the_live_service_minus_the_authorized_key(adapter_client):
    """C4.1 — a store-assertive body would carry the EDITED row's new triple and drop B/C.

    Forbidden: the edited row's new triple in the body. Discriminating: the body is exactly
    the live service minus ``A``, with ``B`` and ``C`` verbatim — including the leaf the store
    has no column for.
    """
    device_id = await seed_device(nso_device_name="sr-c41", netbox_device_id=7401)
    # An unrelated EDITED row: its store triple is D, and it was last deployed as B.
    await seed_rows(device_id, [{"triple": D, "route_id": 2, "deployed_key": list(B)}])
    fake = SrFake(
        "sr-c41",
        service=[wire(A), wire(B, **{"nso-only-leaf": "keep"}), wire(C, metric=7)],
    )
    job_id = await seed_removal_job(device_id, {"removed": {"route": [list(A)]}})
    await seed_tomb(device_id, A, job_id=job_id, route_id=1)

    job = await run_removal_job(device_id, job_id, sr_client(fake))

    assert job.status == JobStatus.succeeded
    assert fake.sent_keys() == {B, C}, "the body must be the live service minus A"
    assert D not in fake.sent_keys(), "an unrelated edited row's new triple must not be forward-deployed"
    sent = {key_of(e): e for e in fake.sent_routes()}
    assert sent[B] == wire(B, **{"nso-only-leaf": "keep"})
    assert sent[C] == wire(C, metric=7)


async def test_c4_2_a_never_applied_accepted_row_is_not_in_the_body(adapter_client):
    """C4.2 — a removal must not deploy intent that no apply has ever pushed."""
    device_id = await seed_device(nso_device_name="sr-c42", netbox_device_id=7402)
    await seed_rows(device_id, [{"triple": D, "route_id": 2, "deployed_key": None}])
    fake = SrFake("sr-c42", service=[wire(A), wire(B)])
    job_id = await seed_removal_job(device_id, {"removed": {"route": [list(A)]}})
    await seed_tomb(device_id, A, job_id=job_id, route_id=1)

    await run_removal_job(device_id, job_id, sr_client(fake))

    assert fake.sent_keys() == {B}


# ── C4.3/C4.4/C4.5 — X6: authorized = {triple} ∪ {deployed_key} ──────────────


async def test_c4_3_delete_origin_drops_both_the_triple_and_the_predecessor(adapter_client):
    """C4.3 — authorizing only the triple leaves the predecessor entry service-owned forever."""
    device_id = await seed_device(nso_device_name="sr-c43", netbox_device_id=7403)
    fake = SrFake("sr-c43", service=[wire(A), wire(B), wire(C)])
    job_id = await seed_removal_job(device_id, {})
    await seed_tomb(device_id, B, job_id=job_id, route_id=1, deployed_key=list(A))

    job = await run_removal_job(device_id, job_id, sr_client(fake))

    assert job.status == JobStatus.succeeded
    assert fake.sent_keys() == {C}
    assert fake.device_keys == {C}
    assert await tombstone_ids(device_id) == []


async def test_c4_4_detach_drops_and_proves_both_markings(adapter_client):
    """C4.4 — a detach un-owns the row's triple AND its ``deployed_key``, and proves both gone."""
    device_id = await seed_device(nso_device_name="sr-c44", netbox_device_id=7404)
    fake = SrFake("sr-c44", service=[wire(A), wire(B), wire(C)])
    job_id = await seed_removal_job(device_id, {"detach": True})
    await seed_tomb(device_id, B, job_id=job_id, route_id=1, deployed_key=list(A), marking="detach")

    job = await run_removal_job(device_id, job_id, sr_client(fake))

    assert job.status == JobStatus.succeeded
    assert job.result["detach"] is True
    assert fake.writes[-1]["no_networking"] is True
    assert fake.service_keys == {C}, "both A and B must leave the SERVICE"
    assert fake.device_keys == {A, B, C}, "a detach must not touch the device — that is the point"
    assert await tombstone_ids(device_id) == []


async def test_c4_5_detach_with_a_null_deployed_key_still_drops_the_triple(adapter_client):
    """C4.5 — authorizing only ``deployed_key`` would make a NULL one un-own nothing at all."""
    device_id = await seed_device(nso_device_name="sr-c45", netbox_device_id=7405)
    fake = SrFake("sr-c45", service=[wire(A), wire(B)])
    job_id = await seed_removal_job(device_id, {"detach": True})
    await seed_tomb(device_id, A, job_id=job_id, route_id=1, deployed_key=None, marking="detach")

    job = await run_removal_job(device_id, job_id, sr_client(fake))

    assert job.status == JobStatus.succeeded
    assert fake.service_keys == {B}
    assert await tombstone_ids(device_id) == []


# ── C4.6/C4.7 — supersession is ownership, not eligibility ───────────────────


@pytest.mark.parametrize("shape", ["triple", "deployed_key"])
async def test_c4_6_a_reclaimed_key_is_not_dropped(adapter_client, shape):
    """C4.6 — another route claims ``A``; deleting it would retract a live route's config."""
    device_id = await seed_device(nso_device_name=f"sr-c46-{shape}", netbox_device_id=7406 + len(shape))
    spec = {"triple": A, "route_id": 2} if shape == "triple" else {"triple": D, "route_id": 2, "deployed_key": list(A)}
    await seed_rows(device_id, [spec])
    fake = SrFake(f"sr-c46-{shape}", service=[wire(A), wire(B)])
    job_id = await seed_removal_job(device_id, {})
    # Two authorized keys, so the PUT still runs and the reclaimed one is visibly preserved.
    await seed_tomb(device_id, A, job_id=job_id, route_id=1)
    await seed_tomb(device_id, B, job_id=job_id, route_id=3)

    job = await run_removal_job(device_id, job_id, sr_client(fake))

    assert job.status == JobStatus.succeeded
    assert fake.sent_keys() == {A}, "A is claimed by a live row — it is no longer this deletion's to drop"
    assert fake.device_keys == {A}


async def test_c4_7_a_fully_superseded_removal_issues_no_http_at_all(adapter_client):
    """C4.7 — consumption by supersession, not by failure: no PUT, no read, job succeeds."""
    device_id = await seed_device(nso_device_name="sr-c47", netbox_device_id=7407)
    await seed_rows(device_id, [{"triple": A, "route_id": 2}])
    fake = SrFake("sr-c47", service=[wire(A), wire(B)])
    client = sr_client(fake)
    job_id = await seed_removal_job(device_id, {})
    await seed_tomb(device_id, A, job_id=job_id, route_id=1)

    job = await run_removal_job(device_id, job_id, client)

    assert job.status == JobStatus.succeeded
    assert job.result["superseded"] is True
    assert fake.calls == [], "a superseded removal must issue no HTTP at all"
    client.service_instance_state.assert_not_awaited()
    assert await tombstone_ids(device_id) == []


# ── C4.8/C4.9 — an unproven deletion keeps its carrier and fails ─────────────


async def test_c4_8_service_absent_still_runs_the_proof(adapter_client):
    """C4.8 — ``absent`` proves the SERVICE has no instance, never that the device is clean."""
    device_id = await seed_device(nso_device_name="sr-c48", netbox_device_id=7408)
    fake = SrFake("sr-c48", service=None, device=[wire(A)])
    job_id = await seed_removal_job(device_id, {})
    tomb = await seed_tomb(device_id, A, job_id=job_id, route_id=1)

    job = await run_removal_job(device_id, job_id, sr_client(fake))

    assert fake.writes == [], "no service instance, nothing to PUT"
    assert job.result["residue_check"] == "found"
    assert job.status == JobStatus.failed
    assert await tombstone_ids(device_id) == [tomb], "consuming here strands the route permanently"


async def test_c4_9_residue_found_fails_the_job_and_the_next_sweep_reissues(adapter_client):
    """C4.9 — a ``succeeded`` job would make the tombstone permanently un-sweepable (G17)."""
    from nso_adapter.core.tombstone_sweep import sweep_tombstones

    device_id = await seed_device(nso_device_name="sr-c49", netbox_device_id=7409)
    fake = SrFake("sr-c49", service=[wire(A), wire(B)])
    job_id = await seed_removal_job(device_id, {})
    tomb = await seed_tomb(device_id, A, job_id=job_id, route_id=1)
    # The device keeps A after the commit — FASTMAP held an entry carrying a foreign leaf.
    client = sr_client(fake)
    original = fake.section

    def _sticky_section():
        section = original()
        if section.get("status") == "ok" and A not in {key_of(e) for e in section.get("route", [])}:
            section["route"] = [*section.get("route", []), wire(A)]
        return section

    fake.section = _sticky_section

    job = await run_removal_job(device_id, job_id, client)

    assert job.status == JobStatus.failed
    assert job.error["code"] == "static_route_removal_residue_found"
    assert await tombstone_ids(device_id) == [tomb]
    assert await sweep_tombstones() == 1, "a failed owner makes the tombstone eligible again"


# ── C4.10/C4.11 — the detach proof's other two halves ────────────────────────


async def test_c4_10_detach_whose_service_still_holds_the_key_fails(adapter_client):
    """C4.10 — the un-own did not happen; succeeding would throw away the only record of it."""
    device_id = await seed_device(nso_device_name="sr-c410", netbox_device_id=7410)
    fake = SrFake("sr-c410", service=[wire(A), wire(B)])
    job_id = await seed_removal_job(device_id, {"detach": True})
    tomb = await seed_tomb(device_id, A, job_id=job_id, route_id=1, marking="detach")
    client = sr_client(fake)
    # The post-commit read still shows A: the service did not really drop it.
    fake.state = lambda: __import__("nso_adapter.nso.client", fromlist=["ServiceInstanceState"]).ServiceInstanceState(
        "present", {"device": "sr-c410", "route": [wire(A), wire(B)]}
    )

    job = await run_removal_job(device_id, job_id, client)

    assert job.status == JobStatus.failed
    assert job.error["code"] == "static_route_removal_unproven"
    assert await tombstone_ids(device_id) == [tomb]


async def test_c4_11_detach_whose_sync_from_never_lands_fails(adapter_client):
    """C4.11 — G11's unconditional success is gone: CDB keeps the reverse diff, so it is unproven."""
    device_id = await seed_device(nso_device_name="sr-c411", netbox_device_id=7411)
    fake = SrFake("sr-c411", service=[wire(A), wire(B)])
    job_id = await seed_removal_job(device_id, {"detach": True})
    tomb = await seed_tomb(device_id, A, job_id=job_id, route_id=1, marking="detach")

    job = await run_removal_job(
        device_id, job_id, sr_client(fake), sync_from=AsyncMock(side_effect=RuntimeError("read eof"))
    )

    assert job.status == JobStatus.failed
    assert job.result["sync_from"] == "failed"
    assert await tombstone_ids(device_id) == [tomb]


# ── C4.12/C4.13 — the terminal transaction ───────────────────────────────────


async def test_c4_12_consumption_and_status_are_one_transaction(adapter_client):
    """C4.12 — a crash between the tombstone delete and the status leaves BOTH undone.

    Driven by making the terminal COMMIT itself fail: the runner must not fall back to a
    second terminal write, because the first may have landed (§4.6).
    """
    from nso_adapter.core.claim import BookkeepingOutcomeUnknown, ClaimOutcome

    device_id = await seed_device(nso_device_name="sr-c412", netbox_device_id=7412)
    fake = SrFake("sr-c412", service=[wire(A), wire(B)])
    job_id = await seed_removal_job(device_id, {})
    tomb = await seed_tomb(device_id, A, job_id=job_id, route_id=1)

    async def _unknown(db):
        await db.rollback()
        return ClaimOutcome.OUTCOME_UNKNOWN

    with (
        patch("nso_adapter.core.claim._commit_outcome", new=_unknown),
        pytest.raises(BookkeepingOutcomeUnknown),
    ):
        await run_removal_job(device_id, job_id, sr_client(fake))

    async with session() as db:
        job = await db.get(Job, job_id)
    assert job.status == JobStatus.running, "no second terminal write may land over a maybe-committed one"
    assert await tombstone_ids(device_id) == [tomb], "both or neither"


async def test_c4_13_a_tombstone_written_during_the_call_survives(adapter_client):
    """C4.13 — only the SNAPSHOTTED ids die; nothing has proven anything about a newer one."""
    device_id = await seed_device(nso_device_name="sr-c413", netbox_device_id=7413)
    fake = SrFake("sr-c413", service=[wire(A), wire(B)])
    job_id = await seed_removal_job(device_id, {})
    owned = await seed_tomb(device_id, A, job_id=job_id, route_id=1)
    client = sr_client(fake)
    late: dict[str, int] = {}

    handle = fake.handle

    async def _insert_then_handle(method, url, content=None, headers=None):
        if not late:
            late["id"] = await seed_tomb(device_id, C, job_id=job_id, route_id=5)
        return await handle(method, url, content, headers)

    fake.handle = _insert_then_handle

    job = await run_removal_job(device_id, job_id, client)

    assert job.status == JobStatus.succeeded
    assert await tombstone_ids(device_id) == [late["id"]]
    assert owned not in await tombstone_ids(device_id)


# ── C4.14 — force is untouched ───────────────────────────────────────────────


async def test_c4_14_force_removal_keeps_the_store_assertive_flush(adapter_client):
    """C4.14 — a force-removal carries no tombstone and no ``removed``; live-relative would no-op."""
    device_id = await seed_device(nso_device_name="sr-c414", netbox_device_id=7414)
    await seed_rows(device_id, [{"triple": B, "route_id": 2}])
    fake = SrFake("sr-c414", service=[wire(A), wire(B)])
    job_id = await seed_removal_job(device_id, {"force": True})

    job = await run_removal_job(device_id, job_id, sr_client(fake))

    assert job.status == JobStatus.succeeded
    assert fake.sent_keys() == {B}, "the store-assertive body flushes the reviewed orphan A"
    assert fake.device_keys == {B}
    assert job.result["residue_check"] == "unsupported"


# ── C4.15/C4.16/C4.17 — the leaf-level clear overlay ─────────────────────────


async def test_c4_15_a_pure_clear_deletes_only_the_named_leaf(adapter_client):
    """C4.15 — a whole-row store overlay would deploy the co-edited ``tag`` in the same push.

    Forbidden: the clear lost; ``tag 200`` deployed; the unrelated row force-deployed; the
    replacement-open row's new identity deployed from a removal job.
    """
    device_id = await seed_device(nso_device_name="sr-c415", netbox_device_id=7415)
    ids = await seed_rows(
        device_id,
        [
            # X: metric cleared in the store, tag co-edited to 200 (still live as 100).
            {"triple": A, "route_id": 1, "pending_clear": {"authorized": ["metric"], "store_only": []}},
            {"triple": B, "route_id": 2},  # Y — unrelated, edited
            # Z — replacement open AND cleared: waits for the Apply PUT.
            {
                "triple": C,
                "route_id": 3,
                "deployed_key": list(D),
                "pending_clear": {"authorized": ["metric"], "store_only": []},
            },
        ],
    )
    async with session() as db:
        row = await db.get(StaticRouteIntent, ids[A])
        row.tag = 200
        await db.commit()
    fake = SrFake(
        "sr-c415",
        service=[wire(A, metric=10, tag=100), wire(B, metric=5), wire(D, metric=1)],
    )
    job_id = await seed_removal_job(device_id, {})

    job = await run_removal_job(device_id, job_id, sr_client(fake))

    assert job.status == JobStatus.succeeded
    sent = {key_of(e): e for e in fake.sent_routes()}
    assert sent[A] == wire(A, tag=100), "only metric is deleted — the live tag 100 survives"
    assert sent[B] == wire(B, metric=5), "an unrelated row is carried verbatim, never re-rendered"
    assert sent[D] == wire(D, metric=1), "a replacement-open row's live entry is untouched"
    assert C not in sent, "a removal must never deploy a replacement-open row's new identity"
    assert (await carriers(device_id))[A] is None, "X's carrier is consumed by per-field evidence"
    assert (await carriers(device_id))[C] == {"authorized": ["metric"], "store_only": []}, "Z waits for the PUT"


async def test_c4_16_a_mixed_delete_origin_and_clear_delivers_both(adapter_client):
    """C4.16 — one PUT: ``A`` gone AND ``B``'s cleared leaf gone, every other ``B`` leaf live."""
    device_id = await seed_device(nso_device_name="sr-c416", netbox_device_id=7416)
    await seed_rows(
        device_id, [{"triple": B, "route_id": 2, "pending_clear": {"authorized": ["metric"], "store_only": []}}]
    )
    fake = SrFake("sr-c416", service=[wire(A), wire(B, metric=10, tag=7)])
    job_id = await seed_removal_job(device_id, {})
    await seed_tomb(device_id, A, job_id=job_id, route_id=1)

    job = await run_removal_job(device_id, job_id, sr_client(fake))

    assert job.status == JobStatus.succeeded
    assert len(fake.writes) == 1, "one push, not two"
    assert fake.sent_keys() == {B}
    assert fake.sent_routes()[0] == wire(B, tag=7)
    assert await tombstone_ids(device_id) == []
    assert (await carriers(device_id))[B] is None


async def test_c4_17_the_same_mixed_push_with_the_fence_shut(adapter_client):
    """C4.17 — no tombstone at all: the same outcome via the ``context["removed"]`` fallback."""
    device_id = await seed_device(nso_device_name="sr-c417", netbox_device_id=7417)
    # route_id NULL on a surviving row = the fence is shut, so no tombstone was ever written.
    await seed_rows(
        device_id, [{"triple": B, "route_id": None, "pending_clear": {"authorized": ["metric"], "store_only": []}}]
    )
    fake = SrFake("sr-c417", service=[wire(A), wire(B, metric=10, tag=7)])
    job_id = await seed_removal_job(device_id, {"removed": {"route": [list(A)]}})

    job = await run_removal_job(device_id, job_id, sr_client(fake))

    assert job.status == JobStatus.succeeded
    assert fake.sent_routes() == [wire(B, tag=7)]
    assert fake.device_keys == {B}
    assert (await carriers(device_id))[B] is None


# ── C4.18/C4.18b — the carrier survives a detach and a sweeper re-issue ──────


async def test_c4_18_a_clear_riding_a_detach_is_deferred_not_delivered(adapter_client):
    """C4.18 — a ``no-networking`` PUT can never deliver a clear; it must not try, or lose it."""
    device_id = await seed_device(nso_device_name="sr-c418", netbox_device_id=7418)
    await seed_rows(
        device_id, [{"triple": B, "route_id": 2, "pending_clear": {"authorized": ["metric"], "store_only": []}}]
    )
    fake = SrFake("sr-c418", service=[wire(A), wire(B, metric=10)])
    job_id = await seed_removal_job(device_id, {"detach": True})
    await seed_tomb(device_id, A, job_id=job_id, route_id=1, marking="detach")

    job = await run_removal_job(device_id, job_id, sr_client(fake))

    assert job.status == JobStatus.succeeded
    assert fake.writes[-1]["no_networking"] is True
    assert fake.sent_routes() == [wire(B, metric=10)], "the detach body leaves the cleared leaf alone"
    assert (await carriers(device_id))[B] == {"authorized": ["metric"], "store_only": []}

    # A later networked retract — the job §4.11's retry path enqueues — delivers it.
    follow = await seed_removal_job(device_id, {})
    job2 = await run_removal_job(device_id, follow, sr_client(fake))
    assert job2.status == JobStatus.succeeded
    assert fake.sent_routes() == [wire(B)]
    assert (await carriers(device_id))[B] is None


async def test_c4_18b_a_sweeper_reissued_job_rederives_the_clear(adapter_client):
    """C4.18b — the sweeper rebuilds a context from the tombstone alone (G37); the carrier survives.

    A job-context snapshot of the clear cannot survive this, which is why the carrier is store
    state rather than a context key.
    """
    from nso_adapter.core.tombstone_sweep import sweep_tombstones

    device_id = await seed_device(nso_device_name="sr-c418b", netbox_device_id=7419)
    await seed_rows(
        device_id, [{"triple": B, "route_id": 2, "pending_clear": {"authorized": ["metric"], "store_only": []}}]
    )
    fake = SrFake("sr-c418b", service=[wire(A), wire(B, metric=10)])
    failed = await seed_removal_job(device_id, {})
    await seed_tomb(device_id, A, job_id=failed, route_id=1)
    async with session() as db:
        job = await db.get(Job, failed)
        job.status = JobStatus.failed
        await db.commit()

    assert await sweep_tombstones() == 1
    async with session() as db:
        reissued = (
            (
                await db.execute(
                    select(Job.id)
                    .where(Job.device_id == device_id, Job.status == JobStatus.queued)
                    .order_by(Job.id.desc())
                )
            )
            .scalars()
            .first()
        )
        assert "cleared" not in (await db.get(Job, reissued)).context

    job2 = await run_removal_job(device_id, reissued, sr_client(fake))

    assert job2.status == JobStatus.succeeded
    assert fake.sent_routes() == [wire(B)], "the re-issued job re-derives the clear from the carrier"
    assert (await carriers(device_id))[B] is None
    assert await tombstone_ids(device_id) == []


# ── C4.19/C4.20/C4.21 — fence-shut and crash-retry shapes ───────────────────


async def test_c4_19_fence_shut_removal_is_live_relative(adapter_client):
    """C4.19 — R1 BLOCKED on ``C``; R2 retains it and drops only what was authorized."""
    device_id = await seed_device(nso_device_name="sr-c419", netbox_device_id=7420)
    await seed_rows(device_id, [{"triple": B, "route_id": None}])
    fake = SrFake("sr-c419", service=[wire(A), wire(B), wire(C, metric=3)])
    job_id = await seed_removal_job(device_id, {"removed": {"route": [list(A)]}})

    job = await run_removal_job(device_id, job_id, sr_client(fake))

    assert job.status == JobStatus.succeeded
    assert fake.sent_keys() == {B, C}
    assert job.result["retained_orphans"] == [list(C)], "C is retained, and named as an orphan"


async def test_c4_20_a_requeued_delete_origin_whose_put_already_landed(adapter_client):
    """C4.20 — the key is already gone; the retry must be a no-op PUT, not a second failure."""
    device_id = await seed_device(nso_device_name="sr-c420", netbox_device_id=7421)
    fake = SrFake("sr-c420", service=[wire(B)], device=[wire(B)])
    job_id = await seed_removal_job(device_id, {})
    await seed_tomb(device_id, A, job_id=job_id, route_id=1)

    job = await run_removal_job(device_id, job_id, sr_client(fake))

    assert job.status == JobStatus.succeeded
    assert fake.sent_keys() == {B}
    assert job.result["residue_check"] == "clean"
    assert await tombstone_ids(device_id) == []


async def test_c4_21_a_requeued_detach_whose_put_removed_the_instance(adapter_client):
    """C4.21 — demanding a literal 2xx makes this retry permanently unprovable, forever re-swept."""
    device_id = await seed_device(nso_device_name="sr-c421", netbox_device_id=7422)
    fake = SrFake("sr-c421", service=None, device=[wire(A)])
    job_id = await seed_removal_job(device_id, {"detach": True})
    await seed_tomb(device_id, A, job_id=job_id, route_id=1, marking="detach")

    job = await run_removal_job(device_id, job_id, sr_client(fake))

    assert fake.writes == [], "no instance to PUT"
    assert job.status == JobStatus.succeeded
    assert await tombstone_ids(device_id) == []
    assert fake.device_keys == {A}, "the device keeps it — that is what a detach means"


# ── C4.24 — apply × removal, both orders, both markings, literal vectors ─────


@pytest.mark.parametrize("marking", ["delete_origin", "detach"])
@pytest.mark.parametrize("order", ["removal_first", "apply_first"])
async def test_c4_24_apply_and_removal_interleaving_literal_vector(adapter_client, marking, order):
    """C4.24 — one stated initial state, a LITERAL final vector per (order × marking).

    Initial: service and device both hold ``A(metric 10)``, ``B``, ``C``. Store holds ``B``
    (deployed ``B``) and ``C`` (deployed ``D`` — a replacement the apply owes), plus one
    tombstone for ``A``. Asserting only that the two orders agree would pass on a pair of
    no-ops; every value below is written out instead.
    """
    tag = f"{order}-{marking}"
    device_id = await seed_device(nso_device_name=f"sr-c424-{tag}", netbox_device_id=7430 + hash(tag) % 300)
    await seed_rows(
        device_id,
        [
            {"triple": B, "route_id": 2, "deployed_key": list(B)},
            {"triple": C, "route_id": 3, "deployed_key": list(D)},
        ],
    )
    fake = SrFake(f"sr-c424-{tag}", service=[wire(A, metric=10), wire(B), wire(C)])
    removal_id = await seed_removal_job(device_id, {"detach": True} if marking == "detach" else {})
    await seed_tomb(device_id, A, job_id=removal_id, route_id=1, marking=marking)

    if order == "removal_first":
        removal = await run_removal_job(device_id, removal_id, sr_client(fake))
        apply_job = await run_apply_job(device_id, sr_client(fake))
    else:
        apply_job = await run_apply_job(device_id, sr_client(fake))
        removal = await run_removal_job(device_id, removal_id, sr_client(fake))

    assert removal.status == JobStatus.succeeded
    assert apply_job.status == JobStatus.succeeded
    assert fake.service_keys == {B, C}
    assert fake.device_keys == ({A, B, C} if marking == "detach" else {B, C})
    assert await deployed(device_id) == {B: list(B), C: list(C)}
    assert await tombstone_ids(device_id) == []
    assert {tuple(r["key"]): r["outcome"] for r in apply_job.result["static_route_results"]} == {
        B: "in_sync",
        C: "in_sync",
    }


# ── C4.25 — every clear is re-validated at execution ────────────────────────


@pytest.mark.parametrize("case", ["reset", "deleted", "moved", "reclaimed", "unchanged"])
async def test_c4_25_a_queued_clear_is_revalidated_under_the_claim(adapter_client, case):
    """C4.25 — a snapshot of the clear would re-break a re-set value or edit another route."""
    device_id = await seed_device(nso_device_name=f"sr-c425-{case}", netbox_device_id=7800 + len(case))
    carrier = {"authorized": ["metric"], "store_only": []}
    if case == "deleted":
        rows: list[dict] = []
    elif case == "moved":
        rows = [{"triple": D, "route_id": 2, "pending_clear": carrier}]
    elif case == "reclaimed":
        rows = [{"triple": D, "route_id": 2, "pending_clear": carrier}, {"triple": A, "route_id": 4}]
    else:
        rows = [{"triple": A, "route_id": 2, "pending_clear": carrier}]
    ids = await seed_rows(device_id, rows)
    if case == "reset":
        async with session() as db:
            row = await db.get(StaticRouteIntent, ids[A])
            row.metric = 20
            await db.commit()

    fake = SrFake(f"sr-c425-{case}", service=[wire(A, metric=10), wire(B)])
    job_id = await seed_removal_job(device_id, {})
    await seed_tomb(device_id, B, job_id=job_id, route_id=1)

    job = await run_removal_job(device_id, job_id, sr_client(fake))

    assert job.status == JobStatus.succeeded
    entry = {key_of(e): e for e in fake.sent_routes()}[A]
    if case == "unchanged":
        assert entry == wire(A), "the valid clear is applied and its carrier consumed"
        assert (await carriers(device_id))[A] is None
    else:
        assert entry == wire(A, metric=10), f"{case}: the queued clear must be discarded at execution"


# ── C4.26/C4.27 — an uncertified read never becomes a PUT (A2(ii) = C2.9b) ──


@pytest.mark.parametrize("branch", ["detach", "networked"])
async def test_c2_9b_an_inconclusive_read_issues_no_removal_put(adapter_client, branch):
    """C2.9b's remaining two arms (A2(ii)) + C4.26/C4.27 — zero PUTs, nothing consumed.

    A 200 with an unrecognized root reads as "empty" to ``get_service_config`` (G31). Building
    a live-relative body from it drops every entry it was meant to retain — and then verifies
    cleanly.
    """
    device_id = await seed_device(nso_device_name=f"sr-c29b-{branch}", netbox_device_id=7860 + len(branch))
    fake = SrFake(
        f"sr-c29b-{branch}",
        service=[wire(A), wire(B)],
        service_status="inconclusive",
    )
    job_id = await seed_removal_job(device_id, {"detach": True} if branch == "detach" else {})
    tomb = await seed_tomb(
        device_id, A, job_id=job_id, route_id=1, marking="detach" if branch == "detach" else "delete_origin"
    )

    job = await run_removal_job(device_id, job_id, sr_client(fake))

    assert fake.writes == [], "no destructive PUT may be built on an uncertified read"
    assert job.status == JobStatus.failed
    assert await tombstone_ids(device_id) == [tomb]


# ── C4.28 / C3.5 — the carrier-owning removal counter-case (A3(i)) ──────────


@pytest.mark.parametrize(
    "signal",
    ["residue_unsupported", "residue_error", "verify_inconclusive", "verify_disabled", "field_unsupported"],
)
async def test_c3_5_c4_28_a_carrier_owning_removal_fails_on_any_inconclusive_signal(adapter_client, signal):
    """C3.5 + C4.28 — OQ-R2-1's apply-side leniency does NOT apply here.

    An apply that cannot prove itself succeeds and records ``unproven``. A removal that owns a
    carrier cannot: a ``succeeded`` job makes its tombstone permanently un-sweepable (G17), so
    it fails and is retried until the deletion is proven.
    """
    device_id = await seed_device(nso_device_name=f"sr-c35-{signal}", netbox_device_id=7900 + len(signal))
    section_status = {"residue_unsupported": "unsupported", "residue_error": "error"}.get(signal, "ok")
    clear_case = signal == "field_unsupported"
    if clear_case:
        await seed_rows(
            device_id, [{"triple": B, "route_id": 2, "pending_clear": {"authorized": ["metric"], "store_only": []}}]
        )
        section_status = "unsupported"
    fake = SrFake(
        f"sr-c35-{signal}",
        service=[wire(A), wire(B, metric=10)],
        section_status=section_status,
        dry_run_status=500 if signal == "verify_inconclusive" else 200,
    )
    job_id = await seed_removal_job(device_id, {})
    tomb = None if clear_case else await seed_tomb(device_id, A, job_id=job_id, route_id=1)
    if clear_case:
        # A pure-clear job owns a carrier too — the pending_clear entry itself.
        pass

    ctx = patch("nso_adapter.nso.apply.VERIFY_AFTER_APPLY", False) if signal == "verify_disabled" else None
    if ctx is None:
        job = await run_removal_job(device_id, job_id, sr_client(fake))
    else:
        with ctx:
            job = await run_removal_job(device_id, job_id, sr_client(fake))

    assert job.status == JobStatus.failed, f"{signal}: an unproven carrier-owning removal must fail"
    assert job.error["code"] == "static_route_removal_unproven"
    if clear_case:
        assert (await carriers(device_id))[B] == {"authorized": ["metric"], "store_only": []}
    else:
        assert await tombstone_ids(device_id) == [tomb]


async def test_c3_5_a_carrierless_removal_keeps_the_apply_side_leniency(adapter_client):
    """The counter-case's counter-case: with nothing to strand, an inconclusive proof succeeds."""
    device_id = await seed_device(nso_device_name="sr-c35-none", netbox_device_id=7950)
    await seed_rows(device_id, [{"triple": B, "route_id": None}])
    fake = SrFake("sr-c35-none", service=[wire(A), wire(B)], section_status="unsupported")
    job_id = await seed_removal_job(device_id, {"removed": {"route": [list(A)]}})

    job = await run_removal_job(device_id, job_id, sr_client(fake))

    assert job.status == JobStatus.succeeded
    assert job.result["unproven"] is True
    assert job.result["residue_check"] == "unsupported"


async def test_c3_5_any_carried_static_route_generation_makes_unproven_removal_fail(adapter_client):
    """A coalesced non-static generation cannot hide a static-route promotion obligation."""
    from nso_adapter.core.generation import digest_document
    from nso_adapter.store.models import DeploymentGeneration, GenerationMode

    device_id = await seed_device(nso_device_name="sr-c35-multi-generation", netbox_device_id=None)
    await seed_rows(device_id, [{"triple": B, "route_id": None}])
    job_id = await seed_removal_job(device_id, {"removed": {"route": [list(A)]}})
    mode = GenerationMode.networked
    async with session() as db:
        for seq, stream_revisions in ((1, {"vlan": 1}), (2, {"static_route": 1})):
            db.add(
                DeploymentGeneration(
                    device_id=device_id,
                    seq=seq,
                    mode=mode,
                    document={},
                    digest=digest_document(mode, {}, {}),
                    allowed_removal_keys={},
                    source_push_seq={},
                    stream_revisions=stream_revisions,
                    removal_context={"scope": "static_route", "removed": {"route": [list(A)]}},
                    job_id=job_id,
                )
            )
        await db.commit()

    fake = SrFake("sr-c35-multi-generation", service=[wire(A), wire(B)], section_status="unsupported")
    job = await run_removal_job(device_id, job_id, sr_client(fake))

    assert job.status == JobStatus.failed
    assert job.error["code"] == "static_route_removal_unproven"


# ── C4.29 — the pure-clear removal's terminal commit is guarded ──────────────


async def test_c4_29_a_revoked_claim_stops_a_pure_clear_terminal_commit(adapter_client):
    """C4.29 — this job owns no tombstone, so it never passes through ``delete_tombstones``' guard."""
    from nso_adapter.core.claim import ClaimLostError
    from nso_adapter.store.models import DeviceClaim

    device_id = await seed_device(nso_device_name="sr-c429", netbox_device_id=7960)
    await seed_rows(
        device_id, [{"triple": B, "route_id": 2, "pending_clear": {"authorized": ["metric"], "store_only": []}}]
    )
    fake = SrFake("sr-c429", service=[wire(B, metric=10)])
    job_id = await seed_removal_job(device_id, {})
    reg = await acquire_claim(device_id, "job", job_id=job_id)

    section = fake.section

    async def _revoke(*args, **kwargs):
        from sqlalchemy import delete as sa_delete

        async with session() as db:
            await db.execute(sa_delete(DeviceClaim).where(DeviceClaim.device_id == device_id))
            await db.commit()
        return {"static-route": section()}

    client = sr_client(fake)
    client.run_device_state_read = AsyncMock(side_effect=_revoke)

    with pytest.raises(ClaimLostError):
        await run_removal_job(device_id, job_id, client, reg=reg)

    assert (await carriers(device_id))[B] == {"authorized": ["metric"], "store_only": []}
    async with session() as db:
        assert (await db.get(Job, job_id)).status == JobStatus.running


async def test_c4_29b_consuming_a_tombstone_refuses_an_unregistered_claim(adapter_client):
    """G19/§4.7 — carrier deletion is never made unguarded to keep a caller convenient."""
    device_id = await seed_device(nso_device_name="sr-c429b", netbox_device_id=7961)
    fake = SrFake("sr-c429b", service=[wire(A), wire(B)])
    job_id = await seed_removal_job(device_id, {})
    tomb = await seed_tomb(device_id, A, job_id=job_id, route_id=1)

    from nso_adapter.core.claim import ClaimRegistration

    job = await run_removal_job(device_id, job_id, sr_client(fake), reg=ClaimRegistration(device_id, None))

    assert job.status == JobStatus.failed
    assert "REGISTERED claim" in job.error["message"]
    assert await tombstone_ids(device_id) == [tomb]


async def test_a_superseded_run_attempt_refuses_the_removal_terminal_write(adapter_client):
    """S1 — the removal's terminal CAS is fenced by the run ATTEMPT, not by the status alone.

    Recovery re-dispatched this job to a successor while this run was in flight, so the row
    stands at attempt 2 when the run reaches terminalize. The write must be refused and the
    carrier this transaction consumed must roll back with it, ready for the owning execution.
    """
    device_id = await seed_device(nso_device_name="sr-attempt-fence", netbox_device_id=7962)
    fake = SrFake("sr-attempt-fence", service=[wire(A), wire(B)])
    job_id = await seed_removal_job(device_id, {})
    tomb = await seed_tomb(device_id, A, job_id=job_id, route_id=1)

    section = fake.section

    async def _supersede(*args, **kwargs):
        from sqlalchemy import update as sa_update

        async with session() as db:
            await db.execute(sa_update(Job).where(Job.id == job_id).values(run_attempt=2))
            await db.commit()
        return {"static-route": section()}

    client = sr_client(fake)
    client.run_device_state_read = AsyncMock(side_effect=_supersede)

    job = await run_removal_job(device_id, job_id, client)

    client.run_device_state_read.assert_awaited_once()
    assert job.status == JobStatus.running, "a superseded execution must not write a terminal status"
    assert job.result is None and job.settle_seq is None
    assert await tombstone_ids(device_id) == [tomb], "the refused write must discard its consumption"


# ── C4.31/C4.32 and C1.13's device half ─────────────────────────────────────


async def test_c4_31_retained_orphans_names_exactly_the_unclaimed_keys(adapter_client):
    """C4.31 — the guard cannot block here, so the event is the operator's only signal."""
    device_id = await seed_device(nso_device_name="sr-c431", netbox_device_id=7970)
    await seed_rows(device_id, [{"triple": B, "route_id": 2}, {"triple": D, "route_id": 3, "deployed_key": list(C)}])
    fake = SrFake("sr-c431", service=[wire(A), wire(B), wire(C), wire(D)])
    job_id = await seed_removal_job(device_id, {})
    await seed_tomb(device_id, A, job_id=job_id, route_id=1)

    job = await run_removal_job(device_id, job_id, sr_client(fake))

    assert job.status == JobStatus.succeeded
    # B is a live triple, C is a live deployed_key — neither is an orphan. Nothing else is
    # retained, so the event must not fire at all here.
    assert "retained_orphans" not in job.result
    assert fake.sent_keys() == {B, C, D}


async def test_c4_31b_an_unclaimed_retained_key_is_named(adapter_client):
    device_id = await seed_device(nso_device_name="sr-c431b", netbox_device_id=7971)
    await seed_rows(device_id, [{"triple": B, "route_id": 2}])
    fake = SrFake("sr-c431b", service=[wire(A), wire(B), wire(C), wire(D)])
    job_id = await seed_removal_job(device_id, {})
    await seed_tomb(device_id, A, job_id=job_id, route_id=1)

    job = await run_removal_job(device_id, job_id, sr_client(fake))

    assert job.result["retained_orphans"] == [list(C), list(D)]


@pytest.mark.parametrize(
    ("live", "consumable"),
    [
        ({"metric": 0}, False),
        ({"tag": 0}, False),
        ({"permanent": False}, True),
        ({}, True),
    ],
)
async def test_c4_32_per_field_neutrality_on_the_removal_path(adapter_client, live, consumable):
    """C4.32 — ``0`` is a real value; a falsiness check would eat the carrier while it is live.

    Driven through the NETWORKED REMOVAL path (C2.11 is the apply-side twin), with the device
    section deliberately NOT following the PUT: the write succeeded and the key proof is clean,
    so per-field evidence is the only thing that can distinguish these cases.
    """
    field = next(iter(live), "permanent")
    tag = f"{field}-{list(live.values())[0] if live else 'absent'}"
    device_id = await seed_device(nso_device_name=f"sr-c432-{tag}", netbox_device_id=7980 + len(tag))
    store_field = {"metric": "metric", "tag": "tag", "permanent": "permanent"}[field]
    await seed_rows(
        device_id, [{"triple": B, "route_id": 2, "pending_clear": {"authorized": [store_field], "store_only": []}}]
    )
    fake = SrFake("sr-c432", service=[wire(B, metric=10)], device=[wire(B, **live)])
    job_id = await seed_removal_job(device_id, {})
    # Freeze the device view: the PUT would otherwise overwrite the leaf we are probing.
    frozen = [dict(e) for e in fake.device]
    fake.section = lambda: {"status": "ok", "route": [dict(e) for e in frozen]}

    job = await run_removal_job(device_id, job_id, sr_client(fake))

    assert job.status == (JobStatus.succeeded if consumable else JobStatus.failed)
    assert ((await carriers(device_id))[B] is None) is consumable


async def test_c1_13_device_half_a_store_only_clear_is_invisible_to_a_removal(adapter_client):
    """C1.13 (device half) — the store-only push must not cause a device write, via ANY job.

    An unrelated networked delete-origin removal for route Y reads the carrier; if it saw the
    ``store_only`` half it would delete X's live ``metric`` — breaking both the store-only
    contract and "a removal never forward-deploys store intent".
    """
    device_id = await seed_device(nso_device_name="sr-c113", netbox_device_id=7990)
    await seed_rows(
        device_id, [{"triple": B, "route_id": 2, "pending_clear": {"authorized": [], "store_only": ["metric"]}}]
    )
    fake = SrFake("sr-c113", service=[wire(A), wire(B, metric=10)])
    job_id = await seed_removal_job(device_id, {})
    await seed_tomb(device_id, A, job_id=job_id, route_id=1)

    job = await run_removal_job(device_id, job_id, sr_client(fake))

    assert job.status == JobStatus.succeeded
    assert fake.sent_routes() == [wire(B, metric=10)], "X's live metric must be untouched"
    assert fake.device_entry(B) == wire(B, metric=10)
    assert (await carriers(device_id))[B] == {"authorized": [], "store_only": ["metric"]}


# ── A2(iii) — one authorized clear can queue TWO removals ───────────────────


async def test_a2_iii_the_duplicate_retract_is_a_no_op_via_supersession(adapter_client):
    """A2(iii) — the endpoint queues one retract and a PATCH-mode apply queues another.

    The second must find nothing left to deliver and no-op through supersession: no PUT, no
    failure, and the ``removal_superseded`` record.
    """
    device_id = await seed_device(nso_device_name="sr-a2iii", netbox_device_id=7995)
    await seed_rows(
        device_id, [{"triple": B, "route_id": 2, "pending_clear": {"authorized": ["metric"], "store_only": []}}]
    )
    fake = SrFake("sr-a2iii", service=[wire(B, metric=10)])
    first = await seed_removal_job(device_id, {})
    second = await seed_removal_job(device_id, {})

    job1 = await run_removal_job(device_id, first, sr_client(fake))
    writes_after_first = len(fake.writes)
    job2 = await run_removal_job(device_id, second, sr_client(fake))

    assert job1.status == JobStatus.succeeded
    assert (await carriers(device_id))[B] is None
    assert job2.status == JobStatus.succeeded
    assert job2.result["superseded"] is True
    assert len(fake.writes) == writes_after_first, "the duplicate must issue no PUT"


# ── codex C4-F1 — a body with nothing left to deliver must not be PUT ────────


async def test_a_clear_that_live_validation_rejects_issues_no_put(adapter_client):
    """codex C4-F1 — the store-side clear passes, the LIVE entry is gone, nothing is authorized.

    ``candidate_clears`` is what gets us past the pre-read no-op branch, but the live entry
    for that row is absent, so the body delivers nothing. PUT-replacing the whole instance
    anyway is a device commit with no authority behind it — and it would retract any service
    change made between the snapshot and the write.
    """
    device_id = await seed_device(nso_device_name="sr-f1", netbox_device_id=7996)
    # The row's identity moved to D; the service still holds only A and B.
    await seed_rows(
        device_id, [{"triple": D, "route_id": 2, "pending_clear": {"authorized": ["metric"], "store_only": []}}]
    )
    fake = SrFake("sr-f1", service=[wire(A), wire(B, metric=10)])
    job_id = await seed_removal_job(device_id, {})

    job = await run_removal_job(device_id, job_id, sr_client(fake))

    assert fake.writes == [], "nothing authorized and nothing deliverable ⇒ no device commit"
    assert job.status == JobStatus.succeeded
    assert job.result["superseded"] is True
    assert (await carriers(device_id))[D] == {"authorized": ["metric"], "store_only": []}
