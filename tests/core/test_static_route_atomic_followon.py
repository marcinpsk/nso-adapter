# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""#1396 R2 chunk C5 — atomic exclusion and the follow-on PUT.

Pins C5.1-C5.5 plus the empty-combined-body case C2 handed over.

This is A3(iv)'s acceptance criterion: until C5, atomic mode staged the static-route scope
as a merge-PATCH (staging ignores ``replace``, G4) and no PUT ever followed, so an atomic
apply on a replacement-open device honestly reported ``unproven`` and closed nothing. Every
case here drives the REAL ``run_apply`` with ``NSO_ADAPTER_ATOMIC_APPLY=1`` against a real
PostgreSQL clone; only the RESTCONF boundary is faked, and it records every request in
order, so the assertions are about the bytes that reached NSO and their sequence.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from sqlalchemy import select

from nso_adapter.store.models import JobStatus
from tests.conftest import seed_device, session
from tests.core.test_static_route_proof import _ProofRecorder, dev_state, outcomes, run_the_apply
from tests.core.test_static_route_put import _SR_ROOT, A, B, deployed_keys, present, seed_rows, wire

pytestmark = pytest.mark.anyio

_NOW = datetime(2026, 6, 1, tzinfo=UTC)

_DATA_URL = "http://nso/restconf/data"
_VLAN_ROOT = "vlan-reconciler:vlan-config"


@pytest.fixture(autouse=True)
def _atomic_on(monkeypatch):
    """Every case in this module is about the atomic implementation."""
    monkeypatch.setenv("NSO_ADAPTER_ATOMIC_APPLY", "1")


# ── seeding ──────────────────────────────────────────────────────────────────


async def seed_vlan(device_id: int, vlan_id: int = 100) -> int:
    """A sibling scope, so the combined transaction is a real one to be excluded FROM."""
    from nso_adapter.store.models import VlanIntent

    async with session() as db:
        row = VlanIntent(device_id=device_id, vlan_id=vlan_id, name="probe", accepted_at=_NOW)
        db.add(row)
        await db.commit()
        return row.id


async def vlan_rows(device_id: int) -> list:
    from nso_adapter.store.models import VlanIntent

    async with session() as db:
        rows = (await db.execute(select(VlanIntent).where(VlanIntent.device_id == device_id))).scalars().all()
        return [(r.last_apply_at, r.last_apply_error) for r in rows]


async def static_rows(device_id: int) -> list:
    from nso_adapter.store.models import StaticRouteIntent

    async with session() as db:
        rows = (
            (await db.execute(select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id)))
            .scalars()
            .all()
        )
        return [(r.last_apply_at, r.last_apply_error) for r in rows]


# ── the recorded RESTCONF boundary ───────────────────────────────────────────


class _AtomicRecorder(_ProofRecorder):
    """C3's recorder plus the two failures C5.2/C5.3 need to tell apart.

    ``fail_combined`` rejects the combined ``/restconf/data`` commit; ``fail_static_put``
    rejects the follow-on keyed PUT. Dry-runs are never rejected — the failure has to be the
    COMMIT, or atomic-failure localisation would attribute it to the wrong place.
    """

    def __init__(self, device_name: str, dry_run_status: int = 200):
        super().__init__(device_name, dry_run_status=dry_run_status)
        self.fail_combined = False
        self.fail_static_put = False

    async def _handle(self, method: str, url: str, content=None, headers=None):
        live = "dry-run=" not in url
        combined = url.split("?")[0] == _DATA_URL
        reject = (live and combined and self.fail_combined) or (live and method == "put" and self.fail_static_put)
        if reject:
            self.calls.append(
                {"method": method, "url": url, "body": json.loads(content) if content else None, "dry_run": False}
            )
            return httpx.Response(
                400,
                request=httpx.Request(method.upper(), url),
                json={"errors": {"error": [{"error-message": "device rejected the commit"}]}},
            )
        return await super()._handle(method, url, content, headers)

    # ── views ──
    @property
    def combined_commits(self) -> list[dict]:
        return [c for c in self.commits if c["url"].split("?")[0] == _DATA_URL]


def atomic_client(device_name: str, *, state, section: dict, dry_run_status: int = 200):
    """A spec'd NsoClient with both planes faked and EVERY family answerable.

    The sibling scope's device-state family answers ``unsupported`` — the reader-compare
    verdict "the NED exports no such section", which proves nothing and fails nothing. Only
    the static-route section carries real content, because that is the plane under test.
    """
    from nso_adapter.nso.client import NsoClient

    rec = _AtomicRecorder(device_name, dry_run_status=dry_run_status)
    http = AsyncMock()
    for method in ("get", "put", "patch", "post"):

        def _bind(m=method):
            async def _call(url, content=None, headers=None, **kwargs):
                return await rec._handle(m, url, content, headers)

            return _call

        getattr(http, method).side_effect = _bind()

    async def _device_state(_device_name, wires, timeout=None):
        return {w: (section if w == "static-route" else {"status": "unsupported"}) for w in wires}

    client = MagicMock(spec=NsoClient)
    client._base = "http://nso"
    client._action_timeout = 120.0
    cm = client._client.return_value
    cm.__aenter__.return_value = http
    cm.__aexit__.return_value = False
    client.service_instance_state = AsyncMock(return_value=state)
    client.get_service_config = AsyncMock(return_value=None)
    client.sync_from = AsyncMock(return_value=None)
    client.run_device_state_read = AsyncMock(side_effect=_device_state)
    return client, rec


async def seed_replacement(device_id: int, *, last_apply_at=None) -> None:
    await seed_rows(device_id, [{"triple": B, "route_id": 7, "deployed_key": list(A), "last_apply_at": last_apply_at}])


# ── C5.1 — the replacement leaves the combined transaction, and a PUT follows ─


async def test_c5_1_a_put_mode_plan_is_excluded_from_the_combined_patch(adapter_client):
    """C5.1 — atomic on, replacement open.

    Staging is merge-PATCH only and explicitly ignores ``replace`` (G4). A staged PUT-mode
    plan therefore adds ``B`` to the combined PATCH and leaves the predecessor ``A`` live
    while the job reports success — the exact false green R2 exists to prevent. The scope
    must be absent from ``modules`` and delivered by its own PUT afterwards.
    """
    device_id = await seed_device(nso_device_name="sr-atomic", netbox_device_id=7501)
    await seed_replacement(device_id)
    await seed_vlan(device_id)
    client, rec = atomic_client(
        "sr-atomic", state=present(wire(A), device_name="sr-atomic"), section=dev_state(wire(B))
    )

    job = await run_the_apply(device_id, client)

    assert job.status == JobStatus.succeeded, job.error
    combined = rec.combined_commits
    assert len(combined) == 1, "the sibling scope still commits atomically"
    assert _VLAN_ROOT in combined[0]["body"], "the combined transaction must be a REAL one"
    assert _SR_ROOT not in combined[0]["body"], "a merge-PATCH cannot deliver a replacement"

    puts = rec.sr_commits("put")
    assert len(puts) == 1, "the replacement is delivered by exactly one follow-on PUT"
    assert rec.routes(puts[0]) == [wire(B)]
    assert rec.commits.index(combined[0]) < rec.commits.index(puts[0]), "the follow-on runs AFTER the commit"
    assert rec.sr_commits("patch") == [], "and nothing merged the static scope on the side"


# ── C5.2 — a rolled-back combined commit issues no follow-on ─────────────────


async def test_c5_2_a_failed_combined_commit_issues_no_follow_on_put(adapter_client):
    """C5.2 — the combined commit fails ⇒ no static HTTP at all, rows pending.

    The static rows were excluded from that transaction, so its rollback says nothing about
    them — but PUTting them anyway would deliver a replacement on top of a device the rest
    of the apply just failed to change. They are treated exactly as a non-offending scope
    is: untouched, retried next apply, never stamped failed.
    """
    device_id = await seed_device(nso_device_name="sr-atomic", netbox_device_id=7502)
    await seed_replacement(device_id)
    await seed_vlan(device_id)
    client, rec = atomic_client(
        "sr-atomic", state=present(wire(A), device_name="sr-atomic"), section=dev_state(wire(B))
    )
    rec.fail_combined = True

    job = await run_the_apply(device_id, client)

    assert job.status == JobStatus.failed
    assert rec.sr_commits() == [], "no static-route write may follow a rolled-back combined commit"
    assert client.service_instance_state.await_count == 0, "not even the pre-PUT snapshot read"
    assert await static_rows(device_id) == [(None, None)], "pending, NOT stamped failed"
    assert await deployed_keys(device_id) == {B: list(A)}, "the replacement stays open"
    assert outcomes(job) == {B: "unproven"}, "nothing was delivered, so nothing is proven"


# ── C5.3 — a failed follow-on fails the job, and only the static rows ────────


async def test_c5_3_a_failed_follow_on_put_fails_the_job_and_only_its_own_rows(adapter_client):
    """C5.3 — combined commit succeeds, the follow-on PUT is rejected.

    Reporting ``succeeded`` here would settle a replacement the device refused. The
    documented loss is the other half: the combined commit has already landed, so the
    sibling scope stays applied — the replacement is NOT transactional with it.
    """
    device_id = await seed_device(nso_device_name="sr-atomic", netbox_device_id=7503)
    await seed_replacement(device_id)
    await seed_vlan(device_id)
    client, rec = atomic_client(
        "sr-atomic", state=present(wire(A), device_name="sr-atomic"), section=dev_state(wire(B))
    )
    rec.fail_static_put = True

    job = await run_the_apply(device_id, client)

    assert job.status == JobStatus.failed
    assert len(rec.sr_commits("put")) == 1
    ((sr_applied, sr_error),) = await static_rows(device_id)
    assert sr_applied is None and sr_error["code"] == "nso_put_failed", sr_error
    ((vlan_applied, vlan_error),) = await vlan_rows(device_id)
    assert vlan_applied is not None and vlan_error is None, "only the static rows failed"
    assert job.result["vlan_count_by_outcome"] == {"in_sync": 1, "apply_failed": 0}
    assert job.result["static_route_count_by_outcome"] == {"in_sync": 0, "apply_failed": 1}
    assert await deployed_keys(device_id) == {B: list(A)}, "a refused PUT closes nothing"
    assert outcomes(job) == {B: "apply_failed"}


# ── C5.4 — no replacement open ⇒ the scope is staged as before ───────────────


async def test_c5_4_without_a_replacement_the_scope_still_rides_the_combined_patch(adapter_client):
    """C5.4 — the exclusion is scoped to PUT mode, not to static routes.

    Excluding the scope unconditionally would drop every ordinary static-route apply out of
    the one-transaction guarantee for nothing.
    """
    device_id = await seed_device(nso_device_name="sr-atomic", netbox_device_id=7504)
    await seed_rows(device_id, [{"triple": B, "route_id": 7, "deployed_key": list(B)}])
    await seed_vlan(device_id)
    client, rec = atomic_client(
        "sr-atomic", state=present(wire(B), device_name="sr-atomic"), section=dev_state(wire(B))
    )

    job = await run_the_apply(device_id, client)

    assert job.status == JobStatus.succeeded, job.error
    combined = rec.combined_commits
    assert len(combined) == 1
    assert _SR_ROOT in combined[0]["body"], "a PATCH-mode plan is staged exactly as before"
    assert rec.sr_commits("put") == [], "and no PUT follows"
    assert client.service_instance_state.await_count == 0, "a merge needs no snapshot"


# ── C5.5 — the clean end-to-end record ──────────────────────────────────────


async def test_c5_5_a_clean_follow_on_records_the_full_static_route_bookkeeping(adapter_client):
    """C5.5 — A3(iv)'s acceptance criterion, end to end.

    Before C5 this same setup produced no PUT, no ``reader_compare["static_route"]`` (the
    scope was staged but its verdict rode the combined commit's) and a per-route
    ``unproven`` that closed nothing. All three must now be the real thing: the PUT
    delivered the store's desired state, the per-row evidence proved ``B`` present, and the
    CAS moved ``deployed_key`` off the predecessor.
    """
    device_id = await seed_device(nso_device_name="sr-atomic", netbox_device_id=7505)
    await seed_replacement(device_id)
    await seed_vlan(device_id)
    client, rec = atomic_client(
        "sr-atomic", state=present(wire(A), device_name="sr-atomic"), section=dev_state(wire(B))
    )

    job = await run_the_apply(device_id, client)

    assert job.status == JobStatus.succeeded, job.error
    assert rec.routes(rec.sr_commits("put")[0]) == [wire(B)]
    assert job.result["reader_compare"]["static_route"] == "ok"
    assert outcomes(job) == {B: "in_sync"}
    assert await deployed_keys(device_id) == {B: list(B)}, "the replacement is CLOSED — the C3-declined P1 is dead"
    assert job.result["static_route_count_by_outcome"] == {"in_sync": 1, "apply_failed": 0}
    result = job.result["static_route_results"][0]
    assert result["route_id"] == 7 and result["key"] == list(B) and result["fingerprint"]


async def test_c5_5b_an_inconclusive_follow_on_verify_closes_nothing_and_still_succeeds(adapter_client):
    """The follow-on carries §4.4's proof rule too, not just the happy path.

    Its verdict is its OWN — the combined commit's verify says nothing about a PUT that had
    not happened yet. An implementation that reused the combined verdict would CAS a
    never-proven replacement here (§6/OQ-R2-1 still keeps the job green).
    """
    device_id = await seed_device(nso_device_name="sr-atomic", netbox_device_id=7506)
    await seed_replacement(device_id)
    await seed_vlan(device_id)
    client, rec = atomic_client(
        "sr-atomic",
        state=present(wire(A), device_name="sr-atomic"),
        section=dev_state(wire(B)),
        dry_run_status=500,
    )

    job = await run_the_apply(device_id, client)

    assert job.status == JobStatus.succeeded, job.error
    assert len(rec.sr_commits("put")) == 1
    assert await deployed_keys(device_id) == {B: list(A)}, "an unproven replacement stays open"
    assert outcomes(job) == {B: "unproven"}


# ── C2's hand-off: PUT mode must never produce an empty combined PATCH ───────


async def test_the_excluded_scope_never_leaves_an_empty_combined_patch(adapter_client):
    """C2's hand-off, made unreachable rather than merely unreached.

    ``any_eligible`` comes from ``plan.rows`` (C1.4), so a ``force=False`` apply of a
    replacement-open row with a clean ``last_apply_at`` admits the atomic branch with an
    EMPTY eligible list. Once C5 also takes the static scope out of the staging, nothing at
    all is left to stage — and an empty ``/restconf/data`` PATCH would be a write whose
    verify verdict belongs to no scope. Unreachable in production only because the worker
    passes ``force=True``; the structure must not depend on that.
    """
    device_id = await seed_device(nso_device_name="sr-atomic", netbox_device_id=7507)
    await seed_replacement(device_id, last_apply_at=_NOW)
    client, rec = atomic_client(
        "sr-atomic", state=present(wire(A), device_name="sr-atomic"), section=dev_state(wire(B))
    )

    job = await run_the_apply(device_id, client, force=False)

    assert job.status == JobStatus.succeeded, job.error
    assert rec.combined_commits == [], "an empty combined body must not be committed"
    assert rec.routes(rec.sr_commits("put")[0]) == [wire(B)], "the follow-on still delivers the replacement"
    assert await deployed_keys(device_id) == {B: list(B)}
    assert outcomes(job) == {B: "in_sync"}


async def test_a_reader_compare_miss_on_the_follow_on_fails_only_the_missing_row(adapter_client):
    """The follow-on's per-row evidence is real evidence, not a copy of the aggregate.

    The atomic path's own reader-compare covers the STAGED scopes; the excluded scope needs
    its own, or a silently-dropped route would be certified ``in_sync`` on this path while
    the per-scope loop catches it (#26's silent-drop class, one implementation only).
    """
    device_id = await seed_device(nso_device_name="sr-atomic", netbox_device_id=7508)
    await seed_replacement(device_id)
    await seed_vlan(device_id)
    # the device-state section does NOT carry B: the writer accepted the commit and dropped it
    client, rec = atomic_client("sr-atomic", state=present(wire(A), device_name="sr-atomic"), section=dev_state())

    job = await run_the_apply(device_id, client)

    assert job.status == JobStatus.failed
    assert len(rec.sr_commits("put")) == 1
    assert job.result["reader_compare"]["static_route"] == "missing"
    assert outcomes(job) == {B: "apply_failed"}
    assert await deployed_keys(device_id) == {B: list(A)}, "a dropped route closes no replacement"
