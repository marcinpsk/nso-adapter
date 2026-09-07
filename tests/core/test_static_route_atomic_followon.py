# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""#1396 R2 chunk C5 — the replacement rides the one document.

C5 existed because atomic mode staged the static-route family as a merge-PATCH (staging
ignored ``replace``, G4), so a replacement-open device needed its own follow-on PUT after
the combined commit, outside that transaction. The aggregate sender removed the premise:
there is one PUT of one document, so the replacement lands WITH its siblings and the
documented non-transactional loss is gone.

What is pinned here is what replaced it: the one commit carries every family, a rejected
commit leaves every family pending, and the static-route bookkeeping (CAS, per-row
evidence, reader-compare, capability) still runs off that single verdict. Every case drives
the REAL ``run_apply`` against a real PostgreSQL clone; only the RESTCONF boundary is faked,
and it records every request in order, so the assertions are about the bytes that reached
NSO and their sequence.
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

#: The one instance every family is written through.
_DEVICE_URL = "http://nso/restconf/data/device-intent:device-intent=sr-atomic"
_VLAN_CONTAINER = "vlan"
_SR_CONTAINER = "static-route"


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
    """C3's recorder plus the one failure the cases below need: a rejected COMMIT.

    Dry-runs are never rejected. The failure has to be the commit itself, or failure
    localisation would attribute it to the wrong family.
    """

    def __init__(self, device_name: str, dry_run_status: int = 200):
        super().__init__(device_name, dry_run_status=dry_run_status)
        self.fail_commit = False
        #: The device error the rejection carries. The ratified refusal shape names its own
        #: family, which is what lets localisation attribute one without a dry-run sweep.
        self.reject_message = "device rejected the commit"

    async def _handle(self, method: str, url: str, content=None, headers=None):
        live = "dry-run=" not in url
        reject = live and self.fail_commit
        if reject:
            self.calls.append(
                {"method": method, "url": url, "body": json.loads(content) if content else None, "dry_run": False}
            )
            return httpx.Response(
                400,
                request=httpx.Request(method.upper(), url),
                json={
                    "ietf-restconf:errors": {"error": [{"error-message": self.reject_message}]},
                    "errors": {"error": [{"error-message": self.reject_message}]},
                },
            )
        return await super()._handle(method, url, content, headers)

    # ── views ──
    @property
    def documents(self) -> list[dict]:
        """Every device-intent instance this device really committed, in order."""
        return [c["body"][_SR_ROOT][0] for c in self.commits if _SR_ROOT in (c["body"] or {})]


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


async def seed_replacement(device_id: int, *, last_apply_at=None, last_apply_error=None) -> None:
    await seed_rows(
        device_id,
        [
            {
                "triple": B,
                "route_id": 7,
                "deployed_key": list(A),
                "last_apply_at": last_apply_at,
                "last_apply_error": last_apply_error,
            }
        ],
    )


# ── C5.1 — the replacement rides the ONE document, with its siblings ─────────


async def test_c5_1_a_replacement_rides_the_one_document_with_its_siblings(adapter_client):
    """C5.1 — what the exclusion was for is gone: one PUT IS the replace.

    Staging used to be merge-PATCH only and ignored ``replace`` (G4), so a replacement had
    to leave the combined transaction and follow it. The aggregate document is a full
    replace by construction, so the predecessor is retracted in the SAME commit that carries
    every other family, and there is no second write to sequence.
    """
    device_id = await seed_device(nso_device_name="sr-atomic", netbox_device_id=7501)
    await seed_replacement(device_id)
    await seed_vlan(device_id)
    client, rec = atomic_client(
        "sr-atomic", state=present(wire(A), device_name="sr-atomic"), section=dev_state(wire(B))
    )

    job = await run_the_apply(device_id, client)

    assert job.status == JobStatus.succeeded, job.error
    assert len(rec.documents) == 1, "one document, one commit"
    document = rec.documents[0]
    assert _VLAN_CONTAINER in document, "the sibling family rides the same transaction"
    assert document[_SR_CONTAINER]["route"] == [wire(B)], "and the replacement is in it"
    assert [c["method"] for c in rec.commits] == ["put"], "no follow-on write of any kind"


# ── C5.2 — a rejected commit leaves every family pending ────────────────────


async def test_c5_2_a_failed_combined_commit_issues_no_follow_on_put(adapter_client):
    """C5.2 — the commit is rejected ⇒ one write, nothing landed, rows pending.

    The static rows rode that very transaction, so its rollback says everything about them:
    they are untouched, retried next apply, and never stamped failed. There is no second
    write that could deliver a replacement on top of a device the commit just failed to
    change.
    """
    device_id = await seed_device(nso_device_name="sr-atomic", netbox_device_id=7502)
    await seed_replacement(device_id)
    await seed_vlan(device_id)
    client, rec = atomic_client(
        "sr-atomic", state=present(wire(A), device_name="sr-atomic"), section=dev_state(wire(B))
    )
    rec.fail_commit = True

    job = await run_the_apply(device_id, client)

    assert job.status == JobStatus.failed
    assert len(rec.commits) == 1, "one rejected write, and no retry behind it"
    ((applied, error),) = await static_rows(device_id)
    assert applied is None, "nothing landed"
    assert error["code"] == "nso_put_failed", "the family rode the rejected commit, so it records it"
    assert await deployed_keys(device_id) == {B: list(A)}, "the replacement stays open"
    assert outcomes(job) == {B: "apply_failed"}


async def test_localized_refusal_replaces_previous_errors_on_other_families(adapter_client):
    """A refused transaction replaces stale errors on every affected row."""
    device_id = await seed_device(nso_device_name="sr-atomic", netbox_device_id=7504)
    stale = {"code": "internal", "message": "a previous apply failed", "detail": {}}
    await seed_replacement(device_id, last_apply_error=stale)
    await seed_vlan(device_id)
    client, rec = atomic_client(
        "sr-atomic", state=present(wire(A), device_name="sr-atomic"), section=dev_state(wire(B))
    )
    rec.fail_commit = True
    rec.reject_message = "device-intent: refused [family=vlan field=vlan-id]: unsupported"

    job = await run_the_apply(device_id, client)

    entry = job.result["static_route_results"][0]
    assert entry["outcome"] == "apply_failed"
    assert entry["error"] is not None
    assert "vlan" in entry["error"]["message"]
    assert await static_rows(device_id) == [(None, entry["error"])]


# ── C5.3 — a rejected commit fails EVERY family, not just the static rows ────


async def test_c5_3_a_rejected_commit_fails_every_family_together(adapter_client):
    """C5.3 — the documented non-transactional loss is gone with the follow-on.

    A rejected follow-on used to fail the static rows while the sibling stayed applied,
    because the combined commit had already landed. One transaction removes the seam: the
    device took nothing, so no family may be stamped as though it had.
    """
    device_id = await seed_device(nso_device_name="sr-atomic", netbox_device_id=7503)
    await seed_replacement(device_id)
    await seed_vlan(device_id)
    client, rec = atomic_client(
        "sr-atomic", state=present(wire(A), device_name="sr-atomic"), section=dev_state(wire(B))
    )
    rec.fail_commit = True

    job = await run_the_apply(device_id, client)

    assert job.status == JobStatus.failed
    assert len(rec.commits) == 1
    ((vlan_applied, vlan_error),) = await vlan_rows(device_id)
    assert vlan_applied is None, "the sibling family landed nothing either"
    assert vlan_error["code"] == "nso_put_failed", "and it records the same one rejection"
    assert await deployed_keys(device_id) == {B: list(A)}, "a refused commit closes nothing"
    assert outcomes(job) == {B: "apply_failed"}


# ── C5.4 — an ordinary apply rides the same one document ────────────────────


async def test_c5_4_an_apply_with_no_replacement_open_rides_the_same_document(adapter_client):
    """C5.4 — the exclusion was scoped to PUT mode; there is no mode left to scope it to.

    Excluding the family unconditionally would have dropped every ordinary static-route
    apply out of the one-transaction guarantee. It rides that guarantee now whether or not a
    replacement is open, and the retention read runs either way because the document may
    still owe a carrier's entry.
    """
    device_id = await seed_device(nso_device_name="sr-atomic", netbox_device_id=7504)
    await seed_rows(device_id, [{"triple": B, "route_id": 7, "deployed_key": list(B)}])
    await seed_vlan(device_id)
    client, rec = atomic_client(
        "sr-atomic", state=present(wire(B), device_name="sr-atomic"), section=dev_state(wire(B))
    )

    job = await run_the_apply(device_id, client)

    assert job.status == JobStatus.succeeded, job.error
    assert len(rec.documents) == 1
    assert _SR_CONTAINER in rec.documents[0] and _VLAN_CONTAINER in rec.documents[0]


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


# ── C2's hand-off: a force=False pass with nothing eligible ─────────────────


async def test_a_replacement_is_delivered_even_when_the_eligible_list_is_empty(adapter_client):
    """C1.4's rule at the sender: ``any_eligible`` comes from the PLAN, not the eligible list.

    ``force=False`` on a replacement-open row with a clean ``last_apply_at`` leaves the
    eligible list empty while the body still has every accepted row to send and a
    predecessor to retract. Deriving "anything to do" from the eligible list would take the
    all-zero early success and leave the predecessor on the device for ever. Unreachable in
    production only because the worker passes ``force=True``; the structure must not depend
    on that.
    """
    device_id = await seed_device(nso_device_name="sr-atomic", netbox_device_id=7507)
    await seed_replacement(device_id, last_apply_at=_NOW)
    client, rec = atomic_client(
        "sr-atomic", state=present(wire(A), device_name="sr-atomic"), section=dev_state(wire(B))
    )

    job = await run_the_apply(device_id, client, force=False)

    assert job.status == JobStatus.succeeded, job.error
    assert rec.documents[0][_SR_CONTAINER]["route"] == [wire(B)], "the replacement was delivered"
    assert job.result["static_route_count_by_outcome"] != {"in_sync": 0, "apply_failed": 0}, (
        "an all-zero result after a real write is the false no-op C1.4 forbids"
    )


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


# ── the excluded scope keeps its capability bookkeeping (codex P2) ───────────


async def seed_capability_gap(device_id: int) -> None:
    """A stale apply-sourced ``unsupported`` for static_route, as a failed apply records it."""
    from nso_adapter.store.models import Device, DeviceCapability

    async with session() as db:
        device = await db.get(Device, device_id)
        device.ned_id = "vendor-cli-1.0"
        device.sw_version = "1.0.0"
        db.add(
            DeviceCapability(
                ned_id="vendor-cli-1.0",
                sw_version="1.0.0",
                scope="static_route",
                name="static_route",
                status="unsupported",
                detail="an earlier apply was rejected",
                source="apply",
            )
        )
        await db.commit()


async def capability_scopes(device_id: int) -> list[str]:
    from nso_adapter.store.models import DeviceCapability

    async with session() as db:
        rows = (await db.execute(select(DeviceCapability))).scalars().all()
        return [r.scope for r in rows if r.status in ("unsupported", "skipped")]


@pytest.mark.parametrize("put_fails", [False, True], ids=["clean_commit", "rejected_commit"])
async def test_a_clean_commit_clears_the_stale_capability_for_every_family_it_carried(adapter_client, put_fails):
    """A clean commit proves every family in the document applies on this ``(ned, sw)``.

    A stale apply-sourced ``unsupported`` for static_route would otherwise stick forever — a
    probe cannot downgrade an apply-sourced row, so ``/apply/preflight`` would keep warning
    about a family that now applies cleanly. A REJECTED commit proves nothing and clears
    nothing.
    """
    device_id = await seed_device(nso_device_name="sr-atomic", netbox_device_id=7509 + int(put_fails))
    await seed_replacement(device_id)
    await seed_vlan(device_id)
    await seed_capability_gap(device_id)
    client, rec = atomic_client(
        "sr-atomic", state=present(wire(A), device_name="sr-atomic"), section=dev_state(wire(B))
    )
    rec.fail_commit = put_fails

    job = await run_the_apply(device_id, client)

    assert job.status == (JobStatus.failed if put_fails else JobStatus.succeeded), job.error
    assert await capability_scopes(device_id) == (["static_route"] if put_fails else [])
