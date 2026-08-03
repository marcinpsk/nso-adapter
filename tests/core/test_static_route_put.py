# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""#1396 R2 chunk C2 — the guarded static-route PUT-replace and preview parity.

Pins C2.1-C2.9b plus the enqueue half of C2.10 and the A1 amendment's discriminating case.
Every case drives the REAL ``run_apply`` / ``collect_apply_diff`` against a real PostgreSQL
clone; only the RESTCONF boundary is faked, and it records every request it is handed, so
the assertions are about the bytes that would reach NSO rather than about a call graph.

Not in this chunk (C3/C4 own them): the proof verdict, residue enforcement, the CAS,
per-route ``static_route_results`` outcomes and carrier consumption. Where a C2 matrix row
names one of those, the half that exists here is pinned and the rest is named in the report.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from sqlalchemy import select

from nso_adapter.store.models import Job, JobStatus, JobType
from tests.conftest import VALID_TOKEN, seed_device, session

pytestmark = pytest.mark.anyio

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

_NOW = datetime(2026, 6, 1, tzinfo=UTC)

A = ("", "10.0.0.0/24", "192.0.2.1")
B = ("", "10.0.1.0/24", "192.0.2.2")
C = ("", "10.0.2.0/24", "192.0.2.3")
D = ("", "10.0.3.0/24", "192.0.2.4")

_SR_ROOT = "static-route-reconciler:static-route-config"
_SR_PATH = "/restconf/data/static-route-reconciler:static-route-config"


def wire(triple, **extra) -> dict:
    vrf, prefix, next_hop = triple
    return {"vrf": vrf, "prefix": prefix, "next-hop": next_hop, **extra}


# ── seeding ──────────────────────────────────────────────────────────────────


async def seed_rows(device_id: int, specs: list[dict]) -> dict[tuple, int]:
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
                pending_clear=spec.get("pending_clear"),
            )
            db.add(row)
            await db.flush()
            out[spec["triple"]] = row.id
        await db.commit()
    return out


async def seed_tombstone(device_id: int, triple: tuple, *, route_id=99, deployed_key=None, marking="delete_origin"):
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


async def seed_apply_job(device_id: int) -> int:
    async with session() as db:
        job = Job(job_type=JobType.apply, device_id=device_id, status=JobStatus.queued)
        db.add(job)
        await db.commit()
        return job.id


async def read_job(job_id: int) -> Job:
    async with session() as db:
        return await db.get(Job, job_id)


async def removal_contexts(device_id: int) -> list[dict]:
    async with session() as db:
        rows = (
            (
                await db.execute(
                    select(Job).where(Job.device_id == device_id, Job.job_type == JobType.removal).order_by(Job.id)
                )
            )
            .scalars()
            .all()
        )
        return [r.context for r in rows]


async def deployed_keys(device_id: int) -> dict[tuple, list | None]:
    from nso_adapter.store.models import StaticRouteIntent

    async with session() as db:
        rows = (
            (await db.execute(select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id)))
            .scalars()
            .all()
        )
        return {(r.vrf, r.prefix, r.next_hop): r.deployed_key for r in rows}


async def tombstone_ids(device_id: int) -> list[int]:
    from nso_adapter.store.models import StaticRouteTombstone

    async with session() as db:
        rows = (
            (
                await db.execute(
                    select(StaticRouteTombstone).where(StaticRouteTombstone.device_id == device_id).order_by("id")
                )
            )
            .scalars()
            .all()
        )
        return [r.id for r in rows]


# ── the recorded RESTCONF boundary ───────────────────────────────────────────


class _Recorder:
    """Every request the apply/preview hands to the RESTCONF pool, in order."""

    def __init__(self, device_name: str):
        self.device_name = device_name
        self.calls: list[dict] = []

    async def _handle(self, method: str, url: str, content=None, headers=None):
        body = json.loads(content) if content else None
        dry = "dry-run=" in url
        self.calls.append({"method": method, "url": url, "body": body, "dry_run": dry})
        request = httpx.Request(method.upper(), url)
        if dry:
            # A conclusive, empty native delta: the post-apply verify passes.
            return httpx.Response(
                200,
                request=request,
                json={"dry-run-result": {"native": {"device": [{"name": self.device_name, "data": ""}]}}},
            )
        return httpx.Response(204, request=request, text="")

    # ── views over the recording ──
    @property
    def commits(self) -> list[dict]:
        """Real (non-dry-run) writes only."""
        return [c for c in self.calls if not c["dry_run"]]

    def sr_commits(self, method: str | None = None) -> list[dict]:
        out = [c for c in self.commits if _SR_ROOT in (c["body"] or {})]
        return [c for c in out if method is None or c["method"] == method]

    def sr_payloads(self, *, dry_run: bool) -> list[dict]:
        return [c["body"] for c in self.calls if c["dry_run"] is dry_run and _SR_ROOT in (c["body"] or {})]

    def routes(self, call: dict) -> list[dict]:
        return call["body"][_SR_ROOT][0]["route"]


def sr_client(device_name: str, *, state, service_config=None):
    """A spec'd NsoClient whose RESTCONF boundary is recorded.

    *state* is what the certified tri-state reader returns for the static-route service.
    *service_config* is what the LEGACY ``get_service_config`` would return — set to
    something that looks clean so a path that wrongly re-reads is caught, not hidden.
    """
    from nso_adapter.nso.client import NsoClient

    rec = _Recorder(device_name)
    http = AsyncMock()
    for method in ("get", "put", "patch", "post"):

        def _bind(m=method):
            async def _call(url, content=None, headers=None, **kwargs):
                return await rec._handle(m, url, content, headers)

            return _call

        getattr(http, method).side_effect = _bind()

    client = MagicMock(spec=NsoClient)
    client._base = "http://nso"
    client._action_timeout = 120.0
    cm = client._client.return_value
    cm.__aenter__.return_value = http
    cm.__aexit__.return_value = False
    client.service_instance_state = AsyncMock(return_value=state)
    client.get_service_config = AsyncMock(return_value=service_config)
    client.sync_from = AsyncMock(return_value=None)
    return client, rec


def present(*entries, device_name="sr-put"):
    from nso_adapter.nso.client import ServiceInstanceState

    return ServiceInstanceState("present", {"device": device_name, "route": list(entries)})


def absent():
    from nso_adapter.nso.client import ServiceInstanceState

    return ServiceInstanceState("absent", None)


def inconclusive():
    from nso_adapter.nso.client import ServiceInstanceState

    return ServiceInstanceState("inconclusive", None)


async def run_the_apply(device_id: int, client, *, force: bool = True) -> Job:
    from nso_adapter.core.apply import run_apply

    job_id = await seed_apply_job(device_id)
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=client),
        patch("nso_adapter.core.apply._post_apply_refresh_and_notify", new=AsyncMock()),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=force)
    return await read_job(job_id)


# ── C2.1 — the replacement is a PUT, not a merge ─────────────────────────────


async def test_c2_1_a_replacement_open_row_is_delivered_by_a_put_replace(adapter_client):
    """C2.1 — ``deployed_key=A``, triple ``B``, service holds ``A``.

    Today's merge-PATCH adds ``B`` and leaves ``A`` on the device forever — two live routes
    for one NetBox object. The PUT-replace carries exactly the store's desired state.
    """
    device_id = await seed_device(nso_device_name="sr-put", netbox_device_id=7201)
    await seed_rows(device_id, [{"triple": B, "route_id": 7, "deployed_key": list(A)}])
    client, rec = sr_client("sr-put", state=present(wire(A), device_name="sr-put"))

    job = await run_the_apply(device_id, client)

    assert job.status == JobStatus.succeeded, job.error
    assert [c["method"] for c in rec.sr_commits()] == ["put"], "the replacement must not ride a merge-PATCH"
    put = rec.sr_commits("put")[0]
    assert f"{_SR_PATH}=sr-put" in put["url"]
    assert rec.routes(put) == [wire(B)], "the PUT body is the store's full desired state"
    # the one-snapshot contract: the guard never issued its own second read
    client.get_service_config.assert_not_awaited()


# ── C2.2 — the apply-side guard still blocks on collateral ───────────────────


async def test_c2_2_a_service_owned_sibling_blocks_the_replace(adapter_client):
    """C2.2 — a store-assertive body can still flush an unrelated service-owned hop.

    OQ-R2-3 removed the block on the REMOVAL path (a live-relative body cannot flush
    anything). The apply keeps it: this body really would retract ``C``.
    """
    device_id = await seed_device(nso_device_name="sr-put", netbox_device_id=7202)
    await seed_rows(device_id, [{"triple": B, "route_id": 7, "deployed_key": list(A)}])
    client, rec = sr_client("sr-put", state=present(wire(A), wire(C), device_name="sr-put"))

    job = await run_the_apply(device_id, client)

    assert job.status == JobStatus.failed
    assert rec.sr_commits() == [], "nothing may be committed once the guard refuses"
    item = next(i for i in job.error["detail"]["items"] if i["type"] == "static_route")
    assert item["code"] == "removal_blocked_collateral"
    assert item["orphans"] == {"route": [["", C[1], C[2]]]}, "the orphan report must NAME the sibling"

    async with session() as db:
        from nso_adapter.store.models import StaticRouteIntent

        row = (await db.execute(select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id))).scalar_one()
        assert row.last_apply_error["code"] == "removal_blocked_collateral"
        assert row.last_apply_error["detail"]["orphans"] == {"route": [["", C[1], C[2]]]}


# ── C2.3 / C2.4 — unconsumed tombstone entries survive VERBATIM ──────────────


@pytest.mark.parametrize("marking", ["detach", "delete_origin"])
async def test_c2_3_an_unconsumed_tombstone_entry_survives_the_replace_verbatim(adapter_client, marking):
    """C2.3/C2.4 — a tombstone the removal has not consumed yet still owns its live entry.

    The apply must not become the thing that drops it: its removal job is what authorizes
    that, and the entry carries leaves (here a NED-specific ``bfd-fast-detect``) the store
    has no column for, so re-rendering it from the triple would silently rewrite it.
    """
    device_id = await seed_device(nso_device_name="sr-put", netbox_device_id=7203)
    await seed_rows(device_id, [{"triple": B, "route_id": 7, "deployed_key": list(A)}])
    await seed_tombstone(device_id, D, marking=marking)
    live_d = wire(D, metric=44, **{"bfd-fast-detect": {"minimum": 50}})
    client, rec = sr_client("sr-put", state=present(wire(A), live_d, device_name="sr-put"))

    job = await run_the_apply(device_id, client)

    assert job.status == JobStatus.succeeded, job.error
    routes = rec.routes(rec.sr_commits("put")[0])
    assert routes == [wire(B), live_d], f"the {marking} tombstone's entry must ride verbatim"
    assert routes[1]["bfd-fast-detect"] == {"minimum": 50}
    assert await tombstone_ids(device_id), "the apply consumes no tombstone (C3/C4 own consumption)"


async def test_c2_3b_a_tombstone_key_a_live_row_reasserts_is_not_duplicated(adapter_client):
    """A tombstone whose key a body-rendered row re-asserts is NOT retained separately.

    Retention is "what no live row asserts"; without that filter the PUT would carry the
    key twice — the stale live copy winning whichever list position NSO reads last.
    """
    device_id = await seed_device(nso_device_name="sr-put", netbox_device_id=7204)
    await seed_rows(device_id, [{"triple": B, "route_id": 7, "deployed_key": list(A)}])
    await seed_tombstone(device_id, B, marking="delete_origin")  # same key a live row renders
    client, rec = sr_client("sr-put", state=present(wire(A), wire(B, metric=999), device_name="sr-put"))

    job = await run_the_apply(device_id, client)

    assert job.status == JobStatus.succeeded, job.error
    assert rec.routes(rec.sr_commits("put")[0]) == [wire(B)], "the store wins for a route it still owns"


async def test_c2_3c_a_tombstone_authorizes_its_deployed_key_too(adapter_client):
    """X6: a tombstone's ``deployed_key`` is as much its own key as its triple.

    Retaining only the triple leaves the predecessor entry unretained and unauthorized —
    the guard then blocks every apply on a device with an open, unconsumed tombstone.
    """
    device_id = await seed_device(nso_device_name="sr-put", netbox_device_id=7205)
    await seed_rows(device_id, [{"triple": B, "route_id": 7, "deployed_key": list(A)}])
    await seed_tombstone(device_id, D, deployed_key=list(C), marking="detach")
    client, rec = sr_client("sr-put", state=present(wire(A), wire(C, tag=8), device_name="sr-put"))

    job = await run_the_apply(device_id, client)

    assert job.status == JobStatus.succeeded, job.error
    assert rec.routes(rec.sr_commits("put")[0]) == [wire(B), wire(C, tag=8)]


# ── C2.5 — no replacement open ⇒ nothing changes ─────────────────────────────


async def test_c2_5_without_a_replacement_the_apply_is_still_a_merge_patch(adapter_client):
    """C2.5 — a tombstone alone never triggers the destructive path."""
    device_id = await seed_device(nso_device_name="sr-put", netbox_device_id=7206)
    await seed_rows(device_id, [{"triple": B, "route_id": 7, "deployed_key": list(B)}])
    tombs = [await seed_tombstone(device_id, D, marking="delete_origin")]
    client, rec = sr_client("sr-put", state=present(wire(D), device_name="sr-put"))

    job = await run_the_apply(device_id, client)

    assert job.status == JobStatus.succeeded, job.error
    assert [c["method"] for c in rec.sr_commits()] == ["patch"]
    assert rec.routes(rec.sr_commits("patch")[0]) == [wire(B)]
    assert await tombstone_ids(device_id) == tombs
    assert not client.service_instance_state.await_count, "a merge-PATCH needs no snapshot"


# ── C2.6 — preview parity ────────────────────────────────────────────────────


async def test_c2_6_the_preview_payload_is_byte_identical_to_the_applied_one(adapter_client):
    """C2.6 — the operator approves the apply from the preview panel.

    A preview that renders a merge while the apply sends a destructive replace is worse
    than no preview. The equality is over the exact serialized payload, and the preview
    must leave the store — rows, ``deployed_key``s and tombstones — untouched.
    """
    from nso_adapter.core.apply import collect_apply_diff

    device_id = await seed_device(nso_device_name="sr-put", netbox_device_id=7207)
    await seed_rows(
        device_id,
        [
            {"triple": B, "route_id": 7, "deployed_key": list(A)},
            # an accepted-and-clean sibling: it rides in both payloads or in neither
            {"triple": C, "route_id": 8, "deployed_key": list(C), "last_apply_at": _NOW},
        ],
    )
    await seed_tombstone(device_id, D, marking="detach")
    live_d = wire(D, tag=77)
    state = present(wire(A), wire(C), live_d, device_name="sr-put")

    before_keys, before_tombs = await deployed_keys(device_id), await tombstone_ids(device_id)

    preview_client, preview_rec = sr_client("sr-put", state=state)
    with patch("nso_adapter.core.importer.get_nso_client", return_value=preview_client):
        async with session() as db:
            await collect_apply_diff(db, device_id)

    assert await deployed_keys(device_id) == before_keys, "preview must not write"
    assert await tombstone_ids(device_id) == before_tombs, "preview must not consume a tombstone"
    assert preview_rec.commits == [], "preview must commit nothing"

    apply_client, apply_rec = sr_client("sr-put", state=state)
    job = await run_the_apply(device_id, apply_client)
    assert job.status == JobStatus.succeeded, job.error

    previewed = preview_rec.sr_payloads(dry_run=True)
    applied = apply_rec.sr_payloads(dry_run=False)
    assert previewed, "the replacement-open device must produce a static-route preview"
    assert applied[0] == previewed[0]
    assert json.dumps(applied[0], sort_keys=True) == json.dumps(previewed[0], sort_keys=True)
    assert applied[0][_SR_ROOT][0]["route"] == [wire(B), wire(C), live_d]
    # and it was previewed as a PUT dry-run, not a merge dry-run
    assert [c["method"] for c in preview_rec.calls if c["dry_run"] and _SR_ROOT in (c["body"] or {})] == ["put"]


# ── C2.7 — verification off refuses the replace, and closes nothing ──────────


async def test_c2_7_put_is_refused_when_verification_is_disabled_and_nothing_is_closed(adapter_client):
    """C2.7 — a destructive replace whose proof is structurally unavailable must not run.

    The forbidden pair is subtler than the PUT: the merge-PATCH that runs instead must
    ALSO not record ``deployed_key := B``. That would close the replacement while ``A`` is
    still on the device, and no later apply would ever reopen it.
    """
    device_id = await seed_device(nso_device_name="sr-put", netbox_device_id=7208)
    await seed_rows(device_id, [{"triple": B, "route_id": 7, "deployed_key": list(A)}])
    client, rec = sr_client("sr-put", state=present(wire(A), device_name="sr-put"))

    with patch("nso_adapter.nso.apply.VERIFY_AFTER_APPLY", False):
        job = await run_the_apply(device_id, client)

    assert job.status == JobStatus.succeeded, job.error
    assert [c["method"] for c in rec.sr_commits()] == ["patch"]
    assert await deployed_keys(device_id) == {B: list(A)}, "the replacement stays OPEN"


# ── C2.8 — the PUT body is every accepted row, force-independent ─────────────


async def test_c2_8_a_force_false_apply_still_carries_every_accepted_route(adapter_client):
    """C2.8 + C1.4's second half — the eligible list is the wrong basis for a replace.

    An identity-edited row keeps its ``last_apply_at``, so under ``force=False`` the very
    row needing replacement is filtered out, and every accepted-and-clean sibling would be
    retracted by an eligible-only body. ``any_eligible`` must come from the plan too, or
    ``_finalize_job`` reports an all-zero clean no-op AFTER a real PUT.
    """
    device_id = await seed_device(nso_device_name="sr-put", netbox_device_id=7209)
    await seed_rows(
        device_id,
        [
            {"triple": B, "route_id": 7, "deployed_key": list(A), "last_apply_at": _NOW},
            {"triple": C, "route_id": 8, "deployed_key": list(C), "last_apply_at": _NOW},
        ],
    )
    client, rec = sr_client("sr-put", state=present(wire(A), wire(C), device_name="sr-put"))

    job = await run_the_apply(device_id, client, force=False)

    assert job.status == JobStatus.succeeded, job.error
    assert rec.routes(rec.sr_commits("put")[0]) == [wire(B), wire(C)]
    # NOT _finalize_job's all-zero early success, which would report a clean no-op for a
    # device this apply had just PUT-replaced.
    assert job.result["static_route_count_by_outcome"] == {"in_sync": 2, "apply_failed": 0}


# ── C2.9 — an uncertified read is never a licence to replace ─────────────────


async def test_c2_9_an_inconclusive_snapshot_refuses_the_put(adapter_client):
    """C2.9 — a 200 whose root the reader cannot recognize is NOT "the service is empty".

    Treating it as empty retains no tombstoned entry and shows the guard no collateral, so
    the PUT flushes both and the verify then passes: a job that SUCCEEDS after violating
    both preservation rules.
    """
    device_id = await seed_device(nso_device_name="sr-put", netbox_device_id=7210)
    await seed_rows(device_id, [{"triple": B, "route_id": 7, "deployed_key": list(A)}])
    await seed_tombstone(device_id, D, marking="detach")
    client, rec = sr_client("sr-put", state=inconclusive())

    job = await run_the_apply(device_id, client)

    assert job.status == JobStatus.failed
    assert rec.calls == [], "not even a dry-run may be built from an uncertified read"
    item = next(i for i in job.error["detail"]["items"] if i["type"] == "static_route")
    assert "static_route_snapshot_inconclusive" in item["error"] or "certify" in item["error"]
    async with session() as db:
        from nso_adapter.store.models import StaticRouteIntent

        row = (await db.execute(select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id))).scalar_one()
        assert row.last_apply_error["code"] == "static_route_snapshot_inconclusive"
    assert await tombstone_ids(device_id), "nothing is consumed on an uncertified read"


async def test_c2_9_discriminator_a_certified_404_lets_the_put_proceed(adapter_client):
    """The discriminating variant: a conclusive keyed 404 is a real absence.

    Nothing to retain and no orphan possible — refusing here would wedge every device
    whose service instance does not exist yet.
    """
    device_id = await seed_device(nso_device_name="sr-put", netbox_device_id=7211)
    await seed_rows(device_id, [{"triple": B, "route_id": 7, "deployed_key": list(A)}])
    client, rec = sr_client("sr-put", state=absent())

    job = await run_the_apply(device_id, client)

    assert job.status == JobStatus.succeeded, job.error
    assert rec.routes(rec.sr_commits("put")[0]) == [wire(B)]


async def test_c2_9b_the_preview_refuses_an_inconclusive_read_too(adapter_client):
    """C2.9b (the two readers C2 ships) — a fix wired only into the apply passes a
    single-path pin. The preview reads the same service to compute the same retained
    entries, so it takes the same tri-state gate and issues no dry-run PUT either.

    The detach body and the networked-removal body are the other two readers; they are
    created in C4 and pinned there.
    """
    from nso_adapter.core.apply import collect_apply_diff

    device_id = await seed_device(nso_device_name="sr-put", netbox_device_id=7212)
    await seed_rows(device_id, [{"triple": B, "route_id": 7, "deployed_key": list(A)}])
    client, rec = sr_client("sr-put", state=inconclusive())

    with patch("nso_adapter.core.importer.get_nso_client", return_value=client):
        async with session() as db:
            diffs = await collect_apply_diff(db, device_id)

    assert rec.calls == []
    # visible in the panel the operator approves from, not silently omitted
    assert "refusing to build a PUT-replace" in diffs["static_route"], diffs["static_route"]


# ── C2.10 (enqueue half) + the A1 amendment ──────────────────────────────────


def _atomic(on: bool):
    return patch.dict("os.environ", {"NSO_ADAPTER_ATOMIC_APPLY": "1" if on else "0"})


@pytest.mark.parametrize("atomic", [False, True], ids=["per_scope_loop", "atomic_path"])
async def test_c2_10_a_patch_apply_queues_the_retract_it_cannot_deliver(adapter_client, atomic):
    """C2.10 — a merge-PATCH apply structurally cannot remove a cleared leaf.

    The renderer omits the leaf and the merge keeps it live, while reader-compare only
    checks the route KEY — so without this the row is certified over a stale value. The
    atomic path is a separate early return with its own finalization: wiring the follow-on
    only into the per-scope loop passes a single-path pin while atomic applies queue
    nothing.
    """
    device_id = await seed_device(nso_device_name="sr-put", netbox_device_id=7213 + int(atomic))
    await seed_rows(
        device_id,
        [{"triple": B, "route_id": 7, "deployed_key": list(B), "pending_clear": {"authorized": ["metric"]}}],
    )
    client, _rec = sr_client("sr-put", state=present(wire(B), device_name="sr-put"))

    with _atomic(atomic):
        job = await run_the_apply(device_id, client)

    assert job.status == JobStatus.succeeded, job.error
    contexts = await removal_contexts(device_id)
    assert len(contexts) == 1, contexts
    ctx = contexts[0]
    assert ctx["scope"] == "static_route"
    assert not ctx.get("detach"), "a no-networking detach can never deliver a clear"
    assert not ctx.get("force")


@pytest.mark.parametrize("atomic", [False, True], ids=["per_scope_loop", "atomic_path"])
async def test_a1_a_store_only_clear_never_becomes_a_deletion_job(adapter_client, atomic):
    """A1 — promotion is by DELIVERY, and a merge-PATCH delivers nothing.

    r8's premise ("an apply only runs because something else authorized a device write")
    covers the apply's own body. It does not cover enqueueing §4.11's networked retract,
    whose only purpose would be to delete a leaf recorded from a ``?store_only=true``
    observation — the exact hazard r8 closed at the removal, moved one hop downstream.
    """
    device_id = await seed_device(nso_device_name="sr-put", netbox_device_id=7215 + int(atomic))
    await seed_rows(
        device_id,
        [{"triple": B, "route_id": 7, "deployed_key": list(B), "pending_clear": {"store_only": ["metric"]}}],
    )
    client, _rec = sr_client("sr-put", state=present(wire(B), device_name="sr-put"))

    with _atomic(atomic):
        job = await run_the_apply(device_id, client)

    assert job.status == JobStatus.succeeded, job.error
    assert await removal_contexts(device_id) == [], "a store-only observation is not deletion authority"


async def test_a1_discriminator_a_later_authorized_push_releases_the_parked_clear(adapter_client):
    """A1's discriminating half, driven through the REAL intent endpoint.

    The parked entry stays parked across as many PATCH-mode applies as you like; the thing
    that releases it is a later AUTHORIZED push re-observing the cleared state, which the
    endpoint's per-push detection does naturally.
    """
    device_id = await seed_device(nso_device_name="sr-put", netbox_device_id=7217)

    async def put_intent(routes, query=""):
        resp = await adapter_client.put(
            f"/api/v1/devices/{device_id}/static-route-intent{query}", json={"routes": routes}, headers=AUTH
        )
        assert resp.status_code == 200, resp.text

    body = {"vrf": B[0], "prefix": B[1], "next_hop": B[2], "route_id": 7}
    await put_intent([{**body, "metric": 10}], query="?store_only=true")
    await put_intent([body], query="?store_only=true")  # the clear, recorded store-only

    async with session() as db:
        from nso_adapter.store.models import StaticRouteIntent

        row = (await db.execute(select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id))).scalar_one()
        assert row.pending_clear["store_only"] == ["metric"]
        row.accepted_at, row.deployed_key = _NOW, list(B)
        await db.commit()

    client, _rec = sr_client("sr-put", state=present(wire(B), device_name="sr-put"))
    await run_the_apply(device_id, client)
    assert await removal_contexts(device_id) == [], "parked, not promoted"

    # A later authorized push re-observes the cleared state and re-records it.
    await put_intent([{**body, "metric": 10}])
    await put_intent([body])
    async with session() as db:
        from nso_adapter.store.models import StaticRouteIntent

        row = (await db.execute(select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id))).scalar_one()
        assert "metric" in row.pending_clear["authorized"]

    before = len(await removal_contexts(device_id))
    client, _rec = sr_client("sr-put", state=present(wire(B), device_name="sr-put"))
    await run_the_apply(device_id, client)
    after = await removal_contexts(device_id)
    assert len(after) == before + 1
    assert after[-1]["scope"] == "static_route" and not after[-1].get("detach")


async def test_c2_10b_a_put_mode_apply_queues_nothing(adapter_client):
    """In PUT mode the store-rendered body already omits the leaf — the retract is owed
    by the merge path only. Queueing one here would be a device write nothing asked for."""
    device_id = await seed_device(nso_device_name="sr-put", netbox_device_id=7218)
    await seed_rows(
        device_id,
        [{"triple": B, "route_id": 7, "deployed_key": list(A), "pending_clear": {"authorized": ["metric"]}}],
    )
    client, rec = sr_client("sr-put", state=present(wire(A), device_name="sr-put"))

    job = await run_the_apply(device_id, client)

    assert job.status == JobStatus.succeeded, job.error
    assert [c["method"] for c in rec.sr_commits()] == ["put"]
    assert await removal_contexts(device_id) == []


async def test_c2_10c_a_clean_device_queues_nothing(adapter_client):
    """The negative control: no carrier, no follow-on job."""
    device_id = await seed_device(nso_device_name="sr-put", netbox_device_id=7219)
    await seed_rows(device_id, [{"triple": B, "route_id": 7, "deployed_key": list(B)}])
    client, _rec = sr_client("sr-put", state=present(wire(B), device_name="sr-put"))

    await run_the_apply(device_id, client)
    assert await removal_contexts(device_id) == []


async def test_the_follow_on_enqueue_never_rewrites_the_finalized_apply(adapter_client):
    """The retract enqueue runs AFTER the apply's terminal transaction.

    Letting it raise would make ``run_apply``'s outer handler write ``failed`` over an
    already-committed ``succeeded`` — reporting rows the device really did accept as
    failures. The carrier is store state, so the next apply re-derives the same decision.
    """
    device_id = await seed_device(nso_device_name="sr-put", netbox_device_id=7220)
    await seed_rows(
        device_id,
        [{"triple": B, "route_id": 7, "deployed_key": list(B), "pending_clear": {"authorized": ["metric"]}}],
    )
    client, _rec = sr_client("sr-put", state=present(wire(B), device_name="sr-put"))

    with patch("nso_adapter.core.removal.enqueue_removal", side_effect=RuntimeError("db down")):
        job = await run_the_apply(device_id, client)

    assert job.status == JobStatus.succeeded, job.error
    assert job.result["static_route_count_by_outcome"] == {"in_sync": 1, "apply_failed": 0}
    assert await removal_contexts(device_id) == []
