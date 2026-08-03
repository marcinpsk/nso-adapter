# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""#1396 R2 chunk C3 — the proof verdict, residue enforcement, the CAS and per-route results.

Pins C3.1-C3.12c, plus the halves A2 reassigned here: the ``unproven`` outcomes of C2.7 and
C2.10, and C2.11's apply-side carrier consumption (the removal-side half is C4.32).

Every case drives the REAL ``run_apply`` (or, for the handoff pins, the real
``worker._run_one_job``) against a real PostgreSQL clone. Only the RESTCONF boundary is
faked, and it records what it was handed, so the assertions are about the bytes that would
reach NSO and the rows that really landed.

C3.5 — the carrier-owning removal counter-case — is NOT here: a removal only owns a carrier
once ``_replace_static_route`` exists, which is C4. See the chunk report.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from sqlalchemy import select, text

from nso_adapter.store.models import Job, JobStatus, JobType
from tests.conftest import seed_device, session
from tests.core.test_static_route_put import (
    A,
    B,
    C,
    D,
    _Recorder,
    absent,
    deployed_keys,
    present,
    read_job,
    removal_contexts,
    seed_apply_job,
    seed_rows,
    seed_tombstone,
    wire,
)

pytestmark = pytest.mark.anyio

_NOW = datetime(2026, 6, 1, tzinfo=UTC)

A2 = ("", "10.0.4.0/24", "192.0.2.5")
B2 = ("", "10.0.5.0/24", "192.0.2.6")


# ── the recorded RESTCONF boundary, plus a device-state plane ────────────────


class _ProofRecorder(_Recorder):
    """C2's recorder, with a dry-run status the test can push off 2xx.

    A 5xx dry-run is how ``native_dry_run`` returns ``None`` — the one real way a commit's
    verify comes back INCONCLUSIVE rather than conclusive. 4xx is deliberately not used:
    under ``strict`` it raises, which is a different (and already-pinned) outcome.
    """

    def __init__(self, device_name: str, dry_run_delta: str = "", dry_run_status: int = 200):
        super().__init__(device_name, dry_run_delta)
        self.dry_run_status = dry_run_status

    async def _handle(self, method: str, url: str, content=None, headers=None):
        if "dry-run=" in url and self.dry_run_status != 200:
            self.calls.append(
                {"method": method, "url": url, "body": json.loads(content) if content else None, "dry_run": True}
            )
            return httpx.Response(
                self.dry_run_status, request=httpx.Request(method.upper(), url), json={"errors": "boom"}
            )
        return await super()._handle(method, url, content, headers)


def dev_state(*entries, status: str = "ok") -> dict:
    """One ``static-route`` device-state section — the plane residue and §4.11 both read."""
    if status != "ok":
        return {"status": status}
    return {"status": "ok", "route": [dict(entry) for entry in entries]}


def proof_client(device_name: str, *, state, section: dict, dry_run_status: int = 200, service_config=None):
    """A spec'd NsoClient with BOTH planes faked: the service instance and the device state."""
    from unittest.mock import MagicMock

    from nso_adapter.nso.client import NsoClient

    rec = _ProofRecorder(device_name, dry_run_status=dry_run_status)
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
    client.run_device_state_read = AsyncMock(return_value={"static-route": section})
    return client, rec


async def run_the_apply(device_id: int, client, *, force: bool = True, reg=None) -> Job:
    from nso_adapter.core.apply import run_apply

    job_id = await seed_apply_job(device_id)
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=client),
        patch("nso_adapter.core.apply._post_apply_refresh_and_notify", new=AsyncMock()),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=force, reg=reg)
    return await read_job(job_id)


def outcomes(job: Job) -> dict[tuple, str]:
    """``{route key: outcome}`` from the per-route results — the C3 record itself."""
    return {tuple(item["key"]): item["outcome"] for item in job.result["static_route_results"]}


async def carriers(device_id: int) -> dict[tuple, dict | None]:
    from nso_adapter.store.models import StaticRouteIntent

    async with session() as db:
        rows = (
            (await db.execute(select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id)))
            .scalars()
            .all()
        )
        return {(r.vrf, r.prefix, r.next_hop): r.pending_clear for r in rows}


# ── C3.1 — a proven replacement closes the deployed_key ──────────────────────


async def test_c3_1_a_proven_replacement_records_the_new_deployment(adapter_client):
    """C3.1 — clean PUT, conclusive verify, reader-compare ``ok`` ⇒ ``deployed_key == B``.

    Leaving it at ``A`` would make the NEXT apply re-derive a replacement that has already
    been delivered, and would leave a removal authorized to drop a triple nothing owns.
    """
    device_id = await seed_device(nso_device_name="sr-proof", netbox_device_id=7301)
    await seed_rows(device_id, [{"triple": B, "route_id": 7, "deployed_key": list(A)}])
    client, rec = proof_client("sr-proof", state=present(wire(A), device_name="sr-proof"), section=dev_state(wire(B)))

    job = await run_the_apply(device_id, client)

    assert job.status == JobStatus.succeeded, job.error
    assert [c["method"] for c in rec.sr_commits()] == ["put"]
    assert await deployed_keys(device_id) == {B: list(B)}, "the replacement is closed only once proven"
    assert outcomes(job) == {B: "in_sync"}
    assert job.result["reader_compare"]["static_route"] == "ok"


# ── C3.2 / C3.2b — bootstrapping deployed_key needs a CONCLUSIVE verify ──────


@pytest.mark.parametrize(
    ("dry_run_status", "expected_key", "expected_outcome"),
    [(500, None, "unproven"), (200, list(B), "in_sync")],
    ids=["inconclusive-verify", "conclusive-verify"],
)
async def test_c3_2_a_merge_patch_records_a_deployment_only_when_proven(
    adapter_client, dry_run_status, expected_key, expected_outcome
):
    """C3.2 — a never-applied row, HTTP 2xx, native verify inconclusive.

    Writing ``deployed_key`` here would manufacture deletion authority for a route nothing
    proved deployed: a later removal would then be allowed to PUT that triple away.
    """
    device_id = await seed_device(nso_device_name="sr-proof", netbox_device_id=7302)
    await seed_rows(device_id, [{"triple": B, "route_id": 7}])
    client, rec = proof_client("sr-proof", state=absent(), section=dev_state(wire(B)), dry_run_status=dry_run_status)

    job = await run_the_apply(device_id, client)

    assert job.status == JobStatus.succeeded, job.error
    assert [c["method"] for c in rec.sr_commits()] == ["patch"], "no replacement is open — this stays a merge"
    assert await deployed_keys(device_id) == {B: expected_key}
    assert outcomes(job) == {B: expected_outcome}


@pytest.mark.parametrize(
    ("dry_run_status", "expected_key", "expected_outcome"),
    [(500, None, "unproven"), (200, list(B), "in_sync")],
    ids=["inconclusive-verify", "conclusive-verify"],
)
async def test_c3_2b_atomic_mode_bootstraps_only_on_a_conclusive_combined_verify(
    adapter_client, monkeypatch, dry_run_status, expected_key, expected_outcome
):
    """C3.2b — ATOMIC mode, never-applied row, no replacement open.

    Stated as a merge-mode setup on purpose: §4.9 excludes a PUT-mode plan from
    ``apply_combined`` entirely, so a replacement setup would not exercise this path at all.
    The send happens INSIDE ``apply_combined``, which used to discard its verify verdict
    (G39) — so atomic mode could either CAS a never-proven row or never bootstrap at all.
    An inconclusive verdict must not fail the job either (§6/OQ-R2-1).
    """
    monkeypatch.setenv("NSO_ADAPTER_ATOMIC_APPLY", "1")
    device_id = await seed_device(nso_device_name="sr-proof", netbox_device_id=7303)
    await seed_rows(device_id, [{"triple": B, "route_id": 7}])
    client, rec = proof_client("sr-proof", state=absent(), section=dev_state(wire(B)), dry_run_status=dry_run_status)

    job = await run_the_apply(device_id, client)

    assert job.status == JobStatus.succeeded, job.error
    assert [c["url"].split("?")[0] for c in rec.commits] == ["http://nso/restconf/data"], (
        "the atomic path commits ONE combined PATCH — the send is inside apply_combined"
    )
    assert await deployed_keys(device_id) == {B: expected_key}
    assert outcomes(job) == {B: expected_outcome}


# ── C3.3 — a surviving predecessor is a FAILURE, not a green ─────────────────


async def test_c3_3_a_surviving_predecessor_fails_the_scope_and_keeps_the_key_open(adapter_client):
    """C3.3 — replacement-open apply whose old key ``A`` is still on the device.

    The intent landed; what failed is the retraction. Closing ``deployed_key`` here would
    lose the only pointer to the route still live on the box.
    """
    device_id = await seed_device(nso_device_name="sr-proof", netbox_device_id=7304)
    await seed_rows(device_id, [{"triple": B, "route_id": 7, "deployed_key": list(A)}])
    client, _rec = proof_client(
        "sr-proof",
        state=present(wire(A), device_name="sr-proof"),
        section=dev_state(wire(A), wire(B)),  # B landed, A survived the replace
    )

    job = await run_the_apply(device_id, client)

    assert job.status == JobStatus.failed
    assert await deployed_keys(device_id) == {B: list(A)}, "the replacement stays OPEN"
    assert outcomes(job) == {B: "apply_failed"}
    item = next(i for i in job.error["detail"]["items"] if i["type"] == "static_route")
    assert item["code"] == "static_route_residue_found"
    assert item["error"].startswith("static_route: the replaced route(s)")


# ── C3.4 — every decided inconclusive signal: no consumption, job SUCCEEDS ───


def _signal_setup(signal: str):
    """(kwargs for proof_client, deployed_key seed, verify-disabled?) for one C3.4 arm."""
    if signal == "native_inconclusive":
        return {"state": absent(), "section": dev_state(wire(B)), "dry_run_status": 500}, None, False
    if signal == "native_disabled":
        return {"state": absent(), "section": dev_state(wire(B))}, None, True
    if signal == "rc_unknown":
        return {"state": absent(), "section": dev_state(status="unsupported")}, None, False
    if signal == "rc_error":
        return {"state": absent(), "section": dev_state(status="error")}, None, False
    if signal == "residue_unsupported":
        return (
            {"state": present(wire(A), device_name="sr-proof"), "section": dev_state(status="unsupported")},
            list(A),
            False,
        )
    if signal == "residue_error":
        return {"state": present(wire(A), device_name="sr-proof"), "section": dev_state(status="error")}, list(A), False
    raise AssertionError(signal)


@pytest.mark.parametrize(
    "signal",
    ["native_inconclusive", "native_disabled", "rc_unknown", "rc_error", "residue_unsupported", "residue_error"],
)
async def test_c3_4_an_inconclusive_signal_consumes_nothing_and_still_succeeds(adapter_client, monkeypatch, signal):
    """C3.4, per-scope loop — every decided inconclusive signal (§6/OQ-R2-1 = (b)).

    Two halves, and an implementation that only gets one of them is wrong in a way a
    residue-only pin would miss: nothing may be consumed, AND the job must still SUCCEED. A
    transient read failure must not fail an apply whose device write actually landed; the
    honesty lives in the per-route ``unproven`` and the loud log, not in the job status.

    ``rc_unknown``/``rc_error`` and ``residue_unsupported``/``residue_error`` are driven
    separately: the first pair has no consumed key, so no residue read is owed at all, while
    the second pair reads the same certified section for both verdicts.
    """
    kwargs, seed_key, verify_off = _signal_setup(signal)
    if verify_off:
        monkeypatch.setattr("nso_adapter.nso.apply.VERIFY_AFTER_APPLY", False)
    device_id = await seed_device(nso_device_name="sr-proof", netbox_device_id=7305)
    await seed_rows(device_id, [{"triple": B, "route_id": 7, "deployed_key": seed_key}])
    client, _rec = proof_client("sr-proof", **kwargs)

    job = await run_the_apply(device_id, client)

    assert job.status == JobStatus.succeeded, job.error
    assert await deployed_keys(device_id) == {B: seed_key}, "nothing may be recorded as deployed"
    assert outcomes(job) == {B: "unproven"}


@pytest.mark.parametrize("signal", ["native_inconclusive", "native_disabled", "rc_unknown", "rc_error"])
async def test_c3_4_atomic_an_inconclusive_signal_consumes_nothing_and_still_succeeds(
    adapter_client, monkeypatch, signal
):
    """C3.4, ATOMIC path — the same rule on the other apply implementation.

    Only the four merge-shaped signals apply: staging is merge-PATCH only and ignores
    ``replace`` (G4), so an atomic pass never consumes a predecessor key and therefore never
    owes a residue read. ``_run_atomic_apply`` is a separate early return with its own
    finalization, so a fix wired only into the per-scope loop passes every pin above and
    still reports a bare green here.
    """
    monkeypatch.setenv("NSO_ADAPTER_ATOMIC_APPLY", "1")
    kwargs, seed_key, verify_off = _signal_setup(signal)
    if verify_off:
        monkeypatch.setattr("nso_adapter.nso.apply.VERIFY_AFTER_APPLY", False)
    device_id = await seed_device(nso_device_name="sr-proof", netbox_device_id=7306)
    await seed_rows(device_id, [{"triple": B, "route_id": 7, "deployed_key": seed_key}])
    client, _rec = proof_client("sr-proof", **kwargs)

    job = await run_the_apply(device_id, client)

    assert job.status == JobStatus.succeeded, job.error
    assert await deployed_keys(device_id) == {B: seed_key}
    assert outcomes(job) == {B: "unproven"}


# ── C3.6 — per-ROW evidence, not the aggregate scope status ──────────────────


async def test_c3_6_the_proven_sibling_cases_while_the_missing_one_stays_open(adapter_client):
    """C3.6 — two routes in one PUT; one present in the device view, one silently dropped.

    The aggregate reader-compare status is ``missing`` for the whole scope, so an
    implementation that reads it instead of the per-row evidence map CASes NEITHER — and one
    that reads only the 2xx CASes BOTH. Exactly one is right.
    """
    device_id = await seed_device(nso_device_name="sr-proof", netbox_device_id=7307)
    await seed_rows(
        device_id,
        [
            {"triple": B, "route_id": 7, "deployed_key": list(A)},
            {"triple": B2, "route_id": 8, "deployed_key": list(A2)},
        ],
    )
    client, _rec = proof_client(
        "sr-proof",
        state=present(wire(A), wire(A2), device_name="sr-proof"),
        section=dev_state(wire(B)),  # B landed; B2 never did, and both predecessors are gone
    )

    job = await run_the_apply(device_id, client)

    assert job.status == JobStatus.failed
    assert await deployed_keys(device_id) == {B: list(B), B2: list(A2)}
    assert outcomes(job) == {B: "in_sync", B2: "apply_failed"}
    assert job.result["reader_compare"]["static_route"] == "missing", "the aggregate is unchanged"


# ── C3.7 / C3.9 — the CAS itself, driven directly ───────────────────────────


async def _cas(device_id, *, row_id, route_id, sent, expected_old, watermark=0):
    from nso_adapter.store.static_route_store import cas_deployed_key

    async with session() as db:
        verdict = await cas_deployed_key(
            db,
            device_id=device_id,
            row_id=row_id,
            route_id=route_id,
            sent_triple=sent,
            expected_old=expected_old,
            tombstone_id_watermark=watermark,
        )
        await db.commit()
        return verdict


async def tombstone_keys(device_id: int) -> list[list | None]:
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
        return [r.deployed_key for r in rows]


async def _delete_row(row_id: int) -> None:
    from nso_adapter.store.models import StaticRouteIntent

    async with session() as db:
        row = await db.get(StaticRouteIntent, row_id)
        await db.delete(row)
        await db.commit()


async def test_c3_7_a_concurrent_deployed_key_edit_stops_the_cas(adapter_client):
    """C3.7 — another session moved the row to ``C`` between the snapshot and the CAS.

    A C-over-B identity edit landing first is CORRECT: that replacement stays open and this
    apply proved nothing about it. Falling through to the tombstone on the bare miss would
    rewrite an unrelated older carrier that happens to share the ``route_id``.
    """
    from nso_adapter.store.static_route_store import CAS_STOPPED, CAS_TOMBSTONE

    device_id = await seed_device(nso_device_name="sr-cas", netbox_device_id=7308)
    ids = await seed_rows(device_id, [{"triple": C, "route_id": 7, "deployed_key": list(C)}])
    await seed_tombstone(device_id, D, route_id=7, deployed_key=list(A))

    verdict = await _cas(device_id, row_id=ids[C], route_id=7, sent=B, expected_old=list(A))

    assert verdict == CAS_STOPPED
    assert await deployed_keys(device_id) == {C: list(C)}, "the live row keeps the later edit"
    assert await tombstone_keys(device_id) == [list(A)], "the older carrier must not be rewritten"

    # Discriminator: with the row GONE, the same call does land on the tombstone.
    await _delete_row(ids[C])
    assert await _cas(device_id, row_id=ids[C], route_id=7, sent=B, expected_old=list(A)) == CAS_TOMBSTONE
    assert await tombstone_keys(device_id) == [list(B)]


async def test_c3_9_the_tombstone_fallback_needs_exactly_one_post_watermark_candidate(adapter_client):
    """C3.9 — several tombstones can share a ``route_id`` AND an ``expected_old`` (G34).

    Oldest-first would write the sent triple onto a stale carrier while the real one keeps
    the old value, so ITS removal could not authorize what this apply actually sent. The
    watermark picks the one created after the plan's snapshot; ambiguity ABSTAINS, which
    grants no authority at all.
    """
    from nso_adapter.store.static_route_store import CAS_ABSTAINED, CAS_TOMBSTONE

    device_id = await seed_device(nso_device_name="sr-cas", netbox_device_id=7309)
    ids = await seed_rows(device_id, [{"triple": B, "route_id": 7, "deployed_key": list(A)}])
    old_id = await seed_tombstone(device_id, D, route_id=7, deployed_key=list(A))
    await _delete_row(ids[B])
    new_id = await seed_tombstone(device_id, C, route_id=7, deployed_key=list(A))

    verdict = await _cas(device_id, row_id=ids[B], route_id=7, sent=B, expected_old=list(A), watermark=old_id)

    assert verdict == CAS_TOMBSTONE
    assert new_id > old_id
    assert await tombstone_keys(device_id) == [list(A), list(B)], "only the post-watermark carrier moves"

    # Discriminator: a SECOND post-watermark candidate makes the choice ambiguous → abstain.
    other_id = await seed_device(nso_device_name="sr-cas2", netbox_device_id=7339)
    other_rows = await seed_rows(other_id, [{"triple": B, "route_id": 7, "deployed_key": list(A)}])
    base = await seed_tombstone(other_id, D, route_id=7, deployed_key=list(A))
    await _delete_row(other_rows[B])
    await seed_tombstone(other_id, C, route_id=7, deployed_key=list(A))
    await seed_tombstone(other_id, A2, route_id=7, deployed_key=list(A))

    assert await _cas(other_id, row_id=other_rows[B], route_id=7, sent=B, expected_old=list(A), watermark=base) == (
        CAS_ABSTAINED
    )
    assert await tombstone_keys(other_id) == [list(A)] * 3, "an ambiguous fallback touches nothing"


# ── C3.8 — a key the body re-asserts is intent, not residue ──────────────────


async def test_c3_8_a_predecessor_a_sibling_row_reclaims_is_not_residue(adapter_client):
    """C3.8 — ``A`` is re-claimed by a different live row and rides in the same PUT body.

    Counting it as residue would fail an apply that did exactly what it was told: the key is
    on the device because THIS payload put it there.
    """
    device_id = await seed_device(nso_device_name="sr-proof", netbox_device_id=7310)
    await seed_rows(
        device_id,
        [
            {"triple": B, "route_id": 7, "deployed_key": list(A)},
            {"triple": A, "route_id": 8, "deployed_key": list(A)},
        ],
    )
    client, rec = proof_client(
        "sr-proof",
        state=present(wire(A), device_name="sr-proof"),
        section=dev_state(wire(A), wire(B)),  # A is still there — because row 8 asserts it
    )

    job = await run_the_apply(device_id, client)

    assert job.status == JobStatus.succeeded, job.error
    sent = {(e["vrf"], e["prefix"], e["next-hop"]) for e in rec.routes(rec.sr_commits("put")[0])}
    assert sent == {A, B}, "both rows ride the one PUT — A is re-asserted, not retracted"
    assert await deployed_keys(device_id) == {B: list(B), A: list(A)}
    assert outcomes(job) == {B: "in_sync", A: "in_sync"}
    assert "static_route" not in (job.error or {}).get("detail", {})


# ── C3.10 — the fingerprint is the hash of the EXACT wire entry ──────────────


def _expected_fingerprint(entry: dict) -> str:
    return hashlib.sha256(json.dumps(entry, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


async def test_c3_10_the_fingerprint_equals_an_independently_computed_hash(adapter_client):
    """C3.10 — the literal expected wire dict, hashed in the test, not renderer-vs-renderer."""
    from nso_adapter.store.models import StaticRouteIntent

    device_id = await seed_device(nso_device_name="sr-proof", netbox_device_id=7311)
    await seed_rows(device_id, [{"triple": B, "route_id": 7}])
    async with session() as db:
        row = (await db.execute(select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id))).scalar_one()
        row.metric = 0
        row.tag = 0
        row.permanent = False
        row.interface_next_hop = "Gi0/0"
        row.next_hop_vrf = "blue"
        row.name = "ignored-by-the-wire"
        await db.commit()
    client, _rec = proof_client("sr-proof", state=absent(), section=dev_state(wire(B)))

    job = await run_the_apply(device_id, client)

    expected = {
        "vrf": B[0],
        "prefix": B[1],
        "next-hop": B[2],
        "interface-next-hop": "Gi0/0",
        "next-hop-vrf": "blue",
        "metric": 0,
        "tag": 0,
    }
    assert job.result["static_route_results"][0]["fingerprint"] == _expected_fingerprint(expected)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("metric", 5),
        ("tag", 9),
        ("permanent", True),
        ("interface_next_hop", "Gi0/1"),
        ("next_hop_vrf", "green"),
        ("prefix", "10.9.9.0/24"),
        ("next_hop", "192.0.2.99"),
        ("vrf", "red"),
    ],
)
async def test_c3_10_every_emitted_field_moves_the_fingerprint(adapter_client, field, value):
    """C3.10 discriminator — hashing a subset (``metric`` alone) satisfies "moves when metric moves"."""
    from nso_adapter.core.apply import static_route_fingerprint
    from nso_adapter.store.models import StaticRouteIntent

    device_id = await seed_device(nso_device_name="sr-fp", netbox_device_id=7312)
    await seed_rows(device_id, [{"triple": B, "route_id": 7}])
    async with session() as db:
        row = (await db.execute(select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id))).scalar_one()
        before = static_route_fingerprint(row)
        setattr(row, field, value)
        assert static_route_fingerprint(row) != before, f"{field} is on the wire and must move the fingerprint"


async def test_c3_10_a_field_with_no_wire_leaf_does_not_move_the_fingerprint(adapter_client):
    """C3.10 discriminator — ``name`` has no wire form, so editing it changed nothing sent."""
    from nso_adapter.core.apply import static_route_fingerprint
    from nso_adapter.store.models import StaticRouteIntent

    device_id = await seed_device(nso_device_name="sr-fp", netbox_device_id=7313)
    await seed_rows(device_id, [{"triple": B, "route_id": 7}])
    async with session() as db:
        row = (await db.execute(select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id))).scalar_one()
        before = static_route_fingerprint(row)
        row.name = "renamed"
        assert static_route_fingerprint(row) == before


# ── C3.11 / C3.12 — the terminal transaction and its in-doubt COMMIT ─────────


async def test_c3_11_the_cas_and_the_terminal_status_are_one_transaction(adapter_client):
    """C3.11 — the connection dies between the device commit and the bookkeeping commit.

    All-or-nothing: a ``deployed_key`` that moved under a job that never went terminal is a
    closed replacement nobody will ever re-derive.
    """
    device_id = await seed_device(nso_device_name="sr-proof", netbox_device_id=7314)
    await seed_rows(device_id, [{"triple": B, "route_id": 7, "deployed_key": list(A)}])
    client, _rec = proof_client("sr-proof", state=present(wire(A), device_name="sr-proof"), section=dev_state(wire(B)))

    with patch(
        "nso_adapter.core.apply._commit_terminal", new=AsyncMock(side_effect=RuntimeError("connection reset by peer"))
    ):
        job = await run_the_apply(device_id, client)

    assert job.status == JobStatus.failed
    assert job.error["code"] == "internal"
    assert await deployed_keys(device_id) == {B: list(A)}, "the CAS rolled back with the status"


async def _acquire(device_id: int, job_id: int):
    from nso_adapter.core.claim import acquire_claim

    return await acquire_claim(device_id, "job", job_id=job_id)


async def _revoke(device_id: int) -> None:
    """Revocation as the reaper performs it: the claim row is simply gone."""
    async with session() as db:
        await db.execute(text("DELETE FROM device_claim WHERE device_id = :d"), {"d": device_id})
        await db.commit()


async def _claim_rows() -> list[tuple]:
    async with session() as db:
        return list((await db.execute(text("SELECT device_id, job_id FROM device_claim"))).all())


async def _age_heartbeats() -> None:
    async with session() as db:
        await db.execute(text("UPDATE device_claim SET heartbeat_at = now() - interval '1 day'"))
        await db.commit()


async def _drive_one_job(device_id: int, job_id: int, client, reg):
    from nso_adapter.core.jobs import _run_apply
    from nso_adapter.core.worker import _run_one_job

    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=client),
        patch("nso_adapter.core.apply._post_apply_refresh_and_notify", new=AsyncMock()) as refresh,
    ):
        await _run_one_job(1, job_id, device_id, JobType.apply, _run_apply, reg)
    return refresh


async def test_c3_12a_a_landed_commit_with_a_lost_ack_is_left_to_recovery(adapter_client):
    """C3.12a — the bookkeeping COMMIT landed and the acknowledgement was lost.

    A second terminal write would flip a ``succeeded`` job to ``failed`` over a CAS that is
    already in the database. Nothing further is written, the claim is NOT released (so it
    goes stale and the reaper sees it), the post-apply refresh does not run, and recovery
    then leaves the already-terminal job alone — G38 is the whole distinction.
    """
    from nso_adapter.core.claim import ClaimOutcome, revoke_stale_claims

    device_id = await seed_device(nso_device_name="sr-proof", netbox_device_id=7315)
    await seed_rows(device_id, [{"triple": B, "route_id": 7, "deployed_key": list(A)}])
    client, _rec = proof_client("sr-proof", state=present(wire(A), device_name="sr-proof"), section=dev_state(wire(B)))
    job_id = await seed_apply_job(device_id)
    reg = await _acquire(device_id, job_id)

    async def _landed_but_unacked(db):
        await db.commit()
        return ClaimOutcome.OUTCOME_UNKNOWN

    with patch("nso_adapter.core.claim._commit_outcome", new=_landed_but_unacked):
        refresh = await _drive_one_job(device_id, job_id, client, reg)

    job = await read_job(job_id)
    assert job.status == JobStatus.succeeded, "the commit landed — its status stands"
    assert await deployed_keys(device_id) == {B: list(B)}
    assert outcomes(job) == {B: "in_sync"}
    refresh.assert_not_awaited()
    assert await _claim_rows() == [(device_id, job_id)], "the claim is left for the reaper, never released"

    await _age_heartbeats()
    assert [r.device_id for r in await revoke_stale_claims()] == [device_id]
    assert (await read_job(job_id)).status == JobStatus.succeeded, "an already-terminal job is not re-dispositioned"
    assert await _claim_rows() == []


async def test_c3_12b_a_commit_that_did_not_land_is_re_dispositioned(adapter_client):
    """C3.12b — the same handoff, but the transaction did NOT land.

    The job is still ``running`` and would otherwise sit there forever; recovery moves it
    (apply → ``failed``, never a silent re-push).
    """
    from nso_adapter.core.claim import ClaimOutcome, revoke_stale_claims

    device_id = await seed_device(nso_device_name="sr-proof", netbox_device_id=7316)
    await seed_rows(device_id, [{"triple": B, "route_id": 7, "deployed_key": list(A)}])
    client, _rec = proof_client("sr-proof", state=present(wire(A), device_name="sr-proof"), section=dev_state(wire(B)))
    job_id = await seed_apply_job(device_id)
    reg = await _acquire(device_id, job_id)

    async def _lost(db):
        await db.rollback()
        return ClaimOutcome.OUTCOME_UNKNOWN

    with patch("nso_adapter.core.claim._commit_outcome", new=_lost):
        await _drive_one_job(device_id, job_id, client, reg)

    assert (await read_job(job_id)).status == JobStatus.running
    assert await deployed_keys(device_id) == {B: list(A)}, "nothing landed, so nothing is closed"

    await _age_heartbeats()
    await revoke_stale_claims()
    job = await read_job(job_id)
    assert job.status == JobStatus.failed
    assert job.error["code"] == "orphaned"


async def test_c3_12c_a_pre_commit_failure_takes_the_ordinary_fallback(adapter_client):
    """C3.12c — ``ABORT_KNOWN`` is unreachable from a failing COMMIT (G32).

    The aborted variant has to be driven as a PRE-commit failure, which is provably not
    applied — and there the ordinary fallback terminal write is correct, not an abandonment.
    Injected at the CAS statement itself, the first effectful write of that transaction.
    """
    device_id = await seed_device(nso_device_name="sr-proof", netbox_device_id=7317)
    await seed_rows(device_id, [{"triple": B, "route_id": 7, "deployed_key": list(A)}])
    client, _rec = proof_client("sr-proof", state=present(wire(A), device_name="sr-proof"), section=dev_state(wire(B)))
    job_id = await seed_apply_job(device_id)
    reg = await _acquire(device_id, job_id)

    with patch(
        "nso_adapter.store.static_route_store.cas_deployed_key",
        new=AsyncMock(side_effect=RuntimeError("statement failed before COMMIT")),
    ):
        await _drive_one_job(device_id, job_id, client, reg)

    job = await read_job(job_id)
    assert job.status == JobStatus.failed
    assert job.error["code"] == "internal"
    assert await deployed_keys(device_id) == {B: list(A)}
    assert await _claim_rows() == [], "a known-aborted run releases its claim normally"


# ── C1.9, for the transaction C3 adds ───────────────────────────────────────


async def test_a_revoked_claim_stops_the_bookkeeping_transaction(adapter_client):
    """C1.9 on C3's site — a revoked holder must not close a replacement.

    ``ClaimLostError`` propagates instead of the runner writing ``failed`` itself: recovery
    already owns the disposition, and a runner-written status would clobber it.
    """
    from nso_adapter.core.claim import ClaimLostError

    device_id = await seed_device(nso_device_name="sr-proof", netbox_device_id=7318)
    await seed_rows(device_id, [{"triple": B, "route_id": 7, "deployed_key": list(A)}])
    client, _rec = proof_client("sr-proof", state=present(wire(A), device_name="sr-proof"), section=dev_state(wire(B)))
    job_id = await seed_apply_job(device_id)
    reg = await _acquire(device_id, job_id)
    await _revoke(device_id)

    from nso_adapter.core.apply import run_apply

    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=client),
        patch("nso_adapter.core.apply._post_apply_refresh_and_notify", new=AsyncMock()),
        pytest.raises(ClaimLostError),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True, reg=reg)

    assert await deployed_keys(device_id) == {B: list(A)}
    assert (await read_job(job_id)).status == JobStatus.running, "the runner must not write its own terminal status"


# ── C2.7 / C2.10 — the `unproven` halves A2 reassigned to C3 ─────────────────


async def test_c2_7_a_verify_disabled_apply_closes_nothing_and_reports_unproven(adapter_client, monkeypatch):
    """C2.7 (unproven half) — verification off, replacement open.

    ``build_plan`` refuses the PUT, so the merge that follows leaves ``A`` on the device.
    CASing ``deployed_key := B`` there closes the replacement permanently over a route that
    is still live — the one outcome worse than not delivering it at all.
    """
    monkeypatch.setattr("nso_adapter.nso.apply.VERIFY_AFTER_APPLY", False)
    device_id = await seed_device(nso_device_name="sr-proof", netbox_device_id=7319)
    await seed_rows(device_id, [{"triple": B, "route_id": 7, "deployed_key": list(A)}])
    client, rec = proof_client("sr-proof", state=present(wire(A), device_name="sr-proof"), section=dev_state(wire(B)))

    job = await run_the_apply(device_id, client)

    assert job.status == JobStatus.succeeded, job.error
    assert [c["method"] for c in rec.sr_commits()] == ["patch"]
    assert await deployed_keys(device_id) == {B: list(A)}, "the replacement stays OPEN"
    assert outcomes(job) == {B: "unproven"}


@pytest.mark.parametrize("atomic", [False, True], ids=["per-scope", "atomic"])
async def test_a_merge_never_closes_a_replacement_even_with_a_conclusive_proof(adapter_client, monkeypatch, atomic):
    """C2.7's rule with verification ON — the fence-shut half, and the only discriminating one.

    With verification off the row is unproven anyway, so that setup cannot tell a correct
    implementation from one that CASes every proven row. Here the proof IS conclusive and
    the key IS present: the merge added ``B`` and left ``A`` on the device, and recording
    ``deployed_key := B`` would close the replacement over a route that is still live, with
    nothing left pointing at it. Driven on both apply implementations, since staging is
    merge-PATCH only and ignores ``replace`` (G4).
    """
    if atomic:
        monkeypatch.setenv("NSO_ADAPTER_ATOMIC_APPLY", "1")
    device_id = await seed_device(nso_device_name="sr-proof", netbox_device_id=7326)
    await seed_rows(
        device_id,
        [
            {"triple": B, "route_id": 7, "deployed_key": list(A)},
            {"triple": C, "route_id": None},  # the shut fence: nothing may claim a replacement
        ],
    )
    client, rec = proof_client("sr-proof", state=absent(), section=dev_state(wire(A), wire(B), wire(C)))

    job = await run_the_apply(device_id, client)

    assert job.status == JobStatus.succeeded, job.error
    if not atomic:
        assert [c["method"] for c in rec.sr_commits()] == ["patch"], "a shut fence forbids the replace"
    assert await deployed_keys(device_id) == {B: list(A), C: list(C)}, "the replacement stays OPEN"
    assert outcomes(job) == {B: "unproven", C: "in_sync"}


@pytest.mark.parametrize("atomic", [False, True], ids=["per-scope", "atomic"])
async def test_c2_10_a_row_owing_a_clear_is_unproven_on_both_paths(adapter_client, monkeypatch, atomic):
    """C2.10 (unproven half) — a merge-PATCH apply cannot deliver a recorded clear.

    The renderer omits the leaf and the merge never drops one, so the device keeps the old
    value while reader-compare — which checks the route KEY only — is fully satisfied.
    Reporting ``in_sync`` here is precisely the certified false green the carrier exists to
    prevent, and the atomic path is a separate early return that must not be missed.
    """
    if atomic:
        monkeypatch.setenv("NSO_ADAPTER_ATOMIC_APPLY", "1")
    device_id = await seed_device(nso_device_name="sr-proof", netbox_device_id=7320)
    await seed_rows(
        device_id,
        [{"triple": B, "route_id": 7, "pending_clear": {"authorized": ["metric"], "store_only": []}}],
    )
    client, _rec = proof_client("sr-proof", state=absent(), section=dev_state(wire(B, metric=10)))

    job = await run_the_apply(device_id, client)

    assert job.status == JobStatus.succeeded, job.error
    assert outcomes(job) == {B: "unproven"}
    assert await carriers(device_id) == {B: {"authorized": ["metric"], "store_only": []}}
    assert len(await removal_contexts(device_id)) == 1, "exactly one networked retract is queued"


# ── C2.11 (apply side) — per-FIELD evidence, never key-grain, never falsiness ─


@pytest.mark.parametrize(
    ("live_entry", "expect_carrier", "expect_outcome"),
    [
        (wire(B, metric=10), {"authorized": ["metric"], "store_only": []}, "unproven"),
        (wire(B, metric=0), {"authorized": ["metric"], "store_only": []}, "unproven"),
        (wire(B), None, "in_sync"),
    ],
    ids=["leaf-still-set", "leaf-is-zero", "leaf-gone"],
)
async def test_c2_11_a_clear_is_consumed_only_on_per_field_absence(
    adapter_client, live_entry, expect_carrier, expect_outcome
):
    """C2.11 (apply side) — the write succeeded and the KEY proof is clean.

    That is the discriminator: a 2xx + conclusive verify + present key is fully satisfied by
    a route whose old ``metric`` is still live. Only per-field evidence may empty the
    carrier, and ``0`` is a real value the renderer emits — a falsiness check would eat it.
    """
    device_id = await seed_device(nso_device_name="sr-proof", netbox_device_id=7321)
    await seed_rows(
        device_id,
        [
            {
                "triple": B,
                "route_id": 7,
                "deployed_key": list(A),
                "pending_clear": {"authorized": ["metric"], "store_only": []},
            }
        ],
    )
    client, _rec = proof_client(
        "sr-proof", state=present(wire(A), device_name="sr-proof"), section=dev_state(live_entry)
    )

    job = await run_the_apply(device_id, client)

    assert job.status == JobStatus.succeeded, job.error
    assert await carriers(device_id) == {B: expect_carrier}
    assert outcomes(job) == {B: expect_outcome}


@pytest.mark.parametrize(
    ("live_entry", "consumed"),
    [(wire(B, permanent=False), True), (wire(B, permanent=True), False), (wire(B), True)],
    ids=["permanent-false-is-neutral", "permanent-true-is-not", "leaf-absent-is-neutral"],
)
async def test_c2_11_permanent_is_neutral_when_false_because_the_renderer_never_emits_it(
    adapter_client, live_entry, consumed
):
    """C2.11 — ``permanent`` is the ONE field where ``false`` counts as unset (G27)."""
    device_id = await seed_device(nso_device_name="sr-proof", netbox_device_id=7322)
    await seed_rows(
        device_id,
        [
            {
                "triple": B,
                "route_id": 7,
                "deployed_key": list(A),
                "pending_clear": {"authorized": ["permanent"], "store_only": []},
            }
        ],
    )
    client, _rec = proof_client(
        "sr-proof", state=present(wire(A), device_name="sr-proof"), section=dev_state(live_entry)
    )

    job = await run_the_apply(device_id, client)

    assert job.status == JobStatus.succeeded, job.error
    expected = None if consumed else {"authorized": ["permanent"], "store_only": []}
    assert await carriers(device_id) == {B: expected}
    assert outcomes(job) == {B: "in_sync" if consumed else "unproven"}


async def test_a1_a_store_only_clear_is_consumed_by_the_put_that_delivers_it(adapter_client):
    """A1 — promotion is by DELIVERY, and only the PUT path delivers.

    A ``store_only`` entry never authorizes a removal job, but a PUT-mode apply's
    store-rendered body omits the leaf as a consequence of delivering intent that WAS
    authorized — so proof may consume it from whichever list carried it.
    """
    device_id = await seed_device(nso_device_name="sr-proof", netbox_device_id=7323)
    await seed_rows(
        device_id,
        [
            {
                "triple": B,
                "route_id": 7,
                "deployed_key": list(A),
                "pending_clear": {"authorized": [], "store_only": ["tag"]},
            }
        ],
    )
    client, rec = proof_client("sr-proof", state=present(wire(A), device_name="sr-proof"), section=dev_state(wire(B)))

    job = await run_the_apply(device_id, client)

    assert job.status == JobStatus.succeeded, job.error
    assert [c["method"] for c in rec.sr_commits()] == ["put"]
    assert await carriers(device_id) == {B: None}
    assert outcomes(job) == {B: "in_sync"}
    assert await removal_contexts(device_id) == [], "a store_only clear still enqueues no deletion job"


# ── the no-static-route case keeps job.result exactly as it was ──────────────


async def test_a_device_with_no_static_routes_gains_no_result_key(adapter_client):
    """No rows ⇒ no per-route record: an empty list would read as "we looked and found none"."""
    device_id = await seed_device(nso_device_name="sr-proof", netbox_device_id=7324)
    client, _rec = proof_client("sr-proof", state=absent(), section=dev_state())

    job = await run_the_apply(device_id, client)

    assert job.status == JobStatus.succeeded, job.error
    assert "static_route_results" not in (job.result or {})


# ── the residue read is not paid for when nothing depends on it ──────────────


async def test_no_predecessor_and_no_carrier_means_no_extra_device_read(adapter_client):
    """A plain merge apply must cost exactly what it costs today — one reader-compare read."""
    device_id = await seed_device(nso_device_name="sr-proof", netbox_device_id=7325)
    await seed_rows(device_id, [{"triple": B, "route_id": 7}])
    client, _rec = proof_client("sr-proof", state=absent(), section=dev_state(wire(B)))

    await run_the_apply(device_id, client)

    assert client.run_device_state_read.await_count == 1
