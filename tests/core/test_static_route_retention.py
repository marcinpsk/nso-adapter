# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The retained-entry rule of the aggregate sender (#1683, memo A11).

The one exception to encoding from the document alone: the static-route container carries the
document's own rows PLUS, verbatim, the live certified entry of every key the frozen plan
retains. The retained KEY SET is a pure function of the document — ``claimed - reasserted -
operation_selected`` — and only the BYTES come from the live read, so a retry sends the same
keys and the latest authorized value of each.

Every case drives the REAL worker over the real store and asserts the TRANSMITTED document.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
import sqlalchemy as sa

from nso_adapter.core.apply import SNAPSHOT_INCONCLUSIVE
from nso_adapter.nso.apply import static_route_entry_key
from tests.conftest import seed_device, session
from tests.core.removal_helpers import authorize_static_route, seed_tomb
from tests.core.test_static_route_put import (
    _SR_ROOT,
    A,
    B,
    present,
    seed_rows,
    sr_client,
    wire,
)

pytestmark = pytest.mark.anyio

#: A live entry carrying leaves no tombstone triple names. Retention is verbatim or it is
#: nothing: rebuilding this from a store triple would silently rewrite the router's own state.
_RICH_A = wire(A, metric=10, tag=101)
_RICH_A["interface-next-hop"] = "GigabitEthernet0/3"


async def _carrier_for(device_id: int, triple, **kwargs) -> int:
    """A live carrier claiming *triple*, with the fragment its deletion push authorized."""
    tomb = await seed_tomb(device_id, triple, **kwargs)
    await authorize_static_route(device_id)
    return tomb


# ── the preview is bound to the document being committed ─────────────────────


async def test_the_preview_is_the_document_being_committed_not_a_live_store_estimate(adapter_client):
    """A store-only edit renders in no preview, because the commit could never send it.

    The preview used to build its own plan from LIVE rows while execution hydrated the frozen
    document, so an operator approved a diff the commit was structurally unable to produce.
    """
    from nso_adapter.core.apply import PREVIEW_KEY, collect_apply_diff
    from nso_adapter.core.generation import create_generation, note_write
    from nso_adapter.store.models import GenerationMode, StaticRouteIntent

    device_id = await seed_device(nso_device_name="sr-preview-bound", netbox_device_id=17309)
    await seed_rows(device_id, [{"triple": B, "route_id": 2}])
    await _carrier_for(device_id, A, route_id=1)

    # The generation freezes the authorized state, and only THEN does a store-only edit land.
    async with session() as db:
        await note_write(db, device_id, "static_route")
        await create_generation(db, device_id, streams=("static_route",), mode=GenerationMode.networked)
        await db.commit()
    async with session() as db:
        row = (await db.execute(sa.select(StaticRouteIntent).where(StaticRouteIntent.prefix == B[1]))).scalars().one()
        row.metric = 999  # store-only: nothing authorized it
        await db.commit()

    client, rec = sr_client(
        "sr-preview-bound", state=present(_RICH_A, device_name="sr-preview-bound"), dry_run_delta="+ ip route"
    )
    async with session() as db:
        with patch("nso_adapter.core.importer.get_nso_client", return_value=client):
            diffs = await collect_apply_diff(db, device_id)

    assert set(diffs) == {PREVIEW_KEY}, "one document is one transaction, so the preview is one delta"
    previewed = rec.sr_payloads(dry_run=True)[0][_SR_ROOT][0]["static-route"]["route"]
    assert 999 not in {entry.get("metric") for entry in previewed}, "the preview showed a store-only edit"
    assert static_route_entry_key(_RICH_A) in {static_route_entry_key(e) for e in previewed}, (
        "the preview must show the retained entry the commit will send"
    )


@pytest.mark.parametrize("scope", ["vlan", "static_route"])
async def test_force_removal_suppresses_only_the_selected_static_route_section(adapter_client, scope):
    """A force flush of THIS section drops the retention; an unrelated one keeps it.

    The flush carries no removal authority, so retaining every carrier-claimed key would
    preserve exactly what the operator override promises to remove. It still consumes no
    carrier, and it discharges the pending clears of its own section's streams only.
    """
    from nso_adapter.store.models import StreamPendingClear
    from tests.core.removal_helpers import authorize_stream
    from tests.core.test_action_apply_promotion import AUTH, _put_vlans
    from tests.core.test_generation_protocol import job_row, run_head
    from tests.core.test_static_route_removal import SrFake, tombstone_ids
    from tests.core.test_static_route_removal import sr_client as stateful_client

    device_id = await seed_device(nso_device_name="force-scope", netbox_device_id=17310)
    await seed_rows(device_id, [{"triple": B, "route_id": 2}])
    tomb = await _carrier_for(device_id, A, route_id=1)
    response = await _put_vlans(adapter_client, device_id, [100], seq=1, query="?apply=false")
    assert response.status_code == 200, response.text
    await authorize_stream(device_id, "vlan")
    async with session() as db:
        db.add(StreamPendingClear(device_id=device_id, stream="static_route", provenance="store_only", revision=1))
        await db.commit()
    response = await adapter_client.post(
        f"/api/v1/devices/{device_id}/actions/force-removal", json={"scope": scope}, headers=AUTH
    )
    assert response.status_code == 202, response.text
    fake = SrFake("force-scope", service=[_RICH_A, wire(B)])
    job_id = await run_head(device_id, stateful_client(fake))
    job = await job_row(job_id)
    assert job.status.value == "succeeded", job.error
    assert fake.sent_keys() == ({A, B} if scope == "vlan" else {B})
    assert await tombstone_ids(device_id) == [tomb], "a force flush consumes no carrier"
    async with session() as db:
        clears = (await db.execute(sa.select(StreamPendingClear.stream))).scalars().all()
    assert clears == ([] if scope == "static_route" else ["static_route"])
    if scope == "vlan":
        assert _RICH_A in fake.service


async def test_carrier_only_build_refusal_fails_the_document(adapter_client):
    from tests.core.test_action_apply_promotion import _generations, _put_vlans
    from tests.core.test_generation_protocol import job_row, run_head, seed_settings
    from tests.core.test_static_route_removal import SrFake
    from tests.core.test_static_route_removal import sr_client as stateful_client

    device_id = await seed_device(nso_device_name="carrier-only-refusal", netbox_device_id=17311)
    await _carrier_for(device_id, A, route_id=1)
    await seed_settings(device_id, auto_apply=True)
    response = await _put_vlans(adapter_client, device_id, [100], seq=1)
    assert response.status_code == 200, response.text
    fake = SrFake("carrier-only-refusal", service=[], service_status="inconclusive")
    job = await job_row(await run_head(device_id, stateful_client(fake)))
    assert job.status.value == "failed", job.result
    assert all(g.status.value != "settled" for g in await _generations(device_id))
    assert not any(call["method"] == "put" for call in fake.calls)


async def test_preview_uses_the_highest_generation_of_the_coalesced_job(adapter_client):
    from nso_adapter.core.apply import collect_apply_diff
    from tests.core.test_action_apply_promotion import _generations, _put_vlans
    from tests.core.test_generation_protocol import recorded_client, run_head, seed_settings

    device_id = await seed_device(nso_device_name="preview-coalesced", netbox_device_id=17313)
    await seed_settings(device_id, auto_apply=True)
    assert (await _put_vlans(adapter_client, device_id, [100], seq=1)).status_code == 200
    assert (await _put_vlans(adapter_client, device_id, [100, 200], seq=2)).status_code == 200
    generations = await _generations(device_id)
    assert len(generations) == 2
    assert generations[0].job_id == generations[1].job_id
    client, rec = recorded_client("preview-coalesced")
    async with session() as db:
        with patch("nso_adapter.core.importer.get_nso_client", return_value=client):
            await collect_apply_diff(db, device_id)
    preview = next(call["body"] for call in rec.calls if call["dry_run"])
    await run_head(device_id, client)
    assert preview == rec.commits[0]["body"]


async def test_retained_key_set_uses_current_certified_service_bytes(adapter_client):
    from copy import deepcopy

    from tests.core.static_route_harness import K, S, fixture_r

    harness, tomb = await fixture_r(adapter_client)
    first = deepcopy(harness.fake.service)
    await harness.drain()
    assert harness.fake.sent_routes() == [first[1], first[0]]
    assert retained_keys((await _last_generation(harness)).document) == {K}
    harness.fake.service[1]["metric"] = 30
    harness.fake.service[1]["tag"] = 303
    current = deepcopy(harness.fake.service[1])
    generation = await harness.unrelated()
    assert retained_keys(generation.document) == {K}
    await harness.run()
    assert harness.fake.sent_keys() == {K, S}
    assert next(r for r in harness.fake.sent_routes() if static_route_entry_key(r) == K) == current
    from tests.core.test_static_route_removal import tombstone_ids

    assert await tombstone_ids(harness.device_id) == [tomb]


async def _last_generation(harness):
    from tests.core.test_generation_protocol import generations

    return (await generations(harness.device_id))[-1]


def retained_keys(document):
    section = document["static_route"]
    claimed = set()
    for tomb in section["static_route_tombstone"]:
        claimed.add((tomb["vrf"], tomb["prefix"], tomb["next_hop"]))
        if tomb["deployed_key"] is not None:
            claimed.add(tuple(tomb["deployed_key"]))
    rendered = {(row["vrf"], row["prefix"], row["next_hop"]) for row in section["static_route_intent"]}
    operation = section["_execution"].get("operation", {}).get("removal", {})
    selected = {tuple(key) for key in operation.get("authorized_removal_keys", [])}
    return claimed - rendered - selected


async def test_adapter_reauthorization_transmits_the_current_metric(adapter_client):
    from tests.core.static_route_harness import K, S, fixture_r, route

    harness, _ = await fixture_r(adapter_client)
    await harness.drain()
    assert retained_keys((await _last_generation(harness)).document) == {K}
    assert next(r for r in harness.fake.sent_routes() if static_route_entry_key(r) == K)["metric"] == 10
    await harness.push([route(metric=20, tag=202), route(S, route_id=2)])
    await harness.run()
    authorized = next(r for r in harness.fake.sent_routes() if static_route_entry_key(r) == K)
    assert authorized["metric"] == 20
    assert authorized["tag"] == 202
    generation = await harness.unrelated()
    assert retained_keys(generation.document) == set()
    await harness.run()
    assert harness.fake.sent_keys() == {K, S}
    assert next(r for r in harness.fake.sent_routes() if static_route_entry_key(r) == K) == authorized


@pytest.mark.parametrize("status", ["absent", "inconclusive"])
async def test_retention_certification_controls_transmission(adapter_client, monkeypatch, status):
    from tests.core.static_route_harness import S, fixture_r
    from tests.core.test_static_route_removal import tombstone_ids

    harness, tomb = await fixture_r(adapter_client)
    harness.fake.service_status = status
    writes = len(harness.fake.writes)
    from nso_adapter.core import apply as apply_module

    build = apply_module.build_device_containers
    errors = []

    async def observe_build(*args, **kwargs):
        body = await build(*args, **kwargs)
        errors.extend(error.code for error in body.errors.values())
        return body

    monkeypatch.setattr(apply_module, "build_device_containers", observe_build)
    await harness.run(status="failed" if status == "inconclusive" else "succeeded")
    if status == "inconclusive":
        assert len(harness.fake.writes) == writes
        assert errors == [SNAPSHOT_INCONCLUSIVE]
    else:
        assert harness.fake.sent_keys() == {S}
    assert await tombstone_ids(harness.device_id) == [tomb]
