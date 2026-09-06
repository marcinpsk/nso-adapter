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

import json
from types import SimpleNamespace
from unittest.mock import patch

import httpx
import pytest
import sqlalchemy as sa

from nso_adapter.core.apply import SNAPSHOT_INCONCLUSIVE
from nso_adapter.nso.apply import static_route_entry_key
from tests.conftest import seed_device, session
from tests.core.removal_helpers import authorize_static_route, seed_removal_job, seed_tomb
from tests.core.test_static_route_put import (
    _SR_ROOT,
    A,
    B,
    C,
    absent,
    inconclusive,
    present,
    run_the_apply,
    seed_rows,
    sr_client,
    wire,
)

pytestmark = pytest.mark.anyio

#: A live entry carrying leaves the store has no column for. Retention is verbatim or it is
#: nothing: rebuilding this from a store triple would silently rewrite the router's own state.
_RICH_A = wire(A, metric=10, tag=101)
_RICH_A["bfd-fast-detect"] = {"minimum": 50}


async def _carrier_for(device_id: int, triple, **kwargs) -> int:
    """A live carrier claiming *triple*, with the fragment its deletion push authorized."""
    tomb = await seed_tomb(device_id, triple, **kwargs)
    await authorize_static_route(device_id)
    return tomb


# ── the retained entry itself ────────────────────────────────────────────────


async def test_a_carrier_claimed_key_is_retained_verbatim_beside_the_documents_rows(adapter_client):
    """An unconsumed carrier's key survives a write that renders something else entirely.

    Without retention the full-document PUT drops A the moment any other family's push is
    deployed, and the deletion record that owes A's cleanup has nothing left to clean.
    """
    device_id = await seed_device(nso_device_name="sr-retain-verbatim", netbox_device_id=17301)
    await seed_rows(device_id, [{"triple": B, "route_id": 2}])
    await _carrier_for(device_id, A, route_id=1)
    client, rec = sr_client("sr-retain-verbatim", state=present(_RICH_A, wire(B), device_name="sr-retain-verbatim"))

    job = await run_the_apply(device_id, client)

    assert job.status.value == "succeeded", job.error
    routes = rec.routes(rec.sr_commits("put")[0])
    assert routes == [wire(B), _RICH_A], "the body is the document's rows plus the retained entry, verbatim"
    assert routes[1]["bfd-fast-detect"] == {"minimum": 50}, "a leaf the store cannot express must survive"


async def test_a_key_the_document_renders_is_not_retained(adapter_client):
    """``reasserted`` wins: the store owns a key a row still renders, stale bytes and all."""
    device_id = await seed_device(nso_device_name="sr-retain-rendered", netbox_device_id=17302)
    await seed_rows(device_id, [{"triple": A, "route_id": 1}])
    await _carrier_for(device_id, A, route_id=1)
    client, rec = sr_client("sr-retain-rendered", state=present(_RICH_A, device_name="sr-retain-rendered"))

    job = await run_the_apply(device_id, client)

    assert job.status.value == "succeeded", job.error
    assert rec.routes(rec.sr_commits("put")[0]) == [wire(A)], "the rendered row must not lose to the live copy"


async def test_certified_absence_retains_nothing(adapter_client):
    """Nothing on the service means nothing to preserve, and no orphan either."""
    device_id = await seed_device(nso_device_name="sr-retain-absent", netbox_device_id=17303)
    await seed_rows(device_id, [{"triple": B, "route_id": 2}])
    await _carrier_for(device_id, A, route_id=1)
    client, rec = sr_client("sr-retain-absent", state=absent())

    job = await run_the_apply(device_id, client)

    assert job.status.value == "succeeded", job.error
    assert rec.routes(rec.sr_commits("put")[0]) == [wire(B)]


async def test_an_uncertifiable_read_refuses_the_send(adapter_client):
    """A body built from "looks empty" drops what it had to retain and verifies cleanly."""
    device_id = await seed_device(nso_device_name="sr-retain-unread", netbox_device_id=17304)
    await seed_rows(device_id, [{"triple": B, "route_id": 2}])
    await _carrier_for(device_id, A, route_id=1)
    client, rec = sr_client("sr-retain-unread", state=inconclusive())

    job = await run_the_apply(device_id, client)

    assert job.status.value == "failed"
    assert rec.sr_commits() == [], "nothing may reach the device on an uncertified read"
    from nso_adapter.store.models import StaticRouteIntent

    async with session() as db:
        row = (await db.execute(sa.select(StaticRouteIntent))).scalars().first()
    assert row.last_apply_error["code"] == SNAPSHOT_INCONCLUSIVE


# ── the key set is frozen, the bytes are current ─────────────────────────────


async def test_the_key_set_is_frozen_while_the_bytes_track_the_service(adapter_client):
    """Charter amendment 1: the document freezes the KEY SET, never the payload.

    Between two deployments of the same authorized state the service entry can only have
    changed through the adapter's own later authorized write, and then the newer value is the
    correct one: re-asserting a superseded copy would be an unauthorized write.
    """
    device_id = await seed_device(nso_device_name="sr-retain-bytes", netbox_device_id=17305)
    await seed_rows(device_id, [{"triple": B, "route_id": 2}])
    await _carrier_for(device_id, A, route_id=1)

    first_client, first_rec = sr_client("sr-retain-bytes", state=present(_RICH_A, device_name="sr-retain-bytes"))
    assert (await run_the_apply(device_id, first_client)).status.value == "succeeded"

    moved = wire(A, metric=20, tag=202)
    second_client, second_rec = sr_client("sr-retain-bytes", state=present(moved, device_name="sr-retain-bytes"))
    assert (await run_the_apply(device_id, second_client)).status.value == "succeeded"

    first = rec_keys(first_rec)
    second = rec_keys(second_rec)
    assert first == second == {A, B}, "the retained key set is a property of the document"
    assert rec_entry(second_rec, A) == moved, "the retained bytes are the service's CURRENT certified entry"
    assert rec_entry(first_rec, A)["metric"] == 10


def rec_keys(rec) -> set:
    """The route keys the last committed document carried."""
    return {static_route_entry_key(entry) for entry in rec.routes(rec.sr_commits("put")[0])}


def rec_entry(rec, triple) -> dict:
    """The entry the last committed document carried for *triple*."""
    return next(e for e in rec.routes(rec.sr_commits("put")[0]) if static_route_entry_key(e) == triple)


async def test_a_deployed_only_predecessor_is_retained_while_its_replacement_is_rendered(adapter_client):
    """The F2 sequence's sender half: L is rendered, K is retained through the carrier.

    A carrier claiming only a REPLACED predecessor key still owes its cleanup, so the body has
    to keep K alive until that cleanup runs; dropping it would strand the key with no carrier
    able to prove it gone.
    """
    device_id = await seed_device(nso_device_name="sr-retain-predecessor", netbox_device_id=17306)
    await seed_rows(device_id, [{"triple": C, "route_id": 2, "deployed_key": list(A)}])
    await _carrier_for(device_id, A, route_id=1)
    client, rec = sr_client("sr-retain-predecessor", state=present(_RICH_A, device_name="sr-retain-predecessor"))

    job = await run_the_apply(device_id, client)

    assert job.status.value == "succeeded", job.error
    assert rec_keys(rec) == {A, C}, "the predecessor a live carrier still claims must ride the body"


# ── the operation plane wins over any carrier claim ──────────────────────────


async def test_an_operation_selected_key_is_never_retained(adapter_client):
    """Key-level precedence: the removal that authorizes K may not re-assert it.

    ``operation_selected`` is the third subtraction. Without it the very removal that owes K's
    deletion retains K through the carrier it is discharging, and the job reports success
    having changed nothing.
    """
    from tests.core.test_static_route_removal import SrFake, run_removal_job
    from tests.core.test_static_route_removal import sr_client as sr_stateful_client

    device_id = await seed_device(nso_device_name="sr-retain-selected", netbox_device_id=17307)
    await seed_rows(device_id, [{"triple": B, "route_id": 2}])
    tomb = await _carrier_for(device_id, A, route_id=1)
    job_id = await seed_removal_job(device_id, {"removed": {"route": [list(A)]}}, tombs=(tomb,))

    fake = SrFake("sr-retain-selected", service=[wire(A), wire(B)])
    job = await run_removal_job(device_id, job_id, sr_stateful_client(fake))

    assert job.status.value == "succeeded", job.error
    assert fake.sent_keys() == {B}, "the key this operation authorizes must not be retained"
    assert fake.service_keys == {B}


async def test_a_force_selected_section_retains_nothing(adapter_client):
    """The operator's flush stays a flush.

    A force reissue carries no removal authority, so ``operation_selected`` is empty and the
    formula alone would preserve every carrier-claimed key: the opposite of what the endpoint
    promises. Retention is suppressed for the force-selected section instead.
    """
    from nso_adapter.core.removal import enqueue_removal
    from tests.core.test_static_route_removal import SrFake, run_removal_job
    from tests.core.test_static_route_removal import sr_client as sr_stateful_client

    device_id = await seed_device(nso_device_name="sr-retain-force", netbox_device_id=17308)
    await seed_rows(device_id, [{"triple": B, "route_id": 2}])
    tomb = await _carrier_for(device_id, A, route_id=1)

    async with session() as db:
        job = await enqueue_removal(
            db, device_id, "static_route", marking=None, defer_retract=False, promotes=(), force=True
        )
        await db.commit()
        job_id = job.id

    fake = SrFake("sr-retain-force", service=[wire(A), wire(B)])
    finished = await run_removal_job(device_id, job_id, sr_stateful_client(fake))

    assert finished.status.value == "succeeded", finished.error
    assert fake.sent_keys() == {B}, "the flush must not preserve the very keys it is flushing"
    from tests.core.test_static_route_removal import tombstone_ids

    assert await tombstone_ids(device_id) == [tomb], "a flush consumes no carrier (#1475 is untouched)"


# ── the read is of the AGGREGATE instance, at its own path ───────────────────


async def test_the_certified_read_asks_the_aggregate_path_and_ignores_the_legacy_one():
    """Path sensitivity, through the REAL client: one URL, one nesting.

    A fake that answers every certified read from one state whatever path is requested would
    hide the defect this reader exists to prevent, so this drives ``NsoClient`` itself and
    answers by URL: the legacy reconciler instance 404s while the aggregate holds the key.
    """
    from nso_adapter.config import NsoInstanceConfig
    from nso_adapter.core.static_route_reader import certified_static_route_section
    from nso_adapter.nso.client import NsoClient

    requested: list[str] = []

    class _ByPath(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request):
            requested.append(str(request.url))
            if "device-intent" in str(request.url):
                body = {_SR_ROOT: [{"device": "rtr", "static-route": {"route": [_RICH_A]}}]}
                return httpx.Response(200, content=json.dumps(body).encode(), request=request)
            return httpx.Response(404, content=b"{}", request=request)

    client = NsoClient(
        NsoInstanceConfig(
            name="nso-dev",
            base_url="http://nso:8080",
            ca_cert=None,
            username_ref="NSO_USERNAME",
            password_ref="NSO_PASSWORD",
            host_header=None,
        ),
        "admin",
        "secret",
    )
    client._client = lambda timeout=None: httpx.AsyncClient(transport=_ByPath(), base_url="http://nso:8080")

    section = await certified_static_route_section(client, SimpleNamespace(nso_device_name="rtr"))

    assert section.status == "present"
    assert section.routes == [_RICH_A]
    assert requested == ["http://nso:8080/restconf/data/device-intent:device-intent=rtr"]
    assert not any("reconciler" in url for url in requested), "no path may fall back to the legacy instance"


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
