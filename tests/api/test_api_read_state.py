# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""READSEM S4 A4: the read-state API surface.

Covers the aggregate ``GET /devices/{id}/read-state`` (all 19 families, synthesized
``not_ready`` for pointerless ones, the incarnation pair on EVERY entry), the inline
``read_state`` block on family GETs (static-routes is the reference), and the
``result=error`` terminal shape serializing (the R1-F3 row).
"""

from __future__ import annotations

import pytest

from nso_adapter.core.families import ALL_FAMILY_KEYS, FAMILIES_VERSION
from nso_adapter.nso.read_outcome import Freshness, Present, Unavailable, UnavailableReason
from nso_adapter.store import outcome_store
from nso_adapter.store.db import get_session
from nso_adapter.store.meta import get_store_incarnation
from tests.conftest import VALID_TOKEN, seed_device

_AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


async def _terminalize(device_id: int, family: str, outcome, *, result: str, succeeded: bool, rows: int | None = 0):
    async for db in get_session():
        attempt_id = await outcome_store.record_read_outcome(db, device_id, family, outcome, refresh_source="poll")
        await outcome_store.record_result(db, attempt_id, result=result, succeeded=succeeded, row_count=rows)
        return attempt_id
    raise AssertionError("no session")


@pytest.mark.anyio
async def test_aggregate_serves_all_families_with_synthesized_not_ready(adapter_client):
    """Every canonical family appears; pointerless ones synthesize unavailable/not_ready
    with attempt_id null — and EVERY entry carries the incarnation pair."""
    device_id = await seed_device(nso_device_name="rs-agg1", netbox_device_id=8901)
    a1 = await _terminalize(
        device_id, "static_route", Present({"r": []}, Freshness.fresh), result="replaced", succeeded=True, rows=2
    )

    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/read-state", headers=_AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["device_id"] == device_id
    assert body["families_version"] == FAMILIES_VERSION
    assert set(body["families"]) == set(ALL_FAMILY_KEYS)

    incarnation, born = get_store_incarnation()
    sr = body["families"]["static_route"]
    assert (sr["outcome"], sr["result"], sr["succeeded"], sr["attempt_id"]) == ("present", "replaced", True, a1)
    assert sr["freshness"] == "fresh"
    assert sr["incarnation"] == incarnation
    assert sr["incarnation_born"] == born.isoformat() + "Z"
    assert sr["read_at"] is not None

    bgp = body["families"]["bgp"]  # never terminalized → synthesized
    assert (bgp["outcome"], bgp["reason"], bgp["attempt_id"], bgp["result"]) == (
        "unavailable",
        "not_ready",
        None,
        None,
    )
    assert bgp["incarnation"] == incarnation
    assert bgp["incarnation_born"] == born.isoformat() + "Z"


@pytest.mark.anyio
async def test_aggregate_unknown_device_404(adapter_client):
    resp = await adapter_client.get("/api/v1/devices/999999/read-state", headers=_AUTH)
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"


@pytest.mark.anyio
async def test_result_error_row_serializes(adapter_client):
    """The R1-F3 shape: a materializer-failure terminal (result=error, succeeded=False)
    must serialize — a pydantic literal without 'error' would 500 here."""
    device_id = await seed_device(nso_device_name="rs-err1", netbox_device_id=8902)
    await _terminalize(
        device_id, "isis", Present({"i": []}, Freshness.fresh), result="error", succeeded=False, rows=None
    )
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/read-state", headers=_AUTH)
    assert resp.status_code == 200
    isis = resp.json()["families"]["isis"]
    assert (isis["outcome"], isis["result"], isis["succeeded"]) == ("present", "error", False)


@pytest.mark.anyio
async def test_inline_read_state_on_family_get(adapter_client):
    """The reference family GET (static-routes) carries a top-level read_state block
    resolved from the pointer — same session as the rows."""
    device_id = await seed_device(nso_device_name="rs-inline1", netbox_device_id=8903)
    a1 = await _terminalize(
        device_id,
        "static_route",
        Unavailable(UnavailableReason.export_down, "boom"),
        result="kept",
        succeeded=False,
        rows=None,
    )
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/static-routes", headers=_AUTH)
    assert resp.status_code == 200
    body = resp.json()
    rs = body["read_state"]
    assert (rs["outcome"], rs["reason"], rs["result"], rs["succeeded"], rs["attempt_id"]) == (
        "unavailable",
        "export_down",
        "kept",
        False,
        a1,
    )
    incarnation, _born = get_store_incarnation()
    assert rs["incarnation"] == incarnation
    # legacy fields unchanged next to the new block (S5 retires them, not S4)
    assert body["refresh_source"] == "never"
    assert body["routes"] == []


@pytest.mark.anyio
async def test_inline_read_state_synthesized_when_pointerless(adapter_client):
    """A family GET for a device with NO pointer synthesizes not_ready inline — the
    adapter never emits read_state: null (key-absent means pre-S4 adapter, D3)."""
    device_id = await seed_device(nso_device_name="rs-inline2", netbox_device_id=8904)
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/static-routes", headers=_AUTH)
    assert resp.status_code == 200
    rs = resp.json()["read_state"]
    assert (rs["outcome"], rs["reason"], rs["attempt_id"]) == ("unavailable", "not_ready", None)
    assert rs["incarnation"] == get_store_incarnation()[0]
