# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""End-to-end API tests for the route-policy capability endpoints.

Exercises the real FastAPI wiring the plugin calls at attach time / panel render:
  - POST /capability/refresh   — "check now" (probes NSO, persists the verdict)
  - GET  /capability           — cache-only read (refresh=false) vs probe (refresh=true)
  - POST /route-policy/preflight — block-with-override input the plugin uses at attach

The NSO probe action is faked (no live RESTCONF); everything else is real (FastAPI →
capability core → sqlite store).
"""

from __future__ import annotations

import pytest

from tests.conftest import VALID_TOKEN, adapter_client_with_nso, seed_device  # noqa: F401

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}
_NED = "cisco-ios-cli-6.114"

_PROBE_OUT = {
    "ned-id": _NED,
    "sw-version": "17.15.4c",
    "element": [
        {"scope": "community", "name": "color:0:128", "status": "skipped", "detail": "no IOS home"},
        {"scope": "community", "name": "65000:1", "status": "native", "detail": ""},
        {"scope": "rm-set", "name": "set extcommunity color", "status": "native", "detail": ""},
        {"scope": "rm-set", "name": "set metric-type", "status": "native", "detail": ""},
    ],
}


def _fake_probe(monkeypatch, calls):
    from nso_adapter.core import capability

    async def fake_probe(_client, _name):
        calls.append(_name)
        return _PROBE_OUT

    monkeypatch.setattr(capability.actions, "capability_probe", fake_probe)


@pytest.mark.asyncio
async def test_capability_cache_only_before_probe_is_unknown(adapter_client_with_nso, monkeypatch):  # noqa: F811
    calls: list[str] = []
    _fake_probe(monkeypatch, calls)
    device_id = await seed_device(nso_device_name="rg03")

    resp = await adapter_client_with_nso.get(f"/api/v1/devices/{device_id}/capability", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["known"] is False
    assert body["elements"] == []
    assert calls == []  # refresh=false must not probe


@pytest.mark.asyncio
async def test_refresh_then_cache_only_read(adapter_client_with_nso, monkeypatch):  # noqa: F811
    calls: list[str] = []
    _fake_probe(monkeypatch, calls)
    device_id = await seed_device(nso_device_name="rg03")

    # "check now" — probes and persists
    resp = await adapter_client_with_nso.post(f"/api/v1/devices/{device_id}/capability/refresh", headers=AUTH)
    assert resp.status_code == 200
    assert resp.json() == {"ned_id": _NED, "sw_version": "17.15.4c", "count": 4}
    assert calls == ["rg03"]

    # cache-only read now resolves the stored key WITHOUT another probe
    resp = await adapter_client_with_nso.get(f"/api/v1/devices/{device_id}/capability", headers=AUTH)
    body = resp.json()
    assert body["known"] is True
    assert body["ned_id"] == _NED and body["sw_version"] == "17.15.4c"
    assert {(e["scope"], e["name"]) for e in body["elements"]} == {
        ("community", "color:0:128"),
        ("community", "65000:1"),
        ("rm-set", "set extcommunity color"),
        ("rm-set", "set metric-type"),
    }
    assert calls == ["rg03"]  # still only the one probe


@pytest.mark.asyncio
async def test_preflight_flags_unsupported_parts(adapter_client_with_nso, monkeypatch):  # noqa: F811
    calls: list[str] = []
    _fake_probe(monkeypatch, calls)
    device_id = await seed_device(nso_device_name="rg03")
    await adapter_client_with_nso.post(f"/api/v1/devices/{device_id}/capability/refresh", headers=AUTH)

    # cache-only preflight: a color community member is skipped; metric-type is native
    resp = await adapter_client_with_nso.post(
        f"/api/v1/devices/{device_id}/route-policy/preflight?refresh=false",
        headers=AUTH,
        json={"community_members": ["color:0:200", "65000:9"], "set_keys": ["metric_type"]},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["known"] is True
    assert body["fully_supported"] is False
    flagged = {(u["scope"], u["element"]) for u in body["unsupported"]}
    assert ("community", "color:0:200") in flagged
    assert not any(u["element"] == "65000:9" for u in body["unsupported"])
    assert not any(u["element"] == "set metric-type" for u in body["unsupported"])
    assert calls == ["rg03"]  # the refresh above was the only probe


@pytest.mark.asyncio
async def test_uncovered_ned_reports_coverage_unknown(adapter_client_with_nso, monkeypatch):  # noqa: F811
    """A Junos/Nokia probe emits a 'coverage unknown' marker → API surfaces it, never blocks."""
    from nso_adapter.core import capability

    async def fake_probe(_client, _name):
        return {
            "ned-id": "juniper-junos-nc-4.19",
            "sw-version": "24.4R2",
            "element": [
                {
                    "scope": "coverage",
                    "name": "juniper-junos-nc-4.19",
                    "status": "unknown",
                    "detail": "route-policy capability probing not yet implemented for this NED",
                }
            ],
        }

    monkeypatch.setattr(capability.actions, "capability_probe", fake_probe)
    device_id = await seed_device(nso_device_name="rd1")

    resp = await adapter_client_with_nso.get(f"/api/v1/devices/{device_id}/capability?refresh=true", headers=AUTH)
    body = resp.json()
    assert body["known"] is True
    assert body["coverage_unknown"] is True

    # preflight on an attach: not blocked (fully_supported), but flagged as unassessed
    resp = await adapter_client_with_nso.post(
        f"/api/v1/devices/{device_id}/route-policy/preflight?refresh=false",
        headers=AUTH,
        json={"community_members": ["color:0:200"], "set_keys": ["extcommunity_color"]},
    )
    body = resp.json()
    assert body["known"] is True
    assert body["fully_supported"] is True
    assert body["unsupported"] == []
    assert body["coverage_unknown"] is True


@pytest.mark.asyncio
async def test_preflight_unknown_device_is_fail_open(adapter_client_with_nso, monkeypatch):  # noqa: F811
    """A device that's never been probed (refresh=false) is reported unknown, not blocked."""
    calls: list[str] = []
    _fake_probe(monkeypatch, calls)
    device_id = await seed_device(nso_device_name="rg03")

    resp = await adapter_client_with_nso.post(
        f"/api/v1/devices/{device_id}/route-policy/preflight?refresh=false",
        headers=AUTH,
        json={"community_members": ["color:0:200"]},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["known"] is False
    assert body["fully_supported"] is True
    assert calls == []


@pytest.mark.asyncio
async def test_apply_preflight_flags_unsupported_scope(adapter_client_with_nso, monkeypatch):  # noqa: F811
    """The generic apply-preflight flags a scope the matrix recorded unsupported (reactively,
    from a prior apply failure) when the plugin asks about it before a device write."""
    from nso_adapter.core.capability import record_capability_rejection
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device

    device_id = await seed_device(nso_device_name="rg03")
    async for db in get_session():  # known (ned, sw) + a reactively-recorded scope gap
        dev = await db.get(Device, device_id)
        dev.ned_id, dev.sw_version = _NED, "17.15.4c"
        await db.commit()
        break
    async for db in get_session():
        await record_capability_rejection(db, _NED, "17.15.4c", "static_route", "static_route", "NED rejected")
        break

    resp = await adapter_client_with_nso.post(
        f"/api/v1/devices/{device_id}/apply/preflight?refresh=false",
        headers=AUTH,
        json={"scopes": ["static_route", "bgp"]},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["known"] is True
    assert body["fully_supported"] is False
    assert [(u["scope"], u["status"]) for u in body["unsupported"]] == [("static_route", "unsupported")]


@pytest.mark.asyncio
async def test_apply_preflight_unknown_device_is_fail_open(adapter_client_with_nso, monkeypatch):  # noqa: F811
    """A never-probed device (no (ned, sw) key) is fail-open — never blocks the apply."""
    device_id = await seed_device(nso_device_name="rg03")
    resp = await adapter_client_with_nso.post(
        f"/api/v1/devices/{device_id}/apply/preflight?refresh=false",
        headers=AUTH,
        json={"scopes": ["static_route"]},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body == {"known": False, "fully_supported": True, "unsupported": [], "coverage_unknown": False}


# ── POST /read-capability/report — ingest the READ half from the vendor-test harness ──


async def _seed_device_with_key(name: str, ned: str = _NED, sw: str = "17.15.4c") -> int:
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device

    device_id = await seed_device(nso_device_name=name)
    async for db in get_session():
        dev = await db.get(Device, device_id)
        dev.ned_id, dev.sw_version = ned, sw
        await db.commit()
        break
    return device_id


@pytest.mark.asyncio
async def test_read_capability_report_records_rows_under_the_device_key(adapter_client_with_nso):  # noqa: F811
    """The harness posts per-scope read states by NSO device name; the adapter resolves the
    (ned, sw) key from the device row and the rows come back via GET /capability."""
    device_id = await _seed_device_with_key("rg03")

    resp = await adapter_client_with_nso.post(
        "/api/v1/devices/read-capability/report",
        headers=AUTH,
        json={
            "nso_device_name": "rg03",
            "elements": [
                {"scope": "bgp", "status": "native", "detail": "read 11 item(s) on rg03"},
                {"scope": "isis", "status": "unknown", "detail": "reads empty on rg03"},
            ],
        },
    )
    assert resp.status_code == 200
    assert resp.json() == {"ned_id": _NED, "sw_version": "17.15.4c", "count": 2}

    resp = await adapter_client_with_nso.get(f"/api/v1/devices/{device_id}/capability", headers=AUTH)
    body = resp.json()
    assert body["known"] is True
    got = {(e["scope"], e["name"]): e for e in body["elements"]}
    assert got[("bgp", "read")]["status"] == "native"
    assert got[("bgp", "read")]["source"] == "read"
    assert got[("isis", "read")]["status"] == "unknown"


@pytest.mark.asyncio
async def test_read_capability_report_unknown_device_is_404(adapter_client_with_nso):  # noqa: F811
    resp = await adapter_client_with_nso.post(
        "/api/v1/devices/read-capability/report",
        headers=AUTH,
        json={"nso_device_name": "no-such-device", "elements": [{"scope": "bgp", "status": "native"}]},
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_read_capability_report_device_without_ned_id_is_409(adapter_client_with_nso):  # noqa: F811
    """A device that has never learned its NED id has no capability key — report an honest
    conflict instead of writing rows under an empty key."""
    await seed_device(nso_device_name="fresh-device")
    resp = await adapter_client_with_nso.post(
        "/api/v1/devices/read-capability/report",
        headers=AUTH,
        json={"nso_device_name": "fresh-device", "elements": [{"scope": "bgp", "status": "native"}]},
    )
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_read_capability_report_rejects_non_read_status(adapter_client_with_nso):  # noqa: F811
    await _seed_device_with_key("rg03-b")
    resp = await adapter_client_with_nso.post(
        "/api/v1/devices/read-capability/report",
        headers=AUTH,
        json={"nso_device_name": "rg03-b", "elements": [{"scope": "bgp", "status": "would_apply"}]},
    )
    assert resp.status_code == 422
