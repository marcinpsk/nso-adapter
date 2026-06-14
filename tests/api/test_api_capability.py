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
