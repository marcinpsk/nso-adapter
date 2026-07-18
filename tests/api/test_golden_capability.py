# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Golden-body tests — the capability router (get / refresh / read-report / 2 preflights).

Branchy shapes:
  * GET /capability — known adds coverage_unknown + rich elements; unknown returns a fixed
    empty verdict (coverage_unknown ABSENT when unknown → exclude_unset).
  * POST /capability/refresh + POST /read-capability/report — {ned_id, sw_version, count}.
  * POST /route-policy/preflight — unsupported items keyed by ``element``.
  * POST /apply/preflight — unsupported items keyed by ``name``.

refresh probes NSO (external boundary) so its delegate is patched; the other paths run
with ``refresh=false`` against a seeded ``(ned_id, sw_version)`` key + device_capability
rows, so no NSO call happens. Rows are read back in insertion order (SQLite rowid).
"""

from __future__ import annotations

import pytest

from tests.conftest import VALID_TOKEN, seed_device

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}
_NED = "cisco-ios-cli-6.114"
_SW = "17.15.4c"


async def _set_key_and_rows(device_id: int, ned_id: str, sw_version: str, rows: list[dict]) -> None:
    """Stamp the device's learned (ned_id, sw_version) key and seed device_capability rows."""
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device, DeviceCapability

    async for db in get_session():
        device = await db.get(Device, device_id)
        device.ned_id = ned_id
        device.sw_version = sw_version
        for r in rows:
            db.add(DeviceCapability(ned_id=ned_id, sw_version=sw_version, **r))
        await db.commit()
        break


@pytest.mark.anyio
async def test_get_capability_known_golden(adapter_client_with_nso):
    device_id = await seed_device(nso_device_name="cap-known", netbox_device_id=601)
    await _set_key_and_rows(
        device_id,
        _NED,
        _SW,
        [{"scope": "rm-set", "name": "set metric-type", "status": "native", "detail": "", "source": "probe"}],
    )

    body = (await adapter_client_with_nso.get(f"/api/v1/devices/{device_id}/capability", headers=AUTH)).json()

    assert body == {
        "known": True,
        "ned_id": _NED,
        "sw_version": _SW,
        "coverage_unknown": False,
        "elements": [
            {"scope": "rm-set", "name": "set metric-type", "status": "native", "detail": "", "source": "probe"}
        ],
    }


@pytest.mark.anyio
async def test_get_capability_unknown_golden(adapter_client_with_nso):
    """No learned key → known:False and coverage_unknown ABSENT (not null)."""
    device_id = await seed_device(nso_device_name="cap-unknown", netbox_device_id=602)
    body = (await adapter_client_with_nso.get(f"/api/v1/devices/{device_id}/capability", headers=AUTH)).json()
    assert body == {"known": False, "ned_id": "", "sw_version": "", "elements": []}


@pytest.mark.anyio
async def test_refresh_capability_golden(adapter_client_with_nso, monkeypatch):
    from nso_adapter.core import capability

    async def fake_refresh(_db, _client, _name, _device=None):
        return {"ned_id": _NED, "sw_version": _SW, "count": 3}

    monkeypatch.setattr(capability, "refresh_device_capability", fake_refresh)
    device_id = await seed_device(nso_device_name="cap-refresh", netbox_device_id=603)
    body = (await adapter_client_with_nso.post(f"/api/v1/devices/{device_id}/capability/refresh", headers=AUTH)).json()
    assert body == {"ned_id": _NED, "sw_version": _SW, "count": 3}


@pytest.mark.anyio
async def test_refresh_capability_no_ned_fallback_golden(adapter_client_with_nso, monkeypatch):
    """A probe that reports no NED yields {} → the endpoint's zero-key fallback."""
    from nso_adapter.core import capability

    async def fake_refresh(_db, _client, _name, _device=None):
        return {}

    monkeypatch.setattr(capability, "refresh_device_capability", fake_refresh)
    device_id = await seed_device(nso_device_name="cap-refresh-empty", netbox_device_id=604)
    body = (await adapter_client_with_nso.post(f"/api/v1/devices/{device_id}/capability/refresh", headers=AUTH)).json()
    assert body == {"ned_id": "", "sw_version": "", "count": 0}


@pytest.mark.anyio
async def test_report_read_capability_golden(adapter_client_with_nso):
    device_id = await seed_device(nso_device_name="cap-report", netbox_device_id=605)
    await _set_key_and_rows(device_id, _NED, _SW, [])

    body = (
        await adapter_client_with_nso.post(
            "/api/v1/devices/read-capability/report",
            json={
                "nso_device_name": "cap-report",
                "nso_instance": "nso-dev",
                "elements": [
                    {"scope": "bgp", "status": "native", "detail": "ok"},
                    {"scope": "ospf", "status": "unsupported", "detail": ""},
                ],
            },
            headers=AUTH,
        )
    ).json()
    assert body == {"ned_id": _NED, "sw_version": _SW, "count": 2}


@pytest.mark.anyio
async def test_preflight_route_policy_known_golden(adapter_client_with_nso):
    device_id = await seed_device(nso_device_name="cap-rp-known", netbox_device_id=606)
    await _set_key_and_rows(
        device_id,
        _NED,
        _SW,
        [
            {
                "scope": "community",
                "name": "color:0:200",
                "status": "unsupported",
                "detail": "no home",
                "source": "probe",
            }
        ],
    )

    body = (
        await adapter_client_with_nso.post(
            f"/api/v1/devices/{device_id}/route-policy/preflight",
            params={"refresh": "false"},
            json={"community_members": ["color:0:128"]},
            headers=AUTH,
        )
    ).json()
    assert body == {
        "known": True,
        "ned_id": _NED,
        "sw_version": _SW,
        "fully_supported": False,
        "unsupported": [{"scope": "community", "element": "color:0:128", "status": "unsupported", "detail": "no home"}],
        "coverage_unknown": False,
    }


@pytest.mark.anyio
async def test_preflight_route_policy_unknown_golden(adapter_client_with_nso):
    device_id = await seed_device(nso_device_name="cap-rp-unknown", netbox_device_id=607)
    body = (
        await adapter_client_with_nso.post(
            f"/api/v1/devices/{device_id}/route-policy/preflight",
            params={"refresh": "false"},
            json={},
            headers=AUTH,
        )
    ).json()
    assert body == {"known": False, "fully_supported": True, "unsupported": [], "coverage_unknown": False}


@pytest.mark.anyio
async def test_preflight_apply_known_golden(adapter_client_with_nso):
    device_id = await seed_device(nso_device_name="cap-apply-known", netbox_device_id=608)
    await _set_key_and_rows(
        device_id,
        _NED,
        _SW,
        [{"scope": "bgp", "name": "read", "status": "unsupported", "detail": "no reader", "source": "read"}],
    )

    body = (
        await adapter_client_with_nso.post(
            f"/api/v1/devices/{device_id}/apply/preflight",
            params={"refresh": "false"},
            json={"scopes": ["bgp"]},
            headers=AUTH,
        )
    ).json()
    assert body == {
        "known": True,
        "ned_id": _NED,
        "sw_version": _SW,
        "coverage_unknown": False,
        "fully_supported": False,
        "unsupported": [{"scope": "bgp", "name": "read", "status": "unsupported", "detail": "no reader"}],
    }


@pytest.mark.anyio
async def test_preflight_apply_unknown_golden(adapter_client_with_nso):
    device_id = await seed_device(nso_device_name="cap-apply-unknown", netbox_device_id=609)
    body = (
        await adapter_client_with_nso.post(
            f"/api/v1/devices/{device_id}/apply/preflight",
            params={"refresh": "false"},
            json={"scopes": ["bgp"]},
            headers=AUTH,
        )
    ).json()
    assert body == {"known": False, "fully_supported": True, "unsupported": [], "coverage_unknown": False}
