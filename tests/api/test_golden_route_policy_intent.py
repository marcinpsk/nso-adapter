# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Golden-body test — PUT /api/v1/devices/{id}/route-policy-intent.

Drop-guard for S3's ``response_model`` (RoutePolicyIntentPutOut): the response
is byte-identical before and after typing. The PUT stamps ``accepted_at`` from a
write-time ``datetime.now()`` (route_policy.py) so the module clock is frozen,
exactly like the S1b snmp/ip and S2 intent write goldens. A fresh-create PUT (no
pre-existing rows) never triggers removal propagation, so no NSO delegate is hit.

EMIT-NULL shape: every key is always present (no exclude_unset). ``entries`` is an
opaque per-family JSON list stored verbatim; ``unsupported_members`` maps an object
name to the codec-unrepresentable community members for the device's NED.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from tests.conftest import VALID_TOKEN, seed_device, session

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

FROZEN_Z = "2026-06-01T10:00:00Z"


class _FrozenDatetime(datetime):
    """datetime whose .now() is fixed, so the write-time accepted_at is deterministic."""

    @classmethod
    def now(cls, tz=None):
        return datetime(2026, 6, 1, 10, 0, 0, tzinfo=tz)


async def _set_ned(device_id: int, ned_id: str) -> None:
    from nso_adapter.store.models import Device

    async with session() as db:
        dev = await db.get(Device, device_id)
        dev.ned_id = ned_id
        await db.commit()
        return


@pytest.mark.anyio
async def test_route_policy_intent_put_golden(adapter_client, monkeypatch):
    import nso_adapter.api.route_policy as rp_mod

    monkeypatch.setattr(rp_mod, "datetime", _FrozenDatetime)

    device_id = await seed_device(nso_device_name="rp-intent-golden", netbox_device_id=7965)
    await _set_ned(device_id, "timos-nc-23.10")  # Nokia → wildcard color is unrepresentable

    # Payload order == insertion order == autoincrement id order on a fresh DB:
    # community_list gets id 1, prefix_list id 2. The response is ORDER BY (family, name),
    # and "community_list" < "prefix_list", so the response order matches the id order.
    cl_entries = [
        {"sequence": 1, "action": "permit", "community": "64500:*"},
        {"sequence": 2, "action": "permit", "community": "color:0:12."},  # unrepresentable on Nokia
        {"sequence": 3, "action": "permit", "community": "color:0:128"},  # exact color → representable
        {"sequence": 4, "action": "permit", "community": "no-export"},
    ]
    pl_entries = [
        {"sequence": 10, "action": "permit", "prefix": "10.0.0.0/8", "ge": 16, "le": 24},
        {"sequence": 20, "action": "deny", "prefix": "0.0.0.0/0"},
    ]
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/route-policy-intent",
        headers=AUTH,
        json={
            "objects": [
                {"family": "community_list", "name": "CL-1", "entries": cl_entries, "accepted": True},
                {"family": "prefix_list", "name": "PL-1", "entries": pl_entries, "accepted": True},
            ]
        },
    )
    assert resp.status_code == 200, resp.text

    assert resp.json() == {
        "device_id": device_id,
        "objects": [
            {
                "id": 1,
                "family": "community_list",
                "name": "CL-1",
                "entries": cl_entries,
                "accepted_at": FROZEN_Z,
                "last_apply_at": None,
                "last_apply_error": None,
            },
            {
                "id": 2,
                "family": "prefix_list",
                "name": "PL-1",
                "entries": pl_entries,
                "accepted_at": FROZEN_Z,
                "last_apply_at": None,
                "last_apply_error": None,
            },
        ],
        "unsupported_members": {"CL-1": ["color:0:12."]},
    }


@pytest.mark.anyio
async def test_route_policy_intent_put_preserves_structured_apply_error(adapter_client, monkeypatch):
    """``last_apply_error`` is a structured JSON dict (``{code, message, detail}``), not a
    string — core/apply.py stamps it on a failed apply and the handler returns it verbatim.
    A row carrying that dict must round-trip through the response model unchanged; typing the
    field as ``str | None`` would make the next PUT for that device fail response validation
    (a 500). Re-PUTting the same entries keeps the object (no shrink → no removal propagation),
    so the stored failure survives into the response."""
    import nso_adapter.api.route_policy as rp_mod

    monkeypatch.setattr(rp_mod, "datetime", _FrozenDatetime)

    device_id = await seed_device(nso_device_name="rp-intent-golden-apperr", netbox_device_id=7967)
    await _set_ned(device_id, "cisco-iosxr-nc-7.3")

    from nso_adapter.store.models import RoutePolicyObjectIntent

    entries = [{"sequence": 10, "action": "permit"}]
    apply_error = {"code": "apply_failed", "message": "device rejected term", "detail": {"stage": "commit"}}
    async with session() as db:
        db.add(
            RoutePolicyObjectIntent(
                device_id=device_id,
                family="route_map",
                name="RM-FAIL",
                entries=entries,
                accepted_at=datetime(2026, 6, 1, 10, 0, 0),
                last_apply_at=datetime(2026, 6, 1, 10, 0, 0),
                last_apply_error=apply_error,
            )
        )
        await db.commit()

    # Re-PUT the SAME entries → upsert-in-place, no content shrink, no removal job. The stored
    # last_apply_error dict is untouched by the upsert and flows into the response.
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/route-policy-intent",
        headers=AUTH,
        json={"objects": [{"family": "route_map", "name": "RM-FAIL", "entries": entries, "accepted": True}]},
    )
    assert resp.status_code == 200, resp.text

    assert resp.json() == {
        "device_id": device_id,
        "objects": [
            {
                "id": 1,
                "family": "route_map",
                "name": "RM-FAIL",
                "entries": entries,
                "accepted_at": FROZEN_Z,
                "last_apply_at": FROZEN_Z,
                "last_apply_error": apply_error,  # structured dict preserved, not stringified
            }
        ],
        "unsupported_members": {},
    }


@pytest.mark.anyio
async def test_route_policy_intent_put_golden_unaccepted(adapter_client, monkeypatch):
    """An unaccepted object leaves accepted_at NULL (the nullable-emit branch)."""
    import nso_adapter.api.route_policy as rp_mod

    monkeypatch.setattr(rp_mod, "datetime", _FrozenDatetime)

    device_id = await seed_device(nso_device_name="rp-intent-golden-unacc", netbox_device_id=7966)
    await _set_ned(device_id, "cisco-iosxr-nc-7.3")  # identity dialect → no unsupported members

    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/route-policy-intent",
        headers=AUTH,
        json={"objects": [{"family": "as_path", "name": "AP-1", "entries": [], "accepted": False}]},
    )
    assert resp.status_code == 200, resp.text

    assert resp.json() == {
        "device_id": device_id,
        "objects": [
            {
                "id": 1,
                "family": "as_path",
                "name": "AP-1",
                "entries": [],
                "accepted_at": None,
                "last_apply_at": None,
                "last_apply_error": None,
            }
        ],
        "unsupported_members": {},
    }
