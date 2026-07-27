# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Golden-body tests — the scope router (GET + PUT /devices/{id}/scope).

EMIT-NULL fixed-key shape: device_id / attributes / auto_apply / sync_before_apply /
updated_at always present. ``updated_at`` is ``max(attr.updated_at)`` — or, when the
device has NO managed attributes, a READ-TIME ``datetime.now()`` (scope.py). The
empty-scope golden freezes the module clock so that mint is deterministic, exactly
like the S1b snmp/ip write goldens.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from tests.conftest import VALID_TOKEN, seed_device

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

TS = datetime(2026, 6, 1, 10, 0, 0)
TS_Z = "2026-06-01T10:00:00Z"
FROZEN_Z = "2026-06-01T10:00:00Z"


class _FrozenDatetime(datetime):
    """datetime whose .now() is fixed, so the empty-scope read-time mint is deterministic."""

    @classmethod
    def now(cls, tz=None):
        return datetime(2026, 6, 1, 10, 0, 0, tzinfo=tz)


async def _seed_scope_rows(device_id: int, attributes: list[str]) -> None:
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import DeviceSettings, ManagedScope

    async for db in get_session():
        for attr in attributes:
            db.add(ManagedScope(device_id=device_id, attribute=attr, updated_at=TS))
        db.add(DeviceSettings(device_id=device_id, auto_apply=True, sync_before_apply=False))
        await db.commit()
        break


@pytest.mark.anyio
async def test_get_scope_with_settings_golden(adapter_client):
    device_id = await seed_device(nso_device_name="scope-dev", netbox_device_id=301, attributes=[])
    await _seed_scope_rows(device_id, ["description", "mtu"])

    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/scope", headers=AUTH)).json()

    assert body == {
        "device_id": device_id,
        "attributes": ["description", "mtu"],
        "auto_apply": True,
        "sync_before_apply": False,
        "updated_at": TS_Z,
    }


@pytest.mark.anyio
async def test_get_scope_empty_frozen_clock_golden(adapter_client, monkeypatch):
    """No managed attrs, no settings → attributes:[], auto_apply:False, sync_before_apply:True,
    updated_at minted from the (frozen) read-time clock."""
    import nso_adapter.api.scope as scope_mod

    monkeypatch.setattr(scope_mod, "datetime", _FrozenDatetime)

    device_id = await seed_device(nso_device_name="scope-empty", netbox_device_id=302, attributes=[])

    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/scope", headers=AUTH)).json()

    assert body == {
        "device_id": device_id,
        "attributes": [],
        "auto_apply": False,
        "sync_before_apply": True,
        "updated_at": FROZEN_Z,
    }


@pytest.mark.anyio
async def test_put_scope_result_golden(adapter_client):
    """PUT returns the same _scope_out shape. With attrs present, updated_at is
    ``max(attr.updated_at)`` (a server-side timestamp, not the read-time mint), so we
    pin the key set + values and only assert the "<iso>Z" format of updated_at."""
    device_id = await seed_device(nso_device_name="scope-put", netbox_device_id=303, attributes=[])

    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/scope",
        json={"attributes": ["description"], "auto_apply": True, "sync_before_apply": False},
        headers=AUTH,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["device_id"] == device_id
    assert body["attributes"] == ["description"]
    assert body["auto_apply"] is True
    assert body["sync_before_apply"] is False
    assert set(body) == {"device_id", "attributes", "auto_apply", "sync_before_apply", "updated_at"}
    assert body["updated_at"].endswith("Z")


def test_frozen_now_is_fixed():
    assert _FrozenDatetime.now(UTC).replace(tzinfo=None).isoformat() + "Z" == FROZEN_Z
