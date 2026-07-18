# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Golden-body tests — the actions router.

The five ``_trigger`` POSTs (sync / detect-drift / connect / sync-notify / apply) and
force-removal all return the same ``{"job_id": <int>}`` envelope (202). apply-diff
(GET) returns ``{device_id, outformat, diffs}`` where ``diffs`` is a
``{scope: native_delta}`` map. The dry-run delegate (``collect_apply_diff``) reaches
NSO — a true external boundary — so it is patched to a canned map here; the golden
pins the ENDPOINT envelope, and collect_apply_diff has its own core tests.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from tests.conftest import VALID_TOKEN, seed_device

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


@pytest.mark.anyio
async def test_action_sync_job_trigger_golden(adapter_client):
    device_id = await seed_device(nso_device_name="act-sync", netbox_device_id=501)
    resp = await adapter_client.post(f"/api/v1/devices/{device_id}/actions/sync", headers=AUTH)
    assert resp.status_code == 202
    body = resp.json()
    assert set(body) == {"job_id"}
    assert isinstance(body["job_id"], int)


@pytest.mark.anyio
async def test_sync_notify_job_trigger_golden(adapter_client):
    device_id = await seed_device(nso_device_name="act-notify", netbox_device_id=502)
    resp = await adapter_client.post(f"/api/v1/devices/{device_id}/sync-notify", headers=AUTH)
    assert resp.status_code == 202
    body = resp.json()
    assert set(body) == {"job_id"}
    assert isinstance(body["job_id"], int)


@pytest.mark.anyio
async def test_apply_diff_envelope_golden(adapter_client):
    device_id = await seed_device(nso_device_name="act-diff", netbox_device_id=503)
    canned = {"bgp": "+ router bgp 65000", "isis": ""}
    with patch("nso_adapter.core.apply.collect_apply_diff", new=AsyncMock(return_value=canned)):
        resp = await adapter_client.get(
            f"/api/v1/devices/{device_id}/actions/apply-diff", params={"outformat": "cli"}, headers=AUTH
        )
    assert resp.status_code == 200
    assert resp.json() == {"device_id": device_id, "outformat": "cli", "diffs": canned}
