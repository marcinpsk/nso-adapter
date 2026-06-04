# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Unit tests for core/onboarding.provision_nso_device — the NSO-side onboard
orchestrator (create node → fetch-host-keys → unlock → sync-from → map)."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest


def _mock_client(*, exists=False, create=None, fetch=None, admin=None, sync=True):
    """An AsyncMock NsoClient with the onboarding methods stubbed."""
    c = AsyncMock()
    c.device_exists.return_value = exists
    c.create_device.side_effect = create
    c.fetch_host_keys.side_effect = fetch
    c.set_admin_state.side_effect = admin
    c.sync_from.return_value = sync
    return c


def _steps(result):
    return {s["step"]: s["status"] for s in result["steps"]}


async def test_provision_happy_path(adapter_client_with_nso):
    from nso_adapter.core.onboarding import provision_nso_device
    from nso_adapter.store.db import get_session

    client = _mock_client()
    with patch("nso_adapter.core.importer.get_nso_client", return_value=client):
        async for db in get_session():
            res = await provision_nso_device(
                db,
                nso_instance="nso-dev",
                device_name="new-rtr",
                address="10.0.0.5",
                ned_id="cisco-ios-cli-6.114:cisco-ios-cli-6.114",
                authgroup="network",
                netbox_device_id=99,
            )
            break

    assert res["ok"] is True
    s = _steps(res)
    assert s == {"create": "ok", "fetch_host_keys": "ok", "admin_state": "ok", "sync_from": "ok", "adapter_mapping": "ok"}
    assert res["device_id"] is not None
    client.create_device.assert_awaited_once()
    client.fetch_host_keys.assert_awaited_once()
    client.set_admin_state.assert_awaited_once_with("new-rtr", "unlocked")
    client.sync_from.assert_awaited_once()


async def test_provision_idempotent_existing_device(adapter_client_with_nso):
    from nso_adapter.core.onboarding import provision_nso_device
    from nso_adapter.store.db import get_session

    client = _mock_client(exists=True)
    with patch("nso_adapter.core.importer.get_nso_client", return_value=client):
        async for db in get_session():
            res = await provision_nso_device(
                db, nso_instance="nso-dev", device_name="dev-exists", address="1.1.1.1",
                ned_id="x", authgroup="network",
            )
            break
    assert res["ok"] is True
    assert _steps(res)["create"] == "exists"
    client.create_device.assert_not_awaited()


async def test_provision_aborts_on_create_failure(adapter_client_with_nso):
    from nso_adapter.core.onboarding import provision_nso_device
    from nso_adapter.store.db import get_session

    client = _mock_client(create=RuntimeError("boom"))
    with patch("nso_adapter.core.importer.get_nso_client", return_value=client):
        async for db in get_session():
            res = await provision_nso_device(
                db, nso_instance="nso-dev", device_name="bad", address="1.1.1.1",
                ned_id="x", authgroup="network",
            )
            break
    assert res["ok"] is False
    assert _steps(res)["create"] == "failed"
    client.fetch_host_keys.assert_not_awaited()


async def test_provision_aborts_on_fetch_host_keys_failure(adapter_client_with_nso):
    from nso_adapter.core.onboarding import provision_nso_device
    from nso_adapter.store.db import get_session

    client = _mock_client(fetch=RuntimeError("unreachable"))
    with patch("nso_adapter.core.importer.get_nso_client", return_value=client):
        async for db in get_session():
            res = await provision_nso_device(
                db, nso_instance="nso-dev", device_name="unreach", address="1.1.1.1",
                ned_id="x", authgroup="network",
            )
            break
    assert res["ok"] is False
    s = _steps(res)
    assert s["create"] == "ok" and s["fetch_host_keys"] == "failed"
    client.set_admin_state.assert_not_awaited()


async def test_provision_sync_failure_is_nonfatal(adapter_client_with_nso):
    from nso_adapter.core.onboarding import provision_nso_device
    from nso_adapter.store.db import get_session

    client = _mock_client(sync=False)
    with patch("nso_adapter.core.importer.get_nso_client", return_value=client):
        async for db in get_session():
            res = await provision_nso_device(
                db, nso_instance="nso-dev", device_name="nosync", address="1.1.1.1",
                ned_id="x", authgroup="network", netbox_device_id=77,
            )
            break
    assert res["ok"] is True  # sync-from failure does not block onboarding
    assert _steps(res)["sync_from"] == "failed"
    assert res["device_id"] is not None  # mapping still created


async def test_provision_unknown_instance_raises(adapter_client):
    from nso_adapter.core.onboarding import provision_nso_device
    from nso_adapter.store.db import get_session

    async for db in get_session():
        with pytest.raises(ValueError):
            await provision_nso_device(
                db, nso_instance="ghost", device_name="x", address="1.1.1.1",
                ned_id="x", authgroup="network",
            )
        break
