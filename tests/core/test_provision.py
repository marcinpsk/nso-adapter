# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Unit tests for core/onboarding.provision_nso_device — the NSO-side onboard
orchestrator (create node → fetch-host-keys → unlock → sync-from → map)."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from nso_adapter.nso.client import NsoClient


def _mock_client(*, exists=False, create=None, fetch=None, admin=None, sync=True):
    """An AsyncMock NsoClient (a real external HTTP boundary) with the onboarding methods
    stubbed. spec=NsoClient binds it to the real interface, so a stubbed/called method that
    drifts from NsoClient raises AttributeError instead of silently fabricating."""
    c = AsyncMock(spec=NsoClient)
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
    assert s == {
        "create": "ok",
        "fetch_host_keys": "ok",
        "admin_state": "ok",
        "sync_from": "ok",
        "adapter_mapping": "ok",
    }
    assert res["device_id"] is not None
    client.create_device.assert_awaited_once()
    client.fetch_host_keys.assert_awaited_once()
    client.set_admin_state.assert_awaited_once_with("new-rtr", "unlocked")
    client.sync_from.assert_awaited_once()


async def test_provision_derives_netconf_transport_from_ned_id(adapter_client_with_nso):
    """Regression (rd2): a netconf NED must onboard as device-type netconf, not cli.

    No ned_type is passed — it is derived from the ned_id. The old default of
    ``cli`` produced ``device-type cli ned-id juniper-junos-nc-4.19``, an invalid
    transport that left the device unable to sync its netconf config.
    """
    from nso_adapter.core.onboarding import provision_nso_device
    from nso_adapter.store.db import get_session

    client = _mock_client()
    with patch("nso_adapter.core.importer.get_nso_client", return_value=client):
        async for db in get_session():
            res = await provision_nso_device(
                db,
                nso_instance="nso-dev",
                device_name="junos-rtr",
                address="10.0.0.9",
                ned_id="juniper-junos-nc-4.19:juniper-junos-nc-4.19",
                authgroup="network",
            )
            break
    assert res["ok"] is True
    _, kwargs = client.create_device.call_args
    assert kwargs["ned_type"] == "netconf"


async def test_provision_rejects_transport_contradicting_ned_id(adapter_client_with_nso):
    """An explicit ned_type that disagrees with the ned_id raises (no mis-onboard)."""
    import pytest

    from nso_adapter.core.onboarding import provision_nso_device
    from nso_adapter.store.db import get_session

    client = _mock_client()
    with patch("nso_adapter.core.importer.get_nso_client", return_value=client):
        async for db in get_session():
            with pytest.raises(ValueError, match="contradicts"):
                await provision_nso_device(
                    db,
                    nso_instance="nso-dev",
                    device_name="bad-junos",
                    address="10.0.0.9",
                    ned_id="juniper-junos-nc-4.19",
                    authgroup="network",
                    ned_type="cli",
                )
            break
    client.create_device.assert_not_awaited()


async def test_provision_idempotent_existing_device(adapter_client_with_nso):
    from nso_adapter.core.onboarding import provision_nso_device
    from nso_adapter.store.db import get_session

    client = _mock_client(exists=True)
    with patch("nso_adapter.core.importer.get_nso_client", return_value=client):
        async for db in get_session():
            res = await provision_nso_device(
                db,
                nso_instance="nso-dev",
                device_name="dev-exists",
                address="1.1.1.1",
                ned_id="x",
                authgroup="network",
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
                db,
                nso_instance="nso-dev",
                device_name="bad",
                address="1.1.1.1",
                ned_id="x",
                authgroup="network",
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
                db,
                nso_instance="nso-dev",
                device_name="unreach",
                address="1.1.1.1",
                ned_id="x",
                authgroup="network",
            )
            break
    assert res["ok"] is False
    s = _steps(res)
    # admin-state unlock now precedes fetch-host-keys (a southbound-locked device
    # cannot be reached for fetch), so on a fetch failure the device is already
    # unlocked and only sync-from is skipped.
    assert s["create"] == "ok" and s["admin_state"] == "ok" and s["fetch_host_keys"] == "failed"
    client.set_admin_state.assert_awaited_once()
    client.sync_from.assert_not_awaited()


async def test_provision_unlocks_before_fetch_host_keys(adapter_client_with_nso):
    """Regression: unlock MUST happen before fetch-host-keys (locked device blocks SSH)."""
    from nso_adapter.core.onboarding import provision_nso_device
    from nso_adapter.store.db import get_session

    client = _mock_client()
    with patch("nso_adapter.core.importer.get_nso_client", return_value=client):
        async for db in get_session():
            await provision_nso_device(
                db,
                nso_instance="nso-dev",
                device_name="ordered",
                address="1.1.1.1",
                ned_id="x",
                authgroup="network",
            )
            break
    # The client AsyncMock records every onboarding call in order — no separate parent Mock
    # needed: assert the unlock (set_admin_state) precedes fetch_host_keys.
    order = [c[0] for c in client.mock_calls]
    assert order.index("set_admin_state") < order.index("fetch_host_keys")


async def test_provision_sync_failure_is_nonfatal(adapter_client_with_nso):
    from nso_adapter.core.onboarding import provision_nso_device
    from nso_adapter.store.db import get_session

    client = _mock_client(sync=False)
    with patch("nso_adapter.core.importer.get_nso_client", return_value=client):
        async for db in get_session():
            res = await provision_nso_device(
                db,
                nso_instance="nso-dev",
                device_name="nosync",
                address="1.1.1.1",
                ned_id="x",
                authgroup="network",
                netbox_device_id=77,
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
                db,
                nso_instance="ghost",
                device_name="x",
                address="1.1.1.1",
                ned_id="x",
                authgroup="network",
            )
        break


async def test_provision_retries_fetch_host_keys_once(adapter_client_with_nso):
    """First fetch-host-keys reset is retried once (with backoff) and then succeeds."""
    from unittest.mock import AsyncMock

    from nso_adapter.core.onboarding import provision_nso_device
    from nso_adapter.store.db import get_session

    client = _mock_client()
    client.fetch_host_keys.side_effect = [RuntimeError("connection reset"), {"result": "updated"}]
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=client),
        patch("nso_adapter.core.onboarding.asyncio.sleep", new=AsyncMock()),
    ):
        async for db in get_session():
            res = await provision_nso_device(
                db,
                nso_instance="nso-dev",
                device_name="retry-ok",
                address="1.1.1.1",
                ned_id="x",
                authgroup="network",
            )
            break
    assert res["ok"] is True
    assert _steps(res)["fetch_host_keys"] == "ok"
    assert client.fetch_host_keys.await_count == 2


async def test_provision_fetch_host_keys_fails_after_one_retry(adapter_client_with_nso):
    """If both fetch attempts fail, the step fails and provision aborts (exactly 2 tries)."""
    from unittest.mock import AsyncMock

    from nso_adapter.core.onboarding import provision_nso_device
    from nso_adapter.store.db import get_session

    client = _mock_client(fetch=RuntimeError("still down"))
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=client),
        patch("nso_adapter.core.onboarding.asyncio.sleep", new=AsyncMock()),
    ):
        async for db in get_session():
            res = await provision_nso_device(
                db,
                nso_instance="nso-dev",
                device_name="retry-bad",
                address="1.1.1.1",
                ned_id="x",
                authgroup="network",
            )
            break
    assert res["ok"] is False
    assert _steps(res)["fetch_host_keys"] == "failed"
    assert client.fetch_host_keys.await_count == 2


async def test_provision_retries_sync_from_once(adapter_client_with_nso):
    """sync-from returning False on the first attempt is retried once and then succeeds."""
    from unittest.mock import AsyncMock

    from nso_adapter.core.onboarding import provision_nso_device
    from nso_adapter.store.db import get_session

    client = _mock_client()
    client.sync_from.side_effect = [False, True]
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=client),
        patch("nso_adapter.core.onboarding.asyncio.sleep", new=AsyncMock()),
    ):
        async for db in get_session():
            res = await provision_nso_device(
                db,
                nso_instance="nso-dev",
                device_name="sync-retry",
                address="1.1.1.1",
                ned_id="x",
                authgroup="network",
            )
            break
    assert _steps(res)["sync_from"] == "ok"
    assert client.sync_from.await_count == 2
