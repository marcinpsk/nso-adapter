# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""A2: provisioning fills the read-mirror immediately — but ONLY after a successful sync-from.

Reading the network-state export before NSO's CDB is populated returns an empty/404 body that
would commit an empty mirror (the onboarding empty-wipe race), so the initial comprehensive
refresh is gated on ``sync_from`` actually having pulled the running config.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from nso_adapter.nso.client import NsoClient


def _mock_client(*, sync=True):
    c = AsyncMock(spec=NsoClient)
    c.device_exists.return_value = False
    c.sync_from.return_value = sync
    return c


async def _provision(db, client, refresh_spy, *, name):
    from nso_adapter.core.onboarding import provision_nso_device

    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=client),
        patch("nso_adapter.core.importer.refresh_all_surfaces_for_device", refresh_spy),
    ):
        return await provision_nso_device(
            db,
            nso_instance="nso-dev",
            device_name=name,
            address="10.0.0.9",
            ned_id="cisco-ios-cli-6.114:cisco-ios-cli-6.114",
            authgroup="network",
            netbox_device_id=771,
        )


@pytest.mark.anyio
async def test_provision_fills_mirror_after_successful_sync_from(adapter_client_with_nso):
    """sync-from succeeded → the comprehensive mirror refresh runs for the new device (A2)."""
    from nso_adapter.store.db import get_session

    client = _mock_client(sync=True)
    refresh_spy = AsyncMock(return_value=[])
    async for db in get_session():
        res = await _provision(db, client, refresh_spy, name="fresh-ok")
        break

    assert res["ok"] is True
    refresh_spy.assert_awaited_once()
    assert refresh_spy.call_args.kwargs.get("refresh_source") == "onboard"


@pytest.mark.anyio
async def test_provision_skips_mirror_when_sync_from_failed(adapter_client_with_nso):
    """sync-from returned False → skip the initial refresh so a not-yet-populated export
    cannot wipe/commit an empty mirror; provisioning still succeeds and the poll heals later."""
    from nso_adapter.store.db import get_session

    client = _mock_client(sync=False)
    refresh_spy = AsyncMock(return_value=[])
    async for db in get_session():
        res = await _provision(db, client, refresh_spy, name="fresh-nosync")
        break

    assert res["ok"] is True
    refresh_spy.assert_not_awaited()
