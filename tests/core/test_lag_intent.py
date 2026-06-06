# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for core/lag_intent.apply_lag_config (envelope behaviour)."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from nso_adapter.api.lag_config import LagBundleApply, LagConfigApplyRequest, LagMemberApply
from nso_adapter.core.lag_intent import apply_lag_config
from nso_adapter.nso.apply import NsoApplyError


def _payload():
    return LagConfigApplyRequest(
        bundles=[
            LagBundleApply(
                name="Port-channel1",
                lag_id=1,
                min_links=2,
                members=[LagMemberApply(interface_name="Gi0/1", mode="active", port_priority=200)],
            )
        ]
    )


@pytest.mark.anyio
async def test_apply_returns_deployed_envelope():
    device = SimpleNamespace(id=1, nso_device_name="sw03")
    nso_write = AsyncMock()
    with patch("nso_adapter.core.lag_intent._nso_apply_lag_config", nso_write):
        result = await apply_lag_config(device, _payload(), AsyncMock())
    assert result == {"status": "deployed", "device": "sw03", "bundle_count": 1}


@pytest.mark.anyio
async def test_apply_skips_without_nso_device_name():
    device = SimpleNamespace(id=7, nso_device_name=None)
    nso_write = AsyncMock()
    with patch("nso_adapter.core.lag_intent._nso_apply_lag_config", nso_write):
        result = await apply_lag_config(device, _payload(), AsyncMock())
    assert result["status"] == "error"
    assert result["error"] == "no_nso_device_name"
    nso_write.assert_not_awaited()


@pytest.mark.anyio
async def test_apply_maps_nso_error_to_envelope():
    device = SimpleNamespace(id=3, nso_device_name="sw03")
    nso_write = AsyncMock(side_effect=NsoApplyError("nso_patch_failed", "boom", {"x": 1}))
    with patch("nso_adapter.core.lag_intent._nso_apply_lag_config", nso_write):
        result = await apply_lag_config(device, _payload(), AsyncMock())
    assert result["status"] == "error"
    assert result["error"] == "nso_patch_failed"
    assert result["message"] == "boom"
    assert result["detail"] == {"x": 1}
