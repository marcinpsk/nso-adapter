# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Direct unit tests for health.py endpoint functions."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from nso_adapter.api.health import healthz


async def _make_request():
    """Return a minimal Request mock (unused by healthz body)."""
    return MagicMock()


# ── healthz ───────────────────────────────────────────────────────────────────


async def test_healthz_no_nso_instances(adapter_client):
    """healthz() returns ok with empty nso_instances list."""
    request = await _make_request()
    result = await healthz(request=request)
    assert result["status"] == "ok"
    assert result["nso_instances"] == []


async def test_healthz_with_nso_instance_reachable(adapter_client_with_nso):
    """healthz() marks instance as reachable when list_devices succeeds."""
    request = await _make_request()
    mock_client = MagicMock()
    mock_client.list_devices = AsyncMock(return_value=[])

    with patch("nso_adapter.api.health.get_nso_client", return_value=mock_client):
        result = await healthz(request=request)

    assert result["status"] == "ok"
    assert len(result["nso_instances"]) == 1
    assert result["nso_instances"][0]["name"] == "nso-dev"
    assert result["nso_instances"][0]["reachable"] is True


async def test_healthz_with_nso_instance_unreachable(adapter_client_with_nso):
    """healthz() marks instance as unreachable when list_devices raises."""
    request = await _make_request()
    mock_client = MagicMock()
    mock_client.list_devices = AsyncMock(side_effect=ConnectionError("NSO down"))

    with patch("nso_adapter.api.health.get_nso_client", return_value=mock_client):
        result = await healthz(request=request)

    assert result["status"] == "ok"
    assert result["nso_instances"][0]["reachable"] is False
