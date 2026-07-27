# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Direct unit tests for health.py endpoint functions."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from starlette.requests import Request

from nso_adapter.api.health import healthz
from nso_adapter.nso.client import NsoClient


def _request() -> Request:
    """A real (empty) ASGI Request. healthz never reads it, but we stay type-faithful
    rather than fabricating one."""
    return Request({"type": "http", "method": "GET", "path": "/healthz", "headers": [], "query_string": b""})


def _nso_client(*, devices=None, error: Exception | None = None) -> MagicMock:
    """Fake the NSO RESTCONF client (a real external HTTP boundary), bound to the real
    NsoClient interface via spec= so a renamed/removed method can't be fabricated."""
    client = MagicMock(spec=NsoClient)
    client.list_devices = AsyncMock(side_effect=error) if error else AsyncMock(return_value=devices or [])
    return client


# ── healthz ───────────────────────────────────────────────────────────────────


async def test_healthz_no_nso_instances(adapter_client):
    """healthz() returns ok with an empty nso_instances list (real empty-config path)."""
    result = await healthz(request=_request())
    assert result["status"] == "ok"
    assert result["nso_instances"] == []


async def test_healthz_with_nso_instance_reachable(adapter_client_with_nso):
    """healthz() marks the instance reachable when list_devices succeeds."""
    with patch("nso_adapter.api.health.get_nso_client", return_value=_nso_client(devices=[])):
        result = await healthz(request=_request())

    assert result["status"] == "ok"
    assert len(result["nso_instances"]) == 1
    assert result["nso_instances"][0]["name"] == "nso-dev"
    assert result["nso_instances"][0]["reachable"] is True


async def test_healthz_with_nso_instance_unreachable(adapter_client_with_nso):
    """healthz() marks the instance unreachable when list_devices raises."""
    with patch(
        "nso_adapter.api.health.get_nso_client",
        return_value=_nso_client(error=ConnectionError("NSO down")),
    ):
        result = await healthz(request=_request())

    assert result["status"] == "ok"
    assert result["nso_instances"][0]["reachable"] is False
