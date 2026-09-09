# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for the bearer gate and for the one surface that opts out of it.

The first group exercises the real ``api.deps.verify_token`` (no HTTP stack) with a minimal
request stub. The compare is constant-time (hmac.compare_digest); these lock in the
accept/reject behavior so that hardening can never silently regress into accepting a wrong
token. The second group drives the real app over HTTP to pin what the documentation switch
registers.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi.security import HTTPAuthorizationCredentials

from nso_adapter.api.deps import verify_token
from nso_adapter.api.errors import ApiError
from tests.conftest import VALID_TOKEN


def _req(token: str) -> SimpleNamespace:
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(adapter_token=token)))


def _cred(value: str) -> HTTPAuthorizationCredentials:
    return HTTPAuthorizationCredentials(scheme="Bearer", credentials=value)


async def test_verify_token_accepts_matching():
    assert await verify_token(_req("s3cr3t-token"), _cred("s3cr3t-token")) == "s3cr3t-token"


async def test_verify_token_rejects_wrong_token_sharing_a_prefix():
    with pytest.raises(ApiError):
        await verify_token(_req("s3cr3t-token"), _cred("s3cr3t-toke!"))


async def test_verify_token_rejects_missing_credentials():
    with pytest.raises(ApiError):
        await verify_token(_req("s3cr3t-token"), None)


# ── The documentation surface (ENABLE_API_DOCS) ───────────────────────────────

DOCUMENTATION_PATHS = ["/docs", "/docs/oauth2-redirect", "/redoc", "/openapi.json"]


@pytest.fixture
async def docs_enabled_client(store_engine, pg_url, tmp_path, monkeypatch):
    """A client on the app built with ENABLE_API_DOCS=1. ``adapter_client`` is the same app, off."""
    from unittest.mock import AsyncMock, patch

    from httpx import ASGITransport, AsyncClient

    from nso_adapter.config import reset_config
    from nso_adapter.main import create_app
    from tests.conftest import _write_config

    _write_config(tmp_path, monkeypatch, database_url=pg_url)
    monkeypatch.setenv("ENABLE_API_DOCS", "1")
    reset_config()

    app = create_app()
    try:
        # Same stubs as adapter_client: only the background side effects are kept out.
        with (
            patch("nso_adapter.main.init_db"),
            patch("nso_adapter.main._dispose_engine", new=AsyncMock()),
            patch("nso_adapter.main.set_netbox_client"),
            patch("nso_adapter.main.start_scheduler"),
            patch("nso_adapter.main.stop_scheduler"),
            patch("nso_adapter.main.start_workers", new=AsyncMock()),
            patch("nso_adapter.main.stop_workers", new=AsyncMock()),
            patch("nso_adapter.main.persistent_subscriber", new=AsyncMock()),
        ):
            async with app.router.lifespan_context(app):
                async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
                    yield client
    finally:
        # The env settings are a process singleton: drop the enabled one for the next test.
        reset_config()


@pytest.mark.anyio
@pytest.mark.parametrize("path", DOCUMENTATION_PATHS)
@pytest.mark.parametrize("authorization", [None, f"Bearer {VALID_TOKEN}"])
async def test_documentation_is_not_registered_by_default(adapter_client, path, authorization):
    """Off by default means the route does not exist, for a caller with a token as well."""
    headers = {"Authorization": authorization} if authorization else {}
    assert (await adapter_client.get(path, headers=headers)).status_code == 404


@pytest.mark.anyio
async def test_only_healthz_answers_without_a_token_by_default(adapter_client):
    assert (await adapter_client.get("/healthz")).status_code == 200
    assert (await adapter_client.get("/api/v1/devices")).status_code == 401


@pytest.mark.anyio
@pytest.mark.parametrize("path", DOCUMENTATION_PATHS)
async def test_enabled_documentation_serves_a_browser(docs_enabled_client, path):
    """A browser cannot send a bearer header, so the opt-in is the whole authorization."""
    response = await docs_enabled_client.get(path)
    assert response.status_code == 200, response.text


@pytest.mark.anyio
@pytest.mark.parametrize("path", ["/docs", "/redoc"])
async def test_enabled_documentation_pages_fetch_a_schema_they_can_read(docs_enabled_client, path):
    """Round 1 broke exactly here: both pages rendered and then 401ed on the schema fetch."""
    page = await docs_enabled_client.get(path)
    assert "/openapi.json" in page.text
    schema = await docs_enabled_client.get("/openapi.json")
    assert schema.status_code == 200
    assert schema.json()["info"]["title"] == "NSO Adapter"


@pytest.mark.anyio
async def test_enabled_documentation_keeps_the_api_behind_the_bearer(docs_enabled_client):
    assert (await docs_enabled_client.get("/api/v1/devices")).status_code == 401
    assert (
        await docs_enabled_client.get("/api/v1/devices", headers={"Authorization": "Bearer wrong"})
    ).status_code == 401
