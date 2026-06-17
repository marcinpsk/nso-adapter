# SPDX-License-Identifier: Apache-2.0
"""Tests for bindings/netbox/intent.py — fetch_all_intent."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from nso_adapter.bindings.netbox.client import NetboxClient
from nso_adapter.bindings.netbox.intent import fetch_all_intent


def _make_nb_client(base="http://netbox"):
    # NetboxClient is a real external HTTP boundary; bind the fake to its interface via
    # spec= so a renamed/removed client method can't be fabricated. fetch_all_intent reads
    # ._base and calls ._client() — both real members.
    client = MagicMock(spec=NetboxClient)
    client._base = base
    return client


def _httpx_response(json_data, status: int = 200) -> httpx.Response:
    """A REAL httpx.Response (real status_code/.json()/.raise_for_status), not a mock —
    so the parsing fetch_all_intent does runs against genuine response behaviour."""
    return httpx.Response(status, json=json_data, request=httpx.Request("GET", "http://netbox/intent"))


def _mock_http_ctx(responses):
    """Mock for client._client(): a pooled http object whose .get() returns responses
    in order. fetch_all_intent uses the client DIRECTLY (not as a context manager) —
    entering the reused singleton raises "Cannot open a client instance more than once"
    — so there is intentionally no __aenter__/__aexit__ here."""
    if not isinstance(responses, (list, tuple)):
        responses = [responses]
    mock_http = AsyncMock()
    mock_http.get.side_effect = responses
    return mock_http


def _intent_item(
    device_id=10,
    iface_name="ge-0/0/0",
    attribute="description",
    status="accepted",
    intent_value="test-desc",
    accepted_at="2025-01-01T00:00:00Z",
):
    return {
        "interface": {"device": {"id": device_id}, "name": iface_name},
        "attribute": attribute,
        "status": status,
        "intent_value": intent_value,
        "accepted_at": accepted_at,
    }


@pytest.mark.asyncio
async def test_fetch_all_intent_basic():
    """Returns records for accepted items."""
    data = {"results": [_intent_item()], "next": None}
    client = _make_nb_client()
    client._client.return_value = _mock_http_ctx(_httpx_response(data))

    records = await fetch_all_intent(client)

    assert len(records) == 1
    r = records[0]
    assert r.netbox_device_id == 10
    assert r.interface_name == "ge-0/0/0"
    assert r.attribute == "description"
    assert r.intent_value == "test-desc"
    assert r.accepted_at is not None


@pytest.mark.asyncio
async def test_fetch_all_intent_skips_non_accepted():
    """Only 'accepted' status items are returned."""
    data = {
        "results": [
            _intent_item(status="accepted"),
            _intent_item(iface_name="ge-0/0/1", status="pending"),
            _intent_item(iface_name="ge-0/0/2", status="rejected"),
        ],
        "next": None,
    }
    client = _make_nb_client()
    client._client.return_value = _mock_http_ctx(_httpx_response(data))

    records = await fetch_all_intent(client)

    assert len(records) == 1


@pytest.mark.asyncio
async def test_fetch_all_intent_pagination():
    """Follows pagination by hitting the next_url."""
    page1 = {
        "results": [_intent_item(iface_name="ge-0/0/0")],
        "next": "http://netbox/api/plugins/nso/interface-state/?page=2",
    }
    page2 = {
        "results": [_intent_item(iface_name="ge-0/0/1")],
        "next": None,
    }
    client = _make_nb_client()
    client._client.return_value = _mock_http_ctx(
        [
            _httpx_response(page1),
            _httpx_response(page2),
        ]
    )

    records = await fetch_all_intent(client)

    assert len(records) == 2


@pytest.mark.asyncio
async def test_fetch_all_intent_skips_non_dict_interface():
    """Items where interface is not a dict are skipped."""
    item = {
        "interface": "bad-value",
        "attribute": "description",
        "status": "accepted",
        "intent_value": "x",
    }
    data = {"results": [item], "next": None}
    client = _make_nb_client()
    client._client.return_value = _mock_http_ctx(_httpx_response(data))

    records = await fetch_all_intent(client)
    assert len(records) == 0


@pytest.mark.asyncio
async def test_fetch_all_intent_skips_missing_device_id():
    """Items with no device id in interface dict are skipped."""
    item = {
        "interface": {"device": {}, "name": "ge-0/0/0"},
        "attribute": "description",
        "status": "accepted",
        "intent_value": "x",
    }
    data = {"results": [item], "next": None}
    client = _make_nb_client()
    client._client.return_value = _mock_http_ctx(_httpx_response(data))

    records = await fetch_all_intent(client)
    assert len(records) == 0


@pytest.mark.asyncio
async def test_fetch_all_intent_skips_missing_attribute():
    """Items with no attribute field are skipped."""
    item = {
        "interface": {"device": {"id": 1}, "name": "ge-0/0/0"},
        "status": "accepted",
        "intent_value": "x",
    }
    data = {"results": [item], "next": None}
    client = _make_nb_client()
    client._client.return_value = _mock_http_ctx(_httpx_response(data))

    records = await fetch_all_intent(client)
    assert len(records) == 0


@pytest.mark.asyncio
async def test_fetch_all_intent_invalid_accepted_at():
    """Items with unparseable accepted_at get accepted_at=None."""
    item = _intent_item(accepted_at="not-a-date")
    data = {"results": [item], "next": None}
    client = _make_nb_client()
    client._client.return_value = _mock_http_ctx(_httpx_response(data))

    records = await fetch_all_intent(client)

    assert records[0].accepted_at is None


@pytest.mark.asyncio
async def test_fetch_all_intent_nso_value_fallback():
    """Falls back to nso_value when intent_value is absent."""
    item = {
        "interface": {"device": {"id": 5}, "name": "ge-0/0/0"},
        "attribute": "description",
        "status": "accepted",
        "nso_value": "fallback-desc",
        "accepted_at": None,
    }
    data = {"results": [item], "next": None}
    client = _make_nb_client()
    client._client.return_value = _mock_http_ctx(_httpx_response(data))

    records = await fetch_all_intent(client)
    assert records[0].intent_value == "fallback-desc"


@pytest.mark.asyncio
async def test_fetch_all_intent_uses_pooled_client_directly():
    """Regression: the pooled NetboxClient is a reused singleton; entering it as a
    context manager raises httpx's RuntimeError('Cannot open a client instance more
    than once.'), which previously aborted the intent reconcile. fetch_all_intent must
    use the client directly."""
    item = {
        "interface": {"device": {"id": 5}, "name": "ge-0/0/0"},
        "attribute": "description",
        "status": "accepted",
        "intent_value": "desc",
        "accepted_at": None,
    }
    data = {"results": [item], "next": None}
    mock_http = AsyncMock()
    mock_http.get.return_value = _httpx_response(data)
    mock_http.__aenter__.side_effect = RuntimeError("Cannot open a client instance more than once.")
    client = _make_nb_client()
    client._client.return_value = mock_http

    records = await fetch_all_intent(client)

    assert len(records) == 1
    mock_http.__aenter__.assert_not_called()
