# SPDX-License-Identifier: Apache-2.0
"""Tests for bindings/netbox/intent.py — fetch_all_intent."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from nso_adapter.bindings.netbox.intent import fetch_all_intent


def _make_nb_client(base="http://netbox"):
    client = MagicMock()
    client._base = base
    return client


def _mock_httpx_response(json_data, status: int = 200):
    resp = MagicMock()
    resp.status_code = status
    resp.json.return_value = json_data
    if status >= 400:
        resp.raise_for_status.side_effect = Exception(f"HTTP {status}")
    else:
        resp.raise_for_status.return_value = None
    return resp


def _mock_http_ctx(responses):
    """Build a mock context manager whose .get() returns responses in order."""
    if not isinstance(responses, (list, tuple)):
        responses = [responses]
    mock_http = AsyncMock()
    mock_http.get.side_effect = responses
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=mock_http)
    ctx.__aexit__ = AsyncMock(return_value=None)
    return ctx


def _intent_item(device_id=10, iface_name="ge-0/0/0", attribute="description", status="accepted",
                 intent_value="test-desc", accepted_at="2025-01-01T00:00:00Z"):
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
    client._client.return_value = _mock_http_ctx(_mock_httpx_response(data))

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
    client._client.return_value = _mock_http_ctx(_mock_httpx_response(data))

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
    client._client.return_value = _mock_http_ctx([
        _mock_httpx_response(page1),
        _mock_httpx_response(page2),
    ])

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
    client._client.return_value = _mock_http_ctx(_mock_httpx_response(data))

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
    client._client.return_value = _mock_http_ctx(_mock_httpx_response(data))

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
    client._client.return_value = _mock_http_ctx(_mock_httpx_response(data))

    records = await fetch_all_intent(client)
    assert len(records) == 0


@pytest.mark.asyncio
async def test_fetch_all_intent_invalid_accepted_at():
    """Items with unparseable accepted_at get accepted_at=None."""
    item = _intent_item(accepted_at="not-a-date")
    data = {"results": [item], "next": None}
    client = _make_nb_client()
    client._client.return_value = _mock_http_ctx(_mock_httpx_response(data))

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
    client._client.return_value = _mock_http_ctx(_mock_httpx_response(data))

    records = await fetch_all_intent(client)
    assert records[0].intent_value == "fallback-desc"
