# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for NetboxClient — get_interface, patch_interface, create_interface."""

from __future__ import annotations

import httpx
import pytest
import respx

from nso_adapter.bindings.netbox.client import NetboxClient

BASE = "http://netbox.local"
TOKEN = "nb-test-token"


@pytest.fixture
def client() -> NetboxClient:
    return NetboxClient(url=BASE, token=TOKEN, timeout=5.0)


# ── get_interface ──────────────────────────────────────────────────────────────


@respx.mock
async def test_get_interface_returns_first_result(client):
    """get_interface() returns the first object in results when present."""
    payload = {"results": [{"id": 42, "name": "GigabitEthernet0/0"}]}
    respx.get(f"{BASE}/api/dcim/interfaces/").mock(return_value=httpx.Response(200, json=payload))

    result = await client.get_interface(netbox_device_id=1, interface_name="GigabitEthernet0/0")
    assert result == {"id": 42, "name": "GigabitEthernet0/0"}


@respx.mock
async def test_get_interface_returns_none_when_empty(client):
    """get_interface() returns None when results list is empty."""
    payload = {"results": []}
    respx.get(f"{BASE}/api/dcim/interfaces/").mock(return_value=httpx.Response(200, json=payload))

    result = await client.get_interface(netbox_device_id=1, interface_name="NoSuchIface")
    assert result is None


@respx.mock
async def test_get_interface_raises_on_http_error(client):
    """get_interface() propagates httpx.HTTPStatusError on non-2xx."""
    respx.get(f"{BASE}/api/dcim/interfaces/").mock(return_value=httpx.Response(403))

    with pytest.raises(httpx.HTTPStatusError):
        await client.get_interface(netbox_device_id=1, interface_name="eth0")


# ── patch_interface ────────────────────────────────────────────────────────────


@respx.mock
async def test_patch_interface_returns_response_json(client):
    """patch_interface() PATCHes the interface and returns the JSON body."""
    updated = {"id": 10, "name": "eth0", "description": "new-desc"}
    respx.patch(f"{BASE}/api/dcim/interfaces/10/").mock(return_value=httpx.Response(200, json=updated))

    result = await client.patch_interface(interface_id=10, payload={"description": "new-desc"})
    assert result == updated


@respx.mock
async def test_patch_interface_raises_on_http_error(client):
    """patch_interface() propagates httpx.HTTPStatusError on 404."""
    respx.patch(f"{BASE}/api/dcim/interfaces/99/").mock(return_value=httpx.Response(404))

    with pytest.raises(httpx.HTTPStatusError):
        await client.patch_interface(interface_id=99, payload={"description": "x"})


# ── create_interface ───────────────────────────────────────────────────────────


@respx.mock
async def test_create_interface_returns_created_object(client):
    """create_interface() POSTs and returns the created object."""
    created = {"id": 55, "name": "eth0", "device": 1}
    respx.post(f"{BASE}/api/dcim/interfaces/").mock(return_value=httpx.Response(201, json=created))

    result = await client.create_interface(payload={"name": "eth0", "device": 1})
    assert result == created


@respx.mock
async def test_create_interface_raises_on_http_error(client):
    """create_interface() propagates httpx.HTTPStatusError on 400."""
    respx.post(f"{BASE}/api/dcim/interfaces/").mock(return_value=httpx.Response(400, json={"detail": "bad payload"}))

    with pytest.raises(httpx.HTTPStatusError):
        await client.create_interface(payload={"name": ""})


# ── list_interfaces (paginated bulk fetch) ───────────────────────────────────


@respx.mock
async def test_list_interfaces_follows_pagination(client):
    """list_interfaces() follows `next` links and concatenates all results."""
    page2 = f"{BASE}/api/dcim/interfaces/?device_id=1&limit=500&offset=500"
    respx.get(f"{BASE}/api/dcim/interfaces/").mock(
        side_effect=[
            httpx.Response(200, json={"results": [{"id": 1, "name": "a"}], "next": page2}),
            httpx.Response(200, json={"results": [{"id": 2, "name": "b"}], "next": None}),
        ]
    )
    result = await client.list_interfaces(1)
    assert [r["name"] for r in result] == ["a", "b"]


# ── bulk_create_interfaces ───────────────────────────────────────────────────


@respx.mock
async def test_bulk_create_returns_list(client):
    created = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    respx.post(f"{BASE}/api/dcim/interfaces/").mock(return_value=httpx.Response(201, json=created))
    result = await client.bulk_create_interfaces([{"name": "a"}, {"name": "b"}])
    assert result == created


@respx.mock
async def test_bulk_create_empty_is_noop(client):
    route = respx.post(f"{BASE}/api/dcim/interfaces/")
    result = await client.bulk_create_interfaces([])
    assert result == []
    assert not route.called


@respx.mock
async def test_bulk_create_chunks_large_payload(client):
    """>_BULK_CREATE_CHUNK rows are split into multiple serial POSTs and concatenated."""
    from nso_adapter.bindings.netbox import client as client_mod

    n = client_mod._BULK_CREATE_CHUNK * 2 + 5  # 205 → 3 batches (100, 100, 5)
    payloads = [{"name": f"if{i}"} for i in range(n)]

    def _echo(request: httpx.Request) -> httpx.Response:
        import json

        body = json.loads(request.content)
        return httpx.Response(201, json=[{"id": i, "name": p["name"]} for i, p in enumerate(body)])

    route = respx.post(f"{BASE}/api/dcim/interfaces/").mock(side_effect=_echo)
    result = await client.bulk_create_interfaces(payloads)

    assert route.call_count == 3  # 100 + 100 + 5
    assert len(result) == n
    assert [r["name"] for r in result] == [p["name"] for p in payloads]


@respx.mock
async def test_bulk_patch_one_failed_batch_does_not_abandon_rest(client):
    """A batch that times out is skipped; remaining batches still apply (no abort)."""
    from nso_adapter.bindings.netbox import client as client_mod

    n = client_mod._BULK_PATCH_CHUNK * 3  # 3 batches
    payloads = [{"id": i, "description": "x"} for i in range(n)]
    calls = {"i": 0}

    def _handler(request: httpx.Request) -> httpx.Response:
        import json

        calls["i"] += 1
        if calls["i"] == 2:  # second batch times out
            raise httpx.ReadTimeout("boom", request=request)
        body = json.loads(request.content)
        return httpx.Response(200, json=body)

    respx.patch(f"{BASE}/api/dcim/interfaces/").mock(side_effect=_handler)
    result = await client.bulk_patch_interfaces(payloads)

    # batches 1 and 3 succeeded (≈2/3 of rows); the timed-out batch was skipped,
    # NOT allowed to abandon the rest.
    assert len(result) == 2 * client_mod._BULK_PATCH_CHUNK


@respx.mock
async def test_bulk_create_400_drops_bad_row_and_retries(client):
    """On a 400 with positional errors, the bad row is dropped and the rest retried."""
    respx.post(f"{BASE}/api/dcim/interfaces/").mock(
        side_effect=[
            # first attempt: row 1 is bad ({} == ok, non-empty == error)
            httpx.Response(400, json=[{}, {"__all__": ["dup"]}]),
            # retry with only the good row
            httpx.Response(201, json=[{"id": 1, "name": "a"}]),
        ]
    )
    result = await client.bulk_create_interfaces([{"name": "a"}, {"name": "dup"}])
    assert result == [{"id": 1, "name": "a"}]


@respx.mock
async def test_bulk_create_400_all_bad_returns_empty(client):
    respx.post(f"{BASE}/api/dcim/interfaces/").mock(return_value=httpx.Response(400, json=[{"__all__": ["dup"]}]))
    result = await client.bulk_create_interfaces([{"name": "dup"}])
    assert result == []


# ── bulk_patch_interfaces ────────────────────────────────────────────────────


@respx.mock
async def test_bulk_patch_returns_list(client):
    updated = [{"id": 1, "description": "x"}]
    respx.patch(f"{BASE}/api/dcim/interfaces/").mock(return_value=httpx.Response(200, json=updated))
    result = await client.bulk_patch_interfaces([{"id": 1, "description": "x"}])
    assert result == updated


@respx.mock
async def test_bulk_patch_empty_is_noop(client):
    route = respx.patch(f"{BASE}/api/dcim/interfaces/")
    result = await client.bulk_patch_interfaces([])
    assert result == []
    assert not route.called


@respx.mock
async def test_bulk_patch_400_non_positional_body_bisects_to_isolate_bad_row(client):
    """A 400 with a non-positional body (dict) is bisected to isolate the poison row.

    Regression for device-27 'stuck 23': one interface with a pre-existing invalid
    MTU made NetBox reject the whole batch with a dict error (not a positional list),
    so the old code dropped every row. Bisection must write all the innocent rows and
    drop only the genuinely-bad one.
    """
    import json

    bad_id = 99

    def _handler(request: httpx.Request) -> httpx.Response:
        rows = json.loads(request.content)
        if any(r["id"] == bad_id for r in rows):
            # NetBox returns a non-positional (dict) error for this validation failure.
            return httpx.Response(400, json={"mtu": ["Ensure this value is less than or equal to 65536."]})
        return httpx.Response(200, json=rows)

    respx.patch(f"{BASE}/api/dcim/interfaces/").mock(side_effect=_handler)
    payloads = [
        {"id": 1, "description": "a"},
        {"id": 2, "description": "b"},
        {"id": bad_id, "description": "x"},
        {"id": 4, "description": "d"},
    ]
    result = await client.bulk_patch_interfaces(payloads)

    written_ids = sorted(r["id"] for r in result)
    assert written_ids == [1, 2, 4]  # innocent rows written; only the poison row dropped


@respx.mock
async def test_bulk_patch_400_single_non_positional_drops_row(client):
    """A single-row batch that 400s with a non-positional body is dropped, not retried forever."""
    respx.patch(f"{BASE}/api/dcim/interfaces/").mock(return_value=httpx.Response(400, json={"mtu": ["too big"]}))
    result = await client.bulk_patch_interfaces([{"id": 7, "description": "x"}])
    assert result == []


# ── notify_sync_complete (plugin callback) ───────────────────────────────────


@respx.mock
async def test_notify_sync_complete_posts_netbox_device_id(client):
    """notify_sync_complete POSTs the device id to the plugin's sync-complete endpoint."""
    import json

    route = respx.post(f"{BASE}/api/plugins/nso/sync-complete/").mock(
        return_value=httpx.Response(202, json={"queued": True})
    )
    await client.notify_sync_complete(27)
    assert route.called
    assert json.loads(route.calls.last.request.content) == {"netbox_device_id": 27}


@respx.mock
async def test_notify_sync_complete_raises_on_http_error(client):
    """A non-2xx from the plugin propagates (caller in importer swallows it)."""
    respx.post(f"{BASE}/api/plugins/nso/sync-complete/").mock(return_value=httpx.Response(404))
    with pytest.raises(httpx.HTTPStatusError):
        await client.notify_sync_complete(27)


@respx.mock
async def test_notify_provision_complete_posts_job_id(client):
    """notify_provision_complete POSTs the provision job id to the plugin's provision-complete endpoint."""
    import json

    route = respx.post(f"{BASE}/api/plugins/nso/provision-complete/").mock(
        return_value=httpx.Response(202, json={"queued": True})
    )
    await client.notify_provision_complete(38013)
    assert route.called
    assert json.loads(route.calls.last.request.content) == {"provision_job_id": 38013}


@respx.mock
async def test_notify_provision_complete_raises_on_http_error(client):
    """A non-2xx propagates (the job runner's _notify_provision_complete swallows it)."""
    respx.post(f"{BASE}/api/plugins/nso/provision-complete/").mock(return_value=httpx.Response(500))
    with pytest.raises(httpx.HTTPStatusError):
        await client.notify_provision_complete(38013)
