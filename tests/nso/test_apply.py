# SPDX-License-Identifier: Apache-2.0
"""Tests for nso/apply.py — apply_interface_attribute."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from nso_adapter.nso.apply import NsoApplyError, apply_interface_attribute, apply_interface_ips


def _make_nso_client(base="http://nso"):
    client = MagicMock()
    client._base = base
    client._action_timeout = 120.0
    return client


def _mock_httpx_response(status: int = 204, json_data=None):
    resp = MagicMock()
    resp.status_code = status
    resp.json.return_value = json_data or {}
    resp.text = "error body"
    return resp


def _mock_http_ctx(response):
    mock_http = AsyncMock()
    mock_http.patch.return_value = response
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=mock_http)
    ctx.__aexit__ = AsyncMock(return_value=None)
    return ctx


@pytest.mark.asyncio
async def test_apply_description_success():
    """Applies description attribute without raising."""
    client = _make_nso_client()
    client._client.return_value = _mock_http_ctx(_mock_httpx_response(204))

    await apply_interface_attribute(client, "core-rtr-01", "GigabitEthernet0/0", "description", "uplink")

    mock_http = client._client.return_value.__aenter__.return_value
    mock_http.patch.assert_called_once()
    (url,) = mock_http.patch.call_args[0]
    assert "interface-reconciler" in url
    import json

    payload = json.loads(mock_http.patch.call_args[1]["content"])
    entries = payload["interface-reconciler:interface-config"]
    assert entries[0]["description"] == "uplink"
    assert entries[0]["device"] == "core-rtr-01"
    assert entries[0]["interface-name"] == "GigabitEthernet0/0"


@pytest.mark.asyncio
async def test_apply_enabled_true():
    """enabled='True' string maps to boolean True."""
    client = _make_nso_client()
    client._client.return_value = _mock_http_ctx(_mock_httpx_response(200))

    await apply_interface_attribute(client, "rtr", "ge-0/0/0", "enabled", "True")

    mock_http = client._client.return_value.__aenter__.return_value
    import json

    payload = json.loads(mock_http.patch.call_args[1]["content"])
    assert payload["interface-reconciler:interface-config"][0]["enabled"] is True


@pytest.mark.asyncio
async def test_apply_enabled_lowercase_true():
    """Regression: lowercase 'true' (from a JSON-boolean intent push) maps to True.

    Previously the check was `value == "True"`, so a stored 'true' became False and a
    deliberately-enabled interface was silently written as disabled.
    """
    client = _make_nso_client()
    client._client.return_value = _mock_http_ctx(_mock_httpx_response(200))

    await apply_interface_attribute(client, "rtr", "ge-0/0/0", "enabled", "true")

    mock_http = client._client.return_value.__aenter__.return_value
    import json

    payload = json.loads(mock_http.patch.call_args[1]["content"])
    assert payload["interface-reconciler:interface-config"][0]["enabled"] is True


@pytest.mark.asyncio
async def test_apply_enabled_false():
    """enabled != 'True' string maps to boolean False."""
    client = _make_nso_client()
    client._client.return_value = _mock_http_ctx(_mock_httpx_response(201))

    await apply_interface_attribute(client, "rtr", "ge-0/0/0", "enabled", "False")

    mock_http = client._client.return_value.__aenter__.return_value
    import json

    payload = json.loads(mock_http.patch.call_args[1]["content"])
    assert payload["interface-reconciler:interface-config"][0]["enabled"] is False


@pytest.mark.asyncio
async def test_apply_description_none_uses_empty_string():
    """None description is converted to empty string in the payload."""
    client = _make_nso_client()
    client._client.return_value = _mock_http_ctx(_mock_httpx_response(204))

    await apply_interface_attribute(client, "rtr", "ge-0/0/0", "description", None)

    mock_http = client._client.return_value.__aenter__.return_value
    import json

    payload = json.loads(mock_http.patch.call_args[1]["content"])
    assert payload["interface-reconciler:interface-config"][0]["description"] == ""


@pytest.mark.asyncio
async def test_apply_unsupported_attribute_raises():
    """Unsupported attribute raises NsoApplyError immediately (no HTTP call)."""
    client = _make_nso_client()

    with pytest.raises(NsoApplyError) as exc_info:
        await apply_interface_attribute(client, "rtr", "ge-0/0/0", "mtu", "1500")

    assert exc_info.value.code == "unsupported_attribute"
    client._client.assert_not_called()


@pytest.mark.asyncio
async def test_apply_nso_error_status_raises():
    """Non-2xx NSO response raises NsoApplyError."""
    client = _make_nso_client()
    error_body = {"error": {"code": "locked", "message": "device locked"}}
    client._client.return_value = _mock_http_ctx(_mock_httpx_response(409, json_data=error_body))

    with pytest.raises(NsoApplyError) as exc_info:
        await apply_interface_attribute(client, "rtr", "ge-0/0/0", "description", "x")

    assert exc_info.value.code == "nso_patch_failed"
    assert "409" in exc_info.value.message


@pytest.mark.asyncio
async def test_apply_nso_error_non_json_body():
    """Non-JSON NSO error body is captured as raw text."""
    client = _make_nso_client()
    resp = _mock_httpx_response(500)
    resp.json.side_effect = Exception("not json")
    client._client.return_value = _mock_http_ctx(resp)

    with pytest.raises(NsoApplyError) as exc_info:
        await apply_interface_attribute(client, "rtr", "ge-0/0/0", "description", "x")

    assert "raw" in exc_info.value.detail.get("nso_error", {})


class _SapRow:
    def __init__(self, service_name, service_type, sap_id, port="", outer_tag=None, inner_tag=None):
        self.service_name = service_name
        self.service_type = service_type
        self.sap_id = sap_id
        self.port = port
        self.outer_tag = outer_tag
        self.inner_tag = inner_tag


@pytest.mark.asyncio
async def test_apply_l2_saps_builds_patch_body():
    """apply_l2_saps PATCHes the l2-sap-reconciler service with the SAP list."""
    import json

    from nso_adapter.nso.apply import apply_l2_saps

    client = _make_nso_client()
    client._client.return_value = _mock_http_ctx(_mock_httpx_response(204))

    rows = [
        _SapRow("TL", "epipe", "lag-60:3999", port="lag-60", outer_tag=3999),
        _SapRow("701", "vpls", "1/1/c31/3:701.10", port="1/1/c31/3", outer_tag=701, inner_tag=10),
    ]
    await apply_l2_saps(client=client, device_name="ra1", sap_intent_rows=rows)

    mock_http = client._client.return_value.__aenter__.return_value
    (url,) = mock_http.patch.call_args[0]
    assert "l2-sap-reconciler" in url
    payload = json.loads(mock_http.patch.call_args[1]["content"])
    cfg = payload["l2-sap-reconciler:l2-sap-config"]
    assert cfg[0]["device"] == "ra1"
    saps = {s["sap-id"]: s for s in cfg[0]["sap"]}
    assert saps["lag-60:3999"]["service-type"] == "epipe"
    assert saps["lag-60:3999"]["outer-tag"] == 3999
    assert "inner-tag" not in saps["lag-60:3999"]
    assert saps["1/1/c31/3:701.10"]["inner-tag"] == 10


@pytest.mark.asyncio
async def test_apply_l2_saps_nso_error_raises():
    """Non-2xx NSO response from the L2 SAP PATCH raises NsoApplyError."""
    from nso_adapter.nso.apply import apply_l2_saps

    client = _make_nso_client()
    client._client.return_value = _mock_http_ctx(_mock_httpx_response(409, json_data={"error": {}}))

    with pytest.raises(NsoApplyError) as exc_info:
        await apply_l2_saps(
            client=client, device_name="ra1", sap_intent_rows=[_SapRow("TL", "epipe", "lag-60:1")]
        )
    assert exc_info.value.code == "nso_patch_failed"


@pytest.mark.asyncio
async def test_apply_lag_config_builds_patch_body():
    """apply_lag_config PATCHes the lag-reconciler service with the bundle list."""
    import json

    from nso_adapter.nso.apply import apply_lag_config

    client = _make_nso_client()
    client._client.return_value = _mock_http_ctx(_mock_httpx_response(204))

    bundles = [
        {
            "name": "Port-channel1",
            "lag-id": 1,
            "min-links": 2,
            "member": [
                {"interface-name": "GigabitEthernet0/1", "mode": "active", "port-priority": 200},
            ],
        }
    ]
    await apply_lag_config(client=client, device_name="sw03", bundles=bundles)

    mock_http = client._client.return_value.__aenter__.return_value
    (url,) = mock_http.patch.call_args[0]
    assert "lag-reconciler" in url
    payload = json.loads(mock_http.patch.call_args[1]["content"])
    cfg = payload["lag-reconciler:lag-config"]
    assert cfg[0]["device"] == "sw03"
    assert cfg[0]["bundle"][0]["name"] == "Port-channel1"
    assert cfg[0]["bundle"][0]["member"][0]["port-priority"] == 200


@pytest.mark.asyncio
async def test_apply_lag_config_nso_error_raises():
    """Non-2xx NSO response from the LAG PATCH raises NsoApplyError."""
    from nso_adapter.nso.apply import apply_lag_config

    client = _make_nso_client()
    client._client.return_value = _mock_http_ctx(_mock_httpx_response(409, json_data={"error": {}}))

    with pytest.raises(NsoApplyError) as exc_info:
        await apply_lag_config(
            client=client, device_name="sw03", bundles=[{"name": "Port-channel1", "lag-id": 1}]
        )
    assert exc_info.value.code == "nso_patch_failed"


def test_nso_apply_error_str():
    """NsoApplyError carries code, message, and optional detail."""
    err = NsoApplyError("test_code", "test message", {"k": "v"})
    assert str(err) == "test message"
    assert err.code == "test_code"
    assert err.detail == {"k": "v"}


def test_nso_apply_error_default_detail():
    """NsoApplyError default detail is empty dict."""
    err = NsoApplyError("code", "msg")
    assert err.detail == {}


# ── apply_interface_ips ──────────────────────────────────────────────────────


def _make_ip_row(address: str, family: str = "ipv4", secondary: bool = False, vrf: str = "") -> MagicMock:
    row = MagicMock()
    row.address = address
    row.family = family
    row.secondary = secondary
    row.vrf = vrf
    return row


@pytest.mark.asyncio
async def test_apply_interface_ips_ipv4_primary():
    """Single IPv4 primary address produces correct PATCH body."""
    client = _make_nso_client()
    resp = _mock_httpx_response(204)
    client._client.return_value = _mock_http_ctx(resp)

    row = _make_ip_row("10.0.0.1/24", family="ipv4", secondary=False, vrf="")
    await apply_interface_ips(client, "rtr-a", "GigabitEthernet0/1", [row])

    import json

    call_kwargs = client._client.return_value.__aenter__.return_value.patch.call_args
    body = json.loads(call_kwargs.kwargs["content"])
    entry = body["interface-reconciler:interface-config"][0]
    assert entry["device"] == "rtr-a"
    assert entry["interface-name"] == "GigabitEthernet0/1"
    assert entry["ipv4-address"] == [{"address": "10.0.0.1", "prefix-length": 24, "secondary": False}]
    assert "vrf" not in entry


@pytest.mark.asyncio
async def test_apply_interface_ips_ipv6():
    """IPv6 address produces correct PATCH body."""
    client = _make_nso_client()
    resp = _mock_httpx_response(204)
    client._client.return_value = _mock_http_ctx(resp)

    row = _make_ip_row("2001:db8::1/64", family="ipv6")
    await apply_interface_ips(client, "rtr-b", "GigabitEthernet0/2", [row])

    import json

    call_kwargs = client._client.return_value.__aenter__.return_value.patch.call_args
    body = json.loads(call_kwargs.kwargs["content"])
    entry = body["interface-reconciler:interface-config"][0]
    assert entry["ipv6-address"] == [{"address": "2001:db8::1", "prefix-length": 64}]
    assert "ipv4-address" not in entry


@pytest.mark.asyncio
async def test_apply_interface_ips_sets_vrf():
    """Non-empty vrf field is included in the PATCH body."""
    client = _make_nso_client()
    resp = _mock_httpx_response(204)
    client._client.return_value = _mock_http_ctx(resp)

    row = _make_ip_row("10.1.1.1/30", family="ipv4", vrf="MGMT")
    await apply_interface_ips(client, "rtr-c", "GigabitEthernet0/3", [row])

    import json

    call_kwargs = client._client.return_value.__aenter__.return_value.patch.call_args
    body = json.loads(call_kwargs.kwargs["content"])
    entry = body["interface-reconciler:interface-config"][0]
    assert entry["vrf"] == "MGMT"


@pytest.mark.asyncio
async def test_apply_interface_ips_nokia_routed_context():
    """Nokia routed-interface metadata (M27) is included in the PATCH so the reconciler
    targets the router/service interface, not the port."""
    client = _make_nso_client()
    client._client.return_value = _mock_http_ctx(_mock_httpx_response(204))

    row = _make_ip_row("7.7.7.7/32", family="ipv4", vrf="CRPD-VPN")
    await apply_interface_ips(
        client, "ra1", "CRPD-VPN:LO7", [row],
        kind="vprn", service="CRPD-VPN", parent_binding="lag-99", encap_tag="10",
    )

    import json

    call_kwargs = client._client.return_value.__aenter__.return_value.patch.call_args
    entry = json.loads(call_kwargs.kwargs["content"])["interface-reconciler:interface-config"][0]
    assert entry["kind"] == "vprn"
    assert entry["service"] == "CRPD-VPN"
    assert entry["parent-binding"] == "lag-99"
    assert entry["encap-tag"] == "10"
    assert entry["ipv4-address"] == [{"address": "7.7.7.7", "prefix-length": 32, "secondary": False}]


@pytest.mark.asyncio
async def test_apply_interface_ips_no_kind_omits_routed_fields():
    """IOS/Junos (no kind) PATCH carries no Nokia routed-interface fields."""
    client = _make_nso_client()
    client._client.return_value = _mock_http_ctx(_mock_httpx_response(204))

    row = _make_ip_row("10.0.0.1/24")
    await apply_interface_ips(client, "rtr-a", "GigabitEthernet0/1", [row])

    import json

    entry = json.loads(
        client._client.return_value.__aenter__.return_value.patch.call_args.kwargs["content"]
    )["interface-reconciler:interface-config"][0]
    assert "kind" not in entry and "parent-binding" not in entry


@pytest.mark.asyncio
async def test_apply_interface_ips_nso_error_raises():
    """Non-2xx response from NSO raises NsoApplyError."""
    client = _make_nso_client()
    resp = _mock_httpx_response(500, json_data={"error": {"code": "internal"}})
    client._client.return_value = _mock_http_ctx(resp)

    row = _make_ip_row("10.2.0.1/24")
    with pytest.raises(NsoApplyError) as exc_info:
        await apply_interface_ips(client, "rtr-d", "GigabitEthernet0/4", [row])

    assert exc_info.value.code == "nso_patch_failed"
    assert "500" in exc_info.value.message
