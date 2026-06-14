# SPDX-License-Identifier: Apache-2.0
"""Tests for nso/apply.py — apply_interface_attribute."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from nso_adapter.nso import apply as apply_mod
from nso_adapter.nso.apply import (
    NsoApplyError,
    _device_delta_from_dry_run,
    _verify_native_or_raise,
    apply_interface_attribute,
    apply_interface_ips,
    apply_static_routes,
)


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
    # Real PATCH + post-apply native dry-run verify = two calls.
    assert mock_http.patch.call_count == 2
    real_call = mock_http.patch.call_args_list[0]
    (url,) = real_call[0]
    assert "interface-reconciler" in url
    assert "dry-run" not in url
    import json

    payload = json.loads(real_call[1]["content"])
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
        await apply_l2_saps(client=client, device_name="ra1", sap_intent_rows=[_SapRow("TL", "epipe", "lag-60:1")])
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
        await apply_lag_config(client=client, device_name="sw03", bundles=[{"name": "Port-channel1", "lag-id": 1}])
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
        client,
        "ra1",
        "CRPD-VPN:LO7",
        [row],
        kind="vprn",
        service="CRPD-VPN",
        parent_binding="lag-99",
        encap_tag="10",
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

    entry = json.loads(client._client.return_value.__aenter__.return_value.patch.call_args.kwargs["content"])[
        "interface-reconciler:interface-config"
    ][0]
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


@pytest.mark.asyncio
async def test_apply_switchport_config_builds_patch_body():
    """apply_switchport_config PATCHes switchport-reconciler with the interface list (M34)."""
    import json

    from nso_adapter.nso.apply import apply_switchport_config

    client = _make_nso_client()
    client._client.return_value = _mock_http_ctx(_mock_httpx_response(204))
    ifaces = [
        {"interface-name": "GigabitEthernet0/1", "mode": "access", "untagged-vlan": 10},
        {"interface-name": "GigabitEthernet0/2", "mode": "trunk", "untagged-vlan": 99, "tagged-vlan": [20, 30]},
    ]
    await apply_switchport_config(client=client, device_name="sw03", interfaces=ifaces)

    mock_http = client._client.return_value.__aenter__.return_value
    (url,) = mock_http.patch.call_args[0]
    assert "switchport-reconciler" in url
    cfg = json.loads(mock_http.patch.call_args[1]["content"])["switchport-reconciler:switchport-config"]
    assert cfg[0]["device"] == "sw03"
    by = {i["interface-name"]: i for i in cfg[0]["interface"]}
    assert by["GigabitEthernet0/1"]["mode"] == "access"
    assert by["GigabitEthernet0/2"]["tagged-vlan"] == [20, 30]


@pytest.mark.asyncio
async def test_apply_switchport_config_nso_error_raises():
    from nso_adapter.nso.apply import apply_switchport_config

    client = _make_nso_client()
    client._client.return_value = _mock_http_ctx(_mock_httpx_response(409, json_data={"error": {}}))
    with pytest.raises(NsoApplyError) as exc_info:
        await apply_switchport_config(client=client, device_name="sw03", interfaces=[{"interface-name": "Gi0/1"}])
    assert exc_info.value.code == "nso_patch_failed"


# ── Post-apply native dry-run verification (false-success guard) ────────────────


class TestDeviceDeltaFromDryRun:
    def test_empty_native_means_no_delta(self):
        assert _device_delta_from_dry_run({"dry-run-result": {"native": {}}}, "sw03") == ""

    def test_absent_native_means_no_delta(self):
        assert _device_delta_from_dry_run({"dry-run-result": {"native": None}}, "sw03") == ""

    def test_matching_device_returns_delta(self):
        body = {"dry-run-result": {"native": {"device": [{"name": "sw03", "data": "ip route 1.0.0.0 ...\n"}]}}}
        assert _device_delta_from_dry_run(body, "sw03") == "ip route 1.0.0.0 ...\n"

    def test_other_device_only_means_no_delta(self):
        body = {"dry-run-result": {"native": {"device": [{"name": "other", "data": "x"}]}}}
        assert _device_delta_from_dry_run(body, "sw03") == ""

    def test_unexpected_shape_returns_none(self):
        assert _device_delta_from_dry_run({"something-else": 1}, "sw03") is None
        assert _device_delta_from_dry_run("not-a-dict", "sw03") is None


@pytest.mark.asyncio
async def test_verify_raises_on_nonempty_delta():
    """A non-empty native device delta after apply is a false success → raise."""
    client = _make_nso_client()
    body = {
        "dry-run-result": {"native": {"device": [{"name": "sw03", "data": "ip route 1.0.0.0 255.0.0.0 2.2.2.2 1\n"}]}}
    }
    client._client.return_value = _mock_http_ctx(_mock_httpx_response(200, json_data=body))

    with pytest.raises(NsoApplyError) as exc_info:
        await _verify_native_or_raise(client, "http://nso/x", "{}", "sw03", scope="static_route")
    assert exc_info.value.code == "verify_mismatch"
    assert "ip route" in exc_info.value.detail["device_delta"]


@pytest.mark.asyncio
async def test_verify_passes_on_empty_delta():
    client = _make_nso_client()
    client._client.return_value = _mock_http_ctx(
        _mock_httpx_response(200, json_data={"dry-run-result": {"native": {}}})
    )
    await _verify_native_or_raise(client, "http://nso/x", "{}", "sw03", scope="vlan")  # no raise


@pytest.mark.asyncio
async def test_verify_inconclusive_does_not_raise():
    """Unexpected/garbage dry-run body is fail-safe (no raise, apply stands)."""
    client = _make_nso_client()
    client._client.return_value = _mock_http_ctx(_mock_httpx_response(200, json_data={"weird": 1}))
    await _verify_native_or_raise(client, "http://nso/x", "{}", "sw03", scope="vlan")  # no raise


@pytest.mark.asyncio
async def test_verify_disabled_by_toggle(monkeypatch):
    """When VERIFY_AFTER_APPLY is off, no dry-run call is made."""
    monkeypatch.setattr(apply_mod, "VERIFY_AFTER_APPLY", False)
    client = _make_nso_client()
    client._client.return_value = _mock_http_ctx(_mock_httpx_response(200))
    await _verify_native_or_raise(client, "http://nso/x", "{}", "sw03", scope="vlan")
    client._client.assert_not_called()


@pytest.mark.asyncio
async def test_apply_static_routes_verify_mismatch_raises():
    """End-to-end: real PATCH succeeds (204) but the verify dry-run shows a delta → raise."""
    client = _make_nso_client()
    real_resp = _mock_httpx_response(204)
    dry_resp = _mock_httpx_response(
        200, json_data={"dry-run-result": {"native": {"device": [{"name": "sw03", "data": "ip route ...\n"}]}}}
    )
    mock_http = AsyncMock()
    mock_http.patch.side_effect = [real_resp, dry_resp]
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=mock_http)
    ctx.__aexit__ = AsyncMock(return_value=None)
    client._client.return_value = ctx

    row = SimpleNamespace(vrf="", prefix="100.64.0.0/10", next_hop="172.16.0.1", metric=1, permanent=False, tag=None)
    with pytest.raises(NsoApplyError) as exc_info:
        await apply_static_routes(client=client, device_name="sw03", route_intent_rows=[row])
    assert exc_info.value.code == "verify_mismatch"
    # First call is the real PATCH (no dry-run), second is the verify dry-run.
    assert "dry-run" not in mock_http.patch.call_args_list[0][0][0]
    assert "dry-run=native" in mock_http.patch.call_args_list[1][0][0]


@pytest.mark.asyncio
async def test_apply_bgp_config_uses_correct_yang_keys():
    """Regression (finding #31): the bgp-reconciler service YANG uses `afi` (not `af`)
    and `peer-address-family` (not `address-family`) under each peer. Sending the wrong
    keys caused a live 400 `unknown element: address-family` on rg03's IOS BGP."""
    import json

    from nso_adapter.nso.apply import apply_bgp_config

    client = _make_nso_client()
    real_resp = _mock_httpx_response(204)
    # Verify dry-run with no device delta → guard passes.
    dry_resp = _mock_httpx_response(200, json_data={"dry-run-result": {"native": {"device": []}}})
    mock_http = AsyncMock()
    mock_http.patch.side_effect = [real_resp, dry_resp]
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=mock_http)
    ctx.__aexit__ = AsyncMock(return_value=None)
    client._client.return_value = ctx

    paf = SimpleNamespace(
        af="ipv4-unicast",
        enabled=True,
        routemap_in=None,
        routemap_out=None,
        prefixlist_in=None,
        prefixlist_out=None,
    )
    peer = SimpleNamespace(
        peer_address="192.168.204.2",
        enabled=True,
        peer_group=None,
        remote_as=65100,
        local_as=None,
        ttl=None,
        password=None,
        peer_address_families=[paf],
    )
    scope = SimpleNamespace(
        vrf="",
        address_families=[SimpleNamespace(af="ipv4-unicast")],
        peers=[peer],
    )
    router = SimpleNamespace(asn=65100, scopes=[scope])

    await apply_bgp_config(client=client, device_name="rg03", router_intent_rows=[router])

    payload = json.loads(mock_http.patch.call_args_list[0][1]["content"])
    router_out = payload["bgp-reconciler:bgp-config"][0]["router"][0]
    scope_out = router_out["scope"][0]
    # scope-level AF list key is `afi`
    assert scope_out["address-family"][0]["afi"] == "ipv4-unicast"
    assert "af" not in scope_out["address-family"][0]
    # per-peer AF list is `peer-address-family` keyed by `afi`
    peer_out = scope_out["peer"][0]
    assert "address-family" not in peer_out
    assert peer_out["peer-address-family"][0]["afi"] == "ipv4-unicast"


def test_build_isis_process_payload_attaches_flex_algo():
    """Flex-algo rows attach under their process-tag, creating a minimal process
    entry when the tag has no process row (e.g. IOS-XR flex-only)."""
    from nso_adapter.nso.apply import build_isis_process_payload

    fa = SimpleNamespace(
        process_tag="NA4-CORE",
        algo_id=130,
        metric_type="delay-metric",
        priority=200,
        admin_group_exclude="RED",
        admin_group_include_any=None,
        admin_group_include_all=None,
    )
    procs = build_isis_process_payload(isis_process_rows=[], redistribution_rows=[], flex_algo_rows=[fa])
    assert len(procs) == 1
    p = procs[0]
    assert p["process-tag"] == "NA4-CORE"
    assert p["flex-algo"][0]["algo-id"] == 130
    assert p["flex-algo"][0]["metric-type"] == "delay-metric"
    assert p["flex-algo"][0]["admin-group-exclude"] == "RED"


def test_build_isis_process_payload_omits_empty_enums():
    """Empty-string enum leaves (metric-style/is-type) are omitted, not sent as ''."""
    from nso_adapter.nso.apply import build_isis_process_payload

    row = SimpleNamespace(
        process_tag="0",
        net="49.0001.00",
        is_type="",
        metric_style="",
        overload_bit=None,
        area_auth_type="",
        area_auth_key=None,
        domain_auth_type="",
        domain_auth_key=None,
    )
    procs = build_isis_process_payload(isis_process_rows=[row], redistribution_rows=[], flex_algo_rows=[])
    assert "metric-style" not in procs[0]
    assert "is-type" not in procs[0]
    assert procs[0]["net"] == "49.0001.00"


def test_build_isis_interface_payload_normalises_circuit_type():
    """circuit-type 'level-2' (invalid enum) is normalised to 'level-2-only'."""
    from nso_adapter.nso.apply import build_isis_interface_payload

    row = SimpleNamespace(
        interface_name="ae2.0",
        af="ipv4",
        process_tag="",
        passive=False,
        circuit_type="level-2",
        network_type=None,
        metric=10,
    )
    ifaces = build_isis_interface_payload([row])
    assert ifaces[0]["circuit-type"] == "level-2-only"


@pytest.mark.asyncio
async def test_replace_isis_service_puts_keyed_instance():
    """replace_isis_service PUTs the keyed service instance (so empty process-tags
    removal works — PUT key is the device name, not the empty list key)."""
    import json

    from nso_adapter.nso.apply import replace_isis_service

    client = _make_nso_client()
    mock_http = AsyncMock()
    mock_http.put.return_value = _mock_httpx_response(204)
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=mock_http)
    ctx.__aexit__ = AsyncMock(return_value=None)
    client._client.return_value = ctx
    await replace_isis_service(client=client, device_name="rc1", interfaces=[{"interface-name": "ae2.0"}], processes=[])

    (url,) = mock_http.put.call_args[0]
    assert url.split("?")[0].endswith("isis-reconciler:isis-config=rc1")
    assert "reconcile=keep-non-service-config" in url
    body = json.loads(mock_http.put.call_args[1]["content"])
    assert body["isis-reconciler:isis-config"][0]["device"] == "rc1"


@pytest.mark.asyncio
async def test_replace_service_instance_puts_keyed_instance():
    """Generic removal primitive: PUT on the keyed service instance with the full body."""
    import json

    from nso_adapter.nso.apply import replace_service_instance

    client = _make_nso_client()
    mock_http = AsyncMock()
    mock_http.put.return_value = _mock_httpx_response(204)
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=mock_http)
    ctx.__aexit__ = AsyncMock(return_value=None)
    client._client.return_value = ctx

    await replace_service_instance(
        client,
        "/restconf/data/vlan-reconciler:vlan-config",
        "vlan-reconciler:vlan-config",
        "sw3",
        {"device": "sw3", "vlan": [{"vlan-id": 10}]},
    )
    (url,) = mock_http.put.call_args[0]
    assert url.split("?")[0].endswith("vlan-reconciler:vlan-config=sw3")
    assert "reconcile=keep-non-service-config" in url
    body = json.loads(mock_http.put.call_args[1]["content"])
    assert body["vlan-reconciler:vlan-config"][0]["vlan"][0]["vlan-id"] == 10


@pytest.mark.asyncio
async def test_apply_vlan_config_replace_puts_remaining_list():
    """apply_vlan_config(replace=True) PUT-replaces the keyed instance with the full
    remaining vlan list (a dropped vid is simply absent from the body)."""
    import json

    from nso_adapter.nso.apply import apply_vlan_config

    client = _make_nso_client()
    mock_http = AsyncMock()
    mock_http.put.return_value = _mock_httpx_response(204)
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=mock_http)
    ctx.__aexit__ = AsyncMock(return_value=None)
    client._client.return_value = ctx

    rows = [SimpleNamespace(vlan_id=10, name="keep")]  # 3366 dropped → absent from body
    await apply_vlan_config(client=client, device_name="sw3", vlan_intent_rows=rows, replace=True)
    # First PUT is the real replace; the second is the native dry-run verify.
    (url,) = mock_http.put.call_args_list[0][0]
    assert url.split("?")[0].endswith("vlan-reconciler:vlan-config=sw3")
    assert "reconcile=keep-non-service-config" in url
    body = json.loads(mock_http.put.call_args_list[0][1]["content"])
    vids = [v["vlan-id"] for v in body["vlan-reconciler:vlan-config"][0]["vlan"]]
    assert vids == [10]
    assert "dry-run=native" in mock_http.put.call_args_list[1][0][0]  # verify uses PUT too
    mock_http.patch.assert_not_called()  # replace must not merge-PATCH


@pytest.mark.asyncio
async def test_apply_static_routes_replace_puts_keyed_instance():
    """apply_static_routes(replace=True) PUT-replaces instead of merge-PATCH."""
    import json

    client = _make_nso_client()
    mock_http = AsyncMock()
    mock_http.put.return_value = _mock_httpx_response(204)
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=mock_http)
    ctx.__aexit__ = AsyncMock(return_value=None)
    client._client.return_value = ctx

    rows = [SimpleNamespace(vrf="", prefix="10.0.0.0/8", next_hop="1.1.1.1", metric=1, permanent=False, tag=None)]
    await apply_static_routes(client=client, device_name="sw3", route_intent_rows=rows, replace=True)
    (url,) = mock_http.put.call_args_list[0][0]
    assert url.split("?")[0].endswith("static-route-reconciler:static-route-config=sw3")
    assert "reconcile=keep-non-service-config" in url
    body = json.loads(mock_http.put.call_args_list[0][1]["content"])
    assert body["static-route-reconciler:static-route-config"][0]["route"][0]["prefix"] == "10.0.0.0/8"
    mock_http.patch.assert_not_called()


@pytest.mark.asyncio
async def test_apply_route_policy_translates_and_skips_members_per_ned():
    """On a Nokia (timos) device the canonical community members are translated to
    the SR OS dialect and the ones it can't represent (``color:``) are dropped from
    the pushed body — so one bad member can't abort the whole community."""
    import json

    from nso_adapter.nso.apply import apply_route_policy_config

    client = _make_nso_client()
    mock_http = AsyncMock()
    mock_http.patch.return_value = _mock_httpx_response(204)
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=mock_http)
    ctx.__aexit__ = AsyncMock(return_value=None)
    client._client.return_value = ctx

    entries = [
        {"sequence": 10, "action": "permit", "community": "6830:1234"},
        {"sequence": 20, "action": "permit", "community": "6830:.*"},  # digit-domain regex kept
        {"sequence": 30, "action": "permit", "community": "target:6830:1234"},
        {"sequence": 40, "action": "permit", "community": "large:6830:6370:1234"},  # exact → strip large:
        {"sequence": 50, "action": "permit", "community": "color:0:128"},  # no SR OS keyword
        {"sequence": 60, "action": "permit", "community": "color:0:12."},  # no SR OS keyword
        {"sequence": 70, "action": "permit", "community": "no-export"},
    ]
    rows = [SimpleNamespace(family="community_list", name="cnad-test", entries=entries)]

    await apply_route_policy_config(client=client, device_name="ra1", intent_rows=rows, ned_id="timos-nc-23.10")

    body = json.loads(mock_http.patch.call_args_list[0][1]["content"])
    cl = body["route-policy-reconciler:route-policy-config"][0]["community-list"][0]
    members = [e["community"] for e in cl["entry"]]
    assert members == ["6830:1234", "6830:.*", "target:6830:1234", "6830:6370:1234", "no-export"]
    assert cl["invert-match"] is False


@pytest.mark.asyncio
async def test_apply_route_policy_carries_invert_match_and_amp_large_on_nokia():
    """An inverted community-list keeps its invert-match flag in the pushed body, and
    a regex large community is rendered in SR OS `&`-separated form (color: dropped)."""
    import json

    from nso_adapter.nso.apply import apply_route_policy_config

    client = _make_nso_client()
    mock_http = AsyncMock()
    mock_http.patch.return_value = _mock_httpx_response(204)
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=mock_http)
    ctx.__aexit__ = AsyncMock(return_value=None)
    client._client.return_value = ctx

    entries = [
        {"sequence": 10, "action": "permit", "community": "no-export"},
        {"sequence": 20, "action": "permit", "community": "6830:21000"},
        {"sequence": 30, "action": "permit", "community": "large:6830:.*:[0-4]"},  # regex large → &
        {"sequence": 40, "action": "permit", "community": "color:0:128"},  # dropped on Nokia
    ]
    rows = [SimpleNamespace(family="community_list", name="SCRUBBER", entries=entries, invert_match=True)]

    await apply_route_policy_config(client=client, device_name="ra1", intent_rows=rows, ned_id="timos-nc-23.10")

    body = json.loads(mock_http.patch.call_args_list[0][1]["content"])
    cl = body["route-policy-reconciler:route-policy-config"][0]["community-list"][0]
    assert cl["invert-match"] is True
    assert [e["community"] for e in cl["entry"]] == ["no-export", "6830:21000", "6830&.*&[0-4]"]


@pytest.mark.asyncio
async def test_apply_route_policy_keeps_all_members_on_identity_ned():
    """A NED with no dialect override (IOS-XR here) keeps every member verbatim —
    ``color:`` is a valid Cisco extcommunity, so nothing is dropped."""
    import json

    from nso_adapter.nso.apply import apply_route_policy_config

    client = _make_nso_client()
    mock_http = AsyncMock()
    mock_http.patch.return_value = _mock_httpx_response(204)
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=mock_http)
    ctx.__aexit__ = AsyncMock(return_value=None)
    client._client.return_value = ctx

    entries = [
        {"sequence": 10, "action": "permit", "community": "color:0:128"},
        {"sequence": 20, "action": "permit", "community": "large:6830:6370:.*"},
    ]
    rows = [SimpleNamespace(family="community_list", name="cnad-test", entries=entries)]

    await apply_route_policy_config(client=client, device_name="rx", intent_rows=rows, ned_id="cisco-iosxr-cli-7.76")

    body = json.loads(mock_http.patch.call_args_list[0][1]["content"])
    members = [
        e["community"] for e in body["route-policy-reconciler:route-policy-config"][0]["community-list"][0]["entry"]
    ]
    assert members == ["color:0:128", "large:6830:6370:.*"]  # untouched


def test_normalize_route_map_entry_yang_shape_passthrough():
    """New plugin payloads already use the YANG leaf names — preserved verbatim."""
    entry = {
        "sequence": 10,
        "action": "permit",
        "match-prefix-lists": ["PL-1"],
        "match-community-lists": [],
        "match-as-paths": [],
        "match-json": '{"protocol": ["direct"], "to_protocol": ["bgp"]}',
        "set-json": '{"next_hop_self": true}',
    }
    assert apply_mod._normalize_route_map_entry(entry) == entry


def test_normalize_route_map_entry_legacy_shape():
    """Legacy intents carried match/set blobs (dict or str) and no match refs —
    mapped onto match-json/set-json; unknown keys dropped (RESTCONF would 400)."""
    entry = {
        "sequence": 10,
        "action": "deny",
        "match": {"x": 1},
        "set": '{"local_preference": 200}',
        "match_prefix_lists": ["PL-1"],
        "bogus": "dropped",
    }
    out = apply_mod._normalize_route_map_entry(entry)
    assert out == {
        "sequence": 10,
        "action": "deny",
        "match-json": '{"x": 1}',
        "set-json": '{"local_preference": 200}',
        "match-prefix-lists": ["PL-1"],
    }


@pytest.mark.asyncio
async def test_apply_ospf_always_asserts_enabled_delete_guard():
    """Delete-guard: the OSPF process body ALWAYS carries `enabled`, even when the
    intent row leaves it None — so a PUT-replace can never drop admin-state and
    disable OSPF. Default is True; explicit False is preserved."""
    import json

    from nso_adapter.nso.apply import apply_ospf_config

    client = _make_nso_client()
    client._client.return_value = _mock_http_ctx(_mock_httpx_response(204))

    rows = [
        SimpleNamespace(process_id="1", router_id="10.0.0.1", vrf="", enabled=None),
        SimpleNamespace(process_id="2", router_id="10.0.0.2", vrf="", enabled=False),
    ]
    await apply_ospf_config(client=client, device_name="ra1", process_intent_rows=rows, interface_intent_rows=[])

    mock_http = client._client.return_value.__aenter__.return_value
    payload = json.loads(mock_http.patch.call_args[1]["content"])
    procs = payload["ospf-reconciler:ospf-config"][0]["process-config"]
    by_pid = {p["process-id"]: p for p in procs}
    assert by_pid[1]["enabled"] is True  # None → default enable (guard)
    assert by_pid[2]["enabled"] is False  # explicit disable preserved


@pytest.mark.asyncio
async def test_apply_ospf_replace_body_keeps_enabled():
    """The same guard holds on the removal path (replace=True PUT-replace) — the
    body that reverts removed rows still asserts admin-state, so a removal never
    collaterally disables OSPF."""
    import json

    from nso_adapter.nso.apply import apply_ospf_config

    client = _make_nso_client()
    mock_http = AsyncMock()
    mock_http.put.return_value = _mock_httpx_response(204)
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=mock_http)
    ctx.__aexit__ = AsyncMock(return_value=None)
    client._client.return_value = ctx

    rows = [SimpleNamespace(process_id="1", router_id="10.0.0.1", vrf="", enabled=None)]
    await apply_ospf_config(
        client=client, device_name="ra1", process_intent_rows=rows, interface_intent_rows=[], replace=True
    )

    payload = json.loads(mock_http.put.call_args_list[0][1]["content"])
    proc = payload["ospf-reconciler:ospf-config"][0]["process-config"][0]
    assert proc["enabled"] is True


# ── reconcile commit option (brownfield adoption) ────────────────────────────


def test_commit_url_appends_reconcile_by_default(monkeypatch):
    """A plain service write gets ``?reconcile=keep-non-service-config``."""
    from nso_adapter.nso.apply import _commit_url

    monkeypatch.setattr(apply_mod, "RECONCILE_COMMIT", "keep-non-service-config")
    assert _commit_url("http://nso/restconf/data/x") == ("http://nso/restconf/data/x?reconcile=keep-non-service-config")


def test_commit_url_combines_dry_run_and_reconcile(monkeypatch):
    """dry_run=True adds ``dry-run=native`` alongside reconcile (NSO accepts both)."""
    from nso_adapter.nso.apply import _commit_url

    monkeypatch.setattr(apply_mod, "RECONCILE_COMMIT", "keep-non-service-config")
    assert _commit_url("http://nso/x", dry_run=True) == (
        "http://nso/x?dry-run=native&reconcile=keep-non-service-config"
    )


def test_commit_url_uses_ampersand_when_url_has_query(monkeypatch):
    """An existing ``?`` in the URL means the param is joined with ``&``."""
    from nso_adapter.nso.apply import _commit_url

    monkeypatch.setattr(apply_mod, "RECONCILE_COMMIT", "discard-non-service-config")
    assert _commit_url("http://nso/x?already=1") == ("http://nso/x?already=1&reconcile=discard-non-service-config")


def test_commit_url_no_param_when_disabled(monkeypatch):
    """Empty RECONCILE_COMMIT reverts to a plain commit (no reconcile param)."""
    from nso_adapter.nso.apply import _commit_url

    monkeypatch.setattr(apply_mod, "RECONCILE_COMMIT", "")
    assert _commit_url("http://nso/x") == "http://nso/x"
    assert _commit_url("http://nso/x", dry_run=True) == "http://nso/x?dry-run=native"


@pytest.mark.asyncio
async def test_apply_real_commit_carries_reconcile_and_dry_run_does_too(monkeypatch):
    """The real PATCH commits with reconcile; the post-apply verify dry-run carries
    both dry-run=native and reconcile so the preview matches the commit."""
    monkeypatch.setattr(apply_mod, "RECONCILE_COMMIT", "keep-non-service-config")
    client = _make_nso_client()
    client._client.return_value = _mock_http_ctx(_mock_httpx_response(204))

    await apply_interface_attribute(client, "core-rtr-01", "Gi0/0", "description", "uplink")

    mock_http = client._client.return_value.__aenter__.return_value
    real_url = mock_http.patch.call_args_list[0][0][0]
    verify_url = mock_http.patch.call_args_list[1][0][0]
    # Real commit: reconcile present, NOT a dry-run.
    assert "reconcile=keep-non-service-config" in real_url
    assert "dry-run" not in real_url
    # Verify dry-run: both params.
    assert "dry-run=native" in verify_url
    assert "reconcile=keep-non-service-config" in verify_url
