# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Unit tests for NsoClient HTTP methods using httpx mock transport.

These tests exercise list_devices, get_device_config, get_device_ned_id,
and check_sync without hitting a real NSO instance.
"""

from __future__ import annotations

import json

import httpx
import pytest

from nso_adapter.config import NsoInstanceConfig
from nso_adapter.nso.client import NsoClient, NsoExportUnavailableError


def _make_cfg(base_url: str = "http://nso:8080", ca_cert=None, host_header=None):
    # A REAL NsoInstanceConfig — NsoClient reads base_url/ca_cert/host_header off it, so a
    # renamed config field surfaces as a real error instead of a fabricated MagicMock attr.
    return NsoInstanceConfig(
        name="nso-dev",
        base_url=base_url,
        ca_cert=ca_cert,
        username_ref="NSO_USERNAME",
        password_ref="NSO_PASSWORD",
        host_header=host_header,
    )


def _make_client(base_url: str = "http://nso:8080", host_header=None) -> NsoClient:
    return NsoClient(_make_cfg(base_url, host_header=host_header), "admin", "secret")


class MockTransport(httpx.AsyncBaseTransport):
    """Minimal async transport returning a pre-canned response."""

    def __init__(self, status_code: int, body: dict | None = None, raw_bytes: bytes | None = None):
        self.status_code = status_code
        if raw_bytes is not None:
            self._content = raw_bytes
        elif body is not None:
            self._content = json.dumps(body).encode()
        else:
            self._content = b""

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            self.status_code,
            content=self._content,
            headers={"content-type": "application/yang-data+json"},
            request=request,
        )


@pytest.fixture
def patch_client():
    """Return a helper that patches NsoClient._client() to use MockTransport."""
    import contextlib

    @contextlib.contextmanager
    def _patcher(nso_client: NsoClient, status: int, body: dict | None = None, raw: bytes | None = None):
        transport = MockTransport(status, body, raw)
        original = nso_client._client

        def _mock_client(timeout=None):
            return httpx.AsyncClient(transport=transport, base_url="http://nso:8080")

        nso_client._client = _mock_client
        try:
            yield
        finally:
            nso_client._client = original

    return _patcher


# ── list_devices ─────────────────────────────────────────────────────────────


async def test_list_devices_returns_device_list(patch_client):
    """list_devices parses tailf-ncs:devices.device list."""
    client = _make_client()
    payload = {
        "tailf-ncs:devices": {
            "device": [
                {"name": "core-rtr-01", "address": "10.0.0.1"},
                {"name": "edge-rtr-02", "address": "10.0.0.2"},
            ]
        }
    }
    with patch_client(client, 200, payload):
        result = await client.list_devices()
    assert len(result) == 2
    assert result[0]["name"] == "core-rtr-01"
    assert result[1]["name"] == "edge-rtr-02"


async def test_list_devices_empty_when_no_device_key(patch_client):
    """list_devices returns [] when 'device' key is missing from payload."""
    client = _make_client()
    payload = {"tailf-ncs:devices": {}}  # no "device" key
    with patch_client(client, 200, payload):
        result = await client.list_devices()
    assert result == []


async def test_list_devices_empty_when_device_not_list(patch_client):
    """list_devices returns [] when 'device' value is not a list."""
    client = _make_client()
    payload = {"tailf-ncs:devices": {"device": "not-a-list"}}
    with patch_client(client, 200, payload):
        result = await client.list_devices()
    assert result == []


async def test_list_devices_raises_on_http_error(patch_client):
    """list_devices propagates httpx.HTTPStatusError on 4xx/5xx."""
    client = _make_client()
    with patch_client(client, 500, {"error": "internal"}):
        with pytest.raises(httpx.HTTPStatusError):
            await client.list_devices()


# ── get_device_config ────────────────────────────────────────────────────────


async def test_get_device_config_returns_config(patch_client):
    """get_device_config returns config subtree from tailf-ncs:config key."""
    client = _make_client()
    payload = {"tailf-ncs:config": {"interface": [{"name": "GE0/0"}]}}
    with patch_client(client, 200, payload):
        result = await client.get_device_config("core-rtr-01")
    assert "interface" in result


async def test_get_device_config_returns_raw_when_no_tailf_key(patch_client):
    """get_device_config returns the whole payload if tailf-ncs:config key is absent."""
    client = _make_client()
    payload = {"something-else": {"data": 42}}
    with patch_client(client, 200, payload):
        result = await client.get_device_config("core-rtr-01")
    assert result == payload


async def test_get_device_config_returns_empty_on_204(patch_client):
    """get_device_config returns {} on 204 No Content (CDB not populated)."""
    client = _make_client()
    with patch_client(client, 204, raw=b""):
        result = await client.get_device_config("core-rtr-01")
    assert result == {}


async def test_get_device_config_raises_on_404(patch_client):
    """get_device_config propagates HTTP error on 404."""
    client = _make_client()
    with patch_client(client, 404, {"error": "not found"}):
        with pytest.raises(httpx.HTTPStatusError):
            await client.get_device_config("unknown-device")


# ── get_device_ned_id ────────────────────────────────────────────────────────


async def test_get_device_ned_id_cli(patch_client):
    """get_device_ned_id returns NED ID from CLI device-type."""
    client = _make_client()
    payload = {"tailf-ncs:device": {"device-type": {"cli": {"ned-id": "cisco-ios-cli-6.95"}}}}
    with patch_client(client, 200, payload):
        result = await client.get_device_ned_id("core-rtr-01")
    assert result == "cisco-ios-cli-6.95"


async def test_get_device_ned_id_netconf(patch_client):
    """get_device_ned_id returns NED ID from NETCONF device-type."""
    client = _make_client()
    payload = {"tailf-ncs:device": {"device-type": {"netconf": {"ned-id": "juniper-junos-nc-4.1"}}}}
    with patch_client(client, 200, payload):
        result = await client.get_device_ned_id("edge-rtr-02")
    assert result == "juniper-junos-nc-4.1"


async def test_get_device_ned_id_list_response(patch_client):
    """get_device_ned_id handles NSO returning device as a list (keyed list entry)."""
    client = _make_client()
    payload = {"tailf-ncs:device": [{"device-type": {"cli": {"ned-id": "cisco-ios-cli-6.95"}}}]}
    with patch_client(client, 200, payload):
        result = await client.get_device_ned_id("core-rtr-01")
    assert result == "cisco-ios-cli-6.95"


async def test_get_device_ned_id_returns_none_when_no_type(patch_client):
    """get_device_ned_id returns None when device-type has no known keys."""
    client = _make_client()
    payload = {"tailf-ncs:device": {"device-type": {}}}
    with patch_client(client, 200, payload):
        result = await client.get_device_ned_id("mystery-device")
    assert result is None


async def test_get_device_ned_id_uses_fields_filter():
    """get_device_ned_id must send ``fields=device-type`` so NSO returns only the
    small device-type subtree — the unfiltered node pulls the device's full
    config + oper-data (~900 KB, streamed) and truncates mid-body, raising
    JSONDecodeError. Regression test for the device-55 sync failure."""
    client = _make_client()
    captured: dict = {}

    class _CapturingTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            captured["url"] = str(request.url)
            captured["params"] = dict(request.url.params)
            body = {"tailf-ncs:device": {"device-type": {"netconf": {"ned-id": "timos-nc-23.10:timos-nc-23.10"}}}}
            return httpx.Response(
                200,
                content=json.dumps(body).encode(),
                headers={"content-type": "application/yang-data+json"},
                request=request,
            )

    def _mock_client(timeout=None):
        return httpx.AsyncClient(transport=_CapturingTransport(), base_url="http://nso:8080")

    client._client = _mock_client
    result = await client.get_device_ned_id("prod-lab03c-ra1")

    assert result == "timos-nc-23.10:timos-nc-23.10"
    assert captured["params"].get("fields") == "device-type", captured


# ── check_sync ───────────────────────────────────────────────────────────────


async def test_check_sync_returns_true_when_in_sync(patch_client):
    """check_sync returns True when NSO reports 'in-sync'."""
    client = _make_client()
    payload = {"tailf-ncs:output": {"result": "in-sync"}}
    with patch_client(client, 200, payload):
        result = await client.check_sync("core-rtr-01")
    assert result is True


async def test_check_sync_returns_false_when_out_of_sync(patch_client):
    """check_sync returns False when NSO reports any non-'in-sync' result."""
    client = _make_client()
    payload = {"tailf-ncs:output": {"result": "out-of-sync"}}
    with patch_client(client, 200, payload):
        result = await client.check_sync("core-rtr-01")
    assert result is False


async def test_check_sync_raises_on_http_error(patch_client):
    """check_sync must SURFACE an HTTP error (auth/unreachable/500), not silently report a
    false 'out-of-sync' that is indistinguishable from genuine drift (#16a)."""
    client = _make_client()
    with patch_client(client, 500, {"error": "internal"}), pytest.raises(httpx.HTTPStatusError):
        await client.check_sync("core-rtr-01")


async def test_device_name_percent_encoded_in_url():
    """A device name with a reserved char is percent-encoded in the RESTCONF URL path
    segment (a raw '/' would 404 / hit the wrong resource) (#14)."""
    client = _make_client()
    seen: dict = {}

    class _Recorder(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request):
            seen["url"] = str(request.url)
            return httpx.Response(200, json={}, request=request)

    client._client = lambda timeout=None: httpx.AsyncClient(transport=_Recorder(), base_url="http://nso:8080")
    await client.get_device_config("site/rtr1")
    assert "device=site%2Frtr1" in seen["url"]
    assert "device=site/rtr1" not in seen["url"]


@pytest.mark.parametrize(
    "call",
    [
        lambda c: c.set_address("rtr", "10.0.0.1"),
        lambda c: c.create_device("rtr", "10.0.0.1", "ned", "grp"),
        lambda c: c.set_admin_state("rtr"),
        lambda c: c.check_sync("rtr"),
    ],
)
async def test_device_write_methods_use_action_timeout(call):
    """Device-touching writes/probes must run on the 120s action timeout, not the blanket 30s
    — a >30s device commit (e.g. Junos ~35s) would otherwise false-timeout (#15)."""
    client = _make_client()
    seen_timeouts: list = []

    def _spy(timeout=None):
        seen_timeouts.append(timeout)

        class _T(httpx.AsyncBaseTransport):
            async def handle_async_request(self, request):
                return httpx.Response(200, json={"tailf-ncs:output": {"result": "in-sync"}}, request=request)

        return httpx.AsyncClient(transport=_T(), base_url="http://nso:8080")

    client._client = _spy
    await call(client)
    assert seen_timeouts == [client._action_timeout]


async def test_check_sync_returns_false_on_missing_result_key(patch_client):
    """check_sync returns False when output structure has no 'result' key."""
    client = _make_client()
    payload = {"tailf-ncs:output": {}}
    with patch_client(client, 200, payload):
        result = await client.check_sync("core-rtr-01")
    assert result is False


# ── fetch_host_keys (M-onboard: must not report success when no key stored) ────


async def test_fetch_host_keys_success_updated(patch_client):
    """A result of 'updated' with a fingerprint returns the action output."""
    client = _make_client()
    payload = {"tailf-ncs:output": {"result": "updated", "fingerprint": {"algorithm": "ssh-rsa", "value": "aa:bb"}}}
    with patch_client(client, 200, payload):
        out = await client.fetch_host_keys("core-rtr-01")
    assert out["tailf-ncs:output"]["result"] == "updated"


async def test_fetch_host_keys_success_unchanged(patch_client):
    """A result of 'unchanged' (key already trusted) is also success."""
    client = _make_client()
    payload = {"tailf-ncs:output": {"result": "unchanged", "fingerprint": {"algorithm": "ssh-rsa", "value": "aa:bb"}}}
    with patch_client(client, 200, payload):
        out = await client.fetch_host_keys("core-rtr-01")
    assert out["tailf-ncs:output"]["result"] == "unchanged"


async def test_fetch_host_keys_failed_result_raises(patch_client):
    """NSO returns HTTP 200 + result='failed' with no fingerprint → must raise.

    Regression: a failed fetch was previously swallowed and onboarding recorded
    a false 'ok' key-fetch step, so the first real connect failed host-key verify.
    """
    client = _make_client()
    payload = {"tailf-ncs:output": {"result": "failed", "info": "connection refused"}}
    with patch_client(client, 200, payload):  # noqa: SIM117
        with pytest.raises(RuntimeError, match="did not store a key"):
            await client.fetch_host_keys("core-rtr-01")


async def test_fetch_host_keys_no_fingerprint_raises(patch_client):
    """A 'success'-looking result without a fingerprint still raises (no key)."""
    client = _make_client()
    payload = {"tailf-ncs:output": {"result": "updated"}}
    with patch_client(client, 200, payload):  # noqa: SIM117
        with pytest.raises(RuntimeError, match="did not store a key"):
            await client.fetch_host_keys("core-rtr-01")


# ── device oper-data getters (the 18 network-state-export:<X>/device readers) ────
# All share one shape: GET network-state-export:<resource>/device={name} → 404 ⇒ None,
# else parse the device list and return entry[0] (or None when empty). A capturing
# transport asserts each method targets the RIGHT resource so a copy-pasted wrong URL
# is caught, not hidden behind a shared canned payload.

_DEVICE_OPER_METHODS = [
    ("get_lag_topology", "lag-topology"),
    ("get_svi", "svi"),
    ("get_subinterface", "subinterface"),
    ("get_interface_mtu", "interface-mtu"),
    ("get_lag_config", "lag-config"),
    ("get_vlan_database", "vlan-database"),
    ("get_switchport", "switchport"),
    ("get_interface_ips", "interface-ip"),
    ("get_interface_attributes", "interface-attributes"),
    ("get_snmp_config", "snmp-config"),
    ("get_logging_config", "logging-config"),
    ("get_static_routes", "static-route"),
    ("get_l2_services", "l2-service"),
    ("get_isis_interfaces", "isis-interface"),
    ("get_bgp_config", "bgp-config"),
    ("get_route_policy", "route-policy"),
    ("get_ospf", "ospf-config"),
    ("get_bfd_config", "bfd-config"),
]


class _CapturingTransport(httpx.AsyncBaseTransport):
    def __init__(self, status_code: int, body: dict | None):
        self._status = status_code
        self._content = json.dumps(body).encode() if body is not None else b""
        self.url: str | None = None

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.url = str(request.url)
        return httpx.Response(
            self._status,
            content=self._content,
            headers={"content-type": "application/yang-data+json"},
            request=request,
        )


def _capture(client: NsoClient, status: int, body: dict | None):
    transport = _CapturingTransport(status, body)
    client._client = lambda timeout=None: httpx.AsyncClient(transport=transport, base_url="http://nso:8080")
    return transport


@pytest.mark.parametrize(("method", "resource"), _DEVICE_OPER_METHODS)
async def test_device_oper_getter_returns_entry_and_targets_resource(method, resource):
    client = _make_client()
    entry = {"device-name": "sw01", "marker": resource}
    transport = _capture(client, 200, {"network-state-export:device": [entry]})

    result = await getattr(client, method)("sw01")

    assert result == entry
    assert f"network-state-export:{resource}/device=sw01" in transport.url


@pytest.mark.parametrize(
    ("method", "resource"),
    [(m, r) for m, r in _DEVICE_OPER_METHODS if m != "get_route_policy"],
)
async def test_device_oper_getter_returns_none_on_404(method, resource):
    client = _make_client()
    _capture(client, 404, {"error": "not found"})
    assert await getattr(client, method)("sw01") is None


# ── get_route_policy: a 404 is confirmed before it is believed ────────────────────────────────
#
# `refresh_route_policy_for_device` DELETES every mirrored route-policy row for a device when this
# returns None — it is the only refresher in the adapter that deletes on a None read. So None has to
# mean "the export is healthy and this device genuinely has no route-policy", and nothing else.
#
# A bare 404 cannot carry that meaning: it is equally what NSO returns when network-state-export is
# not loaded, is mid-`packages reload`, or its callpoint is erroring — in which case EVERY device
# 404s at once and the fleet's mirrored policy is wiped in one pass, reported as a successful
# refresh. This is the adapter half of RP-F8; the NSO half is RoutePolicyReadError.


class _PathAwareTransport(httpx.AsyncBaseTransport):
    """Answers the device GET and the parent-container probe with independent statuses."""

    def __init__(self, device_status: int, container_status: int):
        self._device_status = device_status
        self._container_status = container_status
        self.urls: list[str] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.urls.append(str(request.url))
        is_probe = "/device=" not in str(request.url)
        status = self._container_status if is_probe else self._device_status
        return httpx.Response(
            status,
            content=json.dumps({"network-state-export:device": []}).encode(),
            headers={"content-type": "application/yang-data+json"},
            request=request,
        )


def _path_aware(client: NsoClient, device_status: int, container_status: int):
    transport = _PathAwareTransport(device_status, container_status)
    client._client = lambda timeout=None: httpx.AsyncClient(transport=transport, base_url="http://nso:8080")
    return transport


async def test_route_policy_404_with_a_healthy_container_is_a_genuinely_empty_device():
    """The export is up and this device simply isn't in it — clearing the rows is CORRECT."""
    client = _make_client()
    transport = _path_aware(client, device_status=404, container_status=200)
    assert await client.get_route_policy("sw01") is None
    assert len(transport.urls) == 2, "the 404 must be confirmed against the parent container"


async def test_route_policy_404_from_a_MISSING_EXPORT_raises_instead_of_returning_none():
    """network-state-export is not exported at all → every device 404s → this would wipe the fleet."""
    client = _make_client()
    _path_aware(client, device_status=404, container_status=404)
    with pytest.raises(NsoExportUnavailableError, match="not exported"):
        await client.get_route_policy("sw01")


async def test_route_policy_404_with_a_broken_container_raises_the_http_error():
    """A 500 on the probe is a failure too — never silently a delete."""
    client = _make_client()
    _path_aware(client, device_status=404, container_status=500)
    with pytest.raises(httpx.HTTPStatusError):
        await client.get_route_policy("sw01")


async def test_route_policy_happy_path_does_not_probe():
    """The extra GET only ever runs on the 404 path."""
    client = _make_client()
    transport = _capture(client, 200, {"network-state-export:device": [{"device-name": "sw01"}]})
    assert await client.get_route_policy("sw01") == {"device-name": "sw01"}
    assert "/device=sw01" in transport.url


@pytest.mark.parametrize(("method", "resource"), _DEVICE_OPER_METHODS)
async def test_device_oper_getter_returns_none_when_device_list_empty(method, resource):
    client = _make_client()
    _capture(client, 200, {"network-state-export:device": []})
    assert await getattr(client, method)("sw01") is None


async def test_device_oper_getter_raises_on_500():
    client = _make_client()
    _capture(client, 500, {"error": "boom"})
    with pytest.raises(httpx.HTTPStatusError):
        await client.get_route_policy("sw01")


# ── list_ned_packages (filters to NED-component packages, parses, sorts) ────────


def _ned_pkg(ned_id: str, vendor: str = "Cisco", os_list=None):
    return {
        "name": ned_id,
        "package-version": "1.0",
        "oper-status": {"up": [None]},
        "component": [
            {
                "name": "x",
                "ned": {"cli": {"ned-id": ned_id}, "device": {"vendor": vendor, "operating-system": os_list or []}},
            }
        ],
    }


async def test_list_ned_packages_filters_to_ned_components_and_sorts(patch_client):
    client = _make_client()
    payload = {
        "tailf-ncs:package": [
            _ned_pkg("cisco-iosxr-cli-7.76", vendor="Cisco", os_list=["IOS XR"]),
            _ned_pkg("cisco-ios-cli-6.114"),
            {"name": "route-policy-reconciler", "component": [{"name": "app", "application": {}}]},  # not a NED
            "not-a-dict",  # skipped
        ]
    }
    with patch_client(client, 200, payload):
        out = await client.list_ned_packages()
    assert [p["ned_id"] for p in out] == ["cisco-ios-cli-6.114", "cisco-iosxr-cli-7.76"]  # NED-only, sorted
    assert out[0]["vendor"] == "Cisco"
    assert out[1]["operating_systems"] == ["IOS XR"]


async def test_list_ned_packages_empty_when_packages_not_a_list(patch_client):
    client = _make_client()
    with patch_client(client, 200, {"tailf-ncs:package": "nope"}):
        assert await client.list_ned_packages() == []


async def test_list_ned_packages_raises_on_http_error(patch_client):
    client = _make_client()
    with patch_client(client, 503, {"error": "unavailable"}):
        with pytest.raises(httpx.HTTPStatusError):
            await client.list_ned_packages()


# ── Management-address failover: get_address / set_address / disconnect ───────


def _capturing(nso_client: NsoClient, status: int = 204, body: dict | None = None) -> list[httpx.Request]:
    """Patch _client() with a request-capturing MockTransport; returns the captured requests.

    The real NsoClient builds the URL/method/body; only the socket is faked, so a regression
    in request construction surfaces as a failed assertion rather than a fabricated call.
    """
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(status, json=body if body is not None else {}, request=request)

    transport = httpx.MockTransport(handler)
    nso_client._client = lambda timeout=None: httpx.AsyncClient(transport=transport, base_url="http://nso:8080")
    return captured


async def test_get_address_returns_address():
    client = _make_client()
    _capturing(client, 200, {"tailf-ncs:device": [{"name": "rtr", "address": "10.0.0.1"}]})
    assert await client.get_address("rtr") == "10.0.0.1"


async def test_get_address_404_returns_none():
    client = _make_client()
    _capturing(client, 404)
    assert await client.get_address("rtr") is None


async def test_set_address_patches_only_address():
    client = _make_client()
    captured = _capturing(client, 204)
    await client.set_address("rtr", "192.0.2.5")
    assert len(captured) == 1
    req = captured[0]
    assert req.method == "PATCH"
    assert "device=rtr" in str(req.url)
    body = json.loads(req.content)
    entry = body["tailf-ncs:device"][0]
    assert entry == {"name": "rtr", "address": "192.0.2.5"}  # no port key when unset


async def test_set_address_includes_port_when_given():
    client = _make_client()
    captured = _capturing(client, 204)
    await client.set_address("rtr", "192.0.2.5", port=2022)
    entry = json.loads(captured[0].content)["tailf-ncs:device"][0]
    assert entry["port"] == 2022


async def test_set_address_raises_on_http_error():
    client = _make_client()
    _capturing(client, 409, {"error": "conflict"})
    with pytest.raises(httpx.HTTPStatusError):
        await client.set_address("rtr", "192.0.2.5")


async def test_disconnect_posts_disconnect_action():
    client = _make_client()
    captured = _capturing(client, 200, {})
    await client.disconnect("rtr")
    req = captured[0]
    assert req.method == "POST"
    assert str(req.url).endswith("device=rtr/disconnect")
