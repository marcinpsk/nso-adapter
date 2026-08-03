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
from nso_adapter.nso.client import NsoClient


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
    result = await client.get_device_ned_id("lab01c-ra1")

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


# ── service_instance_state — the certified tri-state read (#1396 R2 §4.4) ─────
#
# get_service_config returns None for a keyed 404 AND for any 2xx it cannot parse into a
# recognized non-empty root. That conflation is safe where the read can only OMIT work; it
# is not safe under a live-relative PUT body, which would silently drop every entry the
# uncertified read failed to show. These pin that only a 404 certifies an absence.

_SR_PATH = "/restconf/data/static-route-reconciler:static-route-config"
_SR_ROOT = "static-route-reconciler:static-route-config"
_ENTRY = {"device": "rtr", "route": [{"vrf": "", "prefix": "10.0.0.0/24", "next-hop": "192.0.2.1"}]}


async def test_service_instance_state_present_on_a_parsed_instance(patch_client):
    client = _make_client()
    with patch_client(client, 200, {_SR_ROOT: [_ENTRY]}):
        state = await client.service_instance_state(_SR_PATH, "rtr")
    assert (state.status, state.entry) == ("present", _ENTRY)
    assert state.present and not state.inconclusive


async def test_service_instance_state_absent_only_on_a_keyed_404(patch_client):
    client = _make_client()
    with patch_client(client, 404, {"ietf-restconf:errors": {}}):
        state = await client.service_instance_state(_SR_PATH, "rtr")
    assert (state.status, state.entry) == ("absent", None)


@pytest.mark.parametrize(
    ("body", "raw", "label"),
    [
        ({"some-other-module:thing": [{"device": "rtr"}]}, None, "a renamed/unknown root"),
        ({_SR_ROOT: []}, None, "an empty root list"),
        ({_SR_ROOT: [{}]}, None, "an empty instance entry"),
        ({_SR_ROOT: {"device": "rtr"}}, None, "a non-list root"),
        ({}, None, "no root at all"),
        (None, b"<html>not json</html>", "an unparseable body"),
    ],
)
async def test_service_instance_state_is_inconclusive_never_absent(patch_client, body, raw, label):
    """Each of these makes ``get_service_config`` return None — i.e. reads as "absent".

    Consuming a carrier or building a destructive replace on any of them throws away a
    deletion record while the service may still own the key, so they must be a REFUSAL.
    """
    client = _make_client()
    with patch_client(client, 200, body, raw):
        state = await client.service_instance_state(_SR_PATH, "rtr")
    assert state.status == "inconclusive", label
    assert state.entry is None
    assert not state.present


@pytest.mark.parametrize(
    "body",
    [
        {"some-other-module:thing": [{"device": "rtr"}]},
        {_SR_ROOT: []},
        {_SR_ROOT: [{}]},
        {},
    ],
    ids=["unknown root", "empty root list", "empty entry", "no root"],
)
async def test_the_legacy_reader_reports_these_same_bodies_as_no_instance(patch_client, body):
    """The discriminating half of the tri-state: every caller of ``get_service_config``
    treats a falsy answer as "no service instance", which is exactly the misreading a
    destructive replace must not be built on."""
    client = _make_client()
    with patch_client(client, 200, body):
        assert not await client.get_service_config(_SR_PATH, "rtr")


async def test_service_instance_state_accepts_the_short_root_spelling(patch_client):
    """RESTCONF may answer with the module prefix stripped — that is still a real read."""
    client = _make_client()
    with patch_client(client, 200, {"static-route-config": [_ENTRY]}):
        state = await client.service_instance_state(_SR_PATH, "rtr")
    assert state.status == "present"


async def test_service_instance_state_raises_on_a_server_error(patch_client):
    """A 500 is neither an absence nor a certified read — it must not be swallowed."""
    client = _make_client()
    with patch_client(client, 500, {"error": "boom"}):
        with pytest.raises(httpx.HTTPStatusError):
            await client.service_instance_state(_SR_PATH, "rtr")


@pytest.mark.parametrize(
    ("body", "label"),
    [
        ({_SR_ROOT: [_ENTRY, {"device": "other-rtr", "route": []}]}, "two instances"),
        ({_SR_ROOT: [{"device": "other-rtr", "route": []}]}, "another device's instance"),
        ({_SR_ROOT: [{"route": []}]}, "an instance with no device key"),
    ],
)
async def test_service_instance_state_refuses_an_instance_it_did_not_ask_for(patch_client, body, label):
    """A keyed GET answers with exactly ITS instance — anything else is not what we asked for.

    Taking ``entries[0]`` regardless would compute the retained entries and the collateral
    guard's view from another device's instance, so the PUT would omit this device's real
    rows and the verify would still pass.
    """
    client = _make_client()
    with patch_client(client, 200, body):
        state = await client.service_instance_state(_SR_PATH, "rtr")
    assert state.status == "inconclusive", label
    assert state.entry is None
