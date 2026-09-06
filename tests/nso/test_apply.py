# SPDX-License-Identifier: Apache-2.0
"""Tests for nso/apply.py: the wire vocabulary and the one sender.

The per-family senders are gone with the reconcilers. What is left here is the pure
``encode_*`` vocabulary each family's container is built from, and ``apply_device_intent``,
which PUTs the whole document as one instance.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from nso_adapter.core.community_dialect import community_dialect_for
from nso_adapter.core.projection import InterfaceExecution
from nso_adapter.nso import apply as apply_mod
from nso_adapter.nso.apply import (
    NsoApplyError,
    SectionExecution,
    _device_delta_from_dry_run,
    _ospf_interface_entry,
    _ospf_process_entry,
    _verify_native_or_raise,
    apply_device_intent,
    build_interface_ip_body,
    build_interface_ip_entry,
    build_isis_process_payload,
    encode_bgp,
    encode_interface_config,
    encode_l2_sap,
    encode_ospf,
    encode_route_policy,
    encode_static_route,
    encode_vlan,
)
from nso_adapter.nso.client import DEVICE_INTENT_ROOT, NsoClient
from nso_adapter.store.models import (
    BgpRouterIntent,
    IsisFlexAlgoIntent,
    IsisLevelIntent,
    IsisProcessIntent,
    OspfInstanceIntent,
    OspfInterfaceIntent,
    RedistributionIntent,
)

#: What an encoder that reads no NED-conditioned fact is handed.
_PLAIN = SectionExecution(None, community_dialect_for(None))


def _dialect_for(ned_id: str | None) -> SectionExecution:
    """The frozen context a NED-conditioned encoder reads: the id and its dialect."""
    return SectionExecution(ned_id, community_dialect_for(ned_id))


def _iface(interface_id: int = 1, name: str = "Gi0/0", **kwargs) -> SimpleNamespace:
    """A stand-in for the DbInterface row the interface proof carries."""
    fields = {"kind": "physical", "service": "", "vrf": "", "parent_binding": None, "encap_tag": None}
    return SimpleNamespace(id=interface_id, name=name, **{**fields, **kwargs})


def _attr_row(interface_id: int, attribute: str, value) -> SimpleNamespace:
    return SimpleNamespace(interface_id=interface_id, attribute=attribute, intent_value=value)


def _interface_body(attr_rows=(), ip_rows=(), interfaces=None, eligible=None) -> dict:
    """Encode the ``interface`` container from its two tables plus the frozen proof."""
    ifaces = {i.id: i for i in (interfaces or [_iface()])}
    keys = frozenset((r.interface_id, r.attribute) for r in attr_rows) if eligible is None else eligible
    execution = SectionExecution(None, community_dialect_for(None), InterfaceExecution(ifaces, keys))
    return encode_interface_config(
        {"interface_intent": list(attr_rows), "interface_ip_intent": list(ip_rows)}, execution
    )


def _sent_document(client, index: int = 0) -> dict:
    """The ``list device-intent`` entry the mocked PUT carried."""
    import json

    call = client._client.return_value.__aenter__.return_value.put.call_args_list[index]
    return json.loads(call.kwargs["content"])[DEVICE_INTENT_ROOT][0]


def _make_nso_client(base="http://nso"):
    # The NSO RESTCONF client is a real external HTTP boundary; bind the fake to NsoClient
    # via spec= so a renamed member can't be fabricated. Only _base/_action_timeout are read
    # directly; each test fakes the HTTP round-trip via client._client() (see _stub_pool).
    client = MagicMock(spec=NsoClient)
    client._base = base
    client._action_timeout = 120.0
    return client


def _httpx_response(status: int = 204, json_data=None) -> httpx.Response:
    """A REAL httpx.Response. With json_data, .json() returns it (.text is the JSON dump);
    without, the body is non-JSON so .json() raises and the apply error path falls back to
    .text — exactly how a real NSO 4xx/5xx with a non-JSON body behaves."""
    req = httpx.Request("PATCH", "http://nso/apply")
    if json_data is not None:
        return httpx.Response(status, json=json_data, request=req)
    return httpx.Response(status, text="error body", request=req)


def _stub_pool(client, http):
    """Wire client._client() as an async context manager yielding *http* (the pooled-client
    stand-in) WITHOUT a bare MagicMock CM — the spec'd client's auto-created child already
    supports async-with. __aexit__ returns False so exceptions in the body propagate.
    Returns *http* for call-assertion convenience."""
    cm = client._client.return_value
    cm.__aenter__.return_value = http
    cm.__aexit__.return_value = False
    return http


def _mock_http_ctx(client, response):
    """Single-response convenience: wire client._client() to an http whose .patch returns
    *response*; returns the http object."""
    http = AsyncMock()
    http.patch.return_value = response
    return _stub_pool(client, http)


# ── the interface container: description, admin state, addresses in ONE entry ──


def test_the_interface_container_carries_a_description():
    body = _interface_body([_attr_row(1, "description", "uplink")])

    assert body == {"interface": [{"interface-name": "Gi0/0", "description": "uplink"}]}


@pytest.mark.parametrize("value", ["true", "True"])
def test_enabled_true_spellings_map_to_the_boolean(value):
    """The intent value is a string whose case varies by source; both spellings mean up.

    The check used to be ``value == "True"``, so a stored 'true' became False and a
    deliberately-enabled interface was silently written as disabled.
    """
    body = _interface_body([_attr_row(1, "enabled", value)])

    assert body["interface"][0]["enabled"] is True


@pytest.mark.parametrize("value", ["false", "False"])
def test_enabled_false_spellings_map_to_the_boolean(value):
    body = _interface_body([_attr_row(1, "enabled", value)])

    assert body["interface"][0]["enabled"] is False


def test_a_none_description_is_sent_as_the_empty_string():
    """A cleared description must reach the device as "", never as a JSON null."""
    body = _interface_body([_attr_row(1, "description", None)])

    assert body["interface"][0]["description"] == ""


def test_an_ineligible_attribute_never_reaches_the_wire():
    """Eligibility is FROZEN in the section's proof, so the encoder cannot re-decide it."""
    rows = [_attr_row(1, "description", "uplink"), _attr_row(1, "enabled", "false")]

    body = _interface_body(rows, eligible=frozenset({(1, "description")}))

    assert body["interface"] == [{"interface-name": "Gi0/0", "description": "uplink"}]


def test_an_attribute_with_no_wire_leaf_is_refused_not_dropped():
    """The managed scope is operator DATA, so the store can hold an attribute this writer has
    no leaf for. Emitting the entry without it would stamp the row in_sync for a leaf that
    never reached the device (#26), and under a full-document PUT nothing else would say so.
    """
    rows = [_attr_row(1, "description", "uplink"), _attr_row(1, "mtu", "1500")]

    with pytest.raises(NsoApplyError) as exc_info:
        _interface_body(rows)

    assert exc_info.value.code == "unsupported_attribute"
    assert exc_info.value.detail == {"interface": "Gi0/0", "attribute": "mtu"}


def test_a_corrupt_enabled_value_raises_rather_than_shutting_the_interface():
    """A malformed `enabled` value must raise, never silently coerce to False."""
    for bad in (None, "yes"):
        with pytest.raises(NsoApplyError, match="enabled"):
            _interface_body([_attr_row(1, "enabled", bad)])


def test_the_interface_entry_merges_attributes_and_addresses():
    """Attributes and addresses ride ONE keyed entry: two entries would collide on the key."""
    ip = SimpleNamespace(interface_id=1, address="10.0.0.1/24", family="ipv4", secondary=False, vrf="")

    body = _interface_body([_attr_row(1, "description", "uplink")], [ip])

    assert body["interface"] == [
        {
            "interface-name": "Gi0/0",
            "description": "uplink",
            "ipv4-address": [{"address": "10.0.0.1", "prefix-length": 24, "secondary": False}],
        }
    ]


def test_the_interface_entry_carries_the_nokia_routed_context():
    """Nokia routed metadata targets the router/service interface, not the port."""
    iface = _iface(1, "CRPD-VPN:LO7", kind="logical", service="CRPD-VPN", vrf="CRPD-VPN", parent_binding="lag-99")
    ip = SimpleNamespace(interface_id=1, address="7.7.7.7/32", family="ipv4", secondary=False, vrf="CRPD-VPN")

    entry = _interface_body([], [ip], interfaces=[iface])["interface"][0]

    assert entry["kind"] == "vprn"
    assert entry["service"] == "CRPD-VPN"
    assert entry["parent-binding"] == "lag-99"
    assert entry["ipv4-address"] == [{"address": "7.7.7.7", "prefix-length": 32, "secondary": False}]


def test_a_non_nokia_interface_entry_omits_the_routed_fields():
    """IOS/Junos interfaces carry no Nokia routed-interface context."""
    ip = SimpleNamespace(interface_id=1, address="10.0.0.1/24", family="ipv4", secondary=False, vrf="")

    entry = _interface_body([], [ip])["interface"][0]

    assert "kind" not in entry and "parent-binding" not in entry


class _SapRow:
    def __init__(self, service_name, service_type, sap_id, port="", outer_tag=None, inner_tag=None):
        self.service_name = service_name
        self.service_type = service_type
        self.sap_id = sap_id
        self.port = port
        self.outer_tag = outer_tag
        self.inner_tag = inner_tag


def test_the_l2_sap_container_carries_the_sap_list():
    """The l2-sap container is keyed by (service-name, sap-id) with the tags it owns."""
    rows = [
        _SapRow("TL", "epipe", "lag-60:3999", port="lag-60", outer_tag=3999),
        _SapRow("701", "vpls", "1/1/c31/3:701.10", port="1/1/c31/3", outer_tag=701, inner_tag=10),
    ]

    body = encode_l2_sap({"l2_sap_intent": rows}, _PLAIN)

    saps = {s["sap-id"]: s for s in body["sap"]}
    assert saps["lag-60:3999"]["service-type"] == "epipe"
    assert saps["lag-60:3999"]["outer-tag"] == 3999
    assert "inner-tag" not in saps["lag-60:3999"]
    assert saps["1/1/c31/3:701.10"]["inner-tag"] == 10


@pytest.mark.asyncio
async def test_a_rejected_commit_raises_with_the_put_code():
    """A non-2xx on the document PUT surfaces as a structured error, body attached."""
    client = _make_nso_client()
    mock_http = AsyncMock()
    mock_http.put.return_value = _httpx_response(409, json_data={"error": {}})
    _stub_pool(client, mock_http)

    with pytest.raises(NsoApplyError) as exc_info:
        await apply_device_intent(client, "ra1", {"l2-sap": {"sap": []}})
    assert exc_info.value.code == "nso_put_failed"


@pytest.mark.asyncio
async def test_a_rejected_commit_with_a_non_json_body_still_raises():
    """NSO can answer a 4xx with plain text; the error must carry it rather than blow up."""
    client = _make_nso_client()
    mock_http = AsyncMock()
    mock_http.put.return_value = _httpx_response(400)
    _stub_pool(client, mock_http)

    with pytest.raises(NsoApplyError) as exc_info:
        await apply_device_intent(client, "ra1", {"l2-sap": {"sap": []}})
    assert exc_info.value.code == "nso_put_failed"
    assert exc_info.value.detail["nso_error"] == {"raw": "error body"}


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


# ── build_interface_ip_body (the address half of the interface entry) ────────


def _make_ip_row(address: str, family: str = "ipv4", secondary: bool = False, vrf: str = "") -> SimpleNamespace:
    # A plain record stand-in — the builder reads .address/.family/.secondary/.vrf.
    # SimpleNamespace does not fabricate attributes, so a renamed field surfaces as AttributeError.
    return SimpleNamespace(address=address, family=family, secondary=secondary, vrf=vrf)


def test_interface_ip_body_ipv4_primary():
    entry = build_interface_ip_body("GigabitEthernet0/1", [_make_ip_row("10.0.0.1/24")])

    assert entry["interface-name"] == "GigabitEthernet0/1"
    assert entry["ipv4-address"] == [{"address": "10.0.0.1", "prefix-length": 24, "secondary": False}]
    assert "vrf" not in entry


def test_interface_ip_body_ipv6():
    entry = build_interface_ip_body("GigabitEthernet0/2", [_make_ip_row("2001:db8::1/64", family="ipv6")])

    assert entry["ipv6-address"] == [{"address": "2001:db8::1", "prefix-length": 64}]
    assert "ipv4-address" not in entry


def test_interface_ip_body_sets_vrf():
    """VRF is an interface-level concept: the first non-empty row value binds the entry."""
    entry = build_interface_ip_body("GigabitEthernet0/3", [_make_ip_row("10.1.1.1/30", vrf="MGMT")])

    assert entry["vrf"] == "MGMT"


def test_interface_ip_body_carries_the_nokia_routed_context():
    entry = build_interface_ip_body(
        "CRPD-VPN:LO7",
        [_make_ip_row("7.7.7.7/32", vrf="CRPD-VPN")],
        kind="vprn",
        service="CRPD-VPN",
        parent_binding="lag-99",
        encap_tag="10",
    )

    assert entry["kind"] == "vprn"
    assert entry["service"] == "CRPD-VPN"
    assert entry["parent-binding"] == "lag-99"
    assert entry["encap-tag"] == "10"


def test_interface_ip_body_without_a_kind_omits_the_routed_fields():
    entry = build_interface_ip_body("GigabitEthernet0/1", [_make_ip_row("10.0.0.1/24")])

    assert "kind" not in entry and "parent-binding" not in entry


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

    def test_native_not_a_dict_is_inconclusive(self):
        # a non-empty, non-dict `native` is a shape we can't parse → inconclusive (None)
        assert _device_delta_from_dry_run({"dry-run-result": {"native": "weird"}}, "sw03") is None

    def test_native_device_not_a_list_is_inconclusive(self):
        assert _device_delta_from_dry_run({"dry-run-result": {"native": {"device": "nope"}}}, "sw03") is None


@pytest.mark.asyncio
async def test_verify_raises_on_nonempty_delta():
    """A non-empty native device delta after apply is a false success → raise."""
    client = _make_nso_client()
    body = {
        "dry-run-result": {"native": {"device": [{"name": "sw03", "data": "ip route 1.0.0.0 255.0.0.0 2.2.2.2 1\n"}]}}
    }
    _mock_http_ctx(client, _httpx_response(200, json_data=body))

    with pytest.raises(NsoApplyError) as exc_info:
        await _verify_native_or_raise(client, "http://nso/x", "{}", "sw03", scope="static_route")
    assert exc_info.value.code == "verify_mismatch"
    assert "ip route" in exc_info.value.detail["device_delta"]


@pytest.mark.asyncio
async def test_verify_passes_on_empty_delta():
    client = _make_nso_client()
    _mock_http_ctx(client, _httpx_response(200, json_data={"dry-run-result": {"native": {}}}))
    await _verify_native_or_raise(client, "http://nso/x", "{}", "sw03", scope="vlan")  # no raise


@pytest.mark.asyncio
async def test_verify_inconclusive_does_not_raise():
    """Unexpected/garbage dry-run body is fail-safe (no raise, apply stands)."""
    client = _make_nso_client()
    _mock_http_ctx(client, _httpx_response(200, json_data={"weird": 1}))
    await _verify_native_or_raise(client, "http://nso/x", "{}", "sw03", scope="vlan")  # no raise


@pytest.mark.asyncio
async def test_verify_raises_on_conclusive_4xx_rejection():
    """A 4xx on the post-apply verify dry-run is a CONCLUSIVE device rejection, not an
    inconclusive blip: the apply must be reported failed (not a silent false success),
    and the RESTCONF error body carried on the raise."""
    client = _make_nso_client()
    err_body = {"ietf-restconf:errors": {"error": [{"error-message": "unknown command"}]}}
    _mock_http_ctx(client, _httpx_response(400, json_data=err_body))
    with pytest.raises(NsoApplyError) as exc_info:
        await _verify_native_or_raise(client, "http://nso/x", "{}", "sw03", scope="route_policy")
    assert exc_info.value.detail.get("nso_error") == err_body


@pytest.mark.asyncio
async def test_verify_inconclusive_on_5xx_does_not_raise():
    """A 5xx / transport error on the verify dry-run stays fail-safe (inconclusive, no
    raise) — only a conclusive 4xx fails the apply."""
    client = _make_nso_client()
    _mock_http_ctx(client, _httpx_response(503))
    await _verify_native_or_raise(client, "http://nso/x", "{}", "sw03", scope="vlan")  # no raise


@pytest.mark.asyncio
async def test_native_dry_run_non_strict_returns_none_on_4xx():
    """The preview/localize callers (strict=False) still treat a 4xx as inconclusive None —
    only the verify path opts into raising."""
    from nso_adapter.nso.apply import native_dry_run

    client = _make_nso_client()
    _mock_http_ctx(client, _httpx_response(400, json_data={"ietf-restconf:errors": {}}))
    assert await native_dry_run(client, "http://nso/x", "{}", "sw03") is None


@pytest.mark.asyncio
async def test_verify_disabled_by_toggle(monkeypatch):
    """When VERIFY_AFTER_APPLY is off, no dry-run call is made."""
    monkeypatch.setattr(apply_mod, "VERIFY_AFTER_APPLY", False)
    client = _make_nso_client()
    _mock_http_ctx(client, _httpx_response(200))
    await _verify_native_or_raise(client, "http://nso/x", "{}", "sw03", scope="vlan")
    client._client.assert_not_called()


@pytest.mark.asyncio
async def test_a_committed_document_that_did_not_land_raises_verify_mismatch():
    """End-to-end: the PUT succeeds (204) but the verify dry-run still shows a delta."""
    client = _make_nso_client()
    mock_http = AsyncMock()
    mock_http.put.side_effect = [
        _httpx_response(204),
        _httpx_response(
            200, json_data={"dry-run-result": {"native": {"device": [{"name": "sw03", "data": "ip route ...\n"}]}}}
        ),
    ]
    _stub_pool(client, mock_http)

    row = SimpleNamespace(vrf="", prefix="100.64.0.0/10", next_hop="172.16.0.1", metric=1, permanent=False, tag=None)
    body = encode_static_route({"static_route_intent": [row]}, _PLAIN)
    with pytest.raises(NsoApplyError) as exc_info:
        await apply_device_intent(client, "sw03", {"static-route": body})
    assert exc_info.value.code == "verify_mismatch"
    # First call is the real PUT (no dry-run), second is the verify dry-run, same method.
    assert "dry-run" not in mock_http.put.call_args_list[0][0][0]
    assert "dry-run=native" in mock_http.put.call_args_list[1][0][0]


def _bgp_body(routers, redistribution=()):
    return encode_bgp({"bgp_router_intent": list(routers), "redistribution_intent": list(redistribution)}, _PLAIN)


def test_the_bgp_container_uses_the_yang_key_spellings():
    """Regression (finding #31): the BGP YANG uses `afi` (not `af`) and
    `peer-address-family` (not `address-family`) under each peer. Sending the wrong keys
    caused a live 400 `unknown element: address-family` on rg03's IOS BGP."""
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
        source=None,
        peer_address_families=[paf],
    )
    scope = SimpleNamespace(vrf="", address_families=[SimpleNamespace(af="ipv4-unicast")], peers=[peer])
    router = SimpleNamespace(asn=65100, router_id=None, scopes=[scope])

    scope_out = _bgp_body([router])["router"][0]["scope"][0]

    # scope-level AF list key is `afi`
    assert scope_out["address-family"][0]["afi"] == "ipv4-unicast"
    assert "af" not in scope_out["address-family"][0]
    # per-peer AF list is `peer-address-family` keyed by `afi`
    peer_out = scope_out["peer"][0]
    assert "address-family" not in peer_out
    assert peer_out["peer-address-family"][0]["afi"] == "ipv4-unicast"


def test_the_bgp_container_carries_the_peer_source():
    """A peer's `source` (update-source iface / local-address IP) rides `peer/source`.

    Without it the session source was never sent even though the reader exported it and the
    adapter stored it: the BGP session source was silently un-pushable.
    """
    peer = SimpleNamespace(
        peer_address="192.0.2.7",
        enabled=True,
        peer_group=None,
        remote_as=65100,
        local_as=None,
        ttl=None,
        password=None,
        source="Loopback0",
        peer_address_families=[],
    )
    scope = SimpleNamespace(vrf="", address_families=[], peers=[peer])
    router = SimpleNamespace(asn=65100, router_id=None, scopes=[scope])

    peer_out = _bgp_body([router])["router"][0]["scope"][0]["peer"][0]

    assert peer_out["source"] == "Loopback0"


# ── build_isis_process_payload (pure builder; tested against real ORM rows so a
#    renamed model column breaks the test instead of silently passing) ──────────


def test_build_isis_process_payload_empty_inputs():
    """None / empty row lists yield an empty process-config list."""
    assert build_isis_process_payload(None, None, None) == []
    assert build_isis_process_payload([], [], []) == []


def test_build_isis_process_payload_attaches_flex_algo():
    """Flex-algo rows attach under their process-tag, creating a minimal process
    entry when the tag has no process row (e.g. IOS-XR flex-only)."""
    fa = IsisFlexAlgoIntent(
        process_tag="NA4-CORE",
        algo_id=130,
        metric_type="delay-metric",
        priority=200,
        admin_group_exclude="RED",
    )
    procs = build_isis_process_payload(isis_process_rows=[], redistribution_rows=[], flex_algo_rows=[fa])
    assert len(procs) == 1
    p = procs[0]
    assert p["process-tag"] == "NA4-CORE"
    assert p["flex-algo"][0] == {
        "algo-id": 130,
        "metric-type": "delay-metric",
        "priority": 200,
        "admin-group-exclude": "RED",
    }


def test_build_isis_process_payload_flex_algo_attaches_to_existing_process():
    """A flex-algo whose tag already has a process row attaches to that entry (no duplicate),
    and the include-any/include-all groups emit while a None priority is omitted."""
    proc = IsisProcessIntent(process_tag="0", net="49.0001.00")
    fa = IsisFlexAlgoIntent(
        process_tag="0",
        algo_id=128,
        admin_group_include_any="BLUE",
        admin_group_include_all="GREEN",
    )
    procs = build_isis_process_payload([proc], [], [fa])
    assert len(procs) == 1  # attached, not duplicated
    assert procs[0]["net"] == "49.0001.00"
    fa_out = procs[0]["flex-algo"][0]
    assert fa_out == {"algo-id": 128, "admin-group-include-any": "BLUE", "admin-group-include-all": "GREEN"}
    assert "priority" not in fa_out  # None priority omitted


def test_build_isis_process_payload_attaches_levels():
    """Per-level rows attach under their process-tag as the YANG ``level`` list
    (None fields omitted), creating a minimal process entry when the tag has no
    process row — the flex-algo orphan parity, so a level row is never dropped."""
    proc = IsisProcessIntent(process_tag="0", net="49.0001.00")
    lv2 = IsisLevelIntent(process_tag="0", level=2, wide_metrics_only=True, labeled_preference=7)
    lv_orphan = IsisLevelIntent(process_tag="EDGE", level=1, disabled=True)
    procs = build_isis_process_payload([proc], [], [], level_rows=[lv2, lv_orphan])
    by_tag = {p["process-tag"]: p for p in procs}
    assert by_tag["0"]["level"] == [{"level": 2, "wide-metrics-only": True, "labeled-preference": 7}]
    assert by_tag["EDGE"]["level"] == [{"level": 1, "disabled": True}]
    assert "net" not in by_tag["EDGE"]  # minimal synthesized entry


def test_build_isis_process_payload_omits_empty_enums():
    """Empty-string enum leaves (metric-style/is-type) are omitted, not sent as ''."""
    row = IsisProcessIntent(process_tag="0", net="49.0001.00", is_type="", metric_style="")
    procs = build_isis_process_payload(isis_process_rows=[row], redistribution_rows=[], flex_algo_rows=[])
    assert "metric-style" not in procs[0]
    assert "is-type" not in procs[0]
    assert procs[0]["net"] == "49.0001.00"


def test_build_isis_process_payload_normalises_is_type():
    """is-type 'level-2' (invalid enum, same YANG type as circuit-type) → 'level-2-only'.

    The plugin stores is_type as free text; the reconciler's from_canonical rejects
    'level-2' (CodecError → the leaf is silently skipped), so normalise it here like
    circuit_type is on the interface payload."""
    row = IsisProcessIntent(process_tag="0", net="49.0001.00", is_type="level-2")
    procs = build_isis_process_payload([row])
    assert procs[0]["is-type"] == "level-2-only"


def test_build_isis_process_payload_full_process_fields():
    """Every populated process leaf (net/is-type/metric-style/overload/area+domain auth) emits."""
    row = IsisProcessIntent(
        process_tag="CORE",
        net="49.0001.0000.0000.0001.00",
        is_type="level-2-only",
        metric_style="wide",
        overload_bit=True,
        area_auth_type="md5",
        area_auth_key="area-secret",
        domain_auth_type="md5",
        domain_auth_key="domain-secret",
    )
    procs = build_isis_process_payload([row])
    assert procs[0] == {
        "process-tag": "CORE",
        "net": "49.0001.0000.0000.0001.00",
        "is-type": "level-2-only",
        "metric-style": "wide",
        "overload-bit": True,
        "area-auth-type": "md5",
        "area-auth-key": "area-secret",
        "domain-auth-type": "md5",
        "domain-auth-key": "domain-secret",
    }


def test_build_isis_process_payload_overload_bit_false_is_emitted():
    """overload-bit=False is still sent — the guard is `is not None`, not truthiness."""
    row = IsisProcessIntent(process_tag="0", overload_bit=False)
    procs = build_isis_process_payload([row])
    assert procs[0]["overload-bit"] is False


def test_build_isis_process_payload_auth_type_without_key():
    """An auth type set with no key emits the type and omits the key (nested guard)."""
    row = IsisProcessIntent(process_tag="0", area_auth_type="clear-text", domain_auth_type="md5")
    procs = build_isis_process_payload([row])
    assert procs[0]["area-auth-type"] == "clear-text"
    assert "area-auth-key" not in procs[0]
    assert procs[0]["domain-auth-type"] == "md5"
    assert "domain-auth-key" not in procs[0]


def test_build_isis_process_payload_nests_redistribute():
    """Redistribution rows nest under their dest process-tag; optional route-map / metric /
    metric-type are emitted only when set (the per-row optional branches)."""
    proc = IsisProcessIntent(process_tag="0")
    full = RedistributionIntent(
        dest_protocol="isis",
        dest_ref="0",
        source_protocol="bgp",
        source_ref="65000",
        route_map="RM",
        metric=100,
        metric_type="external",
    )
    minimal = RedistributionIntent(
        dest_protocol="isis", dest_ref="0", source_protocol="connected", source_ref="", route_map=None, metric=None
    )
    procs = build_isis_process_payload(isis_process_rows=[proc], redistribution_rows=[full, minimal], flex_algo_rows=[])

    redist = procs[0]["redistribute"]
    assert redist[0] == {
        "source-protocol": "bgp",
        "source-ref": "65000",
        "route-map": "RM",
        "metric": 100,
        "metric-type": "external",
    }
    assert redist[1] == {"source-protocol": "connected", "source-ref": ""}  # optionals omitted


def test_build_isis_process_payload_orphan_redistribute_synthesizes_process():
    """A redistribute row whose destination process has NO process row in this apply
    (parent already applied cleanly → filtered out) must still land: synthesize a
    minimal process-config entry rather than silently dropping the redistribute."""
    redist = RedistributionIntent(
        dest_protocol="isis", dest_ref="0", source_protocol="connected", source_ref="", route_map=None, metric=None
    )
    procs = build_isis_process_payload(isis_process_rows=[], redistribution_rows=[redist], flex_algo_rows=[])
    assert len(procs) == 1
    assert procs[0]["process-tag"] == "0"
    assert procs[0]["redistribute"] == [{"source-protocol": "connected", "source-ref": ""}]


def test_ospf_orphan_redistribute_synthesizes_a_process():
    """A redistribute row with no eligible process row still lands via a synthesized
    minimal process-config entry (parity with IS-IS)."""
    redist = RedistributionIntent(
        dest_protocol="ospf", dest_ref="1", source_protocol="connected", source_ref="", route_map=None, metric=None
    )

    body = encode_ospf(
        {"ospf_instance_intent": [], "ospf_interface_intent": [], "redistribution_intent": [redist]}, _PLAIN
    )

    procs = body["process-config"]
    assert [p["process-id"] for p in procs] == ["1"]
    assert procs[0]["redistribute"] == [{"source-protocol": "connected", "source-ref": ""}]


def test_ospf_omits_an_empty_interface_config():
    """A process-only OSPF body must NOT carry `interface-config: []` — on a keyed list that
    empty array can be read as 'replace with empty', over-deleting the device's OSPF
    interfaces (IS-IS omits the list when empty; OSPF must match)."""
    proc = OspfInstanceIntent(process_id="1", vrf="", enabled=True)

    body = encode_ospf(
        {"ospf_instance_intent": [proc], "ospf_interface_intent": [], "redistribution_intent": []}, _PLAIN
    )

    assert "interface-config" not in body
    assert body["process-config"][0]["process-id"] == "1"


def test_bgp_orphan_redistribute_synthesizes_a_router():
    """A redistribute row whose router/scope/AF is not in the document still lands via a
    synthesized router→scope→address-family skeleton carrying just the redistribute."""
    redist = RedistributionIntent(
        dest_protocol="bgp",
        dest_ref="65001:default:ipv4-unicast",
        source_protocol="connected",
        source_ref="",
        route_map=None,
        metric=None,
    )

    routers = _bgp_body([], [redist])["router"]

    assert len(routers) == 1
    assert routers[0]["asn"] == 65001
    scope = routers[0]["scope"][0]
    assert scope["vrf"] == "default"
    af = scope["address-family"][0]
    assert af["afi"] == "ipv4-unicast"
    assert af["redistribute"] == [{"source-protocol": "connected", "source-ref": ""}]


def test_bgp_asn_asdot_notation_round_trips():
    """A 4-byte ASN in asdot notation ('1.100') must round-trip to its uint32 value, not
    crash the whole document with a bare int() ValueError."""
    routers = _bgp_body([BgpRouterIntent(asn="1.100")])["router"]

    assert routers[0]["asn"] == 1 * 65536 + 100


def test_bgp_sends_an_accepted_router_id():
    """An accepted global router-id is emitted as the `router-id` leaf (sibling of asn)."""
    router_out = _bgp_body([BgpRouterIntent(asn="65100", router_id="10.255.0.1")])["router"][0]

    assert router_out["router-id"] == "10.255.0.1"


def test_bgp_omits_the_router_id_when_unset():
    """No accepted router-id → the `router-id` leaf is omitted (never an empty string)."""
    router_out = _bgp_body([BgpRouterIntent(asn="65100")])["router"][0]

    assert "router-id" not in router_out


def test_bgp_invalid_asn_raises_a_clean_error():
    """A non-numeric ASN raises a descriptive NsoApplyError, not an opaque ValueError."""
    with pytest.raises(NsoApplyError, match="ASN"):
        _bgp_body([BgpRouterIntent(asn="not-an-asn")])


def test_build_interface_ip_entry_rejects_address_without_prefix():
    """An address missing '/prefix' raises a descriptive NsoApplyError (surfaced), not a
    bare ValueError that would abort the whole atomic apply opaquely."""
    row = SimpleNamespace(address="10.0.0.1", family="ipv4", vrf=None, secondary=False)
    with pytest.raises(NsoApplyError, match="prefix"):
        build_interface_ip_entry("d", "Gi0/0", [row])


def test_build_interface_ip_entry_rejects_unknown_family():
    """A row whose family is neither ipv4 nor ipv6 is NOT silently dropped — it raises so
    the address can never be reported in_sync while never emitted."""
    row = SimpleNamespace(address="10.0.0.1/24", family="inet", vrf=None, secondary=False)
    with pytest.raises(NsoApplyError, match="family"):
        build_interface_ip_entry("d", "Gi0/0", [row])


def test_build_interface_ip_entry_secondary_none_is_boolean_not_null():
    """A None `secondary` becomes JSON false, never null (a boolean YANG leaf rejects null)."""
    row = SimpleNamespace(address="10.0.0.1/24", family="ipv4", vrf=None, secondary=None)
    entry = build_interface_ip_entry("d", "Gi0/0", [row])
    assert entry["ipv4-address"][0]["secondary"] is False


def test_normalize_route_map_entry_collision_is_deterministic():
    """When both the canonical YANG key and a legacy spelling map to the same leaf with
    DIFFERENT values, the canonical key wins deterministically (never dict-order roulette)."""
    entry = {
        "sequence": 10,
        "action": "permit",
        "match-json": '{"canonical": true}',
        "match": {"legacy": 1},  # both map to match-json
    }
    out = apply_mod._normalize_route_map_entry(entry)
    assert out["match-json"] == '{"canonical": true}'


# ── OSPF process / interface entry builders (pure; real ORM rows, no mocks) ─────


def test_ospf_process_entry_full_fields_and_redistribute():
    """A populated process row emits process-id/router-id/vrf/enabled and nests redistribute."""
    row = OspfInstanceIntent(process_id="5", router_id="2.2.2.2", vrf="RED", enabled=True)
    redist = [{"source-protocol": "connected", "source-ref": ""}]
    assert _ospf_process_entry(row, redist) == {
        "process-id": "5",
        "router-id": "2.2.2.2",
        "vrf": "RED",
        "enabled": True,
        "redistribute": redist,
    }


def test_ospf_process_entry_named_process_id_round_trips():
    """A named (non-numeric) IOS-XR process-id is emitted verbatim as a string.

    The ospf-reconciler YANG leaf is a string; int() would have crashed here. This is
    the adapter half of the named-process round-trip (e.g. 'router ospf test').
    """
    row = OspfInstanceIntent(process_id="test", router_id="1.1.1.1", vrf="")
    assert _ospf_process_entry(row, [])["process-id"] == "test"


def test_ospf_process_entry_minimal_defaults_enabled_true():
    """Empty router-id/vrf and unset `enabled` → delete-guard defaults enabled True, no redistribute."""
    row = OspfInstanceIntent(process_id="1", router_id="", vrf="")
    assert _ospf_process_entry(row, []) == {"process-id": "1", "enabled": True}


def test_ospf_process_entry_explicit_disable_is_preserved():
    """enabled=False (operator-down) is preserved, not coerced to the default True."""
    row = OspfInstanceIntent(process_id="1", vrf="", enabled=False)
    assert _ospf_process_entry(row, [])["enabled"] is False


def test_ospf_interface_entry_full_fields():
    """A populated interface row emits every optional leaf (priority/cost/network-type/auth)."""
    row = OspfInterfaceIntent(
        interface_name="Gi0/1",
        process_id="5",
        area_id="0",
        passive=True,
        priority=10,
        cost=100,
        network_type="point-to-point",
        auth_type="md5",
        auth_key="secret",
    )
    assert _ospf_interface_entry(row) == {
        "interface-name": "Gi0/1",
        "process-id": "5",
        "area-id": "0",
        "passive": True,
        "priority": 10,
        "cost": 100,
        "network-type": "point-to-point",
        "auth-type": "md5",
        "auth-key": "secret",
    }


def test_ospf_interface_entry_minimal_omits_optionals():
    """Unset optionals are omitted; passive=None falls back to False."""
    row = OspfInterfaceIntent(interface_name="Gi0/2", process_id="1", area_id="0", passive=None)
    assert _ospf_interface_entry(row) == {
        "interface-name": "Gi0/2",
        "process-id": "1",
        "area-id": "0",
        "passive": False,
    }


def test_ospf_interface_entry_auth_type_without_key():
    """An auth-type set with no key emits the type and omits the key (nested guard)."""
    row = OspfInterfaceIntent(
        interface_name="Gi0/3", process_id="1", area_id="0", passive=False, auth_type="clear-text"
    )
    entry = _ospf_interface_entry(row)
    assert entry["auth-type"] == "clear-text"
    assert "auth-key" not in entry


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


def test_build_isis_interface_payload_emits_bfd_enabled_tri_state():
    """bfd-enabled rides the interface intent: emitted only when asserted (True/False),
    omitted when None (no opinion → NED default, reconcile leaves brownfield BFD alone)."""
    from nso_adapter.nso.apply import build_isis_interface_payload

    base = dict(
        interface_name="Gi0/0",
        af="ipv4",
        process_tag="",
        passive=False,
        circuit_type=None,
        network_type=None,
        metric=None,
    )
    on = build_isis_interface_payload([SimpleNamespace(**base, bfd_enabled=True)])
    off = build_isis_interface_payload([SimpleNamespace(**base, bfd_enabled=False)])
    absent = build_isis_interface_payload([SimpleNamespace(**base, bfd_enabled=None)])
    assert on[0]["bfd-enabled"] is True
    assert off[0]["bfd-enabled"] is False
    assert "bfd-enabled" not in absent[0]


def test_build_isis_interface_payload_emits_frr_tri_state():
    """#83: frr-enabled rides the interface intent like bfd-enabled (tri-state);
    frr-protection is emitted only when non-blank (enum leaves reject '')."""
    from nso_adapter.nso.apply import build_isis_interface_payload

    base = dict(
        interface_name="Bundle-Ether1",
        af="ipv4",
        process_tag="CORE",
        passive=False,
        circuit_type=None,
        network_type=None,
        metric=None,
        bfd_enabled=None,
    )
    on = build_isis_interface_payload([SimpleNamespace(**base, frr_enabled=True, frr_protection="node")])
    off = build_isis_interface_payload([SimpleNamespace(**base, frr_enabled=False, frr_protection="")])
    absent = build_isis_interface_payload([SimpleNamespace(**base, frr_enabled=None, frr_protection=None)])
    assert on[0]["frr-enabled"] is True
    assert on[0]["frr-protection"] == "node"
    assert off[0]["frr-enabled"] is False
    assert "frr-protection" not in off[0]
    assert "frr-enabled" not in absent[0]
    assert "frr-protection" not in absent[0]


def test_build_isis_process_payload_emits_frr():
    """#83: fast-reroute (enum — omit when blank) + microloop-avoidance (tri-state)
    ride the process-config entry."""
    from nso_adapter.nso.apply import build_isis_process_payload

    base = dict(
        process_tag="CORE",
        net=None,
        is_type=None,
        metric_style=None,
        overload_bit=None,
        area_auth_type=None,
        area_auth_key=None,
        domain_auth_type=None,
        domain_auth_key=None,
    )
    on = build_isis_process_payload([SimpleNamespace(**base, fast_reroute="ti-lfa", microloop_avoidance=True)], [], [])
    off = build_isis_process_payload([SimpleNamespace(**base, fast_reroute="", microloop_avoidance=False)], [], [])
    absent = build_isis_process_payload([SimpleNamespace(**base, fast_reroute=None, microloop_avoidance=None)], [], [])
    assert on[0]["fast-reroute"] == "ti-lfa"
    assert on[0]["microloop-avoidance"] is True
    assert "fast-reroute" not in off[0]
    assert off[0]["microloop-avoidance"] is False
    assert "fast-reroute" not in absent[0]
    assert "microloop-avoidance" not in absent[0]


@pytest.mark.asyncio
async def test_the_document_put_targets_the_keyed_instance_with_reconcile():
    """One instance per device, keyed by name, committed in reconcile mode."""
    client = _make_nso_client()
    mock_http = AsyncMock()
    mock_http.put.return_value = _httpx_response(204)
    _stub_pool(client, mock_http)

    await apply_device_intent(client, "sw3", {"vlan": encode_vlan({"vlan_intent": []}, _PLAIN)})

    (url,) = mock_http.put.call_args_list[0][0]
    assert url.split("?")[0].endswith("device-intent:device-intent=sw3")
    assert "reconcile=keep-non-service-config" in url
    mock_http.patch.assert_not_called()  # the document is replaced, never merged


@pytest.mark.asyncio
async def test_a_dropped_row_is_simply_absent_from_the_document():
    """Removal is by OMISSION: the body carries the remaining list and nothing says 'delete'."""
    client = _make_nso_client()
    mock_http = AsyncMock()
    mock_http.put.return_value = _httpx_response(204)
    _stub_pool(client, mock_http)

    rows = [SimpleNamespace(vlan_id=10, name="keep")]  # 3366 dropped → absent from the body
    await apply_device_intent(client, "sw3", {"vlan": encode_vlan({"vlan_intent": rows}, _PLAIN)})

    vids = [v["vlan-id"] for v in _sent_document(client)["vlan"]["vlan"]]
    assert vids == [10]
    assert "dry-run=native" in mock_http.put.call_args_list[1][0][0]  # verify re-issues the PUT


def _community_list(name, entries, ned_id, **kwargs):
    rows = [SimpleNamespace(family="community_list", name=name, entries=entries, **kwargs)]
    body = encode_route_policy({"route_policy_object_intent": rows}, _dialect_for(ned_id))
    return body["community-list"][0]


def test_route_policy_translates_and_skips_members_per_ned():
    """On a Nokia (timos) device the canonical community members are translated to the SR OS
    dialect (incl. an exact ``color:`` → its ``ext:030b:`` hex) and the ones it genuinely
    cannot represent (a regex ``color:``) are dropped from the body — so one bad member
    cannot abort the whole community. The dialect comes from the section's FROZEN context,
    never from the device row.
    """
    entries = [
        {"sequence": 10, "action": "permit", "community": "64500:1234"},
        {"sequence": 20, "action": "permit", "community": "64500:.*"},  # digit-domain regex kept
        {"sequence": 30, "action": "permit", "community": "target:64500:1234"},
        {"sequence": 40, "action": "permit", "community": "large:64500:64501:1234"},  # exact → strip large:
        {"sequence": 50, "action": "permit", "community": "color:0:128"},  # exact color → ext:030b hex
        {"sequence": 60, "action": "permit", "community": "color:0:12."},  # regex color → dropped
        {"sequence": 70, "action": "permit", "community": "no-export"},
    ]

    cl = _community_list("example-comm", entries, "timos-nc-23.10")

    assert [e["community"] for e in cl["entry"]] == [
        "64500:1234",
        "64500:.*",
        "target:64500:1234",
        "64500:64501:1234",
        "ext:030b:000000000080",
        "no-export",
    ]
    assert cl["invert-match"] is False


def test_route_policy_carries_invert_match_and_amp_large_on_nokia():
    """An inverted community-list keeps its invert-match flag, a regex large community is
    rendered in SR OS `&`-separated form, and an exact color: becomes its ext:030b: hex."""
    entries = [
        {"sequence": 10, "action": "permit", "community": "no-export"},
        {"sequence": 20, "action": "permit", "community": "64500:21000"},
        {"sequence": 30, "action": "permit", "community": "large:64500:.*:[0-4]"},  # regex large → &
        {"sequence": 40, "action": "permit", "community": "color:0:128"},  # exact color → ext:030b hex
    ]

    cl = _community_list("SCRUBBER", entries, "timos-nc-23.10", invert_match=True)

    assert cl["invert-match"] is True
    assert [e["community"] for e in cl["entry"]] == [
        "no-export",
        "64500:21000",
        "64500&.*&[0-4]",
        "ext:030b:000000000080",
    ]


def test_route_policy_keeps_all_members_on_an_identity_ned():
    """A NED with no dialect override (IOS-XR here) keeps every member verbatim —
    ``color:`` is a valid Cisco extcommunity, so nothing is dropped."""
    entries = [
        {"sequence": 10, "action": "permit", "community": "color:0:128"},
        {"sequence": 20, "action": "permit", "community": "large:64500:64501:.*"},
    ]

    cl = _community_list("example-comm", entries, "cisco-iosxr-cli-7.76")

    assert [e["community"] for e in cl["entry"]] == ["color:0:128", "large:64500:64501:.*"]  # untouched


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


def test_ospf_always_asserts_enabled_as_a_delete_guard():
    """The OSPF process body ALWAYS carries `enabled`, even when the intent row leaves it
    None, so the document PUT can never drop admin-state and disable OSPF. Default is True;
    an explicit False is preserved. One body serves apply and removal alike now, so the
    guard holds on both.
    """
    rows = [
        SimpleNamespace(process_id="1", router_id="10.0.0.1", vrf="", enabled=None),
        SimpleNamespace(process_id="2", router_id="10.0.0.2", vrf="", enabled=False),
    ]

    body = encode_ospf({"ospf_instance_intent": rows, "ospf_interface_intent": [], "redistribution_intent": []}, _PLAIN)

    by_pid = {p["process-id"]: p for p in body["process-config"]}
    assert by_pid["1"]["enabled"] is True  # None → default enable (guard)
    assert by_pid["2"]["enabled"] is False  # explicit disable preserved


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
async def test_the_real_commit_carries_reconcile_and_the_verify_dry_run_does_too(monkeypatch):
    """The commit PUTs with reconcile; the post-apply verify dry-run carries both
    dry-run=native and reconcile so the preview matches the commit."""
    monkeypatch.setattr(apply_mod, "RECONCILE_COMMIT", "keep-non-service-config")
    client = _make_nso_client()
    mock_http = AsyncMock()
    mock_http.put.return_value = _httpx_response(204)
    _stub_pool(client, mock_http)

    await apply_device_intent(client, "core-rtr-01", {"vlan": {"vlan": []}})

    real_url = mock_http.put.call_args_list[0][0][0]
    verify_url = mock_http.put.call_args_list[1][0][0]
    # Real commit: reconcile present, NOT a dry-run.
    assert "reconcile=keep-non-service-config" in real_url
    assert "dry-run" not in real_url
    # Verify dry-run: both params.
    assert "dry-run=native" in verify_url
    assert "reconcile=keep-non-service-config" in verify_url


# ── apply_device_intent (one instance, one transaction, one device commit) ─────


@pytest.mark.asyncio
async def test_the_sender_puts_every_family_in_one_request():
    """Every family is a key of ONE instance: one NSO transaction, one device commit, so
    FASTMAP resolves the cross-family dependencies inside it."""
    import json

    client = _make_nso_client()
    mock_http = AsyncMock()
    mock_http.put.return_value = _httpx_response(204)
    _stub_pool(client, mock_http)

    containers = {
        "subinterface": {"interface": [{"interface-name": "ae99.999"}]},
        "interface": {"interface": [{"interface-name": "ae99.999"}]},
    }
    await apply_device_intent(client, "sw01", containers)

    url = mock_http.put.call_args_list[0][0][0]
    assert url.split("?")[0].endswith("device-intent:device-intent=sw01")
    body = json.loads(mock_http.put.call_args_list[0].kwargs["content"])
    assert set(body[DEVICE_INTENT_ROOT][0]) == {"device", "subinterface", "interface"}


@pytest.mark.asyncio
async def test_the_sender_requests_the_cli_outformat_for_a_preview():
    """dry_run="cli" must reach the wire as the cli dry-run query, not the native default."""
    client = _make_nso_client()
    mock_http = AsyncMock()
    mock_http.put.return_value = _httpx_response(200, json_data={"dry-run-result": {"cli": ""}})
    _stub_pool(client, mock_http)

    await apply_device_intent(client, "sw01", {"interface": {}}, dry_run="cli")

    url = mock_http.put.call_args_list[0][0][0]
    assert url == apply_mod._commit_url(
        f"{client._base}{apply_mod.DEVICE_INTENT_PATH}=sw01",
        dry_run="cli",
    )


@pytest.mark.asyncio
async def test_an_empty_family_body_reaches_the_wire_unchanged():
    """An empty container and an absent one both mean "this family owns nothing" (#1522 D2).

    Which families the document carries is the registry walk's decision; a sender that
    dropped an empty one would no longer transmit the document the generation froze.
    """
    import json

    client = _make_nso_client()
    mock_http = AsyncMock()
    mock_http.put.return_value = _httpx_response(204)
    _stub_pool(client, mock_http)

    await apply_device_intent(client, "sw01", {"interface": {"interface": []}, "subinterface": {}})

    body = json.loads(mock_http.put.call_args_list[0].kwargs["content"])
    assert body[DEVICE_INTENT_ROOT][0] == {
        "device": "sw01",
        "interface": {"interface": []},
        "subinterface": {},
    }


@pytest.mark.asyncio
async def test_the_sender_dry_run_returns_the_delta_and_commits_nothing():
    client = _make_nso_client()
    mock_http = AsyncMock()
    mock_http.put.return_value = _httpx_response(
        200, json_data={"dry-run-result": {"native": {"device": [{"name": "sw01", "data": "X"}]}}}
    )
    _stub_pool(client, mock_http)

    delta = await apply_device_intent(client, "sw01", {"interface": {}}, dry_run=True)

    assert delta == "X"
    assert all("dry-run=native" in call[0][0] for call in mock_http.put.call_args_list)


@pytest.mark.asyncio
async def test_the_sender_raises_on_a_rejected_document():
    """A non-2xx commit raises (all-or-nothing surfaces: nothing landed)."""
    client = _make_nso_client()
    mock_http = AsyncMock()
    mock_http.put.return_value = _httpx_response(500)
    _stub_pool(client, mock_http)

    with pytest.raises(NsoApplyError):
        await apply_device_intent(client, "sw01", {"interface": {}})


def test_build_subif_interfaces_shapes_rows():
    rows = [
        SimpleNamespace(
            interface_name="ae99.999", parent_interface="ae99", dot1q_vlan=999, sub_type="subinterface", vrf=""
        )
    ]
    out = apply_mod.build_subif_interfaces(rows)
    assert out == [
        {"interface-name": "ae99.999", "parent-interface": "ae99", "dot1q-vlan": 999, "type": "subinterface"}
    ]


def test_build_interface_ip_entry_ipv4():
    rows = [_make_ip_row("198.18.1.1/24", family="ipv4")]
    entry = apply_mod.build_interface_ip_entry("sw01", "ae99.999", rows)
    assert entry["interface-name"] == "ae99.999"
    assert entry["ipv4-address"] == [{"address": "198.18.1.1", "prefix-length": 24, "secondary": False}]


# ── cli-outformat dry-run (the diff -u apply preview) ─────────────────────────


def test_commit_url_dry_run_cli_format():
    """dry_run='cli' asks NSO for the NED-uniform +/- tree diff instead of native."""
    url = apply_mod._commit_url("http://nso/restconf/data/x", dry_run="cli")
    assert "dry-run=cli" in url
    assert "dry-run=native" not in url


def test_commit_url_dry_run_true_stays_native():
    url = apply_mod._commit_url("http://nso/restconf/data/x", dry_run=True)
    assert "dry-run=native" in url


def test_cli_delta_from_dry_run_shapes():
    good = {"dry-run-result": {"cli": {"local-node": {"data": " devices {\n +config\n }"}}}}
    assert apply_mod._cli_delta_from_dry_run(good) == " devices {\n +config\n }"
    assert apply_mod._cli_delta_from_dry_run({"dry-run-result": {}}) == ""  # no change
    assert apply_mod._cli_delta_from_dry_run({"dry-run-result": {"cli": {}}}) == ""
    assert apply_mod._cli_delta_from_dry_run({"unexpected": 1}) is None
    assert apply_mod._cli_delta_from_dry_run("nonsense") is None


@pytest.mark.asyncio
async def test_native_dry_run_outformat_cli_requests_and_parses():
    """outformat='cli' issues ?dry-run=cli and returns the cli tree-diff text."""
    from nso_adapter.nso.apply import native_dry_run

    client = _make_nso_client()
    body = {"dry-run-result": {"cli": {"local-node": {"data": "+ isis bfd"}}}}
    http = AsyncMock()
    http.patch.return_value = _httpx_response(200, json_data=body)
    _stub_pool(client, http)
    delta = await native_dry_run(client, "http://nso/x", "{}", "sw03", outformat="cli")
    assert delta == "+ isis bfd"
    url = http.patch.await_args.args[0]
    assert "dry-run=cli" in url


@pytest.mark.asyncio
async def test_the_document_preview_honours_the_cli_outformat():
    """collect_apply_diff threads dry_run='cli' into the sender, and diff2html parses NSO's
    NED-uniform tree diff: handing it raw device CLI lines rendered the panel blank."""
    client = _make_nso_client()
    body = {"dry-run-result": {"cli": {"local-node": {"data": "+ description uplink"}}}}
    http = AsyncMock()
    http.put.return_value = _httpx_response(200, json_data=body)
    _stub_pool(client, http)

    delta = await apply_device_intent(client, "sw03", {"interface": {}}, dry_run="cli")

    url = http.put.await_args.args[0]
    assert "dry-run=cli" in url, "the preview must ask NSO for the NED-uniform tree diff"
    assert "dry-run=native" not in url
    assert delta == "+ description uplink"
