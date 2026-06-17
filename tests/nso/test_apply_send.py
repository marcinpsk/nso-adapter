# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <mazieba@libertyglobal.com>
"""The shared apply send/verify tail (native_dry_run / _send_service_config / verify).

Every apply_* write flows through this machinery, so covering it once via a real httpx
MockTransport (a boundary fake — the actual NsoClient + apply code run for real, no method
mocks) exercises the build→send→dry-run-verify path that the per-family functions reuse.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import httpx
import pytest

from nso_adapter.nso.apply import (
    NsoApplyError,
    _send_service_config,
    _verify_native_or_raise,
    apply_bfd_config,
    apply_bgp_config,
    apply_isis_interfaces,
    apply_logging_config,
    apply_mtu_config,
    apply_ospf_config,
    apply_snmp_config,
    apply_static_routes,
    native_dry_run,
)
from nso_adapter.nso.client import NsoClient
from nso_adapter.store.models import OspfInstanceIntent, OspfInterfaceIntent, RedistributionIntent

_EMPTY_DRYRUN = {"dry-run-result": {"native": {}}}


class _RecordingTransport(httpx.AsyncBaseTransport):
    """Records every request; replies 200 + a dry-run body to ``dry-run=native`` URLs and
    ``send_status`` to the apply PATCH/PUT. ``raise_exc`` simulates a transport failure."""

    def __init__(
        self,
        *,
        send_status: int = 204,
        dryrun_status: int = 200,
        dryrun_body=None,
        raise_exc: Exception | None = None,
    ):
        self.requests: list[httpx.Request] = []
        self.send_status = send_status
        self.dryrun_status = dryrun_status
        self.dryrun_body = _EMPTY_DRYRUN if dryrun_body is None else dryrun_body
        self.raise_exc = raise_exc

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self.raise_exc is not None:
            raise self.raise_exc
        if "dry-run=native" in str(request.url):
            return httpx.Response(
                self.dryrun_status,
                content=json.dumps(self.dryrun_body).encode(),
                headers={"content-type": "application/yang-data+json"},
                request=request,
            )
        return httpx.Response(self.send_status, content=b"", request=request)


def _client_with(transport: _RecordingTransport) -> NsoClient:
    # mock-ok: NsoInstanceConfig stub — only base_url/ca_cert/host_header are read; the
    # real NsoClient + apply code run for real over the httpx MockTransport below.
    cfg = MagicMock()
    cfg.base_url = "http://nso"
    cfg.ca_cert = None
    cfg.host_header = None
    client = NsoClient(cfg, "admin", "secret")
    client._client = lambda timeout=None: httpx.AsyncClient(transport=transport, base_url="http://nso")
    return client


# ── native_dry_run ─────────────────────────────────────────────────────────────


async def test_native_dry_run_returns_device_delta():
    body = {"dry-run-result": {"native": {"device": [{"name": "sw03", "data": "router isis\n"}]}}}
    transport = _RecordingTransport(dryrun_body=body)
    client = _client_with(transport)

    delta = await native_dry_run(client, "http://nso/restconf/data/x:y", '{"a": 1}', "sw03")

    assert delta == "router isis\n"
    assert "dry-run=native" in str(transport.requests[0].url)
    assert transport.requests[0].content == b'{"a": 1}'


async def test_native_dry_run_none_on_non_2xx():
    client = _client_with(_RecordingTransport(dryrun_status=503))
    assert await native_dry_run(client, "http://nso/x", "{}", "sw03") is None


async def test_native_dry_run_none_on_transport_error():
    client = _client_with(_RecordingTransport(raise_exc=httpx.ConnectError("refused")))
    assert await native_dry_run(client, "http://nso/x", "{}", "sw03") is None


# ── _send_service_config ───────────────────────────────────────────────────────


async def test_send_service_config_patches_then_verifies_clean():
    transport = _RecordingTransport(send_status=204)  # PATCH 204, verify dry-run → empty
    client = _client_with(transport)

    result = await _send_service_config(
        client, "/restconf/data/svc:cfg", "svc:cfg", "sw03", {"device": "sw03", "x": 1}, scope="snmp"
    )

    assert result is None
    patch_req = transport.requests[0]
    assert patch_req.method == "PATCH"
    assert json.loads(patch_req.content) == {"svc:cfg": [{"device": "sw03", "x": 1}]}
    assert "reconcile=" in str(patch_req.url)
    # a verify dry-run followed the apply
    assert any("dry-run=native" in str(r.url) for r in transport.requests[1:])


async def test_send_service_config_replace_uses_put_keyed_instance():
    transport = _RecordingTransport(send_status=200)
    client = _client_with(transport)

    await _send_service_config(
        client, "/restconf/data/svc:cfg", "svc:cfg", "sw03", {"device": "sw03"}, scope="isis", replace=True
    )

    put_req = transport.requests[0]
    assert put_req.method == "PUT"
    assert str(put_req.url).startswith("http://nso/restconf/data/svc:cfg=sw03")


async def test_send_service_config_raises_on_send_error():
    transport = _RecordingTransport(send_status=409)
    client = _client_with(transport)

    with pytest.raises(NsoApplyError) as exc:
        await _send_service_config(
            client, "/restconf/data/svc:cfg", "svc:cfg", "sw03", {"device": "sw03"}, scope="snmp"
        )
    assert exc.value.code == "nso_patch_failed"


async def test_send_service_config_dry_run_returns_delta_without_committing():
    body = {"dry-run-result": {"native": {"device": [{"name": "sw03", "data": "snmp-server\n"}]}}}
    transport = _RecordingTransport(dryrun_body=body)
    client = _client_with(transport)

    delta = await _send_service_config(
        client, "/restconf/data/svc:cfg", "svc:cfg", "sw03", {"device": "sw03"}, scope="snmp", dry_run=True
    )

    assert delta == "snmp-server\n"
    # dry-run only — no plain (non-dry-run) PATCH was sent
    assert all("dry-run=native" in str(r.url) for r in transport.requests)


# ── _verify_native_or_raise ────────────────────────────────────────────────────


async def test_verify_raises_when_delta_remains():
    body = {"dry-run-result": {"native": {"device": [{"name": "sw03", "data": "leftover\n"}]}}}
    client = _client_with(_RecordingTransport(dryrun_body=body))

    with pytest.raises(NsoApplyError) as exc:
        await _verify_native_or_raise(client, "http://nso/x", "{}", "sw03", scope="snmp")
    assert exc.value.code == "verify_mismatch"


async def test_verify_passes_when_delta_empty():
    client = _client_with(_RecordingTransport(dryrun_body=_EMPTY_DRYRUN))
    await _verify_native_or_raise(client, "http://nso/x", "{}", "sw03", scope="snmp")  # no raise


# ── per-family apply_* body building (dry-run captures the exact payload sent) ──
# SimpleNamespace intent rows (no mocks); dry_run=True routes through native_dry_run
# so the captured request body is the JSON the apply would PATCH.


def _sent_body(transport: _RecordingTransport) -> dict:
    return json.loads(transport.requests[0].content)


async def test_apply_static_routes_builds_route_body():
    transport = _RecordingTransport()
    client = _client_with(transport)
    rows = [
        SimpleNamespace(vrf="", prefix="10.0.0.0/8", next_hop="192.0.2.1", metric=10, permanent=True, tag=None),
        SimpleNamespace(vrf="MGMT", prefix="0.0.0.0/0", next_hop="192.0.2.254", metric=None, permanent=False, tag=5),
    ]
    delta = await apply_static_routes(client, "sw03", rows, dry_run=True)

    assert delta == ""
    routes = _sent_body(transport)["static-route-reconciler:static-route-config"][0]["route"]
    assert routes[0] == {"vrf": "", "prefix": "10.0.0.0/8", "next-hop": "192.0.2.1", "metric": 10, "permanent": True}
    assert routes[1] == {"vrf": "MGMT", "prefix": "0.0.0.0/0", "next-hop": "192.0.2.254", "tag": 5}


async def test_apply_bfd_config_builds_interface_body():
    transport = _RecordingTransport()
    client = _client_with(transport)
    rows = [
        SimpleNamespace(interface_name="ae1", micro_bfd=True, min_tx=300, min_rx=300, multiplier=3),
        SimpleNamespace(interface_name="ae2", micro_bfd=False, min_tx=None, min_rx=None, multiplier=None),
    ]
    await apply_bfd_config(client, "sw03", rows, dry_run=True)

    ifaces = _sent_body(transport)["bfd-reconciler:bfd-config"][0]["interface"]
    assert ifaces[0] == {"interface-name": "ae1", "micro-bfd": True, "min-tx": 300, "min-rx": 300, "multiplier": 3}
    assert ifaces[1] == {"interface-name": "ae2", "micro-bfd": False}


async def test_apply_mtu_config_builds_interface_body():
    transport = _RecordingTransport()
    client = _client_with(transport)
    rows = [
        SimpleNamespace(interface_name="Gi0/1", mtu=9000, ip_mtu=8986, mpls_mtu=None),
        SimpleNamespace(interface_name="Gi0/2", mtu=None, ip_mtu=None, mpls_mtu=1500),
    ]
    await apply_mtu_config(client, "sw03", rows, dry_run=True)

    ifaces = _sent_body(transport)["mtu-reconciler:mtu-config"][0]["interface"]
    assert ifaces[0] == {"interface-name": "Gi0/1", "mtu": 9000, "ip-mtu": 8986}
    assert ifaces[1] == {"interface-name": "Gi0/2", "mpls-mtu": 1500}


async def test_apply_snmp_config_builds_full_snapshot_body():
    transport = _RecordingTransport()
    client = _client_with(transport)
    communities = [SimpleNamespace(label="ro", vault_ref="kv/snmp#ro", access="ro", acl="ACL-NMS")]
    v3_users = [SimpleNamespace(username="nms", auth_vault_ref="kv/snmp#auth", priv_vault_ref=None)]
    hosts = [SimpleNamespace(address="192.0.2.9", version="3", notify_type="traps", community_or_user="nms")]
    system = SimpleNamespace(location="DC-A", contact=None)

    await apply_snmp_config(client, "sw03", communities, v3_users, hosts, system, dry_run=True)

    entry = _sent_body(transport)["snmp-reconciler:snmp-config"][0]
    assert entry["community"] == [{"label": "ro", "vault-ref": "kv/snmp#ro", "access": "ro", "acl": "ACL-NMS"}]
    assert entry["v3-user"] == [{"username": "nms", "auth-vault-ref": "kv/snmp#auth"}]  # priv omitted (None)
    assert entry["host"][0]["address"] == "192.0.2.9"
    assert entry["location"] == "DC-A"
    assert "contact" not in entry  # None contact omitted


async def test_apply_logging_config_builds_host_body():
    transport = _RecordingTransport()
    client = _client_with(transport)
    rows = [
        SimpleNamespace(
            address="192.0.2.5",
            port=514,
            severity="info",
            facility="local7",
            transport="udp",
            vrf="MGMT",
            source="Loopback0",
        ),
        SimpleNamespace(address="192.0.2.6", port=None, severity="", facility="", transport="", vrf="", source=""),
    ]
    await apply_logging_config(client, "sw03", rows, dry_run=True)

    hosts = _sent_body(transport)["logging-reconciler:logging-config"][0]["host"]
    assert hosts[0] == {
        "address": "192.0.2.5",
        "port": 514,
        "severity": "info",
        "facility": "local7",
        "transport": "udp",
        "vrf": "MGMT",
        "source": "Loopback0",
    }
    assert hosts[1] == {"address": "192.0.2.6"}  # all optionals falsy → omitted


async def test_apply_isis_interfaces_builds_process_and_interface_config():
    transport = _RecordingTransport()
    client = _client_with(transport)
    iface = SimpleNamespace(
        interface_name="ae1.0",
        af="ipv4",
        process_tag="0",
        passive=False,
        circuit_type="level-2",
        network_type=None,
        metric=10,
    )
    proc = SimpleNamespace(
        process_tag="0",
        net="49.0001.00",
        is_type="level-2-only",
        metric_style="wide",
        overload_bit=None,
        area_auth_type="",
        area_auth_key=None,
        domain_auth_type="",
        domain_auth_key=None,
    )
    await apply_isis_interfaces(client, "sw03", [iface], isis_process_rows=[proc], dry_run=True)

    body = _sent_body(transport)["isis-reconciler:isis-config"][0]
    assert body["interface-config"][0]["circuit-type"] == "level-2-only"  # 'level-2' normalised
    assert body["process-config"][0] == {
        "process-tag": "0",
        "net": "49.0001.00",
        "is-type": "level-2-only",
        "metric-style": "wide",
    }


async def test_apply_ospf_config_builds_process_interface_and_redistribute():
    # Real ORM rows so the assembled body reflects the actual model fields, not a fake's
    # attribute names; the NsoClient + apply code run for real over a MockTransport.
    transport = _RecordingTransport()
    client = _client_with(transport)
    proc = OspfInstanceIntent(process_id="1", router_id="1.1.1.1", vrf="")  # enabled unset → default True
    iface = OspfInterfaceIntent(
        interface_name="Gi0/1",
        process_id="1",
        area_id="0",
        passive=False,
        priority=10,
        cost=100,
        network_type="point-to-point",
        auth_type="md5",
        auth_key="secret",
    )
    redist = RedistributionIntent(
        dest_protocol="ospf",
        dest_ref="1",
        source_protocol="connected",
        source_ref="",
        route_map="RM",
        metric=20,
        metric_type="type-1",
    )
    await apply_ospf_config(client, "sw03", [proc], [iface], redistribution_rows=[redist], dry_run=True)

    body = _sent_body(transport)["ospf-reconciler:ospf-config"][0]
    p = body["process-config"][0]
    assert p["process-id"] == 1
    assert p["enabled"] is True  # delete-guard default-enable
    assert p["redistribute"] == [
        {"source-protocol": "connected", "source-ref": "", "route-map": "RM", "metric": 20, "metric-type": "type-1"}
    ]
    i = body["interface-config"][0]
    assert i["network-type"] == "point-to-point"
    assert i["auth-type"] == "md5" and i["auth-key"] == "secret"


async def test_apply_ospf_config_interface_only_omits_process_config():
    """With no process rows the body carries interface-config but no process-config key."""
    transport = _RecordingTransport()
    client = _client_with(transport)
    iface = OspfInterfaceIntent(interface_name="Gi0/9", process_id="1", area_id="0", passive=False)
    await apply_ospf_config(client, "sw03", [], [iface], dry_run=True)

    body = _sent_body(transport)["ospf-reconciler:ospf-config"][0]
    assert "process-config" not in body
    assert body["interface-config"][0]["interface-name"] == "Gi0/9"


async def test_apply_ospf_config_commits_then_verifies():
    """A real (non-dry-run) OSPF apply PATCHes the merge path then runs the verify dry-run."""
    transport = _RecordingTransport(send_status=204)  # PATCH 204, verify dry-run → empty
    client = _client_with(transport)
    proc = OspfInstanceIntent(process_id="1", vrf="", enabled=False)  # operator-down preserved
    iface = OspfInterfaceIntent(interface_name="Gi0/1", process_id="1", area_id="0", passive=False)

    result = await apply_ospf_config(client, "sw03", [proc], [iface])

    assert result is None
    patch_req = transport.requests[0]
    assert patch_req.method == "PATCH"
    assert "dry-run=native" not in str(patch_req.url)
    assert "reconcile=" in str(patch_req.url)
    body = json.loads(patch_req.content)["ospf-reconciler:ospf-config"][0]
    assert body["process-config"][0]["enabled"] is False
    # a verify dry-run followed the commit
    assert any("dry-run=native" in str(r.url) for r in transport.requests[1:])


async def test_apply_bgp_config_builds_router_scope_peer_tree():
    transport = _RecordingTransport()
    client = _client_with(transport)
    paf = SimpleNamespace(
        af="ipv4-unicast", enabled=True, routemap_in="RM-IN", routemap_out=None, prefixlist_in=None, prefixlist_out=None
    )
    peer = SimpleNamespace(
        peer_address="192.0.2.1",
        enabled=True,
        peer_group="UPSTREAM",
        remote_as=65001,
        local_as=None,
        ttl=None,
        password="s3c",
        peer_address_families=[paf],
    )
    af = SimpleNamespace(af="ipv4-unicast")
    scope = SimpleNamespace(vrf="", address_families=[af], peers=[peer])
    router = SimpleNamespace(asn=65000, scopes=[scope])
    redist = SimpleNamespace(
        dest_ref="65000::ipv4-unicast", source_protocol="connected", source_ref="", route_map=None, metric=None
    )
    await apply_bgp_config(client, "sw03", [router], redistribution_rows=[redist], dry_run=True)

    r = _sent_body(transport)["bgp-reconciler:bgp-config"][0]["router"][0]
    assert r["asn"] == 65000
    sc = r["scope"][0]
    assert sc["address-family"][0]["redistribute"] == [{"source-protocol": "connected", "source-ref": ""}]
    p = sc["peer"][0]
    assert p["peer-address"] == "192.0.2.1"
    assert p["remote-as"] == 65001 and p["peer-group"] == "UPSTREAM" and p["password"] == "s3c"
    assert p["peer-address-family"][0] == {"afi": "ipv4-unicast", "enabled": True, "routemap-in": "RM-IN"}
