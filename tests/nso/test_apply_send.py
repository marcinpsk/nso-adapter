# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""The apply send/verify tail: native_dry_run, apply_device_intent, verify.

Every write now flows through ONE sender, so covering it once via a real httpx
MockTransport (a boundary fake — the actual NsoClient + apply code run for real, no method
mocks) exercises the encode→send→dry-run-verify path the whole write path reuses. The
per-family cases drive that same real send with one container in the document, so what they
assert is the bytes NSO would receive, container name included.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

import httpx
import pytest

from nso_adapter.config import NsoInstanceConfig
from nso_adapter.core.community_dialect import community_dialect_for
from nso_adapter.nso.apply import (
    VERIFY_CONCLUSIVE,
    NsoApplyError,
    SectionExecution,
    _verify_native_or_raise,
    apply_device_intent,
    build_interface_ip_body,
    encode_bfd,
    encode_bgp,
    encode_interface_mtu,
    encode_isis,
    encode_logging,
    encode_ospf,
    encode_snmp,
    encode_static_route,
    local_levels_write_enabled,
    native_dry_run,
    refuse_gated_local_levels,
)
from nso_adapter.nso.client import DEVICE_INTENT_ROOT, NsoClient
from nso_adapter.nso.nso_json import NSO_LEX_CHUNK, straddling_bare_tokens
from nso_adapter.store.models import OspfInstanceIntent, OspfInterfaceIntent, RedistributionIntent

_EMPTY_DRYRUN = {"dry-run-result": {"native": {}}}

#: What an encoder that reads no NED-conditioned fact is handed.
_PLAIN = SectionExecution(None, community_dialect_for(None))


class _RecordingTransport(httpx.AsyncBaseTransport):
    """Records every request; replies 200 + a dry-run body to ``dry-run=native`` URLs and
    ``send_status`` to the apply PUT. ``raise_exc`` simulates a transport failure."""

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
    # Real NsoInstanceConfig (NsoClient reads base_url/ca_cert/host_header off it); the real
    # NsoClient + apply code then run for real over the httpx MockTransport below.
    cfg = NsoInstanceConfig(
        name="nso-dev",
        base_url="http://nso",
        ca_cert=None,
        username_ref="NSO_USERNAME",
        password_ref="NSO_PASSWORD",
        host_header=None,
    )
    client = NsoClient(cfg, "admin", "secret")
    client._client = lambda timeout=None: httpx.AsyncClient(transport=transport, base_url="http://nso")
    return client


def _sent_document(transport: _RecordingTransport, index: int = 0) -> dict:
    """The ``list device-intent`` entry the request carried."""
    return json.loads(transport.requests[index].content)[DEVICE_INTENT_ROOT][0]


def _sent(transport: _RecordingTransport, container: str) -> dict:
    """One family's body out of the transmitted document."""
    return _sent_document(transport)[container]


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


# ── apply_device_intent: the ONE sender ────────────────────────────────────────


async def test_the_sender_puts_the_keyed_instance_then_verifies_clean():
    transport = _RecordingTransport(send_status=204)  # PUT 204, verify dry-run → empty
    client = _client_with(transport)

    result = await apply_device_intent(client, "sw03", {"vlan": {"vlan": [{"vlan-id": 10}]}})

    # R2 §4.4: a committing send returns its PROOF VERDICT, not None — a consumer that is
    # about to record deletion authority has to tell "proven" from "we did not look".
    assert result == VERIFY_CONCLUSIVE
    put_req = transport.requests[0]
    assert put_req.method == "PUT"
    assert str(put_req.url).startswith("http://nso/restconf/data/device-intent:device-intent=sw03")
    assert json.loads(put_req.content) == {
        DEVICE_INTENT_ROOT: [{"device": "sw03", "vlan": {"vlan": [{"vlan-id": 10}]}}]
    }
    assert "reconcile=" in str(put_req.url)
    # a verify dry-run followed the apply, and it re-issues the SAME method
    assert any("dry-run=native" in str(r.url) and r.method == "PUT" for r in transport.requests[1:])


async def test_the_sender_carries_every_family_in_one_request():
    """One document is one transaction: the families are keys of ONE instance, not N sends."""
    transport = _RecordingTransport()
    client = _client_with(transport)

    await apply_device_intent(
        client,
        "sw03",
        {"vlan": {"vlan": [{"vlan-id": 10}]}, "svi": {"interface": [{"interface-name": "Vlan10"}]}},
    )

    assert len([r for r in transport.requests if "dry-run" not in str(r.url)]) == 1
    assert set(_sent_document(transport)) == {"device", "vlan", "svi"}


async def test_an_empty_family_body_is_transmitted_rather_than_dropped():
    """An empty container and an absent one both mean "this family owns nothing" (#1522 D2).

    The sender must not silently drop either: which families the document carries is the
    registry walk's decision, and a body the sender edited would no longer be the document
    the generation froze.
    """
    transport = _RecordingTransport()
    client = _client_with(transport)

    await apply_device_intent(client, "sw03", {"vlan": {"vlan": []}, "snmp": {}})

    assert _sent_document(transport) == {"device": "sw03", "vlan": {"vlan": []}, "snmp": {}}


async def test_the_sender_raises_on_a_rejected_commit():
    transport = _RecordingTransport(send_status=409)
    client = _client_with(transport)

    with pytest.raises(NsoApplyError) as exc:
        await apply_device_intent(client, "sw03", {"snmp": {}})
    assert exc.value.code == "nso_put_failed"


async def test_the_sender_dry_run_returns_the_delta_without_committing():
    body = {"dry-run-result": {"native": {"device": [{"name": "sw03", "data": "snmp-server\n"}]}}}
    transport = _RecordingTransport(dryrun_body=body)
    client = _client_with(transport)

    delta = await apply_device_intent(client, "sw03", {"snmp": {}}, dry_run=True)

    assert delta == "snmp-server\n"
    # dry-run only — no plain (non-dry-run) PUT was sent
    assert all("dry-run=native" in str(r.url) for r in transport.requests)


async def test_no_networking_reaches_the_wire_as_a_commit_param():
    """The detach path (#106) drops service governance without touching the device."""
    transport = _RecordingTransport()
    client = _client_with(transport)

    await apply_device_intent(client, "sw03", {"snmp": {}}, no_networking=True)

    assert "no-networking" in str(transport.requests[0].url)


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


# ── per-family wire vocabulary (the real send captures the exact container body) ──
# SimpleNamespace intent rows (no mocks) keyed by TABLE NAME, exactly as the stored
# document keys them; dry_run=True routes through native_dry_run so the captured request
# body is the JSON the commit would PUT.


async def test_static_route_container_carries_the_route_list():
    transport = _RecordingTransport()
    client = _client_with(transport)
    rows = [
        SimpleNamespace(vrf="", prefix="10.0.0.0/8", next_hop="192.0.2.1", metric=10, permanent=True, tag=None),
        SimpleNamespace(vrf="MGMT", prefix="0.0.0.0/0", next_hop="192.0.2.254", metric=None, permanent=False, tag=5),
    ]
    body = encode_static_route({"static_route_intent": rows}, _PLAIN)
    delta = await apply_device_intent(client, "sw03", {"static-route": body}, dry_run=True)

    assert delta == ""
    routes = _sent(transport, "static-route")["route"]
    assert routes[0] == {"vrf": "", "prefix": "10.0.0.0/8", "next-hop": "192.0.2.1", "metric": 10, "permanent": True}
    assert routes[1] == {"vrf": "MGMT", "prefix": "0.0.0.0/0", "next-hop": "192.0.2.254", "tag": 5}


async def test_static_route_emits_interface_and_next_hop_vrf():
    """IOS-XR next-hop forms round-trip: an interface next-hop and an inter-VRF (leaked)
    next-hop VRF are carried into the container (VTEST-9). A row missing the attrs entirely
    (the common plain-IP case) carries neither — getattr-defaulted, not required.
    """
    transport = _RecordingTransport()
    client = _client_with(transport)
    rows = [
        SimpleNamespace(
            vrf="",
            prefix="192.0.2.2/32",
            next_hop="192.0.2.31",
            next_hop_vrf="TMS-P",
            interface_next_hop="",
            metric=None,
            permanent=False,
            tag=None,
        ),
        SimpleNamespace(
            vrf="",
            prefix="172.30.61.0/24",
            next_hop="172.30.150.1",
            next_hop_vrf="",
            interface_next_hop="MgmtEth0/RSP0/CPU0/0",
            metric=None,
            permanent=False,
            tag=None,
        ),
        SimpleNamespace(vrf="", prefix="10.0.0.0/8", next_hop="192.0.2.1", metric=None, permanent=False, tag=None),
    ]
    body = encode_static_route({"static_route_intent": rows}, _PLAIN)
    await apply_device_intent(client, "ra1xr", {"static-route": body}, dry_run=True)

    routes = _sent(transport, "static-route")["route"]
    assert routes[0]["next-hop-vrf"] == "TMS-P"
    assert "interface-next-hop" not in routes[0]  # empty string → omitted
    assert routes[1]["interface-next-hop"] == "MgmtEth0/RSP0/CPU0/0"
    assert "next-hop-vrf" not in routes[1]
    # a plain row without the attrs at all carries neither (getattr default)
    assert "next-hop-vrf" not in routes[2] and "interface-next-hop" not in routes[2]


async def test_bfd_container_carries_the_interface_list():
    transport = _RecordingTransport()
    client = _client_with(transport)
    rows = [
        SimpleNamespace(interface_name="ae1", micro_bfd=True, min_tx=300, min_rx=300, multiplier=3),
        SimpleNamespace(interface_name="ae2", micro_bfd=False, min_tx=None, min_rx=None, multiplier=None),
    ]
    await apply_device_intent(client, "sw03", {"bfd": encode_bfd({"bfd_intent": rows}, _PLAIN)}, dry_run=True)

    ifaces = _sent(transport, "bfd")["interface"]
    assert ifaces[0] == {"interface-name": "ae1", "micro-bfd": True, "min-tx": 300, "min-rx": 300, "multiplier": 3}
    assert ifaces[1] == {"interface-name": "ae2", "micro-bfd": False}


async def test_mtu_container_carries_the_interface_list():
    transport = _RecordingTransport()
    client = _client_with(transport)
    rows = [
        SimpleNamespace(interface_name="Gi0/1", mtu=9000, ip_mtu=8986, mpls_mtu=None),
        SimpleNamespace(interface_name="Gi0/2", mtu=None, ip_mtu=None, mpls_mtu=1500),
    ]
    body = encode_interface_mtu({"interface_mtu_intent": rows}, _PLAIN)
    await apply_device_intent(client, "sw03", {"mtu": body}, dry_run=True)

    ifaces = _sent(transport, "mtu")["interface"]
    assert ifaces[0] == {"interface-name": "Gi0/1", "mtu": 9000, "ip-mtu": 8986}
    assert ifaces[1] == {"interface-name": "Gi0/2", "mpls-mtu": 1500}


async def test_snmp_container_uses_vault_triples_and_yang_enums():
    """Real-shape intent rows (plugin spellings: access=RO, notify_type=trap, full
    mount/path#key refs) must land as the exact snmp YANG contract: list key ``name``,
    split vault-mount/path/key triples, lowercase enums."""
    transport = _RecordingTransport()
    client = _client_with(transport)
    communities = [
        SimpleNamespace(
            label="9f2a41c3d0be77aa",
            vault_ref="network/netbox/snmp/community/9f2a41c3d0be77aa#community",
            access="RO",
            acl="ACL-NMS",
        )
    ]
    v3_users = [
        SimpleNamespace(
            username="nms",
            group_name="v3-test-group",
            auth_protocol="sha-256",
            priv_protocol="aes-128",
            auth_vault_ref="network/netbox/snmp/v3/nms#auth",
            priv_vault_ref="network/netbox/snmp/v3/nms#priv",
        ),
        SimpleNamespace(
            username="audit",
            group_name=None,
            auth_protocol="sha",
            priv_protocol=None,
            auth_vault_ref="network/netbox/snmp/v3/audit#auth",
            priv_vault_ref=None,
        ),
    ]
    hosts = [
        SimpleNamespace(
            address="192.0.2.9",
            version="v2c",
            notify_type="trap",
            community_or_user="9f2a41c3d0be77aa",
            port=None,
        ),
        SimpleNamespace(address="192.0.2.10", version="3", notify_type="informs", community_or_user="nms", port=1162),
    ]
    system = SimpleNamespace(location="DC-A", contact=None)

    body = encode_snmp(
        {
            "snmp_community_intent": communities,
            "snmp_v3_user_intent": v3_users,
            "snmp_host_intent": hosts,
            "snmp_system_info_intent": [system],
        },
        _PLAIN,
    )
    await apply_device_intent(client, "sw03", {"snmp": body}, dry_run=True)

    entry = _sent(transport, "snmp")
    assert entry["community"] == [
        {
            "name": "9f2a41c3d0be77aa",
            "access": "ro",
            "acl": "ACL-NMS",
            "vault-mount": "network",
            "vault-path": "netbox/snmp/community/9f2a41c3d0be77aa",
            "vault-key": "community",
        }
    ]
    assert entry["v3-user"] == [
        {
            "username": "nms",
            "group": "v3-test-group",
            "auth-protocol": "sha-256",
            "priv-protocol": "aes-128",
            "auth-vault-mount": "network",
            "auth-vault-path": "netbox/snmp/v3/nms",
            "auth-vault-key": "auth",
            "priv-vault-mount": "network",
            "priv-vault-path": "netbox/snmp/v3/nms",
            "priv-vault-key": "priv",
        },
        {
            "username": "audit",
            "auth-protocol": "sha",
            "auth-vault-mount": "network",
            "auth-vault-path": "netbox/snmp/v3/audit",
            "auth-vault-key": "auth",
        },
    ]
    assert entry["host"][0] == {
        "address": "192.0.2.9",
        "version": "v2c",
        "notify-type": "traps",
        "community-or-user": "9f2a41c3d0be77aa",
    }
    assert entry["host"][1] == {
        "address": "192.0.2.10",
        "version": "v3",
        "notify-type": "informs",
        "community-or-user": "nms",
        "port": 1162,
    }
    assert entry["location"] == "DC-A"
    assert "contact" not in entry  # None contact omitted


def test_snmp_accepts_the_bare_2_version_spelling():
    """A bare "2" is a legitimate SNMPv2c spelling the API accepts (version is a plain str),
    and pre-#121 host rows may already hold it — but _SNMP_VERSION had no entry for it, so
    _snmp_enum raised WHILE the body was being built. That aborted the whole SNMP family:
    the device's communities and v3 users, which had applied fine for months, were never
    pushed either, on every apply."""
    hosts = [SimpleNamespace(address="192.0.2.98", version="2", notify_type="trap", community_or_user="ro", port=None)]

    body = encode_snmp(
        {
            "snmp_community_intent": [],
            "snmp_v3_user_intent": [],
            "snmp_host_intent": hosts,
            "snmp_system_info_intent": [],
        },
        _PLAIN,
    )

    assert body["host"][0]["version"] == "v2c"


def test_snmp_host_without_binding_omits_community_or_user():
    """A host with no community/user binding (ArcOS targets carry none — the platform
    binds via target-parameters, not the target) must OMIT the optional
    community-or-user leaf, not send a JSON null the RESTCONF layer rejects.
    """
    hosts = [SimpleNamespace(address="192.0.2.99", version="2c", notify_type="trap", community_or_user=None, port=162)]

    body = encode_snmp(
        {
            "snmp_community_intent": [],
            "snmp_v3_user_intent": [],
            "snmp_host_intent": hosts,
            "snmp_system_info_intent": [],
        },
        _PLAIN,
    )

    assert body["host"] == [{"address": "192.0.2.99", "version": "v2c", "notify-type": "traps", "port": 162}]


@pytest.mark.parametrize(
    "bad_ref",
    [
        "",  # empty — a refless community can never satisfy the mandatory triples
        "no-mount#community",  # no '/': mount cannot be determined
        "network/netbox/snmp/ro",  # community refs must carry '#key'
        "network/netbox#a#b",  # more than one '#'
        "network//netbox#community",  # empty path segment
        "network/netbox snmp#community",  # whitespace
    ],
)
def test_snmp_rejects_a_malformed_vault_ref(bad_ref):
    """A community that cannot produce the mandatory vault triples must fail the encode with
    a structured error (never a silent drop: an omitted family is a retracted family, so the
    community would be deleted from the device)."""
    communities = [SimpleNamespace(label="ro", vault_ref=bad_ref, access="RO", acl=None)]

    with pytest.raises(NsoApplyError, match="vault_ref"):
        encode_snmp(
            {
                "snmp_community_intent": communities,
                "snmp_v3_user_intent": [],
                "snmp_host_intent": [],
                "snmp_system_info_intent": [],
            },
            _PLAIN,
        )


async def test_logging_container_carries_the_host_list():
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
    body = encode_logging({"logging_host_intent": rows, "logging_levels_intent": []}, _PLAIN)
    await apply_device_intent(client, "sw03", {"logging": body}, dry_run=True)

    hosts = _sent(transport, "logging")["host"]
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


def _levels_row(console=None, monitor=None, module=None):
    return SimpleNamespace(console_severity=console, monitor_severity=monitor, module_severity=module)


def test_logging_emits_the_set_local_levels():
    """The accepted levels intent rides the logging container; unset severities are omitted
    (no clears — FASTMAP retraction owns removal)."""
    body = encode_logging(
        {"logging_host_intent": [], "logging_levels_intent": [_levels_row(console="CRITICAL", module="NOTICE")]},
        _PLAIN,
    )

    assert body["local-levels"] == {"console-severity": "CRITICAL", "module-severity": "NOTICE"}


def test_the_send_gate_refuses_local_levels_while_it_is_closed(monkeypatch):
    """Gate OFF + an accepted levels intent → structured REFUSAL at the send boundary.

    Proceeding with a host-only body would stamp the levels row in_sync without any severity
    landing (a silent drop), and the document PUT missing local-levels would FASTMAP-retract
    previously-owned severities — on NX that DISABLES the destination. The refusal is the
    SEND's, never the encoder's: one document must encode the same bytes in every process.
    """
    monkeypatch.delenv("NSO_ADAPTER_LOGGING_LOCAL_LEVELS_WRITE", raising=False)
    rows = {"logging_host_intent": [], "logging_levels_intent": [_levels_row(console="CRITICAL")]}

    assert not local_levels_write_enabled()
    with pytest.raises(NsoApplyError, match="NSO_ADAPTER_LOGGING_LOCAL_LEVELS_WRITE"):
        refuse_gated_local_levels(rows)
    # The encoder itself is unconditional: the same rows encode the same bytes either way.
    assert encode_logging(rows, _PLAIN)["local-levels"] == {"console-severity": "CRITICAL"}


def test_the_open_gate_admits_local_levels(monkeypatch):
    monkeypatch.setenv("NSO_ADAPTER_LOGGING_LOCAL_LEVELS_WRITE", "1")
    refuse_gated_local_levels({"logging_levels_intent": [_levels_row(console="CRITICAL")]})  # no raise


def test_the_gate_ignores_a_body_with_no_levels(monkeypatch):
    """The gate guards the levels container only: a hosts-only apply (every non-NX device
    today) proceeds untouched with the gate off."""
    monkeypatch.delenv("NSO_ADAPTER_LOGGING_LOCAL_LEVELS_WRITE", raising=False)
    rows = {
        "logging_host_intent": [
            SimpleNamespace(address="192.0.2.7", port=None, severity="", facility="", transport="", vrf="", source="")
        ],
        "logging_levels_intent": [],
    }

    refuse_gated_local_levels(rows)  # no raise
    body = encode_logging(rows, _PLAIN)
    assert "local-levels" not in body
    assert body["host"] == [{"address": "192.0.2.7"}]


async def test_isis_container_carries_process_and_interface_config():
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
    body = encode_isis(
        {
            "isis_interface_intent": [iface],
            "isis_process_intent": [proc],
            "isis_level_intent": [],
            "isis_flex_algo_intent": [],
            "redistribution_intent": [],
        },
        _PLAIN,
    )
    await apply_device_intent(client, "sw03", {"isis": body}, dry_run=True)

    sent = _sent(transport, "isis")
    assert sent["interface-config"][0]["circuit-type"] == "level-2-only"  # 'level-2' normalised
    assert sent["process-config"][0] == {
        "process-tag": "0",
        "net": "49.0001.00",
        "is-type": "level-2-only",
        "metric-style": "wide",
    }


def _ospf_rows(process_rows, interface_rows, redistribution_rows=()):
    return {
        "ospf_instance_intent": list(process_rows),
        "ospf_interface_intent": list(interface_rows),
        "redistribution_intent": list(redistribution_rows),
    }


async def test_ospf_container_carries_process_interface_and_redistribute():
    # Real ORM rows so the assembled body reflects the actual model fields, not a fake's
    # attribute names; the NsoClient + sender run for real over a MockTransport.
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
    body = encode_ospf(_ospf_rows([proc], [iface], [redist]), _PLAIN)
    await apply_device_intent(client, "sw03", {"ospf": body}, dry_run=True)

    sent = _sent(transport, "ospf")
    p = sent["process-config"][0]
    assert p["process-id"] == "1"
    assert p["enabled"] is True  # delete-guard default-enable
    assert p["redistribute"] == [
        {"source-protocol": "connected", "source-ref": "", "route-map": "RM", "metric": 20, "metric-type": "type-1"}
    ]
    i = sent["interface-config"][0]
    assert i["network-type"] == "point-to-point"
    assert i["auth-type"] == "md5" and i["auth-key"] == "secret"


def test_ospf_interface_only_omits_process_config():
    """With no process rows the body carries interface-config but no process-config key."""
    iface = OspfInterfaceIntent(interface_name="Gi0/9", process_id="1", area_id="0", passive=False)

    body = encode_ospf(_ospf_rows([], [iface]), _PLAIN)

    assert "process-config" not in body
    assert body["interface-config"][0]["interface-name"] == "Gi0/9"


async def test_a_real_ospf_commit_puts_then_verifies():
    """A real (non-dry-run) send PUTs the instance then runs the verify dry-run."""
    transport = _RecordingTransport(send_status=204)  # PUT 204, verify dry-run → empty
    client = _client_with(transport)
    proc = OspfInstanceIntent(process_id="1", vrf="", enabled=False)  # operator-down preserved
    iface = OspfInterfaceIntent(interface_name="Gi0/1", process_id="1", area_id="0", passive=False)

    body = encode_ospf(_ospf_rows([proc], [iface]), _PLAIN)
    result = await apply_device_intent(client, "sw03", {"ospf": body})

    assert result == VERIFY_CONCLUSIVE  # the verdict rides out of the committing send
    put_req = transport.requests[0]
    assert put_req.method == "PUT"
    assert "dry-run=native" not in str(put_req.url)
    assert "reconcile=" in str(put_req.url)
    assert _sent(transport, "ospf")["process-config"][0]["enabled"] is False
    # a verify dry-run followed the commit
    assert any("dry-run=native" in str(r.url) for r in transport.requests[1:])


async def test_bgp_container_carries_the_router_scope_peer_tree():
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
        source=None,
        peer_address_families=[paf],
    )
    af = SimpleNamespace(af="ipv4-unicast")
    scope = SimpleNamespace(vrf="", address_families=[af], peers=[peer])
    router = SimpleNamespace(asn=65000, router_id=None, scopes=[scope])
    redist = SimpleNamespace(
        dest_ref="65000::ipv4-unicast", source_protocol="connected", source_ref="", route_map=None, metric=None
    )
    body = encode_bgp({"bgp_router_intent": [router], "redistribution_intent": [redist]}, _PLAIN)
    await apply_device_intent(client, "sw03", {"bgp": body}, dry_run=True)

    r = _sent(transport, "bgp")["router"][0]
    assert r["asn"] == 65000
    sc = r["scope"][0]
    assert sc["address-family"][0]["redistribute"] == [{"source-protocol": "connected", "source-ref": ""}]
    p = sc["peer"][0]
    assert p["peer-address"] == "192.0.2.1"
    assert p["remote-as"] == 65001 and p["peer-group"] == "UPSTREAM" and p["password"] == "s3c"
    assert p["peer-address-family"][0] == {"afi": "ipv4-unicast", "enabled": True, "routemap-in": "RM-IN"}


# ── 64KiB boundary safety (check-item 134) ─────────────────────────────────────
# NSO 6.7's RESTCONF JSON lexer loses token state at its 64KiB read-buffer
# refill — a bare literal straddling byte k*65536 400s the whole request
# ("1: Bad JSON character: f"). Every apply-path body must therefore leave the
# adapter with no bare token on a boundary. These tests drive the REAL sender
# over the recording transport and assert on the actual bytes handed to httpx;
# each fixture asserts its own premise (default serialization DOES straddle),
# so a sizing drift fails loudly instead of passing vacuously.


def _straddling_container_body(wrap) -> dict:
    """A container body whose default-serialized WRAPPED request straddles a boundary.

    *wrap* replicates exactly how the sender wraps the body into the request payload; the
    pad places the first ``false`` 2 bytes across byte 65536.
    """
    probe = {"pad": "", "rows": [{"secondary": False, "n": 1234} for _ in range(40)]}
    first_false = json.dumps(wrap(probe)).index("false")
    body = {**probe, "pad": "x" * (NSO_LEX_CHUNK - first_false - 2)}
    assert straddling_bare_tokens(json.dumps(wrap(body))), "fixture premise: default dumps DOES straddle"
    return body


def _assert_wire_boundary_safe(transport: _RecordingTransport) -> None:
    """No request body that actually went over the wire straddles a 64KiB multiple."""
    bodies = [r.content.decode() for r in transport.requests if r.content]
    assert any(len(b) > NSO_LEX_CHUNK for b in bodies), "fixture premise: an oversized body was sent"
    for b in bodies:
        assert straddling_bare_tokens(b) == []


def _wrap_container(body: dict) -> dict:
    return {DEVICE_INTENT_ROOT: [{"device": "sw03", "snmp": body}]}


async def test_an_oversized_document_is_boundary_safe():
    transport = _RecordingTransport(send_status=204)
    client = _client_with(transport)
    body = _straddling_container_body(_wrap_container)

    await apply_device_intent(client, "sw03", {"snmp": body})

    _assert_wire_boundary_safe(transport)
    # whitespace-only protection: the parsed intent is unchanged
    assert _sent(transport, "snmp") == body


async def test_a_production_scale_interface_body_is_boundary_safe():
    """The exact shape that hit production scale: thousands of ``"secondary": false`` rows."""
    transport = _RecordingTransport(send_status=204)
    client = _client_with(transport)
    rows = [SimpleNamespace(address="10.0.0.1/24", family="ipv4", secondary=False, vrf="") for _ in range(120)]
    # The interface name precedes every row in the entry, so sizing it shifts each row token
    # by the same amount — place the first `false` across byte 65536.
    base = {
        DEVICE_INTENT_ROOT: [{"device": "sw03", "interface": {"interface": [build_interface_ip_body("ae0", rows)]}}]
    }
    first_false = json.dumps(base).index("false")
    name = "ae0" + "x" * (NSO_LEX_CHUNK - first_false - 2)
    entry = build_interface_ip_body(name, rows)
    payload = json.dumps({DEVICE_INTENT_ROOT: [{"device": "sw03", "interface": {"interface": [entry]}}]})
    assert straddling_bare_tokens(payload), "fixture premise: default dumps DOES straddle"

    await apply_device_intent(client, "sw03", {"interface": {"interface": [entry]}})

    _assert_wire_boundary_safe(transport)
    sent = _sent(transport, "interface")["interface"][0]
    assert sent["interface-name"] == name
    assert len(sent["ipv4-address"]) == 120
