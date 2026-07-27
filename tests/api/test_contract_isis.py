# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Contract test — producer side of GET /api/v1/devices/{id}/isis-interfaces.

The deepest read contract: processes carry a large optional scalar set plus four nested
JSON-bag containers (``settings``, ``levels``, ``segment_routing``, ``flex_algos``);
interfaces carry their own scalars + ``settings``/``levels``. Optional scalars and the
containers are OMITTED when unset. The nested dicts are hyphen→snake normalised by the
adapter (``_snake``) and the plugin reads fixed key sets out of them — those sub-dict key
sets are part of the contract and are pinned here too.

Consumed by the plugin in ``template_content._reconcile_isis_process`` /
``_reconcile_isis_interfaces``.

Canonical contract: ``docs/api-contract.md`` (IS-IS §).
Mirror (consumer side): ``netbox-nso-plugin/.../tests/test_contract_isis.py`` — the
``*_KEYS`` sets MUST stay identical across both files.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from tests.conftest import VALID_TOKEN, seed_device

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

TOP_KEYS = {"device_id", "last_refreshed_at", "refresh_source", "read_state", "processes", "interfaces"}

PROC_REQUIRED_KEYS = {"process_tag"}
PROC_OPTIONAL_SCALARS = {
    "net",
    "is_type",
    "metric_style",
    "overload_bit",
    "area_auth_type",
    "area_auth_present",
    "area_auth_key",
    "domain_auth_type",
    "domain_auth_present",
    "domain_auth_key",
    "spf_initial_wait",
    "spf_max_wait",
    "lsp_initial_wait",
    "lsp_max_wait",
    "lsp_lifetime",
    "lsp_refresh_interval",
    "lsp_mtu",
    "overload_on_startup",
    "overload_timeout",
    "te_enabled",
    "suppress_attached_bit",
    "ignore_attached_bit",
    "fast_reroute",
    "microloop_avoidance",
    "distance",
    "maximum_paths",
    "reference_bandwidth",
    "segment_routing_reported",
    "segment_routing_configured",
}
PROC_CONTAINER_KEYS = {"settings", "levels", "segment_routing", "flex_algos", "srv6_locators"}

IFACE_REQUIRED_KEYS = {"interface_name", "af", "process_tag", "passive"}
IFACE_OPTIONAL_SCALARS = {
    "circuit_type",
    "network_type",
    "metric",
    "bound_port",
    "hello_auth_type",
    "hello_auth_present",
    "bfd_enabled",
    "frr_enabled",
    "frr_protection",
    "csnp_interval",
    "retransmit_interval",
    "lsp_interval",
    "mesh_group",
}
IFACE_CONTAINER_KEYS = {"settings", "levels", "prefix_sids"}
# Per-loopback prefix-SID entry: maximal (SRGB-index) form; sid_label is the
# mutually-exclusive alternative to sid_index.
IFACE_PREFIX_SID_KEYS = {"algorithm", "sid_index", "n_flag", "no_php", "explicit_null", "readvertise"}

# Nested JSON-bag sub-dict key sets (snake-cased output the plugin reads).
INSTANCE_LEVEL_KEYS = {
    "level",
    "default_metric",
    "wide_metrics_only",
    "preference",
    "labeled_preference",
    "disabled",
    "auth_type",
}
IFACE_LEVEL_KEYS = {"level", "metric", "hello_interval", "hello_multiplier", "priority", "passive"}
SR_KEYS = {
    "enabled",
    "prefix_sid_range",
    "srgb_start",
    "srgb_range",
    "node_sid_index",
    "node_sid_label",
    "node_sid_v6_index",
    "node_sid_v6_label",
    "maximum_sid_depth",
    "tunnel_table_pref",
}
FLEX_KEYS = {
    "algo_id",
    "metric_type",
    "priority",
    "admin_group_exclude",
    "admin_group_include_any",
    "admin_group_include_all",
}
SRV6_LOCATOR_KEYS = {
    "name",
    "prefix",
    "algorithm",
    "is_anycast",
    "is_micro_segment",
    "flavor",
    "block_length",
    "node_length",
    "function_length",
    "argument_length",
    "isis_level",
    "enabled",
}


async def _seed_isis(device_id: int) -> None:
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import DeviceIsisInterface, DeviceIsisProcess

    ts = datetime(2026, 6, 1, 10, 0, 0)
    # Nested bags are stored hyphenated (as the extractor writes them); the adapter
    # _snake()s them on output → the snake_case keys the plugin consumes.
    instance_level = {
        "level": "2",
        "default-metric": 10,
        "wide-metrics-only": True,
        "preference": 7,
        "labeled-preference": 7,
        "disabled": False,
        "auth-type": "md5",
    }
    sr = {
        "enabled": True,
        "prefix-sid-range": "global",
        "srgb-start": 100000,
        "srgb-range": 200000,
        "node-sid-index": 100,
        "node-sid-label": 100100,
        "node-sid-v6-index": 200,
        "node-sid-v6-label": 100200,
        "maximum-sid-depth": 10,
        "tunnel-table-pref": 8,
    }
    flex = {
        "algo-id": 128,
        "metric-type": "igp",
        "priority": 100,
        "admin-group-exclude": ["RED"],
        "admin-group-include-any": ["BLUE"],
        "admin-group-include-all": [],
    }
    iface_level = {
        "level": "2",
        "metric": 10,
        "hello-interval": 3,
        "hello-multiplier": 3,
        "priority": 64,
        "passive": False,
    }
    srv6_loc = {
        "name": "LOC1",
        "prefix": "2001:db8:a1::/64",
        "algorithm": 128,
        "is-anycast": False,
        "is-micro-segment": True,
        "flavor": "psp usd",
        "block-length": 40,
        "node-length": 24,
        "function-length": 16,
        "argument-length": 0,
        "isis-level": 2,
        "enabled": True,
    }

    async for db in get_session():
        # MAXIMAL process: every scalar set + all four containers populated.
        db.add(
            DeviceIsisProcess(
                device_id=device_id,
                process_tag="1",
                net="49.0001.0000.0000.0001.00",
                is_type="level-2",
                metric_style="wide",
                overload_bit=False,
                area_auth_type="md5",
                area_auth_present=True,
                area_auth_key="x",
                domain_auth_type="md5",
                domain_auth_present=True,
                domain_auth_key="y",
                spf_initial_wait=50,
                spf_max_wait=5000,
                lsp_initial_wait=50,
                lsp_max_wait=5000,
                lsp_lifetime=65535,
                lsp_refresh_interval=65000,
                lsp_mtu=1492,
                overload_on_startup=True,
                overload_timeout=180,
                te_enabled=True,
                suppress_attached_bit=True,
                ignore_attached_bit=True,
                fast_reroute="ti-lfa",
                microloop_avoidance=True,
                distance=115,
                maximum_paths=8,
                reference_bandwidth=100000,
                segment_routing_reported=True,
                segment_routing_configured=True,
                settings={"some-knob": "v"},
                levels=[instance_level],
                segment_routing=sr,
                flex_algos=[flex],
                srv6_locators=[srv6_loc],
                last_refreshed_at=ts,
                refresh_source="poll",
            )
        )
        # MINIMAL process: only process_tag (every optional/container omitted).
        db.add(DeviceIsisProcess(device_id=device_id, process_tag="2", last_refreshed_at=ts, refresh_source="poll"))
        # Explicit absence provenance from a current reader.
        db.add(
            DeviceIsisProcess(
                device_id=device_id,
                process_tag="3",
                segment_routing_reported=True,
                segment_routing_configured=False,
                last_refreshed_at=ts,
                refresh_source="poll",
            )
        )
        # MAXIMAL interface + MINIMAL interface.
        db.add(
            DeviceIsisInterface(
                device_id=device_id,
                interface_name="GE0/0",
                af="ipv4",
                process_tag="1",
                circuit_type="level-2-only",
                network_type="point-to-point",
                metric=10,
                passive=False,
                bound_port="GE0/0",
                hello_auth_type="md5",
                hello_auth_present=True,
                bfd_enabled=True,
                frr_enabled=True,
                frr_protection="node",
                csnp_interval=10,
                retransmit_interval=5,
                lsp_interval=33,
                mesh_group="1",
                settings={"some-knob": "v"},
                levels=[iface_level],
                prefix_sids=[
                    {
                        "algorithm": 0,
                        "sid-index": 100006,
                        "n-flag": True,
                        "no-php": True,
                        "explicit-null": False,
                        "readvertise": False,
                    },
                    {"algorithm": 128, "sid-label": 17128},
                ],
                last_refreshed_at=ts,
                refresh_source="poll",
            )
        )
        db.add(
            DeviceIsisInterface(
                device_id=device_id, interface_name="GE0/1", af="ipv4", last_refreshed_at=ts, refresh_source="poll"
            )
        )
        await db.commit()
        break


@pytest.mark.anyio
async def test_isis_payload_matches_contract_exactly(adapter_client):
    """Processes/interfaces + their nested JSON bags expose exactly the documented keys."""
    device_id = await seed_device(nso_device_name="isis-ct", netbox_device_id=7900)
    await _seed_isis(device_id)

    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/isis-interfaces", headers=AUTH)).json()
    assert set(body.keys()) == TOP_KEYS

    procs = {p["process_tag"]: p for p in body["processes"]}
    maximal, minimal, empty_sr = procs["1"], procs["2"], procs["3"]
    assert set(maximal.keys()) == PROC_REQUIRED_KEYS | PROC_OPTIONAL_SCALARS | PROC_CONTAINER_KEYS
    assert set(minimal.keys()) == PROC_REQUIRED_KEYS  # all optionals + containers omitted
    assert empty_sr == {
        "process_tag": "3",
        "segment_routing_reported": True,
        "segment_routing_configured": False,
    }

    # Nested bags: hyphen→snake normalised, fixed key sets the plugin reads.
    assert set(maximal["levels"][0].keys()) == INSTANCE_LEVEL_KEYS
    assert set(maximal["segment_routing"].keys()) == SR_KEYS
    assert set(maximal["flex_algos"][0].keys()) == FLEX_KEYS
    assert set(maximal["srv6_locators"][0].keys()) == SRV6_LOCATOR_KEYS

    ifaces = {i["interface_name"]: i for i in body["interfaces"]}
    assert set(ifaces["GE0/0"].keys()) == IFACE_REQUIRED_KEYS | IFACE_OPTIONAL_SCALARS | IFACE_CONTAINER_KEYS
    assert set(ifaces["GE0/1"].keys()) == IFACE_REQUIRED_KEYS  # optionals + containers omitted
    assert set(ifaces["GE0/0"]["levels"][0].keys()) == IFACE_LEVEL_KEYS
    # Per-loopback prefix-SID: maximal (index) form, plus the label alternative.
    assert set(ifaces["GE0/0"]["prefix_sids"][0].keys()) == IFACE_PREFIX_SID_KEYS
    assert set(ifaces["GE0/0"]["prefix_sids"][1].keys()) == {"algorithm", "sid_label"}
    assert isinstance(ifaces["GE0/0"]["passive"], bool)


@pytest.mark.anyio
async def test_isis_no_data_shape(adapter_client):
    """Empty shape keeps the top-level keys (refresh_source='never')."""
    device_id = await seed_device(nso_device_name="isis-ct-empty", netbox_device_id=7901)
    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/isis-interfaces", headers=AUTH)).json()
    assert set(body.keys()) == TOP_KEYS
    assert body["processes"] == [] and body["interfaces"] == []
    assert body["refresh_source"] == "never"
