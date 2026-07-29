# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Golden-body test — GET /api/v1/devices/{id}/isis-interfaces.

The deepest read contract: a maximal process with every optional scalar plus the
five JSON-bag containers (``settings`` verbatim; ``levels`` / ``segment_routing``
/ ``flex_algos`` / ``srv6_locators`` hyphen→snake normalised), a maximal + minimal
interface with ``prefix_sids`` in both index and label forms. Deep-equality pins
the exact bytes so response-model typing cannot drop a scalar or a whole bag, nor
alter the pass-through of the opaque nested dicts.

``last_refreshed_at`` is a formatted "<iso>Z" string. ``settings`` keeps its
hyphenated keys (the reader passes it verbatim, unlike the other bags).
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from tests.conftest import (
    GOLDEN_BORN_ISO,
    GOLDEN_INCARNATION,
    VALID_TOKEN,
    pin_store_incarnation,
    seed_device,
    session,
)

_SYNTH_READ_STATE = {
    "outcome": "unavailable",
    "reason": "not_ready",
    "freshness": None,
    "result": None,
    "succeeded": None,
    "read_at": None,
    "attempt_id": None,
    "source_epoch": 1,
    "payload_revision": None,
    "incarnation": GOLDEN_INCARNATION,
    "incarnation_born": GOLDEN_BORN_ISO,
}

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

TS = datetime(2026, 6, 1, 10, 0, 0, tzinfo=UTC)


async def _seed_isis(device_id: int) -> None:
    from nso_adapter.store.models import DeviceIsisInterface, DeviceIsisProcess

    async with session() as db:
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
                settings={"some-knob": "v"},
                levels=[
                    {
                        "level": "2",
                        "default-metric": 10,
                        "wide-metrics-only": True,
                        "preference": 7,
                        "labeled-preference": 7,
                        "disabled": False,
                        "auth-type": "md5",
                    }
                ],
                segment_routing={
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
                },
                flex_algos=[
                    {
                        "algo-id": 128,
                        "metric-type": "igp",
                        "priority": 100,
                        "admin-group-exclude": ["RED"],
                        "admin-group-include-any": ["BLUE"],
                        "admin-group-include-all": [],
                    }
                ],
                srv6_locators=[
                    {
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
                ],
                last_refreshed_at=TS,
                refresh_source="poll",
            )
        )
        db.add(DeviceIsisProcess(device_id=device_id, process_tag="2", last_refreshed_at=TS, refresh_source="poll"))
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
                levels=[
                    {
                        "level": "2",
                        "metric": 10,
                        "hello-interval": 3,
                        "hello-multiplier": 3,
                        "priority": 64,
                        "passive": False,
                    }
                ],
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
                last_refreshed_at=TS,
                refresh_source="poll",
            )
        )
        db.add(
            DeviceIsisInterface(
                device_id=device_id, interface_name="GE0/1", af="ipv4", last_refreshed_at=TS, refresh_source="poll"
            )
        )
        await db.commit()


@pytest.mark.anyio
async def test_isis_golden_body(adapter_client):
    device_id = await seed_device(nso_device_name="isis-golden", netbox_device_id=7905)
    await pin_store_incarnation()
    await _seed_isis(device_id)

    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/isis-interfaces", headers=AUTH)).json()

    assert body == {
        "device_id": device_id,
        "last_refreshed_at": "2026-06-01T10:00:00Z",
        "refresh_source": "poll",
        "read_state": _SYNTH_READ_STATE,
        "processes": [
            {
                "process_tag": "1",
                "net": "49.0001.0000.0000.0001.00",
                "is_type": "level-2",
                "metric_style": "wide",
                "overload_bit": False,
                "area_auth_type": "md5",
                "area_auth_present": True,
                "area_auth_key": "x",
                "domain_auth_type": "md5",
                "domain_auth_present": True,
                "domain_auth_key": "y",
                "spf_initial_wait": 50,
                "spf_max_wait": 5000,
                "lsp_initial_wait": 50,
                "lsp_max_wait": 5000,
                "lsp_lifetime": 65535,
                "lsp_refresh_interval": 65000,
                "lsp_mtu": 1492,
                "overload_on_startup": True,
                "overload_timeout": 180,
                "te_enabled": True,
                "suppress_attached_bit": True,
                "ignore_attached_bit": True,
                "fast_reroute": "ti-lfa",
                "microloop_avoidance": True,
                "distance": 115,
                "maximum_paths": 8,
                "reference_bandwidth": 100000,
                "settings": {"some-knob": "v"},
                "levels": [
                    {
                        "level": "2",
                        "default_metric": 10,
                        "wide_metrics_only": True,
                        "preference": 7,
                        "labeled_preference": 7,
                        "disabled": False,
                        "auth_type": "md5",
                    }
                ],
                "segment_routing": {
                    "enabled": True,
                    "prefix_sid_range": "global",
                    "srgb_start": 100000,
                    "srgb_range": 200000,
                    "node_sid_index": 100,
                    "node_sid_label": 100100,
                    "node_sid_v6_index": 200,
                    "node_sid_v6_label": 100200,
                    "maximum_sid_depth": 10,
                    "tunnel_table_pref": 8,
                },
                "flex_algos": [
                    {
                        "algo_id": 128,
                        "metric_type": "igp",
                        "priority": 100,
                        "admin_group_exclude": ["RED"],
                        "admin_group_include_any": ["BLUE"],
                        "admin_group_include_all": [],
                    }
                ],
                "srv6_locators": [
                    {
                        "name": "LOC1",
                        "prefix": "2001:db8:a1::/64",
                        "algorithm": 128,
                        "is_anycast": False,
                        "is_micro_segment": True,
                        "flavor": "psp usd",
                        "block_length": 40,
                        "node_length": 24,
                        "function_length": 16,
                        "argument_length": 0,
                        "isis_level": 2,
                        "enabled": True,
                    }
                ],
            },
            {"process_tag": "2"},
        ],
        "interfaces": [
            {
                "interface_name": "GE0/0",
                "af": "ipv4",
                "process_tag": "1",
                "circuit_type": "level-2-only",
                "network_type": "point-to-point",
                "metric": 10,
                "bound_port": "GE0/0",
                "hello_auth_type": "md5",
                "hello_auth_present": True,
                "bfd_enabled": True,
                "frr_enabled": True,
                "frr_protection": "node",
                "csnp_interval": 10,
                "retransmit_interval": 5,
                "lsp_interval": 33,
                "mesh_group": "1",
                "settings": {"some-knob": "v"},
                "levels": [
                    {
                        "level": "2",
                        "metric": 10,
                        "hello_interval": 3,
                        "hello_multiplier": 3,
                        "priority": 64,
                        "passive": False,
                    }
                ],
                "prefix_sids": [
                    {
                        "algorithm": 0,
                        "sid_index": 100006,
                        "n_flag": True,
                        "no_php": True,
                        "explicit_null": False,
                        "readvertise": False,
                    },
                    {"algorithm": 128, "sid_label": 17128},
                ],
                "passive": False,
            },
            {"interface_name": "GE0/1", "af": "ipv4", "process_tag": "", "passive": False},
        ],
    }


@pytest.mark.anyio
async def test_isis_golden_empty(adapter_client):
    device_id = await seed_device(nso_device_name="isis-golden-empty", netbox_device_id=7906)
    await pin_store_incarnation()
    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/isis-interfaces", headers=AUTH)).json()
    assert body == {
        "device_id": device_id,
        "last_refreshed_at": None,
        "refresh_source": "never",
        "read_state": _SYNTH_READ_STATE,
        "processes": [],
        "interfaces": [],
    }
