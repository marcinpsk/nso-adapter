# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""A0: every read-mirror refresher must return a truthful bool.

The comprehensive fan-out (`refresh_all_surfaces_for_device`) builds a failed-surface
list so a device with a swallowed NSO read reports ``partial`` instead of a misleading
``succeeded``.  That contract only holds if EVERY family refresher returns ``False`` when
its NSO read fails (rows left stale) and ``True`` on a definitive read / intentional skip.
The routing/BFD families already did this; the nine device-mirror families below returned
``None`` on both success and swallowed error — indistinguishable — until A0 normalized them.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import httpx
import pytest

from nso_adapter.core.interface_ip import refresh_interface_ips_for_device
from nso_adapter.core.interface_mtu import refresh_interface_mtu_for_device
from nso_adapter.core.l2_service import refresh_l2_services_for_device
from nso_adapter.core.lag_config import refresh_lag_config_for_device
from nso_adapter.core.lag_topology import refresh_lag_topology_for_device
from nso_adapter.core.subinterface import refresh_subinterface_for_device
from nso_adapter.core.svi import refresh_svi_for_device
from nso_adapter.core.vlan import refresh_switchport_for_device, refresh_vlan_database_for_device
from nso_adapter.store.db import get_session
from nso_adapter.store.models import Device
from tests.conftest import seed_device

# (refresher, a minimal empty-but-valid success payload, wire_name)
# Every family reads its device-state envelope section (READSEM S5 retired the legacy getters);
# wire_name is the section name asserted by the flip manifest below.
_NORMALIZED = [
    (refresh_interface_ips_for_device, {"interface": []}, "interface-ip"),
    (refresh_lag_topology_for_device, {"lag": []}, "lag-topology"),
    (refresh_lag_config_for_device, {"lag": []}, "lag-config"),
    (refresh_l2_services_for_device, {"service": []}, "l2-service"),
    (refresh_vlan_database_for_device, {"vlan": []}, "vlan-database"),
    (refresh_switchport_for_device, {"interface": []}, "switchport"),
    (refresh_svi_for_device, {"interface": []}, "svi"),
    (refresh_subinterface_for_device, {"interface": []}, "subinterface"),
    (refresh_interface_mtu_for_device, {"interface": []}, "interface-mtu"),
]

_IDS = [fn.__name__ for fn, _, _ in _NORMALIZED]


@asynccontextmanager
async def _device_session(device_id: int):
    async for db in get_session():
        device = await db.get(Device, device_id)
        assert device is not None
        yield db, device
        return
    raise RuntimeError("no session")


@pytest.mark.anyio
@pytest.mark.parametrize(("refresh_fn", "ok_payload", "wire_name"), _NORMALIZED, ids=_IDS)
async def test_refresher_returns_false_on_swallowed_nso_error(adapter_client, refresh_fn, ok_payload, wire_name):
    """A swallowed NSO read error must surface as ``False`` (degraded surface), not ``None``."""
    device_id = await seed_device(nso_device_name="contract-err")
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_device_state_section.side_effect = httpx.ConnectError("boom")

        result = await refresh_fn(db, device, nso_client, refresh_source="poll")

        assert result is False


@pytest.mark.anyio
@pytest.mark.parametrize(("refresh_fn", "ok_payload", "wire_name"), _NORMALIZED, ids=_IDS)
async def test_refresher_returns_true_on_definitive_read(adapter_client, refresh_fn, ok_payload, wire_name):
    """A successful read (even of an empty surface) must return ``True``."""
    device_id = await seed_device(nso_device_name="contract-ok")
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_device_state_section.return_value = {"status": "ok", **ok_payload}

        result = await refresh_fn(db, device, nso_client, refresh_source="poll")

        assert result is True


@pytest.mark.anyio
@pytest.mark.parametrize(("refresh_fn", "ok_payload", "wire_name"), _NORMALIZED, ids=_IDS)
async def test_refresher_returns_true_on_no_nso_name_skip(adapter_client, refresh_fn, ok_payload, wire_name):
    """An intentional skip (no NSO device name) is not a failure → ``True``."""
    device_id = await seed_device(nso_device_name="contract-skip")
    async with _device_session(device_id) as (db, device):
        device.nso_device_name = None  # in-memory only; force the early skip branch
        nso_client = AsyncMock()

        result = await refresh_fn(db, device, nso_client, refresh_source="poll")

        assert result is True
        nso_client.get_device_state_section.assert_not_awaited()


def test_s3_flipped_families_read_the_envelope():
    """The READSEM S3 flip manifest — one line per family, extended per batch.

    A wire_name here means the family fetches its device-state envelope section
    (status-declared, not-ready self-healing); reverting the spec's wire_name
    reverts the family to the legacy container byte-for-byte.
    """
    from nso_adapter.core.bfd import BFD_SPEC
    from nso_adapter.core.bgp import BGP_SPEC
    from nso_adapter.core.interface_ip import INTERFACE_IP_SPEC
    from nso_adapter.core.interface_mtu import INTERFACE_MTU_SPEC
    from nso_adapter.core.isis import ISIS_SPEC
    from nso_adapter.core.l2_service import L2_SERVICE_SPEC
    from nso_adapter.core.lag_config import LAG_CONFIG_SPEC
    from nso_adapter.core.lag_topology import LAG_TOPOLOGY_SPEC
    from nso_adapter.core.logging_config import LOGGING_CONFIG_SPEC
    from nso_adapter.core.ospf import OSPF_SPEC
    from nso_adapter.core.route_policy import ROUTE_POLICY_SPEC
    from nso_adapter.core.snmp import SNMP_SPEC
    from nso_adapter.core.static_route import STATIC_ROUTE_SPEC
    from nso_adapter.core.subinterface import SUBINTERFACE_SPEC
    from nso_adapter.core.svi import SVI_SPEC
    from nso_adapter.core.vlan import SWITCHPORT_SPEC, VLAN_DATABASE_SPEC

    flipped = {
        spec.name: spec.wire_name
        for spec in (
            STATIC_ROUTE_SPEC,
            INTERFACE_IP_SPEC,
            SVI_SPEC,
            SUBINTERFACE_SPEC,
            INTERFACE_MTU_SPEC,
            LOGGING_CONFIG_SPEC,
            L2_SERVICE_SPEC,
            BFD_SPEC,
            ISIS_SPEC,
            OSPF_SPEC,
            LAG_TOPOLOGY_SPEC,
            LAG_CONFIG_SPEC,
            SNMP_SPEC,
            ROUTE_POLICY_SPEC,
            BGP_SPEC,
            VLAN_DATABASE_SPEC,
            SWITCHPORT_SPEC,
        )
    }
    assert flipped == {
        "static_route": "static-route",
        "interface_ip": "interface-ip",
        "svi": "svi",
        "subinterface": "subinterface",
        "interface_mtu": "interface-mtu",
        "logging": "logging-config",
        "l2_service": "l2-service",
        "bfd": "bfd-config",
        "isis": "isis-interface",
        "ospf": "ospf-config",
        "lag": "lag-topology",
        "lag_config": "lag-config",
        "snmp": "snmp-config",
        "route_policy": "route-policy",
        "bgp": "bgp-config",
        "vlan": "vlan-database",
        "switchport": "switchport",
    }
