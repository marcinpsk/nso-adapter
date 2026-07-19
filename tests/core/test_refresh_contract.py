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

# (refresher, NsoClient getter method, a minimal empty-but-valid success payload)
_NORMALIZED = [
    (refresh_interface_ips_for_device, "get_interface_ips", {"interface": []}),
    (refresh_lag_topology_for_device, "get_lag_topology", {"lag": []}),
    (refresh_lag_config_for_device, "get_lag_config", {"lag": []}),
    (refresh_l2_services_for_device, "get_l2_services", {"service": []}),
    (refresh_vlan_database_for_device, "get_vlan_database", {"vlan": []}),
    (refresh_switchport_for_device, "get_switchport", {"interface": []}),
    (refresh_svi_for_device, "get_svi", {"interface": []}),
    (refresh_subinterface_for_device, "get_subinterface", {"interface": []}),
    (refresh_interface_mtu_for_device, "get_interface_mtu", {"interface": []}),
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
@pytest.mark.parametrize(("refresh_fn", "getter", "ok_payload"), _NORMALIZED, ids=_IDS)
async def test_refresher_returns_false_on_swallowed_nso_error(adapter_client, refresh_fn, getter, ok_payload):
    """A swallowed NSO read error must surface as ``False`` (degraded surface), not ``None``."""
    device_id = await seed_device(nso_device_name="contract-err")
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        getattr(nso_client, getter).side_effect = httpx.ConnectError("boom")

        result = await refresh_fn(db, device, nso_client, refresh_source="poll")

        assert result is False


@pytest.mark.anyio
@pytest.mark.parametrize(("refresh_fn", "getter", "ok_payload"), _NORMALIZED, ids=_IDS)
async def test_refresher_returns_true_on_definitive_read(adapter_client, refresh_fn, getter, ok_payload):
    """A successful read (even of an empty surface) must return ``True``."""
    device_id = await seed_device(nso_device_name="contract-ok")
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        getattr(nso_client, getter).return_value = ok_payload

        result = await refresh_fn(db, device, nso_client, refresh_source="poll")

        assert result is True


@pytest.mark.anyio
@pytest.mark.parametrize(("refresh_fn", "getter", "ok_payload"), _NORMALIZED, ids=_IDS)
async def test_refresher_returns_true_on_no_nso_name_skip(adapter_client, refresh_fn, getter, ok_payload):
    """An intentional skip (no NSO device name) is not a failure → ``True``."""
    device_id = await seed_device(nso_device_name="contract-skip")
    async with _device_session(device_id) as (db, device):
        device.nso_device_name = None  # in-memory only; force the early skip branch
        nso_client = AsyncMock()

        result = await refresh_fn(db, device, nso_client, refresh_source="poll")

        assert result is True
        getattr(nso_client, getter).assert_not_awaited()
