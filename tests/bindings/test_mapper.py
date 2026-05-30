# SPDX-License-Identifier: Apache-2.0
"""Tests for bindings/netbox/mapper.py — _guess_netbox_type and resolve_or_create_interface."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from nso_adapter.bindings.netbox.mapper import _guess_netbox_type, resolve_or_create_interface
from nso_adapter.domain.models import Interface as DomainInterface


# ---------------------------------------------------------------------------
# _guess_netbox_type — pure function tests
# ---------------------------------------------------------------------------

class TestGuessNetboxType:
    def test_gigabit_ethernet(self):
        assert _guess_netbox_type("GigabitEthernet0/0") == "1000base-t"

    def test_ten_gige(self):
        assert _guess_netbox_type("TenGigE0/0/0/1") == "10gbase-x-sfpp"

    def test_ten_gigabit_ethernet(self):
        assert _guess_netbox_type("TenGigabitEthernet0/0/0") == "10gbase-x-sfpp"

    def test_hundred_gige(self):
        assert _guess_netbox_type("HundredGigE0/0/1/0") == "100gbase-x-cfp"

    def test_forty_gigabit_ethernet(self):
        assert _guess_netbox_type("FortyGigabitEthernet1/0/1") == "40gbase-x-qsfpp"

    def test_loopback_capital(self):
        assert _guess_netbox_type("Loopback0") == "virtual"

    def test_loopback_lower(self):
        assert _guess_netbox_type("loopback0") == "virtual"

    def test_bundle_ether(self):
        assert _guess_netbox_type("Bundle-Ether100") == "lag"

    def test_port_channel(self):
        assert _guess_netbox_type("Port-channel1") == "lag"

    def test_vlan(self):
        assert _guess_netbox_type("Vlan10") == "virtual"

    def test_tunnel(self):
        assert _guess_netbox_type("Tunnel1") == "virtual"

    def test_management(self):
        assert _guess_netbox_type("Management0") == "other"

    def test_mgmt_eth(self):
        assert _guess_netbox_type("MgmtEth0/0/CPU0/0") == "other"

    def test_serial(self):
        assert _guess_netbox_type("Serial0/0/0") == "other"

    def test_unknown_falls_back_to_other(self):
        assert _guess_netbox_type("Ethernet1/1") == "other"


# ---------------------------------------------------------------------------
# resolve_or_create_interface — async tests
# ---------------------------------------------------------------------------

def _make_nb_client():
    client = MagicMock()
    client._base = "http://netbox"
    client.get_interface = AsyncMock()
    client.create_interface = AsyncMock()
    return client


def _domain_iface(name="GigabitEthernet0/0") -> DomainInterface:
    iface = MagicMock(spec=DomainInterface)
    iface.name = name
    return iface


@pytest.mark.anyio
async def test_resolve_returns_existing_id():
    client = _make_nb_client()
    client.get_interface.return_value = {"id": 99, "name": "GigabitEthernet0/0"}

    result = await resolve_or_create_interface(client, 42, _domain_iface())

    assert result == 99
    client.create_interface.assert_not_called()


@pytest.mark.anyio
async def test_resolve_creates_missing_interface():
    client = _make_nb_client()
    client.get_interface.return_value = None  # not found
    client.create_interface.return_value = {"id": 55, "name": "GigabitEthernet0/0"}

    result = await resolve_or_create_interface(client, 42, _domain_iface())

    assert result == 55
    client.create_interface.assert_called_once()
    payload = client.create_interface.call_args[0][0]
    assert payload["name"] == "GigabitEthernet0/0"
    assert payload["device"] == 42
    assert payload["type"] == "1000base-t"


@pytest.mark.anyio
async def test_resolve_returns_none_on_create_failure():
    client = _make_nb_client()
    client.get_interface.return_value = None
    client.create_interface.side_effect = Exception("NetBox 403")

    result = await resolve_or_create_interface(client, 42, _domain_iface())

    assert result is None
