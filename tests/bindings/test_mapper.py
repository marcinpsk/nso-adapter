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

    def test_nokia_lag(self):
        assert _guess_netbox_type("lag-99") == "lag"

    def test_nokia_system(self):
        assert _guess_netbox_type("system") == "virtual"

    def test_nokia_loopback(self):
        assert _guess_netbox_type("lo0") == "virtual"

    def test_nokia_mgmt_loopback_outranks_management(self):
        # "Management-lo" is more specific than "Management" → virtual, not other.
        assert _guess_netbox_type("Management-lo0") == "virtual"


# ---------------------------------------------------------------------------
# _split_unit — pure function tests
# ---------------------------------------------------------------------------


class TestSplitUnit:
    def test_dotted_unit(self):
        from nso_adapter.bindings.netbox.mapper import _split_unit

        assert _split_unit("ae98.100") == ("ae98", "100")

    def test_nokia_lag_channel(self):
        from nso_adapter.bindings.netbox.mapper import _split_unit

        assert _split_unit("lag-99:10") == ("lag-99", "10")

    def test_nokia_sap_on_channelized_port(self):
        from nso_adapter.bindings.netbox.mapper import _split_unit

        assert _split_unit("1/1/c22/1:4090") == ("1/1/c22/1", "4090")

    def test_plain_port_id_is_not_a_unit(self):
        from nso_adapter.bindings.netbox.mapper import _split_unit

        assert _split_unit("1/1/c8/1") is None  # slashes are never separators

    def test_plain_name_is_not_a_unit(self):
        from nso_adapter.bindings.netbox.mapper import _split_unit

        assert _split_unit("system") is None

    def test_mixed_separators_rejected(self):
        from nso_adapter.bindings.netbox.mapper import _split_unit

        assert _split_unit("lag-99:10.5") is None

    def test_multiple_colons_rejected(self):
        from nso_adapter.bindings.netbox.mapper import _split_unit

        assert _split_unit("a:b:c") is None

    def test_empty_side_rejected(self):
        from nso_adapter.bindings.netbox.mapper import _split_unit

        assert _split_unit("lag-99:") is None
        assert _split_unit(":10") is None


# ---------------------------------------------------------------------------
# resolve_or_create_interface — async tests
# ---------------------------------------------------------------------------


def _make_nb_client():
    client = MagicMock()
    client._base = "http://netbox"
    client.get_interface = AsyncMock()
    client.create_interface = AsyncMock()
    client.patch_interface = AsyncMock()
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


# ── logical-unit (subinterface) modeling ──────────────────────────────────────


@pytest.mark.anyio
async def test_dotted_unit_creates_base_and_virtual_child():
    """ae98.100 → base ae98 ensured, then unit created virtual + parent=base."""
    client = _make_nb_client()
    client.get_interface.return_value = None  # neither base nor unit exist yet
    client.create_interface.side_effect = [
        {"id": 10, "name": "ae98"},  # base created first
        {"id": 11, "name": "ae98.100"},  # then the unit
    ]

    result = await resolve_or_create_interface(client, 42, _domain_iface("ae98.100"))

    assert result == 11
    assert client.create_interface.await_count == 2
    base_payload = client.create_interface.await_args_list[0][0][0]
    unit_payload = client.create_interface.await_args_list[1][0][0]
    assert base_payload["name"] == "ae98"
    assert unit_payload["name"] == "ae98.100"
    assert unit_payload["type"] == "virtual"
    assert unit_payload["parent"] == 10


@pytest.mark.anyio
async def test_dotted_unit_existing_base_only_creates_unit():
    """If the base already exists, only the unit is created (parented to it)."""
    client = _make_nb_client()

    async def _get(dev_id, name):
        return {"id": 10, "name": "ae98", "parent": None} if name == "ae98" else None

    client.get_interface.side_effect = _get
    client.create_interface.return_value = {"id": 11, "name": "ae98.100"}

    result = await resolve_or_create_interface(client, 42, _domain_iface("ae98.100"))

    assert result == 11
    client.create_interface.assert_awaited_once()
    unit_payload = client.create_interface.await_args[0][0]
    assert unit_payload["parent"] == 10
    assert unit_payload["type"] == "virtual"


@pytest.mark.anyio
async def test_existing_flat_unit_gets_reparented():
    """A pre-existing unit with no parent is patched to point at the base."""
    client = _make_nb_client()

    async def _get(dev_id, name):
        if name == "ae98":
            return {"id": 10, "name": "ae98", "parent": None}
        if name == "ae98.100":
            return {"id": 11, "name": "ae98.100", "parent": None}  # flat, needs parent
        return None

    client.get_interface.side_effect = _get

    result = await resolve_or_create_interface(client, 42, _domain_iface("ae98.100"))

    assert result == 11
    client.create_interface.assert_not_called()
    client.patch_interface.assert_awaited_once_with(11, {"parent": 10})


@pytest.mark.anyio
async def test_already_parented_unit_not_repatched():
    """A unit that already has a parent is returned as-is (no patch)."""
    client = _make_nb_client()

    async def _get(dev_id, name):
        if name == "ae98":
            return {"id": 10, "name": "ae98"}
        if name == "ae98.100":
            return {"id": 11, "name": "ae98.100", "parent": 10}
        return None

    client.get_interface.side_effect = _get

    result = await resolve_or_create_interface(client, 42, _domain_iface("ae98.100"))

    assert result == 11
    client.patch_interface.assert_not_called()
    client.create_interface.assert_not_called()


@pytest.mark.anyio
async def test_nokia_portid_not_treated_as_unit():
    """Nokia port-ids contain '/' not '.', so they resolve as plain interfaces."""
    client = _make_nb_client()
    client.get_interface.return_value = {"id": 7, "name": "1/1/c11/1"}

    result = await resolve_or_create_interface(client, 42, _domain_iface("1/1/c11/1"))

    assert result == 7
    client.create_interface.assert_not_called()
    client.patch_interface.assert_not_called()


# ── bulk_ensure_interfaces (Layer A two-pass inventory) ───────────────────────


def _make_bulk_client():
    client = MagicMock()
    client.list_interfaces = AsyncMock(return_value=[])
    client.bulk_create_interfaces = AsyncMock(return_value=[])
    client.bulk_patch_interfaces = AsyncMock(return_value=[])
    return client


@pytest.mark.anyio
async def test_bulk_ensure_two_pass_bases_then_units():
    """Bases created first, then units as virtual with parent resolved BY NAME."""
    from nso_adapter.bindings.netbox.mapper import bulk_ensure_interfaces

    client = _make_bulk_client()
    # Nothing exists yet.
    client.bulk_create_interfaces.side_effect = [
        [{"id": 10, "name": "ae98"}],  # pass 1: base
        [{"id": 11, "name": "ae98.100"}, {"id": 12, "name": "ae98.15"}],  # pass 2: units
    ]

    result = await bulk_ensure_interfaces(client, 42, ["ae98.100", "ae98.15"])

    assert result == {"ae98": 10, "ae98.100": 11, "ae98.15": 12}
    # pass 1 payload = the base only
    base_call = client.bulk_create_interfaces.await_args_list[0][0][0]
    assert [p["name"] for p in base_call] == ["ae98"]
    # pass 2 payloads = units, virtual, parented to ae98's id (by name, =10)
    unit_call = client.bulk_create_interfaces.await_args_list[1][0][0]
    assert all(p["type"] == "virtual" and p["parent"] == 10 for p in unit_call)


@pytest.mark.anyio
async def test_bulk_ensure_skips_existing():
    """Already-present interfaces are not re-created; their ids come from list."""
    from nso_adapter.bindings.netbox.mapper import bulk_ensure_interfaces

    client = _make_bulk_client()
    client.list_interfaces.return_value = [
        {"id": 10, "name": "ae98", "parent": None},
        {"id": 11, "name": "ae98.100", "parent": 10},  # already parented
    ]

    result = await bulk_ensure_interfaces(client, 42, ["ae98", "ae98.100"])

    assert result == {"ae98": 10, "ae98.100": 11}
    client.bulk_create_interfaces.assert_not_called()
    client.bulk_patch_interfaces.assert_not_called()


@pytest.mark.anyio
async def test_bulk_ensure_reparents_flat_unit():
    """A pre-existing flat unit (no parent) is bulk-patched to its base."""
    from nso_adapter.bindings.netbox.mapper import bulk_ensure_interfaces

    client = _make_bulk_client()
    client.list_interfaces.return_value = [
        {"id": 10, "name": "ae98", "parent": None},
        {"id": 11, "name": "ae98.100", "parent": None},  # flat → needs reparent
    ]

    await bulk_ensure_interfaces(client, 42, ["ae98.100"])

    client.bulk_create_interfaces.assert_not_called()
    client.bulk_patch_interfaces.assert_awaited_once_with([{"id": 11, "parent": 10}])


@pytest.mark.anyio
async def test_bulk_ensure_base_auto_added_for_unit_only_request():
    """Requesting only a unit still ensures its base exists."""
    from nso_adapter.bindings.netbox.mapper import bulk_ensure_interfaces

    client = _make_bulk_client()
    client.bulk_create_interfaces.side_effect = [
        [{"id": 10, "name": "ae98"}],
        [{"id": 11, "name": "ae98.100"}],
    ]

    result = await bulk_ensure_interfaces(client, 42, ["ae98.100"])

    assert result["ae98"] == 10
    assert result["ae98.100"] == 11


@pytest.mark.anyio
async def test_bulk_ensure_nokia_channel_parented_under_lag():
    """Nokia ``lag-99:10`` is created virtual and parented to its ``lag-99`` base,
    with the LAG base typed ``lag`` — the bound_port channel modeling path."""
    from nso_adapter.bindings.netbox.mapper import bulk_ensure_interfaces

    client = _make_bulk_client()
    client.bulk_create_interfaces.side_effect = [
        [{"id": 20, "name": "lag-99"}],  # pass 1: base
        [{"id": 21, "name": "lag-99:10"}],  # pass 2: channel unit
    ]

    result = await bulk_ensure_interfaces(client, 42, ["lag-99:10"])

    assert result == {"lag-99": 20, "lag-99:10": 21}
    base_call = client.bulk_create_interfaces.await_args_list[0][0][0]
    assert base_call == [{"device": 42, "name": "lag-99", "type": "lag"}]
    unit_call = client.bulk_create_interfaces.await_args_list[1][0][0]
    assert unit_call == [{"device": 42, "name": "lag-99:10", "type": "virtual", "parent": 20}]


@pytest.mark.anyio
async def test_bulk_ensure_nokia_sap_parented_under_channelized_port():
    """A SAP ``1/1/c22/1:4090`` parents under the existing channelized port."""
    from nso_adapter.bindings.netbox.mapper import bulk_ensure_interfaces

    client = _make_bulk_client()
    client.list_interfaces.return_value = [{"id": 30, "name": "1/1/c22/1", "parent": None}]
    client.bulk_create_interfaces.side_effect = [[{"id": 31, "name": "1/1/c22/1:4090"}]]

    result = await bulk_ensure_interfaces(client, 42, ["1/1/c22/1", "1/1/c22/1:4090"])

    assert result == {"1/1/c22/1": 30, "1/1/c22/1:4090": 31}
    # base already existed → only the SAP unit is created, parented by name to 30.
    unit_call = client.bulk_create_interfaces.await_args_list[0][0][0]
    assert unit_call == [{"device": 42, "name": "1/1/c22/1:4090", "type": "virtual", "parent": 30}]
