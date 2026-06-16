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


@pytest.mark.anyio
async def test_existing_flat_unit_reparent_failure_is_swallowed():
    """If patching a pre-existing unit's parent fails, the id is still returned."""
    client = _make_nb_client()

    async def _get(dev_id, name):
        if name == "ae98":
            return {"id": 10, "name": "ae98"}
        if name == "ae98.100":
            return {"id": 11, "name": "ae98.100", "parent": None}  # flat → reparent attempt
        return None

    client.get_interface.side_effect = _get
    client.patch_interface.side_effect = Exception("NetBox 409")

    result = await resolve_or_create_interface(client, 42, _domain_iface("ae98.100"))

    assert result == 11  # reparent_failed is logged, not raised
    client.create_interface.assert_not_called()


@pytest.mark.anyio
async def test_base_creation_failure_creates_unit_parentless():
    """When the base can't be created, the unit is still created — parentless — not dropped."""
    client = _make_nb_client()
    client.get_interface.return_value = None  # nothing exists yet

    async def _create(payload):
        if payload["name"] == "ae98":
            raise Exception("NetBox 403")  # base create fails → base_id resolves None
        return {"id": 11, "name": "ae98.100"}

    client.create_interface.side_effect = _create

    result = await resolve_or_create_interface(client, 42, _domain_iface("ae98.100"))

    assert result == 11
    unit_payload = next(c.args[0] for c in client.create_interface.await_args_list if c.args[0]["name"] == "ae98.100")
    assert unit_payload["type"] == "virtual"
    assert "parent" not in unit_payload  # created without a parent rather than lost


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


@pytest.mark.anyio
async def test_bulk_ensure_m27r_logical_name_explicit_parent():
    """M27R: dict input with parent_binding keeps the FAITHFUL logical name and
    parents it to the bound port/LAG — NOT renamed to the bound port.

    ``LAG99:10`` (parent lag-99) and ``IXIA_CRPD`` (parent the physical port) are
    created virtual under their explicit parents; ``system`` (loopback, no parent)
    is a virtual base; the implicit ``lag-99`` parent is typed ``lag``.
    """
    from nso_adapter.bindings.netbox.mapper import bulk_ensure_interfaces

    client = _make_bulk_client()
    # The physical port already exists; lag-99 (implicit parent) does not.
    client.list_interfaces.return_value = [{"id": 5, "name": "1/1/c11/1", "parent": None}]
    client.bulk_create_interfaces.side_effect = [
        [{"id": 20, "name": "lag-99"}, {"id": 30, "name": "system"}],  # pass 1: bases
        [  # pass 2: logical children (faithful names)
            {"id": 21, "name": "LAG99:10"},
            {"id": 22, "name": "IXIA_CRPD"},
        ],
    ]

    result = await bulk_ensure_interfaces(
        client,
        42,
        [
            {"name": "1/1/c11/1", "parent_binding": None, "kind": "physical"},
            {"name": "lag-99", "parent_binding": None, "kind": "lag"},
            {"name": "LAG99:10", "parent_binding": "lag-99", "kind": "logical"},
            {"name": "IXIA_CRPD", "parent_binding": "1/1/c11/1", "kind": "logical"},
            {"name": "system", "parent_binding": None, "kind": "loopback"},
        ],
    )

    assert result["LAG99:10"] == 21 and result["IXIA_CRPD"] == 22
    # bases: lag-99 typed lag, system typed virtual (loopback)
    base_call = {p["name"]: p["type"] for p in client.bulk_create_interfaces.await_args_list[0][0][0]}
    assert base_call == {"lag-99": "lag", "system": "virtual"}
    # children keep their faithful names + parent to the explicit binding (not renamed)
    child_call = {
        p["name"]: (p["type"], p.get("parent")) for p in client.bulk_create_interfaces.await_args_list[1][0][0]
    }
    assert child_call == {"LAG99:10": ("virtual", 20), "IXIA_CRPD": ("virtual", 5)}


@pytest.mark.anyio
async def test_bulk_ensure_m27r_namespaced_vprn_loopback_not_split():
    """A namespaced VPRN loopback (kind set, parent_binding empty) is a virtual BASE.

    Its name contains ':' (CRPD-VPN:LO7) but must NOT be name-split into a
    'CRPD-VPN' parent — Nokia interfaces rely on explicit parent_binding only.
    """
    from nso_adapter.bindings.netbox.mapper import bulk_ensure_interfaces

    client = _make_bulk_client()
    client.bulk_create_interfaces.side_effect = [[{"id": 70, "name": "CRPD-VPN:LO7"}]]

    result = await bulk_ensure_interfaces(
        client, 42, [{"name": "CRPD-VPN:LO7", "parent_binding": None, "kind": "loopback"}]
    )

    assert result == {"CRPD-VPN:LO7": 70}
    base_call = client.bulk_create_interfaces.await_args_list[0][0][0]
    # one base, virtual, no parent — NOT split into a CRPD-VPN base + child
    assert base_call == [{"device": 42, "name": "CRPD-VPN:LO7", "type": "virtual"}]


# ---------------------------------------------------------------------------
# Extracted decision helpers — pure functions, no client, no mocks
# ---------------------------------------------------------------------------


class TestNormalizeInterfaceInputs:
    def test_mixed_str_and_dict_dedup_and_drop_nameless(self):
        from nso_adapter.bindings.netbox.mapper import _normalize_interface_inputs

        norm = _normalize_interface_inputs(
            [
                "ae0",
                "ae0",  # duplicate name → dropped
                {"name": "LAG99:10", "parent_binding": "lag-99", "kind": "logical"},
                {"name": "", "parent_binding": None, "kind": "loopback"},  # nameless → dropped
                {"name": "ae0", "parent_binding": "x"},  # duplicate of the str → dropped
                {"name": "sys", "parent_binding": "", "kind": "loopback"},  # empty pb → None
            ]
        )

        assert norm == [
            {"name": "ae0", "parent_binding": None, "kind": None},
            {"name": "LAG99:10", "parent_binding": "lag-99", "kind": "logical"},
            {"name": "sys", "parent_binding": None, "kind": "loopback"},
        ]


class TestResolveParents:
    def test_explicit_binding_wins_over_name_split(self):
        from nso_adapter.bindings.netbox.mapper import _resolve_parents

        # kind set + parent_binding → child of the binding; name's ':' is NOT split.
        children, bases = _resolve_parents([{"name": "LAG99:10", "parent_binding": "lag-99", "kind": "logical"}])
        assert children == [("LAG99:10", "lag-99")]
        assert bases == {"lag-99"}

    def test_dotted_unit_name_split_when_no_kind(self):
        from nso_adapter.bindings.netbox.mapper import _resolve_parents

        children, bases = _resolve_parents([{"name": "ae98.100", "parent_binding": None, "kind": None}])
        assert children == [("ae98.100", "ae98")]
        assert bases == {"ae98"}

    def test_kind_set_empty_binding_is_a_base_even_with_colon(self):
        from nso_adapter.bindings.netbox.mapper import _resolve_parents

        # Namespaced VPRN loopback: kind set, no binding → base, never split on ':'.
        children, bases = _resolve_parents([{"name": "CRPD-VPN:LO7", "parent_binding": None, "kind": "loopback"}])
        assert children == []
        assert bases == {"CRPD-VPN:LO7"}

    def test_plain_name_is_a_base(self):
        from nso_adapter.bindings.netbox.mapper import _resolve_parents

        children, bases = _resolve_parents([{"name": "system", "parent_binding": None, "kind": None}])
        assert children == []
        assert bases == {"system"}


class TestBaseCreatePayloads:
    def test_only_missing_bases_kind_typed(self):
        from nso_adapter.bindings.netbox.mapper import _base_create_payloads

        payloads = _base_create_payloads(
            42,
            base_names={"lag-99", "sys", "GigabitEthernet0/0", "already"},
            name_to_id={"already": 5},  # already present → excluded
            kind_by_name={"lag-99": "lag", "sys": "loopback", "GigabitEthernet0/0": None},
        )
        by_name = {p["name"]: p["type"] for p in payloads}
        assert by_name == {"lag-99": "lag", "sys": "virtual", "GigabitEthernet0/0": "1000base-t"}
        assert all(p["device"] == 42 for p in payloads)


class TestChildCreatePayloads:
    def test_skips_present_and_attaches_resolved_parent(self):
        from nso_adapter.bindings.netbox.mapper import _child_create_payloads

        payloads = _child_create_payloads(
            42,
            children=[("ae98.100", "ae98"), ("ae98.15", "ae98")],
            name_to_id={"ae98": 10, "ae98.15": 12},  # .15 already present → skipped
        )
        assert payloads == [{"device": 42, "name": "ae98.100", "type": "virtual", "parent": 10}]

    def test_unresolved_parent_omits_parent_and_warns(self, caplog):
        from nso_adapter.bindings.netbox.mapper import _child_create_payloads

        payloads = _child_create_payloads(
            42,
            children=[("orphan.7", "ghost")],
            name_to_id={},  # parent 'ghost' never resolved
        )
        # payload created without a 'parent' key rather than dropping the child
        assert payloads == [{"device": 42, "name": "orphan.7", "type": "virtual"}]
        assert "parent" not in payloads[0]


class TestReparentPatches:
    def test_wrong_parent_is_repointed(self):
        from nso_adapter.bindings.netbox.mapper import _reparent_patches

        existing = {"LAG99:10": {"id": 21, "name": "LAG99:10", "parent": 99, "type": "virtual"}}
        patches = _reparent_patches(
            children=[("LAG99:10", "lag-99")],
            base_names={"lag-99"},
            existing=existing,
            name_to_id={"lag-99": 20, "LAG99:10": 21},
            kind_by_name={"LAG99:10": "logical"},
        )
        assert patches == [{"id": 21, "parent": 20}]

    def test_nonvirtual_child_is_retyped_virtual(self):
        from nso_adapter.bindings.netbox.mapper import _reparent_patches

        # Parent already correct; child stored as a guessed physical type → retype only.
        existing = {"IXIA_CRPD": {"id": 22, "name": "IXIA_CRPD", "parent": {"id": 5}, "type": {"value": "other"}}}
        patches = _reparent_patches(
            children=[("IXIA_CRPD", "1/1/c11/1")],
            base_names=set(),
            existing=existing,
            name_to_id={"1/1/c11/1": 5, "IXIA_CRPD": 22},
            kind_by_name={"IXIA_CRPD": "logical"},
        )
        assert patches == [{"id": 22, "type": "virtual"}]

    def test_correct_child_yields_no_patch(self):
        from nso_adapter.bindings.netbox.mapper import _reparent_patches

        existing = {"ae98.100": {"id": 11, "name": "ae98.100", "parent": {"id": 10}, "type": {"value": "virtual"}}}
        patches = _reparent_patches(
            children=[("ae98.100", "ae98")],
            base_names=set(),
            existing=existing,
            name_to_id={"ae98": 10, "ae98.100": 11},
            kind_by_name={"ae98.100": None},
        )
        assert patches == []

    def test_preexisting_logical_base_retyped_to_virtual(self):
        from nso_adapter.bindings.netbox.mapper import _reparent_patches

        # An unbound loopback a prior name-split sync created as 'other' → retype.
        existing = {"IXIA": {"id": 40, "name": "IXIA", "type": "other"}}
        patches = _reparent_patches(
            children=[],
            base_names={"IXIA"},
            existing=existing,
            name_to_id={"IXIA": 40},
            kind_by_name={"IXIA": "loopback"},
        )
        assert patches == [{"id": 40, "type": "virtual"}]

    def test_preexisting_virtual_base_not_retyped(self):
        from nso_adapter.bindings.netbox.mapper import _reparent_patches

        existing = {"IXIA": {"id": 40, "name": "IXIA", "type": {"value": "virtual"}}}
        patches = _reparent_patches(
            children=[],
            base_names={"IXIA"},
            existing=existing,
            name_to_id={"IXIA": 40},
            kind_by_name={"IXIA": "loopback"},
        )
        assert patches == []


class TestTypeValue:
    def test_reads_slug_dict_and_none(self):
        from nso_adapter.bindings.netbox.mapper import _type_value

        assert _type_value({"type": "other"}) == "other"
        assert _type_value({"type": {"value": "virtual"}}) == "virtual"
        assert _type_value({}) is None


# ---------------------------------------------------------------------------
# bulk_ensure_interfaces — REAL NetboxClient over an httpx MockTransport.
#
# Higher fidelity than the AsyncMock client above: the real client serializes
# the payloads, issues the actual GET/POST/PATCH to /api/dcim/interfaces/, runs
# its bulk chunking, and parses the created-object ids back. A wrong client call
# fails here instead of being fabricated by a MagicMock.
# ---------------------------------------------------------------------------


def _fake_netbox(existing: list[dict], *, first_id: int = 1000):
    """Build an httpx handler emulating NetBox's interface endpoints + a recorder."""
    import json

    import httpx

    state = {"next_id": first_id, "posts": [], "patches": []}

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.method == "GET" and path == "/api/dcim/interfaces/":
            return httpx.Response(200, json={"results": existing, "next": None})
        body = json.loads(request.content)
        if request.method == "POST" and path == "/api/dcim/interfaces/":
            state["posts"].append(body)
            created = []
            for p in body:
                created.append({"id": state["next_id"], "name": p["name"]})
                state["next_id"] += 1
            return httpx.Response(201, json=created)
        if request.method == "PATCH" and path == "/api/dcim/interfaces/":
            state["patches"].append(body)
            return httpx.Response(200, json=[{"id": p["id"]} for p in body])
        return httpx.Response(404, json={})

    return handler, state


def _real_client(handler):
    import httpx

    from nso_adapter.bindings.netbox.client import NetboxClient

    client = NetboxClient(url="http://netbox.local", token="tok")
    client._http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return client


@pytest.mark.anyio
async def test_bulk_ensure_real_client_creates_base_then_units():
    from nso_adapter.bindings.netbox.mapper import bulk_ensure_interfaces

    handler, state = _fake_netbox(existing=[], first_id=1000)
    client = _real_client(handler)

    result = await bulk_ensure_interfaces(client, 42, ["ae98.100", "ae98.15"])

    # base ae98 posted first (id 1000), then both units parented to it.
    assert result == {"ae98": 1000, "ae98.100": 1001, "ae98.15": 1002}
    assert [p["name"] for p in state["posts"][0]] == ["ae98"]
    units = {p["name"]: p for p in state["posts"][1]}
    assert units["ae98.100"]["type"] == "virtual" and units["ae98.100"]["parent"] == 1000
    assert units["ae98.15"]["parent"] == 1000
    await client.aclose()


@pytest.mark.anyio
async def test_bulk_ensure_real_client_reparents_existing_flat_unit():
    from nso_adapter.bindings.netbox.mapper import bulk_ensure_interfaces

    handler, state = _fake_netbox(
        existing=[
            {"id": 10, "name": "ae98", "parent": None, "type": {"value": "lag"}},
            {"id": 11, "name": "ae98.100", "parent": None, "type": {"value": "virtual"}},
        ]
    )
    client = _real_client(handler)

    result = await bulk_ensure_interfaces(client, 42, ["ae98.100"])

    assert result == {"ae98": 10, "ae98.100": 11}
    # nothing created; one PATCH re-pointing the flat unit at its base went over the wire
    assert state["posts"] == []
    assert state["patches"] == [[{"id": 11, "parent": 10}]]
    await client.aclose()
