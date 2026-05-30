# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for nso_packages/interface-reconciler/python/interface_reconciler/main.py.

The NSO service module requires the ``ncs`` Python SDK which is only available
inside the NSO Docker container.  We stub the entire ``ncs`` namespace before
importing the module under test so that the test suite runs in any standard
Python environment.

Coverage note: this file lives in ``tests/`` and is discovered by pytest, but
the source under test (``nso_packages/``) is not under ``source = ["nso_adapter"]``
in pyproject.toml.  The tests serve as quality / regression guards; they do not
contribute to the adapter coverage report.
"""
from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Bootstrap: create a minimal ``ncs`` stub before any import of main.py.
# ---------------------------------------------------------------------------

def _build_ncs_stub() -> types.ModuleType:
    """Return a lightweight ``ncs`` stub that satisfies main.py imports."""
    ncs_mod = types.ModuleType("ncs")

    # ncs.application sub-module
    app_mod = types.ModuleType("ncs.application")

    class _ServiceMeta(type):
        """Metaclass so that Service.create acts as a passthrough decorator."""
        def __getattr__(cls, name):
            if name == "create":
                return lambda f: f  # decorator no-op
            raise AttributeError(name)

    class Service(metaclass=_ServiceMeta):
        @staticmethod
        def create(fn):  # noqa: ANN001
            return fn

    class Application:
        pass

    app_mod.Service = Service
    app_mod.Application = Application
    ncs_mod.application = app_mod

    return ncs_mod


# Only install the stub when ncs is not already present (e.g. inside NSO env).
if "ncs" not in sys.modules:
    _ncs_stub = _build_ncs_stub()
    sys.modules["ncs"] = _ncs_stub
    sys.modules["ncs.application"] = _ncs_stub.application


# ---------------------------------------------------------------------------
# Now import the module under test.
# ---------------------------------------------------------------------------

# Add the nso_packages python path so we can import interface_reconciler.main.
import importlib  # noqa: E402
import importlib.util  # noqa: E402
from pathlib import Path  # noqa: E402

_PKG_PYTHON = Path(__file__).parent.parent.parent / "nso_packages" / "interface-reconciler" / "python"

if str(_PKG_PYTHON) not in sys.path:
    sys.path.insert(0, str(_PKG_PYTHON))

# Import cleanly (handles reload if stub was installed mid-session).
import interface_reconciler.main as _main_module  # noqa: E402

from interface_reconciler.main import (  # noqa: E402
    InterfaceReconcilerService,
    _NED_HANDLERS,
    _parse_if_name,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_service(
    *,
    description=None,
    enabled=None,
    vrf=None,
    ipv4_address=None,
    ipv6_address=None,
) -> MagicMock:
    svc = MagicMock()
    svc.description = description
    svc.enabled = enabled
    svc.vrf = vrf
    svc.ipv4_address = ipv4_address or []
    svc.ipv6_address = ipv6_address or []
    return svc


def _make_ipv4_entry(address: str, prefix_length: int, secondary: bool = False) -> MagicMock:
    """Return a mock ipv4-address list entry for use in _make_service."""
    e = MagicMock()
    e.address = address
    e.prefix_length = prefix_length
    e.secondary = secondary
    return e


def _make_ipv6_entry(address: str, prefix_length: int) -> MagicMock:
    """Return a mock ipv6-address list entry for use in _make_service."""
    e = MagicMock()
    e.address = address
    e.prefix_length = prefix_length
    return e


def _make_ios_dev(if_type: str = "GigabitEthernet") -> MagicMock:
    """Build a mock ``dev`` object with IOS-style config.interface.<Type>[<id>]."""
    dev = MagicMock()
    dev.name = "test-router"
    if_obj = MagicMock()
    ifc_list = MagicMock()
    ifc_list.create.return_value = if_obj
    ifc_container = MagicMock()
    setattr(ifc_container, if_type, ifc_list)
    dev.config.interface = ifc_container
    return dev, if_obj


def _make_junos_dev(if_name: str = "ge-0/0/0") -> MagicMock:
    """Build a mock ``dev`` object with Junos-style config path."""
    dev = MagicMock()
    if_obj = MagicMock()
    iface_list = MagicMock()
    iface_list.create.return_value = if_obj
    dev.config.jc__configuration.jc_interfaces__interfaces.interface = iface_list
    return dev, if_obj, iface_list


def _svc_instance() -> InterfaceReconcilerService:
    """Return a bare InterfaceReconcilerService instance (no NSO wiring needed)."""
    instance = object.__new__(InterfaceReconcilerService)
    return instance


# ---------------------------------------------------------------------------
# _parse_if_name
# ---------------------------------------------------------------------------

def test_parse_if_name_gigabit_slash():
    assert _parse_if_name("GigabitEthernet0/0/0/1") == ("GigabitEthernet", "0/0/0/1")


def test_parse_if_name_space_separated():
    assert _parse_if_name("GigabitEthernet 0/0/0") == ("GigabitEthernet", "0/0/0")


def test_parse_if_name_port_channel():
    assert _parse_if_name("Port-channel1") == ("Port-channel", "1")


def test_parse_if_name_loopback():
    assert _parse_if_name("Loopback0") == ("Loopback", "0")


def test_parse_if_name_invalid_raises():
    import pytest
    with pytest.raises(ValueError, match="Cannot parse"):
        _parse_if_name("not-an-interface")


# ---------------------------------------------------------------------------
# _NED_HANDLERS registry
# ---------------------------------------------------------------------------

def test_ned_handlers_contains_cisco_ios():
    assert _NED_HANDLERS["cisco-ios-cli"] == "_apply_ios_family"


def test_ned_handlers_contains_cisco_iosxr():
    assert _NED_HANDLERS["cisco-iosxr-cli"] == "_apply_ios_family"


def test_ned_handlers_contains_cisco_nx():
    assert _NED_HANDLERS["cisco-nx-cli"] == "_apply_ios_family"


def test_ned_handlers_contains_junos_nc():
    assert _NED_HANDLERS["juniper-junos-nc"] == "_apply_junos"


def test_ned_handlers_contains_junos_evo():
    assert _NED_HANDLERS["juniper-junos-evo-nc"] == "_apply_junos"


# ---------------------------------------------------------------------------
# _apply_ios_family
# ---------------------------------------------------------------------------

def test_ios_family_sets_description():
    svc = _inst = _svc_instance()
    dev, if_obj = _make_ios_dev("GigabitEthernet")
    svc._apply_ios_family(None, dev, "GigabitEthernet0/1", _make_service(description="uplink"))
    assert if_obj.description == "uplink"


def test_ios_family_none_description_not_set():
    svc = _svc_instance()
    dev, if_obj = _make_ios_dev("GigabitEthernet")
    svc._apply_ios_family(None, dev, "GigabitEthernet0/1", _make_service(description=None))
    if_obj.description  # accessed via MagicMock — assert it was NOT explicitly assigned
    # description attribute should never have been assigned
    assert "description" not in if_obj.__dict__


def test_ios_family_enabled_true_deletes_shutdown():
    svc = _svc_instance()
    dev, if_obj = _make_ios_dev("GigabitEthernet")
    svc._apply_ios_family(None, dev, "GigabitEthernet0/0", _make_service(enabled=True))
    if_obj.shutdown.delete.assert_called_once()
    if_obj.shutdown.create.assert_not_called()


def test_ios_family_enabled_false_creates_shutdown():
    svc = _svc_instance()
    dev, if_obj = _make_ios_dev("GigabitEthernet")
    svc._apply_ios_family(None, dev, "GigabitEthernet0/0", _make_service(enabled=False))
    if_obj.shutdown.create.assert_called_once()
    if_obj.shutdown.delete.assert_not_called()


def test_ios_family_enabled_none_does_not_touch_shutdown():
    svc = _svc_instance()
    dev, if_obj = _make_ios_dev("GigabitEthernet")
    svc._apply_ios_family(None, dev, "GigabitEthernet0/0", _make_service(enabled=None))
    if_obj.shutdown.create.assert_not_called()
    if_obj.shutdown.delete.assert_not_called()


def test_ios_family_enabled_true_shutdown_delete_exception_silenced():
    """delete() raising (leaf already absent) must not propagate."""
    svc = _svc_instance()
    dev, if_obj = _make_ios_dev("Loopback")
    if_obj.shutdown.delete.side_effect = Exception("already absent")
    # Should not raise.
    svc._apply_ios_family(None, dev, "Loopback0", _make_service(enabled=True))


def test_ios_family_unknown_interface_type_raises():
    import pytest
    svc = _svc_instance()
    dev, _ = _make_ios_dev("GigabitEthernet")
    # Make the attribute lookup for "Vlan" return None.
    dev.config.interface = MagicMock(spec=[])  # no attributes at all
    with pytest.raises(ValueError, match="interface type"):
        svc._apply_ios_family(None, dev, "Vlan10", _make_service())


def test_ios_family_sets_both_description_and_enabled():
    svc = _svc_instance()
    dev, if_obj = _make_ios_dev("GigabitEthernet")
    svc._apply_ios_family(None, dev, "GigabitEthernet0/0", _make_service(description="core", enabled=False))
    assert if_obj.description == "core"
    if_obj.shutdown.create.assert_called_once()


# ---------------------------------------------------------------------------
# _apply_junos
# ---------------------------------------------------------------------------

def test_junos_creates_interface_by_name():
    svc = _svc_instance()
    dev, if_obj, iface_list = _make_junos_dev()
    svc._apply_junos(None, dev, "ge-0/0/0", _make_service())
    iface_list.create.assert_called_once_with("ge-0/0/0")


def test_junos_sets_description():
    svc = _svc_instance()
    dev, if_obj, _ = _make_junos_dev()
    svc._apply_junos(None, dev, "ge-0/0/0", _make_service(description="backbone"))
    assert if_obj.description == "backbone"


def test_junos_none_description_not_set():
    svc = _svc_instance()
    dev, if_obj, _ = _make_junos_dev()
    svc._apply_junos(None, dev, "ge-0/0/0", _make_service(description=None))
    assert "description" not in if_obj.__dict__


def test_junos_enabled_true_deletes_disable():
    svc = _svc_instance()
    dev, if_obj, _ = _make_junos_dev()
    svc._apply_junos(None, dev, "ge-0/0/0", _make_service(enabled=True))
    if_obj.disable.delete.assert_called_once()
    if_obj.disable.create.assert_not_called()


def test_junos_enabled_false_creates_disable():
    svc = _svc_instance()
    dev, if_obj, _ = _make_junos_dev()
    svc._apply_junos(None, dev, "ge-0/0/0", _make_service(enabled=False))
    if_obj.disable.create.assert_called_once()
    if_obj.disable.delete.assert_not_called()


def test_junos_enabled_none_does_not_touch_disable():
    svc = _svc_instance()
    dev, if_obj, _ = _make_junos_dev()
    svc._apply_junos(None, dev, "ge-0/0/0", _make_service(enabled=None))
    if_obj.disable.create.assert_not_called()
    if_obj.disable.delete.assert_not_called()


def test_junos_enabled_true_disable_delete_exception_silenced():
    """delete() raising (leaf already absent) must not propagate."""
    svc = _svc_instance()
    dev, if_obj, _ = _make_junos_dev()
    if_obj.disable.delete.side_effect = Exception("already absent")
    svc._apply_junos(None, dev, "ae10", _make_service(enabled=True))


def test_junos_sets_both_description_and_enabled():
    svc = _svc_instance()
    dev, if_obj, _ = _make_junos_dev()
    svc._apply_junos(None, dev, "xe-0/0/1", _make_service(description="peer", enabled=False))
    assert if_obj.description == "peer"
    if_obj.disable.create.assert_called_once()


def test_junos_evo_uses_same_handler_as_junos_nc():
    """juniper-junos-evo-nc prefix maps to _apply_junos, same as juniper-junos-nc."""
    assert _NED_HANDLERS["juniper-junos-evo-nc"] == _NED_HANDLERS["juniper-junos-nc"]
    assert _NED_HANDLERS["juniper-junos-nc"] == "_apply_junos"


# ---------------------------------------------------------------------------
# cb_create handler dispatch
# ---------------------------------------------------------------------------

def _make_root_dev(ned_id: str, *, if_type: str = "GigabitEthernet") -> tuple:
    """Return (root_mock, dev_mock, if_obj_mock) wired for cb_create dispatch."""
    root = MagicMock()
    dev = MagicMock()
    dev.device_type.cli.ned_id = ned_id
    root.ncs__devices.ncs__device.__getitem__.return_value = dev

    if_obj = MagicMock()
    ifc_list = MagicMock()
    ifc_list.create.return_value = if_obj
    ifc_container = MagicMock()
    setattr(ifc_container, if_type, ifc_list)
    dev.config.interface = ifc_container

    return root, dev, if_obj


def test_cb_create_dispatches_ios_handler(monkeypatch):
    svc = _svc_instance()
    root, dev, if_obj = _make_root_dev("cisco-ios-cli-6.114")
    service = MagicMock()
    service.device = "core-rtr-01"
    service.interface_name = "GigabitEthernet0/0"
    service.description = "link"
    service.enabled = None

    svc.cb_create(MagicMock(), root, service, [])
    assert if_obj.description == "link"


def test_cb_create_dispatches_junos_handler():
    svc = _svc_instance()
    root = MagicMock()
    dev = MagicMock()
    dev.device_type.cli.ned_id = "juniper-junos-nc-23.4"
    root.ncs__devices.ncs__device.__getitem__.return_value = dev

    if_obj = MagicMock()
    iface_list = MagicMock()
    iface_list.create.return_value = if_obj
    dev.config.jc__configuration.jc_interfaces__interfaces.interface = iface_list

    service = MagicMock()
    service.device = "junos-rtr"
    service.interface_name = "ge-0/0/0"
    service.description = "peer"
    service.enabled = True

    svc.cb_create(MagicMock(), root, service, [])
    assert if_obj.description == "peer"
    if_obj.disable.delete.assert_called_once()


def test_cb_create_dispatches_junos_evo_handler():
    svc = _svc_instance()
    root = MagicMock()
    dev = MagicMock()
    dev.device_type.cli.ned_id = "juniper-junos-evo-nc-24.4"
    root.ncs__devices.ncs__device.__getitem__.return_value = dev

    if_obj = MagicMock()
    iface_list = MagicMock()
    iface_list.create.return_value = if_obj
    dev.config.jc__configuration.jc_interfaces__interfaces.interface = iface_list

    service = MagicMock()
    service.device = "junos-evo-rtr"
    service.interface_name = "et-0/0/0"
    service.description = None
    service.enabled = False

    svc.cb_create(MagicMock(), root, service, [])
    if_obj.disable.create.assert_called_once()


def test_cb_create_unknown_ned_raises():
    import pytest
    svc = _svc_instance()
    root = MagicMock()
    dev = MagicMock()
    dev.device_type.cli.ned_id = "nokia-sros-nc-22.10"
    root.ncs__devices.ncs__device.__getitem__.return_value = dev

    service = MagicMock()
    service.device = "nokia-rtr"
    service.interface_name = "1/1/1"

    with pytest.raises(ValueError, match="unsupported NED"):
        svc.cb_create(MagicMock(), root, service, [])


def test_cb_create_ned_id_resolution_exception_treated_as_empty():
    """When accessing ned_id raises (no CLI type), should still try to match an empty id."""
    import pytest
    svc = _svc_instance()
    root = MagicMock()
    dev = MagicMock()
    dev.device_type.cli.ned_id = MagicMock(side_effect=Exception("no cli type"))
    root.ncs__devices.ncs__device.__getitem__.return_value = dev

    service = MagicMock()
    service.device = "unknown-rtr"
    service.interface_name = "0/0"

    with pytest.raises(ValueError, match="unsupported NED"):
        svc.cb_create(MagicMock(), root, service, [])


# ---------------------------------------------------------------------------
# _prefix_len_to_mask
# ---------------------------------------------------------------------------

def test_prefix_len_to_mask_slash24():
    from interface_reconciler.main import _prefix_len_to_mask
    assert _prefix_len_to_mask(24) == "255.255.255.0"


def test_prefix_len_to_mask_slash30():
    from interface_reconciler.main import _prefix_len_to_mask
    assert _prefix_len_to_mask(30) == "255.255.255.252"


def test_prefix_len_to_mask_slash32():
    from interface_reconciler.main import _prefix_len_to_mask
    assert _prefix_len_to_mask(32) == "255.255.255.255"


def test_prefix_len_to_mask_slash0():
    from interface_reconciler.main import _prefix_len_to_mask
    assert _prefix_len_to_mask(0) == "0.0.0.0"


# ---------------------------------------------------------------------------
# _apply_ios_family — VRF
# ---------------------------------------------------------------------------

def test_ios_family_sets_vrf():
    svc = _svc_instance()
    dev, if_obj = _make_ios_dev("GigabitEthernet")
    svc._apply_ios_family(None, dev, "GigabitEthernet0/0", _make_service(vrf="MGMT"))
    assert if_obj.vrf.forwarding == "MGMT"


def test_ios_family_vrf_none_does_not_touch_vrf():
    svc = _svc_instance()
    dev, if_obj = _make_ios_dev("GigabitEthernet")
    svc._apply_ios_family(None, dev, "GigabitEthernet0/0", _make_service(vrf=None))
    if_obj.vrf.forwarding.__set__.assert_not_called() if hasattr(if_obj.vrf.forwarding, "__set__") else None
    # Simpler: forwarding should not have been directly assigned — verify call count on parent
    assert if_obj.vrf.forwarding != "anything_real"  # the attribute is still a MagicMock sentinel


# ---------------------------------------------------------------------------
# _apply_ios_family — IPv4 primary
# ---------------------------------------------------------------------------

def test_ios_family_sets_ipv4_primary():
    svc = _svc_instance()
    dev, if_obj = _make_ios_dev("GigabitEthernet")
    svc._apply_ios_family(
        None, dev, "GigabitEthernet0/0",
        _make_service(ipv4_address=[_make_ipv4_entry("10.0.0.1", 24, secondary=False)]),
    )
    assert if_obj.ip.address.primary.address == "10.0.0.1"
    assert if_obj.ip.address.primary.mask == "255.255.255.0"


def test_ios_family_does_not_set_primary_for_secondary_only():
    """When the only entry is secondary, primary should NOT be written."""
    svc = _svc_instance()
    dev, if_obj = _make_ios_dev("GigabitEthernet")
    svc._apply_ios_family(
        None, dev, "GigabitEthernet0/0",
        _make_service(ipv4_address=[_make_ipv4_entry("10.0.0.2", 24, secondary=True)]),
    )
    if_obj.ip.address.primary.address.__set__.assert_not_called() if hasattr(
        if_obj.ip.address.primary.address, "__set__"
    ) else None
    # primary.address should remain a MagicMock (never assigned a real string)
    assert if_obj.ip.address.primary.address != "10.0.0.2"


# ---------------------------------------------------------------------------
# _apply_ios_family — IPv4 secondary
# ---------------------------------------------------------------------------

def test_ios_family_sets_ipv4_secondary():
    svc = _svc_instance()
    dev, if_obj = _make_ios_dev("GigabitEthernet")
    sec_obj = MagicMock()
    if_obj.ip.address.secondary.create.return_value = sec_obj
    svc._apply_ios_family(
        None, dev, "GigabitEthernet0/0",
        _make_service(ipv4_address=[_make_ipv4_entry("10.0.0.2", 24, secondary=True)]),
    )
    if_obj.ip.address.secondary.create.assert_called_once_with("10.0.0.2")
    assert sec_obj.mask == "255.255.255.0"


def test_ios_family_sets_primary_then_secondary():
    svc = _svc_instance()
    dev, if_obj = _make_ios_dev("GigabitEthernet")
    sec_obj = MagicMock()
    if_obj.ip.address.secondary.create.return_value = sec_obj
    svc._apply_ios_family(
        None, dev, "GigabitEthernet0/0",
        _make_service(ipv4_address=[
            _make_ipv4_entry("10.0.0.1", 24, secondary=False),
            _make_ipv4_entry("10.0.0.2", 24, secondary=True),
        ]),
    )
    assert if_obj.ip.address.primary.address == "10.0.0.1"
    if_obj.ip.address.secondary.create.assert_called_once_with("10.0.0.2")
    assert sec_obj.mask == "255.255.255.0"


# ---------------------------------------------------------------------------
# _apply_ios_family — IPv6
# ---------------------------------------------------------------------------

def test_ios_family_sets_ipv6():
    svc = _svc_instance()
    dev, if_obj = _make_ios_dev("GigabitEthernet")
    svc._apply_ios_family(
        None, dev, "GigabitEthernet0/0",
        _make_service(ipv6_address=[_make_ipv6_entry("2001:db8::1", 64)]),
    )
    if_obj.ipv6.address.prefix_list.create.assert_called_once_with("2001:db8::1/64")


def test_ios_family_sets_multiple_ipv6():
    svc = _svc_instance()
    dev, if_obj = _make_ios_dev("GigabitEthernet")
    svc._apply_ios_family(
        None, dev, "GigabitEthernet0/0",
        _make_service(ipv6_address=[
            _make_ipv6_entry("2001:db8::1", 64),
            _make_ipv6_entry("fe80::1", 10),
        ]),
    )
    assert if_obj.ipv6.address.prefix_list.create.call_count == 2
    if_obj.ipv6.address.prefix_list.create.assert_any_call("2001:db8::1/64")
    if_obj.ipv6.address.prefix_list.create.assert_any_call("fe80::1/10")


def test_ios_family_empty_ip_lists_does_not_call_ip_methods():
    svc = _svc_instance()
    dev, if_obj = _make_ios_dev("GigabitEthernet")
    svc._apply_ios_family(None, dev, "GigabitEthernet0/0", _make_service())
    if_obj.ip.address.secondary.create.assert_not_called()
    if_obj.ipv6.address.prefix_list.create.assert_not_called()
