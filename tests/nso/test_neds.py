# SPDX-License-Identifier: Apache-2.0
"""Tests for NED helper functions in nso_adapter.nso.neds."""

from __future__ import annotations


def test_ned_family_cisco_ios():
    from nso_adapter.nso.neds import ned_family

    assert ned_family("cisco-ios-cli-6.95") == "ios"


def test_ned_family_cisco_iosxr():
    from nso_adapter.nso.neds import ned_family

    assert ned_family("cisco-iosxr-cli-7.55") == "iosxr"


def test_ned_family_cisco_nx():
    from nso_adapter.nso.neds import ned_family

    assert ned_family("cisco-nx-cli-5.23") == "nxos"


def test_ned_family_junos():
    from nso_adapter.nso.neds import ned_family

    assert ned_family("juniper-junos-nc-4.1") == "junos"


def test_ned_family_junos_evo():
    from nso_adapter.nso.neds import ned_family

    assert ned_family("juniper-junos-evo-nc-24.4") == "junos"


def test_ned_family_unknown_returns_none():
    from nso_adapter.nso.neds import ned_family

    assert ned_family("nokia-sros-nc-22.10") is None
    assert ned_family("arista-eos-cli-1.0") is None
    assert ned_family("") is None


def test_ned_family_bare_prefix_still_matches():
    """A NED ID with no version suffix should still match (some NSO contexts omit the version)."""
    from nso_adapter.nso.neds import ned_family

    assert ned_family("cisco-ios-cli") == "ios"
    assert ned_family("juniper-junos-nc") == "junos"
    assert ned_family("juniper-junos-evo-nc") == "junos"


# ── NED transport derivation (device-type for onboarding) ──────────────────────


def test_ned_transport_cli():
    from nso_adapter.nso.neds import ned_transport

    assert ned_transport("cisco-ios-cli-6.114") == "cli"
    assert ned_transport("cisco-iosxr-cli-7.76") == "cli"


def test_ned_transport_netconf():
    from nso_adapter.nso.neds import ned_transport

    assert ned_transport("juniper-junos-nc-4.19") == "netconf"
    assert ned_transport("juniper-junos-evo-nc-24.4") == "netconf"
    assert ned_transport("timos-nc-23.10") == "netconf"
    assert ned_transport("nokia-sros-nc-22.10") == "netconf"


def test_ned_transport_doubled_identityref_form():
    """Real NSO RESTCONF ned-ids are the doubled prefix:identity form."""
    from nso_adapter.nso.neds import ned_transport

    assert ned_transport("juniper-junos-nc-4.19:juniper-junos-nc-4.19") == "netconf"
    assert ned_transport("cisco-ios-cli-6.114:cisco-ios-cli-6.114") == "cli"


def test_ned_transport_generic_and_unknown():
    from nso_adapter.nso.neds import ned_transport

    assert ned_transport("acme-thing-gen-1.0") == "generic"
    assert ned_transport("no-protocol-token") is None
    assert ned_transport("") is None


def test_resolve_device_type_derives_from_ned_id():
    from nso_adapter.nso.neds import resolve_device_type

    # The bug: a netconf NED defaulting to device-type cli. Derivation fixes it.
    assert resolve_device_type("juniper-junos-nc-4.19") == "netconf"
    assert resolve_device_type("cisco-ios-cli-6.114") == "cli"


def test_resolve_device_type_rejects_contradicting_request():
    """ned_type='cli' for a '-nc-' NED is exactly how rd2 was mis-onboarded → refuse."""
    import pytest

    from nso_adapter.nso.neds import resolve_device_type

    with pytest.raises(ValueError, match="contradicts"):
        resolve_device_type("juniper-junos-nc-4.19", requested="cli")


def test_resolve_device_type_agreeing_request_ok():
    from nso_adapter.nso.neds import resolve_device_type

    assert resolve_device_type("juniper-junos-nc-4.19", requested="netconf") == "netconf"


def test_resolve_device_type_unknown_falls_back_to_request():
    from nso_adapter.nso.neds import resolve_device_type

    assert resolve_device_type("mystery-ned", requested="generic") == "generic"
    assert resolve_device_type("mystery-ned") == "cli"  # default when nothing known


# ── NED package component parsing (NED inventory) ──────────────────────────────


def _ned_pkg(ned_id, vendor="Cisco", os_list=None):
    """A packages/package dict with a NED component (RESTCONF shape)."""
    return {
        "name": ned_id,
        "package-version": "1.0",
        "oper-status": {"up": [None]},
        "component": [
            {
                "name": "x",
                "ned": {
                    "cli": {"ned-id": ned_id},
                    "device": {
                        "vendor": vendor,
                        "operating-system": os_list or [],
                    },
                },
            },
        ],
    }


def test_extract_ned_component_cli():
    from nso_adapter.nso.neds import extract_ned_component

    out = extract_ned_component(_ned_pkg("cisco-ios-cli-6.114", os_list=["IOS"])["component"])
    assert out is not None
    ned_id, meta = out
    assert ned_id == "cisco-ios-cli-6.114"
    assert meta["vendor"] == "Cisco"
    assert meta["operating-system"] == ["IOS"]


def test_extract_ned_component_netconf():
    from nso_adapter.nso.neds import extract_ned_component

    comp = [{"name": "n", "ned": {"netconf": {"ned-id": "juniper-junos-nc-23.4"}, "device": {"vendor": "Juniper"}}}]
    out = extract_ned_component(comp)
    assert out[0] == "juniper-junos-nc-23.4"


def test_extract_ned_component_service_package_is_none():
    from nso_adapter.nso.neds import extract_ned_component

    # An application/callback component (e.g. our reconcilers) has no `ned` → None.
    comp = [{"name": "bgp-reconciler", "application": {"python-class-name": "x"}}]
    assert extract_ned_component(comp) is None


def test_extract_ned_component_non_list_is_none():
    from nso_adapter.nso.neds import extract_ned_component

    assert extract_ned_component(None) is None
    assert extract_ned_component({}) is None


def test_ned_oper_status_container_up():
    from nso_adapter.nso.neds import _ned_oper_status

    assert _ned_oper_status({"up": [None]}) == "up"
    assert _ned_oper_status({"error": {}}) == "error"


def test_ned_oper_status_fallbacks():
    from nso_adapter.nso.neds import _ned_oper_status

    assert _ned_oper_status("up") == "up"
    assert _ned_oper_status(None) == "unknown"
    assert _ned_oper_status({}) == "unknown"
