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
