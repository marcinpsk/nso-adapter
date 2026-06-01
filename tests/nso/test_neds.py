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
