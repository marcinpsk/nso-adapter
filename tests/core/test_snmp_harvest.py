# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Per-NED community extraction for brownfield harvest (pure functions).

Payload shapes mirror network-state-export's device-verified readers; the IOS
path is additionally exercised end-to-end in ``tests/api/test_api_secrets.py``.
"""

from nso_adapter.core.snmp_harvest import find_community, harvest_subpath
from nso_adapter.secrets.refs import secret_fingerprint


def test_harvest_subpath_per_ned():
    assert harvest_subpath("cisco-ios-cli-6.77") == "tailf-ned-cisco-ios:snmp-server/community"
    assert harvest_subpath("cisco-iosxr-cli-7.52") == "tailf-ned-cisco-ios-xr:snmp-server/community"
    assert harvest_subpath("juniper-junos-nc-4.19") == "junos:configuration/snmp/community"
    # timos gated until the live plaintext-vs-hash2 check; arcos has no handler at all
    assert harvest_subpath("timos-nc-9.1") is None
    assert harvest_subpath("arcos-nc-1.0") is None
    assert harvest_subpath("") is None


def test_find_community_iosxr_access_list_leaf():
    payload = {
        "tailf-ned-cisco-ios-xr:community": [
            {"name": "xr-comm", "RW": [None], "access-list": "MGMT-ACL"},
        ]
    }
    found = find_community("cisco-iosxr-cli-7.52", payload, secret_fingerprint("xr-comm"))
    assert found is not None
    assert (found.secret, found.access, found.acl) == ("xr-comm", "RW", "MGMT-ACL")


def test_find_community_junos_authorization():
    payload = {
        "junos:community": [
            {"name": "jn-ro"},  # no authorization leaf → read-only default
            {"name": "jn-rw", "authorization": "read-write"},
        ]
    }
    ro = find_community("juniper-junos-nc-4.19", payload, secret_fingerprint("jn-ro"))
    rw = find_community("juniper-junos-nc-4.19", payload, secret_fingerprint("jn-rw"))
    assert (ro.secret, ro.access, ro.acl) == ("jn-ro", "RO", None)
    assert (rw.secret, rw.access, rw.acl) == ("jn-rw", "RW", None)


def test_find_community_no_match_and_junk_entries():
    payload = {"tailf-ned-cisco-ios:community": [{"RO": [None]}, "junk", {"name": ""}]}
    assert find_community("cisco-ios-cli-6.77", payload, secret_fingerprint("absent")) is None
    assert find_community("cisco-ios-cli-6.77", {}, "x") is None
    assert find_community("cisco-ios-cli-6.77", {"other:leaf": 1}, "x") is None
