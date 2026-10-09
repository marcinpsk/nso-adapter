# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Service read payloads shared by refresh and observation tests."""

BFD_INTERFACE_READ_PAYLOAD = {
    "interface": [
        {"interface-name": "ae10", "min-tx": 300, "min-rx": 300, "multiplier": 3, "micro-bfd": True, "enabled": True},
        {
            "interface-name": "lag-99",
            "bound-port": "lag-99",
            "min-tx": 100,
            "min-rx": 100,
            "multiplier": 3,
            "micro-bfd": False,
            "enabled": True,
        },
    ]
}

LOGGING_LEVELS_READ_PAYLOAD = {
    "host": [{"address": "10.0.0.1"}],
    "local-levels": {"console-severity": "CRITICAL", "monitor-severity": "NOTICE", "module-severity": "NOTICE"},
}

SNMP_COMMUNITIES_READ_PAYLOAD = {
    "community": [
        {"name": "abc123def456abcd", "access": "RO", "acl": "20", "has-secret": True},
        {"name": "def456abc123def4", "access": "RW", "has-secret": True},
    ]
}

SNMP_USERS_READ_PAYLOAD = {
    "v3-user": [
        {"username": "monitor", "has-auth-secret": True, "has-priv-secret": False},
        {"username": "placeholder-user", "has-auth-secret": True, "has-priv-secret": True},
    ]
}

SNMP_RECEIVERS_READ_PAYLOAD = {"host": [{"address": "10.0.1.100", "version": "2c", "notify-type": "trap", "port": 162}]}

SNMP_V3_RECEIVERS_READ_PAYLOAD = {
    "host": [{"address": "10.0.1.101", "version": "3", "notify-type": "inform", "user": "netmon-v3"}]
}

SNMP_SYSTEM_READ_PAYLOAD = {"location": "ITC-Lab", "contact": "noc@example.com"}

STATIC_ROUTES_READ_PAYLOAD = {
    "route": [
        {"vrf": "", "prefix": "10.0.0.0/8", "next-hop": "192.168.1.1"},
        {"vrf": "MGMT", "prefix": "0.0.0.0/0", "next-hop": "10.10.10.1", "metric": 1},
    ]
}
