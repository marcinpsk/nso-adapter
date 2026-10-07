# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Shared IOS, Junos, and Nokia switching read payloads."""

SWITCHING_READ_PAYLOADS = {
    "lag_config": {
        "lag": [
            {
                "name": "Port-channel1",
                "lag-id": 1,
                "min-links": 2,
                "system-priority": 100,
                "timer": "fast",
                "member": [
                    {"interface-name": "GigabitEthernet0/1", "mode": "active", "port-priority": 200},
                    {"interface-name": "GigabitEthernet0/2", "mode": "active"},
                ],
            },
            {"name": "Port-channel2", "lag-id": 2, "member": []},
        ]
    },
    "lag": {
        "lag": [
            {
                "name": "Port-channel1",
                "lag-id": 1,
                "member": [{"interface-name": "GigabitEthernet0/1", "mode": "active"}],
            },
            {"name": "Port-channel2", "lag-id": 2, "member": []},
        ]
    },
    "interface_mtu": {
        "interface": [
            {"interface-name": "Port-channel1", "mtu": 9216},
            {"interface-name": "Port-channel1.100", "ip-mtu": 9000},
            {"interface-name": "LAG99:99", "ip-mtu": 9170, "bound-port": "lag-99"},
        ]
    },
    "svi": {
        "interface": [
            {"interface-name": "Vlan100", "vlan-id": 100, "type": "svi", "vrf": "MGMT"},
            {"interface-name": "Vlan200", "vlan-id": 200, "type": "svi"},
        ]
    },
    "subinterface": {
        "interface": [
            {
                "interface-name": "GigabitEthernet0/1.100",
                "parent-interface": "GigabitEthernet0/1",
                "dot1q-vlan": 100,
                "type": "subinterface",
                "vrf": "TENANT_A",
            },
            {
                "interface-name": "ge-0/0/0.200",
                "parent-interface": "ge-0/0/0",
                "dot1q-vlan": 200,
                "type": "subinterface",
            },
        ]
    },
    "vlan": {"vlan": [{"vlan-id": 10, "name": "MGMT"}, {"vlan-id": 20, "name": "DATA"}]},
    "switchport": {
        "interface": [{"interface-name": "Gi0/1", "mode": "trunk", "untagged-vlan": 99, "tagged-vlans": "10,10,20"}]
    },
}
