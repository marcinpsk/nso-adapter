# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Existing IOS, Junos, Nokia, and ArcOS routing read payloads."""

BGP_ROUTERS_READ = [
    {
        "asn": "65100",
        "scope": [
            {
                "vrf": "",
                "address-family": [{"afi": "ipv4-unicast"}],
                "peer": [
                    {
                        "peer-address": "10.0.0.1",
                        "peer-address-family": [{"afi": "ipv4-unicast", "policy-in": "PIN", "policy-out": "POUT"}],
                    },
                    {
                        "peer-address": "10.0.0.2",
                        "peer-address-family": [{"afi": "ipv4-unicast", "routemap-in": "RIN", "prefixlist-out": "PLO"}],
                    },
                ],
            }
        ],
    }
]

ISIS_LEVEL_READ = {
    "status": "ok",
    "process": [{"process-tag": "0", "is-type": "level-2"}],
    "interface": [{"interface-name": "Gi0/1", "af": "ipv4", "circuit-type": "level-2"}],
}

OSPF_INSTANCES_READ = [{"process-id": "1"}, {"process-id": "2"}]

OSPF_INTERFACES_READ = [
    {"interface-name": "GigabitEthernet0/0", "process-id": "1", "area-id": "0"},
    {"interface-name": "GigabitEthernet0/0", "process-id": "2", "area-id": "0"},
]

ROUTE_POLICY_COMMUNITIES_READ = {
    "community-list": [
        {
            "name": "SCRUBBER",
            "invert-match": True,
            "entry": [
                {"sequence": 10, "action": "permit", "community": "no-export"},
                {"sequence": 20, "action": "permit", "community": "64500&.*&[0-4]"},
            ],
        },
        {"name": "PLAIN", "entry": [{"sequence": 10, "action": "permit", "community": "64500:100"}]},
    ]
}


BGP_ARCOS_CIPHERTEXT_READ = {
    "router": [
        {
            "asn": "64512",
            "scope": [
                {
                    "vrf": "",
                    "address-family": [{"afi": "ipv4-unicast"}, {"afi": "vpnv4-unicast"}],
                    "peer": [
                        {
                            "peer-address": "198.18.255.3",
                            "remote-as": "64512",
                            "enabled": True,
                            "password": "$8$secret==",
                            "source": "198.18.255.38",
                            "peer-address-family": [
                                {
                                    "afi": "vpnv4-unicast",
                                    "enabled": True,
                                    "routemap-in": "IMPORT",
                                    "routemap-out": "EXPORT",
                                }
                            ],
                        }
                    ],
                }
            ],
        }
    ],
}
