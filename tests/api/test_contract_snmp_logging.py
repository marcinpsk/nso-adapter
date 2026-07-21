# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Contract tests — producer side of GET /snmp-config and /logging-config.

SNMP emits a FIXED key set at every level (nullable values, but keys always present);
``system_info`` is either null or a fixed dict. Logging hosts OMIT optional keys when
unset. Consumed by the plugin in template_content._reconcile_snmp_config /
_reconcile_logging_config.

Canonical contract: ``docs/api-contract.md`` (SNMP §; logging §).
Mirror (consumer side): ``netbox-nso-plugin/.../tests/test_contract_snmp_logging.py`` —
the ``*_KEYS`` sets MUST stay identical across both files.
"""

from __future__ import annotations

import pytest

from tests.conftest import VALID_TOKEN, seed_device

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

SNMP_TOP_KEYS = {
    "device_id",
    "last_refreshed_at",
    "refresh_source",
    "read_state",
    "communities",
    "v3_users",
    "hosts",
    "system_info",
}
SNMP_COMMUNITY_KEYS = {"community_hash", "access", "acl"}
SNMP_V3USER_KEYS = {"username", "has_auth_secret", "has_priv_secret"}
# `username` = the SNMPv3 security user name (v3 hosts only). NOT a secret, and the field both
# NSO host writers KEY the receiver on — without it a v3 trap host cannot be pushed (CR-P16).
SNMP_HOST_KEYS = {"address", "version", "notify_type", "port", "username"}
SNMP_SYSINFO_KEYS = {"location", "contact"}
LOGGING_TOP_KEYS = {"device_id", "last_refreshed_at", "refresh_source", "read_state", "hosts"}
LOGGING_HOST_REQUIRED_KEYS = {"address"}
LOGGING_HOST_OPTIONAL_KEYS = {"port", "severity", "facility", "transport", "vrf", "source"}


@pytest.mark.anyio
async def test_snmp_config_contract(adapter_client):
    from datetime import datetime

    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import SnmpCommunity, SnmpHost, SnmpSystemInfo, SnmpV3User

    device_id = await seed_device(nso_device_name="snmp-ct", netbox_device_id=7960)
    ts = datetime(2026, 6, 1, 10, 0, 0)
    async for db in get_session():
        db.add(
            SnmpCommunity(
                device_id=device_id,
                community_hash="abc",
                access="RO",
                acl="ACL-1",
                last_refreshed_at=ts,
                refresh_source="poll",
            )
        )
        db.add(
            SnmpV3User(
                device_id=device_id,
                username="v3-test-group",
                has_auth_secret=True,
                has_priv_secret=False,
                last_refreshed_at=ts,
                refresh_source="poll",
            )
        )
        db.add(
            SnmpHost(
                device_id=device_id,
                address="10.0.0.9",
                version="2c",
                notify_type="trap",
                port=162,
                last_refreshed_at=ts,
                refresh_source="poll",
            )
        )
        db.add(
            SnmpSystemInfo(
                device_id=device_id, location="DC1", contact="noc@x", last_refreshed_at=ts, refresh_source="poll"
            )
        )
        await db.commit()
        break

    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/snmp-config", headers=AUTH)).json()
    assert set(body.keys()) == SNMP_TOP_KEYS
    assert set(body["communities"][0].keys()) == SNMP_COMMUNITY_KEYS
    assert set(body["v3_users"][0].keys()) == SNMP_V3USER_KEYS
    assert set(body["hosts"][0].keys()) == SNMP_HOST_KEYS
    assert set(body["system_info"].keys()) == SNMP_SYSINFO_KEYS


@pytest.mark.anyio
async def test_logging_config_contract(adapter_client):
    from datetime import datetime

    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import DeviceLoggingHost

    device_id = await seed_device(nso_device_name="log-ct", netbox_device_id=7961)
    ts = datetime(2026, 6, 1, 10, 0, 0)
    async for db in get_session():
        # Maximal host (every optional) + minimal host (only address).
        db.add(
            DeviceLoggingHost(
                device_id=device_id,
                address="10.0.0.5",
                port=514,
                severity="informational",
                facility="local7",
                transport="udp",
                vrf="MGMT",
                source="Loopback0",
                last_refreshed_at=ts,
                refresh_source="poll",
            )
        )
        db.add(DeviceLoggingHost(device_id=device_id, address="10.0.0.6", last_refreshed_at=ts, refresh_source="poll"))
        await db.commit()
        break

    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/logging-config", headers=AUTH)).json()
    assert set(body.keys()) == LOGGING_TOP_KEYS
    hosts = {h["address"]: h for h in body["hosts"]}
    assert set(hosts["10.0.0.5"].keys()) == LOGGING_HOST_REQUIRED_KEYS | LOGGING_HOST_OPTIONAL_KEYS
    assert set(hosts["10.0.0.6"].keys()) == LOGGING_HOST_REQUIRED_KEYS  # optionals omitted
