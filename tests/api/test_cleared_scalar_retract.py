# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Clearing an owned scalar must reach the device — on EVERY scope, not just IS-IS/OSPF.

A normal apply is a merge-PATCH (``_send_service_config`` picks ``method="patch"`` when
``replace=False``), and every writer emits its optional leaves only when they are set
(``if row.mtu is not None:``, ``if row.vrf:``). So a value that goes back to unset can
never be dropped by an apply — only a PUT-replace of the whole service reverts it, which
means the intent PUT must enqueue a ``removal`` job.

Every endpoint gated that on DELETED KEYS only (``if removed:``), so a pure clear enqueued
NOTHING: the operator blanked an MTU / a route metric / a syslog severity, the store
updated, the tab showed it cleared — and the device kept the old value forever. The next
reconcile then flipped the overlay to ``changed``, and re-clearing did nothing.

These drive the real FastAPI routes through the real PostgreSQL session. No NSO is needed: the
whole assertion is about which JOB the PUT queues, and with what semantics.

    detach absent  → the replace really networks (a clear is not an un-own: the row stays
                     owned and accepted, only the leaf was blanked)
    removed absent → nothing was un-owned, so the collateral guard has no allowance
"""

from __future__ import annotations

import pytest
from sqlalchemy import select

from tests.conftest import VALID_TOKEN, seed_device, session

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


async def _removal_jobs(device_id: int, scope: str):
    from nso_adapter.store.models import Job, JobType

    async with session() as db:
        jobs = (
            (await db.execute(select(Job).where(Job.device_id == device_id, Job.job_type == JobType.removal)))
            .scalars()
            .all()
        )
        return [j for j in jobs if (j.context or {}).get("scope") == scope]
    return []


# (scope, url suffix, body with the scalar SET, body with the scalar CLEARED)
#
# Each pair keeps the row's KEY identical — only an optional scalar goes away — so the old
# `if removed:` gate sees nothing at all.
_CASES = [
    (
        "interface_mtu",
        "interface-mtu-intent",
        {"interfaces": [{"interface_name": "Gi0/0", "mtu": 9000}]},
        {"interfaces": [{"interface_name": "Gi0/0"}]},
    ),
    (
        "static_route",
        "static-route-intent",
        {"routes": [{"vrf": "", "prefix": "10.9.0.0/24", "next_hop": "10.9.0.1", "metric": 20}]},
        {"routes": [{"vrf": "", "prefix": "10.9.0.0/24", "next_hop": "10.9.0.1"}]},
    ),
    (
        "bfd",
        "bfd-intent",
        {"interfaces": [{"interface_name": "Gi0/1", "min_tx": 300, "min_rx": 300, "multiplier": 3}]},
        {"interfaces": [{"interface_name": "Gi0/1"}]},
    ),
    (
        "logging",
        "logging-intent",
        {"hosts": [{"address": "10.9.1.1", "severity": "warning", "port": 514}]},
        {"hosts": [{"address": "10.9.1.1"}]},  # severity -> "" (NOT NULL default), port -> None
    ),
    (
        "l2_sap",
        "l2-sap-intent",
        {"saps": [{"service_name": "EPIPE-1", "service_type": "epipe", "sap_id": "1/1/1", "outer_tag": 100}]},
        {"saps": [{"service_name": "EPIPE-1", "service_type": "epipe", "sap_id": "1/1/1"}]},
    ),
    (
        "vlan",
        "vlan-intent",
        {"vlans": [{"vlan_id": 10, "name": "USERS"}]},
        {"vlans": [{"vlan_id": 10}]},  # name -> ""
    ),
    (
        "svi",
        "svi-intent",
        {"interfaces": [{"interface_name": "Vlan10", "vlan_id": 10, "vrf": "BLUE"}]},
        {"interfaces": [{"interface_name": "Vlan10", "vlan_id": 10}]},  # vrf -> ""
    ),
    (
        "subinterface",
        "subinterface-intent",
        {"interfaces": [{"interface_name": "Gi0/2.10", "parent_interface": "Gi0/2", "dot1q_vlan": 10, "vrf": "BLUE"}]},
        {"interfaces": [{"interface_name": "Gi0/2.10", "parent_interface": "Gi0/2", "dot1q_vlan": 10}]},
    ),
]


@pytest.mark.anyio
@pytest.mark.parametrize("scope, url, body_set, body_cleared", _CASES, ids=[c[0] for c in _CASES])
async def test_clearing_an_owned_scalar_retracts_for_real(adapter_client, scope, url, body_set, body_cleared):
    device_id = await seed_device(nso_device_name=f"clr-{scope}", netbox_device_id=abs(hash(scope)) % 10000)

    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/{url}", json=body_set, headers=AUTH)
    assert resp.status_code == 200
    assert await _removal_jobs(device_id, scope) == []  # setting a value is not a retraction

    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/{url}", json=body_cleared, headers=AUTH)
    assert resp.status_code == 200

    jobs = await _removal_jobs(device_id, scope)
    assert len(jobs) == 1, f"{scope}: clearing an owned scalar queued no removal — the device keeps the old value"
    assert jobs[0].context.get("detach") is None  # a real, networking replace
    assert not jobs[0].context.get("removed")  # nothing was un-owned


@pytest.mark.anyio
@pytest.mark.parametrize("scope, url, body_set, body_cleared", _CASES, ids=[c[0] for c in _CASES])
async def test_clearing_an_owned_scalar_under_store_only_touches_nothing(
    adapter_client, scope, url, body_set, body_cleared
):
    """The store-only re-sync re-pushes every scope and promises not to touch the device."""
    device_id = await seed_device(nso_device_name=f"clrso-{scope}", netbox_device_id=abs(hash(scope)) % 10000 + 1)

    await adapter_client.put(f"/api/v1/devices/{device_id}/{url}", json=body_set, headers=AUTH)
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/{url}?store_only=true", json=body_cleared, headers=AUTH
    )
    assert resp.status_code == 200
    assert await _removal_jobs(device_id, scope) == []


@pytest.mark.anyio
async def test_setting_a_value_from_blank_is_not_a_retraction(adapter_client):
    """None -> value is a GROW: a merge-PATCH carries it, so no PUT-replace is needed."""
    device_id = await seed_device(nso_device_name="clr-grow", netbox_device_id=9911)

    await adapter_client.put(
        f"/api/v1/devices/{device_id}/interface-mtu-intent",
        json={"interfaces": [{"interface_name": "Gi0/0"}]},
        headers=AUTH,
    )
    await adapter_client.put(
        f"/api/v1/devices/{device_id}/interface-mtu-intent",
        json={"interfaces": [{"interface_name": "Gi0/0", "mtu": 9000}]},
        headers=AUTH,
    )
    assert await _removal_jobs(device_id, "interface_mtu") == []


@pytest.mark.anyio
async def test_toggling_a_boolean_off_is_not_a_retraction(adapter_client):
    """True -> False is not a clear: the writers emit False explicitly, so the merge-PATCH
    carries it. Treating it as a clear would fire a real device PUT-replace on every
    toggle-off (see core.removal.is_cleared)."""
    device_id = await seed_device(nso_device_name="clr-bool", netbox_device_id=9912)
    route = {"vrf": "", "prefix": "10.9.5.0/24", "next_hop": "10.9.5.1"}

    await adapter_client.put(
        f"/api/v1/devices/{device_id}/static-route-intent",
        json={"routes": [{**route, "permanent": True}]},
        headers=AUTH,
    )
    await adapter_client.put(
        f"/api/v1/devices/{device_id}/static-route-intent",
        json={"routes": [{**route, "permanent": False}]},
        headers=AUTH,
    )
    assert await _removal_jobs(device_id, "static_route") == []
