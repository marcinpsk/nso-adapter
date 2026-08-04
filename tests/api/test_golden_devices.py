# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Golden-body tests — the devices router (list / get / by-nso / onboard / provision / rekey).

S2 orchestration. Every device response is built from ``_device_out`` (8 always-present
keys, nullables emitted as null) plus additive keys the caller layers on:
  * ``list``    adds ``sync_state_summary`` (a ``dict[str,int]`` — managed_interfaces + one
                key per SyncState value);
  * ``get``     adds ``scope`` + ``last_job_id`` + ``failover`` (the failover sub-object emits
                all 13 keys, nullables as null);
  * ``by-nso``  adds ``scope`` + ``last_job_id`` (NO failover key at all);
  * ``onboard`` / ``rekey`` return the bare ``_device_out`` shape.

The single ``DeviceOut`` model carries the additive fields as unset-by-default optionals and
the endpoints use ``response_model_exclude_unset=True`` so an absent key stays absent — the
goldens below are the byte-level arbiter (they pass BEFORE and AFTER typing).
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from tests.conftest import VALID_TOKEN, seed_device, session

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

TS = datetime(2026, 6, 1, 10, 0, 0, tzinfo=UTC)
TS_Z = "2026-06-01T10:00:00Z"

# managed_interfaces + every SyncState value, all zero except the one we seed.
_ZERO_SUMMARY = {
    "managed_interfaces": 0,
    "imported": 0,
    "changed": 0,
    "error": 0,
    "unknown": 0,
    "accepted": 0,
    "deploying": 0,
    "in_sync": 0,
    "apply_failed": 0,
    "drifted": 0,
}


async def _seed_failover(device_id: int) -> None:
    from nso_adapter.store.models import DeviceFailover

    async with session() as db:
        db.add(
            DeviceFailover(
                device_id=device_id,
                active_address="primary",
                primary_ip="198.18.0.1",
                oob_ip="198.18.9.1",
                last_probe_result="up",
                last_probe_target="primary",
                last_probe_detail="ok",
                last_probe_at=TS,
                oob_healthy=True,
                oob_health_result="up",
                oob_health_detail="reachable",
                oob_health_checked_at=TS,
                last_switch_at=TS,
                manual_override=False,
            )
        )
        await db.commit()


async def _seed_job(device_id: int) -> int:
    from nso_adapter.store.models import Job, JobType

    async with session() as db:
        job = Job(job_type=JobType.sync, device_id=device_id, created_at=TS, updated_at=TS)
        db.add(job)
        await db.commit()
        return job.id
    raise AssertionError("unreachable")


async def _seed_managed_interface(device_id: int) -> None:
    """One interface carrying one accepted attr-state → summary managed_interfaces=1, accepted=1."""
    from nso_adapter.store.models import DbInterface, InterfaceAttrState, SyncState

    async with session() as db:
        iface = DbInterface(device_id=device_id, name="GigabitEthernet0/0", kind="physical")
        db.add(iface)
        await db.flush()
        db.add(InterfaceAttrState(interface_id=iface.id, attribute="description", sync_state=SyncState.accepted))
        await db.commit()


@pytest.mark.anyio
async def test_list_devices_golden(adapter_client):
    device_id = await seed_device(nso_device_name="dev-list", netbox_device_id=101, attributes=["description"])
    await _seed_managed_interface(device_id)

    body = (await adapter_client.get("/api/v1/devices", headers=AUTH)).json()

    assert body == [
        {
            "id": device_id,
            "nso_instance": "nso-dev",
            "nso_device_name": "dev-list",
            "netbox_device_id": 101,
            "source_epoch": 1,
            "mapping_status": "mapped",
            "last_sync_at": None,
            "last_sync_status": None,
            "degraded_surfaces": None,
            "sync_state_summary": {**_ZERO_SUMMARY, "managed_interfaces": 1, "accepted": 1},
        }
    ]


@pytest.mark.anyio
async def test_get_device_maximal_golden(adapter_client):
    device_id = await seed_device(nso_device_name="dev-max", netbox_device_id=102, attributes=["description", "mtu"])
    await _seed_failover(device_id)
    job_id = await _seed_job(device_id)

    body = (await adapter_client.get(f"/api/v1/devices/{device_id}", headers=AUTH)).json()

    assert body == {
        "id": device_id,
        "nso_instance": "nso-dev",
        "nso_device_name": "dev-max",
        "netbox_device_id": 102,
        "source_epoch": 1,
        "mapping_status": "mapped",
        "last_sync_at": None,
        "last_sync_status": None,
        "degraded_surfaces": None,
        "scope": {"attributes": ["description", "mtu"]},
        "last_job_id": job_id,
        "failover": {
            "active_address": "primary",
            "primary_ip": "198.18.0.1",
            "oob_ip": "198.18.9.1",
            "last_probe_result": "up",
            "last_probe_target": "primary",
            "last_probe_detail": "ok",
            "last_probe_at": TS_Z,
            "oob_healthy": True,
            "oob_health_result": "up",
            "oob_health_detail": "reachable",
            "oob_health_checked_at": TS_Z,
            "last_switch_at": TS_Z,
            "manual_override": False,
        },
    }


@pytest.mark.anyio
async def test_get_device_minimal_golden(adapter_client):
    """No failover row, no jobs, empty scope → failover:null, last_job_id:null, attributes:[]."""
    device_id = await seed_device(nso_device_name="dev-min", netbox_device_id=103, attributes=[])

    body = (await adapter_client.get(f"/api/v1/devices/{device_id}", headers=AUTH)).json()

    assert body == {
        "id": device_id,
        "nso_instance": "nso-dev",
        "nso_device_name": "dev-min",
        "netbox_device_id": 103,
        "source_epoch": 1,
        "mapping_status": "mapped",
        "last_sync_at": None,
        "last_sync_status": None,
        "degraded_surfaces": None,
        "scope": {"attributes": []},
        "last_job_id": None,
        "failover": None,
    }


@pytest.mark.anyio
async def test_get_device_by_nso_golden(adapter_client):
    """by-nso adds scope + last_job_id but NEVER a failover key (absent, not null)."""
    device_id = await seed_device(nso_device_name="dev-bynso", netbox_device_id=104, attributes=["description"])

    body = (
        await adapter_client.get(
            "/api/v1/devices/by-nso", params={"instance": "nso-dev", "name": "dev-bynso"}, headers=AUTH
        )
    ).json()

    assert body == {
        "id": device_id,
        "nso_instance": "nso-dev",
        "nso_device_name": "dev-bynso",
        "netbox_device_id": 104,
        "source_epoch": 1,
        "mapping_status": "mapped",
        "last_sync_at": None,
        "last_sync_status": None,
        "degraded_surfaces": None,
        "scope": {"attributes": ["description"]},
        "last_job_id": None,
    }


@pytest.mark.anyio
async def test_onboard_device_golden(adapter_client_with_nso):
    """POST /devices returns the bare _device_out shape (no additive keys), status 201."""
    resp = await adapter_client_with_nso.post(
        "/api/v1/devices",
        json={"nso_instance": "nso-dev", "nso_device_name": "dev-onboard", "netbox_device_id": 105},
        headers=AUTH,
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body == {
        "id": body["id"],
        "nso_instance": "nso-dev",
        "nso_device_name": "dev-onboard",
        "netbox_device_id": 105,
        "source_epoch": 1,
        "mapping_status": "mapped",
        "last_sync_at": None,
        "last_sync_status": None,
        "degraded_surfaces": None,
    }
    assert isinstance(body["id"], int)
