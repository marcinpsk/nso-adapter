# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""HTTP behavior for the interface-attribute intent endpoints."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import select

from nso_adapter.store.models import (
    DbInterface,
    Device,
    DeviceSettings,
    InterfaceAttrState,
    InterfaceIntent,
    ManagedScope,
    SyncState,
)
from tests.conftest import VALID_TOKEN, session

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


async def _seed_device_with_interface(nso_device_name: str, netbox_id: int):
    """Return (device_id, iface_id) after seeding device + interface."""
    async with session() as db:
        d = Device(nso_instance="nso-dev", nso_device_name=nso_device_name, netbox_device_id=netbox_id)
        db.add(d)
        await db.flush()
        iface = DbInterface(device_id=d.id, name="GigabitEthernet0/2", netbox_interface_id=300)
        db.add(iface)
        await db.flush()
        db.add(ManagedScope(device_id=d.id, attribute="description"))
        await db.commit()
        return d.id, iface.id


# ── put_intent ────────────────────────────────────────────────────────────────


async def test_put_intent_device_not_found(adapter_client):
    response = await adapter_client.put("/api/v1/devices/99992/intent", json={"attributes": []}, headers=AUTH)
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "not_found"


async def test_put_intent_empty_attributes(adapter_client):
    device_id, _ = await _seed_device_with_interface("intent-dev-01", 1200)
    response = await adapter_client.put(f"/api/v1/devices/{device_id}/intent", json={"attributes": []}, headers=AUTH)
    assert response.status_code == 200
    result = response.json()
    assert result.pop("updated_at") is not None
    assert result == {"device_id": device_id, "attribute_count": 0}


async def test_put_intent_inserts_known_interface(adapter_client):
    device_id, _ = await _seed_device_with_interface("intent-dev-02", 1210)
    response = await adapter_client.put(
        f"/api/v1/devices/{device_id}/intent",
        json={
            "attributes": [{"interface": "GigabitEthernet0/2", "attribute": "description", "intent_value": "my-desc"}]
        },
        headers=AUTH,
    )
    assert response.status_code == 200
    assert response.json()["attribute_count"] == 1


async def test_put_intent_unknown_interface_lands(adapter_client):
    """put_intent() materialises a minimal interface for an unknown ref so the attribute intent
    LANDS (I1): stored + apply-eligible (attr_state accepted), never silently dropped."""
    device_id, _ = await _seed_device_with_interface("intent-dev-03", 1220)
    response = await adapter_client.put(
        f"/api/v1/devices/{device_id}/intent",
        json={"attributes": [{"interface": "ae0.7", "attribute": "description", "intent_value": "val"}]},
        headers=AUTH,
    )
    assert response.status_code == 200
    assert response.json()["attribute_count"] == 1
    async with session() as db:
        iface = (
            await db.execute(select(DbInterface).where(DbInterface.device_id == device_id, DbInterface.name == "ae0.7"))
        ).scalar_one()
        intents = (
            (await db.execute(select(InterfaceIntent).where(InterfaceIntent.interface_id == iface.id))).scalars().all()
        )
        assert len(intents) == 1 and intents[0].intent_value == "val"
        state = (
            await db.execute(select(InterfaceAttrState).where(InterfaceAttrState.interface_id == iface.id))
        ).scalar_one()
        assert state.sync_state == SyncState.accepted


async def test_put_intent_transitions_imported_to_accepted(adapter_client):
    """put_intent() transitions attr state from imported → accepted."""
    device_id, iface_id = await _seed_device_with_interface("intent-dev-04", 1230)
    async with session() as db:
        # Seed an attr state in 'imported' status
        attr = InterfaceAttrState(
            interface_id=iface_id,
            attribute="description",
            nso_value="old",
            netbox_value="new",
            sync_state=SyncState.imported,
        )
        db.add(attr)
        await db.commit()

    response = await adapter_client.put(
        f"/api/v1/devices/{device_id}/intent",
        json={
            "attributes": [{"interface": "GigabitEthernet0/2", "attribute": "description", "intent_value": "new-val"}]
        },
        headers=AUTH,
    )
    assert response.status_code == 200

    async with session() as db:
        state = await db.scalar(select(InterfaceAttrState).where(InterfaceAttrState.interface_id == iface_id))
        assert state.sync_state is SyncState.accepted


async def test_put_intent_auto_apply_triggers_enqueue(adapter_client):
    """A non-empty push queues Apply when auto-apply is enabled."""
    from nso_adapter.store.models import DeploymentGeneration, Job, JobStatus, JobType

    device_id, _ = await _seed_device_with_interface("intent-dev-05", 1240)
    async with session() as db:
        db.add(DeviceSettings(device_id=device_id, auto_apply=True))
        await db.commit()

    response = await adapter_client.put(
        f"/api/v1/devices/{device_id}/intent",
        json={"attributes": [{"interface": "GigabitEthernet0/2", "attribute": "description", "intent_value": "v"}]},
        headers=AUTH,
    )
    assert response.status_code == 200
    async with session() as db:
        job = await db.scalar(select(Job).where(Job.device_id == device_id, Job.job_type == JobType.apply))
        assert job is not None
        assert job.status is JobStatus.queued
        generations = (
            (await db.execute(select(DeploymentGeneration).where(DeploymentGeneration.device_id == device_id)))
            .scalars()
            .all()
        )
        assert len(generations) == 1
        assert generations[0].job_id == job.id


async def test_put_intent_replaces_existing_intent(adapter_client):
    device_id, _ = await _seed_device_with_interface("intent-dev-06", 1250)
    url = f"/api/v1/devices/{device_id}/intent"
    first = await adapter_client.put(
        url,
        json={"attributes": [{"interface": "GigabitEthernet0/2", "attribute": "description", "intent_value": "first"}]},
        headers=AUTH,
    )
    assert first.status_code == 200
    second = await adapter_client.put(
        url,
        json={
            "attributes": [{"interface": "GigabitEthernet0/2", "attribute": "description", "intent_value": "second"}]
        },
        headers=AUTH,
    )
    assert second.status_code == 200
    assert second.json()["attribute_count"] == 1

    other_device_id, _ = await _seed_device_with_interface("intent-dev-06-other", 1251)
    other = await adapter_client.put(
        f"/api/v1/devices/{other_device_id}/intent",
        json={"attributes": [{"interface": "GigabitEthernet0/2", "attribute": "description", "intent_value": "other"}]},
        headers=AUTH,
    )
    assert other.status_code == 200

    async with session() as db:
        values = (
            await db.scalars(
                select(InterfaceIntent.intent_value)
                .join(DbInterface, InterfaceIntent.interface_id == DbInterface.id)
                .where(DbInterface.device_id == device_id)
            )
        ).all()
        assert values == ["second"]


# ── get_intent ────────────────────────────────────────────────────────────────


async def test_get_intent_device_not_found(adapter_client):
    response = await adapter_client.get("/api/v1/devices/99991/intent", headers=AUTH)
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "not_found"


async def test_get_intent_empty(adapter_client):
    device_id, _ = await _seed_device_with_interface("intent-dev-07", 1260)
    response = await adapter_client.get(f"/api/v1/devices/{device_id}/intent", headers=AUTH)
    assert response.status_code == 200
    result = response.json()
    assert result["device_id"] == device_id
    assert result["attributes"] == []
    assert "updated_at" in result


async def test_get_intent_returns_set_intent(adapter_client):
    device_id, _ = await _seed_device_with_interface("intent-dev-08", 1270)
    url = f"/api/v1/devices/{device_id}/intent"
    pushed = await adapter_client.put(
        url,
        json={
            "attributes": [
                {
                    "interface": "GigabitEthernet0/2",
                    "attribute": "description",
                    "intent_value": "test-val",
                    "accepted_at": datetime(2025, 6, 1, 12, 0, tzinfo=UTC).isoformat(),
                }
            ]
        },
        headers=AUTH,
    )
    assert pushed.status_code == 200
    response = await adapter_client.get(url, headers=AUTH)
    assert response.status_code == 200
    result = response.json()
    assert len(result["attributes"]) == 1
    row = result["attributes"][0]
    assert row["interface"] == "GigabitEthernet0/2"
    assert row["attribute"] == "description"
    assert row["intent_value"] == "test-val"


async def test_get_intent_is_not_n_plus_one(adapter_client):
    """s3-10: get_intent must not run one intent query per interface."""
    from tests.conftest import count_queries

    async with session() as db:
        d = Device(nso_instance="nso-dev", nso_device_name="intent-nplus", netbox_device_id=1170)
        db.add(d)
        await db.flush()
        db.add(ManagedScope(device_id=d.id, attribute="description"))
        for j in range(6):
            iface = DbInterface(device_id=d.id, name=f"Gi0/{j}", netbox_interface_id=11700 + j)
            db.add(iface)
            await db.flush()
            db.add(InterfaceIntent(interface_id=iface.id, attribute="description", intent_value=f"v{j}"))
        await db.commit()
        device_id = d.id

    with count_queries() as qc:
        response = await adapter_client.get(f"/api/v1/devices/{device_id}/intent", headers=AUTH)
    assert response.status_code == 200
    assert len(response.json()["attributes"]) == 6
    assert qc.count <= 5, f"get_intent ran {qc.count} queries — N+1 across interfaces"
