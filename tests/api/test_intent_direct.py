# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Direct unit tests for intent.py endpoint functions."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import pytest

from nso_adapter.api.errors import ApiError
from nso_adapter.api.intent import IntentAttribute, IntentUpdate, get_intent, put_intent
from nso_adapter.store.db import get_session
from nso_adapter.store.models import (
    DbInterface,
    Device,
    DeviceSettings,
    InterfaceAttrState,
    ManagedScope,
    SyncState,
)


async def _seed_device_with_interface(nso_device_name: str, netbox_id: int):
    """Return (device_id, iface_id) after seeding device + interface."""
    async for db in get_session():
        d = Device(nso_instance="nso-dev", nso_device_name=nso_device_name, netbox_device_id=netbox_id)
        db.add(d)
        await db.flush()
        iface = DbInterface(device_id=d.id, name="GigabitEthernet0/2", netbox_interface_id=300)
        db.add(iface)
        await db.flush()
        db.add(ManagedScope(device_id=d.id, attribute="description"))
        await db.commit()
        return d.id, iface.id
    raise RuntimeError("no DB session")


# ── put_intent ────────────────────────────────────────────────────────────────


async def test_put_intent_device_not_found(adapter_client):
    """put_intent() raises 404 for unknown device_id."""
    async for db in get_session():
        body = IntentUpdate(attributes=[])
        with pytest.raises(ApiError) as exc_info:
            await put_intent(device_id=99992, body=body, db=db)
        assert exc_info.value.status_code == 404
        break


async def test_put_intent_empty_attributes(adapter_client):
    """put_intent() with empty attributes returns attribute_count=0."""
    device_id, _ = await _seed_device_with_interface("intent-dev-01", 1200)
    async for db in get_session():
        body = IntentUpdate(attributes=[])
        result = await put_intent(device_id=device_id, body=body, db=db)
        assert result["device_id"] == device_id
        assert result["attribute_count"] == 0
        break


async def test_put_intent_inserts_known_interface(adapter_client):
    """put_intent() stores intent rows for known interfaces."""
    device_id, _ = await _seed_device_with_interface("intent-dev-02", 1210)
    async for db in get_session():
        body = IntentUpdate(
            attributes=[
                IntentAttribute(interface="GigabitEthernet0/2", attribute="description", intent_value="my-desc")
            ]
        )
        result = await put_intent(device_id=device_id, body=body, db=db)
        assert result["attribute_count"] == 1
        break


async def test_put_intent_unknown_interface_logged_and_skipped(adapter_client):
    """put_intent() skips unknown interfaces (warns) and returns count=0."""
    device_id, _ = await _seed_device_with_interface("intent-dev-03", 1220)
    async for db in get_session():
        body = IntentUpdate(
            attributes=[IntentAttribute(interface="no-such-iface", attribute="description", intent_value="val")]
        )
        result = await put_intent(device_id=device_id, body=body, db=db)
        assert result["attribute_count"] == 0
        break


async def test_put_intent_transitions_imported_to_accepted(adapter_client):
    """put_intent() transitions attr state from imported → accepted."""
    device_id, iface_id = await _seed_device_with_interface("intent-dev-04", 1230)
    async for db in get_session():
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

        body = IntentUpdate(
            attributes=[
                IntentAttribute(interface="GigabitEthernet0/2", attribute="description", intent_value="new-val")
            ]
        )
        result = await put_intent(device_id=device_id, body=body, db=db)
        assert result["attribute_count"] == 1
        break


async def test_put_intent_auto_apply_triggers_enqueue(adapter_client):
    """put_intent() calls enqueue_apply when auto_apply=True and count>0."""
    device_id, _ = await _seed_device_with_interface("intent-dev-05", 1240)
    async for db in get_session():
        db.add(DeviceSettings(device_id=device_id, auto_apply=True))
        await db.commit()

    async for db in get_session():
        body = IntentUpdate(
            attributes=[IntentAttribute(interface="GigabitEthernet0/2", attribute="description", intent_value="v")]
        )
        with patch("nso_adapter.core.apply.enqueue_apply", new_callable=AsyncMock) as mock_enq:
            result = await put_intent(device_id=device_id, body=body, db=db)
        assert result["attribute_count"] == 1
        mock_enq.assert_called_once_with(db, device_id, force=True)
        break


async def test_put_intent_replaces_existing_intent(adapter_client):
    """put_intent() deletes old intent rows and inserts fresh ones."""
    device_id, _ = await _seed_device_with_interface("intent-dev-06", 1250)
    async for db in get_session():
        body1 = IntentUpdate(
            attributes=[IntentAttribute(interface="GigabitEthernet0/2", attribute="description", intent_value="first")]
        )
        await put_intent(device_id=device_id, body=body1, db=db)

    async for db in get_session():
        body2 = IntentUpdate(
            attributes=[IntentAttribute(interface="GigabitEthernet0/2", attribute="description", intent_value="second")]
        )
        result = await put_intent(device_id=device_id, body=body2, db=db)
        assert result["attribute_count"] == 1
        break


# ── get_intent ────────────────────────────────────────────────────────────────


async def test_get_intent_device_not_found(adapter_client):
    """get_intent() raises 404 for unknown device_id."""
    async for db in get_session():
        with pytest.raises(ApiError) as exc_info:
            await get_intent(device_id=99991, db=db)
        assert exc_info.value.status_code == 404
        break


async def test_get_intent_empty(adapter_client):
    """get_intent() returns empty attributes list when no intent set."""
    device_id, _ = await _seed_device_with_interface("intent-dev-07", 1260)
    async for db in get_session():
        result = await get_intent(device_id=device_id, db=db)
        assert result["device_id"] == device_id
        assert result["attributes"] == []
        assert "updated_at" in result
        break


async def test_get_intent_returns_set_intent(adapter_client):
    """get_intent() returns the intent rows set by put_intent."""
    device_id, _ = await _seed_device_with_interface("intent-dev-08", 1270)
    async for db in get_session():
        body = IntentUpdate(
            attributes=[
                IntentAttribute(
                    interface="GigabitEthernet0/2",
                    attribute="description",
                    intent_value="test-val",
                    accepted_at=datetime(2025, 6, 1, 12, 0, tzinfo=UTC),
                )
            ]
        )
        await put_intent(device_id=device_id, body=body, db=db)

    async for db in get_session():
        result = await get_intent(device_id=device_id, db=db)
        assert len(result["attributes"]) == 1
        row = result["attributes"][0]
        assert row["interface"] == "GigabitEthernet0/2"
        assert row["attribute"] == "description"
        assert row["intent_value"] == "test-val"
        break
