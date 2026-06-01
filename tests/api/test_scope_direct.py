# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Direct unit tests for scope.py endpoint functions.

These tests call get_scope() / update_scope() directly with a real
SQLAlchemy session, bypassing FastAPI's HTTP layer.  This approach
guarantees coverage.py tracks all lines in async function bodies.
"""

from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.errors import ApiError
from nso_adapter.api.scope import ScopeUpdate, get_scope, update_scope
from nso_adapter.store.db import get_session
from nso_adapter.store.models import Device, DeviceSettings, ManagedScope


async def _seed(db: AsyncSession, nso_device_name: str, netbox_id: int, attrs: list[str]) -> int:
    """Insert a device with managed scope and return its id."""
    d = Device(nso_instance="nso-dev", nso_device_name=nso_device_name, netbox_device_id=netbox_id)
    db.add(d)
    await db.flush()
    for attr in attrs:
        db.add(ManagedScope(device_id=d.id, attribute=attr))
    await db.commit()
    await db.refresh(d)
    return d.id


# ── get_scope ─────────────────────────────────────────────────────────────────


async def test_get_scope_returns_correct_attributes(adapter_client):
    """get_scope() returns device_id, attributes list, auto_apply=False."""
    async for db in get_session():
        device_id = await _seed(db, "scope-direct-01", 700, ["description", "enabled"])
        result = await get_scope(device_id=device_id, db=db)
        assert result["device_id"] == device_id
        assert set(result["attributes"]) == {"description", "enabled"}
        assert result["auto_apply"] is False
        assert "updated_at" in result
        break


async def test_get_scope_empty_attributes(adapter_client):
    """get_scope() with no managed attrs returns empty list."""
    async for db in get_session():
        device_id = await _seed(db, "scope-direct-02", 701, [])
        result = await get_scope(device_id=device_id, db=db)
        assert result["attributes"] == []
        break


async def test_get_scope_unknown_device_raises_404(adapter_client):
    """get_scope() with non-existent device_id raises ApiError 404."""
    async for db in get_session():
        with pytest.raises(ApiError) as exc_info:
            await get_scope(device_id=99999, db=db)
        assert exc_info.value.status_code == 404
        break


async def test_get_scope_respects_auto_apply_from_settings(adapter_client):
    """get_scope() picks up auto_apply from DeviceSettings when present."""
    async for db in get_session():
        device_id = await _seed(db, "scope-direct-03", 702, ["description"])
        db.add(DeviceSettings(device_id=device_id, auto_apply=True))
        await db.commit()

        result = await get_scope(device_id=device_id, db=db)
        assert result["auto_apply"] is True
        break


# ── update_scope ──────────────────────────────────────────────────────────────


async def test_update_scope_replaces_attributes(adapter_client):
    """update_scope() replaces managed attributes with new list."""
    async for db in get_session():
        device_id = await _seed(db, "scope-direct-04", 710, ["description"])
        body = ScopeUpdate(attributes=["description", "enabled"], auto_apply=False)
        result = await update_scope(device_id=device_id, body=body, db=db)
        assert set(result["attributes"]) == {"description", "enabled"}
        break


async def test_update_scope_clears_all_attributes(adapter_client):
    """update_scope() with empty list removes all managed attributes."""
    async for db in get_session():
        device_id = await _seed(db, "scope-direct-05", 711, ["description", "enabled"])
        body = ScopeUpdate(attributes=[], auto_apply=False)
        result = await update_scope(device_id=device_id, body=body, db=db)
        assert result["attributes"] == []
        break


async def test_update_scope_creates_device_settings(adapter_client):
    """update_scope() creates DeviceSettings with auto_apply when none exists."""
    async for db in get_session():
        device_id = await _seed(db, "scope-direct-06", 712, [])
        body = ScopeUpdate(attributes=["description"], auto_apply=True)
        result = await update_scope(device_id=device_id, body=body, db=db)
        assert result["auto_apply"] is True
        break


async def test_update_scope_updates_existing_device_settings(adapter_client):
    """update_scope() updates auto_apply on an existing DeviceSettings row."""
    async for db in get_session():
        device_id = await _seed(db, "scope-direct-07", 713, ["description"])
        db.add(DeviceSettings(device_id=device_id, auto_apply=False))
        await db.commit()

        body = ScopeUpdate(attributes=["description"], auto_apply=True)
        result = await update_scope(device_id=device_id, body=body, db=db)
        assert result["auto_apply"] is True
        break


async def test_update_scope_unknown_device_raises_404(adapter_client):
    """update_scope() with non-existent device_id raises ApiError 404."""
    async for db in get_session():
        body = ScopeUpdate(attributes=["description"], auto_apply=False)
        with pytest.raises(ApiError) as exc_info:
            await update_scope(device_id=99999, body=body, db=db)
        assert exc_info.value.status_code == 404
        break
