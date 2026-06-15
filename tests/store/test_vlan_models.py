# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""M34: DeviceVlan / DeviceSwitchport / DeviceSwitchportTaggedVlan model tests."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from nso_adapter.store.models import (
    Base,
    Device,
    DeviceSwitchport,
    DeviceSwitchportTaggedVlan,
    DeviceVlan,
)


@pytest.fixture
def db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session


def _make_device(session):
    device = Device(nso_instance="nso-dev", nso_device_name="sw01", netbox_device_id=1)
    session.add(device)
    session.flush()
    return device


def test_vlan_models_can_be_created(db):
    device = _make_device(db)
    vlan = DeviceVlan(
        device_id=device.id,
        vlan_id=100,
        name="DATA",
        last_refreshed_at=datetime.now(UTC),
        refresh_source="notification",
    )
    db.add(vlan)
    db.flush()
    sp = DeviceSwitchport(
        device_id=device.id,
        interface_name="GigabitEthernet0/1",
        mode="access",
        untagged_vlan_id=vlan.id,
        last_refreshed_at=datetime.now(UTC),
        refresh_source="notification",
    )
    db.add(sp)
    db.flush()
    db.add(DeviceSwitchportTaggedVlan(switchport_id=sp.id, vlan_id=vlan.id))
    db.commit()

    loaded = db.execute(select(DeviceSwitchport).where(DeviceSwitchport.device_id == device.id)).scalars().one()
    assert loaded.untagged_vlan_id == vlan.id


def test_vlan_unique_constraint(db):
    device = _make_device(db)
    ts = datetime.now(UTC)
    db.add(
        DeviceVlan(device_id=device.id, vlan_id=100, name="DATA", last_refreshed_at=ts, refresh_source="notification")
    )
    db.flush()
    db.add(DeviceVlan(device_id=device.id, vlan_id=100, name="VOICE", last_refreshed_at=ts, refresh_source="poll"))
    with pytest.raises(IntegrityError):
        db.flush()
