# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""M36: DeviceSubinterface + SubinterfaceIntent ORM models + unique constraints."""
from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from nso_adapter.store.models import (
    Base,
    Device,
    DeviceSubinterface,
    MappingStatus,
    SubinterfaceIntent,
)


@pytest.fixture
def db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session


def _make_device(session):
    d = Device(nso_instance="nso-dev", nso_device_name="rtr01", mapping_status=MappingStatus.mapped)
    session.add(d)
    session.flush()
    return d


def test_subif_row_can_be_created(db):
    d = _make_device(db)
    db.add(DeviceSubinterface(
        device_id=d.id, interface_name="GigabitEthernet0/1.100", parent_interface="GigabitEthernet0/1",
        dot1q_vlan=100, sub_type="subinterface", vrf="TENANT_A",
        last_refreshed_at=datetime.now(UTC), refresh_source="notification",
    ))
    db.commit()
    row = db.execute(select(DeviceSubinterface).where(DeviceSubinterface.device_id == d.id)).scalars().one()
    assert row.interface_name == "GigabitEthernet0/1.100"
    assert row.parent_interface == "GigabitEthernet0/1"
    assert row.dot1q_vlan == 100 and row.vrf == "TENANT_A"


def test_subif_unique_constraint(db):
    d = _make_device(db)
    ts = datetime.now(UTC)
    db.add(DeviceSubinterface(device_id=d.id, interface_name="ge-0/0/0.10", dot1q_vlan=10,
                              last_refreshed_at=ts, refresh_source="poll"))
    db.flush()
    db.add(DeviceSubinterface(device_id=d.id, interface_name="ge-0/0/0.10", dot1q_vlan=10,
                              last_refreshed_at=ts, refresh_source="poll"))
    with pytest.raises(IntegrityError):
        db.flush()


def test_subif_intent_unique_constraint(db):
    d = _make_device(db)
    db.add(SubinterfaceIntent(device_id=d.id, interface_name="ge-0/0/0.10", dot1q_vlan=10))
    db.flush()
    db.add(SubinterfaceIntent(device_id=d.id, interface_name="ge-0/0/0.10", dot1q_vlan=10))
    with pytest.raises(IntegrityError):
        db.flush()
