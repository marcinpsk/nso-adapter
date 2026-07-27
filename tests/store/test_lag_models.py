# SPDX-License-Identifier: Apache-2.0
"""Unit tests for LagInterface + LagMember ORM models."""

from datetime import UTC, datetime

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from nso_adapter.store.models import Base, Device, LagInterface, LagMember, MappingStatus


@pytest.fixture
def db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session


def _make_device(session: Session) -> Device:
    dev = Device(
        nso_instance="nso-dev",
        nso_device_name="test-device",
        mapping_status=MappingStatus.mapped,
    )
    session.add(dev)
    session.flush()
    return dev


def test_create_lag_interface_and_members(db):
    dev = _make_device(db)
    lag = LagInterface(
        device_id=dev.id,
        name="Port-channel10",
        lag_id=10,
        last_refreshed_at=datetime.now(UTC),
        refresh_source="notification",
    )
    db.add(lag)
    db.flush()

    m1 = LagMember(lag_interface_id=lag.id, interface_name="GigabitEthernet0/1", mode="active")
    m2 = LagMember(lag_interface_id=lag.id, interface_name="GigabitEthernet0/2", mode="active")
    db.add_all([m1, m2])
    db.commit()

    result = db.execute(select(LagInterface).where(LagInterface.device_id == dev.id)).scalars().all()
    assert len(result) == 1
    assert result[0].name == "Port-channel10"
    assert len(result[0].members) == 2


def test_lag_interface_unique_constraint(db):
    dev = _make_device(db)
    ts = datetime.now(UTC)
    db.add(
        LagInterface(
            device_id=dev.id,
            name="Port-channel10",
            lag_id=10,
            last_refreshed_at=ts,
            refresh_source="notification",
        )
    )
    db.flush()
    db.add(
        LagInterface(
            device_id=dev.id,
            name="Port-channel10",
            lag_id=10,
            last_refreshed_at=ts,
            refresh_source="polled-sync",
        )
    )
    with pytest.raises(IntegrityError):
        db.flush()


def test_lag_member_unique_constraint(db):
    dev = _make_device(db)
    ts = datetime.now(UTC)
    lag = LagInterface(
        device_id=dev.id,
        name="Port-channel10",
        lag_id=10,
        last_refreshed_at=ts,
        refresh_source="notification",
    )
    db.add(lag)
    db.flush()
    db.add(LagMember(lag_interface_id=lag.id, interface_name="GigabitEthernet0/1", mode="active"))
    db.flush()
    db.add(LagMember(lag_interface_id=lag.id, interface_name="GigabitEthernet0/1", mode="passive"))
    with pytest.raises(IntegrityError):
        db.flush()


def test_cascade_delete_lag_members(db):
    dev = _make_device(db)
    ts = datetime.now(UTC)
    lag = LagInterface(
        device_id=dev.id,
        name="Port-channel10",
        lag_id=10,
        last_refreshed_at=ts,
        refresh_source="notification",
    )
    db.add(lag)
    db.flush()
    db.add(LagMember(lag_interface_id=lag.id, interface_name="GigabitEthernet0/1", mode="active"))
    db.commit()

    db.delete(lag)
    db.commit()

    assert db.execute(select(LagMember)).scalars().all() == []
