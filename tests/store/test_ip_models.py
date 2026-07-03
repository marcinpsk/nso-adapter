# SPDX-License-Identifier: Apache-2.0
"""Unit tests for InterfaceIpAddress + InterfaceIpIntent ORM models."""

from datetime import UTC, datetime

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from nso_adapter.store.models import (
    Base,
    DbInterface,
    Device,
    InterfaceIpAddress,
    InterfaceIpIntent,
    MappingStatus,
)


@pytest.fixture
def db():
    engine = create_engine("sqlite:///:memory:")
    # Enable FK enforcement (off by default in SQLite)
    from sqlalchemy import event

    @event.listens_for(engine, "connect")
    def set_sqlite_pragma(conn, _):
        conn.execute("PRAGMA foreign_keys=ON")

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


def _make_interface(session: Session, dev: Device, name: str = "GigabitEthernet0/0") -> DbInterface:
    iface = DbInterface(device_id=dev.id, name=name)
    session.add(iface)
    session.flush()
    return iface


def _ts() -> datetime:
    return datetime.now(UTC)


# ---------------------------------------------------------------------------
# InterfaceIpAddress
# ---------------------------------------------------------------------------


def test_create_ip_address(db):
    dev = _make_device(db)
    ip = InterfaceIpAddress(
        device_id=dev.id,
        interface_name="GigabitEthernet0/0",
        address="192.168.1.1/24",
        vrf="",
        family="ipv4",
        secondary=False,
        last_refreshed_at=_ts(),
        refresh_source="notification",
    )
    db.add(ip)
    db.commit()

    result = db.execute(select(InterfaceIpAddress).where(InterfaceIpAddress.device_id == dev.id)).scalars().all()
    assert len(result) == 1
    assert result[0].address == "192.168.1.1/24"
    assert result[0].vrf == ""
    assert result[0].family == "ipv4"
    assert result[0].secondary is False


def test_ip_address_vrf_stored(db):
    dev = _make_device(db)
    db.add(
        InterfaceIpAddress(
            device_id=dev.id,
            interface_name="GigabitEthernet0/0",
            address="10.0.0.1/30",
            vrf="MGMT",
            family="ipv4",
            secondary=False,
            last_refreshed_at=_ts(),
            refresh_source="polled-sync",
        )
    )
    db.commit()
    row = db.execute(select(InterfaceIpAddress)).scalars().first()
    assert row.vrf == "MGMT"


def test_ip_address_unique_constraint(db):
    dev = _make_device(db)
    ts = _ts()
    kwargs = dict(
        device_id=dev.id,
        interface_name="GigabitEthernet0/0",
        address="192.168.1.1/24",
        vrf="",
        family="ipv4",
        secondary=False,
        last_refreshed_at=ts,
        refresh_source="notification",
    )
    db.add(InterfaceIpAddress(**kwargs))
    db.flush()
    db.add(InterfaceIpAddress(**kwargs))
    with pytest.raises(IntegrityError):
        db.flush()


def test_ip_address_same_host_different_vrf_allowed(db):
    """Same address+prefix on different VRFs is two distinct rows."""
    dev = _make_device(db)
    ts = _ts()
    for vrf in ("VRF-A", "VRF-B"):
        db.add(
            InterfaceIpAddress(
                device_id=dev.id,
                interface_name="GigabitEthernet0/0",
                address="10.1.1.1/24",
                vrf=vrf,
                family="ipv4",
                secondary=False,
                last_refreshed_at=ts,
                refresh_source="notification",
            )
        )
    db.commit()
    rows = db.execute(select(InterfaceIpAddress)).scalars().all()
    assert len(rows) == 2


def test_cascade_delete_on_device_delete(db):
    dev = _make_device(db)
    db.add(
        InterfaceIpAddress(
            device_id=dev.id,
            interface_name="GigabitEthernet0/0",
            address="192.168.1.1/24",
            vrf="",
            family="ipv4",
            secondary=False,
            last_refreshed_at=_ts(),
            refresh_source="notification",
        )
    )
    db.commit()
    db.delete(dev)
    db.commit()
    assert db.execute(select(InterfaceIpAddress)).scalars().all() == []


# ---------------------------------------------------------------------------
# InterfaceIpIntent
# ---------------------------------------------------------------------------


def test_create_ip_intent(db):
    dev = _make_device(db)
    iface = _make_interface(db, dev)
    intent = InterfaceIpIntent(
        interface_id=iface.id,
        address="192.168.1.1/24",
        vrf="",
        family="ipv4",
        secondary=False,
    )
    db.add(intent)
    db.commit()

    result = db.execute(select(InterfaceIpIntent).where(InterfaceIpIntent.interface_id == iface.id)).scalars().all()
    assert len(result) == 1
    assert result[0].address == "192.168.1.1/24"
    assert result[0].accepted_at is None
    assert result[0].last_apply_error is None


def test_ip_intent_unique_constraint(db):
    dev = _make_device(db)
    iface = _make_interface(db, dev)
    kwargs = dict(interface_id=iface.id, address="10.0.0.1/30", vrf="", family="ipv4", secondary=False)
    db.add(InterfaceIpIntent(**kwargs))
    db.flush()
    db.add(InterfaceIpIntent(**kwargs))
    with pytest.raises(IntegrityError):
        db.flush()


def test_ip_intent_same_addr_different_vrf_allowed(db):
    dev = _make_device(db)
    iface = _make_interface(db, dev)
    for vrf in ("VRF-A", "VRF-B"):
        db.add(InterfaceIpIntent(interface_id=iface.id, address="10.0.0.1/30", vrf=vrf, family="ipv4", secondary=False))
    db.commit()
    rows = db.execute(select(InterfaceIpIntent)).scalars().all()
    assert len(rows) == 2


def test_ip_intent_stores_apply_error(db):
    dev = _make_device(db)
    iface = _make_interface(db, dev)
    intent = InterfaceIpIntent(
        interface_id=iface.id,
        address="192.168.1.1/24",
        vrf="",
        family="ipv4",
        secondary=False,
        last_apply_error={"code": "apply_failed", "message": "RESTCONF error"},
    )
    db.add(intent)
    db.commit()
    row = db.execute(select(InterfaceIpIntent)).scalars().first()
    assert row.last_apply_error["code"] == "apply_failed"


def test_cascade_delete_ip_intent_on_interface_delete(db):
    dev = _make_device(db)
    iface = _make_interface(db, dev)
    db.add(InterfaceIpIntent(interface_id=iface.id, address="10.0.0.1/30", vrf="", family="ipv4", secondary=False))
    db.commit()
    db.delete(iface)
    db.commit()
    assert db.execute(select(InterfaceIpIntent)).scalars().all() == []
