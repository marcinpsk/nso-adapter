# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the DeviceL2Sap ORM model."""

from datetime import UTC, datetime

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from nso_adapter.store.models import Device, DeviceL2Sap, MappingStatus


@pytest.fixture
def db(pg_sync_session):
    """Sync Session on a private PostgreSQL clone (tests/conftest.py::pg_sync_session)."""
    return pg_sync_session


def _make_device(session: Session) -> Device:
    dev = Device(
        nso_instance="nso-dev",
        nso_device_name="ra1",
        mapping_status=MappingStatus.mapped,
    )
    session.add(dev)
    session.flush()
    return dev


def test_create_l2_sap(db):
    dev = _make_device(db)
    sap = DeviceL2Sap(
        device_id=dev.id,
        service_name="701",
        service_type="vpls",
        sap_id="1/1/c31/3:701",
        port="1/1/c31/3",
        outer_tag=701,
        last_refreshed_at=datetime.now(UTC),
        refresh_source="notification",
    )
    db.add(sap)
    db.commit()

    loaded = db.execute(select(DeviceL2Sap).where(DeviceL2Sap.device_id == dev.id)).scalars().one()
    assert loaded.service_type == "vpls"
    assert loaded.outer_tag == 701
    assert loaded.inner_tag is None
    assert loaded.service_id is None


def test_qinq_sap_keeps_inner_tag(db):
    dev = _make_device(db)
    db.add(
        DeviceL2Sap(
            device_id=dev.id,
            service_name="TL",
            service_type="epipe",
            service_id=4022,
            sap_id="1/1/c28/1:100.10",
            port="1/1/c28/1",
            outer_tag=100,
            inner_tag=10,
            last_refreshed_at=datetime.now(UTC),
            refresh_source="poll",
        )
    )
    db.commit()
    loaded = db.execute(select(DeviceL2Sap)).scalars().one()
    assert (loaded.outer_tag, loaded.inner_tag, loaded.service_id) == (100, 10, 4022)


def test_l2_sap_unique_constraint(db):
    dev = _make_device(db)
    ts = datetime.now(UTC)
    db.add(
        DeviceL2Sap(
            device_id=dev.id,
            service_name="TL",
            service_type="epipe",
            sap_id="lag-60:3999",
            port="lag-60",
            outer_tag=3999,
            last_refreshed_at=ts,
            refresh_source="poll",
        )
    )
    db.flush()
    db.add(
        DeviceL2Sap(
            device_id=dev.id,
            service_name="TL",
            service_type="epipe",
            sap_id="lag-60:3999",
            port="lag-60",
            outer_tag=3999,
            last_refreshed_at=ts,
            refresh_source="poll",
        )
    )
    with pytest.raises(IntegrityError):
        db.flush()
