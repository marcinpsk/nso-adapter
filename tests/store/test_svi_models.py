# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""M35: DeviceSvi ORM model + unique constraint."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from nso_adapter.store.models import Base, Device, DeviceSvi, MappingStatus


@pytest.fixture
def db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session


def _make_device(session):
    d = Device(nso_instance="nso-dev", nso_device_name="sw01", mapping_status=MappingStatus.mapped)
    session.add(d)
    session.flush()
    return d


def test_svi_row_can_be_created(db):
    d = _make_device(db)
    db.add(
        DeviceSvi(
            device_id=d.id,
            interface_name="Vlan100",
            vlan_id=100,
            svi_type="svi",
            vrf="MGMT",
            last_refreshed_at=datetime.now(UTC),
            refresh_source="notification",
        )
    )
    db.commit()
    row = db.execute(select(DeviceSvi).where(DeviceSvi.device_id == d.id)).scalars().one()
    assert row.interface_name == "Vlan100" and row.vlan_id == 100 and row.svi_type == "svi"


def test_svi_unique_constraint(db):
    d = _make_device(db)
    ts = datetime.now(UTC)
    db.add(
        DeviceSvi(
            device_id=d.id,
            interface_name="Vlan100",
            vlan_id=100,
            svi_type="svi",
            last_refreshed_at=ts,
            refresh_source="poll",
        )
    )
    db.flush()
    db.add(
        DeviceSvi(
            device_id=d.id,
            interface_name="Vlan100",
            vlan_id=100,
            svi_type="svi",
            last_refreshed_at=ts,
            refresh_source="poll",
        )
    )
    with pytest.raises(IntegrityError):
        db.flush()
