# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the DeviceFailover ORM model (mgmt-IP failover)."""

from datetime import UTC, datetime

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from nso_adapter.store.models import ActiveAddress, Base, Device, DeviceFailover, FailoverConfig, MappingStatus


@pytest.fixture
def db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session


def _make_device(session: Session) -> Device:
    dev = Device(nso_instance="nso-dev", nso_device_name="ra1", mapping_status=MappingStatus.mapped)
    session.add(dev)
    session.flush()
    return dev


def test_failover_defaults(db):
    """A fresh row defaults to on-primary, zeroed counters, no override, unknown OOB health."""
    dev = _make_device(db)
    db.add(DeviceFailover(device_id=dev.id, primary_ip="10.0.0.1", oob_ip="192.0.2.5"))
    db.commit()

    row = db.execute(select(DeviceFailover).where(DeviceFailover.device_id == dev.id)).scalars().one()
    assert row.active_address == ActiveAddress.primary.value == "primary"
    assert row.consecutive_failures == 0
    assert row.consecutive_successes == 0
    assert row.manual_override is False
    assert row.oob_healthy is None  # tri-state: not yet checked
    assert row.oob_health_result is None
    assert row.oob_health_detail is None
    assert row.last_probe_at is None and row.last_switch_at is None


def test_failover_on_oob_state(db):
    dev = _make_device(db)
    db.add(
        DeviceFailover(
            device_id=dev.id,
            primary_ip="10.0.0.1",
            oob_ip="192.0.2.5",
            active_address=ActiveAddress.oob.value,
            consecutive_successes=2,
            last_switch_at=datetime.now(UTC),
            last_probe_result="fail",
        )
    )
    db.commit()
    row = db.execute(select(DeviceFailover)).scalars().one()
    assert row.active_address == "oob"
    assert row.consecutive_successes == 2
    assert row.last_probe_result == "fail"


def test_failover_one_row_per_device(db):
    dev = _make_device(db)
    db.add(DeviceFailover(device_id=dev.id, primary_ip="10.0.0.1"))
    db.flush()
    db.add(DeviceFailover(device_id=dev.id, primary_ip="10.0.0.2"))
    with pytest.raises(IntegrityError):
        db.flush()


def test_device_failover_relationship(db):
    dev = _make_device(db)
    db.add(DeviceFailover(device_id=dev.id, primary_ip="10.0.0.1", oob_ip="192.0.2.5"))
    db.commit()
    loaded = db.execute(select(Device).where(Device.id == dev.id)).scalars().one()
    # lazy="raise" → must load the relationship explicitly; assert the back-ref resolves.
    fo = db.execute(select(DeviceFailover).where(DeviceFailover.device_id == loaded.id)).scalars().one()
    assert fo.device_id == dev.id
    assert fo.oob_ip == "192.0.2.5"


def test_failover_config_defaults(db):
    """A fresh FailoverConfig row carries the spike-derived prod defaults (enabled, 15m/6h,
    3/5 thresholds, 10s timeout, concurrency 8, 8 flips/tick, sync-from on)."""
    cfg = FailoverConfig()
    db.add(cfg)
    db.commit()

    row = db.execute(select(FailoverConfig)).scalars().one()
    assert row.id == 1  # singleton
    assert row.enabled is True
    assert row.primary_probe_interval == 15
    assert row.oob_probe_interval == 360
    assert row.failure_threshold == 3
    assert row.success_threshold == 5
    assert row.probe_timeout == 10.0
    assert row.active_probe_timeout == 45.0
    assert row.probe_concurrency == 8
    assert row.max_flips_per_tick == 8
    assert row.sync_from_after_switch is True
    assert row.updated_at is not None


def test_failover_config_overrides_persist(db):
    """Operator-set values round-trip (the plugin pushes these via the config endpoint)."""
    db.add(
        FailoverConfig(
            enabled=False,
            primary_probe_interval=5,
            oob_probe_interval=720,
            failure_threshold=2,
            success_threshold=4,
            probe_timeout=7.5,
            probe_concurrency=12,
            max_flips_per_tick=10,
            sync_from_after_switch=False,
        )
    )
    db.commit()
    row = db.execute(select(FailoverConfig)).scalars().one()
    assert row.enabled is False
    assert (row.primary_probe_interval, row.oob_probe_interval) == (5, 720)
    assert (row.failure_threshold, row.success_threshold) == (2, 4)
    assert row.probe_timeout == 7.5
    assert (row.probe_concurrency, row.max_flips_per_tick) == (12, 10)
    assert row.sync_from_after_switch is False
