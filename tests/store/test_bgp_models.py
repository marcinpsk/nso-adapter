# SPDX-License-Identifier: Apache-2.0
"""Unit tests for BGP config read-mirror ORM models (M15 A2)."""
import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from nso_adapter.store.models import (
    Base,
    Device,
    DeviceBgpAddressFamily,
    DeviceBgpPeer,
    DeviceBgpPeerAddressFamily,
    DeviceBgpRouter,
    DeviceBgpScope,
    MappingStatus,
)


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


def _seed_router(session: Session, dev: Device, asn: str = "65100") -> DeviceBgpRouter:
    router = DeviceBgpRouter(device_id=dev.id, asn=asn, refresh_source="test")
    session.add(router)
    session.flush()
    return router


def _seed_scope(session: Session, router: DeviceBgpRouter, vrf: str = "") -> DeviceBgpScope:
    scope = DeviceBgpScope(router_id=router.id, vrf=vrf)
    session.add(scope)
    session.flush()
    return scope


def test_create_router_and_scopes(db):
    dev = _make_device(db)
    router = _seed_router(db, dev, asn="65100")
    _seed_scope(db, router, vrf="")
    _seed_scope(db, router, vrf="ASPAN")
    db.commit()

    routers = db.execute(select(DeviceBgpRouter).where(DeviceBgpRouter.device_id == dev.id)).scalars().all()
    assert len(routers) == 1
    assert routers[0].asn == "65100"

    scopes = db.execute(select(DeviceBgpScope).where(DeviceBgpScope.router_id == router.id)).scalars().all()
    assert len(scopes) == 2
    vrfs = {s.vrf for s in scopes}
    assert vrfs == {"", "ASPAN"}


def test_create_address_families(db):
    dev = _make_device(db)
    router = _seed_router(db, dev)
    scope = _seed_scope(db, router)
    af_v4 = DeviceBgpAddressFamily(scope_id=scope.id, af="ipv4-unicast")
    af_v6 = DeviceBgpAddressFamily(scope_id=scope.id, af="ipv6-unicast")
    db.add_all([af_v4, af_v6])
    db.commit()

    afs = db.execute(select(DeviceBgpAddressFamily).where(DeviceBgpAddressFamily.scope_id == scope.id)).scalars().all()
    assert {a.af for a in afs} == {"ipv4-unicast", "ipv6-unicast"}


def test_create_peer_with_peer_afs(db):
    dev = _make_device(db)
    router = _seed_router(db, dev)
    scope = _seed_scope(db, router)

    peer = DeviceBgpPeer(
        scope_id=scope.id,
        peer_address="192.0.2.1",
        enabled=False,
        peer_group="UPSTREAM",
        remote_as="65001",
        local_as=None,
        ttl=2,
        password="s3cr3t",
    )
    db.add(peer)
    db.flush()

    paf = DeviceBgpPeerAddressFamily(peer_id=peer.id, af="ipv4-unicast", enabled=True)
    db.add(paf)
    db.commit()

    peers = db.execute(select(DeviceBgpPeer).where(DeviceBgpPeer.scope_id == scope.id)).scalars().all()
    assert len(peers) == 1
    p = peers[0]
    assert p.peer_address == "192.0.2.1"
    assert p.enabled is False
    assert p.peer_group == "UPSTREAM"
    assert p.remote_as == "65001"
    assert p.ttl == 2
    assert p.password == "s3cr3t"

    pafs = db.execute(select(DeviceBgpPeerAddressFamily).where(DeviceBgpPeerAddressFamily.peer_id == peer.id)).scalars().all()
    assert len(pafs) == 1
    assert pafs[0].af == "ipv4-unicast"


def test_router_unique_constraint(db):
    dev = _make_device(db)
    _seed_router(db, dev, asn="65100")
    db.commit()
    with pytest.raises(IntegrityError):
        _seed_router(db, dev, asn="65100")
        db.commit()


def test_scope_unique_constraint(db):
    dev = _make_device(db)
    router = _seed_router(db, dev)
    _seed_scope(db, router, vrf="ASPAN")
    db.commit()
    with pytest.raises(IntegrityError):
        _seed_scope(db, router, vrf="ASPAN")
        db.commit()


def test_peer_unique_constraint(db):
    dev = _make_device(db)
    router = _seed_router(db, dev)
    scope = _seed_scope(db, router)
    peer1 = DeviceBgpPeer(scope_id=scope.id, peer_address="192.0.2.1")
    db.add(peer1)
    db.commit()
    with pytest.raises(IntegrityError):
        peer2 = DeviceBgpPeer(scope_id=scope.id, peer_address="192.0.2.1")
        db.add(peer2)
        db.commit()


def test_af_unique_constraint(db):
    dev = _make_device(db)
    router = _seed_router(db, dev)
    scope = _seed_scope(db, router)
    db.add(DeviceBgpAddressFamily(scope_id=scope.id, af="ipv4-unicast"))
    db.commit()
    with pytest.raises(IntegrityError):
        db.add(DeviceBgpAddressFamily(scope_id=scope.id, af="ipv4-unicast"))
        db.commit()


def test_cascade_delete_router_removes_children(db):
    dev = _make_device(db)
    router = _seed_router(db, dev)
    scope = _seed_scope(db, router)
    peer = DeviceBgpPeer(scope_id=scope.id, peer_address="192.0.2.1")
    db.add(peer)
    db.flush()
    db.add(DeviceBgpPeerAddressFamily(peer_id=peer.id, af="ipv4-unicast", enabled=True))
    db.add(DeviceBgpAddressFamily(scope_id=scope.id, af="ipv4-unicast"))
    db.commit()

    db.delete(router)
    db.commit()

    assert db.execute(select(DeviceBgpScope)).scalars().first() is None
    assert db.execute(select(DeviceBgpPeer)).scalars().first() is None
    assert db.execute(select(DeviceBgpPeerAddressFamily)).scalars().first() is None
    assert db.execute(select(DeviceBgpAddressFamily)).scalars().first() is None
