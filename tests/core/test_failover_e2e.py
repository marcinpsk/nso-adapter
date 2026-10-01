# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""End-to-end mgmt-IP failover: scheduler job → real NsoClient → real ORM.

Only the NSO socket is faked (a stateful httpx.MockTransport simulating NSO RESTCONF). The
scheduler job, the NsoClient request/URL/body construction, the failover state machine and
the PostgreSQL store are all REAL — this is the whole-flow test the unit tests trust.
"""

from __future__ import annotations

import json

import httpx
import pytest
from sqlalchemy import select

from nso_adapter.core import scheduler as sched
from nso_adapter.nso.client import NsoClient
from nso_adapter.store.models import ActiveAddress, Device, DeviceFailover, FailoverConfig
from tests._secret_discipline import assert_keys_absent
from tests.conftest import AUTH, session


class _NsoSim:
    """A tiny stateful NSO RESTCONF simulator routed over httpx.MockTransport.

    NSO dials the device's CURRENT address; a connect is reachable iff that address is in
    ``reachable_addrs``. PATCH updates the address, GET returns it (manual-override check),
    disconnect/sync-from are accepted. Tests flip ``reachable_addrs`` to model recovery.
    """

    def __init__(self, address: str = "10.0.0.1"):
        self.address = address
        self.reachable_addrs: set[str] = set()
        self.always_reachable = False  # address-agnostic "always up" — for multi-device tests
        self.patches: list[str] = []
        self.connects = 0
        self.get_address_failures = 0
        # When set, a connect while NSO is dialing this address raises an *unexpected* error
        # (not an httpx error probe_reachable swallows) — models a probe blowing up mid-flip.
        self.raise_on_connect_addr: str | None = None

    def handler(self, request: httpx.Request) -> httpx.Response:
        url, method = str(request.url), request.method
        if method == "POST" and url.endswith("/connect"):
            self.connects += 1
            if self.address == self.raise_on_connect_addr:
                raise RuntimeError(f"nso connect blew up for {self.address}")
            if self.always_reachable or self.address in self.reachable_addrs:
                out = {"result": "connected"}
            else:
                out = {"result": False, "info": "no route to host"}
            return httpx.Response(200, json={"tailf-ncs:output": out}, request=request)
        if method == "POST" and url.endswith("/disconnect"):
            return httpx.Response(200, json={}, request=request)
        if method == "POST" and url.endswith("/sync-from"):
            return httpx.Response(200, json={"tailf-ncs:output": {"result": True}}, request=request)
        if method == "PATCH":
            entry = json.loads(request.content)["tailf-ncs:device"][0]
            self.address = entry["address"]
            self.patches.append(entry["address"])
            return httpx.Response(204, request=request)
        if method == "GET":
            if self.get_address_failures:
                self.get_address_failures -= 1
                return httpx.Response(503, request=request)
            body = {"tailf-ncs:device": [{"name": "ra1", "address": self.address}]}
            return httpx.Response(200, json=body, request=request)
        return httpx.Response(404, request=request)


def _client_for(sim: _NsoSim) -> NsoClient:
    from nso_adapter.config import NsoInstanceConfig

    cfg = NsoInstanceConfig(
        name="nso-dev",
        base_url="http://nso-dev:8080",
        username_ref="NSO_USERNAME",
        password_ref="NSO_PASSWORD",
    )
    client = NsoClient(cfg, "placeholder-user", "placeholder-password")
    client._client = lambda timeout=None: httpx.AsyncClient(
        transport=httpx.MockTransport(sim.handler), base_url="http://nso-dev:8080"
    )
    return client


def _client_for_devices(sims: dict[str, _NsoSim]) -> NsoClient:
    def _route(request):
        name = request.url.path.split("device=", 1)[1].split("/", 1)[0]
        return sims[name].handler(request)

    client = _client_for(next(iter(sims.values())))
    client._client = lambda timeout=None: httpx.AsyncClient(
        transport=httpx.MockTransport(_route), base_url="http://nso-dev:8080"
    )
    return client


async def _seed(primary="10.0.0.1", oob="192.0.2.5", active="primary") -> int:
    async with session() as db:
        dev = Device(nso_instance="nso-dev", nso_device_name="ra1", netbox_device_id=42)
        db.add(dev)
        await db.flush()
        db.add(DeviceFailover(device_id=dev.id, primary_ip=primary, oob_ip=oob, active_address=active))
        await db.commit()
        return dev.id


async def _arm_and_load(device_id: int) -> DeviceFailover:
    """Force the primary probe due (clear staggering) and return a fresh copy of the row."""
    async with session() as db:
        row = (await db.execute(select(DeviceFailover).where(DeviceFailover.device_id == device_id))).scalar_one()
        row.next_primary_probe_at = None
        await db.commit()
        await db.refresh(row)
        db.expunge(row)
        return row


@pytest.mark.parametrize("invalid_field", ["primary_ip", "oob_ip"])
async def test_scheduler_defers_invalid_stored_address(adapter_client, monkeypatch, invalid_field):
    from datetime import UTC, datetime

    sim = _NsoSim(address="198.18.0.1")
    client = _client_for(sim)
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda *_: client)
    device_id = await _seed(primary="198.18.0.1", oob="198.18.0.2")
    now = datetime.now(UTC)
    async with session() as db:
        row = await db.scalar(select(DeviceFailover).where(DeviceFailover.device_id == device_id))
        setattr(row, invalid_field, "not-an-ip")
        await db.commit()
        assert device_id in await sched._due_failover_device_ids(db, now)

    await sched._scheduled_failover_probe()

    async with session() as db:
        row = await db.scalar(select(DeviceFailover).where(DeviceFailover.device_id == device_id))
        assert row.failback_blocked_reason == "stored_address_invalid"
        assert row.next_primary_probe_at > now
        assert row.next_oob_probe_at > now
        assert device_id not in await sched._due_failover_device_ids(db, datetime.now(UTC))
    assert sim.patches == []
    assert sim.connects == 0


@pytest.mark.parametrize("invalid_field", ["primary_ip", "oob_ip"])
@pytest.mark.parametrize("active", ["primary", "oob"])
@pytest.mark.parametrize("remove", [False, True])
async def test_upsert_repairs_invalid_stored_address(adapter_client, invalid_field, active, remove):
    from datetime import UTC, datetime

    from nso_adapter.core.failover import upsert_failover_ips

    device_id = await _seed(primary="198.18.0.1", oob="198.18.0.2", active=active)
    far = datetime(2099, 1, 1, tzinfo=UTC)
    async with session() as db:
        device = await db.get(Device, device_id)
        row = await db.scalar(select(DeviceFailover).where(DeviceFailover.device_id == device_id))
        setattr(row, invalid_field, "not-an-ip")
        row.failback_blocked_reason = "stored_address_invalid"
        row.next_primary_probe_at = row.next_oob_probe_at = far
        row.consecutive_failures = 2
        row.consecutive_successes = 4
        row.last_probe_result = "unreachable"
        row.oob_healthy = False
        row.oob_health_result = "unreachable"
        await db.commit()
        primary = None if remove and invalid_field == "primary_ip" else "198.18.0.1"
        oob = None if remove and invalid_field == "oob_ip" else "198.18.0.2"

        assert await upsert_failover_ips(db, device, primary, oob)
        await db.commit()
        await db.refresh(row)

        assert (row.primary_ip, row.oob_ip) == (primary, oob)
        assert row.failback_blocked_reason is None
        assert row.active_address == active
        if invalid_field == "primary_ip":
            assert (row.consecutive_failures, row.consecutive_successes) == (0, 0)
            assert row.last_probe_result is None
            if not remove:
                assert row.next_primary_probe_at < far
            assert row.next_oob_probe_at == far
        else:
            assert row.oob_healthy is None
            assert row.oob_health_result is None
            if not remove:
                assert row.next_oob_probe_at < far
            assert row.next_primary_probe_at == far


async def test_plugin_scope_preserves_bootstrapped_oob(adapter_client_with_nso, monkeypatch):
    sim = _NsoSim(address="192.0.2.5")
    sim.reachable_addrs = {"192.0.2.5"}
    client = _client_for(sim)
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda *_: client)

    response = await adapter_client_with_nso.post(
        "/api/v1/devices",
        json={"nso_instance": "nso-dev", "nso_device_name": "ra1", "netbox_device_id": 42},
        headers=AUTH,
    )
    assert response.status_code == 201
    device_id = response.json()["id"]
    response = await adapter_client_with_nso.put(
        f"/api/v1/devices/{device_id}/scope",
        json={"attributes": ["description"], "primary_ip": "10.0.0.1", "oob_ip": "192.0.2.5"},
        headers=AUTH,
    )
    assert response.status_code == 200
    assert (await _load(device_id)).active_address == "primary"

    await sched._scheduled_failover_probe()

    row = await _load(device_id)
    assert "10.0.0.1" not in sim.patches
    assert sim.address == "192.0.2.5"
    assert row.active_address == "oob"
    assert row.last_probe_target == "oob"
    assert row.last_probe_result == "ok"

    from nso_adapter.config import get_config

    sim.reachable_addrs.add("10.0.0.1")
    threshold = get_config().scheduler.failover_success_threshold
    for _ in range(threshold - 1):
        await _arm(device_id)
        await sched._scheduled_failover_probe()
        assert (await _load(device_id)).active_address == "oob"
        assert sim.address == "192.0.2.5"
    await _arm(device_id)
    await sched._scheduled_failover_probe()
    assert (await _load(device_id)).active_address == "primary"
    assert sim.address == "10.0.0.1"


async def test_scheduler_reconciles_oob_only_row_with_default_primary_role(adapter_client, monkeypatch):
    from datetime import UTC, datetime

    sim = _NsoSim(address="198.18.0.5")
    sim.reachable_addrs = {"198.18.0.5"}
    client = _client_for(sim)
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda *_: client)
    async with session() as db:
        dev = Device(nso_instance="nso-dev", nso_device_name="ra1", netbox_device_id=42)
        db.add(dev)
        await db.flush()
        device_id = dev.id
        db.add(DeviceFailover(device_id=device_id, primary_ip=None, oob_ip="198.18.0.5"))
        await db.commit()
        assert device_id in await sched._due_failover_device_ids(db, datetime.now(UTC))

    assert (await _load(device_id)).active_address == "primary"
    await sched._scheduled_failover_probe()

    row = await _load(device_id)
    assert row.active_address == "oob"
    assert row.manual_override is False
    assert row.last_probe_target == "oob"
    assert row.last_probe_result == "ok"
    assert row.next_oob_probe_at is not None
    assert sim.connects == 1
    assert sim.patches == []


async def test_fresh_device_fails_over_to_oob_then_back(adapter_client, monkeypatch):
    """The headline scenario: a fresh box only reachable on OOB → NSO bootstraps over OOB,
    then transparently fails back to primary once the in-band address comes up."""
    sim = _NsoSim(address="10.0.0.1")
    sim.reachable_addrs = {"192.0.2.5"}  # only OOB works on a fresh device
    client = _client_for(sim)
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda *_: client)
    device_id = await _seed()

    # ── Fail over: primary unreachable for failure_threshold ticks → switch to OOB.
    from nso_adapter.config import get_config

    cfg = get_config().scheduler
    for _ in range(cfg.failover_failure_threshold):
        await _arm_and_load(device_id)
        await sched._scheduled_failover_probe()

    row = await _arm_and_load(device_id)
    assert row.active_address == ActiveAddress.oob.value
    assert sim.address == "192.0.2.5"
    assert "192.0.2.5" in sim.patches
    assert row.last_switch_at is not None

    # ── Recover: primary comes up; after success_threshold flip-probes → fail back.
    sim.reachable_addrs.add("10.0.0.1")
    for _ in range(cfg.failover_success_threshold):
        await _arm_and_load(device_id)
        await sched._scheduled_failover_probe()

    row = await _arm_and_load(device_id)
    assert row.active_address == ActiveAddress.primary.value
    assert sim.address == "10.0.0.1"


async def test_unlinked_device_is_ignored(adapter_client, monkeypatch):
    """A device with no DeviceFailover row (e.g. not plugin-linked) is never probed."""
    sim = _NsoSim()
    client = _client_for(sim)
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda *_: client)

    async with session() as db:
        db.add(Device(nso_instance="nso-dev", nso_device_name="lonely", netbox_device_id=7))
        await db.commit()

    await sched._scheduled_failover_probe()
    assert sim.connects == 0  # no failover row → not in the join → never touched


async def test_ingestion_helpers_seed_and_upsert_ips(adapter_client):
    """IP ingestion changes IPs without changing the active role.

    ONLY the IPs — the ACTIVE address in particular is never touched. The per-address probe
    verdicts (counters, last probe) are a different matter: see the reset test below.
    """
    from nso_adapter.core.failover import upsert_failover_ips

    async with session() as db:
        dev = Device(nso_instance="nso-dev", nso_device_name="up1", netbox_device_id=55)
        db.add(dev)
        await db.flush()

        fo = DeviceFailover(
            device_id=dev.id, primary_ip="10.0.0.1", oob_ip="192.0.2.5", active_address=ActiveAddress.oob.value
        )
        db.add(fo)
        await db.commit()
        assert (fo.primary_ip, fo.oob_ip, fo.active_address) == ("10.0.0.1", "192.0.2.5", "oob")

        # Hand-set live state, then upsert new IPs.
        fo.consecutive_successes = 4
        await db.commit()
        changed = await upsert_failover_ips(db, dev, "10.0.0.2", "192.0.2.5")
        assert changed is True  # primary changed
        assert await upsert_failover_ips(db, dev, "10.0.0.2", "192.0.2.5") is False  # idempotent
        await db.commit()

        reloaded = (await db.execute(select(DeviceFailover).where(DeviceFailover.device_id == dev.id))).scalar_one()
        assert reloaded.primary_ip == "10.0.0.2"
        assert reloaded.active_address == "oob"  # never touched by the IP upsert
        # The 4 successes were 4 successes against 10.0.0.1, which is gone. They say nothing
        # about 10.0.0.2, and counting them toward the failback to it would fail back on a
        # verdict never earned. (This assertion used to read `== 4`.)
        assert reloaded.consecutive_successes == 0


async def test_a_changed_primary_does_not_inherit_the_old_addresss_failure_count(adapter_client):
    """The failure threshold is hysteresis: N consecutive failures before we flip to OOB.

    A device sitting one probe below the threshold on a DEAD primary is exactly the device
    an operator repoints at a working management IP. The upsert re-arms the probe to fire
    promptly — but it used to leave consecutive_failures at the old address's tally, so the
    first probe of the NEW address flipped the device to OOB the moment it so much as
    blipped. The hysteresis had already been spent on an address that no longer exists.
    """
    from nso_adapter.core.failover import upsert_failover_ips

    async with session() as db:
        dev = Device(nso_instance="nso-dev", nso_device_name="reset1", netbox_device_id=58)
        db.add(dev)
        await db.flush()
        fo = DeviceFailover(
            device_id=dev.id, primary_ip="10.0.0.1", oob_ip="192.0.2.9", active_address=ActiveAddress.primary.value
        )
        db.add(fo)
        fo.consecutive_failures = 2  # the old primary is all but declared dead
        fo.last_probe_result = "fail"
        await db.commit()

        assert await upsert_failover_ips(db, dev, "10.0.0.77", "192.0.2.9") is True
        await db.commit()

        reloaded = (await db.execute(select(DeviceFailover).where(DeviceFailover.device_id == dev.id))).scalar_one()
        assert reloaded.primary_ip == "10.0.0.77"
        assert reloaded.consecutive_failures == 0, "the new address starts with its full hysteresis budget"
        assert reloaded.last_probe_result is None, "and with no verdict about the address it replaced"
        assert reloaded.active_address == "primary"  # still not the upsert's business


async def test_upsert_new_oob_rearms_probe_schedule(adapter_client):
    """A NEW/changed OOB IP must be probed promptly. The probe schedule backs off while
    the OLD OOB is absent/dead, so without a re-arm a freshly configured OOB sits behind
    hours of accumulated deferral (sw03: next_oob_probe_at ~7h out while the operator
    added the OOB precisely to restore reachability NOW). The old address's health
    verdict is stale for the new one → reset. The primary schedule is untouched."""
    from datetime import UTC, datetime

    from nso_adapter.core.failover import upsert_failover_ips

    async with session() as db:
        dev = Device(nso_instance="nso-dev", nso_device_name="rearm1", netbox_device_id=57)
        db.add(dev)
        await db.flush()
        fo = DeviceFailover(
            device_id=dev.id, primary_ip="10.0.0.1", oob_ip=None, active_address=ActiveAddress.primary.value
        )
        db.add(fo)
        far = datetime(2099, 1, 1, tzinfo=UTC)
        fo.next_oob_probe_at = far
        fo.next_primary_probe_at = far
        fo.oob_healthy = True  # stale claim (about no/old OOB)
        await db.commit()

        assert await upsert_failover_ips(db, dev, "10.0.0.1", "192.0.2.9") is True
        await db.commit()

        reloaded = (await db.execute(select(DeviceFailover).where(DeviceFailover.device_id == dev.id))).scalar_one()
        assert reloaded.next_oob_probe_at < far  # due promptly, not behind old backoff
        assert reloaded.oob_healthy is None  # no verdict about the new address yet
        assert reloaded.oob_health_result is None
        assert reloaded.oob_health_detail is None
        assert reloaded.next_primary_probe_at == far  # primary unchanged → schedule untouched


async def test_upsert_new_primary_rearms_primary_probe_only(adapter_client):
    from datetime import UTC, datetime

    from nso_adapter.core.failover import upsert_failover_ips

    async with session() as db:
        dev = Device(nso_instance="nso-dev", nso_device_name="rearm2", netbox_device_id=58)
        db.add(dev)
        await db.flush()
        fo = DeviceFailover(
            device_id=dev.id, primary_ip="10.0.0.1", oob_ip="192.0.2.5", active_address=ActiveAddress.primary.value
        )
        db.add(fo)
        far = datetime(2099, 1, 1, tzinfo=UTC)
        fo.next_oob_probe_at = far
        fo.next_primary_probe_at = far
        await db.commit()

        assert await upsert_failover_ips(db, dev, "10.0.0.2", "192.0.2.5") is True
        await db.commit()

        reloaded = (await db.execute(select(DeviceFailover).where(DeviceFailover.device_id == dev.id))).scalar_one()
        assert reloaded.next_primary_probe_at < far
        assert reloaded.next_oob_probe_at == far  # OOB unchanged → schedule untouched


async def test_upsert_skips_empty_row_creation(adapter_client):
    """A device reported with no IPs (older plugin) must NOT get an empty failover row."""
    from nso_adapter.core.failover import upsert_failover_ips

    async with session() as db:
        dev = Device(nso_instance="nso-dev", nso_device_name="noips", netbox_device_id=56)
        db.add(dev)
        await db.flush()
        assert await upsert_failover_ips(db, dev, None, None) is False
        await db.commit()
        row = (await db.execute(select(DeviceFailover).where(DeviceFailover.device_id == dev.id))).scalar_one_or_none()
        assert row is None  # no empty row created


async def _seed_config(**kw) -> None:
    """Insert the FailoverConfig singleton with overrides (the plugin would PUT these)."""
    async with session() as db:
        db.add(FailoverConfig(**kw))
        await db.commit()
        return


async def _seed_extra(name: str, netbox_id: int, primary: str, oob: str, active: str = "primary") -> int:
    async with session() as db:
        dev = Device(nso_instance="nso-dev", nso_device_name=name, netbox_device_id=netbox_id)
        db.add(dev)
        await db.flush()
        db.add(DeviceFailover(device_id=dev.id, primary_ip=primary, oob_ip=oob, active_address=active))
        await db.commit()
        return dev.id


async def _load(device_id: int) -> DeviceFailover:
    async with session() as db:
        row = (await db.execute(select(DeviceFailover).where(DeviceFailover.device_id == device_id))).scalar_one()
        db.expunge(row)
        return row


async def _arm(device_id: int, *, primary_due: bool = True, oob_due: bool = False) -> None:
    """Set each address's due-time precisely (None = due now, far-future = not due)."""
    from datetime import UTC, datetime

    far = datetime(2030, 1, 1, tzinfo=UTC)
    async with session() as db:
        row = (await db.execute(select(DeviceFailover).where(DeviceFailover.device_id == device_id))).scalar_one()
        row.next_primary_probe_at = None if primary_due else far
        row.next_oob_probe_at = None if oob_due else far
        await db.commit()
        return


async def test_effective_config_falls_back_then_reads_db(adapter_client):
    """get_effective_failover_config returns SchedulerConfig fallbacks with no row, the row after."""
    from nso_adapter.config import get_config
    from nso_adapter.core.failover import get_effective_failover_config

    async with session() as db:
        eff = await get_effective_failover_config(db, get_config().scheduler)
        assert eff.enabled is True
        assert eff.failover_failure_threshold == get_config().scheduler.failover_failure_threshold

    await _seed_config(enabled=False, failure_threshold=2, probe_concurrency=3, max_flips_per_tick=1)
    async with session() as db:
        eff = await get_effective_failover_config(db, get_config().scheduler)
        assert eff.enabled is False
        assert eff.failover_failure_threshold == 2
        assert eff.probe_concurrency == 3
        assert eff.max_flips_per_tick == 1


async def test_disabled_config_makes_tick_a_noop(adapter_client, monkeypatch):
    """FailoverConfig.enabled=False → the base tick probes nothing (live off-switch)."""
    sim = _NsoSim(address="10.0.0.1")  # primary unreachable (empty reachable_addrs)
    client = _client_for(sim)
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda *_: client)
    device_id = await _seed()
    await _seed_config(enabled=False)

    for _ in range(5):
        await _arm_and_load(device_id)
        await sched._scheduled_failover_probe()

    assert sim.connects == 0  # never probed
    assert (await _load(device_id)).active_address == ActiveAddress.primary.value


async def test_live_db_threshold_drives_failover(adapter_client, monkeypatch):
    """A DB failure_threshold=2 fails the device over after 2 ticks (not the static default 3)."""
    sim = _NsoSim(address="10.0.0.1")
    sim.reachable_addrs = {"192.0.2.5"}  # only OOB works
    client = _client_for(sim)
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda *_: client)
    device_id = await _seed()
    await _seed_config(failure_threshold=2)

    for _ in range(2):
        await _arm_and_load(device_id)
        await sched._scheduled_failover_probe()

    assert (await _load(device_id)).active_address == ActiveAddress.oob.value  # switched at 2, per DB


async def test_concurrency_probes_all_due_devices(adapter_client, monkeypatch):
    """One tick probes every due device (each on its own session, gathered under the semaphore)."""
    addresses = {"ra1": "10.0.0.1", "rb1": "10.0.1.1", "rc1": "10.0.2.1"}
    sims = {name: _NsoSim(address=address) for name, address in addresses.items()}
    for sim in sims.values():
        sim.reachable_addrs = {sim.address}

    client = _client_for_devices(sims)
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda *_: client)
    ids = [
        await _seed_extra("ra1", 42, "10.0.0.1", "192.0.2.5"),
        await _seed_extra("rb1", 43, "10.0.1.1", "192.0.2.6"),
        await _seed_extra("rc1", 44, "10.0.2.1", "192.0.2.7"),
    ]
    for did in ids:
        await _arm(did, primary_due=True, oob_due=False)  # only the cheap primary liveness is due

    await sched._scheduled_failover_probe()

    assert all(sim.connects == 1 for sim in sims.values())
    for did in ids:
        assert (await _load(did)).next_primary_probe_at is not None  # each advanced (staggered)


@pytest.mark.parametrize("limit", [1, 2])
async def test_flip_budget_caps_flips_across_tick(adapter_client, monkeypatch, limit):
    """Only devices with flip budget can probe primary and commit failback."""
    sims = {"fa1": _NsoSim(address="192.0.2.5"), "fb1": _NsoSim(address="192.0.2.6")}
    for sim in sims.values():
        sim.reachable_addrs = {"10.0.0.1", "10.0.0.2"}
    client = _client_for_devices(sims)
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda *_: client)
    a_id = await _seed_extra("fa1", 51, "10.0.0.1", "192.0.2.5", active="oob")
    b_id = await _seed_extra("fb1", 52, "10.0.0.2", "192.0.2.6", active="oob")
    # One successful primary probe commits failback when budget permits the flip.
    await _seed_config(success_threshold=1, max_flips_per_tick=limit)
    # Only the failback (primary) probe is due — keep the OOB liveness out so the count is exact.
    await _arm(a_id, primary_due=True, oob_due=False)
    await _arm(b_id, primary_due=True, oob_due=False)

    await sched._scheduled_failover_probe()

    actives = sorted([(await _load(a_id)).active_address, (await _load(b_id)).active_address])
    assert actives == ["oob"] * (2 - limit) + ["primary"] * limit
    assert sum(sim.connects for sim in sims.values()) == limit
    assert sum(len(sim.patches) for sim in sims.values()) == limit
    rows = [await _load(device_id) for device_id in (a_id, b_id)]
    assert all(not row.manual_override for row in rows)


async def test_failback_flip_reverts_to_oob_when_probe_blows_up(adapter_client, monkeypatch):
    """A failback flip-probe that raises mid-flight must still revert NSO to OOB (not strand it).

    Guards the try/finally guaranteed revert: the device is on OOB, the flip points NSO at the
    primary, then the connect blows up. NSO must end back on the OOB address it can reach.
    """
    sim = _NsoSim(address="192.0.2.5")  # physically on OOB (where the device lives)
    sim.raise_on_connect_addr = "10.0.0.1"  # the primary probe blows up after the flip
    client = _client_for(sim)
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda *_: client)
    device_id = await _seed(active="oob")
    await _arm(device_id, primary_due=True, oob_due=False)

    await sched._scheduled_failover_probe()  # the RuntimeError is caught + rolled back by the tick

    assert sim.address == "192.0.2.5"  # reverted to OOB despite the blow-up (not left on primary)
    assert sim.patches[-1] == "192.0.2.5"  # last write was the guaranteed revert
    assert (await _load(device_id)).active_address == ActiveAddress.oob.value  # state unchanged


async def test_proactive_oob_flip_reverts_to_primary_when_probe_blows_up(adapter_client, monkeypatch):
    """A proactive OOB health flip-probe that raises must still flip NSO back to primary."""
    sim = _NsoSim(address="10.0.0.1")  # physically on primary
    sim.raise_on_connect_addr = "192.0.2.5"  # the OOB health probe blows up after the flip
    client = _client_for(sim)
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda *_: client)
    device_id = await _seed(active="primary")
    await _arm(device_id, primary_due=False, oob_due=True)  # only the proactive OOB probe is due

    await sched._scheduled_failover_probe()

    assert sim.address == "10.0.0.1"  # flipped back to primary despite the blow-up
    assert sim.patches[-1] == "10.0.0.1"  # last write was the guaranteed flip-back
    assert (await _load(device_id)).active_address == ActiveAddress.primary.value


async def test_device_on_oob_keeps_liveness_after_the_primary_ip_is_cleared(adapter_client, monkeypatch):
    """The plugin clearing the primary IP must not take a device sitting on OOB off the tick.

    OOB is the address the operator is connecting through, and its liveness needs no primary
    address — but both the due-device query and the tick used to require one, so such a device
    got zero health monitoring for as long as NetBox had no primary IP for it.
    """
    from nso_adapter.core.failover import upsert_failover_ips

    sim = _NsoSim(address="192.0.2.5")
    sim.reachable_addrs = {"192.0.2.5"}  # only OOB is up, which is why the device sits on it
    client = _client_for(sim)
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda *_: client)
    device_id = await _seed(active="oob")
    async with session() as db:
        dev = (await db.execute(select(Device).where(Device.id == device_id))).scalar_one()
        assert await upsert_failover_ips(db, dev, None, "192.0.2.5") is True  # NetBox lost the primary IP
        await db.commit()
    await _arm(device_id, primary_due=False, oob_due=True)

    await sched._scheduled_failover_probe()

    row = await _load(device_id)
    assert row.oob_healthy is True  # still monitored
    assert row.oob_health_checked_at is not None
    assert sim.patches == []  # cheap liveness, no flip
    assert row.active_address == ActiveAddress.oob.value


async def test_manual_override_clears_once_address_restored(adapter_client, monkeypatch):
    """A stale manual_override flag clears as soon as NSO is back on a managed address."""
    sim = _NsoSim(address="10.0.0.1")  # operator restored the managed (primary) address
    sim.always_reachable = True
    client = _client_for(sim)
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda *_: client)
    device_id = await _seed(active="primary")
    async with session() as db:
        row = (await db.execute(select(DeviceFailover).where(DeviceFailover.device_id == device_id))).scalar_one()
        row.manual_override = True  # left over from an earlier foreign-address detection
        await db.commit()
    await _arm(device_id, primary_due=True, oob_due=False)

    await sched._scheduled_failover_probe()

    assert (await _load(device_id)).manual_override is False  # cleared (current address is managed)


async def test_active_oob_report_conflict_keeps_liveness(adapter_client, monkeypatch):
    """A report cannot collapse the active OOB address into the primary role.

    NSO still dials the stored OOB address. Keep both established roles, surface the
    conflicting report, and continue the active-address liveness probe.
    """
    from nso_adapter.core.failover import upsert_failover_ips

    sim = _NsoSim(address="192.0.2.5")
    sim.reachable_addrs = {"192.0.2.5"}
    client = _client_for(sim)
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda *_: client)
    device_id = await _seed(active=ActiveAddress.oob.value)

    async with session() as db:
        dev = await db.get(Device, device_id)
        assert dev is not None
        assert await upsert_failover_ips(db, dev, "192.0.2.5", "192.0.2.5") is True
        await db.commit()

    await _arm(device_id, primary_due=True, oob_due=True)
    await sched._scheduled_failover_probe()

    row = await _load(device_id)
    assert (row.primary_ip, row.oob_ip) == ("10.0.0.1", "192.0.2.5")
    assert row.failback_blocked_reason == "active_oob_address_conflict"
    assert row.manual_override is False
    assert row.oob_healthy is True
    assert row.oob_health_checked_at is not None


async def test_successful_failback_clears_active_oob_conflict(adapter_client, monkeypatch):
    """A conflict marker is obsolete once the device returns to primary."""
    from nso_adapter.config import get_config
    from nso_adapter.core.failover import upsert_failover_ips

    sim = _NsoSim(address="192.0.2.5")
    sim.reachable_addrs = {"10.0.0.1", "192.0.2.5"}
    client = _client_for(sim)
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda *_: client)
    device_id = await _seed(active=ActiveAddress.oob.value)

    async with session() as db:
        dev = await db.get(Device, device_id)
        assert dev is not None
        assert await upsert_failover_ips(db, dev, "192.0.2.5", "192.0.2.5") is True
        row = (await db.execute(select(DeviceFailover).where(DeviceFailover.device_id == device_id))).scalar_one()
        row.consecutive_successes = get_config().scheduler.failover_success_threshold - 1
        await db.commit()

    await _arm(device_id, primary_due=True, oob_due=False)
    await sched._scheduled_failover_probe()

    row = await _load(device_id)
    assert row.active_address == ActiveAddress.primary.value
    assert row.failback_blocked_reason is None


async def test_active_oob_report_conflict_survives_address_read_failure(adapter_client, monkeypatch):
    """A transient NSO read failure cannot erase a durable ingestion conflict."""
    from nso_adapter.core.failover import upsert_failover_ips

    sim = _NsoSim(address="192.0.2.5")
    sim.reachable_addrs = {"192.0.2.5"}
    sim.get_address_failures = 1
    client = _client_for(sim)
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda *_: client)
    device_id = await _seed(active=ActiveAddress.oob.value)

    async with session() as db:
        dev = await db.get(Device, device_id)
        assert dev is not None
        assert await upsert_failover_ips(db, dev, "192.0.2.5", "192.0.2.5") is True
        await db.commit()

    await _arm(device_id, primary_due=True, oob_due=False)
    await sched._scheduled_failover_probe()
    assert (await _load(device_id)).failback_blocked_reason == "active_oob_address_conflict"

    await _arm(device_id, primary_due=True, oob_due=False)
    await sched._scheduled_failover_probe()
    assert (await _load(device_id)).failback_blocked_reason == "active_oob_address_conflict"


async def test_upsert_retains_active_oob_and_accepts_distinct_primary(adapter_client):
    """A conflict retains active OOB but accepts a distinct primary failback target.

    The stored OOB address is retained (the way back stays known), the stuck state is
    surfaced on the row, and a later usable OOB address clears it again.
    """
    from structlog.testing import capture_logs

    from nso_adapter.core.failover import upsert_failover_ips

    async with session() as db:
        dev = Device(nso_instance="nso-dev", nso_device_name="up-oob-clear", netbox_device_id=56)
        db.add(dev)
        await db.flush()
        fo = DeviceFailover(
            device_id=dev.id, primary_ip="10.0.0.1", oob_ip="192.0.2.5", active_address=ActiveAddress.oob.value
        )
        db.add(fo)
        await db.commit()

        with capture_logs() as logs:
            changed = await upsert_failover_ips(db, dev, "10.0.0.1", None)
        await db.commit()
        assert changed is True  # the surfaced stuck state is a change
        conflict_log = next(log for log in logs if log["event"] == "failover.active_oob_change_refused")
        assert conflict_log["device_id"] == dev.id
        assert_keys_absent(conflict_log, ["device"])
        fo = (await db.execute(select(DeviceFailover).where(DeviceFailover.device_id == dev.id))).scalar_one()
        assert fo.oob_ip == "192.0.2.5", "the address the device lives on must be retained"
        assert fo.failback_blocked_reason == "active_oob_address_conflict"

        # The degenerate report (oob == primary) is the same class.
        await upsert_failover_ips(db, dev, "10.0.0.1", "10.0.0.1")
        await db.commit()
        assert fo.oob_ip == "192.0.2.5"

        # Re-reporting the RETAINED address itself (the operator restored it in NetBox)
        # must clear the stuck marker even though the stored value never changed.
        changed = await upsert_failover_ips(db, dev, "10.0.0.1", "192.0.2.5")
        await db.commit()
        assert changed is True
        assert (fo.oob_ip, fo.failback_blocked_reason) == ("192.0.2.5", None)

        # Re-refuse so the different-address tail below still exercises its own clear.
        await upsert_failover_ips(db, dev, "10.0.0.1", None)
        await db.commit()
        assert fo.failback_blocked_reason == "active_oob_address_conflict"

        # A new primary is a safe failback target, but a usable OOB replacement still
        # cannot erase the address NSO is dialing.
        changed = await upsert_failover_ips(db, dev, "10.0.0.2", "192.0.2.9")
        await db.commit()
        assert changed is True
        assert (fo.primary_ip, fo.oob_ip, fo.failback_blocked_reason) == (
            "10.0.0.2",
            "192.0.2.5",
            "active_oob_address_conflict",
        )

        # Once failback completes, the next report can replace the inactive OOB address.
        fo.active_address = ActiveAddress.primary.value
        await db.commit()
        changed = await upsert_failover_ips(db, dev, "10.0.0.2", "192.0.2.9")
        await db.commit()
        assert changed is True
        assert (fo.oob_ip, fo.failback_blocked_reason) == ("192.0.2.9", None)


@pytest.mark.parametrize("field", ["primary_ip", "oob_ip"])
@pytest.mark.parametrize("address", ["", "198.18.0.1/32", " 198.18.0.1"])
async def test_stored_invalid_address_fails_closed(adapter_client, monkeypatch, debug_logs, field, address):
    from datetime import UTC, datetime

    sim = _NsoSim(address="198.18.0.1")
    sim.always_reachable = True
    client = _client_for(sim)
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda *_: client)
    device_id = await _seed(
        primary=address if field == "primary_ip" else None,
        oob=address if field == "oob_ip" else None,
    )

    await sched._scheduled_failover_probe()

    errors = [entry for entry in debug_logs if entry["event"] == "failover.stored_address_invalid"]
    assert len(errors) == 1
    assert errors[0] == {
        "event": "failover.stored_address_invalid",
        "log_level": "error",
        "device_id": device_id,
        "fields": [field],
    }
    assert sim.connects == 0
    assert sim.patches == []
    row = await _load(device_id)
    assert getattr(row, field) == address
    assert row.failback_blocked_reason == "stored_address_invalid"
    async with session() as db:
        assert device_id not in await sched._due_failover_device_ids(db, datetime.now(UTC))


@pytest.mark.parametrize("role", ["primary", "oob"])
@pytest.mark.parametrize("address", ["2001:DB8::1", "2001:0db8:0000:0000:0000:0000:0000:0001"])
async def test_scheduler_monitors_equivalent_ipv6_address(adapter_client, monkeypatch, role, address):
    sim = _NsoSim(address=address)
    sim.always_reachable = True
    client = _client_for(sim)
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda *_: client)
    device_id = await _seed(
        primary="2001:db8::1" if role == "primary" else None,
        oob="2001:db8::1" if role == "oob" else None,
    )
    await sched._scheduled_failover_probe()
    row = await _load(device_id)
    assert row.active_address == role
    assert row.manual_override is False
    assert row.last_probe_target == role
    assert row.last_probe_result == "ok"
    assert sim.connects == 1
    assert sim.patches == []


async def test_scheduler_keeps_mapped_ipv6_foreign_to_ipv4(adapter_client, monkeypatch):
    # No vendor has been observed to report the mapped form.
    sim = _NsoSim(address="::ffff:198.18.0.1")
    client = _client_for(sim)
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda *_: client)
    device_id = await _seed(primary="198.18.0.1", oob=None)
    await sched._scheduled_failover_probe()
    assert (await _load(device_id)).manual_override is True
    assert sim.connects == 0
    assert sim.patches == []


@pytest.mark.parametrize("address", ["", "invalid", "198.18.0.1/32", " 198.18.0.1"])
async def test_scheduler_treats_invalid_nso_address_as_unreadable(adapter_client, monkeypatch, address):
    sim = _NsoSim(address=address)
    client = _client_for(sim)
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda *_: client)
    device_id = await _seed(primary="198.18.0.1", oob=None)
    await sched._scheduled_failover_probe()
    row = await _load(device_id)
    assert row.failback_blocked_reason == "address_unreadable"
    assert row.manual_override is False
    assert sim.connects == 0
    assert sim.patches == []


async def test_ingestion_preserves_verdicts_for_equivalent_ipv6_addresses(adapter_client):
    from nso_adapter.core.failover import upsert_failover_ips

    device_id = await _seed(primary="2001:DB8::1", oob="2001:0db8:0:0:0:0:0:2", active="oob")
    async with session() as db:
        device = await db.get(Device, device_id)
        fo = (await db.execute(select(DeviceFailover).where(DeviceFailover.device_id == device_id))).scalar_one()
        fo.consecutive_successes = 2
        fo.oob_healthy = True
        assert await upsert_failover_ips(db, device, "2001:db8::1", "2001:DB8::2") is True
        await db.commit()
    row = await _load(device_id)
    assert (row.primary_ip, row.oob_ip) == ("2001:db8::1", "2001:db8::2")
    assert row.failback_blocked_reason is None
    assert row.consecutive_successes == 2
    assert row.oob_healthy is True
