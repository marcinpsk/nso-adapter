# SPDX-License-Identifier: Apache-2.0
"""End-to-end mgmt-IP failover: scheduler job → real NsoClient → real ORM.

Only the NSO socket is faked (a stateful httpx.MockTransport simulating NSO RESTCONF). The
scheduler job, the NsoClient request/URL/body construction, the failover state machine and
the SQLite store are all REAL — this is the whole-flow test the unit tests trust.
"""

from __future__ import annotations

import json

import httpx
from sqlalchemy import select

from nso_adapter.core import scheduler as sched
from nso_adapter.nso.client import NsoClient
from nso_adapter.store.db import get_session
from nso_adapter.store.models import ActiveAddress, Device, DeviceFailover, FailoverConfig


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

    def handler(self, request: httpx.Request) -> httpx.Response:
        url, method = str(request.url), request.method
        if method == "POST" and url.endswith("/connect"):
            self.connects += 1
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
    client = NsoClient(cfg, "admin", "admin")
    client._client = lambda timeout=None: httpx.AsyncClient(
        transport=httpx.MockTransport(sim.handler), base_url="http://nso-dev:8080"
    )
    return client


async def _seed(primary="10.0.0.1", oob="192.0.2.5", active="primary") -> int:
    async for db in get_session():
        dev = Device(nso_instance="nso-dev", nso_device_name="ra1", netbox_device_id=42)
        db.add(dev)
        await db.flush()
        db.add(DeviceFailover(device_id=dev.id, primary_ip=primary, oob_ip=oob, active_address=active))
        await db.commit()
        return dev.id
    raise AssertionError("no session")


async def _arm_and_load(device_id: int) -> DeviceFailover:
    """Force the primary probe due (clear staggering) and return a fresh copy of the row."""
    async for db in get_session():
        row = (await db.execute(select(DeviceFailover).where(DeviceFailover.device_id == device_id))).scalar_one()
        row.next_primary_probe_at = None
        await db.commit()
        await db.refresh(row)
        db.expunge(row)
        return row
    raise AssertionError("no session")


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

    async for db in get_session():
        db.add(Device(nso_instance="nso-dev", nso_device_name="lonely", netbox_device_id=7))
        await db.commit()
        break

    await sched._scheduled_failover_probe()
    assert sim.connects == 0  # no failover row → not in the join → never touched


async def test_ingestion_helpers_seed_and_upsert_ips(adapter_client):
    """set_initial_failover_state seeds a row; upsert_failover_ips changes ONLY the IPs."""
    from nso_adapter.core.failover import set_initial_failover_state, upsert_failover_ips

    async for db in get_session():
        dev = Device(nso_instance="nso-dev", nso_device_name="up1", netbox_device_id=55)
        db.add(dev)
        await db.flush()

        fo = await set_initial_failover_state(db, dev.id, "10.0.0.1", "192.0.2.5", ActiveAddress.oob.value)
        await db.commit()
        assert (fo.primary_ip, fo.oob_ip, fo.active_address) == ("10.0.0.1", "192.0.2.5", "oob")

        # Hand-set live state, then upsert new IPs — state must be preserved.
        fo.consecutive_successes = 4
        await db.commit()
        changed = await upsert_failover_ips(db, dev, "10.0.0.2", "192.0.2.5")
        assert changed is True  # primary changed
        assert await upsert_failover_ips(db, dev, "10.0.0.2", "192.0.2.5") is False  # idempotent
        await db.commit()

        reloaded = (await db.execute(select(DeviceFailover).where(DeviceFailover.device_id == dev.id))).scalar_one()
        assert reloaded.primary_ip == "10.0.0.2"
        assert reloaded.active_address == "oob"  # never touched by the IP upsert
        assert reloaded.consecutive_successes == 4
        break


async def test_upsert_skips_empty_row_creation(adapter_client):
    """A device reported with no IPs (older plugin) must NOT get an empty failover row."""
    from nso_adapter.core.failover import upsert_failover_ips

    async for db in get_session():
        dev = Device(nso_instance="nso-dev", nso_device_name="noips", netbox_device_id=56)
        db.add(dev)
        await db.flush()
        assert await upsert_failover_ips(db, dev, None, None) is False
        await db.commit()
        row = (await db.execute(select(DeviceFailover).where(DeviceFailover.device_id == dev.id))).scalar_one_or_none()
        assert row is None  # no empty row created
        break


async def _seed_config(**kw) -> None:
    """Insert the FailoverConfig singleton with overrides (the plugin would PUT these)."""
    async for db in get_session():
        db.add(FailoverConfig(**kw))
        await db.commit()
        return
    raise AssertionError("no session")


async def _seed_extra(name: str, netbox_id: int, primary: str, oob: str, active: str = "primary") -> int:
    async for db in get_session():
        dev = Device(nso_instance="nso-dev", nso_device_name=name, netbox_device_id=netbox_id)
        db.add(dev)
        await db.flush()
        db.add(DeviceFailover(device_id=dev.id, primary_ip=primary, oob_ip=oob, active_address=active))
        await db.commit()
        return dev.id
    raise AssertionError("no session")


async def _load(device_id: int) -> DeviceFailover:
    async for db in get_session():
        row = (await db.execute(select(DeviceFailover).where(DeviceFailover.device_id == device_id))).scalar_one()
        db.expunge(row)
        return row
    raise AssertionError("no session")


async def _arm(device_id: int, *, primary_due: bool = True, oob_due: bool = False) -> None:
    """Set each address's due-time precisely (None = due now, far-future = not due)."""
    from datetime import datetime

    far = datetime(2030, 1, 1)
    async for db in get_session():
        row = (await db.execute(select(DeviceFailover).where(DeviceFailover.device_id == device_id))).scalar_one()
        row.next_primary_probe_at = None if primary_due else far
        row.next_oob_probe_at = None if oob_due else far
        await db.commit()
        return
    raise AssertionError("no session")


async def test_effective_config_falls_back_then_reads_db(adapter_client):
    """get_effective_failover_config returns SchedulerConfig fallbacks with no row, the row after."""
    from nso_adapter.config import get_config
    from nso_adapter.core.failover import get_effective_failover_config

    async for db in get_session():
        eff = await get_effective_failover_config(db, get_config().scheduler)
        assert eff.enabled is True
        assert eff.failover_failure_threshold == get_config().scheduler.failover_failure_threshold
        break

    await _seed_config(enabled=False, failure_threshold=2, probe_concurrency=3, max_flips_per_tick=1)
    async for db in get_session():
        eff = await get_effective_failover_config(db, get_config().scheduler)
        assert eff.enabled is False
        assert eff.failover_failure_threshold == 2
        assert eff.probe_concurrency == 3
        assert eff.max_flips_per_tick == 1
        break


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
    sim = _NsoSim()
    sim.always_reachable = True  # address-agnostic up, so the shared sim serves all devices
    client = _client_for(sim)
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda *_: client)
    ids = [
        await _seed_extra("ra1", 42, "10.0.0.1", "192.0.2.5"),
        await _seed_extra("rb1", 43, "10.0.1.1", "192.0.2.6"),
        await _seed_extra("rc1", 44, "10.0.2.1", "192.0.2.7"),
    ]
    for did in ids:
        await _arm(did, primary_due=True, oob_due=False)  # only the cheap primary liveness is due

    await sched._scheduled_failover_probe()

    assert sim.connects == 3  # all three probed in the one tick (one cheap connect each)
    for did in ids:
        assert (await _load(did)).next_primary_probe_at is not None  # each advanced (staggered)


async def test_flip_budget_caps_flips_across_tick(adapter_client, monkeypatch):
    """max_flips_per_tick=1 lets only one of two OOB devices run its (disruptive) failback flip."""
    sim = _NsoSim()
    sim.always_reachable = True  # primary "recovered" for both
    client = _client_for(sim)
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda *_: client)
    a_id = await _seed_extra("fa1", 51, "10.0.0.1", "192.0.2.5", active="oob")
    b_id = await _seed_extra("fb1", 52, "10.0.0.2", "192.0.2.6", active="oob")
    # success_threshold=1 → a single good flip-probe commits failback; budget=1 → only one flips.
    await _seed_config(success_threshold=1, max_flips_per_tick=1)
    # Only the failback (primary) probe is due — keep the OOB liveness out so the count is exact.
    await _arm(a_id, primary_due=True, oob_due=False)
    await _arm(b_id, primary_due=True, oob_due=False)

    await sched._scheduled_failover_probe()

    actives = sorted([(await _load(a_id)).active_address, (await _load(b_id)).active_address])
    assert actives == [ActiveAddress.oob.value, ActiveAddress.primary.value]  # exactly one failed back
    assert sim.connects == 1  # the budget-skipped device never even probed
