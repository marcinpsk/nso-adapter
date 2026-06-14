# SPDX-License-Identifier: Apache-2.0
"""Capability matrix store: probe/apply upserts, apply-wins precedence, (sw,ned) keying."""

from __future__ import annotations

import pytest

# adapter_client inits the DB (runs app lifespan -> init_db -> create_all).
from tests.conftest import adapter_client  # noqa: F401

_NED = "cisco-ios-cli-6.114"


@pytest.mark.asyncio
async def test_probe_then_apply_precedence(adapter_client):  # noqa: F811
    from nso_adapter.core.capability import (
        get_device_capability,
        record_capability_rejection,
        record_probe_capability,
    )
    from nso_adapter.store.db import get_session

    sw = "15.2(4)E10"
    async for db in get_session():
        await record_probe_capability(
            db,
            _NED,
            sw,
            [
                {"scope": "rm-set", "name": "set extcommunity", "status": "native", "detail": ""},
                {"scope": "community", "name": "color:0:128", "status": "skipped", "detail": "no home"},
            ],
        )
        rows = {(r.scope, r.name): r for r in await get_device_capability(db, _NED, sw)}
        assert rows[("rm-set", "set extcommunity")].status == "native"
        assert rows[("rm-set", "set extcommunity")].source == "probe"
        assert rows[("community", "color:0:128")].status == "skipped"

        # accepted-half rejection: sw03's device parser refused `set extcommunity color`
        await record_capability_rejection(db, _NED, sw, "rm-set", "set extcommunity", "% Invalid input")
        rows = {(r.scope, r.name): r for r in await get_device_capability(db, _NED, sw)}
        assert rows[("rm-set", "set extcommunity")].status == "unsupported"
        assert rows[("rm-set", "set extcommunity")].source == "apply"

        # a later representable probe must NOT downgrade the apply-recorded rejection
        await record_probe_capability(
            db, _NED, sw, [{"scope": "rm-set", "name": "set extcommunity", "status": "native", "detail": ""}]
        )
        rows = {(r.scope, r.name): r for r in await get_device_capability(db, _NED, sw)}
        assert rows[("rm-set", "set extcommunity")].status == "unsupported"  # apply wins
        break


@pytest.mark.asyncio
async def test_distinct_sw_version_is_separate_key(adapter_client):  # noqa: F811
    from nso_adapter.core.capability import get_device_capability, record_probe_capability
    from nso_adapter.store.db import get_session

    async for db in get_session():
        for sw in ("17.15.4c", "15.2(4)E10"):
            await record_probe_capability(
                db, _NED, sw, [{"scope": "rm-set", "name": "x", "status": "native", "detail": ""}]
            )
        assert len(await get_device_capability(db, _NED, "17.15.4c")) == 1
        assert len(await get_device_capability(db, _NED, "15.2(4)E10")) == 1
        break


@pytest.mark.asyncio
async def test_refresh_parses_and_stores_probe_output(adapter_client, monkeypatch):  # noqa: F811
    from nso_adapter.core import capability
    from nso_adapter.store.db import get_session

    async def fake_probe(_client, _name):
        return {
            "ned-id": _NED,
            "sw-version": "17.15.4c",
            "element": [
                {"scope": "community", "name": "color:0:128", "status": "skipped", "detail": "no home"},
                {"scope": "rm-set", "name": "set extcommunity", "status": "native", "detail": ""},
            ],
        }

    monkeypatch.setattr(capability.actions, "capability_probe", fake_probe)
    async for db in get_session():
        res = await capability.refresh_device_capability(db, object(), "rg03")
        assert res == {"ned_id": _NED, "sw_version": "17.15.4c", "count": 2}
        rows = await capability.get_device_capability(db, _NED, "17.15.4c")
        assert {(r.scope, r.name) for r in rows} == {("community", "color:0:128"), ("rm-set", "set extcommunity")}
        break


@pytest.mark.asyncio
async def test_refresh_persists_key_then_cache_only_resolve(adapter_client, monkeypatch):  # noqa: F811
    """A probe persists (ned_id, sw_version) on the device so a later read needs no probe."""
    from nso_adapter.core import capability
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device
    from tests.conftest import seed_device

    device_id = await seed_device(nso_device_name="rg03")

    probe_calls = {"n": 0}

    async def fake_probe(_client, _name):
        probe_calls["n"] += 1
        return {
            "ned-id": _NED,
            "sw-version": "17.15.4c",
            "element": [{"scope": "rm-set", "name": "set extcommunity", "status": "native", "detail": ""}],
        }

    monkeypatch.setattr(capability.actions, "capability_probe", fake_probe)

    async for db in get_session():
        device = await db.get(Device, device_id)
        # refresh=False before any probe → key unknown, no probe fired
        assert await capability.resolve_capability_key(db, object(), device, refresh=False) == {}
        assert probe_calls["n"] == 0

        # refresh=True probes once and persists the learned key onto the device row
        info = await capability.resolve_capability_key(db, object(), device, refresh=True)
        assert info["ned_id"] == _NED and info["sw_version"] == "17.15.4c"
        assert probe_calls["n"] == 1
        refreshed = await db.get(Device, device_id)
        assert (refreshed.ned_id, refreshed.sw_version) == (_NED, "17.15.4c")

        # refresh=False now resolves from the stored key WITHOUT another probe
        cached = await capability.resolve_capability_key(db, object(), refreshed, refresh=False)
        assert cached == {"ned_id": _NED, "sw_version": "17.15.4c", "count": 0}
        assert probe_calls["n"] == 1
        break


def _row(scope, name, status, source="probe", detail=""):
    from nso_adapter.store.models import DeviceCapability

    return DeviceCapability(
        ned_id=_NED, sw_version="x", scope=scope, name=name, status=status, detail=detail, source=source
    )


def test_preflight_flags_community_kind_and_constructs():
    from nso_adapter.core.capability import preflight

    rows = [
        _row("community", "color:0:128", "skipped", detail="no IOS home"),
        _row("community", "65000:1", "native"),
        _row("rm-set", "set extcommunity", "native"),
        _row("rm-set", "set extcommunity color", "unsupported", source="apply", detail="% Invalid input"),
        _row("rm-set", "set metric-type", "native"),
    ]
    res = preflight(
        rows,
        community_members=["color:0:200", "65000:9"],  # color flagged (by kind), exact native
        set_keys=["extcommunity_color", "metric_type"],  # color flagged via apply row, metric-type ok
    )
    assert res["fully_supported"] is False
    flagged = {(u["scope"], u["element"]) for u in res["unsupported"]}
    assert ("community", "color:0:200") in flagged
    assert ("rm-set", "set extcommunity color") in flagged
    assert not any(u["element"] == "65000:9" for u in res["unsupported"])
    assert not any(u["element"] == "set metric-type" for u in res["unsupported"])


def test_preflight_all_native_is_fully_supported():
    from nso_adapter.core.capability import preflight

    rows = [_row("community", "65000:1", "native"), _row("rm-set", "set extcommunity", "native")]
    res = preflight(rows, community_members=["65000:5"], set_keys=["extcommunity_rt"])
    assert res == {"fully_supported": True, "unsupported": [], "coverage_unknown": False}


def test_preflight_flags_non_numeric_aspath_on_ios():
    """With the IOS 'as-path named-list unsupported' row, a named as-path is flagged; numeric is fine."""
    from nso_adapter.core.capability import preflight

    rows = [_row("as-path", "named-list", "unsupported", detail="IOS as-path is numbered 1-500")]
    res = preflight(rows, aspath_names=["AP-NAMED", "50", "501"])
    flagged = {u["element"] for u in res["unsupported"]}
    assert flagged == {"AP-NAMED", "501"}  # named + out-of-range; "50" is a valid number
    assert res["fully_supported"] is False


def test_preflight_aspath_not_flagged_without_named_list_row():
    """A NED that supports named as-path lists (no probe row) never flags an as-path name."""
    from nso_adapter.core.capability import preflight

    rows = [_row("community", "65000:1", "native")]  # e.g. IOS-XR/Junos — no as-path named-list row
    res = preflight(rows, aspath_names=["AP-NAMED"])
    assert res["fully_supported"] is True
    assert res["unsupported"] == []


def test_preflight_coverage_unknown_does_not_block():
    """An unassessed NED (coverage marker) reports coverage_unknown but stays fully_supported.

    Block only on a KNOWN-negative — a Junos/Nokia attach must not be blocked just because
    the probe hasn't classified that platform yet.
    """
    from nso_adapter.core.capability import coverage_unknown, preflight

    rows = [_row("coverage", "juniper-junos-nc-4.19", "unknown", detail="not yet implemented")]
    assert coverage_unknown(rows) is True
    res = preflight(rows, community_members=["color:0:200"], set_keys=["extcommunity_color"])
    assert res["fully_supported"] is True  # never block on unknown
    assert res["unsupported"] == []
    assert res["coverage_unknown"] is True
    # an assessed device has no coverage marker
    assert coverage_unknown([_row("community", "65000:1", "native")]) is False


def test_parse_rejected_construct():
    from nso_adapter.core.capability import parse_rejected_construct

    msg = "External error ...: command: set extcommunity color 12\r\n: ... % Invalid input"
    assert parse_rejected_construct(msg) == ("rm-set", "set extcommunity color")
    assert parse_rejected_construct("command: match as-path AP-X\n") == ("rm-match", "match as-path")
    assert parse_rejected_construct("command: set comm-list FOO delete\n") == ("rm-set", "set comm-list delete")
    assert parse_rejected_construct("no command here") == (None, None)
