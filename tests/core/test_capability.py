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
async def test_clear_capability_rejections_clears_only_applied_generic_scopes(adapter_client):  # noqa: F811
    """A clean commit clears the coarse apply-sourced rejection for the applied scopes only —
    a scope not in the applied set, and any route-policy fine-grained construct row, survive.
    Without this a scope rejected once would stay 'unsupported' forever (a probe cannot downgrade
    an apply-rejection)."""
    from nso_adapter.core.capability import (
        clear_capability_rejections,
        get_device_capability,
        record_capability_rejection,
    )
    from nso_adapter.store.db import get_session

    sw = "15.7"
    async for db in get_session():
        await record_capability_rejection(db, _NED, sw, "snmp", "snmp", "old error")
        await record_capability_rejection(db, _NED, sw, "isis", "isis", "old error")
        await record_capability_rejection(db, _NED, sw, "rm-set", "set extcommunity color", "fine-grained")

        cleared = await clear_capability_rejections(db, _NED, sw, {"snmp"})
        assert cleared == 1
        by_key = {(r.scope, r.name): r for r in await get_device_capability(db, _NED, sw)}
        assert ("snmp", "snmp") not in by_key  # applied scope → stale rejection cleared
        assert ("isis", "isis") in by_key  # NOT applied → untouched
        assert ("rm-set", "set extcommunity color") in by_key  # fine-grained → never cleared
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


async def test_empty_sw_version_resolves_on_cache_read(adapter_client, monkeypatch):  # noqa: F811
    """A NED that reports no software version (e.g. Nokia timos) stores its rows under
    ``sw_version=''`` and must still resolve on the cheap panel read — otherwise its capability
    reads 'unknown' forever even after a probe (the panel can't find the coverage row)."""
    from nso_adapter.core import capability
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device
    from tests.conftest import seed_device

    device_id = await seed_device(nso_device_name="nokia-ra1")

    async def fake_probe(_client, _name):
        return {
            "ned-id": "timos-nc-23.10",
            "sw-version": "",  # Nokia NED reports no version
            "element": [{"scope": "coverage", "name": "timos-nc-23.10", "status": "unknown", "detail": ""}],
        }

    monkeypatch.setattr(capability.actions, "capability_probe", fake_probe)

    async for db in get_session():
        device = await db.get(Device, device_id)
        info = await capability.resolve_capability_key(db, object(), device, refresh=True)
        assert info["ned_id"] == "timos-nc-23.10" and info["sw_version"] == ""

        refreshed = await db.get(Device, device_id)
        # The cache-only read MUST resolve the (ned, "") key, not bail to {} → 'unknown'.
        cached = await capability.resolve_capability_key(db, object(), refreshed, refresh=False)
        assert cached == {"ned_id": "timos-nc-23.10", "sw_version": "", "count": 0}
        # and the coverage row is findable → 'unassessed', not 'unknown'.
        rows = await capability.get_device_capability(db, "timos-nc-23.10", "")
        assert capability.coverage_unknown(rows)
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
    # A rejection whose command is the LAST line (no trailing newline) must still parse (#20).
    assert parse_rejected_construct("aborted: command: set tag 5") == ("rm-set", "set tag")


@pytest.mark.asyncio
async def test_refresh_ned_id_literal_none_not_persisted(adapter_client, monkeypatch):  # noqa: F811
    """A probe reporting the literal string 'None' for ned-id (an unselected device_type.cli)
    must NOT become a capability key or be persisted onto the device (#13)."""
    from nso_adapter.core import capability
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device
    from tests.conftest import seed_device

    device_id = await seed_device(nso_device_name="rgX")

    async def fake_probe(_client, _name):
        return {"ned-id": "None", "sw-version": "None", "element": []}

    monkeypatch.setattr(capability.actions, "capability_probe", fake_probe)
    async for db in get_session():
        device = await db.get(Device, device_id)
        res = await capability.refresh_device_capability(db, object(), "rgX", device)
        assert res == {}  # 'None' ned-id → treated as no NED, nothing recorded
        assert device.ned_id != "None"  # never persisted as a bogus key
        break


@pytest.mark.asyncio
async def test_record_probe_capability_tolerates_single_element_dict(adapter_client):  # noqa: F811
    """RESTCONF may render a singleton `element` list as a bare object; record_probe_capability
    must coerce it, not crash iterating dict keys (#24)."""
    from nso_adapter.core.capability import get_device_capability, record_probe_capability
    from nso_adapter.store.db import get_session

    single = {"scope": "rm-set", "name": "set extcommunity", "status": "native", "detail": ""}
    async for db in get_session():
        count = await record_probe_capability(db, _NED, "1.0", single)  # a dict, not a list
        assert count == 1
        rows = await get_device_capability(db, _NED, "1.0")
        assert {(r.scope, r.name) for r in rows} == {("rm-set", "set extcommunity")}
        break


# ── preflight_scopes (generic apply-preflight: scope-level matrix check) ────────


def test_preflight_scopes_flags_unsupported_and_skipped():
    from types import SimpleNamespace

    from nso_adapter.core.capability import preflight_scopes

    rows = [
        SimpleNamespace(scope="static_route", name="static_route", status="unsupported", detail="NED rejected"),
        SimpleNamespace(scope="snmp", name="snmp", status="skipped", detail="no home"),
        SimpleNamespace(scope="bgp", name="bgp", status="native", detail=""),
    ]
    result = preflight_scopes(rows, ["static_route", "snmp", "bgp", "ospf"])
    assert result["fully_supported"] is False
    flagged = {(u["scope"], u["status"]) for u in result["unsupported"]}
    assert flagged == {("static_route", "unsupported"), ("snmp", "skipped")}
    # bgp is native (not flagged); ospf has no matrix row (not flagged)


def test_preflight_scopes_only_checks_requested_scopes():
    from types import SimpleNamespace

    from nso_adapter.core.capability import preflight_scopes

    rows = [SimpleNamespace(scope="static_route", name="static_route", status="unsupported", detail="x")]
    # static_route is unsupported but NOT requested → not flagged
    assert preflight_scopes(rows, ["bgp"]) == {"fully_supported": True, "unsupported": []}


def test_preflight_scopes_empty_request_is_fully_supported():
    from types import SimpleNamespace

    from nso_adapter.core.capability import preflight_scopes

    rows = [SimpleNamespace(scope="static_route", name="static_route", status="unsupported", detail="x")]
    assert preflight_scopes(rows, []) == {"fully_supported": True, "unsupported": []}


# ── record_read_capability (the READ half, fed by the vendor-test read matrix) ──


@pytest.mark.asyncio
async def test_record_read_capability_writes_read_sourced_rows(adapter_client):  # noqa: F811
    """Read elements land as (scope, name='read') rows with source='read'."""
    from nso_adapter.core.capability import get_device_capability, record_read_capability
    from nso_adapter.store.db import get_session

    async for db in get_session():
        count = await record_read_capability(
            db,
            _NED,
            "17.15.4c",
            [
                {"scope": "bgp", "status": "native", "detail": "read 11 item(s) on rg03"},
                {"scope": "vlan", "status": "skipped", "detail": "not applicable on this platform"},
                {"scope": "isis", "status": "unknown", "detail": "reads empty on rg03"},
                {"scope": "ospf", "status": "unsupported", "detail": "read raised: boom"},
            ],
        )
        assert count == 4
        rows = {(r.scope, r.name): r for r in await get_device_capability(db, _NED, "17.15.4c")}
        assert rows[("bgp", "read")].status == "native"
        assert rows[("bgp", "read")].source == "read"
        assert rows[("vlan", "read")].status == "skipped"
        assert rows[("isis", "read")].status == "unknown"
        assert rows[("ospf", "read")].status == "unsupported"
        break


@pytest.mark.asyncio
async def test_read_unknown_never_downgrades_a_definite_read_row(adapter_client):  # noqa: F811
    """A no-information 'unknown' (empty read on a device without the config) must not
    clobber a definite verdict learned from another device on the same (ned, sw) key;
    a later definite observation upgrades an 'unknown'."""
    from nso_adapter.core.capability import get_device_capability, record_read_capability
    from nso_adapter.store.db import get_session

    sw = "23.10.R3"
    async for db in get_session():
        await record_read_capability(db, _NED, sw, [{"scope": "bgp", "status": "native", "detail": "read on A"}])
        await record_read_capability(db, _NED, sw, [{"scope": "bgp", "status": "unknown", "detail": "empty on B"}])
        rows = {(r.scope, r.name): r for r in await get_device_capability(db, _NED, sw)}
        assert rows[("bgp", "read")].status == "native"  # definite survives

        await record_read_capability(db, _NED, sw, [{"scope": "isis", "status": "unknown", "detail": "empty on B"}])
        await record_read_capability(db, _NED, sw, [{"scope": "isis", "status": "native", "detail": "read on A"}])
        rows = {(r.scope, r.name): r for r in await get_device_capability(db, _NED, sw)}
        assert rows[("isis", "read")].status == "native"  # definite upgrades unknown
        break


@pytest.mark.asyncio
async def test_read_rows_coexist_with_apply_rows_for_the_same_scope(adapter_client):  # noqa: F811
    """Read-support and write-rejection are different facts: a read row must not overwrite
    the coarse apply row (name == scope), and a clean-commit clear removes only the apply row."""
    from nso_adapter.core.capability import (
        clear_capability_rejections,
        get_device_capability,
        record_capability_rejection,
        record_read_capability,
    )
    from nso_adapter.store.db import get_session

    sw = "7.11.2"
    async for db in get_session():
        await record_capability_rejection(db, _NED, sw, "bgp", "bgp", "NED rejected")
        await record_read_capability(db, _NED, sw, [{"scope": "bgp", "status": "native", "detail": "read fine"}])
        rows = {(r.scope, r.name): r for r in await get_device_capability(db, _NED, sw)}
        assert rows[("bgp", "bgp")].status == "unsupported"  # the apply fact survives
        assert rows[("bgp", "read")].status == "native"  # alongside the read fact

        await clear_capability_rejections(db, _NED, sw, ["bgp"])
        rows = {(r.scope, r.name): r for r in await get_device_capability(db, _NED, sw)}
        assert ("bgp", "bgp") not in rows  # apply rejection cleared by the clean commit
        assert rows[("bgp", "read")].status == "native"  # read fact untouched
        break


@pytest.mark.asyncio
async def test_record_read_capability_skips_invalid_elements(adapter_client):  # noqa: F811
    from nso_adapter.core.capability import get_device_capability, record_read_capability
    from nso_adapter.store.db import get_session

    sw = "9.9.9"
    async for db in get_session():
        count = await record_read_capability(
            db,
            _NED,
            sw,
            [
                {"scope": "", "status": "native", "detail": ""},  # no scope
                {"scope": "bgp", "status": "would_apply", "detail": ""},  # not a read status
                "not-a-dict",
                {"scope": "ospf", "status": "native", "detail": ""},
            ],
        )
        assert count == 1
        rows = await get_device_capability(db, _NED, sw)
        assert [(r.scope, r.name, r.status) for r in rows] == [("ospf", "read", "native")]
        break


def test_preflight_scopes_flags_read_gap_rows():
    """A definite read gap ((scope, 'read') unsupported/skipped) participates in the generic
    apply-preflight: no reader for a scope on this NED strongly implies no writer either
    (per-NED handler pairs), so the operator is warned before the write fails loudly."""
    from types import SimpleNamespace

    from nso_adapter.core.capability import preflight_scopes

    rows = [
        SimpleNamespace(scope="bgp", name="read", status="unsupported", detail="expected read but empty"),
        SimpleNamespace(scope="vlan", name="read", status="skipped", detail="not applicable"),
        SimpleNamespace(scope="isis", name="read", status="unknown", detail="empty, no belief"),
        SimpleNamespace(scope="ospf", name="read", status="native", detail=""),
    ]
    result = preflight_scopes(rows, ["bgp", "vlan", "isis", "ospf"])
    assert result["fully_supported"] is False
    flagged = {(u["scope"], u["status"]) for u in result["unsupported"]}
    # unknown carries no verdict (fail-open) and native is positive — only definite gaps flag
    assert flagged == {("bgp", "unsupported"), ("vlan", "skipped")}
