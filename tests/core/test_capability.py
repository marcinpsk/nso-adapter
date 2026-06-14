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
