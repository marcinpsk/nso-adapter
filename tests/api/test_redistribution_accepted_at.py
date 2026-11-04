# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""A retained redistribution row carries the acceptance of its current parent snapshot."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import select

from nso_adapter.store.models import BgpRouterIntent, IsisProcessIntent, OspfInstanceIntent, RedistributionIntent
from tests.conftest import AUTH, push_seq, seed_device, session

pytestmark = pytest.mark.anyio
FIRST = "2026-10-20T12:00:00Z"
SECOND = "2026-10-21T08:30:00Z"
REDISTRIBUTION = [{"source_protocol": "connected", "source_ref": "", "route_map": None, "metric": None}]


def _bgp(accepted_at):
    router = {
        "asn": "65100",
        "scopes": [
            {"vrf": "", "address_families": [{"af": "ipv4-unicast", "redistribution": REDISTRIBUTION}], "peers": []}
        ],
    }
    return {"routers": [router | ({"accepted_at": accepted_at} if accepted_at else {})]}


def _isis(accepted_at):
    process = {"process_tag": "CORE", "redistribution": REDISTRIBUTION}
    return {"interfaces": [], "processes": [process | ({"accepted_at": accepted_at} if accepted_at else {})]}


def _ospf(accepted_at):
    instance = {"process_id": "1", "router_id": "1.1.1.1", "vrf": "", "areas": [], "redistribution": REDISTRIBUTION}
    return {"interfaces": [], "instances": [instance | ({"accepted_at": accepted_at} if accepted_at else {})]}


FAMILIES = {
    "bgp": ("bgp-intent", _bgp, BgpRouterIntent),
    "isis": ("isis-interface-intent", _isis, IsisProcessIntent),
    "ospf": ("ospf-intent", _ospf, OspfInstanceIntent),
}


async def _accepted(device_id: int, family: str):
    parent_model = FAMILIES[family][2]
    async with session() as db:
        parent = (await db.execute(select(parent_model).where(parent_model.device_id == device_id))).scalar_one()
        row = (
            await db.execute(select(RedistributionIntent).where(RedistributionIntent.device_id == device_id))
        ).scalar_one()
    return parent.accepted_at, row.accepted_at


@pytest.mark.parametrize("family", FAMILIES)
@pytest.mark.parametrize("second", [SECOND, None], ids=["explicit", "omitted"])
async def test_retained_redistribution_row_follows_its_parent_acceptance(adapter_client, family, second):
    endpoint, body, _ = FAMILIES[family]
    device_id = await seed_device(nso_device_name=f"redist-accepted-{family}", netbox_device_id=7881)
    for accepted_at in (FIRST, second):
        resp = await adapter_client.put(
            f"/api/v1/devices/{device_id}/{endpoint}", headers=AUTH | push_seq(), json=body(accepted_at)
        )
        assert resp.status_code == 200, resp.text

    parent_accepted, row_accepted = await _accepted(device_id, family)
    assert row_accepted == parent_accepted
    if second is not None:
        assert row_accepted == datetime(2026, 10, 21, 8, 30, tzinfo=UTC)
