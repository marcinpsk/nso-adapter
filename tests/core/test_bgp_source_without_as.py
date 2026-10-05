# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""A device-read BGP redistribution source without an AS (Junos, TiMOS) is valid content."""

import pytest
from sqlalchemy import select

from nso_adapter.domain.asn import AsnRuleViolation, asn_row_identity, validate_source_as_numbers
from nso_adapter.store.models import Device
from tests.conftest import AUTH, seed_device, session

_UNQUALIFIED = {"source-protocol": "bgp", "source-ref": ""}


@pytest.mark.parametrize("protocol", ["ospf", "isis"])
async def test_protocol_refresh_stores_bgp_source_without_as(adapter_client, protocol):
    from nso_adapter.core.isis import refresh_isis_interfaces_for_device
    from nso_adapter.core.ospf import refresh_ospf_for_device
    from nso_adapter.store.models import DeviceIsisProcess, DeviceOspfInstance

    refresh, model, list_key, identity_key = (
        (refresh_ospf_for_device, DeviceOspfInstance, "instance", "process-id")
        if protocol == "ospf"
        else (refresh_isis_interfaces_for_device, DeviceIsisProcess, "process", "process-tag")
    )
    process = {identity_key: "placeholder-new", "redistribute": [_UNQUALIFIED]}

    class JunosShapedRead:
        async def get_device_state_section(self, device_name, section):
            return {"status": "ok", list_key: [process]}

    device_id = await seed_device(nso_device_name="placeholder-device")
    async with session() as db:
        device = await db.get(Device, device_id)
        await refresh(db, device, JunosShapedRead())
        rows = (await db.scalars(select(model).where(model.device_id == device_id))).all()
        assert [getattr(row, identity_key.replace("-", "_")) for row in rows] == ["placeholder-new"]


async def test_redistribution_refresh_stores_and_serves_bgp_source_without_as(adapter_client):
    from nso_adapter.core.redistribution import refresh_redistribution_from_outcomes
    from nso_adapter.nso.read_outcome import Freshness, Present
    from nso_adapter.store.models import DeviceRedistribution

    device_id = await seed_device(nso_device_name="placeholder-device")
    isis = {"process": [{"process-tag": "placeholder-process", "redistribute": [_UNQUALIFIED]}]}
    outcomes = {p: Present(isis if p == "isis" else {}, Freshness.fresh) for p in ["ospf", "isis", "bgp"]}
    async with session() as db:
        device = await db.get(Device, device_id)
        assert await refresh_redistribution_from_outcomes(db, device, outcomes)
        rows = (await db.scalars(select(DeviceRedistribution))).all()
        assert [(row.source_protocol, row.source_ref) for row in rows] == [("bgp", "")]
    response = await adapter_client.get(f"/api/v1/devices/{device_id}/redistribution", headers=AUTH)
    assert response.status_code == 200
    assert response.json()["entries"][0]["source_ref"] == ""


def test_identity_accepts_bgp_source_without_as_only_on_device_reads():
    row = {"id": 1, "dest_protocol": "isis", "dest_ref": "core", "source_protocol": "bgp", "source_ref": ""}
    assert asn_row_identity("device_redistribution", row) == ("isis", "core", "bgp", "")
    with pytest.raises(AsnRuleViolation):
        asn_row_identity("redistribution_intent", row)


@pytest.mark.parametrize("rejected", ["064512", " ", "4294967296"])
def test_nonempty_bgp_source_still_needs_rfc5396(rejected):
    with pytest.raises(AsnRuleViolation):
        validate_source_as_numbers([{"source-protocol": "bgp", "source-ref": rejected}], "device_read.isis", "p[0]")
