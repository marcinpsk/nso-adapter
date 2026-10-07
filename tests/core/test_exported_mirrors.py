# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Exporter reads retain the mirror behavior captured from develop."""

from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest
from sqlalchemy import select

from nso_adapter.domain.observation import OBSERVERS, observe_family
from nso_adapter.store.models import (
    Device,
    DeviceIsisInterface,
    DeviceIsisProcess,
    DeviceSwitchport,
    DeviceSwitchportTaggedVlan,
    DeviceVlan,
)
from tests.conftest import seed_device, session

CASES_PATH = Path(__file__).parents[1] / "fixtures" / "exported_mirror_cases.json"
CASES = json.loads(CASES_PATH.read_text())


async def materialize(db, device, family, payload):
    if family == "isis":
        from nso_adapter.core import isis

        await isis._upsert_isis_data(db, device, payload.get("process", []), payload.get("interface", []), "poll")
    else:
        from nso_adapter.core import vlan

        function = "_upsert_vlans" if family == "vlan" else "_upsert_switchports"
        await getattr(vlan, function)(db, device, payload, "poll")
    await db.flush()


async def mirror_rows(db, device, family):
    if family == "switchport":
        vlans = {
            row.id: row.vlan_id
            for row in (await db.scalars(select(DeviceVlan).where(DeviceVlan.device_id == device.id))).all()
        }
        rows = []
        for row in (await db.scalars(select(DeviceSwitchport).where(DeviceSwitchport.device_id == device.id))).all():
            tagged = (
                await db.scalars(
                    select(DeviceSwitchportTaggedVlan.vlan_id).where(DeviceSwitchportTaggedVlan.switchport_id == row.id)
                )
            ).all()
            rows.append(
                {
                    "interface_name": row.interface_name,
                    "mode": row.mode,
                    "untagged_vlan": vlans.get(row.untagged_vlan_id),
                    "tagged_vlans": sorted(vlans[value] for value in tagged),
                }
            )
        return sorted(rows, key=lambda row: row["interface_name"])
    from nso_adapter.store.models import DeviceSubinterface, DeviceSvi

    models = {
        "vlan": (DeviceVlan,),
        "isis": (DeviceIsisProcess, DeviceIsisInterface),
        "svi": (DeviceSvi,),
        "subinterface": (DeviceSubinterface,),
    }[family]
    result = {}
    for model in models:
        rows = (await db.scalars(select(model).where(model.device_id == device.id))).all()
        result[model.__tablename__] = sorted(
            [
                {
                    column.name: getattr(row, column.name)
                    for column in model.__table__.columns
                    if column.name not in {"id", "device_id", "last_refreshed_at"}
                }
                for row in rows
            ],
            key=lambda row: json.dumps(row, sort_keys=True),
        )
    return result


@pytest.mark.anyio
@pytest.mark.parametrize("case", [case for case in CASES if case["family"] in OBSERVERS], ids=lambda case: case["name"])
async def test_exporter_reads_preserve_develop_mirrors(adapter_client, case):
    device_id = await seed_device(nso_device_name="export-fixture", netbox_device_id=1790)
    async with session() as db:
        device = await db.get(Device, device_id)
        for vid in (10, 20, 30, 99, 100, 200):
            db.add(DeviceVlan(device_id=device_id, vlan_id=vid))
        await db.flush()
        await materialize(db, device, case["family"], deepcopy(case["payload"]))
        assert await mirror_rows(db, device, case["family"]) == case["mirror"]
    observed = observe_family(case["family"], case["payload"])
    assert observed is not None
    assert observed.document.unprojectable == []


@pytest.mark.anyio
@pytest.mark.parametrize("family", ["vlan", "switchport", "svi", "subinterface"])
async def test_unknown_fields_are_observation_gaps_only(adapter_client, family):
    from nso_adapter.core.subinterface import _upsert_subinterface
    from nso_adapter.core.svi import _upsert_svi
    from tests.fixtures.switching_read_payloads import SWITCHING_READ_PAYLOADS

    payload = deepcopy(SWITCHING_READ_PAYLOADS[family])
    key = "vlan" if family == "vlan" else "interface"
    device_id = await seed_device(nso_device_name="coverage-fixture", netbox_device_id=1790)
    async with session() as db:
        device = await db.get(Device, device_id)

        async def write():
            if family == "svi":
                await _upsert_svi(db, device, payload[key], "poll")
            elif family == "subinterface":
                await _upsert_subinterface(db, device, payload[key], "poll")
            else:
                await materialize(db, device, family, payload)
            await db.flush()

        await write()
        before = await mirror_rows(db, device, family)
        for row in payload[key]:
            row["vendor-extension"] = {"opaque": "placeholder"}
        await write()
        assert await mirror_rows(db, device, family) == before
    observed = observe_family(family, payload)
    assert observed is not None
    assert len(observed.document.unprojectable) == len(payload[key])
    assert all("vendor-extension" in item.reason for item in observed.document.unprojectable)
    assert "placeholder" not in observed.document.model_dump_json()


@pytest.mark.parametrize(
    ("family", "key", "field", "value"),
    [
        ("vlan", "vlan", "source", "vlan-database"),
        ("switchport", "interface", "source", "switchport"),
        ("svi", "interface", "source", "svi"),
        ("subinterface", "interface", "source", "subinterface"),
        ("lag", "lag", "vpc-sensitive", True),
    ],
)
def test_exporter_fields_outside_observation_are_excluded(family, key, field, value):
    from tests.fixtures.switching_read_payloads import SWITCHING_READ_PAYLOADS

    payload = deepcopy(SWITCHING_READ_PAYLOADS[family])
    for row in payload[key]:
        row[field] = value
    observed = observe_family(family, payload)
    assert observed is not None
    assert observed.document.unprojectable == []


def test_unknown_values_are_available_to_mirrors_only():
    from nso_adapter.domain.read_projection import entry_payload
    from nso_adapter.domain.switching_observation import project_switchports

    payload = {"interface": [{"interface-name": "Gi0/1", "vendor-extension": {"value": "placeholder-unknown-value"}}]}
    projected = project_switchports(payload)
    assert entry_payload(projected.interfaces[0])["vendor-extension"] == payload["interface"][0]["vendor-extension"]
    observed = observe_family("switchport", payload)
    assert "placeholder-unknown-value" not in observed.document.model_dump_json()


@pytest.mark.anyio
async def test_unknown_json_fields_preserve_isis_mirror(adapter_client):
    from nso_adapter.core.isis import _upsert_isis_data

    segment_routing = {"enabled": True, "srgb": {"lower-bound": 16000, "secret": "placeholder-unknown-value"}}
    payload = {"process": [{"process-tag": "CORE", "segment-routing": segment_routing}]}
    device_id = await seed_device(nso_device_name="unknown-field-fixture")
    async with session() as db:
        device = await db.get(Device, device_id)
        await _upsert_isis_data(db, device, payload["process"], [], "poll")
        await db.flush()
        row = await db.scalar(select(DeviceIsisProcess).where(DeviceIsisProcess.device_id == device_id))
        assert row.segment_routing == segment_routing
    observed = observe_family("isis", payload)
    assert observed.document.unprojectable[0].reason == "process[0].segment_routing[0]: unsupported fields: srgb"
    assert "placeholder-unknown-value" not in observed.document.model_dump_json()
