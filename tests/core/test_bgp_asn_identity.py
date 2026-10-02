# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""BGP identity checks compare AS numbers independently of their notation."""

from __future__ import annotations

import pytest
from sqlalchemy import select

from nso_adapter.core.apply import _reader_compare_expected, _reader_compare_walk
from nso_adapter.core.removal import _document_orphans, _residue_after_removal, section_guard_lists
from nso_adapter.domain.asn import AsnRuleViolation
from nso_adapter.store.models import BgpRouterIntent, Device
from tests._secret_discipline import assert_text_free_of
from tests.conftest import AUTH, seed_device, session


@pytest.mark.parametrize(
    ("intent_asn", "view_asn", "verdict"),
    [
        ("4200000000", "64086.59904", "ok"),
        ("64086.59904", "4200000000", "ok"),
        ("4200000000", 4200000000, "ok"),
        ("4200000000", "64086.59905", "missing"),
        ("4200000000", None, "missing"),
    ],
)
def test_reader_compare_bgp_as_number_identity(intent_asn, view_asn, verdict):
    row = BgpRouterIntent(id=1, device_id=1, asn=intent_asn, scopes=[])
    expected = _reader_compare_expected("bgp", [row], "juniper-junos-nc")
    section = {"router": [{"asn": view_asn}] if view_asn is not None else []}
    ok, failed, errors, status, evidence = _reader_compare_walk(
        "bgp",
        expected,
        [],
        section,
        section_guard_lists("bgp"),
        1,
        job_id=1,
        device_id=1,
    )
    assert status == verdict
    assert evidence == {1: "present" if verdict == "ok" else "missing"}
    assert (ok, failed) == ((1, 0) if verdict == "ok" else (0, 1))
    if verdict == "missing":
        assert row.last_apply_error["code"] == "reader_compare_missing"
        assert errors
    else:
        assert row.last_apply_error is None
        assert errors == []


class BgpDeviceView:
    """Return a device-state action result at the external NSO boundary."""

    def __init__(self, asn):
        self.asn = asn

    async def run_device_state_read(self, device_name, families, *, timeout):
        assert families == ["bgp-config"]
        return {"bgp-config": {"status": "ok", "router": [{"asn": self.asn}]}}


@pytest.mark.parametrize(
    ("removed_asn", "view_asn"),
    [("4200000000", "64086.59904"), ("64086.59904", "4200000000"), ("4200000000", 4200000000)],
)
async def test_removal_reports_bgp_as_number_survivor(removed_asn, view_asn):
    residue, unverifiable = await _residue_after_removal(
        BgpDeviceView(view_asn),
        Device(id=1, nso_device_name="placeholder-device"),
        "bgp",
        {"removed": {"router": [removed_asn]}},
    )
    assert residue == {"router": [["4200000000"]]}
    assert unverifiable == []


async def test_removal_reports_clean_for_a_different_as_number():
    residue, unverifiable = await _residue_after_removal(
        BgpDeviceView("64086.59905"),
        Device(id=1, nso_device_name="placeholder-device"),
        "bgp",
        {"removed": {"router": ["4200000000"]}},
    )
    assert residue == {}
    assert unverifiable == []


def test_removal_authority_matches_bgp_as_number_identity():
    live = {"bgp": {"router": [{"asn": "64086.59904"}]}}
    body = {"bgp": {"router": [{"asn": 4200000000}]}}
    assert _document_orphans(live, body, {}) == {}
    assert _document_orphans(live, {}, {"bgp": {"router": ["4200000000"]}}) == {}


@pytest.mark.parametrize("side", ["intent", "view"])
@pytest.mark.parametrize("invalid_asn", ["invalid", "", "-1", "4294967296", "65536.0", "0.65536", "1.-1"])
def test_reader_compare_rejects_invalid_as_number(side, invalid_asn):
    row = BgpRouterIntent(id=1, device_id=1, asn=invalid_asn if side == "intent" else "64512", scopes=[])
    section = {"router": [{"asn": invalid_asn if side == "view" else "64512"}]}
    with pytest.raises(AsnRuleViolation, match="violates RFC 5396"):
        expected = _reader_compare_expected("bgp", [row], "juniper-junos-nc")
        _reader_compare_walk("bgp", expected, [], section, section_guard_lists("bgp"), 1, job_id=1, device_id=1)


@pytest.mark.parametrize("side", ["removed", "view"])
async def test_removal_rejects_invalid_as_number(side):
    with pytest.raises(AsnRuleViolation, match="violates RFC 5396"):
        await _residue_after_removal(
            BgpDeviceView("invalid" if side == "view" else "64512"),
            Device(id=1, nso_device_name="placeholder-device"),
            "bgp",
            {"removed": {"router": ["invalid" if side == "removed" else "64512"]}},
        )


async def test_bgp_mirror_preserves_device_as_notation(adapter_client):
    from nso_adapter.core.bgp import _upsert_bgp_data
    from nso_adapter.store.models import DeviceBgpRouter

    device_id = await seed_device(nso_device_name="placeholder-device", netbox_device_id=1)
    routers = [
        {
            "asn": "64086.59904",
            "scope": [
                {
                    "vrf": "",
                    "peer": [{"peer-address": "198.18.0.1", "remote-as": "64086.59904", "local-as": "64086.59905"}],
                    "peer-group": [{"name": "placeholder-group", "remote-as": "64086.59904"}],
                }
            ],
        }
    ]
    async with session() as db:
        device = await db.get(Device, device_id)
        await _upsert_bgp_data(db, device, routers, "test")
        await db.commit()
        stored = (await db.execute(select(DeviceBgpRouter))).scalars().one()
        assert stored.asn == "64086.59904"

    response = await adapter_client.get(f"/api/v1/devices/{device_id}/bgp-config", headers=AUTH)
    assert response.status_code == 200
    router = response.json()["routers"][0]
    assert router["asn"] == "64086.59904"
    scope = router["scopes"][0]
    assert scope["peers"][0]["remote_as"] == "64086.59904"
    assert scope["peers"][0]["local_as"] == "64086.59905"
    assert scope["peer_groups"][0]["remote_as"] == "64086.59904"


async def test_bgp_mirror_refresh_preserves_redistribution_key_and_compares_asplain_intent(adapter_client):
    from nso_adapter.core.bgp import _upsert_bgp_data
    from nso_adapter.core.redistribution import refresh_redistribution_from_outcomes
    from nso_adapter.nso.read_outcome import Freshness, Present
    from nso_adapter.store.models import DeviceRedistribution

    device_id = await seed_device(nso_device_name="placeholder-device", netbox_device_id=1)
    bgp = {
        "router": [
            {
                "asn": "64086.59904",
                "scope": [
                    {
                        "vrf": "",
                        "address-family": [
                            {
                                "afi": "ipv4-unicast",
                                "redistribute": [{"source-protocol": "connected", "source-ref": ""}],
                            }
                        ],
                    }
                ],
            }
        ]
    }
    async with session() as db:
        device = await db.get(Device, device_id)
        await refresh_redistribution_from_outcomes(
            db,
            device,
            {
                "ospf": Present({}, Freshness.fresh),
                "isis": Present({}, Freshness.fresh),
                "bgp": Present(bgp, Freshness.fresh),
            },
        )
        stored = (await db.execute(select(DeviceRedistribution))).scalars().one()
        assert stored.dest_ref == "64086.59904//ipv4-unicast"
        await _upsert_bgp_data(db, device, bgp["router"], "test")
        await db.commit()

    response = await adapter_client.get(f"/api/v1/devices/{device_id}/redistribution", headers=AUTH)
    assert response.status_code == 200
    assert response.json()["entries"][0]["dest_ref"] == "64086.59904//ipv4-unicast"
    intent = BgpRouterIntent(id=1, device_id=device_id, asn="4200000000", scopes=[])
    expected = _reader_compare_expected("bgp", [intent], "juniper-junos-nc")
    ok, failed, errors, status, evidence = _reader_compare_walk(
        "bgp",
        expected,
        [],
        bgp,
        section_guard_lists("bgp"),
        1,
        job_id=1,
        device_id=device_id,
    )
    assert (ok, failed, errors, status, evidence) == (1, 0, [], "ok", {1: "present"})


@pytest.mark.parametrize("invalid", ["064520", " 64512", "64512 ", "01.2", "1.02"])
def test_shared_asn_parser_refuses_noncanonical_digits(invalid):
    from nso_adapter.domain.asn import parse_asn

    with pytest.raises(ValueError, match="ASN"):
        parse_asn(invalid)


@pytest.mark.parametrize("field", ["asn", "remote-as", "local-as", "group-remote-as", "duplicate-remote-as"])
async def test_bgp_read_refuses_malformed_as_number(adapter_client, field):
    from nso_adapter.core.bgp import _upsert_bgp_data

    device_id = await seed_device(nso_device_name="placeholder-device")
    scope = {"peer": [{"peer-address": "198.18.0.1"}], "peer-group": [{"name": "placeholder-group"}]}
    router = {"asn": "64512", "scope": [scope]}
    if field == "asn":
        router["asn"] = "064520"
    elif field == "group-remote-as":
        scope["peer-group"][0]["remote-as"] = "064520"
    elif field == "duplicate-remote-as":
        scope["peer"].append({"peer-address": "198.18.0.1", "remote-as": "064520"})
    else:
        scope["peer"][0][field] = "064520"
    async with session() as db:
        device = await db.get(Device, device_id)
        with pytest.raises(AsnRuleViolation, match="violates RFC 5396"):
            await _upsert_bgp_data(db, device, [router], "test")


async def test_redistribution_read_refuses_malformed_as_number(adapter_client):
    from nso_adapter.core.redistribution import refresh_redistribution_from_outcomes
    from nso_adapter.nso.read_outcome import Freshness, Present
    from nso_adapter.store.models import DeviceRedistribution

    device_id = await seed_device(nso_device_name="placeholder-device")
    async with session() as db:
        device = await db.get(Device, device_id)
        with pytest.raises(AsnRuleViolation, match="violates RFC 5396"):
            await refresh_redistribution_from_outcomes(
                db,
                device,
                {
                    "ospf": Present({}, Freshness.fresh),
                    "isis": Present({}, Freshness.fresh),
                    "bgp": Present(
                        {
                            "router": [
                                {
                                    "asn": "064520",
                                    "scope": [
                                        {
                                            "address-family": [
                                                {
                                                    "afi": "ipv4-unicast",
                                                    "redistribute": [{"source-protocol": "connected"}],
                                                }
                                            ]
                                        }
                                    ],
                                }
                            ]
                        },
                        Freshness.fresh,
                    ),
                },
            )
        assert list((await db.execute(select(DeviceRedistribution))).scalars()) == []


@pytest.mark.parametrize("field", ["remote-as", "local-as"])
def test_bgp_verifiers_refuse_malformed_peer_as_numbers(field):
    row = BgpRouterIntent(id=1, device_id=1, asn="64512", scopes=[])
    expected = _reader_compare_expected("bgp", [row], "juniper-junos-nc")
    bgp = {"router": [{"asn": "64512", "scope": [{"peer": [{"peer-address": "198.18.0.1", field: "064520"}]}]}]}
    with pytest.raises(AsnRuleViolation, match="violates RFC 5396"):
        _reader_compare_walk("bgp", expected, [], bgp, section_guard_lists("bgp"), 1, job_id=1, device_id=1)
    with pytest.raises(AsnRuleViolation, match="violates RFC 5396"):
        _document_orphans({"bgp": bgp}, {"bgp": bgp}, {})


@pytest.mark.parametrize("invalid", [1.0, 64512.0, True, False, None])
def test_shared_asn_parser_refuses_noninteger_device_scalars(invalid):
    from nso_adapter.domain.asn import parse_asn

    with pytest.raises(ValueError, match="ASN"):
        parse_asn(invalid)


@pytest.mark.parametrize(
    ("spelling", "number"),
    [
        ("0", 0),
        ("0.100", 100),
        ("65535", 65535),
        ("1.0", 65536),
        ("4294967295", 4294967295),
        ("65535.65535", 4294967295),
        ("65535.0", 4294901760),
    ],
)
def test_shared_asn_parser_accepts_rfc_boundaries(spelling, number):
    from nso_adapter.domain.asn import parse_asn

    assert parse_asn(spelling) == number


@pytest.mark.parametrize("protocol", ["ospf", "isis", "bgp"])
def test_redistribution_read_refuses_malformed_bgp_source_as(protocol):
    from datetime import UTC, datetime

    from nso_adapter.core.redistribution import _build_rows

    with pytest.raises(AsnRuleViolation, match="violates RFC 5396"):
        _build_rows(
            1,
            protocol,
            "placeholder-process",
            [{"source-protocol": "bgp", "source-ref": " 64512"}],
            datetime.now(UTC),
            "test",
            location="instance[0]",
        )


@pytest.mark.parametrize("invalid", [1.0, True])
def test_redistribution_read_refuses_noninteger_source_scalars(invalid):
    from datetime import UTC, datetime

    from nso_adapter.core.redistribution import _build_rows

    with pytest.raises(AsnRuleViolation, match="violates RFC 5396"):
        _build_rows(
            1,
            "ospf",
            "placeholder-process",
            [{"source-protocol": "bgp", "source-ref": invalid}],
            datetime.now(UTC),
            "test",
            location="instance[0]",
        )


async def test_bgp_refresh_records_error_and_retains_valid_mirror(adapter_client):
    from nso_adapter.core.bgp import _upsert_bgp_data, refresh_bgp_config_for_device
    from tests.core.test_bgp_refresh import _FakeNso

    device_id = await seed_device(nso_device_name="placeholder-device")
    async with session() as db:
        device = await db.get(Device, device_id)
        await _upsert_bgp_data(db, device, [{"asn": "64512"}], "test")
        await db.commit()
        with pytest.raises(AsnRuleViolation, match="violates RFC 5396"):
            await refresh_bgp_config_for_device(db, device, _FakeNso(entry={"router": [{"asn": "064520"}]}))
    response = await adapter_client.get(f"/api/v1/devices/{device_id}/bgp-config", headers=AUTH)
    assert_text_free_of(response.text, ["placeholder-device"])
    assert response.status_code == 200, response.text
    assert response.json()["routers"][0]["asn"] == "64512"
    assert response.json()["read_state"]["result"] == "error"
    assert response.json()["read_state"]["succeeded"] is False


def test_bgp_verifiers_refuse_malformed_redistribution_source_as():
    row = BgpRouterIntent(id=1, device_id=1, asn="64512", scopes=[])
    expected = _reader_compare_expected("bgp", [row], "juniper-junos-nc")
    bgp = {
        "router": [
            {
                "asn": "64512",
                "scope": [
                    {
                        "address-family": [
                            {
                                "afi": "ipv4-unicast",
                                "redistribute": [{"source-protocol": "bgp", "source-ref": "064520"}],
                            }
                        ]
                    }
                ],
            }
        ]
    }
    with pytest.raises(AsnRuleViolation, match="violates RFC 5396"):
        _reader_compare_walk("bgp", expected, [], bgp, section_guard_lists("bgp"), 1, job_id=1, device_id=1)
    with pytest.raises(AsnRuleViolation, match="violates RFC 5396"):
        _document_orphans({"bgp": bgp}, {"bgp": bgp}, {})


@pytest.mark.parametrize("protocol", ["ospf", "isis"])
def test_igp_verifiers_refuse_malformed_bgp_redistribution_source_as(protocol):
    from nso_adapter.store.models import IsisProcessIntent, OspfInstanceIntent

    if protocol == "ospf":
        row = OspfInstanceIntent(id=1, device_id=1, process_id="placeholder-process")
        collection, key = "instance", "process-id"
    else:
        row = IsisProcessIntent(id=1, device_id=1, process_tag="placeholder-process")
        collection, key = "process", "process-tag"
    expected = _reader_compare_expected(protocol, [row], "juniper-junos-nc")
    parent = {key: "placeholder-process", "redistribute": [{"source-protocol": "bgp", "source-ref": "064512"}]}
    section = {collection: [parent]}
    with pytest.raises(AsnRuleViolation, match="violates RFC 5396"):
        _reader_compare_walk(protocol, expected, [], section, section_guard_lists(protocol), 1, job_id=1, device_id=1)
    body = {protocol: {"process-config": [parent]}}
    with pytest.raises(AsnRuleViolation, match="violates RFC 5396"):
        _document_orphans(body, body, {})


@pytest.mark.parametrize("dest_ref", ["64512/", "64512/placeholder-vrf", "64512//ipv4-unicast"])
def test_device_redistribution_accepts_builder_destination_shapes(dest_ref):
    from nso_adapter.domain.asn import asn_row_identity

    row = {"dest_protocol": "bgp", "dest_ref": dest_ref, "source_protocol": "connected", "source_ref": ""}
    assert asn_row_identity("device_redistribution", row) == ("bgp", (64512, *dest_ref.split("/")[1:]), "connected", "")


@pytest.mark.parametrize(
    "table,dest_ref",
    [
        ("device_redistribution", "64512"),
        ("redistribution_intent", "64512:vrf"),
    ],
)
def test_redistribution_refuses_unknown_destination_shapes(table, dest_ref):
    from nso_adapter.domain.asn import asn_row_identity

    with pytest.raises(AsnRuleViolation) as caught:
        asn_row_identity(table, {"dest_protocol": "bgp", "dest_ref": dest_ref, "source_protocol": "connected"})
    assert caught.value.error["detail"]["field"] == "dest_ref"


async def test_redistribution_refresh_accepts_an_afi_less_bgp_device_family(adapter_client):
    from nso_adapter.core.redistribution import refresh_redistribution_from_outcomes
    from nso_adapter.nso.read_outcome import Freshness, Present
    from nso_adapter.store.models import DeviceRedistribution

    device_id = await seed_device(nso_device_name="placeholder-device")
    bgp = {
        "router": [
            {
                "asn": "64512",
                "scope": [
                    {"vrf": "placeholder-vrf", "address-family": [{"redistribute": [{"source-protocol": "connected"}]}]}
                ],
            }
        ]
    }
    outcomes = {
        protocol: Present(bgp if protocol == "bgp" else {}, Freshness.fresh) for protocol in ["bgp", "ospf", "isis"]
    }
    async with session() as db:
        device = await db.get(Device, device_id)
        assert await refresh_redistribution_from_outcomes(db, device, outcomes)
        row = await db.scalar(select(DeviceRedistribution).where(DeviceRedistribution.device_id == device_id))
        assert row.dest_ref == "64512/placeholder-vrf"
    response = await adapter_client.get(f"/api/v1/devices/{device_id}/redistribution", headers=AUTH)
    assert response.status_code == 200
    assert response.json()["entries"][0]["dest_ref"] == "64512/placeholder-vrf"
