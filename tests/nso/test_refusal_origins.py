# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Encoder and resolver contracts over real stored intent rows."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from nso_adapter.core.community_dialect import community_dialect_for
from nso_adapter.core.projection import InterfaceExecution, section_registry
from nso_adapter.nso.apply import SectionExecution
from nso_adapter.store.models import (
    BgpPeerIntent,
    BgpRouterIntent,
    BgpScopeIntent,
    DbInterface,
    Device,
    LagBundleIntent,
    SwitchportIntent,
)
from tests.nso.device_intent_rows import ELIGIBLE_ATTRIBUTES, NOKIA_NED, interfaces, section_rows
from tests.nso.test_device_intent_encoders import _execution, _rows


@pytest.fixture
def stored_sections(pg_sync_session):
    db = pg_sync_session
    db.add(Device(id=1, nso_instance="test", nso_device_name="lab-device"))
    db.flush()
    db.add_all(interfaces().values())
    db.flush()
    for model_rows in section_rows().values():
        for rows in model_rows.values():
            for row in rows:
                if row.__tablename__ == "route_policy_object_intent" and row.family == "community_list":
                    row.entries = [{**entry, "sequence": (i + 1) * 10} for i, entry in enumerate(row.entries)]
                if row.__tablename__ == "isis_interface_intent":
                    row.af = row.af.removesuffix("-unicast")
            db.add_all(rows)
    db.commit()
    db.expire_all()
    sections = {}
    for section, entry in section_registry().items():
        tables = {}
        for spec in entry.tables:
            query = select(spec.model).order_by(spec.model.id)
            if spec.model is BgpRouterIntent:
                query = query.options(
                    selectinload(BgpRouterIntent.scopes).selectinload(BgpScopeIntent.address_families),
                    selectinload(BgpRouterIntent.scopes)
                    .selectinload(BgpScopeIntent.peers)
                    .selectinload(BgpPeerIntent.peer_address_families),
                )
            elif spec.model is LagBundleIntent:
                query = query.options(selectinload(LagBundleIntent.members))
            elif spec.model is SwitchportIntent:
                query = query.options(selectinload(SwitchportIntent.tagged_vlans))
            if spec.discriminator:
                column, value = spec.discriminator
                query = query.where(getattr(spec.model, column) == value)
            tables[spec.model.__tablename__] = list(db.scalars(query))
        proof = None
        ned_id = NOKIA_NED if section == "route_policy" else None
        if section == "interface_config":
            proof = InterfaceExecution({row.id: row for row in db.scalars(select(DbInterface))}, ELIGIBLE_ATTRIBUTES)
        sections[section] = (tables, SectionExecution(ned_id, community_dialect_for(ned_id), proof))
    return sections


def _encode(stored_sections, section):
    rows, execution = stored_sections[section]
    return section_registry()[section].encode(rows, execution)


def test_encoder_body_is_unchanged_with_origins():
    # These strings were captured from the pre-change encoders at 92e8241.
    snapshots = json.loads((Path(__file__).parent / "device_intent_body_bytes.json").read_text())
    for section, expected in snapshots.items():
        encoded = section_registry()[section].encode(_rows(section), _execution(section))
        assert hasattr(encoded, "origins"), "section encoding must carry origins with its body"
        assert json.dumps(encoded.body, separators=(",", ":"), ensure_ascii=True).encode("ascii") == expected.encode(
            "ascii"
        )


def _origins(stored_sections, section):
    encoded = _encode(stored_sections, section)
    assert encoded.origins.bindings, "emitted rows need exact source bindings"
    return encoded.origins


def _key(subject, path, field=None):
    from nso_adapter.nso.origins import OriginKey
    from nso_adapter.nso.refusal_protocol import SUBJECTS, row_ref

    family = SUBJECTS[subject].family
    return OriginKey(family, row_ref(family, subject, path), field)


def _refusal(key):
    from nso_adapter.nso.refusal_protocol import refusal

    return refusal(key.family, key.row, "write-blocked", field=key.field)


def test_refusal_origins_cover_all_declared_subjects(stored_sections):
    from nso_adapter.nso.refusal_protocol import SUBJECTS

    bound, unsupported = set(), set()
    for section in stored_sections:
        origins = _origins(stored_sections, section)
        bound.update(key.row.subject for key in origins.bindings)
        unsupported.update(key.row.subject for key in origins.unsupported)
        for key in origins.bindings:
            for declared in SUBJECTS[key.row.subject].fields:
                assert (
                    type(key)(key.family, key.row, declared) in origins.bindings
                    or type(key)(key.family, key.row, declared) in origins.unsupported
                )
    assert bound | unsupported == set(SUBJECTS)
    assert len(SUBJECTS) == 60
    for section, omitted_table in (("interface_config", "interface_ip_intent"), ("isis", "isis_flex_algo_intent")):
        rows, execution = stored_sections[section]
        encoded = section_registry()[section].encode({**rows, omitted_table: []}, execution)
        bound.update(key.row.subject for key in encoded.origins.bindings)
    assert bound == set(SUBJECTS)


@pytest.mark.parametrize(
    "field,protocol", [("snmp.v3_user.auth_secret", "auth_protocol"), ("snmp.v3_user.priv_secret", "priv_protocol")]
)
def test_absent_declared_fields_bind_current_rows(stored_sections, field, protocol):
    rows, _ = stored_sections["snmp"]
    user = rows["snmp_v3_user_intent"][1]
    setattr(user, protocol, "sha" if protocol == "auth_protocol" else "aes")
    origins = _origins(stored_sections, "snmp")
    from dataclasses import asdict

    from nso_adapter.nso.origins import AttributedRefusal, resolve_refusals

    key = _key("snmp.v3-user", (("v3-user", (user.username,)),), field)
    binding = origins.bindings[key]
    assert binding.provenance == "current"
    assert binding.stream == "snmp"
    assert binding.sources[0].frozen_row_identity == (user.id,)
    result = resolve_refusals((_refusal(key),), origins)
    assert isinstance(result, tuple) and isinstance(result[0], AttributedRefusal)
    assert result[0].logical_rows == binding.logical_rows
    assert len(result[0].logical_rows) == 1
    assert result[0].logical_rows[0].row == key.row
    from tests._secret_discipline import assert_text_free_of

    assert_text_free_of(asdict(binding), [user.username, rows["snmp_v3_user_intent"][0].username])


def test_refusal_origins_partition_split_streams(stored_sections):
    origins = _origins(stored_sections, "interface_config")
    from nso_adapter.nso.origins import AttributionFailure, resolve_refusals

    name = "1/1/1:100"
    path = (("interface", (name,)),)
    assert (
        origins.bindings[_key("interface.description", path, "interface.attrs.description")].stream
        == "interface_config"
    )
    # A missing enabled leaf belongs to the same current attribute construct.
    assert origins.bindings[_key("interface.enabled", path, "interface.attrs.enabled")].stream == "interface_config"
    assert origins.bindings[_key("interface.vrf", path, "interface.ip.vrf")].stream == "ip"
    for field in (
        "interface.attrs.kind",
        "interface.attrs.service",
        "interface.attrs.parent_binding",
        "interface.attrs.encap_tag",
    ):
        assert origins.bindings[_key("interface.interface", path, field)].stream == "interface_config"
    parent = _key("interface.interface", path)
    assert parent in origins.unsupported
    assert isinstance(resolve_refusals((_refusal(parent),), origins), AttributionFailure)
    address = _key("interface.ipv6-address", (*path, ("ipv6-address", ("2001:db8::1",))))
    assert origins.bindings[address].stream == "ip"


def test_child_origins_keep_logical_and_carrier_identity(stored_sections):
    origins = _origins(stored_sections, "bgp")
    router_path = (("router", ("65100",)),)
    peer_path = (*router_path, ("scope", ("",)), ("peer", ("192.0.2.2",)))
    peer_key = _key("bgp.router.scope.peer", peer_path)
    peer = origins.bindings[peer_key].logical_rows[0]
    assert peer.row == peer_key.row
    assert peer.carrier.root_row == _key("bgp.router", router_path).row
    assert peer.source_refs[0].source_table == "bgp_peer_intent"
    assert peer.acceptance_identity == peer.carrier.accepted_at.isoformat()
    af_key = _key("bgp.router.scope.peer.peer-address-family", (*peer_path, ("peer-address-family", ("ipv4-unicast",))))
    assert origins.bindings[af_key].logical_rows[0].row == peer.row
    lag = _origins(stored_sections, "lag")
    member = lag.bindings[
        _key("lag.bundle.member", (("bundle", ("Port-channel1",)), ("member", ("GigabitEthernet0/1",))))
    ].logical_rows[0]
    assert member.row.subject == "lag.bundle.member"
    assert member.carrier.root_row.subject == "lag.bundle"
    assert member.source_refs[0].source_table == "lag_member_intent"
    assert member.carrier.source_table == "lag_bundle_intent"
    redist = next(
        binding
        for key, binding in origins.bindings.items()
        if key.row.subject.endswith("redistribute") and key.field is None
    )
    assert redist.logical_rows[0].carrier.source_table == "redistribution_intent"
    assert redist.logical_rows[0].row == redist.logical_rows[0].carrier.root_row


def test_overlay_mapping_matches_plugin_settlement_grain(stored_sections):
    _origins(stored_sections, "bgp")
    from nso_adapter.nso.origins import OVERLAY_GRAIN

    assert OVERLAY_GRAIN == {
        "bgp.router.scope.peer": ("NSOBGPPeerState", "bgp.router.scope.peer", "bgp.router"),
        "bgp.router.scope.peer.peer-address-family": ("NSOBGPPeerState", "bgp.router.scope.peer", "bgp.router"),
        "lag.bundle.member": ("NSOLACPMemberState", "lag.bundle.member", "lag.bundle"),
        "switchport.interface": ("NSOSwitchportState", "switchport.interface", "switchport.interface"),
        "route-policy.prefix-list.entry": (
            "NSORoutePolicyState",
            "route-policy.prefix-list",
            "route-policy.prefix-list",
        ),
        "route-policy.community-list.entry": (
            "NSORoutePolicyState",
            "route-policy.community-list",
            "route-policy.community-list",
        ),
        "route-policy.as-path.entry": ("NSORoutePolicyState", "route-policy.as-path", "route-policy.as-path"),
        "route-policy.route-map.entry": ("NSORoutePolicyState", "route-policy.route-map", "route-policy.route-map"),
        "bgp.router.scope.address-family.redistribute": (
            "NSORedistributionState",
            "bgp.router.scope.address-family.redistribute",
            "bgp.router.scope.address-family.redistribute",
        ),
        "isis.process-config.redistribute": (
            "NSORedistributionState",
            "isis.process-config.redistribute",
            "isis.process-config.redistribute",
        ),
        "ospf.process-config.redistribute": (
            "NSORedistributionState",
            "ospf.process-config.redistribute",
            "ospf.process-config.redistribute",
        ),
    }
    switchport = _origins(stored_sections, "switchport")
    key = _key("switchport.interface", (("interface", ("GigabitEthernet0/3",)),), "switchport.tagged_vlans")
    assert len(switchport.bindings[key].logical_rows) == 1
    assert len(switchport.bindings[key].sources) == 3
    policies = _origins(stored_sections, "route_policy")
    key = _key("route-policy.prefix-list.entry", (("prefix-list", ("PL-OUT",)), ("entry", (10,))))
    assert policies.bindings[key].logical_rows[0].row.subject == "route-policy.prefix-list"


@pytest.mark.parametrize(
    "section,table",
    [("vlan", "vlan_intent"), ("snmp", "snmp_v3_user_intent"), ("interface_config", "interface_ip_intent")],
)
def test_duplicate_origin_fails_before_send(stored_sections, section, table):
    rows, _ = stored_sections[section]
    rows[table].append(rows[table][0])
    with pytest.raises(ValueError, match="duplicate origin"):
        _encode(stored_sections, section)


def test_synthetic_parent_has_exact_stream_or_is_unsupported(stored_sections):
    rows, _ = stored_sections["isis"]
    origins = _origins(stored_sections, "isis")
    assert origins.bindings[_key("isis.process-config", (("process-config", ("ORPHAN",)),))].stream == "isis_flex_algo"
    rows["isis_flex_algo_intent"][0].process_tag = "CORE"
    origins = _origins(stored_sections, "isis")
    parent = _key("isis.process-config", (("process-config", ("CORE",)),))
    assert parent in origins.unsupported
    assert origins.bindings[type(parent)(parent.family, parent.row, "isis.proc.net")].stream == "isis"
    assert (
        origins.bindings[
            _key(
                "isis.process-config.flex-algo",
                (("process-config", ("CORE",)), ("flex-algo", (128,))),
                "isis.flex.priority",
            )
        ].stream
        == "isis_flex_algo"
    )
    ospf = _origins(stored_sections, "ospf")
    binding = ospf.bindings[_key("ospf.process-config", (("process-config", ("9",)),), "ospf.proc.router_id")]
    assert binding.stream == "ospf"
    assert binding.sources[0].source_table == "redistribution_intent"


def test_invalid_logical_source_fails_complete_attribution(stored_sections):
    from dataclasses import replace

    from nso_adapter.nso.origins import AttributionFailure, resolve_refusals

    origins = _origins(stored_sections, "bgp")
    key = _key("bgp.router.scope.peer", (("router", ("65100",)), ("scope", ("",)), ("peer", ("192.0.2.2",))))
    binding = origins.bindings[key]
    logical = binding.logical_rows[0]
    invalid = replace(logical.source_refs[0], frozen_row_identity=())
    origins.bindings[key] = replace(binding, logical_rows=(replace(logical, source_refs=(invalid,)),))
    assert isinstance(resolve_refusals((_refusal(key),), origins), AttributionFailure)


def test_refusal_attribution_is_atomic_and_related_is_context(stored_sections):
    from nso_adapter.nso.origins import AttributionFailure, OriginIndex, resolve_refusals
    from nso_adapter.nso.refusal_protocol import RelatedSite, RowRef, refusal

    origins = OriginIndex()
    origins.merge(_origins(stored_sections, "snmp"))
    origins.merge(_origins(stored_sections, "vlan"))
    snmp_key = _key("snmp.v3-user", (("v3-user", ("viewer",)),), "snmp.v3_user.auth_secret")
    vlan_key = _key("vlan.vlan", (("vlan", (10,)),))
    explanatory = RelatedSite("vlan", RowRef("vlan.vlan", "f" * 64), "vlan.name")
    named = refusal(snmp_key.family, snmp_key.row, "write-blocked", field=snmp_key.field, related=explanatory)
    assert isinstance(resolve_refusals((named,), origins), tuple)
    unknown = refusal("vlan", RowRef("vlan.vlan", "e" * 64), "write-blocked")
    result = resolve_refusals((named, unknown), origins)
    assert isinstance(result, AttributionFailure)
    assert result.reason == "missing_binding"
    assert result.key.row == unknown.row
    valid = resolve_refusals((named, _refusal(vlan_key)), origins)
    assert isinstance(valid, tuple) and len(valid) == 2


def test_retention_and_omission_require_history(stored_sections):
    from nso_adapter.core.apply import overlay_retained_routes
    from nso_adapter.nso.origins import AttributionFailure, PriorContribution, RemovalAuthority, resolve_refusals

    encoded = _encode(stored_sections, "static_route")
    retained = {"vrf": "", "prefix": "198.18.9.0/24", "next-hop": "198.18.1.9", "metric": 77, "vendor-leaf": "preserve"}
    overlay_retained_routes(encoded, [retained])
    assert encoded.body["route"][-1] == retained
    key = _key("static-route.route", (("route", ("", "198.18.9.0/24", "198.18.1.9")),))
    result = resolve_refusals((_refusal(key),), encoded.origins)
    assert isinstance(result, AttributionFailure) and result.reason == "missing_baseline_provenance"
    assert len(encoded.origins.retained) == 1
    omitted = _key("static-route.route", (("route", ("", "198.18.10.0/24", "198.18.1.10")),))
    authority = RemovalAuthority(17, "static_route", 7, "removal", omitted.row)
    encoded.origins.record_omission(PriorContribution("static-route", omitted.row, authority))
    result = resolve_refusals((_refusal(omitted),), encoded.origins)
    assert isinstance(result, AttributionFailure) and result.reason == "missing_removal_provenance"
    # A retained collision loses to the current row and adds no historical origin.
    before = len(encoded.origins.retained)
    overlay_retained_routes(encoded, [{**encoded.body["route"][0], "metric": 999}])
    assert len(encoded.origins.retained) == before
    assert encoded.body["route"][0]["metric"] == 10
    family = _key("static-route", ())
    assert isinstance(resolve_refusals((_refusal(family),), encoded.origins), AttributionFailure)


def test_conflicting_vrf_producers_fail_before_send(stored_sections):
    rows, _ = stored_sections["interface_config"]
    rows["interface_ip_intent"][1].vrf = "OTHER"
    with pytest.raises(ValueError, match="competing field producers"):
        _encode(stored_sections, "interface_config")


@pytest.mark.parametrize("vrfs", [("", "RED"), ("RED", "")])
def test_global_and_named_vrf_producers_fail_before_send(stored_sections, vrfs):
    rows, _ = stored_sections["interface_config"]
    for row, vrf in zip(rows["interface_ip_intent"], vrfs, strict=True):
        row.vrf = vrf
    with pytest.raises(ValueError, match="competing field producers"):
        _encode(stored_sections, "interface_config")


def test_empty_families_have_no_row_attribution(stored_sections):
    from nso_adapter.nso.origins import AttributionFailure, resolve_refusals

    rows, execution = stored_sections["vlan"]
    encoded = section_registry()["vlan"].encode({table: [] for table in rows}, execution)
    assert encoded.origins.bindings == {}
    assert isinstance(resolve_refusals((_refusal(_key("vlan", ())),), encoded.origins), AttributionFailure)


def test_synthetic_mixed_parent_absent_fields_are_unsupported(stored_sections):
    rows, _ = stored_sections["isis"]
    rows["isis_process_intent"] = []
    rows["isis_flex_algo_intent"][0].process_tag = "CORE"
    origins = _origins(stored_sections, "isis")
    for field in (None, "isis.proc.net", "isis.proc.area_auth_key", "isis.proc.vrf"):
        assert _key("isis.process-config", (("process-config", ("CORE",)),), field) in origins.unsupported
    flex = _key(
        "isis.process-config.flex-algo", (("process-config", ("CORE",)), ("flex-algo", (128,))), "isis.flex.priority"
    )
    assert origins.bindings[flex].stream == "isis_flex_algo"


def test_omitted_policy_members_have_no_origin(stored_sections):
    origins = _origins(stored_sections, "route_policy")
    skipped = _key("route-policy.community-list.entry", (("community-list", ("CL-BANDWIDTH",)), ("entry", (10,))))
    assert skipped not in origins.bindings
    assert skipped not in origins.unsupported


def test_canonical_bgp_keys_include_all_ancestors(stored_sections, pg_sync_session):
    from nso_adapter.store.models import BgpPeerIntent, BgpScopeIntent

    rows, _ = stored_sections["bgp"]
    router = rows["bgp_router_intent"][0]
    router.asn = "0.65100"
    scope = BgpScopeIntent(id=2, router_id=router.id, vrf="RED")
    scope.address_families = []
    peer = BgpPeerIntent(id=2, peer_address="192.0.2.2", enabled=True)
    peer.peer_address_families = []
    scope.peers = [peer]
    router.scopes.append(scope)
    pg_sync_session.flush()
    origins = _origins(stored_sections, "bgp")
    base = _key("bgp.router.scope.peer", (("router", ("65100",)), ("scope", ("",)), ("peer", ("192.0.2.2",))))
    red = _key("bgp.router.scope.peer", (("router", ("65100",)), ("scope", ("RED",)), ("peer", ("192.0.2.2",))))
    assert base.row.key != red.row.key
    assert origins.bindings[base].logical_rows[0].row == base.row
    assert origins.bindings[red].logical_rows[0].row == red.row
    assert len(origins.bindings[_key("bgp", ())].logical_rows) == 4


async def test_document_origins_carry_generation_fences(adapter_client):
    from nso_adapter.core.apply import build_device_containers, encode_device_document
    from nso_adapter.core.generation import create_generation, note_write
    from nso_adapter.nso.origins import OriginContext
    from nso_adapter.store.models import GenerationMode, SnmpV3UserIntent
    from tests.conftest import seed_device, session
    from tests.nso.device_intent_rows import ACCEPTED

    device_id = await seed_device()
    async with session() as db:
        db.add(
            SnmpV3UserIntent(
                device_id=device_id, username="placeholder-user", auth_protocol="sha", accepted_at=ACCEPTED
            )
        )
        await db.flush()
        await note_write(db, device_id, "snmp", push_seq=41)
        generation = await create_generation(db, device_id, streams=("snmp",), mode=GenerationMode.networked)
        context = OriginContext.from_generation(generation)
        encoded = encode_device_document(generation.document, origin_context=context)
        # This body does not read NSO. The client is absent and static-route is absent.
        device = await db.get(Device, device_id)
        assembled = await build_device_containers(None, device, generation.document, origin_context=context)
        key = _key("snmp.v3-user", (("v3-user", ("placeholder-user",)),), "snmp.v3_user.auth_secret")
        logical = assembled.origins.bindings[key].logical_rows[0]
        assert assembled.containers == encoded.body
        assert assembled.origins.bindings == encoded.origins.bindings
        assert logical.source_push_seq == 41
        assert logical.frozen_revision == generation.stream_revisions["snmp"]
        assert logical.generation_id == generation.id
        assert logical.settlement_cohort == generation.settlement_cohort
        assert logical.acceptance_identity == ACCEPTED.isoformat()
        assert logical.carrier.source_push_seq == logical.source_push_seq
        assert assembled.errors == {}


def _shared_fixture():
    return json.loads((Path(__file__).parents[1] / "fixtures" / "refusal_settlement.json").read_text())


def test_shared_mapping_fixture_joins_exact_outcome_grain(stored_sections):
    from dataclasses import asdict

    from nso_adapter.nso.origins import OriginIndex, OriginKey
    from nso_adapter.nso.refusal_protocol import RowRef

    rows, _ = stored_sections["bgp"]
    rows["bgp_router_intent"][0].asn = "64512"
    rows["bgp_router_intent"][0].scopes[0].peers[0].peer_address = "192.0.2.1"
    rows["redistribution_intent"][0].dest_ref = "64512::ipv4-unicast"
    stored_sections["ospf"][0]["redistribution_intent"][0].dest_ref = "1"
    origins = OriginIndex()
    for section in ("bgp", "lag", "switchport", "route_policy", "isis", "ospf"):
        origins.merge(_origins(stored_sections, section))
    for case in _shared_fixture()["mapping_cases"]:
        key = OriginKey(case["family"], RowRef(**case["cause"]))
        binding = origins.bindings[key]
        assert len(binding.logical_rows) == 1
        logical = binding.logical_rows[0]
        assert asdict(logical.row) == case["logical"]
        assert asdict(logical.carrier.root_row) == case["carrier"]


@pytest.mark.anyio
async def test_shared_peer_fixture_has_public_fences_and_frozen_child_content(adapter_client):
    from datetime import datetime

    from nso_adapter.core.bgp_load import attach_bgp_relationships
    from nso_adapter.nso.origins import OriginContext, OriginKey, resolve_refusals
    from nso_adapter.nso.refusal_protocol import RowRef, refusal
    from tests.conftest import AUTH, push_seq, seed_device, session

    fixture = _shared_fixture()["bgp"]
    accepted = fixture["accepted_payload"]
    fence = fixture["fence"]
    device_id = await seed_device(nso_device_name="lab-device")
    response = await adapter_client.put(
        f"/api/v1/devices/{device_id}/bgp-intent",
        json={"routers": [{**accepted, "accepted_at": fence["acceptance_identity"]}]},
        headers=AUTH | push_seq(fence["source_push_seq"]),
    )
    assert response.status_code == 200, response.text
    async with session() as db:
        router = (await db.scalars(select(BgpRouterIntent).where(BgpRouterIntent.device_id == device_id))).one()
        await attach_bgp_relationships(db, [router])
        assert [(af.af, af.enabled) for af in router.scopes[0].peers[0].peer_address_families] == [
            ("ipv4-unicast", True)
        ]
        context = OriginContext(
            fence["generation_id"],
            {"bgp": fence["source_push_seq"]},
            {"bgp": fence["frozen_revision"]},
            fence["settlement_cohort"],
        )
        execution = SectionExecution(None, community_dialect_for(None), origin_context=context)
        rows = {"bgp_router_intent": [router], "redistribution_intent": []}
        encoded = section_registry()["bgp"].encode(rows, execution)
        logicals = []
        for expected in fixture["expected"]:
            key = OriginKey("bgp", RowRef(**expected["row"]))
            logical = encoded.origins.bindings[key].logical_rows[0]
            # D can join these records using only the accepted payload and its receipt fence.
            assert logical.public_record() == {
                key: value for key, value in expected.items() if key not in {"outcome", "cause"}
            }
            logicals.append(logical)
        assert len({logical.carrier for logical in logicals}) == fixture["failed_carrier_count"]
        named = fixture["refusal"]
        result = resolve_refusals((refusal("bgp", RowRef(**named["row"]), named["message_id"]),), encoded.origins)
        assert isinstance(result, tuple) and result[0].logical_rows == (logicals[0],)
        assert logicals[1] not in result[0].logical_rows
        peer_af_case = next(
            case
            for case in _shared_fixture()["mapping_cases"]
            if case["cause"]["subject"] == "bgp.router.scope.peer.peer-address-family"
        )
        peer_af_refusal = refusal("bgp", RowRef(**peer_af_case["cause"]), named["message_id"])
        peer_af_result = resolve_refusals((peer_af_refusal,), encoded.origins)
        assert isinstance(peer_af_result, tuple) and peer_af_result[0].logical_rows == (logicals[0],)
        peer_af_key = OriginKey("bgp", peer_af_refusal.row)
        assert any(
            source.source_table == "bgp_peer_af_intent" for source in encoded.origins.bindings[peer_af_key].sources
        )
        prior_public = logicals[1].public_record()
        prior_content = logicals[1].source_refs[0].content_identity
        successor = fixture["executions"][2]["fence"]
        router.scopes[0].peers[1].remote_as = "64515"
        router.accepted_at = datetime.fromisoformat(successor["acceptance_identity"])
        await db.flush()
        successor_context = OriginContext(
            successor["generation_id"],
            {"bgp": successor["source_push_seq"]},
            {"bgp": successor["frozen_revision"]},
            successor["settlement_cohort"],
        )
        newer = section_registry()["bgp"].encode(rows, execution._replace(origin_context=successor_context))
        peer_key = OriginKey("bgp", logicals[1].row)
        successor_logical = newer.origins.bindings[peer_key].logical_rows[0]
        assert successor_logical.public_record() != prior_public
        assert successor_logical.source_refs[0].content_identity != prior_content
        assert successor_logical.carrier.content_identity != logicals[1].carrier.content_identity
        assert logicals[1].public_record() == prior_public
        assert logicals[1].source_refs[0].content_identity == prior_content


def test_parent_construct_sources_include_logical_children(stored_sections):
    origins = _origins(stored_sections, "bgp")
    parent = _key("bgp.router", (("router", ("65100",)),))
    binding = origins.bindings[parent]
    assert any(source.source_table == "bgp_peer_intent" for source in binding.sources)
    assert any(source.source_table == "bgp_peer_af_intent" for source in binding.sources)
    scope = _key("bgp.router.scope", (("router", ("65100",)), ("scope", ("",))))
    assert any(source.source_table == "bgp_peer_intent" for source in origins.bindings[scope].sources)
    assert {
        source.source_table
        for source in origins.bindings[_key("bgp.router", (("router", ("65100",)),), "bgp.router.router_id")].sources
    } == {"bgp_router_intent"}


def test_duplicate_tagged_membership_fails_before_send(stored_sections):
    rows, _ = stored_sections["switchport"]
    row = rows["switchport_intent"][0]
    row.tagged_vlans.append(row.tagged_vlans[0])
    with pytest.raises(ValueError, match="duplicate origin"):
        _encode(stored_sections, "switchport")


def test_duplicate_origin_check_stays_linear_in_the_section_rows():
    from nso_adapter.nso.origins import OriginConstructionError, OriginContext, OriginRecorder, SourceRef

    class CountingStream(str):
        comparisons = 0

        def __eq__(self, other):
            type(self).comparisons += 1
            return str.__eq__(self, other)

        __hash__ = str.__hash__

    stream = CountingStream("static_route")
    recorder = OriginRecorder("static-route", OriginContext())
    for index in range(300):
        path = (("route", ("", f"198.18.{index // 250}.{index % 250}/32", "198.18.255.1")),)
        source = SourceRef("static_route_intent", (index,), "digest", None, None, None)
        recorder.logical("static-route.route", path, source, stream)
    assert CountingStream.comparisons < 300
    with pytest.raises(OriginConstructionError, match="duplicate origin"):
        recorder.logical("static-route.route", path, source, stream)


@pytest.mark.parametrize(
    ("carried", "member"),
    [
        ({"vlan": (5, {"vlan": 7}, {"vlan": 41})}, True),
        ({"vlan": (5, {"vlan": 7}, {"vlan": 42})}, False),
        ({"vlan": (5, {"vlan": 7}, {})}, False),
        ({"vlan": (5, {}, {"vlan": 41})}, False),
        ({"vlan": (6, {"vlan": 7}, {"vlan": 41})}, False),
        ({}, False),
    ],
)
def test_current_membership_requires_the_stream_generation_revision_and_push(carried, member):
    from nso_adapter.nso.origins import OriginContext, OriginRecorder, SourceRef, current_membership

    recorder = OriginRecorder("vlan", OriginContext(5, {"vlan": 41}, {"vlan": 7}))
    path = (("vlan", (10,)),)
    logical = recorder.logical("vlan.vlan", path, SourceRef("vlan_intent", (1,), "digest", 7, None, None), "vlan")
    recorder.emit("vlan.vlan", path, (logical,))
    origins = recorder.finish({}).origins
    contexts = {
        stream: OriginContext(generation, push, revisions) for stream, (generation, revisions, push) in carried.items()
    }
    context = OriginContext(5, {"vlan": 41}, {"vlan": 7}, None, contexts)
    assert current_membership(origins, context) == ((logical,) if member else ())
