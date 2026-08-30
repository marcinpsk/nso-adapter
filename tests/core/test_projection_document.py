# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The projection: the two partitions, and snapshot ↔ hydrate round-tripping (#1522 §G1).

A stored document is only executable if rebuilding it gives back the rows it was taken from.
And a section that is neither document-executed nor listed with its reason is a family that
quietly fell out of the protocol — so the partition is pinned, not documented.

The second partition is stream OWNERSHIP: which intent tables each endpoint's lane
authorizes. It decides what a push may carry to the device, so a table falling into neither
lane of a shared family, or into both, has to fail loudly rather than be resolved by
whichever stream happens to promote first (#1558 rework 3, finding 1).
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from tests.conftest import seed_device, session

pytestmark = pytest.mark.anyio

_INCREMENT_ONE_SECTIONS = frozenset(
    {"svi", "subinterface", "bfd", "interface_mtu", "l2_sap", "isis", "route_policy", "ospf"}
)
_INCREMENT_TWO_SECTIONS = frozenset({"snmp", "logging"})
_INCREMENT_THREE_SECTIONS = frozenset({"bgp"})
_INCREMENT_FOUR_SECTIONS = frozenset({"interface_config"})
_INCREMENT_FIVE_SECTIONS = frozenset({"static_route"})


def test_every_section_is_either_document_executed_or_names_its_blocker():
    """No third state. Manual selection and execution stay equal but distinct concepts."""
    from nso_adapter.core.projection import (
        ACTION_APPLY_EXECUTABLE_SECTIONS,
        DOCUMENT_EXECUTED_SECTIONS,
        LIVE_READ_SECTIONS,
        projection_sections,
    )

    partition = DOCUMENT_EXECUTED_SECTIONS | set(LIVE_READ_SECTIONS)
    assert partition == projection_sections(), (
        f"sections with no disposition: {sorted(projection_sections() - partition)}; "
        f"unknown sections named: {sorted(partition - projection_sections())}"
    )
    assert not (DOCUMENT_EXECUTED_SECTIONS & set(LIVE_READ_SECTIONS)), "a section cannot be both"
    assert all(reason for reason in LIVE_READ_SECTIONS.values()), "every live-read section must state why"
    assert ACTION_APPLY_EXECUTABLE_SECTIONS == DOCUMENT_EXECUTED_SECTIONS, (
        "manual Apply may select exactly the sections that execute from their documents"
    )
    assert ACTION_APPLY_EXECUTABLE_SECTIONS is DOCUMENT_EXECUTED_SECTIONS, (
        "selection and execution must share one section-set source of truth"
    )
    incremented = (
        _INCREMENT_ONE_SECTIONS
        | _INCREMENT_TWO_SECTIONS
        | _INCREMENT_THREE_SECTIONS
        | _INCREMENT_FOUR_SECTIONS
        | _INCREMENT_FIVE_SECTIONS
    )
    assert incremented | {"vlan"} == DOCUMENT_EXECUTED_SECTIONS, (
        "vlan is intentionally document-executed without an incremental rollout step"
    )


def test_every_projection_column_has_a_supported_json_round_trip():
    from datetime import date
    from decimal import Decimal

    from sqlalchemy import JSON, BigInteger, Boolean, DateTime, Integer, String, Text
    from sqlalchemy.dialects.postgresql import JSONB

    from nso_adapter.core.projection import _SECTION_TABLES

    supported_types = {BigInteger, Boolean, DateTime, Integer, JSON, JSONB, String, Text}
    supported_python_types = {bool, bytes, date, datetime, Decimal, dict, int, str}
    models = {spec.model for specs in _SECTION_TABLES.values() for spec in specs}
    unsupported = []
    for model in sorted(models, key=lambda candidate: candidate.__name__):
        for column in model.__table__.columns:
            try:
                python_type = column.type.python_type
            except NotImplementedError:
                python_type = None
            if type(column.type) not in supported_types or python_type not in supported_python_types:
                unsupported.append(
                    f"{model.__name__}.{column.name}: {type(column.type).__name__}/{getattr(python_type, '__name__', None)}"
                )

    assert not unsupported, f"projection columns without a proven JSON round trip: {unsupported}"


def test_hydrate_section_refuses_an_absent_section():
    from nso_adapter.core.projection import hydrate_section

    with pytest.raises(ValueError, match="document does not carry section 'vlan'"):
        hydrate_section({}, "vlan")


def test_hydrate_section_accepts_an_explicitly_empty_section():
    from nso_adapter.core.projection import hydrate_section

    assert hydrate_section({"vlan": {}}, "vlan") == {}


def test_composing_an_empty_promoted_stream_preserves_its_section():
    from nso_adapter.core.generation import _compose_document

    assert _compose_document({"vlan": {}}) == {"vlan": {}}


def test_increment_one_sections_are_document_executed():
    from nso_adapter.core.projection import DOCUMENT_EXECUTED_SECTIONS, LIVE_READ_SECTIONS

    assert _INCREMENT_ONE_SECTIONS <= DOCUMENT_EXECUTED_SECTIONS
    assert not (_INCREMENT_ONE_SECTIONS & set(LIVE_READ_SECTIONS))


def test_increment_two_sections_are_document_executed():
    from nso_adapter.core.projection import DOCUMENT_EXECUTED_SECTIONS, LIVE_READ_SECTIONS

    assert _INCREMENT_TWO_SECTIONS <= DOCUMENT_EXECUTED_SECTIONS
    assert not (_INCREMENT_TWO_SECTIONS & set(LIVE_READ_SECTIONS))


def test_increment_three_sections_are_document_executed():
    from nso_adapter.core.projection import DOCUMENT_EXECUTED_SECTIONS, LIVE_READ_SECTIONS

    assert _INCREMENT_THREE_SECTIONS <= DOCUMENT_EXECUTED_SECTIONS
    assert not (_INCREMENT_THREE_SECTIONS & set(LIVE_READ_SECTIONS))


def test_increment_four_sections_are_document_executed():
    from nso_adapter.core.projection import DOCUMENT_EXECUTED_SECTIONS, LIVE_READ_SECTIONS

    assert _INCREMENT_FOUR_SECTIONS <= DOCUMENT_EXECUTED_SECTIONS
    assert not (_INCREMENT_FOUR_SECTIONS & set(LIVE_READ_SECTIONS))


def test_increment_five_completes_document_execution():
    from nso_adapter.core.projection import (
        ACTION_APPLY_EXECUTABLE_SECTIONS,
        DOCUMENT_EXECUTED_SECTIONS,
        LIVE_READ_SECTIONS,
        projection_sections,
        projection_streams,
        stream_section,
    )

    assert _INCREMENT_FIVE_SECTIONS <= DOCUMENT_EXECUTED_SECTIONS
    assert LIVE_READ_SECTIONS == {}
    assert ACTION_APPLY_EXECUTABLE_SECTIONS == DOCUMENT_EXECUTED_SECTIONS == projection_sections()
    assert ACTION_APPLY_EXECUTABLE_SECTIONS is DOCUMENT_EXECUTED_SECTIONS
    assert len(projection_streams()) == 16
    assert all(stream_section(stream) in ACTION_APPLY_EXECUTABLE_SECTIONS for stream in projection_streams())


@pytest.mark.parametrize(
    ("section", "model_name", "values", "changed_field", "successor_value"),
    [
        pytest.param(
            "svi",
            "SviIntent",
            {"interface_name": "Vlan100", "vlan_id": 100, "svi_type": "svi"},
            "vlan_id",
            200,
            id="svi",
        ),
        pytest.param(
            "subinterface",
            "SubinterfaceIntent",
            {
                "interface_name": "GigabitEthernet0/1.100",
                "parent_interface": "GigabitEthernet0/1",
                "dot1q_vlan": 100,
                "sub_type": "subinterface",
            },
            "dot1q_vlan",
            200,
            id="subinterface",
        ),
        pytest.param(
            "bfd",
            "BfdIntent",
            {"interface_name": "Port-channel1", "min_tx": 300, "min_rx": 300, "multiplier": 3},
            "min_tx",
            900,
            id="bfd",
        ),
        pytest.param(
            "interface_mtu",
            "InterfaceMtuIntent",
            {"interface_name": "Port-channel1", "mtu": 9216},
            "mtu",
            9000,
            id="interface_mtu",
        ),
        pytest.param(
            "l2_sap",
            "L2SapIntent",
            {
                "service_name": "EXAMPLE",
                "service_type": "epipe",
                "sap_id": "lag-60:3999",
                "port": "lag-60",
            },
            "port",
            "lag-61",
            id="l2_sap",
        ),
        pytest.param(
            "isis",
            "IsisInterfaceIntent",
            {"interface_name": "system", "af": "ipv4", "process_tag": "0", "metric": 10},
            "metric",
            20,
            id="isis",
        ),
        pytest.param(
            "route_policy",
            "RoutePolicyObjectIntent",
            {"family": "prefix_list", "name": "EXAMPLE-PFX", "entries": [{"sequence": 10}]},
            "entries",
            [{"sequence": 20}],
            id="route_policy",
        ),
        pytest.param(
            "ospf",
            "OspfInstanceIntent",
            {"process_id": "1", "router_id": "192.0.2.1", "vrf": ""},
            "router_id",
            "192.0.2.2",
            id="ospf",
        ),
    ],
)
async def test_increment_one_apply_rows_come_from_the_generation_document(
    adapter_client,
    section,
    model_name,
    values,
    changed_field,
    successor_value,
):
    """A successor can change the store, but the run still pushes the selected document."""
    from sqlalchemy import inspect as sa_inspect

    from nso_adapter.core.apply import _Projection
    from nso_adapter.core.projection import snapshot_stream
    from nso_adapter.store import models

    device_id = await seed_device(nso_device_name=f"document-{section}")
    model = getattr(models, model_name)
    accepted = datetime(2026, 8, 1, tzinfo=UTC)
    async with session() as db:
        row = model(device_id=device_id, accepted_at=accepted, **values)
        db.add(row)
        await db.flush()
        original_value = getattr(row, changed_field)
        document = {section: await snapshot_stream(db, device_id, section)}
        setattr(row, changed_field, successor_value)
        await db.commit()

    async with session() as db:
        source = _Projection(db, device_id, True, document, frozenset({section}))
        selected = await source.collect(model, section=section)

    assert [getattr(row, changed_field) for row in selected.push] == [original_value]
    assert all(sa_inspect(row).transient for row in selected.push)
    assert selected.stamp == [], "the successor row differs from the document and must stay pending"


async def test_snmp_apply_rows_and_vault_refs_come_from_the_generation_document(adapter_client):
    """A successor ref stays pending while the selected document's ref reaches send."""
    from sqlalchemy import inspect as sa_inspect

    from nso_adapter.core.apply import _Projection
    from nso_adapter.core.projection import snapshot_stream
    from nso_adapter.store.models import SnmpCommunityIntent

    device_id = await seed_device(nso_device_name="document-snmp", netbox_device_id=9824)
    accepted = datetime(2026, 8, 1, tzinfo=UTC)
    async with session() as db:
        row = SnmpCommunityIntent(
            device_id=device_id,
            label="readonly",
            vault_ref="network/snmp/communities/selected#community",
            access="RO",
            accepted_at=accepted,
        )
        db.add(row)
        await db.flush()
        document = {"snmp": await snapshot_stream(db, device_id, "snmp")}
        row.vault_ref = "network/snmp/communities/successor#community"
        await db.commit()

    async with session() as db:
        source = _Projection(db, device_id, True, document, frozenset({"snmp"}))
        selected = await source.collect(SnmpCommunityIntent, section="snmp")

    assert [row.vault_ref for row in selected.push] == ["network/snmp/communities/selected#community"]
    assert all(sa_inspect(row).transient for row in selected.push)
    assert selected.stamp == [], "the successor ref differs from the selected document and must stay pending"


async def test_logging_apply_rows_come_from_the_generation_document(adapter_client):
    """A successor can change logging intent, but the selected document still reaches send."""
    from sqlalchemy import inspect as sa_inspect

    from nso_adapter.core.apply import _Projection
    from nso_adapter.core.projection import snapshot_stream
    from nso_adapter.store.models import LoggingHostIntent

    device_id = await seed_device(nso_device_name="document-logging", netbox_device_id=9825)
    accepted = datetime(2026, 8, 1, tzinfo=UTC)
    async with session() as db:
        row = LoggingHostIntent(
            device_id=device_id,
            address="198.18.0.10",
            severity="ERROR",
            accepted_at=accepted,
        )
        db.add(row)
        await db.flush()
        document = {"logging": await snapshot_stream(db, device_id, "logging")}
        row.severity = "WARNING"
        await db.commit()

    async with session() as db:
        source = _Projection(db, device_id, True, document, frozenset({"logging"}))
        selected = await source.collect(LoggingHostIntent, section="logging")

    assert [row.severity for row in selected.push] == ["ERROR"]
    assert all(sa_inspect(row).transient for row in selected.push)
    assert selected.stamp == [], "the changed successor differs from the selected document and must stay pending"


async def test_a_snapshot_hydrates_back_into_the_rows_it_was_taken_from(adapter_client):
    """Round-trip fidelity, including the types JSON cannot hold natively."""
    from nso_adapter.core.projection import hydrate_section, snapshot_stream
    from nso_adapter.store.models import VlanIntent

    device_id = await seed_device(nso_device_name="projection-roundtrip", netbox_device_id=9820)
    accepted = datetime(2026, 6, 1, 12, 30, tzinfo=UTC)
    async with session() as db:
        db.add(VlanIntent(device_id=device_id, vlan_id=10, name="MGMT", accepted_at=accepted))
        db.add(VlanIntent(device_id=device_id, vlan_id=20, name=None, accepted_at=accepted))
        await db.commit()

    async with session() as db:
        document = {"vlan": await snapshot_stream(db, device_id, "vlan")}

    rows = hydrate_section(document, "vlan")[VlanIntent]
    assert [(r.vlan_id, r.name) for r in rows] == [(10, "MGMT"), (20, None)]
    # A datetime survives as a datetime, not as the ISO string the document stores.
    assert [r.accepted_at for r in rows] == [accepted, accepted]


async def test_bgp_snapshot_hydrates_the_relationship_graph_for_the_writer(adapter_client):
    """Durable parent identities rebuild the complete BGP writer graph."""
    from nso_adapter.core.projection import hydrate_section, rows_by_intent_identity, snapshot_stream
    from nso_adapter.nso.apply import apply_bgp_config
    from nso_adapter.store.models import (
        BgpAfIntent,
        BgpPeerAfIntent,
        BgpPeerIntent,
        BgpRouterIntent,
        BgpScopeIntent,
    )

    device_id = await seed_device(nso_device_name="projection-bgp-graph", netbox_device_id=9826)
    accepted = datetime(2026, 8, 1, tzinfo=UTC)
    async with session() as db:
        db.add(
            BgpRouterIntent(
                device_id=device_id,
                asn="64512",
                router_id="192.0.2.254",
                accepted_at=accepted,
                scopes=[
                    BgpScopeIntent(
                        vrf="",
                        address_families=[BgpAfIntent(af="ipv4-unicast")],
                        peers=[
                            BgpPeerIntent(
                                peer_address="192.0.2.1",
                                remote_as="64513",
                                peer_address_families=[BgpPeerAfIntent(af="ipv4-unicast", enabled=True)],
                            )
                        ],
                    )
                ],
            )
        )
        await db.commit()

    async with session() as db:
        fragment = await snapshot_stream(db, device_id, "bgp")

    assert set(rows_by_intent_identity(fragment, "bgp_peer_af_intent")) == {("64512", "", "192.0.2.1", "ipv4-unicast")}
    rows = hydrate_section({"bgp": fragment}, "bgp")
    stage: dict[str, list] = {}
    await apply_bgp_config(None, "projection-bgp-graph", rows[BgpRouterIntent], stage=stage)

    router = stage["bgp-reconciler:bgp-config"][0]["router"][0]
    assert router == {
        "asn": 64512,
        "router-id": "192.0.2.254",
        "scope": [
            {
                "vrf": "",
                "address-family": [{"afi": "ipv4-unicast"}],
                "peer": [
                    {
                        "peer-address": "192.0.2.1",
                        "enabled": True,
                        "remote-as": "64513",
                        "peer-address-family": [{"afi": "ipv4-unicast", "enabled": True}],
                    }
                ],
            }
        ],
    }


def test_bgp_hydration_resolves_parents_in_linear_work():
    """Parent lookup may inspect each peer a few times, but not once per peer AF."""
    from nso_adapter.core.projection import hydrate_section
    from nso_adapter.store.models import BgpRouterIntent

    class CountingRows(list):
        def __init__(self, rows):
            super().__init__(rows)
            self.visits = 0

        def __iter__(self):
            for row in super().__iter__():
                self.visits += 1
                yield row

    peer_count = 100
    peers = CountingRows(
        [
            {
                "id": 1000 + index,
                "scope_id": 2,
                "peer_address": f"198.18.0.{index + 1}",
            }
            for index in range(peer_count)
        ]
    )
    fragment = {
        "bgp_router_intent": [{"id": 1, "device_id": 1, "asn": "64512"}],
        "bgp_scope_intent": [{"id": 2, "router_id": 1, "vrf": ""}],
        "bgp_af_intent": [],
        "bgp_peer_intent": peers,
        "bgp_peer_af_intent": [
            {"id": 2000 + index, "peer_id": 1000 + index, "af": "ipv4-unicast"} for index in range(peer_count)
        ],
        "redistribution_intent": [],
    }

    rows = hydrate_section({"bgp": fragment}, "bgp")

    (router,) = rows[BgpRouterIntent]
    assert len(router.scopes[0].peers) == peer_count
    assert all(len(peer.peer_address_families) == 1 for peer in router.scopes[0].peers)
    assert peers.visits <= peer_count * 4


async def test_snmp_snapshot_stores_vault_references_verbatim(adapter_client):
    """The durable document carries locators only. NSO resolves them when it sends."""
    from nso_adapter.core.projection import snapshot_stream
    from nso_adapter.store.models import SnmpCommunityIntent, SnmpV3UserIntent

    device_id = await seed_device(nso_device_name="projection-snmp-refs", netbox_device_id=9822)
    accepted = datetime(2026, 8, 1, tzinfo=UTC)
    async with session() as db:
        db.add(
            SnmpCommunityIntent(
                device_id=device_id,
                label="readonly",
                vault_ref="network/snmp/communities/readonly#community",
                access="RO",
                accepted_at=accepted,
            )
        )
        db.add(
            SnmpV3UserIntent(
                device_id=device_id,
                username="monitor",
                auth_vault_ref="network/snmp/users/monitor#auth",
                priv_vault_ref="network/snmp/users/monitor#priv",
                accepted_at=accepted,
            )
        )
        await db.commit()

    async with session() as db:
        snapshot = await snapshot_stream(db, device_id, "snmp")

    assert snapshot["snmp_community_intent"][0]["vault_ref"] == ("network/snmp/communities/readonly#community")
    assert snapshot["snmp_v3_user_intent"][0]["auth_vault_ref"] == "network/snmp/users/monitor#auth"
    assert snapshot["snmp_v3_user_intent"][0]["priv_vault_ref"] == "network/snmp/users/monitor#priv"


async def test_snmp_snapshot_refuses_secret_material_in_a_vault_reference_column(adapter_client):
    """A resolved secret must never cross the durable-document serialization boundary."""
    from nso_adapter.core.projection import snapshot_stream
    from nso_adapter.store.models import SnmpCommunityIntent

    device_id = await seed_device(nso_device_name="projection-snmp-secret-refusal", netbox_device_id=9823)
    async with session() as db:
        db.add(
            SnmpCommunityIntent(
                device_id=device_id,
                label="readonly",
                vault_ref="resolved-secret-placeholder",
                access="RO",
                accepted_at=datetime(2026, 8, 1, tzinfo=UTC),
            )
        )
        await db.commit()

    async with session() as db:
        with pytest.raises(ValueError, match="refusing to serialize non-reference secret material") as exc:
            await snapshot_stream(db, device_id, "snmp")

    assert "resolved-secret-placeholder" not in str(exc.value)


async def test_snmp_snapshot_refuses_a_keyless_path_reference(adapter_client):
    """SNMP intent columns require the '#key' form; a path-style reference is not one."""
    from nso_adapter.core.projection import snapshot_stream
    from nso_adapter.store.models import SnmpCommunityIntent

    device_id = await seed_device(nso_device_name="projection-snmp-keyless-refusal", netbox_device_id=9827)
    async with session() as db:
        db.add(
            SnmpCommunityIntent(
                device_id=device_id,
                label="readonly",
                vault_ref="network/snmp/communities/readonly",
                access="RO",
                accepted_at=datetime(2026, 8, 1, tzinfo=UTC),
            )
        )
        await db.commit()

    async with session() as db:
        with pytest.raises(ValueError, match="refusing to serialize non-reference secret material"):
            await snapshot_stream(db, device_id, "snmp")


async def test_snmp_snapshot_treats_an_empty_optional_vault_reference_as_absent(adapter_client):
    """The API stores '' for an absent optional v3 leg; serialization must not parse it."""
    from nso_adapter.core.projection import snapshot_stream
    from nso_adapter.store.models import SnmpV3UserIntent

    device_id = await seed_device(nso_device_name="projection-snmp-empty-ref", netbox_device_id=9824)
    async with session() as db:
        db.add(
            SnmpV3UserIntent(
                device_id=device_id,
                username="monitor",
                auth_vault_ref="network/snmp/users/monitor#auth",
                priv_vault_ref="",
                accepted_at=datetime(2026, 8, 1, tzinfo=UTC),
            )
        )
        await db.commit()

    async with session() as db:
        snapshot = await snapshot_stream(db, device_id, "snmp")

    assert snapshot["snmp_v3_user_intent"][0]["priv_vault_ref"] == ""


def test_hydrating_an_unknown_table_or_column_is_refused():
    """A document the schema no longer matches fails loudly instead of deploying a subset."""
    from nso_adapter.core.projection import hydrate_section

    with pytest.raises(ValueError, match="unknown table"):
        hydrate_section({"vlan": {"not_a_table": []}}, "vlan")
    with pytest.raises(ValueError, match="unknown column"):
        hydrate_section({"vlan": {"vlan_intent": [{"nope": 1}]}}, "vlan")


def test_hydrating_a_row_without_its_primary_key_is_refused():
    """An id-less row stamps nothing: the apply would report a (0, 0) success."""
    from nso_adapter.core.projection import hydrate_section

    with pytest.raises(ValueError, match="primary key"):
        hydrate_section({"vlan": {"vlan_intent": [{"device_id": 1, "vlan_id": 10}]}}, "vlan")
    with pytest.raises(ValueError, match="primary key"):
        hydrate_section({"vlan": {"vlan_intent": [{"id": None, "device_id": 1, "vlan_id": 10}]}}, "vlan")


def test_only_the_static_route_tables_carry_a_correlation_column():
    """``projection_row_state`` drops these by NAME, for every table.

    They are NetBox lineage the device payload never renders, so dropping them stops a
    correlation-only repair from reading as a device delta. A table that gained a column of
    either name for real content would have it silently dropped instead, so pin the fact.
    """
    from nso_adapter.core.projection import _SECTION_TABLES, CORRELATION_COLUMNS

    carriers = {
        spec.model.__tablename__
        for specs in _SECTION_TABLES.values()
        for spec in specs
        if CORRELATION_COLUMNS & {column.key for column in spec.model.__table__.columns}
    }
    assert carriers == {"static_route_intent", "static_route_tombstone"}


def test_hydrating_a_known_table_under_the_wrong_section_is_refused():
    """A document cannot smuggle one section's rows into another section."""
    from nso_adapter.core.projection import hydrate_section

    with pytest.raises(ValueError, match="does not belong"):
        hydrate_section({"svi": {"vlan_intent": []}}, "svi")


def test_interface_execution_context_hydrates_beside_intent_tables():
    """Execution metadata is explicit and does not masquerade as an intent table."""
    from nso_adapter.core.projection import EXECUTION_KEY, hydrate_interface_execution, hydrate_section
    from nso_adapter.store.models import InterfaceIntent

    document = {
        "interface_config": {
            "interface_intent": [
                {
                    "id": 11,
                    "interface_id": 7,
                    "attribute": "description",
                    "intent_value": "selected description",
                }
            ],
            "interface_ip_intent": [],
            EXECUTION_KEY: {
                "interfaces": [
                    {
                        "id": 7,
                        "name": "GigabitEthernet0/1",
                        "kind": None,
                        "parent_binding": None,
                        "encap_tag": None,
                        "vrf": None,
                        "service": None,
                    }
                ],
                "eligible_interface_attributes": [{"interface_id": 7, "attribute": "description"}],
            },
        }
    }

    rows = hydrate_section(document, "interface_config")
    execution = hydrate_interface_execution(document)

    assert rows[InterfaceIntent][0].intent_value == "selected description"
    assert execution.interfaces[7].name == "GigabitEthernet0/1"
    assert execution.eligible_attributes == frozenset({(7, "description")})


def test_every_parented_table_has_a_local_durable_identity():
    """A child identity is its parent's prefixed by its own; an empty local part slices to ().

    ``identity[:-0]`` is the empty tuple, so such a table would report every child row as
    referencing a missing parent. :func:`_attach_hydrated_relationships` refuses it; this
    fails first, on the schema itself.
    """
    from nso_adapter.core.projection import _SECTION_TABLES, _SPEC_BY_MODEL, _identity_fields

    for specs in _SECTION_TABLES.values():
        for spec in specs:
            if spec.parent in _SPEC_BY_MODEL:
                assert _identity_fields(spec), f"{spec.model.__tablename__} has no local durable identity"


# ── stream ownership: the authorization partition ────────────────────────────


def test_every_stream_owns_a_disjoint_slice_of_its_section():
    """Each family's tables are partitioned across its streams — no gap, no overlap."""
    from nso_adapter.core.projection import _stream_tables, projection_sections, projection_streams, stream_section

    by_section: dict[str, list[str]] = {}
    for stream in projection_streams():
        for spec in _stream_tables()[stream]:
            by_section.setdefault(stream_section(stream), []).append(spec.model.__tablename__)
    for section, tables in by_section.items():
        assert len(tables) == len(set(tables)), f"{section}: a table is owned by two streams — {sorted(tables)}"
    assert set(by_section) == projection_sections(), "a section has no stream to authorize it"


def test_the_two_shared_families_split_their_tables_by_endpoint():
    """The concrete ownership the #103 leak turns on, spelled out."""
    from nso_adapter.core.projection import _stream_tables, section_streams

    assert section_streams("interface_config") == ("interface_config", "ip")
    assert section_streams("isis") == ("isis", "isis_flex_algo")

    def tables(stream: str) -> set[str]:
        return {spec.model.__tablename__ for spec in _stream_tables()[stream]}

    assert tables("interface_config") == {"interface_intent"}
    assert tables("ip") == {"interface_ip_intent"}
    assert tables("isis_flex_algo") == {"isis_flex_algo_intent"}
    assert "isis_flex_algo_intent" not in tables("isis")


def test_an_unpaired_family_is_its_own_single_stream():
    from nso_adapter.core.projection import section_streams, stream_section

    assert section_streams("vlan") == ("vlan",)
    assert stream_section("vlan") == "vlan"
    assert stream_section("ip") == "interface_config"


def test_a_name_outside_either_vocabulary_is_refused():
    """The two vocabularies are different sizes, so one cannot stand in for the other."""
    from nso_adapter.core.projection import section_streams, stream_section

    with pytest.raises(ValueError, match="unknown projection stream"):
        stream_section("not_a_stream")
    with pytest.raises(ValueError, match="unknown projection section"):
        # A STREAM name, not a section: sixteen streams, fourteen sections.
        section_streams("ip")


def test_section_models_validates_sections_and_is_exported():
    from nso_adapter.core import projection
    from nso_adapter.store.models import BfdIntent, VlanIntent

    models = projection.section_models(section for section in ("vlan", "bfd"))
    assert models == frozenset({VlanIntent, BfdIntent})
    with pytest.raises(ValueError, match="unknown projection section 'unknown'"):
        projection.section_models(["unknown"])
    assert "section_models" in projection.__all__


async def test_a_snapshot_is_a_fragment_of_only_the_tables_its_stream_owns(adapter_client):
    """The producer a generation stores: one lane's tables, never its sibling's."""
    from nso_adapter.core.projection import snapshot_stream

    device_id = await seed_device(nso_device_name="projection-fragment", netbox_device_id=9821)
    async with session() as db:
        assert set(await snapshot_stream(db, device_id, "ip")) == {"interface_ip_intent"}
        assert set(await snapshot_stream(db, device_id, "interface_config")) == {"interface_intent"}
        with pytest.raises(ValueError, match="unknown projection stream"):
            await snapshot_stream(db, device_id, "interface")


def _clear_projection_caches() -> None:
    from nso_adapter.core import projection

    for fn in (
        projection._stream_tables,
        projection._stream_section,
        projection.projection_streams,
        projection.projection_sections,
    ):
        fn.cache_clear()


def test_a_split_that_leaves_a_table_unowned_is_refused(monkeypatch):
    """The loud failure the ownership map depends on.

    A table added to a shared family and named by neither lane would be authorized by
    whichever stream promoted first — the leak this partition exists to close, back by
    omission. Restated as a build-time refusal, not a convention.
    """
    from nso_adapter.core import projection
    from nso_adapter.store.models import IsisFlexAlgoIntent, IsisProcessIntent

    _clear_projection_caches()
    try:
        monkeypatch.setitem(
            projection._SPLIT_SECTION_STREAMS,
            "isis",
            {"isis": (IsisProcessIntent,), "isis_flex_algo": (IsisFlexAlgoIntent,)},
        )
        with pytest.raises(RuntimeError, match="does not partition its tables"):
            projection._stream_tables()
    finally:
        monkeypatch.undo()
        _clear_projection_caches()
    # The real map is intact for every test that runs after this one.
    assert projection.projection_streams() == frozenset(projection._stream_tables())


def test_an_endpoint_with_no_stream_is_refused(monkeypatch):
    """The endpoint registry and the ownership map are pinned to each other, both ways.

    A seventeenth intent PUT that nobody gave tables to would promote a lane owning nothing,
    and a stream no endpoint delivers to could never be authorized at all.
    """
    from nso_adapter.core import intent_protocol, projection

    _clear_projection_caches()
    try:
        monkeypatch.setitem(
            intent_protocol.INTENT_PUT_ENDPOINTS,
            "/api/v1/devices/{device_id}/lldp-intent",
            intent_protocol.IntentEndpoint("lldp", "vlan"),
        )
        with pytest.raises(RuntimeError, match="only-endpoints"):
            projection.projection_streams()
    finally:
        monkeypatch.undo()
        _clear_projection_caches()
    assert "lldp" not in projection.projection_streams()


def test_a_stream_whose_section_is_not_what_its_endpoint_promotes_is_refused(monkeypatch):
    """The `promotes` column and the table ownership must name the same family."""
    from nso_adapter.core import intent_protocol, projection

    _clear_projection_caches()
    try:
        monkeypatch.setitem(
            intent_protocol.INTENT_PUT_ENDPOINTS,
            "/api/v1/devices/{device_id}/ip-intent",
            intent_protocol.IntentEndpoint("ip", "vlan"),
        )
        with pytest.raises(RuntimeError, match="differs from what their endpoint promotes"):
            projection.projection_streams()
    finally:
        monkeypatch.undo()
        _clear_projection_caches()
    assert projection.stream_section("ip") == "interface_config"
