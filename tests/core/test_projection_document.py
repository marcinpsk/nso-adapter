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


def test_every_section_is_either_document_executed_or_names_its_blocker():
    """No third state. Adding a section forces a decision about how it deploys."""
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
    assert ACTION_APPLY_EXECUTABLE_SECTIONS == DOCUMENT_EXECUTED_SECTIONS
    assert ACTION_APPLY_EXECUTABLE_SECTIONS is not DOCUMENT_EXECUTED_SECTIONS


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
    from nso_adapter.core.projection import section_streams, stream_for_model, stream_section
    from nso_adapter.store.models import Device

    with pytest.raises(ValueError, match="unknown projection stream"):
        stream_section("not_a_stream")
    with pytest.raises(ValueError, match="unknown projection section"):
        # A STREAM name, not a section: sixteen streams, fourteen sections.
        section_streams("ip")
    with pytest.raises(ValueError, match="belongs to no projection stream"):
        stream_for_model(Device)


def test_section_models_validates_sections_and_is_exported():
    from nso_adapter.core import projection
    from nso_adapter.store.models import BfdIntent, VlanIntent

    models = projection.section_models(section for section in ("vlan", "bfd"))
    assert models == frozenset({VlanIntent, BfdIntent})
    with pytest.raises(ValueError, match="unknown projection section 'unknown'"):
        projection.section_models(["unknown"])
    assert "section_models" in projection.__all__


def test_a_model_resolves_to_the_stream_that_owns_it():
    """What ``replace_on_removal`` promotes on: the model, not the family it sits in."""
    from nso_adapter.core.projection import stream_for_model
    from nso_adapter.store.models import InterfaceIntent, InterfaceIpIntent, IsisFlexAlgoIntent, VlanIntent

    assert stream_for_model(VlanIntent) == "vlan"
    assert stream_for_model(InterfaceIntent) == "interface_config"
    assert stream_for_model(InterfaceIpIntent) == "ip"
    assert stream_for_model(IsisFlexAlgoIntent) == "isis_flex_algo"


def test_a_model_several_families_share_has_no_single_owner():
    """A redistribution row belongs to whichever protocol it points at, so it names no lane.

    Answering with whichever stream came first would promote an unrelated family.
    """
    from nso_adapter.core.projection import stream_for_model
    from nso_adapter.store.models import RedistributionIntent

    with pytest.raises(ValueError, match="is shared by streams"):
        stream_for_model(RedistributionIntent)


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
