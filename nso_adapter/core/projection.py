# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The projection: which sections exist, and what each one's desired state IS (#1522 §G1).

Two vocabularies, deliberately different sizes.

A *section* is one family of the device's outbound DOCUMENT — the same vocabulary the
removal scopes already use, so there is one name for ``isis`` and not two. The section set
is DERIVED from :data:`core.removal.VALID_REMOVAL_SCOPES`; it is never restated here,
because a section the removal path can address and the generation path cannot would be a
family whose shrink has no promotion record.

A *stream* is one endpoint's delivery lane: sixteen of them, one per in-protocol intent PUT
(:mod:`core.intent_protocol`). It is the AUTHORIZATION unit — what a receipt keys on and
what a promotion promotes — and it owns an explicit, disjoint subset of its section's intent
tables. Fourteen sections, sixteen streams: ``interface_config``/``ip`` split the interface
document and ``isis``/``isis_flex_algo`` split the IS-IS one. Promoting at section grain
would let a normal push on one of a pair authorize the OTHER lane's un-promoted store-only
state, which is exactly what ``authorized_document`` exists to prevent (#103).

:func:`snapshot_stream` is the DOCUMENT PRODUCER a deployment generation stores. It reads
one stream's intent tables — parents and their children — and returns a stable, JSON-safe
fragment. The generation machinery treats the result as OPAQUE: it stores, digests, orders
and replays it, and never looks inside. That is the seam #1522's aggregate device-intent
builder plugs into as one more producer, replacing this one snapshot function without the
state machine noticing.

:func:`hydrate_section` is its inverse, and it is what makes a stored document EXECUTABLE:
it rebuilds the section's rows as transient ORM instances the apply's payload builders
accept unchanged. So the apply pushes the document the generation carries, not whatever the
store holds when a worker gets round to it.
"""

from __future__ import annotations

from contextlib import suppress
from datetime import date, datetime
from decimal import Decimal
from functools import cache
from typing import Any, NamedTuple

from sqlalchemy import inspect as sa_inspect
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.store.models import (
    BfdIntent,
    BgpAfIntent,
    BgpPeerAfIntent,
    BgpPeerIntent,
    BgpRouterIntent,
    BgpScopeIntent,
    DbInterface,
    InterfaceIntent,
    InterfaceIpIntent,
    InterfaceMtuIntent,
    IsisFlexAlgoIntent,
    IsisInterfaceIntent,
    IsisLevelIntent,
    IsisProcessIntent,
    L2SapIntent,
    LoggingHostIntent,
    LoggingLevelsIntent,
    OspfInstanceIntent,
    OspfInterfaceIntent,
    RedistributionIntent,
    RoutePolicyObjectIntent,
    SnmpCommunityIntent,
    SnmpHostIntent,
    SnmpSystemInfoIntent,
    SnmpV3UserIntent,
    StaticRouteIntent,
    StaticRouteTombstone,
    SubinterfaceIntent,
    SviIntent,
    VlanIntent,
)


class _Spec(NamedTuple):
    """One intent table inside a section, and how its rows reach the device.

    *parent* names the model whose ``id`` this table's foreign key points at; ``None`` means
    the table carries ``device_id`` itself. The foreign-key COLUMN is derived from the
    mapper rather than restated, so a schema change cannot leave a stale column name here.
    *discriminator* is the one case where a table serves several sections at once — a
    redistribution row belongs to the section named by its destination protocol.
    """

    model: type
    parent: type | None = None
    discriminator: tuple[str, str] | None = None


_SECTION_TABLES: dict[str, tuple[_Spec, ...]] = {
    "snmp": (
        _Spec(SnmpCommunityIntent),
        _Spec(SnmpV3UserIntent),
        _Spec(SnmpHostIntent),
        _Spec(SnmpSystemInfoIntent),
    ),
    # Tombstones ride the static-route section: an unconsumed one changes which entries the
    # document must retain verbatim, so a document built without them is a different document.
    "static_route": (_Spec(StaticRouteIntent), _Spec(StaticRouteTombstone)),
    "logging": (_Spec(LoggingHostIntent), _Spec(LoggingLevelsIntent)),
    "svi": (_Spec(SviIntent),),
    "subinterface": (_Spec(SubinterfaceIntent),),
    "vlan": (_Spec(VlanIntent),),
    "bfd": (_Spec(BfdIntent),),
    "interface_mtu": (_Spec(InterfaceMtuIntent),),
    "l2_sap": (_Spec(L2SapIntent),),
    "isis": (
        _Spec(IsisProcessIntent),
        _Spec(IsisInterfaceIntent),
        _Spec(IsisLevelIntent),
        _Spec(IsisFlexAlgoIntent),
        _Spec(RedistributionIntent, discriminator=("dest_protocol", "isis")),
    ),
    "bgp": (
        _Spec(BgpRouterIntent),
        _Spec(BgpScopeIntent, parent=BgpRouterIntent),
        _Spec(BgpAfIntent, parent=BgpScopeIntent),
        _Spec(BgpPeerIntent, parent=BgpScopeIntent),
        _Spec(BgpPeerAfIntent, parent=BgpPeerIntent),
        _Spec(RedistributionIntent, discriminator=("dest_protocol", "bgp")),
    ),
    "route_policy": (_Spec(RoutePolicyObjectIntent),),
    "ospf": (
        _Spec(OspfInstanceIntent),
        _Spec(OspfInterfaceIntent),
        _Spec(RedistributionIntent, discriminator=("dest_protocol", "ospf")),
    ),
    "interface_config": (
        _Spec(InterfaceIntent, parent=DbInterface),
        _Spec(InterfaceIpIntent, parent=DbInterface),
    ),
}


#: The sections whose tables are owned by MORE THAN ONE endpoint stream, and which tables
#: each stream owns. Every section absent from here has exactly one stream, spelled the same.
#:
#: Ownership is stated table by table because it is what an authorization covers: the ``ip``
#: endpoint authorizes addresses and nothing else, so a normal ``ip`` push must not carry the
#: interface ATTRIBUTES a store-only repair left in the store (#103). The two halves are
#: checked against the section's own table list below, so a table added to a split section
#: fails loudly instead of falling into neither lane.
_SPLIT_SECTION_STREAMS: dict[str, dict[str, tuple[type, ...]]] = {
    "interface_config": {
        "interface_config": (InterfaceIntent,),
        "ip": (InterfaceIpIntent,),
    },
    "isis": {
        "isis": (IsisProcessIntent, IsisInterfaceIntent, IsisLevelIntent, RedistributionIntent),
        "isis_flex_algo": (IsisFlexAlgoIntent,),
    },
}


@cache
def _stream_tables() -> dict[str, tuple[_Spec, ...]]:
    """Stream name -> the intent tables it owns. Built once, validated on the way."""
    specs: dict[str, tuple[_Spec, ...]] = {}
    for section, section_specs in _SECTION_TABLES.items():
        split = _SPLIT_SECTION_STREAMS.get(section)
        if split is None:
            specs[section] = section_specs
            continue
        by_model = {spec.model: spec for spec in section_specs}
        claimed: list[type] = [model for models in split.values() for model in models]
        if sorted(m.__name__ for m in claimed) != sorted(m.__name__ for m in by_model):
            raise RuntimeError(
                f"section {section!r} stream ownership does not partition its tables: "
                f"claimed {sorted(m.__name__ for m in claimed)} vs {sorted(m.__name__ for m in by_model)}"
            )
        for stream, models in split.items():
            specs[stream] = tuple(by_model[model] for model in models)
    return specs


@cache
def _stream_section() -> dict[str, str]:
    """Stream name -> the document section it contributes a fragment to."""
    owner = {stream: section for section, split in _SPLIT_SECTION_STREAMS.items() for stream in split}
    return {stream: owner.get(stream, stream) for stream in _stream_tables()}


@cache
def projection_sections() -> frozenset[str]:
    """Every section name a stored DOCUMENT can carry — the outbound device families.

    Derived, not restated, from two directions and checked at the first caller's import
    rather than as an empty document later:

    * the removal scopes ARE the write-path families, so a scope missing from
      :data:`_SECTION_TABLES` is a family whose document could not be built;
    * every in-protocol intent endpoint promotes one of these, so an endpoint naming a
      family with no tables would bump a revision nothing can ever deploy.

    This is NOT the promotion vocabulary — see :func:`projection_streams`.
    """
    from nso_adapter.core.intent_protocol import INTENT_PUT_ENDPOINTS
    from nso_adapter.core.removal import VALID_REMOVAL_SCOPES

    missing = VALID_REMOVAL_SCOPES - set(_SECTION_TABLES)
    if missing:
        raise RuntimeError(f"projection sections missing intent tables: {sorted(missing)}")
    extra = set(_SECTION_TABLES) - VALID_REMOVAL_SCOPES
    if extra:
        raise RuntimeError(f"projection sections name no removal scope: {sorted(extra)}")
    unpromotable = {e.promotes for e in INTENT_PUT_ENDPOINTS.values()} - set(_SECTION_TABLES)
    if unpromotable:
        raise RuntimeError(f"intent endpoints promote sections with no intent tables: {sorted(unpromotable)}")
    return frozenset(_SECTION_TABLES)


@cache
def projection_streams() -> frozenset[str]:
    """Every stream name the PROMOTION protocol accepts — the authorization lanes.

    Pinned against the endpoint registry in BOTH directions, so a stream and its receipt
    cannot drift apart: an endpoint with no stream would promote a lane nothing owns tables
    for, and a stream no endpoint delivers to could never be authorized. Each stream's
    section must also be the family its endpoint declares it ``promotes``.
    """
    from nso_adapter.core.intent_protocol import INTENT_PUT_ENDPOINTS

    projection_sections()
    streams = frozenset(_stream_tables())
    endpoints = {e.stream: e.promotes for e in INTENT_PUT_ENDPOINTS.values()}
    if streams != set(endpoints):
        raise RuntimeError(
            f"projection streams and intent endpoints disagree: only-projection "
            f"{sorted(streams - set(endpoints))}, only-endpoints {sorted(set(endpoints) - streams)}"
        )
    mismatched = {s: (sec, endpoints[s]) for s, sec in _stream_section().items() if endpoints[s] != sec}
    if mismatched:
        raise RuntimeError(f"streams whose section differs from what their endpoint promotes: {mismatched}")
    return streams


def stream_section(stream: str) -> str:
    """Return the document section *stream*'s fragment belongs to."""
    if stream not in projection_streams():
        raise ValueError(f"unknown projection stream {stream!r}")
    return _stream_section()[stream]


def stream_for_model(model: type) -> str:
    """Return the stream that OWNS *model*'s table.

    The ownership map read backwards, for the one caller that identifies its write by the
    intent model rather than by the endpoint it arrived on
    (:func:`core.removal.replace_on_removal`).

    A model several sections share — ``RedistributionIntent`` belongs to IS-IS, BGP and OSPF,
    told apart by a discriminator — has no single owner, and picking one would promote an
    unrelated family. Refused, because only the endpoint knows which one it meant.
    """
    projection_streams()
    owners = [stream for stream, specs in _stream_tables().items() if any(spec.model is model for spec in specs)]
    if len(owners) != 1:
        raise ValueError(
            f"{model.__name__} belongs to no projection stream"
            if not owners
            else f"{model.__name__} is shared by streams {sorted(owners)} — name the stream at the call site"
        )
    return owners[0]


def section_streams(section: str) -> tuple[str, ...]:
    """Return every stream that owns part of *section*, sorted.

    The section-to-lanes direction of the ownership map. Nothing PROMOTES at this grain: a
    promotion is an authorization, and only the endpoint a write arrived on authorizes it —
    promoting a whole section carries the sibling lane's un-promoted store-only state (#103).
    Deliberately so: the operator's force-removal reached for this and promoted a family it
    had no write behind, so it now orders a promotion-free reissue instead.
    """
    if section not in projection_sections():
        raise ValueError(f"unknown projection section {section!r}")
    return tuple(sorted(s for s, owner in _stream_section().items() if owner == section))


def _jsonable(value: Any) -> Any:
    """Coerce one column value into something ``json.dumps`` accepts, losslessly.

    ``datetime``/``date`` render ISO-8601 and ``Decimal`` renders its exact string form. A
    lossy coercion (``float(Decimal)``) would make two different documents digest alike.
    """
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        return value.hex()
    return value


def _row_dict(row) -> dict:
    return {attr.key: _jsonable(getattr(row, attr.key)) for attr in sa_inspect(type(row)).column_attrs}


_SPEC_BY_MODEL: dict[type, _Spec] = {spec.model: spec for specs in _SECTION_TABLES.values() for spec in specs}


def _fk_column(model: type, parent: type):
    """Return the column on *model* whose foreign key references *parent*'s table."""
    parent_table = parent.__table__.name
    for column in model.__table__.columns:
        for fk in column.foreign_keys:
            if fk.column.table.name == parent_table:
                return column
    raise RuntimeError(f"{model.__name__} has no foreign key to {parent.__name__}")


def _scope_ids(model: type, device_id: int):
    """Build the subquery of *model*'s ids for *device_id*, walking up to the device-scoped root."""
    if hasattr(model, "device_id"):
        return select(model.id).where(model.device_id == device_id)
    parent = _SPEC_BY_MODEL[model].parent
    if parent is None:  # pragma: no cover — a table with neither device_id nor a parent
        raise RuntimeError(f"{model.__name__} is neither device-scoped nor parented")
    return select(model.id).where(_fk_column(model, parent).in_(_scope_ids(parent, device_id)))


async def _rows_for(db: AsyncSession, device_id: int, spec: _Spec) -> list[dict]:
    model = spec.model
    if spec.parent is None:
        stmt = select(model).where(model.device_id == device_id)
    else:
        stmt = select(model).where(_fk_column(model, spec.parent).in_(_scope_ids(spec.parent, device_id)))
    if spec.discriminator is not None:
        field, value = spec.discriminator
        stmt = stmt.where(getattr(model, field) == value)
    rows = (await db.execute(stmt.order_by(model.id))).scalars().all()
    return [_row_dict(row) for row in rows]


#: Sections whose apply pass is served from the executing generation's stored document
#: rather than from live intent rows (#1522 §G1).
#:
#: Membership is a property of the SECTION, not a switch: a section joins when its whole
#: outbound payload can be rebuilt from :func:`snapshot_sections` alone. Every other section
#: is named in :data:`LIVE_READ_SECTIONS` with the reason it cannot yet, and
#: ``test_projection_document.py`` pins the partition — so a new section cannot drift out of
#: the protocol unnoticed, and closing a reason without wiring the section fails.
DOCUMENT_EXECUTED_SECTIONS: frozenset[str] = frozenset({"vlan"})

#: Why each remaining section still reads live rows at apply time. Most await #1522's
#: aggregate device-intent builder, which is the general producer of a complete document;
#: three need something more specific, named here.
LIVE_READ_SECTIONS: dict[str, str] = {
    "snmp": "the payload resolves Vault refs per row at send time",
    "static_route": "build_plan classifies against tombstones, carriers and deployed keys",
    "logging": "shares the snmp module's per-row secret resolution",
    "svi": "awaits the aggregate builder",
    "subinterface": "awaits the aggregate builder",
    "bfd": "awaits the aggregate builder",
    "interface_mtu": "awaits the aggregate builder",
    "l2_sap": "awaits the aggregate builder",
    "isis": "awaits the aggregate builder",
    "bgp": "the payload walks the router -> scope -> af -> peer relationship graph",
    "route_policy": "awaits the aggregate builder",
    "ospf": "awaits the aggregate builder",
    "interface_config": "eligibility is keyed off InterfaceAttrState, which is not intent",
}

_MODEL_BY_TABLE: dict[str, type] = {spec.model.__tablename__: spec.model for spec in _SPEC_BY_MODEL.values()}


def _from_jsonable(value: Any, column) -> Any:
    """Undo :func:`_jsonable` for one column, so a hydrated row equals the row snapshotted."""
    if value is None:
        return None
    python_type = None
    with suppress(NotImplementedError):
        python_type = column.type.python_type
    if python_type is datetime and isinstance(value, str):
        return datetime.fromisoformat(value)
    if python_type is date and isinstance(value, str):
        return date.fromisoformat(value)
    if python_type is Decimal and isinstance(value, str):
        return Decimal(value)
    if python_type is bytes and isinstance(value, str):
        return bytes.fromhex(value)
    return value


def section_models(sections) -> frozenset[type]:
    """Return every intent model the named *sections* are built from."""
    return frozenset(spec.model for section in sections for spec in _SECTION_TABLES[section])


#: Columns an apply pass WRITES onto an intent row: the deployment's outcome, never the
#: operator's intent. Excluded from :func:`intent_state` so a previous run's own stamps do
#: not read as a successor's edit.
APPLY_BOOKKEEPING_COLUMNS: frozenset[str] = frozenset({"last_apply_at", "last_apply_error"})


def intent_state(row) -> dict:
    """Return the row's INTENT as a comparable mapping — its identity, values and authorization.

    What a deployment must match to be allowed to stamp a live row. ``id`` alone is not that
    match: a successor push rewrites a row IN PLACE, keeping its id, so an older document
    that carried the same id would report the successor's intent — a different name, a newer
    ``accepted_at`` — as applied by a write that never carried it.
    """
    return {key: value for key, value in _row_dict(row).items() if key not in APPLY_BOOKKEEPING_COLUMNS}


def hydrate_section(document: dict, section: str) -> dict[type, list]:
    """Rebuild *section*'s rows from a stored document as TRANSIENT ORM instances.

    Transient on purpose: these carry what the deployment must send, and nothing about them
    may reach the store. The live rows are what an apply stamps — matched back by ``id``,
    which every snapshotted row carries.

    A row that omits the primary key is refused: it would hydrate with ``id`` None, match no
    live row, and let a successful device write report an all-zero bookkeeping outcome.
    """
    tables = document.get(section) or {}
    rows: dict[type, list] = {}
    for table_name, records in tables.items():
        model = _MODEL_BY_TABLE.get(table_name)
        if model is None:
            raise ValueError(f"document section {section!r} names unknown table {table_name!r}")
        columns = {column.key: column for column in model.__table__.columns}
        keys = [column.key for column in model.__table__.primary_key.columns]
        built = []
        for record in records:
            instance = model()
            for key, value in record.items():
                column = columns.get(key)
                if column is None:
                    raise ValueError(f"document row for {table_name!r} names unknown column {key!r}")
                setattr(instance, key, _from_jsonable(value, column))
            if any(record.get(key) is None for key in keys):
                raise ValueError(f"document row for {table_name!r} omits the primary key {keys!r}")
            built.append(instance)
        rows[model] = built
    return rows


async def snapshot_stream(db: AsyncSession, device_id: int, stream: str) -> dict[str, list[dict]]:
    """Serialize the tables *stream* owns into one JSON-safe document FRAGMENT.

    A fragment, not a section: the sibling lane's tables are absent, so folding this over the
    section's other last-authorized fragment is what composes the family's document.

    The caller must already hold the device's projection lock: this reads the very rows the
    generation is promoted from, and a write landing between two of these SELECTs would
    produce a fragment that never existed as a state.
    """
    if stream not in projection_streams():
        raise ValueError(f"unknown projection stream {stream!r}")
    return {spec.model.__tablename__: await _rows_for(db, device_id, spec) for spec in _stream_tables()[stream]}


__all__ = [
    "APPLY_BOOKKEEPING_COLUMNS",
    "DOCUMENT_EXECUTED_SECTIONS",
    "LIVE_READ_SECTIONS",
    "hydrate_section",
    "intent_state",
    "projection_sections",
    "projection_streams",
    "section_streams",
    "snapshot_stream",
    "stream_for_model",
    "stream_section",
]
