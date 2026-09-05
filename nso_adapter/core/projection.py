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
from copy import deepcopy
from datetime import date, datetime
from decimal import Decimal
from functools import cache
from typing import Any, NamedTuple

from sqlalchemy import UniqueConstraint, select
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.attributes import set_committed_value

from nso_adapter.store.models import (
    BfdIntent,
    BgpAfIntent,
    BgpPeerAfIntent,
    BgpPeerIntent,
    BgpRouterIntent,
    BgpScopeIntent,
    DbInterface,
    InterfaceAttrState,
    InterfaceIntent,
    InterfaceIpIntent,
    InterfaceMtuIntent,
    IsisFlexAlgoIntent,
    IsisInterfaceIntent,
    IsisLevelIntent,
    IsisProcessIntent,
    L2SapIntent,
    LagBundleIntent,
    LagMemberIntent,
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
    SwitchportIntent,
    SwitchportTaggedVlanIntent,
    SyncState,
    VlanIntent,
)


class _Spec(NamedTuple):
    """One intent table inside a section, and how its rows reach the device.

    *parent* names the model whose ``id`` this table's foreign key points at; ``None`` means
    the table carries ``device_id`` itself. The foreign-key COLUMN is derived from the
    mapper rather than restated, so a schema change cannot leave a stale column name here.
    *discriminator* is the one case where a table serves several sections at once — a
    redistribution row belongs to the section named by its destination protocol.
    *lifecycle* marks a proof carrier whose disappearance is settlement, not new intent.
    """

    model: Any
    parent: Any | None = None
    discriminator: tuple[str, str] | None = None
    identity: tuple[str, ...] | None = None
    lifecycle: bool = False


_SECTION_TABLES: dict[str, tuple[_Spec, ...]] = {
    "snmp": (
        _Spec(SnmpCommunityIntent),
        _Spec(SnmpV3UserIntent),
        _Spec(SnmpHostIntent),
        _Spec(SnmpSystemInfoIntent),
    ),
    # Tombstones ride the static-route section: an unconsumed one changes which entries the
    # document must retain verbatim, so a document built without them is a different document.
    "static_route": (
        _Spec(StaticRouteIntent),
        _Spec(
            StaticRouteTombstone,
            identity=("route_id", "vrf", "prefix", "next_hop", "marking", "created_at"),
            lifecycle=True,
        ),
    ),
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
    # Prepared by an Apply POST rather than an intent PUT (#1612): no receipt lane, no
    # discriminator and no lifecycle carrier, and every identity comes from the schema.
    "switchport": (_Spec(SwitchportIntent), _Spec(SwitchportTaggedVlanIntent, parent=SwitchportIntent)),
    "lag": (_Spec(LagBundleIntent), _Spec(LagMemberIntent, parent=LagBundleIntent)),
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

    Eighteen streams, sixteen of them endpoint lanes. The two out-of-protocol streams are
    prepared by an Apply POST (:data:`core.intent_protocol.OUT_OF_PROTOCOL_APPLY_POSTS`),
    so the route pin replaces the two endpoint clauses for them: they promote no endpoint
    section, they name a section spelled the same, and they are never split.
    """
    from nso_adapter.core.intent_protocol import INTENT_PUT_ENDPOINTS, OUT_OF_PROTOCOL_STREAMS

    projection_sections()
    streams = frozenset(_stream_tables())
    endpoints = {e.stream: e.promotes for e in INTENT_PUT_ENDPOINTS.values()}
    expected = set(endpoints) | OUT_OF_PROTOCOL_STREAMS
    if streams != expected:
        raise RuntimeError(
            f"projection streams and intent endpoints disagree: only-projection "
            f"{sorted(streams - expected)}, only-endpoints {sorted(expected - streams)}"
        )
    overlap = set(endpoints) & OUT_OF_PROTOCOL_STREAMS
    if overlap:
        raise RuntimeError(f"streams that are both an intent PUT lane and an Apply POST: {sorted(overlap)}")
    split_streams = {stream for split in _SPLIT_SECTION_STREAMS.values() for stream in split}
    for stream in sorted(OUT_OF_PROTOCOL_STREAMS):
        if stream not in _SECTION_TABLES:
            raise RuntimeError(f"out-of-protocol stream {stream!r} names no section spelled the same")
        if stream in split_streams:
            raise RuntimeError(f"out-of-protocol stream {stream!r} may not share a split section")
    mismatched = {
        s: (sec, endpoints[s]) for s, sec in _stream_section().items() if s in endpoints and endpoints[s] != sec
    }
    if mismatched:
        raise RuntimeError(f"streams whose section differs from what their endpoint promotes: {mismatched}")
    for stream_specs in _stream_tables().values():
        for spec in stream_specs:
            _identity_fields(spec)
    return streams


def stream_tables(stream: str) -> tuple[str, ...]:
    """Return the table names *stream* owns, parents before children.

    Derived from the registry, so a writer that has to walk its own parent/child pair
    reads the ownership from one place instead of restating it.
    """
    if stream not in projection_streams():
        raise ValueError(f"unknown projection stream {stream!r}")
    return tuple(spec.model.__tablename__ for spec in _stream_tables()[stream])


def stream_section(stream: str) -> str:
    """Return the document section *stream*'s fragment belongs to."""
    if stream not in projection_streams():
        raise ValueError(f"unknown projection stream {stream!r}")
    return _stream_section()[stream]


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


_VAULT_REFERENCE_COLUMNS: dict[type, frozenset[str]] = {
    SnmpCommunityIntent: frozenset({"vault_ref"}),
    SnmpV3UserIntent: frozenset({"auth_vault_ref", "priv_vault_ref"}),
}


def _document_value(row, key: str) -> Any:
    """Return one durable-document value after enforcing the secret boundary."""
    value = getattr(row, key)
    # '' is the API's absent optional leg (apply_snmp_config skips it) — nothing to parse
    if value is not None and value != "" and key in _VAULT_REFERENCE_COLUMNS.get(type(row), ()):
        from nso_adapter.secrets.refs import VaultRefError, parse_vault_ref

        try:
            parse_vault_ref(value, require_key=True)
        except VaultRefError:
            raise ValueError(
                f"{type(row).__tablename__}.{key}: refusing to serialize non-reference secret material"
            ) from None
    return _jsonable(value)


def _row_dict(row) -> dict:
    return {attr.key: _document_value(row, attr.key) for attr in sa_inspect(type(row)).column_attrs}


_SPEC_BY_MODEL: dict[Any, _Spec] = {spec.model: spec for specs in _SECTION_TABLES.values() for spec in specs}
_SPEC_BY_TABLE: dict[str, _Spec] = {spec.model.__tablename__: spec for spec in _SPEC_BY_MODEL.values()}


def _fk_column(model: Any, parent: Any):
    """Return the column on *model* whose foreign key references *parent*'s table."""
    parent_table = parent.__table__.name
    for column in model.__table__.columns:
        for fk in column.foreign_keys:
            if fk.column.table.name == parent_table:
                return column
    raise RuntimeError(f"{model.__name__} has no foreign key to {parent.__name__}")


@cache
def _identity_fields(spec: _Spec) -> tuple[str, ...]:
    """Return the table's schema-defined logical key, excluding its scope column."""
    if spec.identity is not None:
        return spec.identity
    scope = _fk_column(spec.model, spec.parent).name if spec.parent is not None else "device_id"
    candidates = [
        tuple(column.name for column in constraint.columns)
        for constraint in spec.model.__table__.constraints
        if isinstance(constraint, UniqueConstraint) and scope in constraint.columns
    ]
    candidates.extend(
        (column.name,) for column in spec.model.__table__.columns if column.unique and column.name == scope
    )
    if len(candidates) != 1:
        raise RuntimeError(
            f"{spec.model.__name__} needs one durable projection identity containing {scope!r}, got {candidates}"
        )
    return tuple(field for field in candidates[0] if field != scope)


def _row_identity(spec: _Spec, row: dict, identities_by_id: dict[Any, dict[Any, tuple]]) -> tuple:
    parent_identity: tuple = ()
    if spec.parent is not None:
        fk = _fk_column(spec.model, spec.parent)
        parent_id = row.get(fk.name)
        parent_spec = _SPEC_BY_MODEL.get(spec.parent)
        if parent_spec is None:
            # DbInterface is outside the projection and is not rebuilt by an intent PUT.
            parent_identity = (parent_id,)
        else:
            found = identities_by_id.get(spec.parent, {}).get(parent_id)
            if found is None:
                raise RuntimeError(
                    f"{spec.model.__tablename__} row references missing {spec.parent.__tablename__} id {parent_id!r}"
                )
            parent_identity = found
    return (*parent_identity, *(row.get(field) for field in _identity_fields(spec)))


def _identity_indexes(fragment: dict[str, list[dict]], specs: tuple[_Spec, ...]) -> dict[Any, dict[tuple, dict]]:
    """Index each table once, with parents before children."""
    identities_by_id: dict[Any, dict[Any, tuple]] = {}
    rows_by_identity: dict[type, dict[tuple, dict]] = {}
    for spec in specs:
        indexed: dict[tuple, dict] = {}
        by_id: dict[Any, tuple] = {}
        for row in fragment.get(spec.model.__tablename__, []):
            identity = _row_identity(spec, row, identities_by_id)
            if identity in indexed:
                raise RuntimeError(
                    f"{spec.model.__tablename__} projection contains duplicate durable identity {identity!r}"
                )
            indexed[identity] = row
            by_id[row.get("id")] = identity
        rows_by_identity[spec.model] = indexed
        identities_by_id[spec.model] = by_id
    return rows_by_identity


def _identity_lineage(spec: _Spec) -> tuple[_Spec, ...]:
    """Return the projected ancestors of *spec*, followed by *spec*."""
    lineage = [spec]
    parent = _SPEC_BY_MODEL.get(spec.parent)
    while parent is not None:
        lineage.append(parent)
        parent = _SPEC_BY_MODEL.get(parent.parent)
    return tuple(reversed(lineage))


def rows_by_intent_identity(fragment: dict[str, list[dict]], table: str) -> dict[tuple, dict]:
    """Index one projection table by its durable logical identity.

    Root identities come from the model's unique constraint. Child identities prepend the
    logical parent identity, so full-replace writers can mint new database ids without making
    unchanged BGP scopes, peers, or address families look deleted.

    Each table in the lineage is indexed ONCE per call, parents before children: BGP repeats
    the walk at four levels, so re-deriving a parent per child row is quadratic.
    """
    spec = _SPEC_BY_TABLE.get(table)
    if spec is None:
        raise ValueError(f"unknown projection table {table!r}")
    return _identity_indexes(fragment, _identity_lineage(spec))[spec.model]


def is_intent_deletion(table: str, identity: tuple, desired_rows: dict[tuple, dict]) -> bool:
    """Whether a missing projection row is an operator intent deletion, not lifecycle."""
    spec = _SPEC_BY_TABLE.get(table)
    if spec is None:
        raise ValueError(f"unknown projection table {table!r}")
    return not spec.lifecycle and identity not in desired_rows


def projection_row_state(table: str, row: dict) -> dict:
    """Return the row state that the device-facing renderer consumes."""
    spec = _SPEC_BY_TABLE.get(table)
    if spec is None:
        raise ValueError(f"unknown projection table {table!r}")
    if spec.model is StaticRouteIntent:
        from nso_adapter.nso.apply import static_route_entry

        return static_route_entry(row)
    excluded = {"id", "device_id", "accepted_at", *APPLY_BOOKKEEPING_COLUMNS}
    if spec.parent is not None:
        excluded.add(_fk_column(spec.model, spec.parent).name)
    return {key: value for key, value in row.items() if key not in excluded}


def _scope_ids(model: Any, device_id: int):
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
#: Membership is a property of the SECTION, not a switch. Every outbound payload can now be
#: rebuilt from the stored document. ``test_projection_document.py`` pins the complete set.
DOCUMENT_EXECUTED_SECTIONS: frozenset[str] = frozenset(
    {
        "bgp",
        "vlan",
        "snmp",
        "logging",
        "svi",
        "subinterface",
        "bfd",
        "interface_config",
        "interface_mtu",
        "l2_sap",
        "isis",
        "route_policy",
        "ospf",
        "static_route",
    }
)

#: The manual Apply selection boundary equals the document-executed boundary. Every
#: projection stream now maps to a section that executes from its stored document.
ACTION_APPLY_EXECUTABLE_SECTIONS: frozenset[str] = DOCUMENT_EXECUTED_SECTIONS

#: No section reads live intent to decide what a generation executes.
LIVE_READ_SECTIONS: dict[str, str] = {}

#: The sections that have no device writer yet, so manual Apply refuses to select them and
#: force-removal refuses to address them (#1612). A COMPLETION PIN, not a capability flag:
#: C9's aggregate sender moves both names into :data:`DOCUMENT_EXECUTED_SECTIONS`, empties
#: this set and restores the two equalities the partition test carries.
AWAITING_SENDER_SECTIONS: frozenset[str] = frozenset({"switchport", "lag"})

#: Reserved section key for immutable interface and static-route execution metadata.
EXECUTION_KEY = "_execution"
#: The sections that record execution metadata under :data:`EXECUTION_KEY`. The two
#: switching sections carry the frozen encoding context there (#1612).
EXECUTION_METADATA_SECTIONS: frozenset[str] = frozenset({"interface_config", "static_route", "switchport", "lag"})
INTERFACE_ATTRIBUTE_ELIGIBLE_STATES: frozenset[SyncState] = frozenset(
    {
        SyncState.accepted,
        SyncState.apply_failed,
        SyncState.drifted,
        SyncState.in_sync,
    }
)


class InterfaceEligibilityUnresolved(RuntimeError):
    """Interface intent whose non-intent eligibility state is missing at creation."""


class InterfaceExecution(NamedTuple):
    """Creation-time interface context and the explicit eligible attribute set."""

    interfaces: dict[int, DbInterface]
    eligible_attributes: frozenset[tuple[int, str]]


async def record_interface_execution(db: AsyncSession, device_id: int, document: dict) -> None:
    """Resolve and record all non-intent facts needed to execute interface_config."""
    section = document.get("interface_config")
    if section is None:
        return
    attr_rows = section.get(InterfaceIntent.__tablename__, [])
    ip_rows = section.get(InterfaceIpIntent.__tablename__, [])
    interface_ids = sorted({row["interface_id"] for row in [*attr_rows, *ip_rows]})
    interfaces = (
        (
            await db.execute(
                select(DbInterface)
                .where(DbInterface.device_id == device_id, DbInterface.id.in_(interface_ids))
                .order_by(DbInterface.id)
            )
        )
        .scalars()
        .all()
        if interface_ids
        else []
    )
    by_id = {iface.id: iface for iface in interfaces}
    missing_interfaces = sorted(set(interface_ids) - set(by_id))
    if missing_interfaces:
        raise InterfaceEligibilityUnresolved(f"interface_config references missing interface ids {missing_interfaces}")

    states = (
        (await db.execute(select(InterfaceAttrState).where(InterfaceAttrState.interface_id.in_(interface_ids))))
        .scalars()
        .all()
        if interface_ids
        else []
    )
    state_by_key = {(state.interface_id, state.attribute): state for state in states}
    attr_keys = [(row["interface_id"], row["attribute"]) for row in attr_rows]
    unresolved = sorted(key for key in attr_keys if key not in state_by_key)
    if unresolved:
        raise InterfaceEligibilityUnresolved(
            "interface_config attribute eligibility is missing for "
            + ", ".join(f"interface {interface_id} attribute {attribute!r}" for interface_id, attribute in unresolved)
        )
    eligible = [
        {"interface_id": interface_id, "attribute": attribute}
        for interface_id, attribute in sorted(attr_keys)
        if state_by_key[(interface_id, attribute)].sync_state in INTERFACE_ATTRIBUTE_ELIGIBLE_STATES
    ]
    section[EXECUTION_KEY] = {
        "interfaces": [
            {
                "id": iface.id,
                "name": iface.name,
                "kind": iface.kind,
                "parent_binding": iface.parent_binding,
                "encap_tag": iface.encap_tag,
                "vrf": iface.vrf,
                "service": iface.service,
            }
            for iface in interfaces
        ],
        "eligible_interface_attributes": eligible,
    }


def hydrate_interface_execution(document: dict) -> InterfaceExecution:
    """Rebuild interface writer context and eligibility from the stored document."""
    section = document.get("interface_config") or {}
    execution = section.get(EXECUTION_KEY)
    if execution is None:
        raise ValueError("document section 'interface_config' has no recorded execution context")
    if not isinstance(execution, dict) or set(execution) != {
        "interfaces",
        "eligible_interface_attributes",
    }:
        raise ValueError("document section 'interface_config' has invalid execution context")
    serialized_interfaces = execution.get("interfaces")
    serialized_eligible = execution.get("eligible_interface_attributes")
    if not isinstance(serialized_interfaces, list) or not isinstance(serialized_eligible, list):
        raise ValueError("document section 'interface_config' has invalid execution context")
    interfaces: dict[int, DbInterface] = {}
    allowed = {"id", "name", "kind", "parent_binding", "encap_tag", "vrf", "service"}
    for record in serialized_interfaces:
        if not isinstance(record, dict) or set(record) != allowed:
            raise ValueError("document section 'interface_config' has invalid interface context")
        iface = DbInterface(**record)
        if iface.id in interfaces:
            raise ValueError(f"document section 'interface_config' repeats interface id {iface.id}")
        interfaces[iface.id] = iface
    eligible: set[tuple[int, str]] = set()
    for record in serialized_eligible:
        if not isinstance(record, dict) or set(record) != {"interface_id", "attribute"}:
            raise ValueError("document section 'interface_config' has invalid eligible attribute")
        key = (record["interface_id"], record["attribute"])
        if key in eligible:
            raise ValueError(f"document section 'interface_config' repeats eligible attribute {key!r}")
        eligible.add(key)
    referenced_interfaces = {
        row["interface_id"]
        for table in (InterfaceIntent.__tablename__, InterfaceIpIntent.__tablename__)
        for row in section.get(table, [])
    }
    if set(interfaces) != referenced_interfaces:
        raise ValueError("document section 'interface_config' execution context does not match its rows")
    attribute_keys = {(row["interface_id"], row["attribute"]) for row in section.get(InterfaceIntent.__tablename__, [])}
    if not eligible <= attribute_keys:
        raise ValueError("document section 'interface_config' eligibility does not match its attribute rows")
    return InterfaceExecution(interfaces, frozenset(eligible))


_MODEL_BY_TABLE: dict[str, Any] = {spec.model.__tablename__: spec.model for spec in _SPEC_BY_MODEL.values()}


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
    models: set[type] = set()
    for section in sections:
        if section not in projection_sections():
            raise ValueError(f"unknown projection section {section!r}")
        models.update(spec.model for spec in _SECTION_TABLES[section])
    return frozenset(models)


#: Columns the apply side MUTATES on an intent row: deployment state, never the operator's
#: intent. Excluded from every intent comparison so a previous run's settlement does not
#: read as a successor's edit.
APPLY_BOOKKEEPING_COLUMNS: frozenset[str] = frozenset(
    {"last_apply_at", "last_apply_error", "pending_clear", "deployed_key"}
)


def intent_state(row) -> dict:
    """Return the row's INTENT as a comparable mapping — its identity, values and authorization.

    What a deployment must match to be allowed to stamp a live row. ``id`` alone is not that
    match: a successor push rewrites a row IN PLACE, keeping its id, so an older document
    that carried the same id would report the successor's intent — a different name, a newer
    ``accepted_at`` — as applied by a write that never carried it.
    """
    return {key: value for key, value in _row_dict(row).items() if key not in APPLY_BOOKKEEPING_COLUMNS}


@cache
def _collection_relationship(parent: Any, child: Any) -> str:
    """Return the parent's one collection relationship to *child*."""
    relationships = [
        relation.key
        for relation in sa_inspect(parent).relationships
        if relation.uselist and relation.mapper.class_ is child
    ]
    if len(relationships) != 1:
        raise RuntimeError(
            f"{parent.__name__} needs one collection relationship to {child.__name__}, got {relationships}"
        )
    return relationships[0]


def _attach_hydrated_relationships(
    fragment: dict[str, list[dict]], section: str, records: dict[Any, list[tuple[dict, object]]]
) -> None:
    """Rebuild in-document parent collections from durable logical identities."""
    section_specs = _SECTION_TABLES[section]
    relationship_models = {
        model for spec in section_specs if spec.parent in _SPEC_BY_MODEL for model in (spec.parent, spec.model)
    }
    indexes = _identity_indexes(fragment, tuple(spec for spec in section_specs if spec.model in relationship_models))
    for spec in section_specs:
        parent_spec = _SPEC_BY_MODEL.get(spec.parent)
        if parent_spec is None:
            continue
        parent_pairs = records.get(spec.parent, [])
        child_pairs = records.get(spec.model, [])
        instance_by_record = {id(record): instance for record, instance in parent_pairs}
        parents = {identity: instance_by_record[id(record)] for identity, record in indexes[spec.parent].items()}
        grouped: dict[tuple, list] = {identity: [] for identity in parents}
        child_instance_by_record = {id(record): instance for record, instance in child_pairs}
        local_identity_size = len(_identity_fields(spec))
        if local_identity_size == 0:
            # identity[:-0] is the EMPTY tuple, so every child row would report a missing
            # parent. No table has this shape today; name the real cause if one gains it.
            raise RuntimeError(
                f"{spec.model.__tablename__} has no local durable identity, so its parent identity "
                "cannot be derived by prefix"
            )
        for identity, record in indexes[spec.model].items():
            parent_identity = identity[:-local_identity_size]
            if parent_identity not in grouped:
                raise RuntimeError(
                    f"{spec.model.__tablename__} row references missing durable parent identity {parent_identity!r}"
                )
            grouped[parent_identity].append(child_instance_by_record[id(record)])
        relationship = _collection_relationship(spec.parent, spec.model)
        for identity, parent in parents.items():
            set_committed_value(parent, relationship, grouped[identity])


def hydrate_section(document: dict, section: str) -> dict[type, list]:
    """Rebuild *section*'s rows from a stored document as TRANSIENT ORM instances.

    Transient on purpose: these carry what the deployment must send, and nothing about them
    may reach the store. The live rows are what an apply stamps — matched back by ``id``,
    which every snapshotted row carries.

    A row that omits the primary key is refused: it would hydrate with ``id`` None, match no
    live row, and let a successful device write report an all-zero bookkeeping outcome.
    """
    if section not in document:
        raise ValueError(f"document does not carry section {section!r}")
    tables = document[section] or {}
    allowed_models = {spec.model for spec in _SECTION_TABLES[section]}
    rows: dict[type, list] = {}
    row_records: dict[type, list[tuple[dict, object]]] = {}
    for table_name, serialized_rows in tables.items():
        if table_name == EXECUTION_KEY and section in EXECUTION_METADATA_SECTIONS:
            continue
        model = _MODEL_BY_TABLE.get(table_name)
        if model is None:
            raise ValueError(f"document section {section!r} names unknown table {table_name!r}")
        if model not in allowed_models:
            raise ValueError(f"document table {table_name!r} does not belong to section {section!r}")
        columns = {column.key: column for column in model.__table__.columns}
        keys = [column.key for column in model.__table__.primary_key.columns]
        built = []
        model_records = []
        for record in serialized_rows:
            instance = model()
            for key, value in record.items():
                column = columns.get(key)
                if column is None:
                    raise ValueError(f"document row for {table_name!r} names unknown column {key!r}")
                setattr(instance, key, _from_jsonable(value, column))
            if any(record.get(key) is None for key in keys):
                raise ValueError(f"document row for {table_name!r} omits the primary key {keys!r}")
            built.append(instance)
            model_records.append((record, instance))
        rows[model] = built
        row_records[model] = model_records
    _attach_hydrated_relationships(tables, section, row_records)
    return rows


def fragment_tables(fragment: dict | None) -> dict[str, list[dict]]:
    """Return a fragment's TABLES, without its execution metadata."""
    return {table: rows for table, rows in (fragment or {}).items() if table != EXECUTION_KEY}


def fragment_context(fragment: dict | None) -> dict | None:
    """Return the encoding context a fragment was frozen with, or ``None`` for an unfrozen one."""
    return ((fragment or {}).get(EXECUTION_KEY) or {}).get("context")


def freeze_fragment(tables: dict[str, list[dict]], device) -> dict:
    """Return the FRAGMENT for *tables*: the rows plus the state they must execute with.

    The encoding context is read HERE, in the authorizing transaction, and preserved
    verbatim: ``ned_id`` keeps an explicit null (a device with no NED id records ``null``
    and dialect ``identity``), and ``dialect`` is the stable name of the dialect that NED id
    resolves to, so an encode never re-reads the device row.
    """
    from nso_adapter.core.community_dialect import community_dialect_for

    return {
        **deepcopy(tables),
        EXECUTION_KEY: {
            "context": {"ned_id": device.ned_id, "dialect": community_dialect_for(device.ned_id).name},
        },
    }


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
    "ACTION_APPLY_EXECUTABLE_SECTIONS",
    "APPLY_BOOKKEEPING_COLUMNS",
    "AWAITING_SENDER_SECTIONS",
    "DOCUMENT_EXECUTED_SECTIONS",
    "INTERFACE_ATTRIBUTE_ELIGIBLE_STATES",
    "EXECUTION_KEY",
    "InterfaceEligibilityUnresolved",
    "LIVE_READ_SECTIONS",
    "fragment_context",
    "fragment_tables",
    "freeze_fragment",
    "hydrate_section",
    "hydrate_interface_execution",
    "intent_state",
    "is_intent_deletion",
    "projection_sections",
    "projection_row_state",
    "projection_streams",
    "rows_by_intent_identity",
    "section_models",
    "section_streams",
    "record_interface_execution",
    "snapshot_stream",
    "stream_section",
    "stream_tables",
]
