# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The projection: which sections exist, and what each one's desired state IS (#1522 §G1).

Two vocabularies, deliberately different sizes.

A *section* is one family of the device's outbound DOCUMENT — the same vocabulary the
removal scopes already use, so there is one name for ``isis`` and not two. The registry here
IS that vocabulary: a removal scope is one of these families emptied, so there is no second
list for the removal path to drift from.

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

from collections.abc import Callable, Mapping
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

from nso_adapter.nso.apply import (
    INTERFACE_ATTRIBUTE_LEAVES,
    encode_bfd,
    encode_bgp,
    encode_interface_config,
    encode_interface_mtu,
    encode_isis,
    encode_l2_sap,
    encode_lag,
    encode_logging,
    encode_ospf,
    encode_route_policy,
    encode_snmp,
    encode_static_route,
    encode_subinterface,
    encode_svi,
    encode_switchport,
    encode_vlan,
)
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


class TableCompare(NamedTuple):
    """Verify by comparing the section's keyed rows against the reader's own lists.

    Each entry is ``(model, the reader list's label, row -> key tuple)``: the intended
    objects a post-apply read must find, in the reader's own key grain.
    """

    entries: tuple[tuple[Any, str, Callable[[Any], tuple]], ...]


class BespokeExpansion(NamedTuple):
    """Verify through the section's own expected-key expansion (bgp, route_policy)."""


class NoComparison(NamedTuple):
    """Residue only. The section has no post-apply key comparison at C9."""


#: The three verification dispositions, as singletons where they carry no data.
BESPOKE_EXPANSION = BespokeExpansion()
NO_COMPARISON = NoComparison()

Verification = TableCompare | BespokeExpansion | NoComparison


class GuardList(NamedTuple):
    """A guarded list, its authority label, and its parent-qualified wire identity."""

    label: str
    path: tuple[str, ...]
    keys: tuple[str, ...]
    parent_key: str | None = None
    scalar: bool = False
    presence: bool = False


class _Section(NamedTuple):
    """One document section: its intent tables plus everything the write side needs.

    The ONE write-side family table (#1522 memo A8). Every derived consumer — the sender,
    failure localisation, refusal attribution, capability recording, job results, reader
    comparison and the device-wide residue check — reads this record instead of keeping a
    hand-agreed copy of its own.

    *container* is the YANG container under ``list device-intent``; it is stated rather
    than derived because three sections do not spell it the way they spell themselves
    (``interface_config`` is ``interface``, ``interface_mtu`` is ``mtu``, ``l2_sap`` is
    ``l2-sap``). *read_family* is stated for the same reason: ``l2_sap`` reads
    ``l2_service``, ``interface_config`` reads ``interface_ip``, ``lag`` reads
    ``lag_config``. *capability_scopes* and *result_keys* default to the section's own
    name; ``interface_config`` is the one section that records under two of each.
    """

    tables: tuple[_Spec, ...]
    container: str
    encode: Callable[[Mapping[str, list[Any]], Any], dict]
    read_family: str
    verify: Verification
    guard_lists: tuple[GuardList, ...] = ()
    capability_scopes: tuple[str, ...] = ()
    result_keys: tuple[str, ...] = ()


#: Iteration order is the order a device's document is built and its job results are
#: named in. It is deliberately NOT coupled to the packages' plan order: a planner
#: reorder must not need an adapter release.
_SECTION_REGISTRY: dict[str, _Section] = {
    "snmp": _Section(
        tables=(
            _Spec(SnmpCommunityIntent),
            _Spec(SnmpV3UserIntent),
            _Spec(SnmpHostIntent),
            _Spec(SnmpSystemInfoIntent),
        ),
        guard_lists=(
            GuardList("community", ("community",), ("name",)),
            GuardList("v3-user", ("v3-user",), ("username",)),
            GuardList("host", ("host",), ("address",)),
        ),
        container="snmp",
        encode=encode_snmp,
        read_family="snmp",
        # A community's intent key is its label and the export keys it by a digest of the
        # secret; _translate_expected re-keys what Vault can answer for.
        verify=TableCompare(
            (
                (SnmpCommunityIntent, "community", lambda r: (r.label,)),
                (SnmpV3UserIntent, "v3-user", lambda r: (r.username,)),
                (SnmpHostIntent, "host", lambda r: (r.address,)),
            )
        ),
    ),
    # Tombstones ride the static-route section: an unconsumed one changes which entries the
    # document must retain verbatim, so a document built without them is a different document.
    "static_route": _Section(
        tables=(
            _Spec(StaticRouteIntent),
            _Spec(
                StaticRouteTombstone,
                identity=("route_id", "vrf", "prefix", "next_hop", "marking", "created_at"),
                lifecycle=True,
            ),
        ),
        guard_lists=(GuardList("route", ("route",), ("vrf", "prefix", "next-hop")),),
        container="static-route",
        encode=encode_static_route,
        read_family="static_route",
        verify=TableCompare(((StaticRouteIntent, "route", lambda r: (r.vrf, r.prefix, r.next_hop)),)),
    ),
    "logging": _Section(
        tables=(_Spec(LoggingHostIntent), _Spec(LoggingLevelsIntent)),
        guard_lists=(GuardList("host", ("host",), ("address",)),),
        container="logging",
        encode=encode_logging,
        read_family="logging",
        verify=TableCompare(((LoggingHostIntent, "host", lambda r: (r.address,)),)),
    ),
    "svi": _Section(
        tables=(_Spec(SviIntent),),
        guard_lists=(GuardList("interface", ("interface",), ("interface-name",)),),
        container="svi",
        encode=encode_svi,
        read_family="svi",
        verify=TableCompare(((SviIntent, "interface", lambda r: (r.interface_name,)),)),
    ),
    "subinterface": _Section(
        tables=(_Spec(SubinterfaceIntent),),
        guard_lists=(GuardList("interface", ("interface",), ("interface-name",)),),
        container="subinterface",
        encode=encode_subinterface,
        read_family="subinterface",
        verify=TableCompare(((SubinterfaceIntent, "interface", lambda r: (r.interface_name,)),)),
    ),
    "vlan": _Section(
        tables=(_Spec(VlanIntent),),
        guard_lists=(GuardList("vlan", ("vlan",), ("vlan-id",)),),
        container="vlan",
        encode=encode_vlan,
        read_family="vlan",
        verify=TableCompare(((VlanIntent, "vlan", lambda r: (r.vlan_id,)),)),
    ),
    "bfd": _Section(
        tables=(_Spec(BfdIntent),),
        guard_lists=(GuardList("interface", ("interface",), ("interface-name",)),),
        container="bfd",
        encode=encode_bfd,
        read_family="bfd",
        verify=TableCompare(((BfdIntent, "interface", lambda r: (r.interface_name,)),)),
    ),
    "interface_mtu": _Section(
        tables=(_Spec(InterfaceMtuIntent),),
        guard_lists=(GuardList("interface", ("interface",), ("interface-name",)),),
        container="mtu",
        encode=encode_interface_mtu,
        read_family="interface_mtu",
        verify=TableCompare(((InterfaceMtuIntent, "interface", lambda r: (r.interface_name,)),)),
    ),
    "l2_sap": _Section(
        tables=(_Spec(L2SapIntent),),
        guard_lists=(GuardList("sap", ("sap",), ("service-name", "sap-id")),),
        container="l2-sap",
        encode=encode_l2_sap,
        read_family="l2_service",
        verify=TableCompare(((L2SapIntent, "sap", lambda r: (r.service_name, r.sap_id)),)),
    ),
    "isis": _Section(
        tables=(
            _Spec(IsisProcessIntent),
            _Spec(IsisInterfaceIntent),
            _Spec(IsisLevelIntent),
            _Spec(IsisFlexAlgoIntent),
            _Spec(RedistributionIntent, discriminator=("dest_protocol", "isis")),
        ),
        guard_lists=(
            GuardList("interface-config", ("interface-config",), ("interface-name", "af")),
            GuardList("process-config", ("process-config",), ("process-tag",)),
        ),
        container="isis",
        encode=encode_isis,
        read_family="isis",
        verify=TableCompare(
            (
                (IsisInterfaceIntent, "interface-config", lambda r: (r.interface_name, r.af)),
                (IsisProcessIntent, "process-config", lambda r: (r.process_tag,)),
            )
        ),
    ),
    "bgp": _Section(
        tables=(
            _Spec(BgpRouterIntent),
            _Spec(BgpScopeIntent, parent=BgpRouterIntent),
            _Spec(BgpAfIntent, parent=BgpScopeIntent),
            _Spec(BgpPeerIntent, parent=BgpScopeIntent),
            _Spec(BgpPeerAfIntent, parent=BgpPeerIntent),
            _Spec(RedistributionIntent, discriminator=("dest_protocol", "bgp")),
        ),
        guard_lists=(
            GuardList("router", ("router",), ("asn",)),
            # device-wide flatten: the trigger can only produce peer addresses across all
            # routers/scopes, so the guard compares at the same grain
            GuardList("peer", ("router", "scope", "peer"), ("peer-address",)),
        ),
        container="bgp",
        encode=encode_bgp,
        read_family="bgp",
        verify=BESPOKE_EXPANSION,
    ),
    "route_policy": _Section(
        tables=(_Spec(RoutePolicyObjectIntent),),
        guard_lists=(
            GuardList("prefix-list", ("prefix-list",), ("name",)),
            GuardList("community-list", ("community-list",), ("name",)),
            GuardList("as-path", ("as-path",), ("name",)),
            GuardList("route-map", ("route-map",), ("name",)),
        ),
        container="route-policy",
        encode=encode_route_policy,
        read_family="route_policy",
        verify=BESPOKE_EXPANSION,
    ),
    "ospf": _Section(
        tables=(
            _Spec(OspfInstanceIntent),
            _Spec(OspfInterfaceIntent),
            _Spec(RedistributionIntent, discriminator=("dest_protocol", "ospf")),
        ),
        guard_lists=(
            GuardList("interface-config", ("interface-config",), ("interface-name",)),
            GuardList("process-config", ("process-config",), ("process-id",)),
        ),
        container="ospf",
        encode=encode_ospf,
        read_family="ospf",
        verify=TableCompare(
            (
                (OspfInstanceIntent, "process-config", lambda r: (r.process_id,)),
                (OspfInterfaceIntent, "interface-config", lambda r: (r.interface_name,)),
            )
        ),
    ),
    # Prepared by an Apply POST rather than an intent PUT (#1612): no receipt lane, no
    # discriminator and no lifecycle carrier, and every identity comes from the schema.
    "switchport": _Section(
        tables=(_Spec(SwitchportIntent), _Spec(SwitchportTaggedVlanIntent, parent=SwitchportIntent)),
        guard_lists=(
            GuardList(SwitchportIntent.__tablename__, ("interface",), ("interface-name",)),
            GuardList(
                SwitchportTaggedVlanIntent.__tablename__, ("interface", "tagged-vlan"), (), "interface-name", True
            ),
        ),
        container="switchport",
        encode=encode_switchport,
        read_family="switchport",
        verify=NO_COMPARISON,
    ),
    "lag": _Section(
        tables=(_Spec(LagBundleIntent, identity=("name",)), _Spec(LagMemberIntent, parent=LagBundleIntent)),
        guard_lists=(
            GuardList(LagBundleIntent.__tablename__, ("bundle",), ("name",)),
            GuardList(LagMemberIntent.__tablename__, ("bundle", "member"), ("interface-name",), "name"),
        ),
        container="lag",
        encode=encode_lag,
        read_family="lag_config",
        verify=NO_COMPARISON,
    ),
    # The residue check covers retracted addresses only; a dropped new address or
    # description is not detected, and adding that comparison is a follow-up, not C9.
    "interface_config": _Section(
        tables=(
            _Spec(InterfaceIntent, parent=DbInterface),
            _Spec(InterfaceIpIntent, parent=DbInterface),
        ),
        # Exempt from the post-apply key comparison, never from the device-wide collateral
        # guard: an omitted root or address is a retraction like any other section's.
        guard_lists=(
            GuardList("interface", ("interface",), ("interface-name",)),
            GuardList("ipv4-address", ("interface", "ipv4-address"), ("address",), "interface-name"),
            GuardList("ipv6-address", ("interface", "ipv6-address"), ("address",), "interface-name"),
            *(
                GuardList(attribute, ("interface", attribute), (), "interface-name", presence=True)
                for attribute in sorted(INTERFACE_ATTRIBUTE_LEAVES)
            ),
        ),
        container="interface",
        encode=encode_interface_config,
        read_family="interface_ip",
        verify=NO_COMPARISON,
        capability_scopes=("interface_attribute", "interface_ip"),
        result_keys=("attribute", "ip"),
    ),
}


@cache
def section_registry() -> dict[str, _Section]:
    """Return the write-side section registry, with every default resolved.

    ``capability_scopes`` and ``result_keys`` default to the section's own name, so only
    the section that departs from that states it.
    """
    return {
        section: entry._replace(
            capability_scopes=entry.capability_scopes or (section,),
            result_keys=entry.result_keys or (section,),
        )
        for section, entry in _SECTION_REGISTRY.items()
    }


class _SplitSection(NamedTuple):
    """A section written by more than one stream: who owns its context, and who owns what.

    *context_owner* is the stream whose fragment supplies the composed section's encoding
    context. Owner-wins is right for the context, which is a property of the SECTION, and
    it is the one always-available way to move a section to a new NED: reauthorize the
    owner. Row-grain proof does NOT follow it — see :func:`compose_section_execution`.
    """

    context_owner: str
    streams: dict[str, tuple[type, ...]]


#: The sections whose tables are owned by MORE THAN ONE endpoint stream, and which tables
#: each stream owns. Every section absent from here has exactly one stream, spelled the same.
#:
#: Ownership is stated table by table because it is what an authorization covers: the ``ip``
#: endpoint authorizes addresses and nothing else, so a normal ``ip`` push must not carry the
#: interface ATTRIBUTES a store-only repair left in the store (#103). The two halves are
#: checked against the section's own table list below, so a table added to a split section
#: fails loudly instead of falling into neither lane.
_SPLIT_SECTION_STREAMS: dict[str, _SplitSection] = {
    "interface_config": _SplitSection(
        context_owner="interface_config",
        streams={
            "interface_config": (InterfaceIntent,),
            "ip": (InterfaceIpIntent,),
        },
    ),
    "isis": _SplitSection(
        context_owner="isis",
        streams={
            "isis": (IsisProcessIntent, IsisInterfaceIntent, IsisLevelIntent, RedistributionIntent),
            "isis_flex_algo": (IsisFlexAlgoIntent,),
        },
    ),
}


@cache
def _stream_tables() -> dict[str, tuple[_Spec, ...]]:
    """Stream name -> the intent tables it owns. Built once, validated on the way."""
    specs: dict[str, tuple[_Spec, ...]] = {}
    for section, entry in _SECTION_REGISTRY.items():
        section_specs = entry.tables
        split = _SPLIT_SECTION_STREAMS.get(section)
        if split is None:
            specs[section] = section_specs
            continue
        by_model = {spec.model: spec for spec in section_specs}
        claimed: list[type] = [model for models in split.streams.values() for model in models]
        if sorted(m.__name__ for m in claimed) != sorted(m.__name__ for m in by_model):
            raise RuntimeError(
                f"section {section!r} stream ownership does not partition its tables: "
                f"claimed {sorted(m.__name__ for m in claimed)} vs {sorted(m.__name__ for m in by_model)}"
            )
        if split.context_owner not in split.streams:
            raise RuntimeError(
                f"section {section!r} names context owner {split.context_owner!r}, which is not one of its "
                f"streams {sorted(split.streams)}"
            )
        for stream, models in split.streams.items():
            specs[stream] = tuple(by_model[model] for model in models)
    return specs


@cache
def _stream_section() -> dict[str, str]:
    """Stream name -> the document section it contributes a fragment to."""
    owner = {stream: section for section, split in _SPLIT_SECTION_STREAMS.items() for stream in split.streams}
    return {stream: owner.get(stream, stream) for stream in _stream_tables()}


def _spec_or_refuse(table: str) -> _Spec:
    """Return a table's registry entry, or name the table that is not in it.

    Four call sites need the same refusal, and a bare ``KeyError`` from indexing reaches the
    generic job-failure handler naming nothing an operator can act on.
    """
    spec = _SPEC_BY_TABLE.get(table)
    if spec is None:
        raise ValueError(f"unknown projection table {table!r}")
    return spec


@cache
def projection_sections() -> frozenset[str]:
    """Every section name a stored DOCUMENT can carry — the outbound device families.

    The registry IS the vocabulary: a section is a family the aggregate document can carry,
    and a removal scope is such a family emptied — there is no second list to agree with.
    One direction is still checked here, at the first caller's import rather than as an empty
    document later: every in-protocol intent endpoint promotes one of these, so an endpoint
    naming a family with no tables would bump a revision nothing can ever deploy.

    This is NOT the promotion vocabulary — see :func:`projection_streams`.
    """
    from nso_adapter.core.intent_protocol import INTENT_PUT_ENDPOINTS

    unpromotable = {e.promotes for e in INTENT_PUT_ENDPOINTS.values()} - set(_SECTION_REGISTRY)
    if unpromotable:
        raise RuntimeError(f"intent endpoints promote sections with no intent tables: {sorted(unpromotable)}")
    return frozenset(_SECTION_REGISTRY)


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
    split_streams = {stream for split in _SPLIT_SECTION_STREAMS.values() for stream in split.streams}
    for stream in sorted(OUT_OF_PROTOCOL_STREAMS):
        if stream not in _SECTION_REGISTRY:
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
    _validate_section_registry(OUT_OF_PROTOCOL_STREAMS)
    return streams


def _validate_section_registry(out_of_protocol: frozenset[str]) -> None:
    """Refuse a registry a derived consumer could not read. Called from startup.

    Every clause fails the process at boot rather than on the first request that reaches
    the broken section: an unnamed container is a family the aggregate cannot carry, a
    duplicate container is two families writing one place, and an unregistered read family
    is a residue check with no reader list to look in.
    """
    from nso_adapter.core.families import ENGINE_FAMILY_KEYS

    containers: dict[str, str] = {}
    for section, entry in section_registry().items():
        if not entry.container:
            raise RuntimeError(f"section {section!r} names no device-intent container")
        if not callable(entry.encode):
            raise RuntimeError(f"section {section!r} has no wire encoder")
        if not isinstance(entry.verify, TableCompare | BespokeExpansion | NoComparison):
            raise RuntimeError(f"section {section!r} has no verification disposition")
        if not entry.capability_scopes:
            raise RuntimeError(f"section {section!r} records under no capability scope")
        if not entry.result_keys:
            raise RuntimeError(f"section {section!r} names no job-result counter")
        if entry.read_family not in ENGINE_FAMILY_KEYS:
            raise RuntimeError(f"section {section!r} reads through unregistered family {entry.read_family!r}")
        owner = containers.setdefault(entry.container, section)
        if owner != section:
            raise RuntimeError(f"sections {owner!r} and {section!r} both claim container {entry.container!r}")
    if out_of_protocol != CLAIM_LESS_SECTIONS:
        raise RuntimeError(
            f"the out-of-protocol streams {sorted(out_of_protocol)} are not the two claim-less "
            f"sections {sorted(CLAIM_LESS_SECTIONS)}"
        )


def stream_tables(stream: str) -> tuple[str, ...]:
    """Return the table names *stream* owns, parents before children.

    Derived from the registry, so a writer that has to walk its own parent/child pair
    reads the ownership from one place instead of restating it.
    """
    if stream not in projection_streams():
        raise ValueError(f"unknown projection stream {stream!r}")
    return tuple(spec.model.__tablename__ for spec in _stream_tables()[stream])


def section_container(section: str) -> str:
    """Return the YANG container *section* is carried under inside ``list device-intent``."""
    if section not in projection_sections():
        raise ValueError(f"unknown projection section {section!r}")
    return section_registry()[section].container


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
            malformed = True
        else:
            malformed = False
        # Raised OUTSIDE the except block: a raise inside it attaches the parser exception as
        # __context__, whose repr echoes the very material this refusal exists to withhold.
        if malformed:
            raise ValueError(f"{type(row).__tablename__}.{key}: refusing to serialize non-reference secret material")
    return _jsonable(value)


def _row_dict(row) -> dict:
    return {attr.key: _document_value(row, attr.key) for attr in sa_inspect(type(row)).column_attrs}


_SPEC_BY_MODEL: dict[Any, _Spec] = {spec.model: spec for entry in _SECTION_REGISTRY.values() for spec in entry.tables}
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
    spec = _spec_or_refuse(table)
    return _identity_indexes(fragment, _identity_lineage(spec))[spec.model]


def is_intent_deletion(table: str, identity: tuple, desired_rows: dict[tuple, dict]) -> bool:
    """Whether a missing projection row is an operator intent deletion, not lifecycle."""
    return not _spec_or_refuse(table).lifecycle and identity not in desired_rows


def projection_row_state(table: str, row: dict) -> dict:
    """Return the row state that the device-facing renderer consumes."""
    spec = _spec_or_refuse(table)
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
#: DERIVED from the registry, which is what document execution iterates: a second hand-kept
#: list could only ever disagree with it.
DOCUMENT_EXECUTED_SECTIONS: frozenset[str] = frozenset(_SECTION_REGISTRY)

#: The manual Apply selection boundary equals the document-executed boundary. Every
#: projection stream now maps to a section that executes from its stored document.
ACTION_APPLY_EXECUTABLE_SECTIONS: frozenset[str] = DOCUMENT_EXECUTED_SECTIONS

#: No section reads live intent to decide what a generation executes.
LIVE_READ_SECTIONS: dict[str, str] = {}

#: The two sections a claim-less Apply POST prepares instead of an intent PUT (#1612).
#: Startup pins the out-of-protocol stream set against this, so a third such stream cannot
#: appear without the registry changing with it.
CLAIM_LESS_SECTIONS: frozenset[str] = frozenset({"switchport", "lag"})

#: Reserved section key for a section's execution metadata: its frozen encoding context,
#: the proof its own rows were authorized with, and the operation plane one generation
#: writes over them. Never a table, and never transmitted.
EXECUTION_KEY = "_execution"
INTERFACE_ATTRIBUTE_ELIGIBLE_STATES: frozenset[SyncState] = frozenset(
    {
        SyncState.accepted,
        SyncState.apply_failed,
        SyncState.drifted,
        SyncState.in_sync,
    }
)

#: The seven interface fields the interface writer reads besides the intent rows.
_INTERFACE_PROOF_FIELDS = ("id", "name", "kind", "parent_binding", "encap_tag", "vrf", "service")


class InterfaceEligibilityUnresolved(RuntimeError):
    """Interface intent whose non-intent eligibility state is missing at authorization."""


class InterfaceExecution(NamedTuple):
    """Creation-time interface context and the explicit eligible attribute set."""

    interfaces: dict[int, DbInterface]
    eligible_attributes: frozenset[tuple[int, str]]


def _attribute_eligibility_key(interface_id: int, attribute: str) -> str:
    return f"{interface_id}/{attribute}"


def build_interface_proof(stream: str, tables: dict[str, list[dict]], interfaces, decisions: dict) -> dict:
    """Build the interface section's proof from rows and already-resolved non-intent facts.

    Pure, so the runtime freeze and the one-shot migration that stamps pre-contract fragments
    produce the same bytes from the same inputs. *decisions* maps ``(interface_id, attribute)``
    to the eligibility verdict; every attribute row of *tables* must have one.
    """
    attr_rows = tables.get(InterfaceIntent.__tablename__, [])
    ip_rows = tables.get(InterfaceIpIntent.__tablename__, [])
    interface_ids = sorted({row["interface_id"] for row in [*attr_rows, *ip_rows]})
    by_id = {iface.id: iface for iface in interfaces}
    missing_interfaces = sorted(set(interface_ids) - set(by_id))
    if missing_interfaces:
        raise InterfaceEligibilityUnresolved(f"interface_config references missing interface ids {missing_interfaces}")
    proof: dict = {
        "interfaces": {
            str(interface_id): {field: getattr(by_id[interface_id], field) for field in _INTERFACE_PROOF_FIELDS}
            for interface_id in interface_ids
        }
    }
    if not attr_rows and stream != "interface_config":
        return proof
    attr_keys = sorted({(row["interface_id"], row["attribute"]) for row in attr_rows})
    unresolved = [key for key in attr_keys if key not in decisions]
    if unresolved:
        raise InterfaceEligibilityUnresolved(
            "interface_config attribute eligibility is missing for "
            + ", ".join(f"interface {interface_id} attribute {attribute!r}" for interface_id, attribute in unresolved)
        )
    # EVERY attribute row gets an explicit decision, ``false`` included: a decision that is
    # merely absent cannot be told apart from one nobody made.
    proof["attribute_eligibility"] = {
        _attribute_eligibility_key(interface_id, attribute): decisions[(interface_id, attribute)]
        for interface_id, attribute in attr_keys
    }
    return proof


async def _freeze_interface_proof(db: AsyncSession, device_id: int, stream: str, tables: dict[str, list[dict]]) -> dict:
    """Resolve the live non-intent facts the interface section's own rows need.

    The ``ip`` stream contributes ``interfaces`` only: address rows carry no eligibility
    decision, and resolving the SIBLING stream's decisions here would re-resolve rows this
    authorization does not cover.
    """
    attr_rows = tables.get(InterfaceIntent.__tablename__, [])
    ip_rows = tables.get(InterfaceIpIntent.__tablename__, [])
    interface_ids = sorted({row["interface_id"] for row in [*attr_rows, *ip_rows]})
    if not interface_ids:
        return build_interface_proof(stream, tables, [], {})
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
    )
    states = (
        (await db.execute(select(InterfaceAttrState).where(InterfaceAttrState.interface_id.in_(interface_ids))))
        .scalars()
        .all()
    )
    decisions = {
        (state.interface_id, state.attribute): state.sync_state in INTERFACE_ATTRIBUTE_ELIGIBLE_STATES
        for state in states
    }
    return build_interface_proof(stream, tables, interfaces, decisions)


def _section_execution(document: dict, section: str) -> dict:
    execution = (document.get(section) or {}).get(EXECUTION_KEY)
    if not isinstance(execution, dict):
        raise ValueError(f"document section {section!r} has no execution metadata")
    return execution


def section_context(document: dict, section: str) -> dict:
    """Return the frozen encoding context *section* must be encoded under.

    Every encode, every residue expectation and every unrenderable-object exclusion takes
    its NED id and dialect from here and from no device row, so a NED change reaches a
    section only when an operation reauthorizes it.
    """
    from nso_adapter.core.community_dialect import community_dialect_by_name

    context = _section_execution(document, section).get("context")
    if not isinstance(context, dict) or set(context) != {"ned_id", "dialect"}:
        raise ValueError(f"document section {section!r} has an invalid execution context")
    ned_id = context["ned_id"]
    if ned_id is not None and not isinstance(ned_id, str):
        raise ValueError(f"document section {section!r} records a non-string ned_id")
    try:
        community_dialect_by_name(context["dialect"])
    except ValueError as exc:
        raise ValueError(f"document section {section!r} names an unregistered dialect: {exc}") from None
    return context


def section_proof(document: dict, section: str) -> dict | None:
    """Return *section*'s recorded proof metadata, or ``None`` when it declares none."""
    proof = _section_execution(document, section).get("proof")
    if proof is not None and not isinstance(proof, dict):
        raise ValueError(f"document section {section!r} has invalid proof metadata")
    return proof


def section_operation(document: dict, section: str) -> dict:
    """Return the operation plane a generation wrote over *section*, or an empty mapping."""
    operation = _section_execution(document, section).get("operation")
    if operation is None:
        return {}
    if not isinstance(operation, dict):
        raise ValueError(f"document section {section!r} has an invalid operation plane")
    return operation


def hydrate_interface_execution(document: dict) -> InterfaceExecution:
    """Rebuild interface writer context and eligibility from the stored document.

    Both halves are checked by EXACT equality against the section's own rows: a missing
    decision, an extra one or an interface record the rows never reference is a document
    that does not describe what it carries, and repairing it here would re-derive at
    execution exactly what the freeze exists to fix.
    """
    section = document.get("interface_config") or {}
    # The context is validated HERE, not only where the encoder reads it: this hydrator runs
    # before any device I/O, and a pre-contract section with a valid proof would otherwise
    # execute and only fail at the encode site.
    section_context(document, "interface_config")
    proof = section_proof(document, "interface_config") or {}
    if not set(proof) <= {"interfaces", "attribute_eligibility"} or "interfaces" not in proof:
        raise ValueError("document section 'interface_config' has invalid execution proof")
    serialized_interfaces = proof["interfaces"]
    serialized_eligibility = proof.get("attribute_eligibility") or {}
    if not isinstance(serialized_interfaces, dict) or not isinstance(serialized_eligibility, dict):
        raise ValueError("document section 'interface_config' has invalid execution proof")
    interfaces: dict[int, DbInterface] = {}
    for key, record in serialized_interfaces.items():
        if not isinstance(record, dict) or set(record) != set(_INTERFACE_PROOF_FIELDS):
            raise ValueError("document section 'interface_config' has invalid interface context")
        iface = DbInterface(**record)
        if str(iface.id) != str(key):
            raise ValueError(f"document section 'interface_config' files interface {iface.id} under key {key!r}")
        interfaces[iface.id] = iface
    referenced_interfaces = {
        row["interface_id"]
        for table in (InterfaceIntent.__tablename__, InterfaceIpIntent.__tablename__)
        for row in section.get(table, [])
    }
    if set(interfaces) != referenced_interfaces:
        raise ValueError("document section 'interface_config' execution context does not match its rows")
    attribute_keys = {(row["interface_id"], row["attribute"]) for row in section.get(InterfaceIntent.__tablename__, [])}
    # Compared as WRITTEN, before decoding: "7/description" and "07/description" decode to one
    # tuple, so a set of decoded keys can match the expected set while one decision silently
    # overwrote the other and turned an eligible attribute ineligible.
    expected_written = {
        _attribute_eligibility_key(interface_id, attribute) for interface_id, attribute in attribute_keys
    }
    if set(serialized_eligibility) != expected_written:
        raise ValueError("document section 'interface_config' eligibility does not match its attribute rows")
    decisions: dict[tuple[int, str], bool] = {}
    for key, value in serialized_eligibility.items():
        interface_id, _, attribute = str(key).partition("/")
        if not interface_id.isdigit() or not attribute or not isinstance(value, bool):
            raise ValueError(f"document section 'interface_config' has an invalid eligibility decision {key!r}")
        decisions[(int(interface_id), attribute)] = value
    return InterfaceExecution(interfaces, frozenset(key for key, eligible in decisions.items() if eligible))


def _merge_interface_records(section: str, into: dict, record: dict, key: str) -> None:
    """Merge one interface record FIELD-WISE, refusing two different non-null values.

    Owner-wins is wrong for rows: the IP endpoint backfills ``parent_binding`` and
    ``encap_tag`` onto an interface an attribute fragment recorded while both were null, and
    discarding the populated record would encode the address without its binding.
    """
    existing = into.get(key)
    if existing is None:
        into[key] = deepcopy(record)
        return
    merged = dict(existing)
    for field, value in record.items():
        previous = merged.get(field)
        if previous is None:
            merged[field] = value
        elif value is not None and value != previous:
            raise ValueError(
                f"document section {section!r} interface {key} has conflicting {field!r} values "
                f"{previous!r} and {value!r} across its fragments"
            )
    into[key] = merged


def compose_section_execution(section: str, contributions: list[tuple[str, dict]]) -> dict:
    """Compose one section's ``_execution`` from the fragments that build it.

    Context is OWNER-WINS: it belongs to the section, and reauthorizing the owner is the one
    way to move the section to a new NED. Proof is per ROW, so object-valued proof merges key
    by key and interface records merge field-wise; a non-object proof value may come from one
    fragment only.
    """
    owner = _SPLIT_SECTION_STREAMS[section].context_owner if section in _SPLIT_SECTION_STREAMS else section
    contexts: dict[str, dict] = {}
    proof: dict = {}
    proof_source: dict[str, str] = {}
    for stream, fragment in contributions:
        frozen = fragment.get(EXECUTION_KEY)
        if not isinstance(frozen, dict):
            raise ValueError(f"section {section!r} stream {stream!r} contributes an unfrozen fragment")
        if "operation" in frozen:
            raise ValueError(
                f"section {section!r} stream {stream!r} contributes a fragment carrying an operation plane"
            )
        contexts[stream] = section_context({section: fragment}, section)
        for key, value in (frozen.get("proof") or {}).items():
            if key == "interfaces":
                merged = proof.setdefault("interfaces", {})
                for interface_key, record in value.items():
                    _merge_interface_records(section, merged, record, interface_key)
                proof_source.setdefault(key, stream)
            elif isinstance(value, dict):
                proof.setdefault(key, {}).update(deepcopy(value))
                proof_source.setdefault(key, stream)
            elif key in proof:
                raise ValueError(
                    f"section {section!r} proof key {key!r} is contributed by both {proof_source[key]!r} and {stream!r}"
                )
            else:
                proof[key] = deepcopy(value)
                proof_source[key] = stream
    if owner in contexts:
        context = contexts[owner]
    elif len(contexts) == 1:
        context = next(iter(contexts.values()))
    else:
        raise ValueError(
            f"section {section!r} is composed from {sorted(contexts)} with its context owner "
            f"{owner!r} absent, so no fragment's context is authoritative"
        )
    execution: dict = {"context": deepcopy(context)}
    if proof:
        execution["proof"] = proof
    return execution


def retained_proof(
    stream: str, desired: dict | None, source: dict | None, retained: dict[str, list[dict]]
) -> dict | None:
    """Extend *desired*'s proof with the entries the retained rows carried in *source*.

    A retained row is a row an earlier authorization froze and this one keeps on the wire
    until its detach link runs. Its decisions come from the fragment it was retained FROM,
    never from live state: re-resolving them would let an interface that fell out of the
    eligible set drop a description the intermediate document must still carry.
    """
    if not any(retained.values()):
        return deepcopy(desired) if desired is not None else None
    result: dict = deepcopy(desired) if desired is not None else {}
    origin = source or {}
    if stream in ("interface_config", "ip"):
        interfaces = result.setdefault("interfaces", {})
        for rows in retained.values():
            for row in rows:
                key = str(row["interface_id"])
                record = (origin.get("interfaces") or {}).get(key)
                if record is None:
                    raise ValueError(f"retained interface row references interface {key} with no recorded context")
                # MERGED, not skipped: the desired fragment may name the same interface with a
                # field the source had populated and a refresh has since nulled, and taking the
                # desired record whole would drop the binding the retained row must keep.
                _merge_interface_records(stream, interfaces, record, key)
        if stream == "interface_config":
            eligibility = result.setdefault("attribute_eligibility", {})
            for row in retained.get(InterfaceIntent.__tablename__, []):
                key = _attribute_eligibility_key(row["interface_id"], row["attribute"])
                if key in eligibility:
                    continue
                decision = (origin.get("attribute_eligibility") or {}).get(key)
                if decision is None:
                    raise ValueError(f"retained attribute row {key} has no recorded eligibility decision")
                eligibility[key] = decision
    elif stream == "static_route":
        from nso_adapter.core.static_route_plan import extend_apply_plan

        result["apply"] = extend_apply_plan(
            result.get("apply"),
            (origin.get("apply") or {}),
            retained.get(StaticRouteIntent.__tablename__, []),
        )
    return result


def prune_consumed_carriers(fragment: dict, existing_ids: frozenset[int]) -> dict:
    """Drop the lifecycle carriers a settlement already consumed, and rebuild their proof.

    The ONLY path that rewrites a fragment without an authorization, and admissible for one
    reason: a carrier's disappearance is settlement by definition, so removing its reference
    withdraws an authority the store has already discharged. It changes no intent value, no
    context and no non-carrier proof, and it never reads live intent.
    """
    carriers = [table for table in fragment_tables(fragment) if _spec_or_refuse(table).lifecycle]
    consumed = {table: [row for row in fragment[table] if row.get("id") not in existing_ids] for table in carriers}
    if not any(consumed.values()):
        return fragment
    result = deepcopy(fragment)
    for table in carriers:
        result[table] = [row for row in result[table] if row.get("id") in existing_ids]
    apply_plan = ((result.get(EXECUTION_KEY) or {}).get("proof") or {}).get("apply")
    if apply_plan is not None:
        from nso_adapter.core.static_route_plan import prune_apply_plan

        result[EXECUTION_KEY]["proof"]["apply"] = prune_apply_plan(
            apply_plan,
            result.get(StaticRouteIntent.__tablename__, []),
            result.get(StaticRouteTombstone.__tablename__, []),
        )
    return result


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
        models.update(spec.model for spec in _SECTION_REGISTRY[section].tables)
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
    section_specs = _SECTION_REGISTRY[section].tables
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
    section_context(document, section)
    tables = document[section] or {}
    allowed_models = {spec.model for spec in _SECTION_REGISTRY[section].tables}
    rows: dict[type, list] = {}
    row_records: dict[type, list[tuple[dict, object]]] = {}
    for table_name, serialized_rows in tables.items():
        if table_name == EXECUTION_KEY:
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


def section_rows_by_table(document: dict, section: str) -> dict[str, list]:
    """Rebuild *section*'s rows the way its encoder reads them: table name -> rows.

    Every table the section declares is present, empty when the document carries none, so
    an encoder indexes its tables directly and a renamed table raises instead of silently
    encoding an empty list.
    """
    hydrated = hydrate_section(document, section)
    return {spec.model.__tablename__: hydrated.get(spec.model, []) for spec in section_registry()[section].tables}


def fragment_tables(fragment: dict | None) -> dict[str, list[dict]]:
    """Return a fragment's TABLES, without its execution metadata."""
    return {table: rows for table, rows in (fragment or {}).items() if table != EXECUTION_KEY}


def fragment_context(fragment: dict | None) -> dict | None:
    """Return the encoding context a fragment was frozen with, or ``None`` for an unfrozen one."""
    return ((fragment or {}).get(EXECUTION_KEY) or {}).get("context")


async def freeze_fragment(db: AsyncSession, device, stream: str, tables: dict[str, list[dict]]) -> dict:
    """Return the FRAGMENT for *tables*: the rows plus the state they must execute with.

    The one authorization-time fragment producer. Every fragment carries a context, and a
    stream whose section declares proof carries the proof of ITS OWN rows as well; the
    caller's transaction holds the projection lock, so the live non-intent rows read here
    belong to the same state the tables were serialized from.

    The encoding context is preserved verbatim: ``ned_id`` keeps an explicit null (a device
    with no NED id records ``null`` and dialect ``identity``), and ``dialect`` is the stable
    name of the dialect that NED id resolves to, so an encode never re-reads the device row.
    """
    from nso_adapter.core.community_dialect import community_dialect_for

    execution: dict = {
        "context": {"ned_id": device.ned_id, "dialect": community_dialect_for(device.ned_id).name},
    }
    proof = await _freeze_proof(db, device.id, stream, tables, execution["context"])
    if proof is not None:
        execution["proof"] = proof
    return {**deepcopy(tables), EXECUTION_KEY: execution}


async def _freeze_proof(
    db: AsyncSession, device_id: int, stream: str, tables: dict[str, list[dict]], context: dict
) -> dict | None:
    """Return the proof *stream*'s own rows must be executed with, or ``None``."""
    if stream in ("interface_config", "ip"):
        return await _freeze_interface_proof(db, device_id, stream, tables)
    if stream == "static_route":
        from nso_adapter.core.static_route_plan import freeze_static_route_proof

        return freeze_static_route_proof(tables, device_id=device_id, context=context)
    return None


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
    "BESPOKE_EXPANSION",
    "CLAIM_LESS_SECTIONS",
    "NO_COMPARISON",
    "BespokeExpansion",
    "DOCUMENT_EXECUTED_SECTIONS",
    "INTERFACE_ATTRIBUTE_ELIGIBLE_STATES",
    "EXECUTION_KEY",
    "InterfaceEligibilityUnresolved",
    "build_interface_proof",
    "compose_section_execution",
    "prune_consumed_carriers",
    "retained_proof",
    "section_context",
    "section_operation",
    "section_proof",
    "InterfaceExecution",
    "LIVE_READ_SECTIONS",
    "NoComparison",
    "TableCompare",
    "Verification",
    "section_container",
    "section_registry",
    "section_rows_by_table",
    "fragment_context",
    "fragment_tables",
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
    "freeze_fragment",
    "snapshot_stream",
    "stream_section",
    "stream_tables",
]
