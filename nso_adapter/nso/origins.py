# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Exact encoding origins and atomic refusal attribution, without settlement effects.

Certified retained entries join acknowledged baseline fingerprints. Historical omission
requires prior origins and immutable removal or clear authority. C5 activates settlement.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal, NamedTuple

from sqlalchemy import inspect as sa_inspect

from nso_adapter.nso.refusal_protocol import SUBJECTS, Refusal, RowRef, row_ref


@dataclass(frozen=True)
class OriginContext:
    """Frozen generation metadata, supplied by the document caller."""

    generation_id: int | None = None
    source_push_seq: Mapping[str, int | None] = field(default_factory=dict)
    stream_revisions: Mapping[str, int] = field(default_factory=dict)
    settlement_cohort: int | None = None

    @classmethod
    def from_generation(cls, generation: Any) -> OriginContext:
        return cls(
            generation.id,
            dict(generation.source_push_seq),
            dict(generation.stream_revisions),
            generation.settlement_cohort,
        )


@dataclass(frozen=True)
class SourceRef:
    source_table: str
    frozen_row_identity: tuple[int, ...]
    content_identity: str
    frozen_revision: int | None
    accepted_at: datetime | None
    live_stamp_join: tuple[str, int] | None


@dataclass(frozen=True)
class SettlementOwner:
    stream: str
    source_table: str
    root_row: RowRef
    source_push_seq: int | None
    frozen_revision: int | None
    accepted_at: datetime | None
    generation_id: int | None
    settlement_cohort: int | None
    content_identity: str

    def public_record(self) -> dict:
        return {
            "stream": self.stream,
            "row": {"subject": self.root_row.subject, "key": self.root_row.key},
            "source_push_seq": self.source_push_seq,
            "frozen_revision": self.frozen_revision,
            "accepted_at": self.accepted_at.isoformat() if self.accepted_at is not None else None,
            "generation_id": self.generation_id,
            "settlement_cohort": self.settlement_cohort,
        }


@dataclass(frozen=True)
class LogicalRow:
    stream: str
    row: RowRef
    source_refs: tuple[SourceRef, ...]
    source_push_seq: int | None
    frozen_revision: int | None
    acceptance_identity: str | None
    generation_id: int | None
    settlement_cohort: int | None
    carrier: SettlementOwner

    def public_record(self) -> dict:
        """Publish the logical fence separately from the carrier, without ORM keys."""
        return {
            "stream": self.stream,
            "row": {"subject": self.row.subject, "key": self.row.key},
            "source_push_seq": self.source_push_seq,
            "frozen_revision": self.frozen_revision,
            "acceptance_identity": self.acceptance_identity,
            "generation_id": self.generation_id,
            "settlement_cohort": self.settlement_cohort,
            "carrier": self.carrier.public_record(),
        }


@dataclass(frozen=True)
class OriginKey:
    family: str
    row: RowRef
    field: str | None = None


@dataclass(frozen=True)
class OriginBinding:
    stream: str
    sources: tuple[SourceRef, ...]
    construct: str
    provenance: Literal["current", "retained", "prior"]
    logical_rows: tuple[LogicalRow, ...]
    baseline_attempt_id: int | None = None
    entry_fingerprint: str | None = None
    authority: RemovalAuthority | None = None


FailureReason = Literal[
    "missing_binding",
    "mixed_stream",
    "missing_baseline_provenance",
    "missing_removal_provenance",
    "missing_natural_key",
    "invalid_source_identity",
]


@dataclass(frozen=True)
class UnsupportedOrigin:
    reason: FailureReason


@dataclass(frozen=True)
class RemovalAuthority:
    """An immutable operation fence joined to a recorded prior origin."""

    generation_id: int
    stream: str
    frozen_revision: int
    operation: Literal["removal", "clear"]
    row: RowRef
    fields: tuple[str, ...] = ()


@dataclass(frozen=True)
class RetainedContribution:
    row: RowRef
    entry_fingerprint: str


@dataclass(frozen=True)
class PriorContribution:
    family: str
    row: RowRef
    authority: RemovalAuthority | None


@dataclass
class OriginIndex:
    bindings: dict[OriginKey, OriginBinding] = field(default_factory=dict)
    unsupported: dict[OriginKey, UnsupportedOrigin] = field(default_factory=dict)
    retained: list[RetainedContribution] = field(default_factory=list)
    omitted: list[PriorContribution] = field(default_factory=list)
    unsupported_subjects: dict[str, UnsupportedOrigin] = field(default_factory=dict)

    def merge(self, other: OriginIndex) -> None:
        """Combine sections without allowing a competing producer."""
        if (self.bindings.keys() | self.unsupported.keys()) & (other.bindings.keys() | other.unsupported.keys()):
            raise OriginConstructionError("duplicate origin")
        self.bindings.update(other.bindings)
        self.unsupported.update(other.unsupported)
        self.retained.extend(other.retained)
        self.omitted.extend(other.omitted)
        self.unsupported_subjects.update(other.unsupported_subjects)

    def record_retained(self, entry: dict) -> None:
        """Keep verbatim retention sendable while refusing unproven attribution."""
        row = row_ref(
            "static-route",
            "static-route.route",
            (("route", (entry.get("vrf") or "", entry["prefix"], entry.get("next-hop") or "")),),
        )
        self.retained.append(RetainedContribution(row, content_digest(entry)))
        self.mark_unsupported("static-route", row, "missing_baseline_provenance")
        family_row = row_ref("static-route", "static-route", ())
        self.mark_unsupported("static-route", family_row, "missing_baseline_provenance")

    def record_omission(self, contribution: PriorContribution) -> None:
        self.omitted.append(contribution)
        self.mark_unsupported(
            contribution.family, contribution.row, "missing_removal_provenance", preserve_current=True
        )

    def mark_unsupported(
        self, family: str, row: RowRef, reason: FailureReason, *, preserve_current: bool = False
    ) -> None:
        for declared in (None, *SUBJECTS[row.subject].fields):
            key = OriginKey(family, row, declared)
            if preserve_current and key in self.bindings and self.bindings[key].provenance == "current":
                continue
            self.bindings.pop(key, None)
            self.unsupported[key] = UnsupportedOrigin(reason)


class EncodedSection(NamedTuple):
    body: dict
    origins: OriginIndex


class OriginConstructionError(ValueError):
    """The encoder has competing producers. Never include natural keys or values."""


@dataclass(frozen=True)
class AttributedRefusal:
    refusal: Refusal
    sources: tuple[SourceRef, ...]
    logical_rows: tuple[LogicalRow, ...]


@dataclass(frozen=True)
class AttributionFailure:
    reason: FailureReason
    key: OriginKey


def _valid_source(source: SourceRef) -> bool:
    return (
        bool(source.source_table)
        and len(source.frozen_row_identity) == 1
        and type(source.frozen_row_identity[0]) is int
        and source.frozen_row_identity[0] > 0
        and len(source.content_identity) == 64
        and all(char in "0123456789abcdef" for char in source.content_identity)
        and (source.accepted_at is None or source.accepted_at.utcoffset() is not None)
    )


def resolve_refusals(
    refusals: Iterable[Refusal], origins: OriginIndex
) -> tuple[AttributedRefusal, ...] | AttributionFailure:
    """Resolve every primary record exactly before returning any attributed record."""
    attributed = []
    for refusal_record in refusals:
        key = OriginKey(refusal_record.family, refusal_record.row, refusal_record.field)
        unsupported = origins.unsupported.get(key)
        if unsupported is not None:
            return AttributionFailure(unsupported.reason, key)
        binding = origins.bindings.get(key)
        if binding is None:
            if key.row.subject in origins.unsupported_subjects:
                return AttributionFailure(origins.unsupported_subjects[key.row.subject].reason, key)
            return AttributionFailure("missing_binding", key)
        if (
            binding.provenance == "retained"
            and (binding.baseline_attempt_id is None or binding.entry_fingerprint is None)
        ) or (binding.provenance == "prior" and (binding.baseline_attempt_id is None or binding.authority is None)):
            reason: FailureReason = (
                "missing_baseline_provenance" if binding.provenance == "retained" else "missing_removal_provenance"
            )
            return AttributionFailure(reason, key)
        if (
            not binding.logical_rows
            or not binding.sources
            or any(not _valid_source(source) for source in binding.sources)
        ):
            return AttributionFailure("invalid_source_identity", key)
        if any(
            not logical.source_refs or any(not _valid_source(source) for source in logical.source_refs)
            for logical in binding.logical_rows
        ):
            return AttributionFailure("invalid_source_identity", key)
        if any(
            logical.stream != binding.stream or logical.carrier.stream != binding.stream
            for logical in binding.logical_rows
        ):
            return AttributionFailure("mixed_stream", key)
        attributed.append(AttributedRefusal(refusal_record, binding.sources, binding.logical_rows))
    return tuple(attributed)


def content_digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()


def source_ref(
    row: Any,
    table: str,
    stream: str,
    context: OriginContext,
    *,
    children: Iterable[SourceRef] = (),
    stampable: bool = True,
    writer_context: Any = None,
) -> SourceRef:
    """Freeze safe source identity and hash content, including explicit child content."""
    from nso_adapter.core.projection import APPLY_BOOKKEEPING_COLUMNS

    mapper = sa_inspect(type(row), raiseerr=False)
    values = {attr.key: getattr(row, attr.key) for attr in mapper.column_attrs} if mapper is not None else vars(row)
    intent = {key: value for key, value in values.items() if key not in APPLY_BOOKKEEPING_COLUMNS}
    identity = getattr(row, "id", None)
    row_id = (identity,) if type(identity) is int else ()
    return SourceRef(
        table,
        row_id,
        content_digest((intent, tuple(child.content_identity for child in children), writer_context)),
        context.stream_revisions.get(stream),
        getattr(row, "accepted_at", None),
        (table, identity) if stampable and type(identity) is int else None,
    )


class OriginRecorder:
    """Record row and declared-field producers at the encoder's emission sites."""

    def __init__(self, family: str, context: OriginContext | None = None):
        self.family = family
        self.context = context or OriginContext()
        self.index = OriginIndex()
        self.contributors: list[LogicalRow] = []

    def logical(
        self,
        subject: str,
        path: tuple,
        source: SourceRef,
        stream: str,
        *,
        carrier: SettlementOwner | None = None,
        children: tuple[SourceRef, ...] = (),
    ) -> LogicalRow:
        row = row_ref(self.family, subject, path)
        if carrier is None:
            carrier = SettlementOwner(
                stream,
                source.source_table,
                row,
                self.context.source_push_seq.get(stream),
                source.frozen_revision,
                source.accepted_at,
                self.context.generation_id,
                self.context.settlement_cohort,
                source.content_identity,
            )
        acceptance = source.accepted_at if source.accepted_at is not None else carrier.accepted_at
        logical = LogicalRow(
            stream,
            row,
            (source, *children),
            self.context.source_push_seq.get(stream),
            source.frozen_revision,
            acceptance.isoformat() if acceptance is not None else None,
            self.context.generation_id,
            self.context.settlement_cohort,
            carrier,
        )
        if any(previous.stream == stream and previous.row == row for previous in self.contributors):
            raise OriginConstructionError("duplicate origin")
        self.contributors.append(logical)
        return logical

    def source(self, row: Any, table: str, stream: str, **kwargs) -> SourceRef:
        return source_ref(row, table, stream, self.context, **kwargs)

    def emit(
        self,
        subject: str,
        path: tuple,
        logical_rows: Iterable[LogicalRow],
        *,
        field_rows: Mapping[str, Iterable[LogicalRow]] | None = None,
        sources: tuple[SourceRef, ...] | None = None,
        field_sources: Mapping[str, tuple[SourceRef, ...]] | None = None,
    ) -> RowRef:
        """Bind the construct and every declared field, including current absent fields."""
        row = row_ref(self.family, subject, path)
        logicals = tuple(dict.fromkeys(logical_rows))
        self._bind(OriginKey(self.family, row), logicals, subject, sources)
        for declared in SUBJECTS[subject].fields:
            owners = (
                tuple(dict.fromkeys(field_rows[declared]))
                if field_rows is not None and declared in field_rows
                else logicals
            )
            refs = field_sources.get(declared) if field_sources is not None else sources if owners == logicals else None
            self._bind(OriginKey(self.family, row, declared), owners, subject, refs)
        return row

    def _bind(
        self, key: OriginKey, logicals: tuple[LogicalRow, ...], construct: str, sources: tuple[SourceRef, ...] | None
    ) -> None:
        if key in self.index.bindings or key in self.index.unsupported:
            raise OriginConstructionError("duplicate origin")
        if len({logical.stream for logical in logicals}) != 1:
            self.index.unsupported[key] = UnsupportedOrigin("mixed_stream" if logicals else "missing_binding")
            return
        refs = (
            sources
            if sources is not None
            else tuple(dict.fromkeys(source for logical in logicals for source in logical.source_refs))
        )
        self.index.bindings[key] = OriginBinding(logicals[0].stream, refs, construct, "current", logicals)

    def finish(self, body: dict) -> EncodedSection:
        if self.contributors:
            self.emit(self.family, (), self.contributors)
        return EncodedSection(body, self.index)


# Pinned from plugin models.py; public adapter CI does not require the sibling checkout.
OVERLAY_GRAIN = {
    "bgp.router.scope.peer": ("NSOBGPPeerState", "bgp.router.scope.peer", "bgp.router"),
    "bgp.router.scope.peer.peer-address-family": ("NSOBGPPeerState", "bgp.router.scope.peer", "bgp.router"),
    "lag.bundle.member": ("NSOLACPMemberState", "lag.bundle.member", "lag.bundle"),
    "switchport.interface": ("NSOSwitchportState", "switchport.interface", "switchport.interface"),
    **{
        f"route-policy.{kind}.entry": ("NSORoutePolicyState", f"route-policy.{kind}", f"route-policy.{kind}")
        for kind in ("prefix-list", "community-list", "as-path", "route-map")
    },
    **{
        subject: ("NSORedistributionState", subject, subject)
        for subject in (
            "bgp.router.scope.address-family.redistribute",
            "isis.process-config.redistribute",
            "ospf.process-config.redistribute",
        )
    },
}
