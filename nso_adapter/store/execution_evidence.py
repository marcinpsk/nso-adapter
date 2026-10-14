# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Safe serialization for durable execution evidence."""

import json
from collections.abc import Iterable
from dataclasses import asdict
from datetime import datetime

from nso_adapter.nso.origins import (
    LogicalRow,
    OriginBinding,
    OriginIndex,
    OriginKey,
    RemovalAuthority,
    SettlementOwner,
    SourceRef,
    UnsupportedOrigin,
)
from nso_adapter.nso.refusal_protocol import PREFIX, Refusal, RelatedSite, RowRef, parse_refusals, render_refusals


def catalog_records(records: Iterable[Refusal]) -> list[dict]:
    """Validate each site through the codec, including multiple sites of one family."""
    result = []
    valid = True
    try:
        for record in records:
            message = render_refusals((record,))
            parse_refusals(message)
            result.extend(json.loads(message[len(PREFIX) :])["refusals"])
    except (ValueError, TypeError, AttributeError):
        valid = False
    if not valid:
        raise ValueError("Refusal evidence must use the codec catalog")
    return result


def _refusal_from_record(record: dict) -> Refusal:
    related = record["related"]
    site = (
        None
        if related is None
        else RelatedSite(
            related["family"], None if related["row"] is None else RowRef(**related["row"]), related["field"]
        )
    )
    return Refusal(
        record["family"],
        RowRef(**record["row"]),
        record["field"],
        record["reason"],
        record["message_id"],
        record["message"],
        site,
    )


def validate_catalog_payload(payload: list[dict]) -> None:
    valid = True
    try:
        for record in payload:
            if catalog_records((_refusal_from_record(record),)) != [record]:
                valid = False
    except (ValueError, KeyError, TypeError, AttributeError):
        valid = False
    if not valid:
        raise ValueError("Refusal evidence must use the codec catalog")


def source_record(source: SourceRef) -> dict:
    """Persist digest identity and joins, without any natural path or intent content."""
    record = asdict(source)
    record["frozen_row_identity"] = list(source.frozen_row_identity)
    record["live_stamp_join"] = list(source.live_stamp_join) if source.live_stamp_join is not None else None
    record["accepted_at"] = source.accepted_at.isoformat() if source.accepted_at is not None else None
    return record


def logical_record(logical: LogicalRow) -> dict:
    return {
        **logical.public_record(),
        "sources": [source_record(source) for source in logical.source_refs],
        "carrier_content_identity": logical.carrier.content_identity,
        "carrier_source_table": logical.carrier.source_table,
    }


def _source_from_record(record: dict) -> SourceRef:
    return SourceRef(
        record["source_table"],
        tuple(record["frozen_row_identity"]),
        record["content_identity"],
        record["frozen_revision"],
        datetime.fromisoformat(record["accepted_at"]) if record["accepted_at"] else None,
        tuple(record["live_stamp_join"]) if record["live_stamp_join"] else None,
    )


def logical_from_record(record: dict) -> LogicalRow:
    carrier = record["carrier"]
    sources = tuple(_source_from_record(source) for source in record["sources"])
    owner = SettlementOwner(
        carrier["stream"],
        record["carrier_source_table"],
        RowRef(**carrier["row"]),
        carrier["source_push_seq"],
        carrier["frozen_revision"],
        datetime.fromisoformat(carrier["accepted_at"]) if carrier["accepted_at"] else None,
        carrier["generation_id"],
        carrier["settlement_cohort"],
        record["carrier_content_identity"],
    )
    return LogicalRow(
        record["stream"],
        RowRef(**record["row"]),
        sources,
        record["source_push_seq"],
        record["frozen_revision"],
        record["acceptance_identity"],
        record["generation_id"],
        record["settlement_cohort"],
        owner,
    )


def origin_records(origins: OriginIndex) -> list[dict]:
    records = [
        {
            "family": key.family,
            "row": {"subject": key.row.subject, "key": key.row.key},
            "field": key.field,
            "stream": binding.stream,
            "construct": binding.construct,
            "provenance": binding.provenance,
            "sources": [source_record(source) for source in binding.sources],
            "logical_rows": [logical_record(logical) for logical in binding.logical_rows],
            "baseline_attempt_id": binding.baseline_attempt_id,
            "entry_fingerprint": binding.entry_fingerprint,
            "authority": None
            if binding.authority is None
            else {**asdict(binding.authority), "fields": list(binding.authority.fields)},
        }
        for key, binding in origins.bindings.items()
    ]
    records.extend(
        {
            "family": key.family,
            "row": {"subject": key.row.subject, "key": key.row.key},
            "field": key.field,
            "unsupported_reason": unsupported.reason,
            "logical_rows": [],
        }
        for key, unsupported in origins.unsupported.items()
    )
    records.extend(
        {"subject": subject, "unsupported_reason": unsupported.reason, "logical_rows": []}
        for subject, unsupported in origins.unsupported_subjects.items()
    )
    return records


def origins_from_records(records: list[dict]) -> OriginIndex:
    origins = OriginIndex()
    for record in records:
        if "family" not in record:
            origins.unsupported_subjects[record["subject"]] = UnsupportedOrigin(record["unsupported_reason"])
            continue
        key = OriginKey(record["family"], RowRef(**record["row"]), record["field"])
        if "unsupported_reason" in record:
            origins.unsupported[key] = UnsupportedOrigin(record["unsupported_reason"])
            continue
        authority = record.get("authority")
        fence = (
            None
            if authority is None
            else RemovalAuthority(
                authority["generation_id"],
                authority["stream"],
                authority["frozen_revision"],
                authority["operation"],
                RowRef(**authority["row"]),
                tuple(authority["fields"]),
            )
        )
        origins.bindings[key] = OriginBinding(
            record["stream"],
            tuple(_source_from_record(source) for source in record["sources"]),
            record["construct"],
            record["provenance"],
            tuple(logical_from_record(logical) for logical in record["logical_rows"]),
            record.get("baseline_attempt_id"),
            record.get("entry_fingerprint"),
            fence,
        )
    return origins


def refusals_from_payload(payload: list[dict]) -> tuple[Refusal, ...]:
    validate_catalog_payload(payload)
    return tuple(_refusal_from_record(record) for record in payload)
