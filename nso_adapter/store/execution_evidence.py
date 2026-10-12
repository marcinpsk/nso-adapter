# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Safe serialization for durable execution evidence."""

import json
from collections.abc import Iterable
from dataclasses import asdict

from nso_adapter.nso.origins import SourceRef
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


def validate_catalog_payload(payload: list[dict]) -> None:
    valid = True
    try:
        for record in payload:
            related = record["related"]
            site = (
                None
                if related is None
                else RelatedSite(
                    related["family"],
                    None if related["row"] is None else RowRef(**related["row"]),
                    related["field"],
                )
            )
            parsed = Refusal(
                record["family"],
                RowRef(**record["row"]),
                record["field"],
                record["reason"],
                record["message_id"],
                record["message"],
                site,
            )
            if catalog_records((parsed,)) != [record]:
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
