# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Classify raw RESTCONF errors before redaction."""

import json
from collections.abc import Iterator
from dataclasses import dataclass

from nso_adapter.nso.refusal_protocol import Refusal, claims_refusal, parse_refusals


@dataclass(frozen=True, slots=True)
class DefiniteRefusal:
    """A canonical refusal of the transaction, with catalog records only."""

    records: tuple[Refusal, ...]


class ProtocolFault(ValueError):
    """A safe fault that carries no response or parsing exception."""

    message = "NSO returned an invalid refusal protocol response"

    def __init__(self) -> None:
        super().__init__(self.message)


@dataclass(frozen=True, slots=True)
class Unavailable:
    """The response gives no conclusive delivery outcome."""


@dataclass(frozen=True, slots=True)
class OrdinaryRejection:
    """An unclaimed, conclusive HTTP rejection."""


ErrorClassification = DefiniteRefusal | ProtocolFault | Unavailable | OrdinaryRejection


class _Object(list[tuple[str, object]]):
    """Preserve all JSON member occurrences until validation finishes."""


_ERROR_MEMBERS = frozenset(("error-type", "error-tag", "error-app-tag", "error-path", "error-message", "error-info"))


def _walk(value: object) -> Iterator[object]:
    pending = [value]
    while pending:
        node = pending.pop()
        yield node
        if isinstance(node, _Object):
            for key, child in node:
                pending.extend((key, child))
        elif isinstance(node, list):
            pending.extend(node)


def _members(value: object) -> dict[str, object]:
    if not isinstance(value, _Object):
        raise ValueError("RESTCONF requires an object")
    members = dict(value)
    if len(members) != len(value):
        raise ValueError("RESTCONF has duplicate members")
    return members


def _reject_constant(_value: str) -> object:
    raise ValueError("JSON has a nonstandard constant")


def _decode_outer(body: bytes | str | None) -> tuple[object, bool]:
    if body is None:
        return None, False
    try:
        decoded = json.loads(body, object_pairs_hook=_Object, parse_constant=_reject_constant)
    except (ValueError, UnicodeError, RecursionError):
        decoded = None
        valid = False
    else:
        valid = True
    return decoded, valid


def _envelope_message(decoded: object) -> str:
    claim_count = 0
    for node in _walk(decoded):
        if isinstance(node, _Object):
            _members(node)
        elif claims_refusal(node):
            claim_count += 1
    if claim_count != 1:
        raise ValueError("RESTCONF has competing refusal claims")

    root = _members(decoded)
    if set(root) in ({"ietf-restconf:errors"}, {"errors"}):
        root = _members(next(iter(root.values())))
    if set(root) != {"error"}:
        raise ValueError("RESTCONF has invalid envelope members")
    errors = root["error"]
    if not isinstance(errors, list) or isinstance(errors, _Object) or len(errors) != 1:
        raise ValueError("RESTCONF requires one error")
    error = _members(errors[0])
    if "error-message" not in error or not set(error) <= _ERROR_MEMBERS:
        raise ValueError("RESTCONF has invalid error members")
    for name, value in error.items():
        if name == "error-info":
            _members(value)
        elif not isinstance(value, str):
            raise ValueError("RESTCONF error metadata requires text")
    # A refusal is NSO's observed application/operation-failed; other tags (rollback-failed) contradict a rollback.
    if error.get("error-type") != "application" or error.get("error-tag") != "operation-failed":
        raise ValueError("RESTCONF refusal requires application operation-failed")
    message = error["error-message"]
    if not isinstance(message, str):
        raise ValueError("RESTCONF requires an error message")
    return message


def _claimed_records(decoded: object) -> tuple[Refusal, ...] | None:
    try:
        records = parse_refusals(_envelope_message(decoded))
    except (ValueError, UnicodeError, RecursionError):
        records = None
    return records


def classify_error(status: int | None, body: bytes | str | None) -> ErrorClassification:
    """Classify claims before HTTP errors, without retaining external text.

    Successful responses stay with the native and CLI decoders. Transport failures
    have no HTTP status. Construct faults after parsing handlers have finished.
    """
    if status is None:
        return Unavailable()
    raw_text = body.decode("utf-8", errors="surrogateescape") if isinstance(body, bytes) else body
    claimed = claims_refusal(raw_text)
    decoded, valid_json = _decode_outer(body)
    if valid_json:
        for node in _walk(decoded):
            if isinstance(node, str):
                claimed = claims_refusal(node) or claimed
    if claimed:
        records = _claimed_records(decoded) if valid_json else None
        if status == 500 and records is not None:
            return DefiniteRefusal(records)
        return ProtocolFault()
    if status == 500 and not valid_json:
        return ProtocolFault()
    if 500 <= status < 600:
        return Unavailable()
    if 400 <= status < 500:
        return OrdinaryRejection()
    raise ValueError("classify_error requires an HTTP error status")
