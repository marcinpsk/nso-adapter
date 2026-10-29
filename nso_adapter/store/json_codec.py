# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Decode stored JSON without losing exact numeric values."""

import json
import math
from decimal import Decimal, InvalidOperation
from typing import Any


class UnrepresentableJSONNumber:
    """Retain the number literal and prevent ordinary JSON serialization."""

    __slots__ = ("literal",)

    def __init__(self, literal: str) -> None:
        self.literal = literal


def _parse_float(literal: str) -> float | UnrepresentableJSONNumber:
    value = float(literal)
    try:
        if math.isfinite(value) and Decimal(repr(value)) == Decimal(literal):
            return value
    except InvalidOperation:
        pass
    return UnrepresentableJSONNumber(literal)


def _parse_int(literal: str) -> int | UnrepresentableJSONNumber:
    try:
        return int(literal)
    except ValueError:
        return UnrepresentableJSONNumber(literal)


# This limit is far above writer depth and low enough to keep the standard recursive encoder, Pydantic, and canonicalization safe.
MAX_STORED_JSON_DEPTH = 256


class UnrepresentableJSONDocument:
    """Retain the stored text and prevent ordinary JSON serialization."""

    __slots__ = ("text",)

    def __init__(self, text: str) -> None:
        self.text = text


def load_store_json(value: str) -> Any:
    """Decode exact numbers and retain documents beyond the safe depth."""
    try:
        result = json.loads(value, parse_float=_parse_float, parse_int=_parse_int)
    except RecursionError:
        return UnrepresentableJSONDocument(value)
    pending = [(result, 1)]
    while pending:
        item, depth = pending.pop()
        if isinstance(item, (dict, list)):
            if depth > MAX_STORED_JSON_DEPTH:
                return UnrepresentableJSONDocument(value)
            children = item.values() if isinstance(item, dict) else item
            pending.extend((child, depth + 1) for child in children if isinstance(child, (dict, list)))
    return result
