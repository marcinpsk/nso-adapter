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
        if math.isfinite(value) and Decimal.from_float(value) == Decimal(literal):
            return value
    except InvalidOperation:
        pass
    return UnrepresentableJSONNumber(literal)


def _parse_int(literal: str) -> int | UnrepresentableJSONNumber:
    try:
        return int(literal)
    except ValueError:
        return UnrepresentableJSONNumber(literal)


def load_store_json(value: str) -> Any:
    """Keep exact native numbers and retain literals that Python cannot represent."""
    return json.loads(value, parse_float=_parse_float, parse_int=_parse_int)
