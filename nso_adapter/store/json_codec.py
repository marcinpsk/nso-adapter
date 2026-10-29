# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Decode stored JSON without losing numbers outside the finite float range."""

import json
import math
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class UnrepresentableJSONNumber:
    """Retain the number literal and prevent ordinary JSON serialization."""

    literal: str


def _parse_float(literal: str) -> float | UnrepresentableJSONNumber:
    value = float(literal)
    return value if math.isfinite(value) else UnrepresentableJSONNumber(literal)


def load_store_json(value: str) -> Any:
    """Keep ordinary floats and retain the literal when conversion overflows."""
    return json.loads(value, parse_float=_parse_float)
