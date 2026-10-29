# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Decode stored JSON without losing exact numeric values."""

import json
import math
import re
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


_WHITESPACE = re.compile(r"[ \t\n\r]*")
_SCALAR_DECODER = json.JSONDecoder(parse_float=_parse_float, parse_int=_parse_int)


def _skip_whitespace(value: str, index: int) -> int:
    match = _WHITESPACE.match(value, index)
    assert match is not None
    return match.end()


def _read_value(value: str, index: int) -> tuple[Any, int]:
    index = _skip_whitespace(value, index)
    token = value[index : index + 1]
    if token == "[":
        return [], index + 1
    if token == "{":
        return {}, index + 1
    return _SCALAR_DECODER.raw_decode(value, index)


def _read_member(value: str, index: int, container: dict | list) -> tuple[Any, int]:
    if isinstance(container, dict):
        key, index = _SCALAR_DECODER.raw_decode(value, index)
        if not isinstance(key, str):
            raise json.JSONDecodeError("Expected object key", value, index)
        index = _skip_whitespace(value, index)
        if value[index : index + 1] != ":":
            raise json.JSONDecodeError("Expected colon", value, index)
        item, index = _read_value(value, index + 1)
        container[key] = item
    else:
        item, index = _read_value(value, index)
        container.append(item)
    return item, index


def load_store_json(value: str) -> Any:
    """Decode stored JSON with exact numbers and no Python recursion limit."""
    result, index = _read_value(value, 0)
    stack = [(result, False)] if isinstance(result, (dict, list)) else []
    while stack:
        container, has_member = stack[-1]
        closing = "}" if isinstance(container, dict) else "]"
        index = _skip_whitespace(value, index)
        token = value[index : index + 1]
        if token == closing:
            stack.pop()
            index += 1
            continue
        if has_member:
            if token != ",":
                raise json.JSONDecodeError("Expected comma or container end", value, index)
            index = _skip_whitespace(value, index + 1)
        item, index = _read_member(value, index, container)
        stack[-1] = (container, True)
        if isinstance(item, (dict, list)):
            stack.append((item, False))
    if _skip_whitespace(value, index) != len(value):
        raise json.JSONDecodeError("Extra data", value, index)
    return result
