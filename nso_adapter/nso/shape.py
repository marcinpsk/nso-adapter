# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Shape helpers for NSO RESTCONF/JSON oper-data.

NSO's RESTCONF output does not reliably follow RFC 7951 array encoding: a YANG
list with a single entry can be rendered as a bare object instead of a
one-element JSON array (and an absent list as ``None`` or a missing key). A
parser that iterates the raw value assuming an array would, for a singleton,
loop over the dict *keys* (strings) and crash on ``x.get(...)`` / ``x[...]`` or
mis-count with ``len(dict)``. Route every child-list value through ``as_list``.

This is the same defence ``NsoClient.list_devices``/``list_ned_packages`` already
apply inline with ``isinstance(..., list)`` guards.
"""

from __future__ import annotations

from typing import Any


def as_list(value: Any) -> list:
    """Normalize an NSO child-list value to a Python list.

    ``None`` (absent list) → ``[]``; an existing list is returned unchanged; any
    other scalar/mapping (a singleton rendered as a bare object) → ``[value]``.
    """
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]
