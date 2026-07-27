# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Canonical IS-IS enum forms shared by the reader (core.isis) and writer (nso.apply).

``is-type`` and ``circuit-type`` share the YANG enum ``level-1 | level-2-only |
level-1-2``; ``level-2`` is a common free-text alias. Both sides must fold it to the
same canonical value or the plugin overlay sees phantom drift (a read "level-2" vs
the writer's "level-2-only").
"""

from __future__ import annotations


def isis_level(value: str | None) -> str | None:
    """Fold the IS-IS level alias ``level-2`` to its YANG form ``level-2-only``."""
    return "level-2-only" if value == "level-2" else value
