# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The adapter's single wire-timestamp serializer.

Every store datetime is a UTC instant, and every API response renders one as
``"<iso>Z"`` — the only form all four plugin consumers parse (they do
``fromisoformat(value.replace("Z", "+00:00"))``, which a raw
``.isoformat() + "Z"`` on a tz-aware value breaks with ``"...+00:00Z"``).
"""

from __future__ import annotations

from datetime import datetime


def iso_z(ts: datetime | None) -> str | None:
    """Serialize a store datetime as ``"<iso>Z"``, or ``None``."""
    if ts is None:
        return None
    naive = ts.replace(tzinfo=None) if ts.tzinfo is not None else ts
    return naive.isoformat() + "Z"
