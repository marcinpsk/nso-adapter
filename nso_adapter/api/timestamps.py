# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The adapter's two wire-timestamp boundaries — one in, one out.

Every store datetime is a UTC instant. Outbound, every API response renders one as
``"<iso>Z"`` — the only form all four plugin consumers parse (they do
``fromisoformat(value.replace("Z", "+00:00"))``, which a raw
``.isoformat() + "Z"`` on a tz-aware value breaks with ``"...+00:00Z"``).

Inbound, a request body may carry a zone or omit it. A zone-less value bound to a
``timestamptz`` column is shifted by the adapter PROCESS's local zone, so the same
instant reads back hours off. :data:`UtcInstant` is the request-model annotation that
closes that: zone-less means UTC, and an offset-carrying value is canonicalized to UTC
so the store only ever holds one clock domain.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated

from pydantic import AfterValidator


def to_utc(ts: datetime) -> datetime:
    """Interpret a zone-less datetime as UTC; convert an aware one to UTC."""
    return ts.astimezone(UTC) if ts.tzinfo is not None else ts.replace(tzinfo=UTC)


#: Annotation for every INBOUND request-model datetime field.
UtcInstant = Annotated[datetime, AfterValidator(to_utc)]


def iso_z(ts: datetime | None) -> str | None:
    """Serialize a store datetime as ``"<iso>Z"``, or ``None``."""
    if ts is None:
        return None
    naive = ts.replace(tzinfo=None) if ts.tzinfo is not None else ts
    return naive.isoformat() + "Z"
