# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Blocking preconditions for C9's cutover window (#1683).

The cutover deletes the legacy per-family instances before installing the aggregate. A
PARKED carrier — one claiming a key that no authorized positive row renders — would lose its
only payload source at that moment: the aggregate reads certifiably absent, retention keeps
nothing, and the owed deletion has no entry left to clean up.

Seeding the key back as a positive row is NOT the answer: a rendered key makes the next
removal reissue classify the owed deletion as superseded, silently converting it into
adopted intent. So the window REFUSES to open while any device holds such a carrier, and the
operator resolves each one by Apply (the removal settles and consumes the carrier) or by
abandoning the blocked head and reauthorizing.
"""

from __future__ import annotations

from typing import NamedTuple

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.get_logger(__name__)

#: Read over the AUTHORIZED fragment rather than live intent, so a store-only identity edit
#: cannot mask a carrier: only what an authorization froze counts as rendered. The one
#: departure from the design's literal SQL is the ``::jsonb`` cast, because
#: ``authorized_document`` is a ``json`` column and ``jsonb_array_elements`` needs ``jsonb``.
_PARKED_CARRIERS = text("""
WITH rendered AS (SELECT s.device_id, coalesce(r->>'vrf','') vrf, r->>'prefix' prefix, coalesce(r->>'next_hop','') next_hop
  FROM device_projection_stream s, LATERAL jsonb_array_elements(coalesce(s.authorized_document::jsonb->'static_route_intent','[]'::jsonb)) r
  WHERE s.stream='static_route' AND s.authorized_document IS NOT NULL)
SELECT t.device_id, t.id, t.vrf, t.prefix, t.next_hop, t.deployed_key FROM static_route_tombstone t
WHERE NOT EXISTS (SELECT 1 FROM rendered k WHERE k.device_id=t.device_id AND (k.vrf,k.prefix,k.next_hop)=(coalesce(t.vrf,''),t.prefix,coalesce(t.next_hop,'')))
   OR (t.deployed_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM rendered k WHERE k.device_id=t.device_id
       AND (k.vrf,k.prefix,k.next_hop)=(coalesce(t.deployed_key->>0,''),t.deployed_key->>1,coalesce(t.deployed_key->>2,''))))
ORDER BY t.device_id, t.id
""")


class ParkedCarrier(NamedTuple):
    """One carrier whose current or deployed key no authorized positive row renders."""

    device_id: int
    tombstone_id: int
    keys: tuple[tuple[str, str, str], ...]


class CutoverBlocked(RuntimeError):
    """The cutover window may not open: at least one device holds a parked carrier."""

    def __init__(self, parked: list[ParkedCarrier]):
        self.parked = parked
        named = ", ".join(
            f"device {carrier.device_id} tombstone {carrier.tombstone_id} keys {list(carrier.keys)}"
            for carrier in parked
        )
        super().__init__(f"{len(parked)} static-route carrier(s) are parked and must drain first: {named}")


async def parked_static_route_carriers(db: AsyncSession) -> list[ParkedCarrier]:
    """Return every carrier that would lose its payload source when the legacy instances go."""
    rows = (await db.execute(_PARKED_CARRIERS)).mappings().all()
    parked: list[ParkedCarrier] = []
    for row in rows:
        keys = [(row["vrf"] or "", row["prefix"] or "", row["next_hop"] or "")]
        deployed = row["deployed_key"]
        if deployed:
            key = (deployed[0] or "", deployed[1] or "", deployed[2] or "")
            if key not in keys:
                keys.append(key)
        parked.append(ParkedCarrier(row["device_id"], row["id"], tuple(keys)))
    return parked


async def refuse_cutover_while_carriers_are_parked(db: AsyncSession) -> None:
    """Run the drain preflight with workers stopped. Only an empty result opens the window."""
    parked = await parked_static_route_carriers(db)
    if parked:
        logger.error("cutover.drain_preflight_blocked", carriers=len(parked))
        raise CutoverBlocked(parked)
    logger.info("cutover.drain_preflight_clear")


__all__ = [
    "CutoverBlocked",
    "ParkedCarrier",
    "parked_static_route_carriers",
    "refuse_cutover_while_carriers_are_parked",
]
