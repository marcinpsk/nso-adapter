# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""One static-route replacement classifier, one plan — #1396 R2 §3.

Every consumer (the per-scope apply, preview, the atomic path, the removal branches)
calls :func:`build_plan` instead of re-deriving a predicate. Two predicates decide
everything:

* ``FENCE_OPEN(rows)``    — no row of the device carries ``route_id IS NULL``. Only the
  plugin (R3) fills that column, so a shut fence means no row can be correlated with a
  NetBox route pk and no replacement may be claimed.
* ``REPLACEMENT_OPEN(row)`` — the row's proven-deployed triple is not the triple it now
  renders, i.e. an identity edit has not yet been delivered.

The module also owns the static-route clear vocabulary (§4.11): which store fields have a
wire leaf, what "set" means to the renderer, and the ``pending_clear`` carrier's shape.
Both live here so the endpoint, the removal branches and the proof cannot drift apart.
"""

from __future__ import annotations

from typing import NamedTuple

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.get_logger(__name__)

#: A destructive replace whose proof is structurally unavailable must not run (§4.4).
PUT_REFUSED_EVENT = "static_route.put_refused_verify_disabled"

#: Store fields whose clearing must reach the device, in ``_STATE_FIELDS`` order.
#:
#: ``name`` is deliberately absent: it has no wire leaf, so a recorded ``name`` clear could
#: never be delivered or proven and the carrier would live forever (§4.11). Clearing it is a
#: documented no-op — no carrier, no removal job, no ``unproven`` outcome.
SR_CLEAR_FIELDS: tuple[str, ...] = ("interface_next_hop", "next_hop_vrf", "metric", "permanent", "tag")

#: The two halves of ``static_route_intent.pending_clear``.
AUTHORIZED = "authorized"
STORE_ONLY = "store_only"

Triple = tuple[str, str, str]


def triple_of(row) -> Triple:
    """Return the row's current identity as the wire keys the renderer emits."""
    return (row.vrf or "", row.prefix or "", row.next_hop or "")


def as_triple(value) -> Triple | None:
    """Normalize a stored ``deployed_key`` (a 3-element JSON array) into a tuple."""
    if not value:
        return None
    vrf, prefix, next_hop = value
    return (vrf or "", prefix or "", next_hop or "")


def null_route_id_count(rows) -> int:
    return sum(1 for r in rows if r.route_id is None)


def fence_open(rows) -> bool:
    """``FENCE_OPEN`` — no row of the device is missing its NetBox route pk.

    Evaluated on whatever row set the caller holds; the intent endpoint deliberately
    passes the PRE-mutation rows, so the very push that fills the last NULL does not
    already claim deletion authority for a triple nothing ever correlated.
    """
    return null_route_id_count(rows) == 0


def replacement_open(row) -> bool:
    """``REPLACEMENT_OPEN`` — a proven predecessor triple the row no longer renders.

    Element-wise, not identity: an ``A -> B -> A`` round trip leaves ``deployed_key``
    equal to the current triple as a *value* and is not open.
    """
    deployed = as_triple(row.deployed_key)
    return deployed is not None and deployed != triple_of(row)


# ── the static-route clear vocabulary (§4.11) ────────────────────────────────


def wire_set(field: str, value) -> bool:
    """Whether ``static_route_entry`` would emit *field* at *value*.

    The renderer is the definition of "set" here, never falsiness. It emits ``metric: 0``
    and ``tag: 0`` as real values, and never emits ``permanent: false`` (G27) — so a
    ``permanent`` flipped ``True -> False`` is wire-unset and counts as a clear for static
    routes, while ``metric 10 -> 0`` does not.
    """
    if field in ("metric", "tag"):
        return value is not None
    return bool(value)


def sr_is_cleared(field: str, before, after) -> bool:
    """Whether *field* went from wire-set to wire-unset on this push."""
    return wire_set(field, before) and not wire_set(field, after)


def pending_clear_fields(carrier: dict | None) -> set[str]:
    """Return every field name the carrier still holds, from either half.

    Either half being non-empty blocks a proven ``in_sync``: a stale leaf is a stale leaf
    regardless of which push recorded it.
    """
    if not carrier:
        return set()
    return {*(carrier.get(AUTHORIZED) or ()), *(carrier.get(STORE_ONLY) or ())}


def authorized_clear_fields(carrier: dict | None) -> set[str]:
    """Return the half a networked removal may deliver — never ``store_only``.

    A ``?store_only=true`` push may mutate the intent store but must never cause a device
    write. Without this split an unrelated networked removal would read the carrier and
    delete a leaf the store-only resync merely recorded.
    """
    if not carrier:
        return set()
    return {*(carrier.get(AUTHORIZED) or ())}


def update_pending_clear(row, *, cleared: set[str], reset: set[str], store_only: bool) -> None:
    """Fold this push's clears and re-sets into ``row.pending_clear``.

    *store_only* routes new clears into the half a networked removal must ignore: a
    ``?store_only=true`` request may mutate the intent store but must never cause a device
    write, and the plugin's resync genuinely re-pushes nullable values — so an undifferentiated
    carrier would let an unrelated networked removal deliver a clear no operator authorized.

    An authorized clear promotes out of ``store_only``; a store-only push never demotes one
    that is already authorized. A field re-set to a wire-emitting value leaves both halves.
    """
    carrier = row.pending_clear or {}
    authorized = {*(carrier.get(AUTHORIZED) or ())} - reset
    store = {*(carrier.get(STORE_ONLY) or ())} - reset
    if store_only:
        store |= cleared - authorized
    else:
        authorized |= cleared
        store -= cleared
    row.pending_clear = {AUTHORIZED: sorted(authorized), STORE_ONLY: sorted(store)} if (authorized or store) else None


# ── the plan ─────────────────────────────────────────────────────────────────


class SrCas(NamedTuple):
    """One row's compare-and-set coordinates, snapshotted BEFORE the network call."""

    row_id: int
    route_id: int | None
    sent_triple: Triple
    expected_old: list | None


class SrPlan(NamedTuple):
    """The whole static-route apply decision. Read-only: no HTTP, no writes."""

    #: ``"PUT"`` (guarded replace) or ``"PATCH"`` (today's merge).
    mode: str
    #: The rows the body is built from AND the rows every caller stamps. In ``PUT`` mode
    #: every accepted row of the device, force-independent — a body built from the eligible
    #: subset retracts every accepted-and-clean route, and an identity-edited row keeps its
    #: ``last_apply_at`` so ``force=False`` would filter out the very row needing replacement.
    rows: list
    #: Keys the collateral guard may see disappear from the service. Empty in ``PATCH``.
    allowed: set[Triple]
    #: Every tombstone row of the device — unconsumed is the same as present.
    tombstones: list
    #: Per-row CAS coordinates for §4.6.
    cas: list[SrCas]
    #: Highest tombstone id at plan time; the CAS fallback only considers ids above it.
    tombstone_id_watermark: int

    @property
    def tombstone_ids(self) -> list[int]:
        return [t.id for t in self.tombstones]


def _verify_after_apply() -> bool:
    """Read the flag live — it is a module constant tests flip, not a config object."""
    from nso_adapter.nso import apply as nso_apply

    return nso_apply.VERIFY_AFTER_APPLY


async def build_plan(db: AsyncSession, device, *, eligible_rows: list) -> SrPlan:
    """Classify the device's static-route apply and snapshot everything it depends on.

    Callers derive scope execution, ``any_eligible``, atomic admission, row stamping and
    the per-route results from ``plan.rows`` — never from the old eligible list. Deriving
    ``any_eligible`` from the eligible list lets a ``force=False`` apply take the all-zero
    early success AFTER a real PUT.
    """
    from nso_adapter.store.models import StaticRouteIntent, StaticRouteTombstone

    device_id = device.id
    all_rows = list(
        (
            await db.execute(
                select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id).order_by(StaticRouteIntent.id)
            )
        )
        .scalars()
        .all()
    )
    tombstones = list(
        (
            await db.execute(
                select(StaticRouteTombstone)
                .where(StaticRouteTombstone.device_id == device_id)
                .order_by(StaticRouteTombstone.id)
            )
        )
        .scalars()
        .all()
    )

    accepted = [r for r in all_rows if r.accepted_at is not None]
    open_fence = fence_open(all_rows)
    wants_put = open_fence and any(replacement_open(r) for r in accepted)

    mode = "PATCH"
    if wants_put:
        if _verify_after_apply():
            mode = "PUT"
        else:
            # A destructive replace whose proof is structurally unavailable must not run:
            # the merge-PATCH that follows must also not CAS, or it closes the replacement
            # while the predecessor is still on the device.
            logger.warning(PUT_REFUSED_EVENT, device_id=device_id)

    rows = accepted if mode == "PUT" else list(eligible_rows)

    allowed: set[Triple] = set()
    if mode == "PUT":
        for row in rows:
            deployed = as_triple(row.deployed_key)
            if deployed is not None and deployed != triple_of(row):
                allowed.add(deployed)
        # X4 belt: rev 4.1's body retains every unconsumed tombstone's still-present keys
        # verbatim, so those keys are re-asserted and cannot be orphans. Kept because the
        # authority names it, not because it is reachable.
        for tomb in tombstones:
            allowed.add((tomb.vrf or "", tomb.prefix or "", tomb.next_hop or ""))
            deployed = as_triple(tomb.deployed_key)
            if deployed is not None:
                allowed.add(deployed)

    cas = [SrCas(row.id, row.route_id, triple_of(row), row.deployed_key) for row in rows]
    watermark = max((t.id for t in tombstones), default=0)
    return SrPlan(mode, rows, allowed, tombstones, cas, watermark)


__all__: list[str] = [
    "AUTHORIZED",
    "SR_CLEAR_FIELDS",
    "STORE_ONLY",
    "SrCas",
    "SrPlan",
    "as_triple",
    "authorized_clear_fields",
    "build_plan",
    "fence_open",
    "null_route_id_count",
    "pending_clear_fields",
    "replacement_open",
    "sr_is_cleared",
    "triple_of",
    "update_pending_clear",
    "wire_set",
]
