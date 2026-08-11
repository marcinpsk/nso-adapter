# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""What a static-route push's ``deleted_routes`` authority amounts to (#1503 §4.4).

A full-replace push deletes every row its body omits, and the body alone cannot say WHY:
an operator deleting a NetBox route and an operator un-owning it produce the same shrink.
``deleted_routes`` is the authority that separates them — one record per deleted NetBox pk,
carrying the route's triple LINEAGE most-authoritative-first, because the triple the adapter
holds is not necessarily the one NetBox last acknowledged.

The classifier answers, for each requested id, which of three things happened:

``executed``  a removed row carries that ``route_id``. The deletion is real and its removal
              job retracts from the device.
``degraded``  no correlated row, but a ``route_id IS NULL`` row this push removed carries one
              of the id's triples — or the id's lineage is ``unverified`` and the push removed
              an uncorrelated row anyway. The device outcome is a detach either way
              (`authority:399` ratifies exactly that); the classification is what keeps it
              from being SILENT.
``moot``      the id matched nothing and nothing was detached on its behalf.

plus ``uncorrelated``: the triples of the ``route_id IS NULL`` rows this push removed that no
requested id claimed. Reported on every mode — normal, store-only and backfill-only — because
a normal push detaches such a row exactly as a backfill pass prunes one.

**Two ordered passes, and the order is the contract (R8-B4).** Stating the genuine rule and
the equivalence-class rule side by side produces no partition at all for a request like
``{1: [A, C], 2: [C]}`` against rows ``{route_id: 1, triple: A}`` and ``{route_id: NULL,
triple: C}``: id 1 is genuine by the first rule and belongs to C's class by the second. So
``route_id`` binds FIRST and EXCLUSIVELY, and the triple pass runs over what is left. It is
the same precedence :func:`api.static_route._match_payload_to_rows` already applies.

**Classification is per equivalence class, never per request position (R7-M2).** NetBox
dropped ``UniqueConstraint(vrf, prefix, next_hop)`` on ``StaticRoute`` in its migration 0030,
so two deleted pks can legitimately carry one triple against ONE adapter row. A row therefore
classifies EVERY remaining id whose lineage contains its triple, identically. A one-to-one
match would degrade whichever id it reached first and moot the other, which makes the answer
depend on request order.

**Emission is id-oriented, exactly one outcome per requested id (R9-M3).** A lineage
``[A, B]`` against two NULL rows A and B belongs to two classes whose verdicts agree; a
row-oriented implementation emits that id twice, and the plugin's partition validator then
rejects the stored response forever.

Every output is sorted, so two orderings of one request produce byte-identical responses —
and therefore byte-identical stored receipts.
"""

from __future__ import annotations

from dataclasses import dataclass

from nso_adapter.core.static_route_plan import Triple, triple_of


@dataclass(frozen=True)
class DeletionRecord:
    """One deleted NetBox route pk and the triples it may be holding on the adapter.

    *unverified* is DECLARED by the pusher, never inferred from the lineage's shape: a
    verified ``[C, C]`` deduplicates to exactly what a genuinely unverified ``[C]`` produces,
    so the shape carries no information at all (R10-B1).
    """

    route_id: int
    triples: tuple[Triple, ...]
    unverified: bool


@dataclass(frozen=True)
class DeletionPartition:
    """The three lists, the uncorrelated residue, and which rows the ids actually bound.

    *genuine_row_ids* is what the per-object marking is read from: those rows are the ones a
    NetBox deletion authorized, so their carriers are marked ``delete_origin`` and every other
    removed row detaches.
    """

    executed: tuple[int, ...]
    degraded: tuple[int, ...]
    moot: tuple[int, ...]
    uncorrelated: tuple[Triple, ...]
    genuine_row_ids: frozenset[int]


def classify_deletions(records, removed_rows) -> DeletionPartition:
    """Partition *records* over the rows *this* push removes. Pure: it reads and writes nothing.

    *removed_rows* are ``StaticRouteIntent`` rows, pre-deletion, so their ``route_id`` and
    triple are still readable.
    """
    by_route_id = {row.route_id: row for row in removed_rows if row.route_id is not None}

    # Pass 1, by route_id, exclusively: the id and the row both leave the pool.
    genuine = {record.route_id: by_route_id[record.route_id] for record in records if record.route_id in by_route_id}

    # Pass 2, by triple, over the remainder only — and only against the rows that carry no
    # provenance at all, since a row with a route_id was either bound above or belongs to a
    # different NetBox route entirely.
    remaining = [record for record in records if record.route_id not in genuine]
    residue = [row for row in removed_rows if row.route_id is None]

    degraded: set[int] = set()
    claimed: set[int] = set()
    for row in residue:
        triple = triple_of(row)
        equivalence_class = [record.route_id for record in remaining if triple in record.triples]
        if equivalence_class:
            claimed.add(row.id)
            degraded.update(equivalence_class)
    uncorrelated = tuple(sorted(triple_of(row) for row in residue if row.id not in claimed))

    # The unmatched ids, decided against the FINISHED class assignment above rather than
    # against a set this loop is still growing — the residue is a request-wide fact, not a
    # token the first id to reach it consumes.
    moot: set[int] = set()
    conservative: set[int] = set()
    for record in remaining:
        if record.route_id in degraded:
            continue
        # The conservative direction is attribution, not a different device outcome: an
        # unverified id that matched nothing, on a push that detached a row nobody claimed,
        # is recorded against that row rather than left to be mooted silently.
        if record.unverified and uncorrelated:
            conservative.add(record.route_id)
        else:
            moot.add(record.route_id)
    degraded |= conservative

    return DeletionPartition(
        executed=tuple(sorted(genuine)),
        degraded=tuple(sorted(degraded)),
        moot=tuple(sorted(moot)),
        uncorrelated=uncorrelated,
        genuine_row_ids=frozenset(row.id for row in genuine.values()),
    )


__all__ = ["DeletionPartition", "DeletionRecord", "classify_deletions"]
