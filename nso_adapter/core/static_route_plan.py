# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""One static-route replacement classifier, one plan — #1396 R2 §3.

Generation creation classifies the store snapshot and records the result; workers and the
preview hydrate that result, so nothing re-classifies live rows at execution.
Two predicates decide the apply plan:

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

from copy import deepcopy
from typing import NamedTuple

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.get_logger(__name__)

#: A destructive replace whose proof is structurally unavailable must not run (§4.4). Logged
#: by the worker, which refuses the job rather than sending a document it cannot prove.
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

#: Store field → the wire leaf :func:`nso_adapter.nso.apply.static_route_entry` renders it as.
#: The WRITE side (a removal's leaf-level clear overlay) deletes exactly these leaves; the READ
#: side (proof) consumes a carrier entry only once the same leaf is gone or neutral. ``name`` is
#: absent by design — no wire leaf, so nothing to delete and nothing to prove.
CLEAR_WIRE_LEAF: dict[str, str] = {
    "interface_next_hop": "interface-next-hop",
    "next_hop_vrf": "next-hop-vrf",
    "metric": "metric",
    "permanent": "permanent",
    "tag": "tag",
}

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


def clears_suppressed(context: dict) -> bool:
    """Detach and deferred-retract removals never deliver pending clears."""
    return bool(context.get("detach") or context.get("retract_deferred"))


def candidate_clear_fields(row) -> tuple[str, ...]:
    """Return the still-wire-unset authorized clear fields of one row (``()`` for a non-candidate).

    The ONE per-row rule both the creation-time classifier and the live reissue path apply.
    A replacement-open row is skipped — it waits for the Apply PUT, whose store-rendered
    body omits the leaf anyway. Only the ``authorized`` carrier half is visible.
    """
    fields = authorized_clear_fields(row.pending_clear)
    if not fields or replacement_open(row):
        return ()
    return tuple(sorted(field for field in fields if not wire_set(field, getattr(row, field, None))))


def leaf_is_neutral(field: str, entry: dict) -> bool:
    """Whether *entry* proves *field* is no longer set on the device (§4.11's table).

    Never falsiness. ``metric: 0`` and ``tag: 0`` are real values the renderer emits, so a
    generic truthiness check would empty the carrier while the old value is still live —
    the same false green the carrier exists to prevent. ``permanent`` is the one field where
    ``false`` IS neutral, because the renderer never emits it (G27).

    Shared by both proof planes on purpose: the apply's post-write evidence and the networked
    removal's (C2.11 / C4.32 are the same rule seen from two paths), so a per-path copy could
    not drift into treating ``0`` as neutral on one of them.
    """
    leaf = CLEAR_WIRE_LEAF[field]
    if leaf not in entry:
        return True
    value = entry[leaf]
    if value is None:  # an explicit null is the export's spelling of "absent"
        return True
    if field == "permanent":
        return value is False or str(value).strip().lower() == "false"
    if field in ("interface_next_hop", "next_hop_vrf"):
        return str(value) == ""
    return False


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


class SrClear(NamedTuple):
    """One creation-time clear decision for a surviving route."""

    row_id: int
    key: Triple
    fields: tuple[str, ...]


class SrRemovalPlan(NamedTuple):
    """The immutable store-side classification a static-route removal executes."""

    authorized: frozenset[Triple]
    claimed: frozenset[Triple]
    tombstone_ids: tuple[int, ...]
    clears: tuple[SrClear, ...]
    reclaimed: tuple[Triple, ...]


def classify_apply_plan(all_rows: list, tombstones: list, *, device_id: int) -> SrPlan:
    """Classify one immutable row snapshot into the static-route apply plan.

    Every send is the device's whole document, so the body is ALWAYS the accepted rows and
    the authority always names the predecessor keys the send drops. ``mode`` no longer picks
    a transport — there is one — it records whether this document DELIVERS A REPLACEMENT, so
    the worker can refuse to send one whose proof is structurally unavailable
    (``_refuse_unverifiable_recorded_put``).

    ``FENCE_OPEN`` no longer gates that record. It gated the PATCH-versus-PUT choice, and a
    shut fence therefore used to leave a predecessor on the device; one document drops it
    either way, so making the record fence-dependent would only drop the PROOF requirement on
    exactly the devices least able to justify the write. The fence's real job is unchanged and
    lives at the intent endpoint, where it decides whether a removed row earns a deletion
    record at all.

    Callers derive scope execution, ``any_eligible``, row stamping and the per-route results
    from ``plan.rows``, never from an eligible list: an eligible-only body would retract
    every accepted-and-clean route.
    """
    accepted = [r for r in all_rows if r.accepted_at is not None]
    mode = "PUT" if any(replacement_open(r) for r in accepted) else "PATCH"
    rows = accepted

    allowed: set[Triple] = set()
    for row in rows:
        deployed = as_triple(row.deployed_key)
        if deployed is not None and deployed != triple_of(row):
            allowed.add(deployed)
    # X4 belt: the body retains every unconsumed tombstone's still-present keys verbatim, so
    # those keys are re-asserted and cannot be orphans. Kept because the authority names it,
    # not because it is reachable.
    for tomb in tombstones:
        allowed.add((tomb.vrf or "", tomb.prefix or "", tomb.next_hop or ""))
        deployed = as_triple(tomb.deployed_key)
        if deployed is not None:
            allowed.add(deployed)

    cas = [SrCas(row.id, row.route_id, triple_of(row), row.deployed_key) for row in rows]
    watermark = max((t.id for t in tombstones), default=0)
    return SrPlan(mode, rows, allowed, tombstones, cas, watermark)


def validate_removal_authority(value: object) -> None:
    """Refuse removal authority that does not name its section and guarded list.

    The scope vocabulary is the section registry. A scope outside it reads as an EMPTY
    authority downstream, because each reader indexes its own section by name, so a
    near-miss spelling would guard nothing while looking populated.
    """
    from nso_adapter.core.projection import projection_sections

    if not isinstance(value, dict) or any(not isinstance(labels, dict) for labels in value.values()):
        raise ValueError("allowed_removal_keys must be scope-qualified")
    unknown = sorted(set(value) - projection_sections())
    if unknown:
        raise ValueError(f"unknown removal-authority scope {unknown[0]!r}")


def _removal_keys(value) -> set[Triple]:
    """Normalize the generation's guarded static-route key set.

    The authority is SCOPE-QUALIFIED (``{section: {list: keys}}``) because the collateral
    guard is device-wide and two families both have a ``host`` list. Through :func:`_sr_key`:
    these entries come from the stored document too, so a malformed one must name itself
    rather than raise ``as_triple``'s bare unpack error.
    """
    validate_removal_authority(value)
    section = value.get("static_route") or {}
    return {_sr_key(raw) for raw in section.get("route") or ()}


def promotion_removal_keys(removed_rows: dict[str, list[dict]]) -> dict[str, list[list[str]]]:
    """Return every current and deployed key authorized by promoted row removals."""
    keys: set[Triple] = set()
    for row in removed_rows.get("static_route_intent", []):
        keys.add((row.get("vrf") or "", row.get("prefix") or "", row.get("next_hop") or ""))
        if (deployed := as_triple(row.get("deployed_key"))) is not None:
            keys.add(deployed)
    return {"route": [list(key) for key in sorted(keys)]} if keys else {}


def scope_qualified(scope: str, removed: dict | None) -> dict:
    """Return one scope's guarded keys under the device-wide authority's own shape."""
    return {scope: dict(removed)} if removed else {}


def classify_removal_plan(
    rows: list,
    tombstones: list,
    *,
    allowed_removal_keys: dict,
    context: dict,
) -> SrRemovalPlan:
    """Classify removal authority and clear carriers from one creation-time snapshot."""
    authorized = _removal_keys(allowed_removal_keys)
    for tombstone in tombstones:
        authorized.add(triple_of(tombstone))
        if (deployed := as_triple(tombstone.deployed_key)) is not None:
            authorized.add(deployed)

    # Supersession is by a RENDERED key only, never by a deployed-only claim. A row whose
    # identity moved from K to L still names K as its predecessor, and subtracting K would
    # consume a carrier that owes K's cleanup as `superseded` while K is still on the
    # service. `classify_apply_plan` already treats a predecessor key this way.
    rendered: set[Triple] = {triple_of(row) for row in rows}
    claimed: set[Triple] = set(rendered)
    for row in rows:
        if (deployed := as_triple(row.deployed_key)) is not None:
            claimed.add(deployed)
    reclaimed = tuple(sorted(authorized & rendered))
    authorized -= rendered

    clears: list[SrClear] = []
    if not clears_suppressed(context):
        for row in rows:
            if still_unset := candidate_clear_fields(row):
                clears.append(SrClear(row.id, triple_of(row), still_unset))
    return SrRemovalPlan(
        frozenset(authorized),
        frozenset(claimed),
        tuple(tombstone.id for tombstone in tombstones),
        tuple(clears),
        reclaimed,
    )


def _serialize_apply_plan(plan: SrPlan) -> dict:
    return {
        "mode": plan.mode,
        "row_ids": [row.id for row in plan.rows],
        "allowed_removal_keys": [list(key) for key in sorted(plan.allowed)],
        "tombstone_ids": plan.tombstone_ids,
        "cas": [
            {
                "row_id": item.row_id,
                "route_id": item.route_id,
                "sent_triple": list(item.sent_triple),
                "expected_old": item.expected_old,
            }
            for item in plan.cas
        ],
        "tombstone_id_watermark": plan.tombstone_id_watermark,
    }


def _serialize_removal_plan(plan: SrRemovalPlan) -> dict:
    return {
        "authorized_removal_keys": [list(key) for key in sorted(plan.authorized)],
        "claimed_keys": [list(key) for key in sorted(plan.claimed)],
        "tombstone_ids": list(plan.tombstone_ids),
        "candidate_clears": [
            {"row_id": clear.row_id, "key": list(clear.key), "fields": list(clear.fields)} for clear in plan.clears
        ],
        "reclaimed_keys": [list(key) for key in plan.reclaimed],
    }


def freeze_static_route_proof(tables: dict[str, list[dict]], *, device_id: int, context: dict) -> dict:
    """Freeze the APPLY PLAN of a static-route fragment's own rows and carriers.

    Pure: the plan is classified from the tables the authorization serialized, so the mode,
    the selected rows, the CAS coordinates and the carrier watermark are properties of that
    fragment and not of whatever the store holds when a worker gets round to it.
    """
    from nso_adapter.core.projection import EXECUTION_KEY, hydrate_section
    from nso_adapter.store.models import StaticRouteIntent, StaticRouteTombstone

    hydrated = hydrate_section({"static_route": {**tables, EXECUTION_KEY: {"context": context}}}, "static_route")
    rows = hydrated.get(StaticRouteIntent, [])
    tombstones = hydrated.get(StaticRouteTombstone, [])
    return {"apply": _serialize_apply_plan(classify_apply_plan(rows, tombstones, device_id=device_id))}


def prune_apply_plan(plan: dict, rows: list[dict], tombstones: list[dict]) -> dict:
    """Rebuild the CARRIER-derived halves of a frozen apply plan over the surviving carriers.

    Everything else is left byte for byte: the mode, the selected row ids and the CAS
    coordinates are properties of the fragment's intent rows, which a consumed carrier does
    not touch.
    """
    allowed: set[Triple] = set()
    selected = set(plan["row_ids"])
    for row in rows:
        if row.get("id") not in selected:
            continue
        deployed = as_triple(row.get("deployed_key"))
        if deployed is not None and deployed != _row_triple(row):
            allowed.add(deployed)
    for tomb in tombstones:
        allowed.add(_row_triple(tomb))
        deployed = as_triple(tomb.get("deployed_key"))
        if deployed is not None:
            allowed.add(deployed)
    return {
        **plan,
        "allowed_removal_keys": [list(key) for key in sorted(allowed)],
        "tombstone_ids": [tomb["id"] for tomb in tombstones],
        "tombstone_id_watermark": max((tomb["id"] for tomb in tombstones), default=0),
    }


def _row_triple(row: dict) -> Triple:
    return (row.get("vrf") or "", row.get("prefix") or "", row.get("next_hop") or "")


def extend_apply_plan(desired: dict | None, source: dict, retained_rows: list[dict]) -> dict:
    """Add the retained rows' plan entries, taken from the fragment they were retained FROM."""
    if desired is None:
        raise ValueError("a retained static-route fragment carries no apply plan to extend")
    if not retained_rows:
        return desired
    source_cas = {item["row_id"]: item for item in source.get("cas") or []}
    by_id = {row_id: None for row_id in desired["row_ids"]}
    cas = {item["row_id"]: item for item in desired["cas"]}
    allowed = {tuple(key) for key in desired["allowed_removal_keys"]}
    mode = desired["mode"]
    for row in retained_rows:
        row_id = row.get("id")
        if row_id in by_id:
            continue
        item = source_cas.get(row_id)
        if item is None:
            raise ValueError(f"retained static-route row {row_id} has no recorded CAS coordinates")
        by_id[row_id] = None
        cas[row_id] = deepcopy(item)
        deployed = as_triple(row.get("deployed_key"))
        if deployed is not None and deployed != _row_triple(row):
            allowed.add(deployed)
            mode = "PUT"
    ordered = sorted(by_id)
    return {
        **desired,
        "mode": mode,
        "row_ids": ordered,
        "cas": [cas[row_id] for row_id in ordered],
        "allowed_removal_keys": [list(key) for key in sorted(allowed)],
    }


async def build_static_route_operation(
    db: AsyncSession,
    device_id: int,
    document: dict,
    *,
    removal_context: dict | None,
    allowed_removal_keys: dict,
    tombstone_ids: tuple[int, ...],
) -> dict:
    """Build the OPERATION plane of a static-route removal from its section.

    The operation plane belongs to one deployment and to no fragment: the removal
    classification, and the carriers this operation selects. The selected carriers are read
    by primary key from the carrier table, scoped to the device — the only creation-time
    lifecycle read a removal makes, and it promotes nothing.
    """
    from nso_adapter.core.projection import hydrate_section
    from nso_adapter.store.models import StaticRouteIntent, StaticRouteTombstone

    section = document.get("static_route")
    if section is None:
        raise ValueError("a static_route removal generation carries no static_route section to classify")
    hydrated = hydrate_section(document, "static_route")
    rows = hydrated.get(StaticRouteIntent, [])
    selected: list = []
    if tombstone_ids:
        selected = list(
            (
                await db.execute(
                    select(StaticRouteTombstone)
                    .where(
                        StaticRouteTombstone.device_id == device_id,
                        StaticRouteTombstone.id.in_(list(tombstone_ids)),
                    )
                    .order_by(StaticRouteTombstone.id)
                )
            )
            .scalars()
            .all()
        )
        if {row.id for row in selected} != set(tombstone_ids):
            raise RuntimeError(f"static_route removal references missing tombstones {sorted(tombstone_ids)}")
    plan = classify_removal_plan(
        rows,
        selected,
        allowed_removal_keys=allowed_removal_keys,
        context=removal_context or {},
    )
    return {"removal": _serialize_removal_plan(plan), "tombstone_ids": sorted(tombstone_ids)}


def _recorded_apply(document: dict) -> dict | None:
    from nso_adapter.core.projection import section_proof

    return (section_proof(document, "static_route") or {}).get("apply")


def recorded_static_route_apply_mode(document: dict) -> str | None:
    """Return the generation's recorded apply mode, or None without this section."""
    if "static_route" not in document:
        return None
    record = _recorded_apply(document)
    mode = record.get("mode") if isinstance(record, dict) else None
    if mode not in {"PATCH", "PUT"}:
        raise ValueError("document section 'static_route' has an invalid recorded apply mode")
    return mode


def hydrate_static_route_apply_plan(document: dict) -> SrPlan:
    """Hydrate the immutable apply plan from a generation document."""
    from nso_adapter.core.projection import hydrate_section, section_context
    from nso_adapter.store.models import StaticRouteIntent, StaticRouteTombstone

    # Validated HERE, before any device I/O: a pre-contract section with a valid plan would
    # otherwise hydrate and only fail at the encode site.
    section_context(document, "static_route")
    record = _recorded_apply(document)
    required = {"mode", "row_ids", "allowed_removal_keys", "tombstone_ids", "cas", "tombstone_id_watermark"}
    if not isinstance(record, dict) or set(record) != required or record.get("mode") not in {"PATCH", "PUT"}:
        raise ValueError("document section 'static_route' has an invalid recorded apply plan")
    hydrated = hydrate_section(document, "static_route")
    rows_by_id = {row.id: row for row in hydrated.get(StaticRouteIntent, [])}
    tombstones_by_id = {row.id: row for row in hydrated.get(StaticRouteTombstone, [])}
    row_ids = record["row_ids"]
    tombstone_ids = record["tombstone_ids"]
    if not isinstance(row_ids, list) or any(row_id not in rows_by_id for row_id in row_ids):
        raise ValueError("document section 'static_route' apply plan does not match its rows")
    if not isinstance(tombstone_ids, list) or any(row_id not in tombstones_by_id for row_id in tombstone_ids):
        raise ValueError("document section 'static_route' apply plan does not match its tombstones")
    selected_ids = set(row_ids)
    rows = [rows_by_id[row_id] for row_id in row_ids]
    cas = [
        SrCas(
            item["row_id"],
            item["route_id"],
            _sr_key(item["sent_triple"]),
            item["expected_old"],
        )
        for item in record["cas"]
        if item.get("row_id") in selected_ids
    ]
    if {item.row_id for item in cas} != {row.id for row in rows}:
        raise ValueError("document section 'static_route' apply plan has invalid CAS coordinates")
    return SrPlan(
        record["mode"],
        rows,
        {_sr_key(key) for key in record["allowed_removal_keys"]},
        [tombstones_by_id[row_id] for row_id in tombstone_ids],
        cas,
        record["tombstone_id_watermark"],
    )


def _certify_clears(document: dict, clears: tuple[SrClear, ...]) -> None:
    """Refuse a recorded clear the document's own rows do not describe.

    Checking the outer keys only let a plan name any row id, any key and any field: the clear
    is discharged against the row it names and its fields are what settlement proves gone, so
    a mismatched entry retires an obligation the document never carried. Each clear must name
    a row THIS document carries, at the triple that row renders, over fields that row's own
    authorized carrier holds wire-unset.
    """
    if not clears:
        return
    rows = {row.get("id"): row for row in (document.get("static_route") or {}).get("static_route_intent", [])}
    for clear in clears:
        row = rows.get(clear.row_id)
        if row is None:
            raise ValueError(
                f"recorded static-route clear names row {clear.row_id}, which this document does not carry"
            )
        if clear.key != _row_triple(row):
            raise ValueError(
                f"recorded static-route clear for row {clear.row_id} names key {list(clear.key)}, "
                f"which is not the {list(_row_triple(row))} that row renders"
            )
        authorized = authorized_clear_fields(row.get("pending_clear"))
        unset = {field for field in authorized if not wire_set(field, row.get(field))}
        if not set(clear.fields) <= unset:
            raise ValueError(
                f"recorded static-route clear for row {clear.row_id} names fields "
                f"{sorted(set(clear.fields) - unset)} its carrier does not hold wire-unset"
            )


def _sr_key(value) -> Triple:
    """Return the execution key *value* names, naming a malformed one.

    The length is checked here because ``as_triple`` UNPACKS: a two-element key raised
    "not enough values to unpack" and said nothing about the document that carried it.
    """
    key = as_triple(value) if isinstance(value, (list, tuple)) and len(value) == 3 else None
    if key is None:
        raise ValueError(f"a static-route execution key must contain three values; got {value!r}")
    return key


def hydrate_static_route_removal_plan(document: dict) -> SrRemovalPlan:
    """Hydrate the immutable removal classification from a generation document."""
    from nso_adapter.core.projection import section_context, section_operation

    section_context(document, "static_route")
    record = section_operation(document, "static_route").get("removal")
    required = {"authorized_removal_keys", "claimed_keys", "tombstone_ids", "candidate_clears", "reclaimed_keys"}
    if not isinstance(record, dict) or set(record) != required:
        raise ValueError("document section 'static_route' has an invalid recorded removal plan")
    clears = tuple(
        SrClear(item["row_id"], _sr_key(item["key"]), tuple(item["fields"])) for item in record["candidate_clears"]
    )
    _certify_clears(document, clears)
    return SrRemovalPlan(
        frozenset(_sr_key(key) for key in record["authorized_removal_keys"]),
        frozenset(_sr_key(key) for key in record["claimed_keys"]),
        tuple(record["tombstone_ids"]),
        clears,
        tuple(_sr_key(key) for key in record["reclaimed_keys"]),
    )


__all__: list[str] = [
    "AUTHORIZED",
    "CLEAR_WIRE_LEAF",
    "SR_CLEAR_FIELDS",
    "STORE_ONLY",
    "SrCas",
    "SrClear",
    "SrPlan",
    "SrRemovalPlan",
    "as_triple",
    "authorized_clear_fields",
    "build_static_route_operation",
    "candidate_clear_fields",
    "classify_apply_plan",
    "classify_removal_plan",
    "clears_suppressed",
    "extend_apply_plan",
    "fence_open",
    "freeze_static_route_proof",
    "hydrate_static_route_apply_plan",
    "hydrate_static_route_removal_plan",
    "leaf_is_neutral",
    "null_route_id_count",
    "pending_clear_fields",
    "promotion_removal_keys",
    "prune_apply_plan",
    "recorded_static_route_apply_mode",
    "replacement_open",
    "sr_is_cleared",
    "triple_of",
    "update_pending_clear",
    "wire_set",
]
