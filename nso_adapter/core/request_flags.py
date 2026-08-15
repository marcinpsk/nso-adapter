# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Request-scoped flags threaded from the HTTP layer to the core job-enqueue choke points.

``STORE_ONLY`` carries the in-flight request's ``?store_only=true`` query flag (set by the
middleware in ``main``). A store-only request may mutate the adapter's intent STORE but must
never cause a device write: :func:`core.removal.enqueue_removal` and
:func:`core.apply.enqueue_apply` — the only two places an intent PUT can create a
device-touching job — skip when it is set. This is what makes the plugin's intent re-sync
safe: re-pushing a reduced owned snapshot reconciles the store without the shrink
auto-enqueueing a removal that would retract FASTMAP-owned config from the real device
(tracker #103, the ra1.lab logging incident).

A contextvar guarded at the enqueue layer (not a per-endpoint parameter) so the guarantee
holds for every intent endpoint — present and future — without each one having to remember
to thread a flag.
"""

from __future__ import annotations

from contextvars import ContextVar

STORE_ONLY: ContextVar[bool] = ContextVar("store_only", default=False)

# ``DELETE_ORIGIN`` carries ``?delete_origin=true``: the plugin stamps it on intent pushes
# that originate from a NetBox OBJECT DELETION, where the operator's intent is "remove
# this from the device". Every UNMARKED intent shrink is an un-own ("NetBox stops
# governing") and its removal job runs as a DETACH — no-networking replace + sync-from,
# device untouched (tracker #106: a real PUT-replace of an ADOPTED entry plays FASTMAP's
# reverse diff against the live device and stripped an IOS route-map filter). Read ONCE per
# request, by :func:`request_marking`, and passed on as an explicit argument from there:
# a removal job's marking is part of its identity, and a request can produce one job per
# marking (#1503 §4.5).
DELETE_ORIGIN: ContextVar[bool] = ContextVar("delete_origin", default=False)

# ``BACKFILL_ONLY`` carries ``?backfill_only=true``: the pusher is opening a device's
# replacement fence, not delivering content. The pass adopts the ``route_id`` of every row the
# payload still names, prunes the uncorrelated NULL-id rows that hold the fence shut, and does
# nothing else — no content write, no tombstone, no job. It exists because an ordinary push
# that omits a deleted route destroys the before-image that route's pending deletion needs
# (#1503 §4.4, OQ-O-8). Only the static-route stream implements it; any other in-protocol
# delivery carrying it is refused at the boundary, never silently treated as an ordinary push.
BACKFILL_ONLY: ContextVar[bool] = ContextVar("backfill_only", default=False)

#: The deletion provenance one removal job carries. ``delete_origin`` retracts from the
#: device; ``detach`` un-owns without touching it. The values are also the
#: ``static_route_tombstone.marking`` domain, one source of truth for both.
DELETE_ORIGIN_MARKING = "delete_origin"
DETACH_MARKING = "detach"
REMOVAL_MARKINGS: tuple[str, ...] = (DELETE_ORIGIN_MARKING, DETACH_MARKING)

_TRUTHY = frozenset({"1", "true", "yes", "on"})
_FALSY = frozenset({"0", "false", "no", "off"})

#: The admissible ``X-Push-Seq`` domain, and the bounds DECLARED on every in-protocol intent
#: PUT (:func:`api.intent_push.get_intent_delivery`). The upper bound is the BIGINT the
#: receipt and the projection row store it in: a wider value must be refused at the boundary,
#: because the alternative is an asyncpg range error deep inside the mutation — a 500 for a
#: client mistake, after part of the request has already run.
MIN_PUSH_SEQ = 1
MAX_PUSH_SEQ = 2**63 - 1


def parse_request_flag(raw: str | None) -> bool:
    """Parse one request-mode boolean, or refuse an unknown spelling."""
    if raw is None:
        return False
    normalized = raw.strip().lower()
    if normalized in _TRUTHY:
        return True
    if normalized in _FALSY:
        return False
    raise ValueError("invalid boolean query value")


def request_marking() -> str:
    """Return the deletion provenance THIS request marks the rows it deletes with.

    The one read of :data:`DELETE_ORIGIN` on the removal path. Everything downstream takes
    the marking as an argument, so a request that deletes at both markings (§4.5's
    per-object static routes) can build one job per marking instead of one job whose
    job-wide ``detach`` flag would misdeliver half of them.
    """
    return DELETE_ORIGIN_MARKING if DELETE_ORIGIN.get() else DETACH_MARKING
