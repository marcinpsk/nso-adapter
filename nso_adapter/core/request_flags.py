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
# reverse diff against the live device and stripped an IOS route-map filter). Guarded at
# the same enqueue choke point as STORE_ONLY so the safe default holds for every intent
# endpoint without each one threading a flag.
DELETE_ORIGIN: ContextVar[bool] = ContextVar("delete_origin", default=False)

# ``PUSH_SEQ`` carries the plugin's ``X-Push-Seq`` header: the identity of the outbox claim
# this request delivers. It is the key receipt admission dedupes on (#1522 §G2) and the
# provenance recorded on every projection write and on the generation a normal write
# promotes. Request-scoped at the same layer as the two flags above, for the same reason.
PUSH_SEQ: ContextVar[int | None] = ContextVar("push_seq", default=None)

_TRUTHY = frozenset({"1", "true", "yes", "on"})

#: The admissible ``X-Push-Seq`` domain. The upper bound is the BIGINT the receipt and the
#: projection row store it in: a wider value must be refused at the boundary, because the
#: alternative is an asyncpg range error deep inside the mutation — a 500 for a client
#: mistake, after part of the request has already run.
MIN_PUSH_SEQ = 1
MAX_PUSH_SEQ = 2**63 - 1


class InvalidPushSequence(ValueError):
    """The request carried an ``X-Push-Seq`` that cannot identify a claim."""


def parse_store_only(raw: str | None) -> bool:
    """Parse a raw boolean query value (mirrors FastAPI's bool query coercion)."""
    return raw is not None and raw.strip().lower() in _TRUTHY


def parse_push_seq(raw: str | None) -> int | None:
    """Parse the ``X-Push-Seq`` header. Absent → None; present and unusable → raises.

    A PRESENT header that cannot be a claim identity is a client error and is refused, not
    downgraded: silently treating it as absent turns a keyed, replay-protected delivery into
    an unkeyed one, and the plugin's retry then applies a second time under a receipt nobody
    wrote. Absence itself stays legal — the ratified #1503 contract keeps lacp/switchport
    out of the protocol as claim-less direct-apply deliveries.
    """
    if raw is None:
        return None
    try:
        seq = int(raw.strip())
    except ValueError:
        raise InvalidPushSequence("X-Push-Seq must be an integer") from None
    if not MIN_PUSH_SEQ <= seq <= MAX_PUSH_SEQ:
        raise InvalidPushSequence(f"X-Push-Seq must be between {MIN_PUSH_SEQ} and {MAX_PUSH_SEQ}")
    return seq
