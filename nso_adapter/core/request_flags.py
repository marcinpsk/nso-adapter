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

_TRUTHY = frozenset({"1", "true", "yes", "on"})


def parse_store_only(raw: str | None) -> bool:
    """Parse a raw boolean query value (mirrors FastAPI's bool query coercion)."""
    return raw is not None and raw.strip().lower() in _TRUTHY
