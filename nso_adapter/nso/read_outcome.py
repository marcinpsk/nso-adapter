# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""The read-outcome vocabulary — one explicit classification of every device-family read.

Historically each read-mirror family re-decided, in its own copy-pasted refresher, what an
empty / absent / failed read from ``network-state-export`` *means* — overloading ``None`` and
bare exceptions. The meaning of a read is actually a small closed set of ground-truth states;
this module names them and classifies a getter's ``dict | None`` / exception result into them
exactly once, so every downstream refresher acts on a uniform 3-case outcome instead of
re-deriving the semantics per family (the READSEM paradigm, ``read_semantics_design_nso.md``).

Ground-truth vs the wire (per-device ``GET .../network-state-export:<family>/device={name}``):

* **Present(data)** — a 200 carrying this device's entry (its child lists may be empty; an
  empty *present* entry is an authoritative "this device has none", which replaces → clears).
* **AbsentAuthoritative** — the device is genuinely absent from a HEALTHY export: for a
  ``pop``-policy (clear-on-None) family, the getter's ``None`` already means "confirmed absent"
  because :meth:`NsoClient._get_device_oper_entry` probed the parent container first. Clearing
  the mirror is correct.
* **Unavailable(reason)** — no authoritative answer: the export is down
  (``NsoExportUnavailableError``), the read errored (5xx / transport / parse), or — for a
  ``present``-policy (keep-on-None) inventory family — a 404 that means unsupported-NED /
  unknown / not-ready rather than "empty". The mirror is KEPT.

``Freshness`` is carried on ``Present`` for the degraded-success policy: until the composite
envelope carries a real per-family status (READSEM end-state C), staleness is approximated from
the snapshot's ``last-updated`` age; ``aged`` still replaces (the export's best-known) but the
refresh is recorded degraded, not a clean success.
"""

from __future__ import annotations

import enum
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from nso_adapter.nso.client import NsoExportUnavailableError


class EmptyPolicy(str, enum.Enum):
    """How a family's ``None`` read (a per-device 404) must be interpreted.

    ``pop`` — clear-on-None: the 16 "config" families. The export omits a synced-but-empty
    device (pop-on-empty), so a container-confirmed 404 is an authoritative "removed" → clear.

    ``present`` — keep-on-None: the interface-ip / interface-attributes inventory families. A
    synced device always returns a present (possibly empty) 200, so a 404 can only be
    unsupported-NED / unknown / not-ready — NOT authoritative emptiness → keep.
    """

    pop = "pop"
    present = "present"


class Freshness(str, enum.Enum):
    fresh = "fresh"  # served from a live/recent read
    aged = "aged"  # last-updated older than the family's staleness horizon (approximation)


class UnavailableReason(str, enum.Enum):
    export_down = "export_down"  # confirmed: parent container 404 → NsoExportUnavailableError
    read_error = "read_error"  # 5xx / transport / parse — no cached answer
    not_authoritative = "not_authoritative"  # keep-on-None family 404 (unsupported / unknown / not-ready)


@dataclass(frozen=True)
class Present:
    """A 200 carrying this device's authoritative entry (child lists may be empty)."""

    data: dict
    freshness: Freshness = Freshness.fresh


@dataclass(frozen=True)
class AbsentAuthoritative:
    """The device is genuinely absent from a healthy export → clear the mirror."""


@dataclass(frozen=True)
class Unavailable:
    """No authoritative answer → keep the last-known mirror rows."""

    reason: UnavailableReason
    # Diagnostic only (exception repr); excluded from equality so tests can assert on reason alone.
    detail: str = field(default="", compare=False)


ReadOutcome = Present | AbsentAuthoritative | Unavailable


async def classify_read(
    get_entry: Callable[[], Awaitable[dict | None]],
    empty_policy: EmptyPolicy,
) -> ReadOutcome:
    """Run a family getter and classify its result into a :data:`ReadOutcome`.

    This is the single boundary where a getter's ``dict | None`` return and its exceptions
    become the explicit vocabulary. The getter is expected to be a
    :meth:`NsoClient.get_*` coroutine that already confirms a bare 404 against the parent
    container (raising :class:`NsoExportUnavailableError` on a real outage) — so its ``None``
    is an authoritative absence for a ``pop`` family, never a masked outage.
    """
    try:
        entry = await get_entry()
    except NsoExportUnavailableError as exc:
        return Unavailable(UnavailableReason.export_down, detail=repr(exc))
    except Exception as exc:  # noqa: BLE001 — any read failure is Unavailable; the mirror is kept
        return Unavailable(UnavailableReason.read_error, detail=repr(exc))

    if entry is not None:
        return Present(entry)
    if empty_policy is EmptyPolicy.pop:
        return AbsentAuthoritative()
    return Unavailable(UnavailableReason.not_authoritative)
