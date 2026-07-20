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
    stale = "stale"  # WIRE-DECLARED by the envelope: the export served last-known after a failed extract


class UnavailableReason(str, enum.Enum):
    export_down = "export_down"  # confirmed: parent container 404 → NsoExportUnavailableError
    read_error = "read_error"  # 5xx / transport / parse — no cached answer
    not_authoritative = "not_authoritative"  # keep-on-None family 404 (unsupported / unknown / not-ready)
    # Envelope-declared (READSEM S3) — the wire finally distinguishes what not_authoritative merges:
    unsupported = "unsupported"  # this NED has no reader for the family — keep rows, not degraded
    not_ready = "not_ready"  # no record yet (post-reload / NED remount) — the engine escalates to the action


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


def classify_envelope_section(section: dict | None, empty_policy: EmptyPolicy) -> ReadOutcome:
    """Classify one device-state envelope section into a :data:`ReadOutcome` (READSEM S3).

    The envelope carries the ground truth the legacy wire could not: a per-family
    ``status`` leaf. Classification is therefore a direct mapping — no probes, no
    empty-policy inference at the section level:

    * ``ok`` → :class:`Present` (fresh). RESTCONF omits empty lists, so ok with absent
      list keys IS the authoritative empty — the full-replace materialize path clears.
    * ``stale`` → :class:`Present` with ``Freshness.stale`` — **degraded-success**
      (operator decision): the rows are the export's best-known and replace, but the
      recorded outcome carries the degradation.
    * ``unsupported`` → :class:`Unavailable`(``unsupported``): declared not-authoritative
      absence — keep rows. (The legacy wire conflated this with authoritative emptiness
      and cleared; the envelope ends that.)
    * ``not-ready`` → :class:`Unavailable`(``not_ready``): no record under the current
      mount (post-reload, NED remount). The engine escalates to ``device-state-read run``
      exactly once — the envelope itself never extracts.
    * ``error`` → :class:`Unavailable`(``read_error``) with the wire's ``error-reason``.

    ``section is None`` is DEVICE-level absence (the client already confirmed the
    ``device-state`` container is alive): the one branch where :class:`EmptyPolicy` still
    applies — ``pop`` reads it as the device (and thus its config) being genuinely gone,
    ``present`` keeps rows. This preserves today's device-removed semantics; the policy
    column dissolves entirely at S5.

    An unknown/missing status is never guessed at: ``Unavailable(read_error)``, rows kept.
    """
    if section is None:
        if empty_policy is EmptyPolicy.pop:
            return AbsentAuthoritative()
        return Unavailable(UnavailableReason.not_authoritative)

    status = section.get("status")
    if status == "ok":
        return Present(section)
    if status == "stale":
        return Present(section, Freshness.stale)
    if status == "unsupported":
        return Unavailable(UnavailableReason.unsupported)
    if status == "not-ready":
        return Unavailable(UnavailableReason.not_ready)
    if status == "error":
        return Unavailable(UnavailableReason.read_error, detail=str(section.get("error-reason") or ""))
    return Unavailable(UnavailableReason.read_error, detail=f"unrecognized envelope status {status!r}")
