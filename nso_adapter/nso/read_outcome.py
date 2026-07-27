# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""The read-outcome vocabulary — one explicit classification of every device-family read.

Historically each read-mirror family re-decided, in its own copy-pasted refresher, what an
empty / absent / failed read from ``network-state-export`` *means* — overloading ``None`` and
bare exceptions. The meaning of a read is a small closed set of ground-truth states; this module
names them and classifies one ``device-state`` envelope section into them exactly once, so every
downstream refresher acts on a uniform outcome instead of re-deriving the semantics per family
(the READSEM paradigm, ``read_semantics_design_nso.md``).

The classification input is a per-family **envelope section** carrying an explicit ``status``
leaf (``ok|stale|unsupported|not-ready|error``) — the ground truth the legacy 200/404 wire could
not carry. Classification is therefore a direct mapping, no inference:

* **Present(data)** — ``status=ok`` (or ``stale`` — degraded-success). RESTCONF omits empty
  lists, so ``ok`` with the family's list keys absent IS an authoritative "this device has none",
  which materializes as a clear (replace with nothing).
* **AbsentAuthoritative** — the device genuinely has none of this family and clearing the mirror
  is correct. In the envelope world this is expressed via ``Present`` with an empty payload; the
  type remains a first-class outcome the executor + outcome store still handle.
* **Unavailable(reason)** — no authoritative answer: the export is down
  (``NsoExportUnavailableError``), the read errored (``error`` / 5xx / transport / parse), the NED
  has no reader (``unsupported``), the record is not yet warmed (``not_ready`` → the engine
  escalates to the action), or the device is genuinely absent from the export
  (``not_authoritative`` — section None; READSEM S5 retired the per-family pop/present policy, so
  device-absence now KEEPS the last-known rows uniformly). The mirror is KEPT.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field


class Freshness(str, enum.Enum):
    fresh = "fresh"  # served from a live/recent read
    stale = "stale"  # WIRE-DECLARED by the envelope: the export served last-known after a failed extract


class UnavailableReason(str, enum.Enum):
    export_down = "export_down"  # confirmed: parent container 404 → NsoExportUnavailableError
    read_error = "read_error"  # 5xx / transport / parse — no cached answer
    not_authoritative = "not_authoritative"  # device absent from the export (section None) — keep rows
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


def classify_envelope_section(section: dict | None) -> ReadOutcome:
    """Classify one device-state envelope section into a :data:`ReadOutcome` (READSEM S3/S5).

    The envelope carries the ground truth the legacy wire could not: a per-family
    ``status`` leaf. Classification is a direct mapping — no probes, no empty-policy inference:

    * ``ok`` → :class:`Present` (fresh). RESTCONF omits empty lists, so ok with absent
      list keys IS the authoritative empty — the full-replace materialize path clears.
    * ``stale`` → :class:`Present` with ``Freshness.stale`` — **degraded-success**
      (operator decision): the rows are the export's best-known and replace, but the
      recorded outcome carries the degradation.
    * ``unsupported`` → :class:`Unavailable`(``unsupported``): declared not-authoritative
      absence — keep rows.
    * ``not-ready`` → :class:`Unavailable`(``not_ready``): no record under the current
      mount (post-reload, NED remount). The engine escalates to ``device-state-read run``
      exactly once — the envelope itself never extracts.
    * ``error`` → :class:`Unavailable`(``read_error``) with the wire's ``error-reason``.

    ``section is None`` is DEVICE-level absence (the client already confirmed the
    ``device-state`` container is alive): the device is genuinely unknown to NSO. READSEM S5
    retired the per-family ``empty_policy`` — device-absence now resolves UNIFORMLY to
    :class:`Unavailable`(``not_authoritative``), keeping the last-known rows for every family.
    A true removal is handled by the device-lifecycle deleting the device (and cascading its
    rows), never by a per-family poll wiping a mirror on a bare 404.

    An unknown/missing status is never guessed at: ``Unavailable(read_error)``, rows kept.
    """
    if section is None:
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
