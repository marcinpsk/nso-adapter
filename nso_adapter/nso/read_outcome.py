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
from dataclasses import dataclass, field, replace

import httpx


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


#: The family slot of a read that asked for the WHOLE device envelope, not one family.
#: :meth:`ReadFailure.for_family` narrows it when the fan-out serves a single family.
WHOLE_DEVICE = "device-state"


class ReadOperation(str, enum.Enum):
    """WHICH read failed. Ours, never derived from what the server answered."""

    section_get = "section_get"  # GET one family's envelope section
    doc_get = "doc_get"  # GET the whole device-state envelope entry
    device_state_read = "device_state_read"  # POST device-state-read run (the extraction action)
    section_classify = "section_classify"  # the served section itself broke the read contract


class ReadFailureCode(str, enum.Enum):
    """The authored contract reason, for a failure the server ANSWERED instead of raising.

    Closed set. Each member names one way the read contract broke, so an operator can tell a
    device-reported extract error from a malformed body from a failed heal.
    """

    section_status_error = "section_status_error"  # the section declared status=error
    section_status_unrecognized = "section_status_unrecognized"  # the status leaf is not in the wire set
    section_malformed = "section_malformed"  # a 200 doc served a non-dict where a section belongs
    heal_action_failed = "heal_action_failed"  # the not-ready heal action could not re-serve the family
    action_section_missing = "action_section_missing"  # the action output has no section for the family
    action_returned_not_ready = "action_returned_not_ready"  # the action answered a non-terminal status
    action_output_not_atomic = "action_output_not_atomic"  # the action output is not a certified snapshot


@dataclass(frozen=True)
class ReadFailure:
    """The AUTHORED classification of ONE failed read. Every field is ours; none is the server's.

    An operator has to tell a 401 from a 503 and a device-reported extract error from a
    malformed body, so a bare exception TYPE is not a classification. ``http_status`` carries
    the numeric status whenever the failure was an HTTP answer, and ``code`` carries the
    contract reason whenever the server answered a 200 the reader had to refuse. Neither the
    server's reason phrase, body, URL nor exception text is ever kept.
    """

    operation: ReadOperation
    device: str
    family: str
    error_type: str | None = None  # the raised type, when the read raised
    http_status: int | None = None  # the numeric status, when the server answered one
    code: ReadFailureCode | None = None  # the contract reason, when the server answered a refused 200

    def for_family(self, family: str) -> ReadFailure:
        """Narrow a whole-device read failure to the family it is being reported for."""
        return replace(self, family=family)

    def log_fields(self) -> dict[str, object]:
        """Render the classification as record fields — the only shape any sink prints."""
        return {
            "device_name": self.device,
            "family": self.family,
            "read_operation": self.operation.value,
            "error_type": self.error_type,
            "http_status": self.http_status,
            "failure_code": self.code.value if self.code is not None else None,
        }


def read_failure_from_exception(
    exc: BaseException,
    *,
    operation: ReadOperation,
    device: str,
    family: str,
) -> ReadFailure:
    """Classify a read that RAISED: the type always, plus the numeric status the server answered.

    Only :class:`httpx.HTTPStatusError` carries a server status code, and only the code is
    taken from it — never the reason phrase, the URL, the redirect location or the body.
    """
    status = exc.response.status_code if isinstance(exc, httpx.HTTPStatusError) else None
    return ReadFailure(
        operation=operation,
        device=device,
        family=family,
        error_type=type(exc).__name__,
        http_status=status,
    )


@dataclass(frozen=True)
class Unavailable:
    """No authoritative answer → keep the last-known mirror rows."""

    reason: UnavailableReason
    # The AUTHORED classification of the failure, never server or exception text: it reaches
    # the operator log. None for a DECLARED state (unsupported / not-ready / device-absent),
    # which is not a failure. Excluded from equality so tests can assert on reason alone.
    failure: ReadFailure | None = field(default=None, compare=False)


ReadOutcome = Present | AbsentAuthoritative | Unavailable


def classify_envelope_section(section: dict | None, *, device: str, family: str) -> ReadOutcome:
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
    * ``error`` → :class:`Unavailable`(``read_error``) with an authored
      :class:`ReadFailureCode`. The wire's ``error-reason`` is the server's own text, so it
      is classified, never carried.

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
    # The error-reason is the server's own text and can name a community-keyed path, so the
    # refusal carries the authored code instead. The status leaf is the server's too.
    code = ReadFailureCode.section_status_error if status == "error" else ReadFailureCode.section_status_unrecognized
    return Unavailable(
        UnavailableReason.read_error,
        failure=ReadFailure(operation=ReadOperation.section_classify, device=device, family=family, code=code),
    )
