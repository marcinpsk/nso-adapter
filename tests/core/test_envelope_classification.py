# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""READSEM S3/S5 — classify_envelope_section: the wire status → vocabulary mapping.

The envelope's ``status`` leaf is the ground truth the legacy 200/404 wire could not
carry; classification is a direct mapping with no inference. READSEM S5 retired the
per-family ``empty_policy`` — device-level absence (section None) now resolves uniformly.
"""

from __future__ import annotations

import pytest

from nso_adapter.nso.read_outcome import (
    Freshness,
    Present,
    ReadFailure,
    ReadFailureCode,
    ReadOperation,
    Unavailable,
    UnavailableReason,
    classify_envelope_section,
)
from tests._secret_discipline import assert_text_free_of

_ASKED = {"device": "rg03", "family": "static-route"}


def test_read_failure_representations_redact_the_device_from_every_container():
    device = "placeholder-distinctive-device"
    failure = ReadFailure(
        operation=ReadOperation.device_state_read,
        device=device,
        family="static-route",
        error_type="NsoReadContractError",
        http_status=503,
        code=ReadFailureCode.action_output_not_atomic,
    )
    unavailable = Unavailable(UnavailableReason.read_error, failure)
    present = Present.composite({}, Freshness.stale, [unavailable])

    for rendered in (repr(failure), str(failure), repr(unavailable), str(unavailable), repr(present), str(present)):
        assert_text_free_of(rendered, [device])

    classification = repr(failure)
    for field in ("device_state_read", "static-route", "NsoReadContractError", "503", "action_output_not_atomic"):
        if field not in classification:
            raise AssertionError("the read failure representation omits an authored classification field")


class TestStatusMapping:
    def test_ok_is_present_fresh_with_the_section_as_data(self):
        section = {"status": "ok", "last-updated": "2026-07-20T12:00:00+00:00", "route": [{"prefix": "10.0.0.0/8"}]}
        outcome = classify_envelope_section(section, **_ASKED)
        assert outcome == Present(section)
        assert outcome.freshness is Freshness.fresh

    def test_ok_without_list_keys_is_the_authoritative_empty(self):
        """RESTCONF omits empty lists: ok + absent keys REPLACES (clears) via Present."""
        outcome = classify_envelope_section({"status": "ok"}, **_ASKED)
        assert isinstance(outcome, Present)
        assert outcome.data == {"status": "ok"}

    def test_stale_is_present_degraded(self):
        """Operator decision: stale-200 = degraded-success — replace rows, record degraded."""
        section = {"status": "stale", "route": []}
        outcome = classify_envelope_section(section, **_ASKED)
        assert isinstance(outcome, Present)
        assert outcome.freshness is Freshness.stale

    def test_unsupported_keeps_rows(self):
        """The envelope ends the legacy conflation of unsupported with authoritative emptiness."""
        outcome = classify_envelope_section({"status": "unsupported"}, **_ASKED)
        assert outcome == Unavailable(UnavailableReason.unsupported)

    def test_not_ready_is_the_escalation_trigger(self):
        outcome = classify_envelope_section({"status": "not-ready"}, **_ASKED)
        assert outcome == Unavailable(UnavailableReason.not_ready)

    def test_error_classifies_the_failure_and_drops_the_wire_reason(self):
        """Classify the error. The server's error-reason must not reach diagnostic sinks."""
        outcome = classify_envelope_section({"status": "error", "error-reason": "extract boom"}, **_ASKED)
        assert isinstance(outcome, Unavailable)
        assert outcome.reason is UnavailableReason.read_error
        assert outcome.failure.code is ReadFailureCode.section_status_error
        assert outcome.failure.operation is ReadOperation.section_classify
        assert (outcome.failure.device, outcome.failure.family) == ("rg03", "static-route")
        assert_text_free_of(repr(outcome), ["extract boom"])

    @pytest.mark.parametrize("status", [None, "bogus", ""])
    def test_unknown_status_is_never_guessed_at(self, status):
        """A status the adapter does not recognize keeps rows — never clears on a guess."""
        section = {"status": status} if status is not None else {}
        outcome = classify_envelope_section(section, **_ASKED)
        assert isinstance(outcome, Unavailable)
        assert outcome.reason is UnavailableReason.read_error
        assert outcome.failure.code is ReadFailureCode.section_status_unrecognized


class TestDeviceLevelAbsence:
    """section None = the device is unknown to a HEALTHY export. READSEM S5: KEEP rows uniformly
    (was: pop families cleared) — a bare 404 never wipes a mirror; true removal is the device
    lifecycle's job."""

    def test_device_absence_keeps_rows_for_every_family(self):
        assert classify_envelope_section(None, **_ASKED) == Unavailable(UnavailableReason.not_authoritative)


class TestStoredStringValues:
    """The outcome store persists enum ``.value`` strings — pin them (schema-visible contract)."""

    def test_new_vocabulary_values(self):
        assert Freshness.stale.value == "stale"
        assert UnavailableReason.unsupported.value == "unsupported"
        assert UnavailableReason.not_ready.value == "not_ready"
