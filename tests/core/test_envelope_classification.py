# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""READSEM S3/S5 — classify_envelope_section: the wire status → vocabulary mapping.

The envelope's ``status`` leaf is the ground truth the legacy 200/404 wire could not
carry; classification is a direct mapping with no inference. READSEM S5 retired the
per-family ``empty_policy`` — device-level absence (section None) now resolves uniformly.
"""

from __future__ import annotations

import dataclasses

import pytest

from nso_adapter.nso.client import NsoExportUnavailableError
from nso_adapter.nso.read_outcome import (
    AbsentAuthoritative,
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
        # The heal is the one path that stamps a code onto a raised read, so it is the shape
        # that carries every field this renderer has to redact and keep.
        code=ReadFailureCode.heal_action_failed,
    )
    unavailable = Unavailable(UnavailableReason.read_error, failure)
    present = Present.composite({}, Freshness.stale, [unavailable])

    for rendered in (repr(failure), str(failure), repr(unavailable), str(unavailable), repr(present), str(present)):
        assert_text_free_of(rendered, [device])

    classification = repr(failure)
    for field in ("device_state_read", "static-route", "NsoReadContractError", "503", "heal_action_failed"):
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


class TestUnproducibleClassifications:
    """A fixture that blesses a shape no reader emits hides the regression it was added to catch."""

    def test_a_served_section_code_refuses_a_raised_read(self):
        """503 raises, so the reader never sees a section to take a status off."""
        with pytest.raises(ValueError, match="cannot come from this read"):
            ReadFailure(
                operation=ReadOperation.section_get,
                device="rg03",
                family="bgp-config",
                error_type="HTTPStatusError",
                http_status=503,
                code=ReadFailureCode.section_status_error,
            )

    def test_a_served_section_code_refuses_the_wrong_operation(self):
        with pytest.raises(ValueError, match="operation must be section_classify"):
            ReadFailure(
                operation=ReadOperation.section_get,
                device="rg03",
                family="bgp-config",
                code=ReadFailureCode.section_status_unrecognized,
            )

    def test_the_classifier_still_builds_its_own_verdict(self):
        """Not vacuous: the one real producer of these codes stays constructible."""
        outcome = classify_envelope_section({"status": "error"}, **_ASKED)
        assert outcome.failure.code is ReadFailureCode.section_status_error
        assert outcome.failure.operation is ReadOperation.section_classify

    def test_export_down_refuses_a_failure_the_liveness_probe_cannot_raise(self):
        """export_down is reached only by the container 404; a 503 is a read_error."""
        with pytest.raises(ValueError, match="confirmed by the liveness probe"):
            Unavailable(
                UnavailableReason.export_down,
                failure=ReadFailure(
                    operation=ReadOperation.doc_get,
                    device="rg03",
                    family="bgp-config",
                    error_type="HTTPStatusError",
                    http_status=503,
                ),
            )

    def test_export_down_refuses_a_failure_that_names_no_exception(self):
        """An authored code alone is not the liveness probe's verdict."""
        with pytest.raises(ValueError, match="confirmed by the liveness probe"):
            Unavailable(
                UnavailableReason.export_down,
                failure=ReadFailure(
                    operation=ReadOperation.device_state_read,
                    device="rg03",
                    family="bgp-config",
                    code=ReadFailureCode.action_output_not_atomic,
                ),
            )

    def test_every_code_declares_where_it_can_come_from(self):
        """A new code without an entry would otherwise be validated by nothing."""
        from nso_adapter.nso.read_outcome import _CODE_PROVENANCE

        assert set(_CODE_PROVENANCE) == set(ReadFailureCode)

    def test_an_action_code_refuses_a_read_that_cannot_author_it(self):
        """The action codes answer a device-state read, not a section GET."""
        with pytest.raises(ValueError, match="operation must be device_state_read"):
            ReadFailure(
                operation=ReadOperation.section_get,
                device="rg03",
                family="bgp-config",
                code=ReadFailureCode.action_returned_not_ready,
            )

    def test_the_heal_code_keeps_the_details_the_exception_classified(self):
        """heal_action_failed is stamped onto a raised read, so the type and status travel."""
        healed = ReadFailure(
            operation=ReadOperation.device_state_read,
            device="rg03",
            family="bgp-config",
            error_type="HTTPStatusError",
            http_status=503,
            code=ReadFailureCode.heal_action_failed,
        )
        assert healed.http_status == 503

    def test_the_pinned_exception_name_is_the_class_name(self):
        """The invariant names the exception instead of importing it; the two must not drift."""
        from nso_adapter.nso.read_outcome import _EXPORT_DOWN_ERROR_TYPE

        assert _EXPORT_DOWN_ERROR_TYPE == NsoExportUnavailableError.__name__

    def test_the_refusal_names_only_the_fault_it_found(self):
        """A correct operation must not be reported as the problem."""
        with pytest.raises(ValueError) as caught:
            ReadFailure(
                operation=ReadOperation.section_classify,
                device="rg03",
                family="bgp-config",
                http_status=503,
                code=ReadFailureCode.section_status_error,
            )
        assert str(caught.value) == ("section_status_error cannot come from this read: http_status must be unset")

    def test_export_down_accepts_the_outage_the_probe_does_raise(self):
        outage = Unavailable(
            UnavailableReason.export_down,
            failure=ReadFailure(
                operation=ReadOperation.doc_get,
                device="rg03",
                family="bgp-config",
                error_type=NsoExportUnavailableError.__name__,
            ),
        )
        assert outage.failure.http_status is None


class TestTheVocabularyDoesNotRenderTheDeviceItWasAskedFor:
    """``repr`` is a diagnostic surface, and a generated one renders every field.

    ``log_fields`` and ``persistence_fields`` are the projections a sink prints, and both
    already omit the device. A default dataclass repr bypasses them, so anything that formats
    an outcome (a log kwarg, an f-string, a pytest diff) publishes the caller's device name.
    """

    #: What the diagnostic-identity rule keeps out of a record: a caller-provided name, never
    #: an adapter-owned id. The opengrep rule states it for ``logger`` kwargs; a repr is the
    #: same sink reached a different way.
    CALLER_PROVIDED = frozenset({"device", "device_name", "nso_device_name", "stream_url", "url"})

    @pytest.mark.parametrize("kind", [Present, AbsentAuthoritative, Unavailable, ReadFailure])
    def test_no_vocabulary_type_renders_a_caller_provided_identifier(self, kind):
        rendered = [
            field.name for field in dataclasses.fields(kind) if field.name in self.CALLER_PROVIDED and field.repr
        ]
        assert rendered == [], f"{kind.__name__} renders {rendered} in its repr"

    def test_a_classified_failure_renders_its_classification_without_the_device(self):
        outcome = classify_envelope_section({"status": "error", "error-reason": "extract boom"}, **_ASKED)

        assert_text_free_of(repr(outcome), ["rg03", "extract boom"])
        assert "section_status_error" in repr(outcome), "the classification itself must stay legible"
        assert outcome.failure.device == "rg03", "the field still carries it for for_family and the join"
