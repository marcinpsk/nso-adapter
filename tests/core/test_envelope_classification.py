# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""READSEM S3 — classify_envelope_section: the wire status → vocabulary mapping.

The envelope's ``status`` leaf is the ground truth the legacy 200/404 wire could not
carry; classification must be a direct mapping with no inference. The one branch where
:class:`EmptyPolicy` still matters is DEVICE-level absence (section None).
"""

from __future__ import annotations

import pytest

from nso_adapter.nso.read_outcome import (
    AbsentAuthoritative,
    EmptyPolicy,
    Freshness,
    Present,
    Unavailable,
    UnavailableReason,
    classify_envelope_section,
)


class TestStatusMapping:
    def test_ok_is_present_fresh_with_the_section_as_data(self):
        section = {"status": "ok", "last-updated": "2026-07-20T12:00:00+00:00", "route": [{"prefix": "10.0.0.0/8"}]}
        outcome = classify_envelope_section(section, EmptyPolicy.pop)
        assert outcome == Present(section)
        assert outcome.freshness is Freshness.fresh

    def test_ok_without_list_keys_is_the_authoritative_empty(self):
        """RESTCONF omits empty lists: ok + absent keys REPLACES (clears) via Present."""
        outcome = classify_envelope_section({"status": "ok"}, EmptyPolicy.pop)
        assert isinstance(outcome, Present)
        assert outcome.data == {"status": "ok"}

    def test_stale_is_present_degraded(self):
        """Operator decision: stale-200 = degraded-success — replace rows, record degraded."""
        section = {"status": "stale", "route": []}
        outcome = classify_envelope_section(section, EmptyPolicy.pop)
        assert isinstance(outcome, Present)
        assert outcome.freshness is Freshness.stale

    @pytest.mark.parametrize("empty_policy", [EmptyPolicy.pop, EmptyPolicy.present])
    def test_unsupported_keeps_rows_regardless_of_empty_policy(self, empty_policy):
        """The envelope ends the legacy conflation of unsupported with authoritative emptiness."""
        outcome = classify_envelope_section({"status": "unsupported"}, empty_policy)
        assert outcome == Unavailable(UnavailableReason.unsupported)

    def test_not_ready_is_the_escalation_trigger(self):
        outcome = classify_envelope_section({"status": "not-ready"}, EmptyPolicy.pop)
        assert outcome == Unavailable(UnavailableReason.not_ready)

    def test_error_carries_the_wire_reason(self):
        outcome = classify_envelope_section({"status": "error", "error-reason": "extract boom"}, EmptyPolicy.pop)
        assert isinstance(outcome, Unavailable)
        assert outcome.reason is UnavailableReason.read_error
        assert "extract boom" in outcome.detail

    @pytest.mark.parametrize("status", [None, "bogus", ""])
    def test_unknown_status_is_never_guessed_at(self, status):
        """A status the adapter does not recognize keeps rows — never clears on a guess."""
        section = {"status": status} if status is not None else {}
        outcome = classify_envelope_section(section, EmptyPolicy.pop)
        assert isinstance(outcome, Unavailable)
        assert outcome.reason is UnavailableReason.read_error


class TestDeviceLevelAbsence:
    """section None = the device is unknown to a HEALTHY export — the one EmptyPolicy branch."""

    def test_pop_family_clears(self):
        assert classify_envelope_section(None, EmptyPolicy.pop) == AbsentAuthoritative()

    def test_present_family_keeps(self):
        assert classify_envelope_section(None, EmptyPolicy.present) == Unavailable(UnavailableReason.not_authoritative)


class TestStoredStringValues:
    """The outcome store persists enum ``.value`` strings — pin them (schema-visible contract)."""

    def test_new_vocabulary_values(self):
        assert Freshness.stale.value == "stale"
        assert UnavailableReason.unsupported.value == "unsupported"
        assert UnavailableReason.not_ready.value == "not_ready"
