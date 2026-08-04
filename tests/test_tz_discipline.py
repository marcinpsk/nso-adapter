# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The tz-discipline guard itself: flags the strip, honors the marker and the allowlist."""

from __future__ import annotations

from tests.tz_discipline import _ALLOWED_FILES, scan_source, scan_tree


def test_flags_a_bare_tzinfo_none_replace():
    hits = scan_source("x = ts.replace(tzinfo=None)\n", "mod.py")
    assert hits == [("mod.py", 1)]


def test_flags_only_tzinfo_none_not_other_replaces():
    src = "a = ts.replace(tzinfo=UTC)\nb = s.replace('Z', '+00:00')\nc = ts.replace(hour=1)\n"
    assert scan_source(src, "mod.py") == []


def test_inline_marker_exempts_a_reviewed_site():
    src = "x = ts.replace(tzinfo=None)  # tz-ok: proving rejection of naive input\n"
    assert scan_source(src, "mod.py") == []


def test_marker_on_a_multiline_call_span_counts():
    src = "x = ts.replace(\n    tzinfo=None,  # tz-ok: reviewed\n)\n"
    assert scan_source(src, "mod.py") == []


def test_the_tree_is_clean_and_the_serializer_is_the_only_allowlisted_file():
    assert scan_tree() == []
    assert _ALLOWED_FILES == {"nso_adapter/api/timestamps.py"}
