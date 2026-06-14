# SPDX-License-Identifier: Apache-2.0
"""Per-NED community-member dialect codec (nso_adapter/core/community_dialect.py).

Grounded in live-device + SR OS doc facts (see the module docstring): Nokia keeps
exact std/ext/large members and digit-domain regex verbatim, uses `&&` only for
regex large communities, and has no `color:`/`bandwidth:` policy keyword.
"""

from __future__ import annotations

import pytest

from nso_adapter.core.community_dialect import (
    UNREPRESENTABLE,
    community_dialect_for,
)

NOKIA = "timos-nc-23.10"


def _nokia():
    return community_dialect_for(NOKIA)


# ── dialect selection ────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "ned_id",
    ["timos-nc-23.10", "timos-nc"],
)
def test_timos_selects_nokia_dialect(ned_id):
    assert _nokia() is community_dialect_for(ned_id)


@pytest.mark.parametrize(
    "ned_id",
    ["cisco-iosxr-cli-7.76", "cisco-ios-cli-6.114", "juniper-junos-nc-4.19", "", None],
)
def test_other_neds_get_identity_default(ned_id):
    d = community_dialect_for(ned_id)
    for m in ("6830:1234", "target:6830:1", "color:0:128", "large:1:2:3", "no-export"):
        assert d.to_canonical(m) == m
        assert d.from_canonical(m) == m  # identity never declares anything unrepresentable


# ── Nokia: members that pass through unchanged (confirmed live) ───────────────


@pytest.mark.parametrize(
    "member",
    [
        "6830:1234",  # exact standard
        "6830:1113.",  # dot regex (live on device)
        "6830:.*",  # star regex (live on device)
        "6830:1.3.",
        "target:6830:1234",  # exact route-target
        "origin:6830:1234",  # exact route-origin
        "ext:030b:000000000080",  # raw RFC 4360 ext-community hex (live: FLEX128)
        "ext:030b:000000000081",  # live: FLEX129
        "no-export",
        "no-advertise",
        "NO-EXPORT",  # case-insensitive well-known
    ],
)
def test_nokia_passes_supported_members_verbatim(member):
    d = _nokia()
    assert d.from_canonical(member) == member
    assert d.to_canonical(member) == member


# ── Nokia: exact large community = 3 colon parts, no keyword (verified live) ──


def test_nokia_exact_large_strips_keyword():
    assert _nokia().from_canonical("large:6830:6370:1234") == "6830:6370:1234"


def test_nokia_large_round_trips_through_canonical():
    d = _nokia()
    assert d.to_canonical(d.from_canonical("large:6830:6370:1234")) == "large:6830:6370:1234"


def test_nokia_reads_bare_three_part_member_as_large():
    assert _nokia().to_canonical("6830:6370:1234") == "large:6830:6370:1234"


def test_nokia_two_part_standard_is_not_misread_as_large():
    assert _nokia().to_canonical("6830:1234") == "6830:1234"


def test_nokia_ext_hex_round_trips_verbatim():
    # Regression: ext: members are a native SR OS form (read live from FLEX128/129);
    # the codec must not mark a member the device itself holds as UNREPRESENTABLE.
    d = _nokia()
    member = "ext:030b:000000000080"
    assert d.to_canonical(member) == member
    assert d.from_canonical(member) == member


# ── Nokia: regex large community = 3 ``&``-separated parts (from live config) ──
# `&` is SR OS's large-community part separator; exact large uses `:`, regex uses `&`.
# Source: live `expression expr "NOT (… OR 6830&.*&[0-4])"` ⇄ Junos `large:6830:.*:[0-4]`.


def test_nokia_reads_amp_large_as_canonical_large():
    assert _nokia().to_canonical("6830&.*&[0-4]") == "large:6830:.*:[0-4]"


def test_nokia_writes_regex_large_with_amp_separators():
    assert _nokia().from_canonical("large:6830:.*:[0-4]") == "6830&.*&[0-4]"


def test_nokia_regex_large_round_trips_through_canonical():
    d = _nokia()
    assert d.to_canonical(d.from_canonical("large:6830:.*:[0-4]")) == "large:6830:.*:[0-4]"


def test_nokia_exact_large_still_uses_colons():
    # Exact (no regex) large communities keep the colon form, not `&`.
    assert _nokia().from_canonical("large:6830:6370:1234") == "6830:6370:1234"


# ── Nokia: members not representable in `community member` are reported ───────
# color:/bandwidth:/soo: are genuine SR OS gaps (not policy-community keywords).
# These are surfaced (skip + per-device journal), never silently kept in the push.


@pytest.mark.parametrize(
    "member",
    [
        "color:0:128",
        "color:0:12.",
        "bandwidth:6830:100",
        "soo:6830:1",
    ],
)
def test_nokia_reports_unrepresentable_keywords(member):
    assert _nokia().from_canonical(member) is UNREPRESENTABLE
