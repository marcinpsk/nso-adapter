# SPDX-License-Identifier: Apache-2.0
"""Per-NED community-member dialect codec (nso_adapter/core/community_dialect.py).

Grounded in live-device + SR OS doc facts (see the module docstring): Nokia keeps
exact std/ext/large members and digit-domain regex verbatim, uses `&` only for
regex large communities, and represents an exact `color:F:V` as the Color
Ext-Community hex `ext:030b:FFFFVVVVVVVV` (regex color + `bandwidth:` stay gaps).
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
    for m in ("64500:1234", "target:64500:1", "color:0:128", "large:1:2:3", "no-export"):
        assert d.to_canonical(m) == m
        assert d.from_canonical(m) == m  # identity never declares anything unrepresentable


# ── Nokia: members that pass through unchanged (confirmed live) ───────────────


@pytest.mark.parametrize(
    "member",
    [
        "64500:1234",  # exact standard
        "64500:1113.",  # dot regex (live on device)
        "64500:.*",  # star regex (live on device)
        "64500:1.3.",
        "target:64500:1234",  # exact route-target
        "origin:64500:1234",  # exact route-origin
        "ext:4300:0000075bcd15",  # non-color ext-community hex → round-trips raw
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
    assert _nokia().from_canonical("large:64500:64501:1234") == "64500:64501:1234"


def test_nokia_large_round_trips_through_canonical():
    d = _nokia()
    assert d.to_canonical(d.from_canonical("large:64500:64501:1234")) == "large:64500:64501:1234"


def test_nokia_reads_bare_three_part_member_as_large():
    assert _nokia().to_canonical("64500:64501:1234") == "large:64500:64501:1234"


def test_nokia_two_part_standard_is_not_misread_as_large():
    assert _nokia().to_canonical("64500:1234") == "64500:1234"


def test_nokia_non_color_ext_round_trips_verbatim():
    # A non-color ext: sub-type is a native SR OS form the device holds verbatim;
    # the codec must not translate or drop it.
    d = _nokia()
    member = "ext:4300:0000075bcd15"
    assert d.to_canonical(member) == member
    assert d.from_canonical(member) == member


# ── Nokia: color:F:V ⇄ Color Ext-Community hex ext:030b:FFFFVVVVVVVV ──────────
# RFC 9012 §4.3: Type 0x03 / Sub-Type 0x0b. Verified live — FLEX128 member
# `ext:030b:000000000080` is color value 128 (= example-comm's `color:0:128`).


def test_nokia_exact_color_writes_ext_hex():
    d = _nokia()
    assert d.from_canonical("color:0:128") == "ext:030b:000000000080"  # FLEX128
    assert d.from_canonical("color:0:129") == "ext:030b:000000000081"  # FLEX129


def test_nokia_color_ext_reads_back_as_color():
    assert _nokia().to_canonical("ext:030b:000000000080") == "color:0:128"
    assert _nokia().to_canonical("ext:030b:000000000081") == "color:0:129"


def test_nokia_color_round_trips_through_canonical():
    d = _nokia()
    assert d.to_canonical(d.from_canonical("color:0:128")) == "color:0:128"


@pytest.mark.parametrize(
    ("co", "hexflags"),
    [(0, "0000"), (1, "4000"), (2, "8000"), (3, "c000")],
)
def test_nokia_color_co_bits_map_to_top_two_flag_bits(co, hexflags):
    # CO occupies the top 2 bits of the flags field (flags = CO << 14): CO is
    # semantically significant (SR-policy next-hop resolution), so it must be exact.
    d = _nokia()
    member = f"color:{co}:128"
    ext = f"ext:030b:{hexflags}00000080"
    assert d.from_canonical(member) == ext
    assert d.to_canonical(ext) == member


def test_nokia_color_out_of_range_co_is_unrepresentable():
    # CO is a 2-bit field; a value > 3 is not a valid color community.
    assert _nokia().from_canonical("color:4:128") is UNREPRESENTABLE


def test_nokia_reads_native_color_keyword_normalised():
    # Newer SR OS reports color by keyword (color:00:600); READ normalises CO/V to
    # ints so it unifies with the Cisco/Junos color:0:600 form.
    d = _nokia()
    assert d.to_canonical("color:00:600") == "color:0:600"
    assert d.to_canonical("color:01:128") == "color:1:128"


def test_nokia_regex_color_stays_unrepresentable():
    # A regex color (no single hex value) cannot be a Color Ext-Community.
    assert _nokia().from_canonical("color:0:12.") is UNREPRESENTABLE


def test_nokia_unrepresentable_members_reports_only_the_gaps_deduped_in_order():
    # example-comm's member set: only the wildcard color is unrepresentable on Nokia;
    # the helper reports exactly it (order-preserving, de-duped), so a caller can flag
    # it "unsupported on Nokia" without a device write.
    members = [
        "64500:*",
        "64500:1234",
        "64500:1.3.",
        "color:0:12.",  # the only gap (wildcard color)
        "color:0:128",  # exact color → ext hex, representable
        "large:64500:64501:1234",
        "no-export",
        "target:64500:1234",
        "color:0:12.",  # duplicate — must not appear twice
    ]
    assert _nokia().unrepresentable_members(members) == ["color:0:12."]


def test_identity_dialect_represents_everything():
    # The default (Cisco/Junos/IOS-XR) dialect holds every canonical member verbatim,
    # so nothing is ever reported unsupported.
    d = community_dialect_for("cisco-iosxr-nc-7.3")
    assert d.unrepresentable_members(["color:0:12.", "bandwidth:100", "large:1:2:3"]) == []


# ── Nokia: regex large community = 3 ``&``-separated parts (from live config) ──
# `&` is SR OS's large-community part separator; exact large uses `:`, regex uses `&`.
# Source: live `expression expr "NOT (… OR 64500&.*&[0-4])"` ⇄ Junos `large:64500:.*:[0-4]`.


def test_nokia_reads_amp_large_as_canonical_large():
    assert _nokia().to_canonical("64500&.*&[0-4]") == "large:64500:.*:[0-4]"


def test_nokia_writes_regex_large_with_amp_separators():
    assert _nokia().from_canonical("large:64500:.*:[0-4]") == "64500&.*&[0-4]"


def test_nokia_regex_large_round_trips_through_canonical():
    d = _nokia()
    assert d.to_canonical(d.from_canonical("large:64500:.*:[0-4]")) == "large:64500:.*:[0-4]"


def test_nokia_exact_large_still_uses_colons():
    # Exact (no regex) large communities keep the colon form, not `&`.
    assert _nokia().from_canonical("large:64500:64501:1234") == "64500:64501:1234"


# ── Nokia: members not representable in `community member` are reported ───────
# regex color / bandwidth: / soo: are genuine SR OS gaps (not policy-community
# keywords, or no single hex value). Surfaced (skip + per-device journal), never
# silently kept in the push. (An EXACT color: IS representable — see above.)


@pytest.mark.parametrize(
    "member",
    [
        "color:0:12.",  # regex color — no single hex value
        "bandwidth:64500:100",
        "soo:64500:1",
    ],
)
def test_nokia_reports_unrepresentable_keywords(member):
    assert _nokia().from_canonical(member) is UNREPRESENTABLE
