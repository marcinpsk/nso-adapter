# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Per-NED BGP community-member dialect translation.

NetBox stores **one canonical** member string per community: Cisco/Junos
POSIX-style regex for pattern members, plus RFC text for exact members
(``asn:val``, ``target:asn:val``, ``origin:asn:val``, ``large:a:b:c``, and the
well-known names). Different NEDs spell the same member differently — or cannot
represent it at all. This module translates canonical <-> per-NED dialect in
**both** directions, and reports members a NED genuinely cannot hold so the
apply path can skip them (with a per-device log warning) instead of letting one
unrepresentable member abort the device's whole community.

Why a codec and not a drop: a community list is global/dedup-by-name, so the
same ``cnad-test`` attaches to Junos + Nokia + IOS-XR. Each NED keeps the members
it supports; only the truly-unrepresentable ones are skipped, and only for the
device that can't take them — never lost from NetBox.

Community members are the first user. The :class:`CommunityDialect` registry is
the seam future per-NED string translators (prefix-list / as-path / ...) plug
into; add a dialect class and one prefix→dialect line.

Concrete Nokia (timos) facts that drive the rules below, confirmed against the
live device (apply commit results) + SR OS 23.10 route-policy docs:
- exact ``asn:val``, ``target:…``, ``origin:…`` and the well-known names sit on
  the device verbatim → pass through unchanged;
- ``.`` / ``*`` / ``[]`` / ``()`` / ``-`` regex over the digit:colon domain are
  accepted verbatim (e.g. ``6830:1113.``, ``6830:.*`` and ``6830:*`` commit live);
- ``ext:`` is a raw RFC 4360 extended community in hex (type+value, e.g.
  ``ext:030b:000000000080``); it sits on the device verbatim → round-trips
  unchanged on Nokia. The ``030b`` sub-type is the Color Ext-Community, so an
  ``ext:030b:…`` member re-canonicalises to ``color:F:V`` on READ (see below);
- ``color:CO:V`` IS representable on Nokia: it is the Color Extended Community
  (RFC 9012, Type 0x03 / Sub-Type 0x0b). The middle field is the Color-Only (CO)
  bits — the top 2 bits of the flags field (``flags = CO << 14``) — which carry
  real SR-policy semantics, so they are encoded exactly. Older SR OS takes the hex
  form ``ext:030b:FFFFVVVVVVVV`` (``color:0:128`` ⇄ ``ext:030b:000000000080`` —
  verified live: FLEX128); newer SR OS also accepts the native ``color:CO:V``
  keyword, which READ normalises (``color:00:600`` → ``color:0:600``). Only an
  EXACT color maps; a regex color (``color:0:12.``) stays UNREPRESENTABLE.
  ``bandwidth:`` is still not an SR OS keyword → UNREPRESENTABLE;
- ``large:`` (RFC 8092) — ``&`` is SR OS's large-community part separator. An EXACT
  large community is three **colon** parts with NO keyword (``large:a:b:c`` ⇄ Nokia
  ``a:b:c`` — verified live: ``a:b:c`` commits, ``large:a:b:c`` rejected); a large
  community carrying a **regex** is three **``&``**-separated parts
  (``large:6830:.*:[0-4]`` ⇄ Nokia ``6830&.*&[0-4]`` — straight from the live
  ``expression`` config, and the exact Junos ``large:6830:.*:[0-4]`` member). My
  earlier probe wrongly doubled the separator (``a&&b&&c`` → MGMT_CORE #2301); the
  separator is a single ``&``. On READ a bare 3-part member (colon or ``&``) is
  re-prefixed to canonical ``large:``.
"""

from __future__ import annotations

# Sentinel returned by ``from_canonical`` for a member the NED cannot represent.
UNREPRESENTABLE = object()

# Well-known community names — valid (and identical) on every NED we target.
_WELL_KNOWN: frozenset[str] = frozenset(
    {
        "no-export",
        "no-advertise",
        "no-export-subconfed",
        "no-peer",
        "local-as",
        "internet",
        "none",
    }
)


# Characters that make a member a regex/wildcard rather than an exact value.
_REGEX_METACHARS: frozenset[str] = frozenset(".*[]()?+^$|\\")


def _typed_keyword(member: str) -> str | None:
    """Return the extended/large keyword (lowercased) of a typed member, else None.

    ``target:6830:1234`` → ``"target"``; ``large:1:2:3`` → ``"large"``;
    ``6830:1234`` → None (standard, head is numeric); ``no-export`` → None.
    """
    head, sep, _rest = member.partition(":")
    if not sep or not head or head[0].isdigit():
        return None
    return head.lower()


def _has_regex(value: str) -> bool:
    """True if *value* contains any regex metacharacter."""
    return any(ch in _REGEX_METACHARS for ch in value)


# Color Extended Community (RFC 9012 §4.3): a transitive-opaque ext-community —
# Type ``0x03`` + Sub-Type ``0x0b`` (the ``030b`` head), then a 2-byte Flags field
# and a 4-byte Color Value. Everyone spells it ``color:CO:V``: Cisco/Junos canonical,
# AND Nokia SR OS natively (e.g. ``color:00:600``). The middle field is the two
# Color-Only (CO) bits, which occupy the TOP 2 bits of the flags field's first
# octet — so CO 0/1/2/3 ⇒ flags ``0x0000``/``0x4000``/``0x8000``/``0xC000`` ⇒
# ``flags = CO << 14``. CO bits are semantically significant (they govern SR-policy
# next-hop resolution), so the encoding must be exact, not a raw passthrough.
#
# Older SR OS takes the hex form ``ext:030b:FFFFVVVVVVVV`` (FLEX128/129 live on the
# 23.10 lab device are stored this way); newer SR OS also accepts the native
# ``color:CO:V`` keyword (see :meth:`_NokiaCommunityDialect.to_canonical`). We
# translate so all dialects unify on the canonical ``color:CO:V``. Only an EXACT
# color maps — a regex color (``color:0:12.``) has no single hex value → skipped.
_COLOR_EXT_PREFIX = "ext:030b:"
_HEX_DIGITS: frozenset[str] = frozenset("0123456789abcdefABCDEF")
_CO_MAX = 0x3  # CO is a 2-bit field (values 0..3)


def _color_to_nokia_ext(member: str):
    """``color:CO:V`` (CO bits 0..3, exact integer V) → ``ext:030b:FFFFVVVVVVVV``, else None.

    CO occupies the top 2 bits of the 2-byte flags field (``flags = CO << 14``); V is
    the 4-byte color value. None signals "not an exact color" (regex/wildcard or
    out-of-range) so the caller falls through to UNREPRESENTABLE.
    """
    parts = member.split(":", 1)[1].split(":")
    if len(parts) != 2:
        return None
    co_s, value_s = parts
    if not (co_s.isdigit() and value_s.isdigit()):
        return None
    co, value = int(co_s), int(value_s)
    if co > _CO_MAX or value > 0xFFFFFFFF:
        return None
    return f"{_COLOR_EXT_PREFIX}{co << 14:04x}{value:08x}"


def _nokia_ext_to_color(member: str):
    """``ext:030b:FFFFVVVVVVVV`` (12 hex) → ``color:CO:V``, else None.

    CO is recovered from the top 2 bits of the flags field (``flags >> 14``); only the
    color sub-type (``030b``) with exactly 12 hex digits maps. Any other ``ext:``
    extended community returns None and round-trips raw.
    """
    if not member.startswith(_COLOR_EXT_PREFIX):
        return None
    hexbody = member[len(_COLOR_EXT_PREFIX) :]
    if len(hexbody) != 12 or any(c not in _HEX_DIGITS for c in hexbody):
        return None
    co = int(hexbody[:4], 16) >> 14
    return f"color:{co}:{int(hexbody[4:], 16)}"


def _normalize_native_color(member: str):
    """Native SR OS ``color:CO:V`` → canonical ``color:CO:V`` with int-normalised fields.

    Newer SR OS writes the color community by keyword (``color:00:600``); normalise
    CO/V to plain ints (``color:0:600``) so it unifies with the Cisco/Junos form.
    Returns None when *member* is not an exact native color (e.g. a regex color).
    """
    if not member.startswith("color:"):
        return None
    parts = member.split(":")
    if len(parts) != 3 or not (parts[1].isdigit() and parts[2].isdigit()):
        return None
    return f"color:{int(parts[1])}:{int(parts[2])}"


class CommunityDialect:
    """Default dialect: canonical == device wire form (IOS, IOS-XR, Junos).

    Subclasses override only the cases where their NED diverges from canonical.
    """

    def to_canonical(self, member: str) -> str:
        """Device wire form → canonical NetBox form (READ/import path)."""
        return member

    def from_canonical(self, member: str):
        """Canonical NetBox form → device wire form (WRITE/apply path).

        Returns the device member string, or :data:`UNREPRESENTABLE` if this NED
        cannot hold the member at all.
        """
        return member


class _NokiaCommunityDialect(CommunityDialect):
    """Nokia SR OS (timos) community-member dialect."""

    # SR OS ``policy-options community member`` keywords that pass through verbatim.
    # ``target``/``origin`` are route-target/route-origin extended communities;
    # ``ext`` is a raw RFC 4360 extended community in hex (``ext:030b:00000000…``,
    # confirmed live on the device). ``large`` is handled specially (keyword
    # stripped); ``color`` is translated to/from its ``ext:030b:…`` hex form;
    # ``bandwidth`` and any other Cisco/Junos keyword aren't SR OS keywords →
    # UNREPRESENTABLE.
    _SUPPORTED_KEYWORDS: frozenset[str] = frozenset({"target", "origin", "ext"})

    def to_canonical(self, member: str) -> str:
        m = member.strip()
        # ``ext:030b:…`` is the Color Extended Community in hex — re-canonicalise it
        # to the ``color:CO:V`` form so it unifies with Cisco/Junos. Other ``ext:``
        # sub-types fall through and round-trip raw.
        color = _nokia_ext_to_color(m)
        if color is not None:
            return color
        # Newer SR OS reports color natively as ``color:CO:V`` — normalise CO/V to
        # plain ints so it unifies with the canonical form (``color:00:600`` →
        # ``color:0:600``). Prep for a NED that emits the keyword rather than hex.
        native_color = _normalize_native_color(m)
        if native_color is not None:
            return native_color
        # A bare 3-part member with a numeric head is an RFC 8092 large community on
        # SR OS (no keyword) — restore the canonical ``large:`` prefix. Exact large
        # communities use ``:`` separators; regex large communities use ``&``.
        if _typed_keyword(m) is None and m[:1].isdigit():
            if m.count(":") == 2:
                return "large:" + m
            if m.count("&") == 2:
                return "large:" + m.replace("&", ":")
        return m

    def from_canonical(self, member: str):
        m = member.strip()
        if m.lower() in _WELL_KNOWN:
            return m
        keyword = _typed_keyword(m)
        if keyword is None:
            # Standard `asn:val` or a digit-domain regex — Nokia takes these verbatim.
            return m
        if keyword in self._SUPPORTED_KEYWORDS:
            # target: / origin: — exact and Nokia regex forms pass through unchanged.
            return m
        if keyword == "large":
            return self._large_from_canonical(m)
        if keyword == "color":
            # An exact color → the Color Ext-Community hex; a regex color has no
            # single hex value and stays unrepresentable on SR OS.
            ext = _color_to_nokia_ext(m)
            return ext if ext is not None else UNREPRESENTABLE
        # bandwidth: / … — no SR OS policy-community keyword.
        return UNREPRESENTABLE

    @staticmethod
    def _large_from_canonical(member: str):
        body = member.split(":", 1)[1]  # drop the canonical `large:` prefix
        parts = body.split(":")
        if len(parts) != 3:
            return UNREPRESENTABLE
        # SR OS keeps an exact large community as three colon parts (keyword-less),
        # and a regex large community as three ``&``-separated parts.
        if any(_has_regex(p) for p in parts):
            return "&".join(parts)
        return body


# Default identity dialect, shared by every NED without a registered override.
_DEFAULT_DIALECT = CommunityDialect()

# NED-id prefix → dialect. Match is by ``startswith`` so version suffixes
# (``timos-nc-23.10``) and bare prefixes (``timos-nc``) both resolve.
_DIALECTS: tuple[tuple[str, CommunityDialect], ...] = (("timos-nc", _NokiaCommunityDialect()),)


def community_dialect_for(ned_id: str | None) -> CommunityDialect:
    """Return the :class:`CommunityDialect` for *ned_id* (default = identity)."""
    if ned_id:
        for prefix, dialect in _DIALECTS:
            if ned_id.startswith(prefix):
                return dialect
    return _DEFAULT_DIALECT
