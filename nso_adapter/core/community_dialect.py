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
  unchanged on Nokia;
- ``color:`` / ``bandwidth:`` are not SR OS policy-community keywords → those
  members are UNREPRESENTABLE on Nokia (genuine device limitation);
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
    # stripped); ``color``/``bandwidth`` and any other Cisco/Junos keyword aren't
    # SR OS keywords → UNREPRESENTABLE.
    _SUPPORTED_KEYWORDS: frozenset[str] = frozenset({"target", "origin", "ext"})

    def to_canonical(self, member: str) -> str:
        m = member.strip()
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
        # color: / bandwidth: / … — no SR OS policy-community keyword.
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
