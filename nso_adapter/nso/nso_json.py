# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Boundary-safe JSON serialization for NSO RESTCONF request bodies.

NSO 6.7's RESTCONF JSON lexer loses token state at its 64KiB read-buffer
refill: a bare literal cut by the boundary 400s the WHOLE request with
``1: Bad JSON character: f`` (check-item 134, found by a production-scale
2190-row interface_ip roundtrip — 363KB body, byte 65536 landed mid-``false``).
Probe-proven on the live NSO: byte-identical content re-serialized so the
boundary lands mid-STRING parses fine — strings survive the refill, bare
literals and (by the same lexer path) numbers must be assumed not to.

JSON allows whitespace between tokens, so :func:`boundary_safe_dumps` pushes
any bare token that would straddle a 64KiB multiple past it with spaces.
``json.dumps`` defaults keep the output ASCII (``ensure_ascii=True``), so
character offsets ARE byte offsets — load-bearing for the boundary math.

Every RESTCONF body that can grow with device size (service-intent lists)
MUST be serialized through :func:`boundary_safe_dumps`, never bare
``json.dumps`` — ``tests/nso/test_nso_json.py`` pins the apply-path sites.
This is the production copy of the nso-vendor-test board harness's
``vendor_test/nso_json.py``; keep the two in sync.
"""

from __future__ import annotations

import json

# NSO's observed read-buffer size. Deterministic across runs/probes on 6.7.
NSO_LEX_CHUNK = 65536

# Chars that may START a bare (non-string, non-structural) JSON token, and the
# full char set such tokens are drawn from (digits/sign/exponent + the letters
# of true/false/null). A bare token always terminates on a structural char or
# whitespace, none of which are in this set.
_BARE_START = "-0123456789tfn"
_BARE_CHARS = frozenset("-+.0123456789eEtruefalsn")


def boundary_safe_dumps(obj) -> str:
    """``json.dumps(obj)`` with no bare token straddling a 64KiB-multiple offset.

    Semantics-preserving: only inter-token whitespace is inserted. Strings are
    copied verbatim (they may straddle — probe-proven safe, and padding inside
    one would change its value).
    """
    s = json.dumps(obj)
    if len(s) <= NSO_LEX_CHUNK:
        return s
    out: list[str] = []
    pos = 0  # output offset (== byte offset: ASCII by construction)
    i = 0
    n = len(s)
    while i < n:
        c = s[i]
        if c == '"':
            j = i + 1
            while j < n:
                if s[j] == "\\":
                    j += 2
                    continue
                if s[j] == '"':
                    j += 1
                    break
                j += 1
            out.append(s[i:j])
            pos += j - i
            i = j
        elif c in _BARE_START:
            j = i
            while j < n and s[j] in _BARE_CHARS:
                j += 1
            token = s[i:j]
            boundary = ((pos // NSO_LEX_CHUNK) + 1) * NSO_LEX_CHUNK
            if pos < boundary < pos + len(token):
                pad = boundary - pos
                out.append(" " * pad)
                pos += pad
            out.append(token)
            pos += len(token)
            i = j
        else:
            out.append(c)
            pos += 1
            i += 1
    return "".join(out)


def straddling_bare_tokens(s: str, chunk: int = NSO_LEX_CHUNK) -> list[tuple[int, str]]:
    """Return (offset, token) for every bare token straddling a *chunk* multiple.

    The checker the tests pin :func:`boundary_safe_dumps`'s output against.
    """
    found: list[tuple[int, str]] = []
    i = 0
    n = len(s)
    while i < n:
        c = s[i]
        if c == '"':
            j = i + 1
            while j < n:
                if s[j] == "\\":
                    j += 2
                    continue
                if s[j] == '"':
                    j += 1
                    break
                j += 1
            i = j
        elif c in _BARE_START:
            j = i
            while j < n and s[j] in _BARE_CHARS:
                j += 1
            boundary = ((i // chunk) + 1) * chunk
            if i < boundary < j:
                found.append((i, s[i:j]))
            i = j
        else:
            i += 1
    return found
