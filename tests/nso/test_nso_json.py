# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""boundary_safe_dumps — the NSO 64KiB lexer-refill workaround (check-item 134)."""

from __future__ import annotations

import json
from pathlib import Path

import nso_adapter.nso.apply
from nso_adapter.nso.nso_json import NSO_LEX_CHUNK, boundary_safe_dumps, straddling_bare_tokens


def _obj_with_false_straddling(chunk: int = NSO_LEX_CHUNK) -> dict:
    """An object whose DEFAULT json.dumps puts a ``false`` across the chunk boundary.

    The exact shape that hit production-scale interface_ip (byte 65536 landed
    mid-``false`` in a 363KB body).
    """
    probe = {"pad": "", "rows": [{"secondary": False, "n": 1234} for _ in range(40)]}
    base_len = len(json.dumps({**probe, "pad": ""}))
    # place the boundary 2 bytes into the first "false" token
    first_false = json.dumps({**probe, "pad": ""}).index("false")
    pad = chunk - first_false - 2
    obj = {**probe, "pad": "x" * pad}
    s = json.dumps(obj)
    assert len(s) > chunk >= base_len - first_false, "fixture must exceed one chunk"
    assert straddling_bare_tokens(s), "fixture premise: default dumps DOES straddle"
    return obj


def test_small_payloads_pass_through_identically():
    """A body under one chunk is returned byte-identical to json.dumps."""
    obj = {"a": [1, 2, 3], "b": False, "c": None}
    assert boundary_safe_dumps(obj) == json.dumps(obj)


def test_straddling_false_is_pushed_past_the_boundary():
    """A ``false`` that would straddle the boundary is padded past it, value-preserving."""
    obj = _obj_with_false_straddling()
    safe = boundary_safe_dumps(obj)
    assert straddling_bare_tokens(safe) == []
    assert json.loads(safe) == obj  # whitespace-only change


def test_numbers_are_protected_too():
    """A number straddling the boundary is padded — same lexer path as literals."""
    probe = {"pad": "", "nums": [1234567890] * 40}
    first_num = json.dumps(probe).index("1234567890")
    obj = {**probe, "pad": "x" * (NSO_LEX_CHUNK - first_num - 4)}
    assert straddling_bare_tokens(json.dumps(obj))
    safe = boundary_safe_dumps(obj)
    assert straddling_bare_tokens(safe) == []
    assert json.loads(safe) == obj


def test_strings_may_straddle_untouched():
    """Strings survive the refill (probe-proven) and MUST NOT be padded."""
    # padding inside one would change its value; a string spanning the boundary is fine
    obj = {"pad": "x" * (NSO_LEX_CHUNK + 50)}
    safe = boundary_safe_dumps(obj)
    assert safe == json.dumps(obj)


def test_multi_chunk_payload_protects_every_boundary():
    """Every 64KiB multiple in a multi-chunk body is protected, not just the first."""
    rows = [{"secondary": False, "prefix-length": 24, "address": "10.0.0.1"} for _ in range(6000)]
    obj = {"root": rows}
    s = json.dumps(obj)
    assert len(s) > 3 * NSO_LEX_CHUNK
    safe = boundary_safe_dumps(obj)
    assert straddling_bare_tokens(safe) == []
    assert json.loads(safe) == obj


def test_apply_py_serializes_no_restconf_body_with_bare_json_dumps():
    """Ratchet: every RESTCONF body site in apply.py uses boundary_safe_dumps.

    A service-intent body grows with device size, so a new ``payload =
    json.dumps(...)`` site reintroduces the 64KiB lexer-refill 400 at production
    scale. The single allowed bare call serializes the match-json/set-json LEAF
    value — it travels inside the outer body as a JSON *string*, which survives
    the refill (and padding it would corrupt the stored value).
    """
    src = Path(nso_adapter.nso.apply.__file__).read_text()
    offenders = [line.strip() for line in src.splitlines() if "json.dumps(" in line]
    assert offenders == ["value = json.dumps(value or {}, sort_keys=True)"]


def test_escaped_quotes_inside_strings_do_not_desync_the_scanner():
    """A ``\\"``-laden string before a straddling ``false`` must not derail string skipping."""
    # if the scanner mis-handled the escape it would treat string content as tokens
    probe = {"esc": '\\"' * 400, "pad": "", "rows": [{"secondary": False, "n": 1234} for _ in range(40)]}
    first_false = json.dumps(probe).index("false")
    obj = {**probe, "pad": "x" * (NSO_LEX_CHUNK - first_false - 2)}
    assert straddling_bare_tokens(json.dumps(obj)), "fixture premise: default dumps DOES straddle"
    safe = boundary_safe_dumps(obj)
    assert straddling_bare_tokens(safe) == []
    assert json.loads(safe) == obj
