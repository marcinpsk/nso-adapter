# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Shared binding, scope, and control-flow contract for hand-written AST scanners."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from textwrap import dedent

import pytest

from tests.core.test_importer_failure_sinks import _raw_log_exception_renderers
from tests.credential_discipline import scan_source as scan_credentials
from tests.test_secret_discipline import _non_disclosure_assertion_lines


@dataclass(frozen=True)
class ScannerSpec:
    name: str
    source: str
    clean: str
    sink: str
    expression_sink: str
    scan: Callable[[str], bool]


@dataclass(frozen=True)
class ConformanceCase:
    name: str
    tainted: str
    clean: str
    skips: dict[str, str] | None = None


def _credential_verdict(source: str) -> bool:
    return bool(scan_credentials(source, "conformance.py"))


def _secret_verdict(source: str) -> bool:
    return bool(_non_disclosure_assertion_lines(source))


SCANNERS = (
    ScannerSpec(
        "exception-log",
        "exc",
        '"authored detail"',
        'logger.warning("event", detail=value)',
        'logger.warning("event", detail=value)',
        lambda source: bool(_raw_log_exception_renderers(source)),
    ),
    ScannerSpec(
        "credential",
        '"admin"',
        '"placeholder-user"',
        "username = value",
        "connect(username=value)",
        _credential_verdict,
    ),
    ScannerSpec(
        "non-disclosure",
        "response.text",
        '"authored detail"',
        "assert protected not in value",
        "",
        _secret_verdict,
    ),
)


CASES = (
    ConformanceCase(
        "binding-tuple",
        "value, authored = SOURCE, CLEAN\nSINK",
        "value, authored = CLEAN, SOURCE\nSINK",
    ),
    ConformanceCase(
        "binding-list",
        "[value, authored] = [SOURCE, CLEAN]\nSINK",
        "[value, authored] = [CLEAN, SOURCE]\nSINK",
    ),
    ConformanceCase(
        "binding-starred",
        "[*value, authored] = [SOURCE, CLEAN]\nSINK",
        "[*value, authored] = [CLEAN, SOURCE]\nSINK",
    ),
    ConformanceCase("binding-annotated", "value: object = SOURCE\nSINK", "value: object = CLEAN\nSINK"),
    ConformanceCase(
        "binding-augmented",
        'value = ""\nvalue += SOURCE\nSINK',
        'value = ""\nvalue += CLEAN\nSINK',
    ),
    ConformanceCase(
        "binding-walrus",
        "if value := SOURCE:\n    SINK",
        "if value := CLEAN:\n    SINK",
    ),
    ConformanceCase(
        "scope-function-inward",
        "value = SOURCE\ndef nested():\n    SINK",
        "value = CLEAN\ndef nested():\n    SINK",
    ),
    ConformanceCase(
        "scope-function-outward",
        "def nested():\n    value = SOURCE\n    SINK",
        "def nested():\n    value = SOURCE\nvalue = CLEAN\nSINK",
    ),
    ConformanceCase(
        "scope-function-shadow",
        "value = SOURCE\ndef nested():\n    SINK",
        "value = SOURCE\ndef nested():\n    value = CLEAN\n    SINK",
    ),
    ConformanceCase(
        "scope-async-function-inward",
        "value = SOURCE\nasync def nested():\n    SINK",
        "value = CLEAN\nasync def nested():\n    SINK",
    ),
    ConformanceCase(
        "scope-async-function-outward",
        "async def nested():\n    value = SOURCE\n    SINK",
        "async def nested():\n    value = SOURCE\nvalue = CLEAN\nSINK",
    ),
    ConformanceCase(
        "scope-class-inward",
        "value = SOURCE\nclass Nested:\n    SINK",
        "value = CLEAN\nclass Nested:\n    SINK",
    ),
    ConformanceCase(
        "scope-class-outward",
        "class Nested:\n    value = SOURCE\n    SINK",
        "class Nested:\n    value = SOURCE\nvalue = CLEAN\nSINK",
    ),
    ConformanceCase(
        "scope-class-inward-before-shadow",
        "value = SOURCE\nclass Nested:\n    SINK\n    value = CLEAN",
        "value = SOURCE\nclass Nested:\n    value = CLEAN\n    SINK",
    ),
    ConformanceCase(
        "scope-class-compound-inward-before-shadow",
        "value = SOURCE\nclass Nested:\n    if condition:\n        SINK\n        value = CLEAN",
        "value = SOURCE\nclass Nested:\n    if condition:\n        value = CLEAN\n        SINK",
    ),
    ConformanceCase(
        "scope-class-try-finally-before-shadow",
        "value = SOURCE\nclass Nested:\n    try:\n        SINK\n    finally:\n        value = CLEAN",
        "value = SOURCE\nclass Nested:\n    try:\n        value = CLEAN\n        SINK\n    finally:\n        value = CLEAN",
    ),
    ConformanceCase(
        "scope-class-except-intermediate-state",
        "value = CLEAN\nclass Nested:\n    try:\n        value = SOURCE\n        work()\n        value = CLEAN\n    except Exception:\n        SINK",
        "value = CLEAN\nclass Nested:\n    try:\n        value = CLEAN\n        work()\n    except Exception:\n        SINK",
    ),
    ConformanceCase(
        "scope-class-loop-before-shadow",
        "value = SOURCE\nclass Nested:\n    while condition:\n        SINK\n        value = CLEAN",
        "value = SOURCE\nclass Nested:\n    while condition:\n        value = CLEAN\n        SINK",
    ),
    ConformanceCase(
        "scope-class-loop-fixed-point",
        "value = CLEAN\nclass Nested:\n    while condition:\n        SINK\n        value = SOURCE",
        "value = CLEAN\nclass Nested:\n    while condition:\n        SINK\n        value = CLEAN",
    ),
    ConformanceCase(
        "scope-method-skips-class",
        "value = SOURCE\nclass Nested:\n    value = CLEAN\n    def method():\n        SINK",
        "value = CLEAN\nclass Nested:\n    value = SOURCE\n    def method():\n        SINK",
    ),
    ConformanceCase(
        "scope-comprehension-inward",
        "value = SOURCE\n[EXPRESSION_SINK for _ in items]",
        "value = CLEAN\n[EXPRESSION_SINK for _ in items]",
        {"non-disclosure": "the scanner checks assert statements, which comprehensions cannot contain"},
    ),
    ConformanceCase(
        "scope-comprehension-outward",
        "[EXPRESSION_SINK for value in [SOURCE]]",
        "[value for value in [SOURCE]]\nvalue = CLEAN\nSINK",
        {"non-disclosure": "the scanner checks assert statements, which comprehensions cannot contain"},
    ),
    ConformanceCase(
        "control-try-except",
        "value = CLEAN\ntry:\n    work()\nexcept Exception:\n    value = SOURCE\nSINK",
        "value = CLEAN\ntry:\n    work()\nexcept Exception:\n    value = CLEAN\nSINK",
    ),
    ConformanceCase(
        "control-try-else",
        "value = CLEAN\ntry:\n    work()\nexcept Exception:\n    value = CLEAN\nelse:\n    value = SOURCE\nSINK",
        "value = CLEAN\ntry:\n    work()\nexcept Exception:\n    value = CLEAN\nelse:\n    value = CLEAN\nSINK",
    ),
    ConformanceCase(
        "control-try-finally",
        "value = CLEAN\ntry:\n    work()\nexcept Exception:\n    pass\nfinally:\n    value = SOURCE\nSINK",
        "value = CLEAN\ntry:\n    work()\nexcept Exception:\n    pass\nfinally:\n    value = CLEAN\nSINK",
    ),
    ConformanceCase(
        "control-finally-exceptional-state",
        "value = CLEAN\nclass Nested:\n    try:\n        value = SOURCE\n        work()\n        value = CLEAN\n    finally:\n        SINK",
        "value = CLEAN\nclass Nested:\n    try:\n        value = SOURCE\n        work()\n        value = CLEAN\n    except:\n        value = CLEAN\n    finally:\n        SINK",
    ),
    ConformanceCase(
        "control-handler-exceptional-finally-state",
        "value = CLEAN\nclass Nested:\n    try:\n        work()\n    except:\n        value = SOURCE\n        work_again()\n        value = CLEAN\n    finally:\n        SINK",
        "value = CLEAN\nclass Nested:\n    try:\n        work()\n    except:\n        value = CLEAN\n        work_again()\n    finally:\n        SINK",
    ),
    ConformanceCase(
        "control-nested-try-exception-propagation",
        "value = CLEAN\nclass Nested:\n    try:\n        try:\n            value = SOURCE\n            work()\n            value = CLEAN\n        except ValueError:\n            value = CLEAN\n    finally:\n        SINK",
        "value = CLEAN\nclass Nested:\n    try:\n        try:\n            value = SOURCE\n            work()\n            value = CLEAN\n        except:\n            value = CLEAN\n    finally:\n        SINK",
    ),
    ConformanceCase(
        "control-nested-try-handler-propagation",
        "value = CLEAN\nclass Nested:\n    try:\n        try:\n            value = SOURCE\n            work()\n            value = CLEAN\n        except ValueError:\n            value = CLEAN\n    except:\n        SINK",
        "value = CLEAN\nclass Nested:\n    try:\n        try:\n            value = SOURCE\n            work()\n            value = CLEAN\n        except:\n            value = CLEAN\n        work_outer()\n    except:\n        SINK",
    ),
    ConformanceCase(
        "control-break-in-try",
        "for item in items:\n    value = CLEAN\n    try:\n        if condition:\n            value = SOURCE\n            break\n    finally:\n        SINK",
        "for item in items:\n    value = CLEAN\n    try:\n        if condition:\n            value = CLEAN\n            break\n    finally:\n        SINK",
    ),
    ConformanceCase(
        "control-continue-in-try",
        "for item in items:\n    value = CLEAN\n    try:\n        if condition:\n            value = SOURCE\n            continue\n    finally:\n        SINK",
        "for item in items:\n    value = CLEAN\n    try:\n        if condition:\n            value = CLEAN\n            continue\n    finally:\n        SINK",
    ),
    ConformanceCase(
        "control-finally-replaces-exit",
        "value = CLEAN\nfor item in items:\n    try:\n        value = SOURCE\n        break\n    finally:\n        pass\nelse:\n    value = CLEAN\nSINK",
        "value = SOURCE\nfor item in items:\n    try:\n        break\n    finally:\n        value = CLEAN\n        continue\nelse:\n    value = CLEAN\nSINK",
        {"non-disclosure": "the scanner is lexical and does not model control-flow overwrites"},
    ),
    ConformanceCase(
        "control-if-body",
        "value = CLEAN\nif condition:\n    value = SOURCE\nSINK",
        "value = CLEAN\nif condition:\n    value = CLEAN\nSINK",
    ),
    ConformanceCase(
        "control-if-else",
        "value = CLEAN\nif condition:\n    value = CLEAN\nelse:\n    value = SOURCE\nSINK",
        "value = CLEAN\nif condition:\n    value = CLEAN\nelse:\n    value = CLEAN\nSINK",
    ),
)


def _render(case_source: str, scanner: ScannerSpec) -> str:
    return (
        dedent(case_source)
        .replace("EXPRESSION_SINK", scanner.expression_sink)
        .replace("SOURCE", scanner.source)
        .replace("CLEAN", scanner.clean)
        .replace("SINK", scanner.sink)
    )


@pytest.mark.parametrize("scanner", SCANNERS, ids=lambda scanner: scanner.name)
@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
@pytest.mark.parametrize("tainted", [True, False], ids=["tainted", "clean"])
def test_scanner_conformance(scanner: ScannerSpec, case: ConformanceCase, tainted: bool) -> None:
    if case.skips and scanner.name in case.skips:
        pytest.skip(case.skips[scanner.name])
    source = _render(case.tainted if tainted else case.clean, scanner)

    assert scanner.scan(source) is tainted
