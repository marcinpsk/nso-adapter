# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Shared binding, scope, and control-flow contract for hand-written AST scanners."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from textwrap import dedent, indent

import pytest

from tests.core.test_importer_failure_sinks import _raw_log_exception_renderers
from tests.credential_discipline import scan_source as scan_credentials
from tests.test_secret_discipline import _non_disclosure_assertion_lines

ROOT = Path(__file__).parents[1]
OPENGREP = shutil.which(os.environ.get("OPENGREP_BIN") or "opengrep")
OPENGREP_RULES = ROOT / ".opengrep" / "nso-rules.yaml"
OPENGREP_EXCEPTION_RULES = {
    "nso-outcome-raw-exception-alias-renderer",
    "nso-outcome-with-exit-raw-exception-alias-renderer",
}


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
    gaps: dict[str, str] | None = None


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
    # PEP 572: a walrus inside a comprehension binds its target in the CONTAINING scope, unlike
    # the generator targets, which stay isolated to the comprehension.
    ConformanceCase(
        "binding-walrus-in-comprehension-body",
        "[value := SOURCE for _ in (0,)]\nSINK",
        "[value := CLEAN for _ in (0,)]\nSINK",
    ),
    ConformanceCase(
        "binding-walrus-in-comprehension-condition",
        "[_ for _ in (0,) if (value := SOURCE)]\nSINK",
        "[_ for _ in (0,) if (value := CLEAN)]\nSINK",
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
    # A nested scope can bind a name without an ast.Name store: an import alias and a match
    # capture both shadow the inherited alias, so a scanner that misses them reports the
    # rebound local as the outer taint.
    ConformanceCase(
        "scope-function-shadow-by-import",
        "value = SOURCE\ndef nested():\n    SINK",
        "value = SOURCE\ndef nested():\n    import value\n    SINK",
    ),
    ConformanceCase(
        "scope-function-shadow-by-import-alias",
        "value = SOURCE\ndef nested():\n    SINK",
        "value = SOURCE\ndef nested():\n    from package import thing as value\n    SINK",
    ),
    ConformanceCase(
        "scope-function-shadow-by-match-capture",
        "value = SOURCE\ndef nested():\n    SINK",
        "value = SOURCE\ndef nested():\n    match subject:\n        case [value]:\n            SINK",
    ),
    # The other direction of the same visitor: a capture shadows the outer alias, but it must
    # carry the SUBJECT's value with it, or `case [name]` launders the taint.
    ConformanceCase(
        "binding-match-capture-from-subject",
        "match SOURCE:\n    case value:\n        SINK",
        "match CLEAN:\n    case value:\n        SINK",
    ),
    ConformanceCase(
        "binding-match-capture-from-subject-in-nested-scope",
        "value = CLEAN\ndef nested():\n    match SOURCE:\n        case value:\n            SINK",
        "value = SOURCE\ndef nested():\n    match CLEAN:\n        case value:\n            SINK",
    ),
    ConformanceCase(
        "scope-function-shadow-by-match-star",
        "value = SOURCE\ndef nested():\n    SINK",
        "value = SOURCE\ndef nested():\n    match subject:\n        case [_, *value]:\n            SINK",
    ),
    ConformanceCase(
        "scope-function-shadow-by-match-mapping-rest",
        "value = SOURCE\ndef nested():\n    SINK",
        "value = SOURCE\ndef nested():\n    match subject:\n        case {'k': _, **value}:\n            SINK",
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
        "control-with-exit-post-body-state",
        "class Nested:\n    value = CLEAN\n    try:\n        with context():\n            value = SOURCE\n    except Exception:\n        pass\n    else:\n        value = CLEAN\n    SINK",
        "class Nested:\n    value = CLEAN\n    try:\n        with context():\n            value = CLEAN\n    except Exception:\n        pass\n    else:\n        value = CLEAN\n    SINK",
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

OPENGREP_FUNCTION_SCOPE_CASES = (
    ConformanceCase(
        "control-finally-exceptional-state-at-function-scope",
        "value = CLEAN\ntry:\n    value = SOURCE\n    work()\n    value = CLEAN\nfinally:\n    SINK",
        "value = CLEAN\ntry:\n    value = SOURCE\n    work()\n    value = CLEAN\nexcept:\n    value = CLEAN\nfinally:\n    SINK",
    ),
    ConformanceCase(
        "control-handler-exceptional-finally-state-at-function-scope",
        "value = CLEAN\ntry:\n    work()\nexcept:\n    value = SOURCE\n    work_again()\n    value = CLEAN\nfinally:\n    SINK",
        "value = CLEAN\ntry:\n    work()\nexcept:\n    value = CLEAN\n    work_again()\nfinally:\n    SINK",
    ),
    ConformanceCase(
        "control-nested-try-exception-propagation-at-function-scope",
        "value = CLEAN\ntry:\n    try:\n        value = SOURCE\n        work()\n        value = CLEAN\n    except ValueError:\n        value = CLEAN\nfinally:\n    SINK",
        "value = CLEAN\ntry:\n    try:\n        value = SOURCE\n        work()\n        value = CLEAN\n    except:\n        value = CLEAN\nfinally:\n    SINK",
    ),
    ConformanceCase(
        "control-nested-try-handler-propagation-at-function-scope",
        "value = CLEAN\ntry:\n    try:\n        value = SOURCE\n        work()\n        value = CLEAN\n    except ValueError:\n        value = CLEAN\nexcept:\n    SINK",
        "value = CLEAN\ntry:\n    try:\n        value = SOURCE\n        work()\n        value = CLEAN\n    except:\n        value = CLEAN\n    work_outer()\nexcept:\n    SINK",
    ),
)
#: Measured at this commit: putting `from package import thing as value` inside a function body
#: changes OpenGrep's verdict for NINE unrelated cases elsewhere in the same file (both directions:
#: strict xfails start XPASSing and clean variants start reporting). Appending the same case at the
#: end of the file changes nothing, so the effect is positional, not a parse failure — the scan
#: reports no errors and skips no file. Keeping it in the shared file would mean nine xfail entries
#: documenting OpenGrep's reaction to an unrelated neighbour rather than any real gap, so it moves
#: to OPENGREP_ISOLATED_CASES and is scanned in a file of its own instead.
OPENGREP_CONTEXT_SENSITIVE_CASES = frozenset({"scope-function-shadow-by-import-alias"})
OPENGREP_CASES = tuple(
    case for case in (*CASES, *OPENGREP_FUNCTION_SCOPE_CASES) if case.name not in OPENGREP_CONTEXT_SENSITIVE_CASES
)
#: Measured on a file of its own, so it cannot move another case's verdict. Excluding it from the
#: shared file is not a reason to stop measuring it: its clean variant is a real OpenGrep false
#: positive, recorded in OPENGREP_XFAILS below.
OPENGREP_ISOLATED_CASES = tuple(
    case for case in (*CASES, *OPENGREP_FUNCTION_SCOPE_CASES) if case.name in OPENGREP_CONTEXT_SENSITIVE_CASES
)

_OPENGREP_CLASS_SCOPE_GAPS = {
    "scope-class-inward",
    "scope-class-inward-before-shadow",
    "scope-class-compound-inward-before-shadow",
    "scope-class-try-finally-before-shadow",
    "scope-class-except-intermediate-state",
    "scope-class-loop-before-shadow",
    "scope-class-loop-fixed-point",
    "scope-method-skips-class",
    "control-finally-exceptional-state",
    "control-handler-exceptional-finally-state",
    "control-nested-try-exception-propagation",
    "control-nested-try-handler-propagation",
}
OPENGREP_XFAILS = {
    **{
        (name, True): "OpenGrep does not propagate the exception taint into a nested class body"
        for name in _OPENGREP_CLASS_SCOPE_GAPS
    },
    **{
        (name, True): "OpenGrep does not bind a comprehension walrus in the containing scope (PEP 572)"
        for name in ("binding-walrus-in-comprehension-body", "binding-walrus-in-comprehension-condition")
    },
    (
        "scope-function-shadow-by-import",
        False,
    ): "OpenGrep does not treat an import as a local binding that shadows an inherited alias",
    (
        "scope-function-shadow-by-import-alias",
        False,
    ): "OpenGrep does not treat an import alias as a local binding that shadows an inherited alias",
    (
        "control-break-in-try",
        True,
    ): "OpenGrep does not replay a finally block over a pending break path",
    (
        "control-continue-in-try",
        True,
    ): "OpenGrep does not replay a finally block over a pending continue path",
    (
        "control-finally-replaces-exit",
        False,
    ): "OpenGrep reports stale taint after finally replaces a pending loop exit",
    (
        "control-finally-exceptional-state-at-function-scope",
        False,
    ): "OpenGrep retains taint after a catch-all handler overwrites exceptional state",
    (
        "control-nested-try-exception-propagation-at-function-scope",
        False,
    ): "OpenGrep retains taint after a nested catch-all handler overwrites exceptional state",
    (
        "control-nested-try-handler-propagation-at-function-scope",
        False,
    ): "OpenGrep propagates stale taint past a nested catch-all handler",
}

CREDENTIAL_CONSTANT_CASES = (
    ("direct-literal", '"admin"', True, None),
    (
        "lower",
        '"ADMIN".lower()',
        True,
        "OpenGrep does not constant-fold str.lower() when identifying a taint source",
    ),
    (
        "join",
        '"".join(("ad", "min"))',
        True,
        "OpenGrep does not constant-fold str.join() when identifying a taint source",
    ),
    ("clean-literal", '"placeholder-user"', False, None),
)


def _render(case_source: str, scanner: ScannerSpec) -> str:
    return (
        dedent(case_source)
        .replace("EXPRESSION_SINK", scanner.expression_sink)
        .replace("SOURCE", scanner.source)
        .replace("CLEAN", scanner.clean)
        .replace("SINK", scanner.sink)
    )


def _opengrep_case_parameters() -> list[object]:
    parameters = []
    for case in (*OPENGREP_CASES, *OPENGREP_ISOLATED_CASES):
        for tainted in (True, False):
            marks = []
            if reason := OPENGREP_XFAILS.get((case.name, tainted)):
                marks.append(pytest.mark.xfail(reason=reason, strict=True))
            parameters.append(
                pytest.param(
                    case,
                    tainted,
                    marks=marks,
                    id=f"{case.name}-{'tainted' if tainted else 'clean'}",
                )
            )
    return parameters


def _scanner_case_parameters() -> list[object]:
    parameters = []
    for scanner in SCANNERS:
        for case in CASES:
            gap_reason = (case.gaps or {}).get(scanner.name)
            if gap_reason:
                continue
            parameters.append(pytest.param(scanner, case, id=f"{case.name}-{scanner.name}"))
    return parameters


@pytest.fixture(scope="module")
def opengrep_verdicts(tmp_path_factory: pytest.TempPathFactory) -> set[tuple[str, bool]]:
    assert OPENGREP is not None

    target_dir = tmp_path_factory.mktemp("opengrep-conformance")
    target = target_dir / "review-patterns.py"
    source_lines: list[str] = []
    line_owners: dict[int, tuple[str, bool]] = {}
    exception_scanner = SCANNERS[0]
    for case in OPENGREP_CASES:
        for tainted in (True, False):
            case_source = _render(case.tainted if tainted else case.clean, exception_scanner)
            function_name = f"case_{case.name.replace('-', '_')}_{'tainted' if tainted else 'clean'}"
            # The live source pattern does not match a try body that contains only pass.
            block = (
                f"def {function_name}():\n"
                "    try:\n"
                "        work()\n"
                "    except Exception as exc:\n"
                f"{indent(case_source, '        ')}\n"
            )
            start_line = len(source_lines) + 1
            block_lines = block.splitlines()
            source_lines.extend(block_lines)
            source_lines.append("")
            for line in range(start_line, start_line + len(block_lines)):
                line_owners[line] = (case.name, tainted)
    target.write_text("\n".join(source_lines), encoding="utf-8")

    result = subprocess.run(
        [
            OPENGREP,
            "scan",
            "--config",
            str(OPENGREP_RULES),
            "--quiet",
            "--json",
            "--",
            str(target),
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout)
    assert not report["errors"], report["errors"]
    verdicts = {
        line_owners[finding["start"]["line"]]
        for finding in report["results"]
        if any(finding["check_id"].endswith(rule) for rule in OPENGREP_EXCEPTION_RULES)
    }

    for case in OPENGREP_ISOLATED_CASES:
        for tainted in (True, False):
            if _opengrep_isolated_verdict(target_dir, case, tainted):
                verdicts.add((case.name, tainted))
    return verdicts


def _opengrep_isolated_verdict(target_dir, case: ConformanceCase, tainted: bool) -> bool:
    """Scan one case in a file of its own and return whether OpenGrep reports it."""
    name = f"{case.name.replace('-', '_')}_{'tainted' if tainted else 'clean'}"
    isolated_dir = target_dir / name
    isolated_dir.mkdir()
    target = isolated_dir / "review-patterns.py"
    case_source = _render(case.tainted if tainted else case.clean, SCANNERS[0])
    target.write_text(
        f"def case_{name}():\n"
        "    try:\n"
        "        work()\n"
        "    except Exception as exc:\n"
        f"{indent(case_source, '        ')}\n",
        encoding="utf-8",
    )
    result = subprocess.run(
        [OPENGREP, "scan", "--config", str(OPENGREP_RULES), "--quiet", "--json", "--", str(target)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout)
    assert not report["errors"], report["errors"]
    return any(
        any(finding["check_id"].endswith(rule) for rule in OPENGREP_EXCEPTION_RULES) for finding in report["results"]
    )


@pytest.fixture(scope="module")
def opengrep_credential_verdicts(tmp_path_factory: pytest.TempPathFactory) -> set[str]:
    assert OPENGREP is not None

    target_dir = tmp_path_factory.mktemp("opengrep-credential-conformance")
    config = target_dir / "credential-rule.yaml"
    config.write_text(
        dedent(
            """
            rules:
              - id: credential-literal-conformance
                languages: [python]
                severity: ERROR
                message: Credential literal reached a credential sink.
                mode: taint
                pattern-sources:
                  - pattern: '"admin"'
                pattern-sinks:
                  - patterns:
                      - pattern: username = $VALUE
                      - focus-metavariable: $VALUE
            """
        ),
        encoding="utf-8",
    )
    target = target_dir / "credential-conformance.py"
    source_lines: list[str] = []
    line_owners: dict[int, str] = {}
    for name, expression, _expected, _reason in CREDENTIAL_CONSTANT_CASES:
        block = (
            f"def case_{name.replace('-', '_')}():\n"
            "    try:\n"
            "        work()\n"
            "    except Exception:\n"
            f"        username = {expression}\n"
        )
        start_line = len(source_lines) + 1
        block_lines = block.splitlines()
        source_lines.extend(block_lines)
        source_lines.append("")
        for line in range(start_line, start_line + len(block_lines)):
            line_owners[line] = name
    target.write_text("\n".join(source_lines), encoding="utf-8")

    result = subprocess.run(
        [OPENGREP, "scan", "--config", str(config), "--quiet", "--json", "--", str(target)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout)
    assert not report["errors"], report["errors"]
    return {line_owners[finding["start"]["line"]] for finding in report["results"]}


@pytest.mark.parametrize(("scanner", "case"), _scanner_case_parameters())
@pytest.mark.parametrize("tainted", [True, False], ids=["tainted", "clean"])
def test_scanner_conformance(scanner: ScannerSpec, case: ConformanceCase, tainted: bool) -> None:
    source = _render(case.tainted if tainted else case.clean, scanner)

    assert scanner.scan(source) is tainted


if OPENGREP is not None:

    @pytest.mark.parametrize(("case", "tainted"), _opengrep_case_parameters())
    def test_opengrep_taint_conformance(
        opengrep_verdicts: set[tuple[str, bool]],
        case: ConformanceCase,
        tainted: bool,
    ) -> None:
        assert ((case.name, tainted) in opengrep_verdicts) is tainted

    @pytest.mark.parametrize(
        ("name", "_expression", "expected", "_reason"),
        [
            pytest.param(
                name,
                expression,
                expected,
                reason,
                marks=pytest.mark.xfail(reason=reason, strict=True) if reason else (),
                id=name,
            )
            for name, expression, expected, reason in CREDENTIAL_CONSTANT_CASES
        ],
    )
    def test_opengrep_credential_constant_conformance(
        opengrep_credential_verdicts: set[str],
        name: str,
        _expression: str,
        expected: bool,
        _reason: str | None,
    ) -> None:
        assert (name in opengrep_credential_verdicts) is expected
